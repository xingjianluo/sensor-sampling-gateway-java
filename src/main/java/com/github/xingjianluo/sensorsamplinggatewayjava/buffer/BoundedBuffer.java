package com.github.xingjianluo.sensorsamplinggatewayjava.buffer;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.OverflowPolicy;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.RawReading;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * 有界采样缓冲：容量与溢出策略可配置，线程安全。本类只负责排队语义，
 * 所有计数由网关的统一 StatsCollector 在调用侧记录（单一计数源，杜绝重复计数）。
 *
 * <p>规则版本盖章：读数在“真正受理（进入队列 / 被确认直接拒绝）”的那一刻，在缓冲内部
 * 锁保护下通过 {@code admitTagger} 盖上当前规则版本标签（{@link Staged}）。运行中切换
 * 规则版本同样在这把锁内完成标签版本翻转，因此“入队/拒绝”与“版本切换”线性化：
 * 切换前已受理的读数带着旧版本走完流程，切换后受理的读数带新版本。</p>
 *
 * <p>关闭语义：{@link #close()} 之后不再受理新读数并唤醒全部等待者；
 * 已在队列中的读数仍可被消费者排空，排空后 {@link #poll(long, TimeUnit)} 返回 null。</p>
 */
public final class BoundedBuffer {

    private final int capacity;
    private final OverflowPolicy policy;
    private final LinkedBlockingDeque<Staged> deque;
    /** 受理盖章器：在缓冲锁内、读数被受理/被直接拒绝时调用一次，给出当时的规则版本标签。 */
    private final Function<RawReading, String> admitTagger;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();
    private final Condition notEmpty = lock.newCondition();
    private boolean closed;

    public BoundedBuffer(int capacity, OverflowPolicy policy) {
        this(capacity, policy, r -> null);
    }

    public BoundedBuffer(int capacity, OverflowPolicy policy, Function<RawReading, String> admitTagger) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be >= 1");
        }
        this.capacity = capacity;
        this.policy = policy;
        this.deque = new LinkedBlockingDeque<>(capacity);
        this.admitTagger = admitTagger == null ? r -> null : admitTagger;
    }

    /** 已受理（带规则版本标签）的读数。 */
    public record Staged(RawReading reading, String rulesVersion) {
    }

    public OfferResult offer(RawReading reading) throws InterruptedException {
        return switch (policy) {
            case DROP_OLDEST -> offerDropOldest(reading);
            case DROP_NEWEST -> offerDropNewest(reading);
            case BLOCK -> offerBlocking(reading);
        };
    }

    private OfferResult offerDropOldest(RawReading reading) {
        Staged evicted = null;
        lock.lock();
        try {
            if (closed) {
                // 关闭期被直接拒绝也在锁内盖章：带切换后/关闭时的版本，归 REJECTED_NEW
                return OfferResult.rejected(reading, admitTagger.apply(reading), null, policy);
            }
            if (deque.size() >= capacity) {
                evicted = deque.pollFirst();
            }
            // 新读数在此刻受理：锁内盖章，随后切换版本不会影响它；被驱逐读数保留其旧标签
            Staged staged = new Staged(reading, admitTagger.apply(reading));
            boolean added = deque.offerLast(staged);
            if (added) {
                notEmpty.signal();
            }
            return added
                    ? OfferResult.enqueued(staged, evicted, policy)
                    : OfferResult.rejected(reading, staged.rulesVersion(), null, policy);
        } finally {
            lock.unlock();
        }
    }

    private OfferResult offerDropNewest(RawReading reading) {
        lock.lock();
        try {
            if (closed || deque.size() >= capacity) {
                return OfferResult.rejected(reading, admitTagger.apply(reading), null, policy);
            }
            Staged staged = new Staged(reading, admitTagger.apply(reading));
            boolean added = deque.offerLast(staged);
            if (added) {
                notEmpty.signal();
                return OfferResult.enqueued(staged, null, policy);
            }
            return OfferResult.rejected(reading, staged.rulesVersion(), null, policy);
        } finally {
            lock.unlock();
        }
    }

    private OfferResult offerBlocking(RawReading reading) throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (deque.size() >= capacity && !closed) {
                notFull.await();
            }
            if (closed) {
                return OfferResult.rejected(reading, admitTagger.apply(reading), null, policy);
            }
            Staged staged = new Staged(reading, admitTagger.apply(reading));
            boolean added = deque.offerLast(staged);
            if (added) {
                notEmpty.signal();
            }
            return added
                    ? OfferResult.enqueued(staged, null, policy)
                    : OfferResult.rejected(reading, staged.rulesVersion(), null, policy);
        } finally {
            lock.unlock();
        }
    }

    /** 阻塞取下一条；关闭且排空后返回 null。等待可被中断。 */
    public Staged poll() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (deque.isEmpty() && !closed) {
                notEmpty.await();
            }
            Staged s = deque.pollFirst();
            if (s != null) {
                notFull.signalAll();
            }
            return s;
        } finally {
            lock.unlock();
        }
    }

    /** 关闭前排空时使用：限时取下一条，超时返回 null。 */
    public Staged poll(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        lock.lockInterruptibly();
        try {
            while (deque.isEmpty() && !closed) {
                if (nanos <= 0L) {
                    return null;
                }
                nanos = notEmpty.awaitNanos(nanos);
            }
            Staged s = deque.pollFirst();
            if (s != null) {
                notFull.signalAll();
            }
            return s;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 关闭边界兜底：取出并返回当前仍滞留缓冲的全部读数（不改变关闭状态）。
     * 这些读数都已受理但未得到裁决，由调用方落为 SHUTDOWN_INCOMPLETE。
     */
    public java.util.List<Staged> drainRemaining() {
        lock.lock();
        try {
            java.util.List<Staged> remaining = new java.util.ArrayList<>(deque.size());
            Staged s;
            while ((s = deque.pollFirst()) != null) {
                remaining.add(s);
            }
            return remaining;
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        lock.lock();
        try {
            return deque.size();
        } finally {
            lock.unlock();
        }
    }

    public int capacity() {
        return capacity;
    }

    public int queued() {
        return size();
    }

    /** 标记不再接受新读数；唤醒所有等待者，已入队读数仍可被排空。 */
    public void close() {
        lock.lock();
        try {
            closed = true;
            notFull.signalAll();
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public boolean isClosed() {
        lock.lock();
        try {
            return closed;
        } finally {
            lock.unlock();
        }
    }

    public OverflowPolicy policy() {
        return policy;
    }

    /** offer 的确定结果。新受理读数与被驱逐读数都带各自受理时的规则版本标签。 */
    public record OfferResult(boolean enqueued, Staged staged, Staged evicted,
                              RawReading rejectedReading, String rejectedRulesVersion,
                              OverflowPolicy policy) {

        static OfferResult enqueued(Staged s, Staged evicted, OverflowPolicy p) {
            return new OfferResult(true, s, evicted, null, null, p);
        }

        static OfferResult rejected(RawReading r, String rulesVersion, Staged evicted, OverflowPolicy p) {
            return new OfferResult(false, null, evicted, r, rulesVersion, p);
        }
    }
}
