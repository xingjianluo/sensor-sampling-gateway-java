package com.github.xingjianluo.sensorsamplinggatewayjava.buffer;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.OverflowPolicy;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.RawReading;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 有界采样缓冲：容量与溢出策略可配置，线程安全。本类只负责排队语义，
 * 所有计数由网关的统一 StatsCollector 在调用侧记录（单一计数源，杜绝重复计数）。
 *
 * <p>关闭语义：{@link #close()} 之后不再受理新读数并唤醒全部等待者；
 * 已在队列中的读数仍可被消费者排空，排空后 {@link #poll(long, TimeUnit)} 返回 null。</p>
 */
public final class BoundedBuffer {

    private final int capacity;
    private final OverflowPolicy policy;
    private final LinkedBlockingDeque<RawReading> deque;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();
    private final Condition notEmpty = lock.newCondition();
    private boolean closed;

    public BoundedBuffer(int capacity, OverflowPolicy policy) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be >= 1");
        }
        this.capacity = capacity;
        this.policy = policy;
        this.deque = new LinkedBlockingDeque<>(capacity);
    }

    public OfferResult offer(RawReading reading) throws InterruptedException {
        switch (policy) {
            case DROP_OLDEST:
                return offerDropOldest(reading);
            case DROP_NEWEST:
                return offerDropNewest(reading);
            case BLOCK:
            default:
                return offerBlocking(reading);
        }
    }

    private OfferResult offerDropOldest(RawReading reading) {
        RawReading evicted = null;
        lock.lock();
        try {
            if (closed) {
                return OfferResult.rejected(reading, policy);
            }
            if (deque.size() >= capacity) {
                evicted = deque.pollFirst();
            }
            boolean added = deque.offerLast(reading);
            if (added) {
                notEmpty.signal();
            }
            return added
                    ? OfferResult.enqueued(reading, evicted, policy)
                    : OfferResult.rejected(reading, policy);
        } finally {
            lock.unlock();
        }
    }

    private OfferResult offerDropNewest(RawReading reading) {
        lock.lock();
        try {
            if (closed || deque.size() >= capacity) {
                return OfferResult.rejected(reading, policy);
            }
            boolean added = deque.offerLast(reading);
            if (added) {
                notEmpty.signal();
                return OfferResult.enqueued(reading, null, policy);
            }
            return OfferResult.rejected(reading, policy);
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
                return OfferResult.rejected(reading, policy);
            }
            boolean added = deque.offerLast(reading);
            if (added) {
                notEmpty.signal();
            }
            return added
                    ? OfferResult.enqueued(reading, null, policy)
                    : OfferResult.rejected(reading, policy);
        } finally {
            lock.unlock();
        }
    }

    /** 阻塞取下一条；关闭且排空后返回 null。等待可被中断。 */
    public RawReading poll() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (deque.isEmpty() && !closed) {
                notEmpty.await();
            }
            RawReading r = deque.pollFirst();
            if (r != null) {
                notFull.signalAll();
            }
            return r;
        } finally {
            lock.unlock();
        }
    }

    /** 关闭前排空时使用：限时取下一条，超时返回 null。 */
    public RawReading poll(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        lock.lockInterruptibly();
        try {
            while (deque.isEmpty() && !closed) {
                if (nanos <= 0L) {
                    return null;
                }
                nanos = notEmpty.awaitNanos(nanos);
            }
            RawReading r = deque.pollFirst();
            if (r != null) {
                notFull.signalAll();
            }
            return r;
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

    /** offer 的确定结果。 */
    public record OfferResult(boolean enqueued, RawReading rejectedReading,
                              RawReading evictedReading, OverflowPolicy policy) {

        static OfferResult enqueued(RawReading r, RawReading evicted, OverflowPolicy p) {
            return new OfferResult(true, null, evicted, p);
        }

        static OfferResult rejected(RawReading r, OverflowPolicy p) {
            return new OfferResult(false, r, null, p);
        }
    }
}
