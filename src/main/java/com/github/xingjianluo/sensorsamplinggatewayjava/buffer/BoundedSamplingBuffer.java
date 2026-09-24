package com.github.xingjianluo.sensorsamplinggatewayjava.buffer;

import com.github.xingjianluo.sensorsamplinggatewayjava.config.OverflowPolicy;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.RawReading;

import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 有界采样缓冲。容量与溢出策略固定且可配置，所有入队/出队线程安全。
 *
 * <ul>
 *   <li>{@code BLOCK}：缓冲满时 {@link #put} 阻塞生产者，形成背压，不丢数；</li>
 *   <li>{@code DROP_NEWEST}：满时 {@link #offer} 立即拒绝新读数，调用方负责按 BUFFER_OVERFLOW 记账；</li>
 *   <li>{@code DROP_OLDEST}：满时挤出最旧读数再入队，被挤出读数交由调用方记账。</li>
 * </ul>
 * 拒绝/挤出都通过返回值显式表达，绝不静默丢弃。
 */
public class BoundedSamplingBuffer {

    private final int capacity;
    private final OverflowPolicy overflowPolicy;
    private final ArrayDeque<RawReading> queue = new ArrayDeque<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();
    private final Condition notEmpty = lock.newCondition();

    public BoundedSamplingBuffer(int capacity, OverflowPolicy overflowPolicy) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("buffer capacity must be positive: " + capacity);
        }
        this.capacity = capacity;
        this.overflowPolicy = overflowPolicy;
    }

    public int capacity() {
        return capacity;
    }

    public OverflowPolicy overflowPolicy() {
        return overflowPolicy;
    }

    public int size() {
        lock.lock();
        try {
            return queue.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 非阻塞写。仅 DROP_NEWEST / DROP_OLDEST 策略下使用；
     * BLOCK 策略请使用 {@link #put(RawReading)}（此处对 BLOCK 也给出确定的拒绝结果以便统一调用）。
     */
    public OfferResult offer(RawReading reading) {
        lock.lock();
        try {
            if (queue.size() < capacity) {
                queue.addLast(reading);
                notEmpty.signal();
                return OfferResult.enqueued();
            }
            return switch (overflowPolicy) {
                case DROP_OLDEST -> {
                    RawReading oldest = queue.pollFirst();
                    queue.addLast(reading);
                    notEmpty.signal();
                    yield OfferResult.enqueuedWithEviction(oldest);
                }
                case DROP_NEWEST, BLOCK -> OfferResult.rejectedFull();
            };
        } finally {
            lock.unlock();
        }
    }

    /** 缓冲满时阻塞直到有空位（BLOCK 背压语义）。 */
    public void put(RawReading reading) throws InterruptedException {
        lock.lock();
        try {
            while (queue.size() >= capacity) {
                notFull.await();
            }
            queue.addLast(reading);
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    /** 限时阻塞出队一条读数；超时返回 null。 */
    public RawReading poll(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        lock.lockInterruptibly();
        try {
            while (queue.isEmpty()) {
                if (nanos <= 0L) {
                    return null;
                }
                nanos = notEmpty.awaitNanos(nanos);
            }
            RawReading r = queue.pollFirst();
            notFull.signal();
            return r;
        } finally {
            lock.unlock();
        }
    }
}
