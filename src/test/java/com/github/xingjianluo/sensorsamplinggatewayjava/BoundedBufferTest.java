package com.github.xingjianluo.sensorsamplinggatewayjava;

import com.github.xingjianluo.sensorsamplinggatewayjava.buffer.BoundedBuffer;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.OverflowPolicy;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.RawReading;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 有界缓冲三种策略的确定性：DROP_NEWEST/DROP_OLDEST 不阻塞且结果可预测，BLOCK 满时等待。 */
class BoundedBufferTest {

    private RawReading r(long tick) {
        return new RawReading("s", tick, 0L, (double) tick, 0L);
    }

    @Test
    void dropNewestRejectsWhenFull() throws Exception {
        BoundedBuffer buf = new BoundedBuffer(2, OverflowPolicy.DROP_NEWEST);
        assertTrue(buf.offer(r(0)).enqueued());
        assertTrue(buf.offer(r(1)).enqueued());
        BoundedBuffer.OfferResult third = buf.offer(r(2));
        assertFalse(third.enqueued());
        assertEquals(2L, third.rejectedReading().tick());
        // 最旧数据仍在
        RawReading a = buf.poll(1, TimeUnit.MILLISECONDS);
        RawReading b = buf.poll(1, TimeUnit.MILLISECONDS);
        assertNotNull(a);
        assertNotNull(b);
        assertEquals(0L, a.tick());
        assertEquals(1L, b.tick());
    }

    @Test
    void dropOldestEvictsDeterministically() throws Exception {
        BoundedBuffer buf = new BoundedBuffer(2, OverflowPolicy.DROP_OLDEST);
        buf.offer(r(0));
        buf.offer(r(1));
        BoundedBuffer.OfferResult third = buf.offer(r(2));
        assertTrue(third.enqueued());
        assertEquals(0L, third.evictedReading().tick());
        RawReading c = buf.poll(1, TimeUnit.MILLISECONDS);
        RawReading d = buf.poll(1, TimeUnit.MILLISECONDS);
        assertNotNull(c);
        assertNotNull(d);
        assertEquals(1L, c.tick());
        assertEquals(2L, d.tick());
    }

    @Test
    void blockPolicyWaitsUntilSpaceAvailable() throws Exception {
        BoundedBuffer buf = new BoundedBuffer(1, OverflowPolicy.BLOCK);
        assertTrue(buf.offer(r(0)).enqueued());

        AtomicReference<BoundedBuffer.OfferResult> ref = new AtomicReference<>();
        Thread producer = new Thread(() -> {
            try {
                ref.set(buf.offer(r(1)));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        producer.start();
        // 满队：生产者被阻塞，尚未入队
        Thread.sleep(100);
        assertNull(ref.get());
        assertEquals(1, buf.size());

        RawReading e = buf.poll(1, TimeUnit.MILLISECONDS);
        assertNotNull(e);
        assertEquals(0L, e.tick());
        producer.join(2000);
        assertNotNull(ref.get());
        assertTrue(ref.get().enqueued());
    }

    @Test
    void closeDrainsEnqueuedAndRejectsNew() throws Exception {
        BoundedBuffer buf = new BoundedBuffer(2, OverflowPolicy.BLOCK);
        buf.offer(r(7));
        buf.close();
        // 已入队的可排空
        RawReading f = buf.poll(1, TimeUnit.MILLISECONDS);
        assertNotNull(f);
        assertEquals(7L, f.tick());
        // 排空后 poll 返回 null
        assertNull(buf.poll(50, TimeUnit.MILLISECONDS));
        // 关闭后不再受理
        assertFalse(buf.offer(r(8)).enqueued());
    }
}
