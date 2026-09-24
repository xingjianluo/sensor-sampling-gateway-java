package com.github.xingjianluo.sensorsamplinggatewayjava.tests;

import com.github.xingjianluo.sensorsamplinggatewayjava.buffer.BoundedSamplingBuffer;
import com.github.xingjianluo.sensorsamplinggatewayjava.buffer.OfferResult;
import com.github.xingjianluo.sensorsamplinggatewayjava.config.OverflowPolicy;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.RawReading;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 有界缓冲单测：DROP_NEWEST / DROP_OLDEST 的确定结果，
 * 以及 BLOCK 策略“满则背压、有空位才放行”的行为。所有结果显式可见，不静默丢弃。
 */
class BoundedSamplingBufferTest {

    private RawReading r(long seq) {
        return new RawReading("s", seq, seq, 1000L,
                RawReading.expectedChecksum("s", seq, seq, 1000L));
    }

    @Test
    void dropNewestRejectsWhenFull() {
        BoundedSamplingBuffer buf = new BoundedSamplingBuffer(2, OverflowPolicy.DROP_NEWEST);
        assertEquals(OfferResult.Status.ENQUEUED, buf.offer(r(1)).status());
        assertEquals(OfferResult.Status.ENQUEUED, buf.offer(r(2)).status());
        OfferResult third = buf.offer(r(3));
        System.out.printf("[drop-newest] cap=2 offer seq=3 -> %s size=%d%n", third.status(), buf.size());
        assertEquals(OfferResult.Status.REJECTED_FULL, third.status());
        assertEquals(2, buf.size(), "缓冲不得溢出容量");
    }

    @Test
    void dropOldestEvictsAndReportsIt() throws Exception {
        BoundedSamplingBuffer buf = new BoundedSamplingBuffer(2, OverflowPolicy.DROP_OLDEST);
        buf.offer(r(1));
        buf.offer(r(2));
        OfferResult third = buf.offer(r(3));
        System.out.printf("[drop-oldest] cap=2 offer seq=3 -> %s evictedSeq=%d%n",
                third.status(), third.evicted().seq());
        assertEquals(OfferResult.Status.ENQUEUED_WITH_EVICTION, third.status());
        assertNotNull(third.evicted());
        assertEquals(1L, third.evicted().seq(), "被挤出的必须是最旧读数 seq=1");
        assertEquals(2L, buf.poll(1, TimeUnit.MILLISECONDS).seq(), "队首现在应是 seq=2");
    }

    @Test
    void blockParksProducerUntilConsumerFreesSlot() throws Exception {
        BoundedSamplingBuffer buf = new BoundedSamplingBuffer(1, OverflowPolicy.BLOCK);
        buf.put(r(1));
        AtomicBoolean done = new AtomicBoolean(false);
        Thread producer = new Thread(() -> {
            try {
                buf.put(r(2));
                done.set(true);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "test-producer");
        producer.start();

        Thread.sleep(50);
        assertFalse(done.get(), "缓冲满时生产者必须被背压阻塞");
        System.out.println("[block] producer parked while buffer full");

        RawReading consumed = buf.poll(1, TimeUnit.MILLISECONDS);
        assertEquals(1L, consumed.seq());
        producer.join(1000);
        assertTrue(done.get(), "消费腾出空位后生产者必须解除阻塞并写入");
        System.out.println("[block] producer resumed after consumer freed a slot");
        assertEquals(2L, buf.poll(1, TimeUnit.MILLISECONDS).seq());
    }

    @Test
    void pollReturnsNullOnTimeoutAndFifoOrder() throws Exception {
        BoundedSamplingBuffer buf = new BoundedSamplingBuffer(3, OverflowPolicy.BLOCK);
        assertNull(buf.poll(5, TimeUnit.MILLISECONDS), "空缓冲超时应返回 null");
        buf.put(r(7));
        buf.put(r(8));
        assertEquals(7L, buf.poll(1, TimeUnit.MILLISECONDS).seq());
        assertEquals(8L, buf.poll(1, TimeUnit.MILLISECONDS).seq());
        System.out.println("[fifo] empty-timeout=null, dequeue order 7 then 8");
    }
}
