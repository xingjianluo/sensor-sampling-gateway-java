package com.github.xingjianluo.sensorsamplinggatewayjava.buffer;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.RawReading;

/**
 * 向有界缓冲写入读数的确定性结果。
 *
 * @param status  写入状态
 * @param evicted 当 DROP_OLDEST 时被挤出的最旧读数；否则为 null
 */
public record OfferResult(Status status, RawReading evicted) {

    public enum Status {
        /** 成功入缓冲 */
        ENQUEUED,
        /** 缓冲满，按 DROP_NEWEST 拒绝新读数 */
        REJECTED_FULL,
        /** 入缓冲成功，但按 DROP_OLDEST 挤出了一个旧读数 */
        ENQUEUED_WITH_EVICTION
    }

    public static OfferResult enqueued() {
        return new OfferResult(Status.ENQUEUED, null);
    }

    public static OfferResult rejectedFull() {
        return new OfferResult(Status.REJECTED_FULL, null);
    }

    public static OfferResult enqueuedWithEviction(RawReading evicted) {
        return new OfferResult(Status.ENQUEUED_WITH_EVICTION, evicted);
    }
}
