package com.github.xingjianluo.sensorsamplinggatewayjava.model;

import java.util.List;
import java.util.Map;

/**
 * 网关计数快照（不可变）。所有计数均为单调递增的长整型，用于并发下的
 * “不重复、不遗漏”核对。沿用历史口径（旧看板/脚本不变）：
 * <ul>
 *   <li>{@code submitted = enqueued + rejected}；</li>
 *   <li>{@code consumed = accepted + (DATA_CORRUPT|TIMESTAMP_ANOMALY|OUT_OF_RANGE)}；</li>
 *   <li>{@code enqueued = consumed + evictedOld}（DROP_OLDEST 驱逐发生在消费前）；</li>
 *   <li>{@code dropped = evictedOld + rejectedNew}（被顶掉与直接丢弃分列），
 *       其中 {@code rejectedNew = rejectedByReason[BUFFER_OVERFLOW]}；</li>
 *   <li>{@code submitted = accepted + rejected + evictedOld}
 *       （被顶掉读数是一条独立结论，但不重复计入 rejected 老口径）；</li>
 *   <li>{@code unfinished}：关闭排空超时后仍滞留缓冲、未完成判定的已受理读数。</li>
 * </ul>
 */
public record GatewayStats(long submitted, long enqueued, long consumed,
                           long accepted, long rejected, long dropped,
                           long acquisitionFailures,
                           Map<RejectReason, Long> rejectedByReason,
                           List<String> isolatedSensors,
                           long evictedOld, long unfinished) {

    /** 直接丢弃（DROP_NEWEST / 关闭期 BLOCK）数 = BUFFER_OVERFLOW 老口径计数。 */
    public long rejectedNew() {
        return rejectedByReason.getOrDefault(RejectReason.BUFFER_OVERFLOW, 0L);
    }
}
