package com.github.xingjianluo.sensorsamplinggatewayjava.gateway;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.GatewayStats;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.RejectReason;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 全局并发计数。所有计数均为单调递增的长整型原子计数，杜绝并发下的重复/丢失计数。
 *
 * <p><b>口径保持兼容（不随对账改造改老计数）</b>：被顶掉（DROP_OLDEST 驱逐）的读数
 * 只计背压，不抬高拒绝总数；它与“直接拒绝/丢弃”作为两类背压单独分列。</p>
 *
 * <p>每个采样尝试（一个 {@code sensor#tick}）恰好终结于一次计数，不变量：</p>
 * <ul>
 *   <li>{@code submitted = enqueued + rejectedTotal}
 *       （隔离/采集失败/缓冲直接拒绝都计 submitted 且计对应 rejected 原因；
 *       驱逐的旧读数已计 enqueued，不再计 rejected，避免重复）；</li>
 *   <li>消费侧 {@code consumed = accepted + (DATA_CORRUPT|TIMESTAMP_ANOMALY|OUT_OF_RANGE)}；</li>
 *   <li>{@code enqueued = consumed + evictedOld + shutdownIncomplete}
 *       （驱逐发生在消费前；关闭排空超时滞留的已入队读数不再被消费）；</li>
 *   <li>{@code dropped = evictedOld + rejectedNew}（两类背压分列）；
 *       {@code rejectedNew = byReason[BUFFER_OVERFLOW]}（仅直接拒绝/丢弃最新者）；</li>
 *   <li>{@code submitted = accepted + rejectedTotal + evictedOld + shutdownIncomplete}
 *       （生产总数与各终结方式严格闭合，与对账台账互证）。</li>
 * </ul>
 */
public final class StatsCollector {

    private final AtomicLong submitted = new AtomicLong();
    private final AtomicLong enqueued = new AtomicLong();
    private final AtomicLong consumed = new AtomicLong();
    private final AtomicLong accepted = new AtomicLong();
    private final AtomicLong rejectedTotal = new AtomicLong();
    private final AtomicLong evictedOld = new AtomicLong();
    private final AtomicLong rejectedNew = new AtomicLong();
    private final AtomicLong shutdownIncomplete = new AtomicLong();
    private final AtomicLong acquisitionFailures = new AtomicLong();
    private final Map<RejectReason, AtomicLong> byReason = new EnumMap<>(RejectReason.class);

    public StatsCollector() {
        for (RejectReason r : RejectReason.values()) {
            byReason.put(r, new AtomicLong());
        }
    }

    public void submitted() {
        submitted.incrementAndGet();
    }

    public void enqueued() {
        enqueued.incrementAndGet();
    }

    public void consumed() {
        consumed.incrementAndGet();
    }

    public void accepted() {
        accepted.incrementAndGet();
    }

    /**
     * 直接拒绝/丢弃（最新者）：计入拒绝总数与对应原因；BUFFER_OVERFLOW 同时计
     * {@code rejectedNew} 背压。DROP_OLDEST 的驱逐不走这里（见 {@link #evictedOldest()}）。
     */
    public void rejected(RejectReason reason) {
        rejectedTotal.incrementAndGet();
        byReason.get(reason).incrementAndGet();
        if (reason == RejectReason.BUFFER_OVERFLOW) {
            rejectedNew.incrementAndGet();
        }
    }

    /**
     * DROP_OLDEST 顶掉最旧读数：只记背压 {@code evictedOld}（同时使 dropped+1），
     * <b>不</b>计入拒绝总数、不抬高 BUFFER_OVERFLOW 原因计数——保持既有统计口径。
     * 对账台账侧该读数仍作为 EVICTED_OLD 一类终结单列，账目闭合不依赖改老计数。
     */
    public void evictedOldest() {
        evictedOld.incrementAndGet();
    }

    /**
     * 关闭边界未完成：已提交读数在关闭排空超时/消费者中断后仍滞留缓冲，
     * 没得到接受或拒绝裁决。独立分列，不计入 rejectedTotal，对账报告以
     * {@code incomplete} 与不完整标记显式呈现。
     */
    public void incompleteAtShutdown() {
        shutdownIncomplete.incrementAndGet();
    }

    public void acquisitionFailure() {
        acquisitionFailures.incrementAndGet();
    }

    public GatewayStats snapshot(List<String> isolated) {
        Map<RejectReason, Long> reasons = new EnumMap<>(RejectReason.class);
        byReason.forEach((k, v) -> reasons.put(k, v.get()));
        long dropped = evictedOld.get() + rejectedNew.get();
        return new GatewayStats(
                submitted.get(), enqueued.get(), consumed.get(),
                accepted.get(), rejectedTotal.get(), dropped,
                evictedOld.get(), rejectedNew.get(), shutdownIncomplete.get(),
                acquisitionFailures.get(),
                Map.copyOf(reasons),
                List.copyOf(isolated));
    }
}
