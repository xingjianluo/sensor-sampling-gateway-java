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
 * <p><b>沿用历史统计口径（不改老计数）</b>：DROP_OLDEST 被顶掉的旧读数形成一条
 * BUFFER_OVERFLOW 结论并计入 {@code dropped}，但<b>不</b>计入 {@code rejectedTotal}
 * 与 {@code byReason[BUFFER_OVERFLOW]}；后者只统计“直接丢弃最新读数”。两类背压
 * 各自单列（{@code evictedOld} / {@code rejectedNew}），由对账台账负责分列与闭合。</p>
 *
 * <p>每个采样尝试（一个 {@code sensor#tick}）恰好终结于一次计数，不变量：</p>
 * <ul>
 *   <li>{@code submitted = enqueued + rejected}
 *       （隔离/采集失败/直接丢弃都计 submitted 且计对应 rejected 原因；被顶掉的旧读数
 *       早已计入 enqueued，不再重复计入 submitted/rejected）；</li>
 *   <li>消费侧 {@code consumed = accepted + (DATA_CORRUPT|TIMESTAMP_ANOMALY|OUT_OF_RANGE)}；</li>
 *   <li>{@code enqueued = consumed + evictedOld}（DROP_OLDEST 驱逐发生在消费前）；</li>
 *   <li>{@code dropped = evictedOld + rejectedNew}，其中
 *       {@code rejectedNew = byReason[BUFFER_OVERFLOW]}
 *       （DROP_NEWEST 及关闭期 BLOCK 的直接拒绝）；</li>
 *   <li>{@code submitted = accepted + rejected + evictedOld}；</li>
 *   <li>{@code unfinished}：关闭排空超时后仍滞留缓冲的已受理读数（SHUTDOWN_INCOMPLETE，
 *       已计入 enqueued/consumed 之外的未完成口径，保证生产总数可对账）。</li>
 * </ul>
 */
public final class StatsCollector {

    private final AtomicLong submitted = new AtomicLong();
    private final AtomicLong enqueued = new AtomicLong();
    private final AtomicLong consumed = new AtomicLong();
    private final AtomicLong accepted = new AtomicLong();
    private final AtomicLong rejectedTotal = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();
    private final AtomicLong evictedOld = new AtomicLong();
    private final AtomicLong unfinished = new AtomicLong();
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

    public void rejected(RejectReason reason) {
        rejectedTotal.incrementAndGet();
        byReason.get(reason).incrementAndGet();
        if (reason == RejectReason.BUFFER_OVERFLOW) {
            // 仅“直接丢弃最新读数”（DROP_NEWEST / 关闭期 BLOCK）计入 dropped 老口径；
            // DROP_OLDEST 驱逐走 droppedEviction()，两套计数互不污染。
            dropped.incrementAndGet();
        }
    }

    /** 记录 DROP_OLDEST 的驱逐：被顶掉的旧读数形成独立 BUFFER_OVERFLOW 结论，单列计数。 */
    public void droppedEviction() {
        evictedOld.incrementAndGet();
        dropped.incrementAndGet();
    }

    /** 记录一条关闭排空超时后仍滞留缓冲的未完成读数。 */
    public void unfinished() {
        unfinished.incrementAndGet();
    }

    public void acquisitionFailure() {
        acquisitionFailures.incrementAndGet();
    }

    public GatewayStats snapshot(List<String> isolated) {
        Map<RejectReason, Long> reasons = new EnumMap<>(RejectReason.class);
        byReason.forEach((k, v) -> reasons.put(k, v.get()));
        return new GatewayStats(
                submitted.get(), enqueued.get(), consumed.get(),
                accepted.get(), rejectedTotal.get(), dropped.get(),
                acquisitionFailures.get(),
                Map.copyOf(reasons),
                List.copyOf(isolated),
                evictedOld.get(), unfinished.get());
    }
}
