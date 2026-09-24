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
 * <p>每个采样尝试（一个 {@code sensor#tick}）恰好终结于一次计数，不变量：</p>
 * <ul>
 *   <li>{@code submitted = enqueued + rejectedTotal}
 *       （隔离/采集失败/缓冲拒绝都计 submitted 且计对应 rejected 原因）；</li>
 *   <li>消费侧 {@code consumed = accepted + (DATA_CORRUPT|TIMESTAMP_ANOMALY|OUT_OF_RANGE)}；</li>
 *   <li>{@code enqueued = consumed + droppedOldest}（DROP_OLDEST 驱逐发生在消费前）；</li>
 *   <li>{@code dropped = DROP_OLDEST 驱逐数 + DROP_NEWEST(及关闭期BLOCK) 拒绝数}，
 *       即 {@code byReason[BUFFER_OVERFLOW] + droppedOldest}。驱逐不重复计入 rejectedTotal。</li>
 * </ul>
 */
public final class StatsCollector {

    private final AtomicLong submitted = new AtomicLong();
    private final AtomicLong enqueued = new AtomicLong();
    private final AtomicLong consumed = new AtomicLong();
    private final AtomicLong accepted = new AtomicLong();
    private final AtomicLong rejectedTotal = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();
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
            dropped.incrementAndGet();
        }
    }

    /** 记录 DROP_OLDEST 的驱逐：被驱逐的读数同样形成可区分的 BUFFER_OVERFLOW 结论。 */
    public void droppedEviction() {
        dropped.incrementAndGet();
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
                List.copyOf(isolated));
    }
}
