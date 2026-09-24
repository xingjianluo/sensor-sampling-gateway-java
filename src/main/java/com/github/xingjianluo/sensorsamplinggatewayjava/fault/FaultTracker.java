package com.github.xingjianluo.sensorsamplinggatewayjava.fault;

import com.github.xingjianluo.sensorsamplinggatewayjava.config.RetryPolicy;

import java.time.Duration;

/**
 * 单个传感器的故障状态机。
 *
 * <p>一个“采样轮次”是一个 period 内的尝试序列（首次 + 有界重试）。
 * <ul>
 *   <li>轮次内的瞬时故障：由采样引擎按 {@link RetryPolicy#backoffFor(int)} 退避后重试，
 *       退避只暂停该传感器当前轮次，不影响其他传感器；</li>
 *   <li>轮次最终仍失败：{@link #recordFailure(boolean, long)} 使连续失败轮次 +1，
 *       并设置跨轮退避窗口；</li>
 *   <li>连续失败轮次达到 failureThreshold：隔离传感器，此后不进入正常调度，
 *       仅在 probeInterval 到期时放行一次恢复探测；</li>
 *   <li>任意轮次成功：{@link #recordSuccess()} 全部状态复位。</li>
 * </ul>
 * 隔离只影响该传感器自身的调度，其他传感器不受影响。所有时钟均可注入。
 */
public class FaultTracker {

    private final String sensorId;
    private final RetryPolicy policy;

    private int consecutiveFailures;
    private boolean isolated;
    private long nextAllowedMs = Long.MIN_VALUE;

    public FaultTracker(String sensorId, RetryPolicy policy) {
        this.sensorId = sensorId;
        this.policy = policy;
    }

    public String sensorId() {
        return sensorId;
    }

    public boolean isIsolated() {
        return isolated;
    }

    public int consecutiveFailures() {
        return consecutiveFailures;
    }

    /** 当前跨轮/隔离窗口放行的最早时间（测试观测用）。 */
    public long waitUntilMs() {
        return nextAllowedMs;
    }

    /**
     * 采样时刻决策。
     *
     * @return PROBE 正常尝试（隔离传感器在探测窗口到期时也返回 PROBE）；
     *         SKIP 非隔离但处于跨轮退避窗口；ISOLATED 已隔离且探测窗口未到
     */
    public FaultDecision decide(long nowMs) {
        if (isolated) {
            return nowMs >= nextAllowedMs ? FaultDecision.PROBE : FaultDecision.ISOLATED;
        }
        if (consecutiveFailures > 0 && nowMs < nextAllowedMs) {
            return FaultDecision.SKIP;
        }
        return FaultDecision.PROBE;
    }

    /**
     * 记录一个采样轮次失败（重试耗尽或持续故障）。
     *
     * @param permanent 是否持续故障；持续故障立即隔离
     */
    public void recordFailure(boolean permanent, long nowMs) {
        consecutiveFailures++;
        if (permanent || consecutiveFailures >= policy.failureThreshold()) {
            isolated = true;
            nextAllowedMs = nowMs + policy.probeInterval().toMillis();
        } else {
            // 跨轮退避：随连续失败轮次指数增长，封顶 maxBackoff。
            Duration backoff = policy.backoffFor(consecutiveFailures);
            nextAllowedMs = nowMs + backoff.toMillis();
        }
    }

    /**
     * 记录一个采样轮次成功，完全复位（含隔离恢复）。
     */
    public void recordSuccess() {
        consecutiveFailures = 0;
        isolated = false;
        nextAllowedMs = Long.MIN_VALUE;
    }

    /** 第 retryNumber 次重试前的退避时长（委托给重试策略）。 */
    public Duration nextBackoff(int retryNumber) {
        return policy.backoffFor(retryNumber);
    }
}
