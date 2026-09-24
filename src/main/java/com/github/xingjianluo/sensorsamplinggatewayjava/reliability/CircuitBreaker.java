package com.github.xingjianluo.sensorsamplinggatewayjava.reliability;

import java.util.concurrent.atomic.AtomicReference;

/**
 * 单传感器故障隔离断路器（线程安全）。
 *
 * <ul>
 *   <li>{@code CLOSED}：正常放行；连续失败达到阈值后转 {@code OPEN}（隔离）；</li>
 *   <li>{@code OPEN}：拒绝一切采样；冷却期后由 {@link #allowRequest} 原子地
 *       放行一次探测并转 {@code HALF_OPEN}；</li>
 *   <li>{@code HALF_OPEN}：探测成功转 {@code CLOSED} 并清零，失败回到 {@code OPEN}
 *       并重新计时冷却。持续失败的传感器因此被隔离，不影响其他传感器。</li>
 * </ul>
 */
public final class CircuitBreaker {

    public enum State { CLOSED, OPEN, HALF_OPEN }

    private static final class Snapshot {
        final State state;
        final int failures;
        final long openedAt;

        Snapshot(State state, int failures, long openedAt) {
            this.state = state;
            this.failures = failures;
            this.openedAt = openedAt;
        }
    }

    private final String sensorId;
    private final int failureThreshold;
    private final long cooldownMs;
    private final AtomicReference<Snapshot> ref = new AtomicReference<>(new Snapshot(State.CLOSED, 0, 0L));

    public CircuitBreaker(String sensorId, int failureThreshold, long cooldownMs) {
        if (failureThreshold < 1) {
            throw new IllegalArgumentException("failureThreshold must be >= 1");
        }
        this.sensorId = sensorId;
        this.failureThreshold = failureThreshold;
        this.cooldownMs = cooldownMs;
    }

    /**
     * 是否允许本次采样。OPEN 且冷却期满时，原子地只放行一个探测线程。
     */
    public boolean allowRequest(long nowMs) {
        Snapshot s = ref.get();
        if (s.state != State.OPEN) {
            return true;
        }
        if (nowMs - s.openedAt < cooldownMs) {
            return false;
        }
        return ref.compareAndSet(s, new Snapshot(State.HALF_OPEN, s.failures, s.openedAt));
    }

    public void recordSuccess() {
        ref.set(new Snapshot(State.CLOSED, 0, 0L));
    }

    public void recordFailure(long nowMs) {
        Snapshot s;
        do {
            s = ref.get();
            int failures = s.state == State.CLOSED ? s.failures + 1 : s.failures;
            if (failures >= failureThreshold || s.state == State.HALF_OPEN) {
                ref.set(new Snapshot(State.OPEN, failures, nowMs));
                return;
            }
            if (ref.compareAndSet(s, new Snapshot(State.CLOSED, failures, 0L))) {
                return;
            }
        } while (true);
    }

    public State state(long nowMs) {
        return ref.get().state;
    }

    public String sensorId() {
        return sensorId;
    }

    public int consecutiveFailures() {
        return ref.get().failures;
    }
}
