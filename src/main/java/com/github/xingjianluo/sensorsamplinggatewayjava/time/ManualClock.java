package com.github.xingjianluo.sensorsamplinggatewayjava.time;

/** 测试用手动时钟，时间只随测试推进，确保时间戳判定完全确定、可复现。 */
public final class ManualClock implements Clock {
    private volatile long current;

    public ManualClock(long start) {
        this.current = start;
    }

    @Override
    public long nowMillis() {
        return current;
    }

    /** 向前推进 deltaMs（不允许倒流，避免与时间戳单调性规则冲突）。 */
    public void advance(long deltaMs) {
        if (deltaMs < 0) {
            throw new IllegalArgumentException("clock cannot move backwards: " + deltaMs);
        }
        current += deltaMs;
    }

    public void setTime(long t) {
        current = t;
    }
}
