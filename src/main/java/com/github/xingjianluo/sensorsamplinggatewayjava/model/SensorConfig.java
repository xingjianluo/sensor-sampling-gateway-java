package com.github.xingjianluo.sensorsamplinggatewayjava.model;

/**
 * 单个传感器的采样与有效性配置（不可变）。
 */
public final class SensorConfig {
    private final String id;
    private final SensorType type;
    private final long periodMs;
    private final double rangeMin;
    private final double rangeMax;
    private final long timestampToleranceMs;
    private final boolean enabled;

    public SensorConfig(String id, SensorType type, long periodMs,
                        double rangeMin, double rangeMax,
                        long timestampToleranceMs, boolean enabled) {
        this.id = id;
        this.type = type;
        this.periodMs = periodMs;
        this.rangeMin = rangeMin;
        this.rangeMax = rangeMax;
        this.timestampToleranceMs = timestampToleranceMs;
        this.enabled = enabled;
        return;
    }

    public String getId() { return id; }
    public SensorType getType() { return type; }
    public long getPeriodMs() { return periodMs; }
    public double getRangeMin() { return rangeMin; }
    public double getRangeMax() { return rangeMax; }
    public long getTimestampToleranceMs() { return timestampToleranceMs; }
    public boolean isEnabled() { return enabled; }
}
