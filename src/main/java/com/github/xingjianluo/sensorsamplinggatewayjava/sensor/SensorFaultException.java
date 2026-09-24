package com.github.xingjianluo.sensorsamplinggatewayjava.sensor;

/**
 * 模拟设备故障。{@code recoverable} 为 true 表示瞬时故障（应经重试恢复），
 * false 表示持续故障（重试无果后应被隔离）。
 */
public final class SensorFaultException extends Exception {

    private final String sensorId;
    private final long tick;
    private final boolean recoverable;

    public SensorFaultException(String sensorId, long tick, String message, boolean recoverable) {
        super(message);
        this.sensorId = sensorId;
        this.tick = tick;
        this.recoverable = recoverable;
    }

    public String sensorId() {
        return sensorId;
    }

    public long tick() {
        return tick;
    }

    public boolean isRecoverable() {
        return recoverable;
    }
}
