package com.github.xingjianluo.sensorsamplinggatewayjava.sensor;

/**
 * 协作式停止信号：传感器（典型为测试包装器）声明不再有采样。
 *
 * <p>这不是设备故障：网关生产者捕获后应干净退出该传感器的节拍循环，
 * 不分配裁决、不计入任何拒绝/失败统计，从而让“采样拍数”在源头精确可复现。</p>
 */
public final class SamplingLimitReached extends RuntimeException {

    private final String sensorId;
    private final long tick;

    public SamplingLimitReached(String sensorId, long tick) {
        super("sampling limit reached for " + sensorId + " at tick " + tick);
        this.sensorId = sensorId;
        this.tick = tick;
    }

    public String sensorId() {
        return sensorId;
    }

    public long tick() {
        return tick;
    }
}
