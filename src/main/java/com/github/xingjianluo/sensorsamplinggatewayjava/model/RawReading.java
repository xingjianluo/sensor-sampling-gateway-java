package com.github.xingjianluo.sensorsamplinggatewayjava.model;

/**
 * 一次原始采样。
 *
 * @param sensorId    传感器编号
 * @param tick        该传感器第几次采样（从 0 开始，单调递增，用于幂等与丢重判定）
 * @param timestampMs 读数携带的时间戳（毫秒，可能异常）
 * @param value       原始测量值（可能超量程；损坏时与 checksum 不一致）
 * @param checksum    对 (tick,timestampMs,value) 的校验和
 */
public record RawReading(String sensorId, long tick, long timestampMs, double value, long checksum) {

    /** 传感器内唯一键：用于重复/遗漏核对。 */
    public String key() {
        return sensorId + "#" + tick;
    }
}
