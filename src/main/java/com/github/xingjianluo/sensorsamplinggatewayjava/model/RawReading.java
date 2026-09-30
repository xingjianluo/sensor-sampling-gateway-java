package com.github.xingjianluo.sensorsamplinggatewayjava.model;

/**
 * 一次原始采样。
 *
 * @param sensorId     传感器编号
 * @param tick         该传感器第几次采样（从 0 开始，单调递增，用于幂等与丢重判定）
 * @param timestampMs  读数携带的时间戳（毫秒，可能异常）
 * @param value        原始测量值（可能超量程；损坏时与 checksum 不一致）
 * @param checksum     对 (tick,timestampMs,value) 的校验和
 * @param rulesVersion 该读数被“受理”时生效的规则版本（运行中切换规则后，
 *                     切换前已入队的读数仍按旧版本判定）；
 *                     直接构造的旧调用方为 null，表示按当前配置版本处理
 */
public record RawReading(String sensorId, long tick, long timestampMs, double value, long checksum,
                         String rulesVersion) {

    /** 兼容旧签名：不携带规则版本（按当前配置版本判定）。 */
    public RawReading(String sensorId, long tick, long timestampMs, double value, long checksum) {
        this(sensorId, tick, timestampMs, value, checksum, null);
    }

    /** 返回携带指定受理规则版本的读数副本。 */
    public RawReading withRulesVersion(String version) {
        return new RawReading(sensorId, tick, timestampMs, value, checksum, version);
    }

    /** 传感器内唯一键：用于重复/遗漏核对。 */
    public String key() {
        return sensorId + "#" + tick;
    }
}
