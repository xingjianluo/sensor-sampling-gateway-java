package com.github.xingjianluo.sensorsamplinggatewayjava.model;

/**
 * 单个传感器的完整采样配置：采样周期、量程、时间戳容忍度、线性校准参数与规则版本。
 *
 * @param sensorId        传感器唯一标识
 * @param type            传感器类型
 * @param periodMs        采样周期（毫秒）
 * @param minValue        量程下限（含）
 * @param maxValue        量程上限（含）
 * @param maxSkewPastMs   时间戳相对网关时钟允许的最大滞后（毫秒）
 * @param maxSkewFutureMs 时间戳相对网关时钟允许的最大超前（毫秒）
 * @param scale           线性校准系数：calibrated = raw * scale + offset
 * @param offset          线性校准偏移
 * @param ruleVersion     规则版本（配置/校准变更时必须递增），结论按版本可复现
 */
public record SensorSpec(
        String sensorId,
        SensorType type,
        long periodMs,
        double minValue,
        double maxValue,
        long maxSkewPastMs,
        long maxSkewFutureMs,
        double scale,
        double offset,
        String ruleVersion
) {
}
