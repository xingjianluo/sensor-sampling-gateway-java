package com.github.xingjianluo.sensorsamplinggatewayjava.model;

/**
 * 通过全部有效性校验并完成校准的样本。
 *
 * @param sensorId        传感器标识
 * @param seq             原始读数序列号
 * @param rawValue        原始值
 * @param calibratedValue 校准后的值
 * @param timestampMs     原始时间戳
 * @param ruleVersion     判定时使用的规则版本
 */
public record Sample(
        String sensorId,
        long seq,
        double rawValue,
        double calibratedValue,
        long timestampMs,
        String ruleVersion
) {
}
