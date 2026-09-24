package com.github.xingjianluo.sensorsamplinggatewayjava.calibration;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.Sample;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorSpec;

/**
 * 线性校准模型：calibrated = raw * scale + offset。
 * 相同输入与规则版本下输出确定、可复现。
 */
public final class CalibrationModel {

    private CalibrationModel() {
    }

    /**
     * 将已通过有效性校验的原始值校准为样本值。
     */
    public static double apply(SensorSpec spec, double rawValue) {
        return rawValue * spec.scale() + spec.offset();
    }

    /**
     * 使用更新后的 scale/offset 与新版本号生成新配置，旧版本保留可追溯。
     * 其余字段（量程、周期、时间戳容忍度等）保持不变。
     */
    public static SensorSpec withCalibration(SensorSpec spec, double scale, double offset, String newRuleVersion) {
        return new SensorSpec(
                spec.sensorId(),
                spec.type(),
                spec.periodMs(),
                spec.minValue(),
                spec.maxValue(),
                spec.maxSkewPastMs(),
                spec.maxSkewFutureMs(),
                scale,
                offset,
                newRuleVersion
        );
    }

    /**
     * 由原始值直接构造校准样本。
     */
    public static Sample toSample(SensorSpec spec, String sensorId, long seq,
                                  double rawValue, long timestampMs) {
        return new Sample(
                sensorId,
                seq,
                rawValue,
                apply(spec, rawValue),
                timestampMs,
                spec.ruleVersion()
        );
    }
}
