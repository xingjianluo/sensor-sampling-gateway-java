package com.github.xingjianluo.sensorsamplinggatewayjava.validation;

import com.github.xingjianluo.sensorsamplinggatewayjava.calibration.CalibrationModel;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.RawReading;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.RejectReason;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.Sample;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SampleOutcome;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorSpec;

/**
 * 读数有效性校验器，按固定顺序应用规则：
 * 校验和（损坏） → 时间戳（超前/滞后） → 量程。
 * 任一规则不通过即返回携带可区分原因与判定依据的拒绝结论。
 * 顺序固定保证同一读数无论何时校验，得到的拒绝原因一致、可复现。
 */
public class ReadingValidator {

    public ReadingValidator() {
    }

    /**
     * 校验单个读数。
     *
     * @param reading 原始读数
     * @param spec    传感器配置与规则版本
     * @param nowMs   网关当前时间（时间戳判定基准，可注入以保证复现）
     */
    public SampleOutcome validate(RawReading reading, SensorSpec spec, long nowMs) {
        // 规则 1：数据损坏（校验和）。损坏读数无法信任任何字段，最先判定。
        if (!reading.checksumValid()) {
            long expected = RawReading.expectedChecksum(
                    reading.sensorId(), reading.seq(), reading.value(), reading.timestampMs());
            String basis = String.format(
                    "CORRUPT_DATA sensor=%s seq=%d rule=%s checksum=%d expected=%d",
                    reading.sensorId(), reading.seq(), spec.ruleVersion(),
                    reading.checksum(), expected);
            return new SampleOutcome.Rejected(reading, RejectReason.CORRUPT_DATA, basis);
        }

        // 规则 2：时间戳异常。先判未来，再判过去，原因可区分。
        long ts = reading.timestampMs();
        if (ts > nowMs + spec.maxSkewFutureMs()) {
            String basis = String.format(
                    "FUTURE_TIMESTAMP sensor=%s seq=%d rule=%s ts=%d now=%d allowedAhead=%d delta=%d",
                    reading.sensorId(), reading.seq(), spec.ruleVersion(),
                    ts, nowMs, spec.maxSkewFutureMs(), ts - nowMs);
            return new SampleOutcome.Rejected(reading, RejectReason.FUTURE_TIMESTAMP, basis);
        }
        if (ts < nowMs - spec.maxSkewPastMs()) {
            String basis = String.format(
                    "STALE_TIMESTAMP sensor=%s seq=%d rule=%s ts=%d now=%d allowedBehind=%d delta=%d",
                    reading.sensorId(), reading.seq(), spec.ruleVersion(),
                    ts, nowMs, spec.maxSkewPastMs(), nowMs - ts);
            return new SampleOutcome.Rejected(reading, RejectReason.STALE_TIMESTAMP, basis);
        }

        // 规则 3：量程（校验通过的数据才值得判断物理范围）。
        double v = reading.value();
        if (Double.isNaN(v) || v < spec.minValue() || v > spec.maxValue()) {
            String basis = String.format(
                    "OUT_OF_RANGE sensor=%s seq=%d rule=%s value=%s range=[%s,%s]",
                    reading.sensorId(), reading.seq(), spec.ruleVersion(),
                    fmt(v), fmt(spec.minValue()), fmt(spec.maxValue()));
            return new SampleOutcome.Rejected(reading, RejectReason.OUT_OF_RANGE, basis);
        }

        // 全部通过：做线性校准并产出样本。
        Sample sample = CalibrationModel.toSample(
                spec, reading.sensorId(), reading.seq(), v, ts);
        return new SampleOutcome.Accepted(sample);
    }

    private static String fmt(double d) {
        if (d == (long) d) {
            return Long.toString((long) d);
        }
        return Double.toString(d);
    }
}
