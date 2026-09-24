package com.github.xingjianluo.sensorsamplinggatewayjava.pipeline;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.RawReading;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.RejectReason;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorConfig;
import com.github.xingjianluo.sensorsamplinggatewayjava.sensor.ScriptedSensor;
import com.github.xingjianluo.sensorsamplinggatewayjava.time.Clock;

/**
 * 读数有效性校验。固定优先级（保证同输入同结论、拒绝原因可区分）：
 * <ol>
 *   <li>{@link RejectReason#DATA_CORRUPT}：校验和不匹配，数值不可信；</li>
 *   <li>{@link RejectReason#TIMESTAMP_ANOMALY}：|读数时间戳 - 网关时钟| 超过容差；</li>
 *   <li>{@link RejectReason#OUT_OF_RANGE}：值超出 [rangeMin, rangeMax]。</li>
 * </ol>
 * 边界值视为有效（闭区间）。
 */
public final class ReadingValidator {

    private final Clock clock;

    public ReadingValidator(Clock clock) {
        this.clock = clock;
    }

    public RejectReason validate(RawReading reading, SensorConfig config) {
        long expected = ScriptedSensor.checksum(reading.tick(), reading.timestampMs(), reading.value());
        if (expected != reading.checksum()) {
            return RejectReason.DATA_CORRUPT;
        }
        long now = clock.nowMillis();
        long drift = Math.abs(reading.timestampMs() - now);
        if (drift > config.getTimestampToleranceMs()) {
            return RejectReason.TIMESTAMP_ANOMALY;
        }
        if (reading.value() < config.getRangeMin() || reading.value() > config.getRangeMax()) {
            return RejectReason.OUT_OF_RANGE;
        }
        return null;
    }

    /** 判定依据描述：打印输入与所依据的规则，供日志与可解释的裁决记录使用。 */
    public String describe(RawReading reading, SensorConfig config, RejectReason reason) {
        long now = clock.nowMillis();
        long drift = Math.abs(reading.timestampMs() - now);
        long expected = ScriptedSensor.checksum(reading.tick(), reading.timestampMs(), reading.value());
        String input = String.format(
                "input{sensor=%s,tick=%d,ts=%d,value=%.4f,checksum=%d,now=%d,drift=%dms}",
                reading.sensorId(), reading.tick(), reading.timestampMs(), reading.value(),
                reading.checksum(), now, drift);
        String rule = switch (reason == null ? "ACCEPT" : reason.name()) {
            case "DATA_CORRUPT" -> String.format(
                    "rule=checksum(expected=%d,actual=%d) -> DATA_CORRUPT", expected, reading.checksum());
            case "TIMESTAMP_ANOMALY" -> String.format(
                    "rule=|ts-now|=%dms > tolerance=%dms -> TIMESTAMP_ANOMALY",
                    drift, config.getTimestampToleranceMs());
            case "OUT_OF_RANGE" -> String.format(
                    "rule=%.4f outside [%.4f,%.4f] -> OUT_OF_RANGE",
                    reading.value(), config.getRangeMin(), config.getRangeMax());
            default -> String.format(
                    "rule=checksum ok,drift %dms<=tolerance %dms,%.4f in [%.4f,%.4f] -> ACCEPT",
                    drift, config.getTimestampToleranceMs(), reading.value(),
                    config.getRangeMin(), config.getRangeMax());
        };
        return input + "; " + rule;
    }
}
