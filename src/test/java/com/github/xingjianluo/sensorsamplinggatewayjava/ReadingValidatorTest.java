package com.github.xingjianluo.sensorsamplinggatewayjava;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.RawReading;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.RejectReason;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorConfig;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorType;
import com.github.xingjianluo.sensorsamplinggatewayjava.pipeline.ReadingValidator;
import com.github.xingjianluo.sensorsamplinggatewayjava.sensor.ScriptedSensor;
import com.github.xingjianluo.sensorsamplinggatewayjava.time.ManualClock;
import org.junit.jupiter.api.Test;

import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** 校验器：三类拒绝原因必须可区分，边界值有效，日志打印输入与判定依据。 */
class ReadingValidatorTest {

    private static final Logger LOG = Logger.getLogger(ReadingValidatorTest.class.getName());

    private final ManualClock clock = new ManualClock(1_000_000L);
    private final ReadingValidator validator = new ReadingValidator(clock);
    private final SensorConfig cfg = new SensorConfig(
            "temp-1", SensorType.TEMPERATURE, 100L, -40.0, 80.0, 1000L, true);

    private RawReading good(double value, long ts) {
        return new RawReading("temp-1", 0L, ts, value, ScriptedSensor.checksum(0L, ts, value));
    }

    @Test
    void acceptsValueWithinRangeAndTimeTolerance() {
        RawReading r = good(25.0, 1_000_500L);
        RejectReason reason = validator.validate(r, cfg);
        assertNull(reason);
        LOG.info(() -> "[ACCEPT-CASE] " + validator.describe(r, cfg, null));
    }

    @Test
    void boundaryValuesAreValid() {
        assertNull(validator.validate(good(-40.0, 1_000_000L), cfg));
        assertNull(validator.validate(good(80.0, 1_000_000L), cfg));
    }

    @Test
    void distinguishesCorruptTimestampAndRangeFailures() {
        // 1) 数据损坏（即使值与时间都正常）
        RawReading corrupt = new RawReading("temp-1", 1L, 1_000_000L, 25.0,
                ScriptedSensor.checksum(1L, 1_000_000L, 25.0) ^ 0xffL);
        assertEquals(RejectReason.DATA_CORRUPT, validator.validate(corrupt, cfg));
        LOG.info(() -> "[CORRUPT-CASE] " + validator.describe(corrupt, cfg, RejectReason.DATA_CORRUPT));

        // 2) 时间戳异常（校验和仍正确）
        RawReading late = good(25.0, 1_002_500L);
        assertEquals(RejectReason.TIMESTAMP_ANOMALY, validator.validate(late, cfg));
        LOG.info(() -> "[TS-CASE] " + validator.describe(late, cfg, RejectReason.TIMESTAMP_ANOMALY));

        // 3) 超量程（校验和与时间正常）
        RawReading over = good(99.0, 1_000_000L);
        assertEquals(RejectReason.OUT_OF_RANGE, validator.validate(over, cfg));
        LOG.info(() -> "[RANGE-CASE] " + validator.describe(over, cfg, RejectReason.OUT_OF_RANGE));
    }

    @Test
    void corruptionTakesPrecedenceOverRangeAndTimestamp() {
        // 同时损坏 + 超量程 + 时间戳异常：必须稳定判为 DATA_CORRUPT（固定优先级）
        RawReading multi = new RawReading("temp-1", 2L, 5_000_000L, 500.0, 42L);
        assertEquals(RejectReason.DATA_CORRUPT, validator.validate(multi, cfg));
        LOG.info(() -> "[PRECEDENCE-CASE] " + validator.describe(multi, cfg, RejectReason.DATA_CORRUPT));
    }
}
