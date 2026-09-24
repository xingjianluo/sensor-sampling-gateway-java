package com.github.xingjianluo.sensorsamplinggatewayjava.tests;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.RawReading;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.RejectReason;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SampleOutcome;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorSpec;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorType;
import com.github.xingjianluo.sensorsamplinggatewayjava.validation.ReadingValidator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 读数有效性判定单测：同一组固定输入在固定时钟下结论唯一、可复现，
 * 且超量程/时间戳异常/数据损坏三类拒绝原因互不混淆。每个用例打印输入与判定依据。
 */
class ReadingValidatorTest {

    private static final long NOW = 1_000_000L;

    private final ReadingValidator validator = new ReadingValidator();

    private SensorSpec spec() {
        return new SensorSpec("t1", SensorType.TEMPERATURE,
                100L, -40.0, 80.0,
                5_000L, 1_000L,
                2.0, 1.0, "rules-v1");
    }

    private RawReading reading(double value, long ts, boolean corrupt) {
        long checksum = RawReading.expectedChecksum("t1", 1L, value, ts);
        if (corrupt) {
            checksum ^= 0xdeadbeefL;
        }
        return new RawReading("t1", 1L, value, ts, checksum);
    }

    private void log(String name, RawReading in, SampleOutcome out) {
        System.out.printf("[%s] INPUT sensor=%s seq=%d value=%s ts=%d checksumValid=%s | OUTCOME=%s%n",
                name, in.sensorId(), in.seq(), in.value(), in.timestampMs(), in.checksumValid(),
                out instanceof SampleOutcome.Rejected r
                        ? "REJECTED(" + r.reason() + ") basis=" + r.basis()
                        : "ACCEPTED sample=" + ((SampleOutcome.Accepted) out).sample());
    }

    @Test
    void acceptsInRangeAndAppliesCalibration() {
        SensorSpec spec = spec();
        RawReading in = reading(20.0, NOW, false);
        SampleOutcome out = validator.validate(in, spec, NOW);
        log("accept", in, out);
        SampleOutcome.Accepted a = assertInstanceOf(SampleOutcome.Accepted.class, out);
        assertEquals(41.0, a.sample().calibratedValue(), 0.0001, "20*2+1=41");
        assertEquals("rules-v1", a.sample().ruleVersion());
    }

    @Test
    void rejectsOutOfRangeHigh() {
        RawReading in = reading(120.0, NOW, false);
        SampleOutcome out = validator.validate(in, spec(), NOW);
        log("range-high", in, out);
        SampleOutcome.Rejected r = assertInstanceOf(SampleOutcome.Rejected.class, out);
        assertEquals(RejectReason.OUT_OF_RANGE, r.reason());
        assertTrue(r.basis().contains("range=[-40,80]"));
    }

    @Test
    void rejectsOutOfRangeLow() {
        RawReading in = reading(-50.0, NOW, false);
        SampleOutcome out = validator.validate(in, spec(), NOW);
        log("range-low", in, out);
        assertEquals(RejectReason.OUT_OF_RANGE,
                ((SampleOutcome.Rejected) out).reason());
    }

    @Test
    void rejectsFutureTimestamp() {
        RawReading in = reading(20.0, NOW + 5_000L, false);
        SampleOutcome out = validator.validate(in, spec(), NOW);
        log("future-ts", in, out);
        assertEquals(RejectReason.FUTURE_TIMESTAMP,
                ((SampleOutcome.Rejected) out).reason());
    }

    @Test
    void rejectsStaleTimestamp() {
        RawReading in = reading(20.0, NOW - 60_000L, false);
        SampleOutcome out = validator.validate(in, spec(), NOW);
        log("stale-ts", in, out);
        assertEquals(RejectReason.STALE_TIMESTAMP,
                ((SampleOutcome.Rejected) out).reason());
    }

    @Test
    void rejectsCorruptDataFirstEvenWhenAlsoOutOfRange() {
        // 同时损坏且超量程：固定规则顺序下必须判定为 CORRUPT_DATA。
        RawReading in = reading(999.0, NOW, true);
        SampleOutcome out = validator.validate(in, spec(), NOW);
        log("corrupt-and-range", in, out);
        assertEquals(RejectReason.CORRUPT_DATA,
                ((SampleOutcome.Rejected) out).reason());
    }

    @Test
    void sameInputAndRuleVersionIsReproducible() {
        RawReading in = reading(25.0, NOW, false);
        SampleOutcome o1 = validator.validate(in, spec(), NOW);
        SampleOutcome o2 = validator.validate(in, spec(), NOW);
        double v1 = ((SampleOutcome.Accepted) o1).sample().calibratedValue();
        double v2 = ((SampleOutcome.Accepted) o2).sample().calibratedValue();
        System.out.printf("[reproducible] run1=%s run2=%s%n", v1, v2);
        assertEquals(v1, v2, 0.0);
    }
}
