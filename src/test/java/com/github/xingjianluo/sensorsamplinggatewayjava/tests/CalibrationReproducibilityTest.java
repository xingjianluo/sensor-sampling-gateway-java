package com.github.xingjianluo.sensorsamplinggatewayjava.tests;

import com.github.xingjianluo.sensorsamplinggatewayjava.calibration.CalibrationModel;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.RawReading;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.Sample;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SampleOutcome;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorSpec;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorType;
import com.github.xingjianluo.sensorsamplinggatewayjava.registry.SensorRegistry;
import com.github.xingjianluo.sensorsamplinggatewayjava.validation.ReadingValidator;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 校准与规则版本可复现性单测：
 * 相同输入在相同规则版本下结论恒定；校准更新产生新版本后结论可区分；旧版本保留可追溯；
 * 登记表拒绝版本回退。
 */
class CalibrationReproducibilityTest {

    private SensorSpec v1() {
        return new SensorSpec("t", SensorType.TEMPERATURE, 100L,
                -40.0, 80.0, 60_000L, 60_000L, 2.0, 1.0, "v1");
    }

    private RawReading fixedReading(double raw, String checksumSensorId) {
        long ts = 1_000_000L;
        return new RawReading("t", 1L, raw, ts,
                RawReading.expectedChecksum("t", 1L, raw, ts));
    }

    @Test
    void sameInputSameVersionProducesSameResult() {
        ReadingValidator v = new ReadingValidator();
        SensorSpec spec = v1();
        RawReading in = fixedReading(20.0, "t");
        double c1 = ((SampleOutcome.Accepted) v.validate(in, spec, 1_000_000L))
                .sample().calibratedValue();
        double c2 = ((SampleOutcome.Accepted) v.validate(in, spec, 1_000_000L))
                .sample().calibratedValue();
        System.out.printf("[repro] raw=20 v1(scale=2,offset=1) -> %s / %s%n", c1, c2);
        assertEquals(41.0, c1, 0.0);
        assertEquals(c1, c2, 0.0);
    }

    @Test
    void calibrationUpdateChangesResultAndKeepsHistory() {
        SensorRegistry registry = new SensorRegistry();
        SensorSpec v1 = v1();
        registry.register(v1);
        SensorSpec v2 = CalibrationModel.withCalibration(v1, 3.0, -5.0, "v2");
        registry.register(v2);

        RawReading in = fixedReading(20.0, "t");
        ReadingValidator validator = new ReadingValidator();
        Sample s1 = ((SampleOutcome.Accepted)
                validator.validate(in, registry.history("t").get(0), 1_000_000L)).sample();
        Sample s2 = ((SampleOutcome.Accepted)
                validator.validate(in, registry.current("t").orElseThrow(), 1_000_000L)).sample();
        System.out.printf("[calib] same raw=20 -> v1=%s(rule=%s), v2=%s(rule=%s)%n",
                s1.calibratedValue(), s1.ruleVersion(), s2.calibratedValue(), s2.ruleVersion());
        assertEquals(41.0, s1.calibratedValue(), 0.0);
        assertEquals(55.0, s2.calibratedValue(), 0.0, "20*3-5=55");
        assertEquals("v1", s1.ruleVersion());
        assertEquals("v2", s2.ruleVersion());

        List<SensorSpec> history = registry.history("t");
        assertEquals(2, history.size(), "旧规则版本必须保留");
        assertEquals("v1", history.get(0).ruleVersion());
    }

    @Test
    void registryRejectsVersionRollback() {
        SensorRegistry registry = new SensorRegistry();
        registry.register(v1());
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> registry.register(CalibrationModel.withCalibration(v1(), 1.0, 0.0, "v1")));
        System.out.println("[version] rollback rejected: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("ruleVersion must increase"));
    }
}
