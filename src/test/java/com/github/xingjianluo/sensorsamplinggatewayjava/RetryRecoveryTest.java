package com.github.xingjianluo.sensorsamplinggatewayjava;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.RawReading;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SampleScript;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorConfig;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorType;
import com.github.xingjianluo.sensorsamplinggatewayjava.sensor.ScriptedSensor;
import com.github.xingjianluo.sensorsamplinggatewayjava.sensor.SensorFaultException;
import com.github.xingjianluo.sensorsamplinggatewayjava.time.ManualClock;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 瞬时故障有界重试后恢复；持续故障直接暴露给上层走隔离路径。 */
class RetryRecoveryTest {

    private static final Logger LOG = Logger.getLogger(RetryRecoveryTest.class.getName());

    private final SensorConfig cfg = new SensorConfig(
            "s", SensorType.TEMPERATURE, 100L, 0, 100, 1000L, true);

    @Test
    void transientFaultCanSucceedOnRetry() throws Exception {
        // tick=0 瞬时故障一次，随后重试：脚本对同一 tick 始终返回同一指令，
        // 因此这里用一个可在第 N 次调用恢复的自定义传感器验证“重试带来恢复”。
        AtomicInteger calls = new AtomicInteger();
        ScriptedSensor backing = new ScriptedSensor(cfg,
                List.of(SampleScript.normal(42.0)), new ManualClock(1000L));
        com.github.xingjianluo.sensorsamplinggatewayjava.sensor.Sensor flaky = new com.github.xingjianluo.sensorsamplinggatewayjava.sensor.Sensor() {
            @Override
            public SensorConfig config() {
                return cfg;
            }

            @Override
            public RawReading sample(long tick) throws Exception {
                if (calls.incrementAndGet() < 3) {
                    throw new SensorFaultException("s", tick, "transient", true);
                }
                return backing.sample(tick);
            }
        };

        var policy = new com.github.xingjianluo.sensorsamplinggatewayjava.reliability.RetryPolicy(
                4, 1L, 2.0, 10L, 0.0);
        RawReading result = null;
        for (int attempt = 1; attempt <= policy.maxAttempts(); attempt++) {
            try {
                result = flaky.sample(0L);
                break;
            } catch (SensorFaultException f) {
                LOG.info("attempt " + attempt + " failed (recoverable=" + f.isRecoverable()
                        + "), retrying after bounded backoff");
            }
        }
        assertEquals(42.0, result.value(), 0.0001);
        assertEquals(3, calls.get());
    }

    @Test
    void scriptedTransientFaultIsMarkedRecoverableAndPersistentIsNot() {
        ScriptedSensor sensor = new ScriptedSensor(cfg,
                List.of(new SampleScript(SampleScript.Kind.TRANSIENT_FAULT, 0, 0, false),
                        new SampleScript(SampleScript.Kind.PERSISTENT_FAULT, 0, 0, false)),
                new ManualClock(1000L));
        SensorFaultException t = assertThrows(SensorFaultException.class, () -> sensor.sample(0L));
        SensorFaultException p = assertThrows(SensorFaultException.class, () -> sensor.sample(1L));
        assertTrue(t.isRecoverable());
        assertFalse(p.isRecoverable());
        LOG.info("transient vs persistent faults distinguished: "
                + t.isRecoverable() + " / " + p.isRecoverable());
    }
}
