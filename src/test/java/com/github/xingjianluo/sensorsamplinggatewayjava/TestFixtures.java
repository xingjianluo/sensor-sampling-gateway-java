package com.github.xingjianluo.sensorsamplinggatewayjava;

import com.github.xingjianluo.sensorsamplinggatewayjava.gateway.GatewayConfig;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.CalibrationModel;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.OverflowPolicy;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.RawReading;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SampleScript;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorConfig;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorType;
import com.github.xingjianluo.sensorsamplinggatewayjava.reliability.RetryPolicy;
import com.github.xingjianluo.sensorsamplinggatewayjava.reliability.Sleeper;
import com.github.xingjianluo.sensorsamplinggatewayjava.sensor.ScriptedSensor;
import com.github.xingjianluo.sensorsamplinggatewayjava.sensor.Sensor;
import com.github.xingjianluo.sensorsamplinggatewayjava.sensor.SamplingLimitReached;
import com.github.xingjianluo.sensorsamplinggatewayjava.time.ManualClock;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 测试夹具：用手动时钟、无等待退避、节拍睡眠让时钟随每次采样推进，保证完全确定。 */
final class TestFixtures {

    static final long T0 = 1_000_000L;

    private TestFixtures() {
    }

    static SensorConfig config(String id, SensorType type, double min, double max) {
        return new SensorConfig(id, type, 1L, min, max, 1000L, true);
    }

    static GatewayConfig gatewayConfig(String rulesVersion,
                                       List<SensorConfig> sensorConfigs,
                                       int bufferCapacity,
                                       OverflowPolicy policy,
                                       int consumers,
                                       int retryAttempts,
                                       int breakerThreshold) {
        return gatewayConfig(rulesVersion, sensorConfigs, bufferCapacity, policy, consumers,
                retryAttempts, breakerThreshold, 1_000_000L);
    }

    /** 可指定断路器冷却：配合会推进手动时钟的睡眠器，可确定性地驱动 OPEN→HALF_OPEN 探测。 */
    static GatewayConfig gatewayConfig(String rulesVersion,
                                       List<SensorConfig> sensorConfigs,
                                       int bufferCapacity,
                                       OverflowPolicy policy,
                                       int consumers,
                                       int retryAttempts,
                                       int breakerThreshold,
                                       long breakerCooldownMs) {
        RetryPolicy retry = new RetryPolicy(retryAttempts, 1L, 2.0, 50L, 0.0);
        CalibrationModel calib = CalibrationModel.identity(rulesVersion + "-calib");
        return new GatewayConfig(rulesVersion, sensorConfigs, calib,
                bufferCapacity, policy, consumers, retry,
                breakerThreshold, breakerCooldownMs, 5000L, 4096);
    }

    /** 构建脚本传感器：每次 period 睡眠都把手动时钟向前推进，读数时间戳因此确定。 */
    static Map<String, Sensor> scriptedSensors(ManualClock clock,
                                               Map<String, SensorConfig> cfgs,
                                               Map<String, List<SampleScript>> scripts) {
        return scriptedSensors(clock, cfgs, scripts, null);
    }

    /** 为每个传感器统一指定脚本越界后的动作（测试用于精确控制 tick 数）。 */
    static Map<String, Sensor> scriptedSensors(ManualClock clock,
                                               Map<String, SensorConfig> cfgs,
                                               Map<String, List<SampleScript>> scripts,
                                               SampleScript beyond) {
        Map<String, SampleScript> m = new LinkedHashMap<>();
        cfgs.keySet().forEach(id -> m.put(id, beyond));
        return scriptedSensorsPerSensor(clock, cfgs, scripts, m);
    }

    /** 按传感器分别指定脚本越界后的动作。 */
    static Map<String, Sensor> scriptedSensorsPerSensor(ManualClock clock,
                                                        Map<String, SensorConfig> cfgs,
                                                        Map<String, List<SampleScript>> scripts,
                                                        Map<String, SampleScript> beyondPerSensor) {
        Map<String, Sensor> out = new LinkedHashMap<>();
        for (Map.Entry<String, SensorConfig> e : cfgs.entrySet()) {
            out.put(e.getKey(), new ScriptedSensor(e.getValue(),
                    scripts.getOrDefault(e.getKey(), List.of()), clock,
                    beyondPerSensor.get(e.getKey())));
        }
        return out;
    }

    /**
     * 为每个传感器包一层“硬采样上限”：当拍号 {@code tick >= limit} 时抛出
     * 非故障的 {@link SamplingLimitReached}，使采样拍数在源头精确确定。
     * 按 tick（而非调用次数）判定，因此有界重试不会提前耗光配额、不会跨 tick 漏拍。
     */
    static Map<String, Sensor> limited(Map<String, Sensor> sensors, Map<String, Integer> limits) {
        Map<String, Sensor> out = new LinkedHashMap<>();
        for (Map.Entry<String, Sensor> e : sensors.entrySet()) {
            Sensor delegate = e.getValue();
            int limit = limits.getOrDefault(e.getKey(), Integer.MAX_VALUE);
            out.put(e.getKey(), new Sensor() {
                @Override
                public SensorConfig config() {
                    return delegate.config();
                }

                @Override
                public RawReading sample(long tick) throws Exception {
                    if (tick >= limit) {
                        // 协作式停止：不是故障，不产生裁决
                        throw new SamplingLimitReached(config().getId(), tick);
                    }
                    return delegate.sample(tick);
                }
            });
        }
        return out;
    }

    /**
     * 测试睡眠器：不做真实等待，仅让出 CPU。手动时钟固定，
     * 因而读数时间戳始终等于 T0，时间戳判定完全确定；重试退避也零等待，测试快速。
     */
    static Sleeper fastSleeper() {
        return millis -> Thread.yield();
    }
}
