package com.github.xingjianluo.sensorsamplinggatewayjava.gateway;

import com.github.xingjianluo.sensorsamplinggatewayjava.buffer.BoundedSamplingBuffer;
import com.github.xingjianluo.sensorsamplinggatewayjava.calibration.CalibrationModel;
import com.github.xingjianluo.sensorsamplinggatewayjava.config.GatewayProperties;
import com.github.xingjianluo.sensorsamplinggatewayjava.config.RetryPolicy;
import com.github.xingjianluo.sensorsamplinggatewayjava.engine.SamplingEngine;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.RawReading;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.RejectReason;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SampleOutcome;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorSpec;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.StatsSnapshot;
import com.github.xingjianluo.sensorsamplinggatewayjava.registry.SensorRegistry;
import com.github.xingjianluo.sensorsamplinggatewayjava.sensor.SensorDriver;
import com.github.xingjianluo.sensorsamplinggatewayjava.sensor.SensorFaultException;
import com.github.xingjianluo.sensorsamplinggatewayjava.stats.GatewayStats;
import com.github.xingjianluo.sensorsamplinggatewayjava.validation.ReadingValidator;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * {@link SamplingGateway} 的默认实现：组合登记表、有界缓冲、采样引擎与内存结论收集出口。
 * 引擎在首次 {@link #start()} 时按当前传感器集合惰性构建，保证生命周期可重复。
 */
public class DefaultSamplingGateway implements SamplingGateway {

    private final GatewayProperties properties;
    private final RetryPolicy retryPolicy;
    private final LongSupplier clock;
    private final SensorRegistry registry = new SensorRegistry();
    private final Map<String, SensorDriver> drivers = new LinkedHashMap<>();
    private final GatewayStats stats = new GatewayStats();
    private final OutcomeSink sink;
    private final ReadingValidator validator = new ReadingValidator();

    private BoundedSamplingBuffer buffer;
    private SamplingEngine engine;

    public DefaultSamplingGateway(GatewayProperties properties, LongSupplier clock) {
        this(properties, properties.getRetry().toRetryPolicy(), clock, new RecordingOutcomeSink());
    }

    /** 测试/本地验证构造：显式指定重试策略。 */
    public DefaultSamplingGateway(GatewayProperties properties, RetryPolicy retryPolicy, LongSupplier clock) {
        this(properties, retryPolicy, clock, new RecordingOutcomeSink());
    }

    /** 测试/本地验证构造：可注入自定义结论出口（例如带延迟的慢消费出口）。 */
    public DefaultSamplingGateway(GatewayProperties properties, RetryPolicy retryPolicy,
                                  LongSupplier clock, OutcomeSink sink) {
        this.properties = properties;
        this.retryPolicy = retryPolicy;
        this.clock = clock;
        this.sink = sink;
        this.buffer = new BoundedSamplingBuffer(properties.getBufferCapacity(),
                properties.getOverflowPolicy());
    }

    @Override
    public synchronized void attach(SensorSpec spec, SensorDriver driver) {
        registry.register(spec);
        drivers.put(spec.sensorId(), driver);
    }

    @Override
    public synchronized void updateCalibration(String sensorId, double scale, double offset,
                                               String newRuleVersion) {
        SensorSpec current = requireSensor(sensorId);
        registry.register(CalibrationModel.withCalibration(current, scale, offset, newRuleVersion));
    }

    @Override
    public synchronized void updateSpec(SensorSpec newSpec) {
        requireSensor(newSpec.sensorId());
        registry.register(newSpec);
    }

    /**
     * 同步采样单个传感器一次（绕过缓冲），便于本地验证单条“读取→判定”链路。
     * 设备故障同样转成携带 SENSOR_ISOLATED 之外原因不可用时的明确结论；这里故障属于读取失败，
     * 返回一条基于占位读数的拒绝结论，保证结论可解释、不抛异常给调用方。
     */
    @Override
    public SampleOutcome sampleOnce(String sensorId, long nowMs) {
        SensorSpec spec = registry.current(sensorId).orElseThrow(() ->
                new IllegalArgumentException("unknown sensor: " + sensorId));
        SensorDriver driver = drivers.get(sensorId);
        if (driver == null) {
            throw new IllegalStateException("no driver attached for sensor: " + sensorId);
        }
        RawReading reading;
        try {
            reading = driver.read(nowMs);
        } catch (SensorFaultException e) {
            RawReading placeholder = new RawReading(sensorId, -1L, Double.NaN, nowMs, 0L);
            String basis = "SENSOR_FAULT sensor=" + sensorId + " transient=" + e.isTransient()
                    + " message=" + e.getMessage();
            return new SampleOutcome.Rejected(placeholder, RejectReason.SENSOR_FAULT, basis);
        }
        stats.incrementProduced();
        SampleOutcome outcome = validator.validate(reading, spec, nowMs);
        if (outcome instanceof SampleOutcome.Accepted) {
            stats.incrementAccepted();
        } else if (outcome instanceof SampleOutcome.Rejected r) {
            stats.incrementRejected(r.reason());
        }
        sink.deliver(outcome);
        return outcome;
    }

    @Override
    public synchronized void start() {
        if (engine == null) {
            engine = new SamplingEngine(registry, drivers, buffer, validator, sink,
                    stats, retryPolicy, clock);
        }
        engine.start();
    }

    @Override
    public synchronized void stop() {
        if (engine != null) {
            engine.stop();
        }
    }

    @Override
    public synchronized void restart() {
        if (engine != null) {
            engine.restart();
        } else {
            start();
        }
    }

    @Override
    public java.util.Set<String> isolatedSensors() {
        java.util.Set<String> result = new java.util.TreeSet<>();
        if (engine != null) {
            for (String id : registry.allCurrent().keySet()) {
                var t = engine.tracker(id);
                if (t != null && t.isIsolated()) {
                    result.add(id);
                }
            }
        }
        return result;
    }

    @Override
    public StatsSnapshot stats() {
        return stats.snapshot(isolatedSensors().size(), buffer.size());
    }

    @Override
    public Map<String, SensorSpec> sensors() {
        return registry.allCurrent();
    }

    @Override
    public List<SampleOutcome> delivered() {
        return sink instanceof RecordingOutcomeSink r ? r.outcomes() : List.of();
    }

    /** 测试辅助：清空已交付结论与计数。 */
    public synchronized void resetObservations() {
        if (sink instanceof RecordingOutcomeSink r) {
            r.clear();
        }
        stats.reset();
    }

    SamplingEngine engine() {
        return engine;
    }

    private SensorSpec requireSensor(String sensorId) {
        return registry.current(sensorId).orElseThrow(() ->
                new IllegalArgumentException("unknown sensor: " + sensorId));
    }
}
