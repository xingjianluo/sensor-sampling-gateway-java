package com.github.xingjianluo.sensorsamplinggatewayjava.gateway;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.SampleScript;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorConfig;
import com.github.xingjianluo.sensorsamplinggatewayjava.reliability.Sleeper;
import com.github.xingjianluo.sensorsamplinggatewayjava.sensor.ScriptedSensor;
import com.github.xingjianluo.sensorsamplinggatewayjava.sensor.Sensor;
import com.github.xingjianluo.sensorsamplinggatewayjava.time.Clock;
import com.github.xingjianluo.sensorsamplinggatewayjava.time.SystemClock;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Spring 生命周期绑定：按 {@link GatewayProperties} 构建完全本地的脚本化模拟传感器，
 * 启动采样网关，并在上下文关闭时优雅停止（排空已受理数据）。
 *
 * <p>默认脚本：前 8 拍正常，随后周期性演示超量程/坏时间戳/损坏/瞬时故障/持续故障，
 * 以便不接硬件即可在本地观察全部裁决原因与故障隔离。</p>
 */
@Component
public class GatewayLifecycle {

    private final GatewayProperties properties;
    private SamplingGateway gateway;

    public GatewayLifecycle(GatewayProperties properties) {
        this.properties = properties;
    }

    @PostConstruct
    public void start() {
        GatewayConfig config = properties.toConfig();
        Clock clock = new SystemClock();
        Map<String, Sensor> sensors = new LinkedHashMap<>();
        for (SensorConfig sc : config.sensors()) {
            sensors.put(sc.getId(), new ScriptedSensor(sc, defaultScript(), clock));
        }
        gateway = new SamplingGateway(config, sensors, clock, Sleeper.REAL);
        gateway.start();
    }

    /** 可复现的演示脚本：每个传感器相同节拍，便于对照。 */
    static List<SampleScript> defaultScript() {
        return List.of(
                SampleScript.normal(20.0),
                SampleScript.normal(21.5),
                new SampleScript(SampleScript.Kind.OUT_OF_RANGE, 0, 0L, false),
                new SampleScript(SampleScript.Kind.BAD_TIMESTAMP, 22.0, 10_000L, false),
                new SampleScript(SampleScript.Kind.CORRUPT, 22.0, 0L, false),
                new SampleScript(SampleScript.Kind.TRANSIENT_FAULT, 0, 0L, false),
                SampleScript.normal(23.0),
                new SampleScript(SampleScript.Kind.PERSISTENT_FAULT, 0, 0L, false));
    }

    @PreDestroy
    public void stop() {
        if (gateway != null) {
            gateway.stop();
        }
    }

    public SamplingGateway gateway() {
        return gateway;
    }
}
