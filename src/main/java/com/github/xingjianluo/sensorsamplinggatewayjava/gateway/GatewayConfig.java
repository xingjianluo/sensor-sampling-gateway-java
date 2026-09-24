package com.github.xingjianluo.sensorsamplinggatewayjava.gateway;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.CalibrationModel;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.OverflowPolicy;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorConfig;
import com.github.xingjianluo.sensorsamplinggatewayjava.reliability.RetryPolicy;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 网关配置快照（不可变）。带规则版本号：相同输入 + 相同规则版本，裁决结论可复现。
 */
public record GatewayConfig(String rulesVersion,
                            List<SensorConfig> sensors,
                            CalibrationModel calibration,
                            int bufferCapacity,
                            OverflowPolicy overflowPolicy,
                            int consumerThreads,
                            RetryPolicy retryPolicy,
                            int breakerThreshold,
                            long breakerCooldownMs,
                            long shutdownDrainTimeoutMs,
                            int verdictLogCapacity) {

    public GatewayConfig {
        sensors = List.copyOf(sensors);
        if (calibration == null) {
            calibration = CalibrationModel.identity(rulesVersion + "-calib");
        }
        if (consumerThreads < 1) {
            throw new IllegalArgumentException("consumerThreads must be >= 1");
        }
        if (bufferCapacity < 1) {
            throw new IllegalArgumentException("bufferCapacity must be >= 1");
        }
    }

    private Map<String, SensorConfig> byId() {
        return sensors().stream().collect(Collectors.toUnmodifiableMap(
                SensorConfig::getId, Function.identity()));
    }

    public SensorConfig sensorConfig(String sensorId) {
        return byId().get(sensorId);
    }
}
