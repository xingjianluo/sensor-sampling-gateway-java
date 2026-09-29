package com.github.xingjianluo.sensorsamplinggatewayjava.gateway;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.CalibrationModel;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.OverflowPolicy;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorConfig;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.RetentionPolicy;
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
                            int verdictLogCapacity,
                            int reconcileDetailCapacity,
                            int reconcileMaxSealedGenerations,
                            RetentionPolicy reconcileRetentionPolicy) {

    /** 对账明细每代容量默认值。 */
    public static final int DEFAULT_RECONCILE_DETAIL_CAPACITY = 4096;
    /** 封代代次（批次）保留代数默认值。 */
    public static final int DEFAULT_RECONCILE_MAX_SEALED_GENERATIONS = 8;

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
        if (reconcileDetailCapacity < 1) {
            throw new IllegalArgumentException("reconcileDetailCapacity must be >= 1");
        }
        if (reconcileMaxSealedGenerations < 1) {
            throw new IllegalArgumentException("reconcileMaxSealedGenerations must be >= 1");
        }
        if (reconcileRetentionPolicy == null) {
            reconcileRetentionPolicy = RetentionPolicy.ROLL_OLDEST;
        }
    }

    /**
     * 兼容旧签名（不含对账保留配置）：对账明细容量/封代保留代数/超容量策略使用默认值。
     */
    public GatewayConfig(String rulesVersion,
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
        this(rulesVersion, sensors, calibration, bufferCapacity, overflowPolicy, consumerThreads,
                retryPolicy, breakerThreshold, breakerCooldownMs, shutdownDrainTimeoutMs,
                verdictLogCapacity,
                DEFAULT_RECONCILE_DETAIL_CAPACITY,
                DEFAULT_RECONCILE_MAX_SEALED_GENERATIONS,
                RetentionPolicy.ROLL_OLDEST);
    }

    private Map<String, SensorConfig> byId() {
        return sensors().stream().collect(Collectors.toUnmodifiableMap(
                SensorConfig::getId, Function.identity()));
    }

    public SensorConfig sensorConfig(String sensorId) {
        return byId().get(sensorId);
    }
}
