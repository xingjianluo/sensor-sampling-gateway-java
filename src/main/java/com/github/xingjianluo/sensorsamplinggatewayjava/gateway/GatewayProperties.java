package com.github.xingjianluo.sensorsamplinggatewayjava.gateway;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.CalibrationModel;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.OverflowPolicy;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorConfig;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorType;
import com.github.xingjianluo.sensorsamplinggatewayjava.reliability.RetryPolicy;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.LinkedHashMap;
import java.util.List;

import java.util.ArrayList;
import java.util.List;

/**
 * 从 application.properties 读取的可变配置，转换为不可变 {@link GatewayConfig} 快照。
 * 前缀 gateway。
 */
@ConfigurationProperties(prefix = "gateway")
public class GatewayProperties {

    private String rulesVersion = "rules-v1";
    private int bufferCapacity = 64;
    private String overflowPolicy = "BLOCK";
    private int consumerThreads = 2;
    private int retryMaxAttempts = 3;
    private long retryInitialBackoffMs = 20;
    private double retryMultiplier = 2.0;
    private long retryMaxBackoffMs = 500;
    private double retryJitterRatio = 0.1;
    private int breakerThreshold = 3;
    private long breakerCooldownMs = 2000;
    private long shutdownDrainTimeoutMs = 5000;
    private int verdictLogCapacity = 1024;
    private List<SensorProps> sensors = new ArrayList<>();

    /** 转换为不可变快照（含每传感器校准模型）。 */
    public GatewayConfig toConfig() {
        List<SensorConfig> configs = sensors.stream()
                .map(p -> new SensorConfig(
                        p.getId(),
                        SensorType.valueOf(p.getType().trim().toUpperCase()),
                        p.getPeriodMs(),
                        p.getRangeMin(),
                        p.getRangeMax(),
                        p.getTimestampToleranceMs(),
                        p.isEnabled()))
                .toList();
        LinkedHashMap<String, double[]> calib = new LinkedHashMap<>();
        for (SensorProps p : sensors) {
            calib.put(p.getId(), new double[]{p.getCalibrationGain(), p.getCalibrationOffset()});
        }
        CalibrationModel model = new CalibrationModel(rulesVersion + "-calibration", calib);
        RetryPolicy retry = new RetryPolicy(retryMaxAttempts, retryInitialBackoffMs,
                retryMultiplier, retryMaxBackoffMs, retryJitterRatio);
        return new GatewayConfig(rulesVersion, configs, model,
                bufferCapacity, OverflowPolicy.valueOf(overflowPolicy.trim().toUpperCase()),
                consumerThreads, retry, breakerThreshold, breakerCooldownMs,
                shutdownDrainTimeoutMs, verdictLogCapacity);
    }

    public static class SensorProps {
        private String id;
        private String type = "TEMPERATURE";
        private long periodMs = 100;
        private double rangeMin = 0;
        private double rangeMax = 100;
        private long timestampToleranceMs = 1000;
        private boolean enabled = true;
        private double calibrationGain = 1.0;
        private double calibrationOffset = 0.0;

        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
        public long getPeriodMs() { return periodMs; }
        public void setPeriodMs(long periodMs) { this.periodMs = periodMs; }
        public double getRangeMin() { return rangeMin; }
        public void setRangeMin(double rangeMin) { this.rangeMin = rangeMin; }
        public double getRangeMax() { return rangeMax; }
        public void setRangeMax(double rangeMax) { this.rangeMax = rangeMax; }
        public long getTimestampToleranceMs() { return timestampToleranceMs; }
        public void setTimestampToleranceMs(long v) { this.timestampToleranceMs = v; }
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public double getCalibrationGain() { return calibrationGain; }
        public void setCalibrationGain(double v) { this.calibrationGain = v; }
        public double getCalibrationOffset() { return calibrationOffset; }
        public void setCalibrationOffset(double v) { this.calibrationOffset = v; }
    }

    public String getRulesVersion() { return rulesVersion; }
    public void setRulesVersion(String v) { this.rulesVersion = v; }
    public int getBufferCapacity() { return bufferCapacity; }
    public void setBufferCapacity(int v) { this.bufferCapacity = v; }
    public String getOverflowPolicy() { return overflowPolicy; }
    public void setOverflowPolicy(String v) { this.overflowPolicy = v; }
    public int getConsumerThreads() { return consumerThreads; }
    public void setConsumerThreads(int v) { this.consumerThreads = v; }
    public int getRetryMaxAttempts() { return retryMaxAttempts; }
    public void setRetryMaxAttempts(int v) { this.retryMaxAttempts = v; }
    public long getRetryInitialBackoffMs() { return retryInitialBackoffMs; }
    public void setRetryInitialBackoffMs(long v) { this.retryInitialBackoffMs = v; }
    public double getRetryMultiplier() { return retryMultiplier; }
    public void setRetryMultiplier(double v) { this.retryMultiplier = v; }
    public long getRetryMaxBackoffMs() { return retryMaxBackoffMs; }
    public void setRetryMaxBackoffMs(long v) { this.retryMaxBackoffMs = v; }
    public double getRetryJitterRatio() { return retryJitterRatio; }
    public void setRetryJitterRatio(double v) { this.retryJitterRatio = v; }
    public int getBreakerThreshold() { return breakerThreshold; }
    public void setBreakerThreshold(int v) { this.breakerThreshold = v; }
    public long getBreakerCooldownMs() { return breakerCooldownMs; }
    public void setBreakerCooldownMs(long v) { this.breakerCooldownMs = v; }
    public long getShutdownDrainTimeoutMs() { return shutdownDrainTimeoutMs; }
    public void setShutdownDrainTimeoutMs(long v) { this.shutdownDrainTimeoutMs = v; }
    public int getVerdictLogCapacity() { return verdictLogCapacity; }
    public void setVerdictLogCapacity(int v) { this.verdictLogCapacity = v; }
    public List<SensorProps> getSensors() { return sensors; }
    public void setSensors(List<SensorProps> sensors) { this.sensors = sensors; }
}
