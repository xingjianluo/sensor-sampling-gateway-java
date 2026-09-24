package com.github.xingjianluo.sensorsamplinggatewayjava.gateway;

import com.github.xingjianluo.sensorsamplinggatewayjava.calibration.CalibrationModel;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SampleOutcome;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorSpec;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.StatsSnapshot;
import com.github.xingjianluo.sensorsamplinggatewayjava.sensor.SensorDriver;

import java.util.List;
import java.util.Map;

/**
 * 采样网关门面：统一管理传感器接入、校准/配置更新、生命周期与可观测统计。
 */
public interface SamplingGateway {

    /** 接入一个传感器（配置 + 驱动）。 */
    void attach(SensorSpec spec, SensorDriver driver);

    /** 更新校准参数（生成新规则版本，旧版本保留于登记表）。 */
    void updateCalibration(String sensorId, double scale, double offset, String newRuleVersion);

    /** 更新传感器配置。 */
    void updateSpec(SensorSpec newSpec);

    /** 同步采样单个传感器一次（绕过缓冲，便于本地验证单条判定链路）。 */
    SampleOutcome sampleOnce(String sensorId, long nowMs);

    void start();

    void stop();

    void restart();

    /** 当前被隔离的传感器标识集合（快照）。 */
    java.util.Set<String> isolatedSensors();

    StatsSnapshot stats();

    Map<String, SensorSpec> sensors();

    /** 已交付的全部结论（仅在使用内存收集出口时有内容）。 */
    List<SampleOutcome> delivered();
}
