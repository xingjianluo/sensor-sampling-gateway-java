package com.github.xingjianluo.sensorsamplinggatewayjava.sensor;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.RawReading;

/**
 * 传感器驱动抽象。真实硬件、脚本化模拟器或回放器都实现该接口，
 * 使采样链路完全不依赖真实硬件。
 */
public interface SensorDriver {

    /** 该驱动对应的传感器标识。 */
    String sensorId();

    /**
     * 读取一次原始读数。
     *
     * @throws com.github.xingjianluo.sensorsamplinggatewayjava.sensor.SensorFaultException 设备故障
     */
    RawReading read(long nowMs) throws SensorFaultException;

    /** 供故障恢复探测使用：驱动是否处于可响应状态。 */
    boolean isHealthy();
}
