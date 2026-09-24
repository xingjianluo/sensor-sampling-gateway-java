package com.github.xingjianluo.sensorsamplinggatewayjava.sensor;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.RawReading;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorConfig;

/**
 * 传感器统一抽象。真实实现可能是硬件/串口驱动，本工程仅使用可复现的模拟实现。
 */
public interface Sensor {

    SensorConfig config();

    /**
     * 采集第 tick 次读数。实现可能抛出故障（瞬时或持续）。
     */
    RawReading sample(long tick) throws Exception;
}
