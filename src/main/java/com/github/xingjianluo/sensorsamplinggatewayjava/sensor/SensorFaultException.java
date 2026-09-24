package com.github.xingjianluo.sensorsamplinggatewayjava.sensor;

/**
 * 传感器设备故障：瞬时故障（可重试）或持续故障（需隔离）均通过它抛出。
 */
public class SensorFaultException extends Exception {

    private final boolean transientFault;

    public SensorFaultException(String sensorId, String message, boolean transientFault) {
        super("sensor=" + sensorId + " " + message + " transient=" + transientFault);
        this.transientFault = transientFault;
    }

    /** 是否为瞬时故障（瞬时故障允许重试，持续故障直接计入隔离判定）。 */
    public boolean isTransient() {
        return transientFault;
    }
}
