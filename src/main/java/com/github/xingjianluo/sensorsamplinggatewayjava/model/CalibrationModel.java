package com.github.xingjianluo.sensorsamplinggatewayjava.model;

import java.util.Collections;
import java.util.Map;

/**
 * 校准模型：calibrated = gain * raw + offset。
 *
 * <p>带版本号；相同输入 + 相同模型版本必须得到相同结论。模型不可变，
 * 更新校准即整体替换为新版本实例（见 {@code GatewayConfig} 快照）。</p>
 */
public final class CalibrationModel {

    private static final double[] IDENTITY = {1.0d, 0.0d};

    private final String version;
    private final Map<String, double[]> gainOffset;

    public CalibrationModel(String version, Map<String, double[]> gainOffset) {
        this.version = version;
        this.gainOffset = Map.copyOf(gainOffset);
    }

    /** 无校准的恒等模型（gain=1, offset=0）。 */
    public static CalibrationModel identity(String version) {
        return new CalibrationModel(version, Collections.emptyMap());
    }

    public String getVersion() {
        return version;
    }

    /**
     * 对原始值应用线性校准；未配置的传感器使用恒等参数。
     */
    public double apply(String sensorId, double raw) {
        double[] p = gainOffset.getOrDefault(sensorId, IDENTITY);
        return p[0] * raw + p[1];
    }

    /** 返回不可变的 gain/offset 映射（数组值按拷贝返回，避免外部修改）。 */
    public Map<String, double[]> asMap() {
        java.util.HashMap<String, double[]> copy = new java.util.HashMap<>();
        gainOffset.forEach((k, v) -> copy.put(k, new double[]{v[0], v[1]}));
        return Collections.unmodifiableMap(copy);
    }
}
