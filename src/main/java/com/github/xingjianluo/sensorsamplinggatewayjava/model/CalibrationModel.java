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
    /** 可注入的校准函数（测试/非线行校准用）；null 表示用默认线性模型。 */
    private final java.util.function.BiFunction<String, Double, Double> function;

    public CalibrationModel(String version, Map<String, double[]> gainOffset) {
        this(version, gainOffset, null);
    }

    private CalibrationModel(String version, Map<String, double[]> gainOffset,
                             java.util.function.BiFunction<String, Double, Double> function) {
        this.version = version;
        this.gainOffset = Map.copyOf(gainOffset);
        this.function = function;
    }

    /** 无校准的恒等模型（gain=1, offset=0）。 */
    public static CalibrationModel identity(String version) {
        return new CalibrationModel(version, Collections.emptyMap());
    }

    /**
     * 用自定义校准函数构造模型（可复现：同一版本 + 同一函数，结论相同）。
     * 主要用于非线性校准与需要在裁决前注入可控延迟/故障的测试场景。
     */
    public static CalibrationModel custom(String version,
                                          java.util.function.BiFunction<String, Double, Double> function) {
        if (function == null) {
            throw new IllegalArgumentException("calibration function must not be null");
        }
        return new CalibrationModel(version, Collections.emptyMap(), function);
    }

    public String getVersion() {
        return version;
    }

    /**
     * 对原始值应用校准；注入了自定义函数时使用它，否则未配置的传感器使用恒等参数。
     */
    public double apply(String sensorId, double raw) {
        if (function != null) {
            return function.apply(sensorId, raw);
        }
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
