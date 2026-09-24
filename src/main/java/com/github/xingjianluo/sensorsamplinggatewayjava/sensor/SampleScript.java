package com.github.xingjianluo.sensorsamplinggatewayjava.model;

/**
 * 模拟传感器单个采样点的确定性脚本指令。
 *
 * @param kind      脚本动作类型
 * @param rawValue  NORMAL 时的原始值（校验和据此计算）；其余字段见 kind
 * @param tsDeltaMs 相对网关时钟的时间戳偏移（可制造时间戳异常）
 * @param corrupt   是否破坏校验和（数据损坏）
 */
public record SampleScript(Kind kind, double rawValue, long tsDeltaMs, boolean corrupt) {

    public enum Kind {
        NORMAL,
        OUT_OF_RANGE,
        BAD_TIMESTAMP,
        CORRUPT,
        TRANSIENT_FAULT,
        PERSISTENT_FAULT
    }

    public static SampleScript normal(double v) {
        return new SampleScript(Kind.NORMAL, v, 0L, false);
    }
}
