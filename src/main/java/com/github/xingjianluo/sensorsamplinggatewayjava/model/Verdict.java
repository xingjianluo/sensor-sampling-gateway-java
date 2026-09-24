package com.github.xingjianluo.sensorsamplinggatewayjava.model;

/**
 * 对一条原始读数的裁决结论（不可变）。拒绝时 calibratedValue 为 NaN。
 *
 * @param accepted        是否受理为有效采样
 * @param reading         原始读数
 * @param calibratedValue 校准后的值（拒绝时为 NaN）
 * @param reason          拒绝原因（接受时为 null）
 * @param basis           判定依据的人类可读描述（输入 + 规则，用于日志与解释）
 * @param rulesVersion    作出该裁决时的规则版本
 */
public record Verdict(boolean accepted, RawReading reading, double calibratedValue,
                      RejectReason reason, String basis, String rulesVersion) {

    public Verdict {
        if (reading == null) {
            throw new IllegalArgumentException("reading must not be null");
        }
    }

    public static Verdict accept(RawReading r, double calibrated, String basis, String rulesVersion) {
        return new Verdict(true, r, calibrated, null, basis, rulesVersion);
    }

    public static Verdict reject(RawReading r, RejectReason reason, String basis, String rulesVersion) {
        if (reason == null) {
            throw new IllegalArgumentException("rejected verdict must carry a reason");
        }
        return new Verdict(false, r, Double.NaN, reason, basis, rulesVersion);
    }
}
