package com.github.xingjianluo.sensorsamplinggatewayjava.reconcile;

/**
 * 对账查询条件（不可变）。所有条件均可选，null/负值表示不限制。
 *
 * @param runId         运行代次（每次 start/restart 递增；null 表示跨全部代次）
 * @param sensorId      传感器编号（null 表示全部）
 * @param rulesVersion  裁决时的规则版本（null 表示全部）
 * @param fromMillis    记录时间下界（含，&lt;0 表示不限）
 * @param toMillis      记录时间上界（含，&lt;0 表示不限）
 */
public record ReconcileQuery(Long runId, String sensorId, String rulesVersion,
                             long fromMillis, long toMillis) {

    public static ReconcileQuery all() {
        return new ReconcileQuery(null, null, null, -1L, -1L);
    }

    public static ReconcileQuery ofRun(long runId) {
        return new ReconcileQuery(runId, null, null, -1L, -1L);
    }

    public boolean hasTimeRange() {
        return fromMillis >= 0L || toMillis >= 0L;
    }

    public boolean matches(ReconcileEntry entry) {
        if (runId != null && entry.runId() != runId) {
            return false;
        }
        if (sensorId != null && !sensorId.equals(entry.verdict().reading().sensorId())) {
            return false;
        }
        if (rulesVersion != null && !rulesVersion.equals(entry.verdict().rulesVersion())) {
            return false;
        }
        if (fromMillis >= 0L && entry.recordedAtMs() < fromMillis) {
            return false;
        }
        if (toMillis >= 0L && entry.recordedAtMs() > toMillis) {
            return false;
        }
        return true;
    }
}
