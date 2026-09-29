package com.github.xingjianluo.sensorsamplinggatewayjava.reconcile;

/**
 * 对账查询的筛选条件（不可变）。所有条件可组合、可留空；留空表示不限制。
 *
 * <p>口径说明：</p>
 * <ul>
 *   <li>{@code generation}：按运行批次（一次 start/restart 为一代）；&lt;1 表示不限；</li>
 *   <li>{@code sensorId}：按传感器；null/空白表示不限；</li>
 *   <li>{@code rulesVersion}：按作出裁决时的规则版本；null/空白表示不限；</li>
 *   <li>{@code fromRecordedMs}/{@code toRecordedMs}：按网关记录时间（recordedAt）
 *       做闭区间过滤；{@code from&lt;=recordedAt&lt;=to}；为 0/负值表示该侧不限。</li>
 * </ul>
 *
 * @param generation     运行代次 id，&lt;1 表示不限
 * @param sensorId       传感器 id，null 表示不限
 * @param rulesVersion   规则版本，null 表示不限
 * @param fromRecordedMs 记录时间下界（含），&lt;=0 表示不限
 * @param toRecordedMs   记录时间上界（含），&lt;=0 表示不限
 */
public record ReconciliationFilter(long generation, String sensorId, String rulesVersion,
                                   long fromRecordedMs, long toRecordedMs) {

    public static ReconciliationFilter all() {
        return new ReconciliationFilter(0L, null, null, 0L, 0L);
    }

    public static ReconciliationFilter forGeneration(long generation) {
        return new ReconciliationFilter(generation, null, null, 0L, 0L);
    }

    public static ReconciliationFilter forSensor(String sensorId) {
        return new ReconciliationFilter(0L, sensorId, null, 0L, 0L);
    }

    public static ReconciliationFilter forRulesVersion(String rulesVersion) {
        return new ReconciliationFilter(0L, null, rulesVersion, 0L, 0L);
    }

    public static ReconciliationFilter timeRange(long fromRecordedMs, long toRecordedMs) {
        return new ReconciliationFilter(0L, null, null, fromRecordedMs, toRecordedMs);
    }

    boolean matchesGeneration(long g) {
        return generation < 1 || generation == g;
    }

    boolean matchesSensor(String s) {
        return sensorId == null || sensorId.isBlank() || sensorId.equals(s);
    }

    boolean matchesRules(String r) {
        return rulesVersion == null || rulesVersion.isBlank() || rulesVersion.equals(r);
    }

    boolean matchesTime(long recordedAt) {
        if (fromRecordedMs > 0L && recordedAt < fromRecordedMs) {
            return false;
        }
        if (toRecordedMs > 0L && recordedAt > toRecordedMs) {
            return false;
        }
        return true;
    }
}
