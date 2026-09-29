package com.github.xingjianluo.sensorsamplinggatewayjava.reconcile;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.Verdict;

/**
 * 对账账目条目（不可变）：一条裁决 + 归属运行代次 + 记录时间 + 背压去向。
 *
 * @param runId        作出该裁决时的运行代次
 * @param recordedAtMs 条目入账时间（网关时钟，用于时间范围查询）
 * @param verdict      裁决（接受/拒绝及可区分原因、规则版本、输入依据）
 * @param evicted      是否为 DROP_OLDEST 顶掉（false 时若原因为 BUFFER_OVERFLOW 则为 DROP_NEWEST 拒入）
 */
public record ReconcileEntry(long runId, long recordedAtMs, Verdict verdict, boolean evicted) {

    /** 全局唯一身份：运行代次 + 传感器内 tick。跨代 tick 可重置，故必须带代次。 */
    public String key() {
        return runId + ":" + verdict.reading().key();
    }
}
