package com.github.xingjianluo.sensorsamplinggatewayjava.reconcile;

/**
 * 对账明细超出每代容量上限后的确定性行为（只作用于“明细保留”，不影响采样与裁决本身）。
 *
 * <p>无论哪种策略，代次级/传感器级的聚合计数都始终精确，已出具的
 * {@link ReconciliationReport} 都是不可变快照，不会被保留策略修改或删除。</p>
 */
public enum RetentionPolicy {
    /** 容量满后，最旧明细被确定性地滚动掉，为新明细让位；聚合计数不受影响。 */
    ROLL_OLDEST,
    /** 容量满后，新裁决只更新聚合计数、不再保留明细；已保留明细一条不改、一条不删。 */
    IGNORE_NEW
}
