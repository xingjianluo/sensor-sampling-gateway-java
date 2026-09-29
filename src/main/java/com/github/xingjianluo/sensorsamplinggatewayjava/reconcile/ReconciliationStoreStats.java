package com.github.xingjianluo.sensorsamplinggatewayjava.reconcile;

/**
 * 对账台账自身的容量与保留统计（不可变快照），用于明确“保留与容量边界”。
 *
 * @param runningGeneration             当前运行代次 id；无运行代次时为 0
 * @param generationsCreated            累计创建的代次数
 * @param generationsRetained           当前仍保留（运行 + 已封代）的代次数
 * @param generationsPurged             因封代代次保留上限被确定性清除的代数数
 * @param entriesAppended               累计落账条数（每条读数恰好一条）
 * @param detailsRetained               当前保留的明细总条数
 * @param detailsRollover               累计因明细容量上限被滚动/忽略的明细条数
 * @param appendsWhileNoRunningGen      无运行代次时的落账次数（确定性拒绝，不计数）
 * @param detailCapacityPerGeneration   每代明细容量上限
 * @param maxSealedGenerations          封代代次保留上限
 * @param retentionPolicy               明细超容量后的行为
 */
public record ReconciliationStoreStats(long runningGeneration,
                                       long generationsCreated,
                                       long generationsRetained,
                                       long generationsPurged,
                                       long entriesAppended,
                                       long detailsRetained,
                                       long detailsRollover,
                                       long appendsWhileNoRunningGen,
                                       int detailCapacityPerGeneration,
                                       int maxSealedGenerations,
                                       RetentionPolicy retentionPolicy) {
}
