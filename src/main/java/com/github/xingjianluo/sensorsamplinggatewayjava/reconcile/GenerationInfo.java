package com.github.xingjianluo.sensorsamplinggatewayjava.reconcile;

/**
 * 一个运行代次（批次）的元数据与精确聚合计数（不可变快照）。
 *
 * <p>代次级计数在每次裁决落账时原子更新，<b>不受明细保留滚动影响</b>：
 * 即使最旧明细已被保留策略顶掉，代次的总数/接受/各原因拒绝/背压数仍然精确。
 * 因此对“整代”的对账永远闭合。</p>
 *
 * @param id             代次 id（从 1 起，JVM 内单调）
 * @param rulesVersion   该代固定使用的规则版本
 * @param startedAtMs    开代时间
 * @param sealedAtMs     封代时间；运行中为 0
 * @param sealed         是否已封代（优雅停止即封代）
 * @param total          生产总数（该代每个 sensor#tick 恰好一次落账）
 * @param accepted       接受数
 * @param rejected       拒绝总数（含背压拒绝）
 * @param evictedOld     被顶掉（DROP_OLDEST 驱逐最旧）数
 * @param rejectedNew    被丢弃/直接拒绝（DROP_NEWEST/关闭期 BLOCK）数
 * @param detailRetained 该代当前保留的明细条数
 * @param detailRollover 该代因保留策略滚动掉的明细条数（整代闭合仍由聚合计数保证）
 */
public record GenerationInfo(long id, String rulesVersion,
                             long startedAtMs, long sealedAtMs, boolean sealed,
                             long total, long accepted, long rejected,
                             long evictedOld, long rejectedNew,
                             long detailRetained, long detailRollover) {

    /** 因背压终结的总数（被顶掉 + 被丢弃/直接拒绝）。 */
    public long backpressure() {
        return evictedOld + rejectedNew;
    }
}
