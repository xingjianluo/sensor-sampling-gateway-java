package com.github.xingjianluo.sensorsamplinggatewayjava.reconcile;

import java.util.List;

/**
 * 一个运行代次（批次）的元数据与精确聚合计数（不可变快照）。
 *
 * <p>代次级计数在每次裁决落账时原子更新，<b>不受明细保留滚动影响</b>：
 * 即使最旧明细已被保留策略顶掉，代次的总数/接受/各原因拒绝/背压/未完成数仍然精确。
 * 因此对“整代”的对账永远闭合。</p>
 *
 * <p><b>运行中切换规则版本</b>：一次 start/restart 仍是一个代次；同代内每切换一次规则
 * 版本就增加一个“阶段”，读数在入队受理时盖上当时版本的章。{@link #rulesVersions} 列出
 * 该代按生效顺序出现过的全部规则版本，可按版本在同代内切片，总账（整代）仍然闭合。</p>
 *
 * @param id             代次 id（从 1 起，JVM 内单调）
 * @param rulesVersion   该代<b>当前</b>生效的规则版本（最后一次切换后的版本；首阶段即开代版本）
 * @param rulesVersions  该代按生效顺序出现过的全部规则版本（首元素=开代版本）
 * @param startedAtMs    开代时间
 * @param sealedAtMs     封代时间；运行中为 0
 * @param sealed         是否已封代（优雅停止即封代）
 * @param total          生产总数（该代每个 sensor#tick 恰好一次落账）
 * @param accepted       接受数
 * @param rejected       拒绝总数（有效性/故障/隔离 + 直接拒绝最新读数；不含被顶掉、不含未完成）
 * @param evictedOld     被顶掉（DROP_OLDEST 驱逐最旧）数（保持老统计口径，不进 rejected）
 * @param rejectedNew    被丢弃/直接拒绝（DROP_NEWEST/关闭期 BLOCK）数（含在 rejected 中）
 * @param incomplete     已提交但关闭边界未完成裁决的滞留读数（独立分列，不进 rejected）
 * @param detailRetained 该代当前保留的明细条数
 * @param detailRollover 该代因保留策略滚动掉的明细条数（整代闭合仍由聚合计数保证）
 */
public record GenerationInfo(long id, String rulesVersion, List<String> rulesVersions,
                             long startedAtMs, long sealedAtMs, boolean sealed,
                             long total, long accepted, long rejected,
                             long evictedOld, long rejectedNew, long incomplete,
                             long detailRetained, long detailRollover) {

    /** 因背压终结的总数（被顶掉 + 被丢弃/直接拒绝）。 */
    public long backpressure() {
        return evictedOld + rejectedNew;
    }
}
