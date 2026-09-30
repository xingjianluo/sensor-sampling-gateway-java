package com.github.xingjianluo.sensorsamplinggatewayjava.reconcile;

import java.util.List;

/**
 * 一个运行代次（批次）的元数据与精确聚合计数（不可变快照）。
 *
 * <p>代次级计数在每次裁决落账时原子更新，<b>不受明细保留滚动影响</b>：
 * 即使最旧明细已被保留策略顶掉，代次的总数/接受/各原因拒绝/背压数仍然精确。
 * 因此对“整代”的对账永远闭合。</p>
 *
 * <p>支持“运行中切换规则版本”：同一次运行（同一代次）内可出现多个规则版本，
 * {@link #rulesVersions()} 按首次出现顺序给出该代判定过的全部版本；切换前已受理
 * 读数归旧版本、切换后新读数归新版本，每个版本都可单独筛选，而整代总账仍闭合。
 * {@link #rulesVersion()} 为开代时的起始版本（首元素），便于旧调用方使用。</p>
 *
 * @param id             代次 id（从 1 起，JVM 内单调）
 * @param rulesVersion   开代时的起始规则版本（= rulesVersions 首元素）
 * @param startedAtMs    开代时间
 * @param sealedAtMs     封代时间；运行中为 0
 * @param sealed         是否已封代（优雅停止即封代）
 * @param total          生产总数（该代每个 sensor#tick 恰好一次落账，含未完成记录）
 * @param accepted       接受数
 * @param rejected       拒绝总数（沿用历史口径：不含 DROP_OLDEST 被顶掉数）
 * @param evictedOld     被顶掉（DROP_OLDEST 驱逐最旧）数，单列、不计入 rejected
 * @param rejectedNew    被丢弃/直接拒绝（DROP_NEWEST/关闭期 BLOCK）数，计入 rejected
 * @param unfinished     关闭排空超时仍滞留缓冲的未完成读数（SHUTDOWN_INCOMPLETE）数
 * @param detailRetained 该代当前保留的明细条数
 * @param detailRollover 该代因保留策略滚动掉的明细条数（整代闭合仍由聚合计数保证）
 * @param rulesVersions  该代内出现过的全部规则版本（按首次出现顺序，去重）
 */
public record GenerationInfo(long id, String rulesVersion,
                             long startedAtMs, long sealedAtMs, boolean sealed,
                             long total, long accepted, long rejected,
                             long evictedOld, long rejectedNew, long unfinished,
                             long detailRetained, long detailRollover,
                             List<String> rulesVersions) {

    /** 兼容旧签名：无未完成读数、单一规则版本。 */
    public GenerationInfo(long id, String rulesVersion,
                          long startedAtMs, long sealedAtMs, boolean sealed,
                          long total, long accepted, long rejected,
                          long evictedOld, long rejectedNew,
                          long detailRetained, long detailRollover) {
        this(id, rulesVersion, startedAtMs, sealedAtMs, sealed,
                total, accepted, rejected, evictedOld, rejectedNew, 0L,
                detailRetained, detailRollover, List.of(rulesVersion));
    }

    /** 因背压终结的总数（被顶掉 + 被丢弃/直接拒绝）。 */
    public long backpressure() {
        return evictedOld + rejectedNew;
    }

    /** 该代是否为“完整运行”：无滞留未完成读数。 */
    public boolean completeRun() {
        return unfinished == 0L;
    }
}
