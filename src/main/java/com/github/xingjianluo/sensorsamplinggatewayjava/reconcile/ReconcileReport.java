package com.github.xingjianluo.sensorsamplinggatewayjava.reconcile;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.RejectReason;

import java.util.List;
import java.util.Map;

/**
 * 对账结论（不可变）。
 *
 * <p><b>计数恒等式（任何情况下都必须闭合，否则直接抛错）：</b></p>
 * <ul>
 *   <li>{@code produced == accepted + sum(rejectedByReason)}：一条读数只算一次；</li>
 *   <li>{@code dropped == rejectedByReason[BUFFER_OVERFLOW]}：
 *       DROP_NEWEST 拒入与 DROP_OLDEST 顶掉都带 BUFFER_OVERFLOW 原因；</li>
 * </ul>
 *
 * <p><b>明细与计数的关系：</b></p>
 * <ul>
 *   <li>{@code countersExact=true}：计数来自不受明细保留策略影响的精确聚合账目
 *       （只按 run/传感器/规则版本筛选时恒为 true）。此时 {@code complete} 取决于明细窗口
 *       是否也一条不丢；明细被 FIFO 顶掉时计数仍精确，但 {@code deliveredMissing} /
 *       {@code rejectedMissing} 显式给出缺失条数，绝不静默。</li>
 *   <li>{@code countersExact=false}：仅出现在带时间范围的查询上。计数只能基于保留窗口内
 *       明细重算，数字彼此仍闭合，但只覆盖窗口，{@code complete=false}。</li>
 * </ul>
 *
 * @param produced            生产总数（查询范围内每个采样尝试恰好计一次）
 * @param accepted            通过当前规则版本判定的接受数
 * @param rejectedByReason    按可区分原因分列的拒绝数
 * @param dropped             因背压未交付数（DROP_NEWEST 拒入 + DROP_OLDEST 顶掉）
 * @param rejectedDetail      保留窗口内命中条件的拒绝明细（按入账时间从旧到新）
 * @param deliveredDetail     保留窗口内命中条件的交付（接受）明细（按入账时间从旧到新）
 * @param countersExact       计数是否来自全量精确聚合
 * @param complete            计数精确且交付/拒绝明细也一条不缺
 * @param rejectedMissing     精确计数里有、但明细窗口已顶掉的拒绝条数
 * @param deliveredMissing    精确计数里有、但明细窗口已顶掉的交付条数
 * @param retainedDetailCount 命中查询条件、当前仍保留的明细条数
 */
public record ReconcileReport(long produced, long accepted,
                              Map<RejectReason, Long> rejectedByReason, long dropped,
                              List<ReconcileEntry> rejectedDetail,
                              List<ReconcileEntry> deliveredDetail,
                              boolean countersExact, boolean complete,
                              long rejectedMissing, long deliveredMissing,
                              long retainedDetailCount) {

    public ReconcileReport {
        rejectedByReason = Map.copyOf(rejectedByReason);
        rejectedDetail = List.copyOf(rejectedDetail);
        deliveredDetail = List.copyOf(deliveredDetail);
        long rejectedSum = rejectedByReason.values().stream().mapToLong(Long::longValue).sum();
        if (produced != accepted + rejectedSum) {
            throw new IllegalStateException("reconciliation not closed: produced=" + produced
                    + " but accepted=" + accepted + " + rejected=" + rejectedSum);
        }
        long overflow = rejectedByReason.getOrDefault(RejectReason.BUFFER_OVERFLOW, 0L);
        if (dropped != overflow) {
            throw new IllegalStateException("dropped=" + dropped
                    + " must equal BUFFER_OVERFLOW rejections=" + overflow);
        }
        if (rejectedMissing < 0 || deliveredMissing < 0) {
            throw new IllegalStateException("missing counts must be non-negative");
        }
        if (complete != (countersExact && rejectedMissing == 0 && deliveredMissing == 0)) {
            throw new IllegalStateException("complete must equal exact counters with no missing detail");
        }
        if (retainedDetailCount != (long) rejectedDetail.size() + deliveredDetail.size()) {
            throw new IllegalStateException("retainedDetailCount inconsistent with detail rows");
        }
    }
}
