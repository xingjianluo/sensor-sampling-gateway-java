package com.github.xingjianluo.sensorsamplinggatewayjava.reconcile;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.RejectReason;

import java.util.List;
import java.util.Map;

/**
 * 一次对账结论（不可变快照）。由 {@link ReconciliationStore#reconcile} 计算后立即冻结，
 * 后续保留滚动不会修改或删除已出具的报告。
 *
 * <p>闭合口径（与有效性判定完全一致）：</p>
 * <ul>
 *   <li>{@code total = accepted + rejected}；</li>
 *   <li>{@code rejected = Σ rejectedByReason}（每条非接受落账恰好归入一个可区分原因）；</li>
 *   <li>{@code backpressure = evictedOld + rejectedNew}，
 *       且背压拒绝都计入 {@code rejectedByReason[BUFFER_OVERFLOW]}；</li>
 *   <li>明细身份键唯一（同一条读数只算一次）。</li>
 * </ul>
 *
 * <p>{@code complete=true} 表示当前筛选口径下的聚合计数覆盖了全部落账（整代/无时间
 * 切片且涉及代次明细未发生滚动），可直接闭合；否则 {@code detailRollover&gt;0}，
 * 报告只对“当前仍保留的明细”精确，报告中显式标注，不把账目悄悄改掉。</p>
 *
 * @param filter           生成该报告所用筛选条件
 * @param generatedAtMs    报告生成时间
 * @param complete         聚合计数是否覆盖全部落账（true 才可严格闭合）
 * @param total            生产总数
 * @param accepted         接受数
 * @param rejected         拒绝总数
 * @param evictedOld       被顶掉数量
 * @param rejectedNew      被丢弃/直接拒绝数量
 * @param rejectedByReason 按可区分原因分列的拒绝数
 * @param acceptedEntries  真正交付出去的接受明细（按 seq 升序）
 * @param rejectedEntries  拒绝明细（按 seq 升序，含原因与背压方式）
 * @param perSensor        按传感器分列的小计
 * @param detailRollover   涉及代次中因保留策略滚动掉的明细总数
 */
public record ReconciliationReport(ReconciliationFilter filter, long generatedAtMs,
                                   boolean complete,
                                   long total, long accepted, long rejected,
                                   long evictedOld, long rejectedNew,
                                   Map<RejectReason, Long> rejectedByReason,
                                   List<ReconciliationEntry> acceptedEntries,
                                   List<ReconciliationEntry> rejectedEntries,
                                   Map<String, SensorTotals> perSensor,
                                   long detailRollover) {

    /** 单个传感器在筛选口径下的小计（不可变）。 */
    public record SensorTotals(long total, long accepted, long rejected,
                               long evictedOld, long rejectedNew) {
    }
}
