package com.github.xingjianluo.sensorsamplinggatewayjava.reconcile;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.RejectReason;

import java.util.List;
import java.util.Map;

/**
 * 一次对账结论（不可变快照）。由 {@link ReconciliationStore#reconcile} 计算后立即冻结，
 * 后续保留滚动不会修改或删除已出具的报告。
 *
 * <p>闭合口径（四类终结方式互斥且穷尽，与有效性判定完全一致）：</p>
 * <ul>
 *   <li>{@code total = accepted + rejected + evictedOld + incomplete}；</li>
 *   <li>{@code rejected = Σ rejectedByReason}（有效性/故障/隔离类拒绝，
 *       以及直接拒绝最新读数的 {@code BUFFER_OVERFLOW}）；</li>
 *   <li>{@code backpressure = evictedOld + rejectedNew}，
 *       且 {@code rejectedNew = rejectedByReason[BUFFER_OVERFLOW]}；
 *       <b>被顶掉 {@code evictedOld} 不抬高拒绝总数</b>（保持老看板/脚本口径）；</li>
 *   <li>{@code incomplete} 是已提交但关闭边界未完成裁决的读数，单列且令
 *       {@code complete=false}；</li>
 *   <li>明细身份键唯一（同一条读数只算一次）。</li>
 * </ul>
 *
 * <p>{@code complete=true} 要求同时满足：涉及代次的明细一条未滚动、且筛选口径内没有
 * 未完成记录。明细滚动后聚合计数仍精确（整代/无时间切片），但报告以 {@code complete=false}
 * 与 {@code detailRollover} 显式标注，绝不把账目悄悄改掉。</p>
 *
 * @param filter            生成该报告所用筛选条件
 * @param generatedAtMs     报告生成时间
 * @param complete          聚合计数是否覆盖全部终结读数且明细无缺失、无未完成（true 才可严格逐条闭合）
 * @param total             生产总数
 * @param accepted          接受数
 * @param rejected          拒绝总数（不含被顶掉、不含未完成）
 * @param evictedOld        被顶掉数量（背压之一，老 dropped 的一部分，老 rejected 不含它）
 * @param rejectedNew       被丢弃/直接拒绝数量（背压之二，含在 rejected 中）
 * @param incomplete        关闭边界未完成数量
 * @param rejectedByReason  按可区分原因分列的拒绝数（仅 REJECTED 桶；EVICTED_OLD/INCOMPLETE 不在内）
 * @param acceptedEntries   真正交付出去的接受明细（按 seq 升序）
 * @param rejectedEntries   拒绝明细（按 seq 升序，含原因与 REJECTED_NEW 背压）
 * @param evictedEntries    被顶掉明细（按 seq 升序，EVICTED_OLD 背压）
 * @param incompleteEntries 关闭边界未完成明细（按 seq 升序）
 * @param perSensor         按传感器分列的小计
 * @param detailRollover    涉及代次中因保留策略滚动掉的明细总数
 */
public record ReconciliationReport(ReconciliationFilter filter, long generatedAtMs,
                                   boolean complete,
                                   long total, long accepted, long rejected,
                                   long evictedOld, long rejectedNew, long incomplete,
                                   Map<RejectReason, Long> rejectedByReason,
                                   List<ReconciliationEntry> acceptedEntries,
                                   List<ReconciliationEntry> rejectedEntries,
                                   List<ReconciliationEntry> evictedEntries,
                                   List<ReconciliationEntry> incompleteEntries,
                                   Map<String, SensorTotals> perSensor,
                                   long detailRollover) {

    /** 因背压终结的总数（被顶掉 + 被丢弃/直接拒绝）。 */
    public long backpressure() {
        return evictedOld + rejectedNew;
    }

    /** 单个传感器在筛选口径下的小计（不可变）。 */
    public record SensorTotals(long total, long accepted, long rejected,
                               long evictedOld, long rejectedNew, long incomplete) {

        public long backpressure() {
            return evictedOld + rejectedNew;
        }
    }
}
