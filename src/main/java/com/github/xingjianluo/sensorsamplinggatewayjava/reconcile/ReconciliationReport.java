package com.github.xingjianluo.sensorsamplinggatewayjava.reconcile;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.RejectReason;

import java.util.List;
import java.util.Map;

/**
 * 一次对账结论（不可变快照）。由 {@link ReconciliationStore#reconcile} 计算后立即冻结，
 * 后续保留滚动不会修改或删除已出具的报告。
 *
 * <p>闭合口径（历史统计口径保持不变，两类背压单列）：</p>
 * <ul>
 *   <li>{@code total = accepted + rejected + evictedOld}（每个受理读数恰一条落账；
 *       DROP_OLDEST 被顶掉读数单列，不重复计入 {@code rejected}）；</li>
 *   <li>{@code rejected = Σ rejectedByReason}（每条非接受、非顶掉落账恰归一个可区分原因）；</li>
 *   <li>{@code rejectedNew = rejectedByReason[BUFFER_OVERFLOW]}（直接丢弃最新读数）；</li>
 *   <li>{@code backpressure = evictedOld + rejectedNew}；</li>
 *   <li>{@code unfinished}：关闭排空超时仍滞留缓冲、未完成判定的已受理读数
 *       （SHUTDOWN_INCOMPLETE），计入 {@code rejected}/{@code total} 并以独立原因可见，
 *       保证生产总数与对账总数永久可对平；</li>
 *   <li>明细身份键唯一（同一条读数只算一次）。</li>
 * </ul>
 *
 * <p>{@code complete=true} 表示：运行中没有滞留未完成读数，且当前筛选口径下明细一条不少
 * （整代/无时间切片且涉及代次明细未发生滚动）。否则为 {@code false}——可能是明细被
 * 保留策略滚动（{@code detailRollover&gt;0}），也可能是存在 {@code unfinished&gt;0}
 * 的未完成读数；两者都在报告中显式标注，绝不把不完整运行伪装成完整。</p>
 *
 * @param filter           生成该报告所用筛选条件
 * @param generatedAtMs    报告生成时间
 * @param complete         是否完整（无未完成读数且明细全覆盖）；false 为显式不完整标记
 * @param total            生产总数（= accepted + rejected + evictedOld）
 * @param accepted         接受数
 * @param rejected         拒绝总数（历史口径：不含 DROP_OLDEST 被顶掉数）
 * @param evictedOld       被顶掉数量（单列，不在 rejected 内）
 * @param rejectedNew      被丢弃/直接拒绝数量（在 rejected 内，= BUFFER_OVERFLOW 计数）
 * @param unfinished       关闭滞留未完成读数数量（SHUTDOWN_INCOMPLETE）
 * @param rejectedByReason 按可区分原因分列的拒绝数（不含被顶掉）
 * @param acceptedEntries  真正交付出去的接受明细（按 seq 升序）
 * @param rejectedEntries  拒绝明细（按 seq 升序，含原因与背压方式，含未完成记录）
 * @param perSensor        按传感器分列的小计
 * @param detailRollover   涉及代次中因保留策略滚动掉的明细总数
 */
public record ReconciliationReport(ReconciliationFilter filter, long generatedAtMs,
                                   boolean complete,
                                   long total, long accepted, long rejected,
                                   long evictedOld, long rejectedNew, long unfinished,
                                   Map<RejectReason, Long> rejectedByReason,
                                   List<ReconciliationEntry> acceptedEntries,
                                   List<ReconciliationEntry> rejectedEntries,
                                   Map<String, SensorTotals> perSensor,
                                   long detailRollover) {

    /** 兼容旧签名：无未完成读数。 */
    public ReconciliationReport(ReconciliationFilter filter, long generatedAtMs,
                                boolean complete,
                                long total, long accepted, long rejected,
                                long evictedOld, long rejectedNew,
                                Map<RejectReason, Long> rejectedByReason,
                                List<ReconciliationEntry> acceptedEntries,
                                List<ReconciliationEntry> rejectedEntries,
                                Map<String, SensorTotals> perSensor,
                                long detailRollover) {
        this(filter, generatedAtMs, complete, total, accepted, rejected,
                evictedOld, rejectedNew, 0L, rejectedByReason,
                acceptedEntries, rejectedEntries, perSensor, detailRollover);
    }

    /** 单个传感器在筛选口径下的小计（不可变）。 */
    public record SensorTotals(long total, long accepted, long rejected,
                               long evictedOld, long rejectedNew, long unfinished) {

        /** 兼容旧签名：无未完成读数。 */
        public SensorTotals(long total, long accepted, long rejected,
                            long evictedOld, long rejectedNew) {
            this(total, accepted, rejected, evictedOld, rejectedNew, 0L);
        }
    }
}
