package com.github.xingjianluo.sensorsamplinggatewayjava.reconcile;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.RejectReason;

/**
 * 一条读数在对账台账中的最终归宿（不可变）。与 {@code Verdict} 一一对应（或在关闭边界
 * 为一条滞留读数补记的未完成记录），额外携带代次、规则版本、全局序列号、记录时间、
 * 背压方式与终结方式，使账目可按批次/传感器/规则版本/时间范围切片核对。
 *
 * <p>身份键 {@code (generation, sensorId, tick)} 全局唯一：同一条读数在台账中
 * 恰好出现一次，不会多算也不会漏算。</p>
 *
 * <p>顶层终结方式 {@link #disposition()} 四分类互斥且穷尽：
 * {@code ACCEPTED / REJECTED / EVICTED_OLD / INCOMPLETE}。
 * 其中被顶掉（EVICTED_OLD）不抬高“拒绝总数”，与关闭未完成（INCOMPLETE）都单列。</p>
 *
 * @param seq             全局单调递增序列号（裁决发生顺序，用于稳定排序）
 * @param generation      运行代次 id（一次 start/restart 为一代；运行中切换规则版本不开新代）
 * @param rulesVersion    作出该裁决时生效的规则版本（读数入队受理时盖章）
 * @param sensorId        传感器编号
 * @param tick            该传感器生产者线程内单调递增的拍号
 * @param readingTsMs     读数自带时间戳
 * @param recordedAtMs    网关记录该裁决的时间（时间范围过滤口径）
 * @param accepted        是否接受（通过当前规则版本的有效性判定）
 * @param reason          拒绝原因；接受时为 null；被顶掉为 BUFFER_OVERFLOW；未完成记录为 SHUTDOWN_INCOMPLETE
 * @param calibratedValue 校准后值；非接受时为 NaN
 * @param backpressure    背压终结方式
 * @param disposition     顶层终结方式（四分类）
 */
public record ReconciliationEntry(long seq, long generation, String rulesVersion,
                                  String sensorId, long tick,
                                  long readingTsMs, long recordedAtMs,
                                  boolean accepted, RejectReason reason,
                                  double calibratedValue, BackpressureKind backpressure,
                                  Disposition disposition) {

    public ReconciliationEntry {
        if (rulesVersion == null || sensorId == null || backpressure == null || disposition == null) {
            throw new IllegalArgumentException(
                    "rulesVersion/sensorId/backpressure/disposition must not be null");
        }
        if (!accepted && reason == null) {
            throw new IllegalArgumentException("non-accepted entry must carry a reason");
        }
        Disposition expected = resolve(accepted, reason, backpressure);
        if (disposition != expected) {
            throw new IllegalArgumentException(
                    "disposition mismatch: " + disposition + " vs " + expected);
        }
    }

    /** 按 (accepted, reason, backpressure) 唯一确定顶层终结方式。 */
    public static Disposition resolve(boolean accepted, RejectReason reason, BackpressureKind backpressure) {
        if (accepted) {
            return Disposition.ACCEPTED;
        }
        if (backpressure == BackpressureKind.EVICTED_OLD) {
            return Disposition.EVICTED_OLD;
        }
        if (reason == RejectReason.SHUTDOWN_INCOMPLETE) {
            return Disposition.INCOMPLETE;
        }
        return Disposition.REJECTED;
    }

    /** 代次内唯一键：sensor#tick。 */
    public String sensorTickKey() {
        return sensorId + "#" + tick;
    }
}
