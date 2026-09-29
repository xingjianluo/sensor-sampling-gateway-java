package com.github.xingjianluo.sensorsamplinggatewayjava.reconcile;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.RejectReason;

/**
 * 一条读数在对账台账中的最终归宿（不可变）。与 {@code Verdict} 一一对应，
 * 但额外携带代次、全局序列号、记录时间与背压方式，使账目可按批次/传感器/
 * 规则版本/时间范围切片核对。
 *
 * <p>身份键 {@code (generation, sensorId, tick)} 全局唯一：同一条读数在台账中
 * 恰好出现一次，不会多算也不会漏算。</p>
 *
 * @param seq             全局单调递增序列号（裁决发生顺序，用于稳定排序）
 * @param generation      运行代次 id（每次 start/restart 开新一代）
 * @param rulesVersion    作出该裁决时的规则版本
 * @param sensorId        传感器编号
 * @param tick            该传感器生产者线程内单调递增的拍号
 * @param readingTsMs     读数自带时间戳
 * @param recordedAtMs    网关记录该裁决的时间（时间范围过滤口径）
 * @param accepted        是否接受（通过当前规则版本的有效性判定）
 * @param reason          拒绝原因；接受时为 null
 * @param calibratedValue 校准后值；拒绝时为 NaN
 * @param backpressure    背压终结方式
 */
public record ReconciliationEntry(long seq, long generation, String rulesVersion,
                                  String sensorId, long tick,
                                  long readingTsMs, long recordedAtMs,
                                  boolean accepted, RejectReason reason,
                                  double calibratedValue, BackpressureKind backpressure) {

    public ReconciliationEntry {
        if (rulesVersion == null || sensorId == null || backpressure == null) {
            throw new IllegalArgumentException("rulesVersion/sensorId/backpressure must not be null");
        }
        if (!accepted && reason == null) {
            throw new IllegalArgumentException("rejected entry must carry a reason");
        }
    }

    /** 代次内唯一键：sensor#tick。 */
    public String sensorTickKey() {
        return sensorId + "#" + tick;
    }
}
