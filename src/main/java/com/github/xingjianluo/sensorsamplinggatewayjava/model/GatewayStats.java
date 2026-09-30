package com.github.xingjianluo.sensorsamplinggatewayjava.model;

import java.util.List;
import java.util.Map;

/**
 * 网关计数快照（不可变）。所有计数均为单调递增的长整型，用于并发下的
 * “不重复、不遗漏”核对。
 *
 * <p>老口径保持不变（{@code submitted/enqueued/consumed/accepted/rejected/dropped}
 * 含义与对账改造前一致），新增的三类分列字段只是把老数字拆细，不改其总和：</p>
 * <ul>
 *   <li>{@code submitted = enqueued + rejected}（关闭前；关闭期阻塞提交被直接拒绝）；</li>
 *   <li>{@code dropped = evictedOld + rejectedNew}（两类背压分列；被顶掉不抬高 rejected）；</li>
 *   <li>{@code submitted = accepted + rejected + evictedOld + shutdownIncomplete}（总闭合）；</li>
 *   <li>{@code enqueued = consumed + evictedOld + shutdownIncomplete}（关闭排空后）。</li>
 * </ul>
 *
 * @param evictedOld        DROP_OLDEST 为腾空间顶掉的最旧读数（背压之一，不计 rejected）
 * @param rejectedNew       缓冲满/关闭期被直接拒绝或丢弃的最新读数（背压之二，= byReason[BUFFER_OVERFLOW]）
 * @param shutdownIncomplete 已提交但关闭边界未完成裁决的滞留读数（独立分列，不计 rejected）
 */
public record GatewayStats(long submitted, long enqueued, long consumed,
                           long accepted, long rejected, long dropped,
                           long evictedOld, long rejectedNew, long shutdownIncomplete,
                           long acquisitionFailures,
                           Map<RejectReason, Long> rejectedByReason,
                           List<String> isolatedSensors) {
}
