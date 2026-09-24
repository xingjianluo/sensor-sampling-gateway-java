package com.github.xingjianluo.sensorsamplinggatewayjava.model;

import java.util.List;
import java.util.Map;

/**
 * 网关计数快照（不可变）。所有计数均为单调递增的长整型，用于并发下的
 * “不重复、不遗漏”核对：submitted = enqueued + rejectedAtSubmit，
 * 关闭排空后 enqueued = consumed + droppedOldest。
 */
public record GatewayStats(long submitted, long enqueued, long consumed,
                           long accepted, long rejected, long dropped,
                           long acquisitionFailures,
                           Map<RejectReason, Long> rejectedByReason,
                           List<String> isolatedSensors) {
}
