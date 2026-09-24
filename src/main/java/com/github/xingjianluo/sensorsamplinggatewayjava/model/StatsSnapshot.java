package com.github.xingjianluo.sensorsamplinggatewayjava.model;

import java.util.Map;

/**
 * 网关某一时刻的可观测计数快照（不可变）。
 *
 * @param produced       驱动成功产生的原始读数总数
 * @param accepted       接受并交付的样本总数
 * @param rejected       按拒绝原因分类的拒绝计数
 * @param retries        瞬时故障重试总次数
 * @param isolatedSensors 当前被隔离的传感器数量
 * @param buffered       当前缓冲中等待消费的读数数量
 */
public record StatsSnapshot(
        long produced,
        long accepted,
        Map<RejectReason, Long> rejected,
        long retries,
        int isolatedSensors,
        int buffered
) {
    public StatsSnapshot {
        rejected = Map.copyOf(rejected);
    }
}
