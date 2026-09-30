package com.github.xingjianluo.sensorsamplinggatewayjava.model;

/**
 * 读数被拒绝/采样未形成有效结论的原因，必须可区分，不得静默丢弃。
 */
public enum RejectReason {
    /** 数据损坏：校验和不匹配，数值不可信。 */
    DATA_CORRUPT,
    /** 时间戳异常：与网关时钟偏差超过容差。 */
    TIMESTAMP_ANOMALY,
    /** 超量程：数值超出传感器配置量程。 */
    OUT_OF_RANGE,
    /** 有界缓冲已满，按背压策略丢弃（仅 DROP_NEWEST / DROP_OLDEST）。 */
    BUFFER_OVERFLOW,
    /** 传感器已被故障隔离，不再受理其读数。 */
    SENSOR_ISOLATED,
    /** 采样动作在有界重试后仍失败（设备瞬时/持续故障）。 */
    ACQUISITION_FAILED,
    /**
     * 关闭边界的未完成：读数已提交（已入队），但关闭排空超时、消费者卡住等原因使其
     * 在进程停止前没有得到接受/拒绝裁决。与普通拒绝分列，绝不静默丢失。
     */
    SHUTDOWN_INCOMPLETE
}
