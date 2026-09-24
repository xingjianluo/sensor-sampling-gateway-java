package com.github.xingjianluo.sensorsamplinggatewayjava.model;

/**
 * 读数被拒绝的可区分原因。每种原因对应一条独立的判定规则，
 * 拒绝记录中必须携带该枚举，禁止静默丢弃。
 */
public enum RejectReason {
    /** 校验和不匹配，数据损坏 */
    CORRUPT_DATA,
    /** 数值超出传感器量程 */
    OUT_OF_RANGE,
    /** 时间戳远晚于采集网关当前时间 */
    FUTURE_TIMESTAMP,
    /** 时间戳远早于采集网关当前时间（过期读数） */
    STALE_TIMESTAMP,
    /** 传感器已被故障隔离，本次采样被跳过（不计入读数，仅在需要结论时使用） */
    SENSOR_ISOLATED,
    /** 读取时设备故障（瞬时重试耗尽或持续故障），本轮未取得有效读数 */
    SENSOR_FAULT,
    /** 有界缓冲已满，按溢出策略拒绝新读数 */
    BUFFER_OVERFLOW
}
