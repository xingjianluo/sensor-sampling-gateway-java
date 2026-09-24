package com.github.xingjianluo.sensorsamplinggatewayjava.config;

/**
 * 生产快于消费、有界缓冲写满时的确定性行为策略。
 */
public enum OverflowPolicy {
    /** 阻塞生产者直到缓冲有空位（背压，不丢数） */
    BLOCK,
    /** 拒绝并丢弃最新到达的读数（计数可观测） */
    DROP_NEWEST,
    /** 移除缓冲中最旧的读数后再写入新读数（计数可观测） */
    DROP_OLDEST
}
