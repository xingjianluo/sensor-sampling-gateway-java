package com.github.xingjianluo.sensorsamplinggatewayjava.model;

/** 生产快于消费、有界缓冲已满时的确定性行为策略。 */
public enum OverflowPolicy {
    /** 阻塞生产者直到有空位（关闭时被中断的提交不计为已受理）。 */
    BLOCK,
    /** 丢弃队列中最旧读数，为新读数让位，计入 BUFFER_OVERFLOW。 */
    DROP_OLDEST,
    /** 拒绝最新读数，计入 BUFFER_OVERFLOW。 */
    DROP_NEWEST
}
