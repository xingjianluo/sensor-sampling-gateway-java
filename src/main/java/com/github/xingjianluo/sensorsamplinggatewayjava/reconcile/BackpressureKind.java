package com.github.xingjianluo.sensorsamplinggatewayjava.reconcile;

/**
 * 背压终结方式：区分一条读数是“因缓冲满被直接拒绝/丢弃（最新者）”，
 * 还是“被顶掉（驱逐最旧者）”。非背压裁决为 {@link #NONE}。
 */
public enum BackpressureKind {
    /** 非背压裁决（接受，或有效性/故障/隔离类拒绝）。 */
    NONE,
    /** 有界缓冲满（或关闭期 BLOCK）：本次最新读数被直接拒绝/丢弃。 */
    REJECTED_NEW,
    /** DROP_OLDEST：队列中最旧读数被驱逐，为新读数让位。 */
    EVICTED_OLD
}
