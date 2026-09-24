package com.github.xingjianluo.sensorsamplinggatewayjava.fault;

/**
 * 故障跟踪器对一次采样时刻给出的动作。
 */
public enum FaultDecision {
    /** 正常采样 */
    PROBE,
    /** 处于退避窗口，本次跳过 */
    SKIP,
    /** 已持续失败并被隔离，本次跳过（恢复探测由单独时钟控制） */
    ISOLATED
}
