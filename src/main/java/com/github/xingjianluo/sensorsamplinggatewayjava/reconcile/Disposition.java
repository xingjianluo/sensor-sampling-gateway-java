package com.github.xingjianluo.sensorsamplinggatewayjava.reconcile;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.RejectReason;

/**
 * 一条已提交读数在对账台账中的终结方式（四分类，互斥且穷尽）。
 *
 * <p>这是“账目闭合”用的顶层桶：每条读数恰好归入其中一类，
 * {@code total = accepted + rejected + evictedOld + incomplete}。</p>
 */
public enum Disposition {
    /** 接受：通过作出裁决时规则版本的有效性判定并交付。 */
    ACCEPTED,
    /** 拒绝：有效性/故障/隔离类拒绝，或缓冲满时直接拒绝最新读数（REJECTED_NEW 背压）。 */
    REJECTED,
    /** 背压：DROP_OLDEST 为腾空间顶掉的最旧读数（保持老统计口径，不抬高拒绝总数）。 */
    EVICTED_OLD,
    /** 关闭边界未完成：已提交但排空超时/消费者中断使其没得到接受或拒绝裁决。 */
    INCOMPLETE
}
