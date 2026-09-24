package com.github.xingjianluo.sensorsamplinggatewayjava.gateway;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.SampleOutcome;

/**
 * 采样结论的最终交付出口。消费端对每个原始读数恰好调用一次，
 * 保证不重不漏；实现需线程安全。
 */
public interface OutcomeSink {

    void deliver(SampleOutcome outcome);
}
