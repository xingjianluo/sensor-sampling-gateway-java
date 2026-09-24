package com.github.xingjianluo.sensorsamplinggatewayjava.gateway;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.SampleOutcome;

import java.util.ArrayList;
import java.util.List;

/**
 * 测试/本地验证用的内存结论收集器，按交付顺序保存全部结论，
 * 便于核对计数、拒绝原因与可复现性。
 */
public class RecordingOutcomeSink implements OutcomeSink {

    private final List<SampleOutcome> outcomes = new ArrayList<>();

    public RecordingOutcomeSink() {
    }

    @Override
    public synchronized void deliver(SampleOutcome outcome) {
        outcomes.add(outcome);
    }

    /** 已交付结论的线程安全快照（按交付顺序）。 */
    public synchronized List<SampleOutcome> outcomes() {
        return new ArrayList<>(outcomes);
    }

    public synchronized int size() {
        return outcomes.size();
    }

    public synchronized void clear() {
        outcomes.clear();
    }
}
