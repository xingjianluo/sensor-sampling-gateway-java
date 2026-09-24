package com.github.xingjianluo.sensorsamplinggatewayjava.pipeline;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.Verdict;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/**
 * 裁决结论的有界环形记录（最近 N 条），供测试与本地验证逐条核对输入与依据。
 * 线程安全：以一把锁保护内部队列，所有读取返回快照拷贝。
 */
public final class VerdictLog {

    private final int capacity;
    private final ArrayDeque<Verdict> deque = new ArrayDeque<>();

    public VerdictLog(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be >= 1");
        }
        this.capacity = capacity;
    }

    public synchronized void append(Verdict verdict) {
        deque.addLast(verdict);
        while (deque.size() > capacity) {
            deque.removeFirst();
        }
    }

    /** 最近的裁决，按时间从旧到新排列。 */
    public synchronized List<Verdict> recent() {
        return new ArrayList<>(deque);
    }

    public synchronized List<Verdict> bySensor(String sensorId) {
        List<Verdict> out = new ArrayList<>();
        for (Verdict v : deque) {
            if (v.reading().sensorId().equals(sensorId)) {
                out.add(v);
            }
        }
        return out;
    }

    /** 按 sensor#tick 唯一键精确查找（环形窗口内）。 */
    public synchronized Verdict byKey(String key) {
        for (Verdict v : deque) {
            if (v.reading().key().equals(key)) {
                return v;
            }
        }
        return null;
    }

    public synchronized int size() {
        return deque.size();
    }

    public synchronized void clear() {
        deque.clear();
    }
}
