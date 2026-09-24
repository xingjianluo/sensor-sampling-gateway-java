package com.github.xingjianluo.sensorsamplinggatewayjava.registry;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorSpec;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 传感器配置与规则版本登记表。线程安全；支持配置/校准更新，
 * 并保留每个传感器的历史版本，使“相同输入 + 规则版本”的结论可追溯、可复现。
 *
 * <p>同一传感器的 ruleVersion 必须严格单调递增，防止旧配置覆盖新配置造成结论不确定。
 */
public class SensorRegistry {

    private final Map<String, CopyOnWriteArrayList<SensorSpec>> historyBySensor = new ConcurrentHashMap<>();

    public SensorRegistry() {
    }

    /** 注册或更新一个传感器配置；ruleVersion 必须单调递增。 */
    public void register(SensorSpec spec) {
        CopyOnWriteArrayList<SensorSpec> history =
                historyBySensor.computeIfAbsent(spec.sensorId(), k -> new CopyOnWriteArrayList<>());
        synchronized (history) {
            if (!history.isEmpty()) {
                SensorSpec latest = history.get(history.size() - 1);
                if (spec.ruleVersion().compareTo(latest.ruleVersion()) <= 0) {
                    throw new IllegalArgumentException(String.format(
                            "ruleVersion must increase for sensor=%s existing=%s rejected=%s",
                            spec.sensorId(), latest.ruleVersion(), spec.ruleVersion()));
                }
            }
            history.add(spec);
        }
    }

    public Optional<SensorSpec> current(String sensorId) {
        CopyOnWriteArrayList<SensorSpec> history = historyBySensor.get(sensorId);
        if (history == null || history.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(history.get(history.size() - 1));
    }

    public Map<String, SensorSpec> allCurrent() {
        Map<String, SensorSpec> result = new java.util.TreeMap<>();
        historyBySensor.forEach((id, history) -> {
            if (!history.isEmpty()) {
                result.put(id, history.get(history.size() - 1));
            }
        });
        return result;
    }

    /** 某传感器全部历史版本（按注册顺序）。 */
    public List<SensorSpec> history(String sensorId) {
        CopyOnWriteArrayList<SensorSpec> history = historyBySensor.get(sensorId);
        if (history == null) {
            return List.of();
        }
        return new ArrayList<>(history);
    }
}
