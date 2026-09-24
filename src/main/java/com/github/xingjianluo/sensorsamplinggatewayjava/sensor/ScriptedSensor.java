package com.github.xingjianluo.sensorsamplinggatewayjava.sensor;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.RawReading;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SampleScript;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorConfig;
import com.github.xingjianluo.sensorsamplinggatewayjava.time.Clock;

import java.util.List;
import java.util.Objects;

/**
 * 完全确定性的模拟传感器：按脚本逐拍产出读数或抛出故障，不依赖任何硬件。
 *
 * <ul>
 *   <li>{@code NORMAL/OUT_OF_RANGE/BAD_TIMESTAMP/CORRUPT} 产出一条读数；</li>
 *   <li>{@code TRANSIENT_FAULT/PERSISTENT_FAULT} 抛出 {@link SensorFaultException}，
 *       由上层按重试与断路器规则处理；</li>
 *   <li>脚本越界后产出量程中值，保证长跑压力测试可无限进行且仍确定。</li>
 * </ul>
 */
public final class ScriptedSensor implements Sensor {

    private final SensorConfig config;
    private final List<SampleScript> script;
    private final Clock clock;
    private final SampleScript beyond;

    public ScriptedSensor(SensorConfig config, List<SampleScript> script, Clock clock) {
        this(config, script, clock, null);
    }

    /**
     * @param beyondScript 脚本越界后的动作；null 表示产出量程中值（长跑压力测试用）。
     */
    public ScriptedSensor(SensorConfig config, List<SampleScript> script, Clock clock,
                          SampleScript beyondScript) {
        this.config = Objects.requireNonNull(config, "config");
        this.script = List.copyOf(script);
        this.clock = Objects.requireNonNull(clock, "clock");
        this.beyond = beyondScript;
    }

    @Override
    public SensorConfig config() {
        return config;
    }

    @Override
    public RawReading sample(long tick) throws Exception {
        SampleScript s = tick < script.size()
                ? script.get((int) tick)
                : (beyond != null ? beyond
                        : new SampleScript(SampleScript.Kind.NORMAL, midRange(), 0L, false));

        return switch (s.kind()) {
            case TRANSIENT_FAULT -> throw new SensorFaultException(
                    config.getId(), tick, "simulated transient fault", true);
            case PERSISTENT_FAULT -> throw new SensorFaultException(
                    config.getId(), tick, "simulated persistent fault", false);
            default -> buildReading(tick, s);
        };
    }

    private RawReading buildReading(long tick, SampleScript s) {
        double value = switch (s.kind()) {
            case OUT_OF_RANGE -> produceOutOfRange();
            case NORMAL, BAD_TIMESTAMP, CORRUPT -> s.rawValue();
            default -> s.rawValue();
        };
        long timestampMs = clock.nowMillis() + s.tsDeltaMs();
        long checksum = checksum(tick, timestampMs, value);
        if (s.corrupt() || s.kind() == SampleScript.Kind.CORRUPT) {
            // 翻转最低位，制造“数据损坏但数值本身仍可能落在量程内”的情形
            checksum ^= 1L;
        }
        return new RawReading(config.getId(), tick, timestampMs, value, checksum);
    }

    private double produceOutOfRange() {
        // 确定性地越过较远的一端，避免边界歧义
        double span = config.getRangeMax() - config.getRangeMin();
        return config.getRangeMax() + Math.max(span, 1.0d);
    }

    private double midRange() {
        return (config.getRangeMin() + config.getRangeMax()) / 2.0d;
    }

    /**
     * 对 (tick,timestampMs,value) 的确定性校验和（FNV-1a 风格，仅用于本地演示）。
     */
    public static long checksum(long tick, long timestampMs, double value) {
        long hash = 0xcbf29ce484222325L;
        hash = fnv(hash, tick);
        hash = fnv(hash, timestampMs);
        hash = fnv(hash, Double.doubleToRawLongBits(value));
        return hash;
    }

    private static long fnv(long hash, long bits8) {
        for (int i = 0; i < 8; i++) {
            hash ^= (bits8 >>> (i * 8)) & 0xffL;
            hash *= 0x100000001b3L;
        }
        return hash;
    }
}
