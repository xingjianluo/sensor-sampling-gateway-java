package com.github.xingjianluo.sensorsamplinggatewayjava.sensor;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.RawReading;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 可脚本化的确定性传感器：按预置步骤序列依次产生读数或故障，
 * 全部行为来自内存数据，无硬件/网络依赖，保证测试可复现。
 * 步骤耗尽后，最后一个 READ 步骤会被重复，使持续压力测试可无限进行；
 * 若最后一步是故障，则持续抛该故障。
 */
public class ScriptedSensor implements SensorDriver {

    /**
     * 单个脚本步骤。
     *
     * @param kind            步骤类型：READ / FAULT_TRANSIENT / FAULT_PERMANENT
     * @param value           READ 时的原始值
     * @param timestampMs     READ 时的绝对时间戳；为 null 时使用 read(nowMs) 的 nowMs
     * @param corruptChecksum READ 时是否故意破坏校验和
     */
    public record Step(Kind kind, Double value, Long timestampMs, boolean corruptChecksum) {
        public enum Kind {READ, FAULT_TRANSIENT, FAULT_PERMANENT}

        public static Step read(double value, long timestampMs) {
            return new Step(Kind.READ, value, timestampMs, false);
        }

        public static Step read(double value, long timestampMs, boolean corruptChecksum) {
            return new Step(Kind.READ, value, timestampMs, corruptChecksum);
        }

        public static Step readAtNow(double value) {
            return new Step(Kind.READ, value, null, false);
        }

        public static Step transientFault() {
            return new Step(Kind.FAULT_TRANSIENT, null, null, false);
        }

        public static Step permanentFault() {
            return new Step(Kind.FAULT_PERMANENT, null, null, false);
        }
    }

    private final String sensorId;
    private final List<Step> steps;
    private final AtomicLong nextIndex = new AtomicLong();
    private volatile boolean healthy = true;

    public ScriptedSensor(String sensorId, List<Step> steps) {
        if (steps == null || steps.isEmpty()) {
            throw new IllegalArgumentException("scripted sensor requires at least one step: " + sensorId);
        }
        this.sensorId = sensorId;
        this.steps = new ArrayList<>(steps);
    }

    @Override
    public String sensorId() {
        return sensorId;
    }

    @Override
    public RawReading read(long nowMs) throws SensorFaultException {
        long i = nextIndex.getAndIncrement();
        Step step;
        if (i < steps.size()) {
            step = steps.get((int) i);
        } else {
            // 步骤耗尽：循环使用整个脚本，使长期压力测试行为确定且可无限运行。
            step = steps.get((int) (i % steps.size()));
        }
        return switch (step.kind()) {
            case READ -> {
                long seq = i + 1;
                long ts = step.timestampMs() == null ? nowMs : step.timestampMs();
                long expected = RawReading.expectedChecksum(sensorId, seq, step.value(), ts);
                long checksum = step.corruptChecksum() ? expected ^ 0x5a5a5a5aL : expected;
                yield new RawReading(sensorId, seq, step.value(), ts, checksum);
            }
            case FAULT_TRANSIENT -> {
                healthy = false;
                throw new SensorFaultException(sensorId, "simulated transient fault #" + (i + 1), true);
            }
            case FAULT_PERMANENT -> {
                healthy = false;
                throw new SensorFaultException(sensorId, "simulated permanent fault #" + (i + 1), false);
            }
        };
    }

    @Override
    public boolean isHealthy() {
        return healthy;
    }
}
