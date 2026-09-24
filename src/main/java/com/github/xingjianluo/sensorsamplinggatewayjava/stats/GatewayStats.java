package com.github.xingjianluo.sensorsamplinggatewayjava.stats;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.RejectReason;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.StatsSnapshot;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/**
 * 线程安全的网关计数器。并发采集/消费下所有计数均不重不漏：
 * 每个原始读数在其唯一归属的处理分支中恰好递增一个结论计数。
 */
public class GatewayStats {

    private final LongAdder produced = new LongAdder();
    private final LongAdder accepted = new LongAdder();
    private final LongAdder retries = new LongAdder();
    private final Map<RejectReason, LongAdder> rejected = new EnumMap<>(RejectReason.class);

    public GatewayStats() {
        for (RejectReason reason : RejectReason.values()) {
            rejected.put(reason, new LongAdder());
        }
    }

    public void incrementProduced() {
        produced.increment();
    }

    public void incrementAccepted() {
        accepted.increment();
    }

    public void incrementRejected(RejectReason reason) {
        rejected.get(reason).increment();
    }

    public void incrementRetries() {
        retries.increment();
    }

    public long produced() {
        return produced.sum();
    }

    public long accepted() {
        return accepted.sum();
    }

    public long retries() {
        return retries.sum();
    }

    public StatsSnapshot snapshot(int isolatedSensors, int buffered) {
        Map<RejectReason, Long> copy = new EnumMap<>(RejectReason.class);
        rejected.forEach((reason, adder) -> {
            long n = adder.sum();
            if (n > 0) {
                copy.put(reason, n);
            }
        });
        return new StatsSnapshot(produced.sum(), accepted.sum(), copy,
                retries.sum(), isolatedSensors, buffered);
    }

    /** 测试辅助：重置全部计数。 */
    public void reset() {
        produced.reset();
        accepted.reset();
        retries.reset();
        rejected.values().forEach(LongAdder::reset);
    }
}
