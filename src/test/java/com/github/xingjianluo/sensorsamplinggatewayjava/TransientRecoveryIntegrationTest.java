package com.github.xingjianluo.sensorsamplinggatewayjava;

import com.github.xingjianluo.sensorsamplinggatewayjava.gateway.GatewayConfig;
import com.github.xingjianluo.sensorsamplinggatewayjava.gateway.SamplingGateway;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.GatewayStats;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.OverflowPolicy;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.RawReading;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.RejectReason;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorConfig;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorType;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.Verdict;
import com.github.xingjianluo.sensorsamplinggatewayjava.reliability.CircuitBreaker;
import com.github.xingjianluo.sensorsamplinggatewayjava.sensor.SamplingLimitReached;
import com.github.xingjianluo.sensorsamplinggatewayjava.sensor.Sensor;
import com.github.xingjianluo.sensorsamplinggatewayjava.sensor.SensorFaultException;
import com.github.xingjianluo.sensorsamplinggatewayjava.sensor.ScriptedSensor;
import com.github.xingjianluo.sensorsamplinggatewayjava.time.ManualClock;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 端到端瞬时故障恢复：某几拍前两次尝试抛瞬时故障、第三次成功。
 * 经过有界重试（退避在测试中零等待）后该拍被正常受理，断路器保持 CLOSED，
 * 不产生 ACQUISITION_FAILED，也不隔离传感器。
 */
class TransientRecoveryIntegrationTest {

    private static final Logger LOG = Logger.getLogger(TransientRecoveryIntegrationTest.class.getName());

    @Test
    void transientFaultsRecoverWithinRetryBudgetAndDoNotIsolate() throws Exception {
        ManualClock clock = new ManualClock(TestFixtures.T0);
        SensorConfig cfg = TestFixtures.config("flaky", SensorType.TEMPERATURE, -40, 80);
        AtomicInteger ticksDone = new AtomicInteger();

        // 用 20 拍全为 33.0 的脚本，避免越界回退到量程中值
        Sensor healthy = new ScriptedSensor(cfg,
                java.util.stream.IntStream.range(0, 20)
                        .mapToObj(i -> com.github.xingjianluo.sensorsamplinggatewayjava.model.SampleScript.normal(33.0))
                        .toList(),
                clock);

        Sensor flaky = new Sensor() {
            @Override
            public SensorConfig config() {
                return cfg;
            }

            @Override
            public RawReading sample(long tick) throws Exception {
                // tick 2 和 tick 5：前两次瞬时故障，第三次成功；其余拍正常
                if (tick == 2L || tick == 5L) {
                    int attempt = attemptCounter(tick).incrementAndGet();
                    if (attempt < 3) {
                        throw new SensorFaultException("flaky", tick, "transient IO glitch", true);
                    }
                }
                if (ticksDone.incrementAndGet() > 20) {
                    throw new SamplingLimitReached("flaky", tick);
                }
                return healthy.sample(tick);
            }

            private final Map<Long, AtomicInteger> attempts = new java.util.concurrent.ConcurrentHashMap<>();

            private AtomicInteger attemptCounter(long tick) {
                return attempts.computeIfAbsent(tick, k -> new AtomicInteger());
            }
        };

        GatewayConfig gwCfg = TestFixtures.gatewayConfig(
                "rules-v1", List.of(cfg), 32, OverflowPolicy.BLOCK, 2,
                4, // 最多尝试4次，足够在第3次恢复
                5);
        SamplingGateway gw = new SamplingGateway(gwCfg, Map.of("flaky", flaky),
                clock, TestFixtures.fastSleeper());
        gw.start();
        waitTicks(gw, 19);
        gw.requestStop();
        gw.stop();

        GatewayStats s = gw.stats();
        assertEquals(0L, s.rejectedByReason().getOrDefault(RejectReason.ACQUISITION_FAILED, 0L),
                "retried transient faults must not become acquisition failures");
        assertEquals(0L, s.rejectedByReason().getOrDefault(RejectReason.SENSOR_ISOLATED, 0L));
        assertEquals(CircuitBreaker.State.CLOSED, gw.breaker("flaky").state(TestFixtures.T0),
                "breaker stays closed after successful retries");
        List<Verdict> verdicts = gw.verdictLog().bySensor("flaky");
        assertEquals(20, verdicts.size());
        assertTrue(verdicts.stream().allMatch(Verdict::accepted), "all 20 ticks eventually accepted");
        // tick 2 与 5 的恢复值正确
        assertEquals(33.0, verdicts.stream().filter(v -> v.reading().tick() == 2)
                .findFirst().orElseThrow().calibratedValue(), 1e-9);
        LOG.info(() -> "transient recovery: acquisitionFailure attempts=" + s.acquisitionFailures()
                + " (retried, recovered), accepted=" + s.accepted()
                + ", breaker=" + gw.breaker("flaky").state(TestFixtures.T0));
    }

    private void waitTicks(SamplingGateway gw, long until) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            java.util.Set<Long> ticks = new java.util.HashSet<>();
            gw.verdictLog().bySensor("flaky").forEach(v -> ticks.add(v.reading().tick()));
            boolean ok = true;
            for (long t = 0; t <= until; t++) {
                if (!ticks.contains(t)) {
                    ok = false;
                    break;
                }
            }
            if (ok) {
                return;
            }
            Thread.sleep(5);
        }
        throw new AssertionError("timeout");
    }
}
