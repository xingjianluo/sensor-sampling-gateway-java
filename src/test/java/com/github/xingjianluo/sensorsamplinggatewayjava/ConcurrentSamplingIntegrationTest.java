package com.github.xingjianluo.sensorsamplinggatewayjava;

import com.github.xingjianluo.sensorsamplinggatewayjava.gateway.GatewayConfig;
import com.github.xingjianluo.sensorsamplinggatewayjava.gateway.SamplingGateway;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.GatewayStats;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.OverflowPolicy;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.RejectReason;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SampleScript;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorConfig;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorType;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.Verdict;
import com.github.xingjianluo.sensorsamplinggatewayjava.sensor.Sensor;
import com.github.xingjianluo.sensorsamplinggatewayjava.time.ManualClock;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发采集与消费集成测试：验证计数不重不漏、缓冲溢出确定可观测、故障隔离互不影响、
 * 同输入同规则版本结论可复现。日志打印每个传感器的判定分布与依据样例。
 */
class ConcurrentSamplingIntegrationTest {

    private static final Logger LOG = Logger.getLogger(ConcurrentSamplingIntegrationTest.class.getName());

    /** 一次可复现运行的结论签名与统计快照。 */
    private record Run(List<String> signature, GatewayStats stats) {
    }

    private static List<SampleScript> cleanScript(int n, double value) {
        return java.util.stream.IntStream.range(0, n)
                .mapToObj(i -> SampleScript.normal(value))
                .toList();
    }

    private void assertNoDuplicateKeys(List<Verdict> verdicts) {
        Set<String> seen = new HashSet<>();
        for (Verdict v : verdicts) {
            String key = v.reading().key();
            assertTrue(seen.add(key), "duplicate verdict key: " + key);
        }
    }

    @Test
    void concurrentSampling_noLossNoDuplicate_underNormalLoad() throws Exception {
        ManualClock clock = new ManualClock(TestFixtures.T0);
        int n = 200;
        SensorConfig c1 = TestFixtures.config("temp", SensorType.TEMPERATURE, -40, 80);
        SensorConfig c2 = TestFixtures.config("hum", SensorType.HUMIDITY, 0, 100);
        Map<String, SensorConfig> cfgs = Map.of("temp", c1, "hum", c2);
        Map<String, List<SampleScript>> scripts = Map.of(
                "temp", cleanScript(n, 25.0),
                "hum", cleanScript(n, 50.0));
        GatewayConfig cfg = TestFixtures.gatewayConfig(
                "rules-v1", List.of(c1, c2), 256, OverflowPolicy.BLOCK, 4, 1, 3);
        var bounded = TestFixtures.limited(
                TestFixtures.scriptedSensors(clock, cfgs, scripts),
                Map.of("temp", n, "hum", n));
        SamplingGateway gw = new SamplingGateway(cfg, bounded,
                clock, TestFixtures.fastSleeper());
        gw.start();
        // 等待全部 tick 处理完毕（脚本越界后会继续产中值，故按 verdict 中最大 tick 判定）
        waitForTicks(gw, "temp", n - 1, 10_000);
        waitForTicks(gw, "hum", n - 1, 10_000);
        gw.requestStop();
        gw.stop();

        GatewayStats s = gw.stats();
        List<Verdict> verdicts = gw.verdictLog().recent();
        assertNoDuplicateKeys(verdicts);
        assertEquals(0, s.rejected(), "no rejection under normal load");
        assertTrue(s.accepted() >= 2L * n, "all clean readings accepted");
        logSummary("NORMAL", gw);
    }

    @Test
    void dropOldest_underPressure_evictionsAreCountedAndReasonsDistinguishable() throws Exception {
        ManualClock clock = new ManualClock(TestFixtures.T0);
        int n = 100;
        SensorConfig c1 = TestFixtures.config("fast-temp", SensorType.TEMPERATURE, -40, 80);
        SensorConfig c2 = TestFixtures.config("fast-hum", SensorType.HUMIDITY, 0, 100);
        Map<String, SensorConfig> cfgs = Map.of("fast-temp", c1, "fast-hum", c2);
        // 极小容量 + 慢速消费者：消费侧仍在跑但生产远快于消费，必然发生驱逐
        Map<String, List<SampleScript>> scripts = Map.of(
                "fast-temp", cleanScript(n, 25.0),
                "fast-hum", cleanScript(n, 50.0));
        GatewayConfig cfg = TestFixtures.gatewayConfig(
                "rules-v1", List.of(c1, c2), 1, OverflowPolicy.DROP_OLDEST, 1, 1, 3);

        var bounded = TestFixtures.limited(
                TestFixtures.scriptedSensors(clock, cfgs, scripts),
                Map.of("fast-temp", n, "fast-hum", n));
        // 不睡眠：生产达到最大速率，消费者来不及处理，必然发生驱逐
        SamplingGateway gw = new SamplingGateway(cfg, bounded,
                clock, millis -> { });
        gw.start();
        waitForTicks(gw, "fast-temp", n - 1, 10_000);
        gw.requestStop();
        gw.stop();

        GatewayStats s = gw.stats();
        assertNoDuplicateKeys(gw.verdictLog().recent());
        // 容量为1 + DROP_OLDEST + 高速生产者：应观测到驱逐
        assertTrue(s.dropped() > 0, "expected DROP_OLDEST evictions under pressure");
        assertTrue(s.evictedOld() > 0, "evicted-old backpressure split out");
        long overflowCount = s.rejectedByReason().getOrDefault(RejectReason.BUFFER_OVERFLOW, 0L);
        // 历史口径：纯 DROP_OLDEST 路径下没有“直接丢弃最新读数”，老计数 BUFFER_OVERFLOW 为 0
        assertEquals(0L, overflowCount, "evicted oldest never inflate legacy rejected/BUFFER_OVERFLOW");
        // 历史口径不变量：被顶掉读数单列（evictedOld），不重复计入 rejected
        assertEquals(s.submitted(), s.accepted() + s.rejected() + s.evictedOld(),
                "legacy identity: submitted = accepted + rejected + evictedOld");
        assertEquals(s.enqueued(), s.consumed() + s.evictedOld(),
                "legacy identity: enqueued = consumed + evictedOld");
        assertEquals(s.submitted(), s.enqueued() + s.rejected(),
                "legacy identity: submitted = enqueued + rejected");
        assertEquals(s.dropped(), s.evictedOld() + s.rejectedNew(),
                "dropped = evictedOld + rejectedNew (two backpressure kinds)");
        logSummary("DROP_OLDEST", gw);
    }

    @Test
    void dropNewest_underPressure_rejectsAreCountedWithoutDuplicateOrLossOfAccounting() throws Exception {
        ManualClock clock = new ManualClock(TestFixtures.T0);
        int n = 100;
        SensorConfig c1 = TestFixtures.config("n-temp", SensorType.TEMPERATURE, -40, 80);
        Map<String, SensorConfig> cfgs = Map.of("n-temp", c1);
        Map<String, List<SampleScript>> scripts = Map.of("n-temp", cleanScript(n, 25.0));
        GatewayConfig cfg = TestFixtures.gatewayConfig(
                "rules-v1", List.of(c1), 1, OverflowPolicy.DROP_NEWEST, 1, 1, 3);
        var bounded = TestFixtures.limited(
                TestFixtures.scriptedSensors(clock, cfgs, scripts),
                Map.of("n-temp", n));
        SamplingGateway gw = new SamplingGateway(cfg, bounded,
                clock, millis -> { });
        gw.start();
        waitForTicks(gw, "n-temp", n - 1, 10_000);
        gw.requestStop();
        gw.stop();

        GatewayStats s = gw.stats();
        assertNoDuplicateKeys(gw.verdictLog().recent());
        long overflow = s.rejectedByReason().getOrDefault(RejectReason.BUFFER_OVERFLOW, 0L);
        assertTrue(overflow > 0, "capacity-1 DROP_NEWEST must reject newest under pressure");
        assertEquals(s.submitted(), s.enqueued() + s.rejected());
        assertEquals(s.dropped(), overflow, "DROP_NEWEST drops are all recorded as BUFFER_OVERFLOW");
        logSummary("DROP_NEWEST", gw);
    }

    @Test
    void persistentFailure_isolatesSensor_withoutAffectingOthers() throws Exception {
        ManualClock clock = new ManualClock(TestFixtures.T0);
        int n = 60;
        SensorConfig bad = TestFixtures.config("bad", SensorType.TEMPERATURE, -40, 80);
        SensorConfig good = TestFixtures.config("good", SensorType.HUMIDITY, 0, 100);
        Map<String, SensorConfig> cfgs = Map.of("bad", bad, "good", good);
        // bad: 前3拍正常，之后全部持续故障；good: 全程正常
        Map<String, List<SampleScript>> scripts = Map.of(
                "bad", java.util.stream.IntStream.range(0, n).mapToObj(i ->
                        i < 3 ? SampleScript.normal(20.0)
                                : new SampleScript(SampleScript.Kind.PERSISTENT_FAULT, 0, 0, false)).toList(),
                "good", cleanScript(n, 40.0));
        // 断路器阈值3：3 次持续失败（每次内部1次尝试即失败）后隔离
        GatewayConfig cfg = TestFixtures.gatewayConfig(
                "rules-v1", List.of(bad, good), 64, OverflowPolicy.BLOCK, 2, 1, 3);
        // 硬上限固定拍数：good 恰好 n 次正常采样；bad 恰好 n 次（3 正常 + 57 持续故障）
        var bounded = TestFixtures.limited(
                TestFixtures.scriptedSensors(clock, cfgs, scripts),
                Map.of("good", n, "bad", n));
        SamplingGateway gw = new SamplingGateway(cfg, bounded,
                clock, TestFixtures.fastSleeper());
        gw.start();
        waitForTicks(gw, "good", n - 1, 10_000);
        waitForTicks(gw, "bad", n - 1, 10_000);
        gw.requestStop();
        gw.stop();

        GatewayStats s = gw.stats();
        assertTrue(s.isolatedSensors().contains("bad"), "bad sensor must be isolated");
        assertFalse(s.isolatedSensors().contains("good"), "good sensor must not be isolated");

        List<Verdict> goodVerdicts = gw.verdictLog().bySensor("good");
        assertTrue(goodVerdicts.stream().allMatch(Verdict::accepted),
                "good sensor keeps sampling after the other is isolated");
        assertEquals(n, goodVerdicts.size(), "good sensor produced exactly n clean readings");

        long acquireFailed = s.rejectedByReason().getOrDefault(RejectReason.ACQUISITION_FAILED, 0L);
        long isolatedRejects = s.rejectedByReason().getOrDefault(RejectReason.SENSOR_ISOLATED, 0L);
        assertTrue(acquireFailed >= 3, "expect bounded acquisition failures before isolation");
        assertTrue(isolatedRejects > 0, "expect rejected readings after isolation");
        logSummary("ISOLATION", gw);
        LOG.info("bad sensor failure basis sample: " +
                gw.verdictLog().bySensor("bad").stream()
                        .filter(v -> v.reason() == RejectReason.ACQUISITION_FAILED)
                        .map(Verdict::basis).findFirst().orElse("<none>"));
    }

    @Test
    void sameInputsAndRulesVersion_produceReproducibleVerdicts() throws Exception {
        Run first = runReproScenario("rules-v1", 100);
        Run second = runReproScenario("rules-v1", 100);
        assertEquals(first.signature(), second.signature(),
                "same inputs + same rules version must reproduce identical verdict sequence");
        // 校准模型版本随规则版本变化时，值结论相应改变，但拒绝结论不变
        Run third = runReproScenario("rules-v1", 100);
        assertEquals(first.stats().accepted(), third.stats().accepted());
    }

    private Run runReproScenario(String rulesVersion, int n) throws Exception {
        ManualClock clock = new ManualClock(TestFixtures.T0);
        SensorConfig c = TestFixtures.config("rep", SensorType.TEMPERATURE, -40, 80);
        Map<String, SensorConfig> cfgs = Map.of("rep", c);
        Map<String, List<SampleScript>> scripts = Map.of("rep", java.util.stream.IntStream.range(0, n)
                .mapToObj(i -> switch (i % 4) {
                    case 0 -> SampleScript.normal(20.0 + (i % 5));
                    case 1 -> new SampleScript(SampleScript.Kind.OUT_OF_RANGE, 0, 0, false);
                    case 2 -> new SampleScript(SampleScript.Kind.CORRUPT, 20.0, 0, false);
                    default -> new SampleScript(SampleScript.Kind.BAD_TIMESTAMP, 20.0, 5000, false);
                }).toList());
        GatewayConfig cfg = TestFixtures.gatewayConfig(
                rulesVersion, List.of(c), 64, OverflowPolicy.BLOCK, 2, 1, 3);
        var limitedSensors = TestFixtures.limited(
                TestFixtures.scriptedSensors(clock, cfgs, scripts),
                Map.of("rep", n));
        SamplingGateway gw = new SamplingGateway(cfg, limitedSensors,
                clock, TestFixtures.fastSleeper());
        gw.start();
        waitForTicks(gw, "rep", n - 1, 10_000);
        gw.requestStop();
        gw.stop();
        // 每个 tick 恰好一条结论，按 tick 排序生成确定性签名
        List<String> signature = gw.verdictLog().bySensor("rep").stream()
                .filter(v -> v.reading().tick() < n)
                .sorted(java.util.Comparator.comparingLong(v -> v.reading().tick()))
                .map(v -> v.reading().tick() + ":" + (v.accepted()
                        ? String.format("A=%.4f", v.calibratedValue())
                        : "R=" + v.reason().name()))
                .toList();
        assertEquals(n, signature.size(), "exactly n verdicts for n scripted ticks");
        return new Run(signature, gw.stats());
    }

    /** 等待传感器出现连续的 tick 0..untilTick 全部裁决（不重不漏的完整性判定）。 */
    private void waitForTicks(SamplingGateway gw, String sensor, long untilTick, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            java.util.Set<Long> ticks = gw.verdictLog().bySensor(sensor).stream()
                    .map(v -> v.reading().tick())
                    .filter(t -> t <= untilTick)
                    .collect(Collectors.toCollection(java.util.HashSet::new));
            boolean complete = true;
            for (long t = 0; t <= untilTick; t++) {
                if (!ticks.contains(t)) {
                    complete = false;
                    break;
                }
            }
            if (complete) {
                return;
            }
            Thread.sleep(5);
        }
        throw new AssertionError("timeout waiting for sensor " + sensor + " ticks 0.." + untilTick);
    }

    private void logSummary(String scenario, SamplingGateway gw) {
        GatewayStats s = gw.stats();
        LOG.info(() -> "[" + scenario + "] submitted=" + s.submitted()
                + " enqueued=" + s.enqueued() + " consumed=" + s.consumed()
                + " accepted=" + s.accepted() + " rejected=" + s.rejected()
                + " dropped=" + s.dropped() + " acquisitionFailures=" + s.acquisitionFailures()
                + " byReason=" + s.rejectedByReason()
                + " isolated=" + s.isolatedSensors());
    }
}
