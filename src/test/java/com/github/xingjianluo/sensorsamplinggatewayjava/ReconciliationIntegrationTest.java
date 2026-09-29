package com.github.xingjianluo.sensorsamplinggatewayjava;

import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.ReconciliationEntry;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.ReconciliationFilter;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.ReconciliationReport;

import com.github.xingjianluo.sensorsamplinggatewayjava.gateway.GatewayConfig;
import com.github.xingjianluo.sensorsamplinggatewayjava.gateway.SamplingGateway;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.GatewayStats;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.OverflowPolicy;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.RejectReason;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SampleScript;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorConfig;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorType;
import com.github.xingjianluo.sensorsamplinggatewayjava.sensor.Sensor;
import com.github.xingjianluo.sensorsamplinggatewayjava.time.ManualClock;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 网关级采样对账集成测试：
 * <ul>
 *   <li>并发采样下，各种不合规读数都落进拒绝明细且原因可区分，账目与 StatsCollector 互证；</li>
 *   <li>DROP_OLDEST 被顶掉 / DROP_NEWEST 被丢弃分列且账目闭合；</li>
 *   <li>持续故障隔离后仍能对平；规则版本切换（重启）按代次隔离、各自闭合；</li>
 *   <li>同输入同规则版本反复运行，对账结论完全一致。</li>
 * </ul>
 */
class ReconciliationIntegrationTest {

    private static List<SampleScript> mixedScript(int n) {
        return IntStream.range(0, n).mapToObj(i -> switch (i % 5) {
            case 0 -> SampleScript.normal(20.0 + (i % 3));
            case 1 -> new SampleScript(SampleScript.Kind.OUT_OF_RANGE, 0, 0, false);
            case 2 -> new SampleScript(SampleScript.Kind.CORRUPT, 20.0, 0, false);
            case 3 -> new SampleScript(SampleScript.Kind.BAD_TIMESTAMP, 20.0, 5000, false);
            default -> SampleScript.normal(25.0);
        }).toList();
    }

    private static void assertReportClosed(ReconciliationReport r) {
        assertEquals(r.total(), r.accepted() + r.rejected());
        long byReason = r.rejectedByReason().values().stream().mapToLong(Long::longValue).sum();
        assertEquals(r.rejected(), byReason);
        long overflow = r.rejectedByReason().getOrDefault(RejectReason.BUFFER_OVERFLOW, 0L);
        assertEquals(r.evictedOld() + r.rejectedNew(), overflow);
        Set<String> keys = new HashSet<>();
        for (ReconciliationEntry e : r.acceptedEntries()) {
            assertTrue(e.accepted());
            assertTrue(keys.add(e.generation() + "#" + e.sensorTickKey()));
        }
        for (ReconciliationEntry e : r.rejectedEntries()) {
            assertFalse(e.accepted());
            assertTrue(e.reason() != null, "rejected entry must carry a distinguishable reason");
            assertTrue(keys.add(e.generation() + "#" + e.sensorTickKey()), "no double counting");
        }
    }

    /** 与 StatsCollector 互证：台账聚合必须等于网关统一计数（在明细未滚动时含明细数）。 */
    private static void assertMatchesStats(ReconciliationReport r, GatewayStats s) {
        assertEquals(s.accepted(), r.accepted(), "accepted matches stats");
        assertEquals(s.rejected(), r.rejected(), "rejected matches stats");
        assertEquals(s.submitted(), r.total(), "every submitted tick has exactly one ledger entry");
        assertEquals(s.dropped(), r.evictedOld() + r.rejectedNew(), "dropped matches backpressure");
        for (RejectReason reason : RejectReason.values()) {
            assertEquals(s.rejectedByReason().getOrDefault(reason, 0L),
                    r.rejectedByReason().getOrDefault(reason, 0L),
                    "byReason " + reason + " matches stats");
        }
    }

    private void waitForTicks(SamplingGateway gw, String sensor, long untilTick, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            long gen = gw.reconciliation().runningGeneration();
            Set<Long> ticks = gw.reconciliation()
                    .reconcile(ReconciliationFilter.forGeneration(gen)).rejectedEntries().stream()
                    .filter(e -> e.sensorId().equals(sensor))
                    .map(ReconciliationEntry::tick)
                    .collect(Collectors.toCollection(HashSet::new));
            gw.reconciliation()
                    .reconcile(ReconciliationFilter.forGeneration(gen)).acceptedEntries().stream()
                    .filter(e -> e.sensorId().equals(sensor))
                    .map(ReconciliationEntry::tick)
                    .forEach(ticks::add);
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

    @Test
    void concurrentMixedReadings_everyInvalidGoesToRejectDetails_andBalancesWithStats() throws Exception {
        ManualClock clock = new ManualClock(TestFixtures.T0);
        int n = 100;
        SensorConfig c1 = TestFixtures.config("temp", SensorType.TEMPERATURE, -40, 80);
        SensorConfig c2 = TestFixtures.config("hum", SensorType.HUMIDITY, 0, 100);
        Map<String, SensorConfig> cfgs = Map.of("temp", c1, "hum", c2);
        Map<String, List<SampleScript>> scripts = Map.of(
                "temp", mixedScript(n), "hum", mixedScript(n));
        GatewayConfig cfg = TestFixtures.gatewayConfig(
                "rules-v1", List.of(c1, c2), 64, OverflowPolicy.BLOCK, 4, 1, 3);
        var sensors = TestFixtures.limited(
                TestFixtures.scriptedSensors(clock, cfgs, scripts),
                Map.of("temp", n, "hum", n));
        SamplingGateway gw = new SamplingGateway(cfg, sensors, clock, TestFixtures.fastSleeper());
        gw.start();
        long gen = gw.reconciliation().runningGeneration();
        waitForTicks(gw, "temp", n - 1, 10_000);
        waitForTicks(gw, "hum", n - 1, 10_000);
        gw.requestStop();
        gw.stop();

        ReconciliationReport r = gw.reconciliation().reconcile(ReconciliationFilter.forGeneration(gen));
        assertReportClosed(r);
        assertTrue(r.complete());
        assertEquals(2L * n, r.total(), "two sensors x n ticks, exactly one entry each");
        assertMatchesStats(r, gw.stats());

        // 三类有效性拒绝都必须出现在拒绝明细中且可区分
        for (RejectReason reason : List.of(RejectReason.DATA_CORRUPT,
                RejectReason.TIMESTAMP_ANOMALY, RejectReason.OUT_OF_RANGE)) {
            long count = r.rejectedByReason().get(reason);
            assertTrue(count > 0, "expected " + reason + " rejections");
            boolean present = r.rejectedEntries().stream()
                    .anyMatch(e -> e.reason() == reason);
            assertTrue(present, reason + " must appear in reject details");
        }
        // 接受明细即“真正交付出去”的读数，全部 accepted 且有校准值
        assertTrue(r.acceptedEntries().stream().allMatch(ReconciliationEntry::accepted));
        // VerdictLog 与台账逐条一致（身份键集合相同）
        Set<String> ledgerKeys = new HashSet<>();
        r.acceptedEntries().forEach(e -> ledgerKeys.add(e.sensorTickKey()));
        r.rejectedEntries().forEach(e -> ledgerKeys.add(e.sensorTickKey()));
        Set<String> verdictKeys = gw.verdictLog().recent().stream()
                .map(v -> v.reading().key()).collect(Collectors.toSet());
        assertEquals(ledgerKeys, verdictKeys, "ledger and verdict log cover the same readings");
    }

    @Test
    void dropOldestAndDropNewest_backpressureKindsSplitAndBalance() throws Exception {
        // DROP_OLDEST：被顶掉
        ManualClock clock = new ManualClock(TestFixtures.T0);
        int n = 120;
        SensorConfig c = TestFixtures.config("fast", SensorType.TEMPERATURE, -40, 80);
        Map<String, SensorConfig> cfgs = Map.of("fast", c);
        Map<String, List<SampleScript>> scripts = Map.of(
                "fast", IntStream.range(0, n).mapToObj(i -> SampleScript.normal(20.0)).toList());
        GatewayConfig cfg = TestFixtures.gatewayConfig(
                "rules-v1", List.of(c), 1, OverflowPolicy.DROP_OLDEST, 1, 1, 3);
        var sensors = TestFixtures.limited(
                TestFixtures.scriptedSensors(clock, cfgs, scripts), Map.of("fast", n));
        SamplingGateway gw = new SamplingGateway(cfg, sensors, clock, millis -> { });
        gw.start();
        long gen = gw.reconciliation().runningGeneration();
        waitForTicks(gw, "fast", n - 1, 10_000);
        gw.requestStop();
        gw.stop();

        ReconciliationReport r = gw.reconciliation().reconcile(ReconciliationFilter.forGeneration(gen));
        assertReportClosed(r);
        assertTrue(r.evictedOld() > 0, "DROP_OLDEST must evict oldest under pressure");
        assertEquals(0, r.rejectedNew(), "no newest rejection on pure DROP_OLDEST path");
        assertMatchesStats(r, gw.stats());

        // DROP_NEWEST：被丢弃
        ManualClock clock2 = new ManualClock(TestFixtures.T0);
        GatewayConfig cfg2 = TestFixtures.gatewayConfig(
                "rules-v1", List.of(c), 1, OverflowPolicy.DROP_NEWEST, 1, 1, 3);
        var sensors2 = TestFixtures.limited(
                TestFixtures.scriptedSensors(clock2, cfgs, scripts), Map.of("fast", n));
        SamplingGateway gw2 = new SamplingGateway(cfg2, sensors2, clock2, millis -> { });
        gw2.start();
        long gen2 = gw2.reconciliation().runningGeneration();
        waitForTicks(gw2, "fast", n - 1, 10_000);
        gw2.requestStop();
        gw2.stop();

        ReconciliationReport r2 = gw2.reconciliation().reconcile(ReconciliationFilter.forGeneration(gen2));
        assertReportClosed(r2);
        assertTrue(r2.rejectedNew() > 0, "DROP_NEWEST must reject newest under pressure");
        assertEquals(0, r2.evictedOld());
        assertMatchesStats(r2, gw2.stats());
    }

    @Test
    void persistentFaultIsolation_reconciliationStillBalanced() throws Exception {
        ManualClock clock = new ManualClock(TestFixtures.T0);
        int n = 60;
        SensorConfig bad = TestFixtures.config("bad", SensorType.TEMPERATURE, -40, 80);
        SensorConfig good = TestFixtures.config("good", SensorType.HUMIDITY, 0, 100);
        Map<String, SensorConfig> cfgs = Map.of("bad", bad, "good", good);
        Map<String, List<SampleScript>> scripts = Map.of(
                "bad", IntStream.range(0, n).mapToObj(i -> i < 3
                        ? SampleScript.normal(20.0)
                        : new SampleScript(SampleScript.Kind.PERSISTENT_FAULT, 0, 0, false)).toList(),
                "good", IntStream.range(0, n).mapToObj(i -> SampleScript.normal(40.0)).toList());
        // 冷却 = 2 拍；睡眠器每次节拍把手动时钟推进 1 拍，确定性驱动 OPEN->HALF_OPEN 探测
        GatewayConfig cfg = TestFixtures.gatewayConfig(
                "rules-v1", List.of(bad, good), 64, OverflowPolicy.BLOCK, 2, 1, 3, 2L);
        var sensors = TestFixtures.limited(
                TestFixtures.scriptedSensors(clock, cfgs, scripts), Map.of("good", n, "bad", n));
        SamplingGateway gw = new SamplingGateway(cfg, sensors, clock, millis -> clock.advance(1L));
        gw.start();
        long gen = gw.reconciliation().runningGeneration();
        waitForTicks(gw, "good", n - 1, 10_000);
        // bad：tick 0..2 正常，3..5 持续失败至断路器 OPEN，其后为隔离/冷却探测，
        // 探测到达 tick=n 时由硬上限协作式停止。等待其落账数稳定（生产者已退出）。
        long badVerdicts = waitForSensorDone(gw, gen, "bad", 10_000);
        gw.requestStop();
        gw.stop();

        ReconciliationReport r = gw.reconciliation().reconcile(ReconciliationFilter.forGeneration(gen));
        assertReportClosed(r);
        assertEquals(n + badVerdicts, r.total());
        assertMatchesStats(r, gw.stats());
        assertTrue(r.rejectedByReason().getOrDefault(RejectReason.ACQUISITION_FAILED, 0L) >= 3);
        assertTrue(r.rejectedByReason().getOrDefault(RejectReason.SENSOR_ISOLATED, 0L) > 0,
                "isolated readings must be rejected and counted");
        // 每传感器小计也闭合，且 good 全部接受
        assertEquals(n, r.perSensor().get("good").accepted());
        assertEquals(0, r.perSensor().get("good").rejected());
        var badTotals = r.perSensor().get("bad");
        assertEquals(badVerdicts, badTotals.total());
        assertEquals(3, badTotals.accepted(), "first three ticks accepted");
        assertEquals(badTotals.accepted() + badTotals.rejected(), badTotals.total());
    }

    /** 等待某传感器的生产者协作式退出：其落账数连续多次不再增长，返回最终落账数。 */
    private long waitForSensorDone(SamplingGateway gw, long gen, String sensor, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        long last = -1;
        int stableRounds = 0;
        while (System.currentTimeMillis() < deadline) {
            ReconciliationReport r = gw.reconciliation()
                    .reconcile(new ReconciliationFilter(gen, sensor, null, 0L, 0L));
            long count = r.total();
            if (count == last) {
                if (++stableRounds >= 40) {
                    return count;
                }
            } else {
                stableRounds = 0;
                last = count;
            }
            Thread.sleep(5);
        }
        throw new AssertionError("timeout waiting sensor " + sensor + " producer to finish");
    }

    @Test
    void restartWithNewRulesVersion_generationsAreIsolatedAndEachBalanced() throws Exception {
        ManualClock clock = new ManualClock(TestFixtures.T0);
        int n1 = 40, n2 = 24;
        SensorConfig c = TestFixtures.config("s", SensorType.TEMPERATURE, -40, 80);

        GatewayConfig cfg1 = TestFixtures.gatewayConfig(
                "rules-v1", List.of(c), 16, OverflowPolicy.BLOCK, 2, 1, 3);
        var sensors1 = TestFixtures.limited(
                TestFixtures.scriptedSensors(clock, Map.of("s", c),
                        Map.of("s", mixedScript(n1))), Map.of("s", n1));
        SamplingGateway gw = new SamplingGateway(cfg1, sensors1, clock, TestFixtures.fastSleeper());
        gw.start();
        long g1 = gw.reconciliation().runningGeneration();
        waitForTicks(gw, "s", n1 - 1, 10_000);
        gw.requestStop();
        gw.stop();

        ReconciliationReport r1 = gw.reconciliation().reconcile(ReconciliationFilter.forGeneration(g1));
        assertReportClosed(r1);
        assertTrue(r1.complete());
        assertEquals(n1, r1.total());
        assertTrue(r1.acceptedEntries().stream().allMatch(e -> e.rulesVersion().equals("rules-v1")));
        assertTrue(gw.reconciliation().generation(g1).sealed());

        GatewayConfig cfg2 = TestFixtures.gatewayConfig(
                "rules-v2", List.of(c), 16, OverflowPolicy.BLOCK, 2, 1, 3);
        var sensors2 = TestFixtures.limited(
                TestFixtures.scriptedSensors(clock, Map.of("s", c),
                        Map.of("s", mixedScript(n2))), Map.of("s", n2));
        gw.restart(cfg2, sensors2);
        long g2 = gw.reconciliation().runningGeneration();
        assertTrue(g2 > g1, "each restart opens a new monotonic generation");
        waitForTicks(gw, "s", n2 - 1, 10_000);
        gw.requestStop();
        gw.stop();

        ReconciliationReport r2 = gw.reconciliation().reconcile(ReconciliationFilter.forGeneration(g2));
        assertReportClosed(r2);
        assertEquals(n2, r2.total());
        assertTrue(r2.acceptedEntries().stream().allMatch(e -> e.rulesVersion().equals("rules-v2")));

        // 两代都可回溯，tick 都从 0 起，但 (generation,sensor,tick) 身份不冲突
        assertNotNull_(gw.reconciliation().findEntry(g1, "s#0"));
        assertNotNull_(gw.reconciliation().findEntry(g2, "s#0"));
        ReconciliationReport across = gw.reconciliation().reconcile(ReconciliationFilter.forSensor("s"));
        assertReportClosed(across);
        assertEquals(n1 + n2, across.total(), "across generations still single-counted per reading");
        // 按规则版本切片
        assertEquals(n1, gw.reconciliation()
                .reconcile(ReconciliationFilter.forRulesVersion("rules-v1")).total());
        assertEquals(n2, gw.reconciliation()
                .reconcile(ReconciliationFilter.forRulesVersion("rules-v2")).total());
    }

    @Test
    void sameInputsAndRulesVersion_reconciliationIsReproducible() throws Exception {
        List<Long> sig1 = runSignature("rules-v1");
        List<Long> sig2 = runSignature("rules-v1");
        assertEquals(sig1, sig2, "same inputs + rules version -> identical reconciliation outcomes");
    }

    /** 等待某传感器在指定代次内累计落账达到 expectedTotal（按精确聚合，不受明细保留影响）。 */
    private void waitForCount(SamplingGateway gw, long gen, String sensor,
                              long expectedTotal, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        ReconciliationFilter f = new ReconciliationFilter(gen, sensor, null, 0L, 0L);
        while (System.currentTimeMillis() < deadline) {
            ReconciliationReport r = gw.reconciliation().reconcile(f);
            if (r.total() >= expectedTotal) {
                return;
            }
            Thread.sleep(5);
        }
        throw new AssertionError("timeout waiting for " + sensor + " count " + expectedTotal);
    }

    @Test
    void reconcileDetailRetention_boundaryIsConfigurableAggregatesStayExactAndIssuedReportFrozen()
            throws Exception {
        // 极小明细容量 + ROLL_OLDEST：压力下明细滚动，但聚合精确、报告标注不完整
        ManualClock clock = new ManualClock(TestFixtures.T0);
        int n = 120;
        SensorConfig c = TestFixtures.config("p", SensorType.TEMPERATURE, -40, 80);
        var cfg = new com.github.xingjianluo.sensorsamplinggatewayjava.gateway.GatewayConfig(
                "rules-v1", List.of(c),
                com.github.xingjianluo.sensorsamplinggatewayjava.model.CalibrationModel
                        .identity("rules-v1-calib"),
                8, OverflowPolicy.BLOCK, 2,
                new com.github.xingjianluo.sensorsamplinggatewayjava.reliability.RetryPolicy(
                        1, 1L, 2.0, 50L, 0.0),
                3, 1_000_000L, 5000L, 4096,
                20, 4, com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.RetentionPolicy.ROLL_OLDEST);
        Map<String, SensorConfig> cfgs = Map.of("p", c);
        Map<String, List<SampleScript>> scripts = Map.of(
                "p", IntStream.range(0, n).mapToObj(i -> SampleScript.normal(20.0)).toList());
        var sensors = TestFixtures.limited(
                TestFixtures.scriptedSensors(clock, cfgs, scripts), Map.of("p", n));
        SamplingGateway gw = new SamplingGateway(cfg, sensors, clock, TestFixtures.fastSleeper());
        gw.start();
        long gen = gw.reconciliation().runningGeneration();
        waitForCount(gw, gen, "p", n, 10_000);
        // 运行中先出一份报告（不可变快照）
        ReconciliationReport mid = gw.reconciliation().reconcile(ReconciliationFilter.forGeneration(gen));
        assertReportClosed(mid);
        long midTotal = mid.total();
        int midDetails = mid.acceptedEntries().size();
        gw.requestStop();
        gw.stop();

        ReconciliationReport r = gw.reconciliation().reconcile(ReconciliationFilter.forGeneration(gen));
        assertReportClosed(r);
        assertEquals(n, r.total(), "aggregate total exact despite rollover");
        assertEquals(n, r.accepted(), "aggregate accepted exact");
        assertFalse(r.complete(), "details beyond capacity -> report explicitly incomplete");
        assertEquals(20, r.acceptedEntries().size(), "only capacity-many details retained");
        assertEquals(n - 20L, r.detailRollover());
        var storeStats = gw.reconciliation().storeStats();
        assertEquals(20, storeStats.detailCapacityPerGeneration());
        assertEquals(n - 20L, storeStats.detailsRollover());
        // 运行中已出具的报告不被随后的滚动修改
        assertEquals(midTotal, mid.total(), "issued report frozen");
        assertEquals(midDetails, mid.acceptedEntries().size(), "issued report details frozen");
    }

    private List<Long> runSignature(String rulesVersion) throws Exception {
        ManualClock clock = new ManualClock(TestFixtures.T0);
        int n = 80;
        SensorConfig c = TestFixtures.config("rep", SensorType.TEMPERATURE, -40, 80);
        Map<String, SensorConfig> cfgs = Map.of("rep", c);
        Map<String, List<SampleScript>> scripts = Map.of("rep", mixedScript(n));
        GatewayConfig cfg = TestFixtures.gatewayConfig(
                rulesVersion, List.of(c), 64, OverflowPolicy.BLOCK, 2, 1, 3);
        var sensors = TestFixtures.limited(
                TestFixtures.scriptedSensors(clock, cfgs, scripts), Map.of("rep", n));
        SamplingGateway gw = new SamplingGateway(cfg, sensors, clock, TestFixtures.fastSleeper());
        gw.start();
        long gen = gw.reconciliation().runningGeneration();
        waitForTicks(gw, "rep", n - 1, 10_000);
        gw.requestStop();
        gw.stop();
        ReconciliationReport r = gw.reconciliation().reconcile(ReconciliationFilter.forGeneration(gen));
        assertReportClosed(r);
        List<Long> sig = new ArrayList<>();
        sig.add(r.total());
        sig.add(r.accepted());
        sig.add(r.rejected());
        for (RejectReason reason : RejectReason.values()) {
            sig.add(r.rejectedByReason().get(reason));
        }
        return sig;
    }

    private static void assertNotNull_(Object o) {
        assertTrue(o != null);
    }
}
