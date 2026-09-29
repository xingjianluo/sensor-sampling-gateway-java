package com.github.xingjianluo.sensorsamplinggatewayjava;

import com.github.xingjianluo.sensorsamplinggatewayjava.gateway.GatewayConfig;
import com.github.xingjianluo.sensorsamplinggatewayjava.gateway.SamplingGateway;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.OverflowPolicy;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.RejectReason;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SampleScript;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorConfig;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorType;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.ReconcileEntry;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.ReconcileLedger;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.ReconcileQuery;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.ReconcileReport;
import com.github.xingjianluo.sensorsamplinggatewayjava.sensor.Sensor;
import com.github.xingjianluo.sensorsamplinggatewayjava.time.ManualClock;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 对账能力端到端集成测试：并发采样 + 缓冲压力 + 故障隔离/恢复 + 优雅重启 +
 * 运行中规则版本切换下，对账恒等式闭合、同读数只算一次、拒绝原因可区分且可回溯。
 */
class ReconciliationIntegrationTest {

    private static List<SampleScript> clean(int n, double v) {
        return java.util.stream.IntStream.range(0, n)
                .mapToObj(i -> SampleScript.normal(v)).toList();
    }

    private void waitTicks(SamplingGateway gw, String sensor, long until, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            Set<Long> ticks = gw.verdictLog().bySensor(sensor).stream()
                    .map(v -> v.reading().tick())
                    .filter(t -> t <= until)
                    .collect(Collectors.toCollection(HashSet::new));
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
        throw new AssertionError("timeout waiting " + sensor + " ticks 0.." + until);
    }

    /** 等待某运行代次的精确聚合生产总数达到 expected（聚合不受明细窗口影响）。 */
    private void waitForLedgerSize(SamplingGateway gw, long run, long expected, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (gw.reconcileLedger().report(ReconcileQuery.ofRun(run)).produced() >= expected) {
                return;
            }
            Thread.sleep(5);
        }
        throw new AssertionError("timeout waiting run " + run + " produced=" + expected);
    }

    /** 报告内逐条核对：每条读数只出现一次，且明细条数与计数对齐。 */
    private void assertNoDuplicateKeys(ReconcileReport r) {
        Set<String> seen = new HashSet<>();
        for (ReconcileEntry e : r.deliveredDetail()) {
            assertTrue(seen.add("D:" + e.key()), "duplicate delivered " + e.key());
        }
        for (ReconcileEntry e : r.rejectedDetail()) {
            assertTrue(seen.add("R:" + e.key()), "duplicate rejected " + e.key());
        }
    }

    @Test
    void concurrentCleanRun_reconcilesProducedAcceptedDeliveredAndCloses() throws Exception {
        ManualClock clock = new ManualClock(TestFixtures.T0);
        int n = 150;
        SensorConfig c1 = TestFixtures.config("temp", SensorType.TEMPERATURE, -40, 80);
        SensorConfig c2 = TestFixtures.config("hum", SensorType.HUMIDITY, 0, 100);
        Map<String, SensorConfig> cfgs = Map.of("temp", c1, "hum", c2);
        Map<String, List<SampleScript>> scripts = Map.of(
                "temp", clean(n, 25.0), "hum", clean(n, 50.0));
        GatewayConfig cfg = TestFixtures.gatewayConfig(
                "rules-v1", List.of(c1, c2), 256, OverflowPolicy.BLOCK, 4, 1, 3);
        var sensors = TestFixtures.limited(
                TestFixtures.scriptedSensors(clock, cfgs, scripts),
                Map.of("temp", n, "hum", n));
        SamplingGateway gw = new SamplingGateway(cfg, sensors, clock, TestFixtures.fastSleeper());
        gw.start();
        long run = gw.reconcileLedger().currentRunId();
        waitTicks(gw, "temp", n - 1, 10_000);
        waitTicks(gw, "hum", n - 1, 10_000);
        gw.requestStop();
        gw.stop();

        ReconcileReport r = gw.reconcileLedger().report(ReconcileQuery.ofRun(run));
        assertEquals(2L * n, r.produced());
        assertEquals(2L * n, r.accepted());
        assertEquals(2L * n, r.deliveredDetail().size());
        assertEquals(0L, r.dropped());
        assertTrue(r.complete());
        assertNoDuplicateKeys(r);
        // 与既有计数源互相对上
        assertEquals(2L * n, gw.stats().accepted());
    }

    @Test
    void everyInvalidReadingLandsInRejectedDetailWithDistinguishableReason() throws Exception {
        ManualClock clock = new ManualClock(TestFixtures.T0);
        SensorConfig c = TestFixtures.config("s", SensorType.TEMPERATURE, -40, 80);
        // 8 拍：超量程 / 时间戳异常 / 损坏，其余正常
        Map<String, List<SampleScript>> scripts = Map.of("s", List.of(
                SampleScript.normal(20.0),
                new SampleScript(SampleScript.Kind.OUT_OF_RANGE, 0, 0, false),
                new SampleScript(SampleScript.Kind.BAD_TIMESTAMP, 20.0, 5000, false),
                new SampleScript(SampleScript.Kind.CORRUPT, 20.0, 0, false),
                SampleScript.normal(21.0)));
        GatewayConfig cfg = TestFixtures.gatewayConfig(
                "rules-v1", List.of(c), 16, OverflowPolicy.BLOCK, 2, 1, 3);
        var sensors = TestFixtures.limited(
                TestFixtures.scriptedSensors(clock, Map.of("s", c), scripts),
                Map.of("s", 5));
        SamplingGateway gw = new SamplingGateway(cfg, sensors, clock, TestFixtures.fastSleeper());
        gw.start();
        long run = gw.reconcileLedger().currentRunId();
        waitTicks(gw, "s", 4, 10_000);
        gw.requestStop();
        gw.stop();

        ReconcileReport r = gw.reconcileLedger().report(ReconcileQuery.ofRun(run));
        assertEquals(5, r.produced());
        assertEquals(2, r.accepted());
        assertEquals(1L, r.rejectedByReason().get(RejectReason.OUT_OF_RANGE));
        assertEquals(1L, r.rejectedByReason().get(RejectReason.TIMESTAMP_ANOMALY));
        assertEquals(1L, r.rejectedByReason().get(RejectReason.DATA_CORRUPT));
        assertEquals(3, r.rejectedDetail().size());
        // 每一类不合规读数都能在拒绝明细里按 tick 找到对应原因
        Map<RejectReason, Long> ticksByReason = r.rejectedDetail().stream()
                .collect(Collectors.groupingBy(e -> e.verdict().reason(),
                        Collectors.counting()));
        assertEquals(Long.valueOf(1L), ticksByReason.get(RejectReason.OUT_OF_RANGE));
        assertEquals(Long.valueOf(1L), ticksByReason.get(RejectReason.TIMESTAMP_ANOMALY));
        assertEquals(Long.valueOf(1L), ticksByReason.get(RejectReason.DATA_CORRUPT));
        assertTrue(r.complete());
        assertNoDuplicateKeys(r);
    }

    @Test
    void backpressure_evictedOldestAndRejectedNewest_bothReconcileAsOverflow() throws Exception {
        ManualClock clock = new ManualClock(TestFixtures.T0);
        int n = 100;
        SensorConfig c = TestFixtures.config("p", SensorType.TEMPERATURE, -40, 80);
        Map<String, SensorConfig> cfgs = Map.of("p", c);
        Map<String, List<SampleScript>> scripts = Map.of("p", clean(n, 25.0));
        GatewayConfig cfg = TestFixtures.gatewayConfig(
                "rules-v1", List.of(c), 1, OverflowPolicy.DROP_OLDEST, 1, 1, 3, 30);
        var sensors = TestFixtures.limited(
                TestFixtures.scriptedSensors(clock, cfgs, scripts), Map.of("p", n));
        // 生产拉满、容量 1：必然发生大量顶掉
        SamplingGateway gw = new SamplingGateway(cfg, sensors, clock, millis -> { });
        gw.start();
        long run = gw.reconcileLedger().currentRunId();
        waitTicks(gw, "p", n - 1, 10_000);
        gw.requestStop();
        gw.stop();

        ReconcileReport r = gw.reconcileLedger().report(ReconcileQuery.ofRun(run));
        long overflow = r.rejectedByReason().get(RejectReason.BUFFER_OVERFLOW);
        assertTrue(overflow > 0, "expected evictions under capacity-1 pressure");
        assertEquals(overflow, r.dropped());
        assertEquals(n, r.produced(), "every sampling attempt is counted exactly once");
        assertEquals(r.produced(),
                r.accepted() + r.rejectedByReason().values().stream().mapToLong(Long::longValue).sum());
        // 顶掉明细必须带 evicted 标记且原因是 BUFFER_OVERFLOW；窗口内溢出明细 +
        // 已被明细窗口顶掉的溢出缺失数 = 精确溢出总数
        long evictedInWindow = r.rejectedDetail().stream()
                .filter(ReconcileEntry::evicted).count();
        long rejectedOverflowInWindow = r.rejectedDetail().stream()
                .filter(e -> e.verdict().reason() == RejectReason.BUFFER_OVERFLOW).count();
        assertEquals(rejectedOverflowInWindow, evictedInWindow,
                "DROP_OLDEST rejections in window are all evicted-oldest verdicts");
        assertEquals(overflow, rejectedOverflowInWindow + r.rejectedMissing());
        // 小窗口会截断明细：精确计数仍在，缺失条数显式给出且与窗口一致
        assertTrue(r.countersExact());
        assertFalse(r.complete(), "detail window of 30 must truncate a 100-reading run");
        assertTrue(r.rejectedMissing() > 0 || r.deliveredMissing() > 0);
        assertEquals(30, r.retainedDetailCount());
        assertEquals(r.accepted(),
                (long) r.deliveredDetail().size() + r.deliveredMissing());
        assertEquals(overflow,
                (long) r.rejectedDetail().size() + r.rejectedMissing());
    }

    @Test
    void dropNewest_rejectsReconcileWithoutLossOrDoubleCount() throws Exception {
        ManualClock clock = new ManualClock(TestFixtures.T0);
        int n = 100;
        SensorConfig c = TestFixtures.config("q", SensorType.TEMPERATURE, -40, 80);
        Map<String, SensorConfig> cfgs = Map.of("q", c);
        Map<String, List<SampleScript>> scripts = Map.of("q", clean(n, 25.0));
        GatewayConfig cfg = TestFixtures.gatewayConfig(
                "rules-v1", List.of(c), 1, OverflowPolicy.DROP_NEWEST, 1, 1, 3);
        var sensors = TestFixtures.limited(
                TestFixtures.scriptedSensors(clock, cfgs, scripts), Map.of("q", n));
        SamplingGateway gw = new SamplingGateway(cfg, sensors, clock, millis -> { });
        gw.start();
        long run = gw.reconcileLedger().currentRunId();
        waitTicks(gw, "q", n - 1, 10_000);
        gw.requestStop();
        gw.stop();

        ReconcileReport r = gw.reconcileLedger().report(ReconcileQuery.ofRun(run));
        long overflow = r.rejectedByReason().get(RejectReason.BUFFER_OVERFLOW);
        assertTrue(overflow > 0);
        assertEquals(n, r.produced());
        assertEquals(r.accepted() + overflow, r.produced());
        assertTrue(r.rejectedDetail().stream().noneMatch(ReconcileEntry::evicted),
                "DROP_NEWEST rejections must not be marked evicted-oldest");
        assertNoDuplicateKeys(r);
    }

    @Test
    void faultIsolationAndRecovery_reconcileAcquisitionFailureAndIsolationPerSensor() throws Exception {
        ManualClock clock = new ManualClock(TestFixtures.T0);
        int n = 60;
        SensorConfig bad = TestFixtures.config("bad", SensorType.TEMPERATURE, -40, 80);
        SensorConfig good = TestFixtures.config("good", SensorType.HUMIDITY, 0, 100);
        Map<String, SensorConfig> cfgs = Map.of("bad", bad, "good", good);
        Map<String, List<SampleScript>> scripts = Map.of(
                "bad", java.util.stream.IntStream.range(0, n).mapToObj(i ->
                        i < 3 ? SampleScript.normal(20.0)
                                : new SampleScript(SampleScript.Kind.PERSISTENT_FAULT, 0, 0, false))
                        .toList(),
                "good", clean(n, 40.0));
        // 断路器阈值 3：第 3、4、5 拍采集失败使断路器打开，之后拍点全部 SENSOR_ISOLATED
        GatewayConfig cfg = TestFixtures.gatewayConfig(
                "rules-v1", List.of(bad, good), 64, OverflowPolicy.BLOCK, 2, 1, 3,
                4096, n);
        // 用网关级拍数预算确定边界（隔离拍不调用 sample()，预算在更上层生效）
        var sensors = TestFixtures.scriptedSensors(clock, cfgs, scripts);
        SamplingGateway gw = new SamplingGateway(cfg, sensors, clock, TestFixtures.fastSleeper());
        gw.start();
        long run = gw.reconcileLedger().currentRunId();
        waitForLedgerSize(gw, run, 2L * n, 10_000);
        gw.requestStop();
        gw.stop();

        ReconcileLedger ledger = gw.reconcileLedger();
        ReconcileReport badReport = ledger.report(new ReconcileQuery(run, "bad", null, -1, -1));
        ReconcileReport goodReport = ledger.report(new ReconcileQuery(run, "good", null, -1, -1));
        ReconcileReport all = ledger.report(ReconcileQuery.ofRun(run));

        assertEquals(n, badReport.produced(), "bad sensor: one account entry per attempt");
        assertEquals(3, badReport.accepted(), "first three clean readings delivered");
        long acquisition = badReport.rejectedByReason().getOrDefault(RejectReason.ACQUISITION_FAILED, 0L);
        long isolated = badReport.rejectedByReason().getOrDefault(RejectReason.SENSOR_ISOLATED, 0L);
        assertEquals(3, acquisition, "three acquisition failures before isolation");
        assertEquals(n - 6L, isolated, "all attempts after breaker opens are SENSOR_ISOLATED");
        assertEquals(3L + acquisition + isolated, badReport.produced());

        assertEquals(n, goodReport.produced());
        assertEquals(n, goodReport.accepted());
        assertEquals(0L, goodReport.dropped());

        // 全局报告 = 两个传感器相加，且只算一次
        assertEquals(badReport.produced() + goodReport.produced(), all.produced());
        assertEquals(badReport.accepted() + goodReport.accepted(), all.accepted());
        assertTrue(all.complete());
        assertNoDuplicateKeys(all);
    }

    @Test
    void transientFaultRecovery_allAttemptsStillReconcileExactlyOnce() throws Exception {
        ManualClock clock = new ManualClock(TestFixtures.T0);
        SensorConfig c = TestFixtures.config("t", SensorType.TEMPERATURE, -40, 80);
        // 第 1 拍瞬时故障（最多重试 3 次仍失败 -> ACQUISITION_FAILED），其余正常；断路器阈值大不隔离
        Map<String, List<SampleScript>> scripts = Map.of("t", List.of(
                new SampleScript(SampleScript.Kind.TRANSIENT_FAULT, 0, 0, false),
                SampleScript.normal(20.0),
                SampleScript.normal(21.0)));
        GatewayConfig cfg = TestFixtures.gatewayConfig(
                "rules-v1", List.of(c), 16, OverflowPolicy.BLOCK, 2, 3, 99,
                4096, 3);
        var sensors = TestFixtures.scriptedSensors(clock, Map.of("t", c), scripts);
        SamplingGateway gw = new SamplingGateway(cfg, sensors, clock, TestFixtures.fastSleeper());
        gw.start();
        long run = gw.reconcileLedger().currentRunId();
        waitForLedgerSize(gw, run, 3, 10_000);
        gw.requestStop();
        gw.stop();

        ReconcileReport r = gw.reconcileLedger().report(ReconcileQuery.ofRun(run));
        assertEquals(3, r.produced());
        assertEquals(2, r.accepted());
        assertEquals(1L, r.rejectedByReason().get(RejectReason.ACQUISITION_FAILED));
        assertEquals(0L, r.rejectedByReason().getOrDefault(RejectReason.SENSOR_ISOLATED, 0L),
                "recovered transient fault must not isolate the sensor");
        assertNoDuplicateKeys(r);
    }

    @Test
    void acrossRestart_eachRunStaysClosedAndIndependentlyReplayable() throws Exception {
        ManualClock clock = new ManualClock(TestFixtures.T0);
        SensorConfig c = TestFixtures.config("s", SensorType.TEMPERATURE, -40, 80);

        GatewayConfig cfg1 = TestFixtures.gatewayConfig(
                "rules-v1", List.of(c), 16, OverflowPolicy.BLOCK, 2, 1, 3, 4096, 50);
        SamplingGateway gw = new SamplingGateway(cfg1,
                TestFixtures.scriptedSensors(clock, Map.of("s", c),
                        Map.of("s", clean(50, 20.0))),
                clock, TestFixtures.fastSleeper());
        gw.start();
        long run1 = gw.reconcileLedger().currentRunId();
        waitForLedgerSize(gw, run1, 50, 10_000);
        gw.requestStop();
        gw.stop();

        GatewayConfig cfg2 = TestFixtures.gatewayConfig(
                "rules-v2", List.of(c), 16, OverflowPolicy.BLOCK, 2, 1, 3, 4096, 30);
        gw.restart(cfg2, TestFixtures.scriptedSensors(clock, Map.of("s", c),
                Map.of("s", clean(30, 20.0))));
        long run2 = gw.reconcileLedger().currentRunId();
        assertEquals(run1 + 1, run2);
        waitForLedgerSize(gw, run2, 30, 10_000);
        gw.requestStop();
        gw.stop();

        ReconcileLedger ledger = gw.reconcileLedger();
        // 旧运行封口后账目不变、仍可独立回溯
        ReconcileReport r1 = ledger.report(ReconcileQuery.ofRun(run1));
        ReconcileReport r2 = ledger.report(ReconcileQuery.ofRun(run2));
        assertEquals(50, r1.produced());
        assertEquals(50, r1.accepted());
        assertEquals(30, r2.produced());
        assertEquals(30, r2.accepted());
        assertTrue(r1.complete() && r2.complete());
        // 跨全部运行：只算一次，数字相加闭合
        ReconcileReport all = ledger.report(ReconcileQuery.all());
        assertEquals(80, all.produced());
        assertEquals(80, all.accepted());
        // 按规则版本：v1 仅 run1、v2 仅 run2
        assertEquals(50, ledger.report(new ReconcileQuery(null, null, "rules-v1", -1, -1)).produced());
        assertEquals(30, ledger.report(new ReconcileQuery(null, null, "rules-v2", -1, -1)).produced());
        // 每一代 tick 都从 0 开始：同 sensor#tick 跨代允许，但代次+sensor#tick 全局唯一。
        // 明细按入账时间排序（并发消费顺序不定），故按 tick 定位而不是取首条。
        long run1Tick0 = run1;
        long run2Tick0 = run2;
        assertTrue(r1.deliveredDetail().stream()
                .anyMatch(e -> e.key().equals(run1Tick0 + ":s#0")));
        assertTrue(r2.deliveredDetail().stream()
                .anyMatch(e -> e.key().equals(run2Tick0 + ":s#0")));
        Set<String> global = new HashSet<>();
        for (ReconcileEntry e : all.deliveredDetail()) {
            assertTrue(global.add(e.key()));
        }
    }

    @Test
    void rulesVersionSwitchedMidRun_verdictsSplitByVersionAndRunStillCloses() throws Exception {
        ManualClock clock = new ManualClock(TestFixtures.T0);
        SensorConfig c = TestFixtures.config("s", SensorType.TEMPERATURE, -40, 80);
        GatewayConfig v1 = TestFixtures.gatewayConfig(
                "rules-v1", List.of(c), 256, OverflowPolicy.BLOCK, 2, 1, 3,
                4096, 80);
        // 脚本第 30 拍超量程，用于确认切换前后拒绝口径一致
        Map<String, List<SampleScript>> scripts = new HashMap<>();
        scripts.put("s", java.util.stream.IntStream.range(0, 80).mapToObj(i ->
                i == 30 ? new SampleScript(SampleScript.Kind.OUT_OF_RANGE, 0, 0, false)
                        : SampleScript.normal(20.0)).toList());
        // 生产者在第 25 拍采样处被门闩挡住，放行前不可能产生 tick>=25 的裁决，
        // 从而让“切换规则版本”的分界精确落在 24/25 之间。
        java.util.concurrent.CountDownLatch gate = new java.util.concurrent.CountDownLatch(1);
        Sensor delegate = TestFixtures.scriptedSensors(clock, Map.of("s", c), scripts).get("s");
        Sensor gated = new Sensor() {
            @Override
            public SensorConfig config() {
                return c;
            }

            @Override
            public com.github.xingjianluo.sensorsamplinggatewayjava.model.RawReading sample(long tick)
                    throws Exception {
                if (tick >= 25) {
                    gate.await();
                }
                return delegate.sample(tick);
            }
        };
        SamplingGateway gw = new SamplingGateway(v1, Map.of("s", gated),
                clock, TestFixtures.fastSleeper());
        gw.start();
        long run = gw.reconcileLedger().currentRunId();
        waitTicks(gw, "s", 24, 10_000);
        gw.switchRulesVersion(TestFixtures.gatewayConfig(
                "rules-v2", List.of(c), 256, OverflowPolicy.BLOCK, 2, 1, 3,
                4096, 80));
        gate.countDown(); // 放行后 tick25 起全部按 v2 采样与裁决
        waitTicks(gw, "s", 79, 10_000);
        gw.requestStop();
        gw.stop();

        ReconcileLedger ledger = gw.reconcileLedger();
        ReconcileReport underV1 = ledger.report(new ReconcileQuery(run, "s", "rules-v1", -1, -1));
        ReconcileReport underV2 = ledger.report(new ReconcileQuery(run, "s", "rules-v2", -1, -1));
        ReconcileReport whole = ledger.report(ReconcileQuery.ofRun(run));

        assertEquals(80, whole.produced());
        assertEquals(underV1.produced() + underV2.produced(), whole.produced(),
                "mid-run switch partitions the same run without overlap or gap");
        assertEquals(1L, underV2.rejectedByReason().get(RejectReason.OUT_OF_RANGE),
                "tick 30 rejected under v2 with the same validity rule");
        assertEquals(0L, underV1.rejectedByReason().getOrDefault(RejectReason.OUT_OF_RANGE, 0L));
        // v1 段恰好前 25 拍；切换是边界点，每拍只属于一个版本
        assertEquals(25, underV1.produced());
        assertEquals(25, underV1.accepted());
        assertTrue(whole.complete());
        assertNoDuplicateKeys(whole);
    }
}
