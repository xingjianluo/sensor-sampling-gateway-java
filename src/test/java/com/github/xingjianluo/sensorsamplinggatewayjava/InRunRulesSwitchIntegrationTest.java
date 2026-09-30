package com.github.xingjianluo.sensorsamplinggatewayjava;

import com.github.xingjianluo.sensorsamplinggatewayjava.gateway.GatewayConfig;
import com.github.xingjianluo.sensorsamplinggatewayjava.gateway.SamplingGateway;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.CalibrationModel;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.OverflowPolicy;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.RawReading;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorConfig;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorType;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.ReconciliationEntry;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.ReconciliationFilter;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.ReconciliationReport;
import com.github.xingjianluo.sensorsamplinggatewayjava.reliability.RetryPolicy;
import com.github.xingjianluo.sensorsamplinggatewayjava.sensor.SamplingLimitReached;
import com.github.xingjianluo.sensorsamplinggatewayjava.sensor.Sensor;
import com.github.xingjianluo.sensorsamplinggatewayjava.time.ManualClock;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 运行中切换规则版本（不停机、不开新批次）：
 * <ul>
 *   <li>切换前已受理读数按旧版本判定、切换后新读数按新版本判定，两类各自可筛选；</li>
 *   <li>同一次运行只有一个代次，整代总账闭合、同一读数只算一次；</li>
 *   <li>并发 + 缓冲压力 + 故障/隔离与切换叠加时仍不重不漏。</li>
 * </ul>
 */
class InRunRulesSwitchIntegrationTest {

    private static GatewayConfig cfg(String version, SensorConfig c, int buffer,
                                     OverflowPolicy policy, int consumers, long drainMs) {
        RetryPolicy retry = new RetryPolicy(1, 1L, 2.0, 50L, 0.0);
        return new GatewayConfig(version, List.of(c),
                CalibrationModel.identity(version + "-calib"),
                buffer, policy, consumers, retry, 3, 1_000_000L,
                drainMs, 4096, 4096, 8,
                com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.RetentionPolicy.ROLL_OLDEST);
    }

    private static void assertClosed(ReconciliationReport r) {
        assertEquals(r.total(), r.accepted() + r.rejected() + r.evictedOld());
        long byReason = r.rejectedByReason().values().stream().mapToLong(Long::longValue).sum();
        assertEquals(r.rejected(), byReason);
        Set<String> keys = new HashSet<>();
        for (ReconciliationEntry e : r.acceptedEntries()) {
            assertTrue(keys.add(e.generation() + "#" + e.sensorTickKey()));
        }
        for (ReconciliationEntry e : r.rejectedEntries()) {
            assertTrue(keys.add(e.generation() + "#" + e.sensorTickKey()), "single counted");
        }
    }

    /** 可在指定 tick 边界切换规则、可制造压力/故障的脚本传感器。 */
    private static final class SwitchableSensor implements Sensor {
        private final SensorConfig cfg;
        private final int total;
        private final long switchAfterTickExclusive;
        private final boolean faultyAfterSwitch;

        SwitchableSensor(SensorConfig cfg, int total, long switchAfterTickExclusive,
                         boolean faultyAfterSwitch) {
            this.cfg = cfg;
            this.total = total;
            this.switchAfterTickExclusive = switchAfterTickExclusive;
            this.faultyAfterSwitch = faultyAfterSwitch;
        }

        @Override
        public SensorConfig config() {
            return cfg;
        }

        @Override
        public RawReading sample(long tick) throws Exception {
            if (tick >= total) {
                throw new SamplingLimitReached(cfg.getId(), tick);
            }
            if (faultyAfterSwitch && tick >= switchAfterTickExclusive) {
                throw new com.github.xingjianluo.sensorsamplinggatewayjava.sensor.SensorFaultException(
                        cfg.getId(), tick, "post-switch persistent fault", false);
            }
            // v1 量程 [-40,80] 下 20.0 接受；切换到 v2（量程收窄为 [0,10]）后超量程
            double value = 20.0;
            long ck = com.github.xingjianluo.sensorsamplinggatewayjava.sensor.ScriptedSensor
                    .checksum(tick, TestFixtures.T0, value);
            return new RawReading(cfg.getId(), tick, TestFixtures.T0, value, ck);
        }
    }

    private void waitForTotal(SamplingGateway gw, long gen, long expected, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        ReconciliationFilter f = ReconciliationFilter.forGeneration(gen);
        while (System.currentTimeMillis() < deadline) {
            if (gw.reconciliation().reconcile(f).total() >= expected) {
                return;
            }
            Thread.sleep(5);
        }
        throw new AssertionError("timeout waiting generation total " + expected
                + ", got " + gw.reconciliation().reconcile(f).total());
    }

    @Test
    void inRunSwitch_oldAdmissionsUseOldRules_newAdmissionsUseNewRules_sameGenerationClosed()
            throws Exception {
        ManualClock clock = new ManualClock(TestFixtures.T0);
        // 容量 1 + DROP_OLDEST：切换时缓冲里必然有“已受理未消费”的旧读数
        SensorConfig c1 = TestFixtures.config("s", SensorType.TEMPERATURE, -40, 80);
        int n = 80;
        long switchAt = 40; // tick >= 40 为切换后新读数
        GatewayConfig v1 = cfg("rules-v1", c1, 1, OverflowPolicy.DROP_OLDEST, 2, 5000L);
        Sensor sensor = new SwitchableSensor(c1, n, switchAt, false);
        SamplingGateway gw = new SamplingGateway(v1, Map.of("s", sensor), clock, millis -> { });
        gw.start();
        long gen = gw.reconciliation().runningGeneration();

        // 等到缓冲压力下已有读数落账（必然含驱逐），再在运行中切换
        waitForMin(gw, gen, 10);
        SensorConfig c2 = new SensorConfig("s", SensorType.TEMPERATURE, 1L, 0, 10, 1000L, true);
        GatewayConfig v2 = cfg("rules-v2", c2, 1, OverflowPolicy.DROP_OLDEST, 2, 5000L);
        gw.switchRules(v2);
        assertTrue(gw.isRunning());
        assertEquals(gen, gw.reconciliation().runningGeneration(),
                "in-run switch must NOT open a new generation");

        waitForTotal(gw, gen, n, 10_000);
        gw.requestStop();
        gw.stop();

        // 同一代次内出现两个规则版本，起始版本为 v1
        var info = gw.reconciliation().generation(gen);
        assertEquals(List.of("rules-v1", "rules-v2"), info.rulesVersions());
        assertEquals("rules-v1", info.rulesVersion());
        assertTrue(info.sealed());

        ReconciliationReport all = gw.reconciliation().reconcile(ReconciliationFilter.forGeneration(gen));
        assertClosed(all);
        assertTrue(all.complete());
        assertEquals(n, all.total(), "one entry per tick, no loss/dup under pressure + switch");

        ReconciliationReport r1 = gw.reconciliation().reconcile(
                new ReconciliationFilter(gen, null, "rules-v1", 0L, 0L));
        ReconciliationReport r2 = gw.reconciliation().reconcile(
                new ReconciliationFilter(gen, null, "rules-v2", 0L, 0L));
        assertClosed(r1);
        assertClosed(r2);
        assertEquals(all.total(), r1.total() + r2.total(),
                "every entry belongs to exactly one rules version");

        // 切换前已受理（含在切换瞬间仍在缓冲中的）读数：v1 量程 [-40,80]，20.0 全部接受，
        // v1 侧的非接受只有 DROP_OLDEST 顶掉
        assertTrue(r1.total() > 0);
        assertEquals(0L, r1.rejectedByReason().getOrDefault(
                com.github.xingjianluo.sensorsamplinggatewayjava.model.RejectReason.OUT_OF_RANGE, 0L),
                "pre-switch admitted readings judged by old wide range, none OUT_OF_RANGE");
        // 切换后新读数按 v2 窄量程判定 -> 必然出现 OUT_OF_RANGE
        assertTrue(r2.rejectedByReason().getOrDefault(
                com.github.xingjianluo.sensorsamplinggatewayjava.model.RejectReason.OUT_OF_RANGE, 0L) > 0,
                "post-switch readings judged by new narrow range -> OUT_OF_RANGE");
        // 两版本各自明细上的版本标记正确
        assertTrue(r1.acceptedEntries().stream().allMatch(e -> e.rulesVersion().equals("rules-v1")));
        assertTrue(r1.rejectedEntries().stream().allMatch(e -> e.rulesVersion().equals("rules-v1")));
        assertTrue(r2.rejectedEntries().stream().allMatch(e -> e.rulesVersion().equals("rules-v2")));
        // 压力下发生了顶掉；被顶掉读数单列，不抬高老 rejected/BUFFER_OVERFLOW 计数
        assertTrue(all.evictedOld() > 0, "evictions occurred under capacity-1 pressure");
        assertEquals(0L, all.rejectedByReason().getOrDefault(
                com.github.xingjianluo.sensorsamplinggatewayjava.model.RejectReason.BUFFER_OVERFLOW, 0L),
                "evicted-old never counted in legacy BUFFER_OVERFLOW");
    }

    @Test
    void inRunSwitch_rejectsIncompatibleStructuralChange() {
        ManualClock clock = new ManualClock(TestFixtures.T0);
        SensorConfig c1 = TestFixtures.config("s", SensorType.TEMPERATURE, -40, 80);
        GatewayConfig v1 = cfg("rules-v1", c1, 16, OverflowPolicy.BLOCK, 2, 5000L);
        SamplingGateway gw = new SamplingGateway(v1,
                Map.of("s", new SwitchableSensor(c1, 5, 99, false)), clock, TestFixtures.fastSleeper());
        gw.start();
        try {
            // 相同版本号不允许
            assertThrows(IllegalArgumentException.class, () -> gw.switchRules(v1));
            // 缓冲容量变化不允许
            SensorConfig c2 = TestFixtures.config("s", SensorType.TEMPERATURE, 0, 10);
            assertThrows(IllegalArgumentException.class,
                    () -> gw.switchRules(cfg("rules-v2", c2, 8, OverflowPolicy.BLOCK, 2, 5000L)));
            // 溢出策略变化不允许
            assertThrows(IllegalArgumentException.class,
                    () -> gw.switchRules(cfg("rules-v2", c2, 16, OverflowPolicy.DROP_OLDEST, 2, 5000L)));
            // 传感器集合变化不允许
            SensorConfig extra = TestFixtures.config("s2", SensorType.TEMPERATURE, 0, 10);
            assertThrows(IllegalArgumentException.class,
                    () -> gw.switchRules(cfg("rules-v2", extra, 16, OverflowPolicy.BLOCK, 2, 5000L)));
        } finally {
            gw.requestStop();
            gw.stop();
        }
    }

    @Test
    void inRunSwitch_underConcurrencyBackpressureAndFaults_stillBalancedAndSingleCounted()
            throws Exception {
        ManualClock clock = new ManualClock(TestFixtures.T0);
        SensorConfig good = TestFixtures.config("good", SensorType.HUMIDITY, 0, 100);
        SensorConfig bad = TestFixtures.config("bad", SensorType.TEMPERATURE, -40, 80);
        int n = 120;
        long switchAt = 60;
        GatewayConfig v1 = new GatewayConfig("rules-v1", List.of(good, bad),
                CalibrationModel.identity("rules-v1-calib"), 1, OverflowPolicy.DROP_OLDEST, 1,
                new RetryPolicy(1, 1L, 2.0, 50L, 0.0), 3, 1_000_000L, 5000L, 4096,
                4096, 8, com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.RetentionPolicy.ROLL_OLDEST);
        Map<String, Sensor> sensors = new LinkedHashMap<>();
        sensors.put("good", new SwitchableSensor(good, n, switchAt, false));
        // bad 在切换后持续故障 -> 切换与 ACQUISITION_FAILED/SENSOR_ISOLATED 叠加
        sensors.put("bad", new SwitchableSensor(bad, n, switchAt, true));
        SamplingGateway gw = new SamplingGateway(v1, sensors, clock, millis -> { });
        gw.start();
        long gen = gw.reconciliation().runningGeneration();
        waitForMin(gw, gen, 20);

        SensorConfig good2 = new SensorConfig("good", SensorType.HUMIDITY, 1L, 0, 10, 1000L, true);
        SensorConfig bad2 = new SensorConfig("bad", SensorType.TEMPERATURE, 1L, 0, 10, 1000L, true);
        GatewayConfig v2 = new GatewayConfig("rules-v2", List.of(good2, bad2),
                CalibrationModel.identity("rules-v2-calib"), 1, OverflowPolicy.DROP_OLDEST, 1,
                new RetryPolicy(1, 1L, 2.0, 50L, 0.0), 3, 1_000_000L, 5000L, 4096,
                4096, 8, com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.RetentionPolicy.ROLL_OLDEST);
        gw.switchRules(v2);

        // good 在 tick=n 协作式停止；bad 隔离后不再调用 sample()、会持续产出隔离结论，
        // 因此等到 good 的 n 条全部落账 + bad 至少 n 条，再确定性请求停止。
        waitForSensorTotal(gw, gen, "good", n, 15_000);
        waitForTotal(gw, gen, 2L * n, 15_000);
        gw.requestStop();
        gw.stop();

        ReconciliationReport all = gw.reconciliation().reconcile(ReconciliationFilter.forGeneration(gen));
        assertClosed(all);
        var s = gw.stats();
        assertEquals(s.submitted(), all.total(), "ledger closes against production total");
        assertEquals(s.submitted(), s.accepted() + s.rejected() + s.evictedOld());
        assertEquals(n, all.perSensor().get("good").total(),
                "good produced exactly n ticks then stopped cooperatively");
        assertTrue(all.perSensor().get("bad").total() >= n, "bad accounted through faults/isolation");
        // 同一条读数只算一次：完整明细下身份键（代次#sensor#tick）集合大小 == 总数
        Set<String> keys = new HashSet<>();
        all.acceptedEntries().forEach(e -> assertTrue(
                keys.add(e.generation() + "#" + e.sensorTickKey())));
        all.rejectedEntries().forEach(e -> assertTrue(
                keys.add(e.generation() + "#" + e.sensorTickKey())));
        assertEquals(all.total(), keys.size(), "no duplicate or missing reading under switch+pressure+fault");
        assertTrue(all.evictedOld() > 0, "capacity-1 pressure evicts oldest");
        // 故障/隔离在切换后仍每条恰一次落账
        assertTrue(all.rejectedByReason().getOrDefault(
                com.github.xingjianluo.sensorsamplinggatewayjava.model.RejectReason.ACQUISITION_FAILED, 0L) > 0);
        // 新版本窄量程：good 在切换后超量程
        ReconciliationReport r2 = gw.reconciliation().reconcile(
                new ReconciliationFilter(gen, null, "rules-v2", 0L, 0L));
        assertClosed(r2);
        assertTrue(r2.rejectedByReason().getOrDefault(
                com.github.xingjianluo.sensorsamplinggatewayjava.model.RejectReason.OUT_OF_RANGE, 0L) > 0);
        // v1+v2 不重不漏
        ReconciliationReport r1 = gw.reconciliation().reconcile(
                new ReconciliationFilter(gen, null, "rules-v1", 0L, 0L));
        assertEquals(all.total(), r1.total() + r2.total());
    }

    private void waitForSensorTotal(SamplingGateway gw, long gen, String sensor,
                                    long expected, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        ReconciliationFilter f = new ReconciliationFilter(gen, sensor, null, 0L, 0L);
        while (System.currentTimeMillis() < deadline) {
            if (gw.reconciliation().reconcile(f).total() >= expected) {
                return;
            }
            Thread.sleep(5);
        }
        throw new AssertionError("timeout waiting sensor " + sensor + " total " + expected);
    }

    private void waitForMin(SamplingGateway gw, long gen, long minTotal) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        ReconciliationFilter f = ReconciliationFilter.forGeneration(gen);
        while (System.currentTimeMillis() < deadline) {
            if (gw.reconciliation().reconcile(f).total() >= minTotal) {
                return;
            }
            Thread.sleep(2);
        }
        throw new AssertionError("timeout waiting min total " + minTotal);
    }
}
