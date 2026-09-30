package com.github.xingjianluo.sensorsamplinggatewayjava;

import com.github.xingjianluo.sensorsamplinggatewayjava.buffer.BoundedBuffer;
import com.github.xingjianluo.sensorsamplinggatewayjava.gateway.GatewayConfig;
import com.github.xingjianluo.sensorsamplinggatewayjava.gateway.SamplingGateway;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.CalibrationModel;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.GatewayStats;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.OverflowPolicy;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.RawReading;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SampleScript;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorConfig;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorType;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.ReconciliationEntry;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.ReconciliationFilter;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.ReconciliationReport;
import com.github.xingjianluo.sensorsamplinggatewayjava.reliability.RetryPolicy;
import com.github.xingjianluo.sensorsamplinggatewayjava.sensor.Sensor;
import com.github.xingjianluo.sensorsamplinggatewayjava.time.ManualClock;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 运行中切换规则版本（不重启）：
 * <ul>
 *   <li>同一次运行（同一代次）内含两个规则版本阶段，各自可按 rulesVersion 筛选；</li>
 *   <li>切换前已受理读数按原规则版本完成（包括被顶掉的旧读数保留旧版本标签）；</li>
 *   <li>切换后新读数按新版本判定（v2 收紧量程使同值变为 OUT_OF_RANGE）；</li>
 *   <li>同代总账闭合，身份键唯一，StatsCollector 与台账互证。</li>
 * </ul>
 */
class HotRulesSwitchIntegrationTest {

    private static void assertClosed(ReconciliationReport r) {
        assertEquals(r.total(), r.accepted() + r.rejected() + r.evictedOld() + r.incomplete());
        Set<String> keys = new HashSet<>();
        for (ReconciliationEntry e : r.acceptedEntries()) {
            assertTrue(keys.add(e.generation() + "#" + e.sensorTickKey()));
        }
        for (ReconciliationEntry e : r.rejectedEntries()) {
            assertTrue(keys.add(e.generation() + "#" + e.sensorTickKey()));
        }
        for (ReconciliationEntry e : r.evictedEntries()) {
            assertTrue(keys.add(e.generation() + "#" + e.sensorTickKey()));
        }
        for (ReconciliationEntry e : r.incompleteEntries()) {
            assertTrue(keys.add(e.generation() + "#" + e.sensorTickKey()));
        }
    }

    /** 每拍固定产出值 30.0 的确定脚本（tick 足够多）。 */
    private static List<SampleScript> fixedValueScript(int n, double value) {
        return java.util.stream.IntStream.range(0, n).mapToObj(i -> SampleScript.normal(value)).toList();
    }

    private static GatewayConfig config(String rulesVersion, double max) {
        SensorConfig c = TestFixtures.config("s", SensorType.TEMPERATURE, -40, max);
        return TestFixtures.gatewayConfig(
                rulesVersion, List.of(c), 64, OverflowPolicy.BLOCK, 2, 1, 3);
    }

    /**
     * 在指定拍数处可被外部闸门阻塞的确定传感器：拍号到达 gateTick 后先发出 reached 信号，
     * 再等待 release 放行，用来在生产流中切出一个确定性的“切换边界”；值固定、校验和合法。
     */
    private static final class GateSensor implements Sensor {
        private final SensorConfig cfg;
        private final int gateTick;
        private final int limit;
        private final double value;
        private final CountDownLatch reached;
        private final CountDownLatch release;

        GateSensor(SensorConfig cfg, int gateTick, int limit, double value,
                   CountDownLatch reached, CountDownLatch release) {
            this.cfg = cfg;
            this.gateTick = gateTick;
            this.limit = limit;
            this.value = value;
            this.reached = reached;
            this.release = release;
        }

        @Override
        public SensorConfig config() {
            return cfg;
        }

        @Override
        public RawReading sample(long tick) throws Exception {
            if (tick >= limit) {
                throw new com.github.xingjianluo.sensorsamplinggatewayjava.sensor
                        .SamplingLimitReached(cfg.getId(), tick);
            }
            if (tick == gateTick) {
                reached.countDown();
                release.await();
            }
            long ts = TestFixtures.T0;
            long ck = com.github.xingjianluo.sensorsamplinggatewayjava.sensor.ScriptedSensor
                    .checksum(tick, ts, value);
            return new RawReading(cfg.getId(), tick, ts, value, ck);
        }
    }

    @Test
    void switchRulesVersionAtRuntime_sameGenerationTwoPhases_slicableAndClosed() throws Exception {
        ManualClock clock = new ManualClock(TestFixtures.T0);
        int phase1 = 40;
        int phase2 = 40;
        int totalTicks = phase1 + phase2;
        double value = 30.0;
        SensorConfig c1 = TestFixtures.config("s", SensorType.TEMPERATURE, -40, 80);

        // 第 phase1 拍（切换后的第一拍）前会阻塞，等我们完成切换后放行
        CountDownLatch reachedGate = new CountDownLatch(1);
        CountDownLatch releaseGate = new CountDownLatch(1);
        Sensor gateSensor = new GateSensor(c1, phase1, totalTicks, value, reachedGate, releaseGate);

        GatewayConfig v1 = config("rules-v1", 80);
        SamplingGateway gw = new SamplingGateway(v1, Map.of("s", gateSensor),
                clock, TestFixtures.fastSleeper());
        gw.start();
        long gen = gw.reconciliation().runningGeneration();

        // 等生产者停在第 phase1 拍门口：此时前 phase1 拍已全部产出
        assertTrue(reachedGate.await(10, java.util.concurrent.TimeUnit.SECONDS));
        // 等这 phase1 拍全部按 v1 落账（BLOCK + 消费线程，全部接受）
        waitForCount(gw, gen, "s", phase1, 10_000);

        // 运行中切换到 v2（量程收紧 [-40,25]，30.0 变为 OUT_OF_RANGE）
        SensorConfig c2 = TestFixtures.config("s", SensorType.TEMPERATURE, -40, 25);
        GatewayConfig v2 = new GatewayConfig(
                "rules-v2", List.of(c2),
                CalibrationModel.identity("rules-v2-calib"),
                v1.bufferCapacity(), v1.overflowPolicy(), v1.consumerThreads(),
                new RetryPolicy(1, 1L, 2.0, 50L, 0.0),
                v1.breakerThreshold(), v1.breakerCooldownMs(),
                v1.shutdownDrainTimeoutMs(), v1.verdictLogCapacity(),
                v1.reconcileDetailCapacity(), v1.reconcileMaxSealedGenerations(),
                v1.reconcileRetentionPolicy());
        gw.switchRulesVersion(v2);
        assertEquals("rules-v2", gw.config().rulesVersion());
        assertEquals(gen, gw.reconciliation().runningGeneration(), "hot switch must not open a new generation");

        // 放行切换后的拍
        releaseGate.countDown();
        waitForCount(gw, gen, "s", totalTicks, 10_000);
        gw.requestStop();
        gw.stop();

        // 代次元数据：同代两个版本阶段，按生效顺序
        var info = gw.reconciliation().generation(gen);
        assertTrue(info.sealed());
        assertEquals(List.of("rules-v1", "rules-v2"), info.rulesVersions());
        assertEquals("rules-v2", info.rulesVersion());

        ReconciliationReport whole = gw.reconciliation().reconcile(ReconciliationFilter.forGeneration(gen));
        assertClosed(whole);
        assertTrue(whole.complete());
        assertEquals(totalTicks, whole.total());

        // v1 阶段：phase1 拍全部接受
        ReconciliationReport p1 = gw.reconciliation()
                .reconcile(ReconciliationFilter.forRulesVersion("rules-v1"));
        assertClosed(p1);
        assertEquals(phase1, p1.total());
        assertEquals(phase1, p1.accepted());
        assertEquals(0, p1.rejected());
        assertTrue(p1.acceptedEntries().stream().allMatch(e -> e.rulesVersion().equals("rules-v1")));

        // v2 阶段：phase2 拍全部 OUT_OF_RANGE
        ReconciliationReport p2 = gw.reconciliation()
                .reconcile(ReconciliationFilter.forRulesVersion("rules-v2"));
        assertClosed(p2);
        assertEquals(phase2, p2.total());
        assertEquals(phase2, p2.rejected());
        assertEquals(phase2,
                p2.rejectedByReason().get(com.github.xingjianluo.sensorsamplinggatewayjava.model.RejectReason.OUT_OF_RANGE));
        assertTrue(p2.rejectedEntries().stream().allMatch(e -> e.rulesVersion().equals("rules-v2")));

        // 总账闭合 + 同一条读数只算一次：两阶段之和 == 整代
        assertEquals(whole.total(), p1.total() + p2.total());
        assertEquals(whole.accepted(), p1.accepted() + p2.accepted());
        assertEquals(whole.rejected(), p1.rejected() + p2.rejected());
        // 每传感器小计也闭合
        var stTotals = whole.perSensor().get("s");
        assertEquals(totalTicks, stTotals.total());
        assertEquals(phase1, stTotals.accepted());
        assertEquals(phase2, stTotals.rejected());

        // stats 与台账互证（四分类闭合）
        GatewayStats st = gw.stats();
        assertEquals(st.submitted(),
                whole.accepted() + whole.rejected() + whole.evictedOld() + whole.incomplete());
        assertEquals(st.accepted(), whole.accepted());
        assertEquals(st.rejected(), whole.rejected());
        assertEquals(0L, whole.evictedOld());
        assertEquals(0L, whole.incomplete());
    }

    @Test
    void switchRulesVersion_isRejectedForStructuralChangesAndWhenStopped() {
        ManualClock clock = new ManualClock(TestFixtures.T0);
        SensorConfig c = TestFixtures.config("s", SensorType.TEMPERATURE, -40, 80);
        GatewayConfig v1 = TestFixtures.gatewayConfig(
                "rules-v1", List.of(c), 8, OverflowPolicy.BLOCK, 1, 1, 3);
        var sensors = TestFixtures.limited(
                TestFixtures.scriptedSensors(clock, Map.of("s", c),
                        Map.of("s", List.of(SampleScript.normal(20.0)))), Map.of("s", 4));
        SamplingGateway gw = new SamplingGateway(v1, sensors, clock, TestFixtures.fastSleeper());

        // 未运行不能切换
        assertThrows(IllegalStateException.class, () -> gw.switchRulesVersion("rules-v2"));

        gw.start();
        // 改缓冲容量属于结构变化，热切换拒绝（需要 restart）
        GatewayConfig structural = TestFixtures.gatewayConfig(
                "rules-v2", List.of(c), 16, OverflowPolicy.BLOCK, 1, 1, 3);
        assertThrows(IllegalArgumentException.class, () -> gw.switchRulesVersion(structural));
        gw.requestStop();
        gw.stop();
    }

    /** 等待某传感器在当前代的落账总数达到 expected（四类终结都算）。 */
    private void waitForCount(SamplingGateway gw, long gen, String sensor,
                              long expected, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        ReconciliationFilter f = new ReconciliationFilter(gen, sensor, null, 0L, 0L);
        while (System.currentTimeMillis() < deadline) {
            if (gw.reconciliation().reconcile(f).total() >= expected) {
                return;
            }
            Thread.sleep(5);
        }
        throw new AssertionError("timeout waiting count " + expected);
    }

    /** 等待 ticks 0..untilTick 全部在台账中出现（任意终结方式）。 */
    private void waitForAllTicks(SamplingGateway gw, String sensor, long untilTick, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            long gen = gw.reconciliation().runningGeneration();
            ReconciliationReport r = gw.reconciliation()
                    .reconcile(ReconciliationFilter.forGeneration(gen));
            Set<Long> ticks = new HashSet<>();
            r.acceptedEntries().stream().filter(e -> e.sensorId().equals(sensor))
                    .map(ReconciliationEntry::tick).forEach(ticks::add);
            r.rejectedEntries().stream().filter(e -> e.sensorId().equals(sensor))
                    .map(ReconciliationEntry::tick).forEach(ticks::add);
            r.evictedEntries().stream().filter(e -> e.sensorId().equals(sensor))
                    .map(ReconciliationEntry::tick).forEach(ticks::add);
            r.incompleteEntries().stream().filter(e -> e.sensorId().equals(sensor))
                    .map(ReconciliationEntry::tick).forEach(ticks::add);
            boolean ok = true;
            for (long t = 0; t <= untilTick; t++) {
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
        throw new AssertionError("timeout waiting all ticks 0.." + untilTick);
    }
}
