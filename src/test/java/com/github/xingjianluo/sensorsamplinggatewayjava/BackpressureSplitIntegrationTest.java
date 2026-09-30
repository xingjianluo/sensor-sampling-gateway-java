package com.github.xingjianluo.sensorsamplinggatewayjava;

import com.github.xingjianluo.sensorsamplinggatewayjava.gateway.GatewayConfig;
import com.github.xingjianluo.sensorsamplinggatewayjava.gateway.SamplingGateway;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.GatewayStats;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.OverflowPolicy;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.RawReading;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.RejectReason;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SampleScript;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorConfig;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorType;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.Disposition;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.ReconciliationEntry;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.ReconciliationFilter;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.ReconciliationReport;
import com.github.xingjianluo.sensorsamplinggatewayjava.sensor.Sensor;
import com.github.xingjianluo.sensorsamplinggatewayjava.time.ManualClock;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 同一运行中“顶掉最旧（EVICTED_OLD）”与“直接丢弃/拒绝最新（REJECTED_NEW）”并存：
 * 断言两类背压在台账与统计中单独分列、老口径（rejected 不含驱逐）不被改动、账目闭合、
 * 同一条读数只算一次。
 *
 * <p>确定性手法：容量 1 + DROP_OLDEST；slow 传感器先入队占住唯一位置，fast 传感器随后
 * 高速提交制造大量驱逐；停止时先关闭缓冲（slow 那条仍滞留），再放行 slow 的下一拍，
 * 其 offer 在“已关闭的满 DROP_OLDEST 缓冲”上被直接拒绝（REJECTED_NEW）。</p>
 */
class BackpressureSplitIntegrationTest {

    /** 第 1 拍产出后，在第 2 拍（tick==1）offer 之前阻塞，直到外部放行。 */
    private static final class SlowGateSensor implements Sensor {
        private final SensorConfig cfg;
        private final int limit;
        private final double value;
        private final CountDownLatch reached;
        private final CountDownLatch release;

        SlowGateSensor(SensorConfig cfg, int limit, double value,
                       CountDownLatch reached, CountDownLatch release) {
            this.cfg = cfg;
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
            if (tick == 1) {
                reached.countDown();
                release.await();
            }
            long ck = com.github.xingjianluo.sensorsamplinggatewayjava.sensor.ScriptedSensor
                    .checksum(tick, TestFixtures.T0, value);
            return new RawReading(cfg.getId(), tick, TestFixtures.T0, value, ck);
        }
    }

    private static void assertClosed(ReconciliationReport r) {
        assertEquals(r.total(), r.accepted() + r.rejected() + r.evictedOld() + r.incomplete());
        long byReason = r.rejectedByReason().values().stream().mapToLong(Long::longValue).sum();
        assertEquals(r.rejected(), byReason);
        assertEquals(r.rejectedNew(),
                r.rejectedByReason().getOrDefault(RejectReason.BUFFER_OVERFLOW, 0L));
        Set<String> keys = new HashSet<>();
        List<ReconciliationEntry> all = new ArrayList<>();
        all.addAll(r.acceptedEntries());
        all.addAll(r.rejectedEntries());
        all.addAll(r.evictedEntries());
        all.addAll(r.incompleteEntries());
        for (ReconciliationEntry e : all) {
            assertTrue(keys.add(e.generation() + "#" + e.sensorTickKey()),
                    "reading counted exactly once: " + e.sensorTickKey());
        }
        assertEquals(r.total(), all.size());
    }

    @Test
    void evictedOldAndRejectedNew_coexist_splitAndBalanceWithoutInflatingLegacyRejected()
            throws Exception {
        ManualClock clock = new ManualClock(TestFixtures.T0);
        SensorConfig cf = TestFixtures.config("fast", SensorType.TEMPERATURE, -40, 80);
        SensorConfig cs = TestFixtures.config("slow", SensorType.HUMIDITY, 0, 100);

        int fastN = 300;
        Map<String, List<SampleScript>> scripts = Map.of("fast",
                java.util.stream.IntStream.range(0, fastN).mapToObj(i -> SampleScript.normal(20.0)).toList());
        Sensor fast = TestFixtures.limited(
                TestFixtures.scriptedSensors(clock, Map.of("fast", cf), scripts),
                Map.of("fast", fastN)).get("fast");

        CountDownLatch reached = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Sensor slow = new SlowGateSensor(cs, 3, 50.0, reached, release);

        // 容量 1 + DROP_OLDEST + 单消费者高速：fast 必产生大量顶掉最旧；slow tick0 占缓冲
        GatewayConfig cfg = TestFixtures.gatewayConfig(
                "rules-v1", List.of(cf, cs), 1, OverflowPolicy.DROP_OLDEST, 1, 1, 3);
        SamplingGateway gw = new SamplingGateway(cfg, Map.of("fast", fast, "slow", slow),
                clock, millis -> { });
        gw.start();
        long gen = gw.reconciliation().runningGeneration();

        // slow 已产出 tick0（入队/被处理），停在 tick1 的 offer 之前
        assertTrue(reached.await(10, java.util.concurrent.TimeUnit.SECONDS));
        // 等 fast 制造足够多的顶掉（EVICTED_OLD）
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline && gw.stats().evictedOld() < 100) {
            Thread.sleep(5);
        }
        assertTrue(gw.stats().evictedOld() >= 100, "expected many EVICTED_OLD under pressure");

        // 请求 fast 停拍并等它到限退出，避免关闭时产生新的入队干扰
        gw.requestStop();
        long deadline2 = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline2
                && gw.reconciliation().reconcile(
                        new ReconciliationFilter(gen, "fast", null, 0L, 0L)).total() < fastN) {
            Thread.sleep(5);
        }

        // 关键：先关闭缓冲（不 join stop，直接用缓冲视图关闭），此时容量 1 缓冲要么有 1 条滞留、
        // 要么空。为确定性让 slow tick0 仍滞留：用极短 drain 的 stop 在另一线程，
        // 我们在缓冲 isClosed 后立刻放行 tick1。
        // 简化确定性：直接关闭缓冲受理（与 stop 第一步等价），再放行 slow tick1。
        gw.bufferView().close();
        release.countDown(); // slow tick1 对“已关闭的 DROP_OLDEST 缓冲”offer -> REJECTED_NEW
        Thread.sleep(100);

        gw.stop();

        ReconciliationReport r = gw.reconciliation().reconcile(ReconciliationFilter.forGeneration(gen));
        assertClosed(r);

        // 两类背压都必须出现
        assertTrue(r.evictedOld() >= 100, "expected EVICTED_OLD under pressure");
        assertTrue(r.rejectedNew() > 0, "expected REJECTED_NEW from offering after buffer closed");
        assertTrue(r.evictedEntries().size() > 0);
        assertTrue(r.rejectedEntries().stream()
                .anyMatch(e -> e.reason() == RejectReason.BUFFER_OVERFLOW
                        && e.backpressure()
                        == com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.BackpressureKind.REJECTED_NEW));
        // 被顶掉条目的背压标记正确
        r.evictedEntries().forEach(e ->
                assertEquals(Disposition.EVICTED_OLD, e.disposition()));

        // 老统计口径不被改动：rejected 只含直接拒绝，不含被顶掉
        GatewayStats s = gw.stats();
        assertEquals(s.evictedOld(), r.evictedOld());
        assertEquals(s.rejectedNew(), r.rejectedNew());
        assertEquals(s.dropped(), s.evictedOld() + s.rejectedNew(),
                "dropped is the union of the two backpressure kinds");
        assertEquals(s.dropped(), r.evictedOld() + r.rejectedNew());
        long overflowReason = s.rejectedByReason().getOrDefault(RejectReason.BUFFER_OVERFLOW, 0L);
        assertEquals(s.rejectedNew(), overflowReason,
                "legacy rejectedByReason[BUFFER_OVERFLOW] must exclude evictions");
        // 总闭合（生产总数 = 四类终结之和）
        assertEquals(s.submitted(),
                s.accepted() + s.rejected() + s.evictedOld() + s.shutdownIncomplete());
        assertEquals(s.submitted(),
                r.accepted() + r.rejected() + r.evictedOld() + r.incomplete());
    }
}
