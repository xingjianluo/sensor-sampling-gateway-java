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
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.ReconciliationEntry;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.ReconciliationFilter;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.ReconciliationReport;
import com.github.xingjianluo.sensorsamplinggatewayjava.reliability.RetryPolicy;
import com.github.xingjianluo.sensorsamplinggatewayjava.sensor.SamplingLimitReached;
import com.github.xingjianluo.sensorsamplinggatewayjava.sensor.Sensor;
import com.github.xingjianluo.sensorsamplinggatewayjava.time.ManualClock;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 两类边界的自动化覆盖：
 * <ol>
 *   <li>同一批次内“顶掉最旧（EVICTED_OLD）”与“直接丢弃最新（REJECTED_NEW）”并存，
 *       老统计口径不变、两类背压分列、账目闭合、读数只算一次；</li>
 *   <li>关闭排空超时 / 消费者卡住后仍有读数滞留缓冲：滞留读数明确落为
 *       SHUTDOWN_INCOMPLETE、报告带未完成数量与不完整标记；重启后跨批次生产总数
 *       与对账总数仍永久对平、可按批次定位。</li>
 * </ol>
 */
class BackpressureAndShutdownBoundaryTest {

    private static GatewayConfig config(String version, SensorConfig c, int buffer,
                                        OverflowPolicy policy, int consumers, long drainMs) {
        return new GatewayConfig(version, List.of(c),
                com.github.xingjianluo.sensorsamplinggatewayjava.model.CalibrationModel
                        .identity(version + "-calib"),
                buffer, policy, consumers,
                new RetryPolicy(1, 1L, 2.0, 50L, 0.0),
                3, 1_000_000L, drainMs, 4096,
                4096, 8, com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.RetentionPolicy.ROLL_OLDEST);
    }

    private static void assertClosed(ReconciliationReport r) {
        assertEquals(r.total(), r.accepted() + r.rejected() + r.evictedOld(),
                "total = accepted + rejected + evictedOld");
        long byReason = r.rejectedByReason().values().stream().mapToLong(Long::longValue).sum();
        assertEquals(r.rejected(), byReason, "rejected = sum(byReason)");
        assertEquals(r.rejectedNew(),
                r.rejectedByReason().getOrDefault(RejectReason.BUFFER_OVERFLOW, 0L),
                "rejectedNew = legacy BUFFER_OVERFLOW bucket");
        Set<String> keys = new HashSet<>();
        r.acceptedEntries().forEach(e -> assertTrue(
                keys.add(e.generation() + "#" + e.sensorTickKey())));
        r.rejectedEntries().forEach(e -> assertTrue(
                keys.add(e.generation() + "#" + e.sensorTickKey()), "single counted"));
        if (r.complete()) {
            assertEquals(r.total(), keys.size(), "complete report enumerates every reading once");
        }
    }

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
        throw new AssertionError("timeout waiting " + sensor + " count " + expected);
    }

    /**
     * 确定性闸门传感器：先产出前 {@code burst} 拍（制造 DROP_OLDEST 压力），
     * 然后阻塞在 {@code gate} 上，直到测试关闭缓冲后再放行——放行后产生的每一拍
     * 都必然撞上“已关闭缓冲”而被直接拒绝（REJECTED_NEW），消除时序竞态。
     */
    private static final class GatedSensor implements Sensor {
        private final SensorConfig cfg;
        private final int total;
        private final int burst;
        private final CountDownLatch gate;

        GatedSensor(SensorConfig cfg, int total, int burst, CountDownLatch gate) {
            this.cfg = cfg;
            this.total = total;
            this.burst = burst;
            this.gate = gate;
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
            if (tick == burst) {
                gate.await();
            }
            long ck = com.github.xingjianluo.sensorsamplinggatewayjava.sensor.ScriptedSensor
                    .checksum(tick, TestFixtures.T0, 20.0);
            return new RawReading(cfg.getId(), tick, TestFixtures.T0, 20.0, ck);
        }
    }

    @Test
    void evictedOldAndRejectedNew_coexist_splitSeparately_legacyCountersUnchanged() throws Exception {
        // DROP_OLDEST：开闸前的突发高压力 -> 顶掉最旧；测试在生产者被闸门挡住期间关闭缓冲，
        // 开闸后所有新 offer 确定性地走“关闭期拒绝最新”= REJECTED_NEW，
        // 两种背压因此在同一批次内必然同时出现，且无时序竞态。
        ManualClock clock = new ManualClock(TestFixtures.T0);
        int burst = 30;
        int total = 60;
        SensorConfig c = TestFixtures.config("fast", SensorType.TEMPERATURE, -40, 80);
        CountDownLatch gate = new CountDownLatch(1);
        GatewayConfig cfg = config("rules-v1", c, 1, OverflowPolicy.DROP_OLDEST, 1, 5000L);
        SamplingGateway gw = new SamplingGateway(cfg,
                Map.of("fast", new GatedSensor(c, total, burst, gate)), clock, millis -> { });
        gw.start();
        long gen = gw.reconciliation().runningGeneration();
        // 等到前 burst 拍都已终结（压力下必然含 EVICTED_OLD）
        waitForCount(gw, gen, "fast", burst, 10_000);
        // 生产者此刻阻塞在闸门（下一拍 tick==burst）；确认压力已造成顶掉
        assertTrue(gw.stats().evictedOld() > 0, "burst pressure must have evicted oldest");
        assertEquals(0L, gw.stats().rejectedNew(), "no newest rejection before close");

        // 在生产者仍“存活”时先关闭受理（等价于 stop 关闭与 offer 竞争那一刻），再放行
        assertTrue(gw.isRunning());
        gw.bufferView().close();
        gate.countDown();

        // 等待开闸后 (total-burst) 拍全部撞关闭被拒绝
        waitForCount(gw, gen, "fast", total, 10_000);
        gw.requestStop();
        gw.stop();

        ReconciliationReport r = gw.reconciliation().reconcile(ReconciliationFilter.forGeneration(gen));
        assertClosed(r);
        assertTrue(r.complete());
        assertTrue(r.evictedOld() > 0, "EVICTED_OLD present");
        assertTrue(r.rejectedNew() > 0, "REJECTED_NEW present");
        // 开闸后共 total-burst 拍全部直接拒绝
        assertEquals(total - burst, r.rejectedNew(),
                "every post-close offer is a directly-rejected newest reading");
        // 老口径：两类背压中只有 REJECTED_NEW 进 BUFFER_OVERFLOW 老计数
        assertEquals(r.rejectedNew(), r.rejectedByReason().get(RejectReason.BUFFER_OVERFLOW));
        // 两类背压在明细上可区分
        long detailEvicted = r.rejectedEntries().stream()
                .filter(e -> e.backpressure() == com.github.xingjianluo.sensorsamplinggatewayjava
                        .reconcile.BackpressureKind.EVICTED_OLD).count();
        long detailRejectedNew = r.rejectedEntries().stream()
                .filter(e -> e.backpressure() == com.github.xingjianluo.sensorsamplinggatewayjava
                        .reconcile.BackpressureKind.REJECTED_NEW).count();
        assertEquals(r.evictedOld(), detailEvicted);
        assertEquals(r.rejectedNew(), detailRejectedNew);
        // 与统一计数互证（老看板关系全部成立）
        GatewayStats s = gw.stats();
        assertEquals(s.submitted(), r.total());
        assertEquals(total, r.total(), "exactly one ledger entry per tick");
        assertEquals(s.submitted(), s.enqueued() + s.rejected());
        assertEquals(s.enqueued(), s.consumed() + s.evictedOld());
        assertEquals(s.dropped(), s.evictedOld() + s.rejectedNew());
        assertEquals(s.dropped(), r.evictedOld() + r.rejectedNew());
        assertEquals(s.submitted(), s.accepted() + s.rejected() + s.evictedOld());
    }

    /** 固定产出 n 条正常值的传感器。 */
    private static final class FixedSensor implements Sensor {
        private final SensorConfig cfg;
        private final int n;

        FixedSensor(SensorConfig cfg, int n) {
            this.cfg = cfg;
            this.n = n;
        }

        @Override
        public SensorConfig config() {
            return cfg;
        }

        @Override
        public RawReading sample(long tick) throws Exception {
            if (tick >= n) {
                throw new SamplingLimitReached(cfg.getId(), tick);
            }
            long ck = com.github.xingjianluo.sensorsamplinggatewayjava.sensor.ScriptedSensor
                    .checksum(tick, TestFixtures.T0, 20.0);
            return new RawReading(cfg.getId(), tick, TestFixtures.T0, 20.0, ck);
        }
    }

    @Test
    void drainTimeoutWithStuckConsumers_strandedReadingsRecorded_markedIncomplete_afterRestartStillBalanced()
            throws Exception {
        ManualClock clock = new ManualClock(TestFixtures.T0);
        int n = 30;
        SensorConfig c = TestFixtures.config("s", SensorType.TEMPERATURE, -40, 80);
        // 容量 4、单消费者、排空超时仅 50ms；消费者在处理完第 1 条后被测试钩子卡住
        GatewayConfig cfg = config("rules-v1", c, 4, OverflowPolicy.BLOCK, 1, 50L);
        SamplingGateway gw = new SamplingGateway(cfg, Map.of("s", new FixedSensor(c, n)),
                clock, millis -> { });
        CountDownLatch stuck = new CountDownLatch(1);
        CountDownLatch processedFirst = new CountDownLatch(1);
        gw.setConsumerHookForTesting(() -> {
            processedFirst.countDown();
            try {
                stuck.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        gw.start();
        long g1 = gw.reconciliation().runningGeneration();
        // 等消费者处理完第一条后永久卡住：此后缓冲被填满且生产者在 BLOCK 上等待
        assertTrue(processedFirst.await(10, java.util.concurrent.TimeUnit.SECONDS));
        // 再给生产者时间把缓冲填满（容量4，零睡眠，自旋等待即可）
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline && gw.bufferView().queued() < 4) {
            Thread.sleep(2);
        }
        assertEquals(4, gw.bufferView().queued(), "buffer is full of accepted readings while consumer stuck");

        // stop：排空超时 -> 4 条滞留被 drainAll 兜底落为 SHUTDOWN_INCOMPLETE；
        // 消费者随后 shutdownNow 被中断，drainAll 与其 poll 互斥，不重不漏。
        gw.stop();
        stuck.countDown();

        ReconciliationReport r1 = gw.reconciliation().reconcile(ReconciliationFilter.forGeneration(g1));
        assertClosed(r1);
        assertFalse(r1.complete(), "stranded readings must mark report incomplete");
        assertTrue(r1.unfinished() >= 4, "at least the four queued readings are unfinished: "
                + r1.unfinished());
        assertEquals(r1.unfinished(),
                r1.rejectedByReason().get(RejectReason.SHUTDOWN_INCOMPLETE));
        // 滞留读数以可区分记录定位
        List<ReconciliationEntry> stranded = r1.rejectedEntries().stream()
                .filter(e -> e.reason() == RejectReason.SHUTDOWN_INCOMPLETE).toList();
        assertEquals(r1.unfinished(), stranded.size());
        Set<String> strandedKeys = new HashSet<>();
        stranded.forEach(e -> assertTrue(strandedKeys.add(e.generation() + "#" + e.sensorTickKey())));
        // 生产总数与对账总数对平（accepted + 各类 rejected + evictedOld）
        GatewayStats s = gw.stats();
        assertEquals(s.submitted(), r1.total());
        assertEquals(s.unfinished(), r1.unfinished());
        assertEquals(s.submitted(), s.accepted() + s.rejected() + s.evictedOld());
        // 缓冲已被兜底清空
        assertEquals(0, gw.bufferView().queued());
        var info1 = gw.reconciliation().generation(g1);
        assertFalse(info1.completeRun());
        assertEquals(r1.unfinished(), info1.unfinished());

        // 重启开启新批次（新规则版本），运行正常且完整；跨批次总账仍闭合、可按批次定位
        gw.setConsumerHookForTesting(null);
        GatewayConfig cfg2 = config("rules-v2", c, 16, OverflowPolicy.BLOCK, 2, 5000L);
        gw.restart(cfg2, Map.of("s", new FixedSensor(c, n)));
        long g2 = gw.reconciliation().runningGeneration();
        assertTrue(g2 > g1);
        waitForCount(gw, g2, "s", n, 10_000);
        gw.requestStop();
        gw.stop();

        ReconciliationReport r2 = gw.reconciliation().reconcile(ReconciliationFilter.forGeneration(g2));
        assertClosed(r2);
        assertTrue(r2.complete(), "the restarted run drains normally and is complete");
        assertEquals(n, r2.total());
        assertEquals(0L, r2.unfinished());

        ReconciliationReport across = gw.reconciliation().reconcile(ReconciliationFilter.all());
        assertClosed(across);
        assertEquals(r1.total() + r2.total(), across.total(),
                "production total reconciles across restart incl. the stranded batch");
        // 不完整只标在 g1，复查时可定位
        assertFalse(gw.reconciliation().generation(g1).completeRun());
        assertTrue(gw.reconciliation().generation(g2).completeRun());
        // 按 SHUTDOWN_INCOMPLETE 原因切片也能直接找回滞留明细
        long unfinishedAcrossAllGens = gw.reconciliation().generations().stream()
                .mapToLong(g -> g.unfinished()).sum();
        assertEquals(r1.unfinished(), unfinishedAcrossAllGens);
    }
}
