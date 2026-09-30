package com.github.xingjianluo.sensorsamplinggatewayjava;

import com.github.xingjianluo.sensorsamplinggatewayjava.gateway.GatewayConfig;
import com.github.xingjianluo.sensorsamplinggatewayjava.gateway.SamplingGateway;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.CalibrationModel;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.GatewayStats;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.OverflowPolicy;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.RawReading;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.RejectReason;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SampleScript;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorConfig;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorType;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.GenerationInfo;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.ReconciliationEntry;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.ReconciliationFilter;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.ReconciliationReport;
import com.github.xingjianluo.sensorsamplinggatewayjava.reliability.RetryPolicy;
import com.github.xingjianluo.sensorsamplinggatewayjava.sensor.Sensor;
import com.github.xingjianluo.sensorsamplinggatewayjava.time.ManualClock;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 关闭边界：排空超时 / 消费者短暂卡住时，已提交但未裁决的读数被明确落为
 * SHUTDOWN_INCOMPLETE（可区分、可定位），报告带不完整标记；重启后新一代正常运行，
 * 跨代总账仍闭合、同一条读数只算一次。
 */
class ShutdownDrainIncompleteTest {

    /** 前 firstQuick 拍快速通过，之后每拍在校准阶段阻塞直到放行（模拟消费者卡住）。 */
    private static CalibrationModel stuckCalibration(String version, int blockAfter,
                                                     CountDownLatch entered,
                                                     CountDownLatch release,
                                                     AtomicInteger enteredCount) {
        return CalibrationModel.custom(version, (sensorId, raw) -> {
            if (enteredCount.getAndIncrement() >= blockAfter) {
                entered.countDown();
                try {
                    // 消费者在裁决前卡住，直到 stop 打断
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
            return raw;
        });
    }

    private static GatewayConfig cfg(String rules, CalibrationModel calib, long drainMs, int buffer) {
        SensorConfig c = TestFixtures.config("s", SensorType.TEMPERATURE, -40, 80);
        return new GatewayConfig(rules, List.of(c), calib,
                buffer, OverflowPolicy.BLOCK, 1,
                new RetryPolicy(1, 1L, 2.0, 50L, 0.0),
                3, 1_000_000L, drainMs, 4096);
    }

    private static void assertClosed(ReconciliationReport r) {
        assertEquals(r.total(), r.accepted() + r.rejected() + r.evictedOld() + r.incomplete());
        Set<String> keys = new HashSet<>();
        List<ReconciliationEntry> all = new java.util.ArrayList<>();
        all.addAll(r.acceptedEntries());
        all.addAll(r.rejectedEntries());
        all.addAll(r.evictedEntries());
        all.addAll(r.incompleteEntries());
        for (ReconciliationEntry e : all) {
            assertTrue(keys.add(e.generation() + "#" + e.sensorTickKey()));
        }
        assertEquals(r.total(), all.size());
    }

    @Test
    void drainTimeout_strandedReadingsBecomeIncomplete_thenRestartBalances() throws Exception {
        ManualClock clock = new ManualClock(TestFixtures.T0);
        int n = 30;
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger enteredCount = new AtomicInteger();
        // 消费者处理第 2 条读数时在校准处卡住；缓冲容量 8，后续读数滞留在缓冲中
        CalibrationModel stuck = stuckCalibration(
                "rules-v1-calib", 1, entered, release, enteredCount);

        SensorConfig c = TestFixtures.config("s", SensorType.TEMPERATURE, -40, 80);
        Map<String, List<SampleScript>> scripts = Map.of("s",
                java.util.stream.IntStream.range(0, n).mapToObj(i -> SampleScript.normal(20.0)).toList());
        Sensor sensor = TestFixtures.limited(
                TestFixtures.scriptedSensors(clock, Map.of("s", c), scripts),
                Map.of("s", n)).get("s");

        GatewayConfig v1 = cfg("rules-v1", stuck, 300L, 8);
        SamplingGateway gw = new SamplingGateway(v1, Map.of("s", sensor),
                clock, TestFixtures.fastSleeper());
        gw.start();
        long g1 = gw.reconciliation().runningGeneration();

        // 等到消费者卡在第 2 条读数；此时生产者已提交更多读数（1 条在途 + 若干滞留缓冲）
        assertTrue(entered.await(10, java.util.concurrent.TimeUnit.SECONDS));
        // 给生产者时间填满缓冲
        Thread.sleep(100);

        // stop：排空超时（消费者卡住），在途读数被打断、滞留读数被兜底补记
        Thread stopper = new Thread(gw::stop);
        stopper.start();
        Thread.sleep(700);
        // 放行卡住的校准调用；它会被 stop 的中断打断 -> 在途读数也落为 INCOMPLETE
        release.countDown();
        stopper.join(10_000);
        assertFalse(gw.isRunning());

        ReconciliationReport r1 = gw.reconciliation().reconcile(ReconciliationFilter.forGeneration(g1));
        assertClosed(r1);
        // 明确有未完成记录
        assertTrue(r1.incomplete() > 0, "stranded/in-flight readings must be recorded incomplete");
        assertFalse(r1.complete(), "report must be explicitly marked incomplete");
        r1.incompleteEntries().forEach(e -> {
            assertEquals(RejectReason.SHUTDOWN_INCOMPLETE, e.reason());
            assertEquals(com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.Disposition.INCOMPLETE,
                    e.disposition());
        });
        // 每条已提交读数都有归宿（接受 + 未完成 == 总提交，无静默丢失）
        GatewayStats s1 = gw.stats();
        assertEquals(s1.submitted(),
                r1.accepted() + r1.rejected() + r1.evictedOld() + r1.incomplete());
        assertEquals(s1.shutdownIncomplete(), r1.incomplete());
        GenerationInfo info1 = gw.reconciliation().generation(g1);
        assertTrue(info1.sealed());
        assertEquals(r1.incomplete(), info1.incomplete());

        // —— 重启：新一代，生产与对账恢复正常 ——
        CalibrationModel healthy = CalibrationModel.identity("rules-v2-calib");
        int n2 = 20;
        Map<String, List<SampleScript>> scripts2 = Map.of("s",
                java.util.stream.IntStream.range(0, n2).mapToObj(i -> SampleScript.normal(20.0)).toList());
        Sensor sensor2 = TestFixtures.limited(
                TestFixtures.scriptedSensors(clock, Map.of("s", c), scripts2),
                Map.of("s", n2)).get("s");
        GatewayConfig v2 = cfg("rules-v2", healthy, 5000L, 8);
        gw.restart(v2, Map.of("s", sensor2));
        long g2 = gw.reconciliation().runningGeneration();
        assertTrue(g2 > g1);
        waitForCount(gw, g2, n2, 10_000);
        gw.requestStop();
        gw.stop();

        ReconciliationReport r2 = gw.reconciliation().reconcile(ReconciliationFilter.forGeneration(g2));
        assertClosed(r2);
        assertTrue(r2.complete(), "fresh generation fully processed");
        assertEquals(n2, r2.total());
        assertEquals(n2, r2.accepted());
        assertEquals(0, r2.incomplete());

        // 跨代总账闭合：两代各自闭合，和为合计；tick 都从 0 起但身份键含代次，不重复
        ReconciliationReport across = gw.reconciliation().reconcile(ReconciliationFilter.forSensor("s"));
        assertClosed(across);
        assertEquals(r1.total() + r2.total(), across.total());
        // 未完成记录可在跨代账中定位
        assertEquals(r1.incomplete(), across.incomplete());
        assertNotNull_(gw.reconciliation().findEntry(g1,
                "s#" + r1.incompleteEntries().get(0).tick()));
    }

    private void waitForCount(SamplingGateway gw, long gen, long expected, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (gw.reconciliation()
                    .reconcile(ReconciliationFilter.forGeneration(gen)).total() >= expected) {
                return;
            }
            Thread.sleep(5);
        }
        throw new AssertionError("timeout");
    }

    private static void assertNotNull_(Object o) {
        assertTrue(o != null);
    }
}
