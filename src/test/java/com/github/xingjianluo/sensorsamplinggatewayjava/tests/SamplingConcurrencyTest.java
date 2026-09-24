package com.github.xingjianluo.sensorsamplinggatewayjava.tests;

import com.github.xingjianluo.sensorsamplinggatewayjava.config.GatewayProperties;
import com.github.xingjianluo.sensorsamplinggatewayjava.config.OverflowPolicy;
import com.github.xingjianluo.sensorsamplinggatewayjava.config.RetryPolicy;
import com.github.xingjianluo.sensorsamplinggatewayjava.gateway.DefaultSamplingGateway;
import com.github.xingjianluo.sensorsamplinggatewayjava.gateway.OutcomeSink;
import com.github.xingjianluo.sensorsamplinggatewayjava.gateway.RecordingOutcomeSink;
import com.github.xingjianluo.sensorsamplinggatewayjava.gateway.SamplingGateway;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.RejectReason;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SampleOutcome;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorSpec;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorType;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.StatsSnapshot;
import com.github.xingjianluo.sensorsamplinggatewayjava.sensor.ScriptedSensor;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发采集/消费集成测试：使用内存脚本传感器（无硬件、无外部服务），
 * 在真实多线程下验证背压、溢出记账、故障隔离与关闭重启的“不重不漏”。
 * 每个用例打印输入规模、各项计数与判定核对结果。
 */
class SamplingConcurrencyTest {

    private SensorSpec spec(String id, long periodMs, String version) {
        return new SensorSpec(id, SensorType.TEMPERATURE, periodMs,
                -40.0, 80.0, 60_000L, 60_000L, 1.0, 0.0, version);
    }

    private GatewayProperties properties(int capacity, OverflowPolicy policy) {
        GatewayProperties p = new GatewayProperties();
        p.setBufferCapacity(capacity);
        p.setOverflowPolicy(policy);
        return p;
    }

    private ScriptedSensor healthy(String id) {
        return new ScriptedSensor(id, List.of(ScriptedSensor.Step.readAtNow(25.0)));
    }

    /** 每条结论交付前 sleep 的慢消费出口（继承记录出口，仍线程安全、按序记录）。 */
    private static final class SlowRecordingSink extends RecordingOutcomeSink {
        private final long delayMs;

        SlowRecordingSink(long delayMs) {
            this.delayMs = delayMs;
        }

        @Override
        public synchronized void deliver(SampleOutcome outcome) {
            super.deliver(outcome);
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private Map<String, List<Long>> acceptedSeqs(List<SampleOutcome> outcomes) {
        Map<String, List<Long>> map = new TreeMap<>();
        for (SampleOutcome o : outcomes) {
            if (o instanceof SampleOutcome.Accepted a) {
                map.computeIfAbsent(o.sensorId(), k -> new ArrayList<>()).add(a.sample().seq());
            }
        }
        return map;
    }

    private void assertContiguousFromOne(List<Long> seqs) {
        for (int i = 0; i < seqs.size(); i++) {
            assertEquals(i + 1L, seqs.get(i),
                    "序列号必须从 1 连续，实际在位置 " + i + " 出现 " + seqs);
        }
    }

    private void printStats(String name, SamplingGateway gw) {
        StatsSnapshot s = gw.stats();
        System.out.printf("[%s] produced=%d accepted=%d rejected=%s retries=%d isolated=%s buffered=%d%n",
                name, s.produced(), s.accepted(), s.rejected(), s.retries(),
                gw.isolatedSensors(), s.buffered());
    }

    private long sumRejected(StatsSnapshot s) {
        return s.rejected().values().stream().mapToLong(Long::longValue).sum();
    }

    @Test
    void blockBackpressureLosesNothingUnderPressure() throws Exception {
        // 3 个生产者 5ms 周期，缓冲容量 4，消费端每条处理 2ms，制造生产快于消费。
        GatewayProperties props = properties(4, OverflowPolicy.BLOCK);
        DefaultSamplingGateway gw = new DefaultSamplingGateway(
                props, RetryPolicy.noRetry(), System::currentTimeMillis, new SlowRecordingSink(2));
        for (String id : List.of("A", "B", "C")) {
            gw.attach(spec(id, 5, "v1"), healthy(id));
        }
        gw.start();
        Thread.sleep(400);
        gw.stop();
        printStats("block", gw);

        StatsSnapshot s = gw.stats();
        assertEquals(0, s.buffered(), "关闭后缓冲必须排空");
        assertEquals(s.produced(), s.accepted(), "BLOCK 背压下不允许任何读数丢失");
        assertEquals(s.produced(), s.produced(), "sanity");
        acceptedSeqs(gw.delivered()).forEach((id, seqs) -> {
            System.out.printf("[block] sensor=%s acceptedCount=%d seqHead=%s%n",
                    id, seqs.size(), seqs.subList(0, Math.min(5, seqs.size())));
            assertContiguousFromOne(seqs);
        });
    }

    @Test
    void dropOldestOverflowIsCountedAndAccountsBalance() throws Exception {
        GatewayProperties props = properties(2, OverflowPolicy.DROP_OLDEST);
        SlowRecordingSink sink = new SlowRecordingSink(4);
        DefaultSamplingGateway gw = new DefaultSamplingGateway(
                props, RetryPolicy.noRetry(), System::currentTimeMillis, sink);
        for (String id : List.of("A", "B")) {
            gw.attach(spec(id, 1, "v1"), healthy(id));
        }
        gw.start();
        Thread.sleep(300);
        gw.stop();
        printStats("drop-oldest", gw);

        StatsSnapshot s = gw.stats();
        long overflow = s.rejected().getOrDefault(RejectReason.BUFFER_OVERFLOW, 0L);
        System.out.printf("[drop-oldest] overflowRejected=%d delivered=%d%n",
                overflow, sink.outcomes().size());
        assertTrue(overflow > 0, "持续压力下必须观测到溢出拒绝");
        assertEquals(s.produced(), s.accepted() + sumRejected(s),
                "账目守恒：produced 必须等于 accepted+rejected");
        assertEquals(s.produced(), sink.outcomes().size(), "每条产出恰好一条结论，不重不漏");
    }

    @Test
    void permanentFaultIsolatesOneSensorWithoutAffectingOthers() throws Exception {
        GatewayProperties props = properties(8, OverflowPolicy.BLOCK);
        DefaultSamplingGateway gw = new DefaultSamplingGateway(
                props, RetryPolicy.noRetry(), System::currentTimeMillis, new RecordingOutcomeSink());
        gw.attach(spec("bad", 5, "v1"),
                new ScriptedSensor("bad", List.of(ScriptedSensor.Step.permanentFault())));
        gw.attach(spec("good1", 5, "v1"), healthy("good1"));
        gw.attach(spec("good2", 5, "v1"), healthy("good2"));

        gw.start();
        Thread.sleep(150);
        gw.stop();
        printStats("isolate", gw);

        StatsSnapshot s = gw.stats();
        assertTrue(gw.isolatedSensors().contains("bad"), "持续故障传感器必须被隔离");
        assertTrue(s.isolatedSensors() >= 1);
        assertTrue(s.produced() > 0, "其他传感器必须继续采样，不受隔离影响");
        assertEquals(s.produced(), s.accepted(), "健康传感器读数应全部被接受");
        acceptedSeqs(gw.delivered()).keySet().forEach(id ->
                assertFalse(id.equals("bad"), "被隔离传感器不得产出任何读数，实际出现 " + id));
        System.out.println("[isolate] bad sensor produced=0; good sensors kept sampling");
    }

    @Test
    void transientFaultRecoversWithinBoundedRetries() throws Exception {
        GatewayProperties props = properties(8, OverflowPolicy.BLOCK);
        // 每轮允许 3 次尝试，退避 5ms 起，倍率 2；连续 3 轮失败才隔离
        RetryPolicy retry = new RetryPolicy(3, Duration.ofMillis(5), Duration.ofMillis(50),
                2.0, 3, Duration.ofMillis(500));
        DefaultSamplingGateway gw = new DefaultSamplingGateway(
                props, retry, System::currentTimeMillis, new RecordingOutcomeSink());
        // 脚本循环：瞬时故障、瞬时故障、正常读数 -> 每轮第 3 次尝试恢复
        ScriptedSensor flaky = new ScriptedSensor("flaky", List.of(
                ScriptedSensor.Step.transientFault(),
                ScriptedSensor.Step.transientFault(),
                ScriptedSensor.Step.readAtNow(30.0)));
        gw.attach(spec("flaky", 30, "v1"), flaky);

        gw.start();
        Thread.sleep(250);
        gw.stop();
        printStats("transient", gw);

        assertFalse(gw.isolatedSensors().contains("flaky"), "能恢复的瞬时故障不得导致隔离");
        assertTrue(gw.stats().retries() >= 2, "必须观测到有界重试");
        assertTrue(gw.stats().accepted() >= 1, "退避重试后必须自动恢复并产出样本");
        System.out.println("[transient] recovered via bounded retry; accepted >= 1, isolated=false");
    }

    @Test
    void stopAndRestartDeliversAcceptedDataWithoutDuplicateOrLoss() throws Exception {
        GatewayProperties props = properties(16, OverflowPolicy.BLOCK);
        DefaultSamplingGateway gw = new DefaultSamplingGateway(
                props, RetryPolicy.noRetry(), System::currentTimeMillis, new RecordingOutcomeSink());
        gw.attach(spec("A", 5, "v1"), healthy("A"));

        gw.start();
        Thread.sleep(150);
        gw.stop();
        long phase1Produced = gw.stats().produced();
        int phase1Delivered = gw.delivered().size();
        assertEquals(phase1Produced, phase1Delivered, "关闭时已受理数据必须全部交付");

        gw.restart();
        Thread.sleep(150);
        gw.stop();
        printStats("restart", gw);
        assertEquals(gw.stats().produced(), gw.delivered().size(),
                "全生命周期每条读数恰好一条结论");
        List<Long> seqs = acceptedSeqs(gw.delivered()).get("A");
        System.out.printf("[restart] phase1Produced=%d totalDelivered=%d uniqueSeqs=%d%n",
                phase1Produced, gw.delivered().size(), seqs.stream().distinct().count());
        assertEquals(seqs.size(), seqs.stream().distinct().count(), "不得有重复序列号");
        assertContiguousFromOne(seqs);
    }

}
