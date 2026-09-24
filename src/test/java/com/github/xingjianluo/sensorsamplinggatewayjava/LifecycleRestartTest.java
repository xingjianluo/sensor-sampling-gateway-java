package com.github.xingjianluo.sensorsamplinggatewayjava;

import com.github.xingjianluo.sensorsamplinggatewayjava.gateway.GatewayConfig;
import com.github.xingjianluo.sensorsamplinggatewayjava.gateway.SamplingGateway;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.GatewayStats;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.OverflowPolicy;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 关闭与重启生命周期：
 * <ul>
 *   <li>优雅关闭后，已受理（已入队）读数全部被消费，缓冲排空为 0；</li>
 *   <li>重启前每个 sensor#tick 恰好一条裁决，重启后 tick 重新从 0 计，
 *       新规则版本下结论随之更新，全程无重复 key（以“代次+key”核对）。</li>
 * </ul>
 */
class LifecycleRestartTest {

    private static final Logger LOG = Logger.getLogger(LifecycleRestartTest.class.getName());

    private static final class GenSensor implements Sensor {
        private final SensorConfig cfg;
        private final String generation;
        private final int count;
        private int done;

        GenSensor(SensorConfig cfg, String generation, int count) {
            this.cfg = cfg;
            this.generation = generation;
            this.count = count;
        }

        @Override
        public SensorConfig config() {
            return cfg;
        }

        @Override
        public com.github.xingjianluo.sensorsamplinggatewayjava.model.RawReading sample(long tick) {
            if (done++ >= count) {
                throw new com.github.xingjianluo.sensorsamplinggatewayjava.sensor.SamplingLimitReached(
                        cfg.getId(), tick);
            }
            // 每一代产出与代次绑定的确定值，便于区分重启前后
            double value = generation.equals("g1") ? 10.0 : 20.0;
            long ck = com.github.xingjianluo.sensorsamplinggatewayjava.sensor.ScriptedSensor
                    .checksum(tick, TestFixtures.T0, value);
            return new com.github.xingjianluo.sensorsamplinggatewayjava.model.RawReading(
                    cfg.getId(), tick, TestFixtures.T0, value, ck);
        }
    }

    @Test
    void gracefulShutdownDrainsAllAcceptedDataAndRestartIsClean() throws Exception {
        ManualClock clock = new ManualClock(TestFixtures.T0);
        SensorConfig c = TestFixtures.config("s", SensorType.TEMPERATURE, -40, 80);

        GatewayConfig cfg1 = TestFixtures.gatewayConfig(
                "rules-v1", List.of(c), 16, OverflowPolicy.BLOCK, 2, 1, 3);
        Map<String, Sensor> g1 = Map.of("s", new GenSensor(c, "g1", 50));
        SamplingGateway gw = new SamplingGateway(cfg1, g1, clock, TestFixtures.fastSleeper());
        gw.start();
        waitTicks(gw, 49);
        gw.requestStop();
        gw.stop();

        // 关闭后缓冲必须排空
        assertEquals(0, gw.bufferView().queued(), "buffer fully drained after graceful shutdown");
        GatewayStats after1 = gw.stats();
        List<Verdict> v1 = gw.verdictLog().bySensor("s");
        Set<String> keys1 = new HashSet<>();
        v1.forEach(v -> assertTrue(keys1.add(v.reading().key()), "duplicate in gen1"));
        assertEquals(50, v1.size(), "exactly one verdict per accepted tick before restart");
        assertEquals(50, after1.accepted());
        LOG.info(() -> "gen1 drained: " + after1.accepted() + " accepted, queued=0");

        // 用新规则版本重启（校准不同 -> 值结论改变；传感器为第二代，产出不同值）
        GatewayConfig cfg2 = TestFixtures.gatewayConfig(
                "rules-v2", List.of(c), 16, OverflowPolicy.BLOCK, 2, 1, 3);
        Map<String, Sensor> g2 = Map.of("s", new GenSensor(c, "g2", 30));
        gw.restart(cfg2, g2);
        assertTrue(gw.isRunning());
        waitTicks(gw, 29);
        gw.requestStop();
        gw.stop();

        List<Verdict> v2 = gw.verdictLog().bySensor("s");
        Set<String> keys2 = new HashSet<>();
        v2.forEach(v -> assertTrue(keys2.add(v.reading().key()), "duplicate in gen2"));
        assertEquals(30, v2.size(), "gen2 has its own clean tick sequence 0..29");
        assertEquals("rules-v2", v2.get(0).rulesVersion());
        // 重启后所有裁决都属于新规则版本
        assertTrue(v2.stream().allMatch(v -> v.rulesVersion().equals("rules-v2")));
        LOG.info(() -> "gen2 ran under " + v2.get(0).rulesVersion()
                + " with " + v2.size() + " verdicts; lifecycle counters preserved: "
                + gw.stats().accepted() + " accepted total");
    }

    private void waitTicks(SamplingGateway gw, long until) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            Set<Long> ticks = new HashSet<>();
            gw.verdictLog().bySensor("s").forEach(v -> ticks.add(v.reading().tick()));
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
        throw new AssertionError("timeout waiting ticks 0.." + until);
    }
}
