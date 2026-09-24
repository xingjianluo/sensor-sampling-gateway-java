package com.github.xingjianluo.sensorsamplinggatewayjava.tests;

import com.github.xingjianluo.sensorsamplinggatewayjava.config.RetryPolicy;
import com.github.xingjianluo.sensorsamplinggatewayjava.fault.FaultDecision;
import com.github.xingjianluo.sensorsamplinggatewayjava.fault.FaultTracker;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 故障状态机单测：使用固定时钟验证指数退避、连续失败隔离、
 * 隔离后的恢复探测放行与成功后的完全复位，行为确定可复现。
 */
class FaultTrackerTest {

    private RetryPolicy policy() {
        // 最多 3 次尝试；初始退避 10ms，倍率 2，上限 100ms；连续 3 轮失败隔离；探测周期 200ms
        return new RetryPolicy(3, Duration.ofMillis(10), Duration.ofMillis(100),
                2.0, 3, Duration.ofMillis(200));
    }

    @Test
    void exponentialBackoffIsBounded() {
        FaultTracker t = new FaultTracker("s", policy());
        assertEquals(10, t.nextBackoff(1).toMillis());
        assertEquals(20, t.nextBackoff(2).toMillis());
        assertEquals(40, t.nextBackoff(3).toMillis());
        assertEquals(80, t.nextBackoff(4).toMillis());
        assertEquals(100, t.nextBackoff(5).toMillis(), "退避必须封顶");
        System.out.println("[backoff] retry1..5 = 10,20,40,80,100 ms (capped)");
    }

    @Test
    void transientFailuresBackoffThenRecover() {
        FaultTracker t = new FaultTracker("s", policy());
        long now = 1000;
        assertEquals(FaultDecision.PROBE, t.decide(now), "初始应正常探测");

        t.recordFailure(false, now); // 第 1 轮失败 -> 退避到 1010
        System.out.printf("[transient] after fail#1 failures=%d isolated=%s waitUntil=%d%n",
                t.consecutiveFailures(), t.isIsolated(), t.waitUntilMs());
        assertFalse(t.isIsolated());
        assertEquals(FaultDecision.SKIP, t.decide(1005));
        assertEquals(FaultDecision.PROBE, t.decide(1010), "跨轮退避结束后应重新放行");

        t.recordSuccess();
        assertEquals(0, t.consecutiveFailures());
        assertEquals(FaultDecision.PROBE, t.decide(1011));
        System.out.println("[transient] recordSuccess -> failures reset to 0, PROBE");
    }

    @Test
    void isolatesAfterThresholdAndOpensRecoveryProbe() {
        FaultTracker t = new FaultTracker("s", policy());
        long now = 0;
        t.recordFailure(false, now);
        t.recordFailure(false, now + 100);
        assertFalse(t.isIsolated(), "2 轮失败尚未达到阈值");
        t.recordFailure(false, now + 300); // 第 3 轮 -> 隔离，下次探测 500
        assertTrue(t.isIsolated(), "连续 3 轮失败必须隔离");
        System.out.printf("[isolate] failures=%d isolated=%s probeAt=%d%n",
                t.consecutiveFailures(), t.isIsolated(), t.waitUntilMs());

        assertEquals(FaultDecision.ISOLATED, t.decide(499));
        assertEquals(FaultDecision.PROBE, t.decide(500), "探测周期到期应放行一次恢复探测");

        t.recordSuccess();
        assertFalse(t.isIsolated(), "恢复探测成功后应解除隔离");
        assertEquals(0, t.consecutiveFailures());
        System.out.println("[isolate] probe succeeded -> isolated=false, failures=0");
    }

    @Test
    void permanentFailureIsolatesImmediately() {
        FaultTracker t = new FaultTracker("s", policy());
        t.recordFailure(true, 0);
        assertTrue(t.isIsolated(), "持续故障必须立即隔离，不走重试");
        System.out.println("[permanent] single permanent fault -> isolated immediately");
    }
}
