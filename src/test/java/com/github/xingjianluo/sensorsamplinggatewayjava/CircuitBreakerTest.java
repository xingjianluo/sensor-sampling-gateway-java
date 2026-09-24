package com.github.xingjianluo.sensorsamplinggatewayjava;

import com.github.xingjianluo.sensorsamplinggatewayjava.reliability.CircuitBreaker;
import org.junit.jupiter.api.Test;

import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 断路器：阈值内放行，达到阈值隔离；冷却后仅放行一次探测，成功恢复、失败再隔离。 */
class CircuitBreakerTest {

    private static final Logger LOG = Logger.getLogger(CircuitBreakerTest.class.getName());

    @Test
    void opensAfterThresholdAndAllowsSingleProbeAfterCooldown() {
        CircuitBreaker cb = new CircuitBreaker("s", 3, 100L);
        long t = 0L;
        assertTrue(cb.allowRequest(t));
        cb.recordFailure(t);
        cb.recordFailure(t);
        assertTrue(cb.allowRequest(t), "below threshold still allowed");
        cb.recordFailure(t);
        assertFalse(cb.allowRequest(t), "threshold reached -> isolated OPEN");
        assertFalse(cb.allowRequest(t + 99L), "still within cooldown");

        // 冷却期满：恰好一个探测被放行到 HALF_OPEN
        assertTrue(cb.allowRequest(t + 100L), "cooldown elapsed -> one probe allowed");
        LOG.info("probe allowed after cooldown, state=" + cb.state(t + 100L));

        // 探测失败 -> 立即重新 OPEN，其他并发探测不会被放行
        cb.recordFailure(t + 100L);
        assertFalse(cb.allowRequest(t + 100L), "probe failed -> back to OPEN");

        // 下一轮冷却，探测成功 -> CLOSED 恢复
        assertTrue(cb.allowRequest(t + 300L));
        cb.recordSuccess();
        assertTrue(cb.allowRequest(t + 300L), "successful probe -> CLOSED");
    }
}
