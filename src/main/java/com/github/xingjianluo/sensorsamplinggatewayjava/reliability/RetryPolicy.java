package com.github.xingjianluo.sensorsamplinggatewayjava.reliability;

import java.util.function.LongSupplier;

/**
 * 瞬时故障的有界重试与指数退避配置。
 *
 * <p>退避 = min(initialBackoff * multiplier^(attempt-1), maxBackoff)，
 * 抖动按确定的 LongSupplier 注入：测试可传固定值，生产可传随机数，
 * 从而既保证测试可复现又避免生产环境惊群。</p>
 *
 * @param maxAttempts       最大尝试总次数（含首次），&gt;=1
 * @param initialBackoffMs  首次失败后的基础退避
 * @param multiplier        指数因子
 * @param maxBackoffMs      退避上限
 * @param jitterRatio       抖动比例（0 表示无抖动），范围 [0,1)
 */
public record RetryPolicy(int maxAttempts, long initialBackoffMs,
                          double multiplier, long maxBackoffMs, double jitterRatio) {

    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        if (initialBackoffMs < 0 || maxBackoffMs < initialBackoffMs) {
            throw new IllegalArgumentException("invalid backoff bounds");
        }
        if (jitterRatio < 0 || jitterRatio >= 1) {
            throw new IllegalArgumentException("jitterRatio must be in [0,1)");
        }
    }

    /**
     * 计算第 attempt 次失败后的退避毫秒数（attempt 从 1 开始）。
     *
     * @param jitterUnitFraction 抖动注入：返回 [0,1) 的小数，决定抖动方向与幅度
     */
    public long backoffMillis(int attempt, LongSupplier jitterUnitFraction) {
        double base = initialBackoffMs * Math.pow(multiplier, attempt - 1);
        long capped = (long) Math.min(base, (double) maxBackoffMs);
        if (jitterRatio == 0.0d) {
            return capped;
        }
        double f = jitterUnitFraction.getAsLong() / (double) Long.MAX_VALUE;
        double factor = 1.0d - jitterRatio + 2.0d * jitterRatio * f;
        return (long) Math.max(0, capped * factor);
    }
}
