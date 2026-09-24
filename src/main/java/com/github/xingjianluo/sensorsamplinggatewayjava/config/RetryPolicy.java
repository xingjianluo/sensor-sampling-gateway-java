package com.github.xingjianluo.sensorsamplinggatewayjava.config;

import java.time.Duration;

/**
 * 瞬时故障的有界重试、指数退避以及持续失败后的隔离配置。
 *
 * @param maxAttempts       单个采样周期内最大尝试次数（含首次）
 * @param initialBackoff    首次重试前的退避时长
 * @param maxBackoff        退避上限
 * @param backoffMultiplier 退避倍率（每次失败后乘以此值）
 * @param failureThreshold  连续失败的采样轮次达到该次数后隔离传感器
 * @param probeInterval     隔离后尝试恢复探测的周期
 */
public record RetryPolicy(
        int maxAttempts,
        Duration initialBackoff,
        Duration maxBackoff,
        double backoffMultiplier,
        int failureThreshold,
        Duration probeInterval
) {

    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        if (failureThreshold < 1) {
            throw new IllegalArgumentException("failureThreshold must be >= 1");
        }
        if (backoffMultiplier < 1.0) {
            throw new IllegalArgumentException("backoffMultiplier must be >= 1.0");
        }
    }

    /** 不重试、失败一次即隔离的策略，供测试使用。 */
    public static RetryPolicy noRetry() {
        return new RetryPolicy(1, Duration.ZERO, Duration.ZERO, 1.0, 1, Duration.ofMillis(50));
    }

    /**
     * 计算第 retryNumber 次重试（从 1 开始）前的退避时长：
     * initial * multiplier^(retryNumber-1)，并以 maxBackoff 封顶。
     */
    public Duration backoffFor(int retryNumber) {
        if (retryNumber < 1) {
            return Duration.ZERO;
        }
        long base = initialBackoff.toMillis();
        long capped = maxBackoff.toMillis();
        double factor = Math.pow(backoffMultiplier, retryNumber - 1);
        long backoff = (long) Math.min(capped, base * factor);
        return Duration.ofMillis(backoff);
    }
}
