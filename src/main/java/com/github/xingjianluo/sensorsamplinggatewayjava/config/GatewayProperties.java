package com.github.xingjianluo.sensorsamplinggatewayjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 网关级可配置参数（缓冲容量、溢出策略、默认重试策略）。
 */
@ConfigurationProperties(prefix = "gateway")
public class GatewayProperties {

    private int bufferCapacity = 16;
    private OverflowPolicy overflowPolicy = OverflowPolicy.BLOCK;
    private RetryProperties retry = new RetryProperties();

    public int getBufferCapacity() {
        return bufferCapacity;
    }

    public void setBufferCapacity(int bufferCapacity) {
        this.bufferCapacity = bufferCapacity;
    }

    public OverflowPolicy getOverflowPolicy() {
        return overflowPolicy;
    }

    public void setOverflowPolicy(OverflowPolicy overflowPolicy) {
        this.overflowPolicy = overflowPolicy;
    }

    public RetryProperties getRetry() {
        return retry;
    }

    public void setRetry(RetryProperties retry) {
        this.retry = retry;
    }

    /** 重试策略的可绑定属性载体。 */
    public static class RetryProperties {
        private int maxAttempts = 3;
        private java.time.Duration initialBackoff = java.time.Duration.ofMillis(10);
        private java.time.Duration maxBackoff = java.time.Duration.ofMillis(100);
        private double backoffMultiplier = 2.0;
        private int failureThreshold = 3;
        private java.time.Duration probeInterval = java.time.Duration.ofMillis(200);

        public int getMaxAttempts() {
            return maxAttempts;
        }

        public void setMaxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
        }

        public java.time.Duration getInitialBackoff() {
            return initialBackoff;
        }

        public void setInitialBackoff(java.time.Duration initialBackoff) {
            this.initialBackoff = initialBackoff;
        }

        public java.time.Duration getMaxBackoff() {
            return maxBackoff;
        }

        public void setMaxBackoff(java.time.Duration maxBackoff) {
            this.maxBackoff = maxBackoff;
        }

        public double getBackoffMultiplier() {
            return backoffMultiplier;
        }

        public void setBackoffMultiplier(double backoffMultiplier) {
            this.backoffMultiplier = backoffMultiplier;
        }

        public int getFailureThreshold() {
            return failureThreshold;
        }

        public void setFailureThreshold(int failureThreshold) {
            this.failureThreshold = failureThreshold;
        }

        public java.time.Duration getProbeInterval() {
            return probeInterval;
        }

        public void setProbeInterval(java.time.Duration probeInterval) {
            this.probeInterval = probeInterval;
        }

        public RetryPolicy toRetryPolicy() {
            return new RetryPolicy(
                    maxAttempts,
                    initialBackoff,
                    maxBackoff,
                    backoffMultiplier,
                    failureThreshold,
                    probeInterval
            );
        }
    }
}
