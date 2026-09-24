package com.github.xingjianluo.sensorsamplinggatewayjava.engine;

import com.github.xingjianluo.sensorsamplinggatewayjava.buffer.BoundedSamplingBuffer;
import com.github.xingjianluo.sensorsamplinggatewayjava.buffer.OfferResult;
import com.github.xingjianluo.sensorsamplinggatewayjava.config.OverflowPolicy;
import com.github.xingjianluo.sensorsamplinggatewayjava.config.RetryPolicy;
import com.github.xingjianluo.sensorsamplinggatewayjava.fault.FaultDecision;
import com.github.xingjianluo.sensorsamplinggatewayjava.fault.FaultTracker;
import com.github.xingjianluo.sensorsamplinggatewayjava.gateway.OutcomeSink;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.RawReading;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.RejectReason;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SampleOutcome;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorSpec;
import com.github.xingjianluo.sensorsamplinggatewayjava.registry.SensorRegistry;
import com.github.xingjianluo.sensorsamplinggatewayjava.sensor.SensorDriver;
import com.github.xingjianluo.sensorsamplinggatewayjava.sensor.SensorFaultException;
import com.github.xingjianluo.sensorsamplinggatewayjava.stats.GatewayStats;
import com.github.xingjianluo.sensorsamplinggatewayjava.validation.ReadingValidator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

/**
 * 并发采样引擎。
 *
 * <p>生产者：每个传感器一个周期任务，按各自 period 调度；故障传感器由 {@link FaultTracker}
 * 决策为跳过/探测。产出读数进入有界缓冲，溢出行为确定且计数可观测。
 * <p>消费者：单线程串行做校验/校准/交付。单消费者使每个原始读数有唯一归属处理点，
 * 从结构上杜绝重复计数、丢失计数与数据竞争。
 * <p>关闭：先停止调度并等待所有在途生产者结束（不再有新读数入队），再让消费者把缓冲排空，
 * 因此所有“已受理（成功入缓冲）”的读数恰好被交付一次，不重不漏。之后可再次 {@link #start()}。
 */
public class SamplingEngine {

    private final SensorRegistry registry;
    private final Map<String, SensorDriver> drivers;
    private final Map<String, FaultTracker> trackers;
    private final BoundedSamplingBuffer buffer;
    private final ReadingValidator validator;
    private final OutcomeSink sink;
    private final GatewayStats stats;
    private final RetryPolicy retryPolicy;
    private final LongSupplier clock;
    private final OverflowPolicy overflowPolicy;

    private ScheduledExecutorService producerPool;
    private ExecutorService consumerPool;
    private Future<?> consumerTask;
    private final Map<String, Future<?>> producerTasks = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);

    public SamplingEngine(SensorRegistry registry,
                          Map<String, SensorDriver> drivers,
                          BoundedSamplingBuffer buffer,
                          ReadingValidator validator,
                          OutcomeSink sink,
                          GatewayStats stats,
                          RetryPolicy retryPolicy,
                          LongSupplier clock) {
        this.registry = registry;
        this.drivers = new ConcurrentHashMap<>(drivers);
        this.buffer = buffer;
        this.validator = validator;
        this.sink = sink;
        this.stats = stats;
        this.retryPolicy = retryPolicy;
        this.clock = clock;
        this.overflowPolicy = buffer.overflowPolicy();
        this.trackers = new ConcurrentHashMap<>();
        this.drivers.keySet().forEach(id -> trackers.put(id, new FaultTracker(id, retryPolicy)));
    }

    /** 启动所有生产者与消费者。 */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        Map<String, SensorSpec> sensors = registry.allCurrent();
        producerPool = Executors.newScheduledThreadPool(Math.max(1, sensors.size()),
                r -> {
                    Thread t = new Thread(r, "sampling-producer");
                    t.setDaemon(true);
                    return t;
                });
        consumerPool = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "sampling-consumer");
            t.setDaemon(true);
            return t;
        });
        consumerTask = consumerPool.submit(this::consumeLoop);
        scheduleAll(sensors);
    }

    private void scheduleAll(Map<String, SensorSpec> sensors) {
        sensors.forEach((id, spec) -> {
            ScheduledExecutorService pool = producerPool;
            Future<?> task = pool.scheduleAtFixedRate(
                    () -> safeProduce(id), 0L, spec.periodMs(), TimeUnit.MILLISECONDS);
            producerTasks.put(id, task);
        });
    }

    private void safeProduce(String sensorId) {
        try {
            produceOnce(sensorId);
        } catch (Throwable t) {
            // 生产者异常绝不允许杀死调度线程。
            System.err.printf("[producer] sensor=%s unexpected error=%s%n", sensorId, t);
        }
    }

    /** 单个传感器一次采样轮次：故障决策 → 有界重试读取 → 入缓冲（确定溢出处理）。 */
    void produceOnce(String sensorId) {
        SensorSpec spec = registry.current(sensorId).orElse(null);
        SensorDriver driver = drivers.get(sensorId);
        FaultTracker tracker = trackers.computeIfAbsent(sensorId,
                k -> new FaultTracker(k, retryPolicy));
        if (spec == null || driver == null) {
            return;
        }
        long now = clock.getAsLong();
        FaultDecision decision = tracker.decide(now);
        if (decision == FaultDecision.SKIP || decision == FaultDecision.ISOLATED) {
            return;
        }

        RawReading reading = readWithRetry(driver, tracker);
        if (reading == null) {
            return; // 轮次失败已由 readWithRetry 记账并更新故障状态
        }
        tracker.recordSuccess();
        stats.incrementProduced();
        enqueue(reading);
    }

    private RawReading readWithRetry(SensorDriver driver, FaultTracker tracker) {
        int attempt = 0;
        while (true) {
            attempt++;
            try {
                return driver.read(clock.getAsLong());
            } catch (SensorFaultException e) {
                if (!e.isTransient()) {
                    tracker.recordFailure(true, clock.getAsLong());
                    return null;
                }
                if (attempt >= retryPolicy.maxAttempts()) {
                    tracker.recordFailure(false, clock.getAsLong());
                    return null;
                }
                stats.incrementRetries();
                sleep(retryPolicy.backoffFor(attempt).toMillis());
            }
        }
    }

    private void enqueue(RawReading reading) {
        try {
            if (overflowPolicy == OverflowPolicy.BLOCK) {
                buffer.put(reading);
            } else {
                OfferResult result = buffer.offer(reading);
                switch (result.status()) {
                    case ENQUEUED -> { }
                    case REJECTED_FULL -> rejectWithoutReading(reading, RejectReason.BUFFER_OVERFLOW,
                            String.format("BUFFER_OVERFLOW sensor=%s seq=%d bufferFull capacity=%d policy=%s",
                                    reading.sensorId(), reading.seq(), buffer.capacity(), overflowPolicy));
                    case ENQUEUED_WITH_EVICTION -> rejectWithoutReading(result.evicted(),
                            RejectReason.BUFFER_OVERFLOW,
                            String.format("BUFFER_OVERFLOW sensor=%s seq=%d evictedOldest capacity=%d policy=%s",
                                    result.evicted().sensorId(), result.evicted().seq(),
                                    buffer.capacity(), overflowPolicy));
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // 关闭期间被中断：尽力非阻塞入队，若仍失败则显式拒绝，保证读数有明确去向。
            OfferResult fallback = buffer.offer(reading);
            if (fallback.status() == OfferResult.Status.REJECTED_FULL) {
                rejectWithoutReading(reading, RejectReason.BUFFER_OVERFLOW,
                        "BUFFER_OVERFLOW on shutdown " + reading.sensorId() + " seq=" + reading.seq());
            }
        }
    }

    /**
     * 被溢出策略丢弃的读数同样形成一条可区分、可解释的拒绝结论并计入统计，
     * 不经过缓冲，直接交付（此时读数未损坏也未必超量程，仅因缓冲容量被拒绝）。
     */
    private void rejectWithoutReading(RawReading reading, RejectReason reason, String basis) {
        SampleOutcome.Rejected rejected = new SampleOutcome.Rejected(reading, reason, basis);
        stats.incrementRejected(reason);
        sink.deliver(rejected);
    }

    private void consumeLoop() {
        try {
            while (true) {
                RawReading reading = buffer.poll(10, TimeUnit.MILLISECONDS);
                if (reading != null) {
                    process(reading);
                    continue;
                }
                // 只有在“已停止且生产者线程池已终止（不会再有新读数）且缓冲为空”时才退出，
                // 排空期间继续处理，杜绝提前退出造成的遗漏。
                if (!running.get() && producerPoolTerminated() && buffer.size() == 0) {
                    break;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private boolean producerPoolTerminated() {
        ExecutorService pool = producerPool;
        return pool != null && pool.isTerminated();
    }

    /** 单个读数的唯一处理点：按当前规则版本校验/校准/交付/记账。 */
    private void process(RawReading reading) {
        SensorSpec spec = registry.current(reading.sensorId()).orElse(null);
        if (spec == null) {
            SampleOutcome rejected = new SampleOutcome.Rejected(reading, RejectReason.SENSOR_ISOLATED,
                    "SENSOR_ISOLATED sensor=" + reading.sensorId() + " seq=" + reading.seq()
                            + " reason=spec-missing");
            stats.incrementRejected(RejectReason.SENSOR_ISOLATED);
            sink.deliver(rejected);
            return;
        }
        SampleOutcome outcome = validator.validate(reading, spec, clock.getAsLong());
        if (outcome instanceof SampleOutcome.Accepted) {
            stats.incrementAccepted();
        } else if (outcome instanceof SampleOutcome.Rejected r) {
            stats.incrementRejected(r.reason());
        }
        sink.deliver(outcome);
    }

    public boolean isRunning() {
        return running.get();
    }

    /** 优雅关闭：停止生产、等待在途生产者结束、排空缓冲、关闭消费者。 */
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        producerTasks.values().forEach(f -> f.cancel(false));
        producerTasks.clear();
        producerPool.shutdown();
        try {
            // 等待已经开始执行的采样轮次（含退避重试）结束，确保不再有读数入队。
            if (!producerPool.awaitTermination(10, TimeUnit.SECONDS)) {
                producerPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            producerPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
        try {
            if (consumerTask != null) {
                consumerTask.get(10, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            if (consumerTask != null) {
                consumerTask.cancel(true);
            }
        }
        consumerPool.shutdown();
    }

    /** 关闭并重新启动。故障跟踪状态保留（隔离不因重启而被悄悄解除）。 */
    public void restart() {
        stop();
        start();
    }

    public Map<String, SensorSpec> registeredSensors() {
        return registry.allCurrent();
    }

    public FaultTracker tracker(String sensorId) {
        return trackers.get(sensorId);
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
