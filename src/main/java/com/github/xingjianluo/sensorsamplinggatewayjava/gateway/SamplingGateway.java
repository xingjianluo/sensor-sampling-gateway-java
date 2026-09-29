package com.github.xingjianluo.sensorsamplinggatewayjava.gateway;

import com.github.xingjianluo.sensorsamplinggatewayjava.buffer.BoundedBuffer;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.GatewayStats;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.OverflowPolicy;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.RawReading;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.RejectReason;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorConfig;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.Verdict;
import com.github.xingjianluo.sensorsamplinggatewayjava.pipeline.ReadingValidator;
import com.github.xingjianluo.sensorsamplinggatewayjava.pipeline.VerdictLog;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.BackpressureKind;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.ReconciliationStore;
import com.github.xingjianluo.sensorsamplinggatewayjava.reliability.CircuitBreaker;
import com.github.xingjianluo.sensorsamplinggatewayjava.reliability.RetryPolicy;
import com.github.xingjianluo.sensorsamplinggatewayjava.reliability.Sleeper;
import com.github.xingjianluo.sensorsamplinggatewayjava.sensor.SamplingLimitReached;
import com.github.xingjianluo.sensorsamplinggatewayjava.sensor.Sensor;
import com.github.xingjianluo.sensorsamplinggatewayjava.sensor.SensorFaultException;
import com.github.xingjianluo.sensorsamplinggatewayjava.time.Clock;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 采样网关核心。
 *
 * <p>线程模型：每个启用传感器一个生产者线程（周期采样→断路器→有界重试退避→入队）；
 * {@code consumerThreads} 个消费者线程（出队→校验→校准→裁决→记录）。</p>
 *
 * <p>一致性保证：每个采样尝试只消费一个单调 {@code tick}，最终恰好形成一条
 * {@code sensor#tick} 唯一的 {@link Verdict}（接受或带可区分原因的拒绝）。
 * tick 由生产者线程内的计数器单调分配，因此不重不漏。</p>
 */
public final class SamplingGateway {

    private static final Logger LOG = Logger.getLogger(SamplingGateway.class.getName());

    private volatile GatewayConfig config;
    private volatile Map<String, Sensor> sensors;
    private final Clock clock;
    private final Sleeper sleeper;
    private final java.util.function.Function<GatewayConfig, Map<String, Sensor>> sensorFactory;
    private final StatsCollector stats;
    private volatile VerdictLog verdictLog;
    private final ReadingValidator validator;
    private final ReconciliationStore reconciliation;

    private BoundedBuffer buffer;
    private final Map<String, CircuitBreaker> breakers = new ConcurrentHashMap<>();

    private ExecutorService producerPool;
    private ExecutorService consumerPool;
    private final AtomicBoolean running = new AtomicBoolean(false);
    /** 测试用：请求生产者在当前拍完成后停止，但不关闭缓冲（随后再 stop 排空）。 */
    private final AtomicBoolean stopRequested = new AtomicBoolean(false);

    public SamplingGateway(GatewayConfig config, Map<String, Sensor> sensors,
                           Clock clock, Sleeper sleeper) {
        this(config, sensors, clock, sleeper, new StatsCollector());
    }

    public SamplingGateway(GatewayConfig config, Map<String, Sensor> sensors,
                           Clock clock, Sleeper sleeper, StatsCollector stats) {
        this(config, ignored -> Map.copyOf(sensors), clock, sleeper, stats);
    }

    public SamplingGateway(GatewayConfig config,
                           java.util.function.Function<GatewayConfig, Map<String, Sensor>> sensorFactory,
                           Clock clock, Sleeper sleeper) {
        this(config, sensorFactory, clock, sleeper, new StatsCollector());
    }

    private SamplingGateway(GatewayConfig config,
                            java.util.function.Function<GatewayConfig, Map<String, Sensor>> sensorFactory,
                            Clock clock, Sleeper sleeper, StatsCollector stats) {
        this.config = config;
        this.sensorFactory = sensorFactory;
        this.sensors = sensorFactory.apply(config);
        this.clock = clock;
        this.sleeper = sleeper;
        this.stats = stats;
        this.verdictLog = new VerdictLog(config.verdictLogCapacity());
        this.validator = new ReadingValidator(clock);
        this.reconciliation = new ReconciliationStore(
                config.reconcileDetailCapacity(),
                config.reconcileMaxSealedGenerations(),
                config.reconcileRetentionPolicy());
        this.buffer = new BoundedBuffer(config.bufferCapacity(), config.overflowPolicy());
        for (SensorConfig sc : config.sensors()) {
            breakers.put(sc.getId(),
                    new CircuitBreaker(sc.getId(), config.breakerThreshold(), config.breakerCooldownMs()));
        }
    }

    public synchronized void start() {
        if (running.get()) {
            throw new IllegalStateException("gateway already running");
        }
        running.set(true);
        stopRequested.set(false);
        reconciliation.beginGeneration(config.rulesVersion(), clock.nowMillis());
        bufferPoolsUp();
        LOG.info(() -> "SamplingGateway started: rulesVersion=" + config.rulesVersion()
                + ", buffer=" + config.bufferCapacity() + "/" + config.overflowPolicy()
                + ", consumers=" + config.consumerThreads());
    }

    private void bufferPoolsUp() {
        int producers = (int) config.sensors().stream().filter(SensorConfig::isEnabled).count();
        producerPool = Executors.newFixedThreadPool(Math.max(1, producers),
                r -> {
                    Thread t = new Thread(r, "sensor-producer");
                    t.setDaemon(true);
                    return t;
                });
        consumerPool = Executors.newFixedThreadPool(config.consumerThreads(),
                r -> {
                    Thread t = new Thread(r, "sensor-consumer");
                    t.setDaemon(true);
                    return t;
                });
        for (int i = 0; i < config.consumerThreads(); i++) {
            consumerPool.submit(this::consumeLoop);
        }
        for (SensorConfig sc : config.sensors()) {
            if (sc.isEnabled()) {
                Sensor sensor = sensors.get(sc.getId());
                if (sensor != null) {
                    producerPool.submit(() -> produceLoop(sensor));
                }
            }
        }
        producerPool.shutdown();
    }

    /** 生产者循环：周期节拍 + 断路器 + 有界重试退避 + 入队。 */
    private void produceLoop(Sensor sensor) {
        String id = sensor.config().getId();
        long period = sensor.config().getPeriodMs();
        AtomicLong tick = new AtomicLong();
        CircuitBreaker breaker = breakers.get(id);
        while (running.get() && !stopRequested.get()) {
            long t = tick.getAndIncrement();
            try {
                if (!breaker.allowRequest(clock.nowMillis())) {
                    stats.submitted();
                    stats.rejected(RejectReason.SENSOR_ISOLATED);
                    record(Verdict.reject(
                            new RawReading(id, t, clock.nowMillis(), Double.NaN, Long.MIN_VALUE),
                            RejectReason.SENSOR_ISOLATED,
                            "input{tick=" + t + "}; rule=breaker OPEN after "
                                    + breaker.consecutiveFailures() + " consecutive failures -> SENSOR_ISOLATED",
                            config.rulesVersion()));
                    sleepPeriod(period);
                    continue;
                }
                RawReading reading = sampleWithRetry(sensor, t, breaker);
                if (reading == null) {
                    // 有界重试后仍失败：breakFailure 已记失败结论
                    sleepPeriod(period);
                    continue;
                }
                breaker.recordSuccess();
                if (!running.get()) {
                    // 关闭已开始：未提交读数不进入“已受理”集合，避免重/漏歧义
                    return;
                }
                stats.submitted();
                BoundedBuffer.OfferResult result = buffer.offer(reading);
                handleOfferResult(result);
            } catch (SamplingLimitReached stop) {
                // 协作式停止：该传感器声明采样结束，生产者干净退出，不产生裁决与计数
                LOG.fine(() -> "producer for " + id + " finished at tick " + t);
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Throwable unexpected) {
                LOG.log(Level.WARNING, "producer for " + id + " tick " + t + " failed unexpectedly", unexpected);
            }
            try {
                sleepPeriod(period);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /** 有界重试 + 指数退避；全部失败返回 null 并记录失败结论。 */
    private RawReading sampleWithRetry(Sensor sensor, long tick, CircuitBreaker breaker)
            throws InterruptedException {
        final RetryPolicy retry = config.retryPolicy();
        SensorFaultException lastFault = null;
        for (int attempt = 1; attempt <= retry.maxAttempts(); attempt++) {
            try {
                return sensor.sample(tick);
            } catch (SamplingLimitReached stop) {
                throw stop;
            } catch (SensorFaultException fault) {
                lastFault = fault;
                stats.acquisitionFailure();
                if (!fault.isRecoverable() || attempt == retry.maxAttempts()) {
                    break;
                }
                long backoff = retry.backoffMillis(attempt, () -> 0L);
                final int a = attempt;
                final long b = backoff;
                LOG.fine(() -> sensor.config().getId() + " tick=" + tick + " attempt=" + a
                        + " transient fault, backing off " + b + "ms");
                sleeper.sleep(backoff);
            } catch (Exception other) {
                lastFault = new SensorFaultException(sensor.config().getId(), tick,
                        other.getMessage() == null ? other.toString() : other.getMessage(), false);
                stats.acquisitionFailure();
                break;
            }
        }
        breaker.recordFailure(clock.nowMillis());
        stats.submitted();
        stats.rejected(RejectReason.ACQUISITION_FAILED);
        boolean isolatedNow = breaker.state(clock.nowMillis()) == CircuitBreaker.State.OPEN;
        record(Verdict.reject(
                new RawReading(sensor.config().getId(), tick, clock.nowMillis(), Double.NaN, Long.MIN_VALUE),
                RejectReason.ACQUISITION_FAILED,
                "input{tick=" + tick + ",fault=" + lastFault.getMessage()
                        + ",recoverable=" + lastFault.isRecoverable()
                        + ",attempts=" + retry.maxAttempts() + "}; rule=bounded retry exhausted"
                        + (isolatedNow ? ", breaker OPEN -> sensor isolated" : "")
                        + " -> ACQUISITION_FAILED",
                config.rulesVersion()));
        return null;
    }

    private void handleOfferResult(BoundedBuffer.OfferResult result) {
        if (result.enqueued()) {
            stats.enqueued();
        } else {
            // DROP_NEWEST 满队拒绝，或 BLOCK 在关闭后唤醒
            stats.rejected(RejectReason.BUFFER_OVERFLOW);
            RawReading r = result.rejectedReading();
            record(Verdict.reject(r, RejectReason.BUFFER_OVERFLOW,
                    "input{sensor=" + r.sensorId() + ",tick=" + r.tick()
                            + "}; rule=buffer(capacity=" + buffer.capacity() + ",policy=" + result.policy()
                            + ") rejected newest on full/closed -> BUFFER_OVERFLOW",
                    config.rulesVersion()), BackpressureKind.REJECTED_NEW);
        }
        if (result.evictedReading() != null) {
            // DROP_OLDEST：新读数已入队（已计 enqueued），被驱逐的旧读数单独形成一条可区分结论
            RawReading ev = result.evictedReading();
            // 被顶掉的旧读数同样是一次拒绝落账：统一走 rejected(BUFFER_OVERFLOW)，
            // 它同时使 dropped+1。最终 dropped = evicted + rejectedNew
            // = byReason[BUFFER_OVERFLOW]，且 submitted = accepted + rejected 严格成立，
            // 与对账台账（evictedOld / rejectedNew 分列）口径一致。
            stats.rejected(RejectReason.BUFFER_OVERFLOW);
            record(Verdict.reject(ev, RejectReason.BUFFER_OVERFLOW,
                    "input{sensor=" + ev.sensorId() + ",tick=" + ev.tick()
                            + "}; rule=buffer(capacity=" + buffer.capacity()
                            + ",policy=DROP_OLDEST) evicted oldest to make room -> BUFFER_OVERFLOW",
                    config.rulesVersion()), BackpressureKind.EVICTED_OLD);
        }
    }

    /** 消费者循环：排空优先，关闭且队列清空后退出。 */
    private void consumeLoop() {
        try {
            while (true) {
                RawReading reading = buffer.poll(200, TimeUnit.MILLISECONDS);
                if (reading == null) {
                    if (buffer.isClosed() && buffer.size() == 0) {
                        return;
                    }
                    continue;
                }
                process(reading);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void process(RawReading reading) {
        stats.consumed();
        SensorConfig sc = config.sensorConfig(reading.sensorId());
        RejectReason reason = sc == null ? null : validator.validate(reading, sc);
        if (reason != null) {
            stats.rejected(reason);
            record(Verdict.reject(reading, reason,
                    sc == null ? "sensor config missing" : validator.describe(reading, sc, reason),
                    config.rulesVersion()));
            return;
        }
        double calibrated = config.calibration().apply(reading.sensorId(), reading.value());
        stats.accepted();
        record(Verdict.accept(reading, calibrated,
                String.format("input{sensor=%s,tick=%d,value=%.4f}; rule=valid, calibrated=%.6f (model=%s)",
                        reading.sensorId(), reading.tick(), reading.value(),
                        calibrated, config.calibration().getVersion()),
                config.rulesVersion()));
    }

    private void record(Verdict verdict) {
        record(verdict, BackpressureKind.NONE);
    }

    private void record(Verdict verdict, BackpressureKind backpressure) {
        verdictLog.append(verdict);
        reconciliation.append(verdict, backpressure, clock.nowMillis());
        LOG.info(() -> "VERDICT " + (verdict.accepted() ? "ACCEPT" : "REJECT:" + verdict.reason())
                + " " + verdict.basis() + " [rules=" + verdict.rulesVersion() + "]");
    }

    private void sleepPeriod(long periodMs) throws InterruptedException {
        if (periodMs > 0) {
            sleeper.sleep(periodMs);
        }
    }

    public synchronized void stop() {
        if (!running.getAndSet(false)) {
            return;
        }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.shutdownDrainTimeoutMs());
        // 1) 停止受理：关闭缓冲，生产者阻塞/睡眠会在下次节拍退出
        buffer.close();
        // 2) 等待已受理数据被消费排空
        long remaining = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
        try {
            if (!awaitDrain(Math.max(0, remaining))) {
                LOG.warning("drain timeout before all enqueued readings were consumed");
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.warning("interrupted while draining enqueued readings");
        }
        // 3) 关闭线程池（此后不再有新裁决产生；优雅关闭要求排空在超时内完成，
        //    正常路径缓冲必为 0，见 LifecycleRestartTest）
        shutdownPool(consumerPool);
        shutdownPool(producerPool);
        // 4) 封代：冻结本批次账目。已入队但排空超时仍滞留的读数不产生裁决，
        //    会在对账中表现为缺口并触发闭合校验，属于需调大 shutdownDrainTimeoutMs 的告警情形。
        reconciliation.sealGeneration(clock.nowMillis());
        LOG.info("SamplingGateway stopped");
    }

    private void shutdownPool(ExecutorService pool) {
        if (pool == null) {
            return;
        }
        pool.shutdownNow();
        try {
            if (!pool.awaitTermination(2, TimeUnit.SECONDS)) {
                LOG.warning("executor did not terminate in time");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 以新配置快照与新传感器集合重启（例如重新创建模拟传感器、清空脚本游标）。
     * 先优雅停止并排空旧数据，再启动；生命周期计数保留，裁决日志按新一代重置。
     */
    public synchronized void restart(GatewayConfig newConfig, Map<String, Sensor> newSensors) {
        stop();
        restartInternal(newConfig, Map.copyOf(newSensors));
    }

    public synchronized void restart(GatewayConfig newConfig) {
        stop();
        restartInternal(newConfig, Map.copyOf(sensorFactory.apply(newConfig)));
    }

    private void restartInternal(GatewayConfig newConfig, Map<String, Sensor> newSensors) {
        this.config = newConfig;
        this.sensors = newSensors;
        this.breakers.clear();
        for (SensorConfig sc : newConfig.sensors()) {
            breakers.put(sc.getId(), new CircuitBreaker(
                    sc.getId(), newConfig.breakerThreshold(), newConfig.breakerCooldownMs()));
        }
        reinitBuffer(newConfig);
        this.verdictLog = new VerdictLog(newConfig.verdictLogCapacity());
        stopRequested.set(false);
        running.set(true);
        reconciliation.beginGeneration(newConfig.rulesVersion(), clock.nowMillis());
        bufferPoolsUp();
        LOG.info(() -> "SamplingGateway restarted with rulesVersion=" + newConfig.rulesVersion());
    }

    private void reinitBuffer(GatewayConfig cfg) {
        // field reassigned only while stopped
        this.buffer = new BoundedBuffer(cfg.bufferCapacity(), cfg.overflowPolicy());
    }

    /** 请求生产者停止节拍（不关闭缓冲）。测试用于精确控制采样拍数。 */
    public void requestStop() {
        stopRequested.set(true);
    }

    public boolean awaitDrain(long timeoutMs) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (buffer.size() > 0) {
            if (System.nanoTime() >= deadline) {
                return false;
            }
            Thread.sleep(5);
        }
        return true;
    }

    public boolean isRunning() {
        return running.get();
    }

    public GatewayStats stats() {
        List<String> isolated = new ArrayList<>();
        long now = clock.nowMillis();
        breakers.forEach((id, cb) -> {
            if (cb.state(now) == CircuitBreaker.State.OPEN) {
                isolated.add(id);
            }
        });
        return stats.snapshot(isolated);
    }

    public VerdictLog verdictLog() {
        return verdictLog;
    }

    public ReconciliationStore reconciliation() {
        return reconciliation;
    }

    public GatewayConfig config() {
        return config;
    }

    public BoundedBuffer bufferView() {
        return buffer;
    }

    public CircuitBreaker breaker(String sensorId) {
        return breakers.get(sensorId);
    }
}
