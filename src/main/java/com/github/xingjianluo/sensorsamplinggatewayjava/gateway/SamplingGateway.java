package com.github.xingjianluo.sensorsamplinggatewayjava.gateway;

import com.github.xingjianluo.sensorsamplinggatewayjava.buffer.BoundedBuffer;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.GatewayStats;
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
 * {@code sensor#tick} 唯一的落账（接受 / 拒绝 / 被顶掉 / 关闭未完成 四类之一）。
 * tick 由生产者线程内的计数器单调分配，台账对同代 {@code sensor#tick} 幂等去重，
 * 因此并发、缓冲压力与故障恢复并存时也不重不漏。</p>
 *
 * <p><b>运行中切换规则版本</b>（{@link #switchRulesVersion(GatewayConfig)}）：不重启、
 * 不停采样、不开新批次；在当前代次内开启一个规则版本“阶段”。读数在入队受理时于缓冲
 * 锁内盖上当时版本的章，故切换前已受理（在缓冲中或已被顶掉）的读数按原版本完成，
 * 切换后新受理的读数按新版本判定；两个版本在同代内各自可筛选，总账仍闭合。</p>
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
    /**
     * 本次运行（当前代次）内每个规则版本对应的规则配置快照。读数在受理时盖上版本章，
     * 判定时按章取这里的配置，因此 BLOCK 在途阻塞期间发生切换也不会出现“旧章配新规则”。
     * 每次 start/restart 重置（仅覆盖当前代次）。
     */
    private final Map<String, GatewayConfig> configsByRulesVersion = new ConcurrentHashMap<>();

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
        this.configsByRulesVersion.put(config.rulesVersion(), config);
        this.verdictLog = new VerdictLog(config.verdictLogCapacity());
        this.validator = new ReadingValidator(clock);
        this.reconciliation = new ReconciliationStore(
                config.reconcileDetailCapacity(),
                config.reconcileMaxSealedGenerations(),
                config.reconcileRetentionPolicy());
        this.buffer = newBuffer(config);
        for (SensorConfig sc : config.sensors()) {
            breakers.put(sc.getId(),
                    new CircuitBreaker(sc.getId(), config.breakerThreshold(), config.breakerCooldownMs()));
        }
    }

    private BoundedBuffer newBuffer(GatewayConfig cfg) {
        // 受理盖章在缓冲锁内执行：读取当前生效配置的规则版本，与版本切换严格线性化
        return new BoundedBuffer(cfg.bufferCapacity(), cfg.overflowPolicy(),
                r -> this.config.rulesVersion());
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
            if (result.evicted() != null) {
                // DROP_OLDEST：新读数已入队（已计 enqueued）；被顶掉的旧读数保持老口径：
                // 只计背压 evictedOld（dropped 的一部分），不抬高 rejected/拒绝原因计数。
                BoundedBuffer.Staged evStaged = result.evicted();
                stats.evictedOldest();
                RawReading ev = evStaged.reading();
                record(Verdict.reject(ev, RejectReason.BUFFER_OVERFLOW,
                        "input{sensor=" + ev.sensorId() + ",tick=" + ev.tick()
                                + "}; rule=buffer(capacity=" + buffer.capacity()
                                + ",policy=DROP_OLDEST) evicted oldest to make room -> EVICTED_OLD",
                        evStaged.rulesVersion()), BackpressureKind.EVICTED_OLD);
            }
        } else {
            // DROP_NEWEST 满队拒绝，或关闭期 BLOCK/DROP_OLDEST 唤醒：直接拒绝最新读数（第二类背压）
            stats.rejected(RejectReason.BUFFER_OVERFLOW);
            RawReading r = result.rejectedReading();
            String rules = result.rejectedRulesVersion();
            if (rules == null) {
                rules = config.rulesVersion();
            }
            record(Verdict.reject(r, RejectReason.BUFFER_OVERFLOW,
                    "input{sensor=" + r.sensorId() + ",tick=" + r.tick()
                            + "}; rule=buffer(capacity=" + buffer.capacity() + ",policy=" + result.policy()
                            + ") rejected newest on full/closed -> BUFFER_OVERFLOW",
                    rules), BackpressureKind.REJECTED_NEW);
        }
    }

    /** 消费者循环：排空优先，关闭且队列清空后退出。 */
    private void consumeLoop() {
        try {
            while (true) {
                BoundedBuffer.Staged staged = buffer.poll(200, TimeUnit.MILLISECONDS);
                if (staged == null) {
                    if (buffer.isClosed() && buffer.size() == 0) {
                        return;
                    }
                    continue;
                }
                process(staged);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void process(BoundedBuffer.Staged staged) {
        stats.consumed();
        RawReading reading = staged.reading();
        // 用该读数受理时盖章的规则版本对应的配置来判定（运行中切换不影响在途读数；
        // BLOCK 阻塞期间发生切换也按盖章版本取配置，杜绝“旧章配新规则”）
        GatewayConfig rulesConfig = configsByRulesVersion.getOrDefault(staged.rulesVersion(), config);
        SensorConfig sc = rulesConfig.sensorConfig(reading.sensorId());
        RejectReason reason;
        String basis;
        try {
            reason = sc == null ? null : validator.validate(reading, sc);
            if (reason != null) {
                stats.rejected(reason);
                basis = sc == null
                        ? "sensor config missing"
                        : validator.describe(reading, sc, reason);
                record(Verdict.reject(reading, reason, basis, staged.rulesVersion()));
                return;
            }
            double calibrated = rulesConfig.calibration().apply(reading.sensorId(), reading.value());
            stats.accepted();
            record(Verdict.accept(reading, calibrated,
                    String.format("input{sensor=%s,tick=%d,value=%.4f}; rule=valid, calibrated=%.6f (model=%s)",
                            reading.sensorId(), reading.tick(), reading.value(),
                            calibrated, rulesConfig.calibration().getVersion()),
                    staged.rulesVersion()));
        } catch (Throwable failure) {
            // 通常只在关闭打断“卡住的消费者”时发生：读数已提交却无法形成裁决。
            // 绝不让它消失——落为可区分的关闭未完成记录，保证生产总数与对账总数闭合。
            recordIncomplete(staged, failure);
        }
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

    private void recordIncomplete(BoundedBuffer.Staged staged, Throwable cause) {
        stats.incompleteAtShutdown();
        String note = cause == null ? "" : "; cause=" + cause;
        Verdict verdict = Verdict.reject(staged.reading(), RejectReason.SHUTDOWN_INCOMPLETE,
                "input{sensor=" + staged.reading().sensorId() + ",tick=" + staged.reading().tick()
                        + "}; rule=shutdown boundary, consumer interrupted before verdict" + note
                        + " -> SHUTDOWN_INCOMPLETE",
                staged.rulesVersion());
        verdictLog.append(verdict);
        reconciliation.appendIncomplete(staged.rulesVersion(), staged.reading(), clock.nowMillis());
        LOG.warning(() -> "SHUTDOWN_INCOMPLETE " + verdict.reading().key()
                + " [rules=" + staged.rulesVersion() + "]" + note);
    }

    private void sleepPeriod(long periodMs) throws InterruptedException {
        if (periodMs > 0) {
            sleeper.sleep(periodMs);
        }
    }

    /**
     * 运行中切换规则版本：不停止采样、不重建缓冲/线程/断路器、不开新批次（代次不变）。
     * 只原子替换规则相关的配置快照（规则版本、校准、传感器量程/容差、重试策略、
     * 断路器阈值/冷却参数随新快照生效；既有断路器隔离状态延续）。切换前已受理的读数
     * 带着旧版本标签留在缓冲中并按旧版本判定，切换后新受理的读数按新版本判定。
     *
     * @throws IllegalStateException 网关未运行，或新快照改变了缓冲/线程等需重启的结构参数
     */
    public synchronized void switchRulesVersion(GatewayConfig newConfig) {
        if (!running.get()) {
            throw new IllegalStateException("gateway not running");
        }
        requireHotCompatible(config, newConfig);
        this.config = newConfig;
        this.configsByRulesVersion.put(newConfig.rulesVersion(), newConfig);
        reconciliation.noteRulesVersion(newConfig.rulesVersion());
        LOG.info(() -> "rules version switched at runtime: "
                + "generation=" + reconciliation.runningGeneration()
                + ", " + config.rulesVersion() + " -> " + newConfig.rulesVersion());
    }

    private static void requireHotCompatible(GatewayConfig oldCfg, GatewayConfig newCfg) {
        if (newCfg.bufferCapacity() != oldCfg.bufferCapacity()
                || newCfg.overflowPolicy() != oldCfg.overflowPolicy()
                || newCfg.consumerThreads() != oldCfg.consumerThreads()
                || newCfg.shutdownDrainTimeoutMs() != oldCfg.shutdownDrainTimeoutMs()) {
            throw new IllegalArgumentException(
                    "buffer/thread/drain structure cannot change at runtime; use restart() for that");
        }
    }

    /**
     * 便捷重载：仅切换规则版本标签（校准等规则内容不变，用于演示“同输入不同版本标签”）。
     */
    public synchronized void switchRulesVersion(String newRulesVersion) {
        GatewayConfig old = config;
        GatewayConfig copy = new GatewayConfig(
                newRulesVersion, old.sensors(), old.calibration(),
                old.bufferCapacity(), old.overflowPolicy(), old.consumerThreads(),
                old.retryPolicy(), old.breakerThreshold(), old.breakerCooldownMs(),
                old.shutdownDrainTimeoutMs(), old.verdictLogCapacity(),
                old.reconcileDetailCapacity(), old.reconcileMaxSealedGenerations(),
                old.reconcileRetentionPolicy());
        switchRulesVersion(copy);
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
        boolean drained = true;
        try {
            if (!awaitDrain(Math.max(0, remaining))) {
                drained = false;
                LOG.warning("drain timeout before all enqueued readings were consumed");
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            drained = false;
            LOG.warning("interrupted while draining enqueued readings");
        }
        // 3) 关闭消费者：打断“卡住”的消费者，其在途读数由 process 的兜底落为未完成
        shutdownPool(consumerPool);
        // 4) 兜底：排空超时仍滞留缓冲（含关闭时消费者短暂卡住）的已提交读数，
        //    明确落为 SHUTDOWN_INCOMPLETE，绝不静默丢失；与消费者竞争时台账幂等去重。
        if (!drained || buffer.size() > 0) {
            List<BoundedBuffer.Staged> stranded = buffer.drainRemaining();
            for (BoundedBuffer.Staged s : stranded) {
                stats.incompleteAtShutdown();
                Verdict v = Verdict.reject(s.reading(), RejectReason.SHUTDOWN_INCOMPLETE,
                        "input{sensor=" + s.reading().sensorId() + ",tick=" + s.reading().tick()
                                + "}; rule=drain timeout with reading still buffered -> SHUTDOWN_INCOMPLETE",
                        s.rulesVersion());
                verdictLog.append(v);
                reconciliation.appendIncomplete(s.rulesVersion(), s.reading(), clock.nowMillis());
            }
            if (!stranded.isEmpty()) {
                LOG.warning("sealed generation with " + stranded.size() + " SHUTDOWN_INCOMPLETE readings");
            }
        }
        // 5) 关闭生产者并封代：此后不再有新裁决产生
        shutdownPool(producerPool);
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
        this.configsByRulesVersion.clear();
        this.configsByRulesVersion.put(newConfig.rulesVersion(), newConfig);
        this.sensors = newSensors;
        this.breakers.clear();
        for (SensorConfig sc : newConfig.sensors()) {
            breakers.put(sc.getId(), new CircuitBreaker(
                    sc.getId(), newConfig.breakerThreshold(), newConfig.breakerCooldownMs()));
        }
        this.buffer = newBuffer(newConfig);
        this.verdictLog = new VerdictLog(newConfig.verdictLogCapacity());
        stopRequested.set(false);
        running.set(true);
        reconciliation.beginGeneration(newConfig.rulesVersion(), clock.nowMillis());
        bufferPoolsUp();
        LOG.info(() -> "SamplingGateway restarted with rulesVersion=" + newConfig.rulesVersion());
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
