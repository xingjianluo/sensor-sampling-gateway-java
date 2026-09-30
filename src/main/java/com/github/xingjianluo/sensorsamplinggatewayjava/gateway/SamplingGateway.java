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
import java.util.Set;
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

    /**
     * 运行中切换规则版本时，历史配置快照仍要服务于“切换前已受理、尚未消费”的读数
     * （按其受理版本判定与校准）。按 rulesVersion 保留本次运行中出现过的全部配置。
     */
    private final Map<String, GatewayConfig> configsByRulesVersion = new ConcurrentHashMap<>();
    /** 测试钩子：消费者每取走一条读数后执行（用于确定性制造关闭排空滞留）。 */
    private volatile Runnable consumerHook = () -> { };

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
        configsByRulesVersion.put(config.rulesVersion(), config);
        for (SensorConfig sc : config.sensors()) {
            breakers.put(sc.getId(),
                    new CircuitBreaker(sc.getId(), config.breakerThreshold(), config.breakerCooldownMs()));
        }
    }

    /**
     * 测试用：设置消费者钩子（每消费一条读数后运行）。例如阻塞住消费者以确定性地
     * 制造“关闭时仍有读数滞留缓冲”的边界。生产路径保持无操作。
     */
    public void setConsumerHookForTesting(Runnable hook) {
        this.consumerHook = hook == null ? () -> { } : hook;
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

    /** 生产者循环：周期节拍 + 断路器 + 有界重试退避 + 入站。 */
    private void produceLoop(Sensor sensor) {
        String id = sensor.config().getId();
        AtomicLong tick = new AtomicLong();
        CircuitBreaker breaker = breakers.get(id);
        while (running.get() && !stopRequested.get()) {
            // 每拍开始时快照当前规则版本：切换规则后新产生的拍按新版本判定，
            // 切换前已受理（已入队）的读数由消费者按其入队时携带的版本判定。
            GatewayConfig tickConfig = config;
            long period = sensor.config().getPeriodMs();
            long t = tick.getAndIncrement();
            try {
                if (!breaker.allowRequest(clock.nowMillis())) {
                    stats.submitted();
                    stats.rejected(RejectReason.SENSOR_ISOLATED);
                    record(Verdict.reject(
                            new RawReading(id, t, clock.nowMillis(), Double.NaN, Long.MIN_VALUE,
                                    tickConfig.rulesVersion()),
                            RejectReason.SENSOR_ISOLATED,
                            "input{tick=" + t + "}; rule=breaker OPEN after "
                                    + breaker.consecutiveFailures() + " consecutive failures -> SENSOR_ISOLATED",
                            tickConfig.rulesVersion()));
                    sleepPeriod(period);
                    continue;
                }
                RawReading reading = sampleWithRetry(sensor, t, breaker, tickConfig);
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
                // 入队前一刻再次取版本作为“受理版本”：该读数此后无论何时被消费，
                // 都按此版本判定与校准（运行中切换规则不重判在途读数）。
                GatewayConfig admitConfig = config;
                stats.submitted();
                BoundedBuffer.OfferResult result = buffer.offer(
                        reading.withRulesVersion(admitConfig.rulesVersion()));
                handleOfferResult(result, admitConfig);
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
    private RawReading sampleWithRetry(Sensor sensor, long tick, CircuitBreaker breaker,
                                       GatewayConfig tickConfig)
            throws InterruptedException {
        final RetryPolicy retry = tickConfig.retryPolicy();
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
                new RawReading(sensor.config().getId(), tick, clock.nowMillis(), Double.NaN, Long.MIN_VALUE,
                        tickConfig.rulesVersion()),
                RejectReason.ACQUISITION_FAILED,
                "input{tick=" + tick + ",fault=" + lastFault.getMessage()
                        + ",recoverable=" + lastFault.isRecoverable()
                        + ",attempts=" + retry.maxAttempts() + "}; rule=bounded retry exhausted"
                        + (isolatedNow ? ", breaker OPEN -> sensor isolated" : "")
                        + " -> ACQUISITION_FAILED",
                tickConfig.rulesVersion()));
        return null;
    }

    private void handleOfferResult(BoundedBuffer.OfferResult result, GatewayConfig admitConfig) {
        if (result.enqueued()) {
            stats.enqueued();
        } else {
            // DROP_NEWEST 满队拒绝，或 BLOCK 在关闭后唤醒（直接丢弃最新读数）
            stats.rejected(RejectReason.BUFFER_OVERFLOW);
            RawReading r = result.rejectedReading();
            record(Verdict.reject(r, RejectReason.BUFFER_OVERFLOW,
                    "input{sensor=" + r.sensorId() + ",tick=" + r.tick()
                            + "}; rule=buffer(capacity=" + buffer.capacity() + ",policy=" + result.policy()
                            + ") rejected newest on full/closed -> BUFFER_OVERFLOW",
                    admitConfig.rulesVersion()), BackpressureKind.REJECTED_NEW);
        }
        if (result.evictedReading() != null) {
            // DROP_OLDEST：新读数已入队（已计 enqueued），被顶掉的旧读数单独形成一条可区分结论。
            RawReading ev = result.evictedReading();
            // 历史统计口径：被顶掉读数只单列（stats.droppedEviction / 台账 evictedOld），
            // 不计入 rejected / byReason[BUFFER_OVERFLOW]；后者仅统计“直接丢弃最新读数”。
            // 账目闭合：submitted = accepted + rejected + evictedOld（见 README 口径）。
            stats.droppedEviction();
            record(Verdict.reject(ev, RejectReason.BUFFER_OVERFLOW,
                    "input{sensor=" + ev.sensorId() + ",tick=" + ev.tick()
                            + "}; rule=buffer(capacity=" + buffer.capacity()
                            + ",policy=DROP_OLDEST) evicted oldest to make room -> BUFFER_OVERFLOW",
                    ev.rulesVersion() != null ? ev.rulesVersion() : admitConfig.rulesVersion()),
                    BackpressureKind.EVICTED_OLD);
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
                consumerHook.run();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void process(RawReading reading) {
        stats.consumed();
        // 按“受理时”的规则版本取配置快照：切换前入队的在途读数仍按旧版本判定/校准。
        GatewayConfig verdictConfig = reading.rulesVersion() == null
                ? config
                : configsByRulesVersion.getOrDefault(reading.rulesVersion(), config);
        SensorConfig sc = verdictConfig.sensorConfig(reading.sensorId());
        RejectReason reason = sc == null ? null : validator.validate(reading, sc);
        if (reason != null) {
            stats.rejected(reason);
            record(Verdict.reject(reading, reason,
                    sc == null ? "sensor config missing" : validator.describe(reading, sc, reason),
                    verdictConfig.rulesVersion()));
            return;
        }
        double calibrated = verdictConfig.calibration().apply(reading.sensorId(), reading.value());
        stats.accepted();
        record(Verdict.accept(reading, calibrated,
                String.format("input{sensor=%s,tick=%d,value=%.4f}; rule=valid, calibrated=%.6f (model=%s)",
                        reading.sensorId(), reading.tick(), reading.value(),
                        calibrated, verdictConfig.calibration().getVersion()),
                verdictConfig.rulesVersion()));
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
        // 3) 关闭线程池（此后不再有新裁决产生；优雅关闭要求排空在超时内完成）
        shutdownPool(consumerPool);
        shutdownPool(producerPool);
        // 3.1) 边界兜底：排空超时/消费者卡住后仍滞留缓冲的已受理读数，逐条明确落为
        //      SHUTDOWN_INCOMPLETE（计入生产总数与拒绝、可按代次/传感器定位），
        //      既不静默丢失、也不伪装成完整；与消费者取走的读数互斥，不重不漏。
        int stranded = recordStrandedReadings();
        if (stranded > 0) {
            LOG.warning(() -> "shutdown incomplete: " + stranded
                    + " accepted reading(s) stuck in buffer recorded as SHUTDOWN_INCOMPLETE"
                    + " (consider increasing shutdown-drain-timeout-ms)");
        }
        // 4) 封代：冻结本批次账目（含未完成读数与不完整标记）。
        reconciliation.sealGeneration(clock.nowMillis());
        LOG.info("SamplingGateway stopped");
    }

    /**
     * 关闭后排空兜底：把仍滞留在缓冲中的读数全部取出并落为 SHUTDOWN_INCOMPLETE。
     * drainAll 与消费者 poll 互斥，因此每条已受理读数要么被消费、要么在此落账，恰一次。
     */
    private int recordStrandedReadings() {
        List<RawReading> rest = buffer.drainAll();
        for (RawReading r : rest) {
            String version = r.rulesVersion() != null ? r.rulesVersion() : config.rulesVersion();
            stats.unfinished();
            stats.rejected(RejectReason.SHUTDOWN_INCOMPLETE);
            record(Verdict.reject(r, RejectReason.SHUTDOWN_INCOMPLETE,
                    "input{sensor=" + r.sensorId() + ",tick=" + r.tick()
                            + "}; rule=reading stuck in buffer after shutdown drain timeout,"
                            + " never validated -> SHUTDOWN_INCOMPLETE",
                    version), BackpressureKind.NONE);
        }
        return rest.size();
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
        configsByRulesVersion.put(newConfig.rulesVersion(), newConfig);
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

    /**
     * <b>运行中切换规则版本</b>：不停机、不重启、不开新代次，在同一次运行里原子替换规则快照。
     *
     * <p>语义（并发安全）：</p>
     * <ul>
     *   <li>切换前<b>已受理（已入队）</b>的读数继续在同一缓冲内流动，消费者按其入队时
     *       携带的旧版本完成校验/校准（旧配置快照按版本保留），结论与旧版本完全一致；</li>
     *   <li>切换后<b>新产生</b>的读数（含切换后入队、隔离、采集失败等结论）按新版本判定；</li>
     *   <li>两代规则的落账在同一代次内以 {@code rulesVersion} 区分，各自可筛选，
     *       而整代总账仍闭合、身份键 {@code (generation,sensor,tick)} 唯一；</li>
     *   <li>缓冲容量/溢出策略、线程池、断路器状态与 tick 序列都不重置，因此背压、并发、
     *       故障恢复与切换叠加时也不重复、不漏记。</li>
     * </ul>
     *
     * <p>新快照仅允许改变规则相关内容（规则版本、量程/容差等传感器配置、校准模型、
     * 重试/断路器参数等）；传感器 id 集合必须与当前一致（增删传感器仍需走 restart），
     * 缓冲容量与溢出策略也必须一致（运行中更换会破坏在途背压口径）。
     *
     * @throws IllegalStateException 网关未运行
     * @throws IllegalArgumentException 新配置与在途运行结构不兼容
     */
    public synchronized void switchRules(GatewayConfig newConfig) {
        if (!running.get()) {
            throw new IllegalStateException("gateway is not running; use start/restart instead");
        }
        if (newConfig == null) {
            throw new IllegalArgumentException("newConfig must not be null");
        }
        if (newConfig.rulesVersion().equals(config.rulesVersion())) {
            throw new IllegalArgumentException(
                    "switchRules requires a different rulesVersion, got " + newConfig.rulesVersion());
        }
        if (newConfig.bufferCapacity() != config.bufferCapacity()
                || newConfig.overflowPolicy() != config.overflowPolicy()) {
            throw new IllegalArgumentException(
                    "buffer capacity/overflow policy cannot change during a run; use restart instead");
        }
        Set<String> oldIds = config.sensors().stream().map(SensorConfig::getId)
                .collect(java.util.stream.Collectors.toCollection(java.util.TreeSet::new));
        Set<String> newIds = newConfig.sensors().stream().map(SensorConfig::getId)
                .collect(java.util.stream.Collectors.toCollection(java.util.TreeSet::new));
        if (!oldIds.equals(newIds)) {
            throw new IllegalArgumentException(
                    "sensor id set cannot change during a run; use restart instead: " + oldIds + " != " + newIds);
        }
        GatewayConfig old = this.config;
        this.config = newConfig;
        configsByRulesVersion.put(newConfig.rulesVersion(), newConfig);
        LOG.info(() -> "rules switched at runtime within generation "
                + reconciliation.runningGeneration() + ": " + old.rulesVersion()
                + " -> " + newConfig.rulesVersion()
                + " (in-flight enqueued readings keep " + old.rulesVersion() + ")");
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
