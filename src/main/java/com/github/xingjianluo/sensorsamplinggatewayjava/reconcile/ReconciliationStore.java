package com.github.xingjianluo.sensorsamplinggatewayjava.reconcile;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.RejectReason;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.Verdict;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 采样对账台账：所有裁决的单一对账事实源（single source of truth）。
 *
 * <p>每次 start/restart 开启一个“代次（批次）”，优雅停止时封代；<b>运行中切换规则版本
 * 不开新代</b>：同一次运行可含多个规则版本，每条落账携带其“被受理时”的版本，
 * 代内按版本各自精确聚合。身份键 {@code (generation, sensorId, tick)} 全局唯一。</p>
 *
 * <p><b>沿用历史统计口径（不改老计数）</b>：DROP_OLDEST 被顶掉的读数在台账中是一条
 * 带 BUFFER_OVERFLOW 原因、{@code EVICTED_OLD} 背压标记的拒绝明细，但单列计数、
 * <b>不</b>计入 {@code rejected} 与 {@code rejectedByReason}；后者只含直接丢弃
 * （{@code REJECTED_NEW}）。闭合恒等式：</p>
 * <pre>
 * total      = accepted + rejected + evictedOld
 * rejected   = Σ rejectedByReason
 * rejectedNew = rejectedByReason[BUFFER_OVERFLOW]
 * backpressure = evictedOld + rejectedNew
 * </pre>
 *
 * <p>关闭排空超时仍滞留缓冲的已受理读数以 {@code SHUTDOWN_INCOMPLETE} 原因落账
 * （计入 rejected/total，可在明细中定位），并同时累加到 {@code unfinished}；
 * 这类报告 {@code complete=false}，显式标注不完整。</p>
 *
 * <p>线程安全：所有读写由内部一把锁与原子序列号保护；查询/报告返回不可变快照。</p>
 */
public final class ReconciliationStore {

    private final int detailCapacity;
    private final int maxSealedGenerations;
    private final RetentionPolicy retentionPolicy;

    private final Object lock = new Object();
    private final AtomicLong sequence = new AtomicLong();

    /** 按代次 id 升序保留（插入序）的代次账本，含运行代与未被清除的已封代。 */
    private final LinkedHashMap<Long, GenerationLedger> ledgers = new LinkedHashMap<>();
    private long nextGenerationId = 1L;
    private long runningGenerationId = 0L;

    private long generationsCreated;
    private long generationsPurged;
    private long entriesAppended;
    private long detailsRolloverTotal;
    private long appendsWhileNoRunningGen;

    public ReconciliationStore(int detailCapacityPerGeneration,
                               int maxSealedGenerations,
                               RetentionPolicy retentionPolicy) {
        if (detailCapacityPerGeneration < 1) {
            throw new IllegalArgumentException("detailCapacityPerGeneration must be >= 1");
        }
        if (maxSealedGenerations < 1) {
            throw new IllegalArgumentException("maxSealedGenerations must be >= 1");
        }
        this.detailCapacity = detailCapacityPerGeneration;
        this.maxSealedGenerations = maxSealedGenerations;
        this.retentionPolicy = retentionPolicy == null ? RetentionPolicy.ROLL_OLDEST : retentionPolicy;
    }

    /** 开启新代次（批次）。若仍有未封代次，先以同一时刻确定性封代。返回新代次 id。 */
    public long beginGeneration(String rulesVersion, long nowMs) {
        if (rulesVersion == null || rulesVersion.isBlank()) {
            throw new IllegalArgumentException("rulesVersion must not be blank");
        }
        synchronized (lock) {
            if (runningGenerationId != 0L) {
                sealLocked(nowMs);
            }
            long id = nextGenerationId++;
            GenerationLedger ledger = new GenerationLedger(id, rulesVersion, nowMs);
            ledgers.put(id, ledger);
            runningGenerationId = id;
            generationsCreated++;
            return id;
        }
    }

    /** 封代（优雅停止）。返回被封代次 id；无运行代次返回 0。 */
    public long sealGeneration(long nowMs) {
        synchronized (lock) {
            return sealLocked(nowMs);
        }
    }

    private long sealLocked(long nowMs) {
        GenerationLedger running = ledgers.get(runningGenerationId);
        if (running == null) {
            runningGenerationId = 0L;
            return 0L;
        }
        running.sealed = true;
        running.sealedAtMs = nowMs;
        long sealedId = runningGenerationId;
        runningGenerationId = 0L;
        // 只清除“已封代”中最旧者，永不清除运行代；运行代不占封代名额
        while (countSealedLocked() > maxSealedGenerations) {
            Long oldestSealed = null;
            for (Long id : ledgers.keySet()) {
                GenerationLedger l = ledgers.get(id);
                if (l.sealed) {
                    oldestSealed = id;
                    break;
                }
            }
            if (oldestSealed == null) {
                break;
            }
            ledgers.remove(oldestSealed);
            generationsPurged++;
        }
        return sealedId;
    }

    private int countSealedLocked() {
        int n = 0;
        for (GenerationLedger l : ledgers.values()) {
            if (l.sealed) {
                n++;
            }
        }
        return n;
    }

    /** 将一条裁决落账到当前运行代次；无运行代次时确定性忽略并计入边界统计。 */
    public ReconciliationEntry append(Verdict verdict, BackpressureKind backpressure, long recordedAtMs) {
        if (verdict == null || backpressure == null) {
            throw new IllegalArgumentException("verdict/backpressure must not be null");
        }
        synchronized (lock) {
            GenerationLedger g = ledgers.get(runningGenerationId);
            if (g == null) {
                appendsWhileNoRunningGen++;
                return null;
            }
            long seq = sequence.incrementAndGet();
            var r = verdict.reading();
            ReconciliationEntry e = new ReconciliationEntry(
                    seq, g.id, verdict.rulesVersion(),
                    r.sensorId(), r.tick(), r.timestampMs(), recordedAtMs,
                    verdict.accepted(), verdict.reason(), verdict.calibratedValue(),
                    backpressure);
            g.add(e);
            entriesAppended++;
            if (g.details.size() >= detailCapacity) {
                switch (retentionPolicy) {
                    case ROLL_OLDEST -> {
                        g.details.removeFirst();
                        g.details.addLast(e);
                        g.detailRollover++;
                        detailsRolloverTotal++;
                    }
                    case IGNORE_NEW -> {
                        // 只更新聚合计数；已保留明细一条不改、一条不删
                        g.detailRollover++;
                        detailsRolloverTotal++;
                    }
                }
            } else {
                g.details.addLast(e);
            }
            return e;
        }
    }

    /** 按筛选条件对账，返回不可变、自校验闭合的报告快照。 */
    public ReconciliationReport reconcile(ReconciliationFilter filter) {
        ReconciliationFilter f = filter == null ? ReconciliationFilter.all() : filter;
        long generatedAt = System.currentTimeMillis();
        synchronized (lock) {
            List<GenerationLedger> matched = new ArrayList<>();
            long touchedRollover = 0L;
            for (GenerationLedger l : ledgers.values()) {
                if (f.matchesGeneration(l.id) && l.hasRulesVersion(f)) {
                    matched.add(l);
                    touchedRollover += l.detailRollover;
                }
            }

            boolean hasTimeFilter = f.fromRecordedMs() > 0L || f.toRecordedMs() > 0L;
            Accumulator acc = new Accumulator();
            boolean detailsComplete;
            Map<String, SensorTotalsAcc> perSensor = new LinkedHashMap<>();
            List<ReconciliationEntry> acceptedEntries = new ArrayList<>();
            List<ReconciliationEntry> rejectedEntries = new ArrayList<>();

            if (hasTimeFilter) {
                // 时间切片：只能逐条扫描明细。涉及代次明细完整保留且无未完成读数时才完整。
                detailsComplete = true;
                for (GenerationLedger l : matched) {
                    if (l.detailRollover > 0L) {
                        detailsComplete = false;
                    }
                    for (ReconciliationEntry e : l.details) {
                        if (f.matchesSensor(e.sensorId())
                                && f.matchesRules(e.rulesVersion())
                                && f.matchesTime(e.recordedAtMs())) {
                            acc.add(e);
                            perSensor.computeIfAbsent(e.sensorId(), k -> new SensorTotalsAcc()).add(e);
                            classifyDetail(e, acceptedEntries, rejectedEntries);
                        }
                    }
                }
                detailsComplete = detailsComplete && acc.unfinished == 0L;
            } else {
                // 无时间切片：聚合计数来自精确的代次/版本/传感器级累加器，永远精确。
                long exactMatchedTotal = 0L;
                long detailMatchedTotal = 0L;
                for (GenerationLedger l : matched) {
                    // 精确聚合：在代内按匹配的规则版本取版本级聚合，再按传感器筛选
                    for (VersionAgg va : l.byVersion.values()) {
                        if (!f.matchesRules(va.rulesVersion)) {
                            continue;
                        }
                        if (f.sensorId() == null || f.sensorId().isBlank()) {
                            acc.add(va);
                            exactMatchedTotal += va.total;
                            for (SensorAgg sa : va.perSensor.values()) {
                                perSensor.computeIfAbsent(sa.sensorId, k -> new SensorTotalsAcc()).add(sa);
                            }
                        } else {
                            SensorAgg sa = va.perSensor.get(f.sensorId());
                            if (sa != null) {
                                acc.add(sa);
                                exactMatchedTotal += sa.total;
                                perSensor.computeIfAbsent(f.sensorId(), k -> new SensorTotalsAcc()).add(sa);
                            }
                        }
                    }
                    // 明细扫描：版本条件同样作用于条目本身
                    for (ReconciliationEntry e : l.details) {
                        if (f.matchesSensor(e.sensorId()) && f.matchesRules(e.rulesVersion())) {
                            detailMatchedTotal++;
                            classifyDetail(e, acceptedEntries, rejectedEntries);
                        }
                    }
                }
                detailsComplete = (detailMatchedTotal == exactMatchedTotal) && acc.unfinished == 0L;
            }

            acceptedEntries.sort(java.util.Comparator.comparingLong(ReconciliationEntry::seq));
            rejectedEntries.sort(java.util.Comparator.comparingLong(ReconciliationEntry::seq));

            Map<String, ReconciliationReport.SensorTotals> perSensorOut = new LinkedHashMap<>();
            perSensor.forEach((sid, a) -> perSensorOut.put(sid,
                    new ReconciliationReport.SensorTotals(
                            a.total, a.accepted, a.rejected, a.evictedOld, a.rejectedNew, a.unfinished)));

            ReconciliationReport report = new ReconciliationReport(
                    f, generatedAt, detailsComplete,
                    acc.total, acc.accepted, acc.rejected,
                    acc.evictedOld, acc.rejectedNew, acc.unfinished,
                    Map.copyOf(acc.byReason()),
                    List.copyOf(acceptedEntries), List.copyOf(rejectedEntries),
                    Map.copyOf(perSensorOut),
                    detailsComplete ? 0L : touchedRollover);
            validateClosure(report);
            return report;
        }
    }

    private static void classifyDetail(ReconciliationEntry e,
                                       List<ReconciliationEntry> acceptedEntries,
                                       List<ReconciliationEntry> rejectedEntries) {
        if (e.accepted()) {
            acceptedEntries.add(e);
        } else {
            // 含有效性拒绝、直接丢弃、被顶掉（EVICTED_OLD）与未完成（SHUTDOWN_INCOMPLETE）
            rejectedEntries.add(e);
        }
    }

    /**
     * 自校验：任何对账报告内部都必须闭合、身份键唯一；不闭合直接报错而非静默给出错账。
     * 聚合计数（总数/接受/各原因拒绝/背压/未完成）始终精确；明细列表在 complete 时
     * 必须与计数一一对应，不完整（明细滚动或有未完成读数）时允许是真子集，但仍需自洽、无重复。
     */
    private static void validateClosure(ReconciliationReport r) {
        if (r.total() != r.accepted() + r.rejected() + r.evictedOld()) {
            throw new IllegalStateException(
                    "ledger not closed: total != accepted + rejected + evictedOld");
        }
        long sumByReason = 0L;
        for (long v : r.rejectedByReason().values()) {
            sumByReason += v;
        }
        if (r.rejected() != sumByReason) {
            throw new IllegalStateException("ledger not closed: rejected != sum(byReason)");
        }
        long overflow = r.rejectedByReason().getOrDefault(RejectReason.BUFFER_OVERFLOW, 0L);
        if (overflow != r.rejectedNew()) {
            throw new IllegalStateException(
                    "ledger not closed: byReason[BUFFER_OVERFLOW] != rejectedNew");
        }
        long unfinished = r.rejectedByReason().getOrDefault(RejectReason.SHUTDOWN_INCOMPLETE, 0L);
        if (unfinished != r.unfinished()) {
            throw new IllegalStateException(
                    "ledger not closed: byReason[SHUTDOWN_INCOMPLETE] != unfinished");
        }
        if (r.acceptedEntries().size() > r.accepted()
                || r.rejectedEntries().size() > r.rejected() + r.evictedOld()) {
            throw new IllegalStateException("ledger not closed: detail lists exceed exact counts");
        }
        if (r.complete()) {
            if (r.accepted() != r.acceptedEntries().size()
                    || r.rejected() + r.evictedOld() != r.rejectedEntries().size()) {
                throw new IllegalStateException("complete report must carry every matching detail");
            }
            if (r.unfinished() != 0L) {
                throw new IllegalStateException("complete report must not carry unfinished readings");
            }
        }
        java.util.HashSet<String> keys = new java.util.HashSet<>();
        for (ReconciliationEntry e : r.acceptedEntries()) {
            if (!e.accepted() || !keys.add(e.generation() + "#" + e.sensorTickKey())) {
                throw new IllegalStateException("invalid/duplicate accepted entry: " + e.sensorTickKey());
            }
        }
        for (ReconciliationEntry e : r.rejectedEntries()) {
            if (e.accepted() || e.reason() == null
                    || !keys.add(e.generation() + "#" + e.sensorTickKey())) {
                throw new IllegalStateException("invalid/duplicate rejected entry: " + e.sensorTickKey());
            }
            if (e.backpressure() == BackpressureKind.EVICTED_OLD
                    && e.reason() != RejectReason.BUFFER_OVERFLOW) {
                throw new IllegalStateException("evicted-old entry must carry BUFFER_OVERFLOW");
            }
        }
    }

    /** 查询仍保留的单条明细。{@code generation <= 0} 表示当前运行代次。 */
    public ReconciliationEntry findEntry(long generation, String sensorTickKey) {
        if (sensorTickKey == null) {
            return null;
        }
        synchronized (lock) {
            long gid = generation >= 1L ? generation : runningGenerationId;
            GenerationLedger l = ledgers.get(gid);
            if (l == null) {
                return null;
            }
            for (ReconciliationEntry e : l.details) {
                if (e.sensorTickKey().equals(sensorTickKey)) {
                    return e;
                }
            }
            return null;
        }
    }

    /** 当前保留的全部代次元数据（按 id 升序），含运行代与已封代。 */
    public List<GenerationInfo> generations() {
        synchronized (lock) {
            List<GenerationInfo> out = new ArrayList<>(ledgers.size());
            for (GenerationLedger l : ledgers.values()) {
                out.add(l.info());
            }
            return List.copyOf(out);
        }
    }

    /** 单个代次的元数据与精确聚合；不存在返回 null。 */
    public GenerationInfo generation(long id) {
        synchronized (lock) {
            GenerationLedger l = ledgers.get(id);
            return l == null ? null : l.info();
        }
    }

    public long runningGeneration() {
        synchronized (lock) {
            return runningGenerationId;
        }
    }

    public ReconciliationStoreStats storeStats() {
        synchronized (lock) {
            long details = 0L;
            for (GenerationLedger l : ledgers.values()) {
                details += l.details.size();
            }
            return new ReconciliationStoreStats(
                    runningGenerationId, generationsCreated, ledgers.size(), generationsPurged,
                    entriesAppended, details, detailsRolloverTotal, appendsWhileNoRunningGen,
                    detailCapacity, maxSealedGenerations, retentionPolicy);
        }
    }

    // ===== 内部聚合结构 =====

    private static final class SensorTotalsAcc {
        long total;
        long accepted;
        long rejected;
        long evictedOld;
        long rejectedNew;
        long unfinished;

        void add(ReconciliationEntry e) {
            total++;
            if (e.accepted()) {
                accepted++;
            } else if (e.backpressure() == BackpressureKind.EVICTED_OLD) {
                // 历史口径：被顶掉单列，不计 rejected
                evictedOld++;
            } else {
                rejected++;
                if (e.backpressure() == BackpressureKind.REJECTED_NEW) {
                    rejectedNew++;
                }
                if (e.reason() == RejectReason.SHUTDOWN_INCOMPLETE) {
                    unfinished++;
                }
            }
        }

        void add(SensorAgg sa) {
            total += sa.total;
            accepted += sa.accepted;
            rejected += sa.rejected;
            evictedOld += sa.evictedOld;
            rejectedNew += sa.rejectedNew;
            unfinished += sa.unfinished;
        }
    }

    /** 精确聚合计数累加器（代次级 / 版本级 / 传感器级同构）。 */
    private static final class Accumulator {
        long total;
        long accepted;
        long rejected;
        long evictedOld;
        long rejectedNew;
        long unfinished;
        final EnumMap<RejectReason, Long> byReason = new EnumMap<>(RejectReason.class);

        Accumulator() {
            for (RejectReason r : RejectReason.values()) {
                byReason.put(r, 0L);
            }
        }

        void add(ReconciliationEntry e) {
            total++;
            if (e.accepted()) {
                accepted++;
                return;
            }
            if (e.backpressure() == BackpressureKind.EVICTED_OLD) {
                // 历史口径：被顶掉读数单列，不进 rejected / byReason
                evictedOld++;
                return;
            }
            rejected++;
            byReason.merge(e.reason(), 1L, Long::sum);
            if (e.backpressure() == BackpressureKind.REJECTED_NEW) {
                rejectedNew++;
            }
            if (e.reason() == RejectReason.SHUTDOWN_INCOMPLETE) {
                unfinished++;
            }
        }

        void add(SensorAgg sa) {
            total += sa.total;
            accepted += sa.accepted;
            rejected += sa.rejected;
            evictedOld += sa.evictedOld;
            rejectedNew += sa.rejectedNew;
            unfinished += sa.unfinished;
            sa.byReason.forEach((reason, n) -> byReason.merge(reason, n, Long::sum));
        }

        void add(VersionAgg va) {
            total += va.total;
            accepted += va.accepted;
            rejected += va.rejected;
            evictedOld += va.evictedOld;
            rejectedNew += va.rejectedNew;
            unfinished += va.unfinished;
            va.byReason.forEach((reason, n) -> byReason.merge(reason, n, Long::sum));
        }

        Map<RejectReason, Long> byReason() {
            return byReason;
        }
    }

    /** 单个传感器在某代/某版本内的精确聚合。 */
    private static final class SensorAgg {
        final String sensorId;
        long total;
        long accepted;
        long rejected;
        long evictedOld;
        long rejectedNew;
        long unfinished;
        final EnumMap<RejectReason, Long> byReason = new EnumMap<>(RejectReason.class);

        SensorAgg(String sensorId) {
            this.sensorId = sensorId;
            for (RejectReason r : RejectReason.values()) {
                byReason.put(r, 0L);
            }
        }

        void add(ReconciliationEntry e) {
            total++;
            if (e.accepted()) {
                accepted++;
                return;
            }
            if (e.backpressure() == BackpressureKind.EVICTED_OLD) {
                evictedOld++;
                return;
            }
            rejected++;
            byReason.merge(e.reason(), 1L, Long::sum);
            if (e.backpressure() == BackpressureKind.REJECTED_NEW) {
                rejectedNew++;
            }
            if (e.reason() == RejectReason.SHUTDOWN_INCOMPLETE) {
                unfinished++;
            }
        }
    }

    /** 同一代次内一个规则版本的精确聚合（支持运行中切换规则版本后按版本切片）。 */
    private static final class VersionAgg {
        final String rulesVersion;
        long total;
        long accepted;
        long rejected;
        long evictedOld;
        long rejectedNew;
        long unfinished;
        final EnumMap<RejectReason, Long> byReason = new EnumMap<>(RejectReason.class);
        final LinkedHashMap<String, SensorAgg> perSensor = new LinkedHashMap<>();

        VersionAgg(String rulesVersion) {
            this.rulesVersion = rulesVersion;
            for (RejectReason r : RejectReason.values()) {
                byReason.put(r, 0L);
            }
        }

        void add(ReconciliationEntry e) {
            total++;
            if (e.accepted()) {
                accepted++;
            } else if (e.backpressure() == BackpressureKind.EVICTED_OLD) {
                evictedOld++;
            } else {
                rejected++;
                byReason.merge(e.reason(), 1L, Long::sum);
                if (e.backpressure() == BackpressureKind.REJECTED_NEW) {
                    rejectedNew++;
                }
                if (e.reason() == RejectReason.SHUTDOWN_INCOMPLETE) {
                    unfinished++;
                }
            }
            perSensor.computeIfAbsent(e.sensorId(), SensorAgg::new).add(e);
        }
    }

    /** 一个运行代次（批次）的账本：有界明细 + 代/版本/传感器三级精确聚合。 */
    private static final class GenerationLedger {
        final long id;
        final String startRulesVersion;
        final long startedAtMs;
        long sealedAtMs;
        boolean sealed;

        final ArrayDeque<ReconciliationEntry> details = new ArrayDeque<>();
        long total;
        long accepted;
        long rejected;
        long evictedOld;
        long rejectedNew;
        long unfinished;
        long detailRollover;
        /** 该代出现过的规则版本（按首次出现顺序，去重）。 */
        final LinkedHashSet<String> rulesVersions = new LinkedHashSet<>();
        final EnumMap<RejectReason, Long> byReason = new EnumMap<>(RejectReason.class);
        final LinkedHashMap<String, SensorAgg> perSensor = new LinkedHashMap<>();
        final LinkedHashMap<String, VersionAgg> byVersion = new LinkedHashMap<>();

        GenerationLedger(long id, String rulesVersion, long startedAtMs) {
            this.id = id;
            this.startRulesVersion = rulesVersion;
            this.startedAtMs = startedAtMs;
            this.rulesVersions.add(rulesVersion);
            for (RejectReason r : RejectReason.values()) {
                byReason.put(r, 0L);
            }
        }

        boolean hasRulesVersion(ReconciliationFilter f) {
            if (f.rulesVersion() == null || f.rulesVersion().isBlank()) {
                return true;
            }
            for (String v : rulesVersions) {
                if (f.matchesRules(v)) {
                    return true;
                }
            }
            return false;
        }

        void add(ReconciliationEntry e) {
            rulesVersions.add(e.rulesVersion());
            byVersion.computeIfAbsent(e.rulesVersion(), VersionAgg::new).add(e);
            perSensor.computeIfAbsent(e.sensorId(), SensorAgg::new).add(e);

            total++;
            if (e.accepted()) {
                accepted++;
                return;
            }
            if (e.backpressure() == BackpressureKind.EVICTED_OLD) {
                evictedOld++;
                return;
            }
            rejected++;
            byReason.merge(e.reason(), 1L, Long::sum);
            if (e.backpressure() == BackpressureKind.REJECTED_NEW) {
                rejectedNew++;
            }
            if (e.reason() == RejectReason.SHUTDOWN_INCOMPLETE) {
                unfinished++;
            }
        }

        GenerationInfo info() {
            return new GenerationInfo(id, startRulesVersion, startedAtMs, sealedAtMs, sealed,
                    total, accepted, rejected, evictedOld, rejectedNew, unfinished,
                    details.size(), detailRollover, List.copyOf(rulesVersions));
        }
    }
}
