package com.github.xingjianluo.sensorsamplinggatewayjava.reconcile;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.RejectReason;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.Verdict;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 采样对账台账：所有裁决的单一对账事实源（single source of truth）。
 *
 * <p>每次 start/restart 开启一个“代次（批次）”，优雅停止时封代。每条 {@link Verdict}
 * 落账为一条 {@link ReconciliationEntry}，身份键 {@code (generation, sensorId, tick)}
 * 全局唯一。代次级与传感器级的聚合计数在落账时原子维护，<b>精确且不受明细保留滚动
 * 影响</b>；明细按代次有界保留，超容量行为由 {@link RetentionPolicy} 确定。</p>
 *
 * <p>对账闭合：整代（或不带时间切片的任意组合）报告直接来自精确聚合计数，
 * {@code total = accepted + rejected = accepted + ΣbyReason}，
 * 且 {@code byReason[BUFFER_OVERFLOW] = evictedOld + rejectedNew}。
 * 时间范围切片需要逐条扫描明细，仅在涉及代次明细完整保留时才标记 complete。</p>
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
                if (f.matchesGeneration(l.id) && f.matchesRules(l.rulesVersion)) {
                    matched.add(l);
                    touchedRollover += l.detailRollover;
                }
            }

            boolean hasTimeFilter = f.fromRecordedMs() > 0L || f.toRecordedMs() > 0L;
            Accumulator acc = new Accumulator();
            boolean complete;
            Map<String, SensorTotalsAcc> perSensor = new LinkedHashMap<>();
            List<ReconciliationEntry> acceptedEntries = new ArrayList<>();
            List<ReconciliationEntry> rejectedEntries = new ArrayList<>();

            if (hasTimeFilter) {
                // 时间切片：只能逐条扫描明细。涉及代次全部完整保留时结论才精确。
                complete = true;
                for (GenerationLedger l : matched) {
                    if (l.detailRollover > 0L) {
                        complete = false;
                    }
                    for (ReconciliationEntry e : l.details) {
                        if (f.matchesSensor(e.sensorId()) && f.matchesTime(e.recordedAtMs())) {
                            acc.add(e);
                            perSensor.computeIfAbsent(e.sensorId(), k -> new SensorTotalsAcc()).add(e);
                            if (e.accepted()) {
                                acceptedEntries.add(e);
                            } else {
                                rejectedEntries.add(e);
                            }
                        }
                    }
                }
            } else {
                // 无时间切片：聚合计数来自精确的代次/传感器级累加器，永远精确。
                // complete 取决于匹配口径下的明细是否一条不少（明细数 == 精确总数）。
                complete = true;
                long exactMatchedTotal = 0L;
                long detailMatchedTotal = 0L;
                for (GenerationLedger l : matched) {
                    if (f.sensorId() == null || f.sensorId().isBlank()) {
                        acc.add(l);
                        exactMatchedTotal += l.total;
                        for (SensorAgg sa : l.perSensor.values()) {
                            perSensor.computeIfAbsent(sa.sensorId, k -> new SensorTotalsAcc()).add(sa);
                        }
                    } else {
                        SensorAgg sa = l.perSensor.get(f.sensorId());
                        if (sa != null) {
                            acc.add(sa);
                            exactMatchedTotal += sa.total;
                            perSensor.computeIfAbsent(f.sensorId(), k -> new SensorTotalsAcc()).add(sa);
                        }
                    }
                    for (ReconciliationEntry e : l.details) {
                        if (f.matchesSensor(e.sensorId())) {
                            detailMatchedTotal++;
                            if (e.accepted()) {
                                acceptedEntries.add(e);
                            } else {
                                rejectedEntries.add(e);
                            }
                        }
                    }
                }
                complete = (detailMatchedTotal == exactMatchedTotal);
            }

            acceptedEntries.sort(java.util.Comparator.comparingLong(ReconciliationEntry::seq));
            rejectedEntries.sort(java.util.Comparator.comparingLong(ReconciliationEntry::seq));

            Map<String, ReconciliationReport.SensorTotals> perSensorOut = new LinkedHashMap<>();
            perSensor.forEach((sid, a) -> perSensorOut.put(sid,
                    new ReconciliationReport.SensorTotals(
                            a.total, a.accepted, a.rejected, a.evictedOld, a.rejectedNew)));

            ReconciliationReport report = new ReconciliationReport(
                    f, generatedAt, complete,
                    acc.total, acc.accepted, acc.rejected,
                    acc.evictedOld, acc.rejectedNew,
                    Map.copyOf(acc.byReason()),
                    List.copyOf(acceptedEntries), List.copyOf(rejectedEntries),
                    Map.copyOf(perSensorOut),
                    complete ? 0L : touchedRollover);
            validateClosure(report);
            return report;
        }
    }

    /**
     * 自校验：任何对账报告内部都必须闭合、身份键唯一；不闭合直接报错而非静默给出错账。
     * 聚合计数（总数/接受/各原因拒绝/背压）始终精确；明细列表在 complete 时必须与计数
     * 一一对应，在不完整（明细已部分滚动）时允许是计数的真子集，但仍需自洽、无重复。
     */
    private static void validateClosure(ReconciliationReport r) {
        if (r.total() != r.accepted() + r.rejected()) {
            throw new IllegalStateException("ledger not closed: total != accepted + rejected");
        }
        long sumByReason = 0L;
        for (long v : r.rejectedByReason().values()) {
            sumByReason += v;
        }
        if (r.rejected() != sumByReason) {
            throw new IllegalStateException("ledger not closed: rejected != sum(byReason)");
        }
        long overflow = r.rejectedByReason().getOrDefault(RejectReason.BUFFER_OVERFLOW, 0L);
        if (overflow != r.evictedOld() + r.rejectedNew()) {
            throw new IllegalStateException("ledger not closed: BUFFER_OVERFLOW != evictedOld + rejectedNew");
        }
        if (r.acceptedEntries().size() > r.accepted()
                || r.rejectedEntries().size() > r.rejected()) {
            throw new IllegalStateException("ledger not closed: detail lists exceed exact counts");
        }
        if (r.complete()) {
            if (r.accepted() != r.acceptedEntries().size()
                    || r.rejected() != r.rejectedEntries().size()) {
                throw new IllegalStateException("complete report must carry every matching detail");
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

        void add(ReconciliationEntry e) {
            total++;
            if (e.accepted()) {
                accepted++;
            } else {
                rejected++;
            }
            if (e.backpressure() == BackpressureKind.EVICTED_OLD) {
                evictedOld++;
            } else if (e.backpressure() == BackpressureKind.REJECTED_NEW) {
                rejectedNew++;
            }
        }

        void add(SensorAgg sa) {
            total += sa.total;
            accepted += sa.accepted;
            rejected += sa.rejected;
            evictedOld += sa.evictedOld;
            rejectedNew += sa.rejectedNew;
        }
    }

    /** 精确聚合计数累加器（代次级 / 传感器级同构）。 */
    private static final class Accumulator {
        long total;
        long accepted;
        long rejected;
        long evictedOld;
        long rejectedNew;
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
            } else {
                rejected++;
                byReason.merge(e.reason(), 1L, Long::sum);
            }
            if (e.backpressure() == BackpressureKind.EVICTED_OLD) {
                evictedOld++;
            } else if (e.backpressure() == BackpressureKind.REJECTED_NEW) {
                rejectedNew++;
            }
        }

        void add(SensorAgg sa) {
            total += sa.total;
            accepted += sa.accepted;
            rejected += sa.rejected;
            evictedOld += sa.evictedOld;
            rejectedNew += sa.rejectedNew;
            sa.byReason.forEach((reason, n) -> byReason.merge(reason, n, Long::sum));
        }

        void add(GenerationLedger l) {
            total += l.total;
            accepted += l.accepted;
            rejected += l.rejected;
            evictedOld += l.evictedOld;
            rejectedNew += l.rejectedNew;
            l.byReason.forEach((reason, n) -> byReason.merge(reason, n, Long::sum));
        }

        Map<RejectReason, Long> byReason() {
            return byReason;
        }
    }

    /** 单个传感器在一代内的精确聚合。 */
    private static final class SensorAgg {
        final String sensorId;
        long total;
        long accepted;
        long rejected;
        long evictedOld;
        long rejectedNew;
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
            } else {
                rejected++;
                byReason.merge(e.reason(), 1L, Long::sum);
            }
            if (e.backpressure() == BackpressureKind.EVICTED_OLD) {
                evictedOld++;
            } else if (e.backpressure() == BackpressureKind.REJECTED_NEW) {
                rejectedNew++;
            }
        }
    }

    /** 一个运行代次（批次）的账本：有界明细 + 精确聚合。 */
    private static final class GenerationLedger {
        final long id;
        final String rulesVersion;
        final long startedAtMs;
        long sealedAtMs;
        boolean sealed;

        final ArrayDeque<ReconciliationEntry> details = new ArrayDeque<>();
        long total;
        long accepted;
        long rejected;
        long evictedOld;
        long rejectedNew;
        long detailRollover;
        final EnumMap<RejectReason, Long> byReason = new EnumMap<>(RejectReason.class);
        final LinkedHashMap<String, SensorAgg> perSensor = new LinkedHashMap<>();

        GenerationLedger(long id, String rulesVersion, long startedAtMs) {
            this.id = id;
            this.rulesVersion = rulesVersion;
            this.startedAtMs = startedAtMs;
            for (RejectReason r : RejectReason.values()) {
                byReason.put(r, 0L);
            }
        }

        void add(ReconciliationEntry e) {
            total++;
            if (e.accepted()) {
                accepted++;
            } else {
                rejected++;
                byReason.merge(e.reason(), 1L, Long::sum);
            }
            if (e.backpressure() == BackpressureKind.EVICTED_OLD) {
                evictedOld++;
            } else if (e.backpressure() == BackpressureKind.REJECTED_NEW) {
                rejectedNew++;
            }
            perSensor.computeIfAbsent(e.sensorId(), SensorAgg::new).add(e);
        }

        GenerationInfo info() {
            return new GenerationInfo(id, rulesVersion, startedAtMs, sealedAtMs, sealed,
                    total, accepted, rejected, evictedOld, rejectedNew,
                    details.size(), detailRollover);
        }
    }
}
