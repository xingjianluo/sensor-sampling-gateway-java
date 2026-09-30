package com.github.xingjianluo.sensorsamplinggatewayjava.reconcile;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.RawReading;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.RejectReason;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.Verdict;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 采样对账台账：所有裁决的单一对账事实源（single source of truth）。
 *
 * <p>每次 start/restart 开启一个“代次（批次）”，优雅停止时封代。<b>运行中切换规则版本
 * 不开新代</b>：同代内每切换一次就增加一个“规则版本阶段”，按生效顺序记录在
 * {@link GenerationInfo#rulesVersions()} 中；读数在入队受理时盖上当时版本的章，因此
 * 切换前已受理的读数按原版本裁决，切换后的读数按新版本判定，同代总账仍闭合、版本可切片。</p>
 *
 * <p>每条 {@link Verdict}（或关闭边界为滞留读数补记的未完成记录）落账为一条
 * {@link ReconciliationEntry}，身份键 {@code (generation, sensorId, tick)} 全局唯一，
 * 重复落账被确定性忽略（幂等保护，故障恢复/关闭竞争下不重复计数）。</p>
 *
 * <p>顶层终结方式四分类互斥且穷尽：
 * {@code total = accepted + rejected + evictedOld + incomplete}。
 * 代次级/版本阶段级/传感器级的聚合计数在落账时原子维护，<b>精确且不受明细保留滚动
 * 影响</b>；明细按代次有界保留，超容量行为由 {@link RetentionPolicy} 确定。</p>
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
    private long incompleteTotal;
    private long duplicateAppendsIgnored;
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

    /**
     * 运行中切换规则版本：在当前运行代次内登记一个新阶段（不封代、不开新代）。
     * 阶段在第一条该版本读数落账前登记也有效；重复登记同一版本无副作用。
     */
    public void noteRulesVersion(String rulesVersion) {
        if (rulesVersion == null || rulesVersion.isBlank()) {
            throw new IllegalArgumentException("rulesVersion must not be blank");
        }
        synchronized (lock) {
            GenerationLedger g = ledgers.get(runningGenerationId);
            if (g != null) {
                g.noteVersion(rulesVersion);
            }
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
            var r = verdict.reading();
            String key = r.sensorId() + "#" + r.tick();
            if (!g.keys.add(key)) {
                // 幂等保护：同代同 sensor#tick 已落账（故障恢复/关闭竞争），不重复计数
                duplicateAppendsIgnored++;
                return null;
            }
            long seq = sequence.incrementAndGet();
            Disposition disposition = ReconciliationEntry.resolve(
                    verdict.accepted(), verdict.reason(), backpressure);
            ReconciliationEntry e = new ReconciliationEntry(
                    seq, g.id, verdict.rulesVersion(),
                    r.sensorId(), r.tick(), r.timestampMs(), recordedAtMs,
                    verdict.accepted(), verdict.reason(), verdict.calibratedValue(),
                    backpressure, disposition);
            appendDetailLocked(g, e);
            g.add(e);
            entriesAppended++;
            return e;
        }
    }

    /**
     * 关闭边界补记：读数已提交（已入队），但排空超时/消费者中断使其未得到裁决。
     * 落为 {@link Disposition#INCOMPLETE} 一类可区分终结（reason=SHUTDOWN_INCOMPLETE），
     * 保证生产总数与对账总数永久闭合；若同一身份键已有落账（竞争下消费者刚好处理完），
     * 幂等忽略，不重复计数。返回落账条目；未运行/重复时返回 null。
     */
    public ReconciliationEntry appendIncomplete(String rulesVersion, RawReading reading, long recordedAtMs) {
        if (rulesVersion == null || rulesVersion.isBlank() || reading == null) {
            throw new IllegalArgumentException("rulesVersion/reading must not be null");
        }
        synchronized (lock) {
            GenerationLedger g = ledgers.get(runningGenerationId);
            if (g == null) {
                appendsWhileNoRunningGen++;
                return null;
            }
            String key = reading.sensorId() + "#" + reading.tick();
            if (!g.keys.add(key)) {
                duplicateAppendsIgnored++;
                return null;
            }
            Verdict verdict = Verdict.reject(reading, RejectReason.SHUTDOWN_INCOMPLETE,
                    "input{sensor=" + reading.sensorId() + ",tick=" + reading.tick()
                            + "}; rule=shutdown drain timeout/consumer interrupted -> "
                            + "SHUTDOWN_INCOMPLETE (submitted but no verdict before stop)",
                    rulesVersion);
            long seq = sequence.incrementAndGet();
            ReconciliationEntry e = new ReconciliationEntry(
                    seq, g.id, rulesVersion,
                    reading.sensorId(), reading.tick(), reading.timestampMs(), recordedAtMs,
                    false, RejectReason.SHUTDOWN_INCOMPLETE, Double.NaN,
                    BackpressureKind.NONE, Disposition.INCOMPLETE);
            appendDetailLocked(g, e);
            g.add(e);
            entriesAppended++;
            incompleteTotal++;
            return e;
        }
    }

    private void appendDetailLocked(GenerationLedger g, ReconciliationEntry e) {
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
    }

    /** 按筛选条件对账，返回不可变、自校验闭合的报告快照。 */
    public ReconciliationReport reconcile(ReconciliationFilter filter) {
        ReconciliationFilter f = filter == null ? ReconciliationFilter.all() : filter;
        long generatedAt = System.currentTimeMillis();
        synchronized (lock) {
            boolean rulesFiltered = f.rulesVersion() != null && !f.rulesVersion().isBlank();
            boolean sensorFiltered = f.sensorId() != null && !f.sensorId().isBlank();
            boolean hasTimeFilter = f.fromRecordedMs() > 0L || f.toRecordedMs() > 0L;

            Accumulator acc = new Accumulator();
            Map<String, SensorTotalsAcc> perSensor = new LinkedHashMap<>();
            List<ReconciliationEntry> acceptedEntries = new ArrayList<>();
            List<ReconciliationEntry> rejectedEntries = new ArrayList<>();
            List<ReconciliationEntry> evictedEntries = new ArrayList<>();
            List<ReconciliationEntry> incompleteEntries = new ArrayList<>();
            long touchedRollover = 0L;
            boolean complete = true;
            long exactMatchedTotal = 0L;
            long detailMatchedTotal = 0L;

            for (GenerationLedger l : ledgers.values()) {
                if (!f.matchesGeneration(l.id)) {
                    continue;
                }
                boolean versionPresentHere =
                        !rulesFiltered || l.perVersion.containsKey(f.rulesVersion());
                if (hasTimeFilter) {
                    // 时间切片只能逐条扫描明细；涉及代次须明细完整、且无未完成才精确。
                    if (versionPresentHere && l.detailRollover > 0L) {
                        complete = false;
                        touchedRollover += l.detailRollover;
                    }
                    for (ReconciliationEntry e : l.details) {
                        if (f.matchesRules(e.rulesVersion())
                                && f.matchesSensor(e.sensorId())
                                && f.matchesTime(e.recordedAtMs())) {
                            classify(acc, perSensor, acceptedEntries, rejectedEntries,
                                    evictedEntries, incompleteEntries, e);
                        }
                    }
                    continue;
                }
                if (!versionPresentHere) {
                    continue;
                }
                if (l.detailRollover > 0L) {
                    touchedRollover += l.detailRollover;
                }
                // 精确聚合：整代/版本阶段/传感器切片都来自落账时维护的累加器。
                if (rulesFiltered) {
                    VersionAgg va = l.perVersion.get(f.rulesVersion());
                    if (sensorFiltered) {
                        SensorAgg sa = va.perSensor.get(f.sensorId());
                        if (sa != null) {
                            acc.add(sa);
                            exactMatchedTotal += sa.total;
                            perSensor.computeIfAbsent(f.sensorId(), k -> new SensorTotalsAcc()).add(sa);
                        }
                    } else {
                        acc.add(va);
                        exactMatchedTotal += va.total;
                        for (SensorAgg sa : va.perSensor.values()) {
                            perSensor.computeIfAbsent(sa.sensorId, k -> new SensorTotalsAcc()).add(sa);
                        }
                    }
                } else {
                    if (sensorFiltered) {
                        SensorAgg sa = l.perSensor.get(f.sensorId());
                        if (sa != null) {
                            acc.add(sa);
                            exactMatchedTotal += sa.total;
                            perSensor.computeIfAbsent(f.sensorId(), k -> new SensorTotalsAcc()).add(sa);
                        }
                    } else {
                        acc.add(l);
                        exactMatchedTotal += l.total;
                        for (SensorAgg sa : l.perSensor.values()) {
                            perSensor.computeIfAbsent(sa.sensorId, k -> new SensorTotalsAcc()).add(sa);
                        }
                    }
                }
                for (ReconciliationEntry e : l.details) {
                    if (f.matchesSensor(e.sensorId()) && f.matchesRules(e.rulesVersion())) {
                        detailMatchedTotal++;
                        putBucket(acceptedEntries, rejectedEntries,
                                evictedEntries, incompleteEntries, e);
                    }
                }
            }

            if (!hasTimeFilter) {
                complete = (detailMatchedTotal == exactMatchedTotal);
            }
            // 任何口径下出现关闭未完成记录，报告都显式不完整
            if (acc.incomplete > 0L) {
                complete = false;
            }

            sortBySeq(acceptedEntries, rejectedEntries, evictedEntries, incompleteEntries);

            Map<String, ReconciliationReport.SensorTotals> perSensorOut = new LinkedHashMap<>();
            perSensor.forEach((sid, a) -> perSensorOut.put(sid,
                    new ReconciliationReport.SensorTotals(
                            a.total, a.accepted, a.rejected, a.evictedOld, a.rejectedNew, a.incomplete)));

            ReconciliationReport report = new ReconciliationReport(
                    f, generatedAt, complete,
                    acc.total, acc.accepted, acc.rejected,
                    acc.evictedOld, acc.rejectedNew, acc.incomplete,
                    Map.copyOf(acc.byReason),
                    List.copyOf(acceptedEntries), List.copyOf(rejectedEntries),
                    List.copyOf(evictedEntries), List.copyOf(incompleteEntries),
                    Map.copyOf(perSensorOut),
                    complete ? 0L : touchedRollover);
            validateClosure(report);
            return report;
        }
    }

    private static void putBucket(List<ReconciliationEntry> accepted, List<ReconciliationEntry> rejected,
                                  List<ReconciliationEntry> evicted, List<ReconciliationEntry> incomplete,
                                  ReconciliationEntry e) {
        switch (e.disposition()) {
            case ACCEPTED -> accepted.add(e);
            case REJECTED -> rejected.add(e);
            case EVICTED_OLD -> evicted.add(e);
            case INCOMPLETE -> incomplete.add(e);
        }
    }

    private static void classify(Accumulator acc, Map<String, SensorTotalsAcc> perSensor,
                                 List<ReconciliationEntry> accepted, List<ReconciliationEntry> rejected,
                                 List<ReconciliationEntry> evicted, List<ReconciliationEntry> incomplete,
                                 ReconciliationEntry e) {
        acc.add(e);
        perSensor.computeIfAbsent(e.sensorId(), k -> new SensorTotalsAcc()).add(e);
        switch (e.disposition()) {
            case ACCEPTED -> accepted.add(e);
            case REJECTED -> rejected.add(e);
            case EVICTED_OLD -> evicted.add(e);
            case INCOMPLETE -> incomplete.add(e);
        }
    }

    private static void sortBySeq(List<ReconciliationEntry>... lists) {
        for (List<ReconciliationEntry> l : lists) {
            l.sort(java.util.Comparator.comparingLong(ReconciliationEntry::seq));
        }
    }

    /**
     * 自校验：任何对账报告内部都必须闭合、身份键唯一；不闭合直接报错而非静默给出错账。
     * 聚合计数始终精确；明细列表在 complete 时必须与计数一一对应，在不完整（明细部分
     * 滚动/含未完成）时允许是计数的真子集，但仍需自洽、无重复、终结方式分类正确。
     */
    private static void validateClosure(ReconciliationReport r) {
        if (r.total() != r.accepted() + r.rejected() + r.evictedOld() + r.incomplete()) {
            throw new IllegalStateException(
                    "ledger not closed: total != accepted + rejected + evictedOld + incomplete");
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
                    "ledger not closed: BUFFER_OVERFLOW != rejectedNew (evictions must stay out of rejected)");
        }
        if (r.backpressure() != r.evictedOld() + r.rejectedNew()) {
            throw new IllegalStateException("ledger not closed: backpressure split mismatch");
        }
        if (r.acceptedEntries().size() > r.accepted()
                || r.rejectedEntries().size() > r.rejected()
                || r.evictedEntries().size() > r.evictedOld()
                || r.incompleteEntries().size() > r.incomplete()) {
            throw new IllegalStateException("ledger not closed: detail lists exceed exact counts");
        }
        if (r.complete()) {
            if (r.accepted() != r.acceptedEntries().size()
                    || r.rejected() != r.rejectedEntries().size()
                    || r.evictedOld() != r.evictedEntries().size()
                    || r.incomplete() != r.incompleteEntries().size()) {
                throw new IllegalStateException("complete report must carry every matching detail");
            }
        }
        Set<String> keys = new HashSet<>();
        checkBucket(keys, r.acceptedEntries(), true, Disposition.ACCEPTED);
        checkBucket(keys, r.rejectedEntries(), false, Disposition.REJECTED);
        checkBucket(keys, r.evictedEntries(), false, Disposition.EVICTED_OLD);
        checkBucket(keys, r.incompleteEntries(), false, Disposition.INCOMPLETE);
    }

    private static void checkBucket(Set<String> keys, List<ReconciliationEntry> list,
                                    boolean accepted, Disposition expected) {
        for (ReconciliationEntry e : list) {
            if (e.accepted() != accepted || e.disposition() != expected
                    || (!accepted && e.reason() == null)
                    || !keys.add(e.generation() + "#" + e.sensorTickKey())) {
                throw new IllegalStateException(
                        "invalid/duplicate " + expected + " entry: " + e.sensorTickKey());
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
                    entriesAppended, details, detailsRolloverTotal, incompleteTotal,
                    duplicateAppendsIgnored, appendsWhileNoRunningGen,
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
        long incomplete;

        void add(ReconciliationEntry e) {
            total++;
            switch (e.disposition()) {
                case ACCEPTED -> accepted++;
                case REJECTED -> rejected++;
                case EVICTED_OLD -> evictedOld++;
                case INCOMPLETE -> incomplete++;
            }
            if (e.backpressure() == BackpressureKind.REJECTED_NEW) {
                rejectedNew++;
            }
        }

        void add(SensorAgg sa) {
            total += sa.total;
            accepted += sa.accepted;
            rejected += sa.rejected;
            evictedOld += sa.evictedOld;
            rejectedNew += sa.rejectedNew;
            incomplete += sa.incomplete;
        }
    }

    /** 精确聚合计数累加器（代次级 / 版本阶段级 / 传感器级同构）。 */
    private static final class Accumulator {
        long total;
        long accepted;
        long rejected;
        long evictedOld;
        long rejectedNew;
        long incomplete;
        final EnumMap<RejectReason, Long> byReason = new EnumMap<>(RejectReason.class);

        Accumulator() {
            for (RejectReason r : RejectReason.values()) {
                byReason.put(r, 0L);
            }
        }

        void add(ReconciliationEntry e) {
            total++;
            switch (e.disposition()) {
                case ACCEPTED -> accepted++;
                case REJECTED -> {
                    rejected++;
                    byReason.merge(e.reason(), 1L, Long::sum);
                }
                case EVICTED_OLD -> evictedOld++;
                case INCOMPLETE -> incomplete++;
            }
            if (e.backpressure() == BackpressureKind.REJECTED_NEW) {
                rejectedNew++;
            }
        }

        void add(SensorAgg sa) {
            total += sa.total;
            accepted += sa.accepted;
            rejected += sa.rejected;
            evictedOld += sa.evictedOld;
            rejectedNew += sa.rejectedNew;
            incomplete += sa.incomplete;
            sa.byReason.forEach((reason, n) -> byReason.merge(reason, n, Long::sum));
        }

        void add(VersionAgg va) {
            total += va.total;
            accepted += va.accepted;
            rejected += va.rejected;
            evictedOld += va.evictedOld;
            rejectedNew += va.rejectedNew;
            incomplete += va.incomplete;
            va.byReason.forEach((reason, n) -> byReason.merge(reason, n, Long::sum));
        }

        void add(GenerationLedger l) {
            total += l.total;
            accepted += l.accepted;
            rejected += l.rejected;
            evictedOld += l.evictedOld;
            rejectedNew += l.rejectedNew;
            incomplete += l.incomplete;
            l.byReason.forEach((reason, n) -> byReason.merge(reason, n, Long::sum));
        }
    }

    /** 单个传感器在一个聚合范围内的精确计数。 */
    private static final class SensorAgg {
        final String sensorId;
        long total;
        long accepted;
        long rejected;
        long evictedOld;
        long rejectedNew;
        long incomplete;
        final EnumMap<RejectReason, Long> byReason = new EnumMap<>(RejectReason.class);

        SensorAgg(String sensorId) {
            this.sensorId = sensorId;
            for (RejectReason r : RejectReason.values()) {
                byReason.put(r, 0L);
            }
        }

        void add(ReconciliationEntry e) {
            total++;
            switch (e.disposition()) {
                case ACCEPTED -> accepted++;
                case REJECTED -> {
                    rejected++;
                    byReason.merge(e.reason(), 1L, Long::sum);
                }
                case EVICTED_OLD -> evictedOld++;
                case INCOMPLETE -> incomplete++;
            }
            if (e.backpressure() == BackpressureKind.REJECTED_NEW) {
                rejectedNew++;
            }
        }
    }

    /** 同代内一个规则版本阶段的精确聚合。 */
    private static final class VersionAgg {
        long total;
        long accepted;
        long rejected;
        long evictedOld;
        long rejectedNew;
        long incomplete;
        final EnumMap<RejectReason, Long> byReason = new EnumMap<>(RejectReason.class);
        final LinkedHashMap<String, SensorAgg> perSensor = new LinkedHashMap<>();

        VersionAgg() {
            for (RejectReason r : RejectReason.values()) {
                byReason.put(r, 0L);
            }
        }

        void add(ReconciliationEntry e) {
            total++;
            switch (e.disposition()) {
                case ACCEPTED -> accepted++;
                case REJECTED -> {
                    rejected++;
                    byReason.merge(e.reason(), 1L, Long::sum);
                }
                case EVICTED_OLD -> evictedOld++;
                case INCOMPLETE -> incomplete++;
            }
            if (e.backpressure() == BackpressureKind.REJECTED_NEW) {
                rejectedNew++;
            }
            perSensor.computeIfAbsent(e.sensorId(), SensorAgg::new).add(e);
        }
    }

    /** 一个运行代次（批次）的账本：有界明细 + 精确聚合（整代/版本阶段/传感器三级）。 */
    private static final class GenerationLedger {
        final long id;
        final LinkedHashSet<String> versions = new LinkedHashSet<>();
        String currentVersion;
        final long startedAtMs;
        long sealedAtMs;
        boolean sealed;

        final ArrayDeque<ReconciliationEntry> details = new ArrayDeque<>();
        final Set<String> keys = new HashSet<>();
        long detailRollover;

        long total;
        long accepted;
        long rejected;
        long evictedOld;
        long rejectedNew;
        long incomplete;
        final EnumMap<RejectReason, Long> byReason = new EnumMap<>(RejectReason.class);
        final LinkedHashMap<String, SensorAgg> perSensor = new LinkedHashMap<>();
        final LinkedHashMap<String, VersionAgg> perVersion = new LinkedHashMap<>();

        GenerationLedger(long id, String rulesVersion, long startedAtMs) {
            this.id = id;
            this.startedAtMs = startedAtMs;
            for (RejectReason r : RejectReason.values()) {
                byReason.put(r, 0L);
            }
            noteVersion(rulesVersion);
        }

        void noteVersion(String rulesVersion) {
            if (versions.add(rulesVersion)) {
                perVersion.computeIfAbsent(rulesVersion, k -> new VersionAgg());
                currentVersion = rulesVersion;
            }
        }

        void add(ReconciliationEntry e) {
            noteVersion(e.rulesVersion());
            total++;
            switch (e.disposition()) {
                case ACCEPTED -> accepted++;
                case REJECTED -> {
                    rejected++;
                    byReason.merge(e.reason(), 1L, Long::sum);
                }
                case EVICTED_OLD -> evictedOld++;
                case INCOMPLETE -> incomplete++;
            }
            if (e.backpressure() == BackpressureKind.REJECTED_NEW) {
                rejectedNew++;
            }
            perSensor.computeIfAbsent(e.sensorId(), SensorAgg::new).add(e);
            perVersion.get(e.rulesVersion()).add(e);
        }

        String currentVersion() {
            return currentVersion;
        }

        GenerationInfo info() {
            return new GenerationInfo(id, currentVersion(), List.copyOf(versions),
                    startedAtMs, sealedAtMs, sealed,
                    total, accepted, rejected, evictedOld, rejectedNew, incomplete,
                    details.size(), detailRollover);
        }
    }
}
