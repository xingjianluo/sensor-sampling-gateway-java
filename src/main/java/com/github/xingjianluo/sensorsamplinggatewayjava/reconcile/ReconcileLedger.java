package com.github.xingjianluo.sensorsamplinggatewayjava.reconcile;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.RejectReason;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 采样对账总账（线程安全，单把锁保证入账与出账的一致视图）。
 *
 * <p>每条 {@link ReconcileEntry} 同时写入两处：</p>
 * <ol>
 *   <li><b>精确聚合账目</b>：按 (run, 传感器, 规则版本) 三维的 8 个汇总立方体，
 *       只增不减、不受明细保留影响，保证任意 run/传感器/规则组合的计数恒闭合；</li>
 *   <li><b>有界明细窗口</b>：最近 {@code detailCapacity} 条，FIFO 顶掉最旧明细。
 *       顶掉明细只影响“逐条明细”，绝不改动已经出具的任何聚合数字。</li>
 * </ol>
 *
 * <p>逐读数去重表只在运行进行中保留；运行封口后释放，使已封口运行的内存占用
 * 仅为汇总立方体（O(运行数×传感器数×规则版本数)），与读数条数无关。</p>
 */
public final class ReconcileLedger {

    /** 聚合维度键；null 表示该维度聚合（通配）。 */
    private record CubeKey(Long runId, String sensor, String rules) {
    }

    private static final class Cube {
        long produced;
        long accepted;
        final EnumMap<RejectReason, Long> rejected = new EnumMap<>(RejectReason.class);

        Cube() {
            for (RejectReason r : RejectReason.values()) {
                rejected.put(r, 0L);
            }
        }
    }

    /** 运行元数据。 */
    public static final class RunInfo {
        private final long runId;
        private final long startedAtMs;
        private long sealedAtMs = -1L;

        private RunInfo(long runId, long startedAtMs) {
            this.runId = runId;
            this.startedAtMs = startedAtMs;
        }

        public long runId() {
            return runId;
        }

        public long startedAtMs() {
            return startedAtMs;
        }

        public long sealedAtMs() {
            return sealedAtMs;
        }

        public boolean sealed() {
            return sealedAtMs >= 0L;
        }
    }

    private final int detailCapacity;
    private final ArrayDeque<ReconcileEntry> details = new ArrayDeque<>();
    private final Map<CubeKey, Cube> cubes = new HashMap<>();
    private final Map<Long, RunInfo> runs = new HashMap<>();
    /** 当前运行内 sensor#tick 去重；封口后整个集合释放。 */
    private final Map<Long, Set<String>> seenKeysByRun = new HashMap<>();
    private long evictedDetails;
    private long runSeq;
    private long currentRunId = -1L;

    public ReconcileLedger(int detailCapacity) {
        if (detailCapacity < 1) {
            throw new IllegalArgumentException("detailCapacity must be >= 1");
        }
        this.detailCapacity = detailCapacity;
    }

    /** 开启新一代运行，返回其 runId（从 1 单调递增）。 */
    public synchronized long beginRun(long atMillis) {
        runSeq++;
        currentRunId = runSeq;
        runs.put(currentRunId, new RunInfo(currentRunId, atMillis));
        seenKeysByRun.put(currentRunId, new HashSet<>());
        return currentRunId;
    }

    /** 封口当前运行：只记录结束时间并释放逐读数去重表，计数与明细一律不动。 */
    public synchronized void sealRun(long atMillis) {
        if (currentRunId < 0) {
            return;
        }
        RunInfo info = runs.get(currentRunId);
        if (info != null && !info.sealed()) {
            info.sealedAtMs = atMillis;
        }
        seenKeysByRun.remove(currentRunId);
    }

    public synchronized long currentRunId() {
        return currentRunId;
    }

    public synchronized RunInfo runInfo(long runId) {
        return runs.get(runId);
    }

    /**
     * 入账一条裁决。同一运行内 {@code sensor#tick} 必须唯一，重复键直接拒绝，
     * 从账本源头上杜绝同一条读数被算两次。
     */
    public synchronized void append(ReconcileEntry entry) {
        if (entry == null) {
            throw new IllegalArgumentException("entry must not be null");
        }
        if (entry.runId() != currentRunId) {
            throw new IllegalStateException("entry run " + entry.runId()
                    + " is not the current run " + currentRunId);
        }
        RunInfo info = runs.get(entry.runId());
        if (info == null || info.sealed()) {
            throw new IllegalStateException("run " + entry.runId() + " is sealed or unknown");
        }
        Set<String> seen = seenKeysByRun.get(entry.runId());
        String key = entry.verdict().reading().key();
        if (!seen.add(key)) {
            throw new IllegalStateException("duplicate reading in run " + entry.runId() + ": " + key);
        }

        String sensor = entry.verdict().reading().sensorId();
        String rules = entry.verdict().rulesVersion();
        boolean accepted = entry.verdict().accepted();
        for (Long runDim : new Long[]{entry.runId(), null}) {
            for (String sensorDim : new String[]{sensor, null}) {
                for (String rulesDim : new String[]{rules, null}) {
                    Cube c = cubes.computeIfAbsent(
                            new CubeKey(runDim, sensorDim, rulesDim), k -> new Cube());
                    c.produced++;
                    if (accepted) {
                        c.accepted++;
                    } else {
                        c.rejected.merge(entry.verdict().reason(), 1L, Long::sum);
                    }
                }
            }
        }

        details.addLast(entry);
        while (details.size() > detailCapacity) {
            details.removeFirst();
            evictedDetails++;
        }
    }

    /** 按任意筛选条件出对账结论。 */
    public synchronized ReconcileReport report(ReconcileQuery query) {
        List<ReconcileEntry> retained = new ArrayList<>();
        for (ReconcileEntry e : details) {
            if (query.matches(e)) {
                retained.add(e);
            }
        }

        if (!query.hasTimeRange()) {
            return reportFromExactCubes(query, retained);
        }
        return reportFromRetainedWindow(query, retained);
    }

    /** run/传感器/规则版本筛选：计数取自精确立方体，明细缺失条数显式给出。 */
    private ReconcileReport reportFromExactCubes(ReconcileQuery query, List<ReconcileEntry> retained) {
        // 查询的每个筛选维度要么是具体值、要么是通配 null，因此唯一定位一个汇总立方体，
        // 绝不能遍历求和（会把通配立方体重复累加）。
        Cube c = cubes.get(new CubeKey(query.runId(), query.sensorId(), query.rulesVersion()));
        long produced = c == null ? 0L : c.produced;
        long accepted = c == null ? 0L : c.accepted;
        EnumMap<RejectReason, Long> reasons = zeroReasons();
        if (c != null) {
            reasons.putAll(c.rejected);
        }

        List<ReconcileEntry> delivered = new ArrayList<>();
        List<ReconcileEntry> rejected = new ArrayList<>();
        for (ReconcileEntry e : retained) {
            (e.verdict().accepted() ? delivered : rejected).add(e);
        }
        long rejectedTotal = reasons.values().stream().mapToLong(Long::longValue).sum();
        long rejectedMissing = rejectedTotal - rejected.size();
        long deliveredMissing = accepted - delivered.size();
        boolean complete = rejectedMissing == 0 && deliveredMissing == 0;
        return new ReconcileReport(produced, accepted, reasons,
                reasons.getOrDefault(RejectReason.BUFFER_OVERFLOW, 0L),
                rejected, delivered, true, complete,
                rejectedMissing, deliveredMissing, retained.size());
    }

    /**
     * 带时间范围：只能基于保留窗口明细统计。窗口是否完整可确定地判断：
     * 若查询区间与“已被顶掉明细所在的 (-∞, 最旧保留时间)”不相交，则仍完整。
     */
    private ReconcileReport reportFromRetainedWindow(ReconcileQuery query, List<ReconcileEntry> retained) {
        boolean complete;
        if (evictedDetails == 0) {
            complete = true;
        } else {
            // 被顶掉明细的入账时间全部严格早于最旧保留时间。只有查询下界不早于该时间时
            // 才能保证区间内一条不丢；任何更早或无下界的时间查询都保守标记为不完整。
            long oldest = details.isEmpty() ? Long.MAX_VALUE : details.peekFirst().recordedAtMs();
            complete = query.fromMillis() >= 0 && query.fromMillis() >= oldest;
        }

        long accepted = 0;
        EnumMap<RejectReason, Long> reasons = zeroReasons();
        List<ReconcileEntry> delivered = new ArrayList<>();
        List<ReconcileEntry> rejected = new ArrayList<>();
        for (ReconcileEntry e : retained) {
            if (e.verdict().accepted()) {
                accepted++;
                delivered.add(e);
            } else {
                reasons.merge(e.verdict().reason(), 1L, Long::sum);
                rejected.add(e);
            }
        }
        long rejectedTotal = reasons.values().stream().mapToLong(Long::longValue).sum();
        return new ReconcileReport(accepted + rejectedTotal, accepted, reasons,
                reasons.getOrDefault(RejectReason.BUFFER_OVERFLOW, 0L),
                rejected, delivered, complete, complete, 0L, 0L, retained.size());
    }

    private static EnumMap<RejectReason, Long> zeroReasons() {
        EnumMap<RejectReason, Long> m = new EnumMap<>(RejectReason.class);
        for (RejectReason r : RejectReason.values()) {
            m.put(r, 0L);
        }
        return m;
    }

    public synchronized List<Long> runIds() {
        return runs.keySet().stream().sorted().toList();
    }

    public int detailCapacity() {
        return detailCapacity;
    }

    public synchronized int retainedDetails() {
        return details.size();
    }

    public synchronized long evictedDetails() {
        return evictedDetails;
    }
}
