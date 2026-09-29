package com.github.xingjianluo.sensorsamplinggatewayjava;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.RawReading;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.RejectReason;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.Verdict;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.ReconcileEntry;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.ReconcileLedger;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.ReconcileQuery;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.ReconcileReport;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 对账总账单测：恒等式闭合、按 run/传感器/规则版本筛选、FIFO 保留边界、
 * 时间范围完整性、同 run 内重复键拒绝。
 */
class ReconcileLedgerTest {

    private static RawReading reading(String sensor, long tick) {
        return new RawReading(sensor, tick, 1000L, 20.0, 123L);
    }

    private static ReconcileEntry accept(long run, long at, String sensor, long tick, String rules) {
        return new ReconcileEntry(run, at, Verdict.accept(reading(sensor, tick), 20.0, "ok", rules), false);
    }

    private static ReconcileEntry reject(long run, long at, String sensor, long tick,
                                         String rules, RejectReason reason, boolean evicted) {
        return new ReconcileEntry(run, at,
                Verdict.reject(reading(sensor, tick), reason, "bad", rules), evicted);
    }

    @Test
    void allReasonBreakdownsSumToProducedAndDeliveredEqualsAccepted() {
        ReconcileLedger ledger = new ReconcileLedger(100);
        long run = ledger.beginRun(0L);
        ledger.append(accept(run, 1, "s1", 0, "v1"));
        ledger.append(accept(run, 2, "s1", 1, "v1"));
        ledger.append(reject(run, 3, "s1", 2, "v1", RejectReason.OUT_OF_RANGE, false));
        ledger.append(reject(run, 4, "s1", 3, "v1", RejectReason.DATA_CORRUPT, false));
        ledger.append(reject(run, 5, "s1", 4, "v1", RejectReason.TIMESTAMP_ANOMALY, false));
        ledger.append(reject(run, 6, "s1", 5, "v1", RejectReason.BUFFER_OVERFLOW, false));
        // 顶掉（DROP_OLDEST）与拒入（DROP_NEWEST）都落 BUFFER_OVERFLOW
        ledger.append(reject(run, 7, "s1", 6, "v1", RejectReason.BUFFER_OVERFLOW, true));

        ReconcileReport r = ledger.report(ReconcileQuery.ofRun(run));
        assertEquals(7, r.produced());
        assertEquals(2, r.accepted());
        assertEquals(1L, r.rejectedByReason().get(RejectReason.OUT_OF_RANGE));
        assertEquals(1L, r.rejectedByReason().get(RejectReason.DATA_CORRUPT));
        assertEquals(1L, r.rejectedByReason().get(RejectReason.TIMESTAMP_ANOMALY));
        assertEquals(2L, r.rejectedByReason().get(RejectReason.BUFFER_OVERFLOW));
        assertEquals(2L, r.dropped(), "both rejected-newest and evicted-oldest count as dropped");
        assertEquals(2, r.deliveredDetail().size());
        assertEquals(5, r.rejectedDetail().size());
        assertTrue(r.countersExact());
        assertTrue(r.complete());
    }

    @Test
    void filtersBySensorAndRulesVersion() {
        ReconcileLedger ledger = new ReconcileLedger(100);
        long run = ledger.beginRun(0L);
        ledger.append(accept(run, 1, "a", 0, "v1"));
        ledger.append(accept(run, 2, "b", 0, "v1"));
        ledger.append(reject(run, 3, "a", 1, "v2", RejectReason.OUT_OF_RANGE, false));
        ledger.append(accept(run, 4, "a", 2, "v2"));

        ReconcileReport aOnly = ledger.report(
                new ReconcileQuery(run, "a", null, -1, -1));
        assertEquals(3, aOnly.produced());
        assertEquals(2, aOnly.accepted());

        ReconcileReport aV2 = ledger.report(new ReconcileQuery(run, "a", "v2", -1, -1));
        assertEquals(2, aV2.produced());
        assertEquals(1, aV2.accepted());
        assertEquals(1L, aV2.rejectedByReason().get(RejectReason.OUT_OF_RANGE));

        ReconcileReport v1 = ledger.report(new ReconcileQuery(run, null, "v1", -1, -1));
        assertEquals(2, v1.produced());
        assertEquals(2, v1.accepted());
    }

    @Test
    void detailRetentionEvictsOldestButKeepsExactCountersAndReportsMissing() {
        ReconcileLedger ledger = new ReconcileLedger(3);
        long run = ledger.beginRun(0L);
        ledger.append(accept(run, 1, "s", 0, "v1"));
        ledger.append(accept(run, 2, "s", 1, "v1"));
        ledger.append(reject(run, 3, "s", 2, "v1", RejectReason.OUT_OF_RANGE, false));
        ledger.append(accept(run, 4, "s", 3, "v1")); // 顶掉第 1 条
        ledger.append(accept(run, 5, "s", 4, "v1")); // 顶掉第 2 条

        ReconcileReport r = ledger.report(ReconcileQuery.ofRun(run));
        assertEquals(5, r.produced(), "exact aggregate unaffected by detail eviction");
        assertEquals(4, r.accepted());
        assertEquals(1L, r.rejectedByReason().get(RejectReason.OUT_OF_RANGE));
        assertTrue(r.countersExact());
        assertFalse(r.complete(), "details were truncated");
        assertEquals(2L, r.deliveredMissing(), "two accepted details evicted");
        assertEquals(0L, r.rejectedMissing());
        assertEquals(3, r.retainedDetailCount());
        assertEquals(2L, ledger.evictedDetails());
    }

    @Test
    void timeRangeAfterOldestRetainedIsCompleteAndClosed() {
        ReconcileLedger ledger = new ReconcileLedger(2);
        long run = ledger.beginRun(0L);
        ledger.append(accept(run, 10, "s", 0, "v1"));
        ledger.append(accept(run, 20, "s", 1, "v1"));
        ledger.append(accept(run, 30, "s", 2, "v1")); // 顶掉 t=10

        // 下界 >= 最旧保留时间(20)：区间内一条不丢
        ReconcileReport r = ledger.report(new ReconcileQuery(null, null, null, 20, 30));
        assertTrue(r.complete());
        assertEquals(2, r.produced());
        assertEquals(2, r.accepted());

        // 下界早于最旧保留时间：保守标记不完整，只统计窗口内命中条目但仍闭合
        ReconcileReport partial = ledger.report(new ReconcileQuery(null, null, null, 5, 30));
        assertFalse(partial.complete());
        assertFalse(partial.countersExact());
        assertEquals(partial.produced(),
                partial.accepted() + partial.rejectedByReason().values().stream()
                        .mapToLong(Long::longValue).sum());
    }

    @Test
    void timeRangeAlsoRespectsSensorAndRulesFilters() {
        ReconcileLedger ledger = new ReconcileLedger(100);
        long run = ledger.beginRun(0L);
        ledger.append(accept(run, 10, "a", 0, "v1"));
        ledger.append(accept(run, 20, "b", 0, "v1"));
        ledger.append(reject(run, 30, "a", 1, "v2", RejectReason.OUT_OF_RANGE, false));

        ReconcileQuery q = new ReconcileQuery(null, "a", "v2", 0L, 100L);
        ReconcileReport r = ledger.report(q);
        assertEquals(1, r.produced());
        assertEquals(0, r.accepted());
        assertEquals(1L, r.rejectedByReason().get(RejectReason.OUT_OF_RANGE));
        assertTrue(r.complete());
    }

    @Test
    void duplicateSensorTickWithinRunIsRejected() {
        ReconcileLedger ledger = new ReconcileLedger(10);
        long run = ledger.beginRun(0L);
        ledger.append(accept(run, 1, "s", 0, "v1"));
        assertThrows(IllegalStateException.class,
                () -> ledger.append(accept(run, 2, "s", 0, "v1")));
    }

    @Test
    void sealedRunRejectsNoMoreEntriesButStaysQueryableAcrossRuns() {
        ReconcileLedger ledger = new ReconcileLedger(100);
        long r1 = ledger.beginRun(0L);
        ledger.append(accept(r1, 1, "s", 0, "v1"));
        ledger.sealRun(2L);
        long r2 = ledger.beginRun(3L);
        ledger.append(accept(r2, 4, "s", 0, "v2")); // 同 tick 但不同 run，允许

        assertThrows(IllegalStateException.class,
                () -> ledger.append(accept(r1, 5, "s", 1, "v1")));

        assertEquals(1, ledger.report(ReconcileQuery.ofRun(r1)).produced());
        assertEquals(1, ledger.report(ReconcileQuery.ofRun(r2)).produced());
        assertEquals(2, ledger.report(ReconcileQuery.all()).produced());
        assertEquals(1, ledger.report(new ReconcileQuery(null, null, "v1", -1, -1)).produced());
    }
}
