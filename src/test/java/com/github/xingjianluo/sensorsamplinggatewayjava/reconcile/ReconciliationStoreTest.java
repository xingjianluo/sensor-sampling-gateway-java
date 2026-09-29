package com.github.xingjianluo.sensorsamplinggatewayjava.reconcile;

import com.github.xingjianluo.sensorsamplinggatewayjava.model.RawReading;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.RejectReason;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.Verdict;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

/** 对账台账单元测试：闭合口径、按代次/传感器/规则版本/时间切片、有界保留与封代清除。 */
class ReconciliationStoreTest {

    private static RawReading reading(String sensor, long tick) {
        return new RawReading(sensor, tick, 1000L + tick, 20.0d + tick, tick * 7L);
    }

    private static Verdict accept(String sensor, long tick, String rules) {
        return Verdict.accept(reading(sensor, tick), 2.0d * (tick + 1), "accept", rules);
    }

    private static Verdict reject(String sensor, long tick, String rules, RejectReason reason) {
        return Verdict.reject(reading(sensor, tick), reason, "reject:" + reason, rules);
    }

    private void assertClosed(ReconciliationReport r) {
        assertEquals(r.total(), r.accepted() + r.rejected(), "total = accepted + rejected");
        long byReason = r.rejectedByReason().values().stream().mapToLong(Long::longValue).sum();
        assertEquals(r.rejected(), byReason, "rejected = sum(byReason)");
        long overflow = r.rejectedByReason().getOrDefault(RejectReason.BUFFER_OVERFLOW, 0L);
        assertEquals(r.evictedOld() + r.rejectedNew(), overflow,
                "BUFFER_OVERFLOW = evictedOld + rejectedNew");
        assertTrue(r.acceptedEntries().size() <= r.accepted(), "accepted detail is subset");
        assertTrue(r.rejectedEntries().size() <= r.rejected(), "rejected detail is subset");
        if (r.complete()) {
            assertEquals(r.accepted(), r.acceptedEntries().size(), "complete: accepted list matches count");
            assertEquals(r.rejected(), r.rejectedEntries().size(), "complete: rejected list matches count");
        }
        Set<String> keys = new HashSet<>();
        r.acceptedEntries().forEach(e -> assertTrue(keys.add(e.generation() + "#" + e.sensorTickKey()),
                "unique accepted key"));
        r.rejectedEntries().forEach(e -> assertTrue(keys.add(e.generation() + "#" + e.sensorTickKey()),
                "unique rejected key"));
    }

    @Test
    void singleGeneration_allDispositions_balancesAndSplitsReasons() {
        ReconciliationStore store = new ReconciliationStore(64, 4, RetentionPolicy.ROLL_OLDEST);
        long g = store.beginGeneration("rules-v1", 1000L);

        store.append(accept("a", 0, "rules-v1"), BackpressureKind.NONE, 1000L);
        store.append(accept("a", 1, "rules-v1"), BackpressureKind.NONE, 1001L);
        store.append(reject("a", 2, "rules-v1", RejectReason.OUT_OF_RANGE), BackpressureKind.NONE, 1002L);
        store.append(reject("a", 3, "rules-v1", RejectReason.TIMESTAMP_ANOMALY), BackpressureKind.NONE, 1003L);
        store.append(reject("a", 4, "rules-v1", RejectReason.DATA_CORRUPT), BackpressureKind.NONE, 1004L);
        store.append(reject("a", 5, "rules-v1", RejectReason.BUFFER_OVERFLOW),
                BackpressureKind.REJECTED_NEW, 1005L);
        store.append(reject("a", 6, "rules-v1", RejectReason.BUFFER_OVERFLOW),
                BackpressureKind.EVICTED_OLD, 1006L);
        store.append(reject("a", 7, "rules-v1", RejectReason.SENSOR_ISOLATED), BackpressureKind.NONE, 1007L);
        store.append(reject("a", 8, "rules-v1", RejectReason.ACQUISITION_FAILED), BackpressureKind.NONE, 1008L);
        store.sealGeneration(1009L);

        ReconciliationReport r = store.reconcile(ReconciliationFilter.forGeneration(g));
        assertClosed(r);
        assertTrue(r.complete(), "no rollover -> complete");
        assertEquals(9, r.total());
        assertEquals(2, r.accepted());
        assertEquals(7, r.rejected());
        assertEquals(1, r.rejectedNew());
        assertEquals(1, r.evictedOld());
        Map<RejectReason, Long> br = r.rejectedByReason();
        assertEquals(1L, br.get(RejectReason.OUT_OF_RANGE));
        assertEquals(1L, br.get(RejectReason.TIMESTAMP_ANOMALY));
        assertEquals(1L, br.get(RejectReason.DATA_CORRUPT));
        assertEquals(2L, br.get(RejectReason.BUFFER_OVERFLOW));
        assertEquals(1L, br.get(RejectReason.SENSOR_ISOLATED));
        assertEquals(1L, br.get(RejectReason.ACQUISITION_FAILED));

        GenerationInfo info = store.generation(g);
        assertTrue(info.sealed());
        assertEquals(9, info.total());
        assertEquals(2, info.backpressure());
        Set<RejectReason> seenReasons = new HashSet<>();
        r.rejectedEntries().forEach(e -> {
            assertNotNull(e.reason());
            seenReasons.add(e.reason());
        });
        assertEquals(6, seenReasons.size(), "all reject reasons present in detail");
    }

    @Test
    void filterBySensorAndRulesVersion_slicesWithoutDoubleCount() {
        ReconciliationStore store = new ReconciliationStore(64, 4, RetentionPolicy.ROLL_OLDEST);
        long g1 = store.beginGeneration("rules-v1", 0L);
        store.append(accept("a", 0, "rules-v1"), BackpressureKind.NONE, 1L);
        store.append(reject("a", 1, "rules-v1", RejectReason.OUT_OF_RANGE), BackpressureKind.NONE, 2L);
        store.append(accept("b", 0, "rules-v1"), BackpressureKind.NONE, 3L);
        store.append(accept("b", 1, "rules-v1"), BackpressureKind.NONE, 4L);
        store.sealGeneration(5L);
        long g2 = store.beginGeneration("rules-v2", 6L);
        store.append(accept("a", 0, "rules-v2"), BackpressureKind.NONE, 7L);
        store.sealGeneration(8L);

        ReconciliationReport all = store.reconcile(ReconciliationFilter.all());
        assertClosed(all);
        assertEquals(5, all.total());
        assertEquals(4, all.accepted());

        ReconciliationReport byA = store.reconcile(ReconciliationFilter.forSensor("a"));
        assertClosed(byA);
        assertEquals(3, byA.total());
        assertEquals(2, byA.accepted());
        assertEquals(1, byA.rejected());
        assertEquals(3, byA.perSensor().get("a").total());

        ReconciliationReport v1 = store.reconcile(ReconciliationFilter.forRulesVersion("rules-v1"));
        assertClosed(v1);
        assertEquals(4, v1.total());
        assertEquals(3, v1.accepted());

        ReconciliationReport g1Only = store.reconcile(ReconciliationFilter.forGeneration(g1));
        assertClosed(g1Only);
        assertEquals(4, g1Only.total());
        assertEquals(1L, g1Only.perSensor().get("a").rejected());
        assertEquals(2L, g1Only.perSensor().get("b").accepted());

        assertNotNull(store.findEntry(g1, "a#0"));
        assertNotNull(store.findEntry(g2, "a#0"));
    }

    @Test
    void timeRangeSlice_filtersOnRecordedAt() {
        ReconciliationStore store = new ReconciliationStore(64, 4, RetentionPolicy.ROLL_OLDEST);
        store.beginGeneration("rules-v1", 0L);
        store.append(accept("a", 0, "rules-v1"), BackpressureKind.NONE, 10L);
        store.append(accept("a", 1, "rules-v1"), BackpressureKind.NONE, 20L);
        store.append(reject("a", 2, "rules-v1", RejectReason.OUT_OF_RANGE), BackpressureKind.NONE, 30L);

        ReconciliationReport mid = store.reconcile(ReconciliationFilter.timeRange(15L, 25L));
        assertClosed(mid);
        assertTrue(mid.complete());
        assertEquals(1, mid.total());
        assertEquals(1, mid.accepted());

        ReconciliationReport upTo = store.reconcile(ReconciliationFilter.timeRange(0L, 20L));
        assertClosed(upTo);
        assertEquals(2, upTo.total()); // closed range includes 20
    }

    @Test
    void rollOldest_aggregatesStayExactButMarkedIncompleteForSlicedDetails() {
        ReconciliationStore store = new ReconciliationStore(3, 4, RetentionPolicy.ROLL_OLDEST);
        long g = store.beginGeneration("rules-v1", 0L);
        for (int i = 0; i < 6; i++) {
            store.append(accept("a", i, "rules-v1"), BackpressureKind.NONE, i);
        }
        ReconciliationReport all = store.reconcile(ReconciliationFilter.forGeneration(g));
        assertClosed(all);
        assertFalse(all.complete(), "details rolled off -> not complete for detail-level claims");
        assertEquals(6, all.total(), "aggregate total stays exact despite rollover");
        assertEquals(6, all.accepted(), "aggregate accepted stays exact");
        assertEquals(3, all.acceptedEntries().size(), "only capacity many details retained");
        assertEquals(3L, all.detailRollover());

        GenerationInfo info = store.generation(g);
        assertEquals(6, info.total());
        assertEquals(3, info.detailRetained());
        assertEquals(3, info.detailRollover());
    }

    @Test
    void ignoreNew_keepsOldestDetailsUntouched() {
        ReconciliationStore store = new ReconciliationStore(3, 4, RetentionPolicy.IGNORE_NEW);
        long g = store.beginGeneration("rules-v1", 0L);
        store.append(accept("a", 0, "rules-v1"), BackpressureKind.NONE, 0L);
        store.append(accept("a", 1, "rules-v1"), BackpressureKind.NONE, 1L);
        store.append(accept("a", 2, "rules-v1"), BackpressureKind.NONE, 2L);
        store.append(reject("a", 3, "rules-v1", RejectReason.OUT_OF_RANGE), BackpressureKind.NONE, 3L);

        ReconciliationReport r = store.reconcile(ReconciliationFilter.forGeneration(g));
        assertClosed(r);
        assertEquals(4, r.total());
        assertEquals(3, r.accepted());
        assertEquals(1, r.rejected(), "reject still counted in aggregate");
        assertEquals(3, r.acceptedEntries().size(), "oldest three kept");
        assertEquals(0, r.rejectedEntries().size(), "newest reject detail ignored, stored unchanged");
        assertEquals(0L, r.acceptedEntries().get(0).tick());
        assertEquals(2L, r.acceptedEntries().get(2).tick());
    }

    @Test
    void sealingEvictsOldestSealedGenerationsButNeverRunning() {
        ReconciliationStore store = new ReconciliationStore(64, 2, RetentionPolicy.ROLL_OLDEST);
        long g1 = store.beginGeneration("rules-v1", 0L);
        store.append(accept("a", 0, "rules-v1"), BackpressureKind.NONE, 0L);
        store.sealGeneration(1L);
        long g2 = store.beginGeneration("rules-v2", 2L);
        store.append(accept("a", 0, "rules-v2"), BackpressureKind.NONE, 2L);
        store.sealGeneration(3L);
        long g3 = store.beginGeneration("rules-v3", 4L);
        store.append(accept("a", 0, "rules-v3"), BackpressureKind.NONE, 4L);
        store.sealGeneration(5L);

        assertNull(store.generation(g1));
        assertNotNull(store.generation(g2));
        assertNotNull(store.generation(g3));
        assertEquals(1L, store.storeStats().generationsPurged());
        assertEquals(2, store.generations().size());

        ReconciliationReport all = store.reconcile(ReconciliationFilter.all());
        assertClosed(all);
        assertEquals(2, all.total());
    }

    @Test
    void appendWithoutRunningGeneration_isDeterministicallyIgnored() {
        ReconciliationStore store = new ReconciliationStore(64, 2, RetentionPolicy.ROLL_OLDEST);
        assertNull(store.append(accept("a", 0, "rules-v1"), BackpressureKind.NONE, 0L));
        assertEquals(1L, store.storeStats().appendsWhileNoRunningGen());
        ReconciliationReport r = store.reconcile(ReconciliationFilter.all());
        assertClosed(r);
        assertEquals(0, r.total());
    }

    @Test
    void reportIsImmutableSnapshot_laterRolloverDoesNotChangeIssuedReport() {
        ReconciliationStore store = new ReconciliationStore(64, 2, RetentionPolicy.ROLL_OLDEST);
        store.beginGeneration("rules-v1", 0L);
        store.append(accept("a", 0, "rules-v1"), BackpressureKind.NONE, 0L);
        ReconciliationReport before = store.reconcile(ReconciliationFilter.all());
        assertEquals(1, before.total());
        store.append(reject("a", 1, "rules-v1", RejectReason.OUT_OF_RANGE), BackpressureKind.NONE, 1L);
        assertEquals(1, before.total(), "issued report unchanged");
        assertEquals(1, before.acceptedEntries().size());
    }
}
