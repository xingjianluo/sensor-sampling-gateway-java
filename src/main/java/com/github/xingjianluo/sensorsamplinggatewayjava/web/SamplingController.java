package com.github.xingjianluo.sensorsamplinggatewayjava.web;

import com.github.xingjianluo.sensorsamplinggatewayjava.gateway.GatewayLifecycle;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.GatewayStats;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.Verdict;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.ReconciliationEntry;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.ReconciliationFilter;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.ReconciliationReport;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.ReconciliationStoreStats;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 本地验证用只读端点：状态/统计/裁决记录，以及采样对账。 */
@RestController
@RequestMapping("/api/gateway")
public class SamplingController {

    private final GatewayLifecycle lifecycle;

    public SamplingController(GatewayLifecycle lifecycle) {
        this.lifecycle = lifecycle;
    }

    @GetMapping("/status")
    public Map<String, Object> status() {
        var gw = lifecycle.gateway();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("running", gw.isRunning());
        out.put("rulesVersion", gw.config().rulesVersion());
        out.put("calibrationVersion", gw.config().calibration().getVersion());
        out.put("bufferCapacity", gw.config().bufferCapacity());
        out.put("overflowPolicy", gw.config().overflowPolicy());
        out.put("queued", gw.bufferView().queued());
        long genId = gw.reconciliation().runningGeneration();
        out.put("runningGeneration", genId);
        // 当前运行代次内出现过的全部规则版本（运行中切换会在同一批次内追加新版本）
        com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.GenerationInfo gen =
                genId >= 1L ? gw.reconciliation().generation(genId) : null;
        out.put("runningRulesVersions",
                gen == null ? List.of() : gen.rulesVersions());
        return out;
    }

    @GetMapping("/stats")
    public GatewayStats stats() {
        return lifecycle.gateway().stats();
    }

    @GetMapping("/verdicts")
    public List<Verdict> verdicts(@RequestParam(required = false) String sensor) {
        if (sensor == null || sensor.isBlank()) {
            return lifecycle.gateway().verdictLog().recent();
        }
        return lifecycle.gateway().verdictLog().bySensor(sensor);
    }

    @GetMapping("/verdicts/{key}")
    public Map<String, Object> verdictByKey(@PathVariable String key) {
        Verdict v = lifecycle.gateway().verdictLog().byKey(key);
        Map<String, Object> out = new LinkedHashMap<>();
        if (v == null) {
            out.put("found", false);
            out.put("key", key);
        } else {
            out.put("found", true);
            out.put("verdict", v);
        }
        return out;
    }

    // ===== 采样对账 =====

    /**
     * 对账查询。可组合筛选：generation（批次）、sensor、rulesVersion、
     * from/to（按网关记录时间 recordedAt 的闭区间，毫秒）。
     */
    @GetMapping("/reconcile")
    public ReconciliationReport reconcile(@RequestParam(required = false) Long generation,
                                          @RequestParam(required = false) String sensor,
                                          @RequestParam(required = false) String rulesVersion,
                                          @RequestParam(required = false) Long from,
                                          @RequestParam(required = false) Long to) {
        ReconciliationFilter f = new ReconciliationFilter(
                generation == null ? 0L : generation,
                sensor, rulesVersion,
                from == null ? 0L : from,
                to == null ? 0L : to);
        return lifecycle.gateway().reconciliation().reconcile(f);
    }

    /** 批次（代次）列表：含每批精确聚合计数与明细保留情况。 */
    @GetMapping("/reconcile/generations")
    public List<?> generations() {
        return lifecycle.gateway().reconciliation().generations();
    }

    /** 单批次对账（等价于 reconcile?generation={id}，便于直接核对任意一次运行）。 */
    @GetMapping("/reconcile/generations/{id}")
    public ReconciliationReport generationReport(@PathVariable long id) {
        return lifecycle.gateway().reconciliation().reconcile(ReconciliationFilter.forGeneration(id));
    }

    /** 按代次 + sensor#tick 精确取单条已保留明细（generation 留空取当前运行代）。 */
    @GetMapping("/reconcile/entries/{key}")
    public Map<String, Object> entry(@PathVariable String key,
                                     @RequestParam(required = false) Long generation) {
        ReconciliationEntry e = lifecycle.gateway().reconciliation()
                .findEntry(generation == null ? 0L : generation, key);
        Map<String, Object> out = new LinkedHashMap<>();
        if (e == null) {
            out.put("found", false);
            out.put("generation", generation);
            out.put("key", key);
        } else {
            out.put("found", true);
            out.put("entry", e);
        }
        return out;
    }

    /** 台账容量与保留边界统计。 */
    @GetMapping("/reconcile/store")
    public ReconciliationStoreStats reconcileStore() {
        return lifecycle.gateway().reconciliation().storeStats();
    }
}
