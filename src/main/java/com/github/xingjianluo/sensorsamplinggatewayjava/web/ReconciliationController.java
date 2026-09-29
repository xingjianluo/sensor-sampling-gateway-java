package com.github.xingjianluo.sensorsamplinggatewayjava.web;

import com.github.xingjianluo.sensorsamplinggatewayjava.gateway.GatewayLifecycle;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.ReconcileLedger;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.ReconcileQuery;
import com.github.xingjianluo.sensorsamplinggatewayjava.reconcile.ReconcileReport;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 只读对账端点：按运行代次/传感器/规则版本/时间范围出闭合的采样对账结论。
 */
@RestController
@RequestMapping("/api/reconcile")
public class ReconciliationController {

    private final GatewayLifecycle lifecycle;

    public ReconciliationController(GatewayLifecycle lifecycle) {
        this.lifecycle = lifecycle;
    }

    @GetMapping("/report")
    public ReconcileReport report(@RequestParam(required = false) Long runId,
                                  @RequestParam(required = false) String sensor,
                                  @RequestParam(required = false) String rulesVersion,
                                  @RequestParam(required = false) Long fromMillis,
                                  @RequestParam(required = false) Long toMillis) {
        ReconcileQuery query = new ReconcileQuery(runId,
                blankToNull(sensor), blankToNull(rulesVersion),
                fromMillis == null ? -1L : fromMillis,
                toMillis == null ? -1L : toMillis);
        return ledger().report(query);
    }

    @GetMapping("/runs")
    public List<Long> runs() {
        return ledger().runIds();
    }

    private ReconcileLedger ledger() {
        return lifecycle.gateway().reconcileLedger();
    }

    private static String blankToNull(String s) {
        return s == null || s.isBlank() ? null : s;
    }
}
