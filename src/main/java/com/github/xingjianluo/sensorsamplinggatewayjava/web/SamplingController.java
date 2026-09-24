package com.github.xingjianluo.sensorsamplinggatewayjava.web;

import com.github.xingjianluo.sensorsamplinggatewayjava.gateway.GatewayLifecycle;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.GatewayStats;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.Verdict;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 本地验证用只读端点：状态/统计/裁决记录（含输入与判定依据）。 */
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
}
