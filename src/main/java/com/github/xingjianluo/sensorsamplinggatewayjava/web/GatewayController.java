package com.github.xingjianluo.sensorsamplinggatewayjava.web;

import com.github.xingjianluo.sensorsamplinggatewayjava.gateway.SamplingGateway;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SampleOutcome;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.Sample;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.SensorSpec;
import com.github.xingjianluo.sensorsamplinggatewayjava.model.StatsSnapshot;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 只读可观测接口：健康检查、统计、当前传感器/规则版本与已交付结论。
 */
@RestController
@RequestMapping("/api/gateway")
public class GatewayController {

    private final SamplingGateway gateway;

    public GatewayController(SamplingGateway gateway) {
        this.gateway = gateway;
    }

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        StatsSnapshot s = gateway.stats();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("produced", s.produced());
        body.put("accepted", s.accepted());
        body.put("rejected", s.rejected());
        body.put("retries", s.retries());
        body.put("isolatedSensors", s.isolatedSensors());
        body.put("buffered", s.buffered());
        return body;
    }

    @GetMapping("/sensors")
    public Map<String, Object> sensors() {
        Map<String, Object> body = new LinkedHashMap<>();
        for (Map.Entry<String, SensorSpec> e : gateway.sensors().entrySet()) {
            SensorSpec sp = e.getValue();
            Map<String, Object> view = new LinkedHashMap<>();
            view.put("type", sp.type());
            view.put("periodMs", sp.periodMs());
            view.put("range", List.of(sp.minValue(), sp.maxValue()));
            view.put("calibration", Map.of("scale", sp.scale(), "offset", sp.offset()));
            view.put("ruleVersion", sp.ruleVersion());
            body.put(e.getKey(), view);
        }
        return body;
    }

    @GetMapping("/outcomes")
    public Map<String, Object> outcomes() {
        List<Map<String, Object>> list = gateway.delivered().stream()
                .map(GatewayController::outcomeView)
                .toList();
        return Map.of("count", list.size(), "outcomes", list);
    }

    @GetMapping("/health")
    public Map<String, Object> health() {
        return Map.of("status", "UP", "sensors", gateway.sensors().size());
    }

    private static Map<String, Object> outcomeView(SampleOutcome o) {
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("sensorId", o.sensorId());
        view.put("seq", o.seq());
        if (o instanceof SampleOutcome.Accepted a) {
            Sample s = a.sample();
            view.put("result", "ACCEPTED");
            view.put("rawValue", s.rawValue());
            view.put("calibratedValue", s.calibratedValue());
            view.put("ruleVersion", s.ruleVersion());
        } else if (o instanceof SampleOutcome.Rejected r) {
            view.put("result", "REJECTED");
            view.put("reason", r.reason());
            view.put("basis", r.basis());
        }
        return view;
    }
}
