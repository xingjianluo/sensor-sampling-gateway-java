package com.github.xingjianluo.sensorsamplinggatewayjava.tests;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 对账只读端点冒烟测试：启动真实（本地演示）上下文，验证
 * /api/gateway/reconcile* 可访问、返回闭合数字，批次与单条明细端点可用。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ReconciliationEndpointTests {

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    @Test
    @SuppressWarnings("unchecked")
    void reconcileEndpointsReturnBalancedData() throws Exception {
        // 让演示周期产生若干裁决（默认最快 200ms 一拍）
        Thread.sleep(1500);

        ResponseEntity<Map> status = rest.getForEntity(url("/api/gateway/status"), Map.class);
        assertEquals(200, status.getStatusCode().value());
        Object runningGen = status.getBody().get("runningGeneration");
        assertNotNull(runningGen);
        assertTrue(((Number) runningGen).longValue() >= 1L);

        ResponseEntity<Map> reportEntity = rest.getForEntity(url("/api/gateway/reconcile"), Map.class);
        assertEquals(200, reportEntity.getStatusCode().value());
        Map<String, Object> report = reportEntity.getBody();
        long total = ((Number) report.get("total")).longValue();
        long accepted = ((Number) report.get("accepted")).longValue();
        long rejected = ((Number) report.get("rejected")).longValue();
        long evictedOld = ((Number) report.get("evictedOld")).longValue();
        assertTrue(total > 0, "demo run should produce verdicts");
        assertEquals(total, accepted + rejected + evictedOld,
                "endpoint report balanced under legacy split semantics");

        Map<String, Number> byReason = (Map<String, Number>) report.get("rejectedByReason");
        long sumByReason = byReason.values().stream().mapToLong(Number::longValue).sum();
        assertEquals(rejected, sumByReason);

        ResponseEntity<List> gens = rest.getForEntity(url("/api/gateway/reconcile/generations"), List.class);
        assertEquals(200, gens.getStatusCode().value());
        assertTrue(gens.getBody().size() >= 1);

        ResponseEntity<Map> store = rest.getForEntity(url("/api/gateway/reconcile/store"), Map.class);
        assertEquals(200, store.getStatusCode().value());
        assertEquals(4096, ((Number) store.getBody().get("detailCapacityPerGeneration")).intValue());
        assertEquals("ROLL_OLDEST", store.getBody().get("retentionPolicy"));

        // 单条明细（当前运行代，temp-1#0 一定已在 demo 中产生）
        ResponseEntity<Map> entry = rest.getForEntity(
                url("/api/gateway/reconcile/entries/{key}"), Map.class, "temp-1#0");
        assertEquals(200, entry.getStatusCode().value());
        assertEquals(Boolean.TRUE, entry.getBody().get("found"),
                "entry lookup body=" + entry.getBody());
    }
}
