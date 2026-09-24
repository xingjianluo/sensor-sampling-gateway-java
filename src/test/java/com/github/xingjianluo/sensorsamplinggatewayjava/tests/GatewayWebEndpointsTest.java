package com.github.xingjianluo.sensorsamplinggatewayjava.tests;

import com.github.xingjianluo.sensorsamplinggatewayjava.config.GatewayConfiguration;
import com.github.xingjianluo.sensorsamplinggatewayjava.config.GatewayProperties;
import com.github.xingjianluo.sensorsamplinggatewayjava.gateway.SamplingGateway;
import com.github.xingjianluo.sensorsamplinggatewayjava.web.GatewayController;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 只读观测接口切片测试：验证健康、统计、传感器、结论四个端点可访问且字段齐全。
 */
@WebMvcTest(controllers = GatewayController.class)
@EnableConfigurationProperties(GatewayProperties.class)
@Import(GatewayConfiguration.class)
class GatewayWebEndpointsTest {

    @Autowired
    private MockMvc mvc;

    @Autowired
    private SamplingGateway gateway;

    @Test
    void endpointsExposeObservabilityFields() throws Exception {
        System.out.println("[web] gateway bean = " + gateway.getClass().getSimpleName());
        mvc.perform(get("/api/gateway/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"));

        mvc.perform(get("/api/gateway/stats"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.produced").exists())
                .andExpect(jsonPath("$.accepted").exists())
                .andExpect(jsonPath("$.rejected").exists())
                .andExpect(jsonPath("$.buffered").exists());

        mvc.perform(get("/api/gateway/sensors"))
                .andExpect(status().isOk());

        mvc.perform(get("/api/gateway/outcomes"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.count").exists());

        System.out.println("[web] /health /stats /sensors /outcomes all reachable");
    }
}
