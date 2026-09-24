package com.github.xingjianluo.sensorsamplinggatewayjava.config;

import com.github.xingjianluo.sensorsamplinggatewayjava.gateway.DefaultSamplingGateway;
import com.github.xingjianluo.sensorsamplinggatewayjava.gateway.SamplingGateway;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring 装配：提供默认采样网关门面（启动时空载，传感器通过 attach 或演示配置接入）。
 */
@Configuration
public class GatewayConfiguration {

    @Bean
    public SamplingGateway samplingGateway(GatewayProperties properties) {
        return new DefaultSamplingGateway(properties, System::currentTimeMillis);
    }
}
