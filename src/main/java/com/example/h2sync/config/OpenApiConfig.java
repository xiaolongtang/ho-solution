package com.example.h2sync.config;

import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springdoc.core.models.GroupedOpenApi;

@Configuration
public class OpenApiConfig {
    @Bean
    public GroupedOpenApi api() {
        return GroupedOpenApi.builder()
                .group("default")
                .addOpenApiCustomizer(openApi -> openApi.setInfo(new Info()
                        .title("H2 Read API")
                        .version("1.0.0")
                        .description("Simple SQL read API (SELECT only) against embedded H2")))
                .packagesToScan("com.example.h2sync.controller")
                .build();
    }
}
