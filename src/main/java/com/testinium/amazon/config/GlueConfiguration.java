package com.testinium.amazon.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Configuration
public class GlueConfiguration {

    @Value("${glue.iam}")
    private String iam;

    @Value("${glue.cron}")
    private String cron;

    @Value("${glue.databaseName}")
    private String databaseName;

    @Bean
    public GlueClient glueClient() {
        Region region = Region.US_EAST_2;
        return GlueClient.builder()
                .region(region)
                .build();
    }
}

