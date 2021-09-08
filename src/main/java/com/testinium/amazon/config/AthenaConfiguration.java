package com.testinium.amazon.config;

import lombok.*;
import org.springframework.beans.factory.annotation.Value;
import software.amazon.awssdk.regions.Region;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.athena.AthenaClient;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Configuration
public class AthenaConfiguration {

    @Value("${athena.defaultDb}")
    private String defaultDb;

    @Value("${athena.outputBucket}")
    private String outputBucket;

    @Bean
    public AthenaClient athenaClient() {
        return  AthenaClient.builder()
                .region(Region.US_EAST_2)
                .build();
    }
}

