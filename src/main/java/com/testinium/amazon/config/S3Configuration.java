package com.testinium.amazon.config;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import lombok.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.logging.Logger;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Configuration
public class S3Configuration {

    private final Logger logger = Logger.getLogger("S3Config");

    @Value("${s3.accessKey}")
    private String s3AccessKey;

    @Value("${s3.secretKey}")
    private String s3SecretKey;

    @Value("${s3.bucketName}")
    private String s3BucketName;

    @Value("${s3.outputBucket}")
    private String outputBucket;

    @Bean
    public AmazonS3 amazonS3() {
        return AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(s3AccessKey, s3SecretKey)))
                .withClientConfiguration(clientConfiguration())
                .withRegion(Regions.US_EAST_2)
                .build();
    }

    private ClientConfiguration clientConfiguration() {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setMaxConnections(100);
        clientConfiguration.setMaxErrorRetry(5);
        clientConfiguration.setProtocol(Protocol.HTTP);
        return clientConfiguration;
    }
}

