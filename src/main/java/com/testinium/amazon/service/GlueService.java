package com.testinium.amazon.service;

import com.testinium.amazon.config.GlueConfiguration;
import com.testinium.amazon.dto.CrawlerRequestDto;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

@RequiredArgsConstructor
@Service
public class GlueService {

    private final Logger logger = Logger.getLogger("GlueService");

    private final GlueConfiguration glueConfiguration;
    private String crawlerName;

    public void createCrawler(CrawlerRequestDto crawlerRequestDto) {
        this.crawlerName = crawlerRequestDto.getCrawlerName();
        createGlueCrawler(crawlerRequestDto.getS3Uri(), crawlerRequestDto.getCrawlerName());
        startCrawler();
        glueConfiguration.glueClient().close();
    }

    public void startCrawler() {
        Region region = Region.US_EAST_2;
        GlueClient glueClient = GlueClient.builder()
                .region(region)
                .build();

        startSpecificCrawler();
        glueClient.close();
    }

    public void startSpecificCrawler() {
        try {
            StartCrawlerRequest crawlerRequest = StartCrawlerRequest.builder()
                    .name(crawlerName)
                    .build();

            glueConfiguration.glueClient().startCrawler(crawlerRequest);
        } catch (GlueException e) {
            logger.info(e.awsErrorDetails().errorMessage());
        }
    }

    public void createGlueCrawler(String s3Uri, String crawlerName) {
        try {
            S3Target s3Target = S3Target.builder()
                    .path(s3Uri)
                    .build();

            // Add the S3Target to a list
            List<S3Target> targetList = new ArrayList<>();
            targetList.add(s3Target);

            CrawlerTargets targets = CrawlerTargets.builder()
                    .s3Targets(targetList)
                    .build();

            CreateCrawlerRequest crawlerRequest = CreateCrawlerRequest.builder()
                    .databaseName(glueConfiguration.getDatabaseName())
                    .name(crawlerName)
                    .description("Created by the AWS Glue Java API")
                    .targets(targets)
                    .role(glueConfiguration.getIam())
                    .schedule(glueConfiguration.getCron())
                    .build();

            glueConfiguration.glueClient().createCrawler(crawlerRequest);
            logger.info(crawlerName + " was successfully created");

        } catch (GlueException e) {
            logger.info(e.awsErrorDetails().errorMessage());
        }
    }
}

