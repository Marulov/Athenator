package com.testinium.amazon.service;

import com.testinium.amazon.dto.CrawlerRequestDto;
import com.testinium.amazon.dto.MergeFileRequestDto;
import com.testinium.amazon.utilities.results.Result;
import com.testinium.amazon.utilities.results.SuccessResult;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.logging.Logger;

@Service
@RequiredArgsConstructor
public class OptimizationService {

    private final Logger logger = Logger.getLogger("OptimizationService");
    private final AthenaService athenaService;
    private final S3Service s3Service;
    private final GlueService glueService;

    public Result deleteOldFile(MergeFileRequestDto mergeFileRequestDto) {
        s3Service.deleteFileAndFolder(mergeFileRequestDto);
        logger.info("Delete old file");

        return new SuccessResult("Delete old file");
    }

    public Result createCrawler(CrawlerRequestDto crawlerRequestDto) {
        glueService.createCrawler(crawlerRequestDto);
        logger.info("Crawler created");

        return new SuccessResult("Crawler created");
    }

    public String runAthenaQuery(MergeFileRequestDto mergeFileRequestDto) throws InterruptedException {
        athenaService.startMergeFilesQuery(mergeFileRequestDto);
        logger.info("Athena query runned");
        return athenaService.startDeleteTableQuery();
    }

    public String checkQueryStatus(String queryExecutionId) {
        return athenaService.getQueryState(queryExecutionId).toString();
    }
}

