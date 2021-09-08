package com.testinium.amazon.service;

import com.testinium.amazon.config.AthenaConfiguration;
import com.testinium.amazon.dto.MergeFileRequestDto;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.athena.model.*;

import java.util.logging.Logger;

import static com.testinium.amazon.common.QueryTemplates.ATHENA_CREATE_TABLE_QUERY;
import static com.testinium.amazon.common.QueryTemplates.ATHENA_DELETE_QUERY;


@RequiredArgsConstructor
@Service
public class AthenaService {

    public static final long SLEEP_AMOUNT_IN_MS = 1000;
    private final Logger logger = Logger.getLogger("AthenaService");
    private final AthenaConfiguration athenaConfiguration;

    public void startMergeFilesQuery(MergeFileRequestDto mergeFileRequestDto) throws AthenaException, InterruptedException {
        String queryExecutionId = createTable(mergeFileRequestDto.getExternalLocation(), mergeFileRequestDto.getTable());
        waitForQueryToComplete(queryExecutionId);
        logger.info(queryExecutionId + " is started.");

    }

    private String createTable(String externalLocation, String table) {
        return this.submitAthenaQuery(ATHENA_CREATE_TABLE_QUERY.replace("{format}", "JSON")
                .replace("{external_location}", externalLocation)
                .replace("{table}", table));
    }

    private String submitAthenaQuery(String query) throws AthenaException {
        QueryExecutionContext queryExecutionContext = QueryExecutionContext.builder()
                .database(athenaConfiguration.getDefaultDb()).build();

        ResultConfiguration resultConfiguration = ResultConfiguration.builder()
                .outputLocation(athenaConfiguration.getOutputBucket())
                .build();

        StartQueryExecutionRequest startQueryExecutionRequest = StartQueryExecutionRequest.builder()
                .queryString(query)
                .queryExecutionContext(queryExecutionContext)
                .resultConfiguration(resultConfiguration)
                .build();

        StartQueryExecutionResponse startQueryExecutionResponse = athenaConfiguration.athenaClient().startQueryExecution(startQueryExecutionRequest);
        return startQueryExecutionResponse.queryExecutionId();
    }

     public QueryExecutionState getQueryState(String queryExecutionId) {
        GetQueryExecutionRequest getQueryExecutionRequest = GetQueryExecutionRequest.builder()
                .queryExecutionId(queryExecutionId).build();

        GetQueryExecutionResponse getQueryExecutionResponse = athenaConfiguration.athenaClient().getQueryExecution(getQueryExecutionRequest);
        return getQueryExecutionResponse.queryExecution().status().state();

    }

    private void waitForQueryToComplete(String queryExecutionId) throws InterruptedException {
        GetQueryExecutionRequest getQueryExecutionRequest = GetQueryExecutionRequest.builder()
                .queryExecutionId(queryExecutionId).build();

        GetQueryExecutionResponse getQueryExecutionResponse;
        boolean isQueryStillRunning = true;
        while (isQueryStillRunning) {
            getQueryExecutionResponse = athenaConfiguration.athenaClient().getQueryExecution(getQueryExecutionRequest);
            String queryState = getQueryExecutionResponse.queryExecution().status().state().toString();
            if (queryState.equals(QueryExecutionState.FAILED.toString())) {
                throw new RuntimeException("The Amazon Athena query failed to run with error message: " + getQueryExecutionResponse
                        .queryExecution().status().stateChangeReason());
            } else if (queryState.equals(QueryExecutionState.CANCELLED.toString())) {
                throw new RuntimeException("The Amazon Athena query was cancelled.");
            } else if (queryState.equals(QueryExecutionState.SUCCEEDED.toString())) {
                isQueryStillRunning = false;
            } else {

                Thread.sleep(SLEEP_AMOUNT_IN_MS);
            }
        }
    }

    public String startDeleteTableQuery() throws AthenaException, InterruptedException {
        String queryExecutionId = submitAthenaQuery(ATHENA_DELETE_QUERY);
        waitForQueryToComplete(queryExecutionId);
        logger.info(queryExecutionId + " is started.");
        return queryExecutionId;
    }
}

