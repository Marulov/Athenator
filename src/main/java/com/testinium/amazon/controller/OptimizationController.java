package com.testinium.amazon.controller;

import com.testinium.amazon.dto.CrawlerRequestDto;
import com.testinium.amazon.dto.MergeFileRequestDto;
import com.testinium.amazon.service.OptimizationService;
import com.testinium.amazon.utilities.results.Result;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/optimize")
@RequiredArgsConstructor
public class OptimizationController {

    private final OptimizationService optimizationService;

    @PostMapping("/create-crawler")
    @ResponseBody
    public ResponseEntity<?> createCrawler(@RequestBody CrawlerRequestDto crawlerRequestDto) {
        Result result = optimizationService.createCrawler(crawlerRequestDto);
        if (result.isSuccess()){
            return ResponseEntity.ok(result.getMessage());
        }
        return ResponseEntity.badRequest().body(result.getMessage());
    }

    @DeleteMapping("/delete-old-file")
    @ResponseBody
    public ResponseEntity<?> deleteOldFile(@RequestBody MergeFileRequestDto mergeFileRequestDto) {
        Result result = optimizationService.deleteOldFile(mergeFileRequestDto);
        if (result.isSuccess()){
            return ResponseEntity.ok(result);
        }
        return ResponseEntity.badRequest().body(result);
    }

    @PostMapping("/create-merge-file")
    @ResponseBody
    public ResponseEntity<?> createMergeFile(@RequestBody MergeFileRequestDto mergeFileRequestDto) throws InterruptedException {
        return ResponseEntity.ok(optimizationService.runAthenaQuery(mergeFileRequestDto));
    }

    @GetMapping("/check-query-status/{queryExecutionId}")
    @ResponseBody
    public ResponseEntity<?> checkQueryStatus(@PathVariable("queryExecutionId") String queryExecutionId) {
        return ResponseEntity.ok(optimizationService.checkQueryStatus(queryExecutionId));
    }
}

