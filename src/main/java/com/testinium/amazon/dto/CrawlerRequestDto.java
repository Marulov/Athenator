package com.testinium.amazon.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CrawlerRequestDto {
    private String s3Uri;
    private String crawlerName;
}

