package com.testinium.amazon.service;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.testinium.amazon.config.S3Configuration;
import com.testinium.amazon.dto.MergeFileRequestDto;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class S3Service {

    private final S3Configuration s3Configuration;

    public void deleteFileAndFolder(MergeFileRequestDto mergeFileRequestDto) {
        String newFolderName = mergeFileRequestDto.getExternalLocation().substring(s3Configuration.getOutputBucket().length());
        List<S3ObjectSummary> fileList = s3Configuration.amazonS3().listObjects(s3Configuration.getS3BucketName(), newFolderName).getObjectSummaries();
        for (S3ObjectSummary file : fileList) {
            s3Configuration.amazonS3().deleteObject(s3Configuration.getS3BucketName(), file.getKey());
        }
    }
}

