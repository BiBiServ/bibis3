package de.unibi.cebitec.aws.s3.transfer.model.up;

import com.amazonaws.services.s3.AmazonS3;

import java.io.IOException;

public interface IUploadChunk {
    void upload(AmazonS3 s3, String bucketName) throws IOException;
}
