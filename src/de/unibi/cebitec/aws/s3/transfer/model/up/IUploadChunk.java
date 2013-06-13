package de.unibi.cebitec.aws.s3.transfer.model.up;

import com.amazonaws.services.s3.AmazonS3;

public interface IUploadChunk {

    public void upload(AmazonS3 s3, String bucketName);
}
