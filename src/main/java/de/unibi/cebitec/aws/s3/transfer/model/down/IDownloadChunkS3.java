package de.unibi.cebitec.aws.s3.transfer.model.down;

import com.amazonaws.services.s3.AmazonS3;

public interface IDownloadChunkS3 extends IDownloadChunk {
    void download(AmazonS3 s3, String bucketName) throws Exception;
}
