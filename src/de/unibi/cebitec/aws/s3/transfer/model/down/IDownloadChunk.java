package de.unibi.cebitec.aws.s3.transfer.model.down;

import com.amazonaws.services.s3.AmazonS3;

public interface IDownloadChunk {

    public void download(AmazonS3 s3, String bucketName) throws Exception;
    
    public long getSize();
}
