package de.unibi.cebitec.aws.s3.transfer.model.up;

import com.amazonaws.services.s3.AmazonS3;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransferUploadThread implements Callable<Void> {
    public static final Logger log = LoggerFactory.getLogger(TransferUploadThread.class);
    private final AmazonS3 s3;
    private final String bucketName;
    private final IUploadChunk chunk;
    private final int retryCount;

    public TransferUploadThread(AmazonS3 s3, String bucketName, IUploadChunk chunk, int retryCount) {
        this.s3 = s3;
        this.bucketName = bucketName;
        this.chunk = chunk;
        this.retryCount = retryCount;
    }

    @Override
    public Void call() throws Exception {
        for (int i = 0; i < retryCount; i++) {
            try {
                chunk.upload(s3, bucketName);
                break;
            } catch (Exception e) {
                log.warn("Chunk upload failed! Retrying... ({})", e.toString());
            }
            if (i == retryCount - 1) {
                log.error("Chunk upload failed after {} retries. Exiting...", i);
                System.exit(1);
            }
        }
        return null;
    }
}
