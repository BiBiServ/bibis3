package de.unibi.cebitec.aws.s3.transfer.model.down;

import com.amazonaws.services.s3.AmazonS3;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransferDownloadThread implements Callable<Void> {

    public static final Logger log = LoggerFactory.getLogger(TransferDownloadThread.class);
    private AmazonS3 s3;
    private String bucketName;
    private IDownloadChunkS3 chunk;
    private int retryCount;

    public TransferDownloadThread(AmazonS3 s3, String bucketName, IDownloadChunkS3 chunk, int retryCount) {
        this.s3 = s3;
        this.bucketName = bucketName;
        this.chunk = chunk;
        this.retryCount = retryCount;
    }

    @Override
    public Void call() {
        for (int i = 0; i < this.retryCount; i++) {
            try {
                this.chunk.download(this.s3, this.bucketName);
                break;
            } catch (Exception e) {
                log.warn("Chunk download failed! Retrying.... ({})", e.toString());
            }
            if (i == this.retryCount - 1) {
                log.error("Chunk download failed after {} retries. Exiting...", i);
                System.exit(1);
            }
        }
        return null;
    }
}
