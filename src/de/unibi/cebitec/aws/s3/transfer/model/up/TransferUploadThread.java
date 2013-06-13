package de.unibi.cebitec.aws.s3.transfer.model.up;

import com.amazonaws.services.s3.AmazonS3;
import de.unibi.cebitec.aws.s3.transfer.model.up.IUploadChunk;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransferUploadThread implements Callable<Void> {

    public static final Logger log = LoggerFactory.getLogger(TransferUploadThread.class);
    private AmazonS3 s3;
    private String bucketName;
    private IUploadChunk chunk;
    private int retryCount;

    public TransferUploadThread(AmazonS3 s3, String bucketName, IUploadChunk chunk, int retryCount) {
        this.s3 = s3;
        this.bucketName = bucketName;
        this.chunk = chunk;
        this.retryCount = retryCount;
    }

    @Override
    public Void call() throws Exception {
        for (int i = 0; i < this.retryCount; i++) {
            try {
                this.chunk.upload(this.s3, this.bucketName);
                break;
            } catch (Exception e) {
                log.warn("Chunk upload failed! Retrying.... ({})", e.toString());
            }
            if (i == this.retryCount - 1) {
                log.error("Chunk upload failed after {} retries. Exiting...", i);
                System.exit(1);
            }
        }
        return null;
    }
}
