package de.unibi.cebitec.aws.s3.transfer.model.down.url;

import de.unibi.cebitec.aws.s3.transfer.model.down.*;
import com.amazonaws.services.s3.AmazonS3;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransferUrlDownloadThread implements Callable<Void> {

    public static final Logger log = LoggerFactory.getLogger(TransferDownloadThread.class);
    private String url;
    private IDownloadChunkUrl chunk;
    private int retryCount;

    public TransferUrlDownloadThread(String url, IDownloadChunkUrl chunk, int retryCount) {
        this.url = url;
        this.chunk = chunk;
        this.retryCount = retryCount;
    }

    @Override
    public Void call() {
        for (int i = 0; i < this.retryCount; i++) {
            try {
                this.chunk.download(this.url);
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
