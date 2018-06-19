package de.unibi.cebitec.aws.s3.transfer.model.down;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import de.unibi.cebitec.aws.s3.transfer.model.Measurements;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleDownloadFile extends DownloadFile implements IDownloadChunkS3 {
    public static final Logger log = LoggerFactory.getLogger(SingleDownloadFile.class);
    private long size;

    public SingleDownloadFile(String key, Path targetFile, long size) {
        super(key, targetFile);
        this.size = size;
    }

    @Override
    public void download(AmazonS3 s3, String bucketName) throws Exception {
        GetObjectRequest getObjReq = new GetObjectRequest(bucketName, this.key);
        S3Object obj = s3.getObject(getObjReq);
        log.debug("Starting download of single file: {}", this.key);
        try (InputStream in = obj.getObjectContent()) {
            Path parentDir = this.targetFile.getParent();
            if (parentDir != null) {
                parentDir.toFile().mkdirs();
            }
            if (obj.getKey().endsWith("/") && obj.getObjectMetadata().getContentLength() == 0) {
                this.targetFile.toFile().mkdirs();
                log.debug("Empty folder file detected. Creating empty folder on the receiving end ....");
            } else {
                if (!this.targetFile.toFile().isDirectory()) {
                    long bytesRead = Files.copy(in, this.targetFile, StandardCopyOption.REPLACE_EXISTING);
                    if (bytesRead != this.size) {
                        throw new IOException("File transfer of file '" + this.targetFile + "' has been interrupted!");
                    }
                }
            }
            Measurements.countChunkAsFinished();
            log.debug("Download done: Single file: {}", this.key);
        } catch (IOException e) {
            log.debug("Failed to save single file to disk. Reason: {}  ; Filename: {}", e.getClass().getSimpleName(), this.targetFile);
            throw e;
        }
    }

    @Override
    public long getSize() {
        return this.size;
    }
}
