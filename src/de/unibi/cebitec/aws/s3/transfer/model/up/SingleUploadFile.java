package de.unibi.cebitec.aws.s3.transfer.model.up;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import de.unibi.cebitec.aws.s3.transfer.model.Measurements;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleUploadFile extends UploadFile implements IUploadChunk {

    public static final Logger log = LoggerFactory.getLogger(SingleUploadFile.class);

    public SingleUploadFile(Path file, String key) {
        super(file, key);
    }

    @Override
    public void upload(AmazonS3 s3, String bucketName) {
        PutObjectRequest req = new PutObjectRequest(bucketName, this.key, this.file.toFile());
        try {
            log.debug("Starting upload of single file: {}", this.key);
            s3.putObject(req);
            Measurements.countChunkAsFinished();
            log.debug("Upload done: Single file: {}", this.key);
        } catch (AmazonClientException e) {
            log.debug("Failed to upload single file: {} - Reason: {}", this.key, e.toString());
            throw e;
        }
    }
}
