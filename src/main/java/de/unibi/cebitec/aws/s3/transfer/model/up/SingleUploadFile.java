package de.unibi.cebitec.aws.s3.transfer.model.up;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.StorageClass;
import de.unibi.cebitec.aws.s3.transfer.model.Measurements;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleUploadFile extends UploadFile implements IUploadChunk {
    public static final Logger log = LoggerFactory.getLogger(SingleUploadFile.class);
    private final ObjectMetadata metadata;
    private final boolean reducedRedundancy;

    public SingleUploadFile(Path file, String key, ObjectMetadata metadata, boolean reducedRedundancy) {
        super(file, key);
        this.metadata = metadata;
        this.reducedRedundancy = reducedRedundancy;
    }

    @Override
    public void upload(AmazonS3 s3, String bucketName) throws IOException {
        try {
            PutObjectRequest request = new PutObjectRequest(bucketName, key, Files.newInputStream(file), metadata);
            if (reducedRedundancy) {
                request.setStorageClass(StorageClass.ReducedRedundancy);
            }
            log.debug("Starting upload of single file: {}", key);
            s3.putObject(request);
            Measurements.countChunkAsFinished();
            log.debug("Upload done: Single file: {}", key);
        } catch (IOException | AmazonClientException e) {
            log.debug("Failed to upload single file: {} - Reason: {}", key, e.toString());
            throw e;
        }
    }
}
