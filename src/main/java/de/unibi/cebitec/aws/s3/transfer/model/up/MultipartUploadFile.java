package de.unibi.cebitec.aws.s3.transfer.model.up;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.PartETag;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultipartUploadFile extends UploadFile {
    public static final Logger log = LoggerFactory.getLogger(MultipartUploadFile.class);
    protected String uploadId;
    private Queue<UploadPart> remainingParts;
    private List<UploadPart> registeredParts;

    public MultipartUploadFile(Path file, String key, final long initialPartSize) {
        super(file, key);
        remainingParts = new PriorityQueue<>(11, Comparator.comparingInt(UploadPart::getPartNumber));
        registeredParts = new ArrayList<>();

        try {
            long fileSize = Files.size(this.file);
            long pos = 0;
            for (int i = 1; pos < fileSize; i++) {
                long partSize = Math.min(initialPartSize, (fileSize - pos));
                remainingParts.add(new UploadPart(this, i, partSize, pos));
                pos += partSize;
            }
        } catch (IOException e) {
            log.error("Error while reading source file: {}", this.file);
        }
    }

    public void complete(AmazonS3 s3, String bucketName) {
        List<PartETag> tags = new ArrayList<>();
        for (UploadPart p : registeredParts) {
            tags.add(p.getTag());
        }
        CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(bucketName, key, uploadId, tags);
        s3.completeMultipartUpload(compRequest);
        log.debug("Completed multipart upload of file: {}", key);
    }

    public boolean hasMoreParts() {
        return !remainingParts.isEmpty();
    }

    private void addPart(UploadPart part) {
        remainingParts.offer(part);
    }

    public UploadPart next() {
        UploadPart currentPart = remainingParts.remove();
        registeredParts.add(currentPart);
        return currentPart;
    }

    public String getUploadId() {
        return uploadId;
    }

    public void setUploadId(String uploadId) {
        this.uploadId = uploadId;
    }
}
