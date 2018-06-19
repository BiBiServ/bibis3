package de.unibi.cebitec.aws.s3.transfer.model.up;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.PartETag;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultipartUploadFile extends UploadFile {
    public static final Logger log = LoggerFactory.getLogger(MultipartUploadFile.class);
    protected String uploadId;
    private Queue<UploadPart> remainingParts;
    private List<UploadPart> registeredParts;

    public MultipartUploadFile(Path file, String key, final long initialPartSize) {
        super(file, key);
        this.remainingParts = new PriorityQueue<>(11, (p1, p2) -> {
            if (p1.getPartNumber() == p2.getPartNumber()) {
                return 0;
            }
            return p1.getPartNumber() < p2.getPartNumber() ? -1 : 1;
        });
        this.registeredParts = new ArrayList<>();

        try {
            long fileSize = Files.size(this.file);
            long partSize;
            long pos = 0;
            for (int i = 1; pos < fileSize; i++) {
                partSize = Math.min(initialPartSize, (fileSize - pos));
                UploadPart p = new UploadPart(this);
                p.setPartNumber(i);
                p.setPartSize(partSize);
                p.setFileOffset(pos);
                this.remainingParts.add(p);
                pos += partSize;
            }
        } catch (IOException e) {
            log.error("Error while reading source file: {}", this.file);
        }
    }

    public void complete(AmazonS3 s3, String bucketName) {
        List<PartETag> tags = new ArrayList<>();
        for (UploadPart p : this.registeredParts) {
            tags.add(p.getTag());
        }
        CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(
                bucketName, this.key, this.uploadId, tags);
        s3.completeMultipartUpload(compRequest);
        log.debug("Completed multipart upload of file: {}", this.key);
    }

    public boolean hasMoreParts() {
        return !this.remainingParts.isEmpty();
    }

    private void addPart(UploadPart part) {
        this.remainingParts.offer(part);
    }

    public UploadPart next() {
        UploadPart currentPart = this.remainingParts.remove();
        this.registeredParts.add(currentPart);
        return currentPart;
    }

    public String getUploadId() {
        return uploadId;
    }

    public void setUploadId(String uploadId) {
        this.uploadId = uploadId;
    }
}
