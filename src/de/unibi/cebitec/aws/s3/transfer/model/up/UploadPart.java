package de.unibi.cebitec.aws.s3.transfer.model.up;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import de.unibi.cebitec.aws.s3.transfer.model.Measurements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UploadPart implements IUploadChunk {

    public static final Logger log = LoggerFactory.getLogger(UploadPart.class);
    private MultipartUploadFile multipartUploadFile;
    private int partNumber;
    private long fileOffset;
    private long partSize;
    private PartETag tag;

    public UploadPart(MultipartUploadFile multipartUploadFile) {
        this.multipartUploadFile = multipartUploadFile;
    }

    @Override
    public void upload(AmazonS3 s3, String bucketName) {
        UploadPartRequest uploadRequest = new UploadPartRequest()
                .withBucketName(bucketName).withKey(this.multipartUploadFile.getKey())
                .withUploadId(this.multipartUploadFile.getUploadId())
                .withPartNumber(this.partNumber)
                .withFileOffset(this.fileOffset)
                .withFile(this.multipartUploadFile.file.toFile())
                .withPartSize(this.partSize);
        try {
            log.debug("Starting upload of part {} of file: {}", this.partNumber, this.multipartUploadFile.getKey());
            this.tag = s3.uploadPart(uploadRequest).getPartETag();
            Measurements.countChunkAsFinished();
            log.debug("Upload done: Part {} of file: {}", this.partNumber, this.multipartUploadFile.getKey());
        } catch (AmazonClientException e) {
            log.debug("Failed to upload part {} of file: {} - Reason: {}", this.partNumber, this.multipartUploadFile.getKey(), e.toString());
            throw e;
        }
    }

    public int getPartNumber() {
        return partNumber;
    }

    public void setPartNumber(int partNumber) {
        this.partNumber = partNumber;
    }

    public long getFileOffset() {
        return fileOffset;
    }

    public void setFileOffset(long fileOffset) {
        this.fileOffset = fileOffset;
    }

    public long getPartSize() {
        return partSize;
    }

    public void setPartSize(long partSize) {
        this.partSize = partSize;
    }

    public PartETag getTag() {
        return tag;
    }

    public void setTag(PartETag tag) {
        this.tag = tag;
    }

    public MultipartUploadFile getMultipartUploadFile() {
        return multipartUploadFile;
    }
}
