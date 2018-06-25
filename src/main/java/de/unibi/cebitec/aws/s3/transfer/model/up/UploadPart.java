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
    private final MultipartUploadFile multipartUploadFile;
    private final int partNumber;
    private final long fileOffset;
    private final long partSize;
    private PartETag tag;

    public UploadPart(MultipartUploadFile multipartUploadFile, int partNumber, long partSize, long fileOffset) {
        this.multipartUploadFile = multipartUploadFile;
        this.partNumber = partNumber;
        this.partSize = partSize;
        this.fileOffset = fileOffset;
    }

    @Override
    public void upload(AmazonS3 s3, String bucketName) {
        UploadPartRequest uploadRequest = new UploadPartRequest()
                .withBucketName(bucketName)
                .withKey(multipartUploadFile.getKey())
                .withUploadId(multipartUploadFile.getUploadId())
                .withPartNumber(partNumber)
                .withFileOffset(fileOffset)
                .withFile(multipartUploadFile.file.toFile())
                .withPartSize(partSize);
        try {
            log.debug("Starting upload of part {} of file: {}", partNumber, multipartUploadFile.getKey());
            tag = s3.uploadPart(uploadRequest).getPartETag();
            Measurements.countChunkAsFinished();
            log.debug("Upload done: Part {} of file: {}", partNumber, multipartUploadFile.getKey());
        } catch (AmazonClientException e) {
            log.debug("Failed to upload part {} of file: {} - Reason: {}", partNumber, multipartUploadFile.getKey(), e.toString());
            throw e;
        }
    }

    public int getPartNumber() {
        return partNumber;
    }

    public long getFileOffset() {
        return fileOffset;
    }

    public long getPartSize() {
        return partSize;
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
