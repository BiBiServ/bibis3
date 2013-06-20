package de.unibi.cebitec.aws.s3.transfer.model.down;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import de.unibi.cebitec.aws.s3.transfer.model.Measurements;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DownloadPartS3 implements IDownloadChunkS3, DownloadPart {

    public static final Logger log = LoggerFactory.getLogger(DownloadPartS3.class);
    private MultipartDownloadFile multipartDownloadFile;
    private int partNumber;
    private long inputOffset;
    private long outputOffset;
    private long partSize;

    public DownloadPartS3(MultipartDownloadFile multipartDownloadFile) {
        this.multipartDownloadFile = multipartDownloadFile;
    }

    @Override
    public void download(AmazonS3 s3, String bucketName) throws Exception {
        GetObjectRequest partialRequest = new GetObjectRequest(bucketName, this.multipartDownloadFile.key);
        partialRequest.setRange(this.inputOffset, this.inputOffset + this.partSize);
        S3Object objectPart = s3.getObject(partialRequest);
        FileChannel out = this.getMultipartDownloadFile().getOutputFileChannel();

        log.debug("Starting download of part {} of file: {}", this.partNumber, this.multipartDownloadFile.key);
        try (ReadableByteChannel in = Channels.newChannel(objectPart.getObjectContent())) {
            out.transferFrom(in, this.outputOffset, this.partSize); //TODO: mit negativem offset immer an anfang der datei schreiben?
            Measurements.countChunkAsFinished();
            log.debug("Download done: Part {} of file: {}", this.partNumber, this.multipartDownloadFile.key);
        } catch (IOException e) {
            log.debug("Failed to write part to file. Reason: {}  ; Part Number: {}  ; Filename: {}", e.getClass().getSimpleName(), this.getPartNumber(), this.multipartDownloadFile.targetFile);
            throw e;
        }
    }

    @Override
    public MultipartDownloadFile getMultipartDownloadFile() {
        return this.multipartDownloadFile;
    }

    @Override
    public int getPartNumber() {
        return partNumber;
    }

    @Override
    public void setPartNumber(int partNumber) {
        this.partNumber = partNumber;
    }

    @Override
    public long getInputOffset() {
        return inputOffset;
    }

    @Override
    public void setInputOffset(long inputOffset) {
        this.inputOffset = inputOffset;
    }

    @Override
    public long getOutputOffset() {
        return outputOffset;
    }

    @Override
    public void setOutputOffset(long outputOffset) {
        this.outputOffset = outputOffset;
    }

    @Override
    public long getPartSize() {
        return partSize;
    }

    @Override
    public void setPartSize(long partSize) {
        this.partSize = partSize;
    }

    @Override
    public long getSize() {
        return this.partSize;
    }
}
