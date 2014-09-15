package de.unibi.cebitec.aws.s3.transfer.model.down;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import de.unibi.cebitec.aws.s3.transfer.BiBiS3;
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
        long currentInputOffset = this.inputOffset;
        long currentOutputOffset = this.outputOffset;
        long remainingBytes = this.partSize;
        for (int i = 0; i < BiBiS3.INCOMPLETE_HTTP_RESPONSE_RETRIES; i++) {
            GetObjectRequest partialRequest = new GetObjectRequest(bucketName, this.multipartDownloadFile.key);
            partialRequest.setRange(currentInputOffset, currentInputOffset + remainingBytes);
            S3Object objectPart = s3.getObject(partialRequest);
            FileChannel out = this.getMultipartDownloadFile().getOutputFileChannel();

            log.trace("Starting download of part {} of file: {}", this.partNumber, this.multipartDownloadFile.key);
            try (ReadableByteChannel in = Channels.newChannel(objectPart.getObjectContent())) {
                long bytesRead = out.transferFrom(in, currentOutputOffset, remainingBytes); //TODO: mit negativem offset immer an anfang der datei schreiben?
                if (bytesRead == remainingBytes) {
                    Measurements.countChunkAsFinished();
                    log.trace("Download done: Part {} of file: {}", this.partNumber, this.multipartDownloadFile.key);
                    break;
                } else {
                    log.warn("Chunk transfer of part {} of file '{}' has been interrupted! {} out of {} bytes have already been transferred. Retrying transfer of the remaining {} bytes....", this.partNumber, this.multipartDownloadFile.targetFile, bytesRead, remainingBytes, remainingBytes - bytesRead);
                    currentInputOffset = currentInputOffset + bytesRead;
                    currentOutputOffset = currentOutputOffset + bytesRead;
                    remainingBytes = remainingBytes - bytesRead;
                }
                if (i == BiBiS3.INCOMPLETE_HTTP_RESPONSE_RETRIES - 1) {
                    throw new IOException("Chunk transfer failed after " + BiBiS3.INCOMPLETE_HTTP_RESPONSE_RETRIES + " attempts to recover from interrupted HTTP transfers!");
                }
            } catch (IOException e) {
                log.debug("Failed to write part to file. Reason: {}  ; Part Number: {}  ; Filename: {}", e.getClass().getSimpleName(), this.getPartNumber(), this.multipartDownloadFile.targetFile);
                throw e;
            }
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
