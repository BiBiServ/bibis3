package de.unibi.cebitec.aws.s3.transfer.model.down.url;

import de.unibi.cebitec.aws.s3.transfer.BiBiS3;
import de.unibi.cebitec.aws.s3.transfer.model.down.*;
import de.unibi.cebitec.aws.s3.transfer.model.Measurements;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DownloadPartUrl implements IDownloadChunkUrl, DownloadPart {

    public static final Logger log = LoggerFactory.getLogger(DownloadPartS3.class);
    private MultipartDownloadFile multipartDownloadFile;
    private int partNumber;
    private long inputOffset;
    private long outputOffset;
    private long partSize;

    public DownloadPartUrl(MultipartDownloadFile multipartDownloadFile) {
        this.multipartDownloadFile = multipartDownloadFile;
    }

    @Override
    public void download(String url) throws Exception {
        long offset = this.inputOffset;
        long remainingBytes = this.partSize;
        for (int i = 0; i < BiBiS3.INCOMPLETE_HTTP_RESPONSE_RETRIES; i++) {
            DefaultHttpClient httpClient = new DefaultHttpClient();
            HttpGet httpGet = new HttpGet(url);
            long end = offset + remainingBytes;
            httpGet.addHeader("Range", "bytes=" + offset + "-" + end);

            HttpResponse httpResponse = httpClient.execute(httpGet);
            HttpEntity httpEntity = httpResponse.getEntity();


            FileChannel out = this.getMultipartDownloadFile().getOutputFileChannel();

            log.trace("Starting download of part {} of file: {}", this.partNumber, url);
            try (ReadableByteChannel in = Channels.newChannel(httpEntity.getContent())) {
                long bytesRead = out.transferFrom(in, offset, remainingBytes); //TODO: mit negativem offset immer an anfang der datei schreiben?
                if (bytesRead == remainingBytes) {
                    Measurements.countChunkAsFinished();
                    log.trace("Download done: Part {} of file: {}", this.partNumber, url);
                    break;
                } else {
                    log.warn("Chunk transfer of part {} of file '{}' has been interrupted! {} out of {} bytes have already been transferred. Retrying transfer of the remaining {} bytes....", this.partNumber, url, bytesRead, remainingBytes, remainingBytes - bytesRead);
                    offset = offset + bytesRead;
                    remainingBytes = remainingBytes - bytesRead;
                }
                if (i == BiBiS3.INCOMPLETE_HTTP_RESPONSE_RETRIES - 1) {
                    throw new IOException("Chunk transfer failed after " + BiBiS3.INCOMPLETE_HTTP_RESPONSE_RETRIES + " attempts to recover from interrupted HTTP transfers!");
                }
            } catch (IOException e) {
                log.debug("Failed to write part to file. Reason: {}  ; Part Number: {}  ; Filename: {}", e.getClass().getSimpleName(), this.getPartNumber(), this.multipartDownloadFile.getTargetFile());
                throw e;
            } finally {
                httpGet.abort();
                httpClient.getConnectionManager().shutdown();
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
