package de.unibi.cebitec.aws.s3.transfer.model.features;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import de.unibi.cebitec.aws.s3.transfer.BiBiS3;
import de.unibi.cebitec.aws.s3.transfer.model.down.DownloadPart;
import de.unibi.cebitec.aws.s3.transfer.model.down.MultipartDownloadFile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Fastq {
    private static final Logger log = LoggerFactory.getLogger(Fastq.class);
    private AmazonS3 s3;
    private String bucketName;
    private String url;
    private boolean s3download;

    public Fastq(AmazonS3 s3, String bucketName) {
        this.s3 = s3;
        this.bucketName = bucketName;
        s3download = true;
    }

    public Fastq(String url) {
        s3download = false;
        this.url = url;
    }

    public void optimizeSplitStart(DownloadPart firstPart) {
        try {
            int spaceBefore = 4096;
            int spaceAfter = 0;
            long sampleStart = firstPart.getInputOffset() - spaceBefore;
            if (sampleStart <= 0) {
                // if this is the first split part in the file, then there is no need for optimization
                return;
            }
            log.debug("Fastq: Optimizing start.");
            long sampleEnd = firstPart.getInputOffset() + spaceAfter;
            byte[] data = getData(firstPart.getMultipartDownloadFile(), sampleStart, sampleEnd);
            String sample = new String(data);

            try {
                long extraLengthBefore = findSplit(sample, spaceBefore);

                log.info("Fastq: Adding {} bytes to the beginning of the first chunk.", extraLengthBefore);
                firstPart.setInputOffset(firstPart.getInputOffset() - extraLengthBefore);
                firstPart.setPartSize(firstPart.getPartSize() + extraLengthBefore);
            } catch (StringIndexOutOfBoundsException se) {
                throw new IOException("Input file is not a text-based fastq file!");
            }
        } catch (IOException e) {
            log.error("Fastq split start optimization failed: {}", e.getMessage());
            log.trace("", e);
            System.exit(3);
        }
    }

    public void optimizeSplitEnd(DownloadPart lastPart) {
        try {
            int spaceBefore = 4096;
            int spaceAfter = 0;
            long sampleStart = lastPart.getInputOffset() + lastPart.getSize() - spaceBefore;
            long sampleEnd = lastPart.getInputOffset() + lastPart.getSize() + spaceAfter;
            if (sampleEnd >= lastPart.getMultipartDownloadFile().getFileSize()) {
                // if this is the last split part in the file, then there is no need for optimization
                return;
            }
            log.debug("Fastq: Optimizing end.");
            byte[] data = getData(lastPart.getMultipartDownloadFile(), sampleStart, sampleEnd);
            String sample = new String(data);

            try {
                long endCutoff = findSplit(sample, spaceBefore);

                log.info("Fastq: Removing {} bytes from the end of the last chunk.", endCutoff);
                lastPart.setPartSize(lastPart.getPartSize() - endCutoff);
            } catch (StringIndexOutOfBoundsException se) {
                throw new IOException("Input file is not a text-based fastq file!");
            }
        } catch (IOException e) {
            log.error("Fastq split end optimization failed: {}", e.getMessage());
            log.trace("", e);
            System.exit(3);
        }
    }

    public long findSplit(String sample, int spaceBefore) throws StringIndexOutOfBoundsException {
        char prevChar = 0;
        char currChar;
        char pairChar = 0;
        int i = spaceBefore - 1;
        while (i > 0) {
            currChar = sample.charAt(i);
            if (currChar == '/') {
                pairChar = prevChar;
                log.debug("pair indicator found: {}", pairChar);
            }
            if (sample.charAt(i) == '\n' && prevChar == '@') {
                if (pairChar != '2') {
                    log.debug("stopping search with pair indicator '{}'", pairChar);
                    i++;
                    break;
                }
            }
            prevChar = sample.charAt(i);
            i--;
        }
        return spaceBefore - i;
    }

    private byte[] getData(MultipartDownloadFile file, long start, long end) {
        int retries = BiBiS3.RETRIES;
        int sampleSize = (int) (end - start);
        while (retries > 0) {

            if (s3download) {
                GetObjectRequest partialRequest = new GetObjectRequest(this.bucketName, file.getKey());
                log.debug("Requesting data sample in range [{},{}]", start, end);
                partialRequest.setRange(start, end);
                S3Object objectPart = s3.getObject(partialRequest);
                try (ReadableByteChannel in = Channels.newChannel(objectPart.getObjectContent())) {
                    ByteBuffer buffer = ByteBuffer.allocate(sampleSize);
                    int read = in.read(buffer);
                    log.debug("number of bytes read: {}", read);
                    log.debug("byte array of sample data has {} bytes.", buffer.array().length);
                    return buffer.array();
                } catch (Exception e) {
                    log.warn("Unable to determine optimal fastq splits! Could not sample split areas. Exception: {}", e);
                    log.warn("Retrying...");
                }
            } else {

                log.debug("Requesting data sample in range [{},{}]", start, end);

                HttpClient httpClient = HttpClientBuilder.create().build();
                HttpGet httpGet = new HttpGet(url);
                httpGet.addHeader("Range", "bytes=" + start + "-" + end);

                try {
                    HttpResponse httpResponse = httpClient.execute(httpGet);
                    HttpEntity httpEntity = httpResponse.getEntity();

                    try (ReadableByteChannel in = Channels.newChannel(httpEntity.getContent())) {
                        ByteBuffer buffer = ByteBuffer.allocate(sampleSize);
                        int read = in.read(buffer);
                        log.debug("number of bytes read: {}", read);
                        log.debug("byte array of sample data has {} bytes.", buffer.array().length);
                        return buffer.array();
                    } catch (Exception e) {
                        log.warn("Unable to determine optimal fastq splits! Could not sample split areas. Exception: {}", e);
                        log.warn("Retrying...");
                    }
                } catch (Exception e) {
                    log.warn("Unable to determine optimal fastq splits! Could not sample split areas. Exception: {}", e);
                    log.warn("Retrying...");
                }
            }

            retries--;
        }

        System.exit(1);
        return new byte[0];
    }
}
