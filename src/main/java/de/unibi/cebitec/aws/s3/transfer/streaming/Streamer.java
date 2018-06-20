package de.unibi.cebitec.aws.s3.transfer.streaming;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Streamer {
    public static final Logger log = LoggerFactory.getLogger(Streamer.class);
    private String key;
    private Path targetFile;
    private static long overallBytes = 0;
    private static long bytesWritten = 0;

    public Streamer(String key, Path targetFile) {
        this.key = key;
        this.targetFile = targetFile;
    }

    public void download(AmazonS3 s3, String bucketName) throws Exception {
        GetObjectRequest getObjReq = new GetObjectRequest(bucketName, this.key);
        S3Object obj = s3.getObject(getObjReq);
        log.debug("Starting download of single file: {}", this.key);
        //TODO: get size
        try (InputStream in = obj.getObjectContent()) {
            try (OutputStream out = Files.newOutputStream(this.targetFile, StandardOpenOption.CREATE)) {
                overallBytes = obj.getObjectMetadata().getContentLength();
                TimerTask progressInfo = new TimerTask() {
                    @Override
                    public void run() {
                        System.out.println(new StringBuilder().append(overallBytes).append(" ").append(bytesWritten));
                    }
                };
                Timer timer = new Timer();
                timer.scheduleAtFixedRate(progressInfo, 5000, 5000);
                byte[] buffer = new byte[16384];
                int bytesRead;
                while ((bytesRead = in.read(buffer)) != -1) {
                    out.write(buffer, 0, bytesRead);
                    bytesWritten += bytesRead;
                }
                log.debug("Download done: Single file: {}", this.key);
            } catch (IOException e) {
                log.error("Failed to stream file: {}", e.getMessage());
                System.exit(100);
            }
        }
    }
}
