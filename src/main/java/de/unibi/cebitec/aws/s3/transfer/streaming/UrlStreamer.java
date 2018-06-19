package de.unibi.cebitec.aws.s3.transfer.streaming;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UrlStreamer {

    public static final Logger log = LoggerFactory.getLogger(UrlStreamer.class);
    private String key;
    private Path targetFile;
    static long overallBytes = 0;
    static long bytesWritten = 0;

    public UrlStreamer(String key, Path targetFile) {
        this.key = key;
        this.targetFile = targetFile;
    }

    public void download(String url) throws Exception {
        
        final HttpParams httpParams = new BasicHttpParams();
        HttpConnectionParams.setConnectionTimeout(httpParams, 60000); // 1 minute timeout
        
        DefaultHttpClient httpClient = new DefaultHttpClient(httpParams);
        
        HttpGet httpGet = new HttpGet(url);
        HttpResponse httpResponse = httpClient.execute(httpGet);
        HttpEntity httpEntity = httpResponse.getEntity();
        
        log.debug("Starting download of single file: {}", this.key);

        try (InputStream in = httpEntity.getContent()) {
            try (OutputStream out = Files.newOutputStream(this.targetFile, StandardOpenOption.CREATE)) {

                overallBytes = httpEntity.getContentLength();

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
