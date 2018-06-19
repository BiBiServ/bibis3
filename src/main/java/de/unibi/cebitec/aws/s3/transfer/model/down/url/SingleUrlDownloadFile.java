package de.unibi.cebitec.aws.s3.transfer.model.down.url;

import de.unibi.cebitec.aws.s3.transfer.model.Measurements;
import de.unibi.cebitec.aws.s3.transfer.model.down.DownloadFile;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleUrlDownloadFile extends DownloadFile implements IDownloadChunkUrl {
    public static final Logger log = LoggerFactory.getLogger(SingleUrlDownloadFile.class);
    private long size;

    public SingleUrlDownloadFile(String key, Path targetFile, long size) {
        super(key, targetFile);
        this.size = size;
        this.targetFile = targetFile;
    }

    @Override
    public void download(String url) throws Exception {
        HttpClient httpClient = HttpClientBuilder.create().build();
        HttpGet httpGet = new HttpGet(url);
        HttpResponse httpResponse = httpClient.execute(httpGet);
        HttpEntity httpEntity = httpResponse.getEntity();


        log.debug("Starting download of single file: {}", url);
        try (InputStream in = httpEntity.getContent()) {
            Path parentDir = this.targetFile.getParent();
            if (parentDir != null) {
                parentDir.toFile().mkdirs();
            }
            if (!this.targetFile.toFile().isDirectory()) {
                long bytesRead = Files.copy(in, this.targetFile, StandardCopyOption.REPLACE_EXISTING);
                if (bytesRead != this.size) {
                    throw new IOException("File transfer of file '" + this.targetFile + "' has been interrupted!");
                }
            }
            Measurements.countChunkAsFinished();
            log.debug("Download done: Single file: {}", url);
        } catch (IOException e) {
            log.debug("Failed to save single file to disk. Reason: {}  ; Filename: {}", e.getClass().getSimpleName(), this.targetFile);
            throw e;
        } finally {
            httpGet.abort();
            httpClient.getConnectionManager().shutdown();
        }
    }

    @Override
    public long getSize() {
        return this.size;
    }
}
