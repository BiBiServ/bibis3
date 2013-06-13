package de.unibi.cebitec.aws.s3.transfer.util;

import java.net.URI;
import java.net.URISyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3URI {

    private static final Logger log = LoggerFactory.getLogger(S3URI.class);
    private String bucket;
    private String key;

    public S3URI(String s3uri) throws URISyntaxException, IllegalArgumentException {
        URI uri = new URI(s3uri);
        this.bucket = uri.getAuthority();
        this.key = uri.getPath().substring(1);
        if (this.bucket == null) {
            log.warn("URI: {}   BUCKET: {}   KEY: {}", s3uri, this.bucket, this.key);
            throw new IllegalArgumentException("Invalid S3URI - no bucket specified!");
        }
        if (this.key == null) {
            this.key = "";
        }
        log.debug("URI: {}   BUCKET: {}   KEY: {}", s3uri, this.bucket, this.key);
    }

    public String getBucket() {
        return bucket;
    }

    public String getKey() {
        return key;
    }
}
