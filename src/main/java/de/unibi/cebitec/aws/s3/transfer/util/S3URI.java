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
        try {
            this.key = uri.getPath().substring(1);
        } catch (StringIndexOutOfBoundsException se) {
            throw new IllegalArgumentException("Ambiguous S3URI specified. Perhaps missing a trailing '/'?");
        }
        if (this.bucket == null) {
            log.warn("URI: {}   BUCKET: null   KEY: {}", s3uri, this.key);
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
