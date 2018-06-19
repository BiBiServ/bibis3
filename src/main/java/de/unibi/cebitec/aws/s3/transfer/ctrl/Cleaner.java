package de.unibi.cebitec.aws.s3.transfer.ctrl;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cleaner {

    private static final Logger log = LoggerFactory.getLogger(Cleaner.class);
    private final AmazonS3Client s3;
    private final String bucketName;
    private final Date thresholdDateInThePast;

    public Cleaner(AmazonS3Client s3, String bucketName) {
        this.s3 = s3;
        this.bucketName = bucketName;
        Calendar c = Calendar.getInstance();
        c.add(Calendar.DATE, -7);
        this.thresholdDateInThePast = c.getTime();
    }

    public void cleanUpParts() {
        log.info("Cleaning up the remainings of incomplete multipart uploads for this bucket that are older than a week ...");
        log.trace("Threshold date: {}", this.thresholdDateInThePast);
        MultipartUploadListing uploadListing = this.s3.listMultipartUploads(new ListMultipartUploadsRequest(this.bucketName));
        if (uploadListing.getMultipartUploads().isEmpty()) {
            log.info("No incomplete multipart uploads found.");
            return;
        }
        do {
            for (MultipartUpload upload : uploadListing.getMultipartUploads()) {
                if (upload.getInitiated().compareTo(this.thresholdDateInThePast) < 0) {
                    this.s3.abortMultipartUpload(new AbortMultipartUploadRequest(this.bucketName, upload.getKey(), upload.getUploadId()));
                    log.info("Cleaning up the remaining parts of '{}' (originally initiated on {})", upload.getKey(), new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(upload.getInitiated()));
                }
            }
            ListMultipartUploadsRequest request = new ListMultipartUploadsRequest(this.bucketName).withUploadIdMarker(uploadListing.getNextUploadIdMarker()).withKeyMarker(uploadListing.getNextKeyMarker());
            uploadListing = this.s3.listMultipartUploads(request);
        } while (uploadListing.isTruncated());
    }
}
