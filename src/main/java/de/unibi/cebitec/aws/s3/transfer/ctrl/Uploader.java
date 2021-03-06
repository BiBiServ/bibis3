package de.unibi.cebitec.aws.s3.transfer.ctrl;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.StorageClass;
import de.unibi.cebitec.aws.s3.transfer.BiBiS3;
import de.unibi.cebitec.aws.s3.transfer.model.InputFileList;
import de.unibi.cebitec.aws.s3.transfer.model.Measurements;
import de.unibi.cebitec.aws.s3.transfer.model.OutputFileList;
import de.unibi.cebitec.aws.s3.transfer.model.up.IUploadChunk;
import de.unibi.cebitec.aws.s3.transfer.model.up.MultipartUploadFile;
import de.unibi.cebitec.aws.s3.transfer.model.up.SingleUploadFile;
import de.unibi.cebitec.aws.s3.transfer.model.up.TransferUploadThread;
import de.unibi.cebitec.aws.s3.transfer.model.up.UploadFile;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Uploader {
    public static final Logger log = LoggerFactory.getLogger(Uploader.class);
    private final InputFileList<Path> inputFiles;
    private final OutputFileList<Path, String> outputFiles;
    private final String bucketName;
    private final List<UploadFile> files;
    private final List<IUploadChunk> chunks;
    private final AmazonS3 s3;
    private final int numberOfThreads;
    private final long chunkSize;
    private final ObjectMetadata metadata;
    private final boolean reducedRedundancy;

    public Uploader(AmazonS3 s3, InputFileList<Path> inputFiles, String bucketName,
                    OutputFileList<Path, String> uploadTargetKeys, int numberOfThreads, long chunkSize,
                    ObjectMetadata metadata, boolean reducedRedundancy) {
        this.inputFiles = inputFiles;
        this.outputFiles = uploadTargetKeys;
        this.bucketName = bucketName;
        this.numberOfThreads = numberOfThreads;
        this.chunkSize = chunkSize;
        this.metadata = metadata;
        this.reducedRedundancy = reducedRedundancy;

        this.files = new ArrayList<>();
        this.chunks = new ArrayList<>();
        this.s3 = s3;
    }

    public void upload() throws Exception {
        for (Map.Entry<Path, Long> item : inputFiles.entrySet()) {
            Measurements.addToOverallBytes(item.getValue());
            if (item.getValue() < BiBiS3.MIN_CHUNK_SIZE) {
                addSingleFile(item.getKey(), outputFiles.get(item.getKey()), metadata, reducedRedundancy);
            } else {
                addMultipartFile(item.getKey(), outputFiles.get(item.getKey()), metadata, reducedRedundancy);
            }
        }

        log.debug("file list size: {}", files.size());

        //fill chunk list
        for (UploadFile f : files) {
            if (f instanceof SingleUploadFile) {
                chunks.add((IUploadChunk) f);
            } else if (f instanceof MultipartUploadFile) {
                while (((MultipartUploadFile) f).hasMoreParts()) {
                    chunks.add(((MultipartUploadFile) f).next());
                }
            }
        }

        Measurements.setOverallChunks(chunks.size());
        log.info("== Uploading {} of data split into {} chunks...", Measurements.getOverallBytesFormatted(), chunks.size());

        Measurements.start();

        TimerTask measurementsUpdates = new TimerTask() {
            @Override
            public void run() {
                log.info("Chunk uploads complete: {}", Measurements.getChunksFinishedCount());
            }
        };
        Timer timer = new Timer();
        timer.schedule(measurementsUpdates, 3000, 15000);

        //upload all chunks/single files
        ExecutorService threading = Executors.newFixedThreadPool(numberOfThreads);
        List<Future<?>> futures = new ArrayList<>();
        for (IUploadChunk chunk : chunks) {
            futures.add(threading.submit(new TransferUploadThread(s3, bucketName, chunk, 6)));
        }

        //wait for threads to finish
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (ExecutionException | InterruptedException e) {
                log.error("Error while waiting for running thread. ({})", e.getMessage());
            }
        }
        threading.shutdown();

        //complete multipart uploads
        for (UploadFile f : files) {
            if (f instanceof MultipartUploadFile) {
                ((MultipartUploadFile) f).complete(s3, bucketName);
            }
        }

        Measurements.stop();
        log.info("Overall average upload speed: {}", Measurements.getEndResult());
    }

    private void addMultipartFile(Path file, String key, ObjectMetadata metadata, boolean reducedRedundancy) {
        MultipartUploadFile mFile = new MultipartUploadFile(file, key, chunkSize);
        files.add(mFile);
        InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(bucketName, mFile.getKey(), metadata);
        if (reducedRedundancy) {
            request.setStorageClass(StorageClass.ReducedRedundancy);
        }
        InitiateMultipartUploadResult result = s3.initiateMultipartUpload(request);
        mFile.setUploadId(result.getUploadId());
        log.debug("Add multipart file {} with upload id {}", key, result.getUploadId());
    }

    private void addSingleFile(Path file, String key, ObjectMetadata metadata, boolean reducedRedundancy) {
        files.add(new SingleUploadFile(file, key, metadata, reducedRedundancy));
        log.debug("Add single file {}", key);
    }
}
