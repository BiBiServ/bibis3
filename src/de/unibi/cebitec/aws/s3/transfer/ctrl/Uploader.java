package de.unibi.cebitec.aws.s3.transfer.ctrl;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
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
    private InputFileList<Path> inputFiles;
    private OutputFileList<Path, String> outputFiles;
    private String bucketName;
    private List<UploadFile> files;
    private List<IUploadChunk> chunks;
    private final AmazonS3 s3;
    private int numberOfThreads;
    private long chunkSize;

    public Uploader(AmazonS3Client s3, InputFileList<Path> inputFiles, String bucketName, OutputFileList uploadTargetKeys, int numberOfThreads, long chunkSize) {
        this.inputFiles = inputFiles;
        this.outputFiles = uploadTargetKeys;
        this.bucketName = bucketName;
        this.numberOfThreads = numberOfThreads;
        this.chunkSize = chunkSize;

        this.files = new ArrayList<>();
        this.chunks = new ArrayList<>();
        this.s3 = s3;
    }

    public void upload() throws Exception {

        for (Map.Entry<Path, Long> item : this.inputFiles.entrySet()) {
            Measurements.addToOverallBytes(item.getValue());
            if (item.getValue() < BiBiS3.MIN_CHUNK_SIZE) {
                addSingleFile(item.getKey(), this.outputFiles.get(item.getKey()));
            } else {
                addMultipartFile(item.getKey(), this.outputFiles.get(item.getKey()));
            }
        }

        log.debug("file list size: {}", this.files.size());

        //fill chunk list
        for (UploadFile f : this.files) {
            if (f instanceof SingleUploadFile) {
                this.chunks.add((IUploadChunk) f);
            } else if (f instanceof MultipartUploadFile) {
                while (((MultipartUploadFile) f).hasMoreParts()) {
                    this.chunks.add(((MultipartUploadFile) f).next());
                }
            }
        }

        Measurements.setOverallChunks(this.chunks.size());
        log.info("== Uploading {} of data split into {} chunks...", Measurements.getOverallBytesFormatted(), this.chunks.size());

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
        ExecutorService threading = Executors.newFixedThreadPool(this.numberOfThreads);
        List<Future<?>> futures = new ArrayList<>();
        for (IUploadChunk chunk : this.chunks) {
            futures.add(threading.submit(new TransferUploadThread(this.s3, this.bucketName, chunk, 6)));
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
        for (UploadFile f : this.files) {
            if (f instanceof MultipartUploadFile) {
                ((MultipartUploadFile) f).complete(this.s3, this.bucketName);
            }
        }

        Measurements.stop();
        log.info("Overall average upload speed: {}", Measurements.getEndResult());
    }

    private void addMultipartFile(Path file, String key) {
        MultipartUploadFile mFile = new MultipartUploadFile(file, key, this.chunkSize);
        this.files.add(mFile);
        InitiateMultipartUploadRequest mReq = new InitiateMultipartUploadRequest(this.bucketName, mFile.getKey());
        InitiateMultipartUploadResult mRes = this.s3.initiateMultipartUpload(mReq);
        mFile.setUploadId(mRes.getUploadId());
    }

    private void addSingleFile(Path file, String key) {
        this.files.add(new SingleUploadFile(file, key));
    }
}
