package de.unibi.cebitec.aws.s3.transfer.ctrl;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import de.unibi.cebitec.aws.s3.transfer.model.GridDownloadOrganizer;
import de.unibi.cebitec.aws.s3.transfer.model.InputFileList;
import de.unibi.cebitec.aws.s3.transfer.model.Measurements;
import de.unibi.cebitec.aws.s3.transfer.model.OutputFileList;
import de.unibi.cebitec.aws.s3.transfer.model.down.DownloadFile;
import de.unibi.cebitec.aws.s3.transfer.model.down.DownloadPartS3;
import de.unibi.cebitec.aws.s3.transfer.model.down.IDownloadChunk;
import de.unibi.cebitec.aws.s3.transfer.model.down.IDownloadChunkS3;
import de.unibi.cebitec.aws.s3.transfer.model.down.MultipartDownloadFile;
import de.unibi.cebitec.aws.s3.transfer.model.down.SingleDownloadFile;
import de.unibi.cebitec.aws.s3.transfer.model.down.TransferDownloadThread;
import de.unibi.cebitec.aws.s3.transfer.model.features.Fastq;
import de.unibi.cebitec.aws.s3.transfer.util.UnrecoverableErrorException;
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

public class Downloader {

    public static final Logger log = LoggerFactory.getLogger(Downloader.class);
    private String bucketName;
    private InputFileList<String> inputFiles;
    private OutputFileList<String, Path> outputFiles;
    private int numberOfThreads;
    private AmazonS3 s3;
    private List<DownloadFile> files;
    private List<IDownloadChunk> chunks;
    private long chunkSize;
    private boolean gridDownload;
    private GridDownloadOrganizer gridDownloadOrganizer;

    public Downloader(AmazonS3Client s3, String bucketName, InputFileList<String> inputFiles, OutputFileList<String, Path> fileDownloadDestinations, int numberOfThreads, long chunkSize, GridDownloadOrganizer gridDownloadOrganizer) {
        this(s3, bucketName, inputFiles, fileDownloadDestinations, numberOfThreads, chunkSize);
        this.gridDownload = true;
        this.gridDownloadOrganizer = gridDownloadOrganizer;
    }

    public Downloader(AmazonS3Client s3, String bucketName, InputFileList<String> inputFiles, OutputFileList<String, Path> fileDownloadDestinations, int numberOfThreads, long chunkSize) {
        this.gridDownload = false;
        this.bucketName = bucketName;
        this.inputFiles = inputFiles;
        this.outputFiles = fileDownloadDestinations;
        this.numberOfThreads = numberOfThreads;
        this.chunkSize = chunkSize;

        this.files = new ArrayList<>();
        this.chunks = new ArrayList<>();
        this.s3 = s3;
    }

    public void download() throws Exception {


        for (Map.Entry<String, Long> item : this.inputFiles.entrySet()) {

            if (item.getValue() <= this.chunkSize) {
                this.files.add(new SingleDownloadFile(item.getKey(), this.outputFiles.get(item.getKey()), item.getValue()));
            } else {
                log.debug("of from hashmap: {}", this.outputFiles.get(item.getKey()));
                this.files.add(new MultipartDownloadFile(item.getKey(), this.outputFiles.get(item.getKey()), item.getValue(), true, this.chunkSize));
            }
            Measurements.addToOverallBytes(item.getValue());
        }

        //fill chunk list
        for (DownloadFile f : this.files) {
            log.debug("DL File: {}", f.getKey());
            if (f instanceof SingleDownloadFile) {
                this.chunks.add((IDownloadChunkS3) f);
            } else if (f instanceof MultipartDownloadFile) {
                while (((MultipartDownloadFile) f).hasMoreParts()) {
                    this.chunks.add(((MultipartDownloadFile) f).next());
                }
            }
        }

        String approx = "";
        if (this.gridDownload) {
            //if in grid mode then create subset of chunks
            this.chunks = this.gridDownloadOrganizer.getChunkSubset(this.chunks);
            Measurements.setOverallBytes(Measurements.getOverallBytes() / this.gridDownloadOrganizer.getNodesCount());
            switch (this.gridDownloadOrganizer.getFeature()) {
                case "fastq":
                    Fastq q = new Fastq(this.s3, this.bucketName);
                    DownloadPartS3 firstPart = (DownloadPartS3) this.chunks.get(0);
                    DownloadPartS3 lastPart = (DownloadPartS3) this.chunks.get(this.chunks.size() - 1);
                    q.optimizeSplitStart(firstPart);
                    q.optimizeSplitEnd(lastPart);
                case "split":
                    approx = "approx.";
                    long size = 0;
                    for (IDownloadChunk chunk : this.chunks) {
                        size += chunk.getSize();
                    }
                    DownloadPartS3 firstChunk = (DownloadPartS3) this.chunks.get(0);
                    long subsetInputOffset = firstChunk.getInputOffset();
                    for (IDownloadChunk chunk : this.chunks) {
                        if (chunk instanceof DownloadPartS3) {
                            DownloadPartS3 part = (DownloadPartS3) chunk;
                            part.getMultipartDownloadFile().setFileSize(size);
                            part.setOutputOffset(part.getInputOffset() - subsetInputOffset);
                        }
                    }
                    break;
                case "":
                default:
                    approx = "approx.";
                    break;
            }

        }

        Measurements.setOverallChunks(this.chunks.size());
        log.info("== Downloading {} {} of data split into {} chunks...", approx, Measurements.getOverallBytesFormatted(), this.chunks.size());

        // open all multipart files
        for (DownloadFile f : this.files) {
            if (f instanceof MultipartDownloadFile) {
                try {
                    ((MultipartDownloadFile) f).openFile();
                } catch (UnrecoverableErrorException e) {
                    System.exit(1);
                }
            }
        }

        Measurements.start();

        TimerTask measurementsUpdates = new TimerTask() {
            @Override
            public void run() {
                log.info("Chunk downloads complete: {}", Measurements.getChunksFinishedCount());
            }
        };
        Timer timer = new Timer();
        timer.schedule(measurementsUpdates, 3000, 15000);

        //download all chunks/single files
        ExecutorService threading = Executors.newFixedThreadPool(this.numberOfThreads);
        List<Future<?>> futures = new ArrayList<>();
        for (IDownloadChunk chunk : this.chunks) {
            futures.add(threading.submit(new TransferDownloadThread(this.s3, this.bucketName, (IDownloadChunkS3) chunk, 6)));
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

        // close all multipart files
        for (DownloadFile f : this.files) {
            if (f instanceof MultipartDownloadFile) {
                ((MultipartDownloadFile) f).closeFile();
            }
        }

        timer.cancel();
        Measurements.stop();
        log.info("Overall average download speed: {}", Measurements.getEndResult());

    }
}
