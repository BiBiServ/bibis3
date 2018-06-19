package de.unibi.cebitec.aws.s3.transfer.ctrl;

import de.unibi.cebitec.aws.s3.transfer.model.GridDownloadOrganizer;
import de.unibi.cebitec.aws.s3.transfer.model.Measurements;
import de.unibi.cebitec.aws.s3.transfer.model.down.DownloadFile;
import de.unibi.cebitec.aws.s3.transfer.model.down.DownloadPart;
import de.unibi.cebitec.aws.s3.transfer.model.down.IDownloadChunk;
import de.unibi.cebitec.aws.s3.transfer.model.down.MultipartDownloadFile;
import de.unibi.cebitec.aws.s3.transfer.model.down.url.IDownloadChunkUrl;
import de.unibi.cebitec.aws.s3.transfer.model.down.url.SingleUrlDownloadFile;
import de.unibi.cebitec.aws.s3.transfer.model.down.url.TransferUrlDownloadThread;
import de.unibi.cebitec.aws.s3.transfer.model.features.Fastq;
import de.unibi.cebitec.aws.s3.transfer.util.UnrecoverableErrorException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UrlDownloader {
    public static final Logger log = LoggerFactory.getLogger(UrlDownloader.class);
    private String url;
    private Path outputFile;
    private int numberOfThreads;

    private DownloadFile file;
    private List<IDownloadChunk> chunks;
    private long chunkSize;
    private boolean gridDownload;
    private GridDownloadOrganizer gridDownloadOrganizer;

    public UrlDownloader(String url, Path fileDownloadDestination, int numberOfThreads, long chunkSize, GridDownloadOrganizer gridDownloadOrganizer) {
        this(url, fileDownloadDestination, numberOfThreads, chunkSize);
        this.gridDownload = true;
        this.gridDownloadOrganizer = gridDownloadOrganizer;
    }

    public UrlDownloader(String url, Path fileDownloadDestination, int numberOfThreads, long chunkSize) {
        this.gridDownload = false;
        this.numberOfThreads = numberOfThreads;
        this.chunkSize = chunkSize;
        
        this.url = url;
        this.outputFile = fileDownloadDestination;
        
        this.chunks = new ArrayList<>();
          
    }

    public void download() throws Exception {
        //find out length of requested download
        HttpClient httpClient = HttpClientBuilder.create().build();
        HttpGet httpGet = new HttpGet(url);
        HttpResponse httpResponse = httpClient.execute(httpGet);
        HttpEntity httpEntity = httpResponse.getEntity();
        Long fileSize = httpEntity.getContentLength();
        // close stuff again
        httpGet.abort();
        httpClient.getConnectionManager().shutdown();
        
        if (fileSize <= this.chunkSize) {
            this.file = new SingleUrlDownloadFile(url, outputFile, fileSize);
            this.chunks.add((IDownloadChunkUrl) this.file);
        } else {
            this.file = new MultipartDownloadFile(url, outputFile, fileSize, false, this.chunkSize);
            while (((MultipartDownloadFile) this.file).hasMoreParts()) {
                    this.chunks.add(((MultipartDownloadFile) this.file).next());
            }
        }
        Measurements.addToOverallBytes(fileSize);


        String approx = "";
        if (this.gridDownload) {
            //if in grid mode then create subset of chunks
            this.chunks = this.gridDownloadOrganizer.getChunkSubset(this.chunks);
            Measurements.setOverallBytes(Measurements.getOverallBytes() / this.gridDownloadOrganizer.getNodesCount());
            
            switch (this.gridDownloadOrganizer.getFeature()) {
                case "fastq":
                    Fastq q = new Fastq(this.url);
                    DownloadPart firstPart = (DownloadPart) this.chunks.get(0);
                    DownloadPart lastPart = (DownloadPart) this.chunks.get(this.chunks.size() - 1);
                    q.optimizeSplitStart(firstPart);
                    q.optimizeSplitEnd(lastPart);
                case "split":
                    approx = "approx.";
                    long size = 0;
                    for (IDownloadChunk chunk : this.chunks) {
                        size += chunk.getSize();
                    }
                    DownloadPart firstChunk = (DownloadPart) this.chunks.get(0);
                    long subsetInputOffset = firstChunk.getInputOffset();
                    for (IDownloadChunk chunk : this.chunks) {
                        if (chunk instanceof DownloadPart) {
                            DownloadPart part = (DownloadPart) chunk;
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
        if (file instanceof MultipartDownloadFile) {
            try {
                ((MultipartDownloadFile) file).openFile();
            } catch (UnrecoverableErrorException e) {
                System.exit(1);
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
            futures.add(threading.submit(new TransferUrlDownloadThread(this.url, (IDownloadChunkUrl) chunk, 6)));
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
        if (file instanceof MultipartDownloadFile) {
            ((MultipartDownloadFile) file).closeFile();
        }

        timer.cancel();
        Measurements.stop();
        log.info("Overall average download speed: {}", Measurements.getEndResult());

    }
}
