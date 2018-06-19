package de.unibi.cebitec.aws.s3.transfer.model.down;

import de.unibi.cebitec.aws.s3.transfer.model.down.url.DownloadPartUrl;
import de.unibi.cebitec.aws.s3.transfer.util.UnrecoverableErrorException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultipartDownloadFile extends DownloadFile {
    public static final Logger log = LoggerFactory.getLogger(MultipartDownloadFile.class);
    private Queue<DownloadPart> remainingParts;
    private List<DownloadPart> registeredParts;
    private FileChannel outputFileChannel;
    private long fileSize;

    public MultipartDownloadFile(String key, Path targetFile, long fileSize, boolean s3, final long initialPartSize, long offset) {
        this(key, targetFile, fileSize, s3, initialPartSize);
        this.remainingParts.clear();
        gatherParts(initialPartSize, offset, s3);
    }

    public MultipartDownloadFile(String key, Path targetFile, long fileSize, boolean s3, final long initialPartSize) {
        super(key, targetFile);
        this.fileSize = fileSize;
        this.remainingParts = new PriorityQueue<>(11, (p1, p2) -> {
            if (p1.getPartNumber() == p2.getPartNumber()) {
                return 0;
            }
            return p1.getPartNumber() < p2.getPartNumber() ? -1 : 1;
        });
        this.registeredParts = new ArrayList<>();
        gatherParts(initialPartSize, 0, s3);
    }

    /**
     * Create DownloadPart objects for this multipart file. Offset is usually 0.
     *
     * @param initialPartSize Part size for all parts but the last one.
     * @param offset          Offset into file.
     * @param s3              true if s3 download, false if url download
     */
    private void gatherParts(long initialPartSize, long offset, boolean s3) {
        long partSize;
        long pos = 0;
        for (int i = 1; pos < this.fileSize; i++) {
            partSize = Math.min(initialPartSize, (this.fileSize - pos));
            DownloadPart p;
            if (s3) {
                p = new DownloadPartS3(this);
            } else {
                p = new DownloadPartUrl(this);
            }
            p.setPartNumber(i);
            p.setPartSize(partSize);
            p.setInputOffset(pos + offset);
            p.setOutputOffset(pos + offset);
            this.remainingParts.add(p);
            pos += partSize;
        }
    }

    public boolean hasMoreParts() {
        return !this.remainingParts.isEmpty();
    }

    public void addPart(DownloadPart part) {
        this.remainingParts.offer(part);
    }

    public DownloadPart next() {
        DownloadPart currentPart = this.remainingParts.remove();
        this.registeredParts.add(currentPart);
        return currentPart;
    }

    public void openFile() throws UnrecoverableErrorException {
        try {
            log.debug("target file: {}", this.targetFile);
            Path parentDir = this.targetFile.getParent();
            if (parentDir != null) {
                File parentDirFile = parentDir.toFile();
                parentDirFile.mkdirs();
            }
            // allocate space
            try (RandomAccessFile f = new RandomAccessFile(this.targetFile.toFile(), "rw")) {
                f.setLength(this.fileSize);
            }
            // open real filehandle for writing
            this.outputFileChannel = (FileChannel) Files.newByteChannel(this.targetFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        } catch (IOException e) {
            log.error("Failed to open multipart file for writing. Reason: {}  ; Filename: {}", e.getClass().getSimpleName(), this.targetFile);
            throw new UnrecoverableErrorException();
        }
    }

    public void closeFile() {
        try {
            this.outputFileChannel.close();
        } catch (IOException e) {
            log.error("Failed to close multipart file. Reason: {}  ; Filename: {}", e.getClass().getSimpleName(), this.targetFile);
        }
    }

    public FileChannel getOutputFileChannel() {
        return outputFileChannel;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }
}
