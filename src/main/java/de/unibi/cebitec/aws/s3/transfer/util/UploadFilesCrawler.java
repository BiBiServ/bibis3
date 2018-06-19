package de.unibi.cebitec.aws.s3.transfer.util;

import de.unibi.cebitec.aws.s3.transfer.model.InputFileList;
import de.unibi.cebitec.aws.s3.transfer.model.OutputFileList;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UploadFilesCrawler extends SimpleFileVisitor<Path> {
    public final Logger log = LoggerFactory.getLogger(UploadFilesCrawler.class);
    private InputFileList<Path> files;
    private OutputFileList<Path, String> targetKeys;
    private String keyPrefix;

    public UploadFilesCrawler(String keyPrefix) {
        this.keyPrefix = keyPrefix;
        this.files = new InputFileList<>();
        this.targetKeys = new OutputFileList<>();
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        this.files.put(file, attrs.size());
        String key = keyPrefix + file.toString();
        this.targetKeys.put(file, key);
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
        log.error("Preparation of file upload failed for: {} - Reason: {}", file, exc.getClass().getSimpleName());
        return FileVisitResult.CONTINUE;
    }

    public InputFileList<Path> getFiles() {
        return files;
    }

    public OutputFileList<Path, String> getTargetKeys() {
        return targetKeys;
    }
}
