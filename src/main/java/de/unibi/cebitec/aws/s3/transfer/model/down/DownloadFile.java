package de.unibi.cebitec.aws.s3.transfer.model.down;

import java.nio.file.Path;

public class DownloadFile {

    protected String key;
    protected Path targetFile;

    protected DownloadFile(String key, Path targetFile) {
        this.key = key;
        this.targetFile = targetFile;
    }

    public String getKey() {
        return key;
    }

    public Path getTargetFile() {
        return targetFile;
    }
}
