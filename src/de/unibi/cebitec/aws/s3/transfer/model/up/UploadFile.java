package de.unibi.cebitec.aws.s3.transfer.model.up;

import java.nio.file.Path;

public class UploadFile {

    protected Path file;
    protected String key;

    protected UploadFile(Path file, String key) {
        this.file = file;
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public Path getFile() {
        return file;
    }
}
