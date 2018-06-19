package de.unibi.cebitec.aws.s3.transfer.util;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

public class CredentialsProvider {
    private static final String DEFAULT_PROPERTIES_DIRNAME = System.getProperty("user.home");
    private static final String DEFAULT_PROPERTIES_FILENAME = ".aws-credentials.properties";

    public static AWSCredentials getCredentials() {
        Path credentialsFilePath = FileSystems.getDefault().getPath(DEFAULT_PROPERTIES_DIRNAME, DEFAULT_PROPERTIES_FILENAME);
        try {
            InputStream inputStream = Files.newInputStream(credentialsFilePath);
            return new PropertiesCredentials(inputStream);
        } catch (IOException e) {
            return null;
        }
    }
}
