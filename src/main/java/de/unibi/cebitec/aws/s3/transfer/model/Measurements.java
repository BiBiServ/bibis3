package de.unibi.cebitec.aws.s3.transfer.model;

import java.text.DecimalFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Measurements {
    private static long overallBytes;
    private static long overallChunks;
    private static long finishedChunks;
    private static long start;
    private static long end;
    private static boolean started = false;
    private static final Logger log = LoggerFactory.getLogger(Measurements.class);

    private Measurements() {
    }

    public static void addToOverallBytes(long byteCount) {
        overallBytes += byteCount;
    }

    public static void setOverallChunks(long overallChunks) {
        Measurements.overallChunks = overallChunks;
    }

    public static void countChunkAsFinished() {
        finishedChunks += 1;
    }

    public static void start() {
        if (!started) {
            start = System.currentTimeMillis();
            started = true;
        }
    }

    public static void stop() {
        if (started) {
            end = System.currentTimeMillis();
        }
    }

    private static String formatResult(long bytes, String suffix) {
        DecimalFormat f = new DecimalFormat("#0.00");
        if (bytes > 1e9) {
            return new StringBuilder().append(f.format(bytes / 1e9)).append("GB").append(suffix).toString();
        }
        if (bytes > 1e6) {
            return new StringBuilder().append(f.format(bytes / 1e6)).append("MB").append(suffix).toString();
        }
        if (bytes > 1e3) {
            return new StringBuilder().append(f.format(bytes / 1e3)).append("KB").append(suffix).toString();
        }
        return new StringBuilder().append(bytes).append("B").append(suffix).toString();
    }

    public static String getChunksFinishedCount() {
        return new StringBuilder().append(finishedChunks).append(" / ").append(overallChunks).toString();
    }

    public static String getEndResult() {
        long seconds = (end - start) / 1000;
        if (seconds == 0) {
            return "unknown";
        }
        long bps = overallBytes / seconds;
        return formatResult(bps, "/s");
    }

    public static String getOverallBytesFormatted() {
        return formatResult(overallBytes, "");
    }

    public static long getOverallBytes() {
        return overallBytes;
    }

    public static void setOverallBytes(long overallBytes) {
        Measurements.overallBytes = overallBytes;
    }
}
