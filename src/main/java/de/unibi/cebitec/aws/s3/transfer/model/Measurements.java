package de.unibi.cebitec.aws.s3.transfer.model;

import java.text.DecimalFormat;

public final class Measurements {
    private static long overallBytes;
    private static long overallChunks;
    private static long finishedChunks;
    private static long start;
    private static long end;
    private static boolean started = false;

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
            return f.format(bytes / 1e9) + "GB" + suffix;
        }
        if (bytes > 1e6) {
            return f.format(bytes / 1e6) + "MB" + suffix;
        }
        if (bytes > 1e3) {
            return f.format(bytes / 1e3) + "KB" + suffix;
        }
        return String.valueOf(bytes) + "B" + suffix;
    }

    public static String getChunksFinishedCount() {
        return finishedChunks + " / " + overallChunks;
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
