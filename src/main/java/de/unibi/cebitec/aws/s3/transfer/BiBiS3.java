package de.unibi.cebitec.aws.s3.transfer;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Region;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import de.unibi.cebitec.aws.s3.transfer.ctrl.Cleaner;
import de.unibi.cebitec.aws.s3.transfer.ctrl.Downloader;
import de.unibi.cebitec.aws.s3.transfer.ctrl.Uploader;
import de.unibi.cebitec.aws.s3.transfer.ctrl.UrlDownloader;
import de.unibi.cebitec.aws.s3.transfer.model.GridDownloadOrganizer;
import de.unibi.cebitec.aws.s3.transfer.model.InputFileList;
import de.unibi.cebitec.aws.s3.transfer.model.OutputFileList;
import de.unibi.cebitec.aws.s3.transfer.streaming.Streamer;
import de.unibi.cebitec.aws.s3.transfer.streaming.UrlStreamer;
import de.unibi.cebitec.aws.s3.transfer.util.CredentialsProvider;
import de.unibi.cebitec.aws.s3.transfer.util.S3RegionsProvider;
import de.unibi.cebitec.aws.s3.transfer.util.S3URI;
import de.unibi.cebitec.aws.s3.transfer.util.StdinInputReader;
import de.unibi.cebitec.aws.s3.transfer.util.UploadFilesCrawler;

import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BiBiS3. S3 is a storage service for huge amounts of data. EC2 instances can be used to process that data but
 * first they need a way to get to it. BiBiS3 probably takes the most effective approach to transfer data both
 * to and from S3 in a fast manner: Massive parallelization of everything. It takes into account that a fileset
 * may consist of very few large files or lots of small files. Small files and chunks of large files are processed
 * in parallel and therefore allow for data-independent constantly high speed results.
 *
 * @author christian@cebitec.uni-bielefeld.de
 */
public class BiBiS3 {
    public static final Logger log = LoggerFactory.getLogger(BiBiS3.class);
    public static final long CHUNK_SIZE = 26214400; // 25MB
    public static final long MIN_CHUNK_SIZE = 5242880; // 5MB is dictated by s3
    public static final String DEFAULT_REGION = "us-east-1";
    public static final int DEFAULT_THREAD_COUNT = 50;
    public static final int RETRIES = 6;
    public static final int INCOMPLETE_HTTP_RESPONSE_RETRIES = 10;

    /**
     * We disable the logging of the SDK (mostly used by the Apache HTTP Client)
     * as all the important information is thrown as exceptions anyway.
     */
    static {
        System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog");
    }

    public static void main(String[] args) {
        Locale.setDefault(Locale.ENGLISH);

        // tweak connection settings to minimize timeouts
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setConnectionTimeout(1000 * 30); // 30 sec
        clientConfig.setSocketTimeout(1000 * 30); // 30 sec
        clientConfig.setMaxErrorRetry(RETRIES);

        CommandLineParser cli = new DefaultParser();

        Options infoOptions = new Options();
        infoOptions
                .addOption(Option.builder("h").longOpt("help").desc("Help").build())
                .addOption(Option.builder("v").longOpt("version").desc("Version").build());

        OptionGroup intentOptions = new OptionGroup();
        intentOptions.setRequired(true);

        // create mutually exclusive command-line options
        intentOptions
                .addOption(Option.builder("u").longOpt("upload").desc("Upload files. DEST has to be an S3 URL.").build())
                .addOption(Option.builder("d").longOpt("download").desc("Download files. SRC has to be an S3 URL.").build())
                .addOption(Option.builder("g").longOpt("download-url").desc("Download a file with Http GET from (pre-signed) S3-Http-Url. SRC has to be an Http-URL with Range support for Http GET.").build())
                .addOption(Option.builder("c").longOpt("clean-up-parts").desc("Clean up all unfinished parts of previous multipart uploads that were initiated on the specified bucket over a week ago. BUCKET has to be an S3 URL.").build());

        Map<String, Region> regions = S3RegionsProvider.getS3Regions();
        // help text for region selection
        StringBuilder s3RegionInfo = new StringBuilder();
        s3RegionInfo.append("S3 region. For AWS has to be one of: ");
        for (String regionName : regions.keySet()) {
            s3RegionInfo.append(regionName);
            s3RegionInfo.append(", ");
        }

        // remove last comma
        s3RegionInfo.delete(s3RegionInfo.length() - 2, s3RegionInfo.length());
        s3RegionInfo.append(" (default: ").append(DEFAULT_REGION).append(").");

        Options actionOptions = new Options();
        actionOptions
                .addOptionGroup(intentOptions)
                .addOption(Option.builder("r").longOpt("recursive").desc("Enable recursive transfer of a directory.").build())
                .addOption(Option.builder().longOpt("debug").desc("Debug mode.").build())
                .addOption(Option.builder().longOpt("trace").desc("Extended debug mode.").build())
                .addOption(Option.builder("h").longOpt("help").desc("Help.").build())
                .addOption(Option.builder("v").longOpt("version").desc("Version.").build())
                .addOption(Option.builder("q").longOpt("quiet").desc("Disable all log messages.").build())
                .addOption(Option.builder("t").longOpt("threads").hasArg().desc("Number of parallel threads to use (default: " + DEFAULT_THREAD_COUNT + ").").build())
                .addOption(Option.builder().longOpt("access-key").hasArg().desc("AWS Access Key.").build())
                .addOption(Option.builder().longOpt("secret-key").hasArg().desc("AWS Secret Key.").build())
                .addOption(Option.builder().longOpt("session-token").hasArg().desc("AWS Session Token.").build())
                .addOption(Option.builder().longOpt("chunk-size").hasArg().desc("Multipart chunk size in Bytes.").build())
                .addOption(Option.builder().longOpt("streaming-download").desc("Run single threaded download and send special progress info to STDOUT.").build())
                .addOption(Option.builder().longOpt("region").hasArg().desc(s3RegionInfo.toString()).build())
                .addOption(Option.builder().longOpt("endpoint").hasArg().desc("Endpoint for client authentication (default: standard AWS endpoint).").build())
                .addOption(Option.builder().longOpt("create-bucket").desc("Create bucket if nonexistent.").build())
                .addOption(Option.builder().longOpt("grid-download").desc("Download only a subset of all chunks. This is useful for downloading e. g. to a shared filesystem via different machines simultaneously.").build())
                .addOption(Option.builder().longOpt("grid-download-feature-split").desc("Download separate parts of a single file to different nodes into different files all with the same name. (--grid-download required)").build())
                .addOption(Option.builder().longOpt("grid-download-feature-fastq").desc("Download separate parts of a fastq file to different nodes into different files and make sure the file splits conserve the fastq file format.").build())
                .addOption(Option.builder().longOpt("grid-nodes").hasArg().desc("Number of grid nodes.").build())
                .addOption(Option.builder().longOpt("grid-current-node").hasArg().desc("Identifier of the node that is running this program (must be 1 >= i <= grid-nodes.").build())
                .addOption(Option.builder().longOpt("upload-list-stdin").desc("Take list of files to upload from STDIN. In this case the SRC argument has to be omitted.").build())
                .addOption(Option.builder("m").longOpt("metadata").desc("Adds metadata to all uploads. Can be specified multiple times for additional metadata.").hasArgs().numberOfArgs(2).argName("key> <value").build())
                .addOption(Option.builder().longOpt("reduced-redundancy").desc("Set the storage class for uploads to Reduced Redundancy instead of Standard.").build());

        // Get the root logger instance of the logback logger implementation to be able to set the logging level at runtime.
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(ch.qos.logback.classic.Level.INFO);

        try {
            CommandLine cl = cli.parse(infoOptions, args);
            if (cl.hasOption("h")) {
                printHelp(actionOptions);
                System.exit(0);
            }
            if (cl.hasOption("v")) {
                try {
                    URL jarUrl = BiBiS3.class.getProtectionDomain().getCodeSource().getLocation();
                    String jarPath = URLDecoder.decode(jarUrl.getFile(), "UTF-8");
                    JarFile jarFile = new JarFile(jarPath);
                    Manifest m = jarFile.getManifest();
                    String versionInfo =
                            String.format("Version: %s\nBuild: %s\nGit Revision: %s\nMercurial Revision: %s",
                                    m.getMainAttributes().getValue("Version"), m.getMainAttributes().getValue("Build"),
                                    m.getMainAttributes().getValue("Git-Revision"),
                                    m.getMainAttributes().getValue("Mercurial-Revision"));
                    System.out.println(versionInfo);
                } catch (Exception e) {
                    log.error("Version info could not be read.");
                }
            }
            System.exit(0);
        } catch (ParseException ignored) {
        }

        AWSCredentials credentials = null;
        try {
            CommandLine cl = cli.parse(actionOptions, args);
            String[] positionalArgs = cl.getArgs();
            // Adjust number of required CLI parameters depending on whether upload-list-stdin is set or whether this is a clean up operation.
            if (cl.hasOption("upload-list-stdin")) {
                if (positionalArgs.length < 1) {
                    throw new ParseException("Missing required argument: DEST");
                } else if (positionalArgs.length > 1) {
                    throw new ParseException("Using STDIN file list. SRC has to be omitted.");
                }
            } else {
                if (cl.hasOption("clean-up-parts")) {
                    if (positionalArgs.length != 1) {
                        throw new ParseException("Missing required argument: BUCKET");
                    }
                } else {
                    if (positionalArgs.length < 1) {
                        throw new ParseException("Missing required arguments: SRC DEST");
                    } else if (positionalArgs.length < 2) {
                        throw new ParseException("Missing required argument: DEST");
                    }
                }
            }

            if (cl.hasOption("debug")) {
                root.setLevel(ch.qos.logback.classic.Level.DEBUG);
            }
            if (cl.hasOption("trace")) {
                root.setLevel(ch.qos.logback.classic.Level.TRACE);
            }
            if (cl.hasOption("q")) {
                root.setLevel(ch.qos.logback.classic.Level.OFF);
            }

            // Override file credentials with CLI parameters if present.
            if (cl.hasOption("access-key") && cl.hasOption("secret-key")) {
                if (cl.hasOption("session-token")) {
                    credentials = new BasicSessionCredentials(cl.getOptionValue("access-key"),
                            cl.getOptionValue("secret-key"), cl.getOptionValue("session-token"));
                } else {
                    credentials = new BasicAWSCredentials(cl.getOptionValue("access-key"),
                            cl.getOptionValue("secret-key"));
                }
            } else {
                credentials = CredentialsProvider.getCredentials();
            }

            String src = "";
            String dest = "";
            if (positionalArgs.length == 1) {
                dest = positionalArgs[0];
            } else if (positionalArgs.length == 2) {
                src = positionalArgs[0];
                dest = positionalArgs[1];
            }

            // Streaming download has its own handler.
            if (cl.hasOption("streaming-download")) {
                if (cl.hasOption("d")) {
                    // we don't want the logger to mess up our progress output unless he has serious concerns
                    root.setLevel(ch.qos.logback.classic.Level.ERROR);
                    S3URI s3uri = new S3URI(src);
                    Streamer streamer = new Streamer(s3uri.getKey(), FileSystems.getDefault().getPath(dest));
                    streamer.download(credentials, clientConfig, s3uri.getBucket());
                    // Streaming download ends here. No parallelization as of yet.
                    System.exit(0);
                } else if (cl.hasOption("g")) {
                    // we don't want the logger to mess up our progress output unless he has serious concerns
                    root.setLevel(ch.qos.logback.classic.Level.ERROR);
                    UrlStreamer streamer = new UrlStreamer(src, FileSystems.getDefault().getPath(dest));
                    streamer.download(src);
                    // Streaming download ends here. No parallelization as of yet.
                    System.exit(0);
                }
            }

            // Handle override of default chunk size.
            long chunkSize = CHUNK_SIZE;
            if (cl.hasOption("chunk-size")) {
                String chunkSizeStr = cl.getOptionValue("chunk-size");
                try {
                    long newchunkSize = Long.parseLong(chunkSizeStr);
                    if (newchunkSize < MIN_CHUNK_SIZE && cl.hasOption("u")) {
                        log.warn("Invalid chunk size: {}. Must be >= {}. Using defaults.", newchunkSize, MIN_CHUNK_SIZE);
                    } else {
                        chunkSize = newchunkSize;
                    }
                } catch (NumberFormatException e) {
                    log.warn("Chunk size is not a number! Using defaults.");
                }
            }

            // Set up and run the uploader/downloader.
            try {
                try {
                    // Override thread count with CLI parameter if present.
                    int numOfThreads = DEFAULT_THREAD_COUNT;
                    try {
                        numOfThreads = Integer.parseInt(cl.getOptionValue("t", "" + DEFAULT_THREAD_COUNT));
                    } catch (NumberFormatException e) {
                        throw new ParseException("Invalid integer value for -t");
                    }
                    clientConfig.setMaxConnections(numOfThreads + 10);
                    if (cl.hasOption("u") || cl.hasOption("d") || cl.hasOption("g")) {
                        log.info("== Copying from '{}' to '{}' in {} threads. Chunk size: {} Bytes", src, dest, numOfThreads, chunkSize);
                    }

                    String region = null;
                    // Override region with CLI parameter if present.
                    if (cl.hasOption("region")) {
                        region = cl.getOptionValue("region");
                    }
                    if (region == null) {
                        region = DEFAULT_REGION;
                    }
                    log.info("== Access key: {}   Bucket region: {}", credentials == null ? "none" : credentials.getAWSAccessKeyId(), region);

                    String endpoint = null;
                    // Override endpoint with CLI parameter if present.
                    if (cl.hasOption("endpoint")) {
                        endpoint = cl.getOptionValue("endpoint");
                    }

                    AmazonS3ClientBuilder builder = AmazonS3Client.builder();
                    builder = endpoint == null ?
                            builder.withRegion(region) :
                            builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region));
                    final AmazonS3 s3 = builder.withClientConfiguration(clientConfig)
                            .withCredentials(new AWSStaticCredentialsProvider(credentials))
                            .build();

                    if (cl.hasOption("u")) {
                        // Upload task.
                        Path srcPath = null;
                        if (!cl.hasOption("upload-list-stdin")) {
                            srcPath = FileSystems.getDefault().getPath(src);
                        }
                        S3URI s3uri = new S3URI(dest);
                        boolean bucketExists = s3.doesBucketExist(s3uri.getBucket());
                        // Create bucket if necessary and requested.
                        if (cl.hasOption("create-bucket") && !bucketExists && !credentials.getAWSSecretKey().isEmpty()) {
                            log.warn("Bucket '{}' does not exist yet and will be created.", s3uri.getBucket());
                            CreateBucketRequest request = new CreateBucketRequest(s3uri.getBucket(), region);
                            s3.createBucket(request);
                        } else if (!bucketExists) {
                            log.error("Bucket '{}' does not exist! For automatic bucket creation use --create-bucket in combination with --region.", s3uri.getBucket());
                            throw new IllegalArgumentException("Bucket does not exist!");
                        }

                        // File lists to fill with files to be uploaded.
                        InputFileList<Path> filesToUpload = new InputFileList<>();
                        OutputFileList<Path, String> uploadTargetKeys = new OutputFileList<>();

                        if (cl.hasOption("upload-list-stdin")) {
                            log.info("Using STDIN file list.");
                            StdinInputReader reader = new StdinInputReader();
                            List<String> files = reader.getStdinLines();
                            for (String file : files) {
                                if (!file.isEmpty()) {
                                    Path filePath = Paths.get(file);
                                    filesToUpload.put(filePath, Files.size(filePath));
                                    String key = s3uri.getKey() + filePath.getFileName().toString();
                                    uploadTargetKeys.put(filePath, key);
                                    log.debug("Adding file via STDIN: {} {}", filePath, key);
                                }
                            }
                        } else {
                            if (cl.hasOption("r")) {
                                // Recursive upload.
                                if (srcPath.toFile().isFile()) {
                                    log.error("Recursive option is set. Please specify a directory instead of a file as SRC.");
                                }
                                // fill file list
                                UploadFilesCrawler crawler = new UploadFilesCrawler(s3uri.getKey());
                                try {
                                    Files.walkFileTree(srcPath, crawler);
                                } catch (IOException | SecurityException e) {
                                    log.error("Error while accessing some or all files in the source directory.");
                                }
                                filesToUpload = crawler.getFiles();
                                uploadTargetKeys = crawler.getTargetKeys();
                            } else {
                                // Single file upload.
                                if (srcPath.toFile().isDirectory()) {
                                    log.error("{} is a directory. Use -r for recursive upload.", src);
                                }
                                // upload single file
                                filesToUpload.put(srcPath, Files.size(srcPath));
                                String key;
                                if (s3uri.getKey().endsWith("/") || s3uri.getKey().isEmpty()) {
                                    key = s3uri.getKey() + srcPath.getFileName().toString();
                                } else {
                                    key = s3uri.getKey();
                                }
                                uploadTargetKeys.put(srcPath, key);
                            }
                        }
                        ObjectMetadata metadata = new ObjectMetadata();
                        if (cl.hasOption("metadata")) {
                            log.info("Adding metadata to all uploads:");
                            for (Entry<Object, Object> entry : cl.getOptionProperties("metadata").entrySet()) {
                                log.info("    " + entry.getKey().toString() + " = " + entry.getValue().toString());
                                metadata.addUserMetadata(entry.getKey().toString(), entry.getValue().toString());
                            }
                        }
                        // Instantiate uploader and start upload. Finally.
                        Uploader up = new Uploader(s3, filesToUpload, s3uri.getBucket(), uploadTargetKeys, numOfThreads, chunkSize, metadata, cl.hasOption("reduced-redundancy"));
                        up.upload();
                        log.info("Upload successful.");

                    } else if (cl.hasOption("d")) {
                        // Download task.
                        Path destination = Paths.get(dest);
                        S3URI s3uri = new S3URI(src);
                        String keyPrefix = s3uri.getKey();

                        // File lists to fill with files to be downloaded.
                        InputFileList<String> filesToDownload = new InputFileList<>();
                        OutputFileList<String, Path> fileDownloadDestinations = new OutputFileList<>();

                        if (cl.hasOption("r")) {
                            // get list of files to download for given prefix
                            ListObjectsRequest listReq = new ListObjectsRequest();
                            listReq.setBucketName(s3uri.getBucket());
                            listReq.setPrefix(keyPrefix);
                            listReq.setMaxKeys(Integer.MAX_VALUE);
                            ObjectListing listing = s3.listObjects(listReq);
                            List<S3ObjectSummary> items = listing.getObjectSummaries();

                            String dirname1, dirname = s3uri.getBucket();
                            if (!keyPrefix.isEmpty()) {
                                dirname1 = keyPrefix.substring(0, keyPrefix.length() - 1);
                                if (dirname1.contains("/")) {
                                    dirname = dirname1.substring(dirname1.lastIndexOf("/") + 1);
                                } else {
                                    dirname = keyPrefix;
                                }
                            }

                            for (S3ObjectSummary item : items) {
                                if (item.getKey().length() > keyPrefix.length()) {
                                    filesToDownload.put(item.getKey(), item.getSize());
                                    String relativePathString = item.getKey().substring(keyPrefix.length());
                                    log.debug("relative path string: {}", relativePathString);
                                    Path fileDestinationPath = destination;
                                    if (destination.startsWith(".") && destination.endsWith(".")) {
                                        fileDestinationPath = fileDestinationPath.resolve(dirname);
                                    }
                                    fileDestinationPath = fileDestinationPath.resolve(relativePathString);
                                    log.debug("DEST-X: {}", fileDestinationPath);
                                    fileDownloadDestinations.put(item.getKey(), fileDestinationPath);
                                }
                            }
                            if (filesToDownload.isEmpty()) {
                                log.error("No files available for recursive download. Please provide an existing and non-empty directory as SRC.");
                                System.exit(1);
                            }
                        } else {
                            try {
                                ObjectMetadata meta = null;
                                for (int i = 0; i < RETRIES; i++) {
                                    try {
                                        meta = s3.getObjectMetadata(s3uri.getBucket(), keyPrefix);
                                        break;
                                    } catch (Exception e) {
                                        log.warn("Metadata request failed! Retrying.... ({})", e.toString());
                                    }
                                    if (i == RETRIES - 1) {
                                        log.error("Metadata request failed after {} retries. Exiting...", i);
                                        System.exit(1);
                                    }
                                }
                                filesToDownload.put(keyPrefix, meta.getContentLength());
                                if (destination.toFile().isDirectory()) {
                                    String filename;
                                    if (keyPrefix.contains("/")) {
                                        filename = keyPrefix.substring(keyPrefix.lastIndexOf("/") + 1);
                                    } else {
                                        filename = keyPrefix;
                                    }
                                    Path fullDestination = destination.resolve(filename);
                                    fileDownloadDestinations.put(keyPrefix, fullDestination);
                                    log.debug("Full Destination: {}", fullDestination);
                                } else {
                                    fileDownloadDestinations.put(keyPrefix, destination);
                                }
                            } catch (AmazonClientException e) {
                                log.error("SRC does not exist. Or maybe SRC is a directory (in that case use -r for recursive transfer) (Third possibility: Wrong credentials).");
                                System.exit(1);
                            }
                        }

                        Downloader down;
                        if (cl.hasOption("grid-download") && cl.hasOption("grid-nodes") && cl.hasOption("grid-current-node")) {
                            // If this download is a grid download, then parse additional CLI parameters and create an organizer.
                            int nodesCount = Integer.parseInt(cl.getOptionValue("grid-nodes"));
                            int currentNode = Integer.parseInt(cl.getOptionValue("grid-current-node"));
                            GridDownloadOrganizer organizer = new GridDownloadOrganizer(nodesCount, currentNode);
                            if (cl.hasOption("grid-download-feature-split")) {
                                organizer.setFeature("split");
                                if (cl.hasOption("r")) {
                                    log.error("The grid download split feature works with single files only! (no -r)");
                                    System.exit(1);
                                }
                            }
                            if (cl.hasOption("grid-download-feature-fastq")) {
                                organizer.setFeature("fastq");
                                if (cl.hasOption("r")) {
                                    log.error("The grid download fastq feature works with single files only! (no -r)");
                                    System.exit(1);
                                }
                            }
                            down = new Downloader(s3, s3uri.getBucket(), filesToDownload, fileDownloadDestinations, numOfThreads, chunkSize, organizer);
                        } else {
                            // No grid download.
                            down = new Downloader(s3, s3uri.getBucket(), filesToDownload, fileDownloadDestinations, numOfThreads, chunkSize);
                        }
                        // Start download.
                        down.download();
                        log.info("Download successful.");
                    } else if (cl.hasOption("g")) {
                        // Download URL task.
                        Path destination = Paths.get(dest);
                        UrlDownloader down;
                        if (cl.hasOption("grid-download") && cl.hasOption("grid-nodes") && cl.hasOption("grid-current-node")) {
                            // If this download is a grid download, then parse additional CLI parameters and create an organizer.
                            int nodesCount = Integer.parseInt(cl.getOptionValue("grid-nodes"));
                            int currentNode = Integer.parseInt(cl.getOptionValue("grid-current-node"));
                            GridDownloadOrganizer organizer = new GridDownloadOrganizer(nodesCount, currentNode);
                            if (cl.hasOption("grid-download-feature-split")) {
                                organizer.setFeature("split");
                                if (cl.hasOption("r")) {
                                    log.error("The grid download split feature works with single files only! (no -r)");
                                    System.exit(1);
                                }
                            }
                            if (cl.hasOption("grid-download-feature-fastq")) {
                                organizer.setFeature("fastq");
                                if (cl.hasOption("r")) {
                                    log.error("The grid download fastq feature works with single files only! (no -r)");
                                    System.exit(1);
                                }
                            }
                            down = new UrlDownloader(src, destination, numOfThreads, chunkSize, organizer);
                        } else {
                            // No grid download.
                            down = new UrlDownloader(src, destination, numOfThreads, chunkSize);
                        }
                        // Start download.
                        down.download();
                        log.info("Download successful.");
                    } else if (cl.hasOption("c")) {
                        S3URI s3uri = new S3URI(dest);
                        log.info("== Bucket: {}", s3uri.getBucket());
                        Cleaner cleaner = new Cleaner(s3, s3uri.getBucket());
                        cleaner.cleanUpParts();
                    }
                    System.exit(0);
                } catch (IllegalArgumentException e) {
                    log.error("Invalid argument: {}", e.getMessage());
                }
            } catch (IOException e) {
                log.error("IOError: {}", e.getMessage());
            }

        } catch (ParseException e) {
            log.error("{}", e.getMessage());
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                log.error("S3 key not found!");
                System.exit(1);
            }
            switch (e.getErrorCode()) {
                case "AccessDenied":
                    String ak = "NO-CREDENTIALS-PROVIDED";
                    if (credentials != null) {
                        ak = credentials.getAWSAccessKeyId();
                    }
                    log.error("Access denied. The provided credentials have insufficient rights to access this " +
                            "bucket. Access Key: '{}'", ak);
                    break;
                default:
                    log.error("S3 Error: {}", e);
                    break;
            }
        } catch (Exception e) {
            log.error("An error occurred during the transfer: {}", e.toString());
            log.trace("{}", Arrays.asList(e.getStackTrace()));
        }
        System.exit(1);
    }

    private static void printHelp(Options opts) {
        HelpFormatter help = new HelpFormatter();
        // Determine jar filename.
        String jarFilename;
        try {
            String uri = BiBiS3.class.getProtectionDomain().getCodeSource().getLocation().toURI().toString();
            jarFilename = uri.substring(uri.lastIndexOf("/") + 1);
        } catch (Exception e) {
            jarFilename = "<jarfile>";
        }
        String header = "";
        String footer = "S3 URLs have to be in the form of: 's3://<bucket>/<key>', e.g. " +
                "'s3://mybucket/mydatafolder/data.txt'. When using recursive transfer (-r) " +
                "the trailing slash of the directory is mandatory, e.g. 's3://mybucket/mydatafolder/'.";
        help.printHelp("java -jar " + jarFilename + " -u|d|g|c SRC DEST", header, opts, footer);
    }
}
