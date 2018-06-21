# BiBiS3

[![](https://jitpack.io/v/BiBiServ/bibis3.svg)](https://jitpack.io/#BiBiServ/bibis3)

Amazon Simple Storage Service (S3) is a cloud storage service with some very interesting characteristics for storing
large amounts of data. It has virtually infinite scalability both in terms of storage space and transfer speed.
Bioinformatics pipelines running on EC2 compute instances need fast access to the data stored in S3 but existing tools*
for S3 data transfer do not use the full potential S3 has to offer. (*Evaluated tools were `s3cmd` and `boto` (original
and modified versions, Mar 2013))

***BiBiS3*** is a command line tool that attempts to close this gap by pushing the transfer speeds both from and to S3
to the limits of the underlying hardware. Additionally ***BiBiS3*** in itself is as scalable as Amazon's S3 as it is
capable of downloading different chunks of the same data to an arbitrary number of machines simultaneously. The key
to maximum speed using S3 is massive, data-agnostic parallelization.

The targets can either be a single machine, a shared Network File System (NFS) between multiple nodes or the
filesystems of all the nodes. Directories can be copied recursively while ***BiBiS3*** maintains a stable count of
parallel transfer threads regardless of the directory structure.

In another scenario where the parts of a single file are to be evenly distributed across multiple machines,
***BiBiS3*** is performing a split of the data. In case of the FASTQ file format this split is even content-aware
and preserves all FASTQ entries. A distributed download can be invoked e.g. via the Oracle Grid Engine (OGE) which
is part of [BiBiGrid](https://github.com/BiBiServ/bibigrid).

***Features***
- ***Parallel*** transfer of multiple chunks of data.
- ***Recursive*** transfer of directories with ***parallelization of multiple files***.
- Simultaneous download ***via a cluster*** to e.g. a shared NFS where each node only downloads a portion of the data.

***Performance***
On a single AWS instance we have seen download speeds of over to 300 MByte/sec from S3. Using the distributed cluster
download mode ***BiBiS3*** downloads show an aggregate throughput of more than 22 GByte/sec on 80 c3.8xlarge instances.

## Compile, Build & Package

*Requirements: Java >= 8, Maven >= 3.3.9*

~~~BASH
> git clone https://github.com/BiBiServ/bibis3.git
> cd bibis3
> mvn clean package
~~~

## Setup
***Credentials File***

To get access to buckets that need proper authentication, create a .properties file called
`.aws-credentials.properties` in your user home directory with the following content:
~~~
accessKey=XXXXXXXXXXXXXXXX # your AWS access key
secretKey=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX # your AWS secret key
~~~

***Please note***: The Access Key and Secret Key are very sensitive information! Please make sure that the configuration file can only be read by you! E.g. use the following command: `chmod 600 ~/.aws-credentials.properties`

Alternatively the credentials can also be supplied via command-line parameters.

## Command-Line Usage
The basic commands follow the behavior of the Unix command cp as closely as possible.

~~~BASH
usage: java -jar bibis3-1.7.0.jar -u|d|g|c SRC DEST
    --access-key <arg>              AWS Access Key.
 -c,--clean-up-parts                Clean up all unfinished parts of
                                    previous multipart uploads that were
                                    initiated on the specified bucket over
                                    a week ago. BUCKET has to be an S3
                                    URL.
    --chunk-size <arg>              Multipart chunk size in Bytes.
    --create-bucket                 Create bucket if nonexistent.
 -d,--download                      Download files. SRC has to be an S3
                                    URL.
    --debug                         Debug mode.
    --endpoint <arg>                Endpoint for client authentication
                                    (default: standard AWS endpoint).
 -g,--download-url                  Download a file with Http GET from
                                    (pre-signed) S3-Http-Url. SRC has to
                                    be an Http-URL with Range support for
                                    Http GET.
    --grid-current-node <arg>       Identifier of the node that is running
                                    this program (must be 1 >= i <=
                                    grid-nodes.
    --grid-download                 Download only a subset of all chunks.
                                    This is useful for downloading e. g.
                                    to a shared filesystem via different
                                    machines simultaneously.
    --grid-download-feature-fastq   Download separate parts of a fastq
                                    file to different nodes into different
                                    files and make sure the file splits
                                    conserve the fastq file format.
    --grid-download-feature-split   Download separate parts of a single
                                    file to different nodes into different
                                    files all with the same name.
                                    (--grid-download required)
    --grid-nodes <arg>              Number of grid nodes.
 -h,--help                          Help.
 -m,--metadata <key> <value>        Adds metadata to all uploads. Can be
                                    specified multiple times for
                                    additional metadata.
 -q,--quiet                         Disable all log messages.
 -r,--recursive                     Enable recursive transfer of a
                                    directory.
    --reduced-redundancy            Set the storage class for uploads to
                                    Reduced Redundancy instead of
                                    Standard.
    --region <arg>                  S3 region. For AWS has to be one of:
                                    ap-south-1, eu-west-3, eu-west-2,
                                    eu-west-1, ap-northeast-2,
                                    ap-northeast-1, ca-central-1,
                                    sa-east-1, cn-north-1, us-gov-west-1,
                                    ap-southeast-1, ap-southeast-2,
                                    eu-central-1, us-east-1, us-east-2,
                                    us-west-1, cn-northwest-1, us-west-2
                                    (default: us-east-1).
    --secret-key <arg>              AWS Secret Key.
    --session-token <arg>           AWS Session Token.
    --streaming-download            Run single threaded download and send
                                    special progress info to STDOUT.
 -t,--threads <arg>                 Number of parallel threads to use
                                    (default: 50).
    --trace                         Extended debug mode.
 -u,--upload                        Upload files. DEST has to be an S3
                                    URL.
    --upload-list-stdin             Take list of files to upload from
                                    STDIN. In this case the SRC argument
                                    has to be omitted.
 -v,--version                       Version.
S3 URLs have to be in the form of: 's3://<bucket>/<key>', e.g.
's3://mybucket/mydatafolder/data.txt'. When using recursive transfer (-r)
the trailing slash of the directory is mandatory, e.g.
's3://mybucket/mydatafolder/'.
~~~

## Basic Examples
***Upload of a single file from the local directory to S3:***
~~~BASH
java -jar bibis3.jar -u myfile.tgz s3://mybucket/somedir/
~~~

***Download of a single file from S3 to the current directory:***
~~~BASH
java -jar bibis3.jar -d s3://mybucket/somedir/myfile.tgz .
~~~

***Download of a directory from S3 to a local directory called 'mydir' using 20 threads:***
~~~BASH
java -jar bibis3.jar -t 20 -r -d s3://mybucket/somedir/ mydir
~~~

Attention should be paid to the trailing slash of the S3 URL which in addition to the `-r` option is required
for the recursive transfer of a directory.

***Example shell script for the simultaneous download of all the contents of an S3 directory via a cluster:***

***simultaneous-download.sh:***
~~~BASH
#!/bin/bash
java -jar bibis3.jar \
--access-key "XXXXXXX" \
--secret-key "XXXXXXXXXXXX" \
--region eu-west-1 \
--grid-download \
--grid-nodes "$1" \
--grid-current-node "$SGE_TASK_ID" \
-d s3://mybucket/mydir/ targetdir
~~~
which could be run within an SGE/OGE with 5 nodes (4 cores each) as follows:
~~~BASH
qsub -pe multislot 4 -t 1-5 simultaneous-download.sh 5
~~~

The parameter `-pe multislot 4` ensures that the array job ist equally distributed among the nodes (leading to only
one task per node).

The targetdir is usually located inside a shared filesystem (e.g. NFS). However, if `--grid-download-feature-split`
is enabled, then the targetdir has to be local for each node.

## Grid Download Feature Flags
***Grid Download Feature Flags*** can be used in addition to `--grid-download`. When supplying one of these flags, the
file parts are saved to different files on different machines. Additionally these flags can be used to force a specific
split position for individual file types. Grid Download Feature Flags ***cannot be combined***. Only the last one
supplied will be in effect.

***Split:***

`--grid-download-feature-split`

Splits arbitrarily.

***FASTQ:***

`--grid-download-feature-fastq`

Preserves FASTQ entries as well as paired-ends/mate-pairs for files using Illumina sequence identifiers.

## Cleanup
The Amazon S3 documentation says:

> "Once you initiate a multipart upload, Amazon S3 retains all the parts until you either complete or abort the upload. Throughout its lifetime, you are billed for all storage, bandwidth, and requests for this multipart upload and its associated parts."

When an upload encounters a fatal error, the upload gets neither completed nor aborted. Already uploaded multipart
chunks remain in S3 but are invisible to the user. Therefore, it is recommended to clean up interrupted multipart
uploads periodically.

***Clean up the remainings of interrupted multipart uploads for the bucket 'mybucket' that were initiated more than 7 days ago:***
~~~BASH
java -jar bibis3.jar -c s3://mybucket/
~~~
