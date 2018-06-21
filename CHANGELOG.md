# Changelog

## 1.7.0
- TODO

## 1.6.1
- Dependency update: AWS SDK for Java 1.9.27 -> 1.10.16 (includes joda time 2.8.1)

## 1.6.0
- Added mode `-c` to clean up the remains of interrupted multipart uploads.
- Added option `--reduced-redundancy` to store uploads with reduced redundancy.
- Dependency update: AWS SDK for Java 1.9.6 -> 1.9.27

## 1.5.2
- Fix: As S3 has no concept of real folders, AWS S3 Web Console simulates an empty folder by creating a zero-length file with the filename ending on a trailing slash. From now on these empty folders are created on the receiving end when using recursive downloads.

## 1.5.1
- Dependency update: AWS SDK for Java 1.8.9.1 -> 1.9.6 (support for eu-central-1)
- Dependency update: logback 1.0.13 -> 1.1.2

## 1.5.0
- Added the ability to add additional metadata to all uploads by using `-m/--metadata <key> <value>`.

## 1.4.2
- Important bug-fix: Downloads using a Grid Download Feature Flag were using wrong offsets when writing to the output file.
- AWS SDK for Java update: 1.4.5 -> 1.8.9.1
- FASTQ file format detection for FASTQ split downloads.

## 1.4.1
- Raised the logging threshold for streaming mode from WARN to ERROR.

## 1.4.0
- Important bug-fix: Automatically resume interrupted chunk downloads.
- Added retry for interrupted single file downloads.
- Added retry for failed S3 region list request.
- Default connection timeout is now at 30 sec (was 5 min).

## 1.3.3
- Updated dependency 'logback' from 1.0.11 -> 1.0.13 because of [http://jira.qos.ch/browse/LOGBACK-749%7Cbug](http://jira.qos.ch/browse/LOGBACK-749%7Cbug).
- Enabled automatic retries for initial metadata request that is issued before download action occurs.

## 1.3.2
- Improved feedback for ambiguous S3 URL (missing trailing slash).
- Added option `--trace` for easier debugging.

## 1.3.0
`- Added support for the download via a pre-signed S3 HTTP URL. This works for both standard and grid downloads. (Thanks to Thomas!)

## 1.2.1
- Fixed a bug where the minimum chunk upload size restriction of 5MB was also applied to downloads despite the fact that minimum download chunk sizes are not required by the S3 service

## 1.2.0
- Added `--grid-download-feature-fastq`. This feature alters the offsets of the grid download splits in a way that FASTQ file parts remain valid. It also preserves paired-ends/mate-pairs in interleaved FASTQ files containing Illumina sequence identifiers..
- AWS SDK for Java update: 1.4.3 -> 1.4.5

## 1.1.0
- Added `--grid-download-feature-split` which enables the grid download to save the data to independent smaller files (all identically named but on different nodes)
- Added `--session-token` for temporary authentication support

## 1.0.3
- Added automatic retries for all chunk transfer operations to compensate for connection-related S3 failures (default: 6 retries)
- Disabled INFO logging for the HTTP Client logger of the AWS Java SDK

## 1.0.2
- Fixed a bug where the use of the STDIN file list would result in the attempt to upload an empty filename
