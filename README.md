# Processing Large CSV Files in Cloud Blob Storage

## Overview

When dealing with large CSV files stored in a cloud blob store, one may encounter limitations regarding the memory capacity of the service's running environment. This document outlines the challenges and solutions involved in processing such large CSV files efficiently without exceeding memory constraints.

## Problem Statement

The primary issue arises when the size of the CSV file is so large that processing it in one go would exceed the memory available to the service's pod. To address this, we designed functions that stream the data and partition it into manageable chunks for individual processing.

## Challenges

### 1. Streaming Data by Range

The need to stream data starting from a specific offset to an end offset or another specific range.

### 2. Chunking Blob Data

As the data in blob storage is in bytes and not structured by lines, it's challenging to chunk the data without knowing where each line ends.

### 3. Parallel Processing and Database Calls

To minimize the total processing time, it's crucial to perform the downloading and uploading of chunks in parallel with database operations.

## Solutions

### Challenge 1: Streaming Data by Range

AWS S3 offers an API to fetch data from a specified offset of a specified size. This functionality was unavailable in our existing server utility, so we extended it to include this capability in `server-utils > aws > s3utils.go`.
```go
// FileDownloadByRange Downloads the specified byte range of an object. For more information about
// the HTTP Range header, see https://www.rfc-editor.org/rfc/rfc9110.html#name-range
func (svc *S3Instance) FileDownloadByRange(bucketName string, key string, logger *logrus.Entry, offset int64, byteLength int64, fileLength int64) (string, int64, error) {
    ...
}
```

### Challenge 2: Chunking Blob Data

1. We utilize the API above to fetch chunks of data of a user-specified size.
2. This chunk is then converted into a string to extract CSV data. However, the challenge lies in the last row of each chunk, which may be split between two chunks. To address this, we process all rows except the last, storing it temporarily to append to the beginning of the next chunk.

```go
// FileDownloadByRange Downloads the specified byte range of an object. For more information about
// the HTTP Range header, see https://www.rfc-editor.org/rfc/rfc9110.html#name-range
func (svc *S3Instance) FileDownloadByRange(bucketName string, key string, logger *logrus.Entry, offset int64, byt ...
go
Copy code
csvDataArray, partialRowData, err = readAllCSVWithPartialData(partialRowData+string(csvDataBytes), currentOffset
if err != nil {
    resultArr = append(resultArr, CsvProcessingWithFunctionResult{Data: nil, Err: err})
    if optionsObj.BreakOnError {
        return resultArr
    }
}
```
### Challenge 3: Parallel Processing and Database Calls

The function breaks down large files into manageable chunks, uploads them back to S3, updates a database table with information about these chunks, and sends notifications via Kafka. This process involves two main tracks:

Track 1: Downloading chunk data, processing it, and uploading it back to S3.
Track 2: Database operations, including updating the database with chunk data URLs and IDs, and adding entries to the outbox table.
In case of any errors, updating the database is also part of Track 2. To efficiently manage these tracks, we implemented two Go routines:

Producer Go Routine: Handles Track 1, focusing on downloading, processing, and uploading chunk data.
```go
// s3OperationsForChunkCSVUploadAndNotify is responsible for streaming data from an S3 CSV file in
// chunks and uploading the processed chunks back to S3.
// This function operates concurrently with dbOperationschunkCSVUploadAndNotify, sharing state via channels.
// It employs a context to manage cancellation signals across goroutines, ensuring graceful shutdown on errors or completion.
func (fia *FileImportAuditUtility) s3OperationsForChunkCSVUploadAndNotify(ctx context.Context, cancel context.CancelFunc, bucket string,
	fileByteSize int64, fileLength int64, inputUrl string, headerPresent bool, s3BatchUpdateChan chan s3BatchChanData, s3BatchErrorChan chan error){...}
```

Consumer Go Routine: Manages Track 2, dealing with all database operations related to chunk processing and error handling.
```go
// dbOperationschunkCSVUploadAndNotify handles database updates based on the results of the S3 chunk processing.
// It listens for processed chunk information through a channel and updates database entries accordingly.
// Additionally, it sends a message to Kafka to notify other systems of the update.
// This function runs concurrently with s3OperationsForChunkCSVUploadAndNotify and uses a shared context for cancellation.
// It stops on receiving an error, a cancellation signal, or after completing all updates.
func (fia *FileImportAuditUtility) dbOperationschunkCSVUploadAndNotify(ctx context.Context, auditEntryId int, tag string, userId int64,
	kafkaMessageConfig KafkaMessageConfig, s3BatchUpdateChan chan s3BatchChanData, s3BatchErrorChan chan error){...}
```
![image](https://github.com/shoaib4/FileImportUtilis/assets/34734357/e82695ca-eced-405b-abbe-aa305b2e4184)

### Channels
#### dataChannel:

Purpose: Used for passing data from producer to consumer goroutines.

Description: This channel facilitates the transfer of processed data chunk details from the producer goroutine, responsible for downloading and processing chunks, to the consumer goroutine, which handles database updates and Kafka notifications.

#### errorChannel:

Purpose: Used for reporting errors encountered during processing.

Description: The producer goroutine sends any errors encountered during chunk processing or S3 operations to the consumer goroutine through this channel. This enables the consumer to handle errors appropriately, such as updating the database with error status and logging error messages.

#### doneChannel:

Purpose: Used to signal completion of processing.

Description: Once all data chunks have been processed and uploaded, the producer goroutine signals completion by sending a message through this channel. The consumer goroutine, upon receiving this signal, can perform any necessary cleanup tasks or finalize the processing workflow. This helps terminate the goroutines. 

By addressing these challenges with the outlined solutions, we can efficiently process large CSV files stored in cloud blob storage without exceeding the memory limits of our service's running environment.

PlantUML Diagram
<img width="582" alt="Screenshot 2024-05-29 at 3 36 45 PM" src="https://github.com/shoaib4/FileImportUtilis/assets/34734357/d2426496-626e-4f6e-a24c-9886d8a4981e">

