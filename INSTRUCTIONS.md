# File_Processing_Pipeline
interview


# Task
You'll be building a file processing pipeline that accepts file uploads, processes them through
configurable steps, and delivers results. This pattern is common in document processing, media
transcoding, data import, and ETL systems.

## System Overview
System Overview
Build a file processing pipeline with these components:
1. Upload API - Accepts files, returns processing job ID
2. Job Queue - Tracks processing jobs
3. Workers - Execute processing steps
4. Storage - Stores input files, intermediate results, and outputs
5. Status API - Query job progress and retrieve results

## Functional Requirements

### File Upload
* Upload a file with processing instructions
* Support files up to 100MB 
* Return job ID immediately (async processing)
* Validate file type before accepting

### Processing Pipelines
Define pipelines as a sequence of steps. Implement these processing steps:
1. Validate
    * Check file format matches expected type
    * Verify file isn't corrupted
    * Extract metadata (size, type, etc.)
2. Transform (for CSV/JSON files)
    * Filter rows based on criteria
    * Select specific columns/fields
    * Apply simple transformations (uppercase, lowercase, trim)
3. Convert
    * CSV to JSON
    * JSON to CSV
    * (Extensible for other formats)
4. Compress/Decompress
    * Gzip compression
    * Zip extraction
5. Notify
    * Webhook callback when processing completes
    * Include job status and result location


### Pipline Example
example in [here](./resources/pipeline_example.json)

### Job Status
* Get current status of a job
* Get detailed progress (which step, percentage if available)
* Get result file location when complete
* Get error details if failed

### Result Retrieval
* Download processed file
* Download intermediate results (optional)
* Results available for configurable retention period (e.g., 24 hours)

### Step-Level Tracking
Each step has its own status:
* PENDING - Not started yet 
* RUNNING - Currently executing
* COMPLETED - Finished successfully
* FAILED - Failed (with error message)
* SKIPPED - Skipped due to previous failure

### Data Model

Design your data model to support:
#### Job
* Unique identifier
* Input file reference
* Pipeline definition
* Overall status
* Current step index
* Output file reference (when complete)
* Error message (if failed)
* Created at, started at, completed at
* User/API key reference
#### JobStep
* Job relationship
* Step index
* Step type and parameters
* Status
* Input file reference
* Output file reference
* Error message (if failed)
* Started at, completed at
* Duration


#### File Reference
* Unique identifier
* Storage path
* Original filename
* Size
* Content type
* Created at
* Expires at


## Critical Implementation Details
1. Large File Handling 
* Files can be up to 100MB. Consider:
* Streaming uploads (don't load entire file in memory)
* Streaming processing where possible
* Chunked processing for transforms
* Disk-based intermediate storage

2. Step fail recovery

3. cleanup and retention

4. Progress Tracking

5. Webhook Reliability
If the notify step's webhook call fails:
* Retry count - if failed
* Is the job considered failed?
* How do you prevent duplicate notifications? - i chose tabbit mq and a dedicated notifications sevice


## Technical Requirements

Must Have
* Python 3.11+
* Web framework of your choice
* File storage (local filesystem is fine for this exercise)
* Job queue mechanism
* At least 4 processing steps implemented
* Support for pipeline chaining
* Containerized setup (docker-compose)
* At least 6 meaningful tests:
    * File upload and job creation
    * Pipeline execution end-to-end
    * Step failure handling
    * Job status tracking
    * File retrieval

Should Have
* Streaming file upload (not loading full file in memory)
* Step-level progress tracking
* Configurable retry logic 
* Structured logging with job context

Nice To Have
* Resume failed jobs from last successful step 
* Parallel step execution (for independent steps)
* File type auto-detection 
* Processing time estimation