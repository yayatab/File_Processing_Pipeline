# Design Decisions

## 1. Large File Handling
**Approach chosen**: Streaming<br>
**Why:** to make sure i'm reading as little as i need. not to load all the 100+ mb to memory.<br>
**Memory usage:** [how you keep memory bounded]: using as much as i can the context managment in python. 

## 2. Step Failure Strategy
**Approach chosen:** Retry + Fail after X attempts<br>
**Why:** i think it's best user experience + resource managemnet. (not ot let oone job exhaust all the resources)<br>
**Partial progress:** when a step fails, i don't contine to the next one

## 3. File Cleanup Strategy
**Approach chosen:** cron job that deletes files.
**Input files:** deleted after read.
**Intermediate files:** deleted when done
**Output files:** deleted when expired
#### not implemented correctly.
#### in real world scenario, i would make use of an s3 upload link. and read the input form there
 

## 4. Progress Tracking
**Approach chosen:** in DB
**Granularity:** Updated after each step
**Trade-offs:** a lot of overhead, 90% visibility

## 5. One Thing I Would Do Differently With More Time
Expand on test plan and scenarios and would add CRUD
in real worl scenario, i would also add datadog/Elasticsearch and a helm chart with prod/staging/dev values
