# File Processing Pipeline

An asynchronous file processing pipeline that accepts file uploads, processes them through configurable steps, and stores the results. It is built natively for horizontal scale, employing an event-driven architecture using distinct microservice queue subscribers.

## Tech Stack
- **Python 3.12** (uv package manager)
- **FastAPI** (API Layer)
- **FastStream** (RabbitMQ Worker Framework)
- **SQLModel & MySQL** (Data Layer)
- **Docker & Docker Compose** (Containerization)
- **Logfire** (Distributed OpenTelemetry Tracing)

## 1. How to Run: Docker Mode (Recommended)

1. Ensure you have Docker and Docker Compose installed.
2. Clone the repository and navigate to the root directory.
3. Start the infrastructure and application services:
   ```bash
   docker compose up -d --build
   ```
4. Access the API documentation (Swagger UI) at:
   [http://localhost:8000/docs](http://localhost:8000/docs)
5. The services running include:
   - `api`: The FastAPI web server (port 8000).
   - `worker-validate`, `worker-transform`, `worker-convert`, `worker-compress`, `worker-extract`: Independent FastStream worker services scaling specific pipeline steps.
   - `worker-notify`, `notifier`: Webhook routing and external HTTP notification dispatchers.
   - `rabbitmq`: The message broker (port 5672, UI port 15672).
   - `mysql`: The database (port 3306).

*Tip: You can scale specific workers horizontally depending on load:*
```bash
docker compose up --scale worker-extract=3 -d
```

## How to Run: Local CLI Mode

If you prefer to run the Python services natively on your machine (e.g. for development or debugging), you can run the infrastructure via Docker and the apps via CLI.

1. Ensure you have [uv](https://docs.astral.sh/uv/) installed.
2. Spin up the backend dependencies (MySQL and RabbitMQ) in the background:
   ```bash
   docker compose up mysql rabbitmq -d
   ```
3. In a new terminal, install dependencies and start the **API**:
   ```bash
   uv sync
   uv run uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload
   ```
4. In another terminal, start the **Workers** (You can run all steps in a single monolithic worker locally by leaving `ACTIVE_WORKER_STEP` unset):
   ```bash
   uv run faststream run src.worker.main:app
   ```
5. In another terminal, start the **Notifier**:
   ```bash
   uv run faststream run src.notifier.main:app
   ```

## 2. How to Run Tests

The test suite relies heavily on mocked integration boundaries to verify event propagation, stream processing, and HTTP interfaces.

To run the tests (ensure `uv` is installed):
```bash
uv run pytest tests
```

To run a specific test suite (e.g. large file chunking logic):
```bash
uv run pytest tests/test_large_files.py
```

## 3. Example API Calls

**1. Upload a File and Kick off a Pipeline**

This example uses standard `curl` to upload a `customers.csv` file and pass a dynamic JSON array of pipeline steps.

```bash
curl -X POST "http://localhost:8000/jobs/upload" \
  -H "accept: application/json" \
  -H "Content-Type: multipart/form-data" \
  -F "file=@customers.csv;type=text/csv" \
  -F 'pipeline={"pipeline": [{"step": "validate"}, {"step": "transform", "params": {"filter_col": "status", "filter_val": "active"}}, {"step": "convert", "params": {"output_format": "json"}}, {"step": "compress", "params": {"format": "gzip"}}]}'
```
*(Response returns a `job_id`)*

**2. Check Job Status**

Use the returned `job_id` to poll the pipeline status.
```bash
curl -X GET "http://localhost:8000/jobs/<job_id>" -H "accept: application/json"
```

**3. Resume a Failed Job**

If a step fails (e.g. due to a network timeout), you can resume it from the point of failure. All skipped downstream tasks will safely be requeued.
```bash
curl -X POST "http://localhost:8000/jobs/<job_id>/resume" -H "accept: application/json"
```

## 4. Implemented Processing Steps

The following atomic operations are currently supported by the choreographic pipeline engine. 

1. **`validate`**: Efficiently streams over structural tabular data (CSV/JSON/YAML) to verify schema integrity and formatting without loading the file into memory.
2. **`transform`**: Applies row-level mutations, filtering (e.g. `filter_col`, `filter_val`), or column mapping in-flight.
3. **`convert`**: Facilitates streaming zero-dependency translation between formats (e.g. `CSV -> JSON`).
4. **`extract`**: Powers the Map-Reduce engine. Expands compressed archives (e.g. `.zip`), spawns parallel asynchronous `sub-jobs` for every contained file, and merges the state back to the parent job via `pending_dependencies`.
5. **`compress`**: Bundles artifacts into `gzip` or `zip` outputs, capable of acting as a reduction/repack stage following an extraction fan-out.
6. **`notify`**: Dispatches terminal webhooks using `tenacity` exponential-backoff retries.

## Stopping the Project

To stop the containers and keep the data:
```bash
docker compose stop
```

To shut down and remove the volumes (will delete database data and local files):
```bash
docker compose down -v
```
**4. Cancel a Job**

If a job is pending or running, you can cancel it and halt all its subjobs and downstream tasks.
```bash
curl -X POST "http://localhost:8000/jobs/<job_id>/cancel" -H "accept: application/json"
```
