# File Processing Pipeline

An asynchronous file processing pipeline that accepts file uploads, processes them through configurable steps (validate, transform, convert, compress, notify), and stores the results.

## Tech Stack
- **Python 3.12**
- **FastAPI** (API Layer)
- **FastStream** (RabbitMQ Worker Framework)
- **SQLModel & MySQL** (Data Layer)
- **Docker & Docker Compose** (Containerization)

## How to Run: Docker Mode (Recommended)

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

## Stopping the Project

To stop the containers and keep the data:
```bash
docker compose stop
```

To shut down and remove the volumes (will delete database data and local files):
```bash
docker compose down -v
```