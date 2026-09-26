# File Processing Pipeline

An asynchronous file processing pipeline that accepts file uploads, processes them through configurable steps (validate, transform, convert, compress, notify), and stores the results.

## Tech Stack
- **Python 3.12**
- **FastAPI** (API Layer)
- **FastStream** (RabbitMQ Worker Framework)
- **SQLModel & MySQL** (Data Layer)
- **Docker & Docker Compose** (Containerization)

## How to Run

1. Ensure you have Docker and Docker Compose installed.
2. Clone the repository and navigate to the root directory.
3. Start the infrastructure and application services:
   ```bash
   docker-compose up -d --build
   ```
4. Access the API documentation (Swagger UI) at:
   [http://localhost:8000/docs](http://localhost:8000/docs)
5. The services running include:
   - `api`: The FastAPI web server.
   - `worker`: The FastStream pipeline worker.
   - `notifier`: The FastStream webhook notification worker.
   - `rabbitmq`: The message broker.
   - `mysql`: The database.

## Stopping the Project

To stop the containers and keep the data:
```bash
docker-compose stop
```

To shut down and remove the volumes (will delete database data and local files):
```bash
docker-compose down -v
```