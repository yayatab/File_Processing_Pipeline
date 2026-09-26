# Architecture Decisions

## 1. Message Broker & Worker Framework: FastStream
* **Decision**: Use FastStream over Celery or raw aio-pika.
* **Reasoning**: FastStream provides a modern, fully asynchronous, FastAPI-like developer experience. It relies on Pydantic and integrates seamlessly into our async Python 3.12 ecosystem. It abstracts away the boilerplate of raw `aio-pika` while remaining lighter and more modern than Celery.

## 2. Background Tasks & Scheduling: APScheduler
* **Decision**: Use APScheduler to manage the 24-hour cleanup cron job.
* **Reasoning**: Since FastStream is focused on message streaming and doesn't have a built-in cron like Celery Beat, APScheduler is a standard, lightweight library to trigger periodic cleanup tasks within the FastAPI application.

## 3. Web Framework: FastAPI
* **Decision**: Use FastAPI.
* **Reasoning**: Industry standard for high-performance, async Python web APIs.

## 4. ORM & Database: SQLModel + MySQL
* **Decision**: Use SQLModel backed by MySQL.
* **Reasoning**: SQLModel reduces code duplication by combining Pydantic models (for API schemas) and SQLAlchemy models (for database tables) into a single definition.

## 5. Python Version: 3.12
* **Decision**: Use Python 3.12 (Stable) instead of 3.14.
* **Reasoning**: To ensure all third-party libraries (especially those with C-extensions like database drivers) have pre-built stable wheels and avoid bleeding-edge dependency issues.