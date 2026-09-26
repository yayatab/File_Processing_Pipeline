# System Overview

The File Processing Pipeline is an asynchronous, event-driven system orchestrated by **Docker Compose**. It utilizes **FastAPI** for the frontend API, **RabbitMQ** as the message broker, **FastStream** for the queue consumers, and **MySQL** for state management.

```mermaid
graph TD
    User([User]) -->|1. Upload File & Pipeline JSON| API[FastAPI Server]
    User -->|Check Job Status| API

    subgraph Infrastructure
        API -->|2. Save File| Storage[(Local /app/storage)]
        API -->|3. Insert Job & Steps| DB[(MySQL)]
        API -->|4. Publish Job ID| RMQ_Pipeline[RabbitMQ: pipeline_jobs queue]
        
        RMQ_Pipeline -->|5. Consume Job ID| Worker[FastStream Worker]
        Worker <-->|6. Fetch & Update Status| DB
        Worker <-->|7. Process Files| Storage
        
        Worker -->|8. Publish Webhook Info| RMQ_Webhooks[RabbitMQ: webhooks queue]
        RMQ_Webhooks -->|9. Consume Webhook| Notifier[FastStream Notifier]
    end

    Notifier -->|10. Send HTTP POST| WebhookTarget([External Webhook URL])
    Notifier -.->|11. Auto Retry on Failure| RMQ_Webhooks
```

### Component Breakdown
1. **API Server (`api`)**: Handles file streaming to disk, saves initial DB records, and publishes the start signal to RabbitMQ. Provides endpoints for status polling.
2. **Worker (`worker`)**: The heavy lifter. Consumes the job ID, fetches the steps from MySQL, executes the pipeline sequence (validate, transform, etc.), and updates the status.
3. **Notifier (`notifier`)**: An isolated worker dedicated solely to external HTTP calls. If an external webhook is down, the Notifier handles the failure and retries without blocking the main data pipeline.
4. **RabbitMQ**: Decouples the fast API layer from the slow processing layer.
5. **MySQL**: Tracks the exact state (`PENDING`, `RUNNING`, `COMPLETED`, `FAILED`) of every job and sub-step.
