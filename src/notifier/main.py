import os
import httpx
from faststream import FastStream
from faststream.rabbit import RabbitBroker

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
broker = RabbitBroker(RABBITMQ_URL)
app = FastStream(broker)


@broker.subscriber("webhooks")
async def send_webhook(msg: dict) -> None:
    params = msg.get("params", {})
    webhook_url = params.get("webhook_url")
    job_id = msg.get("job_id")

    if not webhook_url:
        return

    async with httpx.AsyncClient() as client:
        try:
            await client.post(webhook_url, json={"job_id": job_id, "status": "COMPLETED"})
        except httpx.RequestError as e:
            # Re-raise to trigger nack/retry in faststream
            raise e
