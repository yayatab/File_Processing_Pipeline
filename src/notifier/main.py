import os
import httpx
import logfire
from faststream import FastStream
from faststream.rabbit import RabbitBroker

logfire.configure(send_to_logfire='if-token-present')
logfire.instrument_httpx()

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
broker = RabbitBroker(RABBITMQ_URL)
app = FastStream(broker)


from tenacity import retry, stop_after_attempt, wait_exponential

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30))
async def post_webhook(url: str, payload: dict) -> None:
    async with httpx.AsyncClient() as client:
        response = await client.post(url, json=payload, timeout=10.0)
        response.raise_for_status()

@broker.subscriber("webhooks")
async def send_webhook(msg: dict) -> None:
    params = msg.get("params", {})
    webhook_url = params.get("webhook_url")
    job_id = msg.get("job_id")

    if not webhook_url:
        return

    try:
        await post_webhook(webhook_url, {"job_id": job_id, "status": "COMPLETED"})
    except Exception as e:
        # Tenacity will retry up to 5 times. 
        # If it still fails, we let faststream nack it (and rabbitmq will hold/drop it).
        raise e
