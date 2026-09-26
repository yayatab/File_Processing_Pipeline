import os
import json
from faststream import FastStream
from faststream.rabbit import RabbitBroker
from sqlmodel import Session
from src.database import engine
from src.models import Job, JobStep

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
broker = RabbitBroker(RABBITMQ_URL)
app = FastStream(broker)

@broker.subscriber("pipeline_jobs")
async def process_job(msg: dict):
    job_id = msg.get("job_id")
    if not job_id:
        return
        
    with Session(engine) as session:
        job = session.get(Job, job_id)
        if not job:
            return
            
        job.status = "RUNNING"
        session.commit()
        
        # Simple step execution engine
        for step in job.steps:
            if step.status == "COMPLETED":
                continue
                
            step.status = "RUNNING"
            job.current_step_index = step.step_index
            session.commit()
            
            try:
                if step.step_type == "notify":
                    # Send to notifier queue
                    await broker.publish({"job_id": job.id, "params": json.loads(step.params)}, queue="webhooks")
                else:
                    # Mock execution for other steps
                    pass
                
                step.status = "COMPLETED"
            except Exception as e:
                step.status = "FAILED"
                step.error_message = str(e)
                job.status = "FAILED"
                job.error_message = f"Failed at step {step.step_type}: {e}"
                session.commit()
                return
                
            session.commit()
            
        job.status = "COMPLETED"
        session.commit()
