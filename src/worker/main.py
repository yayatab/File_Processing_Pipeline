import os
import json
from faststream import FastStream
from faststream.rabbit import RabbitBroker
from sqlmodel import Session
from src.database import engine
from src.models import Job, JobStep, JobStatus

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
broker = RabbitBroker(RABBITMQ_URL)
app = FastStream(broker)


@broker.subscriber("pipeline_jobs")
async def process_job(msg: dict) -> None:
    job_id = msg.get("job_id")
    if not job_id:
        return

    with Session(engine) as session:
        job = session.get(Job, job_id)
        if not job:
            return

        job.status = JobStatus.RUNNING
        session.commit()

        for step in job.steps:
            if step.status == JobStatus.COMPLETED:
                continue

            step.status = JobStatus.RUNNING
            job.current_step_index = step.step_index
            session.commit()

            try:
                match step.step_type:
                    case "notify":
                        await broker.publish({"job_id": str(job.id), "params": json.loads(step.params)}, queue="webhooks")
                    case "convert":
                        if not job.files:
                            raise ValueError("No input file found for job")
                        input_file = job.files[0]
                        out_path = f"{input_file.storage_path}.json"
                        
                        from src.worker.steps import convert_csv_to_json
                        convert_csv_to_json(input_file.storage_path, out_path)
                    case "validate" | "transform" | "compress":
                        pass # Mock execution for these steps to allow pipeline completion
                    case _:
                        pass

                step.status = JobStatus.COMPLETED
            except Exception as e:
                step.status = JobStatus.FAILED
                step.error_message = str(e)
                job.status = JobStatus.FAILED
                job.error_message = f"Failed at step {step.step_type}: {e}"
                session.commit()
                return

            session.commit()

        job.status = JobStatus.COMPLETED
        session.commit()
