import json
import os
import shutil
from fastapi import APIRouter, Depends, UploadFile, File, Form, HTTPException
from sqlmodel import Session
from src.database import get_session
from src.models import Job, JobStep, FileReference
from faststream.rabbit import RabbitBroker
import asyncio

router = APIRouter()
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
broker = RabbitBroker(RABBITMQ_URL)

@router.post("/upload")
async def upload_file(
    pipeline: str = Form(...),
    file: UploadFile = File(...),
    session: Session = Depends(get_session)
):
    try:
        pipeline_data = json.loads(pipeline)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid pipeline JSON")

    job = Job(pipeline_definition=pipeline)
    session.add(job)
    session.commit()
    session.refresh(job)

    os.makedirs("storage", exist_ok=True)
    file_path = f"storage/{job.id}_{file.filename}"
    
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    file_ref = FileReference(
        job_id=job.id,
        storage_path=file_path,
        original_filename=file.filename,
        size_in_mb=os.path.getsize(file_path) / (1024 * 1024),
        content_type=file.content_type
    )
    session.add(file_ref)
    
    for idx, step in enumerate(pipeline_data.get("pipeline", [])):
        job_step = JobStep(
            job_id=job.id,
            step_index=idx,
            step_type=step["step"],
            params=json.dumps(step.get("params", {}))
        )
        session.add(job_step)
    
    session.commit()
    
    await broker.connect()
    await broker.publish({"job_id": job.id}, queue="pipeline_jobs")
    await broker.close()

    return {"job_id": job.id}

@router.get("/{job_id}")
def get_job(job_id: int, session: Session = Depends(get_session)):
    job = session.get(Job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return {
        "id": job.id,
        "status": job.status,
        "current_step": job.current_step_index,
        "steps": [
            {
                "index": s.step_index,
                "type": s.step_type,
                "status": s.status
            } for s in job.steps
        ]
    }
