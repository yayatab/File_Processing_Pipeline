import json
import os
import shutil
from fastapi import APIRouter, Depends, UploadFile, File, Form, HTTPException
from sqlmodel import Session
from src.database import get_session
from src.models import Job, JobStep, FileReference
from faststream.rabbit import RabbitBroker
from fastapi.responses import FileResponse

router = APIRouter()
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
broker = RabbitBroker(RABBITMQ_URL)


@router.post("/upload")
async def upload_file(
        pipeline: str = Form(...),
        file: UploadFile = File(...),
        session: Session = Depends(get_session)
) -> dict:
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
    await broker.publish({"job_id": str(job.id)}, queue="pipeline_jobs")

    return {"job_id": job.id}


@router.get("/{job_id}")
def get_job(job_id: str, session: Session = Depends(get_session)) -> dict:
    job: Job | None = session.get(Job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return job.model_dump(mode="json")


@router.get("/files/{file_id}")
def download_file(file_id: str, session: Session = Depends(get_session)) -> FileResponse:
    file_ref = session.get(FileReference, file_id)
    if not file_ref or not os.path.exists(file_ref.storage_path):
        raise HTTPException(status_code=404, detail="File not found")

    return FileResponse(
        path=file_ref.storage_path,
        filename=file_ref.original_filename,
        media_type=file_ref.content_type
    )
