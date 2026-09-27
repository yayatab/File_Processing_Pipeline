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

    from datetime import datetime, timezone, timedelta
    file_ref = FileReference(
        job_id=job.id,
        storage_path=file_path,
        original_filename=file.filename or "unknown",
        size_in_mb=os.path.getsize(file_path) / (1024 * 1024),
        content_type=file.content_type or "application/octet-stream",
        expires_at=datetime.now(timezone.utc) + timedelta(hours=24)
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

    if job.steps:
        first_step = min(job.steps, key=lambda s: s.step_index)
        queue_name = f"step_{first_step.step_type}"
        step_id = str(first_step.id)
    else:
        queue_name = "pipeline_jobs"
        step_id = ""

    await broker.connect()
    await broker.publish({"job_id": str(job.id), "step_id": step_id}, queue=queue_name)

    return {"job_id": job.id}


@router.get("/{job_id}")
def get_job(job_id: str, session: Session = Depends(get_session)) -> dict:
    job: Job | None = session.get(Job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return job.model_dump(mode="json")

@router.get("/")
def get_all_jobs(session: Session = Depends(get_session)) -> list[dict]:
    from sqlmodel import select
    jobs = session.exec(select(Job)).all()
    return [job.model_dump(mode="json") for job in jobs]


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

async def _resume_job_recursive(job_id: str, session: Session):
    from src.models import JobStatus
    from sqlmodel import select
    
    job: Job | None = session.get(Job, job_id)
    if not job:
        return
        
    children = session.exec(select(Job).where(Job.parent_job_id == str(job.id))).all() # type: ignore
    for child in children:
        await _resume_job_recursive(str(child.id), session)
        
    if job.status == JobStatus.FAILED:
        job.status = JobStatus.PENDING
        job.error_message = None
        
        target_step = None
        for step in sorted(job.steps, key=lambda s: s.step_index):
            if step.status == JobStatus.FAILED:
                step.status = JobStatus.PENDING
                step.retry_count = 0
                step.error_message = None
                target_step = step
            elif step.status == JobStatus.SKIPPED:
                step.status = JobStatus.PENDING
                
        session.commit()
        
        if target_step:
            await broker.publish({"job_id": str(job.id), "step_id": str(target_step.id)}, queue=f"step_{target_step.step_type}")

@router.post("/{job_id}/resume")
async def resume_job(job_id: str, session: Session = Depends(get_session)) -> dict:
    job: Job | None = session.get(Job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
        
    await broker.connect()
    await _resume_job_recursive(job_id, session)
    
    return {"message": f"Resume signal processed for job {job_id} and its subjobs"}
