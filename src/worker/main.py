import os
import json
from faststream import FastStream
from faststream.rabbit import RabbitBroker
from sqlmodel import Session
from datetime import datetime, timezone
from src.database import engine
from src.models import Job, JobStep, JobStatus

import logfire

logfire.configure(send_to_logfire='if-token-present')

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
broker = RabbitBroker(RABBITMQ_URL)
app = FastStream(broker)


async def apply_extraction(job: Job, session: Session, step: JobStep) -> None:
    from src.worker.steps import extract_zip
    from src.models import FileReference
    if not job.files: raise ValueError("No file")
    f = job.files[-1]
    extract_dir = f"{f.storage_path}_extracted"
    os.makedirs(extract_dir, exist_ok=True)
    extracted_files = extract_zip(f.storage_path, extract_dir)

    remaining_steps = [s for s in job.steps if s.step_index > step.step_index]
    repack_idx = -1
    for i, s in enumerate(remaining_steps):
        if s.step_type == "compress":
            p = json.loads(s.params)
            if p.get("format") == "zip":
                repack_idx = i
                break

    if repack_idx == -1:
        repack_idx = len(remaining_steps)

    subjob_steps_defs = remaining_steps[:repack_idx]
    repack_steps_defs = remaining_steps[repack_idx:]

    repack_job = Job(parent_job_id=str(job.id), pending_dependencies=len(extracted_files),
                     pipeline_definition=job.pipeline_definition)
    session.add(repack_job)
    session.commit()

    for s_def in repack_steps_defs:
        session.add(
            JobStep(job_id=repack_job.id, step_index=s_def.step_index, step_type=s_def.step_type, params=s_def.params))

    for e_file in extracted_files:
        subjob = Job(parent_job_id=str(repack_job.id), pipeline_definition=job.pipeline_definition)
        session.add(subjob)
        session.commit()

        session.add(FileReference(job_id=subjob.id, storage_path=e_file, original_filename=os.path.basename(e_file),
                                  size_in_mb=os.path.getsize(e_file) / (1024 * 1024), content_type="text/plain"))

        for s_def in subjob_steps_defs:
            session.add(
                JobStep(job_id=subjob.id, step_index=s_def.step_index, step_type=s_def.step_type, params=s_def.params))

        session.commit()
        if subjob.steps:
            first = min(subjob.steps, key=lambda x: x.step_index)
            await broker.publish({"job_id": str(subjob.id), "step_id": str(first.id)}, queue=f"step_{first.step_type}")

    for s in remaining_steps:
        s.status = JobStatus.SKIPPED
    session.commit()


async def apply_compression(job: Job, step: JobStep) -> None:
    from src.worker.steps import compress_gzip
    import zipfile
    if not job.files: raise ValueError("No file")
    params = json.loads(step.params)
    fmt = params.get("format", "gzip")

    if fmt == "gzip":
        f = job.files[-1]
        out_path = f"{f.storage_path}.gz"
        compress_gzip(f.storage_path, out_path)
        f.storage_path = out_path
    elif fmt == "zip":
        out_path = f"storage/{job.id}_repacked.zip"
        with zipfile.ZipFile(out_path, 'w') as zf:
            for f in job.files:
                zf.write(f.storage_path, os.path.basename(f.storage_path))
        from src.models import FileReference
        job.files = [FileReference(job_id=job.id, storage_path=out_path, original_filename="repacked.zip",
                                   size_in_mb=os.path.getsize(out_path) / (1024 * 1024),
                                   content_type="application/zip")]
    else:
        raise ValueError(f"Unsupported compression format: {fmt}")


async def apply_transform(job: Job, step: JobStep) -> None:
    from src.worker.steps import transform_file
    if not job.files: raise ValueError("No file")
    f = job.files[-1]
    ext = f.original_filename.split('.')[-1].lower()
    out_path = f"{f.storage_path}_transformed.{ext}"
    transform_file(f.storage_path, out_path, ext, json.loads(step.params))
    f.storage_path = out_path


async def apply_validate(job: Job) -> None:
    from src.worker.steps import validate_file
    if not job.files: raise ValueError("No file")
    f = job.files[-1]
    ext = f.original_filename.split('.')[-1].lower()
    validate_file(f.storage_path, ext)


async def apply_convertion(job: Job, step: JobStep) -> None:
    if not job.files:
        raise ValueError("No input file found for job")
    input_file = job.files[-1]

    params = json.loads(step.params)
    to_ext = params.get("output_format", "json").lower()
    from_ext = input_file.original_filename.split('.')[-1].lower()

    out_path = f"{input_file.storage_path}.{to_ext}"

    from src.worker.steps import convert_file
    convert_file(input_file.storage_path, out_path, from_ext, to_ext)
    
    from src.models import FileReference
    input_file.session.add(FileReference(job_id=job.id, storage_path=out_path, original_filename=f"{input_file.original_filename}.{to_ext}", size_in_mb=os.path.getsize(out_path)/(1024*1024), content_type="application/json"))

async def execute_step(msg: dict, step_executor_func) -> None:
    job_id = msg.get("job_id")
    step_id = msg.get("step_id")
    if not job_id or not step_id: return
    
    with Session(engine) as session:
        job = session.get(Job, job_id)
        step = session.get(JobStep, step_id)
        if not job or not step or step.status == JobStatus.COMPLETED:
            return

        if job.status == JobStatus.FAILED:
            step.status = JobStatus.SKIPPED
            session.commit()
            return

        if job.status == JobStatus.CANCELLED:
            step.status = JobStatus.CANCELLED
            session.commit()
            return
            
        job.status = JobStatus.RUNNING
        step.status = JobStatus.RUNNING
        if not step.started_at:
            step.started_at = datetime.now(timezone.utc)
        job.current_step_index = step.step_index
        session.commit()
        
        with logfire.span("execute_step {step_type}", step_type=step.step_type, job_id=job_id, step_id=step_id, retry_count=step.retry_count) as span:
            try:
                await step_executor_func(job, session, step)
                step.status = JobStatus.COMPLETED
                step.completed_at = datetime.now(timezone.utc)
                step.duration_seconds = (step.completed_at - step.started_at).total_seconds()
                session.commit()
                span.set_attribute("status", "COMPLETED")
            except Exception as e:
                span.record_exception(e)
                params = {}
                try:
                    params = json.loads(step.params)
                except Exception:
                    pass
                max_retries = int(params.get("max_retries", os.getenv("MAX_RETRIES", "3")))
                
                if step.retry_count < max_retries:
                    step.retry_count += 1
                    step.status = JobStatus.PENDING
                    session.commit()
                    logfire.warn("Step failed, requeuing", job_id=job_id, step_id=step_id, retry_count=step.retry_count, max_retries=max_retries, error=str(e))
                    await broker.publish(msg, queue=f"step_{step.step_type}")
                    span.set_attribute("status", "REQUEUED")
                    return
                else:
                    step.status = JobStatus.FAILED
                    step.error_message = str(e)
                    step.completed_at = datetime.now(timezone.utc)
                    step.duration_seconds = (step.completed_at - step.started_at).total_seconds()
                    
                    job.status = JobStatus.FAILED
                    job.error_message = f"Failed at step {step.step_type}: {e}"
                    
                    for s in job.steps:
                        if s.status == JobStatus.PENDING:
                            s.status = JobStatus.SKIPPED
                    session.commit()
                    logfire.error("Step failed permanently", job_id=job_id, step_id=step_id, error=str(e))
                    span.set_attribute("status", "FAILED")
                    return

        pending_steps = [s for s in job.steps if s.step_index > step.step_index and s.status == JobStatus.PENDING]
        if pending_steps:
            next_step = min(pending_steps, key=lambda s: s.step_index)
            await broker.publish({"job_id": str(job.id), "step_id": str(next_step.id)}, queue=f"step_{next_step.step_type}")
        else:
            job.status = JobStatus.COMPLETED
            job.completed_at = datetime.now(timezone.utc)
            session.commit()
            
            if job.parent_job_id:
                parent = session.get(Job, job.parent_job_id)
                if parent and parent.pending_dependencies > 0:
                    parent.pending_dependencies -= 1
                    session.commit()
                    if parent.pending_dependencies == 0:
                        from sqlalchemy import select
                        siblings = session.exec(select(Job).where(Job.parent_job_id == str(parent.id))).all() # type: ignore
                        for sib in siblings:
                            for sf in sib.files:
                                sf.job_id = parent.id
                                session.add(sf)
                        session.commit()
                        
                        if parent.steps:
                            parent_next = min([s for s in parent.steps if s.status == JobStatus.PENDING], key=lambda s: s.step_index, default=None)
                            if parent_next:
                                await broker.publish({"job_id": str(parent.id), "step_id": str(parent_next.id)}, queue=f"step_{parent_next.step_type}")


ACTIVE_STEP = os.getenv("ACTIVE_WORKER_STEP", "all")

if ACTIVE_STEP in ("all", "validate"):
    @broker.subscriber("step_validate")
    async def handle_validate(msg: dict):
        async def executor(j, s, st):
            await apply_validate(j)
        await execute_step(msg, executor)

if ACTIVE_STEP in ("all", "transform"):
    @broker.subscriber("step_transform")
    async def handle_transform(msg: dict):
        async def executor(j, s, st):
            await apply_transform(j, st)
        await execute_step(msg, executor)

if ACTIVE_STEP in ("all", "convert"):
    @broker.subscriber("step_convert")
    async def handle_convert(msg: dict):
        async def executor(j, s, st):
            await apply_convertion(j, st)
        await execute_step(msg, executor)

if ACTIVE_STEP in ("all", "compress"):
    @broker.subscriber("step_compress")
    async def handle_compress(msg: dict):
        async def executor(j, s, st):
            await apply_compression(j, st)
        await execute_step(msg, executor)

if ACTIVE_STEP in ("all", "extract"):
    @broker.subscriber("step_extract")
    async def handle_extract(msg: dict):
        await execute_step(msg, apply_extraction)

if ACTIVE_STEP in ("all", "notify"):
    @broker.subscriber("step_notify")
    async def handle_notify(msg: dict):
        async def executor(j, s, st):
            await broker.publish({"job_id": str(j.id), "params": json.loads(st.params)}, queue="webhooks")
        await execute_step(msg, executor)
