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
                        
                        params = json.loads(step.params)
                        to_ext = params.get("output_format", "json").lower()
                        from_ext = input_file.original_filename.split('.')[-1].lower()
                        
                        out_path = f"{input_file.storage_path}.{to_ext}"
                        
                        from src.worker.steps import convert_file
                        convert_file(input_file.storage_path, out_path, from_ext, to_ext)
                        
                    case "validate":
                        from src.worker.steps import validate_file
                        if not job.files: raise ValueError("No file")
                        f = job.files[0]
                        ext = f.original_filename.split('.')[-1].lower()
                        validate_file(f.storage_path, ext)
                        
                    case "transform":
                        from src.worker.steps import transform_file
                        if not job.files: raise ValueError("No file")
                        f = job.files[0]
                        ext = f.original_filename.split('.')[-1].lower()
                        out_path = f"{f.storage_path}_transformed.{ext}"
                        transform_file(f.storage_path, out_path, ext, json.loads(step.params))
                        f.storage_path = out_path
                        
                    case "compress":
                        from src.worker.steps import compress_gzip
                        import zipfile
                        if not job.files: raise ValueError("No file")
                        params = json.loads(step.params)
                        fmt = params.get("format", "gzip")
                        
                        if fmt == "gzip":
                            f = job.files[0]
                            out_path = f"{f.storage_path}.gz"
                            compress_gzip(f.storage_path, out_path)
                            f.storage_path = out_path
                        elif fmt == "zip":
                            out_path = f"storage/{job.id}_repacked.zip"
                            with zipfile.ZipFile(out_path, 'w') as zf:
                                for f in job.files:
                                    zf.write(f.storage_path, os.path.basename(f.storage_path))
                            from src.models import FileReference
                            job.files = [FileReference(job_id=job.id, storage_path=out_path, original_filename="repacked.zip", size_in_mb=os.path.getsize(out_path)/(1024*1024), content_type="application/zip")]
                            
                    case "extract":
                        from src.worker.steps import extract_zip
                        from src.models import FileReference
                        if not job.files: raise ValueError("No file")
                        f = job.files[0]
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
                        
                        repack_job = Job(parent_job_id=str(job.id), pending_dependencies=len(extracted_files), pipeline_definition=job.pipeline_definition)
                        session.add(repack_job)
                        session.commit()
                        
                        for s_def in repack_steps_defs:
                            session.add(JobStep(job_id=repack_job.id, step_index=s_def.step_index, step_type=s_def.step_type, params=s_def.params))
                        
                        for e_file in extracted_files:
                            subjob = Job(parent_job_id=str(repack_job.id), pipeline_definition=job.pipeline_definition)
                            session.add(subjob)
                            session.commit()
                            
                            session.add(FileReference(job_id=subjob.id, storage_path=e_file, original_filename=os.path.basename(e_file), size_in_mb=os.path.getsize(e_file)/(1024*1024), content_type="text/plain"))
                            
                            for s_def in subjob_steps_defs:
                                session.add(JobStep(job_id=subjob.id, step_index=s_def.step_index, step_type=s_def.step_type, params=s_def.params))
                            
                            session.commit()
                            await broker.publish({"job_id": str(subjob.id)}, queue="pipeline_jobs")
                        
                        step.status = JobStatus.COMPLETED
                        job.status = JobStatus.COMPLETED
                        session.commit()
                        return
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
                    await broker.publish({"job_id": str(parent.id)}, queue="pipeline_jobs")
