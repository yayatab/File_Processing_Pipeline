from fastapi import FastAPI
from contextlib import asynccontextmanager
from src.database import init_db
from src.api.routers import jobs
import os

def cleanup_expired_files():
    from src.database import engine
    from src.models import FileReference
    from sqlmodel import Session, select
    from datetime import datetime, timezone
    
    with Session(engine) as session:
        expired = session.exec(select(FileReference).where(FileReference.expires_at < datetime.now(timezone.utc))).all()
        for f in expired:
            if os.path.exists(f.storage_path):
                os.remove(f.storage_path)
            session.delete(f)
        session.commit()

import logfire

logfire.configure(send_to_logfire='if-token-present')

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    from src.database import engine
    logfire.instrument_sqlalchemy(engine=engine)
    from apscheduler.schedulers.background import BackgroundScheduler
    scheduler = BackgroundScheduler()
    scheduler.add_job(cleanup_expired_files, 'interval', hours=1)
    scheduler.start()
    yield
    scheduler.shutdown()

app = FastAPI(lifespan=lifespan, title="File Processing Pipeline")
logfire.instrument_fastapi(app)

app.include_router(jobs.router, prefix="/jobs")

@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
