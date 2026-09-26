from fastapi import FastAPI
from contextlib import asynccontextmanager
from src.database import init_db
from src.api.routers import jobs

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield

app = FastAPI(lifespan=lifespan, title="File Processing Pipeline")

app.include_router(jobs.router, prefix="/jobs")

@app.get("/health")
def health():
    return {"status": "ok"}
