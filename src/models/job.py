from sqlmodel import SQLModel, Field, Relationship
from typing import Optional, List, TYPE_CHECKING
from datetime import datetime, timezone
from sqlalchemy import Column, String
import uuid
import enum

if TYPE_CHECKING:
    from .file_reference import FileReference


class JobStatus(str, enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class Job(SQLModel, table=True):
    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    status: JobStatus = Field(default=JobStatus.PENDING)
    current_step_index: int = Field(default=0)
    pipeline_definition: str = Field(sa_column=Column(String(2000)))
    error_message: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    parent_job_id: Optional[str] = None
    pending_dependencies: int = Field(default=0)

    steps: List["JobStep"] = Relationship(back_populates="job")
    files: List["FileReference"] = Relationship(back_populates="job")


class JobStep(SQLModel, table=True):
    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    job_id: uuid.UUID = Field(foreign_key="job.id")
    step_index: int
    step_type: str
    params: str = Field(sa_column=Column(String(1000)))
    status: JobStatus = Field(default=JobStatus.PENDING)
    error_message: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    job: Job = Relationship(back_populates="steps")
