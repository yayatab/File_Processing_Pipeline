from sqlmodel import SQLModel, Field, Relationship
from typing import Optional, List
from datetime import datetime, timezone
from sqlalchemy import Column, String

class Job(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    status: str = Field(default="PENDING")
    current_step_index: int = Field(default=0)
    pipeline_definition: str = Field(sa_column=Column(String(2000)))
    error_message: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    steps: List["JobStep"] = Relationship(back_populates="job")
    files: List["FileReference"] = Relationship(back_populates="job")

class JobStep(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    job_id: int = Field(foreign_key="job.id")
    step_index: int
    step_type: str
    params: str = Field(sa_column=Column(String(1000)))
    status: str = Field(default="PENDING")
    error_message: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    job: Job = Relationship(back_populates="steps")

class FileReference(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    job_id: int = Field(foreign_key="job.id")
    storage_path: str
    original_filename: str
    size: int
    content_type: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    job: Job = Relationship(back_populates="files")
