from sqlmodel import SQLModel, Field, Relationship
from typing import TYPE_CHECKING
from datetime import datetime, timezone
import uuid

if TYPE_CHECKING:
    from .job import Job


class FileReference(SQLModel, table=True):
    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    job_id: uuid.UUID = Field(foreign_key="job.id")
    storage_path: str
    original_filename: str
    size_in_mb: float
    content_type: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    job: "Job" = Relationship(back_populates="files")
