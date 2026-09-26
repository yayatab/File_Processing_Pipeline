

class FileReference(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    job_id: int = Field(foreign_key="job.id")
    storage_path: str
    original_filename: str
    size: int
    content_type: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    job: Job = Relationship(back_populates="files")
