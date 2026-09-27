import pytest
from httpx import AsyncClient, ASGITransport
from src.api.main import app
from unittest.mock import patch, MagicMock
from src.models import Job, JobStatus, FileReference, JobStep
from src.database import get_session
import uuid

@pytest.fixture
def mock_session():
    mock = MagicMock()
    app.dependency_overrides[get_session] = lambda: mock
    yield mock
    app.dependency_overrides.clear()

@pytest.fixture
def test_client():
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")

@pytest.mark.asyncio
async def test_health(test_client):
    response = await test_client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

@pytest.mark.asyncio
@patch("src.api.routers.jobs.broker.publish")
@patch("src.api.routers.jobs.broker.connect")
async def test_file_upload_and_job_creation(mock_connect, mock_publish, test_client, mock_session, tmp_path):
    pipeline = {"pipeline": [{"step": "validate"}]}
    import json
    data = {"pipeline": json.dumps(pipeline)}
    files = {"file": ("test.csv", b"dummy content", "text/csv")}
    
    response = await test_client.post("/jobs/upload", data=data, files=files)
    
    assert response.status_code == 200
    assert "job_id" in response.json()
    assert mock_session.add.called
    assert mock_session.commit.called
    assert mock_publish.called

@pytest.mark.asyncio
async def test_job_status_tracking(test_client, mock_session):
    job_id = uuid.uuid4()
    mock_job = Job(id=job_id, status=JobStatus.COMPLETED, pipeline_definition="{}")
    mock_session.get.return_value = mock_job
    
    response = await test_client.get(f"/jobs/{str(job_id)}")
    assert response.status_code == 200
    assert response.json()["status"] == "COMPLETED"

@pytest.mark.asyncio
async def test_file_retrieval(test_client, mock_session, tmp_path):
    import os
    file_id = uuid.uuid4()
    temp_file = tmp_path / "test.csv"
    temp_file.write_text("dummy content")
    
    mock_file = FileReference(
        id=file_id, 
        storage_path=str(temp_file), 
        original_filename="test.csv", 
        size_in_mb=1.0, 
        content_type="text/csv", 
        job_id=uuid.uuid4()
    )
    mock_session.get.return_value = mock_file
    
    response = await test_client.get(f"/jobs/files/{str(file_id)}")
            
    assert response.status_code == 200

# Testing worker execution logic directly without HTTP
@pytest.mark.asyncio
@patch("src.worker.main.Session")
async def test_pipeline_execution_end_to_end(mock_session_cls):
    mock_session = MagicMock()
    mock_session_cls.return_value.__enter__.return_value = mock_session
    
    from src.worker.main import handle_validate
    from src.models import JobStep
    
    job_id = uuid.uuid4()
    step_id = uuid.uuid4()
    mock_job = Job(id=job_id, status=JobStatus.PENDING, pipeline_definition="{}")
    mock_step = JobStep(id=step_id, job_id=job_id, step_index=0, step_type="validate", params="{}")
    mock_job.steps = [mock_step]
    
    mock_file = FileReference(job_id=job_id, storage_path="dummy.csv", original_filename="dummy.csv", size_in_mb=1.0, content_type="text/csv")
    mock_job.files = [mock_file]
    
    def side_effect(model, ident):
        if model == Job: return mock_job
        if model == JobStep: return mock_step
        return None
        
    mock_session.get.side_effect = side_effect
    
    with patch("src.worker.steps.validate_file"):
        await handle_validate({"job_id": str(job_id), "step_id": str(step_id)})
    
    assert mock_job.status == JobStatus.COMPLETED
    assert mock_step.status == JobStatus.COMPLETED

@pytest.mark.asyncio
@patch("src.worker.main.Session")
async def test_step_failure_handling(mock_session_cls):
    mock_session = MagicMock()
    mock_session_cls.return_value.__enter__.return_value = mock_session
    
    from src.worker.main import handle_notify
    from src.models import JobStep
    
    job_id = uuid.uuid4()
    step_id = uuid.uuid4()
    mock_job = Job(id=job_id, status=JobStatus.PENDING, pipeline_definition="{}")
    mock_step = JobStep(id=step_id, job_id=job_id, step_index=0, step_type="notify", params="{}")
    mock_step.retry_count = 3 # Force fail instead of requeue
    mock_job.steps = [mock_step]
    
    def side_effect(model, ident):
        if model == Job: return mock_job
        if model == JobStep: return mock_step
        return None
        
    mock_session.get.side_effect = side_effect
    
    with patch("src.worker.main.json.loads", side_effect=Exception("forced error")):
        await handle_notify({"job_id": str(job_id), "step_id": str(step_id)})
        
    assert mock_job.status == JobStatus.FAILED
    assert mock_step.status == JobStatus.FAILED
    assert mock_step.error_message and "forced error" in mock_step.error_message

@pytest.mark.asyncio
@patch("src.api.routers.jobs.broker.publish")
@patch("src.api.routers.jobs.broker.connect")
async def test_job_resume(mock_connect, mock_publish, test_client, mock_session):
    job_id = uuid.uuid4()
    step_id = uuid.uuid4()
    mock_job = Job(id=job_id, status=JobStatus.FAILED, pipeline_definition="{}")
    mock_step_1 = JobStep(id=uuid.uuid4(), job_id=job_id, step_index=0, step_type="validate", status=JobStatus.COMPLETED, params="{}")
    mock_step_2 = JobStep(id=step_id, job_id=job_id, step_index=1, step_type="transform", status=JobStatus.FAILED, retry_count=3, params="{}")
    mock_step_3 = JobStep(id=uuid.uuid4(), job_id=job_id, step_index=2, step_type="notify", status=JobStatus.SKIPPED, params="{}")
    mock_job.steps = [mock_step_1, mock_step_2, mock_step_3]
    
    def side_effect(model, ident=None):
        if model == Job and ident == str(job_id): return mock_job
        return None
        
    mock_session.get.side_effect = side_effect
    
    # Mock the recursive child lookup
    mock_session.exec.return_value.all.return_value = []
    
    response = await test_client.post(f"/jobs/{str(job_id)}/resume")
    
    assert response.status_code == 200
    assert mock_job.status == JobStatus.PENDING
    assert mock_step_1.status == JobStatus.COMPLETED  # Untouched
    assert mock_step_2.status == JobStatus.PENDING    # Reset
    assert mock_step_2.retry_count == 0               # Reset
    assert mock_step_3.status == JobStatus.PENDING    # Reset from SKIPPED
    
    assert mock_publish.called
    args, kwargs = mock_publish.call_args
    assert kwargs["queue"] == "step_transform"
