import pytest
from httpx import AsyncClient, ASGITransport
from src.api.main import app
from unittest.mock import patch, MagicMock
from src.models import Job, JobStatus, FileReference
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
    mock_job = Job(id=job_id, status=JobStatus.COMPLETED)
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
    
    from src.worker.main import process_job
    from src.models import JobStep
    
    job_id = uuid.uuid4()
    mock_job = Job(id=job_id, status=JobStatus.PENDING)
    mock_job.steps = [JobStep(job_id=job_id, step_index=0, step_type="validate", params="{}")]
    
    mock_file = FileReference(job_id=job_id, storage_path="dummy.csv", original_filename="dummy.csv", size_in_mb=1.0, content_type="text/csv")
    mock_job.files = [mock_file]
    
    mock_session.get.return_value = mock_job
    
    with patch("src.worker.steps.validate_file"):
        await process_job({"job_id": str(job_id)})
    
    assert mock_job.status == JobStatus.COMPLETED
    assert mock_job.steps[0].status == JobStatus.COMPLETED

@pytest.mark.asyncio
@patch("src.worker.main.Session")
async def test_step_failure_handling(mock_session_cls):
    mock_session = MagicMock()
    mock_session_cls.return_value.__enter__.return_value = mock_session
    
    from src.worker.main import process_job
    from src.models import JobStep
    
    job_id = uuid.uuid4()
    mock_job = Job(id=job_id, status=JobStatus.PENDING)
    # Unknown step type will pass, but let's force an exception by mocking
    mock_step = JobStep(job_id=job_id, step_index=0, step_type="error_step", params="{}")
    mock_job.steps = [mock_step]
    
    mock_session.get.return_value = mock_job
    
    with patch("src.worker.main.json.loads", side_effect=Exception("forced error")):
        mock_step.step_type = "notify" # Trigger the branch that calls json.loads
        await process_job({"job_id": str(job_id)})
        
    assert mock_job.status == JobStatus.FAILED
    assert mock_step.status == JobStatus.FAILED
    assert "forced error" in mock_step.error_message
