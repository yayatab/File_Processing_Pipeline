import pytest
from httpx import AsyncClient, ASGITransport
from src.api.main import app
from unittest.mock import patch, MagicMock
from src.models import Job, JobStatus, FileReference
import uuid

@pytest.fixture
def test_client():
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")

@pytest.mark.asyncio
async def test_health(test_client):
    response = await test_client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

@pytest.mark.asyncio
@patch("src.api.routers.jobs.get_session")
@patch("src.api.routers.jobs.broker.publish")
async def test_file_upload_and_job_creation(mock_publish, mock_session_dep, test_client, tmp_path):
    mock_session = MagicMock()
    mock_session_dep.return_value = mock_session
    
    pipeline = {"pipeline": [{"step": "validate"}]}
    files = {"file": ("test.csv", b"dummy content", "text/csv")}
    data = {"pipeline": json.dumps(pipeline)}
    
    import json
    
    with patch("os.makedirs"):
        with patch("builtins.open"):
            with patch("shutil.copyfileobj"):
                with patch("os.path.getsize", return_value=1024):
                    response = await test_client.post("/jobs/upload", data=data, files=files)
    
    assert response.status_code == 200
    assert "job_id" in response.json()
    assert mock_session.add.called
    assert mock_session.commit.called
    assert mock_publish.called

@pytest.mark.asyncio
@patch("src.api.routers.jobs.get_session")
async def test_job_status_tracking(mock_session_dep, test_client):
    mock_session = MagicMock()
    mock_session_dep.return_value = mock_session
    
    job_id = str(uuid.uuid4())
    mock_job = Job(id=job_id, status=JobStatus.COMPLETED)
    mock_session.get.return_value = mock_job
    
    response = await test_client.get(f"/jobs/{job_id}")
    assert response.status_code == 200
    assert response.json()["status"] == "COMPLETED"

@pytest.mark.asyncio
@patch("src.api.routers.jobs.get_session")
async def test_file_retrieval(mock_session_dep, test_client):
    mock_session = MagicMock()
    mock_session_dep.return_value = mock_session
    
    file_id = str(uuid.uuid4())
    mock_file = FileReference(id=file_id, storage_path="dummy_path.csv", original_filename="test.csv", size_in_mb=1.0, content_type="text/csv", job_id=uuid.uuid4())
    mock_session.get.return_value = mock_file
    
    with patch("os.path.exists", return_value=True):
        with patch("fastapi.responses.FileResponse") as mock_file_response:
            mock_file_response.return_value = {"status": "file returned"}
            response = await test_client.get(f"/jobs/files/{file_id}")
            
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
    
    mock_session.get.return_value = mock_job
    
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
