import os
import json
import gzip
import zipfile
import pytest
from src.worker.steps import validate_file, transform_file, compress_gzip, extract_zip

def test_validate_file_valid_json(tmp_path):
    valid_json = tmp_path / "valid.json"
    valid_json.write_text('[\n  {"a": 1},\n  {"b": 2}\n]')
    
    # Should not raise any exceptions
    validate_file(str(valid_json), 'json')

def test_validate_file_invalid_json(tmp_path):
    invalid_json = tmp_path / "invalid.json"
    invalid_json.write_text('[\n  {"a": 1, \n]') # missing closing brace for object
    
    with pytest.raises(Exception):
        validate_file(str(invalid_json), 'json')

def test_transform_file(tmp_path):
    input_csv = tmp_path / "input.csv"
    output_csv = tmp_path / "output.csv"
    
    input_csv.write_text(
        "id,name,status,description\n"
        "1,alice,active,  spaces  \n"
        "2,bob,inactive,test\n"
        "3,charlie,active,more spaces\n"
    )
    
    params = {
        "filter_rows": {"status": "active"},
        "transformations": {
            "name": "uppercase",
            "description": "trim"
        },
        "select_columns": ["name", "description"]
    }
    
    transform_file(str(input_csv), str(output_csv), 'csv', params)
    
    assert os.path.exists(output_csv)
    content = output_csv.read_text().strip().split('\n')
    
    # Expected output:
    # name,description
    # ALICE,spaces
    # CHARLIE,more spaces
    
    assert len(content) == 3
    assert content[0] == "name,description"
    assert content[1] == "ALICE,spaces"
    assert content[2] == "CHARLIE,more spaces"

def test_compress_gzip(tmp_path):
    input_file = tmp_path / "test.txt"
    input_file.write_text("hello world")
    
    output_gz = tmp_path / "test.txt.gz"
    
    compress_gzip(str(input_file), str(output_gz))
    
    assert os.path.exists(output_gz)
    
    # Verify content
    with gzip.open(output_gz, 'rb') as f:
        assert f.read() == b"hello world"

def test_extract_zip(tmp_path):
    # Create a dummy zip
    zip_path = tmp_path / "archive.zip"
    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr("file1.txt", "content 1")
        zf.writestr("file2.txt", "content 2")
        
    extract_dir = tmp_path / "extracted"
    os.makedirs(extract_dir, exist_ok=True)
    
    extracted = extract_zip(str(zip_path), str(extract_dir))
    
    assert len(extracted) == 2
    assert os.path.exists(os.path.join(extract_dir, "file1.txt"))
    assert os.path.exists(os.path.join(extract_dir, "file2.txt"))
    
    with open(os.path.join(extract_dir, "file1.txt"), 'r') as f:
        assert f.read() == "content 1"
