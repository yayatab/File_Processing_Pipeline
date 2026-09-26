import os
import pytest
from src.worker.steps import convert_csv_to_json

def generate_csv(path: str, target_size_mb: int) -> None:
    target_bytes = target_size_mb * 1024 * 1024
    row = "id,name,email,status,value\n1,Test User,test@example.com,active,123.45\n"
    
    with open(path, 'w') as f:
        f.write("id,name,email,status,value\n")
        current_bytes = len("id,name,email,status,value\n")
        row_bytes = len(row) - len("id,name,email,status,value\n")
        
        while current_bytes < target_bytes:
            f.write("1,Test User,test@example.com,active,123.45\n")
            current_bytes += row_bytes

@pytest.mark.parametrize("size_mb", [10, 50, 100, 150])
def test_large_csv_to_json_conversion(size_mb: int, tmp_path):
    input_csv = str(tmp_path / f"test_{size_mb}mb.csv")
    output_json = str(tmp_path / f"test_{size_mb}mb.json")
    
    generate_csv(input_csv, size_mb)
    
    assert os.path.getsize(input_csv) >= size_mb * 1024 * 1024
    
    convert_csv_to_json(input_csv, output_json)
    
    assert os.path.exists(output_json)
    assert os.path.getsize(output_json) > 0
    
    # Just check if it starts and ends correctly as a JSON array
    with open(output_json, 'rb') as f:
        f.seek(0, os.SEEK_END)
        end_pos = f.tell()
        
        f.seek(0)
        assert f.read(1) == b'['
        
        f.seek(end_pos - 1)
        assert f.read(1) == b']'
