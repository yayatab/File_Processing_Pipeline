import os
import pytest
from src.worker.steps import convert_file

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

def test_generic_conversions(tmp_path):
    csv_file = str(tmp_path / "test.csv")
    json_file = str(tmp_path / "test.json")
    yaml_file = str(tmp_path / "test.yaml")
    csv_back = str(tmp_path / "test_back.csv")
    
    with open(csv_file, 'w') as f:
        f.write("a,b\n1,2\n3,4\n")
        
    # CSV -> JSON
    convert_file(csv_file, json_file, 'csv', 'json')
    assert os.path.exists(json_file)
    
    # JSON -> YAML
    convert_file(json_file, yaml_file, 'json', 'yaml')
    assert os.path.exists(yaml_file)
    
    # YAML -> CSV
    convert_file(yaml_file, csv_back, 'yaml', 'csv')
    assert os.path.exists(csv_back)
    
    with open(csv_back) as f:
        content = f.read().strip()
        assert "a,b" in content
        assert "1,2" in content
        assert "3,4" in content

@pytest.mark.parametrize("size_mb", [10, 50, 100, 150])
def test_large_csv_to_json_conversion(size_mb: int, tmp_path):
    input_csv = str(tmp_path / f"test_{size_mb}mb.csv")
    output_json = str(tmp_path / f"test_{size_mb}mb.json")
    
    generate_csv(input_csv, size_mb)
    
    convert_file(input_csv, output_json, 'csv', 'json')
    
    assert os.path.exists(output_json)
    assert os.path.getsize(output_json) > 0
