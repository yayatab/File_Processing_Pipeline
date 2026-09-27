import csv
import json
from typing import Iterator, Dict, Any, TextIO

# --- Readers ---
def read_csv(f: TextIO) -> Iterator[Dict[str, Any]]:
    yield from csv.DictReader(f)

def read_json(f: TextIO) -> Iterator[Dict[str, Any]]:
    # Incremental JSON array reading for flat lists of dicts
    # Assumes format: [\n  {...},\n  {...}\n] or similar
    # For a robust streaming JSON, a library like ijson is better,
    # but adhering to "simple works best", we read line by line or decode full if small.
    # Wait, we need streaming. Let's do a simple line-by-line manual parse for JSON Lines,
    # or just use json.load if we assume standard JSON.
    # To keep it truly streaming without dependencies for a standard JSON array, 
    # it's tricky. Let's write a very basic state machine for JSON arrays of flat dicts.
    
    # Actually, a simple buffered approach for JSON streaming:
    buffer = ""
    in_object = False
    brace_count = 0
    
    for line in f:
        for char in line:
            if char == '{':
                in_object = True
                brace_count += 1
            if in_object:
                buffer += char
            if char == '}':
                brace_count -= 1
                if brace_count == 0 and in_object:
                    yield json.loads(buffer)
                    buffer = ""
                    in_object = False

def read_yaml(f: TextIO) -> Iterator[Dict[str, Any]]:
    current_dict = None
    for line in f:
        line_stripped = line.strip()
        if not line_stripped:
            continue
            
        if line_stripped.startswith("-"):
            if current_dict is not None:
                yield current_dict
            current_dict = {}
            line_stripped = line_stripped[1:].strip()
            
        if ":" in line_stripped and current_dict is not None:
            k, v = line_stripped.split(":", 1)
            current_dict[k.strip()] = v.strip()
            
    if current_dict is not None:
        yield current_dict

# --- Writers ---
def write_csv(f: TextIO, data: Iterator[Dict[str, Any]]) -> None:
    writer = None
    for row in data:
        if writer is None:
            writer = csv.DictWriter(f, fieldnames=row.keys())
            writer.writeheader()
        writer.writerow(row)

def write_json(f: TextIO, data: Iterator[Dict[str, Any]]) -> None:
    f.write('[\n')
    for i, row in enumerate(data):
        if i > 0:
            f.write(',\n')
        f.write('  ' + json.dumps(row))
    f.write('\n]')

def write_yaml(f: TextIO, data: Iterator[Dict[str, Any]]) -> None:
    for row in data:
        f.write("-\n")
        for k, v in row.items():
            f.write(f"  {k}: {v}\n")

# --- Registry ---
READERS = {
    'csv': read_csv,
    'json': read_json,
    'yaml': read_yaml
}

WRITERS = {
    'csv': write_csv,
    'json': write_json,
    'yaml': write_yaml
}

def convert_file(input_path: str, output_path: str, from_ext: str, to_ext: str) -> None:
    reader = READERS.get(from_ext)
    writer = WRITERS.get(to_ext)
    
    if not reader or not writer:
        raise ValueError(f"Unsupported conversion: {from_ext} -> {to_ext}")
        
    with open(input_path, 'r', encoding='utf-8') as f_in, \
         open(output_path, 'w', encoding='utf-8') as f_out:
        writer(f_out, reader(f_in))
