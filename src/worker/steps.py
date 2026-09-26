import csv
import json
import os

def convert_csv_to_json(input_path: str, output_path: str) -> None:
    with open(input_path, mode='r', encoding='utf-8') as csv_file:
        reader = csv.DictReader(csv_file)
        with open(output_path, mode='w', encoding='utf-8') as json_file:
            json_file.write('[')
            first = True
            for row in reader:
                if not first:
                    json_file.write(',')
                else:
                    first = False
                json.dump(row, json_file)
            json_file.write(']')

