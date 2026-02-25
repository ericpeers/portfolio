#/usr/bin/python3
import csv
import gzip
import json
import os
import sys

def convert_json_to_csv(directory):
    """
    Converts all .json.gz files in a directory to CSV files.

    Args:
        directory (str): The path to the directory containing the .json.gz files.
    """
    for filename in os.listdir(directory):
        if filename.endswith(".json.gz"):
            json_gz_path = os.path.join(directory, filename)
            csv_path = os.path.join(directory, filename.replace(".json.gz", ".csv"))

            try:
                with gzip.open(json_gz_path, 'rt', encoding='utf-8') as gz_file:
                    data = json.load(gz_file)

                if not data:
                    print(f"No data in {json_gz_path}")
                    continue

                with open(csv_path, 'w', newline='', encoding='utf-8') as csv_file:
                    # Assuming all objects in the list have the same keys
                    json_keys = list(data[0].keys())
                    csv_headers = ["Ticker" if k == "Code" else k for k in json_keys]

                    writer = csv.DictWriter(csv_file, fieldnames=csv_headers)
                    writer.writeheader()
                    for row in data:
                        renamed = {("Ticker" if k == "Code" else k): v for k, v in row.items()}
                        writer.writerow(renamed)

                print(f"Successfully converted {json_gz_path} to {csv_path}")

            except Exception as e:
                print(f"Error converting {json_gz_path}: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python json_to_csv.py <directory>")
        sys.exit(1)

    target_directory = sys.argv[1]
    if not os.path.isdir(target_directory):
        print(f"Error: Directory not found at '{target_directory}'")
        sys.exit(1)

    convert_json_to_csv(target_directory)
