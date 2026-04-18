import csv
import sys
import os

def load_csv(filename):
    data = {}
    with open(filename, mode='r', newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            sid = row['security_id']
            # We care about OHCLV data for comparison
            ohclv = (row['open'], row['high'], row['low'], row['close'], row['volume'])
            data[sid] = ohclv
    return data

def main():
    if len(sys.argv) < 3:
        print("Usage: python3 diff_securities.py <file1.csv> <file2.csv>")
        print("Example: python3 diff_securities.py 2026_04_16_on_17.csv 2026_04_16_on_18.csv")
        sys.exit(1)

    file1 = sys.argv[1]
    file2 = sys.argv[2]

    for f in [file1, file2]:
        if not os.path.exists(f):
            print(f"Error: File '{f}' not found.")
            sys.exit(1)
    
    data1 = load_csv(file1)
    data2 = load_csv(file2)
    
    sids1 = set(data1.keys())
    sids2 = set(data2.keys())
    
    missing_in_first = sids2 - sids1
    removed_in_second = sids1 - sids2
    common_sids = sids1 & sids2
    
    restated = []
    for sid in common_sids:
        if data1[sid] != data2[sid]:
            restated.append(sid)
            
    print(f"Summary Comparison: {file1} vs {file2}")
    print(f"Securities in first file:  {len(sids1)}")
    print(f"Securities in second file: {len(sids2)}")
    print("-" * 40)
    print(f"Securities missing from first file (present in second): {len(missing_in_first)}")
    print(f"Securities restated (different data):                {len(restated)}")
    print(f"Securities removed in second file (present in first):  {len(removed_in_second)}")

if __name__ == "__main__":
    main()
