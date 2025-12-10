import time
import csv
import subprocess
import sys
from pathlib import Path

PY = sys.executable

CSV_PATH = Path('sample_medium.csv')
COLF_PATH = Path('medium.colf')
COLUMN = 'name'   # column to benchmark; change if needed

def read_name_csv(path: Path):
    t0 = time.perf_counter()
    with open(path, encoding='utf-8', newline='') as f:
        r = csv.DictReader(f)
        # read only the requested column
        names = [row[COLUMN] for row in r]
    return time.perf_counter() - t0, len(names)

def read_name_colf(colf: Path):
    t0 = time.perf_counter()
    # use reader.py read_columns CLI which prints CSV to stdout
    subprocess.run([PY, 'reader.py', 'read_columns', str(colf), COLUMN], check=True, stdout=subprocess.DEVNULL)
    return time.perf_counter() - t0

def main():
    if not CSV_PATH.exists():
        print(f"CSV not found: {CSV_PATH}. Please create it (gen_medium.py) and re-run.")
        sys.exit(1)


    print("Writing columnar file (this may take a few seconds)...")
    subprocess.run([PY, 'writer.py', 'csv_to_custom', str(CSV_PATH), str(COLF_PATH)], check=True)


    print("Warming up (1x each)...")
    read_name_csv(CSV_PATH)
    read_name_colf(COLF_PATH)


    repeats = 3
    csv_times = []
    colf_times = []
    count = 0
    for i in range(repeats):
        t_csv, n = read_name_csv(CSV_PATH)
        t_colf = read_name_colf(COLF_PATH)
        csv_times.append(t_csv)
        colf_times.append(t_colf)
        print(f"Run {i+1}: CSV {t_csv:.4f}s  COLF {t_colf:.4f}s")
        count = n

    avg_csv = sum(csv_times) / len(csv_times)
    avg_colf = sum(colf_times) / len(colf_times)
    speedup = avg_csv / avg_colf if avg_colf > 0 else float('inf')

    print("\nRESULTS")
    print(f"Rows read: {count}")
    print(f"Average CSV read time   : {avg_csv:.6f} s")
    print(f"Average COLF read time  : {avg_colf:.6f} s")
    print(f"Speedup (CSV / COLF)    : {speedup:.2f}x")

if __name__ == '__main__':
    main()
