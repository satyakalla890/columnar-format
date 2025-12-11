import subprocess
import pandas as pd
import os
import csv

PY = "python"  # or sys.executable

def run_writer(in_csv, out_colf):
    subprocess.run([PY, "writer.py", "csv_to_custom", in_csv, out_colf], check=True)

def run_reader(in_colf, out_csv):
    subprocess.run([PY, "reader.py", "custom_to_csv", in_colf, out_csv], check=True)

def write_csv(path, header, rows):
    with open(path, "w", newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)

def test_null_column_roundtrip(tmp_path):
    # create CSV with a null column and mixed values
    csv_path = tmp_path / "nulls.csv"
    out_colf = tmp_path / "nulls.colf"
    out_csv = tmp_path / "nulls_out.csv"
    header = ["id", "maybe"]
    rows = [
        [1, ""],
        [2, "hello"],
        [3, ""],
    ]
    write_csv(csv_path, header, rows)
    run_writer(str(csv_path), str(out_colf))
    run_reader(str(out_colf), str(out_csv))
    df_in = pd.read_csv(csv_path).fillna("")
    df_out = pd.read_csv(out_csv).fillna("")
    assert df_in.equals(df_out)

def test_empty_file_roundtrip(tmp_path):
    csv_path = tmp_path / "empty.csv"
    out_colf = tmp_path / "empty.colf"
    out_csv = tmp_path / "empty_out.csv"
    # write only header, no rows
    write_csv(csv_path, ["a","b"], [])
    run_writer(str(csv_path), str(out_colf))
    run_reader(str(out_colf), str(out_csv))
    df_in = pd.read_csv(csv_path)
    df_out = pd.read_csv(out_csv)
    assert df_in.equals(df_out)

def test_mixed_types_roundtrip(tmp_path):
    csv_path = tmp_path / "mix.csv"
    out_colf = tmp_path / "mix.colf"
    out_csv = tmp_path / "mix_out.csv"
    write_csv(csv_path, ["i","f","s"], [
        [1, 1.5, "a"],
        [2, "", "b"],
        ["", 3.1415, "c"]
    ])
    run_writer(str(csv_path), str(out_colf))
    run_reader(str(out_colf), str(out_csv))
    df_in = pd.read_csv(csv_path).fillna("")
    df_out = pd.read_csv(out_csv).fillna("")
    assert df_in.equals(df_out)
