import subprocess
import pandas as pd

def test_roundtrip():
    # Step 1: Run writer
    subprocess.run(["python", "writer.py", "csv_to_custom", "sample_small.csv", "out.colf"], check=True)

    # Step 2: Run reader
    subprocess.run(["python", "reader.py", "custom_to_csv", "out.colf", "out_roundtrip.csv"], check=True)

    # Step 3: Compare input and output
    orig = pd.read_csv("sample_small.csv").fillna("")
    rt = pd.read_csv("out_roundtrip.csv").fillna("")

    assert orig.equals(rt), "Roundtrip failed: CSV does not match"
