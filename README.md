# Columnar Binary Format (COLF)

A minimal columnar storage format implemented in Python.  
Supports selective reads, compression, and round-trip conversion from CSV.

---

## 🚀 Features

- Custom binary columnar format (`.colf`)
- zlib compression per column
- Supports `int32`, `float64`, and `utf8`
- Nullability support
- Fast selective reads
- Fully documented format in `SPEC.md`
- Tests + benchmarks included

---

## Setup Instructions

### Create and Activate Virtual Environment

#### Windows
python -m venv .venv
.venv\Scripts\activate

### Linux / macOS

python3 -m venv .venv
source .venv/bin/activate

### Install Dependencies
pip install faker pytest

### Convert CSV → Columnar Format
python writer.py csv_to_custom sample_small.csv out.colf

### Convert Columnar Format → CSV (Full Read)
python reader.py custom_to_csv out.colf out_roundtrip.csv

### Read Selected Columns Only (Selective Read)
python reader.py read_columns out.colf name,city

## Running Tests

### Run round-trip tests
python tests/test_roundtrip.py

### Run all tests with pytest
pytest -q

### Running Benchmarks
python benchmark/benchmark_read.py

### Header Inspection (Debug Utility)
python inspect_header.py out.colf

## Project Structure

columnar-format/
├── SPEC.md
├── README.md
├── writer.py
├── reader.py
├── inspect_header.py
├── requirements.txt
├── sample_small.csv
├── sample_medium.csv
├── gen_medium.py
├── tests/
│   ├── test_roundtrip.py
│   ├── test_edge_cases.py
├── benchmark/
│   └── bench_selective_vs_csv.py