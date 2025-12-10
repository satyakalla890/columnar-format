#!/usr/bin/env python3
"""
reader.py

Reads custom columnar files (.colf) created by writer.py.

Usage (examples):

# Full file -> CSV
python reader.py custom_to_csv input.colf output.csv

# Selective read (prints CSV to stdout)
python reader.py read_columns input.colf col1,col2
"""

import struct
import json
import zlib
import io
import sys
import csv
import argparse
from typing import List, Dict, Any, Tuple

MAGIC = b'COLF'
DT_INT32 = 1
DT_FLOAT64 = 2
DT_UTF8 = 3

# struct helpers (little-endian)
_unpack_u8  = lambda b: struct.unpack('<B', b)[0]
_unpack_u32 = lambda b: struct.unpack('<I', b)[0]
_unpack_u64 = lambda b: struct.unpack('<Q', b)[0]
_unpack_i32 = lambda b: struct.unpack('<i', b)[0]
_unpack_f64 = lambda b: struct.unpack('<d', b)[0]


def read_header(f) -> Dict[str, Any]:
    """
    Reads and returns header information from the file object f (open in 'rb').
    Returns dict: {version, endianness, header_size, schema(dict), metas(list)}
    Each meta: {'offset', 'comp_size', 'uncomp_size', 'has_nulls'}
    """
    f.seek(0)
    magic = f.read(4)
    if magic != MAGIC:
        raise ValueError(f"Bad magic: expected {MAGIC!r}, got {magic!r}")

    version = _unpack_u8(f.read(1))
    endianness = _unpack_u8(f.read(1))
    header_size = _unpack_u32(f.read(4))

    # read schema length + schema JSON
    schema_len = _unpack_u32(f.read(4))
    schema_json = f.read(schema_len)
    try:
        schema = json.loads(schema_json.decode('utf-8'))
    except Exception as e:
        raise ValueError("Failed to parse schema JSON") from e

    num_cols = len(schema.get('columns', []))

    metas = []
    for _ in range(num_cols):
        offset = _unpack_u64(f.read(8))
        comp_size = _unpack_u64(f.read(8))
        uncomp_size = _unpack_u64(f.read(8))
        has_nulls = _unpack_u8(f.read(1))
        metas.append({
            'offset': offset,
            'comp_size': comp_size,
            'uncomp_size': uncomp_size,
            'has_nulls': bool(has_nulls)
        })

    return {
        'version': version,
        'endianness': endianness,
        'header_size': header_size,
        'schema': schema,
        'metas': metas
    }


def decode_column_payload(payload: bytes, dtype: str, num_rows: int, has_nulls: bool) -> List[Any]:
    """
    Decode the uncompressed payload bytes for one column.
    Returns a list of values of length num_rows (None for NULLs).
    """
    buf = io.BytesIO(payload)

    # Payload starts with DataType (1) and HasNulls (1) bytes per spec
    data_type_byte = struct.unpack('<B', buf.read(1))[0]
    has_nulls_in_payload = struct.unpack('<B', buf.read(1))[0]

    # Null bitmap
    nulls = [False] * num_rows
    if has_nulls:
        bit_bytes = (num_rows + 7) // 8
        bitmap = buf.read(bit_bytes)
        # interpret LSB-first within each byte
        for i in range(num_rows):
            byte_idx = i // 8
            bit_idx = i % 8
            nulls[i] = ((bitmap[byte_idx] >> bit_idx) & 1) == 1

    # Decode by dtype
    if dtype == 'int32':
        values = []
        for i in range(num_rows):
            raw = buf.read(4)
            if len(raw) < 4:
                raise ValueError("Unexpected end of int32 data")
            v = struct.unpack('<i', raw)[0]
            values.append(None if nulls[i] else v)
        return values

    if dtype == 'float64':
        values = []
        for i in range(num_rows):
            raw = buf.read(8)
            if len(raw) < 8:
                raise ValueError("Unexpected end of float64 data")
            v = struct.unpack('<d', raw)[0]
            values.append(None if nulls[i] else v)
        return values

    # utf8: offsets array then concatenated strings
    if dtype == 'utf8':
        offsets = []
        for _ in range(num_rows):
            raw = buf.read(4)
            if len(raw) < 4:
                raise ValueError("Unexpected end of offsets array")
            offsets.append(struct.unpack('<I', raw)[0])
        strings_blob = buf.read()  # remainder is concatenated strings
        values = []
        # Precompute next_nonnull_offset index to determine string end
        for i in range(num_rows):
            if nulls[i]:
                values.append(None)
                continue
            start = offsets[i]
            # find end: next offset of a non-null row after i, else end of blob
            end = len(strings_blob)
            for j in range(i + 1, num_rows):
                if not nulls[j] and offsets[j] != offsets[i]:
                    end = offsets[j]
                    break
            s = strings_blob[start:end]
            try:
                values.append(s.decode('utf-8'))
            except Exception:
                # on decode error, return raw bytes as fallback
                values.append(s)
        return values

    raise ValueError(f"Unknown dtype: {dtype}")


def read_columns(colf_path: str, columns: List[str]) -> Dict[str, List[Any]]:
    """
    Selectively read the requested columns from the .colf file.
    Returns a dict mapping column_name -> list_of_values.
    """
    with open(colf_path, 'rb') as f:
        header = read_header(f)
        schema = header['schema']
        metas = header['metas']
        num_rows = schema['num_rows']

        col_index = {c['name']: idx for idx, c in enumerate(schema['columns'])}
        result: Dict[str, List[Any]] = {}

        for col in columns:
            if col not in col_index:
                raise KeyError(f"Column not found: {col}")
            idx = col_index[col]
            meta = metas[idx]
            # Sanity checks
            if meta['comp_size'] == 0:
                result[col] = [None] * num_rows
                continue
            f.seek(meta['offset'])
            comp = f.read(meta['comp_size'])
            if len(comp) != meta['comp_size']:
                raise ValueError("Failed to read full compressed column block")
            payload = zlib.decompress(comp)
            dtype = schema['columns'][idx]['type']
            vals = decode_column_payload(payload, dtype, num_rows, meta['has_nulls'])
            result[col] = vals
        return result


def read_all(colf_path: str) -> List[Dict[str, Any]]:
    """
    Read entire file and reconstruct rows (list of dicts).
    """
    with open(colf_path, 'rb') as f:
        header = read_header(f)
        schema = header['schema']
        metas = header['metas']
        num_rows = schema['num_rows']

        col_arrays: Dict[str, List[Any]] = {}
        for idx, col in enumerate(schema['columns']):
            meta = metas[idx]
            if meta['comp_size'] == 0:
                col_arrays[col['name']] = [None] * num_rows
                continue
            f.seek(meta['offset'])
            comp = f.read(meta['comp_size'])
            payload = zlib.decompress(comp)
            vals = decode_column_payload(payload, col['type'], num_rows, meta['has_nulls'])
            col_arrays[col['name']] = vals

        rows = []
        column_names = [c['name'] for c in schema['columns']]
        for i in range(num_rows):
            row = {}
            for name in column_names:
                row[name] = col_arrays[name][i]
            rows.append(row)
        return rows


# -------------------------
# CLI: use argparse with subcommands
# -------------------------
def cli():
    parser = argparse.ArgumentParser(description="Reader for custom columnar format (.colf)")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("custom_to_csv", help="Convert full .colf file to CSV")
    p1.add_argument("input", help="input .colf file")
    p1.add_argument("output", help="output CSV file")

    p2 = sub.add_parser("read_columns", help="Read selected columns (prints CSV to stdout)")
    p2.add_argument("input", help="input .colf file")
    p2.add_argument("cols", help="comma-separated column names (e.g. name,salary)")

    args = parser.parse_args()

    if args.cmd == "custom_to_csv":
        rows = read_all(args.input)
        if not rows:
            print("No rows to write")
            return
        cols = list(rows[0].keys())
        with open(args.output, "w", newline='', encoding='utf-8') as out:
            writer = csv.DictWriter(out, fieldnames=cols)
            writer.writeheader()
            for r in rows:
                out_row = {k: ("" if r[k] is None else str(r[k])) for k in cols}
                writer.writerow(out_row)
        print(f"Wrote CSV to {args.output}")

    elif args.cmd == "read_columns":
        cols = [c.strip() for c in args.cols.split(",") if c.strip()]
        result = read_columns(args.input, cols)
        # print CSV to stdout
        writer = csv.writer(sys.stdout)
        writer.writerow(cols)
        num_rows = len(next(iter(result.values()))) if result else 0
        for i in range(num_rows):
            row = [("" if result[c][i] is None else result[c][i]) for c in cols]
            writer.writerow(row)


if __name__ == '__main__':
    cli()
