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

def safe_decompress(comp: bytes, meta: dict, col_name: str):
    try:
        return zlib.decompress(comp)
    except Exception as e:
        raise RuntimeError(f"Failed to decompress column '{col_name}' (offset={meta.get('offset')}, comp_size={meta.get('comp_size')}) - underlying error: {e}") from e


def read_header(f) -> Dict[str, Any]:
    f.seek(0)
    magic = f.read(4)
    if magic != MAGIC:
        raise ValueError(f"Bad magic: expected {MAGIC!r}, got {magic!r}")

    version = _unpack_u8(f.read(1))
    endianness = _unpack_u8(f.read(1))
    header_size = _unpack_u32(f.read(4))

    # Validate version/endianness
    if version != 1:
        raise ValueError(f"Unsupported file version: {version}. Expected version 1.")
    if endianness != 1:
        raise ValueError(f"Unsupported endianness: {endianness}. Only little-endian (1) is supported.")

    # read schema length + schema JSON
    schema_len = _unpack_u32(f.read(4))
    if schema_len <= 0 or schema_len > 10_000_000:
        raise ValueError(f"Suspicious schema length: {schema_len}")
    schema_json = f.read(schema_len)
    try:
        schema = json.loads(schema_json.decode('utf-8'))
    except Exception as e:
        raise ValueError("Failed to parse schema JSON") from e

    num_cols = len(schema.get('columns', []))
    if num_cols == 0:
        # allow zero columns but warn
        # we'll still return the header
        pass

    metas = []
    for _ in range(num_cols):
        # guard reading to avoid partially corrupted file causing exceptions
        off_bytes = f.read(8)
        comp_bytes = f.read(8)
        uncomp_bytes = f.read(8)
        has_nulls_b = f.read(1)
        if len(off_bytes) < 8 or len(comp_bytes) < 8 or len(uncomp_bytes) < 8 or len(has_nulls_b) < 1:
            raise ValueError("Unexpected end of file while reading column metadata")
        offset = _unpack_u64(off_bytes)
        comp_size = _unpack_u64(comp_bytes)
        uncomp_size = _unpack_u64(uncomp_bytes)
        has_nulls = _unpack_u8(has_nulls_b)
        metas.append({'offset': offset, 'comp_size': comp_size, 'uncomp_size': uncomp_size, 'has_nulls': bool(has_nulls)})

    return {'version': version, 'endianness': endianness, 'header_size': header_size, 'schema': schema, 'metas': metas}

def decode_column_payload(payload: bytes, dtype: str, num_rows: int, has_nulls: bool) -> List[Any]:
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
            payload = safe_decompress(comp, meta, col['name'])
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

def cli():
    parser = argparse.ArgumentParser(description="Reader for custom columnar format (.colf)")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("custom_to_csv", help="Convert full .colf file to CSV")
    p1.add_argument("input", help="input .colf file")
    p1.add_argument("output", help="output CSV file")

    p2 = sub.add_parser("read_columns", help="Read selected columns (prints CSV to stdout)")
    p2.add_argument("input", help="input .colf file")
    p2.add_argument("cols", help="comma-separated column names")

    args = parser.parse_args()
    if args.cmd == "custom_to_csv":
        rows = read_all(args.input)

        # If zero rows → write only header
        if not rows:
            with open(args.input, "rb") as f:
                header = read_header(f)
                columns = [c['name'] for c in header['schema']['columns']]

            with open(args.output, "w", newline='', encoding='utf-8') as out:
                writer = csv.writer(out)
                writer.writerow(columns)

            print("No rows to write")
            print(f"Wrote empty CSV to {args.output}")
            return

        # Normal non-empty file
        cols = list(rows[0].keys())
        with open(args.output, "w", newline='', encoding='utf-8') as out:
            writer = csv.DictWriter(out, fieldnames=cols)
            writer.writeheader()
            for r in rows:
                out_row = {k: ("" if r[k] is None else str(r[k])) for k in cols}
                writer.writerow(out_row)

        print(f"Wrote CSV to {args.output}")
        return
    elif args.cmd == "read_columns":
        cols = [c.strip() for c in args.cols.split(",") if c.strip()]
        result = read_columns(args.input, cols)

        writer = csv.writer(sys.stdout)
        writer.writerow(cols)
        num_rows = len(next(iter(result.values()))) if result else 0

        for i in range(num_rows):
            row = [("" if result[c][i] is None else result[c][i]) for c in cols]
            writer.writerow(row)



if __name__ == '__main__':
    cli()
