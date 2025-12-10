#!/usr/bin/env python3

import csv
import json
import struct
import zlib
import io
import sys
from typing import List, Dict, Any, Tuple

# File constants
MAGIC = b'COLF'
VERSION = 1
ENDIANNESS = 1  # 1 = little-endian

# Data type enum
DT_INT32 = 1
DT_FLOAT64 = 2
DT_UTF8 = 3

# struct helpers (little-endian)
_pack_u8  = lambda v: struct.pack('<B', v)
_pack_u32 = lambda v: struct.pack('<I', v)
_pack_u64 = lambda v: struct.pack('<Q', v)
_pack_i32 = lambda v: struct.pack('<i', v)
_pack_f64 = lambda v: struct.pack('<d', v)

_unpack_u8  = lambda b: struct.unpack('<B', b)[0]
_unpack_u32 = lambda b: struct.unpack('<I', b)[0]
_unpack_u64 = lambda b: struct.unpack('<Q', b)[0]


def infer_schema(rows: List[Dict[str, str]]) -> Tuple[List[Dict[str, Any]], int]:
    if not rows:
        return [], 0
    colnames = list(rows[0].keys())
    num_rows = len(rows)
    schema: List[Dict[str, Any]] = []
    for c in colnames:
        is_int = True
        is_float = True
        nullable = False
        for r in rows:
            v = r[c]
            if v is None:
                v = ''
            v = v.strip()
            if v == '':
                nullable = True
                continue
            if is_int:
                try:
                    int(v)
                except Exception:
                    is_int = False
            if is_float:
                try:
                    float(v)
                except Exception:
                    is_float = False
        if is_int:
            dtype = 'int32'
        elif is_float:
            dtype = 'float64'
        else:
            dtype = 'utf8'
        schema.append({'name': c, 'type': dtype, 'nullable': nullable})
    return schema, num_rows


def build_column_payload(col_values: List[str], dtype: str) -> Tuple[bytes, bool]:
    
    n = len(col_values)
    has_nulls = any((v is None) or (str(v).strip() == '') for v in col_values)
    buf = io.BytesIO()

    # DataType byte
    if dtype == 'int32':
        buf.write(_pack_u8(DT_INT32))
    elif dtype == 'float64':
        buf.write(_pack_u8(DT_FLOAT64))
    else:
        buf.write(_pack_u8(DT_UTF8))

    # HasNulls byte
    buf.write(_pack_u8(1 if has_nulls else 0))

    # Null bitmap if needed (LSB-first in each byte)
    if has_nulls:
        bit_bytes = (n + 7) // 8
        bitmap = bytearray(bit_bytes)
        for i, v in enumerate(col_values):
            vv = '' if v is None else str(v).strip()
            if vv == '':
                byte_idx = i // 8
                bit_idx = i % 8
                bitmap[byte_idx] |= (1 << bit_idx)
        buf.write(bytes(bitmap))

    # Column data
    if dtype == 'int32':
        for v in col_values:
            vv = '' if v is None else str(v).strip()
            if vv == '':
                buf.write(_pack_i32(0))
            else:
                try:
                    iv = int(vv)
                except Exception:
                    iv = 0
                buf.write(_pack_i32(iv))

    elif dtype == 'float64':
        for v in col_values:
            vv = '' if v is None else str(v).strip()
            if vv == '':
                buf.write(_pack_f64(0.0))
            else:
                try:
                    fv = float(vv)
                except Exception:
                    fv = 0.0
                buf.write(_pack_f64(fv))

    else:  # utf8
        offsets: List[int] = []
        strings = bytearray()
        for v in col_values:
            vv = '' if v is None else str(v)
            if vv.strip() == '':
                offsets.append(0)
            else:
                offsets.append(len(strings))
                b = vv.encode('utf-8')
                strings.extend(b)
        # write offsets as uint32 for each row
        for off in offsets:
            buf.write(_pack_u32(off))
        # write concatenated strings
        buf.write(bytes(strings))

    return buf.getvalue(), has_nulls


def write_custom(csv_path: str, out_path: str) -> None:
    
    # Read CSV (preserve header order)
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = [r for r in reader]

    schema, num_rows = infer_schema(rows)
    if not schema:
        raise RuntimeError("No columns found in CSV")

    # Build uncompressed payloads for each column
    column_payloads: List[Tuple[bytes, int, bool]] = []  # (uncompressed_bytes, uncomp_size, has_nulls)
    for col in schema:
        values = [r[col['name']] for r in rows]
        payload, has_nulls = build_column_payload(values, col['type'])
        column_payloads.append((payload, len(payload), has_nulls))

    # Compress each column payload and track sizes
    column_blocks: List[Tuple[bytes, int, int, bool]] = []  # (compressed_bytes, comp_size, uncomp_size, has_nulls)
    for payload, uncomp_size, has_nulls in column_payloads:
        comp = zlib.compress(payload)
        comp_size = len(comp)
        column_blocks.append((comp, comp_size, uncomp_size, has_nulls))

    # Build schema JSON (num_rows and columns metadata)
    schema_json = json.dumps({'num_rows': num_rows, 'columns': schema}, separators=(',', ':')).encode('utf-8')
    schema_len = len(schema_json)

    # Compute header size:
    # SchemaLength (4 bytes) + SchemaJSON + per-column metadata entries
    # per-column metadata: Offset(8) + CompSize(8) + UncompSize(8) + HasNulls(1) = 25 bytes each
    per_col_meta_size = len(schema) * (8 + 8 + 8 + 1)
    header_size = 4 + schema_len + per_col_meta_size  # SchemaLength (4) included

    # Open file and write preamble + header + column blocks
    with open(out_path, 'wb') as out:
        # Preamble
        out.write(MAGIC)
        out.write(_pack_u8(VERSION))
        out.write(_pack_u8(ENDIANNESS))
        out.write(_pack_u32(header_size))

        # Schema length + schema JSON
        out.write(_pack_u32(schema_len))
        out.write(schema_json)

        # Compute absolute offsets for each column block.
        # Current offset after header = current file position + per_col_meta_size (we'll write metadata now)
        current_offset = out.tell() + per_col_meta_size

        # Build metadata bytes
        meta_buf = io.BytesIO()
        for comp, comp_size, uncomp_size, has_nulls in column_blocks:
            meta_buf.write(_pack_u64(current_offset))
            meta_buf.write(_pack_u64(comp_size))
            meta_buf.write(_pack_u64(uncomp_size))
            meta_buf.write(_pack_u8(1 if has_nulls else 0))
            current_offset += comp_size

        # Write metadata region
        out.write(meta_buf.getvalue())

        # Write column blocks sequentially
        for comp, comp_size, uncomp_size, has_nulls in column_blocks:
            out.write(comp)

    print(f"Wrote {out_path} with {num_rows} rows and {len(schema)} columns")


def cli_csv_to_custom():
    if len(sys.argv) != 4:
        print("Usage: writer.py csv_to_custom input.csv output.colf")
        sys.exit(1)

    command = sys.argv[1]
    if command != "csv_to_custom":
        print("Unknown command:", command)
        print("Usage: writer.py csv_to_custom input.csv output.colf")
        sys.exit(1)

    csv_path = sys.argv[2]
    out_path = sys.argv[3]
    write_custom(csv_path, out_path)



if __name__ == '__main__':
    cli_csv_to_custom()
