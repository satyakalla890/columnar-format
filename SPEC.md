Custom Columnar File Format - SPEC.md


Magic: 4 bytes: ASCII `COLF` (0x43 0x4F 0x4C 0x46)
Version: 1 byte: unsigned integer (current = 1)
Endianness: 1 byte: 0 = big-endian, 1 = little-endian (current = 1)
HeaderSize: uint32 (4 bytes) - number of bytes in the header section following this field


Header (variable length, total HeaderSize bytes) layout:
- SchemaLength: uint32
- SchemaJSON: bytes (UTF-8), length = SchemaLength. Schema JSON describes column order and types.
- For each column in schema (in order) a ColumnMetadata entry:
- Offset: uint64 (8 bytes) - absolute byte offset in file to start of the *compressed* column block
- CompressedSize: uint64 (8 bytes)
- UncompressedSize: uint64 (8 bytes)
- HasNulls: uint8 (1 byte) - 0 or 1


Notes:
- Offsets are absolute from file start (byte 0).
- All integer numeric fields in file use *little-endian* encoding.


Schema JSON structure (example):
{
"num_rows": 1000,
"columns": [
{"name":"id","type":"int32","nullable":false},
{"name":"price","type":"float64","nullable":true},
{"name":"name","type":"utf8","nullable":false}
]
}


Compressed Column Block (on-disk, each column stored as a compressed blob):
- The compressed blob is produced by applying zlib.compress() to the **uncompressed column payload** described below.
- The ColumnMetadata contains the offset and sizes so the reader can seek & read only required columns.


Uncompressed Column Payload (binary layout)
- DataType: uint8 (1 = int32, 2 = float64, 3 = utf8)
- HasNulls: uint8 (0/1)
- If HasNulls == 1:
- NullBitmap: ceil(num_rows / 8) bytes. Bit i (LSB first within a byte) == 1 indicates NULL at row i.
- Column data payload (depends on data type):
- int32: contiguous int32 values (little-endian). For null entries a placeholder 0 is stored but null bitmap marks it.
- float64: contiguous float64 values (little-endian). For null entries a placeholder 0.0 stored.
- utf8 (variable-length strings):
- Offsets: num_rows uint32 little-endian. Offsets[i] is the start byte index into the ConcatenatedStrings block for row i. For NULL rows, offset value is ignored (may be 0).
- ConcatenatedStrings: bytes - all UTF-8 encoded string bytes concatenated back-to-back. String for row i begins at ConcatenatedStrings[Offsets[i]] and ends at Offsets[i+1] (or end for last row).


Rationale / Design choices:
- Per-column compression with zlib gives good compression on homogeneous data and enables selective reads.
- Header contains schema JSON (human-readable) and compact per-column metadata for offsets and sizes.
- Using offsets array for strings avoids scanning the concatenated data when decoding.
- Null bitmap is 1 bit per row per column to save space.


Compatibility notes:
- Readers should validate magic and version before parsing.
- If HasNulls == 0, readers skip reading the null bitmap.
- All sizes and offsets are 64-bit to support very large files.