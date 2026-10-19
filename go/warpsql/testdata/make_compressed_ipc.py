"""Writes the compressed Arrow IPC streams ipcdecode_test.go reads, with Arrow C++ (pyarrow):

    python make_compressed_ipc.py      # cpp_lz4.arrows.gz, cpp_zstd.arrows.gz (gzipped to keep them small)

Arrow C++ writes each buffer as one LZ4 frame of linked 64 KB blocks; a block that doesn't
compress (the random column) is stored as it is inside the frame. (Buffers left
uncompressed inside a compressed message, length -1, are tested with streams the Go writer
makes, since pyarrow doesn't expose min_space_savings.)
"""

import gzip

import pyarrow as pa
import pyarrow.ipc as ipc

ROWS = 18_000


def splitmix(i: int) -> int:
    z = (i + 0x9E3779B97F4A7C15) & 0xFFFFFFFFFFFFFFFF
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & 0xFFFFFFFFFFFFFFFF
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & 0xFFFFFFFFFFFFFFFF
    return z ^ (z >> 31)


def table() -> pa.Table:
    i = list(range(ROWS))
    return pa.table({
        "i": pa.array(i, type=pa.int64()),
        "rand": pa.array([splitmix(v) >> 40 for v in i], type=pa.int32()),
        "s": pa.array([None if v % 7 == 0 else f"r{v}" + "x" * (v % 5) for v in i]),
        "sv": pa.array([None if v % 5 == 0 else f"view {v % 100} past twelve" for v in i], type=pa.string_view()),
        "l": pa.array([[v, None, v + 1] if v % 3 else None for v in i], type=pa.list_(pa.int32())),
        "st": pa.array([{"a": v, "b": None if v % 2 else "b"} for v in i]),
        "d": pa.array([v / 8 for v in i], type=pa.float64()),
        "dec": pa.array([v * 100 for v in i], type=pa.decimal128(18, 2)),
        "e": pa.array([None] * ROWS, type=pa.int16()),
    })


def write(path: str, codec: str) -> None:
    t = table()
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, t.schema, options=ipc.IpcWriteOptions(compression=codec)) as w:
        for batch in t.to_batches(max_chunksize=9_000):
            w.write_batch(batch)
        w.write_batch(pa.record_batch([pa.array([], type=field.type) for field in t.schema], schema=t.schema))
    with open(path + ".gz", "wb") as f:
        f.write(gzip.compress(sink.getvalue().to_pybytes(), compresslevel=9, mtime=0))


write("cpp_lz4.arrows", "lz4")
write("cpp_zstd.arrows", "zstd")
