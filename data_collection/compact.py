"""
Compact a closed day of recorder output (.csv.gz) into a much smaller Parquet file.

Prices and quantities are stored as integers in their native tick (e.g. 0.1 USDT, 0.001 BTC),
which lets Parquet's DELTA_BINARY_PACKED encoding + zstd get ~3x smaller than gzip CSV
(~130 MB/day vs ~400 MB/day for BTCUSDT perp). The scale is chosen per column by scanning the
file and stored in the Parquet metadata; `bookticker.iter_batches` turns it back into floats,
so downstream code doesn't know the difference.

Safety: the source is only deleted after the Parquet file has been written, re-read, and its
row count and first/last rows compared with the source. Runs in a bounded ~200 MB of memory.

    python -m data_collection.compact                 # every closed .csv.gz in data/raw
    python -m data_collection.compact --keep-source   # don't delete the .csv.gz
"""
from __future__ import annotations

import argparse
import gzip
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

RAW_DIR = Path("data/raw")
FLOAT_COLS = ["best_bid_price", "best_bid_qty", "best_ask_price", "best_ask_qty"]
BATCH_BYTES = 32 << 20
FILE_RE = re.compile(r"^(?P<symbol>[A-Z0-9_]+)-bookTicker-(?P<day>\d{4}-\d{2}-\d{2})\.csv\.gz$")

log = logging.getLogger("compact")


def _batches(path: Path):
    with gzip.open(path, "rb") as fh:
        for b in pcsv.open_csv(fh, read_options=pcsv.ReadOptions(block_size=BATCH_BYTES)):
            if b.num_rows:
                yield b


def _scale_for(col: pa.Array, current: int) -> int:
    """Smallest k (>= current) such that every value * 10**k is an integer."""
    for k in range(current, 9):
        scaled = pc.multiply(col, float(10 ** k))
        err = pc.abs(pc.subtract(pc.round(scaled), scaled))
        if pc.all(pc.less(err, 1e-6)).as_py():      # tolerate float noise (0.326*1000 != 326.0)
            return k
    return 8


def find_scales(path: Path) -> dict[str, int]:
    scales = {c: 0 for c in FLOAT_COLS}
    for b in _batches(path):
        for c in FLOAT_COLS:
            scales[c] = _scale_for(b.column(c), scales[c])
    return scales


def _encode(b: pa.RecordBatch, scales: dict[str, int]) -> pa.RecordBatch:
    cols = []
    for name in b.schema.names:
        col = b.column(name)
        if name in scales:
            col = pc.cast(pc.round(pc.multiply(col, float(10 ** scales[name]))), pa.int64())
        elif pa.types.is_floating(col.type):          # e.g. update_id with nulls -> float
            col = pc.cast(col, pa.int64())
        cols.append(col)
    return pa.RecordBatch.from_arrays(cols, names=b.schema.names)


def compact_file(src: Path, keep_source: bool = False) -> Path:
    dst = src.with_name(src.name.replace(".csv.gz", ".parquet"))
    tmp = dst.with_suffix(".parquet.part")
    scales = find_scales(src)
    log.info("%s scales=%s", src.name, scales)

    n_rows, first, last = 0, None, None
    writer = None
    for b in _batches(src):
        enc = _encode(b, scales)
        if writer is None:
            meta = {f"scale:{c}": str(k) for c, k in scales.items()}
            meta["source"] = src.name
            writer = pq.ParquetWriter(tmp, enc.schema.with_metadata(meta), compression="zstd",
                                      compression_level=9, use_dictionary=False,
                                      column_encoding="DELTA_BINARY_PACKED", data_page_version="2.0")
            first = b.slice(0, 1).to_pylist()[0]
        writer.write_batch(enc)
        n_rows += b.num_rows
        last = b.slice(b.num_rows - 1, 1).to_pylist()[0]
    if writer is None:
        raise ValueError(f"{src.name} is empty")
    writer.close()

    # verify: re-read, compare count and boundary rows (decoded back to floats)
    pf = pq.ParquetFile(tmp)
    if pf.metadata.num_rows != n_rows:
        tmp.unlink(); raise ValueError(f"row count mismatch {pf.metadata.num_rows} != {n_rows}")
    head = decode(pf.read_row_group(0)).slice(0, 1).to_pylist()[0]
    tail_rg = pf.read_row_group(pf.metadata.num_row_groups - 1)
    tail = decode(tail_rg).slice(tail_rg.num_rows - 1, 1).to_pylist()[0]
    for name, got, want in (("first", head, first), ("last", tail, last)):
        for c in FLOAT_COLS + ["transaction_time", "event_time"]:
            if got[c] != want[c]:
                tmp.unlink(); raise ValueError(f"{name} row mismatch on {c}: {got[c]} != {want[c]}")

    os.replace(tmp, dst)
    log.info("%s -> %s  %d rows  %.0f MB -> %.0f MB", src.name, dst.name, n_rows,
             src.stat().st_size / 1e6, dst.stat().st_size / 1e6)
    if not keep_source:
        src.unlink()
    return dst


def decode(tbl: pa.Table) -> pa.Table:
    """Parquet (int-scaled) -> archive layout with float prices/quantities."""
    meta = tbl.schema.metadata or {}
    cols = []
    for name in tbl.schema.names:
        col = tbl.column(name)
        k = meta.get(f"scale:{name}".encode())
        if k is not None:
            col = pc.divide(pc.cast(col, pa.float64()), float(10 ** int(k)))
        cols.append(col)
    return pa.table(cols, names=tbl.schema.names)


def closed_files(raw_dir: Path = RAW_DIR) -> list[Path]:
    """Recorder .csv.gz files for days strictly before today (UTC) - nothing is writing to them."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out = []
    for p in sorted(raw_dir.glob("*-bookTicker-*.csv.gz")):
        m = FILE_RE.match(p.name)
        if m and m["day"] < today:
            out.append(p)
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Compact closed recorder days to Parquet.")
    ap.add_argument("files", nargs="*", type=Path, help="specific .csv.gz files (default: all closed days)")
    ap.add_argument("--raw-dir", type=Path, default=RAW_DIR)
    ap.add_argument("--keep-source", action="store_true")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    files = a.files or closed_files(a.raw_dir)
    if not files:
        log.info("nothing to compact")
    for f in files:
        try:
            compact_file(f, a.keep_source)
        except Exception as exc:          # keep going; the source is untouched on failure
            log.error("%s: %s", f.name, exc)
