"""
Compact a closed day of recorder output (.csv.gz) or a Binance bookTicker archive (.zip) into a
much smaller Parquet file.

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
FILE_RE = re.compile(r"^(?P<symbol>[A-Z0-9_]+)-bookTicker-(?P<day>\d{4}-\d{2}-\d{2})\.(csv\.gz|zip)$")

log = logging.getLogger("compact")


def _batches(path: Path):
    """Recorder .csv.gz or Binance archive .zip, streamed."""
    from .bookticker import iter_batches
    yield from iter_batches(path, block_size=BATCH_BYTES)


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
    dst = src.with_name(FILE_RE.match(src.name).expand(r"\g<symbol>-bookTicker-\g<day>.parquet")
                        if FILE_RE.match(src.name) else src.name.replace(".csv.gz", ".parquet"))
    tmp = dst.with_suffix(".parquet.part")
    scales = find_scales(src)
    log.info("%s scales=%s", src.name, scales)

    n_rows, first, last = 0, None, None
    writer = None
    ordered, prev_t = True, None
    for b in _batches(src):
        enc = _encode(b, scales)
        t = enc.column("transaction_time").to_numpy()
        if (prev_t is not None and t[0] < prev_t) or (len(t) > 1 and (t[1:] < t[:-1]).any()):
            ordered = False
        prev_t = t[-1]
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

    if not ordered:
        # Some Binance archive days (e.g. 2023-11-21 .. 2024-01-02) interleave two time-ordered
        # streams, so rows are not in time order. sample_book's one-pass as-of logic needs
        # monotonic time, so sort by update_id (the engine's sequence) and rewrite.
        log.warning("%s: rows not in time order - sorting by update_id", src.name)
        first, last = _sort_parquet(tmp)

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


def _sort_parquet(path: Path) -> tuple[dict, dict]:
    """Sort a compacted file by update_id in place (needs ~60 bytes/row of RAM: ~2.5 GB for a
    40M-row day). Returns decoded (first, last) rows for the verification step."""
    tbl = pq.read_table(path)
    meta = tbl.schema.metadata
    tbl = tbl.sort_by("update_id").replace_schema_metadata(meta)
    tmp2 = path.with_suffix(".sorted")
    pq.write_table(tbl, tmp2, compression="zstd", compression_level=9, use_dictionary=False,
                   column_encoding="DELTA_BINARY_PACKED", data_page_version="2.0")
    os.replace(tmp2, path)
    dec = decode(tbl)
    return dec.slice(0, 1).to_pylist()[0], dec.slice(tbl.num_rows - 1, 1).to_pylist()[0]


def is_time_ordered(path: Path) -> bool:
    prev = None
    for b in pq.ParquetFile(path).iter_batches(batch_size=1_000_000, columns=["transaction_time"]):
        t = b.column(0).to_numpy()
        if (prev is not None and t[0] < prev) or (len(t) > 1 and (t[1:] < t[:-1]).any()):
            return False
        prev = t[-1]
    return True


def resort(files: list[Path]) -> None:
    """Fix already-compacted files whose rows are out of time order."""
    for f in files:
        if is_time_ordered(f):
            log.info("%s: already ordered", f.name); continue
        n = pq.read_metadata(f).num_rows
        log.warning("%s: not time-ordered - sorting %d rows by update_id", f.name, n)
        _sort_parquet(f)
        assert is_time_ordered(f) and pq.read_metadata(f).num_rows == n
        log.info("%s: sorted", f.name)


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
    for p in sorted(list(raw_dir.glob("*-bookTicker-*.csv.gz")) + list(raw_dir.glob("*-bookTicker-????-??-??.zip"))):
        m = FILE_RE.match(p.name)
        if m and m["day"] < today:
            out.append(p)
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Compact closed recorder days to Parquet.")
    ap.add_argument("files", nargs="*", type=Path, help="specific .csv.gz files (default: all closed days)")
    ap.add_argument("--raw-dir", type=Path, default=RAW_DIR)
    ap.add_argument("--keep-source", action="store_true")
    ap.add_argument("--resort", action="store_true", help="sort the given (or all) compacted .parquet files by update_id if out of time order")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if a.resort:
        resort(a.files or sorted(a.raw_dir.glob("*-bookTicker-????-??-??.parquet")))
        raise SystemExit(0)
    files = a.files or closed_files(a.raw_dir)
    if not files:
        log.info("nothing to compact")
    for f in files:
        try:
            compact_file(f, a.keep_source)
        except Exception as exc:          # keep going; the source is untouched on failure
            log.error("%s: %s", f.name, exc)
