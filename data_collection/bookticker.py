"""
Historical L1 (best bid/ask) for Binance USDⓈ-M futures from data.binance.vision.

Coverage (BTCUSDT): daily archives 2023-05-16 -> 2024-03-30; the 2024-04 monthly file is a
fragment (first 7h of April 1). Nothing after, nothing for spot. ~350 MB / ~39M rows per day.

For later days `fetch_day` falls back to files produced by the other collectors in this
package, so `sample_book` works over any day for which *some* source exists:

    data/raw/<SYMBOL>-bookTicker-<day>.zip          Binance archive
    data/raw/<SYMBOL>-bookTicker-<day>.csv.gz       record_bookticker (live recorder)
    data/raw/tardis/binance-futures_book_ticker_<day>_<SYMBOL>.csv.gz   tardis_free_days

Two public functions:

    load_bookticker(symbol, day)              -> full raw DataFrame for one UTC day
    sample_book(symbol, start, end, step_ms)  -> book state on a fixed grid, streamed
                                                 (never holds a whole day in memory)
"""
from __future__ import annotations

import concurrent.futures as cf
import gzip
import hashlib
import time
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pcmp
import pyarrow.csv as pc
import requests

VISION_BASE = "https://data.binance.vision/data/futures/um"
RAW_DIR     = Path("data/raw")
TARDIS_DIR  = RAW_DIR / "tardis"
VERIFY_SHA  = True
MAX_DL_WORKERS = 3          # files are big; don't hammer it

SESSION = requests.Session()
SESSION.headers["User-Agent"] = "binance-bookticker/1.0"

RAW_COLS = ["update_id", "best_bid_price", "best_bid_qty", "best_ask_price",
            "best_ask_qty", "transaction_time", "event_time"]
STATE_COLS = ["update_id", "best_bid_price", "best_bid_qty", "best_ask_price", "best_ask_qty"]


# --------------------------------------------------------------------------- download

def _archive_url(symbol: str, stamp: str, monthly: bool) -> str:
    kind = "monthly" if monthly else "daily"
    return f"{VISION_BASE}/{kind}/bookTicker/{symbol}/{symbol}-bookTicker-{stamp}.zip"


def download_archive(symbol: str, stamp: str, monthly: bool = False) -> Path | None:
    """Fetch one archive into RAW_DIR. Returns None on 404 (Binance has no such file)."""
    url  = _archive_url(symbol, stamp, monthly)
    dest = RAW_DIR / Path(url).name
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    for attempt in range(5):
        try:
            r = SESSION.get(url, timeout=300, stream=True)
            if r.status_code == 404:
                return None
            r.raise_for_status()
            tmp, sha = dest.with_suffix(".part"), hashlib.sha256()
            with tmp.open("wb") as fh:
                for chunk in r.iter_content(4 << 20):
                    fh.write(chunk)
                    sha.update(chunk)
            break
        except requests.RequestException:
            if attempt == 4:
                raise
            time.sleep(5 * (attempt + 1))

    if VERIFY_SHA:
        c = SESSION.get(url + ".CHECKSUM", timeout=30)
        if c.ok and c.text.split()[0] != sha.hexdigest():
            tmp.unlink(missing_ok=True)
            raise ValueError(f"checksum mismatch for {dest.name}")
    tmp.rename(dest)
    return dest


def fetch_day(symbol: str, day: date) -> tuple[Path, bool] | None:
    """Locate the archive covering `day`: daily first, monthly fallback. Returns (path, is_monthly)."""
    p = download_archive(symbol, day.strftime("%Y-%m-%d"), monthly=False)
    if p is not None:
        return p, False
    p = download_archive(symbol, day.strftime("%Y-%m"), monthly=True)
    if p is not None:
        return p, True
    for local in (RAW_DIR / f"{symbol}-bookTicker-{day:%Y-%m-%d}.csv.gz",
                  TARDIS_DIR / f"binance-futures_book_ticker_{day:%Y-%m-%d}_{symbol}.csv.gz"):
        if local.exists() and local.stat().st_size > 0:
            return local, False
    return None


def prefetch(symbol: str, start: date, end: date) -> None:
    """Download every daily archive in [start, end) in parallel (idempotent)."""
    days = [start + timedelta(days=i) for i in range((end - start).days)]
    with cf.ThreadPoolExecutor(MAX_DL_WORKERS) as ex:
        for d, res in zip(days, ex.map(lambda d: fetch_day(symbol, d), days)):
            print(f"{d}  {'missing' if res is None else res[0].name}")


# --------------------------------------------------------------------------- streaming read

def _tardis_to_archive(b: pa.RecordBatch) -> pa.RecordBatch:
    """Tardis book_ticker/quotes columns -> archive layout. Tardis keeps only the exchange
    event time (µs), so transaction_time is set equal to event_time and update_id is null."""
    ev = pcmp.cast(pcmp.divide(b.column("timestamp"), 1000), pa.int64())
    return pa.RecordBatch.from_arrays(
        [pa.nulls(b.num_rows, pa.int64()), b.column("bid_price"), b.column("bid_amount"),
         b.column("ask_price"), b.column("ask_amount"), ev, ev,
         pcmp.cast(b.column("local_timestamp"), pa.int64())],
        names=RAW_COLS + ["local_time"])


def iter_batches(path: Path, block_size: int = 64 << 20) -> Iterator[pa.RecordBatch]:
    """Stream a bookTicker CSV (.zip archive, or .csv.gz from the recorder / Tardis) as Arrow
    record batches (~1M rows each), always in the archive column layout."""
    tardis = path.name.startswith("binance-futures_")
    if path.suffix == ".zip":
        z = zipfile.ZipFile(path)
        fh = z.open(z.namelist()[0])
    else:
        fh = gzip.open(path, "rb")
    with fh:
        rdr = pc.open_csv(fh, read_options=pc.ReadOptions(block_size=block_size))
        for b in rdr:
            if b.num_rows:
                yield _tardis_to_archive(b) if tardis else b


def iter_day(symbol: str, day: date) -> Iterator[pa.RecordBatch]:
    """Batches for one UTC day, whichever archive it lives in."""
    found = fetch_day(symbol, day)
    if found is None:
        return
    path, monthly = found
    if not monthly:
        yield from iter_batches(path)
        return
    # Monthly archive: filter rows to the day (transaction_time is monotonic so we can stop early).
    d0 = int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp() * 1000)
    d1 = d0 + 86_400_000
    for b in iter_batches(path):
        t = b.column("transaction_time").to_numpy()
        if t[-1] < d0:
            continue
        if t[0] >= d1:
            break
        lo, hi = np.searchsorted(t, [d0, d1])
        if hi > lo:
            yield b.slice(lo, hi - lo)


def load_bookticker(symbol: str, day: date) -> pd.DataFrame:
    """Full raw L1 stream for one day (~39M rows, ~2 GB for BTCUSDT). Prefer sample_book."""
    tbl = pa.Table.from_batches(list(iter_day(symbol, day)))
    df = tbl.to_pandas()
    df["ts"] = pd.to_datetime(df["transaction_time"], unit="ms", utc=True)
    return df


# --------------------------------------------------------------------------- grid sampling

def _last_row(b: pa.RecordBatch) -> dict:
    return {c: (np.nan if (v := b.column(c)[-1].as_py()) is None else v) for c in STATE_COLS}


def _to_ms(t: datetime) -> int:
    return int(t.astimezone(timezone.utc).timestamp() * 1000)


def sample_book(symbol: str, start: datetime, end: datetime, step_ms: int,
                ts_col: str = "transaction_time", derived: bool = True) -> pd.DataFrame:
    """
    Book state (best bid/ask) sampled every `step_ms` on the grid start, start+step, ... < end.

    Each grid point t gets the LAST update with ts_col <= t (as-of / previous-tick sampling),
    i.e. exactly what the top of book looked like at that instant. Also returns n_updates:
    number of L1 updates in (t - step_ms, t], a cheap activity feature.

    ts_col: "transaction_time" (matching engine, default) or "event_time" (when Binance
            published it; use this if you want "what a subscriber could have known").

    Runs in constant memory: streams daily archives batch-by-batch. State carries across
    day boundaries. The first few grid points may be NaN if no update precedes them
    (only when start is at the very beginning of available history).
    """
    g0, g1 = _to_ms(start), _to_ms(end)
    grid = np.arange(g0, g1, step_ms, dtype=np.int64)
    n = len(grid)

    out = {c: np.full(n, np.nan) for c in STATE_COLS}
    cum = np.zeros(n, dtype=np.int64)          # cumulative update count up to each grid point
    last = None                                # carried state: dict col -> scalar
    offset = 0                                 # rows seen before current batch
    gi = 0                                     # next grid index to emit

    def emit(lo: int, hi: int, idx: np.ndarray, b_np: dict) -> None:
        """Fill grid[lo:hi] from batch rows idx (idx == -1 -> carried `last`)."""
        for c in STATE_COLS:
            vals = np.where(idx >= 0, b_np[c][np.maximum(idx, 0)], np.nan)
            if last is not None:
                vals = np.where(idx >= 0, vals, last[c])
            out[c][lo:hi] = vals

    day = start.astimezone(timezone.utc).date()
    last_day = (end.astimezone(timezone.utc) - timedelta(milliseconds=1)).date()
    while day <= last_day and gi < n:
        for b in iter_day(symbol, day):
            t = b.column(ts_col).to_numpy()
            if t[-1] < grid[gi]:
                offset += len(t)
                last = _last_row(b)
                continue
            hi = gi + int(np.searchsorted(grid[gi:], t[-1], side="right"))   # grid points <= t[-1]
            if hi > gi:
                b_np = {c: b.column(c).to_numpy(zero_copy_only=False) for c in STATE_COLS}
                idx = np.searchsorted(t, grid[gi:hi], side="right") - 1
                emit(gi, hi, idx, b_np)
                cum[gi:hi] = offset + idx + 1
                gi = hi
            offset += len(t)
            last = _last_row(b)
            if gi >= n:
                break
        day += timedelta(days=1)

    if gi < n and last is not None:          # tail past the final update: carry state
        for c in STATE_COLS:
            out[c][gi:] = last[c]
        cum[gi:] = offset

    df = pd.DataFrame(out, index=pd.to_datetime(grid, unit="ms", utc=True).rename("ts"))
    df["update_id"] = df["update_id"].astype("Int64")
    df["n_updates"] = np.diff(cum, prepend=cum[0])
    df = df.rename(columns={"best_bid_price": "bid", "best_bid_qty": "bid_qty",
                            "best_ask_price": "ask", "best_ask_qty": "ask_qty"})
    if derived:
        df["mid"]        = (df["bid"] + df["ask"]) / 2
        df["spread"]     = df["ask"] - df["bid"]
        df["microprice"] = (df["bid"] * df["ask_qty"] + df["ask"] * df["bid_qty"]) / (df["bid_qty"] + df["ask_qty"])
        df["imbalance"]  = (df["bid_qty"] - df["ask_qty"]) / (df["bid_qty"] + df["ask_qty"])
    return df


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Sample Binance futures L1 onto a fixed ms grid.")
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--start", required=True, help="UTC, e.g. 2024-03-01 or 2024-03-01T12:00")
    ap.add_argument("--end",   required=True)
    ap.add_argument("--step-ms", type=int, default=1000)
    ap.add_argument("--ts-col", default="transaction_time", choices=["transaction_time", "event_time"])
    ap.add_argument("--out", default=None, help="parquet path (default data/parquet/<auto>.parquet)")
    a = ap.parse_args()
    s = datetime.fromisoformat(a.start).replace(tzinfo=timezone.utc)
    e = datetime.fromisoformat(a.end).replace(tzinfo=timezone.utc)
    df = sample_book(a.symbol, s, e, a.step_ms, a.ts_col)
    out = Path(a.out) if a.out else Path("data/parquet") / f"{a.symbol}_L1_{a.step_ms}ms_{s:%Y%m%dT%H%M}_{e:%Y%m%dT%H%M}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, compression="zstd")
    print(f"{len(df):,} rows -> {out}")
    print(df.head())
