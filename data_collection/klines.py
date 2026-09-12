"""
OHLCV candles for Binance spot or USDⓈ-M futures: archive (data.binance.vision) for finished
days/months, REST for the tail that has no archive yet. Extracted from binance_1s.ipynb.

    python -m data_collection.klines --symbol BTCUSDT --interval 1s --start 2026-09-01 --end 2026-09-04
    python -m data_collection.klines --market um --interval 1m --start 2026-09-01 --end 2026-09-04

Columns: open_time, open, high, low, close, volume, close_time, quote_volume, trades,
taker_buy_base, taker_buy_quote — all derived from executed trades (no book information).
Seconds with no trades have no row; use `to_regular_grid` for a strict grid.
Spot has a 1s interval; futures' finest is 1m.
"""
from __future__ import annotations

import concurrent.futures as cf
import hashlib
import io
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

RAW_DIR    = Path("data/raw")
OUT_DIR    = Path("data/parquet")
VERIFY_SHA = True
MAX_DL_WORKERS = 4

MARKETS = {  # archive prefix, REST base, REST path
    "spot": ("https://data.binance.vision/data/spot", "https://api.binance.com", "/api/v3/klines"),
    "um":   ("https://data.binance.vision/data/futures/um", "https://fapi.binance.com", "/fapi/v1/klines"),
}
INTERVAL_MS = {"1s": 1_000, "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000,
               "30m": 1_800_000, "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000}
COLUMNS = ["open_time", "open", "high", "low", "close", "volume", "close_time",
           "quote_volume", "trades", "taker_buy_base", "taker_buy_quote", "ignore"]
FLOAT_COLS = ("open", "high", "low", "close", "volume", "quote_volume", "taker_buy_base", "taker_buy_quote")

SESSION = requests.Session()
SESSION.headers["User-Agent"] = "binance-klines/1.0"


# --------------------------------------------------------------------------- archive

def _archive_url(market: str, symbol: str, interval: str, stamp: str, monthly: bool) -> str:
    base = MARKETS[market][0]
    kind = "monthly" if monthly else "daily"
    return f"{base}/{kind}/klines/{symbol}/{interval}/{symbol}-{interval}-{stamp}.zip"


def download_archive(market: str, symbol: str, interval: str, stamp: str, monthly: bool) -> Path | None:
    """Fetch one archive into RAW_DIR. Returns None if Binance has no such file (404)."""
    url = _archive_url(market, symbol, interval, stamp, monthly)
    dest = RAW_DIR / Path(url).name
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    r = SESSION.get(url, timeout=120, stream=True)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    tmp, sha = dest.with_suffix(".part"), hashlib.sha256()
    with tmp.open("wb") as fh:
        for chunk in r.iter_content(1 << 20):
            fh.write(chunk)
            sha.update(chunk)
    if VERIFY_SHA:
        c = SESSION.get(url + ".CHECKSUM", timeout=30)
        if c.ok and c.text.split()[0] != sha.hexdigest():
            tmp.unlink(missing_ok=True)
            raise ValueError(f"checksum mismatch for {dest.name}")
    tmp.rename(dest)
    return dest


def read_archive(path: Path) -> pd.DataFrame:
    """Parse a klines ZIP into a DataFrame, normalising the timestamp unit."""
    with zipfile.ZipFile(path) as z:
        raw = z.read(z.namelist()[0])
    first = raw.split(b"\n", 1)[0]
    has_header = not first.split(b",")[0].strip().isdigit()
    df = pd.read_csv(io.BytesIO(raw), header=0 if has_header else None, names=COLUMNS,
                     dtype={c: "float64" for c in FLOAT_COLS})
    return _normalise(df)


def _normalise(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce open/close_time to int64 milliseconds and drop the padding column."""
    for col in ("open_time", "close_time"):
        t = df[col].astype("int64")
        # >1e15 means microseconds (post-2025 archives); REST always returns ms.
        df[col] = (t // 1000).where(t > 10**15, t)
    df["trades"] = df["trades"].astype("int64")
    return df.drop(columns=["ignore"])


# --------------------------------------------------------------------------- REST tail

def fetch_api_klines(market: str, symbol: str, start_ms: int, end_ms: int, interval: str) -> pd.DataFrame:
    """Page the klines REST endpoint over [start_ms, end_ms). Public, no key."""
    _, api_base, path = MARKETS[market]
    step = INTERVAL_MS[interval]
    rows: list[list] = []
    cursor = start_ms
    while cursor < end_ms:
        resp = SESSION.get(api_base + path, timeout=30,
                           params={"symbol": symbol, "interval": interval, "startTime": cursor,
                                   "endTime": end_ms - 1, "limit": 1000})
        if resp.status_code in (418, 429):
            time.sleep(int(resp.headers.get("Retry-After", 10)))
            continue
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        rows.extend(batch)
        cursor = batch[-1][0] + step
        if int(resp.headers.get("x-mbx-used-weight-1m", 0)) > 4000:   # limit 6000/min (spot)
            time.sleep(10)
    if not rows:
        return pd.DataFrame(columns=[c for c in COLUMNS if c != "ignore"])
    return _normalise(pd.DataFrame(rows, columns=COLUMNS).astype({c: "float64" for c in FLOAT_COLS}))


# --------------------------------------------------------------------------- assembly

def _plan(start: datetime, end: datetime) -> list[tuple[str, bool]]:
    """Split [start, end) into (stamp, is_monthly) archive chunks."""
    chunks, cur, last = [], start.date(), (end - timedelta(milliseconds=1)).date()
    while cur <= last:
        nxt_month = (cur.replace(day=1) + timedelta(days=32)).replace(day=1)
        if cur.day == 1 and nxt_month <= last + timedelta(days=1):
            chunks.append((cur.strftime("%Y-%m"), True))
            cur = nxt_month
        else:
            chunks.append((cur.strftime("%Y-%m-%d"), False))
            cur += timedelta(days=1)
    return chunks


def _load_chunk(market: str, symbol: str, interval: str, stamp: str, monthly: bool) -> pd.DataFrame:
    path = download_archive(market, symbol, interval, stamp, monthly)
    if path is not None:
        return read_archive(path)
    # No archive yet (recent days) -> REST fallback for that window.
    if monthly:
        d0 = datetime.strptime(stamp, "%Y-%m").replace(tzinfo=timezone.utc)
        d1 = (d0 + timedelta(days=32)).replace(day=1)
    else:
        d0 = datetime.strptime(stamp, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        d1 = d0 + timedelta(days=1)
    now_ms = int(time.time() * 1000)
    return fetch_api_klines(market, symbol, int(d0.timestamp() * 1000),
                            min(int(d1.timestamp() * 1000), now_ms), interval)


def get_klines(symbol: str, start: datetime, end: datetime, interval: str = "1s",
               market: str = "spot", verbose: bool = True) -> pd.DataFrame:
    """Candles for [start, end) indexed by UTC open time. Archives where they exist, REST otherwise."""
    plan = _plan(start, end)
    if verbose:
        print(f"{len(plan)} chunk(s): " + ", ".join(s for s, _ in plan[:8]) + (" ..." if len(plan) > 8 else ""))
    with cf.ThreadPoolExecutor(MAX_DL_WORKERS) as pool:
        frames = list(pool.map(lambda c: _load_chunk(market, symbol, interval, *c), plan))
    df = pd.concat([f for f in frames if len(f)], ignore_index=True)
    lo, hi = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
    df = df[(df.open_time >= lo) & (df.open_time < hi)]
    df = df.drop_duplicates("open_time").sort_values("open_time", ignore_index=True)
    df.insert(0, "ts", pd.to_datetime(df.open_time, unit="ms", utc=True))
    return df.set_index("ts")


def to_regular_grid(df: pd.DataFrame, interval: str = "1s") -> pd.DataFrame:
    """Reindex onto a strict grid: carry the last close, zero the flow columns."""
    grid = pd.date_range(df.index[0], df.index[-1], freq=interval, tz="UTC", name="ts")
    out = df.reindex(grid)
    out["close"] = out["close"].ffill()
    for c in ("open", "high", "low"):
        out[c] = out[c].fillna(out["close"])
    for c in ("volume", "quote_volume", "trades", "taker_buy_base", "taker_buy_quote"):
        out[c] = out[c].fillna(0)
    out["open_time"] = out.index.view("int64") // 10**6
    out["close_time"] = out["open_time"] + INTERVAL_MS[interval] - 1
    return out


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Pull Binance klines (archive + REST tail) to parquet.")
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--market", default="spot", choices=list(MARKETS))
    ap.add_argument("--interval", default="1s", choices=list(INTERVAL_MS))
    ap.add_argument("--start", required=True, help="UTC, e.g. 2026-09-01 or 2026-09-01T12:00")
    ap.add_argument("--end", required=True)
    ap.add_argument("--out", default=None)
    a = ap.parse_args()
    s = datetime.fromisoformat(a.start).replace(tzinfo=timezone.utc)
    e = datetime.fromisoformat(a.end).replace(tzinfo=timezone.utc)
    df = get_klines(a.symbol, s, e, a.interval, a.market)
    out = Path(a.out) if a.out else OUT_DIR / f"{a.symbol}_{a.market}_{a.interval}_{s:%Y%m%dT%H%M}_{e:%Y%m%dT%H%M}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, compression="zstd")
    print(f"{len(df):,} rows  {df.index[0]} -> {df.index[-1]}  ->  {out}")
