"""
One place to load anything this repo has collected, with the source as an argument.

    from data_collection.load import load_l1, load_l1_raw, load_klines, load_rtds, inventory

    load_l1("BTCUSDT", start, end, step_ms=1000, source="auto")   # book state on a grid
    load_l1_raw("BTCUSDT", day, source="tardis")                  # every L1 update of one day
    load_klines("BTCUSDT", start, end, interval="1s", market="spot")
    load_rtds(start, end, topic="crypto_prices_twap_sixty")       # Polymarket/Chainlink feed
    inventory("BTCUSDT")                                          # which days exist per source

L1 sources (see docs/data/index.md for the fallback chain):
    archive   Binance bookTicker archive (native tick feed, 2023-05-16 -> 2024-03-30)
    recorder  our VPS recorder (native tick feed, 2026-09-12 ->)
    tardis    Tardis.dev free first-of-month days (native tick feed, 2019-11 ->)
    hft       CryptoHFTData L2 replayed to L1 (26 ms events, 2025-06-28 ->)
    auto      best available per day, in that order
"""
from __future__ import annotations

import glob
import gzip
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from . import bookticker as bt

PARQUET_DIR = Path("data/parquet")
RTDS_DIR = Path("data/raw/rtds")


# --------------------------------------------------------------------------- L1

def load_l1(symbol: str, start: datetime, end: datetime, step_ms: int = 1000,
            source: str = "auto", ts_col: str = "transaction_time", derived: bool = True) -> pd.DataFrame:
    """Best bid/ask (and mid, spread, microprice, imbalance, n_updates) as-of every `step_ms`.
    `source` is "auto" or one of bookticker.SOURCES. Wraps `bookticker.sample_book`."""
    return bt.sample_book(symbol, start, end, step_ms, ts_col=ts_col, derived=derived, source=source)


def load_l1_raw(symbol: str, day: date, source: str = "auto") -> pd.DataFrame:
    """Every L1 update of one UTC day in the archive column layout (+ `ts`). ~39M rows / ~2 GB for
    a Binance archive day; Tardis ~9M; hft ~2M. Use load_l1 unless you need every tick."""
    return bt.load_bookticker(symbol, day, source)


def l1_source_for(symbol: str, day: date, source: str = "auto") -> str | None:
    """Which source `auto` would use for this day (or None if nothing is on disk / downloadable)."""
    found = bt.locate(symbol, day, source, download=False)
    return None if found is None else found[2]


# --------------------------------------------------------------------------- klines

def load_klines(symbol: str, start: datetime, end: datetime, interval: str = "1s",
                market: str = "spot", fetch_missing: bool = False) -> pd.DataFrame:
    """Candles for [start, end) from the monthly parquets written by the klines backfill
    (data/parquet/{symbol}_{market}_{interval}_{YYYY-MM}.parquet). With fetch_missing=True,
    months not on disk are pulled via klines.get_klines (archive + REST) and saved."""
    start = start.astimezone(timezone.utc); end = end.astimezone(timezone.utc)
    frames, cur = [], start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while cur < end:
        nxt = (cur + timedelta(days=32)).replace(day=1)
        path = PARQUET_DIR / f"{symbol}_{market}_{interval}_{cur:%Y-%m}.parquet"
        if path.exists():
            frames.append(pd.read_parquet(path))
        elif fetch_missing:
            from .klines import get_klines
            df = get_klines(symbol, cur, min(nxt, end), interval, market, verbose=False)
            PARQUET_DIR.mkdir(parents=True, exist_ok=True)
            df.to_parquet(path, compression="zstd")
            frames.append(df)
        cur = nxt
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames)
    return df[(df.index >= start) & (df.index < end)]


# --------------------------------------------------------------------------- Polymarket RTDS

def load_rtds(start: datetime, end: datetime, topic: str | None = None) -> pd.DataFrame:
    """Rows recorded by record_rtds (topic, ts_recv_ms, ts_msg_ms, ts_payload_ms, symbol, value,
    full_accuracy_value) for [start, end), optionally one topic, indexed by payload time."""
    start = start.astimezone(timezone.utc); end = end.astimezone(timezone.utc)
    frames = []
    for d in pd.date_range(start.date(), (end - timedelta(microseconds=1)).date(), freq="D"):
        path = RTDS_DIR / f"rtds-btc-{d:%Y-%m-%d}.csv.gz"
        if path.exists():
            frames.append(pd.read_csv(path, compression="gzip"))
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    if topic:
        df = df[df.topic == topic]
    df["ts"] = pd.to_datetime(df.ts_payload_ms, unit="ms", utc=True)
    df = df.set_index("ts").sort_index()
    return df[(df.index >= start) & (df.index < end)]


# --------------------------------------------------------------------------- inventory

_DAY_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


def inventory(symbol: str = "BTCUSDT") -> pd.DataFrame:
    """One row per day, one column per L1 source with the file size in MB (NaN = absent), plus
    `auto` = which source `load_l1` would pick, and `klines_1s`/`rtds` flags. Files only - it does
    not ask Binance what could still be downloaded."""
    rows: dict[str, dict] = {}

    def put(day: str, col: str, path: Path) -> None:
        rows.setdefault(day, {})[col] = round(path.stat().st_size / 1e6, 1)

    for p in bt.RAW_DIR.glob(f"{symbol}-bookTicker-????-??-??.parquet"):
        put(_DAY_RE.search(p.name)[1], bt._parquet_source(p), p)
    for p in bt.RAW_DIR.glob(f"{symbol}-bookTicker-????-??-??.zip"):
        put(_DAY_RE.search(p.name)[1], "archive", p)
    for p in bt.RAW_DIR.glob(f"{symbol}-bookTicker-????-??-??.csv.gz"):
        put(_DAY_RE.search(p.name)[1], "recorder", p)
    for p in bt.TARDIS_DIR.glob(f"binance-futures_book_ticker_*_{symbol}.csv.gz"):
        put(_DAY_RE.search(p.name)[1], "tardis", p)
    for p in (bt.RAW_DIR / "hft").glob(f"{symbol}-bookTicker-????-??-??.parquet"):
        put(_DAY_RE.search(p.name)[1], "hft", p)
    for p in RTDS_DIR.glob("rtds-btc-????-??-??.csv.gz"):
        put(_DAY_RE.search(p.name)[1], "rtds", p)

    df = pd.DataFrame.from_dict(rows, orient="index").sort_index()
    df.index = pd.to_datetime(df.index).rename("day")
    for c in list(bt.SOURCES) + ["rtds"]:
        if c not in df:
            df[c] = float("nan")
    months = {Path(p).stem.rsplit("_", 1)[1] for p in glob.glob(str(PARQUET_DIR / f"{symbol}_spot_1s_????-??.parquet"))}
    df["klines_1s"] = [d.strftime("%Y-%m") in months for d in df.index]
    df["auto"] = [l1_source_for(symbol, d.date()) for d in df.index]
    return df[["auto"] + list(bt.SOURCES) + ["rtds", "klines_1s"]]


def summary(symbol: str = "BTCUSDT") -> pd.DataFrame:
    """Days, first/last day and total MB per L1 source."""
    inv = inventory(symbol)
    out = []
    for c in list(bt.SOURCES) + ["rtds"]:
        have = inv[c].dropna()
        out.append({"source": c, "days": len(have), "first": have.index.min().date() if len(have) else None,
                    "last": have.index.max().date() if len(have) else None, "mb": round(have.sum())})
    return pd.DataFrame(out).set_index("source")


if __name__ == "__main__":
    pd.set_option("display.width", 160)
    print(summary())
