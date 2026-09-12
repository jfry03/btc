"""
Tardis.dev publishes the first day of every month free, no API key. For binance-futures the
`book_ticker` dataset is the native Binance bookTicker stream (2019-11-17 -> present), which
covers the gap after Binance's own archives stop (2024-03-30). ~60 MB gz / ~8-9M rows per day.

    python -m data_collection.tardis_free_days --start 2024-04 --end 2026-10          # every 1st
    python -m data_collection.tardis_free_days --start 2026-08 --end 2026-09 --data-type quotes

Files land in data/raw/tardis/ under Tardis' own naming
(binance-futures_book_ticker_2026-08-01_BTCUSDT.csv.gz), which `bookticker.fetch_day` looks
for, so `sample_book` works on these days unchanged. Columns are
exchange,symbol,timestamp,local_timestamp,ask_amount,ask_price,bid_price,bid_amount with
timestamps in µs; `bookticker.iter_batches` maps them onto the archive layout.

`quotes` is the top of book derived from the throttled depth stream (~1M rows/day) - much
coarser; prefer `book_ticker`. Other days need a paid plan (from $350/mo).
"""
from __future__ import annotations

import hashlib
from datetime import date, timedelta
from pathlib import Path

import requests

TARDIS_DIR = Path("data/raw/tardis")
EXCHANGE   = "binance-futures"
SESSION    = requests.Session()
SESSION.headers["User-Agent"] = "binance-tardis-free/1.0"


def url_for(day: date, symbol: str = "BTCUSDT", data_type: str = "book_ticker", exchange: str = EXCHANGE) -> str:
    return f"https://datasets.tardis.dev/v1/{exchange}/{data_type}/{day:%Y/%m/%d}/{symbol}.csv.gz"


def download_day(day: date, symbol: str = "BTCUSDT", data_type: str = "book_ticker",
                 exchange: str = EXCHANGE) -> Path | None:
    """Fetch one free day (must be the 1st of a month). Returns None if Tardis has no file.
    Verifies the x-md5 header. Plain GET only: HEAD returns 404 and Range requests 403."""
    dest = TARDIS_DIR / f"{exchange}_{data_type}_{day:%Y-%m-%d}_{symbol}.csv.gz"
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    TARDIS_DIR.mkdir(parents=True, exist_ok=True)

    r = SESSION.get(url_for(day, symbol, data_type, exchange), timeout=600, stream=True)
    if r.status_code in (401, 403, 404):
        return None
    r.raise_for_status()
    tmp, md5 = dest.with_suffix(".part"), hashlib.md5()
    with tmp.open("wb") as fh:
        for chunk in r.iter_content(4 << 20):
            fh.write(chunk)
            md5.update(chunk)
    expected = r.headers.get("x-md5", "").strip('"')
    if expected and expected != md5.hexdigest():
        tmp.unlink(missing_ok=True)
        raise ValueError(f"md5 mismatch for {dest.name}")
    tmp.rename(dest)
    return dest


def free_days(start: date, end: date) -> list[date]:
    """Every 1st-of-month in [start, end)."""
    days, cur = [], date(start.year, start.month, 1)
    if cur < start:
        cur = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
    while cur < end:
        days.append(cur)
        cur = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
    return days


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Download Tardis.dev free first-of-month days.")
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--data-type", default="book_ticker", choices=["book_ticker", "quotes", "trades", "book_snapshot_5", "incremental_book_L2"])
    ap.add_argument("--start", required=True, help="YYYY-MM (inclusive)")
    ap.add_argument("--end",   required=True, help="YYYY-MM (exclusive)")
    a = ap.parse_args()
    s = date.fromisoformat(a.start + "-01"); e = date.fromisoformat(a.end + "-01")
    for d in free_days(s, e):
        p = download_day(d, a.symbol, a.data_type)
        print(f"{d}  {'missing' if p is None else f'{p.name}  {p.stat().st_size/1e6:.1f} MB'}")
