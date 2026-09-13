"""
Backfill Binance futures L1 history: download each day's bookTicker archive, compact it to
Parquet, delete the zip. Resumable (skips days that already have a .parquet); downloads run
one day ahead of compaction so the network and CPU overlap.

    nohup python -m data_collection.backfill_l1 --start 2023-05-16 --end 2024-03-31 > data/raw/backfill.log 2>&1 &
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import logging
import time
from datetime import date, datetime, timedelta

from .bookticker import RAW_DIR, download_archive
from .compact import compact_file

log = logging.getLogger("backfill_l1")


def days(start: date, end: date) -> list[date]:
    return [start + timedelta(days=i) for i in range((end - start).days)]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True, help="exclusive")
    ap.add_argument("--ahead", type=int, default=1, help="downloads to keep in flight ahead of compaction")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    todo = [d for d in days(date.fromisoformat(a.start), date.fromisoformat(a.end))
            if not (RAW_DIR / f"{a.symbol}-bookTicker-{d:%Y-%m-%d}.parquet").exists()]
    log.info("%d days to do", len(todo))
    t0 = time.time()
    with cf.ThreadPoolExecutor(max_workers=a.ahead + 1) as pool:
        futs = {d: pool.submit(download_archive, a.symbol, d.strftime("%Y-%m-%d"), False) for d in todo[: a.ahead + 1]}
        for i, d in enumerate(todo):
            path = futs.pop(d).result()
            if i + a.ahead + 1 < len(todo):
                nd = todo[i + a.ahead + 1]
                futs[nd] = pool.submit(download_archive, a.symbol, nd.strftime("%Y-%m-%d"), False)
            if path is None:
                log.warning("%s: no archive", d)
                continue
            try:
                compact_file(path)               # deletes the zip on success
            except Exception as exc:  # noqa: BLE001
                log.error("%s: compaction failed: %s (zip kept)", d, exc)
            done = i + 1
            rate = (time.time() - t0) / done
            log.info("%s done (%d/%d, %.0f s/day, ~%.1f h left)", d, done, len(todo), rate, rate * (len(todo) - done) / 3600)


if __name__ == "__main__":
    main()
