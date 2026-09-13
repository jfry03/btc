"""
CryptoHFTData: free hourly Binance-futures L2 diff files (2025-06-28 -> now) replayed to L1.

Each hour file (~23 MB, ~5-6M rows) holds every depth diff (`side, price, quantity`, one event
every ~26 ms, update-id chain unbroken) but NO snapshot. We replay from an empty book: the top of
book converges within seconds because it is touched constantly, so the first WARMUP_S after any
(re)start are dropped. Output per UTC day, archive layout, int-scaled like `compact`:

    data/raw/hft/<SYMBOL>-bookTicker-<day>.parquet      (~7 MB/day)

    update_id        = final_update_id of the event
    best_*           = book state at the end of the event (only rows where L1 changed)
    transaction_time / event_time = ms, from the event
    local_time       = CryptoHFTData receive time, µs

`bookticker.fetch_day` finds these, so `sample_book` works on any day that has one. Raw L2 files
are deleted after replay unless --keep-l2. Resumable: days with a parquet are skipped.

    nohup python -m data_collection.cryptohft --start 2025-06-28 --end 2026-09-12 > data/raw/backfill-hft.log 2>&1 &
"""
from __future__ import annotations

import argparse
import logging
import time
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests
from numba import njit

OUT_DIR   = Path("data/raw/hft")
TMP_DIR   = OUT_DIR / "tmp"
BASE      = "https://api.cryptohftdata.com/download?file=binance_futures/{day:%Y-%m-%d}/{hour:02d}/{symbol}_orderbook.parquet"
MAX_TICKS = 10_000_000         # dense book array size in price ticks (0.1 USDT -> 1M USDT)
WARMUP_S  = 60
SESSION   = requests.Session()
SESSION.headers["User-Agent"] = "btc-research/1.0"

log = logging.getLogger("cryptohft")


# --------------------------------------------------------------------------- download

def download_hour(symbol: str, day: date, hour: int) -> Path | None:
    dest = TMP_DIR / f"{symbol}-{day:%Y-%m-%d}-{hour:02d}.parquet"
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    url = BASE.format(day=day, hour=hour, symbol=symbol)
    for attempt in range(6):
        try:
            r = SESSION.get(url, timeout=300, stream=True, allow_redirects=True)
            if r.status_code == 404:
                return None
            if r.status_code == 429:
                time.sleep(15 * (attempt + 1)); continue
            r.raise_for_status()
            tmp = dest.with_suffix(".part")
            with tmp.open("wb") as fh:
                for chunk in r.iter_content(4 << 20):
                    fh.write(chunk)
            expected = int(r.headers.get("Content-Length", -1))
            if expected >= 0 and tmp.stat().st_size != expected:
                raise requests.ConnectionError(f"truncated: {tmp.stat().st_size} of {expected} bytes")
            with tmp.open("rb") as fh:
                magic = fh.read(4)
            if magic == b"\x28\xb5\x2f\xfd":       # some objects are zstd-wrapped parquet
                raw = tmp.with_suffix(".zst")
                tmp.rename(raw)
                with pa.CompressedInputStream(pa.OSFile(str(raw)), "zstd") as src, tmp.open("wb") as out:
                    while chunk := src.read(8 << 20):
                        out.write(chunk)
                raw.unlink()
            pq.read_metadata(tmp)                 # footer present and parseable
            tmp.rename(dest)
            return dest
        except (requests.RequestException, pa.ArrowInvalid, OSError) as exc:
            log.warning("%s %02d: %s (retry %d)", day, hour, exc, attempt + 1)
            time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"gave up downloading {day} {hour:02d}")


# --------------------------------------------------------------------------- replay

@njit(cache=True)
def _replay(side, price, qty, ev_end, ev_first, book_bid, book_ask, st):
    """
    side: 1 bid / 0 ask; price in ticks; qty scaled int; ev_end: exclusive row index per event.
    st = [bb, ba, bbq, baq, initialised]; book arrays persist across calls.
    Returns per-event (event index, bb, bbq, ba, baq) for events where L1 changed.
    """
    n_ev = ev_end.shape[0]
    out_ev = np.empty(n_ev, np.int64); out = np.empty((n_ev, 4), np.int64)
    k = 0
    bb, ba = st[0], st[1]
    start = 0
    for e in range(n_ev):
        end = ev_end[e]
        for i in range(start, end):
            p = price[i]; q = qty[i]
            if side[i] == 1:
                book_bid[p] = q
                if q > 0:
                    if p > bb: bb = p
                    if p >= ba:                       # stale asks below/at this bid: clear them
                        for a in range(ba, p + 1): book_ask[a] = 0
                        ba = p + 1
                        while ba < MAX_TICKS and book_ask[ba] == 0: ba += 1
                elif p == bb:
                    while bb > 0 and book_bid[bb] == 0: bb -= 1
            else:
                book_ask[p] = q
                if q > 0:
                    if p < ba: ba = p
                    if p <= bb:
                        for b in range(p, bb + 1): book_bid[b] = 0
                        bb = p - 1
                        while bb > 0 and book_bid[bb] == 0: bb -= 1
                elif p == ba:
                    while ba < MAX_TICKS and book_ask[ba] == 0: ba += 1
        start = end
        if bb > 0 and ba < MAX_TICKS:
            bbq = book_bid[bb]; baq = book_ask[ba]
            if bb != st[0] or ba != st[1] or bbq != st[2] or baq != st[3]:
                out_ev[k] = e; out[k, 0] = bb; out[k, 1] = bbq; out[k, 2] = ba; out[k, 3] = baq; k += 1
                st[0] = bb; st[1] = ba; st[2] = bbq; st[3] = baq
    return out_ev[:k], out[:k]


class Book:
    def __init__(self):
        self.bid = np.zeros(MAX_TICKS, np.int64)
        self.ask = np.zeros(MAX_TICKS, np.int64)
        self.reset()

    def reset(self):
        self.bid[:] = 0; self.ask[:] = 0
        self.st = np.array([0, MAX_TICKS, 0, 0, 0], np.int64)
        self.last_final = None
        self.warm_until_ms = None


def replay_hour(path: Path, book: Book, price_scale: int, qty_scale: int) -> pa.Table:
    t = pq.read_table(path, columns=["received_time", "event_time", "transaction_time", "final_update_id",
                                     "prev_final_update_id", "side", "price", "quantity"])
    if t.num_rows == 0:
        return None
    px = pc.cast(pc.round(pc.multiply(pc.cast(t["price"], pa.float64()), float(10 ** price_scale))), pa.int64()).to_numpy()
    qt = pc.cast(pc.round(pc.multiply(pc.cast(t["quantity"], pa.float64()), float(10 ** qty_scale))), pa.int64()).to_numpy()
    sd = pc.cast(pc.equal(t["side"], "bid"), pa.int8()).to_numpy()
    et = t["event_time"].to_numpy(); tx = t["transaction_time"].to_numpy(zero_copy_only=False)
    fu = t["final_update_id"].to_numpy(); pv = t["prev_final_update_id"].to_numpy(); rc = t["received_time"].to_numpy()
    # Some hour files are not in event order (rows shuffled; seen 2025-06-30 15h). An event is one
    # final_update_id and each price appears at most once per event, so a stable sort by
    # final_update_id restores a valid replay order.
    if not np.all(fu[1:] >= fu[:-1]):
        order = np.argsort(fu, kind="stable")
        px, qt, sd, et, tx, fu, pv, rc = (a[order] for a in (px, qt, sd, et, tx, fu, pv, rc))
        log.warning("%s: rows not in event order - sorted (%d distinct events)", path.name, len(np.unique(fu)))
    if px.max() >= MAX_TICKS:                 # junk levels far from mid (seen: asks at 10x price)
        ok = px < MAX_TICKS
        log.warning("%s: dropping %d rows with price >= %d ticks", path.name, int((~ok).sum()), MAX_TICKS)
        # keep event structure intact: zero the qty instead of removing rows
        qt = np.where(ok, qt, 0); px = np.where(ok, px, 0)
    # event boundaries (rows are in event order; an event = one final_update_id)
    change = np.flatnonzero(fu[1:] != fu[:-1]) + 1
    ev_start = np.concatenate(([0], change)); ev_end = np.concatenate((change, [len(fu)]))
    # continuity across hours: reset book if the id chain is broken
    if book.last_final is not None and pv[0] != book.last_final:
        log.warning("%s: update-id gap (%d -> %d), resetting book", path.name, book.last_final, pv[0])
        book.reset()
    if book.warm_until_ms is None:
        book.warm_until_ms = int(et[0]) + WARMUP_S * 1000
    ev_i, out = _replay(sd, px, qt, ev_end.astype(np.int64), ev_start.astype(np.int64), book.bid, book.ask, book.st)
    book.last_final = int(fu[-1])
    rows = ev_start[ev_i]
    keep = et[rows] >= book.warm_until_ms
    rows, out = rows[keep], out[keep]
    return pa.table({
        "update_id": fu[rows], "best_bid_price": out[:, 0], "best_bid_qty": out[:, 1],
        "best_ask_price": out[:, 2], "best_ask_qty": out[:, 3],
        "transaction_time": np.where(np.isnan(tx[rows]), et[rows], tx[rows]).astype(np.int64) if tx.dtype.kind == "f" else tx[rows],
        "event_time": et[rows], "local_time": rc[rows] // 1000,
    })


def write_day(tables: list[pa.Table], dest: Path, symbol: str, day: date, price_scale: int, qty_scale: int) -> int:
    tbl = pa.concat_tables(tables)
    meta = {"scale:best_bid_price": str(price_scale), "scale:best_ask_price": str(price_scale),
            "scale:best_bid_qty": str(qty_scale), "scale:best_ask_qty": str(qty_scale),
            "source": f"cryptohftdata binance_futures {symbol} {day:%Y-%m-%d} L2 replay (26ms events, {WARMUP_S}s warmup)"}
    tmp = dest.with_suffix(".parquet.part")
    pq.write_table(tbl.replace_schema_metadata(meta), tmp, compression="zstd", compression_level=9,
                   use_dictionary=False, column_encoding="DELTA_BINARY_PACKED", data_page_version="2.0")
    tmp.replace(dest)
    return tbl.num_rows


def backfill(symbol: str, start: date, end: date, price_scale: int, qty_scale: int, keep_l2: bool) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    book = Book()
    day = start
    t0, n_done = time.time(), 0
    while day < end:
        dest = OUT_DIR / f"{symbol}-bookTicker-{day:%Y-%m-%d}.parquet"
        if dest.exists():
            day += timedelta(days=1); book.reset(); continue
        tables, missing = [], []
        for h in range(24):
            p = download_hour(symbol, day, h)
            if p is None:
                missing.append(h); book.reset(); continue
            tbl = replay_hour(p, book, price_scale, qty_scale)
            if tbl is not None and tbl.num_rows:
                tables.append(tbl)
            if not keep_l2:
                p.unlink()
        if tables:
            n = write_day(tables, dest, symbol, day, price_scale, qty_scale)
            n_done += 1
            rate = (time.time() - t0) / n_done
            log.info("%s: %d L1 rows, missing hours %s (%.0f s/day, ~%.1f h left)", day, n, missing or "-",
                     rate, rate * (end - day).days / 3600)
        else:
            log.warning("%s: no data", day)
        day += timedelta(days=1)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True, help="exclusive")
    ap.add_argument("--price-scale", type=int, default=1, help="10^k: BTCUSDT tick 0.1 -> 1")
    ap.add_argument("--qty-scale", type=int, default=3, help="10^k: BTCUSDT step 0.001 -> 3")
    ap.add_argument("--keep-l2", action="store_true")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    backfill(a.symbol, date.fromisoformat(a.start), date.fromisoformat(a.end), a.price_scale, a.qty_scale, a.keep_l2)
