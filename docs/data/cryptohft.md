# L1 book: CryptoHFTData (L2 replay)

**Source:** `data_collection/cryptohft.py`
**Market:** Binance USDⓈ-M perpetual, `BTCUSDT`
**Output:** `data/raw/hft/{SYMBOL}-bookTicker-{day}.parquet`, ~7 MB/day, archive layout

[CryptoHFTData](https://www.cryptohftdata.com/datasets/binance-orderbook-data) publishes Binance
futures **L2 diffs** for free, one Parquet per hour, from **2025-06-28**. That covers the period
after Binance's own `bookTicker` archives stop and before our recorder started. We keep only the
replayed top of book: the raw files are ~480 MB/day and don't compress further, whereas the L1
result is ~7 MB/day.

## What the hourly files contain

`received_time` (ns), `event_time`, `transaction_time` (ms), `first/final/prev_final_update_id`,
`side`, `price`, `quantity` (strings; `0` = level removed). One event ≈ one `final_update_id`;
events arrive every **~100 ms in mid-2025, ~52 ms from Oct 2025 and ~26 ms by Sep 2026**, with
~43 level changes each. The update-id chain is unbroken within an
hour; some hour files have rows shuffled out of event order (seen 2025-06-30 15h) and some objects
are zstd-wrapped — both handled. There are **no snapshots**.

## Replay

A numba-jitted dense book (one `int64` per 0.1-USDT tick, up to $1M) applies every diff and
tracks best bid/ask with pointers; crossed stale levels (an ask set at or below the best bid, or
vice versa) are cleared. The book starts empty, so the first `WARMUP_S = 60` seconds after any
start or update-id gap are dropped — the top of book is touched every few ms and converges well
inside that. Levels far from the top that were never touched are simply absent, which doesn't
affect L1. Only events where the L1 state changed are emitted (~80k/hour).

Output columns: `update_id` (= `final_update_id`), `best_bid_price`, `best_bid_qty`,
`best_ask_price`, `best_ask_qty` (int-scaled with `scale:` metadata exactly like
[compaction](compact.md)), `transaction_time`, `event_time` (ms), `local_time` (CryptoHFTData
receive time, µs). `bookticker.iter_batches` decodes it and `load_l1(..., source="hft")` reads it.

## Running

```bash
nohup python -m data_collection.cryptohft --start 2025-06-28 --end 2026-09-12 > data/raw/backfill-hft.log 2>&1 &
```

~6 s per hour file (download-bound), so ~2.5 min/day. Resumable: days with an output file are
skipped; missing hours are logged and the book is reset after them. `--keep-l2` keeps the raw
hourly files under `data/raw/hft/tmp/`. Anonymous downloads are limited to 60/min.

How well the replay matches the native feed is measured in [validation](../notes/validation.md):
best bid/ask prices agree with Tardis on 98 % of seconds with no bias and never cross, but
**queue sizes agree only ~55 %** — use `hft` for price/spread/mid, not for size-based features.
