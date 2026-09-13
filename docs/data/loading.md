# Loading data (any source)

**Source:** `data_collection/load.py`

One facade for everything the collectors produce, with the **source as an argument** so the same
call can be pointed at different origins — which is how the [validation](../notes/validation.md)
compares them.

```python
from datetime import datetime, timezone
from data_collection.load import load_l1, load_l1_raw, load_klines, load_rtds, inventory, summary

s, e = datetime(2025, 7, 1, tzinfo=timezone.utc), datetime(2025, 7, 2, tzinfo=timezone.utc)

book = load_l1("BTCUSDT", s, e, step_ms=100)                 # best source per day ("auto")
book = load_l1("BTCUSDT", s, e, step_ms=100, source="tardis")  # insist on one origin
ticks = load_l1_raw("BTCUSDT", s.date(), source="hft")       # every L1 update of one day
candles = load_klines("BTCUSDT", s, e, interval="1s", market="spot")
twap = load_rtds(s, e, topic="crypto_prices_twap_sixty")     # Polymarket settlement feed
inventory()                                                  # day × source table
summary()                                                    # days / first / last / MB per source
```

## L1 sources

| `source` | Origin | Granularity | Where it lives |
|---|---|---|---|
| `archive` | Binance `bookTicker` archive (compacted to Parquet by the backfill) | native tick feed | `data/raw/BTCUSDT-bookTicker-<day>.parquet` / `.zip` |
| `recorder` | our VPS recorder ([live recorder](record-bookticker.md), synced here) | native tick feed | `data/raw/BTCUSDT-bookTicker-<day>.csv.gz` / `.parquet` |
| `tardis` | Tardis.dev free first-of-month `book_ticker` | native tick feed | `data/raw/tardis/…` |
| `hft` | CryptoHFTData L2 diffs [replayed to L1](cryptohft.md) | ~26 ms events | `data/raw/hft/…` |
| `auto` | first available in the order above; downloads the Binance archive as a last resort | | |

A compacted `.parquet` in `data/raw/` can come from the archive or the recorder; the `source`
metadata that `compact` writes tells them apart (`bookticker._parquet_source`).
`load.l1_source_for(symbol, day)` tells you what `auto` would choose.

All four return the same columns from `sample_book` — `bid, bid_qty, ask, ask_qty, update_id,
n_updates, mid, spread, microprice, imbalance` — so downstream code doesn't care which it got.
Differences to keep in mind:

- `hft` has fewer states per day (~2M vs ~39M) because changes inside a 26 ms event are collapsed;
  `n_updates` counts events, not ticks. Its `update_id` is Binance's `final_update_id`.
- `tardis` has no `transaction_time` of its own (`event_time` is copied into it) and null `update_id`.
- The first grid point of a range is NaN if no update precedes it.

## Klines

`load_klines` reads the monthly Parquets the backfill writes
(`data/parquet/{symbol}_{market}_{interval}_{YYYY-MM}.parquet`). Months that aren't there are
skipped unless `fetch_missing=True`, which pulls them with `klines.get_klines` and saves them.

## Inventory

`inventory()` scans the data directories (no network) and returns one row per day with the file
size per source, the `auto` choice, and whether the month's 1 s klines exist. Use it to see gaps
before a backtest and to pick overlap days for validation.
