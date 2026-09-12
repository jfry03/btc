# L1 book: Tardis free days

**Module:** `data_collection.tardis_free_days` · [reference](../reference/tardis-free-days.md)
**Exchange:** `binance-futures` (Tardis' name for USDⓈ-M)
**Output:** `data/raw/tardis/binance-futures_{data_type}_{YYYY-MM-DD}_{SYMBOL}.csv.gz`

[Tardis.dev](https://tardis.dev) publishes the **first day of every month** free, with no API
key. Its `book_ticker` dataset for `binance-futures` is the native Binance `bookTicker` stream,
from **2019-11-17 to the present** — twelve days a year of L1 data across the entire period,
including the gap after Binance's own archives stop. ~60 MB gz / ~8–9M rows per day.

```bash
python -m data_collection.tardis_free_days --start 2024-04 --end 2026-10          # every 1st in range
python -m data_collection.tardis_free_days --start 2026-08 --end 2026-09 --data-type quotes
```

| Flag | Default | |
|---|---|---|
| `--symbol` | `BTCUSDT` | |
| `--data-type` | `book_ticker` | `book_ticker`, `quotes`, `trades`, `book_snapshot_5`, `incremental_book_L2` |
| `--start` | required | `YYYY-MM`, inclusive |
| `--end` | required | `YYYY-MM`, exclusive |

Files are stored under Tardis' own naming, which is what `bookticker.fetch_day` looks for, so
`sample_book` works on these days unchanged.

## Data types

- **`book_ticker`** — native `bookTicker` stream. ~8–9M rows/day. Use this.
- **`quotes`** — top of book derived from the throttled depth stream, ~1M rows/day. Much
  coarser; only if `book_ticker` is unavailable.

Other days need a paid plan (from $350/month).

## Column mapping

Tardis columns are `exchange, symbol, timestamp, local_timestamp, ask_amount, ask_price,
bid_price, bid_amount` with timestamps in **µs**. `bookticker._tardis_to_archive` maps them onto
the archive layout:

| Archive column | From |
|---|---|
| `update_id` | `null` — Tardis doesn't keep it |
| `best_bid_price`, `best_bid_qty` | `bid_price`, `bid_amount` |
| `best_ask_price`, `best_ask_qty` | `ask_price`, `ask_amount` |
| `transaction_time` | `timestamp / 1000` (ms) |
| `event_time` | `timestamp / 1000` — Tardis keeps only the exchange event time, so this equals `transaction_time` |
| `local_time` | `local_timestamp` (µs, Tardis' receipt time) |

## Download quirks

- Plain `GET` only. `HEAD` returns 404 and `Range` requests 403, so there's no resume; a
  killed download restarts the file.
- Integrity is checked against the `x-md5` response header.
- 401 / 403 / 404 all mean "not free" and return `None`.
