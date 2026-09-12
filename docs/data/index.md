# Data collection

The `data_collection` package holds one module per source. Conventions shared by all of them:

- **Run from the repo root.** Paths are relative: `data/raw`, `data/parquet`.
- **Script and library.** `python -m data_collection.<name> --help` for the CLI; import the
  same functions from Python.
- **Idempotent downloads.** Existing non-empty files are never re-fetched. Downloads go to a
  `.part` file, are checksum-verified (SHA-256 for Binance, MD5 for Tardis), then renamed — a
  killed download never leaves a corrupt file behind.
- **Timestamps are UTC epoch milliseconds** in every output, whatever the source used.

## Two kinds of data

| | Klines | L1 book (`bookTicker`) |
|---|---|---|
| Derived from | Executed trades only | Order-book top-of-book updates |
| Cadence | Fixed bars (1s, 1m, …); empty bars omitted | Every change to best bid/ask (~39M/day for BTCUSDT perp) |
| Modules | `klines` | `bookticker`, `record_bookticker`, `tardis_free_days` |
| Tells you | Price/volume that traded | Quotes, spread, depth at touch, imbalance — including seconds with no trades |

## L1 sources and the fallback chain

Binance published futures `bookTicker` archives only from 2023-05-16 to 2024-03-30. Three
collectors fill in the rest, and `bookticker.fetch_day` checks them in order:

| Order | Source | File | Days covered |
|---|---|---|---|
| 1 | Binance daily archive | `data/raw/{SYMBOL}-bookTicker-{YYYY-MM-DD}.zip` | 2023-05-16 → 2024-03-30 |
| 2 | Binance monthly archive | `data/raw/{SYMBOL}-bookTicker-{YYYY-MM}.zip` | Same range; 2024-04 is a 7-hour fragment |
| 3 | Live recorder | `data/raw/{SYMBOL}-bookTicker-{YYYY-MM-DD}.csv.gz` | Whenever `record_bookticker` was running |
| 4 | Tardis free day | `data/raw/tardis/binance-futures_book_ticker_{YYYY-MM-DD}_{SYMBOL}.csv.gz` | 1st of each month, 2019-11 → now |

Sources 1–2 are downloaded on demand; 3–4 must already be on disk (run the recorder, or
`tardis_free_days`, first). `bookticker.iter_batches` normalises all four into the archive
column layout, so `sample_book` and `load_bookticker` don't care where a day came from.

!!! note "Per-source differences that survive normalisation"
    - **Recorder** rows carry an extra `local_time` column (µs, receipt time on this machine).
    - **Tardis** rows have `update_id = null` and `transaction_time == event_time` (Tardis keeps
      only the exchange event time). `n_updates` still works; `update_id` will be `<NA>`.
    - **Spot** recorder rows have `transaction_time == event_time == local_time` because the spot
      WebSocket payload carries no exchange timestamps.
