# L1 book: historical

**Module:** `data_collection.bookticker` · [reference](../reference/bookticker.md)
**Market:** Binance USDⓈ-M perpetual futures, default `BTCUSDT`
**Output:** `data/parquet/{SYMBOL}_L1_{step}ms_{START}_{END}.parquet`

Best bid/ask (level 1) sampled onto a fixed millisecond grid, streamed so a full day never has
to fit in memory. Reads from any of the [four L1 sources](index.md#l1-sources-and-the-fallback-chain)
transparently.

```bash
python -m data_collection.bookticker --start 2024-03-01 --end 2024-03-02 --step-ms 100
python -m data_collection.bookticker --start 2024-03-01T12:00 --end 2024-03-01T13:00 --step-ms 10 --ts-col event_time
```

## Coverage

!!! warning "Binance's own archives stop at 2024-03-30"
    Daily `bookTicker` archives exist for `BTCUSDT` from **2023-05-16 to 2024-03-30**. The
    2024-04 monthly file is a fragment covering the first ~7 hours of April 1. There is nothing
    after, and nothing for spot.

    For later days, the module falls back to files written by the
    [live recorder](record-bookticker.md) and [Tardis free days](tardis.md). Those must be
    on disk already; `sample_book` will silently produce NaN / carried state for days with no
    source.

Each archive day is ~350 MB compressed and ~39M rows (~2 GB in pandas).

## Raw stream

Each row is one L1 update:

| Column | Meaning |
|---|---|
| `update_id` | Monotonic order-book update sequence (`null` for Tardis rows) |
| `best_bid_price`, `best_bid_qty` | Top of bid |
| `best_ask_price`, `best_ask_qty` | Top of ask |
| `transaction_time` | Matching-engine time, ms |
| `event_time` | When Binance published the update, ms |
| `local_time` | Recorder/Tardis only: receipt time, µs |

`transaction_time` is monotonic within a file, which is what makes single-pass grid sampling
possible.

## Public API

### `sample_book(symbol, start, end, step_ms, ts_col="transaction_time", derived=True)`

The one you normally want. Returns the book state on the grid `start, start+step, … < end`.

- **As-of sampling.** Each grid point `t` gets the *last* update with `ts_col <= t` — exactly what
  the top of book looked like at that instant. No interpolation, no look-ahead.
- **`ts_col`.** `"transaction_time"` (default) is matching-engine time. Use `"event_time"` when you
  want "what a subscriber could have known at `t`". They're equal for Tardis rows.
- **Constant memory.** Streams ~1M-row Arrow batches and carries state across day boundaries.
  Only the output grid lives in memory.
- **Edge behaviour.** Grid points before the first update are NaN. Grid points after the last
  update carry the final state.

### `load_bookticker(symbol, day)`

Full raw DataFrame for one UTC day. ~39M rows / ~2 GB for `BTCUSDT`. Only for when you need
every tick.

### `prefetch(symbol, start, end)`

Downloads every daily Binance archive in `[start, end)` three-wide, idempotently. Only useful
for the 2023-05 → 2024-03 window.

### `fetch_day(symbol, day)` / `iter_day` / `iter_batches`

Lower level. `fetch_day` resolves the source for a day (see the
[fallback chain](index.md#l1-sources-and-the-fallback-chain)); `iter_batches` streams any of the
three file formats as Arrow batches in the archive column layout; `iter_day` combines them and
slices a monthly archive down to the requested day.

## Output schema

Index `ts` is `datetime64[ns, UTC]` on the grid. Columns:

| Column | Type | Notes |
|---|---|---|
| `update_id` | `Int64` (nullable) | Of the update in effect at `t`; `<NA>` on Tardis days |
| `bid`, `ask` | `float64` | Best prices |
| `bid_qty`, `ask_qty` | `float64` | Sizes at best |
| `n_updates` | `int64` | L1 updates in `(t − step, t]` — a cheap activity feature |
| `mid` | `float64` | `(bid + ask) / 2` |
| `spread` | `float64` | `ask − bid` |
| `microprice` | `float64` | `(bid·ask_qty + ask·bid_qty) / (bid_qty + ask_qty)` |
| `imbalance` | `float64` | `(bid_qty − ask_qty) / (bid_qty + ask_qty)`, in `[−1, 1]` |

The four derived columns are skipped with `derived=False`.

## How the sampling works

For each Arrow batch with sorted timestamps `t`:

1. If `t[-1] < grid[gi]` the whole batch precedes the next grid point — record its last row as
   carried state and move on.
2. Otherwise find `hi`, the number of grid points `<= t[-1]`, and for each of `grid[gi:hi]` take
   `searchsorted(t, grid, side="right") − 1`. An index of `−1` means "no update in this batch
   yet" and resolves to the carried state from the previous batch.
3. Cumulative row counts are recorded at each grid point; `n_updates` is their first difference.

One `searchsorted` per batch, so a day at 100 ms (864k grid points, 39M updates) is bound by
CSV decompression, not by the sampling.

## CLI

| Flag | Default | |
|---|---|---|
| `--symbol` | `BTCUSDT` | |
| `--start`, `--end` | required | UTC ISO-8601, `[start, end)` |
| `--step-ms` | `1000` | Grid step |
| `--ts-col` | `transaction_time` | or `event_time` |
| `--out` | auto | Parquet path |

## Download behaviour (Binance archives)

- Cached in `data/raw/`; an existing non-empty file is never re-fetched.
- Written to a `.part` file, SHA-256 verified against the published `.CHECKSUM`, then renamed.
- Up to 5 attempts with linear backoff on connection errors. A 404 returns `None`.
- `MAX_DL_WORKERS = 3` for `prefetch`; files are large and the host is shared.

### Backfilling the whole archive

```bash
nohup python -m data_collection.backfill_l1 --start 2023-05-16 --end 2024-04-01 > data/raw/backfill-l1.log 2>&1 &
```

Downloads each day's zip one day ahead of compaction, compacts it to Parquet, deletes the zip.
~13 s/day for 2023 files, 1–2 min for busy 2024 days; ~40 GB total. Resumable.

### Other sources of historical Binance L1

Nothing else free and complete exists for the perp after 2024-03-30, but these fill parts of the
gap. Full detail and prices in [Sources & costs](../notes/sources-and-costs.md); "100 ms" means
top-of-book read off the throttled depth stream rather than the native tick feed (see
[data sources survey](../notes/data-sources.md#2-perp-l1-after-2024-03-30)).

| Source | Granularity | Coverage | Cost | Reads into `bookticker`? |
|---|---|---|---|---|
| Tardis.dev `book_ticker` — free days | tick | 1st of each month, 2019-11 → | free | yes, via `tardis_free_days` |
| Tardis.dev 30-day trial | tick | random 7–14 recent days | free, no card | same file format — drop into `data/raw/tardis/` |
| CryptoHFTData | L2 diffs (100 → 52 → 26 ms over 2025–26), L1 via replay | 2025-06-28 → | free | yes — `cryptohft` module (`source="hft"`); prices match 98 %, sizes only ~55 % |
| Crypto Lake `level_1` / `book_delta_v2` | 100 ms / tick | 2022-11-14 → | $64 one month | not yet (columns `origin_time, received_time, bid_0_price…`) |
| CoinAPI `T-QUOTES` flat files | 100–250 ms, tick after a 2025 cutover | ~2019 → | per GiB, ≈ $35–40 for the gap | not yet |
| Tardis.dev paid | tick | 2019-11-17 → | $350+/mo | same as free days |
| Binance COIN-M `BTCUSD_PERP` archive | tick | 2023-05-18 → 2024-10-14 | free | yes — `sample_book("BTCUSD_PERP", …)` with `VISION_BASE` pointed at `futures/cm` |

"Not yet" means a small adapter in `iter_batches` (rename columns, scale timestamps) would be
needed; the Tardis adapter (`_tardis_to_archive`) is the template.
