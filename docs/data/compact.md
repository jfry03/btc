# L1 book: compaction (exact format)

**Source:** `data_collection/compact.py`
**Input:** `data/raw/{SYMBOL}-bookTicker-{day}.csv.gz` (recorder output, day strictly before today UTC)
**Output:** `data/raw/{SYMBOL}-bookTicker-{day}.parquet`, ~3× smaller, read transparently by `bookticker`

The recorder writes gzip CSV because it has to stream — one line per update, flushed every 5 s,
no look-ahead. Once a day is closed, the same rows can be stored far more compactly. Measured on
1M BTCUSDT rows (94 MB raw): gzip‑6 8.8 MB · gzip‑9 7.7 MB · zstd‑19 4.7 MB · xz‑6 4.2 MB ·
**this Parquet layout 3.0 MB**. For a 39M-row day that is ≈ 400 MB → ≈ 130 MB.

## What the file contains

Same seven archive columns plus the recorder's `local_time`, one row per L1 update, in the
original order. Nothing is dropped, rounded or resampled.

| Column | Stored as | Why |
|---|---|---|
| `update_id` | int64 | as-is |
| `best_bid_price`, `best_ask_price` | **int64 = price × 10^k**, `k` chosen per file (BTCUSDT: `k=1`, i.e. 0.1 USDT ticks) | prices on a tick grid are integers; consecutive updates differ by a few ticks |
| `best_bid_qty`, `best_ask_qty` | **int64 = qty × 10^k** (BTCUSDT: `k=3`, 0.001 BTC steps) | same idea |
| `transaction_time`, `event_time` | int64 ms | as-is; monotonic, so deltas are small non-negative numbers |
| `local_time` | int64 µs | as-is |

The scale `k` for each column is the smallest `k ∈ 0..8` such that every value in the file
times `10^k` is an integer (tolerance 1e‑6 for float noise), found in a first pass over the file.
It is written into the Parquet key-value metadata as `scale:<column>` (e.g. `scale:best_bid_price = 1`),
so a file for a symbol with a different tick size just gets a different `k` — nothing is hard-coded.
If some value doesn't sit on any 10^-8 grid, `k=8` (Binance's own 8-decimal fixed point) is used.

## Encoding

- Page encoding **`DELTA_BINARY_PACKED`** on every column (`data_page_version="2.0"`,
  `use_dictionary=False`). Each block stores a base value and bit-packed deltas from the block's
  minimum delta: a price that moves 0/±1 tick between updates costs ~2 bits instead of 8 bytes,
  timestamps that advance a few ms cost a few bits. This is where the bulk of the gain comes from —
  plain Parquet with the same ints but default encoding is 6.0 MB, i.e. no better than gzip.
- **zstd level 9** on top of the packed pages (level 19 only gains a further ~3 %).
- Row groups are written batch-by-batch (~32 MB of CSV each), so compaction runs in a bounded
  ~200 MB of memory — it runs on the 1 GB VPS.

## Reading it back

`bookticker.iter_batches` detects `.parquet`, calls `compact.decode`, which divides each scaled
column by `10^k` (float64) and hands back the archive layout. Integer ÷ power-of-ten is correctly
rounded, and the CSV parser also produces the nearest double to the decimal string, so the
floats are **bit-identical** to reading the gzip CSV. Verified: `sample_book` over the Parquet and
gzip versions of the same day gives `assert_frame_equal`-identical output.

## Safety

The source `.csv.gz` is deleted only after the Parquet file has been fully written, re-opened, and:

1. its row count equals the number of rows streamed from the CSV, and
2. its decoded first and last rows equal the CSV's first and last rows (all price/qty/time columns).

A failure of either leaves the `.csv.gz` untouched and the partial `.parquet.part` removed. Only
days strictly before today (UTC) are eligible, so the file the recorder is writing is never touched.

```bash
python -m data_collection.compact                       # all closed days in data/raw
python -m data_collection.compact --keep-source FILE…   # test without deleting
```
