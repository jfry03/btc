# ES futures (CME E-mini S&P 500)

**Source:** `data_collection/es_futures.py`
**Output:** `data/parquet/es/<symbol>_<schema>_<start>_<end>.parquet` (Databento) or `yahoo_ESF_1m_<date>.parquet`

There is no free per-second ES history anywhere; the cheapest self-serve path is **Databento**
(metered per GB, $125 of free credit on signup). Everything else free is 1-minute bars with a
short lookback or daily data. Surveyed 2026-09-12.

## Sources compared

| Source | Resolution | Coverage | Cost | Verified |
|---|---|---|---|---|
| **Databento `GLBX.MDP3`** | `ohlcv-1s`, `ohlcv-1m`, `trades`, `tbbo` (trade + BBO), `mbp-1` (every L1 change), `mbp-10`, `mbo` (full order-by-order), ns timestamps | ES from **2010-06** (all CME Globex); also BTC / MBT futures in the same dataset | metered per GB (rates visible only after login via `metadata.list_unit_prices`; free to query cost); **$125 free credit**; Standard plan $179–199/mo (historical only), Plus $1,399/mo, Unlimited $3,500/mo | plans/credits from Databento's blog and third-party comparisons; per-GB rates **not** public |
| **Yahoo Finance `ES=F`** | 1m (**last 8 days only**, error beyond), 5m → 60 d, 1h → 2 y, 1d → decades | rolling | free, no key, unofficial | yes — 8,877 bars pulled |
| **FirstRate Data** | 1-minute (+5m/30m/1h/1d), continuous + individual contracts, three roll adjustments | **2008-01 →**, updated daily | one-off purchase (price not shown on page; ES futures bundles are typically tens of dollars — unverified), $99.95/yr for updates | page verified, price not |
| **Massive (ex-Polygon.io) futures** | 1 s aggregates via REST, tick trades and top-of-book quotes via WebSocket and daily flat files | not stated | Basic free = end-of-day only; Starter $29/mo (15-min delayed), Developer $79, Advanced $199; futures billed separately from other asset classes | blog verified; history depth and whether flat files are on Starter unverified |
| **Kaggle `choweric/cme-es`** | daily OHLC/volume/OI per contract | 2000 → 2022 | free | listing only |
| **Kibot** | 1-min / tick ES (paid); API needs login | 2009 → | paid, price not checked | 401 without login |
| Barchart Premier, PortaraCQG, TickData, backtest-market, pitrading | 1-min to tick | 10+ y | subscription / one-off, ≥ $50 | not checked |
| CME DataMine | full depth | — | enterprise | no |

### Databento, concretely

- Schemas for ES: `ohlcv-1s` is the cheapest per-second series; `tbbo` gives every trade with the
  BBO at that instant (much smaller than `mbp-1`, which is every top-of-book change and can be
  ~10⁷ records/day on ES); `trades` is every fill.
- Continuous symbology (`stype_in="continuous"`): `ES.c.0` front month by **calendar** roll,
  `ES.n.0` by open interest, `ES.v.0` by volume; `ES.c.1` is the next contract. Outrights like
  `ESZ5` use `raw_symbol`. Continuous series are *not* back-adjusted — do that yourself if needed.
- **Approximate data sizes** (DBN record sizes × typical ES activity; estimate, not verified):
  `ohlcv-1s` ≈ 5 MB/day → **~1.2 GB/year**; `trades` ≈ 50–150 MB/day → ~25 GB/year;
  `tbbo` ≈ 100–200 MB/day → ~40 GB/year; `mbp-1` ≈ 0.5–1.5 GB/day → hundreds of GB/year.
  Cost = size × the schema's $/GB, so a year of `ohlcv-1s` is almost certainly well inside the
  $125 credit; a year of trades or tbbo may or may not be — **always run `--dry-run` first**.
- BTC/MBT (CME Bitcoin futures) live in the same dataset with the same schemas (BTC from 2017-12,
  MBT from 2021-04), so the same script fetches them: `--symbol BTC.c.0`.

## Getting a key

1. Sign up at <https://databento.com> (no card needed for the free credit), create an API key in
   the portal (Keys page).
2. `export DATABENTO_API_KEY=db-…` in the shell (never commit it; the script only reads the env var).
3. `python -m data_collection.es_futures --prices` prints the $/GB table for every schema so you
   can size a pull; `--dry-run` prints the exact cost of a request via `metadata.get_cost`
   (free) before anything is spent.

## Usage

```bash
python -m data_collection.es_futures --yahoo                              # no key: ES=F 1m, last 8 days
python -m data_collection.es_futures --dry-run                            # cost of ES.c.0 ohlcv-1s, last 8 days
python -m data_collection.es_futures --schema ohlcv-1s --start 2025-09-01 --end 2026-09-01 --dry-run
python -m data_collection.es_futures --schema ohlcv-1s --start 2025-09-01 --end 2026-09-01 --yes
python -m data_collection.es_futures --symbol BTC.c.0 --schema tbbo --start 2026-09-01 --end 2026-09-08 --dry-run
```

```python
from data_collection import es_futures as es
es.cost("ES.c.0", "tbbo", "2026-09-01", "2026-09-08")      # {'usd': …, 'gb': …, 'records': …}
path = es.fetch("ES.c.0", "ohlcv-1s", "2026-09-01", "2026-09-08")
```

The CLI refuses to spend without `--yes`. For multi-GB pulls use Databento's batch API
(`client.batch.submit_job`) rather than `get_range`; the script's `fetch` is meant for pulls up to
a few GB.

## Output columns

Databento's native DataFrame: index `ts_event` (UTC, ns), `rtype`, `publisher_id`, `instrument_id`,
`symbol`, then per schema — `ohlcv-*`: `open, high, low, close, volume`; `trades`/`tbbo`: `price,
size, side, action, flags, depth, ts_recv, sequence` (+ `bid_px_00, ask_px_00, bid_sz_00, ask_sz_00`
for tbbo/mbp-1). Prices are already scaled to floats. Yahoo: `open, high, low, close, volume`
indexed by UTC minute.

## Trading hours

ES trades on CME Globex Sunday 18:00 → Friday 17:00 ET with a daily 17:00–18:00 ET maintenance
halt; the cash-session hours (09:30–16:00 ET) are when it is most liquid. See [Market hours](market-hours.md)
for the helpers that compute open/closed for CME and the other venues.

## The planned pull: ES best bid/ask every second

`scripts/pull_es_bbo.sh` prices (and with `--yes`, buys) `ES.c.0` `bbo-1s` from 2021-11-01 to
2026-09-12 in yearly chunks — **$119.02 for 7.1 GB / 88.7M rows** at the rates seen on
2026-09-12 (`bbo-1s` = $18/GB), i.e. inside the $125 signup credit with a small buffer. Each
row is one second of the Globex session with `bid_px_00, ask_px_00, bid_sz_00, ask_sz_00,
bid_ct_00, ask_ct_00` (top of book and order counts) plus the last trade's `price, size, side`.
Chunks land in `data/parquet/es/ES_c_0_bbo-1s_<start>_<end>.parquet`; existing chunks are skipped,
so the script is resumable, and it refuses to execute if the remaining cost exceeds `--budget`.

Why `bbo-1s` rather than `ohlcv-1s`: it carries size on each side (the candle doesn't), and it's
cheaper ($18 vs $70 per GB; $25 vs $44 per year for ES). `tbbo`/`mbp-1` (every change) cost
$146–$240+ per year and don't fit the credit.

Caveats: `ES.c.0` rolls by calendar, so around each quarterly expiry the series jumps between
contracts — compute returns within a `symbol`, not across. Databento's credit balance is not
queryable via API; check the portal before running with `--yes`. Prices are for the
`historical` mode; run `--prices` to re-check.

## Spend ledger

Databento's API has no balance endpoint (verified: nothing in `metadata.*`, and the obvious
unlisted paths 404), and overage is billed to the card on file rather than blocked, so the module
keeps its own ledger at `data/parquet/es/databento_ledger.json`: total credit plus one entry per
purchase with the exact quoted cost. Every buy — plan chunks and single pulls — is refused if it
would exceed the ledger's remaining credit, and plan chunks re-check before each one.

```bash
python -m data_collection.es_futures --balance     # credit / spent / remaining, with purchase list
python -m data_collection.es_futures --credit 200  # set total credit (after a top-up, or minus anything bought outside)
```

It only knows about purchases made through this module; seed it with `--credit` if you buy
anything in the portal. The signup credit expires six months after account creation.

## What was pulled (2026-09-12)

`ES.c.0` `bbo-1s` 2021-11-01 → 2026-09-12: **88,748,062 rows, 1.75 GB** in five yearly Parquets,
$119.02 of the credit. Load with `es_futures.load_bbo(start, end)`, which concatenates the chunks
and adds `mid`, `spread` and a `roll` flag (the `symbol` column is the continuous alias `ES.c.0`
for every row; contract changes are detected from `instrument_id`). Databento flagged
2021-12-05 and 2022-01-02 as degraded-quality days.
