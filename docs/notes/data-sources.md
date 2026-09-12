# Data sources survey

What market data exists for BTC on Binance, where it lives, what dates it covers, and where to
go once Binance's own archives run out. Coverage dates were verified against the
`data.binance.vision` S3 listing on **2026-09-12** (`BTCUSDT` unless stated); third-party
facts were checked the same day. Budget assumption throughout: free, or a one-off < $50.

## 1. Binance public archives (`data.binance.vision`)

Free, no key, CSV inside zip, one file per day (`daily/`) or month (`monthly/`), each with a
`.CHECKSUM` (SHA-256). Listing endpoint:
`https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?prefix=data/...`.

### Spot (`data/spot/`) — trades only, never any book

| Dataset | Content | Coverage | Size/day |
|---|---|---|---|
| `klines/<interval>` | OHLCV candles; intervals `1s 1m 3m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d 3d 1w 1mo` | 2017-08-17 → yesterday | 1s: ~2 MB |
| `aggTrades` | one row per taker order: price, qty, first/last trade id, time (µs since 2025), `isBuyerMaker` | 2017-08-17 → yesterday | ~15 MB, ~1M rows |
| `trades` | one row per maker fill (finer than aggTrades) | 2017-08-17 → yesterday | ~27 MB |

Everything spot is derived from executed trades. **No `bookTicker`, no depth, ever.**

### USDⓈ-M futures (`data/futures/um/`) — BTCUSDT perpetual

| Dataset | Content | Coverage | Notes |
|---|---|---|---|
| `klines/<interval>` | OHLCV; finest interval **1m** (no 1s) | 2020-01 → yesterday | |
| `aggTrades`, `trades` | as spot | 2020-01 (agg) / 2019-09 (trades) → yesterday | |
| **`bookTicker`** | **L1: every best-bid/ask change** — `update_id, best_bid_price/qty, best_ask_price/qty, transaction_time, event_time` | **daily 2023-05-16 → 2024-03-30**; monthly 2023-05 → 2024-04, where **2024-04 is a 7-hour fragment** (Apr 1 00:00–06:46). No 2024-03-31. | ~350 MB, ~39M rows/day. Discontinued — Binance staff: "no timeline" to resume ([#334](https://github.com/binance/binance-public-data/issues/334)). Same cutoff for every symbol. |
| `bookDepth` | liquidity (BTC and notional) inside ±0.2 / 1 / 2 / 3 / 4 / 5 % of mid, one snapshot every ~30 s | 2023-01-01 → yesterday | percent bands, **not price levels**; ~0.5 MB/day |
| `metrics` | open interest, top-trader long/short ratios, taker buy/sell ratio, 5-min | 2020-09-01 → yesterday | ~12 KB/day |
| `markPriceKlines`, `indexPriceKlines`, `premiumIndexKlines` | 1m+ candles of mark / index / premium | 2020-01 → yesterday | |
| `fundingRate` | 8-hourly funding | 2020-01 → last month (monthly only) | |

### COIN-M futures (`data/futures/cm/`) — BTCUSD_PERP and quarterlies

Same datasets as um plus `liquidationSnapshot` (2023-06-25 → 2024-10-14). **`bookTicker` runs
2023-05-18 → 2024-10-14** — six months longer than um, if a coin-margined proxy is acceptable.

### Options (`data/option/`)

`BVOLIndex` (Binance BTC vol index, 2023-06-20 →) and `EOHSummary` (end-of-hour option chain
summary, BTCUSDT 2023-10-23 →). No option trades or books.

### Live REST / WebSocket (no history)

`GET /fapi/v1/ticker/bookTicker` and `/fapi/v1/depth` return the current state only; there is no
historical quotes endpoint. The WebSocket streams `<sym>@bookTicker` (every L1 change),
`<sym>@depth@100ms` (L2 diffs) and `<sym>@depth20@100ms` are what `record_bookticker.py`
captures. Binance's VIP-only "T_DEPTH / S_DEPTH" historical L2 service needs VIP 1+ and its FAQ
page has been deleted.

## 2. Perp L1 after 2024-03-30

Nothing free and complete exists. Ranked for this project:

| Source | What | Coverage | Granularity | Cost | Verified |
|---|---|---|---|---|---|
| **`record_bookticker.py`** (this repo) | native `bookTicker` | from whenever it's started | every L1 change | free | yes |
| **Tardis.dev free days** (`tardis_free_days.py`) | native `book_ticker` CSV | 1st of every month, 2019-11-17 → now | every L1 change (~8–9M rows/day) | free, no key | yes (`GET` only — `HEAD` 404s, `Range` 403s) |
| Tardis.dev 30-day trial | same, raw replay API | random 7–14 day recent window | tick | free, no card | docs only |
| **CryptoHFTData** | raw L2 snapshots + deltas, hourly parquet; L1 by replay | **2025-06-28 → now** | every L2 change | free anonymous download, 60 req/min | yes (`api.cryptohftdata.com/download?file=binance_futures/<day>/<hh>/BTCUSDT_orderbook.parquet`) |
| **Crypto Lake** | `level_1` / `book` (20 levels, ≥100 ms), `book_delta_v2` (every change) | 2022-11-14 → now, ~98 % | 100 ms depth-derived (`level_1`); tick via deltas | **$64 for one month**, 300 GB cap, cancel anytime | pricing/coverage pages |
| CoinAPI flat files | `T-QUOTES` for `BINANCEFTS_PERP_BTC_USDT` | ~2019 → now | depth-derived (100–250 ms) until an unspecified 2025 cutover, then bookTicker | ~$35–40 for the whole gap if files stay small; per-GiB | pricing verified, cutover date not |
| Tardis.dev paid | native `book_ticker` | 2019-11-17 → now (18 logged incidents) | tick | $350/mo academic, $700 solo, quarterly/yearly billing only | out of budget |
| HF `predict-quant/binance-future-orderbook` | `depth20@100ms` dumps | 2026-03-05 → 2026-04-08, patchy | 100 ms | free | sample file checked |
| Kaiko, Amberdata, CoinMetrics | tick L1 | long | tick | enterprise ($9.5k+/yr) | not viable |

Practical plan: run the recorder from today; pull the Tardis free days for 2024-04 → now; use
CryptoHFTData for 2025-06-28 onward if tick-level history matters; buy one month of Crypto Lake
only if the 2024-04 → 2025-06 stretch is needed at 100 ms.

## 3. Chainlink BTC/USD (Polymarket settlement)

Checked against live Polymarket market metadata (Gamma API) on 2026-09-12.

**Which markets use what**

| Polymarket market | Resolution source |
|---|---|
| Bitcoin Up or Down **5m / 15m / 4h** | Chainlink **Data Streams BTC/USD 60 s TWAP** (`btc-usd-twap-60s`), open vs close of the window, tie → Up. Since **2026-08-07**; from Sept 2025 to then it was the plain Chainlink CexPrice stream snapshot. |
| Bitcoin Up or Down **hourly / daily**, "BTC above $X" | **Binance BTCUSDT candles** (1h / 1m close) via UMA — not Chainlink. |

**Getting the settlement feed's history**

| Source | What | Coverage | Cost |
|---|---|---|---|
| Chainlink Data Streams REST | reports for the CexPrice stream (feed id `0x00039d9e…75b8`, 1/s) and TWAP streams | 2024-03-19 → | **$150/mo, no free tier** |
| **PMData** (`pmdata.dev`) | recorded `streams`, `streams_twap60s`, `streams_twap30s`, parquet, 1/s | Feb 2026 → | **$49/mo Plus**; 1-day free samples at `api.pmdata.dev/examples/chainlink/...` |
| **Polymarket RTDS** `wss://ws-live-data.polymarket.com` | topics `crypto_prices_twap_sixty`, `crypto_prices_chainlink`, `crypto_prices` (Binance), 1 msg/s, no auth | live only — ~58 s snapshot on subscribe, no replay | free — **record it** |
| Polymarket resolution txs | binary outcome only; the price used is not published on-chain or via API | — | — |

**Free long-history proxy (not the settlement feed):** the on-chain Polygon BTC/USD aggregator,
proxy `0xc907E116054Ad103354f2D350FD2514433D57F6f` (8 decimals, 0.1 % deviation / 27 s
heartbeat, ~2,600 rounds/day, phase 3 since 2024-04-17). Pull every round via `eth_getLogs` for
`AnswerUpdated` (topic0 `0x0559884f…fc5f`) on `https://polygon-bor-rpc.publicnode.com`
(10,000-block window per call → ~1 year in ~2,100 calls), or via Dune's
`chainlink_polygon.price_feeds`. Ethereum's feed (0.5 % / 1 h) is too sparse.

**Relation to Binance** (one sample day, 2026-07-18, 86k matched seconds): Chainlink CexPrice ≈
Binance BTCUSDT **− 7 bps** (USD vs USDT and venue mix, σ ≈ $4) and **lags it by ~2 s**
(1 s-return cross-correlation peaks at +2 s). The 60 s TWAP is the trailing mean of that, so the
settlement number trails spot by roughly 30–35 s.

## 4. Sources

- Binance archive listings: `https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?prefix=data/…`
- Discontinuation: [binance-public-data #334](https://github.com/binance/binance-public-data/issues/334), [#372](https://github.com/binance/binance-public-data/issues/372), [#380](https://github.com/binance/binance-public-data/issues/380)
- Tardis: [binance-futures details](https://docs.tardis.dev/historical-data-details/binance-futures), [data types](https://docs.tardis.dev/downloadable-csv-files/data-types), [billing](https://docs.tardis.dev/faq/billing-and-subscriptions)
- [CryptoHFTData](https://www.cryptohftdata.com/datasets/binance-orderbook-data) · [Crypto Lake data types](https://crypto-lake.com/data/) / [coverage](https://crypto-lake.com/coverage/) / [pricing](https://crypto-lake.com/subscribe/) · [CoinAPI flat-file pricing](https://www.coinapi.io/products/flat-files/pricing) / [order-book replay blog](https://www.coinapi.io/blog/crypto-order-book-replay)
- [HF predict-quant/binance-future-orderbook](https://huggingface.co/datasets/predict-quant/binance-future-orderbook)
- Polymarket: [Chainlink TWAP docs](https://docs.polymarket.com/market-data/chainlink-twap), [RTDS](https://docs.polymarket.com/market-data/websocket/rtds)
- Chainlink: [Data Streams pricing](https://docs.chain.link/data-streams/sign-up), [historical data](https://docs.chain.link/data-feeds/historical-data), [Polygon feed directory](https://reference-data-directory.vercel.app/feeds-matic-mainnet.json)
- [PMData Chainlink dataset](https://pmdata.dev/docs/datasets/chainlink-twap) / [pricing](https://pmdata.dev/pricing) · [Dune chainlink_polygon spellbook](https://github.com/duneanalytics/spellbook/blob/main/dbt_subprojects/daily_spellbook/models/chainlink/polygon/chainlink_polygon_price_feeds.sql)
