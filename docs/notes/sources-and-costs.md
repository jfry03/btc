# Data sources: what each gives, and what it costs

One entry per source. "Tick" means every top-of-book change (Binance's native `bookTicker`
feed); "100 ms" means top of book read off the throttled depth stream — see
[depth-derived](data-sources.md#2-perp-l1-after-2024-03-30) for why that matters.
Prices checked 2026-09-12. Budget target: free, or a one-off < $50.

## Binance — L1 and prices

### Binance public archives (`data.binance.vision`)
- **Gives:** spot & futures klines, aggTrades, trades (2017/2020 → yesterday); futures
  `bookTicker` **tick L1 for 2023-05-16 → 2024-03-30 only**; `bookDepth` percent-band liquidity
  every 30 s (2023 → now); `metrics` (OI, long/short ratios), mark/index/premium klines, funding.
- **Granularity:** tick (bookTicker), trade-level, or candles.
- **Cost:** free, no key.
- **Access:** `data_collection/bookticker.py`, `data_collection/klines.py`.
- **Catch:** L1 discontinued after 2024-03-30, no return planned. Spot never had a book.

### Live recorder (`data_collection/record_bookticker.py`)
- **Gives:** native `bookTicker` for any symbol, from the moment you start it, written in the
  archive layout so the rest of the code doesn't care.
- **Granularity:** tick (~39M rows/day for BTCUSDT perp).
- **Cost:** free; ~350 MB/day disk; must run continuously — every gap is lost forever.

### Tardis.dev
- **Gives:** everything Binance ever streamed, recorded since 2019-11-17: `book_ticker` (tick
  L1), `quotes` (100 ms L1), `incremental_book_L2`, `book_snapshot_5/25`, trades, liquidations,
  funding. 18 logged outage incidents.
- **Free:** the **1st of every month** as CSV, no key (`data_collection/tardis_free_days.py`);
  a **30-day trial** with a random 7–14-day recent window, no card.
- **Paid:** Perpetuals plan $350/mo (academic, needs .edu proof) / $700 solo / $1,000 pro,
  quarterly or yearly billing only, no one-off purchases. Out of budget.

### CryptoHFTData
- **Gives:** raw Binance futures L2 (`snapshot` + `update` rows) as hourly parquet, plus trades.
  L1 has to be rebuilt by replaying the L2 stream.
- **Coverage:** **2025-06-28 → now**, ~15 min behind live.
- **Granularity:** every L2 change (tick L1 after replay).
- **Cost:** free anonymous download, 60 requests/min; ~20 MB/hour → ~200 GB for full history.
- **Access:** `https://api.cryptohftdata.com/download?file=binance_futures/<YYYY-MM-DD>/<HH>/BTCUSDT_orderbook.parquet`
  (plain GET; HEAD 404s). Verified.

### Crypto Lake
- **Gives:** `level_1` (best bid/ask + size), `book` (20 levels), `book_delta_v2` (every L2
  change), trades, funding, OI, liquidations for `BTC-USDT-PERP`.
- **Coverage:** book/level_1 **2022-11-14 → now**, ~98 % complete.
- **Granularity:** `level_1`/`book` 100 ms depth-derived; `book_delta_v2` tick.
- **Cost:** **$64 for one month** (individual), 300 GB/mo cap, cancel anytime. Fits budget for
  a one-off backfill of 2024-04 → 2025-06.

### CoinAPI
- **Gives:** `T-QUOTES` flat files for `BINANCEFTS_PERP_BTC_USDT` (best bid/ask + size), also
  L2 and trades; REST `quotes/history` for spot checks.
- **Coverage:** ~2019 → now.
- **Granularity:** 100–250 ms depth-derived until an unspecified 2025 cutover, then tick.
  Cutover date for futures not published — ask support before buying.
- **Cost:** pay-as-you-go, $8/GiB for the first 0.5 GiB per day, $4/GiB to 5 GiB, $2/GiB after.
  Whole gap ≈ **$35–40** if files are the small depth-derived kind; $350+ if tick-scale. No free tier.

### Kaiko · Amberdata · CoinMetrics · CoinDesk Data
- **Gives:** tick L1/L2 for Binance perps, long history (Kaiko TOB since Dec 2022).
- **Cost:** enterprise contracts, ~$9.5k–$55k/yr. Not viable.

### Community dumps
- **HuggingFace `predict-quant/binance-future-orderbook`:** `depth20@100ms` for
  2026-03-05 → 2026-04-08, patchy days. Free. 100 ms.
- **HuggingFace `Goooddy/crypto-lob-stream`:** Binance spot L2 Jun–Aug 2026; futures promised
  from Sept 2026, not published yet. Free.
- **Kaggle:** several BTC book datasets (Jan–Apr 2024, one day in Jun 2024, Jan–Mar 2025) with
  undocumented venue — not reliable enough to build on.

## Chainlink / Polymarket — the settlement price

### Polymarket RTDS WebSocket (`wss://ws-live-data.polymarket.com`)
- **Gives:** `crypto_prices_twap_sixty` (the 15m/5m/4h settlement feed), `crypto_prices_chainlink`
  (CexPrice), `crypto_prices` (Binance), 1 msg/s each.
- **Coverage:** live only — ~58 s snapshot on subscribe, no replay.
- **Cost:** free, no auth. **Record it** (recorder not yet written).

### PMData (`pmdata.dev`)
- **Gives:** recorded Chainlink `streams` (CexPrice), `streams_twap60s`, `streams_twap30s` as
  parquet, 1 row/s, plus Polymarket market data.
- **Coverage:** Feb 2026 → now.
- **Cost:** **$49/mo Plus**; one-day free samples at `api.pmdata.dev/examples/chainlink/…`.
  Only third-party archive of the actual settlement stream found.

### Chainlink Data Streams REST (`api.dataengine.chain.link`)
- **Gives:** every report of the BTC/USD CexPrice stream (1/s, since 2024-03-19) and TWAP streams.
- **Cost:** **$150/mo per feed, no free tier.** Out of budget.

### On-chain Polygon BTC/USD aggregator
- **Gives:** every on-chain round: price, roundId, updatedAt. Proxy
  `0xc907E116054Ad103354f2D350FD2514433D57F6f`, 0.1 % deviation / 27 s heartbeat → ~2,600
  points/day. Full history back to 2020-10 (phases 1–3).
- **Cost:** free — `eth_getLogs` on `polygon-bor-rpc.publicnode.com` (10k blocks/call, ~1 year in
  ~2,100 calls) or Dune's `chainlink_polygon.price_feeds`.
- **Catch:** same underlying price as the settlement feed, but **not** the settlement feed —
  it's the sparse on-chain view, and only the 5m/15m/4h markets use Chainlink at all.

### Binance klines (for the other Polymarket markets)
- Hourly/daily "Up or Down" and "BTC above $X" resolve on **Binance BTCUSDT 1h/1m candles**, which
  `data_collection/klines.py` already pulls for free.

## At a glance

| Source | L1 granularity | Coverage | Cost |
|---|---|---|---|
| Binance archives | tick | 2023-05-16 → 2024-03-30 | free |
| `record_bookticker.py` | tick | from start | free |
| Tardis free days | tick | 1st of month, 2019-11 → | free |
| Tardis trial | tick | random 7–14 recent days | free |
| CryptoHFTData | tick (via L2 replay) | 2025-06-28 → | free |
| Crypto Lake | 100 ms (tick via deltas) | 2022-11-14 → | $64 one month |
| CoinAPI | 100–250 ms → tick (2025?) | ~2019 → | ~$35–40 / per GiB |
| Tardis paid | tick | 2019-11-17 → | $350–700 / mo |
| Kaiko et al. | tick | long | $9.5k+ / yr |
| Polymarket RTDS | 1 s (settlement TWAP) | live only | free |
| PMData | 1 s (settlement TWAP) | 2026-02 → | $49 / mo |
| Chainlink Data Streams | 1 s | 2024-03-19 → | $150 / mo |
| Polygon on-chain feed | ~30 s | 2020-10 → | free |
