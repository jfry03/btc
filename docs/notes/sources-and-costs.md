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
- **Gives:** raw Binance futures **L2 diffs** as one Parquet per exchange/symbol/hour
  (`binance_futures/<YYYY-MM-DD>/<HH>/BTCUSDT_orderbook.parquet`, 18–23 MB, 4–6M rows), plus
  `BTCUSDT_trades.parquet` and `BTCUSDT_ticker.parquet` per hour. Columns: `received_time` (ns),
  `event_time`/`transaction_time` (ms), `first/final/prev_final_update_id`, `side`, `price`,
  `quantity` (strings; `0` = level removed).
- **Coverage:** **2025-06-28 → now**, ~15 min behind live.
- **Granularity (verified):** diff events every **~100 ms in mid-2025, ~52 ms from Oct 2025,
  ~26 ms by Sep 2026** (p10–p90 26–28 ms on 2026-09-01), ~43 level changes per event, update-id
  chain **unbroken** within the hour. Changes inside an event window are collapsed — not per-change
  like `bookTicker`. Replayed L1 prices match the native feed 98 % of seconds; **quantities only
  ~55 %** (see [validation](validation.md)). **No snapshots** in the files (only `event_type = update`), so replay from an empty
  book: fine for L1 after a few seconds' warm-up (the top of book is touched constantly), not for
  deep levels.
- **Cost:** free anonymous download (60 req/min; URL 302s to Cloudflare R2, use `curl -L`);
  S3 bulk access needs an account. ~500 MB/day of order-book Parquet.
- **Access:** `https://api.cryptohftdata.com/download?file=binance_futures/<day>/<HH>/BTCUSDT_orderbook.parquet`.
  Needs an L2→L1 replay adapter to feed `bookticker` (not written yet).

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

## Other centralised exchanges (free bulk archives)

All free and no account unless stated; verified 2026-09-12 by fetching the example files. Not
Binance, so a different order book — but several of these give exactly what Binance withholds:
long, free, snapshot + diff L2 history at ≤100 ms.

| Exchange | Data (BTC) | Granularity | Coverage | Example |
|---|---|---|---|---|
| **Bybit** trades (`public.bybit.com`) | perp / inverse / spot tick trades, csv.gz | tick | perp BTCUSDT 2020-03-25 →, spot 2022-11 → | `https://public.bybit.com/trading/BTCUSDT/BTCUSDT2026-09-10.csv.gz` (68 MB, 2M trades) |
| **Bybit L2** (`quote-saver.bycsi.com`) | **500-level (→200 since 2025-08) snapshot + deltas**, NDJSON of WS messages | ~100 ms deltas | linear BTCUSDT **2023-01-18 →**, inverse same, spot 2025-04 → | `https://quote-saver.bycsi.com/orderbook/linear/BTCUSDT/2024-01-01_BTCUSDT_ob500.data.zip` (50–215 MB/day; directory listings work) |
| **OKX** (`static.okx.com`) | trades, 1m candles, funding, **400-level L2 snapshot (every 60 s) + updates**, 5000-level L2 | trades tick; **L2 updates ~10 ms median** | trades spot 2021-09 →, SWAP 2021-10 →; **400-lv L2 2023-03-22 →** (lags a few days); 5000-lv 2025-11 → | `https://static.okx.com/cdn/okx/match/orderbook/L2/400lv/daily/20240101/BTC-USDT-SWAP-L2orderbook-400lv-2024-01-01.tar.gz` (~377 MB/day). No VIP gate found. Trade/candle files use **UTC+8** day boundaries |
| **Gate.io** (`download.gatedata.org`) | spot deals, futures trades, 1m candles, mark, funding, **L2 order books (hourly, snapshot + 100 ms-merged updates)** | tick; L2 100 ms | deals 2018 →; **L2 spot & futures_usdt 2021-08 → now — longest free L2 history found** | `https://download.gatedata.org/futures_usdt/orderbooks/202401/BTC_USDT-2024010100.csv.gz` (9 MB/h, no header) |
| **KuCoin** (`historical-data.kucoin.com`) | spot & perp trades, klines, **50-level L2 snapshots**, funding/mark/index | tick; L2 ~100 ms snapshots | trades 2023 →; L2 spot 2025-01 →, perp 2025-02 → | `https://historical-data.kucoin.com/data/spot/daily/depth/orderbooklv50/BTC-USDT/BTC-USDT-orderbooklv50-2025-01-14.zip` |
| **HTX / Huobi** (`futures.htx.com/data`) | trades, 1m klines, mark/index; new tree adds **L2 400-lv spot / 150-lv perp snapshot + diff** | tick; L2 diffs 20–120 ms | trades 2020-06 →; **L2 2026-05-28 →** | `https://futures.htx.com/data/historical_data/spot/daily/orderbook/lv400/BTC-USDT/BTC-USDT-l2orderbook-400lv-2026-09-11.tar.gz` |
| **Bitget** (`img.bitgetimg.com`) | spot & perp trades, 1m klines (xlsx), **L1 bookTicker** (irregular, 5–20 s), 500-level depth snapshots (~20 s) | tick trades; coarse book | trades 2018-08 →; L1 2024-12 →; depth 2026-01 → | `https://img.bitgetimg.com/online/trades/SPBL/BTCUSDT/BTCUSDT_SPBL_20240101_001.zip` |
| **BitMEX** (`public.bitmex.com`) | daily **L1 quote** and trade CSVs, all symbols | tick (ns) | **2014-11-22 →** | `https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/quote/20200101.csv.gz` (recent trade files look incomplete; quotes fine) |
| **MEXC** | 5m+ klines only | 5 m | 2023 → | weakest source |
| **Coinbase Exchange** (REST) | every BTC-USD trade by id, candles 1m+ | tick (µs) | **2015-01-08 →** (~1.09 B trades, 1000/request) | `https://api.exchange.coinbase.com/products/BTC-USD/trades?limit=1000&after=500000000` |
| **Kraken** | bulk OHLCVT zip (all pairs, 1m+, 7.3 GB on Google Drive) + quarterly updates; REST trades | 1 m bulk; tick via REST | XBTUSD **2013-10 →** | `https://api.kraken.com/0/public/Trades?pair=XBTUSD&since=0` |
| **Kraken Futures** | 1m candles; public **order-event history** (placed/updated/cancelled — effectively L3) | ms | PF_XBTUSD 2022-03 → | `https://futures.kraken.com/api/history/v3/market/PF_XBTUSD/orders?since=1700000000000&sort=asc&count=1000` |
| **Bitfinex** (REST) | trades, candles 1m+, perp | tick | trades **2013-01 →**, 10,000/request | `https://api-pub.bitfinex.com/v2/trades/tBTCUSD/hist?start=0&limit=10000&sort=1` |
| **Bitstamp** (REST) | 1m OHLC from 2012; transactions last 24 h only | 1 m | 2012 → | `https://www.bitstamp.net/api/v2/ohlc/btcusd/?step=60&limit=1000&start=1325376000` |
| **Deribit** (REST) | 1m candles from 2018-08; trades only ~24–36 h back | 1 m | 2018-08 → | use Tardis free days for tick |
| **Tardis free days** (all of the above venues + Hyperliquid, Coinbase Intl…) | `quotes` (tick L1), `book_snapshot_5/25`, `incremental_book_L2`, trades | tick | 1st of each month from each venue's start | `https://datasets.tardis.dev/v1/bybit/quotes/2024/01/01/BTCUSDT.csv.gz` |

Takeaways: **Gate.io (2021-08 →), Bybit (2023-01 →) and OKX (2023-03 →) give free snapshot+diff
L2 for BTC perps at ≤100 ms**, which is finer than anything free for Binance in the same period;
**BitMEX** is the only venue with a free tick L1 archive back to 2014. REST lookback traps:
Deribit trades ~24 h, OKX history-trades ~3 months, Bitget fills 90 d, Gate spot trades 30 d,
MEXC 24 h. OKX and old HTX files use UTC+8 day boundaries; KuCoin/OKX legacy rows aren't time-sorted.

## Polymarket's own market data

Verified 2026-09-12. Context: Polymarket moved to **CTF Exchange V2**
(`0xE111180000d2663C0091e4f400237545B87B996B`) in April 2026; the old CTF/NegRisk contracts and
the Goldsky public subgraphs are dead for new data.

### Enumerating the BTC markets (Gamma API, free)
Slugs: `btc-updown-5m-<unix>` (288/day, since 2025-12-18), `btc-updown-15m-<unix>` (96/day,
900 s-aligned, since 2025-09-12; older ones `btc-up-or-down-15m-<unix>`), `btc-updown-4h-<unix>`,
`bitcoin-up-or-down-<month>-<d>-2026-<h>am-et` (hourly), `bitcoin-above-on-…` (multi-strike hourly,
since 2026-03-20), daily series since 2023. Series ids: 5m 10684 · 15m 10192 · 4h 10331 · hourly
10114 · daily 41. Page with
`https://gamma-api.polymarket.com/events/keyset?series_id=10192&closed=true&limit=100` → `next_cursor`.
Each market gives `conditionId`, `clobTokenIds`, and for closed markets `outcomePrices` (= the result).

### Polymarket APIs
| API | What | Granularity | Coverage | Limits |
|---|---|---|---|---|
| **CLOB `/prices-history`** | outcome-token price series per `clobTokenId` (`startTs/endTs/fidelity`; `interval=max` empty for old markets) | **~60 s samples** (~15 points per 15m market) | verified back to Sept 2025 | free, 1000 req/10 s |
| **Data API `/trades?market=<conditionId>`** | every fill: side, price, size, wallet, tx, timestamp | tick | ≥ 270 days back; `limit≤1000`, `offset` works | free, 200 req/10 s — 429s on bursts |
| CLOB `/book` | live book only; errors for closed tokens | live | none | — |
| CLOB `/trades` | needs an L2 API key | — | — | account |
| **CLOB WebSocket** `wss://ws-subscriptions-clob.polymarket.com/ws/market` | live book + price changes — what every vendor below records | tick | live only | free — **record it** |

### On-chain (Polygon)
V2 `OrderFilled(bytes32 orderHash, address maker, address taker, uint8 side, uint256 tokenId,
uint256 makerAmountFilled, uint256 takerAmountFilled, uint256 fee, bytes32 builder, bytes32 metadata)`,
topic `0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee` (the V1 hash quoted on
some sites is wrong). ~3M fills/day across all markets. Free archive access:
**Tenderly public gateway** `polygon.gateway.tenderly.co` (`eth_getLogs`, ~2000 blocks/call,
30–40 s each); publicnode is pruned to recent blocks; Envio HyperSync needs a (free) token;
Dune `polymarket_polygon.market_trades` has decoded v1+v2 fills with ~1 h lag (free-plan credits).

### Order-book history for the 5m/15m markets
Polymarket keeps none. Third parties:

| Source | What | Granularity | Coverage | Cost |
|---|---|---|---|---|
| **PMData** (`pmdata.dev`) | L2 snapshots + every price-change tick, trades, on-chain fills, Chainlink/TWAP; one Parquet per market slug, 7 coins × 5m/15m/1h (YES side; NO by symmetry) | every book update, µs | since 2026-02 | **free: 1,200 Up/Down market files, then 100/month**; Plus $49/mo |
| **PredictionTicks** | BTC **5m only**: tick prices CSV + full-depth book JSONL per round | ~1.5 s | 2026-02 → 2026-08 (books from Apr) | **$15 one-off**; free daily samples verified |
| HF `kachoio/polymarket-5-minute-crypto-up-down-markets` | second-by-second top of book, 7 coins, 5m markets (`btc_ticks.parquet` 182 MB) | 1 s BBO | 2026-03-24 → 2026-05-18 | free |
| HF `BrockMisner/polymarket-crypto-5m-15m` | 10-level books every 10 s + trades + resolutions, BTC/ETH/SOL/XRP **5m & 15m** | 10 s | 2026-01-09 → 2026-03-13 | free |
| HF `Ligengxin96/polymarket-predict-fun-tick-data` | one day (2026-09-08) of books + trades + Chainlink for BTC 5m | tick | 1 day | free |
| PolyHistorical / PolymarketBacktest / PolymarketData.co / Telonex | 200–300 ms books, 1-min prices, etc. | varies | 2025-08 → | free tiers are tiny; $17–37/mo or undisclosed |

## Other oracles

### Pyth
- **Gives:** ~400 ms signed BTC/USD updates, full history via Hermes `/v2/updates/price/<ts>`.
- **Cost:** **API key required since 2026-08-26**; free plan has no API, Starter $500/mo. Benchmarks
  (the old free TradingView-style history) is gone. Not viable.

### RedStone · Band · Chronicle · Switchboard
- RedStone gateway: 10 s slots, free, but only **~48 h** of history — record-it-yourself only.
  Band's public endpoint is stale (last resolve Sep 2025); Chronicle/Switchboard expose no history.

### Chainlink push feeds on other chains
- **Gives:** the same BTC/USD aggregator as Polygon, on other chains, via `AnswerUpdated` logs.
- **Density (measured):** BNB `0x14Aed7178df8d33755b1c4b8f3CC3e0EAa2B203B` 0.05 % / 30 s → ~31 s;
  Polygon ~33 s; Arbitrum / Base / Optimism ~20 min; Avalanche daily. Nothing beats Polygon by much.
- **Cost:** free. Public RPCs serve recent logs only; full history needs a free-signup key
  (Etherscan V2 `getLogs`, Alchemy, Infura).

## DEX perps

### Hyperliquid S3 archives
- **Gives:** `hyperliquid-archive/market_data/<YYYYMMDD>/<H>/l2Book/BTC.lz4` — L2 snapshots at
  ~550 ms event-driven cadence; `hl-mainnet-node-data/node_fills_by_block/hourly/…lz4` — every
  fill, all coins, ms timestamps; `asset_ctxs` daily CSV with oracle/mark/mid.
- **Coverage:** 2023 → present (monthly uploads, gaps possible); fills from 2025-07 in the current layout.
- **Cost:** requester-pays S3, ~$0.09/GB egress → ~0.8–1 GiB/day of fills ≈ **$30/year**. Needs any AWS account.
- REST `candleSnapshot` keeps only ~5,000 candles per interval (1m → 3.6 days).

### dYdX v4 · GMX · Aevo · Drift
- **dYdX** indexer: free 1m candles (from 2023-12) and tick trades; thin (~1k trades/20 h).
- **GMX** candles: 1m for ~2 weeks, 5m ~5 weeks, daily to 2021. Free.
- **Aevo `index-history`**: **1-second** CEX-spot composite index, free, no key, verified back to
  **Sep 2023**, 500 rows/request (`https://api.aevo.xyz/index-history?asset=BTC&resolution=1&start_time=<ns>&end_time=<ns>&limit=500`).
  Best free second-level spot reference found; occasional 2 s gaps. Not a settlement proxy.
- **Drift** (`data.velocity.exchange`): free tick CSVs incl. oracle price per fill, but BTC-PERP is nearly empty.

## TradFi

### CME Bitcoin futures via Databento (`GLBX.MDP3`, symbols BTC / MBT)
- **Gives:** trades, MBP-1 (L1), TBBO, MBO, OHLCV-1s, ns timestamps; BTC from 2017-12, MBT from 2021-04.
- **Cost:** per-GB, prices only visible after login; **$125 free credits** on signup (6 months) —
  one front-month contract's trades/MBP-1 is MB/day, so a year should fit; Standard plan $179–199/mo.

### ES (E-mini S&P 500) futures — see [ES futures](../data/es-futures.md)
- **Databento `GLBX.MDP3`** is the only self-serve per-second-or-finer source: `ohlcv-1s`,
  `trades`, `tbbo`, `mbp-1`, `mbo` for ES from 2010-06 (same dataset and script as BTC/MBT).
  Metered per GB — rates visible only with a key — **$125 free credit**; a year of `ohlcv-1s`
  is ~1.2 GB (estimate) and should fit, trades/BBO may not. `data_collection/es_futures.py`
  costs every request before spending.
- **Yahoo `ES=F`**: free 1-minute bars, last 8 days only (verified); 5m → 60 d; 1h → 2 y.
- **FirstRate Data**: 1-minute ES from 2008, one-off purchase (price not published on the page).
- **Massive (ex-Polygon.io)**: 1 s aggregates, trades, quotes, flat files for CME futures —
  free tier is end-of-day only, Starter $29/mo delayed; history depth unverified.
- Kaggle `choweric/cme-es`: daily per-contract OHLC/OI 2000–2022, free.

### Coinbase · Kraken spot
- **Coinbase Exchange** REST: every BTC-USD trade since **2015-01** (trade_id 1), 1000/page with
  `cb-after` cursor, free, no key. ~1.1B trades total.
- **Kraken**: REST `Trades?pair=XBTUSD&since=<ns>` from 2015 + free quarterly bulk OHLCVT CSVs.

### Yahoo Finance (`BTC=F`, `IBIT`, `BITO`, `BTC-USD`)
- 1-minute bars for the last 30 days only (≤ 7 days per request), 5m → 60 d, 1h → 2 y. Free, unofficial.

## Aggregator / API providers

Almost nothing free goes below 1-minute bars. Verified 2026-09-12.

| Provider | Best free granularity / depth | Cost beyond free | Verdict |
|---|---|---|---|
| **Coin Metrics Community** | **1 s** and 1 m `ReferenceRateUSD` — but only a **rolling 7 days**; daily since 2010. Market-level candles/trades/books are 403 on community | sales quote | run a daily collector if you want their 1 s rate |
| **Alpaca crypto** | 1 m bars from 2021, tick trades from 2021, L1 quotes from 2023-03 — **Alpaca's own thin venue** (~1k trades/day now), no key needed | $99/mo (irrelevant) | not a proxy for market-wide microstructure |
| **CryptoDataDownload** | Binance BTCUSDT **1 m OHLCV** per-year CSVs 2019-09 → now, plain GET (`…/cdd/Binance_BTCUSDT_2026_minute.csv`) | tick trade prints on "Plus+" | fine for 1 m history; klines archive is better |
| **Polygon.io → Massive** | 1 m aggregates, 2 y history, 5 calls/min, 15 min delayed | **Currencies Starter $49/mo**: second aggs, trades, quotes, flat files, 10+ y | cheapest paid route to tick trades + L1 quotes across venues |
| **Twelve Data** | 1 m OHLC labelled "Binance", deep history, 800 calls/day × 5,000 rows | $79/mo | ok for 1 m backfill |
| **CoinGecko** | 5 m only for the last 24 h; hourly ≤ 90 d; daily; 365 d cap without key | $129/mo; 1 m is enterprise-only | not useful |
| **CoinMarketCap** | 5 m intraday for 1 month, daily 1 y | $29–79/mo | not useful |
| **CoinDesk Data (ex-CryptoCompare)** | **free tier retired 2026-05-21** — all endpoints 401 | sales only | dead for us |
| **Messari · Bitquery · Glassnode** | hourly/daily; Bitquery free = 7-day trial; Glassnode free = web only | $39–999/mo | not useful |
| **Yahoo** | 1 m for 7 days | — | spot checks only |
| **Bitcoinity** | 1 m only for `timespan=24h`; daily back to 2011 | — | history only |
| Nomics · Cryptowatch · Finnhub crypto candles | dead / dead / premium-only | — | skip |

## At a glance

| Source | L1 granularity | Coverage | Cost |
|---|---|---|---|
| Binance archives | tick | 2023-05-16 → 2024-03-30 | free |
| `record_bookticker.py` | tick | from start | free |
| Tardis free days | tick | 1st of month, 2019-11 → | free |
| Tardis trial | tick | random 7–14 recent days | free |
| CryptoHFTData | L2 diffs, 100 → 52 → 26 ms over 2025–26 (L1 via replay) | 2025-06-28 → | free |
| Crypto Lake | 100 ms (tick via deltas) | 2022-11-14 → | $64 one month |
| CoinAPI | 100–250 ms → tick (2025?) | ~2019 → | ~$35–40 / per GiB |
| Tardis paid | tick | 2019-11-17 → | $350–700 / mo |
| Kaiko et al. | tick | long | $9.5k+ / yr |
| Polymarket RTDS | 1 s (settlement TWAP) | live only | free |
| Polymarket Gamma / Data API | market metadata, resolutions, tick fills | tick fills | ≥ 270 d (fills), full (metadata) | free |
| Polymarket CLOB prices-history | outcome prices | ~60 s | 2025-09 → | free |
| Polygon V2 OrderFilled logs (Tenderly) | every fill | tick | 2026-04 → | free |
| PMData Polymarket L2 | every book update, 5m/15m/1h | tick | 2026-02 → | free 1,200 files / $49 mo |
| PredictionTicks | BTC 5m books + ticks | ~1.5 s | 2026-02 → 2026-08 | $15 one-off |
| HF Polymarket dumps (kachoio, BrockMisner) | 1 s BBO / 10 s 10-level | 1–10 s | Jan–May 2026 | free |
| PMData | 1 s (settlement TWAP) | 2026-02 → | $49 / mo |
| Chainlink Data Streams | 1 s | 2024-03-19 → | $150 / mo |
| Polygon on-chain feed | ~30 s | 2020-10 → | free |
| Chainlink on BNB | ~31 s | since deploy | free |
| Gate.io L2 archives (perp + spot) | 100 ms snapshot + diffs | 2021-08 → | free |
| Bybit L2 archives | ~100 ms, 500/200 levels | 2023-01 → | free |
| OKX 400-lv L2 archives | ~10 ms updates | 2023-03 → | free |
| BitMEX L1 quotes | tick | 2014-11 → | free |
| Bybit / OKX / Gate / KuCoin / HTX / Bitget trades | tick | 2018–2023 → | free |
| Aevo index-history | 1 s spot composite | 2023-09 → | free |
| Coin Metrics community | 1 s reference rate, rolling 7 d | 7 days | free |
| Coinbase / Kraken REST | tick trades (spot) | 2015 → | free |
| Hyperliquid S3 | ~550 ms L2 snapshots, tick fills | 2023 → | ~$30 / yr egress |
| Databento CME (BTC/MBT) | MBP-1, trades, ns | 2017 → | $125 free credits, then $179+/mo |
| Databento CME ES | ohlcv-1s, trades, tbbo, mbp-1, mbo | 2010 → | same credit; ~1.2 GB/yr for 1s bars |
| Yahoo ES=F | 1 m bars | last 8 days | free |
| FirstRate Data ES | 1 m bars | 2008 → | one-off, price not shown |
| Massive futures | 1 s aggs, trades, quotes | ? | EOD free; $29+/mo |
| Massive (ex-Polygon.io) | second aggs, trades, quotes | 10+ y | $49 / mo |
| Pyth | 400 ms | full | $500 / mo — no |
