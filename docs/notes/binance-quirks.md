# Binance data quirks

Things about Binance's (and Tardis') public data that have already cost time, collected so they
only cost it once.

## Timestamp units changed in 2025

Spot kline archives switched `open_time` / `close_time` from **milliseconds to microseconds in
January 2025**. The REST API still returns milliseconds. `klines._normalise` treats any value
`> 1e15` as microseconds and divides by 1000, so everything downstream is milliseconds.

Tardis timestamps are µs. The recorder's `local_time` is µs. Everything else in this repo is ms.
If you add a new data type, check the unit before assuming.

## Some archive vintages have a header row

Older spot kline CSVs have no header; newer ones do. `klines.read_archive` sniffs the first
field — if it isn't all digits, it's a header. The `bookTicker` archives always have one.

## Klines skip empty bars

A bar is only emitted for intervals with at least one trade. Early history (2017–2019) is
sparse. See [gaps](../data/klines.md#gaps) for the reindexing helper.

## Futures have no 1s klines

Spot's finest interval is 1s; USDⓈ-M futures' is 1m. For sub-minute futures work you need
`bookTicker` or `aggTrades`.

## `bookTicker` archives stopped in March 2024

USDⓈ-M futures `bookTicker`: daily 2023-05-16 → 2024-03-30. The 2024-04 monthly file exists
but only contains the first ~7 hours of April 1. No spot `bookTicker` archives at all.
Everything after March 2024 comes from the [live recorder](../data/record-bookticker.md) or
[Tardis free days](../data/tardis.md).

## Two timestamps on futures L1 rows

`transaction_time` is when the matching engine applied the update; `event_time` is when Binance
published it. The gap is the exchange's internal latency. Backtests that condition on "what was
knowable" should sample on `event_time`. Tardis keeps only the event time, and the spot WebSocket
payload has neither — see the per-source notes in
[Data collection](../data/index.md#l1-sources-and-the-fallback-chain).

## Archive vs REST publication lag

Daily archives appear roughly a day in arrears. Anything newer must come from REST, at
1000 rows per call — fine for a tail, unusable for bulk.

## Rate limits

| Host | Limit |
|---|---|
| `data.binance.vision` | Unmetered, but be reasonable (`MAX_DL_WORKERS` 3–4) |
| `api.binance.com` / `fapi.binance.com` | 6000 request-weight / min / IP (spot); `x-mbx-used-weight-1m` header reports usage; 429/418 on breach |
| `stream.binance.com` / `fstream.binance.com` | WebSocket; server pings every 20 s, connections are dropped after 24 h — the recorder reconnects |
| `datasets.tardis.dev` | Free days only; no `HEAD`, no `Range` |

## Checksums

Every Binance archive has a sibling `.CHECKSUM` file containing its SHA-256. Tardis sends an
`x-md5` header. Both are verified and the download discarded on mismatch.

## Symbol naming

`BTCUSDT` on Binance.com is the liquid "USD" pair. `BTCUSD` exists only on Binance.US
(different venue, different archive host, much thinner). Tardis calls USDⓈ-M futures
`binance-futures` and COIN-M `binance-delivery`.
