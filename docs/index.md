# btc

Tooling for pulling Binance BTC market data into local files for research.

No API key is required. Everything here uses public endpoints: Binance's bulk archive host
[`data.binance.vision`](https://data.binance.vision), the public REST and WebSocket APIs, and
[Tardis.dev](https://tardis.dev)'s free first-of-month datasets. Nothing touches an account.

## What exists today

Everything lives in the `data_collection` package. Each module is importable as a library and
runnable as a script via `python -m data_collection.<name> --help`.

| Module | What it collects | Market | Coverage |
|---|---|---|---|
| [`klines`](data/klines.md) | OHLCV candles, archive + REST tail | spot (1s+) or USDⓈ-M futures (1m+) | 2017-08 → now |
| [`bookticker`](data/bookticker.md) | Historical L1 best bid/ask → book state on a fixed ms grid | USDⓈ-M futures | Any day with *some* source (below) |
| [`record_bookticker`](data/record-bookticker.md) | Live L1 recorder, WebSocket → daily `.csv.gz` | um / cm / spot | From whenever you start it |
| [`tardis_free_days`](data/tardis.md) | Tardis.dev free 1st-of-month `book_ticker` CSVs | binance-futures | 2019-11-17 → now, 1st of each month only |

`data_collection/binance_1s.ipynb` is the original walkthrough the `klines` module was extracted from; the module
is now canonical.

## L1 coverage

Binance stopped publishing futures `bookTicker` archives on 2024-03-30. `bookticker.sample_book`
therefore reads from whichever source has the day:

```
2019-11 ─────────────────────────────────────────────────────────────── now
   Tardis free days     ●        ●        ●        ●        ●        ●      (1st of each month)
   Binance archives              ████████████████████                       2023-05-16 → 2024-03-30
   Live recorder                                              ████████████  from first run
```

See [Data collection → Overview](data/index.md) for how the fallback works.

## Layout

```
btc/
├── data_collection/
│   ├── __init__.py
│   ├── bookticker.py           # historical L1 -> grid; multi-source fetch_day
│   ├── record_bookticker.py    # live L1 WebSocket recorder
│   ├── klines.py               # OHLCV, spot + futures, archive + REST
│   ├── tardis_free_days.py     # Tardis.dev free-day downloader
│   └── binance_1s.ipynb        # original klines walkthrough (notebook)
├── data/                       # git-ignored
│   ├── raw/                    #   archives, recorder output, recorder log
│   │   └── tardis/             #   Tardis CSVs
│   └── parquet/                #   outputs
├── docs/ · mkdocs.yml          # this site
├── requirements.txt            # research stack
└── requirements-docs.txt       # docs build only
```

## Where to go next

- [Getting started](getting-started.md) — environment and first pull.
- [Binance data quirks](notes/binance-quirks.md) — timestamp units, coverage gaps, rate limits. Read before trusting a dataset.
- [Reference](reference/bookticker.md) — API docs generated from source docstrings.
