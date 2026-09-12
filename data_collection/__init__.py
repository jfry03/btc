"""
Data-collection methods for the btc project. Every module is runnable as a script
(`python -m data_collection.<name> --help`) and importable as a library.

    bookticker         historical futures L1 (bookTicker) archives -> n-ms book state
    record_bookticker  live futures L1 recorder (WebSocket -> daily .csv.gz, archive-compatible)
    klines             spot/futures OHLCV candles, archive + REST tail
    tardis_free_days   Tardis.dev's free first-of-month book_ticker CSVs (post-2024 L1)

All paths are relative to the repo root (data/raw, data/parquet); run from there.
"""
