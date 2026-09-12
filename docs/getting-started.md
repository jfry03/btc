# Getting started

## Environment

Python 3.10+ (the code uses `X | None` unions and the walrus operator).

```bash
git clone git@github.com:jfry03/btc.git
cd btc
pip install -r requirements.txt
```

`requirements.txt` is the full research stack. The collectors themselves only need `pandas`,
`numpy`, `pyarrow`, `requests` and (for the live recorder) `websockets`.

## Data directory

All paths are relative to the repo root — **run everything from there**.

| Path | Contents | Safe to delete? |
|---|---|---|
| `data/raw/` | Binance `.zip` archives, recorder `.csv.gz` output, recorder log | Archives: yes. Recorder output: **no** — it cannot be re-downloaded |
| `data/raw/tardis/` | Tardis.dev `.csv.gz` files | Yes — re-download on next run |
| `data/parquet/` | Output datasets | Only if you're happy to regenerate |

`data/` is git-ignored.

## First pulls

=== "Klines"

    ```bash
    python -m data_collection.klines --symbol BTCUSDT --interval 1s --start 2026-09-01 --end 2026-09-04
    python -m data_collection.klines --market um --interval 1m --start 2026-09-01 --end 2026-09-04
    ```

    Writes `data/parquet/BTCUSDT_spot_1s_20260901T0000_20260904T0000.parquet`. Days without an
    archive yet are filled from REST automatically.

=== "L1 book, historical"

    ```bash
    python -m data_collection.bookticker --start 2024-03-01 --end 2024-03-02 --step-ms 100
    ```

    Downloads the daily archive for 2024-03-01 (~350 MB), streams it, and writes
    `data/parquet/BTCUSDT_L1_100ms_20240301T0000_20240302T0000.parquet` — 864,000 rows.

=== "L1 book, Python"

    ```python
    from datetime import datetime, timezone
    from data_collection.bookticker import sample_book

    df = sample_book(
        "BTCUSDT",
        datetime(2024, 3, 1, tzinfo=timezone.utc),
        datetime(2024, 3, 2, tzinfo=timezone.utc),
        step_ms=100,
    )
    df[["bid", "ask", "mid", "spread", "imbalance", "n_updates"]].head()
    ```

=== "L1 book, live"

    ```bash
    nohup python -m data_collection.record_bookticker --symbol BTCUSDT > /dev/null 2>&1 &
    tail -f data/raw/BTCUSDT-bookTicker.recorder.log
    ```

    Leave it running. Files rotate at UTC midnight; `sample_book` picks them up automatically.

=== "Tardis free days"

    ```bash
    python -m data_collection.tardis_free_days --start 2024-04 --end 2026-10
    ```

    One file per 1st-of-month, ~60 MB each, into `data/raw/tardis/`.

## Reading outputs back

```python
import pandas as pd
df = pd.read_parquet("data/parquet/BTCUSDT_L1_100ms_20240301T0000_20240302T0000.parquet")
```

For many files, prefer a lazy scan:

```python
import polars as pl
lf = pl.scan_parquet("data/parquet/BTCUSDT_spot_1s_*.parquet")
```

## Building these docs

```bash
pip install -r requirements-docs.txt
mkdocs serve        # live preview at http://btc-docs
mkdocs build        # static site in site/
```

`mkdocs.yml` sets `dev_addr: 127.0.0.1:80`. That needs a one-off machine setup so the name
resolves and port 80 is bindable without root:

```bash
echo "127.0.0.1 btc-docs" | sudo tee -a /etc/hosts
echo "net.ipv4.ip_unprivileged_port_start=80" | sudo tee /etc/sysctl.d/99-unprivileged-ports.conf
sudo sysctl -p /etc/sysctl.d/99-unprivileged-ports.conf
```

On a machine without that setup, override the address: `mkdocs serve -a 127.0.0.1:8000`.

Pushes to `master` that touch `docs/`, `mkdocs.yml` or any `.py` file rebuild and deploy the
site to GitHub Pages via `.github/workflows/docs.yml`.
