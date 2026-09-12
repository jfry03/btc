# L1 book: live recorder

**Module:** `data_collection.record_bookticker` · [reference](../reference/record-bookticker.md)
**Markets:** `um` USDⓈ-M futures (default), `cm` COIN-M futures, `spot`
**Output:** `data/raw/{SYMBOL}-bookTicker-{YYYY-MM-DD}.csv.gz`, one file per UTC day

Since Binance stopped publishing futures `bookTicker` archives on 2024-03-30, the only free way
to have continuous L1 data from here on is to record it yourself. This module subscribes to
`<symbol>@bookTicker` over WebSocket and writes the native stream to daily gzip CSVs in the
**same column layout and file naming as the archives** (plus a `local_time` column), so
[`bookticker.sample_book`](bookticker.md) picks them up with no changes.

## Running it

```bash
nohup python -m data_collection.record_bookticker --symbol BTCUSDT > /dev/null 2>&1 &
tail -f data/raw/BTCUSDT-bookTicker.recorder.log
```

For something that survives logout and reboots, a systemd user unit:

```ini
# ~/.config/systemd/user/bookticker.service
[Unit]
Description=Binance bookTicker recorder

[Service]
WorkingDirectory=/home/joshua/btc
ExecStart=/usr/bin/python3 -m data_collection.record_bookticker --symbol BTCUSDT
Restart=always

[Install]
WantedBy=default.target
```

```bash
systemctl --user enable --now bookticker
loginctl enable-linger $USER      # keep user services running after logout
```

| Flag | Default | |
|---|---|---|
| `--symbol` | `BTCUSDT` | |
| `--market` | `um` | `um`, `cm`, or `spot` |
| `--raw-dir` | `data/raw` | |

## Behaviour

- **Reconnects** on any drop with exponential backoff (1 s → 60 s). A 30 s silence is treated
  as a drop.
- **Rotates files at UTC midnight**, keyed on the exchange `transaction_time`, not local time.
  Restarting mid-day appends to the existing file (a second gzip member; Python's `gzip` reads
  these transparently, and so does `pyarrow.csv` via `gzip.open`).
- **Flushes every 5 s**, so a crash loses at most a few seconds.
- **Logs** to `data/raw/{SYMBOL}-bookTicker.recorder.log`: connections, per-minute row rate and
  last-message lag, and every disconnect as a `GAP:` line with timestamp. Grep that file to find
  holes in the data.
- `SIGINT` / `SIGTERM` close the file cleanly.

## Sizing

`BTCUSDT` perp is ~39M updates/day → **~350 MB/day** compressed, ~10 GB/month. Plan disk
accordingly; these files cannot be re-downloaded.

## Columns

Archive layout plus `local_time`:

```
update_id,best_bid_price,best_bid_qty,best_ask_price,best_ask_qty,transaction_time,event_time,local_time
```

`local_time` is receipt time on this machine in µs; `local_time/1000 − event_time` is
network + processing latency, which the log reports once a minute.

!!! note "Spot has no exchange timestamps"
    The spot `bookTicker` payload carries only `u`, `b`, `B`, `a`, `A`. For spot,
    `transaction_time` and `event_time` are both filled with local time.

## In production

The recorder runs on the VPS as a systemd service, with nightly [compaction](compact.md) and a
sync script that pulls finished days here — see [VPS deployment](../notes/vps.md).
