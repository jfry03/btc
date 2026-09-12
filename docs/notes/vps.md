# VPS deployment

The recorder runs 24/7 on the VPS (`ssh scraping-vps`, 1 vCPU / 1 GB / 20 GB, Ubuntu 26.04);
this machine pulls the finished days off it. Everything needed to rebuild the box is in
`deploy/vps/` and `scripts/`.

## What runs there

| Unit | What | Schedule |
|---|---|---|
| `bookticker-recorder.service` | `record_bookticker --symbol BTCUSDT --market um` → `/opt/btc/data/raw/` | always, `Restart=always` |
| `bookticker-compact.timer` → `.service` | `compact` — closed days `.csv.gz` → `.parquet` (see [compaction](../data/compact.md)) | 00:20 UTC daily, `nice 15`, idle I/O |
| `bookticker-status.service` | `status_site` on `127.0.0.1:8080` | always |
| `caddy.service` | HTTPS (Let's Encrypt) + basic auth in front of the status site | always |

Code lives in `/opt/btc` (a copy of `data_collection/` + `deploy/`), Python venv in
`/opt/btc/venv` with just `websockets` and `pyarrow`.

## Status site

**https://45-124-54-199.sslip.io** — user `admin`, password in `/root/btc-status.password` on
the VPS (`ssh scraping-vps cat /root/btc-status.password`). Shows recorder state, rows/s,
seconds since last write, reconnect gaps today, disk free, estimated MB/day and days until
full, the file list and the log tail; buttons start / stop / restart the recorder and trigger
a compaction. Auto-refreshes every 10 s. `GET /api/status` returns the same as JSON.

## Deploy / update

```bash
scripts/deploy_vps.sh          # rsync code, (re)install units, restart recorder + status site
```

Idempotent. Re-run after changing anything under `data_collection/` or `deploy/vps/`.
Restarting the recorder costs a reconnect gap of ~1 s, logged as a `GAP` line.

## Pulling data back

```bash
scripts/sync_vps.sh            # closed days -> data/raw/, verify, delete on VPS
scripts/sync_vps.sh --dry-run
```

Data-safety rules, all enforced by the script:

1. Only files for days **before today (UTC)** are considered — the recorder is still appending to
   today's.
2. `rsync --partial` — an interrupted transfer resumes next run; nothing is deleted on error.
3. A file is removed from the VPS only after its **SHA-256 on the VPS equals the local copy's**
   and the local copy passes an integrity read (`gzip -t`, or the Parquet footer parses).
4. A lock file prevents overlapping runs, so it's safe from cron every hour:
   ```
   0 * * * *  /home/joshua/btc/scripts/sync_vps.sh >> /home/joshua/btc/data/raw/sync-cron.log 2>&1
   ```
5. The recorder log is copied to `data/raw/vps-logs/` but never deleted on the VPS.

If compaction and sync overlap (compaction deletes a `.csv.gz` mid-transfer), rsync errors,
nothing is deleted, and the next run picks up the `.parquet`. Files land in `data/raw/`, which
is exactly where `bookticker.fetch_day` looks, so `sample_book` works on synced days immediately.

## How fast does it fill up?

14 GB free after cleanup. BTCUSDT perp ≈ 39M updates/day on a normal day.

| State of a day's file | Size | Days of disk |
|---|---|---|
| live gzip (`.csv.gz`) | ≈ 400 MB | 35 |
| compacted Parquet | ≈ 130 MB | **~105** |

So with nightly compaction the VPS holds about three months even if this machine never syncs;
with hourly/daily syncing it never holds more than a day or two. The status page's
"days until full" is computed from the actual recent file sizes. Volatile days (liquidation
cascades) can be 2× the average.
