# Source cross-validation

Do the different L1 / kline sources agree where they overlap? Run on **2026-09-12** with
`python -m data_collection.validate_sources --check N` (each check streams; peak memory < 1 GB).
Verdicts: **agree** · **agree with caveat** · **disagree**.

| # | Comparison | Verdict | One-line caveat |
|---|---|---|---|
| 1 | Binance `bookTicker` archive vs Tardis `book_ticker`, 2024-03-01 | **agree** | Tardis `timestamp` = Binance `T` (transaction time), not `E`; 0–1 ms jitter moves a few updates across second boundaries |
| 2 | CryptoHFTData L2 replay vs Tardis `book_ticker`, 2025-07-01 | **agree with caveat** | replay equals the native feed 98 % of the time; the rest is batching lag — CryptoHFTData's depth events were **100 ms apart in mid-2025** (52 ms from ~Oct 2025, 26 ms by Sep 2026), so the replay trails the tick feed by up to one batch |
| 3 | Spot 1 s klines vs perp L1 mid | **agree with caveat** | different instruments: perp sits ~4–5 bps *below* spot, returns correlate 0.79–0.80 |
| 4 | Tardis 2026-09-01 file is 4× bigger than 2026-08-01 | **not an anomaly** | it's a busy day (427 vs 95 rows/s); no duplicates, same format |
| 5 | Compacted Binance Parquet days, internal consistency (3 random days) | **agree** | row counts match the archive, monotonic, no NaNs, never crossed |
| all | **Every file on disk** (329 files: 252 archive days, 31 Tardis, 5 hft, 41 kline months) | **agree with caveats** | 11 Binance archive days are shipped **interleaved / out of time order** — now sorted on compaction (bug fixed); a few known Binance outage gaps; see below |

Two real bugs were found and fixed: CryptoHFTData hour files that are not in event order (check 2) and Binance archive days that are not in time order (all-files scan). Both would silently corrupt `sample_book` output.

## 1. Binance archive vs Tardis `book_ticker` (2024-03-01)

| | Binance archive | Tardis `book_ticker` |
|---|---|---|
| rows | 38,973,486 | 38,973,152 (−334, −0.001 %) |
| consecutive identical BBO rows | 0 | 0 |
| span | 00:00:00 → 23:59:59 | 00:00:00 → 23:59:59 |
| updates / s (mean, max) | 451, 3439 | 451, 3438 |
| seconds with no update | 2 | 2 |

- The first 200k rows of both files are the **identical BBO sequence** (100 % row-by-row equal on
  `bid, bid_qty, ask, ask_qty`). Tardis is the full native feed — not deduplicated, not sampled.
  (The "8–9M rows/day" figure quoted earlier in the survey came from a quiet day, 2026-08-01.)
- **Timestamps:** Tardis `timestamp` equals Binance `transaction_time` (`T`) exactly on 76 % of
  rows and is +1 ms on the rest; Binance `event_time` (`E`) is ~6 ms later (p5–p95: −12…−4 ms
  relative to Tardis). So the Tardis adapter's mapping (`timestamp → transaction_time`) is right;
  the docstring in `bookticker._tardis_to_archive` was corrected to say so. A row-level hash join
  on `(event_time, BBO)` only matched 4.4 % precisely because of this `T` vs `E` difference — not
  a data difference.
- Tardis `local_timestamp − timestamp`: median 4 ms, p99 30 ms (their collector's latency).
- **As-of 1 s grid** (`sample_book`, `ts_col=transaction_time` on both): bid and ask exactly equal
  at **99.65 %** of the 86,399 seconds, mid equal 99.59 %, quantities equal 97.3 %. The remaining
  0.35 % are 273 isolated single seconds where the 0–1 ms timestamp jitter moved one or more
  updates across the second boundary during a burst — visible as `n_updates` differing by a few
  (e.g. 947 vs 920). The largest, 687 ticks at 17:31:52, is a $68 jump that landed exactly on the
  boundary; on a 50 ms grid the two sources re-agree within 150 ms. Not a data quality issue, but
  worth knowing: **at second boundaries during bursts, as-of values can legitimately differ by
  the size of one burst between two feeds with 1 ms timestamp jitter.**

## 2. CryptoHFTData L2 replay vs Tardis `book_ticker` (2025-07-01)

| | hft replay | Tardis `book_ticker` |
|---|---|---|
| L1 states in the day | 743,345 (one per depth event in which L1 changed) | 20,682,145 |
| depth-event spacing | **p50 102 ms**, p90 202 ms, max 4.5 s | native, every change |
| crossed book | 0 | 0 |
| spread median | 0.10 | 0.10 |

As-of comparison (`load_l1(..., source="hft")` vs `source="tardis"`):

| grid | bid equal | ask equal | within 1 tick | within 5 ticks | max |bid diff| | bid_qty equal |
|---|---|---|---|---|---|---|
| 1 s (86,399 pts) | 98.07 % | 98.09 % | 98.19 % | 98.35 % | 436 ticks | 57.8 % |
| 100 ms (864,000 pts) | 98.06 % | 98.07 % | 98.17 % | 98.32 % | 937 ticks | 51.5 % |

- Mean difference +0.006 ticks: **no bias**, and the replay is never stale in the "wrong book"
  sense (never crossed; median spread identical).
- The 1.9 % of disagreeing points are 1,361 short episodes (longest 4 s) concentrated in the
  busiest hours (13–15 UTC: 428 of them) and in seconds with **~7× the usual update rate**
  (Tardis median 726 updates in those seconds vs 101 overall, while the hft event count stays
  at ~9/s). In 36 % of them the hft value equals Tardis **one second earlier**; shifting the
  100 ms grid shows equality peaking at a 0–100 ms lag and decaying after. I.e. the replay is
  exactly the depth stream's batching: **on 2025-07-01 CryptoHFTData's depth events are ~100 ms
  apart**, so during a fast move the replay shows the top of book up to one batch (100 ms) late.
  It is not missing data.
- **The batching interval changed over time** (sampled hour 12 of the 1st of each month):
  2025-07 → 102 ms, 2025-10 / 2026-01 / 2026-04 → 52 ms, 2026-09 → 26 ms. So the hft source is
  "100 ms depth-derived" for mid-2025, "50 ms" from about Oct 2025, and "26 ms" recently — the
  earlier survey text saying 26 ms throughout was based on a Sept 2026 file and is corrected here.
- Warm-up: bid equality in the first 60 s (98.3 %) is no worse than the rest of the day (98.1 %),
  so the 60 s empty-book warm-up is more than enough.
- Quantities agree only ~55 %: queue sizes change far more often than prices, so a batched view
  will rarely land on the same value as the tick feed. Use hft for prices/spread, not for
  queue-size features.

**Bug found and fixed in `cryptohft.py` (row order).** Some hour files are **not in event
order** — from `binance_futures/2025-06-30/13/` through at least 2025-07-02 (59 shuffled hours
logged so far: 11 on 06-30, all 24 on 07-01 and 07-02). `final_update_id`, `event_time` and
`received_time` are all non-monotonic in those files and the first 200k rows of 06-30/15 are all
asks. The replay assumed consecutive rows formed one event; on a shuffled file it fragmented
events and its best-level scans degenerated — the backfill sat on that hour for 15+ minutes at
76 % CPU. Fix: `replay_hour` checks `final_update_id` monotonicity and, if violated, stable-sorts
every column by it (each price appears at most once per event, so intra-event order doesn't
matter) and logs `rows not in event order - sorted`. The shuffled hour now replays in 3 s.

*Which hft days need redoing:* none. The two day files produced before the fix (2025-06-28,
2025-06-29) were regenerated after it and are **byte-identical** to the originals — all their
hours were already in order. Every later day was produced with the fix in place.

## 3. Spot 1 s klines vs perp L1 mid

Kline bar `[t, t+1)` close vs perp mid as-of `t+1` (end of the bar), 86,399 seconds each day.

| day | L1 source | basis perp − spot (bps): mean · std · p1 · p99 | corr of 1 s log-returns | xcorr peak |
|---|---|---|---|---|
| 2023-06-01 | Binance archive (compacted) | −4.05 · 1.00 · −6.23 · −1.88 | 0.791 | 0 s (+1 s: 0.163, −1 s: 0.126) |
| 2025-07-01 | Tardis | −4.63 · 0.68 · −6.23 · −3.10 | 0.785 | 0 s (+1 s: 0.196, −1 s: 0.129) |
| 2026-08-01 | Tardis | −4.56 · 0.57 · −5.80 · −3.23 | 0.802 | 0 s (+1 s: 0.126, −1 s: 0.115) |

- Consistent across three years and both L1 sources: the perp mid sits **~4–5 bps below** the
  spot close, with a tight ±1 bps band. (A persistent negative perp–spot basis on Binance
  BTCUSDT is plausible — the spot pair carries no funding and has a slightly different
  participant mix — but it is a real, stable offset to keep in mind when relating the two.)
- Returns correlate ~0.79 at 1 s; the cross-correlation is symmetric-ish with a slight edge to
  the perp leading (+1 s lag 0.13–0.20 vs −1 s 0.12–0.13). Verdict: **agree with caveat** —
  same price process, different instrument.

## 4. Tardis 2026-09-01 file size

| file | size | rows | rows/s | identical-BBO consecutive rows | exact duplicate rows |
|---|---|---|---|---|---|
| 2026-08-01 | 62 MB | 8,224,914 | 95 | 0.0 % | 0.0 % |
| 2026-09-01 | 251 MB | 36,920,623 | 427 | 0.0 % | 0.0 % |

Same 8 columns, no duplication, 24 h span in both. 2026-09-01 was simply busy (per-hour counts
0.9–3.1 M; 2026-08-01, a Saturday, 0.15–1.4 M). Tardis day-file size tracks activity: 2024-03-01
is 257 MB too. **Not an anomaly.**

## 5. Compacted Binance days (internal)

Three random days out of the 89 compacted at the time:

| day | rows | matches archive row count (backfill log) | `transaction_time` monotonic | `update_id` strictly increasing | NaNs | bid ≥ ask rows |
|---|---|---|---|---|---|---|
| 2023-06-30 | 17,126,526 | yes | yes | yes | 0 | 0 |
| 2023-07-11 | 10,549,648 | yes | yes | yes | 0 | 0 |
| 2023-07-29 | 7,108,989 | yes | yes | yes | 0 | 0 |

Together with the earlier bit-identity test (`sample_book` over Parquet vs gzip of the same day),
the compaction path is verified end-to-end.

## All files on disk

`python -m data_collection.validate_sources --check all-files --table-out docs/notes/_validation_files.md`,
run 2026-09-12 22:00 UTC+8 while the backfills were still going (files modified in the last
5 minutes or with a `.part` sibling are skipped as in-progress; none were hit). 329 files, all
readable, no NaN prices anywhere, **no crossed book anywhere**, spread p50 = 0.10 in every L1 file.

### Anomalies

**1. Eleven Binance archive days are not in time order (bug, fixed).** `2023-11-21, 11-22,
12-04, 12-05, 12-06, 12-11, 12-13, 12-18, 12-20, 12-29, 2024-01-02`. Verified against the
original zip for 2023-12-06: the CSV alternates rows from two time-ordered streams (row 1 is
00:00:00, row 2 is 23:28:56, row 3 is 00:00:00 …) — 11.4 M order inversions on 2023-11-21.
All 24 hours are present; the file is just interleaved. Because `sample_book` is a single
forward pass keyed on `transaction_time`, an interleaved day would have produced garbage as-of
values (which is exactly why the scan flagged "ends 352 min early" — the last row isn't the
latest row). Fix in `compact.py`: it now detects non-monotonic `transaction_time` while
streaming and, for those days, sorts the compacted Parquet by `update_id` (the engine's own
sequence) before verification (needs ~3.3 GB RAM for an 18.6 M-row day — fine here, and the
VPS never compacts archive files). `python -m data_collection.compact --resort [files]` applies
the same fix to already-compacted files; the 11 days above were re-sorted and re-verified.
The still-running archive backfill loaded the old code, so `--resort` over `data/raw/*.parquet`
should be run once more when it finishes (it's a no-op for ordered files).

**2. Binance-side gaps in the archive (not our bug).** `2023-05-16` starts 11:49 (first day of
the archive) and ends 21:44; `2023-05-17` starts 08:46 and has a 354 s hole; `2023-09-12` has a
20 min hole (1191 s); `2023-09-21` ends 22:15 and `2023-09-22` starts 05:20 — a ~7 h outage;
`2023-12-07` 470 s and `2024-01-03` 826 s holes. `sample_book` carries the last state across
them (`n_updates = 0`), so treat those windows as missing, not flat.

**3. Tardis day files start at 23:59:59 of the previous day** (5 of 31 flagged "timestamps
outside day"): Tardis cuts days on its own receive time, so the last few exchange-time ms of
the prior day leak in. Harmless — `sample_book` only uses them as the pre-midnight state.

**4. hft replay days have ≤13 ms backward steps in `transaction_time`** (3–5 per day). These
are Binance's `T` not being strictly ordered across consecutive depth events; `event_time` is
monotonic. Irrelevant on any grid ≥ 100 ms. `2025-06-28` starts at 05:00 because CryptoHFTData's
history begins mid-day; `2025-06-29/30`, `07-02` show 60–72 s holes — single missing/late depth
batches on their side. The scan also shows how thin the replay is: 0.5–0.8 M L1 states per day
vs 20 M+ in the tick feed (see check 2).

**5. Klines**: all 41 months clean (strictly increasing `open_time`, no NaN close, low ≤ high,
no gap > 1 h). Row counts equal seconds-in-month except the current month.

### Per-file table

??? note "329 files (click to expand)"

    Columns: rows; first/last = first/last `transaction_time` in the file (UTC); mono =
    `transaction_time` non-decreasing; uid_inc = `update_id` strictly increasing (blank for
    Tardis, which has no ids); max_gap_ms = largest jump between consecutive rows.
    Values are as scanned *before* the 11 archive days were re-sorted.

    | source | file | mb | status | rows | first | last | mono | uid_inc | nan | crossed | spread_p50 | spread_max | max_gap_ms | flags |
    |---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
    | archive/recorder | BTCUSDT-bookTicker-2023-05-16.parquet | 21.5 | ok | 4,494,018 | 11:49:47 | 21:44:00 | True | True | 0.0 | 0.0 | 0.10 | 7.30 | 1372.0 | starts 710 min late; ends 136 min early |
    | archive/recorder | BTCUSDT-bookTicker-2023-05-17.parquet | 35.7 | ok | 7,299,574 | 08:46:22 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 28.00 | 354368.0 | starts 526 min late; gap 354 s |
    | archive/recorder | BTCUSDT-bookTicker-2023-05-18.parquet | 54.9 | ok | 11,262,224 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 44.10 | 1731.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-05-19.parquet | 44.8 | ok | 9,234,229 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 9.30 | 2840.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-05-20.parquet | 30.4 | ok | 6,218,881 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 7.20 | 2641.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-05-21.parquet | 35.2 | ok | 7,121,436 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 23.00 | 2302.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-05-22.parquet | 43.0 | ok | 8,732,185 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 13.90 | 3079.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-05-23.parquet | 53.3 | ok | 11,678,760 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 14.00 | 2101.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-05-24.parquet | 59.2 | ok | 12,823,828 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 28.00 | 2384.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-05-25.parquet | 52.1 | ok | 10,738,437 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 26.40 | 1825.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-05-26.parquet | 61.6 | ok | 12,961,871 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 10.10 | 1269.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-05-27.parquet | 54.4 | ok | 11,689,238 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 7.00 | 808.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-05-28.parquet | 61.9 | ok | 12,989,401 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 36.10 | 791.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-05-29.parquet | 49.8 | ok | 10,142,362 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 14.50 | 1377.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-05-30.parquet | 49.6 | ok | 10,105,030 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 17.70 | 1952.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-05-31.parquet | 59.1 | ok | 12,388,180 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 27.00 | 3721.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-06-01.parquet | 53.4 | ok | 11,395,780 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 15.80 | 1820.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-06-02.parquet | 47.7 | ok | 9,578,967 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 22.40 | 3532.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-06-03.parquet | 31.8 | ok | 6,299,276 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 11.30 | 2814.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-06-04.parquet | 32.2 | ok | 6,375,038 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 8.80 | 2191.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-06-05.parquet | 57.8 | ok | 11,497,903 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 97.40 | 1967.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-06-06.parquet | 58.6 | ok | 11,753,941 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 34.30 | 2223.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-06-07.parquet | 60.2 | ok | 12,090,970 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 16.40 | 1757.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-06-08.parquet | 43.5 | ok | 8,841,717 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 13.30 | 4744.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-06-09.parquet | 41.7 | ok | 8,542,796 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 18.00 | 5198.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-06-10.parquet | 59.5 | ok | 12,003,912 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 20.20 | 3932.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-06-11.parquet | 41.6 | ok | 8,387,442 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 6.60 | 4033.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-06-12.parquet | 48.7 | ok | 9,964,667 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 8.20 | 4134.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-06-13.parquet | 55.5 | ok | 11,513,500 | 00:00:03 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 97.40 | 3081.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-06-14.parquet | 91.2 | ok | 28,328,074 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 45.20 | 4301.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-06-15.parquet | 84.7 | ok | 23,636,504 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 16.60 | 8005.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-06-16.parquet | 59.7 | ok | 12,811,936 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 43.30 | 4001.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-06-17.parquet | 47.2 | ok | 10,105,252 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 36.90 | 4065.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-06-18.parquet | 41.6 | ok | 8,733,471 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 16.40 | 4496.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-06-19.parquet | 48.1 | ok | 10,082,809 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 33.00 | 4289.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-06-20.parquet | 75.2 | ok | 16,220,737 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 23.40 | 4483.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-06-21.parquet | 90.9 | ok | 19,028,827 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 43.40 | 3597.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-06-22.parquet | 68.7 | ok | 14,354,930 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 33.40 | 3997.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-06-23.parquet | 72.9 | ok | 15,187,504 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 189.50 | 2969.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-06-24.parquet | 53.7 | ok | 11,216,285 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 20.10 | 3594.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-06-25.parquet | 55.0 | ok | 11,417,228 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 16.70 | 2384.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-06-26.parquet | 63.7 | ok | 13,335,914 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 64.50 | 2854.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-06-27.parquet | 61.4 | ok | 12,962,625 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 27.00 | 2925.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-06-28.parquet | 62.7 | ok | 13,173,251 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 40.70 | 2166.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-06-29.parquet | 60.3 | ok | 12,742,819 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 24.20 | 3772.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-06-30.parquet | 82.9 | ok | 17,126,526 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 70.10 | 2501.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-07-01.parquet | 43.8 | ok | 9,072,455 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 8.50 | 4043.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-07-02.parquet | 44.2 | ok | 9,007,796 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 15.30 | 2467.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-07-03.parquet | 52.2 | ok | 10,711,614 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 81.50 | 3952.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-07-04.parquet | 45.3 | ok | 9,185,335 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 38.70 | 4665.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-07-05.parquet | 49.3 | ok | 10,103,698 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 34.20 | 4078.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-07-06.parquet | 72.7 | ok | 15,014,271 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 28.00 | 2447.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-07-07.parquet | 54.2 | ok | 11,169,534 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 27.90 | 4582.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-07-08.parquet | 35.8 | ok | 7,274,870 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 9.80 | 3621.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-07-09.parquet | 37.2 | ok | 7,598,279 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 17.90 | 5067.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-07-10.parquet | 55.6 | ok | 11,432,347 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 28.40 | 4603.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-07-11.parquet | 50.2 | ok | 10,549,648 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 16.60 | 4372.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-07-12.parquet | 60.1 | ok | 13,057,968 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 60.20 | 3804.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-07-13.parquet | 72.5 | ok | 15,371,749 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 34.50 | 2233.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-07-14.parquet | 70.0 | ok | 14,917,725 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 23.90 | 2200.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-07-15.parquet | 39.4 | ok | 8,299,685 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 4.30 | 1935.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-07-16.parquet | 39.2 | ok | 8,064,602 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 26.20 | 3266.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-07-17.parquet | 53.1 | ok | 11,040,677 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 28.10 | 3519.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-07-18.parquet | 53.4 | ok | 11,208,517 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 66.90 | 2451.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-07-19.parquet | 50.6 | ok | 10,613,016 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 7.80 | 3153.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-07-20.parquet | 54.9 | ok | 11,587,818 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 73.90 | 3426.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-07-21.parquet | 43.7 | ok | 9,090,889 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 14.70 | 2394.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-07-22.parquet | 35.5 | ok | 7,348,702 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 19.00 | 2385.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-07-23.parquet | 40.4 | ok | 8,411,111 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 15.00 | 1912.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-07-24.parquet | 54.6 | ok | 11,440,977 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 100.20 | 1735.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-07-25.parquet | 42.9 | ok | 9,032,684 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 17.60 | 4022.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-07-26.parquet | 49.5 | ok | 10,304,902 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 25.30 | 3307.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-07-27.parquet | 46.8 | ok | 9,829,491 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 6.80 | 2819.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-07-28.parquet | 48.2 | ok | 10,312,261 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 18.50 | 5345.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-07-29.parquet | 33.5 | ok | 7,108,989 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 4.10 | 5269.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-07-30.parquet | 38.6 | ok | 8,173,889 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 13.00 | 3450.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-07-31.parquet | 46.4 | ok | 9,789,445 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 20.30 | 2612.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-08-01.parquet | 54.3 | ok | 11,487,743 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 62.60 | 3377.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-08-02.parquet | 59.0 | ok | 12,614,371 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 53.00 | 2852.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-08-03.parquet | 48.0 | ok | 10,245,837 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 11.80 | 3614.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-08-04.parquet | 47.2 | ok | 10,138,637 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 24.30 | 4896.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-08-05.parquet | 32.9 | ok | 6,991,131 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 2.90 | 5349.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-08-06.parquet | 35.7 | ok | 7,740,274 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 6.60 | 5445.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-08-07.parquet | 51.0 | ok | 10,871,968 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 14.00 | 3766.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-08-08.parquet | 64.8 | ok | 14,197,833 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 31.40 | 5273.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-08-09.parquet | 58.3 | ok | 12,999,252 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 8.50 | 5219.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-08-10.parquet | 49.0 | ok | 10,831,376 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 8.30 | 4838.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-08-11.parquet | 42.8 | ok | 9,449,640 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 17.30 | 5763.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-08-12.parquet | 29.5 | ok | 6,497,420 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 2.10 | 3948.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-08-13.parquet | 34.8 | ok | 7,670,486 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 13.10 | 3179.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-08-14.parquet | 54.0 | ok | 11,934,379 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 6.70 | 3110.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-08-15.parquet | 45.0 | ok | 9,904,366 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 39.00 | 5061.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-08-16.parquet | 54.9 | ok | 11,915,611 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 19.80 | 5068.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-08-17.parquet | 78.4 | ok | 16,548,828 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 209.90 | 4054.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-08-18.parquet | 68.3 | ok | 14,709,676 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 29.00 | 3898.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-08-19.parquet | 42.0 | ok | 9,067,895 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 12.00 | 4194.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-08-20.parquet | 35.2 | ok | 7,603,271 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 12.40 | 4362.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-08-21.parquet | 46.9 | ok | 10,146,165 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 42.80 | 4285.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-08-22.parquet | 52.4 | ok | 11,337,014 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 151.50 | 4493.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-08-23.parquet | 59.1 | ok | 12,793,512 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 39.40 | 3311.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-08-24.parquet | 49.6 | ok | 10,922,440 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 12.80 | 2862.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-08-25.parquet | 48.7 | ok | 10,557,613 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 14.60 | 2824.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-08-26.parquet | 24.6 | ok | 5,194,152 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 4.80 | 1436.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-08-27.parquet | 28.7 | ok | 6,078,897 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 2.40 | 1245.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-08-28.parquet | 42.4 | ok | 9,154,044 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 11.40 | 4063.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-08-29.parquet | 64.2 | ok | 13,720,426 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 84.00 | 2815.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-08-30.parquet | 54.0 | ok | 11,998,021 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 51.20 | 4299.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-08-31.parquet | 60.0 | ok | 13,110,447 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 49.80 | 4349.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-09-01.parquet | 53.6 | ok | 11,711,582 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 26.00 | 3195.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-09-02.parquet | 29.7 | ok | 6,338,127 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 6.20 | 4131.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-09-03.parquet | 31.3 | ok | 6,726,361 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 19.90 | 4046.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-09-04.parquet | 37.9 | ok | 8,369,198 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 12.00 | 3615.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-09-05.parquet | 41.3 | ok | 9,067,948 | 00:00:04 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 18.80 | 1960.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-09-06.parquet | 42.2 | ok | 9,222,717 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 25.30 | 1971.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-09-07.parquet | 35.2 | ok | 7,417,596 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 31.50 | 3972.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-09-08.parquet | 34.1 | ok | 7,122,869 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 17.20 | 4164.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-09-09.parquet | 18.9 | ok | 3,867,689 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 2.20 | 4292.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-09-10.parquet | 26.5 | ok | 5,511,031 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 21.10 | 4301.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-09-11.parquet | 44.9 | ok | 9,379,630 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 34.10 | 2475.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-09-12.parquet | 55.8 | ok | 12,017,782 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 130.00 | 1190560.0 | gap 1191 s |
    | archive/recorder | BTCUSDT-bookTicker-2023-09-13.parquet | 68.8 | ok | 17,948,009 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 15.00 | 3014.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-09-14.parquet | 68.0 | ok | 17,639,951 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 38.50 | 3117.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-09-15.parquet | 50.6 | ok | 12,583,798 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 25.00 | 3932.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-09-16.parquet | 33.7 | ok | 8,090,754 | 00:00:02 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 18.50 | 3653.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-09-17.parquet | 27.7 | ok | 6,456,304 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 11.40 | 4315.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-09-18.parquet | 62.6 | ok | 14,959,344 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 34.70 | 1460.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-09-19.parquet | 58.9 | ok | 14,133,936 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 16.40 | 2219.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-09-20.parquet | 56.7 | ok | 13,726,532 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 27.10 | 4253.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-09-21.parquet | 43.3 | ok | 9,243,066 | 00:00:00 | 22:14:31 | True | True | 0.0 | 0.0 | 0.10 | 15.30 | 229009.0 | ends 105 min early; gap 229 s |
    | archive/recorder | BTCUSDT-bookTicker-2023-09-22.parquet | 25.4 | ok | 5,404,588 | 05:19:50 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 17.20 | 4369.0 | starts 320 min late |
    | archive/recorder | BTCUSDT-bookTicker-2023-09-23.parquet | 20.5 | ok | 4,264,838 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 1.10 | 4030.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-09-24.parquet | 26.7 | ok | 5,715,885 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 51.10 | 4838.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-09-25.parquet | 41.4 | ok | 8,964,696 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 11.60 | 3406.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-09-26.parquet | 35.5 | ok | 7,740,828 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 10.10 | 5046.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-09-27.parquet | 46.8 | ok | 10,150,570 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 39.70 | 4967.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-09-28.parquet | 57.0 | ok | 12,857,674 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 23.90 | 4823.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-09-29.parquet | 60.3 | ok | 15,512,809 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 29.10 | 3383.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-09-30.parquet | 44.1 | ok | 12,035,650 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 5.50 | 3236.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-10-01.parquet | 39.1 | ok | 8,620,481 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 192.10 | 3556.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-10-02.parquet | 69.0 | ok | 15,075,560 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 33.70 | 3232.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-10-03.parquet | 67.0 | ok | 17,204,135 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 20.80 | 3933.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-10-04.parquet | 61.9 | ok | 15,899,852 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 16.00 | 3957.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-10-05.parquet | 57.7 | ok | 14,652,305 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 21.50 | 4022.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-10-06.parquet | 68.3 | ok | 17,054,459 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 172.50 | 4585.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-10-07.parquet | 38.8 | ok | 9,932,307 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 6.60 | 3671.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-10-08.parquet | 46.7 | ok | 12,031,402 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 15.90 | 3752.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-10-09.parquet | 60.7 | ok | 15,185,670 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 21.80 | 1978.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-10-10.parquet | 54.5 | ok | 13,525,256 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 9.00 | 4026.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-10-11.parquet | 69.9 | ok | 17,636,493 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 60.60 | 3822.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-10-12.parquet | 56.8 | ok | 14,299,125 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 10.90 | 3993.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-10-13.parquet | 53.3 | ok | 13,605,037 | 00:00:04 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 16.60 | 3642.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-10-14.parquet | 28.9 | ok | 6,908,972 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 2.60 | 4333.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-10-15.parquet | 33.9 | ok | 7,613,922 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 52.80 | 3911.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-10-16.parquet | 67.2 | ok | 14,124,758 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 181.40 | 4169.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-10-17.parquet | 50.2 | ok | 10,641,432 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 22.70 | 4161.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-10-18.parquet | 42.5 | ok | 8,841,008 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 95.80 | 3840.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-10-19.parquet | 47.5 | ok | 9,950,931 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 19.10 | 4455.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-10-20.parquet | 57.4 | ok | 11,690,915 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 73.90 | 4578.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-10-21.parquet | 39.1 | ok | 7,983,341 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 13.30 | 4574.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-10-22.parquet | 36.7 | ok | 7,512,565 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 17.30 | 4677.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-10-23.parquet | 83.7 | ok | 16,873,909 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 448.80 | 3640.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-10-24.parquet | 87.9 | ok | 17,458,159 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 55.00 | 3583.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-10-25.parquet | 71.1 | ok | 14,877,661 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 35.00 | 4342.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-10-26.parquet | 63.2 | ok | 13,379,025 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 28.20 | 4561.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-10-27.parquet | 52.5 | ok | 10,965,961 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 35.30 | 4731.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-10-28.parquet | 32.5 | ok | 6,602,773 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 32.90 | 4724.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-10-29.parquet | 37.6 | ok | 7,729,928 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 21.90 | 4407.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-10-30.parquet | 48.0 | ok | 10,031,754 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 44.10 | 3126.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-10-31.parquet | 45.9 | ok | 9,422,673 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 27.20 | 4771.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-11-01.parquet | 56.8 | ok | 11,724,531 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 49.10 | 4502.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-11-02.parquet | 59.4 | ok | 12,248,266 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 121.70 | 4657.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-11-03.parquet | 47.2 | ok | 9,657,714 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 26.70 | 4168.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-11-04.parquet | 27.6 | ok | 5,471,534 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 56.00 | 4124.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-11-05.parquet | 37.6 | ok | 7,510,945 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 59.10 | 1557.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-11-06.parquet | 39.6 | ok | 8,040,031 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 22.60 | 4475.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-11-07.parquet | 48.4 | ok | 9,701,662 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 54.50 | 1242.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-11-08.parquet | 43.8 | ok | 8,799,660 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 47.50 | 4605.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-11-09.parquet | 77.5 | ok | 15,397,406 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 553.60 | 2471.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-11-10.parquet | 56.1 | ok | 11,564,290 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 37.10 | 2733.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-11-11.parquet | 37.7 | ok | 7,606,944 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 49.20 | 4277.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-11-12.parquet | 31.6 | ok | 6,337,452 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 12.80 | 2275.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-11-13.parquet | 48.9 | ok | 9,966,919 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 24.60 | 4263.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-11-14.parquet | 51.9 | ok | 10,450,900 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 176.40 | 3258.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-11-15.parquet | 57.3 | ok | 11,714,235 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 365.30 | 3509.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-11-16.parquet | 64.8 | ok | 13,315,663 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 30.00 | 3326.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-11-17.parquet | 52.8 | ok | 10,746,490 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 86.10 | 3200.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-11-18.parquet | 30.9 | ok | 6,230,543 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 13.30 | 3209.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-11-19.parquet | 30.9 | ok | 6,182,951 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 39.30 | 2407.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-11-20.parquet | 56.5 | ok | 12,046,442 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 72.80 | 3595.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-11-21.parquet | 253.2 | ok | 29,710,851 | 00:00:00 | 18:08:29 | False | False | 0.0 | 0.0 | 0.10 | 40.10 | 65309759.0 | ends 352 min early; non-monotonic time; gap 65310 s |
    | archive/recorder | BTCUSDT-bookTicker-2023-11-22.parquet | 160.4 | ok | 23,832,257 | 00:00:00 | 18:04:01 | False | False | 0.0 | 0.0 | 0.10 | 20.60 | 69164399.0 | ends 356 min early; non-monotonic time; gap 69164 s |
    | archive/recorder | BTCUSDT-bookTicker-2023-11-23.parquet | 51.1 | ok | 11,003,519 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 28.90 | 1952.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-11-24.parquet | 61.2 | ok | 13,058,140 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 55.10 | 1474.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-11-25.parquet | 30.9 | ok | 6,276,311 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 15.30 | 2654.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-11-26.parquet | 41.2 | ok | 8,732,541 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 33.30 | 2727.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-11-27.parquet | 64.2 | ok | 14,601,622 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 33.80 | 2468.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-11-28.parquet | 68.4 | ok | 15,615,077 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 19.10 | 1590.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-11-29.parquet | 63.1 | ok | 14,418,489 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 40.80 | 2505.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-11-30.parquet | 54.8 | ok | 12,459,642 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 33.80 | 2758.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-12-01.parquet | 73.3 | ok | 16,942,636 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 28.60 | 2953.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-12-02.parquet | 44.2 | ok | 9,711,375 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 119.50 | 1420.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-12-03.parquet | 49.9 | ok | 11,145,026 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 30.70 | 2208.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-12-04.parquet | 145.1 | ok | 22,495,295 | 00:00:00 | 18:47:20 | False | False | 0.0 | 0.0 | 0.10 | 49.60 | 71950705.0 | ends 313 min early; non-monotonic time; gap 71951 s |
    | archive/recorder | BTCUSDT-bookTicker-2023-12-05.parquet | 136.9 | ok | 21,752,304 | 00:00:00 | 20:34:47 | False | False | 0.0 | 0.0 | 0.10 | 73.60 | 74255923.0 | ends 205 min early; non-monotonic time; gap 74256 s |
    | archive/recorder | BTCUSDT-bookTicker-2023-12-06.parquet | 82.9 | ok | 18,570,492 | 00:00:00 | 23:28:56 | False | False | 0.0 | 0.0 | 0.10 | 42.90 | 85451039.0 | ends 31 min early; non-monotonic time; gap 85451 s |
    | archive/recorder | BTCUSDT-bookTicker-2023-12-07.parquet | 73.6 | ok | 16,988,763 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 44.70 | 469904.0 | gap 470 s |
    | archive/recorder | BTCUSDT-bookTicker-2023-12-08.parquet | 67.2 | ok | 15,405,958 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 42.60 | 2223.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-12-09.parquet | 50.3 | ok | 11,219,839 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 28.00 | 2978.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-12-10.parquet | 40.9 | ok | 8,985,984 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 21.40 | 1458.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-12-11.parquet | 136.2 | ok | 21,887,111 | 00:00:00 | 19:31:49 | False | False | 0.0 | 0.0 | 0.10 | 179.80 | 74700911.0 | ends 268 min early; non-monotonic time; gap 74701 s |
    | archive/recorder | BTCUSDT-bookTicker-2023-12-12.parquet | 74.8 | ok | 17,270,578 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 26.70 | 2695.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-12-13.parquet | 86.2 | ok | 18,807,130 | 00:00:00 | 23:23:28 | False | False | 0.0 | 0.0 | 0.10 | 51.60 | 84208569.0 | ends 37 min early; non-monotonic time; gap 84209 s |
    | archive/recorder | BTCUSDT-bookTicker-2023-12-14.parquet | 72.4 | ok | 16,754,851 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 250.30 | 1470.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-12-15.parquet | 60.2 | ok | 13,814,038 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 44.60 | 2753.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-12-16.parquet | 44.4 | ok | 9,955,100 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 21.60 | 2826.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-12-17.parquet | 54.8 | ok | 12,593,154 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 29.30 | 1144.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-12-18.parquet | 84.2 | ok | 18,753,978 | 00:00:00 | 23:22:19 | False | False | 0.0 | 0.0 | 0.10 | 36.60 | 84889007.0 | ends 38 min early; non-monotonic time; gap 84889 s |
    | archive/recorder | BTCUSDT-bookTicker-2023-12-19.parquet | 76.2 | ok | 18,082,198 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 35.40 | 2720.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-12-20.parquet | 87.6 | ok | 18,983,636 | 00:00:02 | 22:52:33 | False | False | 0.0 | 0.0 | 0.10 | 32.80 | 82475198.0 | ends 67 min early; non-monotonic time; gap 82475 s |
    | archive/recorder | BTCUSDT-bookTicker-2023-12-21.parquet | 73.3 | ok | 17,429,494 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 23.80 | 2166.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-12-22.parquet | 65.2 | ok | 15,208,907 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 37.50 | 2739.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-12-23.parquet | 39.4 | ok | 8,749,207 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 29.60 | 1768.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-12-24.parquet | 47.0 | ok | 10,626,324 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 93.40 | 2457.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-12-25.parquet | 54.6 | ok | 12,827,953 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 28.40 | 1861.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-12-26.parquet | 65.8 | ok | 15,332,893 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 141.90 | 1599.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-12-27.parquet | 70.5 | ok | 16,884,204 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 24.70 | 2779.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-12-28.parquet | 74.0 | ok | 17,722,344 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 33.10 | 2149.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-12-29.parquet | 82.6 | ok | 18,719,998 | 00:00:00 | 23:20:45 | False | False | 0.0 | 0.0 | 0.10 | 45.70 | 84463218.0 | ends 39 min early; non-monotonic time; gap 84463 s |
    | archive/recorder | BTCUSDT-bookTicker-2023-12-30.parquet | 55.4 | ok | 13,194,720 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 26.60 | 3177.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2023-12-31.parquet | 50.8 | ok | 11,975,757 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 37.40 | 3054.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2024-01-01.parquet | 51.7 | ok | 11,986,235 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 17.80 | 2662.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2024-01-02.parquet | 83.1 | ok | 18,518,655 | 00:00:00 | 23:38:29 | False | False | 0.0 | 0.0 | 0.10 | 87.90 | 85936060.0 | ends 22 min early; non-monotonic time; gap 85936 s |
    | archive/recorder | BTCUSDT-bookTicker-2024-01-03.parquet | 72.1 | ok | 15,491,931 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 169.90 | 826416.0 | gap 826 s |
    | archive/recorder | BTCUSDT-bookTicker-2024-01-04.parquet | 68.1 | ok | 15,415,254 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 69.90 | 2652.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2024-01-05.parquet | 67.4 | ok | 15,319,837 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 354.10 | 2686.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2024-01-06.parquet | 44.6 | ok | 10,709,154 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 31.20 | 2393.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2024-01-07.parquet | 51.4 | ok | 12,100,941 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 32.10 | 1311.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2024-01-08.parquet | 84.8 | ok | 19,245,805 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 82.50 | 1366.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2024-01-09.parquet | 89.6 | ok | 22,767,271 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 329.30 | 1623.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2024-01-10.parquet | 104.7 | ok | 27,295,396 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 193.80 | 1760.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2024-01-11.parquet | 110.6 | ok | 33,466,411 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 56.20 | 2505.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2024-01-12.parquet | 109.5 | ok | 33,647,896 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 148.50 | 1217.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2024-01-13.parquet | 69.6 | ok | 24,881,894 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 37.60 | 1733.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2024-01-14.parquet | 59.2 | ok | 20,023,202 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 99.50 | 2266.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2024-01-15.parquet | 65.8 | ok | 19,881,490 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 100.90 | 2817.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2024-01-16.parquet | 67.8 | ok | 20,198,595 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 41.90 | 2008.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2024-01-17.parquet | 66.1 | ok | 20,424,568 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 33.20 | 2290.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2024-01-18.parquet | 95.3 | ok | 31,157,880 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 41.30 | 2852.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2024-01-19.parquet | 102.0 | ok | 33,995,066 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 46.40 | 1699.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2024-01-20.parquet | 50.4 | ok | 16,627,979 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 15.60 | 2704.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2024-01-21.parquet | 44.0 | ok | 14,836,148 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 15.60 | 2944.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2024-01-22.parquet | 109.0 | ok | 33,181,537 | 00:00:00 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 74.40 | 2602.0 |  |
    | archive/recorder | BTCUSDT-bookTicker-2024-01-23.parquet | 108.4 | in-progress (skipped) |  |  |  | nan | nan | nan | nan |  |  | nan |  |
    | archive/recorder | BTCUSDT-bookTicker-2024-01-24.parquet | 87.6 | in-progress (skipped) |  |  |  | nan | nan | nan | nan |  |  | nan |  |
    | archive/recorder | BTCUSDT-bookTicker-2024-01-25.parquet | 73.9 | in-progress (skipped) |  |  |  | nan | nan | nan | nan |  |  | nan |  |
    | archive/recorder | BTCUSDT-bookTicker-2024-01-26.parquet | 93.5 | in-progress (skipped) |  |  |  | nan | nan | nan | nan |  |  | nan |  |
    | archive/recorder | BTCUSDT-bookTicker-2024-01-27.parquet | 50.8 | in-progress (skipped) |  |  |  | nan | nan | nan | nan |  |  | nan |  |
    | archive/recorder | BTCUSDT-bookTicker-2024-01-28.parquet | 69.5 | in-progress (skipped) |  |  |  | nan | nan | nan | nan |  |  | nan |  |
    | tardis | binance-futures_book_ticker_2024-03-01_BTCUSDT.csv.gz | 257.3 | ok | 38,973,152 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 | 0.10 | 49.80 | 2191.0 |  |
    | tardis | binance-futures_book_ticker_2024-04-01_BTCUSDT.csv.gz | 107.9 | ok | 12,837,492 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 | 0.10 | 52.90 | 1999.0 |  |
    | tardis | binance-futures_book_ticker_2024-05-01_BTCUSDT.csv.gz | 191.4 | ok | 24,341,505 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 | 0.10 | 248.00 | 2622.0 |  |
    | tardis | binance-futures_book_ticker_2024-06-01_BTCUSDT.csv.gz | 50.3 | ok | 5,596,728 | 23:59:59 | 23:59:59 | True | None | 0.0 | 0.0 | 0.10 | 36.20 | 1263.0 | timestamps outside day |
    | tardis | binance-futures_book_ticker_2024-07-01_BTCUSDT.csv.gz | 108.4 | ok | 13,488,347 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 | 0.10 | 177.30 | 2919.0 |  |
    | tardis | binance-futures_book_ticker_2024-08-01_BTCUSDT.csv.gz | 184.0 | ok | 23,427,125 | 23:59:59 | 23:59:59 | True | None | 0.0 | 0.0 | 0.10 | 115.30 | 2929.0 | timestamps outside day |
    | tardis | binance-futures_book_ticker_2024-09-01_BTCUSDT.csv.gz | 149.9 | ok | 20,327,324 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 | 0.10 | 250.00 | 3157.0 |  |
    | tardis | binance-futures_book_ticker_2024-10-01_BTCUSDT.csv.gz | 187.3 | ok | 24,067,980 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 | 0.10 | 125.20 | 3828.0 |  |
    | tardis | binance-futures_book_ticker_2024-11-01_BTCUSDT.csv.gz | 157.6 | ok | 20,028,781 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 | 0.10 | 172.40 | 3982.0 |  |
    | tardis | binance-futures_book_ticker_2024-12-01_BTCUSDT.csv.gz | 90.3 | ok | 10,639,810 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 | 0.10 | 118.30 | 5655.0 |  |
    | tardis | binance-futures_book_ticker_2025-01-01_BTCUSDT.csv.gz | 77.2 | ok | 9,509,014 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 | 0.10 | 162.70 | 4995.0 |  |
    | tardis | binance-futures_book_ticker_2025-02-01_BTCUSDT.csv.gz | 75.7 | ok | 8,908,390 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 | 0.10 | 118.50 | 5479.0 |  |
    | tardis | binance-futures_book_ticker_2025-03-01_BTCUSDT.csv.gz | 138.2 | ok | 18,119,386 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 | 0.10 | 114.30 | 4209.0 |  |
    | tardis | binance-futures_book_ticker_2025-04-01_BTCUSDT.csv.gz | 180.6 | ok | 23,706,764 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 | 0.10 | 100.10 | 2841.0 |  |
    | tardis | binance-futures_book_ticker_2025-05-01_BTCUSDT.csv.gz | 130.5 | ok | 17,107,176 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 | 0.10 | 120.10 | 4463.0 |  |
    | tardis | binance-futures_book_ticker_2025-06-01_BTCUSDT.csv.gz | 121.7 | ok | 15,781,865 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 | 0.10 | 124.50 | 4670.0 |  |
    | tardis | binance-futures_book_ticker_2025-07-01_BTCUSDT.csv.gz | 152.1 | ok | 20,682,145 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 | 0.10 | 82.00 | 4440.0 |  |
    | tardis | binance-futures_book_ticker_2025-08-01_BTCUSDT.csv.gz | 306.8 | ok | 43,729,440 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 | 0.10 | 167.90 | 1712.0 |  |
    | tardis | binance-futures_book_ticker_2025-09-01_BTCUSDT.csv.gz | 261.1 | ok | 37,154,019 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 | 0.10 | 108.70 | 1068.0 |  |
    | tardis | binance-futures_book_ticker_2025-10-01_BTCUSDT.csv.gz | 278.0 | ok | 40,090,044 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 | 0.10 | 147.00 | 747.0 |  |
    | tardis | binance-futures_book_ticker_2025-11-01_BTCUSDT.csv.gz | 100.7 | ok | 13,435,971 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 | 0.10 | 76.20 | 1835.0 |  |
    | tardis | binance-futures_book_ticker_2025-12-01_BTCUSDT.csv.gz | 360.4 | ok | 52,633,970 | 23:59:59 | 23:59:59 | True | None | 0.0 | 0.0 | 0.10 | 374.30 | 1255.0 | timestamps outside day |
    | tardis | binance-futures_book_ticker_2026-01-01_BTCUSDT.csv.gz | 58.2 | ok | 7,629,432 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 | 0.10 | 71.00 | 2048.0 |  |
    | tardis | binance-futures_book_ticker_2026-02-01_BTCUSDT.csv.gz | 290.8 | ok | 42,679,707 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 | 0.10 | 132.50 | 1643.0 |  |
    | tardis | binance-futures_book_ticker_2026-03-01_BTCUSDT.csv.gz | 257.3 | ok | 36,953,946 | 23:59:59 | 23:59:59 | True | None | 0.0 | 0.0 | 0.10 | 869.10 | 1579.0 | timestamps outside day |
    | tardis | binance-futures_book_ticker_2026-04-01_BTCUSDT.csv.gz | 258.2 | ok | 37,811,406 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 | 0.10 | 100.30 | 1140.0 |  |
    | tardis | binance-futures_book_ticker_2026-05-01_BTCUSDT.csv.gz | 152.4 | ok | 21,532,653 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 | 0.10 | 62.00 | 12373.0 |  |
    | tardis | binance-futures_book_ticker_2026-06-01_BTCUSDT.csv.gz | 225.5 | ok | 33,465,252 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 | 0.10 | 164.40 | 2201.0 |  |
    | tardis | binance-futures_book_ticker_2026-07-01_BTCUSDT.csv.gz | 271.9 | ok | 40,459,753 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 | 0.10 | 120.10 | 1859.0 |  |
    | tardis | binance-futures_book_ticker_2026-08-01_BTCUSDT.csv.gz | 62.2 | ok | 8,224,914 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 | 0.10 | 51.10 | 1943.0 |  |
    | tardis | binance-futures_book_ticker_2026-09-01_BTCUSDT.csv.gz | 250.8 | ok | 36,920,623 | 23:59:59 | 23:59:59 | True | None | 0.0 | 0.0 | 0.10 | 129.60 | 1193.0 | timestamps outside day |
    | hft | BTCUSDT-bookTicker-2025-06-28.parquet | 4.7 | ok | 530,243 | 05:01:00 | 23:59:59 | False | True | 0.0 | 0.0 | 0.10 | 2.40 | 4512.0 | starts 301 min late; non-monotonic time |
    | hft | BTCUSDT-bookTicker-2025-06-29.parquet | 6.4 | ok | 704,786 | 00:00:59 | 23:59:59 | False | True | 0.0 | 0.0 | 0.10 | 5.80 | 60163.0 | non-monotonic time; gap 60 s |
    | hft | BTCUSDT-bookTicker-2025-06-30.parquet | 6.8 | ok | 732,902 | 23:59:59 | 23:59:59 | False | True | 0.0 | 0.0 | 0.10 | 8.20 | 71725.0 | timestamps outside day; non-monotonic time; gap 72 s |
    | hft | BTCUSDT-bookTicker-2025-07-01.parquet | 6.9 | ok | 743,345 | 23:59:59 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 10.20 | 4537.0 | timestamps outside day |
    | hft | BTCUSDT-bookTicker-2025-07-02.parquet | 7.1 | ok | 765,720 | 23:59:59 | 23:59:59 | True | True | 0.0 | 0.0 | 0.10 | 82.10 | 70455.0 | timestamps outside day; gap 70 s |
    | klines | BTCUSDT_spot_1s_2023-05.parquet | 88.3 | ok | 2,678,400 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2023-06.parquet | 86.0 | ok | 2,592,000 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2023-07.parquet | 78.6 | ok | 2,678,400 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2023-08.parquet | 77.9 | ok | 2,678,400 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2023-09.parquet | 76.3 | ok | 2,592,000 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2023-10.parquet | 86.8 | ok | 2,678,400 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2023-11.parquet | 89.7 | ok | 2,592,000 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2023-12.parquet | 95.0 | ok | 2,678,400 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2024-01.parquet | 97.1 | ok | 2,678,400 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2024-02.parquet | 89.6 | ok | 2,505,600 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2024-03.parquet | 106.5 | ok | 2,678,400 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2024-04.parquet | 96.5 | ok | 2,592,000 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2024-05.parquet | 92.5 | ok | 2,678,400 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2024-06.parquet | 83.9 | ok | 2,592,000 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2024-07.parquet | 92.1 | ok | 2,678,400 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2024-08.parquet | 94.1 | ok | 2,678,400 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2024-09.parquet | 85.0 | ok | 2,592,000 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2024-10.parquet | 89.6 | ok | 2,678,400 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2024-11.parquet | 99.5 | ok | 2,592,000 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2024-12.parquet | 100.7 | ok | 2,678,400 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2025-01.parquet | 97.3 | ok | 2,678,400 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2025-02.parquet | 87.0 | ok | 2,419,200 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2025-03.parquet | 98.0 | ok | 2,678,400 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2025-04.parquet | 89.7 | ok | 2,592,000 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2025-05.parquet | 89.1 | ok | 2,678,400 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2025-06.parquet | 82.1 | ok | 2,592,000 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2025-07.parquet | 84.9 | ok | 2,678,400 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2025-08.parquet | 86.9 | ok | 2,678,400 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2025-09.parquet | 79.7 | ok | 2,592,000 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2025-10.parquet | 92.1 | ok | 2,678,400 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2025-11.parquet | 94.3 | ok | 2,592,000 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2025-12.parquet | 86.8 | ok | 2,678,400 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2026-01.parquet | 85.7 | ok | 2,678,400 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2026-02.parquet | 91.2 | ok | 2,419,200 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2026-03.parquet | 92.5 | ok | 2,678,400 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2026-04.parquet | 82.6 | ok | 2,592,000 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2026-05.parquet | 82.2 | ok | 2,678,400 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2026-06.parquet | 86.2 | ok | 2,592,000 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2026-07.parquet | 83.7 | ok | 2,678,400 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2026-08.parquet | 80.5 | ok | 2,678,400 | 00:00:00 | 23:59:59 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |
    | klines | BTCUSDT_spot_1s_2026-09.parquet | 31.9 | ok | 998,015 | 00:00:00 | 13:13:34 | True | None | 0.0 | 0.0 |  |  | 1000.0 |  |

## How to re-run

```bash
python -m data_collection.validate_sources --all                    # checks 1-5 + all-files
python -m data_collection.validate_sources --check 2                # waits up to 30 min for the hft day file
python -m data_collection.validate_sources --check all-files --table-out docs/notes/_validation_files.md
python -m data_collection.compact --resort                          # after an archive backfill: fix any unordered days
```

Checks 1–3 use `data_collection.load.load_l1(..., source=...)` to pin each side to one source.
