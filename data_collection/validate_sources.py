"""
Cross-validation of the L1 / kline sources where they overlap. Each check streams through
`bookticker.iter_batches` / `sample_book`, so memory stays bounded (~1 GB worst case).

    python -m data_collection.validate_sources --check 1            # Binance archive vs Tardis, 2024-03-01
    python -m data_collection.validate_sources --check 2            # CryptoHFTData replay vs Tardis, 2025-07-01
    python -m data_collection.validate_sources --check 3            # spot 1s klines vs perp L1 mid
    python -m data_collection.validate_sources --check 4            # Tardis 2026-09-01 size anomaly
    python -m data_collection.validate_sources --check 5            # internal consistency of compacted days
    python -m data_collection.validate_sources --check all-files [--table-out docs/notes/_validation_files.md]
    python -m data_collection.validate_sources --all

Results are printed; docs/notes/validation.md holds the interpreted report.
"""
from __future__ import annotations

import argparse
import re
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

from . import bookticker as bt
from .load import load_l1

RAW = Path("data/raw")
TARDIS = RAW / "tardis"
HFT = RAW / "hft"
PARQ = Path("data/parquet")
STATE = ["best_bid_price", "best_bid_qty", "best_ask_price", "best_ask_qty"]

_M = np.array([0x9E3779B97F4A7C15, 0xC2B2AE3D27D4EB4F, 0x165667B19E3779F9, 0x27D4EB2F165667C5, 0x94D049BB133111EB],
              dtype=np.uint64)


def _key(b: pa.RecordBatch, cols: list[str]) -> np.ndarray:
    """64-bit hash of (event_time, bid, bid_qty, ask, ask_qty) per row; collisions are negligible."""
    k = np.zeros(b.num_rows, np.uint64)
    with np.errstate(over="ignore"):
        for i, c in enumerate(cols):
            v = b.column(c).to_numpy(zero_copy_only=False)
            if v.dtype.kind == "f":
                v = np.round(v * 1e8).astype(np.int64)
            k ^= (v.astype(np.uint64) + np.uint64(i + 1)) * _M[i]
            k ^= k >> np.uint64(29)
    return k


def _sample(symbol: str, day: date, step_ms: int, source: str) -> pd.DataFrame:
    """Book state on a grid for one UTC day from a specific source (load.load_l1)."""
    d0 = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    return load_l1(symbol, d0, d0 + timedelta(days=1), step_ms, source=source, derived=False)


def _tick_diff_summary(a: np.ndarray, b: np.ndarray, tick: float = 0.1) -> dict:
    ok = ~(np.isnan(a) | np.isnan(b))
    d = np.round((a[ok] - b[ok]) / tick).astype(np.int64)
    ad = np.abs(d)
    return {"n": int(ok.sum()), "equal": float((d == 0).mean()), "within_1_tick": float((ad <= 1).mean()),
            "within_5_ticks": float((ad <= 5).mean()), "max_abs_ticks": int(ad.max()) if len(ad) else 0,
            "mean_ticks": float(d.mean()) if len(d) else 0.0}


def _compare_grid(a: pd.DataFrame, b: pd.DataFrame, label_a: str, label_b: str) -> None:
    for col in ("bid", "ask"):
        s = _tick_diff_summary(a[col].to_numpy(), b[col].to_numpy())
        print(f"  {col}: n={s['n']}  equal={s['equal']:.4f}  |d|<=1 tick={s['within_1_tick']:.4f}  "
              f"|d|<=5={s['within_5_ticks']:.4f}  max={s['max_abs_ticks']}  mean={s['mean_ticks']:+.3f} ticks ({label_a}-{label_b})")
    mid_a = (a["bid"] + a["ask"]) / 2; mid_b = (b["bid"] + b["ask"]) / 2
    s = _tick_diff_summary(mid_a.to_numpy(), mid_b.to_numpy(), tick=0.05)
    print(f"  mid: equal={s['equal']:.4f}  |d|<=1 half-tick={s['within_1_tick']:.4f}  max={s['max_abs_ticks']} half-ticks")
    for col in ("bid_qty", "ask_qty"):
        eq = float((a[col] == b[col]).mean())
        print(f"  {col}: equal={eq:.4f}")
    sa = a["ask"] - a["bid"]; sb = b["ask"] - b["bid"]
    print(f"  spread median {label_a}={sa.median():.2f} {label_b}={sb.median():.2f}; crossed {label_a}={int((sa <= 0).sum())} {label_b}={int((sb <= 0).sum())}")
    print(f"  NaN rows: {label_a}={int(a['bid'].isna().sum())} {label_b}={int(b['bid'].isna().sum())}")


# --------------------------------------------------------------------------- check 1

def check1(day: date = date(2024, 3, 1), symbol: str = "BTCUSDT") -> None:
    """Binance archive vs Tardis book_ticker on the same day."""
    tardis = TARDIS / f"binance-futures_book_ticker_{day:%Y-%m-%d}_{symbol}.csv.gz"
    binance = next((p for p in (RAW / f"{symbol}-bookTicker-{day:%Y-%m-%d}.parquet",
                                RAW / f"{symbol}-bookTicker-{day:%Y-%m-%d}.zip") if p.exists()), None)
    print(f"[check 1] {day}  Binance={binance.name if binance else None}  Tardis={tardis.name if tardis.exists() else None}")
    if binance is None or not tardis.exists():
        print("  missing input"); return

    # --- pass over Binance: keys, counts, consecutive-duplicate share, event_time coverage
    keys, n_b, dups, prev = [], 0, 0, None
    et_min = et_max = None
    for b in bt.iter_batches(binance):
        keys.append(_key(b, ["event_time"] + STATE))
        n_b += b.num_rows
        st = np.stack([b.column(c).to_numpy(zero_copy_only=False) for c in STATE], axis=1)
        same = np.all(st[1:] == st[:-1], axis=1)
        dups += int(same.sum()) + (0 if prev is None else int(np.all(st[0] == prev)))
        prev = st[-1]
        et = b.column("event_time").to_numpy()
        et_min = et[0] if et_min is None else et_min; et_max = et[-1]
    kb = np.concatenate(keys); del keys
    kb.sort()
    print(f"  Binance rows={n_b:,}  consecutive identical BBO rows={dups:,} ({dups / n_b:.1%})  "
          f"event_time span {datetime.fromtimestamp(et_min/1000, timezone.utc):%H:%M:%S} -> {datetime.fromtimestamp(et_max/1000, timezone.utc):%H:%M:%S}")

    # --- pass over Tardis: keys -> membership in Binance; also consecutive dups and time span
    n_t, found, tdups, tprev, ts_min, ts_max = 0, 0, 0, None, None, None
    lag = []
    for b in bt.iter_batches(tardis):
        k = _key(b, ["event_time"] + STATE)
        idx = np.searchsorted(kb, k)
        idx[idx >= len(kb)] = 0
        found += int((kb[idx] == k).sum())
        n_t += b.num_rows
        st = np.stack([b.column(c).to_numpy(zero_copy_only=False) for c in STATE], axis=1)
        tdups += int(np.all(st[1:] == st[:-1], axis=1).sum()) + (0 if tprev is None else int(np.all(st[0] == tprev)))
        tprev = st[-1]
        et = b.column("event_time").to_numpy()
        ts_min = et[0] if ts_min is None else ts_min; ts_max = et[-1]
        lag.append(b.column("local_time").to_numpy() // 1000 - et)
    lag = np.concatenate(lag)
    print(f"  Tardis rows={n_t:,}  consecutive identical BBO rows={tdups:,} ({tdups / max(n_t,1):.1%})  "
          f"span {datetime.fromtimestamp(ts_min/1000, timezone.utc):%H:%M:%S} -> {datetime.fromtimestamp(ts_max/1000, timezone.utc):%H:%M:%S}")
    print(f"  Tardis (event_time, bid, bid_qty, ask, ask_qty) tuples present in Binance: {found:,}/{n_t:,} = {found / max(n_t,1):.4%}")
    print(f"  Tardis local_timestamp - exchange event_time: median {np.median(lag):.0f} ms, p99 {np.percentile(lag, 99):.0f} ms")
    print(f"  ratio Binance/Tardis rows = {n_b / max(n_t,1):.2f}; Binance rows that are NOT consecutive dups = {n_b - dups:,}")

    # --- as-of comparison on a 1 s grid
    a = _sample(symbol, day, 1000, "archive")
    b_ = _sample(symbol, day, 1000, "tardis")
    print("  as-of 1 s grid (Binance vs Tardis):")
    _compare_grid(a, b_, "binance", "tardis")
    print(f"  n_updates per second: Binance mean={a['n_updates'].mean():.0f} max={a['n_updates'].max()}  Tardis mean={b_['n_updates'].mean():.0f} max={b_['n_updates'].max()}")
    zero_b = int((a["n_updates"] == 0).sum()); zero_t = int((b_["n_updates"] == 0).sum())
    print(f"  seconds with zero updates: Binance={zero_b}  Tardis={zero_t}")


# --------------------------------------------------------------------------- check 2

def check2(day: date = date(2025, 7, 1), symbol: str = "BTCUSDT", wait_min: int = 30) -> None:
    """CryptoHFTData L2 replay vs Tardis book_ticker on the same day."""
    hft = HFT / f"{symbol}-bookTicker-{day:%Y-%m-%d}.parquet"
    tardis = TARDIS / f"binance-futures_book_ticker_{day:%Y-%m-%d}_{symbol}.csv.gz"
    t0 = time.time()
    while not hft.exists() and time.time() - t0 < wait_min * 60:
        time.sleep(60)
    print(f"[check 2] {day}  hft={hft.exists()}  tardis={tardis.exists()}")
    if not (hft.exists() and tardis.exists()):
        print("  missing input"); return
    n_h = sum(b.num_rows for b in bt.iter_batches(hft))
    n_t = 0; tdups = 0; tprev = None
    for b in bt.iter_batches(tardis):
        n_t += b.num_rows
        st = np.stack([b.column(c).to_numpy(zero_copy_only=False) for c in STATE], axis=1)
        tdups += int(np.all(st[1:] == st[:-1], axis=1).sum()) + (0 if tprev is None else int(np.all(st[0] == tprev)))
        tprev = st[-1]
    print(f"  L1 states: hft replay={n_h:,}  tardis book_ticker={n_t:,} (of which {tdups:,} consecutive identical)")
    if True:
        for step in (1000, 100):
            a = _sample(symbol, day, step, "hft"); b_ = _sample(symbol, day, step, "tardis")
            print(f"  as-of {step} ms grid (hft vs tardis), {len(a):,} points:")
            _compare_grid(a, b_, "hft", "tardis")
            if step == 1000:
                # warm-up effect: first 2 minutes vs rest
                for lo, hi, lab in ((0, 60, "first 60 s"), (60, 120, "60-120 s"), (120, len(a), "rest of day")):
                    s = _tick_diff_summary(a["bid"].to_numpy()[lo:hi], b_["bid"].to_numpy()[lo:hi])
                    print(f"    bid equal {lab}: {s['equal']:.4f} (n={s['n']})")
                # staleness: |diff| > 5 ticks episodes
                d = np.abs(a["bid"].to_numpy() - b_["bid"].to_numpy()) / 0.1
                bad = np.flatnonzero(d > 5)
                if len(bad):
                    runs = np.split(bad, np.flatnonzero(np.diff(bad) > 1) + 1)
                    print(f"    episodes with |bid diff| > 5 ticks: {len(runs)}, longest {max(len(r) for r in runs)} s, "
                          f"first at {a.index[runs[0][0]].strftime('%H:%M:%S')}")
                else:
                    print("    no episodes with |bid diff| > 5 ticks")


# --------------------------------------------------------------------------- check 3

def check3(days: list[date] | None = None, symbol: str = "BTCUSDT") -> None:
    """Spot 1s kline close vs perp L1 mid."""
    if days is None:
        days = [date(2023, 6, 1), date(2025, 7, 1), date(2026, 8, 1)]
    for day in days:
        kl = PARQ / f"{symbol}_spot_1s_{day:%Y-%m}.parquet"
        if not kl.exists():
            print(f"[check 3] {day}: no kline file {kl.name}"); continue
        src = bt.locate(symbol, day, "auto", download=False)
        if src is None:
            print(f"[check 3] {day}: no L1 source"); continue
        print(f"[check 3] {day}  L1 source={src[0].name} ({src[2]})")
        k = pd.read_parquet(kl, columns=["close", "open_time"])
        d0 = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
        k = k[(k.index >= d0) & (k.index < d0 + timedelta(days=1))]
        l1 = bt.sample_book(symbol, d0, d0 + timedelta(days=1), 1000, derived=True)
        # kline bar [t, t+1) close vs perp mid as-of t+1 (end of the bar)
        mid = l1["mid"].shift(-1)
        j = pd.DataFrame({"close": k["close"], "mid": mid}).dropna()
        basis = (j["mid"] - j["close"]) / j["close"] * 1e4
        print(f"  seconds compared={len(j):,}  basis perp-spot: mean={basis.mean():+.2f} bps  std={basis.std():.2f}  "
              f"p1={basis.quantile(.01):+.2f}  p99={basis.quantile(.99):+.2f}")
        r_c = np.log(j["close"]).diff(); r_m = np.log(j["mid"]).diff()
        print(f"  corr of 1 s log-returns: {r_c.corr(r_m):.3f}")
        xc = {lag: r_c.corr(r_m.shift(lag)) for lag in range(-5, 6)}
        best = max(xc, key=lambda l: xc[l])
        print("  xcorr by lag (perp shifted by lag s; +lag = perp leads): " +
              " ".join(f"{l:+d}:{v:.3f}" for l, v in xc.items()) + f"  -> peak at {best:+d} s")


# --------------------------------------------------------------------------- check 4

def check4(day: date = date(2026, 9, 1), symbol: str = "BTCUSDT") -> None:
    """Why is the Tardis 2026-09-01 file ~4x larger than the others?"""
    p = TARDIS / f"binance-futures_book_ticker_{day:%Y-%m-%d}_{symbol}.csv.gz"
    ref = TARDIS / f"binance-futures_book_ticker_2026-08-01_{symbol}.csv.gz"
    for path in (ref, p):
        if not path.exists():
            print(f"[check 4] missing {path.name}"); continue
        n = dups = 0; prev = None; first = last = None; per_hour = np.zeros(24, np.int64)
        exact_dup = 0; prev_row = None; ncols = None
        for b in bt.iter_batches(path):
            ncols = b.num_columns
            n += b.num_rows
            et = b.column("event_time").to_numpy()
            first = et[0] if first is None else first; last = et[-1]
            hrs = ((et // 1000) % 86400) // 3600
            per_hour += np.bincount(hrs, minlength=24)[:24]
            st = np.stack([b.column(c).to_numpy(zero_copy_only=False) for c in STATE], axis=1)
            same = np.all(st[1:] == st[:-1], axis=1)
            dups += int(same.sum()) + (0 if prev is None else int(np.all(st[0] == prev)))
            prev = st[-1]
            full = np.column_stack([et, st])
            samef = np.all(full[1:] == full[:-1], axis=1)
            exact_dup += int(samef.sum()) + (0 if prev_row is None else int(np.all(full[0] == prev_row)))
            prev_row = full[-1]
        span_s = (last - first) / 1000
        print(f"[check 4] {path.name}: {path.stat().st_size/1e6:.0f} MB, {n:,} rows, {ncols} cols, "
              f"{n/span_s:.0f} rows/s over {span_s/3600:.1f} h; identical-BBO consecutive rows {dups/n:.1%}; "
              f"exact duplicate rows (same time+BBO) {exact_dup/n:.1%}")
        print("   rows per hour (k): " + " ".join(f"{h}:{v//1000}" for h, v in enumerate(per_hour)))


# --------------------------------------------------------------------------- check 5

def check5(n_days: int = 3, symbol: str = "BTCUSDT", seed: int = 0) -> None:
    """Internal consistency of compacted Binance days + row count vs backfill log."""
    files = sorted(RAW.glob(f"{symbol}-bookTicker-????-??-??.parquet"))
    rng = np.random.default_rng(seed)
    pick = [files[i] for i in sorted(rng.choice(len(files), size=min(n_days, len(files)), replace=False))]
    log_rows = {}
    logp = RAW / "backfill-l1.log"
    if logp.exists():
        for m in re.finditer(r"(\S+-bookTicker-(\d{4}-\d{2}-\d{2}))\.zip -> \S+\.parquet\s+(\d+) rows", logp.read_text()):
            log_rows[m.group(2)] = int(m.group(3))
    print(f"[check 5] {len(files)} compacted days on disk; checking {[p.name[-18:-8] for p in pick]}")
    for p in pick:
        n = 0; mono = True; inc = True; nan = 0; crossed = 0; prev_t = prev_u = None
        for b in bt.iter_batches(p):
            t = b.column("transaction_time").to_numpy(); u = b.column("update_id").to_numpy(zero_copy_only=False)
            bid = b.column("best_bid_price").to_numpy(); ask = b.column("best_ask_price").to_numpy()
            mono &= bool((np.diff(t) >= 0).all()) and (prev_t is None or t[0] >= prev_t)
            inc &= bool((np.diff(u) > 0).all()) and (prev_u is None or u[0] > prev_u)
            nan += int(sum(np.isnan(b.column(c).to_numpy(zero_copy_only=False)).sum() for c in STATE))
            crossed += int((bid >= ask).sum())
            prev_t, prev_u = t[-1], u[-1]; n += b.num_rows
        day = p.name[-18:-8]
        exp = log_rows.get(day)
        print(f"  {day}: rows={n:,} (log: {exp if exp else 'n/a'}{' OK' if exp == n else ''})  "
              f"transaction_time monotonic={mono}  update_id strictly increasing={inc}  NaNs={nan}  bid>=ask rows={crossed}")


# --------------------------------------------------------------------------- all-files

def _in_progress(p: Path, minutes: int = 5) -> bool:
    if time.time() - p.stat().st_mtime < minutes * 60:
        return True
    return any(p.parent.glob(p.stem.split(".")[0] + "*.part"))


def _day_of(name: str) -> str | None:
    m = re.search(r"(\d{4}-\d{2}-\d{2})", name)
    return m.group(1) if m else None


def _scan_l1(p: Path) -> dict:
    n = 0; mono = True; inc = True; nan = 0; crossed = 0; prev_t = prev_u = None
    has_uid = True; spreads = []; t_first = t_last = None; max_gap = 0
    for b in bt.iter_batches(p):
        t = b.column("transaction_time").to_numpy()
        u = b.column("update_id").to_numpy(zero_copy_only=False)
        bid = b.column("best_bid_price").to_numpy(); ask = b.column("best_ask_price").to_numpy()
        if prev_t is not None:
            max_gap = max(max_gap, int(t[0] - prev_t))
        if len(t) > 1:
            max_gap = max(max_gap, int(np.diff(t).max()))
        mono &= bool((np.diff(t) >= 0).all()) and (prev_t is None or t[0] >= prev_t)
        if np.isnan(u.astype("float64")).all():
            has_uid = False
        else:
            inc &= bool((np.diff(u) > 0).all()) and (prev_u is None or u[0] > prev_u)
        nan += int(sum(np.isnan(b.column(c).to_numpy(zero_copy_only=False)).sum() for c in STATE))
        crossed += int((bid >= ask).sum())
        sp = ask - bid
        spreads.append(sp[:: max(1, len(sp) // 20000)])
        t_first = t[0] if t_first is None else t_first; t_last = t[-1]
        prev_t, prev_u = t[-1], u[-1]; n += b.num_rows
    sp = np.concatenate(spreads) if spreads else np.array([np.nan])
    return {"rows": n, "first": t_first, "last": t_last, "mono": mono,
            "uid_inc": inc if has_uid else None, "nan": nan, "crossed": crossed,
            "spread_p50": float(np.nanmedian(sp)), "spread_max": float(np.nanmax(sp)), "max_gap_ms": max_gap}


def _scan_klines(p: Path) -> dict:
    df = pd.read_parquet(p, columns=["open_time", "close", "high", "low"])
    ot = df["open_time"].to_numpy()
    return {"rows": len(df), "first": int(ot[0]), "last": int(ot[-1]), "mono": bool((np.diff(ot) > 0).all()),
            "uid_inc": None, "nan": int(df["close"].isna().sum()), "crossed": int((df["low"] > df["high"]).sum()),
            "spread_p50": float("nan"), "spread_max": float("nan"), "max_gap_ms": int(np.diff(ot).max()) if len(ot) > 1 else 0}


def check_all_files(symbol: str = "BTCUSDT", out_md: Path | None = None) -> pd.DataFrame:
    """Every file on disk: readable, counts, monotonicity, NaNs, crossed book, spread, gaps."""
    files = [("archive/recorder", p) for p in sorted(RAW.glob(f"{symbol}-bookTicker-????-??-??.parquet"))]
    files += [("tardis", p) for p in sorted(TARDIS.glob(f"binance-futures_book_ticker_*_{symbol}.csv.gz"))]
    files += [("hft", p) for p in sorted(HFT.glob(f"{symbol}-bookTicker-????-??-??.parquet"))]
    files += [("klines", p) for p in sorted(PARQ.glob(f"{symbol}_spot_1s_????-??.parquet"))]
    rows = []
    for src, p in files:
        rec = {"source": src, "file": p.name, "mb": round(p.stat().st_size / 1e6, 1), "status": "ok", "flags": ""}
        if _in_progress(p):
            rec["status"] = "in-progress (skipped)"; rows.append(rec); continue
        try:
            r = _scan_klines(p) if src == "klines" else _scan_l1(p)
        except Exception as exc:  # noqa: BLE001
            rec["status"] = f"UNREADABLE: {exc}"[:80]; rows.append(rec); continue
        rec.update(r)
        flags = []
        day = _day_of(p.name)
        if day and src != "klines":
            d0 = int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp() * 1000)
            if not (d0 <= r["first"] < d0 + 86_400_000 and d0 <= r["last"] < d0 + 86_400_000):
                flags.append("timestamps outside day")
            if r["first"] - d0 > 600_000:
                flags.append(f"starts {((r['first'] - d0) / 60000):.0f} min late")
            if d0 + 86_400_000 - r["last"] > 600_000:
                flags.append(f"ends {((d0 + 86_400_000 - r['last']) / 60000):.0f} min early")
        if not r["mono"]: flags.append("non-monotonic time")
        if r["uid_inc"] is False: flags.append("update_id not increasing")
        if r["nan"]: flags.append(f"{r['nan']} NaN")
        if r["crossed"]: flags.append(f"{r['crossed']} crossed")
        if src != "klines" and r["max_gap_ms"] > 60_000: flags.append(f"gap {r['max_gap_ms'] / 1000:.0f} s")
        if src == "klines" and r["max_gap_ms"] > 3_600_000: flags.append(f"gap {r['max_gap_ms'] / 3.6e6:.1f} h")
        if src != "klines" and r["rows"] < 500_000: flags.append("few rows")
        rec["flags"] = "; ".join(flags)
        rows.append(rec)
        print(f"  {src:16s} {p.name:60s} {rec['status']:8s} rows={r['rows']:>11,} spread p50={r['spread_p50']:.2f} max={r['spread_max']:.1f}  {rec['flags']}")
    df = pd.DataFrame(rows)
    if out_md:
        def fmt_ts(v):
            return "" if pd.isna(v) else datetime.fromtimestamp(v / 1000, timezone.utc).strftime("%H:%M:%S")
        show = df.copy()
        for c in ("first", "last"):
            if c in show: show[c] = show[c].map(fmt_ts)
        cols = [c for c in ["source", "file", "mb", "status", "rows", "first", "last", "mono", "uid_inc", "nan", "crossed", "spread_p50", "spread_max", "max_gap_ms", "flags"] if c in show]
        show = show[cols]
        for c in ("spread_p50", "spread_max"):
            if c in show: show[c] = show[c].map(lambda v: "" if pd.isna(v) else f"{v:.2f}")
        if "rows" in show: show["rows"] = show["rows"].map(lambda v: "" if pd.isna(v) else f"{int(v):,}")
        hdr = "| " + " | ".join(cols) + " |\n|" + "---|" * len(cols) + "\n"
        body = "".join("| " + " | ".join(str(v) for v in row) + " |\n" for row in show.itertuples(index=False))
        out_md.write_text(hdr + body)
    return df


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", choices=["1", "2", "3", "4", "5", "all-files"])
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--table-out", type=Path, default=None, help="all-files: write the per-file markdown table here")
    a = ap.parse_args()
    checks = {"1": check1, "2": check2, "3": check3, "4": check4, "5": check5}
    for i, fn in checks.items():
        if a.all or a.check == i:
            fn()
    if a.all or a.check == "all-files":
        check_all_files(out_md=a.table_out)
