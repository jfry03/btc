"""Moments of 1-second BTCUSDT (Binance) log returns, for mid and swmid (size-weighted mid /
microprice).

Streams one UTC day at a time through `load_l1` (constant memory: the 1 s grid is 86 400 rows
per day, the tick archive never sits in RAM), so it won't blow up a kernel. Picks `--days`
days spread evenly across whatever bookTicker days are on disk, reports per-day and pooled
stats, and writes them to analysis/onesec_moments.csv.

    python -m analysis.onesec_moments --days 8
"""
from __future__ import annotations

import argparse
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

from data_collection.load import load_l1

RAW = Path(__file__).resolve().parent.parent / "data" / "raw"
SEC_PER_YEAR = 365 * 86_400


def days_on_disk(symbol: str) -> list[date]:
    pat = re.compile(rf"{symbol}-bookTicker-(\d{{4}}-\d{{2}}-\d{{2}})\.")
    found = {m.group(1) for p in RAW.iterdir() if (m := pat.search(p.name))}
    return sorted(date.fromisoformat(d) for d in found)


def pick(days: list[date], n: int) -> list[date]:
    if n >= len(days):
        return days
    idx = np.linspace(0, len(days) - 1, n).round().astype(int)
    return [days[i] for i in sorted(set(idx))]


def moments(r: np.ndarray) -> dict:
    r = r[np.isfinite(r)]
    return {
        "n": len(r),
        "frac_zero": float(np.mean(r == 0)),
        "vol_1s_bp": float(r.std() * 1e4),                       # 1 s stdev in basis points
        "vol_ann": float(r.std() * np.sqrt(SEC_PER_YEAR)),       # annualised, sqrt-time
        "skew": float(stats.skew(r)),
        "ex_kurt": float(stats.kurtosis(r)),                      # excess (normal = 0)
        "min_bp": float(r.min() * 1e4),
        "max_bp": float(r.max() * 1e4),
    }


def one_day(symbol: str, day: date, step_ms: int) -> tuple[dict, dict]:
    s = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    df = load_l1(symbol, s, s + timedelta(days=1), step_ms=step_ms)
    out = {}
    for col in ("mid", "microprice"):
        out[col] = np.diff(np.log(df[col].to_numpy()))
    return out["mid"], out["microprice"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--days", type=int, default=8, help="how many days to sample across the archive")
    ap.add_argument("--step-ms", type=int, default=1000)
    ap.add_argument("--dates", nargs="*", help="explicit YYYY-MM-DD days instead of --days sampling")
    a = ap.parse_args()

    days = [date.fromisoformat(d) for d in a.dates] if a.dates else pick(days_on_disk(a.symbol), a.days)
    rows, pooled = [], {"mid": [], "swmid": []}
    for d in days:
        r_mid, r_sw = one_day(a.symbol, d, a.step_ms)
        pooled["mid"].append(r_mid); pooled["swmid"].append(r_sw)
        for name, r in (("mid", r_mid), ("swmid", r_sw)):
            rows.append({"day": str(d), "price": name, **moments(r)})
        print(f"{d}  mid: {moments(r_mid)}\n{' ' * 12}swmid: {moments(r_sw)}", flush=True)
    for name in ("mid", "swmid"):
        rows.append({"day": "POOLED", "price": name, **moments(np.concatenate(pooled[name]))})

    tab = pd.DataFrame(rows).set_index(["day", "price"])
    pd.set_option("display.width", 200)
    print("\n", tab.round(3).to_string())
    out = Path(__file__).with_name("onesec_moments.csv")
    tab.to_csv(out)
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
