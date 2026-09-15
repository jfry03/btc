"""Distribution of BTCUSDT (Binance perp) log returns at several horizons, for mid and swmid
(size-weighted mid / microprice).

Memory-safe by construction: every day is loaded on its own through `load_l1` (1 s grid, 86 400
rows), cleaned, turned into returns for each horizon, and folded into running power sums and
fixed-bin histograms - then discarded. Nothing grows with the number of days. Each day's grid is
cached under data/derived/l1_1s/<day>.parquet (~3 MB) so re-runs skip the tick files.

Cleaning: days with a NaN or a >60 s run of zero L1 updates are dropped (feed gap); seconds with
spread/mid > 2 bp (hollow book) are masked so returns across them are dropped. Returns use
overlapping 1 s-stride windows inside a day (overlap inflates n, not the shape).

Outputs
    analysis/return_dists_linear.png   log returns in bp, linear density, one panel per horizon
    analysis/return_dists.csv          n, vol, skew, ex-kurt per horizon x price
    analysis/return_dists_days.csv     which days were used / dropped and why
    --nonzero writes *_nonzero.* variants with windows where |price change| < half a tick removed

    python -m analysis.return_dists --days 0     # 0 = every day on disk
"""
from __future__ import annotations

import argparse
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from analysis.onesec_moments import pick
from data_collection.load import load_l1

HERE = Path(__file__).resolve().parent
RAW = HERE.parent / "data" / "raw"
CACHE_DIR = HERE.parent / "data" / "derived" / "l1_1s"
HORIZONS = [("1s", 1), ("2s", 2), ("5s", 5), ("15s", 15), ("1m", 60), ("5m", 300), ("15m", 900), ("1h", 3600)]
# histogram support per horizon in bp (~ +-8 sigma of the 52-day run); plots crop to the 0.5-99.5 % quantiles
HIST_BP = {"1s": 6, "2s": 8, "5s": 14, "15s": 25, "1m": 50, "5m": 110, "15m": 190, "1h": 360}
NBINS = 481
Q_WINDOW = 0.005          # plots crop to the [0.5 %, 99.5 %] quantiles of the mid returns
SERIES = ("mid", "swmid")
COL = {"mid": "#2a78d6", "swmid": "#eb6834", "grid": "#e6e5e1"}
SEC_PER_YEAR = 365 * 86_400
MAX_GAP_S = 60
MAX_REL_SPREAD_BP = 2.0
HALF_TICK = 0.05        # USD; --nonzero drops windows where the price moved less than this


def days_on_disk(symbol: str) -> list[date]:
    """Every UTC day with a tick-level L1 file in any source (archive / recorder / tardis / hft)."""
    pats = [(RAW, rf"{symbol}-bookTicker-(\d{{4}}-\d{{2}}-\d{{2}})\."),
            (RAW / "hft", rf"{symbol}-bookTicker-(\d{{4}}-\d{{2}}-\d{{2}})\."),
            (RAW / "tardis", rf"book_ticker_(\d{{4}}-\d{{2}}-\d{{2}})_{symbol}")]
    found = set()
    for d, pat in pats:
        rx = re.compile(pat)
        found |= {m.group(1) for p in d.iterdir() if (m := rx.search(p.name))} if d.exists() else set()
    return sorted(date.fromisoformat(x) for x in found)


def load_day(symbol: str, d: date) -> pd.DataFrame:
    p = CACHE_DIR / f"{d}.parquet"
    if p.exists():
        return pd.read_parquet(p)
    s = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    df = load_l1(symbol, s, s + timedelta(days=1), step_ms=1000)[["mid", "microprice", "spread", "n_updates"]]
    df = df.rename(columns={"microprice": "swmid"})
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(p)
    return df


def longest_zero_run(x: np.ndarray) -> int:
    z = np.concatenate([[0], (x == 0).astype(int), [0]])
    edges = np.flatnonzero(np.diff(z))
    return int((edges[1::2] - edges[::2]).max()) if len(edges) else 0


def clean(df: pd.DataFrame) -> tuple[pd.DataFrame | None, dict]:
    nan = int(df["mid"].iloc[1:].isna().sum())          # row 0 is NaN by construction (no prior tick)
    gap = longest_zero_run(df["n_updates"].to_numpy())
    info = {"nan": nan, "gap_s": gap, "masked_s": 0, "kept": False}
    if nan or gap > MAX_GAP_S:
        return None, info
    bad = (df["spread"] / df["mid"] * 1e4) > MAX_REL_SPREAD_BP
    df = df.copy(); df.loc[bad, ["mid", "swmid"]] = np.nan
    info.update(masked_s=int(bad.sum()), kept=True)
    return df, info


class Accum:
    """Running n, power sums (in bp) and a fixed-bin histogram for one horizon x series."""

    def __init__(self, name: str):
        self.edges = np.linspace(-HIST_BP[name], HIST_BP[name], NBINS)
        self.counts = np.zeros(NBINS - 1)
        self.n = 0; self.zeros = 0; self.s = np.zeros(4)

    def add(self, r_bp: np.ndarray) -> None:
        self.n += len(r_bp); self.zeros += int((r_bp == 0).sum())
        self.s += [r_bp.sum(), (r_bp**2).sum(), (r_bp**3).sum(), (r_bp**4).sum()]
        self.counts += np.histogram(r_bp, bins=self.edges)[0]

    def quantile(self, q: float) -> float:
        cdf = np.cumsum(self.counts) / self.counts.sum()
        return float(self.edges[1:][np.searchsorted(cdf, q)])

    def moments(self, k: int) -> dict:
        n, (m1, m2, m3, m4) = self.n, self.s / max(self.n, 1)
        mu = m1; var = m2 - mu**2
        c3 = m3 - 3 * mu * m2 + 2 * mu**3
        c4 = m4 - 4 * mu * m3 + 6 * mu**2 * m2 - 3 * mu**4
        return {"n": n, "frac_zero": self.zeros / max(n, 1), "vol_bp": np.sqrt(var),
                "vol_ann": np.sqrt(var) * 1e-4 * np.sqrt(SEC_PER_YEAR / k),
                "skew": c3 / var**1.5, "ex_kurt": c4 / var**2 - 3}


def style(ax, title: str) -> None:
    ax.set_title(title, fontsize=10, loc="left", color="#0b0b0b")
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(COL["grid"])
    ax.grid(axis="y", color=COL["grid"], linewidth=0.6)
    ax.tick_params(colors="#52514e", labelsize=8)


def plot(acc: dict, mom: pd.DataFrame, ndays: int, tag: str = "") -> None:
    fig, axes = plt.subplots(2, 4, figsize=(16, 7.5), constrained_layout=True)
    for ax, (name, k) in zip(axes.flat, HORIZONS):
        m = mom.loc[(name, "mid")]
        for c in SERIES:
            a = acc[(name, c)]
            width = a.edges[1] - a.edges[0]
            dens = a.counts / (a.n * width)
            ax.stairs(dens, a.edges, color=COL[c], linewidth=1.4, label=c)
        a = acc[(name, "mid")]
        lo, hi = a.quantile(Q_WINDOW), a.quantile(1 - Q_WINDOW)
        ax.set_xlim(-max(-lo, hi), max(-lo, hi))
        style(ax, f'{name}   sigma={m["vol_bp"]:.2f} bp   skew={m["skew"]:+.2f}   ex-kurt={m["ex_kurt"]:.0f}')
        ax.set_xlabel("log return (bp, 1e-4)", fontsize=8, color="#52514e")
    axes[0, 0].legend(frameon=False, fontsize=8)
    what = "non-zero (|dP| >= half tick) " if tag else ""
    fig.suptitle(f"BTCUSDT 1 s-grid {what}log returns by horizon - linear density, 0.5-99.5 % window "
                 f"({ndays} days, overlapping windows, gaps/hollow-book clipped)", fontsize=11, x=0.01, ha="left")
    fig.savefig(HERE / f"return_dists_linear{tag}.png", dpi=130)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--days", type=int, default=0, help="days to sample evenly across the archive; 0 = all")
    ap.add_argument("--nonzero", action="store_true",
                    help="drop windows where |price change| < half a tick (mid unchanged / swmid within-spread wiggle)")
    a = ap.parse_args()
    tag = "_nonzero" if a.nonzero else ""

    days = days_on_disk(a.symbol)
    if a.days:
        days = pick(days, a.days)
    acc = {(h, c): Accum(h) for h, _ in HORIZONS for c in SERIES}
    log = []
    for i, d in enumerate(days):
        df, info = clean(load_day(a.symbol, d))
        log.append({"day": str(d), **info})
        if df is not None:
            px = {c: df[c].to_numpy() for c in SERIES}
            lp = {c: np.log(px[c]) for c in SERIES}
            for h, k in HORIZONS:
                for c in SERIES:
                    r = (lp[c][k:] - lp[c][:-k]) * 1e4
                    ok = np.isfinite(r)
                    if a.nonzero:
                        ok &= np.abs(px[c][k:] - px[c][:-k]) >= HALF_TICK
                    acc[(h, c)].add(r[ok])
        print(f"[{i + 1}/{len(days)}] {d} {'kept' if info['kept'] else 'DROPPED'} {info}", flush=True)

    daylog = pd.DataFrame(log); daylog.to_csv(HERE / "return_dists_days.csv", index=False)
    rows = [{"horizon": h, "price": c, **acc[(h, c)].moments(k)} for h, k in HORIZONS for c in SERIES]
    mom = pd.DataFrame(rows).set_index(["horizon", "price"])
    pd.set_option("display.width", 200)
    print(f"\nkept {int(daylog.kept.sum())} of {len(daylog)} days; dropped: "
          f"{daylog[~daylog.kept].day.tolist()}\n")
    print(mom.round(3).to_string())
    mom.to_csv(HERE / f"return_dists{tag}.csv")
    plot(acc, mom, int(daylog.kept.sum()), tag)
    print("wrote", HERE / f"return_dists_linear{tag}.png")


if __name__ == "__main__":
    main()
