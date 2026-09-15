"""How closely do BTC prices on different venues track each other (one Tardis free day)?

Inputs: Tardis `trades` CSVs for several venues (downloaded to --dir) plus the Binance USDT-M
perp L1 already on disk (Tardis book_ticker, via load_l1). Each venue's last trade price is
sampled as-of every second; the perp mid is the reference. Reports the basis (level offset),
its dispersion, the lead/lag from 1 s-return cross-correlation, and the cross-venue price
range per second.

    python -m analysis.venue_compare --day 2026-09-01 --dir <folder with *_trades_*.csv.gz>
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from data_collection.load import load_l1

LABEL = {"coinbase": "Coinbase BTC-USD (spot)", "bitstamp": "Bitstamp BTC/USD (spot)", "binance": "Binance BTCUSDT (spot)",
         "okex": "OKX BTC-USDT (spot)", "bybit": "Bybit BTCUSDT (perp)"}


def asof_1s(ts_us: np.ndarray, px: np.ndarray, grid_ms: np.ndarray) -> np.ndarray:
    idx = np.searchsorted(ts_us // 1000, grid_ms, side="right") - 1
    out = np.where(idx >= 0, px[np.maximum(idx, 0)], np.nan)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", default="2026-09-01")
    ap.add_argument("--dir", required=True)
    a = ap.parse_args()
    d = datetime.fromisoformat(a.day).replace(tzinfo=timezone.utc)
    ref = load_l1("BTCUSDT", d, d + timedelta(days=1), step_ms=1000, source="tardis")
    grid_ms = ((ref.index - pd.Timestamp(0, tz="UTC")) // pd.Timedelta("1ms")).to_numpy().astype(np.int64)
    P = pd.DataFrame({"binance-perp mid": ref["mid"].to_numpy()}, index=ref.index)
    for f in sorted(Path(a.dir).glob("*_trades_*.csv.gz")):
        ex = f.name.split("_")[0]
        if ex not in LABEL:
            continue
        t = pd.read_csv(f, usecols=["timestamp", "price"])
        P[LABEL[ex]] = asof_1s(t["timestamp"].to_numpy(), t["price"].to_numpy(), grid_ms)
        print(f"{LABEL[ex]:28s} {len(t):>9,} trades", flush=True)
    P = P.dropna()
    refp = P["binance-perp mid"]
    lr = np.log(P).diff().dropna()
    print(f"\n{len(P):,} matched seconds on {a.day}.  Basis = venue - Binance perp mid, in bp.\n")
    rows = []
    for c in P.columns:
        if c == "binance-perp mid":
            continue
        b = (P[c] / refp - 1) * 1e4
        x, y = lr["binance-perp mid"].to_numpy(), lr[c].to_numpy()
        lags = range(-5, 6)
        cc = [np.corrcoef(x[max(0, -k):len(x) - max(0, k)], y[max(0, k):len(y) - max(0, -k)])[0, 1] for k in lags]
        rows.append({"venue": c, "median basis": b.median(), "sd basis": b.std(), "|basis| p99": b.abs().quantile(.99),
                     "|basis| max": b.abs().max(), "best lag (s)": list(lags)[int(np.argmax(cc))], "corr@0": cc[5], "corr@best": max(cc)})
    tab = pd.DataFrame(rows).set_index("venue")
    pd.set_option("display.width", 200); print(tab.round(2).to_string())
    print("\n(best lag > 0 means the venue's 1 s return correlates most with the perp's return k seconds EARLIER, i.e. the perp leads)")
    spread = (P.max(axis=1) / P.min(axis=1) - 1) * 1e4
    print(f"\ncross-venue range (max-min over all {P.shape[1]} series, bp):  median {spread.median():.1f}   p90 {spread.quantile(.9):.1f}   "
          f"p99 {spread.quantile(.99):.1f}   max {spread.max():.1f} at {spread.idxmax()}")
    big = lr["binance-perp mid"].abs().idxmax()
    w = P.loc[big - pd.Timedelta(seconds=5): big + pd.Timedelta(seconds=10)]
    print(f"\nlargest 1 s perp move of the day ({lr['binance-perp mid'].abs().max()*1e4:.0f} bp at {big}); venue prices around it, bp vs perp mid at t-5:")
    print(((w / w.iloc[0]["binance-perp mid"] - 1) * 1e4).round(1).to_string())


if __name__ == "__main__":
    main()
