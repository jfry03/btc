"""Do the 1 s / 1 m log-return moments agree across BTC data sources on the days they overlap?

Sources on disk: Binance bookTicker `archive` (2023-05 -> 2024-03), Tardis `tardis` (1st of each
month 2024-03 -> 2026-09), CryptoHFTData `hft` (L2 replayed to L1, 2025-06 -> 2026-01), and spot
1 s klines (close). Overlap days: 2024-03-01 (archive/tardis) and the 1st of 2025-07 .. 2026-01
(tardis/hft). Everything is streamed one day at a time.

    python -m analysis.source_compare
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

from data_collection.load import load_klines, load_l1, l1_source_for

DAYS = {
    date(2024, 3, 1): ["archive", "tardis"],
    **{date(y, m, 1): ["tardis", "hft"] for y, m in [(2025, 7), (2025, 8), (2025, 9), (2025, 10),
                                                     (2025, 11), (2025, 12), (2026, 1)]},
}
HORIZONS = [("1s", 1), ("1m", 60)]


def mom(lp: np.ndarray, k: int) -> dict:
    r = lp[k:] - lp[:-k]; r = r[np.isfinite(r)]
    return {"n": len(r), "zero": float(np.mean(r == 0)), "vol_bp": float(r.std() * 1e4),
            "skew": float(stats.skew(r)), "ex_kurt": float(stats.kurtosis(r))}


def main() -> None:
    rows = []
    for d, sources in DAYS.items():
        s = datetime(d.year, d.month, d.day, tzinfo=timezone.utc); e = s + timedelta(days=1)
        series = {}
        for src in sources:
            if l1_source_for("BTCUSDT", d, src) is None:
                print(f"{d}: no {src} on disk"); continue
            df = load_l1("BTCUSDT", s, e, step_ms=1000, source=src)
            series[f"{src}/mid"] = np.log(df["mid"].to_numpy())
            series[f"{src}/swmid"] = np.log(df["microprice"].to_numpy())
        k = load_klines("BTCUSDT", s, e, interval="1s", market="spot")
        if len(k):
            series["spot_kline/close"] = np.log(k["close"].to_numpy())
        for name, lp in series.items():
            for h, kk in HORIZONS:
                rows.append({"day": str(d), "horizon": h, "series": name, **mom(lp, kk)})
        print("done", d, flush=True)
    tab = pd.DataFrame(rows).set_index(["day", "horizon", "series"]).sort_index()
    pd.set_option("display.width", 200)
    print(tab.round(3).to_string())
    tab.to_csv(Path(__file__).with_name("source_compare.csv"))


if __name__ == "__main__":
    main()
