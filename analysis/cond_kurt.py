"""How much of the k-second return kurtosis is explained by predictable volatility?

For each kept day (cached 1 s mid grid), standardise the forward k-second log return by the
realised vol of the previous W seconds (sum of squared 1 s returns, scaled to k seconds) and
compare the pooled skew / excess kurtosis of raw vs standardised returns. Also standardises by
the day's own full-day vol (the "regime" baseline). Streams one day at a time.

    python -m analysis.cond_kurt
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

CACHE = Path(__file__).resolve().parent.parent / "data" / "derived" / "l1_1s"
DAYS = pd.read_csv(Path(__file__).with_name("return_dists_days.csv"))
HORIZONS = [("1m", 60), ("5m", 300), ("15m", 900)]
WINDOWS = [("prev 15m", 900), ("prev 30m", 1800), ("prev 2h", 7200)]
VAR_FLOOR = (0.1e-4) ** 2   # never assume 1 s vol below 0.1 bp, however dead the last window looked
STRIDE = 10


def main() -> None:
    days = DAYS[DAYS.kept].day.tolist()
    out = {}
    for f in days:
        p = CACHE / f"{f}.parquet"
        if not p.exists():
            continue
        m = pd.read_parquet(p)["mid"].to_numpy()
        lp = np.log(m)
        r1 = np.diff(lp)                                     # 1 s returns, len N-1
        r1 = np.nan_to_num(r1)
        cs = np.concatenate([[0.0], np.cumsum(r1**2)])       # cs[i] = sum of r1[:i]
        day_var = np.nanvar(r1)
        for hn, k in HORIZONS:
            idx = np.arange(7200, len(lp) - k, STRIDE)       # start after the longest lookback
            rk = lp[idx + k] - lp[idx]
            ok = np.isfinite(rk)
            out.setdefault((hn, "raw"), []).append(rk[ok])
            out.setdefault((hn, "same-day vol"), []).append(rk[ok] / np.sqrt(day_var * k))
            for wn, W in WINDOWS:
                rv = (cs[idx] - cs[idx - W]) / W             # avg 1 s variance over the prior W seconds
                z = rk / np.sqrt(np.maximum(rv, VAR_FLOOR) * k)
                out.setdefault((hn, wn), []).append(z[ok])
    rows = []
    for (hn, name), parts in out.items():
        x = np.concatenate(parts)
        rows.append({"horizon": hn, "standardised by": name, "n": len(x),
                     "skew": stats.skew(x), "ex_kurt": stats.kurtosis(x),
                     "P(|z|>3)": np.mean(np.abs(x / x.std()) > 3), "P(|z|>5)": np.mean(np.abs(x / x.std()) > 5)})
    tab = pd.DataFrame(rows).set_index(["horizon", "standardised by"])
    pd.set_option("display.width", 200)
    print(tab.round(4).to_string())
    print("\nGaussian reference: P(|z|>3)=0.0027  P(|z|>5)=5.7e-07  ex_kurt=0")
    tab.to_csv(Path(__file__).with_name("cond_kurt.csv"))


if __name__ == "__main__":
    main()
