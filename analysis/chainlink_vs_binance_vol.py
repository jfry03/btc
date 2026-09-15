"""How well does the volatility of the Chainlink BTC/USD stream (what Polymarket settles on) match
Binance BTCUSDT spot? One day at 1 s: PMData's free Chainlink sample (2026-07-18) vs the Binance 1-s klines.

    python -m analysis.chainlink_vs_binance_vol      # prints the tables, writes analysis/chainlink_vs_binance_vol.csv

Sample: curl -L -o data/raw/pmdata/chainlink_streams_BTCUSD_2026-07-18.parquet https://api.pmdata.dev/examples/chainlink/streams
"""
import numpy as np
import pandas as pd

from analysis.twap_iv import TwapState

SPY = 365 * 86_400
DAY = pd.Timestamp("2026-07-18", tz="UTC")
SAMPLE = "data/raw/pmdata/chainlink_streams_BTCUSD_2026-07-18.parquet"
HORIZONS = [1, 2, 5, 10, 30, 60, 120, 300]


def ann(r, dt):
    return float(np.sqrt(np.nanmean(np.asarray(r) ** 2) / dt * SPY))


def load():
    cl = pd.read_parquet(SAMPLE, columns=["observationsTimestamp", "price"])
    cl = pd.Series(cl.price.astype(float).to_numpy() / 1e18, index=pd.DatetimeIndex(cl.observationsTimestamp).tz_localize("UTC")).sort_index()
    bn = pd.read_parquet(f"data/parquet/BTCUSDT_spot_1s_{DAY:%Y-%m}.parquet", columns=["close"])["close"]
    bn = bn[~bn.index.duplicated()].sort_index(); bn.index = bn.index.as_unit("ns")
    grid = pd.date_range(DAY, DAY + pd.Timedelta("1D") - pd.Timedelta("1s"), freq="1s")
    return cl.reindex(grid).ffill(), bn.reindex(grid).ffill()


def main():
    cl, bn = load()
    rc, rb = np.log(cl).diff(), np.log(bn).diff()
    print(f"basis Chainlink/Binance - 1: mean {((cl / bn - 1) * 1e4).mean():+.1f} bp, sd {((cl / bn - 1) * 1e4).std():.1f} bp")
    print("1-s return cross-correlation, Binance shifted k s (peak at k>0 = Chainlink lags):",
          {k: round(float(rc.corr(rb.shift(k))), 3) for k in range(-2, 5)})

    rows = []
    for h in HORIZONS:                                  # vol signature: non-overlapping h-second returns
        a, b = ann(np.log(cl).diff(h).iloc[::h].dropna(), h), ann(np.log(bn).diff(h).iloc[::h].dropna(), h)
        rows.append({"horizon_s": h, "chainlink_vol": a, "binance_vol": b, "ratio": a / b})
    sig = pd.DataFrame(rows).set_index("horizon_s")
    print("\nannualised realised vol by return horizon\n", sig.to_string(float_format=lambda x: f"{x:.3f}"))

    # the quantity a 5-minute market is settled on: 60-s TWAP at the close vs at the open
    tc, tb = cl.rolling(60).mean(), bn.rolling(60).mean()
    starts = pd.date_range(DAY + pd.Timedelta("5min"), DAY + pd.Timedelta("1D") - pd.Timedelta("5min"), freq="5min")
    mc = np.log(tc.reindex(starts + pd.Timedelta("5min")).to_numpy() / tc.reindex(starts).to_numpy())
    mb = np.log(tb.reindex(starts + pd.Timedelta("5min")).to_numpy() / tb.reindex(starts).to_numpy())
    ok = np.isfinite(mc) & np.isfinite(mb); mc, mb = mc[ok], mb[ok]
    print(f"\n5-min windows ({ok.sum()}): sd of the TWAP-to-TWAP log move  Chainlink {mc.std() * 1e4:.2f} bp  "
          f"Binance {mb.std() * 1e4:.2f} bp  corr {np.corrcoef(mc, mb)[0, 1]:.3f}  same sign {(np.sign(mc) == np.sign(mb)).mean():.1%}")
    c = np.sqrt(TwapState(1, 1, 300).variance_factor())
    print(f"model sd = sigma * c(300 s), c = {c:.2f}, with sigma from h-second Chainlink returns:")
    for h in (1, 10, 30, 60):
        s = ann(np.log(cl).diff(h).iloc[::h].dropna(), h) / np.sqrt(SPY)
        print(f"   h = {h:>3} s: {s * c * 1e4:.2f} bp")

    # the rolling 30-min estimator the live tool uses, from 1-s vs 30-s (overlapping) returns
    for h in (1, 30):
        vc = (np.log(cl).diff(h).pow(2).rolling(1800).mean() / h * SPY) ** 0.5
        vb = (np.log(bn).diff(h).pow(2).rolling(1800).mean() / h * SPY) ** 0.5
        vc, vb = vc.iloc[1830::60], vb.iloc[1830::60]; ratio = vc / vb
        print(f"rolling 30-min vol from {h:>2}-s returns: corr {vc.corr(vb):.3f}, Chainlink/Binance median {ratio.median():.3f} "
              f"(p10 {ratio.quantile(.1):.3f}, p90 {ratio.quantile(.9):.3f})")
    sig.to_csv("analysis/chainlink_vs_binance_vol.csv")


if __name__ == "__main__":
    main()
