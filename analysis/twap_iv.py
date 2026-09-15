"""Implied volatility of a TWAP-settled Up/Down binary (Polymarket crypto 5 m / 15 m markets).

Model
-----
Log price is a driftless Brownian motion with per-second vol sigma. The market resolves Up if
the settlement TWAP - the equal-weighted mean of the last `m` one-second prices of the window -
is >= `ref`, the reference (itself the TWAP at the window open, known at t = 0).

At time `now` with n = seconds_left to the window end, the m TWAP samples split into k already
published (values in `observed`) and r = m - k still in the future at offsets tau_1 < ... < tau_r
seconds from now. In log terms, with a_i = ln(sample_i):

    A       = (1/m) * (sum_observed a_i + sum_future a_j)                 final log-TWAP
    E[A]    = (1/m) * (sum_observed a_i + r * ln(spot))                    driftless
    Var[A]  = sigma^2 * c^2,   c^2 = (1/m^2) * sum_i sum_j min(tau_i, tau_j)

    P(Up)   = Phi( (E[A] - ln ref) / (sigma * c) )

so the binary price is a function of d = E[A] - ln ref and sigma only through d / sigma, and the
implied vol is closed form:

    sigma_implied = d / ( c * Phi^{-1}(p_up) )

It exists only when the market agrees with the model on direction (sign(d) == sign(p_up - 1/2))
and p_up is strictly between 0 and 1; otherwise NaN is returned (the market says "more likely
Up" while the running average is below the line - no vol makes a symmetric model say that).

Nothing here is asset-specific: pass the right `ref`, `spot`, `observed`, `seconds_left` and
`twap_window` for BTC, ETH, SOL, ... and the same function applies. The lognormal-vs-arithmetic
TWAP distinction is a second-order (var/2 ~ 1e-8) effect at these horizons and is ignored.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Sequence

import numpy as np
import pandas as pd
from scipy.stats import norm

SEC_PER_YEAR = 365 * 86_400


@dataclass
class TwapState:
    ref: float                       # price to beat: the TWAP report at the window open
    spot: float                      # latest value of the series being averaged (Chainlink price, or a proxy)
    seconds_left: float              # window_end - now, in seconds (may be fractional)
    twap_window: int = 60            # m: number of one-second samples in the settlement TWAP
    observed: Sequence[float] = field(default_factory=tuple)  # TWAP samples already published, oldest first

    def future_offsets(self) -> np.ndarray:
        """Seconds from now to each not-yet-published TWAP sample (samples sit at T-m+1, ..., T)."""
        m, n = self.twap_window, self.seconds_left
        tau = n - m + np.arange(1, m + 1)          # offset of sample j = (T - m + j) - now
        return tau[tau > 0]

    def variance_factor(self) -> float:
        """c^2 such that Var[final log-TWAP] = sigma^2 * c^2."""
        tau = self.future_offsets()
        if len(tau) == 0:
            return 0.0
        return float(np.minimum.outer(tau, tau).sum()) / self.twap_window**2

    def log_mean(self) -> float:
        """E[final log-TWAP]. Missing observed samples are assumed equal to spot."""
        m = self.twap_window
        r = len(self.future_offsets())
        k = m - r
        obs = np.log(np.asarray(self.observed, dtype=float))[-k:] if k and len(self.observed) else np.array([])
        filled = k - len(obs)                      # published but not supplied -> use spot
        return (obs.sum() + (r + filled) * np.log(self.spot)) / m

    def d(self) -> float:
        """Expected log distance of the final TWAP above the reference."""
        return self.log_mean() - np.log(self.ref)


def prob_up(state: TwapState, sigma_per_sec: float) -> float:
    """P(final TWAP >= ref) under Brownian log price with per-second vol sigma."""
    c = np.sqrt(state.variance_factor())
    d = state.d()
    if c == 0:
        return float(d >= 0)
    return float(norm.cdf(d / (sigma_per_sec * c)))


def implied_vol(state: TwapState, p_up: float) -> float:
    """Per-second vol at which the model reproduces p_up; NaN if none exists."""
    if not 0 < p_up < 1:
        return np.nan
    c = np.sqrt(state.variance_factor())
    d = state.d()
    z = norm.ppf(p_up)
    if c == 0 or z == 0 or np.sign(d) != np.sign(z):
        return np.nan
    return float(d / (c * z))


def annualise(sigma_per_sec: float) -> float:
    return sigma_per_sec * np.sqrt(SEC_PER_YEAR)


def per_sec(sigma_ann: float) -> float:
    return sigma_ann / np.sqrt(SEC_PER_YEAR)


# --------------------------------------------------------------------------- batch helper

def implied_vol_series(trades: pd.DataFrame, price_1s: pd.Series, window_start: pd.Timestamp,
                       window_end: pd.Timestamp, twap_window: int = 60,
                       ref: float | None = None) -> pd.DataFrame:
    """Implied vol for every row of `trades` (needs `ts_exchange` and `price_prob` of the Up token)
    inside one market window, using a one-per-second price series as the settlement proxy.

    `ref` defaults to the mean of price_1s over the twap_window seconds ending at window_start
    (the open TWAP). Observed TWAP samples are read from price_1s as they become available.
    Returns the trades with columns d_bp, c, p_up, iv_sec, iv_ann, seconds_left added.
    """
    px = price_1s.sort_index()
    if ref is None:
        ref = px[window_start - pd.Timedelta(seconds=twap_window): window_start - pd.Timedelta(seconds=1)].mean()
    tail = px[window_end - pd.Timedelta(seconds=twap_window - 1): window_end]     # the m settlement samples
    out = []
    for ts, p in zip(trades["ts_exchange"], trades["price_prob"].astype(float)):
        ts = pd.Timestamp(ts)
        n = (window_end - ts).total_seconds()
        spot = px[:ts].iloc[-1]
        observed = tail[:ts].to_numpy()
        st = TwapState(ref=ref, spot=spot, seconds_left=n, twap_window=twap_window, observed=observed)
        iv = implied_vol(st, p)
        out.append({"seconds_left": n, "d_bp": st.d() * 1e4, "c": np.sqrt(st.variance_factor()),
                    "p_up": p, "iv_sec": iv, "iv_ann": annualise(iv)})
    return pd.concat([trades.reset_index(drop=True), pd.DataFrame(out)], axis=1)
