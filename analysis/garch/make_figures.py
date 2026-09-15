"""Figures + fitted numbers for the GARCH explainer (analysis/garch/garch.md -> PDF).
Uses the cached 1 s mid grids in data/derived/l1_1s; streams a day at a time."""
from __future__ import annotations

import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from numba import njit
from scipy import optimize, stats

HERE = Path(__file__).resolve().parent
CACHE = HERE.parent.parent / "data" / "derived" / "l1_1s"
DAYS = pd.read_csv(HERE.parent / "return_dists_days.csv")
DAYS = DAYS[DAYS.kept].day.tolist()
C = {"blue": "#2a78d6", "orange": "#eb6834", "aqua": "#1baf7a", "grey": "#52514e", "grid": "#e6e5e1", "muted": "#9a9891"}
plt.rcParams.update({"font.size": 9, "axes.titlesize": 10, "axes.titlelocation": "left", "axes.spines.top": False,
                     "axes.spines.right": False, "axes.edgecolor": C["grid"], "axes.grid": True, "grid.color": C["grid"],
                     "grid.linewidth": 0.6, "axes.axisbelow": True, "legend.frameon": False, "figure.dpi": 140})
rng = np.random.default_rng(7)


def minute_returns(day: str) -> np.ndarray:
    m = pd.read_parquet(CACHE / f"{day}.parquet")["mid"].to_numpy()
    lp = np.log(m[::60])                          # one-per-minute samples -> 1440 minute returns
    r = np.diff(lp) * 1e4
    return np.nan_to_num(r)


# ---------------------------------------------------------------- 1. clustering + ACF (one day, 1-min)
day = "2023-10-16"                                # fake ETF-approval tweet at 13:30 UTC + a 05:16 squeeze
if day not in DAYS:
    day = DAYS[len(DAYS) // 2]
r = minute_returns(day)
t = np.arange(len(r)) / 60
fig, ax = plt.subplots(2, 1, figsize=(8, 4.6), sharex=True, constrained_layout=True)
ax[0].plot(t, r, color=C["blue"], lw=0.7); ax[0].set_ylabel("1-min log return (bp)")
ax[0].set_title(f"BTCUSDT {day}: returns are not predictable, but their size is")
ax[1].plot(t, np.abs(r), color=C["orange"], lw=0.7); ax[1].set_ylabel("|return| (bp)"); ax[1].set_xlabel("hour (UTC)")
fig.savefig(HERE / "fig1_clustering.png"); plt.close(fig)


def acf(x: np.ndarray, nlags: int) -> np.ndarray:
    x = x - x.mean(); v = (x**2).sum()
    return np.array([(x[:-k] * x[k:]).sum() / v if k else 1.0 for k in range(nlags + 1)])


# pooled ACF over many days (average per-day ACFs so day boundaries never enter)
sample_days = DAYS[:: max(1, len(DAYS) // 120)]
A_r, A_r2 = [], []
for d in sample_days:
    x = minute_returns(d)
    A_r.append(acf(x, 120)); A_r2.append(acf(x**2, 120))
A_r, A_r2 = np.mean(A_r, 0), np.mean(A_r2, 0)
fig, ax = plt.subplots(1, 2, figsize=(8, 3), constrained_layout=True)
lags = np.arange(1, 121)
for a, y, ttl, col in ((ax[0], A_r, "ACF of 1-min returns  r", C["blue"]), (ax[1], A_r2, "ACF of squared returns  r²", C["orange"])):
    a.bar(lags, y[1:], color=col, width=1.0)
    a.axhline(0, color=C["grey"], lw=0.6); a.set_xlabel("lag (minutes)"); a.set_title(ttl)
    a.set_ylim(-0.05, 0.35)
ci = 1.96 / np.sqrt(1440)
for a in ax:
    a.axhspan(-ci, ci, color=C["grid"], alpha=0.6, lw=0)
fig.suptitle(f"averaged over {len(sample_days)} days; grey band = 95% noise level for one day", fontsize=8, x=0.01, ha="left", color=C["grey"])
fig.savefig(HERE / "fig2_acf.png"); plt.close(fig)


# ---------------------------------------------------------------- 2. simulation: iid vs GARCH
def simulate_garch(n, omega, alpha, beta, nu=None):
    eps = rng.standard_normal(n) if nu is None else rng.standard_t(nu, n) / np.sqrt(nu / (nu - 2))
    s2 = np.empty(n); x = np.empty(n); s2[0] = omega / (1 - alpha - beta)
    for i in range(n):
        if i:
            s2[i] = omega + alpha * x[i - 1] ** 2 + beta * s2[i - 1]
        x[i] = np.sqrt(s2[i]) * eps[i]
    return x, np.sqrt(s2)


n = 5000
sim_iid = rng.standard_normal(n)
sim_g, sig_g = simulate_garch(n, omega=0.02, alpha=0.12, beta=0.86)
fig, ax = plt.subplots(2, 2, figsize=(8, 5), constrained_layout=True, gridspec_kw={"width_ratios": [3, 1.3]})
for row, (x, ttl, col) in enumerate(((sim_iid, "iid Gaussian, σ = 1", C["blue"]), (sim_g, "GARCH(1,1): ω=0.02, α=0.12, β=0.86 (same unconditional σ = 1)", C["orange"]))):
    ax[row, 0].plot(x, color=col, lw=0.6); ax[row, 0].set_title(ttl); ax[row, 0].set_ylim(-10, 10)
    if row:
        ax[row, 0].plot(sig_g, color=C["grey"], lw=1.0, label="σ_t"); ax[row, 0].plot(-sig_g, color=C["grey"], lw=1.0); ax[row, 0].legend(loc="upper right")
    edges = np.linspace(-8, 8, 81)
    ax[row, 1].hist(x, bins=edges, density=True, histtype="step", color=col, lw=1.3)
    z = np.linspace(-8, 8, 300); ax[row, 1].plot(z, stats.norm.pdf(z), color=C["grey"], lw=0.8, ls="--")
    ax[row, 1].set_yscale("log"); ax[row, 1].set_ylim(1e-4, 1)
    ax[row, 1].set_title(f"ex-kurt = {stats.kurtosis(x):.1f}")
ax[1, 0].set_xlabel("time step"); ax[1, 1].set_xlabel("x (log density; dashed = N(0,1))")
fig.savefig(HERE / "fig3_sim.png"); plt.close(fig)


# ---------------------------------------------------------------- 3. fit GARCH(1,1) to BTC 1-min returns
@njit(cache=True)
def garch_recursion(r, omega, alpha, beta, s2_0):
    n = len(r); s2 = np.empty(n); s2[0] = s2_0
    for i in range(1, n):
        s2[i] = omega + alpha * r[i - 1] ** 2 + beta * s2[i - 1]
    return s2


def unpack(theta, r):
    """Variance targeting: omega is pinned so the model's long-run variance equals the sample variance.
    (Unconstrained MLE at 1-min frequency drives alpha+beta -> 1, i.e. IGARCH - see the note in the doc.)"""
    alpha = 1 / (1 + np.exp(-theta[0])); beta = (1 - alpha) / (1 + np.exp(-theta[1]))
    return r.var() * (1 - alpha - beta), alpha, beta


def negloglik(theta, r, dist="normal"):
    omega, alpha, beta = unpack(theta, r)
    s2 = garch_recursion(r, omega, alpha, beta, r.var())
    if dist == "normal":
        return 0.5 * np.sum(np.log(2 * np.pi * s2) + r**2 / s2)
    nu = 2.05 + np.exp(theta[2])
    z = r / np.sqrt(s2)
    return -np.sum(stats.t.logpdf(z, nu, scale=np.sqrt((nu - 2) / nu)) - 0.5 * np.log(s2))


fit_days = [d for d in DAYS if "2023-06-01" <= d <= "2024-03-30"]      # one contiguous era, no regime splices
R = np.concatenate([minute_returns(d) for d in fit_days])
opts = {"maxiter": 4000, "xatol": 1e-8, "fatol": 1e-8}
res_n = optimize.minimize(negloglik, x0=[np.log(0.1 / 0.9), np.log(0.9 / 0.1)], args=(R, "normal"), method="Nelder-Mead", options=opts)
om, al, be = unpack(res_n.x, R)                     # Gaussian QMLE for the variance dynamics ...
om_n, al_n, be_n = om, al, be
s2 = garch_recursion(R, om, al, be, R.var()); sig = np.sqrt(s2); z = R / sig
nu = stats.t.fit(z[np.abs(z) < 50], floc=0)[0]          # ... then a Student-t for the shape of the standardised residual
persist = al + be; half_life = np.log(0.5) / np.log(persist)
uncond = np.sqrt(om / (1 - persist))
fitted = {"days": len(fit_days), "n_minutes": int(len(R)), "omega": om, "alpha": al, "beta": be, "nu": nu,
          "omega_n": om_n, "alpha_n": al_n, "beta_n": be_n, "persistence": persist, "half_life_min": half_life,
          "uncond_vol_bp_per_min": uncond, "sample_vol_bp_per_min": float(R.std()),
          "ex_kurt_raw": float(stats.kurtosis(R)), "ex_kurt_z": float(stats.kurtosis(z)),
          "p_gt3_raw": float(np.mean(np.abs(R / R.std()) > 3)), "p_gt3_z": float(np.mean(np.abs(z) > 3)),
          "p_gt5_raw": float(np.mean(np.abs(R / R.std()) > 5)), "p_gt5_z": float(np.mean(np.abs(z) > 5)),
          "acf1_r2_raw": float(acf(R**2, 1)[1]), "acf1_r2_z": float(acf(z**2, 1)[1])}
json.dump(fitted, open(HERE / "fitted.json", "w"), indent=1)
print(json.dumps(fitted, indent=1))

# fig 4: fitted sigma vs |r| over 3 consecutive fit days
k0 = 1440 * 40; span = slice(k0, k0 + 1440 * 3)
tt = np.arange(1440 * 3) / 60
fig, ax = plt.subplots(figsize=(8, 3.2), constrained_layout=True)
ax.plot(tt, np.abs(R[span]), color=C["blue"], lw=0.5, alpha=0.7, label="|1-min return|")
ax.plot(tt, sig[span], color=C["orange"], lw=1.3, label="GARCH σ_t (fitted, one-step-ahead)")
ax.axhline(uncond, color=C["grey"], lw=0.8, ls="--", label=f"unconditional σ = {uncond:.1f} bp")
ax.set_xlabel(f"hours from {fit_days[40]} 00:00 UTC (three consecutive sampled days)"); ax.set_ylabel("bp per minute")
ax.set_title("The model tracks the level of volatility - and mean-reverts toward the long-run level"); ax.legend(loc="upper right")
fig.savefig(HERE / "fig4_fitted_sigma.png"); plt.close(fig)

# fig 5: raw vs standardised residual distribution
fig, ax = plt.subplots(1, 2, figsize=(8, 3.2), constrained_layout=True)
edges = np.linspace(-10, 10, 101); zz = np.linspace(-10, 10, 400)
ax[0].hist(R / R.std(), bins=edges, density=True, histtype="step", color=C["blue"], lw=1.3, label="raw r / sample σ")
ax[0].hist(z, bins=edges, density=True, histtype="step", color=C["orange"], lw=1.3, label="z = r / σ_t (GARCH)")
ax[0].plot(zz, stats.norm.pdf(zz), color=C["grey"], lw=0.8, ls="--", label="N(0,1)")
ax[0].plot(zz, stats.t.pdf(zz, nu, scale=np.sqrt((nu - 2) / nu)), color=C["aqua"], lw=0.9, label=f"Student-t, ν={nu:.1f}")
ax[0].set_yscale("log"); ax[0].set_ylim(1e-5, 1); ax[0].legend(fontsize=7); ax[0].set_title("Density (log scale)")
q = np.linspace(0.0005, 0.9995, 400)
ax[1].plot(stats.norm.ppf(q), np.quantile(R / R.std(), q), color=C["blue"], lw=1.2, label="raw")
ax[1].plot(stats.norm.ppf(q), np.quantile(z, q), color=C["orange"], lw=1.2, label="GARCH-standardised")
ax[1].plot([-4, 4], [-4, 4], color=C["grey"], lw=0.8, ls="--"); ax[1].set_xlabel("Gaussian quantile"); ax[1].set_ylabel("sample quantile")
ax[1].set_title("QQ plot vs Gaussian"); ax[1].legend(fontsize=7); ax[1].set_ylim(-12, 12)
fig.savefig(HERE / "fig5_residuals.png"); plt.close(fig)

# fig 6: forecast term structure from two starting states
h = np.arange(1, 1441)
fig, ax = plt.subplots(figsize=(8, 3), constrained_layout=True)
for s0, lab, col in ((uncond * 2.5, "after a burst: σ_now = 2.5 × long-run", C["orange"]), (uncond * 0.5, "quiet: σ_now = 0.5 × long-run", C["blue"])):
    f = uncond**2 + (s0**2 - uncond**2) * persist ** (h - 1)
    ax.plot(h / 60, np.sqrt(f), color=col, lw=1.4, label=lab)
ax.axhline(uncond, color=C["grey"], lw=0.8, ls="--", label="long-run σ")
ax.axvline(half_life / 60, color=C["muted"], lw=0.8, ls=":"); ax.text(half_life / 60 + 0.1, uncond * 2.4, f"half-life ≈ {half_life / 60:.1f} h", fontsize=8, color=C["grey"])
ax.axvspan(0, 5 / 60, color=C["aqua"], alpha=0.35, lw=0); ax.text(0.15, uncond * 0.55, "5-min market", fontsize=8, color=C["aqua"])
ax.set_xlabel("hours ahead"); ax.set_ylabel("forecast σ (bp / min)"); ax.legend(); ax.set_title("Forecasts decay geometrically toward the long-run level at rate α+β per step")
fig.savefig(HERE / "fig6_forecast.png"); plt.close(fig)

# fig 7: intraday seasonality of 1-min vol (UTC hour) — what plain GARCH ignores
H = np.zeros(24); cnt = np.zeros(24)
for d in sample_days:
    x = np.r_[minute_returns(d), 0.0].reshape(24, 60)
    H += (x**2).sum(1); cnt += 60
hour_vol = np.sqrt(H / cnt)
fig, ax = plt.subplots(figsize=(8, 2.8), constrained_layout=True)
ax.bar(np.arange(24), hour_vol, color=C["blue"], width=0.8)
ax.set_xlabel("hour of day (UTC)"); ax.set_ylabel("RMS 1-min return (bp)"); ax.set_xticks(range(0, 24, 2))
ax.set_title(f"Deterministic time-of-day pattern in volatility ({len(sample_days)} days) - GARCH does not know about this")
fig.savefig(HERE / "fig7_seasonality.png"); plt.close(fig)
print("figures written")
