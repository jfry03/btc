"""
Shared helpers for the vol notebooks in analysis/ (ES and BTC).

Streaming builders (one pass, bounded memory, cached under data/derived/):
    build_es_rv_1min()   per-minute realised variance of the ES size-weighted mid from the 1s BBO
    build_btc_rv_1min()  per-minute realised variance of BTCUSDT spot from the 1s klines
    build_grid_10s()     BTC close + ES swmid sampled every 10 s (for betas / lead-lag)
    cached(path, fn)     read the parquet if it exists, else build and write it

Analysis:
    acf, fit_acf, MODELS         autocorrelation and exponential / power-law / two-exponential fits
    shock_response               mean of a series around selected positions (event study)
    ann, ET, tod_dow             annualisation and Eastern-time minute-of-day helpers
"""
from __future__ import annotations

import time
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.optimize import curve_fit

ET = "America/New_York"
DERIVED = Path("data/derived")
MIN_PER_YR = 252 * 1380                   # ES trading minutes per year (23 h Globex day)
MIN_PER_YR_24_7 = 365.25 * 1440           # BTC trades every minute

ES_COLS = ["bid_px_00", "ask_px_00", "bid_sz_00", "ask_sz_00", "instrument_id"]


def ann(v, per_yr=MIN_PER_YR):
    """per-minute variance -> annualised vol"""
    return np.sqrt(v * per_yr)


def tod_dow(index):
    """(minute of day, day of week) in Eastern time for a UTC DatetimeIndex"""
    loc = index.tz_convert(ET)
    return loc.hour * 60 + loc.minute, loc.dayofweek


def hhmm(m):
    return f"{(m // 60) % 24:02d}:{m % 60:02d}"


def cached(path, build, **kw):
    path = Path(path)
    if path.exists():
        return pd.read_parquet(path)
    t = time.perf_counter()
    df = build(**kw)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path)
    print(f"built {path} ({len(df):,} rows) in {time.perf_counter() - t:.0f}s")
    return df


# ----------------------------------------------------------------------------- builders

def _in_break(ts_ns):
    """True where a UTC ns timestamp falls in the 17:00-18:00 ET CME maintenance break (sparse, unreliable
    pre-open indications live there; a return *from* one of them must not count towards the 18:00 reopen)."""
    loc = pd.to_datetime(ts_ns, utc=True).tz_convert(ET)
    return np.asarray(loc.hour == 17)


def build_es_rv_1min(start=None, end=None, batch_rows=2_000_000, max_gap="120s"):
    """One pass over the ES 1s BBO. Per UTC minute: rv (sum of squared swmid log-returns between
    consecutive rows with gap <= max_gap and the same contract), n (returns summed), moves (of which
    the mid changed). One ~2M-row batch in memory at a time; the last row is carried across batches."""
    from data_collection.es_futures import iter_bbo
    max_gap_ns = int(pd.Timedelta(max_gap).value)
    parts, carry = [], None
    for b in iter_bbo(start, end, columns=ES_COLS, batch_rows=batch_rows):
        b = b.sort_index()
        if carry is not None:
            b = pd.concat([carry, b])
        px = ((b.bid_sz_00 * b.ask_px_00 + b.ask_sz_00 * b.bid_px_00) / (b.bid_sz_00 + b.ask_sz_00)).to_numpy()
        mid = ((b.bid_px_00 + b.ask_px_00) / 2).to_numpy()
        ts, inst = b.index.asi8, b.instrument_id.to_numpy()
        r = np.diff(np.log(px))
        ok = (np.diff(ts) <= max_gap_ns) & (inst[1:] == inst[:-1]) & np.isfinite(r) & ~_in_break(ts[:-1])
        g = pd.DataFrame({"minute": ts[1:] // 60_000_000_000, "rv": np.where(ok, r * r, 0.0), "n": ok,
                          "moves": ok & (np.diff(mid) != 0)}).groupby("minute").sum()
        parts.append(g)
        carry = b.iloc[-1:]
    rv = pd.concat(parts).groupby(level=0).sum()
    rv.index = pd.to_datetime(rv.index * 60, unit="s", utc=True).rename("minute")
    return rv


def _kline_files(symbol="BTCUSDT", market="spot", interval="1s", start=None, end=None):
    files = sorted(Path("data/parquet").glob(f"{symbol}_{market}_{interval}_????-??.parquet"))
    def month(f):
        return pd.Timestamp(f.stem[-7:] + "-01", tz="UTC")
    lo = pd.Timestamp(start).to_period("M").to_timestamp().tz_localize("UTC") if start else None
    hi = pd.Timestamp(end, tz="UTC") if end else None
    return [f for f in files if (lo is None or month(f) >= lo) and (hi is None or month(f) < hi)]


def build_btc_rv_1min(start=None, end=None, symbol="BTCUSDT", max_gap="120s"):
    """One monthly 1s-kline file at a time (close column only, ~2.6M floats). Per UTC minute:
    rv = sum of squared 1s close-to-close log returns (gap <= max_gap), n = returns summed,
    moves = returns that were non-zero. The last close is carried across months."""
    max_gap_ns = int(pd.Timedelta(max_gap).value)
    parts, carry = [], None
    for f in _kline_files(symbol, start=start, end=end):
        c = pd.read_parquet(f, columns=["close"])["close"]
        c = c[~c.index.duplicated()].sort_index(); c.index = c.index.as_unit("ns")   # klines are stored at ms resolution
        if carry is not None:
            c = pd.concat([carry, c])
        ts, px = c.index.asi8, c.to_numpy()
        r = np.diff(np.log(px)); ok = (np.diff(ts) <= max_gap_ns) & np.isfinite(r)
        g = pd.DataFrame({"minute": ts[1:] // 60_000_000_000, "rv": np.where(ok, r * r, 0.0), "n": ok,
                          "moves": ok & (r != 0)}).groupby("minute").sum()
        parts.append(g)
        carry = c.iloc[-1:]
    rv = pd.concat(parts).groupby(level=0).sum()
    rv.index = pd.to_datetime(rv.index * 60, unit="s", utc=True).rename("minute")
    if start or end:
        rv = rv[(rv.index >= pd.Timestamp(start or rv.index[0], tz="UTC")) & (rv.index < pd.Timestamp(end or rv.index[-1] + pd.Timedelta("1min"), tz="UTC"))]
    return rv


def build_grid_10s(start="2023-05-16", end=None, step="10s", symbol="BTCUSDT"):
    """BTC spot close and ES size-weighted mid sampled every `step` (epoch-aligned), plus es_roll
    (first grid point of a new ES contract) and es_instrument. BTC rows are the base (24/7); ES is
    NaN when Globex is closed. ~3.2M rows per year at 10 s."""
    from data_collection.es_futures import iter_bbo
    step_ns = int(pd.Timedelta(step).value)
    es_parts = []
    for b in iter_bbo(start, end, freq=step, columns=ES_COLS, batch_rows=2_000_000):
        sw = (b.bid_sz_00 * b.ask_px_00 + b.ask_sz_00 * b.bid_px_00) / (b.bid_sz_00 + b.ask_sz_00)
        es_parts.append(pd.DataFrame({"es": sw, "es_instrument": b.instrument_id}))
    es = pd.concat(es_parts).sort_index(); es = es[~es.index.duplicated()]
    es["es_roll"] = es.es_instrument.ne(es.es_instrument.shift()); es.iloc[0, es.columns.get_loc("es_roll")] = False
    btc_parts = []
    for f in _kline_files(symbol, start=start, end=end):
        c = pd.read_parquet(f, columns=["close"])["close"]; c.index = c.index.as_unit("ns")
        c = c[(c.index.asi8 % step_ns) == 0]
        btc_parts.append(c.rename("btc"))
    btc = pd.concat(btc_parts).sort_index(); btc = btc[~btc.index.duplicated()]
    btc = btc[(btc.index >= pd.Timestamp(start, tz="UTC")) & ((end is None) | (btc.index < pd.Timestamp(end or "2100-01-01", tz="UTC")))]
    grid = btc.to_frame().join(es, how="left")
    grid["es_roll"] = grid.es_roll.fillna(False).astype(bool)
    grid.index.name = "ts"
    return grid


# ----------------------------------------------------------------------------- autocorrelation

def acf(a, nlags):
    a = np.asarray(a, float); a = a - a.mean(); n = len(a)
    f = np.fft.rfft(a, 2 * n); ac = np.fft.irfft(f * np.conj(f))[:nlags + 1]
    return ac / ac[0]


MODELS = {"exponential": (lambda k, a, tau: a * np.exp(-k / tau), [0.5, 100], ([0, 1e-3], [1, 1e7])),
          "power law": (lambda k, c, alpha: c * k ** (-alpha), [0.5, 0.3], ([0, 0], [5, 3])),
          "two exponentials": (lambda k, a1, t1, a2, t2: a1 * np.exp(-k / t1) + a2 * np.exp(-k / t2),
                               [0.3, 20, 0.2, 2000], ([0, 1e-3, 0, 1e-3], [1, 1e7, 1, 1e7]))}


def fit_acf(rho, kmax, unit="min"):
    """fit MODELS to rho[1..kmax] on log-spaced lags; returns (table, {name: (lags, fitted)})"""
    ks = np.unique(np.geomspace(1, kmax, 300).astype(int)); y = rho[ks]
    out, curves = [], {}
    for name, (f, p0, bounds) in MODELS.items():
        try:
            p, _ = curve_fit(f, ks, y, p0=p0, bounds=bounds, maxfev=50000)
            yhat = f(ks, *p); r2 = 1 - ((y - yhat) ** 2).sum() / ((y - y.mean()) ** 2).sum()
            curves[name] = (ks, yhat)
            row = {"model": name, "R2 (log-spaced lags)": r2, "params": ", ".join(f"{v:.4g}" for v in p)}
            if name == "exponential":
                row["half-life"] = f"{p[1] * np.log(2):.1f} {unit}"
            if name == "two exponentials":
                (a1, t1), (a2, t2) = sorted([(p[0], p[1]), (p[2], p[3])], key=lambda z: z[1])
                row["half-life"] = f"fast {t1 * np.log(2):.1f} {unit} (weight {a1 / (a1 + a2):.0%}), slow {t2 * np.log(2):.0f} {unit}"
            if name == "power law":
                row["half-life"] = f"alpha={p[1]:.3f}: rho halves every x{2 ** (1 / p[1]):.1f} in lag"
            out.append(row)
        except (RuntimeError, ValueError) as e:
            out.append({"model": name, "params": f"fit failed: {e}"})
    return pd.DataFrame(out).set_index("model"), curves


def plot_acf_fits(ax_loglog, ax_semilog, rho, curves, kmax, label, day_len=None, color="steelblue"):
    k = np.arange(1, kmax + 1)
    for ax in (ax_loglog, ax_semilog):
        ax.plot(k, rho[1:kmax + 1], lw=0.8, color=color, label=label)
        for (name, (ks, yhat)), ls in zip(curves.items(), ("--", "-.", ":")):
            ax.plot(ks, yhat, ls=ls, lw=1.2, color="k", label=f"fit: {name}")
        if day_len:
            for d in range(1, int(kmax // day_len) + 1):
                ax.axvline(day_len * d, color="grey", lw=0.5, ls=":")
        ax.grid(alpha=0.3, which="both"); ax.set_yscale("log"); ax.set_ylim(1e-3, 1)
    ax_loglog.set_xscale("log")


def shock_response(values, pos, K=120, PRE=30):
    """mean of `values` at offsets -PRE..K around each position in `pos`; returns (offsets, means, n)"""
    values = np.asarray(values, float); N = len(values)
    pos = np.asarray(pos); pos = pos[(pos >= PRE) & (pos + K < N)]
    ks = np.arange(-PRE, K + 1)
    return ks, np.array([np.nanmean(values[pos + k]) for k in ks]), len(pos)


def deseasonalise(rv, tod, groups=None, min_n=1):
    """x = rv / mean rv at the same minute of day (and optional extra grouping, e.g. weekend flag).
    Returns (x, seasonal) with x NaN where n < min_n."""
    keys = [tod] if groups is None else [tod, groups]
    seasonal = rv.groupby(keys).transform("mean")
    return rv / seasonal, seasonal


# ----------------------------------------------------------------------------- event lift table

EVENT_TIMES = {"Globex open 18:00": 18 * 60, "Europe open 03:00": 3 * 60, "US data 08:30": 8 * 60 + 30,
               "Cash open 09:30": 9 * 60 + 30, "10:00 data": 10 * 60, "London close 11:30": 11 * 60 + 30,
               "14:00 (FOMC days only matter)": 14 * 60, "Cash close 16:00": 16 * 60}
LIFT_WINDOWS = [("min 0", 0, 0), ("min 1", 1, 1), ("min 0-4", 0, 4), ("min 5-14", 5, 14), ("min 15-29", 15, 29), ("min 30-59", 30, 59)]


def event_lift(rv, tod, events=EVENT_TIMES, windows=LIFT_WINDOWS, pre=(-45, -15)):
    """How much each scheduled time raises vol. For every event, mean per-minute variance in short windows
    after it, as a VOL multiple (sqrt of the variance ratio) of two baselines:
      'x avg minute'  - the all-sample mean variance per minute (how the event compares with an average minute)
      'x pre-event'   - the mean variance in minutes pre[0]..pre[1] before the event (the local lift; NaN when
                        there is no data before, e.g. the Globex reopen after the break)
    rv: per-minute variance Series; tod: minute-of-day (ET) aligned with it."""
    rv = pd.Series(np.asarray(rv, float)); tod = np.asarray(tod)
    base = rv.mean(); rows = []
    for name, t0 in events.items():
        k = (tod - t0) % 1440; k = np.where(k > 720, k - 1440, k)          # signed minutes from the event
        prof = rv.groupby(k).mean()
        pre_m = prof.loc[pre[0]:pre[1]].mean() if len(prof.loc[pre[0]:pre[1]]) >= 5 else np.nan
        row = {"event": name, "pre-event vol (x avg minute)": np.sqrt(pre_m / base)}
        for wname, a, b in windows:
            m = prof.loc[a:b].mean()
            row[f"{wname} x avg minute"] = np.sqrt(m / base)
            row[f"{wname} x pre-event"] = np.sqrt(m / pre_m)
        rows.append(row)
    return pd.DataFrame(rows).set_index("event")


# ----------------------------------------------------------------------------- calendar-aware events

def calendar_events(start, end):
    """Actual per-day event timestamps (UTC) from exchange_calendars via data_collection.market_hours:
    DST of each market, holidays and early closes are all respected. Fixed-clock US releases (08:30, 10:00 ET)
    are placed relative to the NYSE open so they only count on NYSE trading days."""
    from data_collection.market_hours import sessions
    s = {ex: sessions(ex, start, end) for ex in ("CMES", "XTKS", "XLON", "XETR", "XNYS")}
    ev = {"Globex open (CME)": s["CMES"].open,
          "Tokyo open (TSE 09:00 JST)": s["XTKS"].open,
          "London open (LSE 08:00)": s["XLON"].open,
          "Frankfurt open (Xetra 09:00)": s["XETR"].open,
          "US data 08:30 ET (NYSE days)": s["XNYS"].open - pd.Timedelta("60min"),
          "NYSE open 09:30": s["XNYS"].open,
          "US data 10:00 ET (NYSE days)": s["XNYS"].open + pd.Timedelta("30min"),
          "London close (16:30)": s["XLON"].close,
          "NYSE close (incl. early closes)": s["XNYS"].close,
          "Globex close (CME, incl. early)": s["CMES"].close}
    return {k: pd.DatetimeIndex(v.dropna()) for k, v in ev.items()}


def event_lift_cal(rv, events, windows=LIFT_WINDOWS, pre=(-45, -15), ks=range(-60, 121)):
    """Like event_lift but aligned on actual event timestamps (one per day) rather than a clock time.
    rv: per-minute variance Series indexed by UTC minute. Returns (table, profiles) where profiles[event]
    is the mean variance at each minute offset."""
    rv = rv.copy(); rv.index = rv.index.floor("min"); base = rv.mean()
    rows, profiles = [], {}
    for name, ts in events.items():
        ts = ts.as_unit("ns").floor("min"); ts = ts[(ts >= rv.index[0]) & (ts <= rv.index[-1])]
        prof = pd.Series({k: rv.reindex(ts + pd.Timedelta(minutes=k)).mean() for k in ks})
        profiles[name] = prof
        pre_m = prof.loc[pre[0]:pre[1]].mean()
        row = {"event": name, "n days": len(ts), "pre-event vol (x avg minute)": np.sqrt(pre_m / base)}
        for wname, a, b in windows:
            m = prof.loc[a:b].mean(); row[f"{wname} x avg minute"] = np.sqrt(m / base); row[f"{wname} x pre-event"] = np.sqrt(m / pre_m)
        rows.append(row)
    return pd.DataFrame(rows).set_index("event"), profiles


# ----------------------------------------------------------------------------- Polymarket settlement regimes

# How the 5-minute BTC Up/Down markets have been settled (Chainlink BTC/USD). Verified against the Aug-2026 CLOB
# capture: the Binance reconstruction below agrees with the market's converged final price in ~100% of windows
# that moved > 1 bp under the regime in force, and drops to ~96-97% under the wrong one.
#   < 2026-08-07 00:00 UTC : single Chainlink price snapshot at window start and end (CexPrice stream)
#   2026-08-07 -> 08-14    : 30-second Chainlink TWAP at both ends (15m / 4h markets used 60 s from the start)
#   >= 2026-08-14          : 60-second Chainlink TWAP at both ends (data.chain.link/streams/btc-usd-twap-60s)
# Tie (end >= start) resolves Up.
SETTLEMENT_REGIMES = [(pd.Timestamp("2026-08-07", tz="UTC"), 0), (pd.Timestamp("2026-08-14", tz="UTC"), 30), (pd.Timestamp("2100-01-01", tz="UTC"), 60)]


def settlement_twap_seconds(ts) -> int:
    """TWAP window (seconds; 0 = spot snapshot) that settled a market whose window starts at ts."""
    ts = pd.Timestamp(ts).tz_convert("UTC") if pd.Timestamp(ts).tzinfo else pd.Timestamp(ts, tz="UTC")
    for until, secs in SETTLEMENT_REGIMES:
        if ts < until:
            return secs
    return 60


def settlement_reference(closes: pd.Series, ts, secs: int | None = None) -> float:
    """Binance proxy of the Chainlink reference value at instant ts: the mean of 1-second closes over
    (ts - secs, ts] (a spot snapshot when secs == 0). secs defaults to the regime in force at ts."""
    ts = pd.Timestamp(ts); secs = settlement_twap_seconds(ts) if secs is None else secs
    if secs == 0:
        v = closes.reindex([ts]).iloc[0]
        return float(v)
    return float(closes[(closes.index > ts - pd.Timedelta(seconds=secs)) & (closes.index <= ts)].mean())


def settlement_move(closes: pd.Series, t0, t1=None) -> float:
    """log(settlement reference at t1 / reference at t0) under the regime of t0; t1 defaults to t0 + 5 min.
    This is the quantity a 5-minute Up/Down market is settled on (Up iff >= 0)."""
    t0 = pd.Timestamp(t0); t1 = t0 + pd.Timedelta("5min") if t1 is None else pd.Timestamp(t1)
    secs = settlement_twap_seconds(t0)
    return float(np.log(settlement_reference(closes, t1, secs) / settlement_reference(closes, t0, secs)))
