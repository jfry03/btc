"""
Opening / closing times of the major world stock and futures exchanges, for point lookups and
for vectorised features on a UTC 1-second grid.

Sessions and holidays come from `exchange_calendars` (XNYS, XLON, XTKS, CMES, …). A few things the
library lacks are hard-coded and flagged in `EXCHANGES[...]["notes"]`:

    XSHE          Shenzhen: same hours/holidays as Shanghai (XSHG) - aliased
    XNSE          NSE India: 09:15-15:30 IST on XBOM's (BSE) trading days - same holidays in practice
    EUREX_DERIV   Eurex index derivatives: 01:10-22:00 Europe/Berlin on XEUR trading days
                  (the XEUR calendar itself is 08:00-22:00 CET, the cash/main session)
    US_EXTENDED   US equity pre/after-hours: 04:00-09:30 and 16:00-20:00 America/New_York on XNYS
                  sessions (ends at the early close on half days)

Futures: CMES (CME Globex equity index: ES, NQ …) sessions run Sun 17:00 CT -> Fri 16:00 CT with a
daily 16:00-17:00 CT maintenance break, which appears here as the gap between consecutive sessions
(hard-coded: the library has 17:00 -> 17:00 with no break). IEPA = ICE US futures. ICE Europe is
not in the library (XICE is Iceland) and is not included.

All timestamps in and out are tz-aware UTC pandas Timestamps / DatetimeIndex.

    from data_collection.market_hours import is_open, sessions, status, open_flags
    is_open("XNYS", "2026-03-09 14:00Z")          # True
    sessions("XTKS", "2026-03-09", "2026-03-10")   # open/close/break per day, UTC
    status("2026-03-09 14:00Z")                    # one row per exchange
    open_flags(pd.date_range("2026-03-09", periods=86400, freq="1s", tz="UTC"))
"""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Iterable

import numpy as np
import pandas as pd

try:
    import exchange_calendars as xcals
except ImportError as exc:  # pragma: no cover
    raise ImportError("pip install exchange_calendars") from exc

CAL_START, CAL_END = "2015-01-01", "2030-12-31"
MAJOR = ("XNYS", "XNAS", "XLON", "XETR", "XTKS", "XHKG", "XSHG", "CMES")


@dataclass(frozen=True)
class Exchange:
    code: str
    name: str
    tz: str
    calendar: str            # exchange_calendars code the sessions/holidays come from
    kind: str                # equity | futures
    notes: str = ""
    # hard-coded local-time overrides (HH:MM) applied on the calendar's trading days
    local_open: str | None = None
    local_close: str | None = None
    local_windows: tuple[tuple[str, str], ...] | None = None   # multiple windows per day (extended hours)


EXCHANGES: dict[str, Exchange] = {e.code: e for e in [
    Exchange("XNYS", "New York Stock Exchange", "America/New_York", "XNYS", "equity"),
    Exchange("XNAS", "NASDAQ", "America/New_York", "XNAS", "equity"),
    Exchange("XTSE", "Toronto Stock Exchange", "America/Toronto", "XTSE", "equity"),
    Exchange("XLON", "London Stock Exchange", "Europe/London", "XLON", "equity"),
    Exchange("XETR", "Xetra (Frankfurt)", "Europe/Berlin", "XETR", "equity"),
    Exchange("XPAR", "Euronext Paris", "Europe/Paris", "XPAR", "equity"),
    Exchange("XAMS", "Euronext Amsterdam", "Europe/Amsterdam", "XAMS", "equity"),
    Exchange("XSWX", "SIX Swiss Exchange", "Europe/Zurich", "XSWX", "equity"),
    Exchange("XTKS", "Tokyo Stock Exchange", "Asia/Tokyo", "XTKS", "equity", "lunch break 11:30-12:30 JST"),
    Exchange("XHKG", "Hong Kong Exchange", "Asia/Hong_Kong", "XHKG", "equity", "lunch break 12:00-13:00 HKT"),
    Exchange("XSHG", "Shanghai Stock Exchange", "Asia/Shanghai", "XSHG", "equity", "lunch break 11:30-13:00 CST"),
    Exchange("XSHE", "Shenzhen Stock Exchange", "Asia/Shanghai", "XSHG", "equity", "HARD-CODED: aliased to XSHG (same hours and holidays)"),
    Exchange("XKRX", "Korea Exchange", "Asia/Seoul", "XKRX", "equity"),
    Exchange("XSES", "Singapore Exchange", "Asia/Singapore", "XSES", "equity"),
    Exchange("XASX", "Australian Securities Exchange", "Australia/Sydney", "XASX", "equity"),
    Exchange("XBOM", "BSE (Bombay)", "Asia/Kolkata", "XBOM", "equity"),
    Exchange("XNSE", "NSE India", "Asia/Kolkata", "XBOM", "equity",
             "HARD-CODED: 09:15-15:30 IST on BSE trading days (NSE and BSE share holidays)",
             local_open="09:15", local_close="15:30"),
    Exchange("CMES", "CME Globex equity index futures (ES, NQ, YM, RTY)", "America/Chicago", "CMES", "futures",
             "Sun 17:00 -> Fri 16:00 CT. HARD-CODED: the library has sessions 17:00->17:00 CT with no break; "
             "regular 17:00 CT closes are moved to 16:00 CT so the 16:00-17:00 maintenance break is a gap. "
             "Early closes (e.g. 12:15 CT holidays) are kept as the library has them"),
    Exchange("XCBF", "Cboe Futures (VIX)", "America/Chicago", "XCBF", "futures", "regular session only (08:30-15:15 CT)"),
    Exchange("IEPA", "ICE US futures", "America/New_York", "IEPA", "futures"),
    Exchange("XEUR", "Eurex (main session)", "Europe/Berlin", "XEUR", "futures", "08:00-22:00 CET per exchange_calendars"),
    Exchange("EUREX_DERIV", "Eurex index derivatives (FDAX, FESX)", "Europe/Berlin", "XEUR", "futures",
             "HARD-CODED: 01:10-22:00 Europe/Berlin on XEUR trading days", local_open="01:10", local_close="22:00"),
    Exchange("US_EXTENDED", "US equities pre/after-hours", "America/New_York", "XNYS", "equity",
             "HARD-CODED: 04:00-09:30 and 16:00-20:00 ET on NYSE sessions; after-hours starts at the (early) close",
             local_windows=(("04:00", "open"), ("close", "20:00"))),
]}


# --------------------------------------------------------------------------- sessions

@lru_cache(maxsize=None)
def _calendar(code: str):
    """Calendar from CAL_START to CAL_END, clipped to the last year the library has holidays for
    (XSHG/XHKG etc. are only recorded a year or two ahead)."""
    end = pd.Timestamp(CAL_END)
    bound = xcals.get_calendar(code).bound_max()  # library's own limit for this calendar
    if bound is not None and bound < end:
        end = bound
    return xcals.get_calendar(code, start=CAL_START, end=end)


def _to_utc(ts) -> pd.Timestamp:
    t = pd.Timestamp(ts)
    return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")


def _local(day: pd.Timestamp, hhmm: str, tz: str) -> pd.Timestamp:
    h, m = map(int, hhmm.split(":"))
    return pd.Timestamp(day.year, day.month, day.day, h, m, tz=tz).tz_convert("UTC")


@lru_cache(maxsize=None)
def _all_sessions(code: str) -> pd.DataFrame:
    """Every session 2015-2030 for an exchange, UTC: open, close, break_start, break_end."""
    ex = EXCHANGES[code]
    sched = _calendar(ex.calendar).schedule[["open", "break_start", "break_end", "close"]].copy()
    sched.index = pd.DatetimeIndex(sched.index).tz_localize(None)
    if ex.calendar == "CMES":
        local_close = sched["close"].dt.tz_convert(ex.tz)
        regular = (local_close.dt.hour == 17) & (local_close.dt.minute == 0)
        sched.loc[regular, "close"] = sched.loc[regular, "close"] - pd.Timedelta(hours=1)
    if ex.local_open or ex.local_close:
        days = sched.index
        if ex.local_open:
            sched["open"] = pd.DatetimeIndex([_local(d, ex.local_open, ex.tz) for d in days])
        if ex.local_close:
            sched["close"] = pd.DatetimeIndex([_local(d, ex.local_close, ex.tz) for d in days])
        sched["break_start"] = pd.NaT; sched["break_end"] = pd.NaT
    if ex.local_windows:
        # Two windows per day (pre-market, after-hours): encode as open..break_start, break_end..close
        (w0a, w0b), (w1a, w1b) = ex.local_windows
        base = sched.copy()
        def pick(v, d, row):
            return row[v] if v in ("open", "close") else _local(d, v, ex.tz)
        rows = []
        for d, row in base.iterrows():
            rows.append((pick(w0a, d, row), pick(w0b, d, row), pick(w1a, d, row), pick(w1b, d, row)))
        sched = pd.DataFrame(rows, index=base.index, columns=["open", "break_start", "break_end", "close"])
    for c in sched.columns:
        sched[c] = pd.to_datetime(sched[c], utc=True)
    sched.index.name = "session"
    return sched


def sessions(exchange: str, start, end) -> pd.DataFrame:
    """Trading sessions with `start <= session date <= end` (dates, inclusive), UTC timestamps.
    Columns: open, close, break_start, break_end (NaT when there is no lunch break)."""
    s = _all_sessions(exchange)
    lo, hi = pd.Timestamp(start).tz_localize(None).normalize(), pd.Timestamp(end).tz_localize(None).normalize()
    return s.loc[(s.index >= lo) & (s.index <= hi), ["open", "close", "break_start", "break_end"]]


@lru_cache(maxsize=None)
def _intervals(code: str) -> tuple[np.ndarray, np.ndarray]:
    """Sorted, non-overlapping open intervals [start, end) as int64 ns UTC (breaks split sessions)."""
    s = _all_sessions(code)
    has_break = s["break_start"].notna()
    starts = np.concatenate([s["open"].values, s.loc[has_break, "break_end"].values]).astype("datetime64[ns]")
    ends = np.concatenate([np.where(has_break, s["break_start"].values, s["close"].values).astype("datetime64[ns]"),
                           s.loc[has_break, "close"].values.astype("datetime64[ns]")])
    order = np.argsort(starts)
    return starts[order].astype("int64"), ends[order].astype("int64")


# --------------------------------------------------------------------------- queries

def open_mask(index: pd.DatetimeIndex, exchange: str) -> np.ndarray:
    """Vectorised: True where the exchange is open (regular session, outside breaks)."""
    starts, ends = _intervals(exchange)
    idx = pd.DatetimeIndex(index)
    t = (idx.tz_localize("UTC") if idx.tz is None else idx.tz_convert("UTC")).as_unit("ns").asi8
    i = np.searchsorted(starts, t, side="right") - 1
    ok = i >= 0
    out = np.zeros(len(t), dtype=bool)
    out[ok] = t[ok] < ends[i[ok]]
    return out


def is_open(exchange: str, ts) -> bool:
    return bool(open_mask(pd.DatetimeIndex([_to_utc(ts)]), exchange)[0])


def open_flags(index: pd.DatetimeIndex, exchanges: Iterable[str] | None = None) -> pd.DataFrame:
    """One bool column per exchange (default: all), plus n_open and any_major_open."""
    codes = list(exchanges) if exchanges is not None else list(EXCHANGES)
    df = pd.DataFrame({c: open_mask(index, c) for c in codes}, index=index)
    reg = [c for c in codes if c != "US_EXTENDED"]
    df["n_open"] = df[reg].sum(axis=1).astype("int16")
    df["any_major_open"] = df[[c for c in MAJOR if c in codes]].any(axis=1)
    return df


def next_event(exchange: str, ts) -> tuple[str, pd.Timestamp]:
    """('open' | 'close' | 'break_start' | 'break_end', when) - the next boundary after ts."""
    t = _to_utc(ts)
    s = _all_sessions(exchange)
    events = []
    for col in ("open", "break_start", "break_end", "close"):
        v = s[col].dropna()
        nxt = v[v > t]
        if len(nxt):
            events.append((col, nxt.iloc[0]))
    if not events:
        raise ValueError(f"no events after {t} for {exchange} (calendar ends {CAL_END})")
    return min(events, key=lambda e: e[1])


def status(ts=None, exchanges: Iterable[str] | None = None) -> pd.DataFrame:
    """Snapshot for every exchange: is_open, session open/close, next event and minutes to it."""
    t = _to_utc(ts) if ts is not None else pd.Timestamp.now(tz="UTC")
    rows = []
    for code in (exchanges or EXCHANGES):
        ex = EXCHANGES[code]
        s = _all_sessions(code)
        cur = s[(s["open"] <= t) & (s["close"] > t)]
        kind, when = next_event(code, t)
        rows.append({
            "exchange": code, "name": ex.name, "tz": ex.tz, "kind": ex.kind,
            "is_open": is_open(code, t),
            "session_open": cur["open"].iloc[0] if len(cur) else pd.NaT,
            "session_close": cur["close"].iloc[0] if len(cur) else pd.NaT,
            "next_event": kind, "next_event_at": when,
            "minutes_to_next": round((when - t).total_seconds() / 60, 1),
            "local_time": t.tz_convert(ex.tz).strftime("%a %H:%M"),
        })
    return pd.DataFrame(rows).set_index("exchange")


if __name__ == "__main__":
    import sys
    pd.set_option("display.width", 200)
    print(status(sys.argv[1] if len(sys.argv) > 1 else None).drop(columns=["name"]).to_string())
