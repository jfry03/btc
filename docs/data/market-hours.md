# Market hours

**Source:** `data_collection/market_hours.py`
**Backed by:** [`exchange_calendars`](https://github.com/gerrymanoim/exchange_calendars) (sessions, holidays, early closes) plus a few hard-coded calendars listed below.

Opening and closing times of the major stock and futures exchanges, as UTC timestamps, for two
uses: point lookups ("is the CME open right now, when does it next close?") and **vectorised
flags on a 1-second UTC grid** to use as features alongside the L1 data.

```python
import pandas as pd
from data_collection.market_hours import is_open, sessions, status, next_event, open_flags, open_mask

is_open("XNYS", "2026-03-10 14:00Z")                    # True
next_event("CMES", "2026-03-20 21:30Z")                 # ('open', 2026-03-22 22:00 UTC) - Sunday reopen
sessions("XTKS", "2026-03-09", "2026-03-13")            # open / close / break_start / break_end per day
status("2026-03-09 14:00Z")                             # one row per exchange: is_open, next event, minutes to it
```

Building a feature grid for a day (all exchanges, ~0.05 s for 86,400 rows):

```python
grid = pd.date_range("2026-03-10", periods=86400, freq="1s", tz="UTC")
flags = open_flags(grid)            # bool column per exchange + n_open + any_major_open
flags["CMES"].sum() / 3600          # 23.0 - a full Globex day minus the maintenance hour
book = sample_book("BTCUSDT", start, end, step_ms=1000)   # same index -> just concat / join
```

`open_mask(index, "XNYS")` is the single-exchange version; both are `searchsorted` over the
precomputed session intervals (breaks split a session into two intervals), so cost is O(n log m).

## Exchanges

Local hours shown for the reference day 2026-03-10 (a normal Tuesday, US already on DST, Europe not).

| Code | Exchange | Time zone | Local hours (2026-03-10) | Kind | Notes |
|---|---|---|---|---|---|
| `XNYS` | New York Stock Exchange | `America/New_York` | 09:30–16:00 | equity |  |
| `XNAS` | NASDAQ | `America/New_York` | 09:30–16:00 | equity |  |
| `XTSE` | Toronto Stock Exchange | `America/Toronto` | 09:30–16:00 | equity |  |
| `XLON` | London Stock Exchange | `Europe/London` | 08:00–16:30 | equity |  |
| `XETR` | Xetra (Frankfurt) | `Europe/Berlin` | 09:00–17:30 | equity |  |
| `XPAR` | Euronext Paris | `Europe/Paris` | 09:00–17:30 | equity |  |
| `XAMS` | Euronext Amsterdam | `Europe/Amsterdam` | 09:00–17:30 | equity |  |
| `XSWX` | SIX Swiss Exchange | `Europe/Zurich` | 09:00–17:30 | equity |  |
| `XTKS` | Tokyo Stock Exchange | `Asia/Tokyo` | 09:00–15:30 (break 11:30–12:30) | equity | lunch break 11:30-12:30 JST |
| `XHKG` | Hong Kong Exchange | `Asia/Hong_Kong` | 09:30–16:00 (break 12:00–13:00) | equity | lunch break 12:00-13:00 HKT |
| `XSHG` | Shanghai Stock Exchange | `Asia/Shanghai` | 09:30–15:00 (break 11:30–13:00) | equity | lunch break 11:30-13:00 CST |
| `XSHE` | Shenzhen Stock Exchange | `Asia/Shanghai` | 09:30–15:00 (break 11:30–13:00) | equity | **hard-coded** — HARD-CODED: aliased to XSHG (same hours and holidays) |
| `XKRX` | Korea Exchange | `Asia/Seoul` | 09:00–15:30 | equity |  |
| `XSES` | Singapore Exchange | `Asia/Singapore` | 09:00–17:00 | equity |  |
| `XASX` | Australian Securities Exchange | `Australia/Sydney` | 10:00–16:00 | equity |  |
| `XBOM` | BSE (Bombay) | `Asia/Kolkata` | 09:15–15:30 | equity |  |
| `XNSE` | NSE India | `Asia/Kolkata` | 09:15–15:30 | equity | **hard-coded** — HARD-CODED: 09:15-15:30 IST on BSE trading days (NSE and BSE share holidays) |
| `CMES` | CME Globex equity index futures (ES, NQ, YM, RTY) | `America/Chicago` | Mon 17:00–Tue 16:00 | futures | **hard-coded** — Sun 17:00 -> Fri 16:00 CT. HARD-CODED: the library has sessions 17:00->17:00 CT with no break; regular 17:00 CT closes are moved to 16:00 CT so the 16:00-17:00 maintenance break is a gap. Early closes (e.g. 12:15 CT holidays) are kept as the library has them |
| `XCBF` | Cboe Futures (VIX) | `America/Chicago` | 08:30–15:15 | futures | regular session only (08:30-15:15 CT) |
| `IEPA` | ICE US futures | `America/New_York` | Mon 20:00–Tue 18:00 | futures |  |
| `XEUR` | Eurex (main session) | `Europe/Berlin` | 08:00–22:00 | futures | 08:00-22:00 CET per exchange_calendars |
| `EUREX_DERIV` | Eurex index derivatives (FDAX, FESX) | `Europe/Berlin` | 01:10–22:00 | futures | **hard-coded** — HARD-CODED: 01:10-22:00 Europe/Berlin on XEUR trading days |
| `US_EXTENDED` | US equities pre/after-hours | `America/New_York` | 04:00–20:00 (break 09:30–16:00) | equity | **hard-coded** — HARD-CODED: 04:00-09:30 and 16:00-20:00 ET on NYSE sessions; after-hours starts at the (early) close |

`MAJOR` = XNYS, XNAS, XLON, XETR, XTKS, XHKG, XSHG, CMES drives `any_major_open`; `n_open` counts every
regular-session exchange that is open (`US_EXTENDED` excluded).

## Caveats

- **Holidays and early closes** are whatever `exchange_calendars` has (version pinned by
  `requirements.txt`). Calendars are built from 2015 to 2030, clipped to the last year the library
  has holidays for (Shanghai and Hong Kong are only published a year or two ahead) — `next_event`
  raises past that bound.
- **Half days** come from the library (e.g. NYSE 13:00 ET the day after Thanksgiving; CME 12:00 CT
  on Thanksgiving). `US_EXTENDED` after-hours starts at the actual (early) close.
- **CME daily break**: the library models Globex as 17:00 → 17:00 CT with no break, so regular
  17:00 CT closes are moved to 16:00 CT here; the 16:00–17:00 CT maintenance window is then the gap
  between sessions. Early closes are left untouched. Other CME products (rates, FX, metals, energy)
  share these Globex hours closely but not exactly — treat `CMES` as "equity index futures".
- **Hard-coded calendars** (`XSHE`, `XNSE`, `EUREX_DERIV`, `US_EXTENDED`) use another calendar's
  trading days with fixed local hours; they will be wrong on the rare days those venues diverge
  (e.g. an NSE-only holiday, a Eurex-only half day).
- **ICE Europe** (Brent, gilts) is not in the library and is not included; `IEPA` is ICE US.
- Close timestamps are exclusive: `is_open` at exactly 16:00:00 ET is `False`.

Tests: `python -m pytest tests/test_market_hours.py` (regular day, holiday, early close, lunch
break, CME week/break, DST transitions, vectorised mask vs per-timestamp loop).
