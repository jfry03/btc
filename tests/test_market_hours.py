import numpy as np
import pandas as pd
import pytest

from data_collection import market_hours as mh


def T(s):
    return pd.Timestamp(s, tz="UTC")


def test_nyse_regular_day():
    assert mh.is_open("XNYS", "2026-03-10 13:30Z")          # 09:30 ET open (EDT)
    assert mh.is_open("XNYS", "2026-03-10 19:59Z")
    assert not mh.is_open("XNYS", "2026-03-10 20:00Z")      # 16:00 ET close is exclusive
    assert not mh.is_open("XNYS", "2026-03-10 13:29Z")
    s = mh.sessions("XNYS", "2026-03-10", "2026-03-10")
    assert s.iloc[0]["open"] == T("2026-03-10 13:30") and s.iloc[0]["close"] == T("2026-03-10 20:00")


def test_us_holiday_and_weekend():
    assert not mh.is_open("XNYS", "2026-07-03 15:00Z")      # Independence Day observed (Friday)
    assert mh.sessions("XNYS", "2026-07-03", "2026-07-05").empty
    assert not mh.is_open("XNYS", "2026-03-14 15:00Z")      # Saturday


def test_early_close_day_after_thanksgiving():
    s = mh.sessions("XNYS", "2026-11-27", "2026-11-27").iloc[0]
    assert s["close"] == T("2026-11-27 18:00")              # 13:00 ET
    assert mh.is_open("XNYS", "2026-11-27 17:59Z") and not mh.is_open("XNYS", "2026-11-27 18:01Z")
    # US_EXTENDED after-hours starts at the early close
    assert mh.is_open("US_EXTENDED", "2026-11-27 18:30Z")
    assert not mh.is_open("US_EXTENDED", "2026-11-27 17:00Z")


def test_tokyo_lunch_break():
    s = mh.sessions("XTKS", "2026-03-09", "2026-03-09").iloc[0]
    assert s["break_start"] == T("2026-03-09 02:30") and s["break_end"] == T("2026-03-09 03:30")
    assert mh.is_open("XTKS", "2026-03-09 02:00Z")
    assert not mh.is_open("XTKS", "2026-03-09 03:00Z")
    assert mh.is_open("XTKS", "2026-03-09 03:30Z")
    assert mh.next_event("XTKS", "2026-03-09 02:00Z") == ("break_start", T("2026-03-09 02:30"))


def test_cme_week_and_daily_break():
    assert not mh.is_open("CMES", "2026-03-14 12:00Z")      # Saturday
    assert not mh.is_open("CMES", "2026-03-15 21:59Z")      # Sunday before 17:00 CT (CDT = UTC-5)
    assert mh.is_open("CMES", "2026-03-15 22:00Z")          # Sunday 17:00 CT open
    assert mh.is_open("CMES", "2026-03-16 20:59Z")          # Mon 15:59 CT
    assert not mh.is_open("CMES", "2026-03-16 21:00Z")      # 16:00-17:00 CT break
    assert not mh.is_open("CMES", "2026-03-16 21:59Z")
    assert mh.is_open("CMES", "2026-03-16 22:00Z")
    assert not mh.is_open("CMES", "2026-03-20 21:30Z")      # Friday after 16:00 CT close
    assert mh.next_event("CMES", "2026-03-20 21:30Z") == ("open", T("2026-03-22 22:00"))


def test_dst_transitions():
    # US DST starts 2026-03-08: NYSE open moves from 14:30Z to 13:30Z
    assert mh.sessions("XNYS", "2026-03-06", "2026-03-06").iloc[0]["open"] == T("2026-03-06 14:30")
    assert mh.sessions("XNYS", "2026-03-09", "2026-03-09").iloc[0]["open"] == T("2026-03-09 13:30")
    # UK DST starts 2026-03-29: during the mismatch week London opens 08:00Z, after 07:00Z
    assert mh.sessions("XLON", "2026-03-27", "2026-03-27").iloc[0]["open"] == T("2026-03-27 08:00")
    assert mh.sessions("XLON", "2026-03-30", "2026-03-30").iloc[0]["open"] == T("2026-03-30 07:00")
    # CME: Sunday open is 22:00Z in summer, 23:00Z in winter
    assert mh.sessions("CMES", "2026-01-05", "2026-01-05").iloc[0]["open"] == T("2026-01-04 23:00")


def test_open_mask_matches_is_open_loop():
    idx = pd.date_range("2026-03-09", "2026-03-10", freq="97s", tz="UTC")
    for code in ("XNYS", "XTKS", "CMES", "US_EXTENDED", "XNSE"):
        mask = mh.open_mask(idx, code)
        naive = np.array([mh.is_open(code, t) for t in idx])
        assert (mask == naive).all(), code


def test_open_flags_shape_and_counts():
    idx = pd.date_range("2026-03-09", periods=86400, freq="1s", tz="UTC")
    f = mh.open_flags(idx)
    assert set(mh.EXCHANGES) <= set(f.columns) and {"n_open", "any_major_open"} <= set(f.columns)
    assert f["XNYS"].sum() == 6.5 * 3600
    assert f["CMES"].sum() == 23 * 3600                      # 24h minus the 1h break
    assert f["XTKS"].sum() == 5.5 * 3600                     # 6.5h minus 1h lunch
    assert f["US_EXTENDED"].sum() == 9.5 * 3600              # 5.5h pre + 4h after


def test_hardcoded_calendars():
    assert mh.is_open("XNSE", "2026-03-09 04:00Z") and not mh.is_open("XNSE", "2026-03-09 03:44Z")   # 09:15 IST
    assert mh.is_open("EUREX_DERIV", "2026-03-09 00:30Z") and not mh.is_open("XEUR", "2026-03-09 00:30Z")
    assert mh.is_open("XSHE", "2026-03-09 02:00Z") == mh.is_open("XSHG", "2026-03-09 02:00Z")


def test_status_frame():
    st = mh.status("2026-03-09 14:00Z")
    assert st.loc["XNYS", "is_open"] and not st.loc["XTKS", "is_open"]
    assert st.loc["XNYS", "next_event"] == "close" and st.loc["XNYS", "minutes_to_next"] == 360.0
