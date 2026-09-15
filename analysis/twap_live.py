"""Live visualiser for the current Polymarket BTC Up/Down market: Chainlink price, the Up/Down
book, the settlement TWAP as it accrues, and the annualised implied vol from `analysis/twap_iv.py`.

    python -m analysis.twap_live            # then open http://localhost:8765
    python -m analysis.twap_live --market 15m --port 9000

Feeds (all free, no keys):
    RTDS    wss://ws-live-data.polymarket.com    crypto_prices_chainlink   1-s Chainlink BTC/USD (the series the TWAP averages)
                                                 crypto_prices_twap_sixty  Chainlink BTC/USD TWAP-60s (the settlement stream)
    Binance wss://stream.binance.com/ws/btcusdt@bookTicker   spot best bid/ask, every change (mid shown for reference)
    Gamma   https://gamma-api.polymarket.com     market for slug btc-updown-<5m|15m>-<window start unix>
    CLOB    wss://ws-subscriptions-clob.polymarket.com/ws/market   best bid/ask of the Up and Down tokens

Everything is event-driven: any book / price tick triggers a recompute (capped at ~25 Hz) that is
pushed to the page over server-sent events (/api/stream); the chart series ride along once a
second. /api/state returns the latest snapshot for anything that prefers to poll.

Model inputs:
    ref       TWAP-60s stream value stamped at the window start (the "price to beat"). Fallbacks, in
              order: the openPrice polymarket.com's own /api/crypto/crypto-price returns for the window
              (what the site displays), then the mean of the 1-s Chainlink prices over (start-60, start].
              The PM value is always fetched and shown next to ours as a cross-check.
    spot      Binance mid x exp(basis), basis = rolling mean of log(Chainlink[s] / Binance[s-2]) over the last
              BASIS_SECONDS (Chainlink lags Binance ~2 s and sits ~7 bp below it, sd ~0.6 bp). Binance ticks
              ~70x/s so the implied vol moves with the market instead of once a second; --spot chainlink
              uses the latest 1-s Chainlink print instead
    observed  1-s Chainlink prices stamped inside the settlement window (end-59 .. end] published so far
    p_up      mid of the Up token book (also evaluated at the bid and the ask for a range)

Nothing is written to disk.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import json
import logging
import math
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import numpy as np
import requests
import websockets

from analysis.twap_iv import TwapState, annualise, implied_vol, prob_up

RTDS_URL = "wss://ws-live-data.polymarket.com"
BINANCE_URL = "wss://stream.binance.com:443/ws/btcusdt@bookTicker"
CLOB_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA_URL = "https://gamma-api.polymarket.com/markets"
PM_PRICE_URL = "https://polymarket.com/api/crypto/crypto-price"   # what polymarket.com shows as "Price to Beat"
PM_VARIANT = {"5m": "fiveminute", "15m": "fifteenminute"}
TWAP_WINDOW = 60
WINDOW_SECONDS = {"5m": 300, "15m": 900}
KEEP_SECONDS = 3 * 3600          # per-second history kept in memory
RV_SECONDS = 3 * 3600            # realised-vol lookback (s); weights decay with half-life RV_HALF_LIFE
RV_HALF_LIFE = 600               # EWM half-life (s) of the squared-return weights
RV_RETURN = 30                   # return horizon (s) for realised vol: 1-s Chainlink returns are ~20% too quiet
                                 # (aggregation smoothing); by 30 s the vol signature is flat and the
                                 # sigma*c model reproduces the 5-min TWAP move sd (analysis/chainlink_vs_binance_vol.py)
BASIS_SECONDS = 600              # window for the Chainlink/Binance basis estimate
BASIS_LAG = 2                    # Chainlink[s] is compared with Binance[s - BASIS_LAG]
MIN_PUBLISH_GAP = 0.04           # cap on push rate to the page (s)
SERIES_EVERY = 1.0               # chart series are attached to a push at most this often (s)

log = logging.getLogger("twap_live")


class Live:
    def __init__(self, market: str, spot_source: str = "binance", trader=None):
        self.market_kind = market
        self.spot_source = spot_source
        self.trader = trader
        self.window_len = WINDOW_SECONDS[market]
        self.chainlink: dict[int, float] = {}     # second -> price
        self.twap60: dict[int, float] = {}
        self.binance: dict[int, float] = {}       # second -> last Binance mid in that second
        self.binance_live: tuple[float, float] | None = None   # (mid, recv time) of the latest tick
        self.last_msg: dict[str, float] = {}       # topic -> recv time
        self.markets: dict[int, dict] = {}         # window start -> gamma market (slug, tokens, ...)
        self.pm_open: dict[int, float] = {}        # window start -> polymarket.com openPrice ("Price to Beat")
        self.books: dict[str, dict] = {}           # token id -> {bid, ask, last}
        self.levels: dict[str, dict] = {}          # token id -> {"bids": {price: size}, "asks": {price: size}}
        self.hist_up: dict[int, float] = {}        # second -> Up mid
        self.hist_iv: dict[int, float] = {}        # second -> implied vol (annualised) at mid
        self.hist_wrv: dict[int, float] = {}       # second -> realised vol (annualised) inside the window so far
        self.last_result: dict | None = None
        self.results: list[dict] = []              # settled windows seen this session, oldest first
        self.pending_result: tuple | None = None   # (start, end, ref) awaiting the close TWAP
        self.clob_restart = asyncio.Event()
        self.dirty = asyncio.Event()               # set by any feed tick; wakes the publisher
        self.rtds_state = "connecting"
        self.clob_state = "connecting"
        self.binance_state = "connecting"
        self.cond = threading.Condition()          # guards snapshot/version; SSE threads wait on it
        self.version = 0
        self.snapshot = json.dumps({"status": "starting"})
        self.snapshot_full = self.snapshot                 # latest snapshot that carried the chart series

    # ------------------------------------------------------------------ feeds
    def _store(self, topic: str, symbol: str, ts_ms, value) -> None:
        try:
            s, v = int(ts_ms) // 1000, float(value)
        except (TypeError, ValueError):
            return
        if topic == "crypto_prices_chainlink" and symbol == "btc/usd":
            self.chainlink[s] = v
        elif topic == "crypto_prices_twap_sixty" and symbol == "btc/usd":
            self.twap60[s] = v
        else:
            return
        self.last_msg[topic] = time.time()
        self.dirty.set()

    async def rtds(self) -> None:
        subs = [{"topic": t, "type": "update", "filters": '{"symbol":"btc/usd"}'}
                for t in ("crypto_prices_chainlink", "crypto_prices_twap_sixty")]
        backoff = 1
        while True:
            try:
                async with websockets.connect(RTDS_URL, ping_interval=None, close_timeout=1, max_queue=4096) as ws:
                    await ws.send(json.dumps({"action": "subscribe", "subscriptions": subs}))
                    self.rtds_state = "connected"; backoff = 1
                    ping = asyncio.create_task(self._pinger(ws, 5))
                    try:
                        while True:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30)
                            if not raw or raw[0] != "{":
                                continue
                            msg = json.loads(raw)
                            topic, p = msg.get("topic", ""), msg.get("payload")
                            if not isinstance(p, dict):
                                continue
                            if isinstance(p.get("data"), list):          # history snapshot on subscribe
                                sym = p.get("symbol") or "btc/usd"
                                for row in p["data"]:
                                    self._store(topic, str(row.get("symbol", sym)).lower(), row.get("timestamp"), row.get("value"))
                            elif "value" in p:
                                self._store(topic, str(p.get("symbol", "")).lower(), p.get("timestamp") or msg.get("timestamp"), p["value"])
                    finally:
                        ping.cancel()
            except (websockets.WebSocketException, ConnectionError, OSError, asyncio.TimeoutError) as exc:
                self.rtds_state = f"reconnecting ({exc.__class__.__name__})"
                log.warning("RTDS: %s; reconnecting in %ds", exc, backoff)
                await asyncio.sleep(backoff); backoff = min(backoff * 2, 30)

    @staticmethod
    async def _pinger(ws, every: float) -> None:
        while True:
            await asyncio.sleep(every)
            await ws.send("PING")

    async def binance_ws(self) -> None:
        backoff = 1
        while True:
            try:
                async with websockets.connect(BINANCE_URL, ping_interval=None, open_timeout=10, close_timeout=1, max_queue=4096) as ws:
                    self.binance_state = "connected"; backoff = 1
                    while True:
                        raw = await asyncio.wait_for(ws.recv(), timeout=30)
                        m = json.loads(raw)
                        mid = (float(m["b"]) + float(m["a"])) / 2
                        now = time.time()
                        self.binance_live = (mid, now)
                        self.binance[int(now)] = mid
                        self.last_msg["binance"] = now
                        self.dirty.set()
            except (websockets.WebSocketException, ConnectionError, OSError, asyncio.TimeoutError, KeyError, ValueError) as exc:
                self.binance_state = f"reconnecting ({exc.__class__.__name__})"
                log.warning("Binance: %s; reconnecting in %ds", exc, backoff)
                await asyncio.sleep(backoff); backoff = min(backoff * 2, 30)

    def _set_book(self, token: str, bid=None, ask=None, last=None) -> None:
        b = self.books.setdefault(token, {"bid": None, "ask": None, "last": None})
        for k, v in (("bid", bid), ("ask", ask), ("last", last)):
            if v not in (None, ""):
                b[k] = float(v)

    def _on_clob(self, m: dict) -> None:
        et = m.get("event_type")
        if "bids" in m and "asks" in m:                                   # full book (initial snapshot)
            lv = self.levels[m["asset_id"]] = {"bids": {float(x["price"]): float(x["size"]) for x in m["bids"]},
                                               "asks": {float(x["price"]): float(x["size"]) for x in m["asks"]}}
            bids = [p for p, sz in lv["bids"].items() if sz > 0]; asks = [p for p, sz in lv["asks"].items() if sz > 0]
            self._set_book(m["asset_id"], max(bids) if bids else None, min(asks) if asks else None, m.get("last_trade_price"))
        elif "price_changes" in m:
            for c in m["price_changes"]:
                lv = self.levels.setdefault(c["asset_id"], {"bids": {}, "asks": {}})
                try:
                    lv["bids" if c.get("side") == "BUY" else "asks"][float(c["price"])] = float(c["size"])
                except (TypeError, ValueError, KeyError):
                    pass
                self._set_book(c["asset_id"], c.get("best_bid"), c.get("best_ask"))
        elif et == "last_trade_price":
            self._set_book(m["asset_id"], last=m.get("price"))
        elif et == "best_bid_ask" or ("best_bid" in m and "asset_id" in m):
            self._set_book(m["asset_id"], m.get("best_bid"), m.get("best_ask"))

    async def clob(self) -> None:
        backoff = 1
        while True:
            tokens = sorted({t for mk in self.markets.values() for t in mk["tokens"]})
            if not tokens:
                await asyncio.sleep(1); continue
            self.clob_restart.clear()
            try:
                async with websockets.connect(CLOB_URL, ping_interval=None, close_timeout=1, max_queue=8192) as ws:
                    await ws.send(json.dumps({"assets_ids": tokens, "type": "market"}))
                    self.clob_state = "connected"; backoff = 1
                    ping = asyncio.create_task(self._pinger(ws, 10))
                    restart = asyncio.create_task(self.clob_restart.wait())
                    try:
                        while not self.clob_restart.is_set():
                            recv = asyncio.create_task(ws.recv())
                            done, _ = await asyncio.wait({recv, restart}, timeout=30, return_when=asyncio.FIRST_COMPLETED)
                            if recv not in done:
                                recv.cancel()
                                if restart in done:
                                    break
                                raise ConnectionError("no message for 30s")
                            raw = recv.result()
                            if not raw or raw[0] not in "[{":
                                continue
                            msg = json.loads(raw)
                            for m in (msg if isinstance(msg, list) else [msg]):
                                if isinstance(m, dict):
                                    self._on_clob(m)
                            self.last_msg["clob"] = time.time()
                            self.dirty.set()
                    finally:
                        ping.cancel(); restart.cancel()
            except (websockets.WebSocketException, ConnectionError, OSError) as exc:
                self.clob_state = f"reconnecting ({exc.__class__.__name__})"
                log.warning("CLOB: %s; reconnecting in %ds", exc, backoff)
                await asyncio.sleep(backoff); backoff = min(backoff * 2, 30)

    def ask_size(self, token: str) -> float | None:
        """Shares showing at the best ask (None if unknown)."""
        b, lv = self.books.get(token), self.levels.get(token)
        if not b or b.get("ask") is None or not lv:
            return None
        return lv["asks"].get(b["ask"])

    # ------------------------------------------------------------------ markets
    def _fetch_market(self, start: int) -> dict | None:
        slug = f"btc-updown-{self.market_kind}-{start}"
        try:
            r = requests.get(GAMMA_URL, params={"slug": slug}, timeout=10); r.raise_for_status()
            rows = r.json()
        except (requests.RequestException, ValueError) as exc:
            log.warning("gamma %s: %s", slug, exc); return None
        if not rows:
            return None
        mk = rows[0]
        outcomes = json.loads(mk.get("outcomes", "[]")); tokens = json.loads(mk.get("clobTokenIds", "[]"))
        if len(tokens) != 2:
            return None
        by = dict(zip([o.lower() for o in outcomes], tokens))
        return {"slug": slug, "question": mk.get("question"), "start": start, "end": start + self.window_len,
                "tokens": tokens, "up": by.get("up", tokens[0]), "down": by.get("down", tokens[1])}

    def _fetch_pm_prices(self, start: int) -> tuple[float | None, float | None]:
        """(openPrice, closePrice) polymarket.com shows for the window; verified identical to the TWAP-60s stream
        values stamped at the window start / end. Either is None until published (a few s after the boundary)."""
        iso = lambda t: datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")  # noqa: E731
        try:
            r = requests.get(PM_PRICE_URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=10,
                             params={"symbol": "BTC", "eventStartTime": iso(start), "variant": PM_VARIANT[self.market_kind],
                                     "endDate": iso(start + self.window_len), "twapEnabled": "true", "twapLookbackSeconds": TWAP_WINDOW})
            r.raise_for_status()
            j = r.json()
            return (float(j["openPrice"]) if j.get("openPrice") is not None else None,
                    float(j["closePrice"]) if j.get("closePrice") is not None else None)
        except (requests.RequestException, ValueError, AttributeError, TypeError) as exc:
            log.warning("pm crypto-price %s: %s", start, exc); return None, None

    async def market_manager(self) -> None:
        pm_next_try = 0.0
        while True:
            now = time.time()
            cur = int(now // self.window_len) * self.window_len
            wanted = [cur, cur + self.window_len]
            changed = False
            if cur not in self.pm_open and now >= pm_next_try:          # PM publishes its openPrice a few s after the open
                v, _ = await asyncio.to_thread(self._fetch_pm_prices, cur)
                if v is not None:
                    self.pm_open[cur] = v; self.dirty.set()
                    log.info("PM price to beat for %d: %.4f", cur, v)
                pm_next_try = now + 3
            if self.trader and now >= pm_next_try:                       # traded markets that settled while we were down
                for ms in self.trader.unsettled_markets(cur)[:3]:
                    o, c = await asyncio.to_thread(self._fetch_pm_prices, ms)
                    if o is not None and c is not None:
                        self.trader.settle(ms, "Up" if c >= o else "Down", o, c, ms + self.window_len)
                        log.info("settled market %d from PM: open %.4f close %.4f", ms, o, c)
                    pm_next_try = now + 3
            pr = self.pending_result                                    # last window's close missing from the stream? PM has it
            if pr and pr[1] not in self.twap60 and now > pr[1] + 5 and now >= pm_next_try:
                _, close = await asyncio.to_thread(self._fetch_pm_prices, pr[0])
                if close is not None:
                    self.twap60[pr[1]] = close; self.dirty.set()
                    log.info("close of %d taken from PM: %.4f", pr[0], close)
                pm_next_try = now + 3
            for start in [s for s in self.pm_open if s < cur - 3 * self.window_len]:
                self.pm_open.pop(start)
            for start in wanted:
                if start not in self.markets:
                    mk = await asyncio.to_thread(self._fetch_market, start)
                    if mk:
                        self.markets[start] = mk; changed = True
                        log.info("market %s  %s", mk["slug"], mk["question"])
            for start in [s for s in self.markets if s < cur - self.window_len]:
                self.markets.pop(start); changed = True
            if changed:
                self.clob_restart.set()
            await asyncio.sleep(1 if any(s not in self.markets for s in wanted) else 5)

    # ------------------------------------------------------------------ model
    def _prune(self) -> None:
        cutoff = int(time.time()) - KEEP_SECONDS
        for d in (self.chainlink, self.twap60, self.binance, self.hist_up, self.hist_iv, self.hist_wrv):
            for s in [s for s in d if s < cutoff]:
                del d[s]

    def _ref(self, start: int) -> tuple[float | None, str]:
        v = self.twap60.get(start)
        if v is not None:
            return v, "TWAP-60s stream at window open"
        v = self.pm_open.get(start)
        if v is not None:
            return v, "polymarket.com price to beat (stream value at open not captured)"
        xs = [self.chainlink[s] for s in range(start - TWAP_WINDOW + 1, start + 1) if s in self.chainlink]
        if len(xs) >= 50:
            return float(np.mean(xs)), f"estimate: mean of {len(xs)} 1-s Chainlink prices before open"
        return None, "unknown - waiting for polymarket.com price to beat"

    def _basis(self, now: int) -> tuple[float | None, int]:
        """Mean log(Chainlink / lagged Binance) over the last BASIS_SECONDS, and the sample count."""
        ks = [s for s in range(now - BASIS_SECONDS, now + 1) if s in self.chainlink and s - BASIS_LAG in self.binance]
        if len(ks) < 30:
            return None, len(ks)
        return float(np.mean(np.log([self.chainlink[s] / self.binance[s - BASIS_LAG] for s in ks]))), len(ks)

    def _realised_vol(self, now: int) -> float | None:
        """Per-second vol: exponentially weighted mean (half-life RV_HALF_LIFE) of squared overlapping RV_RETURN-second
        Chainlink log returns over the last RV_SECONDS."""
        ks = np.array([s for s in range(now - RV_SECONDS, now + 1) if s in self.chainlink and s - RV_RETURN in self.chainlink])
        if len(ks) < 120:
            return None
        r = np.log([self.chainlink[s] / self.chainlink[s - RV_RETURN] for s in ks])
        w = np.exp(-(now - ks) * np.log(2) / RV_HALF_LIFE)
        return float(np.sqrt(np.sum(w * r * r) / np.sum(w) / RV_RETURN))

    def _window_realised(self, start: int, now_s: int) -> dict:
        """Vol realised since the window opened: overlapping RV_RETURN-s Chainlink returns ending in (start, now],
        annualised (noisy early: ~elapsed/RV_RETURN independent samples), plus the window's high/low."""
        ks = [s for s in range(start + 1, min(now_s, start + self.window_len) + 1) if s in self.chainlink and s - RV_RETURN in self.chainlink]
        px = [self.chainlink[s] for s in range(start, min(now_s, start + self.window_len) + 1) if s in self.chainlink]
        out = {"n": len(ks), "elapsed": max(0, min(now_s, start + self.window_len) - start), "vol_ann": None, "high": None, "low": None}
        if px:
            out["high"], out["low"] = max(px), min(px)
        if len(ks) >= 15:
            r = np.log([self.chainlink[s] / self.chainlink[s - RV_RETURN] for s in ks])
            out["vol_ann"] = annualise(float(np.sqrt(np.mean(r * r) / RV_RETURN)))
        return out

    @staticmethod
    def _f(x):
        return None if x is None or (isinstance(x, float) and not math.isfinite(x)) else x

    def compute(self, with_series: bool = True) -> dict:
        now = time.time(); now_s = int(now)
        start = int(now // self.window_len) * self.window_len; end = start + self.window_len
        seconds_left = end - now
        mk = self.markets.get(start)
        out = {"time": now, "market_kind": self.market_kind, "window_start": start, "window_end": end,
               "seconds_left": seconds_left, "twap_window": TWAP_WINDOW,
               "slug": mk["slug"] if mk else f"btc-updown-{self.market_kind}-{start}", "question": mk["question"] if mk else None,
               "feeds": {"rtds": self.rtds_state, "clob": self.clob_state, "binance": self.binance_state,
                         "ages": {k: round(now - v, 1) for k, v in self.last_msg.items()}}}

        # settlement of the previous window, once its close TWAP arrives
        if self.pending_result and self.pending_result[1] in self.twap60:
            ps, pe, pref = self.pending_result
            close = self.twap60[pe]
            self.last_result = {"start": ps, "end": pe, "ref": pref, "close": close,
                                "outcome": None if pref is None else ("Up" if close >= pref else "Down"),
                                "move_bp": None if pref is None else (close / pref - 1) * 1e4}
            self.results = (self.results + [self.last_result])[-24:]
            if self.trader:
                self.trader.settle(ps, self.last_result["outcome"], pref, close, pe)
            self.pending_result = None
        out["last_result"] = self.last_result
        out["results"] = self.results

        spot_s = max(self.chainlink) if self.chainlink else None
        spot = self.chainlink.get(spot_s) if spot_s else None
        ref, ref_src = self._ref(start)
        basis, n_basis = self._basis(now_s)
        model_spot, spot_src = spot, "chainlink 1-s print"
        if self.spot_source == "binance" and basis is not None and self.binance_live:
            model_spot, spot_src = self.binance_live[0] * math.exp(basis), f"Binance {basis * 1e4:+.1f} bp"
        out.update({"spot": spot, "spot_age": None if spot_s is None else round(now - spot_s, 1),
                    "binance": self.binance_live[0] if self.binance_live else None,
                    "basis_bp": None if basis is None else basis * 1e4, "basis_n": n_basis,
                    "model_spot": model_spot, "model_spot_source": spot_src,
                    "twap60_stream": self.twap60.get(max(self.twap60)) if self.twap60 else None,
                    "ref": ref, "ref_source": ref_src, "pm_open": self.pm_open.get(start)})

        # settlement samples published so far: seconds end-59 .. end that are <= now
        obs_secs = [s for s in range(end - TWAP_WINDOW + 1, min(now_s, end) + 1) if s in self.chainlink]
        observed = [self.chainlink[s] for s in obs_secs]
        twap_started = now >= end - TWAP_WINDOW + 1
        out["twap"] = {"started": twap_started, "starts_in": None if twap_started else end - TWAP_WINDOW + 1 - now,
                       "n": len(observed), "running": float(np.mean(observed)) if observed else None}

        up = self.books.get(mk["up"], {}) if mk else {}
        down = self.books.get(mk["down"], {}) if mk else {}
        mid = None
        if up.get("bid") is not None and up.get("ask") is not None:
            mid = (up["bid"] + up["ask"]) / 2
        out["up"] = {**up, "mid": mid}; out["down"] = down

        model = {"d_bp": None, "c": None, "expected_twap": None, "iv_ann": None, "iv_bid_ann": None, "iv_ask_ann": None,
                 "rv30_ann": None, "p_up_at_rv": None, "note": None}
        rv = self._realised_vol(now_s)
        model["rv30_ann"] = annualise(rv) if rv else None
        wr = self._window_realised(start, now_s)
        if wr["vol_ann"] is not None:
            self.hist_wrv[now_s] = wr["vol_ann"]
        out["window_realised"] = wr
        if ref is not None and model_spot is not None and seconds_left > 0:
            st = TwapState(ref=ref, spot=model_spot, seconds_left=seconds_left, twap_window=TWAP_WINDOW, observed=observed)
            c = math.sqrt(st.variance_factor())
            model.update({"d_bp": st.d() * 1e4, "c": c, "expected_twap": ref * math.exp(st.d())})
            if mid is not None:
                model["iv_ann"] = self._f(annualise(implied_vol(st, mid)))
                model["iv_bid_ann"] = self._f(annualise(implied_vol(st, up["bid"])))
                model["iv_ask_ann"] = self._f(annualise(implied_vol(st, up["ask"])))
                if model["iv_ann"] is None:
                    model["note"] = ("market and expected TWAP disagree on direction - no vol reproduces this price"
                                     if 0 < mid < 1 and c > 0 else "degenerate price")
                self.hist_up[now_s] = mid
                if model["iv_ann"] is not None:
                    self.hist_iv[now_s] = model["iv_ann"]
            if rv:
                model["p_up_at_rv"] = prob_up(st, rv)
        out["model"] = model

        if self.trader and mk:
            self.trader.on_tick(out, {"Up": self.ask_size(mk["up"]), "Down": self.ask_size(mk["down"])})
            out["sim"] = self.trader.summary(out)
        if not with_series:
            return out
        # per-second series for the charts: from 60 s before the open to the end of the window
        secs = list(range(start - TWAP_WINDOW, min(now_s, end) + 1))
        running = {}
        if observed:
            cum = np.cumsum(observed)
            running = {s: cum[i] / (i + 1) for i, s in enumerate(obs_secs)}
        out["series"] = {"t": secs,
                         "chainlink": [self.chainlink.get(s) for s in secs],
                         "binance": [None if self.binance.get(s) is None or basis is None else self.binance[s] * math.exp(basis) for s in secs],
                         "twap60": [self.twap60.get(s) for s in secs],
                         "running": [running.get(s) for s in secs],
                         "up_mid": [self.hist_up.get(s) for s in secs],
                         "iv_ann": [self.hist_iv.get(s) for s in secs],
                         "wrv_ann": [self.hist_wrv.get(s) for s in secs]}
        return out

    async def publisher(self) -> None:
        """Recompute on every feed tick (or at least twice a second) and hand the JSON to the SSE threads."""
        cur, last_series = None, 0.0
        while True:
            try:
                await asyncio.wait_for(self.dirty.wait(), timeout=0.5)
            except asyncio.TimeoutError:
                pass
            self.dirty.clear()
            try:
                now = time.time()
                start = int(now // self.window_len) * self.window_len
                rolled = cur is not None and start != cur
                if rolled:                                                # window rolled
                    self.pending_result = (cur, cur + self.window_len, self._ref(cur)[0])
                    self._prune()
                cur = start
                with_series = rolled or now - last_series >= SERIES_EVERY
                snap = json.dumps(self.compute(with_series), allow_nan=False, default=self._f)
                if with_series:
                    last_series = now
                with self.cond:
                    self.snapshot = snap
                    if with_series:
                        self.snapshot_full = snap
                    self.version += 1
                    self.cond.notify_all()
            except Exception:  # noqa: BLE001
                log.exception("compute failed")
            await asyncio.sleep(MIN_PUBLISH_GAP)

    async def run(self) -> None:
        await asyncio.gather(self.rtds(), self.clob(), self.binance_ws(), self.market_manager(), self.publisher())


# ---------------------------------------------------------------------- http
HTML = Path(__file__).with_name("twap_live.html")


def serve(live: Live, port: int) -> None:
    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path.startswith("/api/stream"):
                return self.stream()
            if self.path.startswith("/api/state"):
                with live.cond:
                    body = live.snapshot_full.encode()
                ctype = "application/json"
            elif self.path in ("/", "/index.html"):
                body, ctype = HTML.read_bytes(), "text/html; charset=utf-8"
            else:
                self.send_response(404); self.end_headers(); return
            self.send_response(200)
            self.send_header("Content-Type", ctype); self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store"); self.end_headers()
            self.wfile.write(body)

        def stream(self):
            """Server-sent events: one `data:` frame per published snapshot; the first carries the series."""
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream"); self.send_header("Cache-Control", "no-store")
            self.send_header("Connection", "keep-alive"); self.end_headers()
            with live.cond:
                seen, first = live.version, live.snapshot_full
            try:
                self.wfile.write(f"data: {first}\n\n".encode()); self.wfile.flush()
                while True:
                    with live.cond:
                        if not live.cond.wait_for(lambda: live.version != seen, timeout=15):
                            self.wfile.write(b": keepalive\n\n"); self.wfile.flush(); continue
                        seen, snap = live.version, live.snapshot
                    self.wfile.write(f"data: {snap}\n\n".encode()); self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass

        def log_message(self, *a):  # quiet
            pass

    srv = ThreadingHTTPServer(("127.0.0.1", port), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    log.info("open http://localhost:%d", port)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--market", choices=list(WINDOW_SECONDS), default="5m")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--spot", choices=["binance", "chainlink"], default="binance", help="model spot input (default: Binance mid x basis)")
    ap.add_argument("--no-sim", action="store_true", help="disable the paper trader")
    ap.add_argument("--sim-file", default="data/sim/paper_trades.json")
    ap.add_argument("--bankroll", type=float, default=1000.0, help="per-market position limit and Kelly bankroll ($)")
    ap.add_argument("--kelly", type=float, default=0.5, help="fraction of Kelly")
    ap.add_argument("--edge", type=float, default=0.05, help="minimum edge at a 0.50 price (grows towards the tails)")
    ap.add_argument("--max-price", type=float, default=0.85, help="never buy a side above this price")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    trader = None
    if not a.no_sim:
        from analysis.twap_sim import PaperTrader
        trader = PaperTrader(a.sim_file, bankroll=a.bankroll, kelly=a.kelly, edge_min=a.edge, max_price=a.max_price)
        log.info("paper trader: %d trades, %d settled markets loaded from %s", len(trader.trades), len(trader.settled), a.sim_file)
    live = Live(a.market, a.spot, trader)
    serve(live, a.port)
    try:
        asyncio.run(live.run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
