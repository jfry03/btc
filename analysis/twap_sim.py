"""Paper trader for the live TWAP visualiser: bets on the model-check probability with Kelly sizing.

Rule, evaluated on every tick of the current market (see analysis/twap_live.py for the inputs):
    p       model P(Up) at the trailing 30-min realised vol (30-s Chainlink returns) - the "model check"
    for each side (Up with p, Down with 1 - p):
        q     = best ask + taker fee per share            fee = FEE_RATE * ask * (1 - ask)  (Polymarket crypto_fees_v2)
        edge  = p - q                                     skip unless edge >= edge_min * phi(0) / phi(Phi^-1(q)):
                                                          5 c at q = 0.50, 5.7 c at 0.70, 7.1 c at 0.80, 8.6 c at 0.85 -
                                                          the Gaussian model has no skew and thin tails so it is most
                                                          wrong near the tails, and buying a favourite = selling a tail
        skip if ask > max_price (0.85)
        f     = (p - q) / (1 - q)                         Kelly fraction for a binary bought at q with win prob p
        target cost on this side = kelly * f * bankroll   bankroll = per-market limit (default $1,000)
        buy the shortfall vs what we already hold on that side, capped by the market limit and by the size
        showing at the best ask (no walking the book); minimum 5 shares (Polymarket's order minimum)
    no trading in the last `min_seconds_left` seconds (the settlement sd is then inside the basis noise)
    positions are held to settlement: PnL = shares * 1{side won} - cost - fees

State (every trade and every settled market) is persisted as JSON so the history survives restarts;
markets that settled while the tool was down are settled from polymarket.com's open/close prices.
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path

from scipy.stats import norm

FEE_RATE = 0.07            # crypto_fees_v2: fee per share = rate * p^k * (1-p)^k, taker only
FEE_EXP = 1
MIN_SHARES = 5


def taker_fee(price: float, shares: float) -> float:
    return FEE_RATE * (price * (1 - price)) ** FEE_EXP * shares


def edge_required(q: float, edge_min: float) -> float:
    """Minimum edge (probability units) to buy at effective price q: edge_min at q = 0.5, growing towards the tails."""
    return edge_min * norm.pdf(0) / norm.pdf(norm.ppf(q))


class PaperTrader:
    def __init__(self, path: str | Path = "data/sim/paper_trades.json", bankroll: float = 1000.0, kelly: float = 0.5,
                 edge_min: float = 0.05, max_price: float = 0.85, min_seconds_left: float = 15.0, min_trade_gap: float = 1.0):
        self.path = Path(path)
        self.bankroll, self.kelly, self.edge_min, self.max_price = bankroll, kelly, edge_min, max_price
        self.min_seconds_left, self.min_trade_gap = min_seconds_left, min_trade_gap
        self.trades: list[dict] = []          # every fill, oldest first
        self.settled: dict[int, dict] = {}    # window start -> {outcome, ref, close, pnl, cost, fees, ...}
        self._last_fill = 0.0
        self._consumed: dict[tuple, float] = {}   # (market, side, price) -> shares we already took at that level
        self._load()

    # ------------------------------------------------------------------ persistence
    def _load(self) -> None:
        if self.path.exists():
            d = json.loads(self.path.read_text())
            self.trades = d.get("trades", [])
            self.settled = {int(k): v for k, v in d.get("settled", {}).items()}

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"params": {"bankroll": self.bankroll, "kelly": self.kelly, "edge_min": self.edge_min,
                                              "max_price": self.max_price, "min_seconds_left": self.min_seconds_left},
                                   "trades": self.trades, "settled": self.settled}, indent=0))
        os.replace(tmp, self.path)

    # ------------------------------------------------------------------ trading
    def market_trades(self, start: int) -> list[dict]:
        return [t for t in self.trades if t["market"] == start]

    def on_tick(self, state: dict, ask_sizes: dict[str, float | None]) -> None:
        """state: the compute() snapshot of analysis/twap_live.py; ask_sizes: {"Up": shares at best ask, "Down": ...}."""
        start = state["window_start"]
        p_up = (state.get("model") or {}).get("p_up_at_rv")
        if p_up is None or state.get("ref") is None or state["seconds_left"] < self.min_seconds_left or start in self.settled:
            return
        now = time.time()
        if now - self._last_fill < self.min_trade_gap:
            return
        held = self.market_trades(start)
        spent = sum(t["cost"] + t["fee"] for t in held)
        for side, p, book in (("Up", p_up, state.get("up") or {}), ("Down", 1 - p_up, state.get("down") or {})):
            ask = book.get("ask")
            if ask is None or not 0 < ask < 1 or ask > self.max_price:
                continue
            fee_ps = taker_fee(ask, 1.0)
            q = ask + fee_ps
            edge = p - q
            if q >= 1 or edge < edge_required(q, self.edge_min):
                continue
            f = (p - q) / (1 - q)
            target = self.kelly * f * self.bankroll
            have = sum(t["cost"] + t["fee"] for t in held if t["side"] == side)
            room = self.bankroll - spent
            size = ask_sizes.get(side)
            if size is not None:                       # what we already lifted at this level is assumed gone
                size = max(0.0, size - self._consumed.get((start, side, ask), 0.0))
            shown = size * (ask + fee_ps) if size is not None else float("inf")
            buy = min(target - have, room, shown)
            shares = buy / (ask + fee_ps)
            if shares < MIN_SHARES:
                continue
            trade = {"ts": now, "market": start, "side": side, "price": ask, "shares": round(shares, 2),
                     "cost": round(shares * ask, 4), "fee": round(taker_fee(ask, shares), 4),
                     "p_model": round(p, 4), "edge": round(edge, 4), "edge_req": round(edge_required(q, self.edge_min), 4),
                     "seconds_left": round(state["seconds_left"], 1),
                     "d_bp": (state.get("model") or {}).get("d_bp"), "rv_ann": (state.get("model") or {}).get("rv30_ann")}
            self.trades.append(trade)
            self._consumed[(start, side, ask)] = self._consumed.get((start, side, ask), 0.0) + trade["shares"]
            self._last_fill = now
            spent += trade["cost"] + trade["fee"]
            self._save()

    def settle(self, start: int, outcome: str | None, ref: float | None, close: float | None, end: int) -> None:
        if start in self.settled or outcome is None:
            return
        held = self.market_trades(start)
        if not held:
            return
        payout = sum(t["shares"] for t in held if t["side"] == outcome)
        cost = sum(t["cost"] for t in held); fees = sum(t["fee"] for t in held)
        self.settled[start] = {"end": end, "outcome": outcome, "ref": ref, "close": close, "payout": round(payout, 4),
                               "cost": round(cost, 4), "fees": round(fees, 4), "pnl": round(payout - cost - fees, 4),
                               "n_trades": len(held), "sides": sorted({t["side"] for t in held})}
        self._save()

    def unsettled_markets(self, before: int) -> list[int]:
        """Markets we traded that have no result yet (e.g. settled while the tool was down)."""
        return sorted({t["market"] for t in self.trades if t["market"] < before and t["market"] not in self.settled})

    # ------------------------------------------------------------------ reporting
    def summary(self, state: dict) -> dict:
        start = state["window_start"]
        held = self.market_trades(start)
        pos = {}
        for side in ("Up", "Down"):
            ts = [t for t in held if t["side"] == side]
            if ts:
                sh = sum(t["shares"] for t in ts); cost = sum(t["cost"] for t in ts); fee = sum(t["fee"] for t in ts)
                bid = (state.get(side.lower()) or {}).get("bid")
                pos[side] = {"shares": sh, "cost": cost, "fee": fee, "avg": cost / sh,
                             "mark": None if bid is None else sh * bid - cost - fee}
        settled = sorted(self.settled.items())
        realised = sum(v["pnl"] for _, v in settled)
        fees = sum(v["fees"] for _, v in settled) + sum(t["fee"] for t in held)
        wins = sum(v["pnl"] > 0 for _, v in settled)
        curve_t, curve_p, cum = [], [], 0.0
        for k, v in settled:
            cum += v["pnl"]; curve_t.append(v["end"]); curve_p.append(round(cum, 2))
        open_mark = sum(p["mark"] for p in pos.values() if p["mark"] is not None)
        return {"params": {"bankroll": self.bankroll, "kelly": self.kelly, "edge_min": self.edge_min, "max_price": self.max_price,
                           "min_seconds_left": self.min_seconds_left},
                "realised": round(realised, 2), "open_mark": round(open_mark, 2), "fees": round(fees, 2),
                "n_markets": len(settled), "n_wins": wins, "n_trades": len(self.trades),
                "invested": round(sum(v["cost"] + v["fees"] for _, v in settled), 2),
                "position": pos, "current_trades": held[-10:],
                "recent": [{"start": k, **v} for k, v in settled[-15:]][::-1],
                "curve": {"t": curve_t, "pnl": curve_p}}
