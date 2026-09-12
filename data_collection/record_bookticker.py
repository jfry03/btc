"""
Live L1 recorder: Binance `<symbol>@bookTicker` WebSocket -> daily gzip CSV.

Binance stopped publishing futures bookTicker archives on 2024-03-30, so from now on the
only free way to have this data is to record it yourself. Output files use the same
column layout as the archives (plus a `local_time` column) and the same file naming, so
`bookticker.sample_book` picks them up automatically for days with no archive:

    data/raw/BTCUSDT-bookTicker-2026-09-12.csv.gz

Run it and leave it running (it reconnects on drops, rotates files at UTC midnight):

    nohup python -m data_collection.record_bookticker --symbol BTCUSDT > /dev/null 2>&1 &

or as a systemd user unit (survives logout):

    [Unit]      Description=Binance bookTicker recorder
    [Service]   WorkingDirectory=/home/joshua/btc
                ExecStart=/usr/bin/python3 -m data_collection.record_bookticker --symbol BTCUSDT
                Restart=always
    [Install]   WantedBy=default.target

Disk: BTCUSDT perp is ~39M updates/day -> ~350 MB/day compressed. Every reconnect is a gap
in the data and is logged (with timestamps) to data/raw/<SYMBOL>-bookTicker.recorder.log.
Markets: um (USDⓈ-M futures, default), cm (COIN-M), spot. Spot bookTicker carries no
exchange timestamps, so for spot transaction_time/event_time are filled with local time.
"""
from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import logging
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

import websockets

WS_BASE = {
    "um":   "wss://fstream.binance.com/ws",
    "cm":   "wss://dstream.binance.com/ws",
    "spot": "wss://stream.binance.com:9443/ws",
}
HEADER = ("update_id,best_bid_price,best_bid_qty,best_ask_price,best_ask_qty,"
          "transaction_time,event_time,local_time\n")
FLUSH_EVERY_S = 5
STATS_EVERY_S = 60

log = logging.getLogger("record_bookticker")


class DailyGzipWriter:
    """Append-only gzip CSV, one file per UTC day keyed on the exchange transaction_time."""

    def __init__(self, raw_dir: Path, symbol: str):
        self.raw_dir, self.symbol = raw_dir, symbol
        self.day: str | None = None
        self.fh = None
        self.rows = 0

    def _open(self, day: str) -> None:
        self.close()
        path = self.raw_dir / f"{self.symbol}-bookTicker-{day}.csv.gz"
        fresh = not path.exists() or path.stat().st_size == 0
        # Appending creates a second gzip member; Python's gzip reads those transparently.
        self.fh = gzip.open(path, "at", compresslevel=6, newline="")
        if fresh:
            self.fh.write(HEADER)
        self.day = day
        log.info("writing %s (%s)", path.name, "new" if fresh else "append")

    def write(self, line: str, ts_ms: int) -> None:
        day = datetime.fromtimestamp(ts_ms / 1000, timezone.utc).strftime("%Y-%m-%d")
        if day != self.day:
            self._open(day)
        self.fh.write(line)
        self.rows += 1

    def flush(self) -> None:
        if self.fh:
            self.fh.flush()

    def close(self) -> None:
        if self.fh:
            self.fh.close()
            self.fh = None


def format_row(msg: dict, local_us: int, market: str) -> tuple[str, int]:
    """Native bookTicker JSON -> archive-layout CSV line. Returns (line, transaction_time_ms)."""
    if market == "spot":                       # spot payload has no E/T
        t = e = local_us // 1000
    else:
        t, e = msg["T"], msg["E"]
    line = f'{msg["u"]},{msg["b"]},{msg["B"]},{msg["a"]},{msg["A"]},{t},{e},{local_us}\n'
    return line, t


async def record(symbol: str, market: str, raw_dir: Path, stop: asyncio.Event) -> None:
    url = f"{WS_BASE[market]}/{symbol.lower()}@bookTicker"
    writer = DailyGzipWriter(raw_dir, symbol)
    backoff = 1
    try:
        while not stop.is_set():
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=1,
                                              max_queue=4096, compression=None) as ws:
                    log.info("connected %s", url)
                    backoff = 1
                    last_flush = last_stats = time.monotonic()
                    n0 = writer.rows
                    while not stop.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30)
                        except asyncio.TimeoutError:
                            raise ConnectionError("no message for 30s")
                        local_us = time.time_ns() // 1000
                        msg = json.loads(raw)
                        line, t = format_row(msg, local_us, market)
                        writer.write(line, t)

                        now = time.monotonic()
                        if now - last_flush >= FLUSH_EVERY_S:
                            writer.flush()
                            last_flush = now
                        if now - last_stats >= STATS_EVERY_S:
                            n, dt = writer.rows - n0, now - last_stats
                            lag = local_us // 1000 - msg.get("E", local_us // 1000)
                            log.info("%d rows  %.0f/s  last lag %d ms", n, n / dt, lag)
                            n0, last_stats = writer.rows, now
            except (websockets.WebSocketException, ConnectionError, OSError) as exc:
                if stop.is_set():
                    break
                log.warning("GAP: disconnected (%s); reconnecting in %ds", exc, backoff)
                writer.flush()
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
    finally:
        writer.close()
        log.info("stopped after %d rows", writer.rows)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--market", default="um", choices=list(WS_BASE))
    ap.add_argument("--raw-dir", default="data/raw")
    a = ap.parse_args()

    raw_dir = Path(a.raw_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.StreamHandler(),
                                  logging.FileHandler(raw_dir / f"{a.symbol}-bookTicker.recorder.log")])

    async def run() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        await record(a.symbol, a.market, raw_dir, stop)

    asyncio.run(run())


if __name__ == "__main__":
    main()
