"""
Live recorder for Polymarket's Real-Time Data Service (RTDS) crypto price topics -> daily gzip CSV.

This is the only free way to get the price series Polymarket actually settles its 5m / 15m / 4h
"Bitcoin Up or Down" markets on: the Chainlink BTC/USD 60-second TWAP data stream. RTDS has no
replay - it sends ~60 s of history on subscribe and then 1 message per second per topic - so
every second not recorded is gone.

Topics (all 1 msg/s, filtered to BTC):
    crypto_prices_twap_sixty   Chainlink BTC/USD TWAP-60s  (settlement feed since 2026-08-07)
    crypto_prices_twap_thirty  Chainlink BTC/USD TWAP-30s
    crypto_prices_chainlink    Chainlink BTC/USD CexPrice stream (spot, ~2 s behind Binance)
    crypto_prices              Binance BTCUSDT spot (Polymarket's own feed of it)

Output: data/raw/rtds/rtds-btc-<UTC day>.csv.gz with columns
    topic, ts_recv_ms, ts_msg_ms, ts_payload_ms, symbol, value, full_accuracy_value
(ts_msg = server timestamp on the envelope, ts_payload = timestamp inside the payload, both ms).
Rotates on ts_payload UTC day; reconnects with backoff; every reconnect is logged as GAP.

    nohup python -m data_collection.record_rtds > /dev/null 2>&1 &
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

URL = "wss://ws-live-data.polymarket.com"
TOPICS = {
    "crypto_prices_twap_sixty":  '{"symbol":"btc/usd"}',
    "crypto_prices_twap_thirty": '{"symbol":"btc/usd"}',
    "crypto_prices_chainlink":   '{"symbol":"btc/usd"}',
    "crypto_prices":             None,   # server-side symbol filter drops everything; filter client-side
}
KEEP_SYMBOLS = {"btc/usd", "btcusdt"}
HEADER = "topic,ts_recv_ms,ts_msg_ms,ts_payload_ms,symbol,value,full_accuracy_value\n"
PING_EVERY_S = 5
FLUSH_EVERY_S = 5
STATS_EVERY_S = 60

log = logging.getLogger("record_rtds")


class DailyGzipWriter:
    def __init__(self, out_dir: Path, prefix: str):
        self.out_dir, self.prefix, self.day, self.fh, self.rows = out_dir, prefix, None, None, 0

    def write(self, line: str, ts_ms: int) -> None:
        day = datetime.fromtimestamp(ts_ms / 1000, timezone.utc).strftime("%Y-%m-%d")
        if day != self.day:
            self.close()
            path = self.out_dir / f"{self.prefix}-{day}.csv.gz"
            fresh = not path.exists() or path.stat().st_size == 0
            self.fh = gzip.open(path, "at", compresslevel=6, newline="")
            if fresh:
                self.fh.write(HEADER)
            self.day = day
            log.info("writing %s (%s)", path.name, "new" if fresh else "append")
        self.fh.write(line)
        self.rows += 1

    def flush(self) -> None:
        if self.fh:
            self.fh.flush()

    def close(self) -> None:
        if self.fh:
            self.fh.close()
            self.fh = None


def subscribe_msg() -> str:
    return json.dumps({"action": "subscribe",
                       "subscriptions": [{"topic": t, "type": "update", **({"filters": f} if f else {})}
                                         for t, f in TOPICS.items()]})


async def pinger(ws, stop: asyncio.Event) -> None:
    while not stop.is_set():
        await asyncio.sleep(PING_EVERY_S)
        await ws.send("PING")


async def record(out_dir: Path, stop: asyncio.Event) -> None:
    writer = DailyGzipWriter(out_dir, "rtds-btc")
    backoff = 1
    try:
        while not stop.is_set():
            try:
                async with websockets.connect(URL, ping_interval=None, close_timeout=1, max_queue=4096) as ws:
                    await ws.send(subscribe_msg())
                    log.info("connected, subscribed to %d topics", len(TOPICS))
                    backoff = 1
                    ping_task = asyncio.create_task(pinger(ws, stop))
                    last_flush = last_stats = time.monotonic()
                    n0, seen = writer.rows, set()
                    try:
                        while not stop.is_set():
                            try:
                                raw = await asyncio.wait_for(ws.recv(), timeout=30)
                            except asyncio.TimeoutError:
                                raise ConnectionError("no message for 30s")
                            recv_ms = time.time_ns() // 1_000_000
                            if not raw or raw == "PONG" or raw[0] != "{":
                                continue
                            msg = json.loads(raw)
                            p = msg.get("payload")
                            if not isinstance(p, dict) or "value" not in p or p.get("symbol") not in KEEP_SYMBOLS:
                                continue
                            topic = msg.get("topic", "")
                            ts_payload = int(p.get("timestamp") or msg.get("timestamp") or recv_ms)
                            # the ~60 s snapshot on subscribe repeats rows already written after a reconnect
                            key = (topic, ts_payload)
                            if key in seen:
                                continue
                            seen.add(key)
                            if len(seen) > 20000:
                                seen = set(list(seen)[-5000:])
                            line = (f'{topic},{recv_ms},{msg.get("timestamp", "")},{ts_payload},'
                                    f'{p.get("symbol", "")},{p["value"]},{p.get("full_accuracy_value", "")}\n')
                            writer.write(line, ts_payload)
                            now = time.monotonic()
                            if now - last_flush >= FLUSH_EVERY_S:
                                writer.flush(); last_flush = now
                            if now - last_stats >= STATS_EVERY_S:
                                n = writer.rows - n0
                                log.info("%d rows  %.1f/s  last lag %d ms", n, n / (now - last_stats), recv_ms - ts_payload)
                                n0, last_stats = writer.rows, now
                    finally:
                        ping_task.cancel()
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
    ap.add_argument("--out-dir", default="data/raw/rtds")
    a = ap.parse_args()
    out = Path(a.out_dir); out.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.StreamHandler(), logging.FileHandler(out / "rtds.recorder.log")])

    async def run() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        await record(out, stop)
    asyncio.run(run())


if __name__ == "__main__":
    main()
