"""
Tiny status/control page for the recorder, meant to run on the VPS behind Caddy (which adds
HTTPS and basic auth). Standard library only.

    python -m data_collection.status_site --port 8080 --raw-dir data/raw

GET  /             HTML page (auto-refreshes)
GET  /api/status   JSON: recorder state, rows/s, last write age, files, disk, days-until-full
POST /api/<action> action in start | stop | restart | compact  (systemctl on the units below)
"""
from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

RECORDER_UNIT = "bookticker-recorder.service"
COMPACT_UNIT  = "bookticker-compact.service"
ACTIONS = {"start":   ["systemctl", "start", RECORDER_UNIT],
           "stop":    ["systemctl", "stop", RECORDER_UNIT],
           "restart": ["systemctl", "restart", RECORDER_UNIT],
           "compact": ["systemctl", "start", "--no-block", COMPACT_UNIT]}
STATS_RE = re.compile(r"(\d+) rows\s+(\d+)/s\s+last lag (-?\d+) ms")

RAW_DIR = Path("data/raw")
SYMBOL = "BTCUSDT"


def sh(*cmd: str) -> str:
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=10).stdout.strip()
    except Exception as exc:  # noqa: BLE001
        return f"error: {exc}"


def status() -> dict:
    now = time.time()
    log_path = RAW_DIR / f"{SYMBOL}-bookTicker.recorder.log"
    lines = log_path.read_text().splitlines()[-400:] if log_path.exists() else []
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    last_stats = next((STATS_RE.search(l) for l in reversed(lines) if STATS_RE.search(l)), None)
    gaps_today = [l for l in lines if "GAP" in l and l.startswith(today)]

    files = []
    for p in sorted(RAW_DIR.glob(f"{SYMBOL}-bookTicker-*")):
        if p.suffix in (".gz", ".parquet"):
            files.append({"name": p.name, "mb": round(p.stat().st_size / 1e6, 1),
                          "modified": datetime.fromtimestamp(p.stat().st_mtime, timezone.utc).isoformat(timespec="seconds")})
    today_file = RAW_DIR / f"{SYMBOL}-bookTicker-{today}.csv.gz"
    write_age = round(now - today_file.stat().st_mtime, 1) if today_file.exists() else None

    du = shutil.disk_usage(RAW_DIR)
    # bytes/day: last compacted day if any, else today's file scaled to a full day
    parq = [p for p in RAW_DIR.glob(f"{SYMBOL}-bookTicker-*.parquet")]
    if parq:
        per_day = max(p.stat().st_size for p in sorted(parq)[-3:])
    elif today_file.exists():
        per_day = None
        try:  # size so far / fraction of a day the file actually spans (first row's exchange time)
            import gzip
            with gzip.open(today_file, "rt") as fh:
                fh.readline(); first_ms = int(fh.readline().split(",")[5])
            span = now - first_ms / 1000
            if span > 600:
                per_day = today_file.stat().st_size * 86400 / span
        except Exception:  # noqa: BLE001
            pass
    else:
        per_day = None

    return {
        "time_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "recorder": sh("systemctl", "is-active", RECORDER_UNIT),
        "compact_timer": sh("systemctl", "is-active", "bookticker-compact.timer"),
        "compact_last": sh("systemctl", "show", COMPACT_UNIT, "-p", "ExecMainStartTimestamp", "--value"),
        "rows_per_s": int(last_stats[2]) if last_stats else None,
        "last_lag_ms": int(last_stats[3]) if last_stats else None,
        "last_write_age_s": write_age,
        "gaps_today": len(gaps_today),
        "recent_gaps": gaps_today[-5:],
        "files": files,
        "disk_free_gb": round(du.free / 1e9, 2),
        "disk_used_pct": round(100 * du.used / du.total, 1),
        "est_mb_per_day": round(per_day / 1e6) if per_day else None,
        "days_until_full": round(du.free / per_day, 1) if per_day else None,
        "log_tail": lines[-12:],
    }


PAGE = """<!doctype html><meta charset=utf-8><title>bookTicker recorder</title>
<meta name=viewport content="width=device-width,initial-scale=1">
<style>
body{font:14px/1.4 system-ui,sans-serif;margin:0;padding:16px 20px;max-width:900px;background:#fafafa;color:#222}
h1{font-size:18px;margin:0 0 12px}.ok{color:#177a2f}.bad{color:#b3261e}.warn{color:#9a6700}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin:12px 0}
.card{background:#fff;border:1px solid #e3e3e3;border-radius:6px;padding:10px}.card b{display:block;font-size:20px}
.card span{color:#666;font-size:12px}button{margin-right:6px;padding:6px 12px}table{border-collapse:collapse;width:100%%;font-size:13px}
td,th{padding:4px 8px;border-bottom:1px solid #eee;text-align:left}pre{background:#fff;border:1px solid #e3e3e3;padding:8px;font-size:12px;overflow-x:auto}
</style>
<h1>bookTicker recorder <small id=t></small></h1>
<div><button onclick="act('start')">start</button><button onclick="act('stop')">stop</button>
<button onclick="act('restart')">restart</button><button onclick="act('compact')">compact now</button> <span id=msg></span></div>
<div class=grid id=cards></div>
<table id=files></table>
<pre id=log></pre>
<script>
const $=id=>document.getElementById(id);
function card(l,v,c){return `<div class=card><b class="${c||''}">${v??'–'}</b><span>${l}</span></div>`}
async function load(){const s=await (await fetch('api/status')).json();$('t').textContent=s.time_utc;
const rec=s.recorder==='active';const age=s.last_write_age_s;
$('cards').innerHTML=card('recorder',s.recorder,rec?'ok':'bad')+card('rows / s',s.rows_per_s)+
card('last write',age==null?'–':age+' s ago',age==null||age>30?'bad':'ok')+card('gaps today',s.gaps_today,s.gaps_today?'warn':'ok')+
card('lag ms',s.last_lag_ms)+card('disk free',s.disk_free_gb+' GB',s.disk_free_gb<2?'bad':'ok')+
card('MB / day',s.est_mb_per_day)+card('days until full',s.days_until_full,s.days_until_full<7?'warn':'')+
card('compact timer',s.compact_timer,s.compact_timer==='active'?'ok':'bad');
$('files').innerHTML='<tr><th>file</th><th>MB</th><th>modified (UTC)</th></tr>'+s.files.map(f=>`<tr><td>${f.name}</td><td>${f.mb}</td><td>${f.modified}</td></tr>`).join('');
$('log').textContent=s.log_tail.join('\\n')}
async function act(a){$('msg').textContent='…';const r=await fetch('api/'+a,{method:'POST'});$('msg').textContent=await r.text();setTimeout(load,1500)}
load();setInterval(load,10000);
</script>"""


class Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, body: str, ctype: str = "text/plain") -> None:
        data = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", f"{ctype}; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        if self.path in ("/", "/index.html"):
            self._send(200, PAGE, "text/html")
        elif self.path == "/api/status":
            self._send(200, json.dumps(status()), "application/json")
        else:
            self._send(404, "not found")

    def do_POST(self) -> None:
        action = self.path.removeprefix("/api/")
        if action not in ACTIONS:
            return self._send(404, "unknown action")
        r = subprocess.run(ACTIONS[action], capture_output=True, text=True, timeout=30)
        self._send(200 if r.returncode == 0 else 500, (r.stdout + r.stderr).strip() or f"{action}: ok")

    def log_message(self, *_: object) -> None:   # keep journald quiet
        pass


def main() -> None:
    global RAW_DIR, SYMBOL
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--raw-dir", default="data/raw")
    ap.add_argument("--symbol", default="BTCUSDT")
    a = ap.parse_args()
    RAW_DIR, SYMBOL = Path(a.raw_dir), a.symbol
    ThreadingHTTPServer((a.host, a.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
