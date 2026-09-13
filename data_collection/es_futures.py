"""
ES (E-mini S&P 500) futures - and any other CME Globex symbol, e.g. BTC / MBT - via Databento,
with a key-less Yahoo fallback for the last ~8 days of 1-minute bars.

Databento (dataset GLBX.MDP3) is the only self-serve source with per-second and finer ES history
(ohlcv-1s / trades / tbbo / mbp-1 / mbo, back to 2010 for ES). It is metered per GB of data
returned; every request here is costed first with `metadata.get_cost` (free) and the CLI refuses
to spend without --yes. New accounts get $125 of credit. Key in the environment only:

    export DATABENTO_API_KEY=db-...
    python -m data_collection.es_futures --dry-run                                   # cost of default pull
    python -m data_collection.es_futures --schema ohlcv-1s --start 2025-09-01 --end 2025-10-01 --yes
    python -m data_collection.es_futures --prices                                    # $/GB per schema
    python -m data_collection.es_futures --yahoo                                     # no key: ES=F 1m, 8 days

Symbols: `ES.c.0` = front month by calendar roll (continuous), `ES.n.0` = by open interest,
`ES.v.0` = by volume, or an outright like `ESZ5`. Output: data/parquet/es/<symbol>_<schema>_<start>_<end>.parquet
with Databento's native columns (ts_event ns UTC, prices as floats, sizes, side/flags for ticks).
For pulls over ~a few GB use `batch.submit_job` (Databento's async bulk path) instead of get_range.
"""
from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import datetime, timezone
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

DATASET = "GLBX.MDP3"
OUT_DIR = Path("data/parquet/es")
SCHEMAS = ["bbo-1s", "bbo-1m", "ohlcv-1s", "ohlcv-1m", "ohlcv-1h", "ohlcv-1d", "trades", "tbbo", "mbp-1", "mbp-10", "mbo", "definition", "statistics"]


def _client():
    key = os.environ.get("DATABENTO_API_KEY")
    if not key:
        sys.exit("set DATABENTO_API_KEY in the environment (https://databento.com/portal/keys); "
                 "use --yahoo for the key-less fallback")
    import databento as db
    return db.Historical(key)


def _stype(symbol: str) -> str:
    return "continuous" if symbol.count(".") == 2 else "raw_symbol"


def cost(symbol: str, schema: str, start: str, end: str) -> dict:
    """Dry run: USD cost and billable size for a get_range pull. Free call."""
    c = _client()
    kw = dict(dataset=DATASET, symbols=[symbol], schema=schema, start=start, end=end, stype_in=_stype(symbol))
    return {"usd": c.metadata.get_cost(**kw), "gb": c.metadata.get_billable_size(**kw) / 1e9,
            "records": c.metadata.get_record_count(**kw)}


def unit_prices() -> pd.DataFrame:
    """USD per GB by schema and pricing mode for GLBX.MDP3."""
    rows = []
    for mode in _client().metadata.list_unit_prices(DATASET):
        for schema, usd_per_gb in mode["unit_prices"].items():
            rows.append({"mode": mode["mode"], "schema": schema, "usd_per_gb": usd_per_gb})
    return pd.DataFrame(rows)


def fetch(symbol: str, schema: str, start: str, end: str, out: Path | None = None) -> Path:
    """Pull [start, end) and write Parquet. Caller is responsible for having checked cost()."""
    c = _client()
    store = c.timeseries.get_range(dataset=DATASET, symbols=[symbol], schema=schema, start=start, end=end,
                                   stype_in=_stype(symbol))
    df = store.to_df()                       # ts_event index (UTC ns), prices already scaled to floats
    out = out or OUT_DIR / f"{symbol.replace('.', '_')}_{schema}_{start}_{end}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, compression="zstd")
    return out


# --------------------------------------------------------------------------- spend ledger

LEDGER = OUT_DIR / "databento_ledger.json"
CREDIT_USD = 125.0                     # signup credit; adjust with `--credit` if yours differs


def ledger_load() -> dict:
    """{"credit_usd": float, "purchases": [{"when", "symbol", "schema", "start", "end", "usd", "gb", "file"}]}.
    Databento has no balance endpoint, so this is the only running record of what's been spent.
    Seed it by hand if anything was bought outside this module."""
    if LEDGER.exists():
        return json.loads(LEDGER.read_text())
    return {"credit_usd": CREDIT_USD, "purchases": []}


def ledger_spent(led: dict | None = None) -> float:
    led = led or ledger_load()
    return float(sum(x["usd"] for x in led["purchases"]))


def ledger_remaining(led: dict | None = None) -> float:
    led = led or ledger_load()
    return float(led["credit_usd"]) - ledger_spent(led)


def ledger_add(symbol: str, schema: str, start: str, end: str, usd: float, gb: float, file: Path) -> None:
    led = ledger_load()
    led["purchases"].append({"when": datetime.now(timezone.utc).isoformat(timespec="seconds"), "symbol": symbol,
                             "schema": schema, "start": start, "end": end, "usd": round(usd, 4),
                             "gb": round(gb, 4), "file": file.name})
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    LEDGER.write_text(json.dumps(led, indent=2))


# --------------------------------------------------------------------------- planned multi-chunk pulls

def plan_chunks(start: str, end: str, months: int = 12) -> list[tuple[str, str]]:
    """Split [start, end) into calendar chunks of `months` (yearly by default)."""
    from datetime import date
    s, e = date.fromisoformat(start), date.fromisoformat(end)
    out, cur = [], s
    while cur < e:
        y, m = cur.year + (cur.month - 1 + months) // 12, (cur.month - 1 + months) % 12 + 1
        nxt = min(date(y, m, 1), e)
        out.append((cur.isoformat(), nxt.isoformat()))
        cur = nxt
    return out


def chunk_path(symbol: str, schema: str, start: str, end: str) -> Path:
    return OUT_DIR / f"{symbol.replace('.', '_')}_{schema}_{start}_{end}.parquet"


def pull_plan(symbol: str, schema: str, start: str, end: str, months: int = 12,
              execute: bool = False, budget_usd: float | None = None) -> pd.DataFrame:
    """
    Price (and optionally execute) a multi-chunk pull. Each chunk becomes one Parquet in OUT_DIR;
    chunks whose file already exists are skipped, so re-running resumes. Nothing is bought unless
    execute=True, and then only if the total of the remaining chunks is within budget_usd.
    Returns a table of chunks with cost / size / rows / status.
    """
    rows = []
    for s, e in plan_chunks(start, end, months):
        path = chunk_path(symbol, schema, s, e)
        if path.exists():
            rows.append({"start": s, "end": e, "usd": 0.0, "gb": 0.0, "records": 0, "status": "have"})
            continue
        c = cost(symbol, schema, s, e)
        rows.append({"start": s, "end": e, **c, "status": "todo"})
    df = pd.DataFrame(rows)
    total = df.loc[df.status == "todo", "usd"].sum()
    if not execute:
        return df
    remaining = ledger_remaining()
    cap = min(budget_usd, remaining) if budget_usd is not None else remaining
    if total > cap:
        raise SystemExit(f"refusing: remaining chunks cost ${total:.2f} > allowed ${cap:.2f} "
                         f"(budget ${budget_usd}, ledger says ${remaining:.2f} of credit left)")
    log = logging.getLogger("es_futures")
    for i, r in df.iterrows():
        if r.status != "todo":
            continue
        if r.usd > ledger_remaining():             # re-check per chunk in case of concurrent spend
            raise SystemExit(f"stopping before {r.start}: ${r.usd:.2f} > ${ledger_remaining():.2f} left")
        t0 = time.time()
        path = fetch(symbol, schema, r.start, r.end, chunk_path(symbol, schema, r.start, r.end))
        ledger_add(symbol, schema, r.start, r.end, r.usd, r.gb, path)
        df.loc[i, "status"] = "done"
        log.info("%s -> %s  (%.0f s, %.0f MB)  spent so far $%.2f, $%.2f left",
                 r.start, path.name, time.time() - t0, path.stat().st_size / 1e6, ledger_spent(), ledger_remaining())
    return df


def load_bbo(start: str | None = None, end: str | None = None, symbol: str = "ES.c.0", schema: str = "bbo-1s") -> pd.DataFrame:
    """Concatenate the chunk Parquets for a symbol/schema, optionally sliced to [start, end).
    Adds `roll` (True on the first row of each new contract - `symbol` is the continuous alias, so
    contract changes are detected from `instrument_id`) and `mid`, `spread`."""
    prefix = f"{symbol.replace('.', '_')}_{schema}_"
    files = sorted(OUT_DIR.glob(prefix + "*.parquet"))
    t0 = pd.Timestamp(start, tz="UTC") if start else None
    t1 = pd.Timestamp(end, tz="UTC") if end else None
    keep = []
    for f in files:                       # chunk bounds are in the file name: <prefix><start>_<end>.parquet
        cs, ce = f.stem[len(prefix):].split("_")
        if (t1 is None or pd.Timestamp(cs, tz="UTC") < t1) and (t0 is None or pd.Timestamp(ce, tz="UTC") > t0):
            keep.append(f)
    if not keep:
        return pd.DataFrame()
    filters = []
    if t0 is not None:
        filters.append(("ts_recv", ">=", t0))
    if t1 is not None:
        filters.append(("ts_recv", "<", t1))
    df = pd.concat(pd.read_parquet(f, filters=filters or None) for f in keep).sort_index()
    df["roll"] = df["instrument_id"].ne(df["instrument_id"].shift()).fillna(False)
    df.loc[df.index[:1], "roll"] = False
    df["mid"] = (df["bid_px_00"] + df["ask_px_00"]) / 2
    df["spread"] = df["ask_px_00"] - df["bid_px_00"]
    return df


def fetch_yahoo(symbol: str = "ES=F", interval: str = "1m", days: int = 8, out: Path | None = None) -> Path:
    """Key-less fallback: Yahoo chart API. 1m bars only for the last ~8 days (Yahoo's cap), 5m for 60 d,
    1h for 730 d. Front-month contract as Yahoo defines it; unofficial endpoint."""
    r = requests.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
                     params={"interval": interval, "range": f"{days}d"},
                     headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    r.raise_for_status()
    res = r.json()["chart"]["result"][0]
    q = res["indicators"]["quote"][0]
    df = pd.DataFrame({"open": q["open"], "high": q["high"], "low": q["low"], "close": q["close"], "volume": q["volume"]},
                      index=pd.to_datetime(res["timestamp"], unit="s", utc=True).rename("ts")).dropna(subset=["close"])
    out = out or OUT_DIR / f"yahoo_{symbol.replace('=', '')}_{interval}_{datetime.now(timezone.utc):%Y%m%d}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, compression="zstd")
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="ES / CME futures via Databento (metered) or Yahoo (free, 8 days of 1m).")
    ap.add_argument("--symbol", default="ES.c.0")
    ap.add_argument("--schema", default="ohlcv-1s", choices=SCHEMAS)
    ap.add_argument("--start", default=(date.today() - timedelta(days=8)).isoformat())
    ap.add_argument("--end", default=date.today().isoformat(), help="exclusive")
    ap.add_argument("--dry-run", action="store_true", help="print cost only")
    ap.add_argument("--yes", action="store_true", help="actually spend credits")
    ap.add_argument("--prices", action="store_true", help="print $/GB per schema and exit")
    ap.add_argument("--yahoo", action="store_true", help="key-less Yahoo fallback (ES=F, 1m, 8 days)")
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--plan", action="store_true", help="multi-chunk pull: price every chunk (yearly), execute with --yes")
    ap.add_argument("--chunk-months", type=int, default=12)
    ap.add_argument("--budget", type=float, default=125.0, help="refuse to execute a plan costing more than this")
    ap.add_argument("--balance", action="store_true", help="show the spend ledger (Databento has no balance endpoint)")
    ap.add_argument("--credit", type=float, default=None, help="set the ledger's total credit (e.g. after topping up)")
    a = ap.parse_args()

    if a.yahoo:
        p = fetch_yahoo(out=a.out); df = pd.read_parquet(p)
        print(f"{len(df):,} bars  {df.index[0]} -> {df.index[-1]}  ->  {p}")
        sys.exit()
    if a.credit is not None:
        led = ledger_load(); led["credit_usd"] = a.credit
        LEDGER.parent.mkdir(parents=True, exist_ok=True); LEDGER.write_text(json.dumps(led, indent=2))
        print(f"ledger credit set to ${a.credit:.2f}")
    if a.balance or a.credit is not None:
        led = ledger_load()
        print(f"credit ${led['credit_usd']:.2f}  spent ${ledger_spent(led):.2f}  remaining ${ledger_remaining(led):.2f}  ({len(led['purchases'])} purchases)")
        for x in led["purchases"]:
            print(f"  {x['when']}  {x['symbol']} {x['schema']} {x['start']}->{x['end']}  ${x['usd']:.2f}  {x['file']}")
        sys.exit()
    if a.prices:
        print(unit_prices().pivot(index="schema", columns="mode", values="usd_per_gb").to_string())
        sys.exit()
    if a.plan:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
        table = pull_plan(a.symbol, a.schema, a.start, a.end, a.chunk_months, execute=a.yes, budget_usd=a.budget)
        print(table.to_string(index=False))
        todo = table[table.status == "todo"]
        print(f"TOTAL remaining: ${todo.usd.sum():.2f}  {todo.gb.sum():.2f} GB  {int(todo.records.sum()):,} rows"
              + ("" if a.yes else "   (dry run - add --yes to buy)"))
        sys.exit()
    q = cost(a.symbol, a.schema, a.start, a.end)
    print(f"{a.symbol} {a.schema} [{a.start}, {a.end}): {q['records']:,} records, {q['gb']:.3f} GB, ${q['usd']:.2f}")
    if a.dry_run or not a.yes:
        if not a.dry_run:
            print("not spending: re-run with --yes to fetch")
        sys.exit()
    if q["usd"] > ledger_remaining():
        sys.exit(f"refusing: ${q['usd']:.2f} > ${ledger_remaining():.2f} of credit left per the ledger")
    p = fetch(a.symbol, a.schema, a.start, a.end, a.out)
    ledger_add(a.symbol, a.schema, a.start, a.end, q["usd"], q["gb"], p)
    print(f"wrote {p}  ({p.stat().st_size / 1e6:.1f} MB)   spent so far ${ledger_spent():.2f}, ${ledger_remaining():.2f} left")
