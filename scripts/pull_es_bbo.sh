#!/usr/bin/env bash
# ES E-mini front-month, best bid/ask + sizes every second (Databento GLBX.MDP3 `bbo-1s`).
# Dry run by default: prints per-year cost and total. Pass --yes to spend the credit.
#   scripts/pull_es_bbo.sh          # price it
#   scripts/pull_es_bbo.sh --yes    # buy it (yearly chunks -> data/parquet/es/, resumable)
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -z "${DATABENTO_API_KEY:-}" ]; then eval "$(grep '^export DATABENTO_API_KEY' ~/.bashrc)"; fi
exec python3 -m data_collection.es_futures --plan --symbol ES.c.0 --schema bbo-1s \
     --start 2021-11-01 --end 2026-09-12 --chunk-months 12 --budget 125 "$@"
