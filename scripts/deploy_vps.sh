#!/usr/bin/env bash
# Push data_collection/ + deploy/vps/ to the VPS and (re)install the services there.
set -euo pipefail
HOST="${VPS_HOST:-scraping-vps}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ssh "$HOST" mkdir -p /opt/btc
rsync -az --delete --exclude '__pycache__' --exclude '*.ipynb' \
    "$ROOT/data_collection" "$ROOT/deploy" "$HOST:/opt/btc/"
ssh "$HOST" 'bash /opt/btc/deploy/vps/install.sh'
