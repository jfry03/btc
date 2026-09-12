#!/usr/bin/env bash
# Pull closed recorder days from the VPS into data/raw/, verify, then delete them on the VPS.
#
# Never loses data:
#   * only files for days before today (UTC) are touched - the recorder is still writing today's
#   * rsync with --partial keeps interrupted transfers resumable; nothing is deleted on error
#   * a file is deleted on the VPS only after its SHA-256 matches the local copy AND the local
#     copy passes an integrity read (gzip -t / parquet footer)
#   * a lock prevents two syncs running at once; safe to run from cron every hour
#
#   scripts/sync_vps.sh            # normal run
#   scripts/sync_vps.sh --dry-run  # show what would be transferred/deleted
set -euo pipefail
HOST="${VPS_HOST:-scraping-vps}"
REMOTE=/opt/btc/data/raw
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOCAL="$ROOT/data/raw"
LOG="$LOCAL/sync.log"
DRY=${1:-}
mkdir -p "$LOCAL/vps-logs"
exec 9>"$LOCAL/.sync.lock"; flock -n 9 || { echo "sync already running"; exit 0; }
log() { printf '%s %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" | tee -a "$LOG"; }

today=$(date -u +%F)
mapfile -t files < <(ssh "$HOST" "cd $REMOTE 2>/dev/null && ls -1 *-bookTicker-*.csv.gz *-bookTicker-*.parquet 2>/dev/null" \
                     | grep -vE -- "-${today}\.(csv\.gz|parquet)$" || true)
if [ ${#files[@]} -eq 0 ]; then log "nothing to sync"; exit 0; fi
log "closed files on VPS: ${files[*]}"
[ "$DRY" = "--dry-run" ] && { echo "(dry run - stopping)"; exit 0; }

# 1. transfer (resumable; a re-run picks up where it left off)
printf '%s\n' "${files[@]}" | rsync -a --partial --partial-dir=.rsync-partial --files-from=- \
    "$HOST:$REMOTE/" "$LOCAL/"

# 2. verify: remote sha256 == local sha256, and the local file reads cleanly
declare -a verified=() failed=()
while read -r sum name; do
    [ -z "$name" ] && continue
    local_sum=$(sha256sum "$LOCAL/$name" 2>/dev/null | cut -d' ' -f1 || true)
    if [ "$sum" != "$local_sum" ]; then failed+=("$name (checksum)"); continue; fi
    case "$name" in
        *.csv.gz)  gzip -t "$LOCAL/$name" 2>/dev/null || { failed+=("$name (gzip -t)"); continue; } ;;
        *.parquet) python3 -c "import sys,pyarrow.parquet as pq; pq.ParquetFile(sys.argv[1]).metadata" "$LOCAL/$name" 2>/dev/null \
                   || { failed+=("$name (parquet)"); continue; } ;;
    esac
    verified+=("$name")
done < <(ssh "$HOST" "cd $REMOTE && sha256sum ${files[*]}")

# 3. delete on the VPS only what was verified
if [ ${#verified[@]} -gt 0 ]; then
    ssh "$HOST" "cd $REMOTE && rm -f ${verified[*]}"
    log "synced+removed from VPS: ${verified[*]}"
fi
# 4. keep a copy of the recorder log (append-only on the VPS; never deleted)
rsync -a "$HOST:$REMOTE/*.recorder.log" "$LOCAL/vps-logs/" 2>/dev/null || true

if [ ${#failed[@]} -gt 0 ]; then log "FAILED verification (kept on VPS): ${failed[*]}"; exit 1; fi
