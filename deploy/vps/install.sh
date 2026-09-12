#!/usr/bin/env bash
# Run ON the VPS as root, from /opt/btc (deploy_vps.sh does this for you). Idempotent.
set -euo pipefail
cd /opt/btc
HOST_NAME="${STATUS_HOST:-$(curl -s4 ifconfig.me | tr . -).sslip.io}"

echo "== python venv"
if ! python3 -c 'import venv' 2>/dev/null || ! [ -x venv/bin/python ]; then
    apt-get install -y -q python3-venv >/dev/null
    python3 -m venv venv
fi
venv/bin/pip install -q --upgrade pip
venv/bin/pip install -q -r deploy/vps/requirements-vps.txt
mkdir -p data/raw

echo "== systemd units"
cp deploy/vps/bookticker-*.service deploy/vps/bookticker-*.timer /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now bookticker-recorder.service bookticker-status.service bookticker-compact.timer
systemctl restart bookticker-recorder.service bookticker-status.service

echo "== caddy"
if ! command -v caddy >/dev/null; then apt-get install -y -q caddy >/dev/null; fi
PW_FILE=/root/btc-status.password
if [ ! -s "$PW_FILE" ]; then
    python3 -c 'import secrets; print(secrets.token_urlsafe(18), end="")' >"$PW_FILE"; chmod 600 "$PW_FILE"
fi
HASH=$(caddy hash-password --plaintext "$(cat "$PW_FILE")")
sed -e "s|__HOST__|$HOST_NAME|" -e "s|__HASH__|$HASH|" deploy/vps/Caddyfile.template >/etc/caddy/Caddyfile
caddy validate --config /etc/caddy/Caddyfile >/dev/null
systemctl enable --now caddy.service
systemctl reload caddy.service || systemctl restart caddy.service

echo "== status"
systemctl --no-pager --no-legend list-units 'bookticker-*' caddy.service
echo "site: https://$HOST_NAME   user: admin   password: cat $PW_FILE"
