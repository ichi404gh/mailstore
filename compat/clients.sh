#!/bin/sh
# Real clients against a running mailstore (compat/README.md): mbsync and OfflineIMAP
# two-way sync, and what a polling service does with imapflow.
# Needs: mailstore on :1143 and compat/setup_accounts.exs run.
set -e
cd "$(dirname "$0")"
image=mailstore-compat-clients
host=${IMAP_HOST:-host.docker.internal}
port=${IMAP_PORT:-1143}

docker build -q -t "$image" clients >/dev/null 2>&1
run() { docker run --rm -v "$PWD/clients:/work:ro" "$image" "$@"; }

run python3 sync_clients.py "$host" "$port" client@compat.test swordfish
echo "== imapflow"
run node imapflow_test.js "$host" "$port" client@compat.test swordfish
