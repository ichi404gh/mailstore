#!/bin/sh
# Migration from Dovecot with imapsync, then a message-by-message comparison of the two
# servers (compat/README.md). Needs: mailstore on :1143, compat/setup_accounts.exs run,
# and the imaptest corpus (compat/imaptest.sh downloads it).
set -e
cd "$(dirname "$0")"

dovecot=dovecot/dovecot@sha256:a2e908c9a93698f3923104ea5be7f4beacde9586242aff5d8059599d0042c62a  # 2.4.5
imapsync=gilleslamiral/imapsync@sha256:ece980cb0fd2806369975d2989a3e8d9fbf28d632cdd45a05961a1e511612ed5  # 2.326
clients=mailstore-compat-clients
corpus=$PWD/.cache/imaptest-f0fbd425be10348a5fc95abdd440fc65d1aaedea/src/tests
mailstore=${IMAP_HOST:-host.docker.internal}
port=${IMAP_PORT:-1143}

[ -d "$corpus" ] || { echo "run compat/imaptest.sh first (downloads the corpus)"; exit 1; }
docker build -q -t "$clients" clients >/dev/null 2>&1
docker network create compat >/dev/null 2>&1 || true
docker rm -f compat-dovecot >/dev/null 2>&1 || true
docker run -d --name compat-dovecot --network compat -e USER_PASSWORD='{PLAIN}swordfish' "$dovecot" >/dev/null
trap 'docker rm -f compat-dovecot >/dev/null 2>&1' EXIT
sleep 4

run() { docker run --rm --network compat -v "$PWD/clients:/work:ro" -v "$corpus:/tests:ro" "$clients" "$@"; }

echo "== seed Dovecot: the imaptest corpus and 200 generated MIME structures"
run sh -c 'python3 gen_mime.py /tmp/generated.mbox 200 &&
  python3 imapcmp.py seed compat-dovecot 31993 source swordfish /tests/*.mbox /tmp/generated.mbox'
run python3 imapcmp.py wipe "$mailstore" "$port" migrated@compat.test swordfish

sync() {
  docker run --rm --network compat "$imapsync" imapsync \
    --host1 compat-dovecot --port1 31993 --ssl1 --sslargs1 SSL_verify_mode=0 \
    --user1 source --password1 swordfish \
    --host2 "$mailstore" --port2 "$port" --nossl2 --notls2 \
    --user2 migrated@compat.test --password2 swordfish \
    --useheader X-Compat-Seq --nolog |
    grep -E "Messages transferred|Messages skipped|Detected [0-9]+ errors|Folders synced"
}

echo "== imapsync Dovecot -> mailstore"
sync
echo "== imapsync again: nothing left to transfer"
sync

echo "== compare every message: flags, internal date, size, ENVELOPE, BODYSTRUCTURE"
run python3 imapcmp.py compare compat-dovecot 31993 tls "$mailstore" "$port" plain \
  source migrated@compat.test swordfish
