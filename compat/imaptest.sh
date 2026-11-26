#!/bin/sh
# Dovecot imaptest against a running mailstore (compat/README.md).
#   compat/imaptest.sh            scripted compliance tests, then a 60 s stress run
#   compat/imaptest.sh scripted   only the scripted tests
#   compat/imaptest.sh stress     only the stress run
# Needs: mailstore on :1143 (mix run --no-halt), accounts from compat/setup_accounts.exs.
set -e
cd "$(dirname "$0")"

image=dovecot/imaptest@sha256:34e3a745a033e16faccf03e68309b97311cd2a42d0c377b5d983bd2b88dbf918
commit=f0fbd425be10348a5fc95abdd440fc65d1aaedea   # dovecot/imaptest main, 2026-11-19
host=${IMAP_HOST:-host.docker.internal}
port=${IMAP_PORT:-1143}
src=.cache/imaptest-$commit

if [ ! -d "$src/src/tests" ]; then
  mkdir -p "$src"
  gh api "repos/dovecot/imaptest/tarball/$commit" | tar xz -C "$src" --strip-components=1
fi

auth="host=$host port=$port user=imaptest@compat.test user2=imaptest2@compat.test pass=swordfish"
what=${1:-all}

if [ "$what" = all ] || [ "$what" = scripted ]; then
  echo "== scripted tests (tests that need capabilities mailstore does not announce are skipped)"
  docker run --rm -v "$PWD/$src/src/tests:/tests:ro" "$image" $auth test=/tests
fi

if [ "$what" = all ] || [ "$what" = stress ]; then
  # logout=0: persistent connections. With a LOGOUT per cycle the clients open ~200
  # connections/s, and Docker Desktop's port forwarding stops accepting new ones after
  # a few seconds (the server is fine; it is the VM's NAT).
  echo "== stress: 10 clients, 60 s, state tracking, stop at the first error"
  docker run --rm -v "$PWD/$src/src/tests:/tests:ro" "$image" $auth \
    mbox=/tests/default.mbox clients=10 secs=60 logout=0 error_quit | tail -6
fi
