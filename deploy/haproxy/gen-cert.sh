#!/bin/sh
# Self-signed certificate for local testing (HAProxy wants cert and key in one PEM).
# ECDSA P-256: the handshake signature costs a fraction of RSA-2048's, which matters
# when many clients reconnect every few minutes.
set -e
cd "$(dirname "$0")/certs"
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 365 \
  -subj "/CN=localhost" -keyout imap.key -out imap.crt 2>/dev/null
cat imap.crt imap.key > imap.pem
rm imap.key imap.crt
echo "wrote $(pwd)/imap.pem"
