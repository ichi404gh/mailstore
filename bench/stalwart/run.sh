#!/bin/sh
# Start the benchmark Stalwart against the local Postgres (see bench/stalwart/README.md).
#   bench/stalwart/run.sh recovery   only the management HTTP on 127.0.0.1:8088
#   bench/stalwart/run.sh            normal mode with the listeners configured by setup.py
set -e
cd "$(dirname "$0")"
mkdir -p .run
export STALWART_RECOVERY_ADMIN=admin:benchpw
export STALWART_RECOVERY_MODE_BIND=127.0.0.1
export STALWART_RECOVERY_MODE_PORT=8088
[ "$1" = recovery ] && export STALWART_RECOVERY_MODE=1
exec .bin/v1.2.0/stalwart -c config.json
