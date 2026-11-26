#!/usr/bin/env bash
# Crash test: kill -9 the consumer mid-stream, start it again with the same consumer
# group, check that every message is stored exactly once and no UID was wasted.
#   bench/ingest/restart.sh [COUNT] [SEED]      (Kafka from kafka.sh, accounts from seed.exs)
set -euo pipefail
cd "$(dirname "$0")/../.."
COUNT=${1:-50000}
SEED=${2:-$RANDOM}
GROUP=bench-restart-$SEED
PG=${PG_CONTAINER:-mailstore-pg}
DB=${DB:-mailstore_dev_shard1}  # the shard the bench tenant lives on
sql() { docker exec "$PG" psql -U postgres -d "$DB" -Atc "$1"; }
run_env=(MAILSTORE_KAFKA_HOSTS=localhost:9092 MAILSTORE_KAFKA_GROUP=$GROUP
  MAILSTORE_KAFKA_OFFSET_RESET=earliest MAILSTORE_IMAP_PORT=0)

bench/ingest/kafka.sh topics >/dev/null
pkill -9 -f "beam.*mix run --no-halt" 2>/dev/null || true
out=$(mix run --no-start bench/ingest/produce.exs -- --count "$COUNT" --seed "$SEED" | tail -1)
echo "$out"
known=$(echo "$out" | sed -E 's/.* ([0-9]+) to existing accounts.*/\1/')
stored() { sql "SELECT count(*) FROM messages WHERE message_id LIKE '<s$SEED-%'"; }
if [ "$(stored)" -ne 0 ]; then echo "seed $SEED was used before: pick another" >&2; exit 1; fi

echo "== first run, killed at ~40%"
env "${run_env[@]}" mix run --no-halt >/tmp/ingest-restart-1.log 2>&1 &
until [ "$(stored)" -ge $((known * 4 / 10)) ]; do sleep 0.2; done
pkill -9 -f "beam.*mix run --no-halt"
sleep 1
before=$(stored)
echo "stored before the crash: $before of $known"
if [ "$before" -ge "$known" ]; then echo "killed too late, nothing to replay" >&2; exit 1; fi

echo "== second run, same consumer group"
env "${run_env[@]}" mix run bench/ingest/measure.exs -- --idle 5 | sed -n '/^records:/,/^CPU per/p'

echo "== check"
sql "SELECT count(*) AS stored, count(DISTINCT (folder_id, message_id)) AS distinct_messages,
       $known AS expected FROM messages WHERE message_id LIKE '<s$SEED-%'"
echo "folders with a UID gap in this run (0 = none wasted):"
sql "SELECT count(*) FROM (SELECT folder_id FROM messages WHERE message_id LIKE '<s$SEED-%'
       GROUP BY folder_id HAVING max(uid) - min(uid) + 1 <> count(*)) g"
