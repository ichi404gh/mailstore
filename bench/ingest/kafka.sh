#!/usr/bin/env bash
# Single-node Kafka (KRaft) for the ingest stand, on localhost:9092.
#   bench/ingest/kafka.sh start [PARTITIONS]   # also creates the inbound topics
#   bench/ingest/kafka.sh topics               # recreate the topics (empty)
#   bench/ingest/kafka.sh stop
set -euo pipefail

IMAGE=apache/kafka@sha256:0bc1bb2478f45b6cea78864df86acdc11e8df2c5172477819a4d12942cbe5d40 # 4.1.1
NAME=mailstore-kafka
PARTITIONS=${2:-16}
TOPICS=(mailstore.inbound mailstore.inbound.priority mailstore.inbound.dead_letter)
kt() { docker exec "$NAME" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 "$@"; }

topics() {
  for t in "${TOPICS[@]}"; do kt --delete --if-exists --topic "$t" >/dev/null; done
  sleep 2
  for t in "${TOPICS[@]}"; do
    kt --create --topic "$t" --partitions "$PARTITIONS" --config max.message.bytes=16777216 >/dev/null
  done
  kt --list
}

case "${1:-}" in
  start)
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    docker run -d --name "$NAME" -p 9092:9092 \
      -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
      -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
      -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
      -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
      -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
      -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
      -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
      -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
      -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=false \
      "$IMAGE" >/dev/null
    until kt --list >/dev/null 2>&1; do sleep 1; done
    topics
    ;;
  topics) topics ;;
  stop) docker rm -f "$NAME" >/dev/null ;;
  *) echo "usage: $0 start [partitions] | topics | stop" >&2; exit 1 ;;
esac
