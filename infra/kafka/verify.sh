#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
# Separate project, port, volume, topics, transaction ID and group for this run.
project=binance-broker-check
export KAFKA_PORT=${KAFKA_CHECK_PORT:-19092}
prefix="check-$(date +%s)-$RANDOM"
compose=(docker compose -p "$project" -f infra/kafka/compose.yaml)
trap 'echo "Check interrupted. Resources preserved: docker compose -p binance-broker-check -f infra/kafka/compose.yaml ps" >&2' ERR
"${compose[@]}" up -d --wait --wait-timeout 180
"${compose[@]}" exec -T kafka bash -s -- "$prefix" < infra/kafka/init-topics.sh
# Test tools are separate from the broker/runtime. No Go dependency is added.
docker build -f infra/kafka/verify.Dockerfile -t binance-kafka-check-tools infra/kafka
check() {
  docker run --rm --network "${project}_default" binance-kafka-check-tools "$1" kafka:9092 "$prefix"
}
check prepare
# Abrupt process death after confirmed Kafka transaction and consumer offset commits.
"${compose[@]}" kill -s SIGKILL kafka
"${compose[@]}" up -d --wait --wait-timeout 180
check restored
# Also verify persistence across container/network recreation with the same volume.
"${compose[@]}" down
"${compose[@]}" up -d --wait --wait-timeout 180
check topics
records=$(mktemp "${TMPDIR:-/tmp}/SET-005-records.XXXXXX")
"${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic "$prefix-subscription-changes" --partition 0 \
  --from-beginning --isolation-level read_committed --max-messages 3 --timeout-ms 15000 > "$records"
expected=$(printf 'committed\nreplacement-committed\nafter-restart')
actual=$(cat "$records")
rm "$records"
[[ "$actual" == "$expected" ]] || { echo "Unexpected restored records: $actual" >&2; exit 1; }
"${compose[@]}" stop
printf 'PASS SET-005: transactions, fencing, read_committed, SIGKILL and container recreation; volume retained\n'
