#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
# Unique resources per run; never operate on binance-broker or SET-005's volume.
run_id="$(date +%s)-$$-$RANDOM"
project="binance-go-kafka-$run_id"
export KAFKA_PORT=${KAFKA_GO_CHECK_PORT:-$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')}
prefix="go-check-$run_id"
compose=(docker compose -p "$project" -f infra/kafka/compose.yaml)
cleanup() {
  result=$?
  trap - EXIT
  if ! "${compose[@]}" down --volumes --remove-orphans --timeout 10; then
    echo "Cleanup failed for $project" >&2
    result=1
  fi
  exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
"${compose[@]}" up -d --wait --wait-timeout 180
"${compose[@]}" exec -T kafka timeout 90 bash -s -- "$prefix" < infra/kafka/init-topics.sh
"${compose[@]}" exec -T kafka timeout 90 bash -s -- "${prefix}-feed" < infra/kafka/init-topics.sh
export KAFKA_PRICEFEED_TEST_PREFIX="${prefix}-feed"
export KAFKA_INTEGRATION=1
export KAFKA_TEST_BROKER="127.0.0.1:$KAFKA_PORT"
export KAFKA_TEST_PREFIX="$prefix"
# The full PostgreSQL run already includes Kafka packages. Run each once:
# their committed fixture identities must not be replayed by a second test run.
if [[ "${KAFKA_RUN_POSTGRES:-0}" == "1" ]]; then
  python3 scripts/check_postgres.py
else
  GOTOOLCHAIN=local go test -race -count=1 -v -timeout 120s ./internal/platform/kafka ./internal/testutil/kafkafault ./internal/marketdata/publish ./internal/matcher/pricefeed
fi
# Explicit opt-in against a closed port must fail, rather than skip the tests.
closed_port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
if KAFKA_TEST_BROKER="127.0.0.1:$closed_port" GOTOOLCHAIN=local go test -count=1 -timeout 25s -run '^TestIntegrationCommitAbortIsolation$' ./internal/platform/kafka; then
  echo 'FAIL: unavailable opt-in broker passed' >&2
  exit 1
fi
if KAFKA_TEST_BROKER="127.0.0.1:$closed_port" GOTOOLCHAIN=local go test -count=1 -timeout 25s -run '^TestIntegrationRegistrationIdentityAndDuplicates$' ./internal/matcher/pricefeed; then
  echo 'FAIL: unavailable opt-in pricefeed broker passed' >&2
  exit 1
fi
printf 'PASS SET-009 / MAT-013: commit/abort/isolation, fencing, lost EndTxn response, stable retry identity, unavailable broker\n'
