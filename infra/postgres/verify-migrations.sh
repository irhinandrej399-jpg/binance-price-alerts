#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/../.."
export POSTGRES_PORT=${MIGRATION_CHECK_PORT:-55434}
project=binance-migrations-check
compose=(docker compose -p "$project" -f infra/postgres/compose.yaml)
"${compose[@]}" up -d --wait --wait-timeout 120
MIGRATION_TEST_PROJECT=$project PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/tests -p test_migrate.py -v
"${compose[@]}" down
echo 'OK: migration checks complete; dedicated test volume retained'
