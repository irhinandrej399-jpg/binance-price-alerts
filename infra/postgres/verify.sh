#!/bin/bash
# Dedicated test project; leaves its volume intact for inspection.
set -euo pipefail
cd "$(dirname "$0")"
export POSTGRES_PORT=${STORAGE_CHECK_PORT:-55432}
compose=(docker compose -p binance-storage-check -f compose.yaml)
"${compose[@]}" up -d --wait --wait-timeout 120
"${compose[@]}" exec -T postgres bash -s < check-roles.sh
"${compose[@]}" down
"${compose[@]}" up -d --wait --wait-timeout 120
"${compose[@]}" exec -T postgres bash -s <<'CHECK'
set -euo pipefail
for owner in api market_data matcher notifications; do
  key=${owner^^}_RUNTIME_PASSWORD
  export PGPASSWORD=${!key}
  value=$(psql -X -h 127.0.0.1 -U "${owner}_runtime" -d "$owner" -v ON_ERROR_STOP=1 -Atqc 'SELECT value FROM app.storage_check')
  [[ $value == updated ]]
  psql -X -h 127.0.0.1 -U "${owner}_runtime" -d "$owner" -v ON_ERROR_STOP=1 -qc 'DELETE FROM app.storage_check'
  key=${owner^^}_MIGRATION_PASSWORD
  export PGPASSWORD=${!key}
  psql -X -h 127.0.0.1 -U "${owner}_migration" -d "$owner" -v ON_ERROR_STOP=1 -qc 'DROP TABLE app.storage_check'
done
echo 'OK: all four committed rows survived down/up and container recreation'
CHECK
"${compose[@]}" down
echo 'OK: storage check complete; test volume retained'
