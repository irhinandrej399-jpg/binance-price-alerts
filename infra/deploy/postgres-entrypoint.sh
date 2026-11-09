#!/bin/bash
set -euo pipefail
set -a
source /run/config/postgres.env
set +a
exec /usr/local/bin/docker-entrypoint.sh "$@"
