#!/bin/sh
set -eu
# Generated locally with shell quoting, mounted read-only for this service only.
set -a
. /run/config/runtime.env
set +a
exec /app/service "$@"
