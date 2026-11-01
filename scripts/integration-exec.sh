#!/bin/sh
set -eu

# Each package owns its reaper so cleanup cannot race the next package.
export TESTCONTAINERS_SESSION_ID="${TESTCONTAINERS_SESSION_ID:-fabriq}-$(date +%s)-$$"
exec "$@"
