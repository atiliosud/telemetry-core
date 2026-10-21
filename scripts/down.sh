#!/usr/bin/env bash
# Stops the local stack. Volumes are kept unless --purge is given (which also deletes the Postgres data).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$ROOT/.env"
[ -f "$ENV_FILE" ] || ENV_FILE="$ROOT/.env.example"
EXTRA=()
if [ "${1:-}" = "--purge" ]; then EXTRA+=(-v); fi
docker compose --env-file "$ENV_FILE" -f "$ROOT/deploy/docker-compose.yml" down ${EXTRA[@]+"${EXTRA[@]}"}
