#!/usr/bin/env bash
# Starts the local stack. Works from any cwd.
# Note: DB role passwords (.env) are applied only on the first init of the pgdata volume.
# After changing them, run scripts/down.sh --purge (or down.ps1 -Purge) and start again.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [ ! -f "$ROOT/.env" ]; then
  cp "$ROOT/.env.example" "$ROOT/.env"
  echo "Created .env from .env.example (change the placeholder values for anything beyond local demo)."
fi
docker compose --env-file "$ROOT/.env" -f "$ROOT/deploy/docker-compose.yml" up -d --build --wait
