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
COMPOSE="$ROOT/deploy/docker-compose.yml"
# Starts Kafka, Redis, Postgres, the worker, the Read API and the observability stack (Prometheus, Grafana, Jaeger,
# kafka-exporter); --wait returns once every service is healthy or running.
docker compose --env-file "$ROOT/.env" -f "$COMPOSE" up -d --build --wait
# The load generator is a one-shot tool (profile "tools"): build it now so the demo starts right away.
docker compose --env-file "$ROOT/.env" -f "$COMPOSE" --profile tools build generator
echo
echo "Stack is up."
echo "  Worker metrics : http://localhost:8081/metrics"
echo "  Worker health  : http://localhost:8081/health/live | /health/ready"
echo "  Read API       : http://localhost:8080 (header X-Api-Key = TELEMETRY_API_KEY from .env)"
echo "  Prometheus     : http://localhost:9090"
echo "  Grafana        : http://localhost:3000 (dashboard \"Telemetry Core\"; anonymous viewer, admin password in .env)"
echo "  Jaeger         : http://localhost:16686 (service telemetry-worker, span batch.process)"
echo "  Demo           : scripts/demo.sh --total 100000"
