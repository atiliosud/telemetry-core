# Starts the local stack. Works from any cwd.
# Note: DB role passwords (.env) are applied only on the first init of the pgdata volume.
# After changing them, run scripts/down.ps1 -Purge (or down.sh --purge) and start again.
# Continue, not Stop: Windows PowerShell 5.1 turns docker's stderr progress into terminating errors under Stop. Native failures are checked via $LASTEXITCODE.
$ErrorActionPreference = 'Continue'
$root = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $root '.env'
if (-not (Test-Path $envFile)) {
    Copy-Item (Join-Path $root '.env.example') $envFile
    Write-Host 'Created .env from .env.example (change the placeholder values for anything beyond local demo).'
}
$compose = Join-Path $root 'deploy\docker-compose.yml'
# Starts Kafka, Redis, Postgres, the worker, the Read API and the observability stack (Prometheus, Grafana, Jaeger,
# kafka-exporter); --wait returns once every service is healthy or running.
& docker compose --env-file $envFile -f $compose up -d --build --wait
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
# The load generator is a one-shot tool (profile "tools"): build it now so the demo starts right away.
& docker compose --env-file $envFile -f $compose --profile tools build generator
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
Write-Host ''
Write-Host 'Stack is up.'
Write-Host '  Worker metrics : http://localhost:8081/metrics'
Write-Host '  Worker health  : http://localhost:8081/health/live | /health/ready'
Write-Host '  Read API       : http://localhost:8080 (header X-Api-Key = TELEMETRY_API_KEY from .env)'
Write-Host '  Prometheus     : http://localhost:9090'
Write-Host '  Grafana        : http://localhost:3000 (dashboard "Telemetry Core"; anonymous viewer, admin password in .env)'
Write-Host '  Jaeger         : http://localhost:16686 (service telemetry-worker, span batch.process)'
Write-Host '  Demo           : scripts\demo.ps1 -Total 100000'
