# Stops the local stack. Volumes are kept unless -Purge is given (which also deletes the Postgres data).
param([switch]$Purge)
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $root '.env'
if (-not (Test-Path $envFile)) { $envFile = Join-Path $root '.env.example' }
$extra = @()
if ($Purge) { $extra += '-v' }
& docker compose --env-file $envFile -f (Join-Path $root 'deploy\docker-compose.yml') down @extra
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
