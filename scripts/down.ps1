# Stops the local stack. Volumes are kept unless -Purge is given (which also deletes the Postgres data).
param([switch]$Purge)
# Continue, not Stop: Windows PowerShell 5.1 turns docker's stderr progress into terminating errors under Stop. Native failures are checked via $LASTEXITCODE.
$ErrorActionPreference = 'Continue'
$root = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $root '.env'
if (-not (Test-Path $envFile)) { $envFile = Join-Path $root '.env.example' }
$extra = @()
if ($Purge) { $extra += '-v' }
& docker compose --env-file $envFile -f (Join-Path $root 'deploy\docker-compose.yml') down @extra
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
