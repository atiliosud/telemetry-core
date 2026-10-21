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
& docker compose --env-file $envFile -f (Join-Path $root 'deploy\docker-compose.yml') up -d --build --wait
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
