# Starts the local stack. Works from any cwd.
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $root '.env'
if (-not (Test-Path $envFile)) {
    Copy-Item (Join-Path $root '.env.example') $envFile
    Write-Host 'Created .env from .env.example (change the placeholder values for anything beyond local demo).'
}
& docker compose --env-file $envFile -f (Join-Path $root 'deploy\docker-compose.yml') up -d --build --wait
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
