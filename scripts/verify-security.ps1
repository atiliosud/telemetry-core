# Security verification (SPEC 13/16): non-root numeric UID, read-only rootfs, cap_drop ALL,
# no-new-privileges, and no secrets in tracked files or image layers.
# Works from any cwd. Exit code 0 = all OK, 1 = at least one check failed.
$ErrorActionPreference = 'Continue'
$root = Split-Path -Parent $PSScriptRoot
$compose = Join-Path $root 'deploy\docker-compose.yml'
$envFile = Join-Path $root '.env'
if (-not (Test-Path $envFile)) { $envFile = Join-Path $root '.env.example' }
Set-Location $root

$script:failed = 0
function Check([string]$Name, [bool]$Ok, [string]$Detail) {
    if ($Ok) { Write-Host "OK    $Name" } else { Write-Host "FAIL  $Name  ($Detail)"; $script:failed++ }
}

$services = @(
    @{ Svc = 'worker';    Image = 'telemetry-core-worker' },
    @{ Svc = 'read-api';  Image = 'telemetry-core-read-api' },
    @{ Svc = 'generator'; Image = 'telemetry-core-generator' }
)

# Secret values from .env
$secrets = @()
$envPath = Join-Path $root '.env'
if (Test-Path $envPath) {
    foreach ($line in Get-Content $envPath) {
        if ($line -match '^\s*[A-Z_]*(PASSWORD|API_KEY|SECRET)[A-Z_]*=(.{4,})$') { $secrets += $Matches[2].Trim() }
    }
}

foreach ($s in $services) {
    $img = $s.Image
    $uid = (& docker run --rm --entrypoint id $img -u 2>$null | Out-String).Trim()
    Check "$($s.Svc): image runs as non-root uid ($uid)" ($uid -match '^\d+$' -and $uid -ne '0') "uid='$uid'"
    $user = (& docker image inspect $img --format '{{.Config.User}}' 2>$null | Out-String).Trim()
    Check "$($s.Svc): Config.User is numeric non-zero ($user)" ($user -match '^[1-9]\d*(:\d+)?$') "Config.User='$user'"

    $hist = & docker history --no-trunc --format '{{.CreatedBy}}' $img 2>$null | Out-String
    $leak = @($secrets | Where-Object { $hist.Contains($_) })
    Check "$($s.Svc): no .env secret values in image history" ($leak.Count -eq 0) "$($leak.Count) secret value(s) found"

    # Runtime flags; the generator is a one-shot tool, so inspect its compose config when not running
    $cid = (& docker compose --env-file $envFile -f $compose ps -q $s.Svc 2>$null | Out-String).Trim()
    if ($cid) {
        $hc = (& docker inspect $cid --format '{{.HostConfig.ReadonlyRootfs}}|{{json .HostConfig.CapDrop}}|{{json .HostConfig.SecurityOpt}}' | Out-String).Trim()
        $p = $hc.Split('|')
        Check "$($s.Svc): read-only root filesystem" ($p[0] -eq 'true') $hc
        Check "$($s.Svc): cap_drop ALL" ($p[1] -match 'ALL') $hc
        Check "$($s.Svc): no-new-privileges" ($p[2] -match 'no-new-privileges') $hc
    } else {
        $cfg = & docker compose --env-file $envFile -f $compose --profile tools config --format json 2>$null | Out-String | ConvertFrom-Json
        $svc = $cfg.services.($s.Svc)
        Check "$($s.Svc): read-only root filesystem (config)" ($svc.read_only -eq $true) 'read_only not set'
        Check "$($s.Svc): cap_drop ALL (config)" (@($svc.cap_drop) -contains 'ALL') 'cap_drop missing'
        Check "$($s.Svc): no-new-privileges (config)" (@($svc.security_opt) -contains 'no-new-privileges:true') 'security_opt missing'
    }
}

# Writes to the root FS must fail in the running worker
$wcid = (& docker compose --env-file $envFile -f $compose ps -q worker 2>$null | Out-String).Trim()
if ($wcid) {
    & docker exec $wcid touch /app/x 2>$null | Out-Null
    Check 'worker: touch /app/x fails (read-only)' ($LASTEXITCODE -ne 0) 'write to /app succeeded'
    & docker exec $wcid touch /tmp/x 2>$null | Out-Null
    Check 'worker: /tmp is writable (tmpfs)' ($LASTEXITCODE -eq 0) 'tmpfs not writable'
}

# Secrets scan over tracked files (placeholders live in .env.example / secrets.example.yaml / docs)
$pattern = '(password|passwd|pwd|api[_-]?key|secret)\s*[:=]\s*["'']?[A-Za-z0-9/+_.!@#%^&*-]{6,}|BEGIN (RSA |EC )?PRIVATE KEY|AKIA[0-9A-Z]{16}'
$hits = & git grep -nIiE $pattern -- . ':!.env.example' ':!deploy/k8s/secrets.example.yaml' ':!docs' ':!*.md' ':!scripts/verify-security.*' ':!tests' ':!.superpowers' 2>$null |
    Where-Object { $_ -notmatch '^(tests|docs|\.superpowers)/|^deploy/k8s/secrets\.example|\$\{|change-me|<[^>]+>|Password=\$|password=\$|\$\$|REPLACE_ME|PASSWORD :''|from \.env|X-Api-Key' }
Check 'secrets scan: no credential literals in tracked files' (@($hits).Count -eq 0) ((@($hits) | Select-Object -First 5) -join ' | ')
if ($secrets.Count -gt 0) {
    $tracked = @()
    foreach ($v in $secrets) { $tracked += @(& git grep -lF -e $v -- . ':!.env.example' ':!.superpowers' 2>$null) }
    Check 'secrets scan: no .env value in tracked files' ($tracked.Count -eq 0) ($tracked -join ',')
}

if ($script:failed -gt 0) { Write-Host ""; Write-Host "$($script:failed) check(s) FAILED"; exit 1 }
Write-Host ""
Write-Host 'All security checks OK'
exit 0
