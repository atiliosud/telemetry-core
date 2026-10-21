# Smoke check for the local stack (Kafka topics, Redis, Postgres schema and least-privilege roles).
# Works from any cwd. Exit code 0 = all OK, 1 = at least one check failed.
$ErrorActionPreference = 'Continue'
$root = Split-Path -Parent $PSScriptRoot
$compose = Join-Path $root 'deploy\docker-compose.yml'
$envFile = Join-Path $root '.env'
if (-not (Test-Path $envFile)) { $envFile = Join-Path $root '.env.example' }

$script:failed = 0
function Run-Compose {
    param([string[]]$ComposeArgs)
    $a = @('compose', '--env-file', $envFile, '-f', $compose) + $ComposeArgs
    $out = & docker @a 2>&1 | Out-String
    return $out
}
function Check {
    param([string]$Name, [bool]$Ok, [string]$Detail = '')
    if ($Ok) { Write-Host "OK    $Name" }
    else { Write-Host "FAIL  $Name  $Detail"; $script:failed++ }
}

# 1. Kafka topics and partition counts
$expected = @{ 'telemetry.raw' = 12; 'telemetry.dlq' = 3; 'telemetry.alerts' = 3 }
foreach ($t in $expected.Keys) {
    $out = Run-Compose @('exec', '-T', 'kafka', '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'kafka:9092', '--describe', '--topic', $t)
    $m = [regex]::Match($out, 'PartitionCount:\s*(\d+)')
    $got = if ($m.Success) { [int]$m.Groups[1].Value } else { -1 }
    Check "topic $t has $($expected[$t]) partitions" ($got -eq $expected[$t]) "(got $got)"
}

# 2. Redis
$out = Run-Compose @('exec', '-T', 'redis', 'redis-cli', 'PING')
Check 'redis PING -> PONG' ($out.Trim() -eq 'PONG') "(got '$($out.Trim())')"

# 3. Postgres tables
foreach ($tbl in @('processed_events', 'vehicle_state', 'alerts', 'outbox')) {
    $out = Run-Compose @('exec', '-T', 'postgres', 'psql', '-U', 'postgres', '-d', 'telemetry', '-tAc', "SELECT to_regclass('public.$tbl') IS NOT NULL")
    Check "table $tbl exists" ($out.Trim() -eq 't') "(got '$($out.Trim())')"
}

# 4. Least privilege
function Test-Denied {
    param([string]$User, [string]$Sql)
    $out = Run-Compose @('exec', '-T', 'postgres', 'psql', '-U', $User, '-d', 'telemetry', '-c', $Sql)
    return ($out -match 'permission denied')
}
Check 'telemetry_worker denied DELETE FROM processed_events' (Test-Denied 'telemetry_worker' 'DELETE FROM processed_events')
Check 'telemetry_reader denied INSERT INTO alerts' (Test-Denied 'telemetry_reader' "INSERT INTO alerts (alert_id, vehicle_id, type, raised_utc, payload) VALUES (gen_random_uuid(), 'v', 't', now(), '{}')")

if ($script:failed -gt 0) { Write-Host "$($script:failed) check(s) failed"; exit 1 }
Write-Host 'All infra checks OK'
exit 0
