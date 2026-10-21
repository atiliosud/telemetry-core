# Chaos scenarios against the running compose stack (SPEC section 16). Requires scripts\up.ps1 first.
#   scripts\chaos.ps1 -Scenario kill|postgres|redis|rebalance|all [-Total 200000]
# Each scenario runs the load generator in the background (fresh --vehicle-prefix), injects the fault mid-run, then waits
# until processed_events grows by the generator's expectedUnique (D1 counting rule: unique rows, not counters) and prints
# PASS/FAIL. Breaker state and consumer lag are read from Prometheus before / during / after the fault.
# Output is saved to artifacts\chaos-<scenario>-<timestamp>.txt.
param(
    [ValidateSet('kill', 'postgres', 'redis', 'rebalance', 'all')]
    [string]$Scenario = 'all',
    [long]$Total = 200000,
    [int]$Rate = 0,
    [int]$TimeoutMinutes = 10,
    [string]$PrometheusUrl = 'http://localhost:9090'
)

# Continue, not Stop: Windows PowerShell 5.1 turns docker's stderr progress into terminating errors under Stop.
$ErrorActionPreference = 'Continue'
$root = Split-Path -Parent $PSScriptRoot
$compose = Join-Path $root 'deploy\docker-compose.yml'
$envFile = Join-Path $root '.env'
if (-not (Test-Path $envFile)) { $envFile = Join-Path $root '.env.example' }
$inv = [System.Globalization.CultureInfo]::InvariantCulture
$stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$script:report = New-Object System.Collections.Generic.List[string]

function Say {
    param([string]$Text = '')
    $line = "[{0}] {1}" -f (Get-Date -Format 'HH:mm:ss'), $Text
    Write-Host $line
    $script:report.Add($line)
}

function Invoke-Compose {
    param([string[]]$ComposeArgs)
    $a = @('compose', '-p', 'telemetry-core', '--env-file', $envFile, '-f', $compose) + $ComposeArgs
    $lines = & docker @a 2>&1 | ForEach-Object { "$_" }
    return @{ ExitCode = $LASTEXITCODE; Lines = $lines }
}

function Get-DbCount {
    for ($i = 0; $i -lt 30; $i++) {
        $r = Invoke-Compose @('exec', '-T', 'postgres', 'psql', '-U', 'postgres', '-d', 'telemetry', '-tAc', 'select count(*) from processed_events')
        $text = ($r.Lines | Where-Object { $_ -match '^\s*\d+\s*$' } | Select-Object -First 1)
        if ($r.ExitCode -eq 0 -and $text) { return [long]$text.Trim() }
        Start-Sleep -Seconds 2 # Postgres may be stopped / restarting
    }
    throw 'Could not read processed_events count'
}

function Get-Prom {
    param([string]$Query)
    try {
        $wc = New-Object System.Net.WebClient
        try { $json = $wc.DownloadString("$PrometheusUrl/api/v1/query?query=" + [uri]::EscapeDataString($Query)) } finally { $wc.Dispose() }
        $r = $json | ConvertFrom-Json
        $vals = @($r.data.result | ForEach-Object { [string]$_.value[1] })
        if ($vals.Count -eq 0) { return 'n/a' }
        return ($vals -join ',')
    }
    catch { return 'unreachable' }
}

function Show-Health {
    param([string]$Phase)
    $breaker = Get-Prom 'max(telemetry_circuit_breaker_state)'
    $lag = Get-Prom 'sum(kafka_consumergroup_lag{consumergroup="telemetry-worker"})'
    Say "$Phase : breaker state=$breaker (0 closed, 1 open, 2 half-open), consumer lag=$lag"
}

function Start-Generator {
    param([string]$Prefix)
    $genArgs = @('compose', '-p', 'telemetry-core', '--env-file', $envFile, '-f', $compose, '--profile', 'tools', 'run', '--rm', '-T', 'generator',
        '--total', $Total.ToString($inv), '--seed', (Get-Random -Minimum 1 -Maximum 2147483647).ToString($inv), '--vehicle-prefix', $Prefix, '--rate', $Rate.ToString($inv))
    $out = Join-Path $env:TEMP "chaos-gen-$Prefix.txt"
    $job = Start-Job -ScriptBlock {
        param($a, $o)
        & docker @a *> $o
        $LASTEXITCODE
    } -ArgumentList (, $genArgs), $out
    return @{ Job = $job; Out = $out }
}

function Wait-Rows {
    param([long]$Target)
    $deadline = (Get-Date).AddMinutes($TimeoutMinutes)
    while ((Get-Date) -lt $deadline) {
        $n = Get-DbCount
        if ($n -ge $Target) { return $n }
        Start-Sleep -Seconds 3
    }
    return (Get-DbCount)
}

function Invoke-Scenario {
    param([string]$Name)
    Say "=== Scenario: $Name (total $Total) ==="
    $prefix = "C$($Name.Substring(0, 1).ToUpper())$($stamp -replace '-', '')-"
    $base = Get-DbCount
    Say "Baseline rows: $base; vehicle prefix $prefix"
    Show-Health 'before'
    $gen = Start-Generator $prefix
    Start-Sleep -Seconds 15 # let the run get going

    switch ($Name) {
        'kill' {
            Say 'docker kill --signal KILL telemetry-core-worker-1'
            $null = & docker kill --signal KILL telemetry-core-worker-1 2>&1
            Start-Sleep -Seconds 5
            Show-Health 'during'
            $r = Invoke-Compose @('up', '-d', 'worker'); Say "compose up -d worker -> exit $($r.ExitCode)"
        }
        'postgres' {
            $r = Invoke-Compose @('stop', 'postgres'); Say "compose stop postgres -> exit $($r.ExitCode)"
            Start-Sleep -Seconds 10
            Show-Health 'during'
            Start-Sleep -Seconds 10
            Show-Health 'during (20 s)'
            $r = Invoke-Compose @('start', 'postgres'); Say "compose start postgres -> exit $($r.ExitCode)"
        }
        'redis' {
            $r = Invoke-Compose @('stop', 'redis'); Say "compose stop redis -> exit $($r.ExitCode) (stays down for the rest of the run)"
            Show-Health 'during'
        }
        'rebalance' {
            $r = Invoke-Compose @('up', '-d', '--no-recreate', '--scale', 'worker=2', 'worker'); Say "scale worker=2 -> exit $($r.ExitCode)"
            Start-Sleep -Seconds 20
            Show-Health 'during (2 workers)'
            $r = Invoke-Compose @('up', '-d', '--no-recreate', '--scale', 'worker=1', 'worker'); Say "scale worker=1 -> exit $($r.ExitCode)"
        }
    }

    $null = Wait-Job $gen.Job -Timeout ($TimeoutMinutes * 60)
    $genExit = Receive-Job $gen.Job | Select-Object -Last 1
    Remove-Job $gen.Job -Force
    $lines = @(Get-Content $gen.Out -ErrorAction SilentlyContinue)
    Remove-Item $gen.Out -ErrorAction SilentlyContinue
    $summaryLine = $lines | Where-Object { $_ -like 'SUMMARY *' } | Select-Object -Last 1
    if ($name -eq 'redis') { $r = Invoke-Compose @('start', 'redis'); Say "compose start redis -> exit $($r.ExitCode)" }
    if (-not $summaryLine) {
        Say "Generator failed (exit $genExit):"
        $lines | Select-Object -Last 15 | ForEach-Object { Say $_ }
        return $false
    }
    Say $summaryLine
    $expected = [long]($summaryLine.Substring(8) | ConvertFrom-Json).expectedUnique
    $final = Wait-Rows ($base + $expected)
    Start-Sleep -Seconds 5
    $final = Get-DbCount # settle: more rows than expected would mean double counting
    Show-Health 'after'
    $delta = $final - $base
    $ok = ($delta -eq $expected)
    if ($ok) { $res = 'PASS' } else { $res = 'FAIL' }
    Say ("RESULT {0}: {1}  unique rows delta {2} / expected {3}" -f $Name, $res, $delta, $expected)
    return $ok
}

$scenarios = @($Scenario)
if ($Scenario -eq 'all') { $scenarios = @('kill', 'postgres', 'redis', 'rebalance') }
$allOk = $true
try {
    foreach ($s in $scenarios) {
        $ok = Invoke-Scenario $s | Select-Object -Last 1
        if (-not $ok) { $allOk = $false }
    }
}
catch {
    Say "ERROR: $($_.Exception.Message)"
    $allOk = $false
}
finally {
    $artifacts = Join-Path $root 'artifacts'
    if (-not (Test-Path $artifacts)) { New-Item -ItemType Directory -Path $artifacts | Out-Null }
    $file = Join-Path $artifacts "chaos-$Scenario-$stamp.txt"
    $script:report | Out-File -FilePath $file -Encoding utf8
    Write-Host "Saved to $file"
}
if ($allOk) { exit 0 } else { exit 1 }
