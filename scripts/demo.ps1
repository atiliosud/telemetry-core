# End-to-end demo (CHECKPOINT 1): runs the load generator against the running stack and checks the numbers.
#   scripts\up.ps1                    # once
#   scripts\demo.ps1 -Total 100000    # PASS / FAIL, exit code 0 / 1
# Baseline and final values come from Postgres (rows in processed_events) and from the worker's /metrics endpoint,
# so earlier runs do not matter: only deltas are compared. Each run uses fresh vehicle ids (--vehicle-prefix) and a
# fresh seed, so it never collides with stored vehicle state or event ids of earlier runs.
# Output is saved to artifacts\demo-<timestamp>.txt.
param(
    [long]$Total = 100000,
    [double]$Duplicates = 2,
    [double]$OutOfOrder = 1,
    [double]$Invalid = 0.5,
    [double]$SpeedingVehicles = 5,
    [int]$Vehicles = 1000,
    [int]$TimeoutMinutes = 10,
    # 0 = random seed per run.
    [int]$Seed = 0,
    [string]$MetricsUrl = 'http://localhost:8081/metrics',
    # Optional extra copy of the output file (e.g. a measurements folder outside the repo).
    [string]$CopyTo = ''
)

# Continue, not Stop: Windows PowerShell 5.1 turns docker's stderr progress into terminating errors under Stop.
# Native failures are checked via $LASTEXITCODE.
$ErrorActionPreference = 'Continue'
$root = Split-Path -Parent $PSScriptRoot
$compose = Join-Path $root 'deploy\docker-compose.yml'
$envFile = Join-Path $root '.env'
if (-not (Test-Path $envFile)) { $envFile = Join-Path $root '.env.example' }
$inv = [System.Globalization.CultureInfo]::InvariantCulture

$script:report = New-Object System.Collections.Generic.List[string]
function Say {
    param([string]$Text = '')
    Write-Host $Text
    $script:report.Add($Text)
}

function Invoke-Compose {
    param([string[]]$ComposeArgs)
    $a = @('compose', '--env-file', $envFile, '-f', $compose) + $ComposeArgs
    $lines = & docker @a 2>&1 | ForEach-Object { "$_" }
    return @{ ExitCode = $LASTEXITCODE; Lines = $lines }
}

function Get-DbCount {
    $r = Invoke-Compose @('exec', '-T', 'postgres', 'psql', '-U', 'postgres', '-d', 'telemetry', '-tAc', 'select count(*) from processed_events')
    $text = ($r.Lines | Where-Object { $_ -match '^\s*\d+\s*$' } | Select-Object -First 1)
    if ($r.ExitCode -ne 0 -or -not $text) { throw "Could not read processed_events count: $($r.Lines -join ' ')" }
    return [long]$text.Trim()
}

function Get-MetricsText {
    $wc = New-Object System.Net.WebClient
    try { return $wc.DownloadString($MetricsUrl) } finally { $wc.Dispose() }
}

# Sum of every series of a metric (all label sets) in the Prometheus text format; 0 when absent.
function Get-MetricSum {
    param([string]$Text, [string]$Name)
    $sum = 0.0
    foreach ($line in ($Text -split "`n")) {
        $m = [regex]::Match($line, '^' + [regex]::Escape($Name) + '(\{[^}]*\})?\s+(\S+)')
        if ($m.Success) { $sum += [double]::Parse($m.Groups[2].Value, $inv) }
    }
    return [long]$sum
}

function Get-Snapshot {
    $text = Get-MetricsText
    return [pscustomobject]@{
        Rows       = Get-DbCount
        Processed  = Get-MetricSum $text 'telemetry_events_processed_total'
        Duplicates = Get-MetricSum $text 'telemetry_duplicates_total'
        OutOfOrder = Get-MetricSum $text 'telemetry_out_of_order_total'
        Dlq        = Get-MetricSum $text 'telemetry_dlq_total'
        Consumed   = Get-MetricSum $text 'telemetry_events_consumed_total'
        Circuit    = Get-MetricSum $text 'telemetry_circuit_breaker_state'
    }
}

# Worker counters only grow; a decrease means the worker process restarted (in-memory counters reset).
$script:restartDetected = $false
function Test-Decrease {
    param($Before, $After)
    return ($After.Consumed -lt $Before.Consumed -or $After.Duplicates -lt $Before.Duplicates -or
        $After.OutOfOrder -lt $Before.OutOfOrder -or $After.Dlq -lt $Before.Dlq -or $After.Processed -lt $Before.Processed)
}

function Same {
    param($A, $B)
    return ($A.Rows -eq $B.Rows -and $A.Consumed -eq $B.Consumed -and $A.Duplicates -eq $B.Duplicates -and $A.Dlq -eq $B.Dlq -and $A.OutOfOrder -eq $B.OutOfOrder)
}

# Waits until nothing changes for $StableSeconds (worker idle), optionally only after $Condition holds.
function Wait-Stable {
    param([int]$StableSeconds, [scriptblock]$Condition = { $true }, [System.Diagnostics.Stopwatch]$Clock = $null)
    $deadline = (Get-Date).AddMinutes($TimeoutMinutes)
    $last = Get-Snapshot
    $stableSince = Get-Date
    $reachedAt = $null
    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Milliseconds 500
        $now = Get-Snapshot
        if (Test-Decrease $last $now) { $script:restartDetected = $true }
        if (-not (Same $now $last)) { $stableSince = Get-Date; $last = $now }
        if ($null -eq $reachedAt -and (& $Condition $now)) {
            if ($Clock) { $reachedAt = $Clock.Elapsed.TotalSeconds } else { $reachedAt = 0 }
        }
        if ($null -ne $reachedAt -and ((Get-Date) - $stableSince).TotalSeconds -ge $StableSeconds) {
            return [pscustomobject]@{ Snapshot = $now; ReachedAt = $reachedAt; TimedOut = $false }
        }
    }
    return [pscustomobject]@{ Snapshot = (Get-Snapshot); ReachedAt = $reachedAt; TimedOut = $true }
}

$stamp = Get-Date -Format 'yyyyMMdd-HHmmss'

function Save-Report {
    $artifacts = Join-Path $root 'artifacts'
    if (-not (Test-Path $artifacts)) { New-Item -ItemType Directory -Path $artifacts | Out-Null }
    $file = Join-Path $artifacts "demo-$stamp.txt"
    $script:report | Out-File -FilePath $file -Encoding utf8
    Write-Host "Saved to $file"
    if ($CopyTo) {
        $dir = Split-Path -Parent $CopyTo
        if ($dir -and -not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir | Out-Null }
        Copy-Item $file $CopyTo -Force
        Write-Host "Copied to $CopyTo"
    }
}

function Invoke-Demo {
    if ($Seed -eq 0) { $Seed = Get-Random -Minimum 1 -Maximum 2147483647 }
    $prefix = "D$($stamp -replace '-', '')-"
    Say "Telemetry Core demo  $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
    Say "Generator: --total $Total --vehicles $Vehicles --duplicates $Duplicates --out-of-order $OutOfOrder --invalid $Invalid --speeding-vehicles $SpeedingVehicles --seed $Seed --vehicle-prefix $prefix"

    try { $null = Get-MetricsText } catch { Say "Worker metrics not reachable at $MetricsUrl. Run scripts\up.ps1 first."; return 1 }

    Write-Host 'Waiting for the worker to be idle (no change for 5 s) before taking the baseline...'
    $idle = Wait-Stable -StableSeconds 5
    if ($idle.TimedOut) { Say "Worker did not become idle within $TimeoutMinutes min; aborting."; return 1 }
    $base = $idle.Snapshot
    $script:restartDetected = $false
    Say "Baseline: rows=$($base.Rows) duplicates=$($base.Duplicates) outOfOrder=$($base.OutOfOrder) dlq=$($base.Dlq)"
    if ($base.Circuit -eq 1) { Say 'WARNING: the DB circuit breaker is OPEN at baseline (telemetry_circuit_breaker_state = 1); processing may stall.' }

    $genArgs = @('--profile', 'tools', 'run', '--rm', 'generator',
        '--total', $Total.ToString($inv), '--vehicles', $Vehicles.ToString($inv),
        '--duplicates', $Duplicates.ToString($inv), '--out-of-order', $OutOfOrder.ToString($inv),
        '--invalid', $Invalid.ToString($inv), '--speeding-vehicles', $SpeedingVehicles.ToString($inv),
        '--seed', $Seed.ToString($inv), '--vehicle-prefix', $prefix)
    Write-Host 'Running the load generator...'
    $clock = [System.Diagnostics.Stopwatch]::StartNew()
    $gen = Invoke-Compose $genArgs
    $gen.Lines | Where-Object { $_ -match '^(\||\+)' } | ForEach-Object { Write-Host $_ }
    $summaryLine = $gen.Lines | Where-Object { $_ -like 'SUMMARY *' } | Select-Object -Last 1
    if ($gen.ExitCode -ne 0 -or -not $summaryLine) {
        Say 'Generator failed:'
        $gen.Lines | Select-Object -Last 20 | ForEach-Object { Say $_ }
        return 1
    }
    $summary = $summaryLine.Substring(8) | ConvertFrom-Json
    $expected = [long]$summary.expectedUnique

    Write-Host "Waiting for $expected new unique rows (timeout $TimeoutMinutes min)..."
    $done = Wait-Stable -StableSeconds 3 -Clock $clock -Condition { param($s) ($s.Rows - $base.Rows) -ge $expected }
    $final = $done.Snapshot
    if ($null -ne $done.ReachedAt) { $elapsed = [double]$done.ReachedAt } else { $elapsed = $clock.Elapsed.TotalSeconds }

    $processed = $final.Rows - $base.Rows
    $dups = $final.Duplicates - $base.Duplicates
    $ooo = $final.OutOfOrder - $base.OutOfOrder
    $dlq = $final.Dlq - $base.Dlq
    $eps = 0
    if ($elapsed -gt 0) { $eps = [long]([double]$summary.sent / $elapsed) }

    function Row {
        param([string]$Name, $Injected, $Counted)
        $ok = ''
        if ($null -ne $Injected -and $null -ne $Counted) { if ($Injected -eq $Counted) { $ok = 'OK' } else { $ok = 'MISMATCH' } }
        return ('| {0,-28} | {1,12} | {2,12} | {3,-8} |' -f $Name, $Injected, $Counted, $ok)
    }
    $sep = '+------------------------------+--------------+--------------+----------+'
    Say ''
    Say $sep
    Say ('| {0,-28} | {1,12} | {2,12} | {3,-8} |' -f 'Check', 'Expected', 'Counted', '')
    Say $sep
    Say (Row 'Sent' $summary.sent $null)
    Say (Row 'Unique -> processed (rows)' $expected $processed)
    Say (Row 'Duplicates' $summary.duplicatesInjected $dups)
    Say (Row 'Out-of-order' $summary.outOfOrderInjected $ooo)
    Say (Row 'Invalid -> DLQ' $summary.invalidInjected $dlq)
    Say $sep
    Say ('| {0,-28} | {1,12} |' -f 'Generator seconds', ([double]$summary.seconds).ToString('F1', $inv))
    Say ('| {0,-28} | {1,12} |' -f 'Generator events/s', ([double]$summary.eventsPerSecond).ToString('F0', $inv))
    Say ('| {0,-28} | {1,12} |' -f 'End-to-end s (incl. startup)', $elapsed.ToString('F1', $inv))
    Say ('| {0,-28} | {1,12} |' -f 'End-to-end events/s', $eps)
    Say $sep

    if ($script:restartDetected -or (Test-Decrease $base $final)) {
        Say 'WORKER RESTART DETECTED: a metrics counter went down during the run. The worker keeps its counters in memory,'
        Say 'so a restart resets them and the duplicate / out-of-order / DLQ deltas are not comparable (replayed batches also'
        Say 'count as duplicates after a restart, D1). Check "docker compose ps" / worker logs and run the demo again.'
        $script:restartDetected = $true
    }
    $pass = (-not $script:restartDetected) -and (-not $done.TimedOut) -and ($processed -eq $expected) -and ($dups -eq [long]$summary.duplicatesInjected) -and
        ($ooo -eq [long]$summary.outOfOrderInjected) -and ($dlq -eq [long]$summary.invalidInjected)
    if ($pass) { $result = 'PASS' } else { $result = 'FAIL' }
    if ($done.TimedOut) { Say "Timed out after $TimeoutMinutes min." }
    Say ("RESULT: {0}  (processed {1}/{2}, duplicates {3}/{4}, out-of-order {5}/{6}, dlq {7}/{8})" -f $result, $processed, $expected,
        $dups, $summary.duplicatesInjected, $ooo, $summary.outOfOrderInjected, $dlq, $summary.invalidInjected)
    if ($pass) { return 0 } else { return 1 }
}

$code = 1
try {
    $code = Invoke-Demo | Select-Object -Last 1
}
catch {
    Say "ERROR: $($_.Exception.Message)"
    $code = 1
}
finally {
    Save-Report
}
exit $code
