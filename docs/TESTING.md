# Testing

## Test pyramid

| Project | Needs Docker | Covers |
|---|---|---|
| `tests/TelemetryCore.Domain.Tests` | no | `EventValidatorTests`, `GeoTests`, `TelemetryRulesTests` (validation, distance, out-of-order, GPS jumps, speeding) |
| `tests/TelemetryCore.LoadGenerator.Tests` | no | `EventPlannerTests`, `GeneratorOptionsTests` (fault planning, deterministic seeds, options) |
| `tests/TelemetryCore.Worker.Tests` | no | `PartitionProcessorTests`, `VehicleStateCacheTests`, `ResiliencePipelinesTests`, `RedisDedupeHintTests`, `TelemetryMetricsTests` (batching, flush, cache, Polly pipelines, fakes) |
| `tests/TelemetryCore.IntegrationTests` | yes | `BatchCommitterTests`, `IdempotencyAndDlqTests`, `OutboxTests`, `PipelineTests`, `ReadApiTests`, `ResilienceTests`, `ShutdownTests`, `WorkerCompositionTests`, `WorkerEndpointTests` against real Kafka, Postgres and Redis |

## Running

```powershell
dotnet test TelemetryCore.sln                                              # everything
dotnet test tests/TelemetryCore.Domain.Tests
dotnet test tests/TelemetryCore.LoadGenerator.Tests
dotnet test tests/TelemetryCore.Worker.Tests
dotnet test tests/TelemetryCore.IntegrationTests                           # Docker required
```

Integration tests use Testcontainers (Kafka, PostgreSQL, Redis; see `TelemetryStackFixture`). Docker Desktop must be running and able to pull images on the first run.

## Demo

```powershell
scripts\up.ps1
scripts\demo.ps1 -Total 100000
```

The demo sends events with injected duplicates (2 %), out-of-order (1 %), invalid (0.5 %) and speeding vehicles, using a fresh `--vehicle-prefix` each run. It compares the injected numbers with unique rows in `processed_events` and with the worker counters `telemetry_duplicates_total`, `telemetry_out_of_order_total` and `telemetry_dlq_total`, prints `PASS` only if all match, and writes the output under `artifacts/`. Linux/macOS: `./scripts/demo.sh --total 100000`.

## Chaos

Requires the stack to be up (`scripts\up.ps1`).

```powershell
scripts\chaos.ps1 -Scenario kill        # docker kill -s KILL the worker 15 s into the load, then start it again
scripts\chaos.ps1 -Scenario postgres    # stop PostgreSQL for 20 s during the load
scripts\chaos.ps1 -Scenario redis       # stop Redis until every row is persisted (the worker falls back to the DB key)
scripts\chaos.ps1 -Scenario rebalance   # scale the worker to 2 replicas and back to 1 during the load
```

Each scenario runs the load generator (100,000 events at 2,000 events/s by default, `-Total` and `-Rate` to change; fresh random seed and vehicle prefix), injects the failure while the load is flowing, waits, and passes only if the number of new **unique rows** in `processed_events` equals the expected unique events. Extra checks: `postgres` requires the circuit breaker to open (gauge max = 1), `redis` requires Redis fallbacks > 0, `rebalance` requires two worker containers running (D31). `-Scenario all` runs the four in sequence. Crash replays are counted as duplicates by the worker, so counters are not compared in chaos runs (D1). Breaker state and consumer lag are printed before, during and after. Output is saved under `artifacts/chaos-*.txt`. Linux/macOS: `./scripts/chaos.sh kill` (or `all`).

Container-level resilience tests live in `ResilienceTests` (integration project): in-process crash mid-batch, Redis down, PostgreSQL paused and a two-worker rebalance.

## Security verification

```powershell
scripts\verify-security.ps1     # or ./scripts/verify-security.sh
scripts\verify-infra.ps1        # topics, Redis, schema, least-privilege roles
```

`verify-security` checks that images run as a non-root numeric user, with a read-only root filesystem, `cap_drop ALL`, `no-new-privileges`, and no secrets in image layers or tracked files. Exit code 0 means all checks passed.

## Load results

Machine: 8 logical CPUs, 15.4 GB RAM, Windows 11; Docker Desktop limited to 8 CPUs / 7.5 GB. One worker replica, single-node Kafka (12 partitions). Not a benchmark.

| Scenario | Result |
|---|---|
| 100,000 events demo | PASS: 97,500 / 97,500 unique rows, duplicates 2,000 / 2,000, out-of-order 1,000 / 1,000, DLQ 500 / 500; 21.0 s end to end |
| 1,000,000 events demo | PASS: 975,000 / 975,000 unique rows, duplicates 20,000 / 20,000, out-of-order 10,000 / 10,000, DLQ 5,000 / 5,000; 310.3 s end to end |
| Throughput | worker peak 6,955 events/s (100k) and 6,864 events/s (1M); generator 14,760 and 18,458 events/s. p95 batch 0.88 s / 0.60 s. p95 latency 18.8 s / 30 s (histogram ceiling): events queue in Kafka because one worker is slower than the generator (max lag 88,551 / 850,717) |
| Chaos: worker kill (100k) | PASS: 97,500 / 97,500 unique rows, none lost or duplicated; lag 49,751 while down, back to 0 about 55 s after the restart |
| Recovery after DB outage (PostgreSQL stopped 20 s, 100k) | PASS: 97,500 / 97,500 unique rows; lag held at 59,901 during the outage (backpressure, nothing dropped) and reached 0 about 27 s after PostgreSQL started. The breaker gauge sampled by Prometheus stayed at 0 (closed), so circuit opening was not observed in this run |
| Chaos: Redis stopped (100k) | PASS: 97,500 / 97,500 unique rows. Note: the script restarts Redis when the generator finishes, so Redis was down only ~2 s in this run; `ResilienceTests` keeps it down longer |
| Chaos: rebalance (100k) | Unique rows 97,500 / 97,500, but `--scale worker=2` failed in Compose (exit 1: the worker publishes the fixed host port 127.0.0.1:8081), so this run did not exercise a rebalance. Rebalance is covered by `Rebalance_TwoWorkers_NoLossNoDoubleCount` (two in-process workers, clean handover, exact row count) |
