# Testing

## Test pyramid

| Project | Needs Docker | Covers |
|---|---|---|
| `tests/TelemetryCore.Domain.Tests` | no | `EventValidatorTests`, `GeoTests`, `TelemetryRulesTests` (validation, distance, out-of-order, GPS jumps, speeding) |
| `tests/TelemetryCore.LoadGenerator.Tests` | no | `EventPlannerTests`, `GeneratorOptionsTests` (fault planning, deterministic seeds, options) |
| `tests/TelemetryCore.Worker.Tests` | no | `PartitionProcessorTests`, `VehicleStateCacheTests`, `ResiliencePipelinesTests`, `RedisDedupeHintTests`, `TelemetryMetricsTests` (batching, flush, cache, Polly pipelines, fakes) |
| `tests/TelemetryCore.IntegrationTests` | yes | `BatchCommitterTests`, `IdempotencyAndDlqTests`, `OutboxTests`, `PipelineTests`, `ReadApiTests`, `ShutdownTests`, `WorkerCompositionTests`, `WorkerEndpointTests` against real Kafka, Postgres and Redis |

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

`scripts/chaos.ps1` is planned and not present yet <!-- VERIFY -->. Expected scenarios: kill the worker mid-load, stop PostgreSQL, stop Redis, stop Kafka; each followed by a check that no event is lost or counted twice. A manual equivalent with Compose:

```powershell
docker compose --env-file .env -f deploy/docker-compose.yml stop postgres   # then start it again
```

## Security verification

```powershell
scripts\verify-security.ps1     # or ./scripts/verify-security.sh
scripts\verify-infra.ps1        # topics, Redis, schema, least-privilege roles
```

`verify-security` checks that images run as a non-root numeric user, with a read-only root filesystem, `cap_drop ALL`, `no-new-privileges`, and no secrets in image layers or tracked files. Exit code 0 means all checks passed.

## Load results

Machine: <!-- MEASURED: CPU / RAM / OS / Docker resources -->

| Scenario | Result |
|---|---|
| 100,000 events demo | <!-- MEASURED --> |
| Throughput | <!-- MEASURED --> |
| Chaos: worker kill | <!-- MEASURED --> |
| Chaos: DB outage | <!-- MEASURED --> |
