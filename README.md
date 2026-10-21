# Telemetry Core

A generic portfolio project: the ingestion core of a vehicle telemetry platform, built with .NET 8.
A worker consumes telemetry events from Kafka, validates them, applies business rules (out-of-order detection, GPS
jumps, daily distance, speeding alerts) and persists them idempotently to PostgreSQL, with Redis as a dedupe hint,
a dead-letter topic for poison messages, Polly resilience policies and Prometheus metrics.

- Specification: [SPEC.md](SPEC.md)
- Design decisions and deviations from the spec: [docs/DECISIONS.md](docs/DECISIONS.md)

## Quick start

Prerequisites: .NET 8 SDK or newer, Docker Desktop, git.

```powershell
scripts\up.ps1                    # Kafka, Redis, Postgres and the worker (Docker Compose); builds the images
scripts\demo.ps1 -Total 100000    # sends 100,000 events with injected faults and checks the numbers
scripts\down.ps1                  # stop (add -Purge to also delete the database volume)
```

On Linux/macOS use `scripts/up.sh`, `scripts/demo.sh --total 100000` and `scripts/down.sh`.
The first `up` creates `.env` from `.env.example` (local demo values only).

The demo runs the load generator (2% duplicates, 1% out-of-order, 0.5% invalid events by default) and then compares
what was injected with what the worker counted: unique rows in `processed_events`, and the
`telemetry_duplicates_total`, `telemetry_out_of_order_total` and `telemetry_dlq_total` counters from the worker's
metrics endpoint. It prints `PASS` only when every number matches, and saves the output under `artifacts/`.

Worker endpoints (local only):

- Metrics: http://localhost:8081/metrics
- Health: http://localhost:8081/health/live and http://localhost:8081/health/ready

## Tests

```powershell
dotnet test TelemetryCore.sln     # unit tests + integration tests (Testcontainers: needs Docker running)
```

## License

[MIT](LICENSE)
