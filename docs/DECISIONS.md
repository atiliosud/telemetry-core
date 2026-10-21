# Design decisions

Short log of the decisions taken while turning `SPEC.md` into code. Each entry: the decision and why.
When the rules in the spec conflict with the acceptance criteria (§16), the criteria win.

## Changes to the specification

### D1. Idempotency: the database is the source of truth, Redis is only a hint (R2)
- **Before:** the worker ran `SET dedupe:{eventId} 1 NX EX 86400` *before* persisting. If the process died after the `SET` and before the batch commit, the replayed event would be seen as a duplicate and dropped. That breaks "killing the worker at any point does not change the final result".
- **Now:** each batch is written with `INSERT ... ON CONFLICT (event_id) DO NOTHING RETURNING event_id` inside the batch transaction. Only returned (new) events go through the rules; the rest are counted as duplicates. Redis keys are set *after* the commit and are only used as a pre-check to skip known duplicates without a DB round trip. A key always refers to a committed event, so Redis can never cause a loss.
- **Counting:** without crashes, `telemetry_duplicates_total` equals the injected duplicates. After a crash, replays also count as duplicates, so crash tests check the number of unique rows in `processed_events`.

### D2. Architecture line in §2 aligned with D1
The pipeline line now reads `validate -> batch -> dedupe (Redis hint + DB key) -> apply rules`.

### D3. Section on external role mapping removed
The spec is kept as a generic engineering document.

## Gaps closed while building

### D4. Extra columns in `vehicle_state`: `overspeed_streak` and `distance_date`
R5 needs "3 consecutive events over 120 km/h" and R4 resets distance at UTC midnight. If those counters lived only in memory, a crash in the middle of a streak would change the result. Persisting them with the state keeps restarts deterministic.

### D5. Rules run against a working copy; memory is updated only after the commit
For each batch, the processor clones the state of the touched vehicles, applies the rules, writes everything in one transaction and only then publishes the copy to the in-memory `ConcurrentDictionary`. A failed or retried batch never leaves memory ahead of the database. On a cache miss, state is loaded from `vehicle_state`.

### D6. Daily distance reset uses the event timestamp, not the wall clock
Replaying the same events gives the same result.

### D7. GPS jumps
A jump (> 5 km in < 10 s) adds no distance but the position is still updated, so one bad fix does not poison the next distance calculation.

### D8. Alert id = id of the event that raised it
Alerts and their outbox rows are written in the same transaction as the event, and the id is deterministic, so a replay cannot create a second alert. Only `SPEEDING` is raised; clearing the alert just resets the flag in `vehicle_state`.

### D9. Offset handling: `StoreOffset` after the DB commit + auto-commit
`enable.auto.offset.store=false`: the processor stores `offset + 1` only after the batch (and its DLQ messages) are durable, and the client commits stored offsets periodically. On revoke and on shutdown the worker commits explicitly. This keeps "commit only after the DB commit" without a commit call per batch.

### D10. Partition revoke waits synchronously
The Kafka client calls the revoke handler synchronously, and the partition must be drained before it returns (offsets of an unassigned partition cannot be stored). This is the only blocking wait in the worker, it runs on the consume thread, and it is bounded by a timeout. Vehicles of revoked partitions are evicted from memory so a later reassignment reloads fresh state from the DB.

### D11. Batch retry under an open circuit
When the DB circuit breaker is open, the processor keeps the batch and retries it after a delay. Its channel fills up, the consume loop waits on the full channel and lag grows: backpressure, no data dropped. Limitation: an outage longer than `max.poll.interval.ms` (5 min) would trigger a rebalance; pausing partitions would be the next step.

### D12. The worker hosts a small HTTP endpoint
`/metrics`, `/health/live` and `/health/ready` are needed for Prometheus and Kubernetes probes, so the worker uses the ASP.NET Core runtime image (`mcr.microsoft.com/dotnet/aspnet`), still running as the non-root `app` user. The load generator uses the plain `runtime` image.

### D13. Least-privilege database users
- `telemetry_worker`: `INSERT`/`UPDATE` on the four tables, plus the `SELECT` that `RETURNING`, upserts, state hydration and the outbox poller need. No `DELETE`, `TRUNCATE` or DDL.
- `telemetry_reader` (Read API): `SELECT` on `vehicle_state` and `alerts` only.
- Roles are created by an init script that reads passwords from environment variables; there are no passwords in SQL files.

### D14. Traces go to Jaeger
The spec does not name a trace backend. Jaeger all-in-one accepts OTLP and has a UI. Each batch span carries **links** to the producer contexts (`traceparent` from Kafka headers), because a batch has many parents.

### D15. Consumer lag from `kafka-exporter`
Lag stays visible even when the worker is down, which is what the chaos demos need. KEDA computes lag on its own in Kubernetes.

### D16. Load generator timestamps and movement
Timestamps are the send time (so `telemetry_processing_latency_seconds` is meaningful). Movement advances by `speed × 1 s` per event, so normal traffic never looks like a GPS jump. Out-of-order is injected by swapping two consecutive events of a vehicle; duplicates resend an already sent valid event with the same `eventId`; invalid events are never duplicated. The summary is printed as text and as one JSON line so scripts can check the numbers.

### D17. DLQ is at-least-once
A DLQ message is produced before the offset is stored. A crash between the two can send it twice; this is acceptable for a dead-letter topic.

### D18. Package and tooling choices
- Build with the .NET 8 SDK or newer (`global.json` rolls forward); every project targets `net8.0`.
- FluentAssertions is pinned to 7.x (Apache-2.0); version 8 changed to a commercial license.
- Central package management (`Directory.Packages.props`).
- Kafka runs in KRaft mode with the official `apache/kafka` image.
