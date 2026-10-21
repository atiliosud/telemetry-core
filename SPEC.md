# Telemetry Core — PoC Specification

A small, production-minded telemetry ingestion core in .NET 8. It consumes vehicle telemetry from Kafka at high volume, keeps per-vehicle ordering, never loses or double-counts an event, survives crashes and restarts, and exposes metrics and traces.

Goal: demonstrate, in working code, the patterns a real-time fleet-telemetry core needs: concurrency, backpressure, idempotency, resilience, zero-downtime operation and observability.

---

## 1. Scope

**In scope**
- Load generator that simulates a fleet and publishes telemetry to Kafka
- Ingestion worker (the core) that validates, deduplicates, processes and persists events
- Business rules: distance per vehicle, speeding alerts, out-of-order handling
- Outbox for alerts, published to a Kafka topic
- Small read API (CQRS read side) for vehicle state and alerts
- Observability: structured logs, metrics, distributed traces, Grafana dashboard
- Docker Compose for the full local stack; Kubernetes manifests with KEDA autoscaling
- Unit, integration and resilience tests, plus a 1-million-event load test

**Out of scope**
- UI, multi-region, real device protocols (MQTT/TCP), real authentication provider

---

## 2. Architecture

```
[Load generator] --> Kafka topic "telemetry.raw" (12 partitions, key = vehicleId)
                          |
                          v
              [Ingestion worker]  (N replicas, one consumer group)
                consume loop
                  -> per-partition bounded Channel<T>  (backpressure)
                  -> one processor per partition       (keeps order per vehicle)
                       validate -> batch -> dedupe (Redis hint + DB key) -> apply rules
                  -> persist batch to PostgreSQL in ONE transaction:
                       (INSERT ... ON CONFLICT DO NOTHING decides what is new)
                       processed_events + vehicle_state + alerts + outbox
                  -> commit Kafka offsets only after the DB commit
                          |
         invalid ---------+--> Kafka "telemetry.dlq" (with reason header)
                          |
              [Outbox publisher] --> Kafka "telemetry.alerts"
                          |
              [Read API]  GET /vehicles/{id}/state, GET /alerts   (CQRS read side)

Observability: OpenTelemetry -> Prometheus + Grafana; traces propagated via Kafka headers (traceparent)
```

Delivery guarantee: at-least-once from Kafka + idempotent processing = effectively once.

---

## 3. Tech stack

| Concern | Choice |
| --- | --- |
| Runtime | .NET 8, C# 12, Server GC |
| Kafka client | Confluent.Kafka |
| Cache / dedupe | Redis (StackExchange.Redis) |
| Database | PostgreSQL (Npgsql + Dapper) |
| Resilience | Polly v8 (ResiliencePipeline) |
| Observability | OpenTelemetry .NET, Prometheus exporter, Grafana; Serilog JSON logs |
| Tests | xUnit, FluentAssertions, Testcontainers for .NET |
| Containers | Docker (non-root), Docker Compose |
| Orchestration | Kubernetes manifests + KEDA (Kafka lag scaler) |

---

## 4. Event contract

Topic `telemetry.raw`, key = `vehicleId`, JSON value:

```json
{
  "eventId": "8b6f0c1e-2c1a-4b7e-9a51-1f2d3c4b5a6e",
  "vehicleId": "VH-000123",
  "seq": 4182,
  "timestampUtc": "2026-10-22T10:15:30.120Z",
  "lat": 40.4168,
  "lon": -3.7038,
  "speedKmh": 87.5,
  "ignition": true
}
```

- `eventId`: unique per event (GUID), used for idempotency
- `seq`: monotonically increasing per vehicle, used to detect out-of-order events
- Serialization with System.Text.Json source generators (no reflection in the hot path)

---

## 5. Business rules

**R1 — Validation (OWASP: never trust input)**
- `lat` in [-90, 90], `lon` in [-180, 180], `speedKmh` in [0, 300]
- `timestampUtc` not more than 5 minutes in the future and not older than 24 hours
- Required fields present; payload size limited (reject > 4 KB)
- Invalid → sent to `telemetry.dlq` with header `reason`; never blocks the partition

**R2 — Idempotency**
- Source of truth: `processed_events.event_id` is the primary key. Each batch is written with `INSERT ... ON CONFLICT (event_id) DO NOTHING RETURNING event_id`; events not returned are duplicates: they are skipped and counted in `telemetry_duplicates_total`. Rules R3–R6 are applied only to the events that were actually inserted, in the same transaction.
- Redis is an optimization only: after the batch transaction commits, the worker marks `dedupe:{eventId}` (`SET ... EX 86400`) for the persisted events. Before writing a batch, the worker checks the keys of its events; a hit means the event was already committed, so it is skipped (and counted as a duplicate) without touching the DB.
- A crash between the DB commit and the Redis write loses only the optimization: on replay, the DB constraint catches the duplicate. A key is never set for an event that is not committed, so no event can be lost because of Redis.
- If Redis is down or slow: skip the pre-check and the marking; the DB constraint alone keeps the result correct (degraded, still correct).
- Counting: in a run without crashes, `telemetry_duplicates_total` equals the injected duplicates. After a crash, replayed events are also counted as duplicates, so crash tests validate the number of unique processed events (rows in `processed_events`), not the counter.
**R3 — Ordering**
- Kafka key = `vehicleId` → all events of a vehicle land in the same partition
- Each partition is processed by exactly one processor, sequentially
- If `seq <= lastSeq` for that vehicle: the event is stored as history but does not change current state; increment `telemetry_out_of_order_total`

**R4 — Distance**
- Haversine distance between the previous in-order point and the current one
- Accumulate `distance_km_today` per vehicle (reset at UTC midnight)
- Ignore jumps > 5 km between consecutive events less than 10 seconds apart (GPS noise); count them in `telemetry_gps_jumps_total`

**R5 — Speeding alert (with hysteresis)**
- Raise `SPEEDING` when speed > 120 km/h for 3 consecutive in-order events
- Raise it once; clear only when speed drops below 110 km/h
- The alert is written to the `outbox` table in the same transaction as the state change

**R6 — Vehicle state**
- `vehicle_state` (upsert): last position, last speed, ignition, lastSeq, lastEventUtc, distance_km_today, speeding flag

Complexity: every rule is O(1) per event (dictionary lookup + constant math). Memory: O(V), where V is the number of active vehicles.

---

## 6. Concurrency and memory design

- **Consume loop:** a single thread per worker instance reads from Kafka and dispatches each message to the bounded `Channel<T>` of its partition (capacity 1,000, `BoundedChannelFullMode.Wait`). When processors fall behind, the loop waits: that is the backpressure.
- **Processors:** one per assigned partition, created and drained on partition assign/revoke (cooperative-sticky rebalance).
- **Vehicle state:** `ConcurrentDictionary<string, VehicleState>`. A vehicle lives in one partition, so it is only touched by one processor; no locks on the hot path.
- **Batching:** flush every 500 events or 100 ms, whichever comes first, in one DB transaction.
- **Offsets:** track the highest processed offset per partition; commit only after the batch is committed to the DB.
- **Memory / GC:** Server GC; `ArrayPool<T>` for batch buffers; no LINQ in the hot path; no allocations per event beyond the deserialized object; check with `dotnet-counters`.
- **No blocking calls:** async end to end, no `.Result` or `.Wait()`.

---

## 7. Resilience

| Dependency | Policy (Polly v8) |
| --- | --- |
| PostgreSQL batch write | Timeout 2 s → Retry 3x exponential backoff with jitter → Circuit breaker (50% failures over 30 s, min 20 calls, break 15 s) |
| Redis dedupe | Timeout 200 ms; on failure fall back to the DB constraint |
| Alert publishing (outbox publisher) | Bulkhead: concurrency limiter of 4, queue of 100, isolated from ingestion |

- **Poison messages:** deserialization or validation errors go to the DLQ, never crash the worker.
- **Graceful shutdown (SIGTERM):** stop consuming → drain channels → flush the last batch → commit offsets → close the consumer. Target: under 20 seconds.
- **Crash safety:** kill the worker at any point; on restart it re-reads from the last committed offset, and idempotency guarantees no double counting.

---

## 8. Data model (PostgreSQL)

```sql
CREATE TABLE processed_events (
  event_id     uuid PRIMARY KEY,
  vehicle_id   text        NOT NULL,
  seq          bigint      NOT NULL,
  ts_utc       timestamptz NOT NULL,
  lat          double precision,
  lon          double precision,
  speed_kmh    real,
  out_of_order boolean     NOT NULL DEFAULT false
);

CREATE TABLE vehicle_state (
  vehicle_id        text PRIMARY KEY,
  last_seq          bigint,
  last_event_utc    timestamptz,
  lat               double precision,
  lon               double precision,
  speed_kmh         real,
  ignition          boolean,
  distance_km_today double precision,
  speeding          boolean,
  updated_utc       timestamptz NOT NULL
);

CREATE TABLE alerts (
  alert_id   uuid PRIMARY KEY,
  vehicle_id text        NOT NULL,
  type       text        NOT NULL,
  raised_utc timestamptz NOT NULL,
  payload    jsonb       NOT NULL
);

CREATE TABLE outbox (
  id           bigserial PRIMARY KEY,
  topic        text        NOT NULL,
  key          text        NOT NULL,
  payload      jsonb       NOT NULL,
  created_utc  timestamptz NOT NULL DEFAULT now(),
  published_utc timestamptz
);
```

The worker connects with a least-privilege DB user (INSERT/UPDATE on these tables only).

---

## 9. Read API (CQRS read side)

- `GET /vehicles/{vehicleId}/state` → current state
- `GET /alerts?vehicleId=&from=&to=` → paged, max 100 per page
- `GET /health/live`, `GET /health/ready`
- Security (OWASP API Security): API key header required, rate limiting middleware, input validation on query parameters, no internal errors exposed in responses

---

## 10. Observability

**Metrics (Prometheus)**
- `telemetry_events_consumed_total`
- `telemetry_events_processed_total`
- `telemetry_duplicates_total`
- `telemetry_out_of_order_total`
- `telemetry_dlq_total{reason}`
- `telemetry_batch_duration_seconds` (histogram)
- `telemetry_processing_latency_seconds` (event timestamp → persisted, histogram)
- `telemetry_channel_depth{partition}`
- `telemetry_alerts_raised_total{type}`
- Consumer lag (from Kafka exporter or computed)

**Traces:** a span per batch and per DB write; `traceparent` propagated from the generator through Kafka headers.

**Logs:** Serilog JSON with `vehicleId`, `partition`, `offset`, `eventId`.

**Grafana dashboard:** events/s, p95 latency, lag, duplicates, DLQ, channel depth, circuit breaker state.

---

## 11. Load generator

- Simulates N vehicles (default 1,000) moving on random routes around Madrid
- Configurable rate and total events (default 1,000,000)
- Fault injection flags: `--duplicates 2%`, `--out-of-order 1%`, `--invalid 0.5%`, `--speeding-vehicles 5%`
- Prints a summary at the end: sent, duplicates injected, invalid injected

---

## 12. Tests

**Unit**
- Haversine distance and GPS jump filter
- Speeding hysteresis (raise once, clear below 110)
- Validation rules and DLQ reasons
- Out-of-order detection

**Integration (Testcontainers: Kafka, Redis, Postgres)**
- Publishing the same event twice results in one processed row and one state change
- Out-of-order events are stored but do not change current state
- Invalid events end up in the DLQ

**Resilience**
- Kill the worker mid-run, restart: final counts equal the expected unique events
- Stop Postgres for 20 s: circuit breaker opens, consumption pauses (lag grows), no data lost after recovery
- Stop Redis: processing continues using the DB constraint

**Load**
- 1,000,000 events with 2% duplicates: `processed = unique events`, `duplicates_total = injected duplicates`, zero lost

---

## 13. Containers and Kubernetes

- Dockerfile: multi-stage build, `mcr.microsoft.com/dotnet/runtime` base, `USER app` (non-root), read-only file system where possible
- Docker Compose: Kafka (KRaft), Redis, Postgres, Prometheus, Grafana, worker, read API, generator
- Kubernetes:
  - Deployment with readiness and liveness probes, `preStop` hook, `terminationGracePeriodSeconds: 30`
  - `securityContext`: `runAsNonRoot: true`, `allowPrivilegeEscalation: false`
  - Secrets from Kubernetes Secrets, never in the image or the code
  - KEDA `ScaledObject` on Kafka consumer lag; max replicas = number of partitions (12)
  - Rolling update `maxUnavailable: 0` for zero-downtime deploys

---

## 14. Repository structure

```
telemetry-core/
  src/
    TelemetryCore.Contracts/      # event contract, JSON source-gen context
    TelemetryCore.Domain/         # rules: validation, distance, speeding, state
    TelemetryCore.Worker/         # consumer loop, channels, processors, persistence, outbox publisher
    TelemetryCore.ReadApi/        # minimal API (CQRS read side)
    TelemetryCore.LoadGenerator/  # fleet simulator
  tests/
    TelemetryCore.Domain.Tests/
    TelemetryCore.IntegrationTests/
  deploy/
    docker-compose.yml
    prometheus/ grafana/
    k8s/  (deployment, service, keda-scaledobject, secrets example)
  db/
    001_init.sql
  README.md
  SPEC.md
```

---

## 15. Build order

1. Contracts + Domain with unit tests (rules R1–R6)
2. Docker Compose with Kafka, Redis, Postgres
3. Load generator
4. Worker: consume loop → channels → processors → batch persist → offset commit
5. Idempotency (Redis + DB constraint) and DLQ
6. Polly policies and graceful shutdown
7. Outbox table + publisher
8. OpenTelemetry metrics/traces + Grafana dashboard
9. Read API
10. Integration and resilience tests, then the 1M load test
11. Dockerfiles (non-root) and Kubernetes manifests with KEDA

---

## 16. Acceptance criteria

- 1,000,000 events processed with zero loss and zero double counting
- Killing the worker at any point does not change the final result
- Database outage: no data lost, processing resumes automatically
- Per-vehicle order respected for current state
- Invalid events in the DLQ with a reason
- Grafana shows throughput, p95 latency, lag, duplicates and DLQ
- Container runs as non-root; no secrets in code or image
