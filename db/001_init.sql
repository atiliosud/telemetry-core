-- Schema from SPEC section 8, plus vehicle_state.overspeed_streak and distance_date (D4).
-- Runs once at first initdb, connected to database "telemetry" as the superuser.

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
  updated_utc       timestamptz NOT NULL,
  overspeed_streak  integer NOT NULL DEFAULT 0,
  distance_date     date
);

CREATE TABLE alerts (
  alert_id   uuid PRIMARY KEY,
  vehicle_id text        NOT NULL,
  type       text        NOT NULL,
  raised_utc timestamptz NOT NULL,
  payload    jsonb       NOT NULL
);

CREATE TABLE outbox (
  id            bigserial PRIMARY KEY,
  topic         text        NOT NULL,
  key           text        NOT NULL,
  payload       jsonb       NOT NULL,
  created_utc   timestamptz NOT NULL DEFAULT now(),
  published_utc timestamptz
);

CREATE INDEX ix_alerts_vehicle_raised ON alerts (vehicle_id, raised_utc DESC);
CREATE INDEX ix_outbox_unpublished ON outbox (id) WHERE published_utc IS NULL;

-- Least privilege (D13). Roles are created by 000_roles.sh, which runs before this file.
-- Grants are explicit and placed after the tables exist (no ALTER DEFAULT PRIVILEGES needed:
-- the schema is created once, here). No DELETE, TRUNCATE or DDL for either role.
GRANT SELECT, INSERT, UPDATE ON processed_events, vehicle_state, alerts, outbox TO telemetry_worker;
GRANT USAGE ON SEQUENCE outbox_id_seq TO telemetry_worker;
GRANT SELECT ON vehicle_state, alerts TO telemetry_reader;
