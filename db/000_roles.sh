#!/bin/sh
# Runs once, inside the postgres image's initdb (sourced by docker-entrypoint.sh, so the exec bit is not needed).
# Creates the least-privilege roles (D13). Passwords come from the container env, never from SQL files.
# Table grants live in 001_init.sql, which runs right after this script.
set -e

: "${TELEMETRY_WORKER_DB_PASSWORD:?TELEMETRY_WORKER_DB_PASSWORD is required}"
: "${TELEMETRY_READER_DB_PASSWORD:?TELEMETRY_READER_DB_PASSWORD is required}"

psql -v ON_ERROR_STOP=1 \
  -v worker_pw="$TELEMETRY_WORKER_DB_PASSWORD" \
  -v reader_pw="$TELEMETRY_READER_DB_PASSWORD" \
  --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
CREATE ROLE telemetry_worker LOGIN PASSWORD :'worker_pw';
CREATE ROLE telemetry_reader LOGIN PASSWORD :'reader_pw';
SQL
