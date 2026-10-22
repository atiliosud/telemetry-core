#!/usr/bin/env bash
# Chaos scenarios against the running compose stack (SPEC section 16). Requires scripts/up.sh first.
#   scripts/chaos.sh [kill|postgres|redis|rebalance|all] [total=100000] [rate=2000]
# Same flow as chaos.ps1: generator in the background (fresh --vehicle-prefix), fault mid-run, wait for
# baseline + expectedUnique rows (D1 counting rule), PASS/FAIL; breaker state + lag from Prometheus.
# Output is saved to artifacts/chaos-<scenario>-<timestamp>.txt.
set -u
SCENARIO="${1:-all}"
TOTAL="${2:-100000}"
RATE="${3:-2000}" # throttled so the load is still flowing while the fault is active
TIMEOUT_MIN="${TIMEOUT_MIN:-10}"
PROM="${PROMETHEUS_URL:-http://localhost:9090}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$ROOT/.env"; [ -f "$ENV_FILE" ] || ENV_FILE="$ROOT/.env.example"
STAMP="$(date +%Y%m%d-%H%M%S)"
mkdir -p "$ROOT/artifacts"
OUT="$ROOT/artifacts/chaos-$SCENARIO-$STAMP.txt"
case "$SCENARIO" in kill|postgres|redis|rebalance|all) ;; *) echo "usage: $0 kill|postgres|redis|rebalance|all [total]"; exit 2 ;; esac

dc() { docker compose -p telemetry-core --env-file "$ENV_FILE" -f "$ROOT/deploy/docker-compose.yml" "$@"; }
say() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$OUT"; }

db_count() {
  local n i
  for i in $(seq 1 30); do
    n="$(dc exec -T postgres psql -U postgres -d telemetry -tAc 'select count(*) from processed_events' 2>/dev/null | tr -d '[:space:]')"
    if [[ "$n" =~ ^[0-9]+$ ]]; then echo "$n"; return 0; fi
    sleep 2
  done
  echo "ERROR"; return 1
}

prom() {
  curl -fsG "$PROM/api/v1/query" --data-urlencode "query=$1" 2>/dev/null \
    | grep -o '"value":\[[^]]*\]' | sed -E 's/.*,"([^"]*)"\]/\1/' | paste -sd, - || true
}

health() {
  local b l
  b="$(prom 'max(telemetry_circuit_breaker_state)')"; l="$(prom 'sum(kafka_consumergroup_lag{consumergroup="telemetry-worker"})')"
  say "$1 : breaker state=${b:-n/a} (0 closed, 1 open, 2 half-open), consumer lag=${l:-n/a}"
}

run_scenario() {
  local name="$1" prefix base gen_out gen_pid summary expected final delta deadline extra=1 note="" v
  say "=== Scenario: $name (total $TOTAL) ==="
  prefix="C$(echo "${name:0:1}" | tr a-z A-Z)${STAMP//-/}-"
  base="$(db_count)" || return 1
  say "Baseline rows: $base; vehicle prefix $prefix"
  health before
  gen_out="$(mktemp)"
  dc --profile tools run --rm -T generator --total "$TOTAL" --seed "$(( (RANDOM << 15 | RANDOM) + 1 ))" --vehicle-prefix "$prefix" --rate "$RATE" >"$gen_out" 2>&1 &
  gen_pid=$!
  sleep 15
  case "$name" in
    kill)
      say "docker kill --signal KILL telemetry-core-worker-1"; docker kill --signal KILL telemetry-core-worker-1 >/dev/null 2>&1
      sleep 5; health during
      dc up -d worker >/dev/null 2>&1; say "compose up -d worker -> exit $?" ;;
    postgres)
      dc stop postgres >/dev/null 2>&1; say "compose stop postgres -> exit $?"
      sleep 10; health during; sleep 10; health "during (20 s)"
      dc start postgres >/dev/null 2>&1; say "compose start postgres -> exit $?"
      ;;
    redis)
      dc stop redis >/dev/null 2>&1; say "compose stop redis -> exit $? (stays down for the rest of the run)"; health during ;;
    rebalance)
      dc -f "$ROOT/deploy/docker-compose.scale.yml" up -d --no-recreate --scale worker=2 worker >/dev/null 2>&1; say "scale worker=2 -> exit $?"
      sleep 20; health "during (2 workers)"
      v="$(docker ps --filter label=com.docker.compose.project=telemetry-core --filter label=com.docker.compose.service=worker -q | wc -l | tr -d ' ')"
      [ "$v" = 2 ] || extra=0; note="; workers running during rebalance: $v"
      dc up -d --no-recreate --scale worker=1 worker >/dev/null 2>&1; say "scale worker=1 -> exit $?" ;;
  esac
  wait "$gen_pid"
  summary="$(grep '^SUMMARY ' "$gen_out" | tail -1)"
  if [ -z "$summary" ]; then say "Generator failed:"; tail -15 "$gen_out" | tee -a "$OUT"; rm -f "$gen_out"; return 1; fi
  rm -f "$gen_out"
  say "$summary"
  expected="$(echo "$summary" | grep -o '"expectedUnique":[0-9]*' | cut -d: -f2)"
  deadline=$(( $(date +%s) + TIMEOUT_MIN * 60 ))
  while :; do
    final="$(db_count)"
    [[ "$final" =~ ^[0-9]+$ ]] && [ $((final - base)) -ge "$expected" ] && break
    [ "$(date +%s)" -ge "$deadline" ] && break
    sleep 3
  done
  sleep 5; final="$(db_count)"
  if [ "$name" = postgres ]; then
    v="$(prom 'max(max_over_time(telemetry_circuit_breaker_state[5m]))')"
    awk -v x="${v:-0}" 'BEGIN{exit !(x>=1)}' || extra=0; note="; breaker max state during the run: ${v:-n/a}"
  fi
  if [ "$name" = redis ]; then
    v="$(prom 'sum(increase(telemetry_redis_fallback_total[5m]))')"
    awk -v x="${v:-0}" 'BEGIN{exit !(x>0)}' || extra=0; note="; Redis fallbacks while down: ${v:-n/a}"
    dc start redis >/dev/null 2>&1; say "compose start redis -> exit $?"
  fi
  health after
  delta=$((final - base))
  if [ "$delta" -eq "$expected" ] && [ "$extra" = 1 ]; then say "RESULT $name: PASS  unique rows delta $delta / expected $expected$note"; return 0; fi
  say "RESULT $name: FAIL  unique rows delta $delta / expected $expected$note"; return 1
}

rc=0
if [ "$SCENARIO" = all ]; then list="kill postgres redis rebalance"; else list="$SCENARIO"; fi
for s in $list; do run_scenario "$s" || rc=1; done
echo "Saved to $OUT"
exit $rc
