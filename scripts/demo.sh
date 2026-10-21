#!/usr/bin/env bash
# End-to-end demo (CHECKPOINT 1): runs the load generator against the running stack and checks the numbers.
#   scripts/up.sh                     # once
#   scripts/demo.sh --total 100000    # PASS / FAIL, exit code 0 / 1
# Baseline and final values come from Postgres (rows in processed_events) and from the worker's /metrics endpoint,
# so earlier runs do not matter: only deltas are compared. Each run uses fresh vehicle ids (--vehicle-prefix) and a
# fresh seed, so it never collides with stored vehicle state or event ids of earlier runs.
# Output is saved to artifacts/demo-<timestamp>.txt. Needs: docker, curl, awk.
set -uo pipefail

TOTAL=100000; DUPLICATES=2; OUT_OF_ORDER=1; INVALID=0.5; SPEEDING=5; VEHICLES=1000; TIMEOUT_MIN=10; SEED=0
METRICS_URL="http://localhost:8081/metrics"; COPY_TO=""
while [ $# -gt 0 ]; do
  case "$1" in
    --total) TOTAL="$2"; shift 2 ;;
    --duplicates) DUPLICATES="$2"; shift 2 ;;
    --out-of-order) OUT_OF_ORDER="$2"; shift 2 ;;
    --invalid) INVALID="$2"; shift 2 ;;
    --speeding-vehicles) SPEEDING="$2"; shift 2 ;;
    --vehicles) VEHICLES="$2"; shift 2 ;;
    --timeout-minutes) TIMEOUT_MIN="$2"; shift 2 ;;
    --seed) SEED="$2"; shift 2 ;;
    --metrics-url) METRICS_URL="$2"; shift 2 ;;
    --copy-to) COPY_TO="$2"; shift 2 ;;
    *) echo "Unknown option $1"; exit 2 ;;
  esac
done

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$ROOT/.env"; [ -f "$ENV_FILE" ] || ENV_FILE="$ROOT/.env.example"
COMPOSE=(docker compose --env-file "$ENV_FILE" -f "$ROOT/deploy/docker-compose.yml")

STAMP="$(date +%Y%m%d-%H%M%S)"
[ "$SEED" = "0" ] && SEED=$(( (RANDOM << 15 | RANDOM) + 1 ))
PREFIX="D${STAMP//-/}-"
REPORT=""
say() { echo "$1"; REPORT+="$1"$'\n'; }

# Milliseconds since the epoch, portable (macOS date has no %N): perl, then python3, then whole seconds.
now_ms() {
  if command -v perl >/dev/null 2>&1; then perl -MTime::HiRes=time -e 'printf "%d", time * 1000'
  elif command -v python3 >/dev/null 2>&1; then python3 -c 'import time; print(int(time.time() * 1000))'
  else echo $(( $(date +%s) * 1000 )); fi
}

# The report is saved on every exit path (PASS, FAIL, generator failure, timeout, errors).
save_report() {
  local code=$?
  mkdir -p "$ROOT/artifacts"
  local file="$ROOT/artifacts/demo-$STAMP.txt"
  printf '%s' "$REPORT" > "$file"
  echo "Saved to $file"
  if [ -n "$COPY_TO" ]; then mkdir -p "$(dirname "$COPY_TO")"; cp "$file" "$COPY_TO"; echo "Copied to $COPY_TO"; fi
  exit "$code"
}
trap save_report EXIT

db_count() {
  local n
  n="$("${COMPOSE[@]}" exec -T postgres psql -U postgres -d telemetry -tAc "select count(*) from processed_events" 2>/dev/null | tr -d '[:space:]')"
  case "$n" in ''|*[!0-9]*) echo "Could not read processed_events count" >&2; return 1 ;; esac
  echo "$n"
}

# Sum of every series of a metric (all label sets) in the Prometheus text format; 0 when absent.
metric_sum() {
  awk -v n="$1" '$0 !~ /^#/ { name=$1; sub(/\{.*/, "", name); if (name == n) { v=$NF; if ($0 ~ /\} /) { split($0, a, "} "); split(a[2], b, " "); v=b[1] } else { v=$2 }; s += v } } END { printf "%d", s }' <<<"$2"
}

# Snapshot: rows consumed duplicates outOfOrder dlq circuit
snapshot() {
  local text rows; text="$(curl -fsS "$METRICS_URL")" || return 1
  rows="$(db_count)" || return 1
  echo "$rows $(metric_sum telemetry_events_consumed_total "$text") $(metric_sum telemetry_duplicates_total "$text") $(metric_sum telemetry_out_of_order_total "$text") $(metric_sum telemetry_dlq_total "$text") $(metric_sum telemetry_circuit_breaker_state "$text")"
}

# Worker counters only grow; a decrease means the worker process restarted (in-memory counters reset).
RESTART=0
decreased() { # decreased "<before snapshot>" "<after snapshot>"
  local b a; read -r -a b <<<"$1"; read -r -a a <<<"$2"
  [ "${a[1]}" -lt "${b[1]}" ] || [ "${a[2]}" -lt "${b[2]}" ] || [ "${a[3]}" -lt "${b[3]}" ] || [ "${a[4]}" -lt "${b[4]}" ]
}

# wait_stable <stable-seconds> <min-rows or empty>: prints "<snapshot>|<seconds since START when min-rows was reached>".
wait_stable() {
  local stable="$1" min_rows="${2:-}" deadline=$(( $(date +%s) + TIMEOUT_MIN * 60 ))
  local last now since reached=""
  last="$(snapshot)"; since=$(date +%s)
  while [ "$(date +%s)" -lt "$deadline" ]; do
    sleep 0.5
    now="$(snapshot)" || return 1
    if decreased "$last" "$now"; then echo 1 > "$RESTART_FLAG"; fi
    if [ "$now" != "$last" ]; then last="$now"; since=$(date +%s); fi
    if [ -z "$reached" ]; then
      if [ -z "$min_rows" ] || [ "${now%% *}" -ge "$min_rows" ]; then reached=$(now_ms); fi
    fi
    if [ -n "$reached" ] && [ $(( $(date +%s) - since )) -ge "$stable" ]; then echo "$now|$reached"; return 0; fi
  done
  echo "$(snapshot)|${reached:-}"; return 1
}

RESTART_FLAG="$(mktemp)"; echo 0 > "$RESTART_FLAG"

say "Telemetry Core demo  $(date '+%Y-%m-%d %H:%M:%S')"
say "Generator: --total $TOTAL --vehicles $VEHICLES --duplicates $DUPLICATES --out-of-order $OUT_OF_ORDER --invalid $INVALID --speeding-vehicles $SPEEDING --seed $SEED --vehicle-prefix $PREFIX"
curl -fsS "$METRICS_URL" >/dev/null || { say "Worker metrics not reachable at $METRICS_URL. Run scripts/up.sh first."; exit 1; }

echo "Waiting for the worker to be idle (no change for 5 s) before taking the baseline..."
IDLE="$(wait_stable 5)" || { say "Worker did not become idle within $TIMEOUT_MIN min (or a snapshot failed); aborting."; exit 1; }
BASE="${IDLE%%|*}"
read -r B_ROWS B_CONSUMED B_DUP B_OOO B_DLQ B_CIRCUIT <<<"$BASE"
echo 0 > "$RESTART_FLAG"
say "Baseline: rows=$B_ROWS duplicates=$B_DUP outOfOrder=$B_OOO dlq=$B_DLQ"
[ "$B_CIRCUIT" = "1" ] && say "WARNING: the DB circuit breaker is OPEN at baseline (telemetry_circuit_breaker_state = 1); processing may stall."

echo "Running the load generator..."
START_MS=$(now_ms)
GEN_OUT="$("${COMPOSE[@]}" --profile tools run --rm generator --total "$TOTAL" --vehicles "$VEHICLES" --duplicates "$DUPLICATES" \
  --out-of-order "$OUT_OF_ORDER" --invalid "$INVALID" --speeding-vehicles "$SPEEDING" --seed "$SEED" --vehicle-prefix "$PREFIX" 2>&1)"
GEN_EXIT=$?
grep -E '^(\||\+)' <<<"$GEN_OUT"
SUMMARY="$(grep '^SUMMARY ' <<<"$GEN_OUT" | tail -n 1)"
if [ "$GEN_EXIT" -ne 0 ] || [ -z "$SUMMARY" ]; then say "Generator failed:"; say "$(tail -n 20 <<<"$GEN_OUT")"; exit 1; fi
json() { sed -E "s/.*\"$1\":([0-9.]+).*/\1/" <<<"$SUMMARY"; }
SENT=$(json sent); DUP_INJ=$(json duplicatesInjected); INV_INJ=$(json invalidInjected); OOO_INJ=$(json outOfOrderInjected)
EXPECTED=$(json expectedUnique); GEN_SECONDS=$(json seconds); GEN_EPS=$(json eventsPerSecond)

echo "Waiting for $EXPECTED new unique rows (timeout $TIMEOUT_MIN min)..."
TIMED_OUT=0
DONE="$(wait_stable 3 $(( B_ROWS + EXPECTED )))" || TIMED_OUT=1
FINAL="${DONE%%|*}"
read -r F_ROWS F_CONSUMED F_DUP F_OOO F_DLQ F_CIRCUIT <<<"$FINAL"
REACHED="${DONE##*|}"; [ -n "$REACHED" ] || REACHED=$(now_ms)
ELAPSED_MS=$(( REACHED - START_MS ))

PROCESSED=$(( F_ROWS - B_ROWS )); DUPS=$(( F_DUP - B_DUP )); OOO=$(( F_OOO - B_OOO )); DLQ=$(( F_DLQ - B_DLQ ))
EPS=$(awk -v s="$SENT" -v ms="$ELAPSED_MS" 'BEGIN { if (ms > 0) printf "%d", s * 1000 / ms; else print 0 }')
row() { local ok=""; if [ -n "$3" ]; then if [ "$2" = "$3" ]; then ok="OK"; else ok="MISMATCH"; fi; fi
        printf '| %-28s | %12s | %12s | %-8s |' "$1" "$2" "$3" "$ok"; }
SEP='+------------------------------+--------------+--------------+----------+'
say ""
say "$SEP"
say "$(printf '| %-28s | %12s | %12s | %-8s |' 'Check' 'Expected' 'Counted' '')"
say "$SEP"
say "$(row 'Sent' "$SENT" '')"
say "$(row 'Unique -> processed (rows)' "$EXPECTED" "$PROCESSED")"
say "$(row 'Duplicates' "$DUP_INJ" "$DUPS")"
say "$(row 'Out-of-order' "$OOO_INJ" "$OOO")"
say "$(row 'Invalid -> DLQ' "$INV_INJ" "$DLQ")"
say "$SEP"
say "$(printf '| %-28s | %12.1f |' 'Generator seconds' "$GEN_SECONDS")"
say "$(printf '| %-28s | %12.0f |' 'Generator events/s' "$GEN_EPS")"
say "$(printf '| %-28s | %12s |' 'End-to-end s (incl. startup)' "$(awk -v ms="$ELAPSED_MS" 'BEGIN { printf "%.1f", ms / 1000 }')")"
say "$(printf '| %-28s | %12s |' 'End-to-end events/s' "$EPS")"
say "$SEP"

if [ "$(cat "$RESTART_FLAG")" = "1" ] || decreased "$BASE" "$FINAL"; then
  RESTART=1
  say "WORKER RESTART DETECTED: a metrics counter went down during the run. The worker keeps its counters in memory,"
  say "so a restart resets them and the duplicate / out-of-order / DLQ deltas are not comparable (replayed batches also"
  say "count as duplicates after a restart, D1). Check 'docker compose ps' / worker logs and run the demo again."
fi
rm -f "$RESTART_FLAG"

RESULT=FAIL
if [ "$RESTART" -eq 0 ] && [ "$TIMED_OUT" -eq 0 ] && [ "$PROCESSED" -eq "$EXPECTED" ] && [ "$DUPS" -eq "$DUP_INJ" ] && [ "$OOO" -eq "$OOO_INJ" ] && [ "$DLQ" -eq "$INV_INJ" ]; then
  RESULT=PASS
fi
[ "$TIMED_OUT" -eq 1 ] && say "Timed out after $TIMEOUT_MIN min."
say "RESULT: $RESULT  (processed $PROCESSED/$EXPECTED, duplicates $DUPS/$DUP_INJ, out-of-order $OOO/$OOO_INJ, dlq $DLQ/$INV_INJ)"

[ "$RESULT" = PASS ] # exit status; the EXIT trap saves the report
