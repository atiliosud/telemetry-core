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

# Snapshot: rows consumed duplicates outOfOrder dlq
snapshot() {
  local text rows; text="$(curl -fsS "$METRICS_URL")" || return 1
  rows="$(db_count)" || return 1
  echo "$rows $(metric_sum telemetry_events_consumed_total "$text") $(metric_sum telemetry_duplicates_total "$text") $(metric_sum telemetry_out_of_order_total "$text") $(metric_sum telemetry_dlq_total "$text")"
}

# wait_stable <stable-seconds> <min-rows or empty>: prints "<snapshot>|<seconds since START when min-rows was reached>".
wait_stable() {
  local stable="$1" min_rows="${2:-}" deadline=$(( $(date +%s) + TIMEOUT_MIN * 60 ))
  local last now since reached=""
  last="$(snapshot)"; since=$(date +%s)
  while [ "$(date +%s)" -lt "$deadline" ]; do
    sleep 0.5
    now="$(snapshot)"
    if [ "$now" != "$last" ]; then last="$now"; since=$(date +%s); fi
    if [ -z "$reached" ]; then
      if [ -z "$min_rows" ] || [ "${now%% *}" -ge "$min_rows" ]; then reached=$(( $(date +%s%N) / 1000000 )); fi
    fi
    if [ -n "$reached" ] && [ $(( $(date +%s) - since )) -ge "$stable" ]; then echo "$now|$reached"; return 0; fi
  done
  echo "$(snapshot)|${reached:-}"; return 1
}

say "Telemetry Core demo  $(date '+%Y-%m-%d %H:%M:%S')"
say "Generator: --total $TOTAL --vehicles $VEHICLES --duplicates $DUPLICATES --out-of-order $OUT_OF_ORDER --invalid $INVALID --speeding-vehicles $SPEEDING --seed $SEED --vehicle-prefix $PREFIX"
curl -fsS "$METRICS_URL" >/dev/null || { echo "Worker metrics not reachable at $METRICS_URL. Run scripts/up.sh first."; exit 1; }

echo "Waiting for the worker to be idle (no change for 5 s) before taking the baseline..."
IDLE="$(wait_stable 5)" || { echo "Worker did not become idle; aborting."; exit 1; }
read -r B_ROWS B_CONSUMED B_DUP B_OOO B_DLQ <<<"${IDLE%%|*}"
say "Baseline: rows=$B_ROWS duplicates=$B_DUP outOfOrder=$B_OOO dlq=$B_DLQ"

echo "Running the load generator..."
START_MS=$(( $(date +%s%N) / 1000000 ))
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
read -r F_ROWS F_CONSUMED F_DUP F_OOO F_DLQ <<<"${DONE%%|*}"
REACHED="${DONE##*|}"; [ -n "$REACHED" ] || REACHED=$(( $(date +%s%N) / 1000000 ))
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

RESULT=FAIL
if [ "$TIMED_OUT" -eq 0 ] && [ "$PROCESSED" -eq "$EXPECTED" ] && [ "$DUPS" -eq "$DUP_INJ" ] && [ "$OOO" -eq "$OOO_INJ" ] && [ "$DLQ" -eq "$INV_INJ" ]; then
  RESULT=PASS
fi
[ "$TIMED_OUT" -eq 1 ] && say "Timed out after $TIMEOUT_MIN min."
say "RESULT: $RESULT  (processed $PROCESSED/$EXPECTED, duplicates $DUPS/$DUP_INJ, out-of-order $OOO/$OOO_INJ, dlq $DLQ/$INV_INJ)"

mkdir -p "$ROOT/artifacts"
FILE="$ROOT/artifacts/demo-$STAMP.txt"
printf '%s' "$REPORT" > "$FILE"
echo "Saved to $FILE"
if [ -n "$COPY_TO" ]; then mkdir -p "$(dirname "$COPY_TO")"; cp "$FILE" "$COPY_TO"; echo "Copied to $COPY_TO"; fi
[ "$RESULT" = PASS ]
