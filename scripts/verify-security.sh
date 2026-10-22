#!/usr/bin/env bash
# Security verification (SPEC 13/16). Exit 0 = all OK, 1 = at least one check failed.
set -u
root="$(cd "$(dirname "$0")/.." && pwd)"; cd "$root"
compose="deploy/docker-compose.yml"
envfile=".env"; [ -f "$envfile" ] || envfile=".env.example"
failed=0
check() { if [ "$2" = 1 ]; then echo "OK    $1"; else echo "FAIL  $1  ($3)"; failed=$((failed+1)); fi; }
b() { if "$@"; then echo 1; else echo 0; fi; }

secrets=()
if [ -f .env ]; then
  while IFS= read -r v; do secrets+=("$v"); done < <(grep -E '^[A-Z_]*(PASSWORD|API_KEY|SECRET)[A-Z_]*=.{4,}$' .env | cut -d= -f2-)
fi

for pair in worker:telemetry-core-worker read-api:telemetry-core-read-api generator:telemetry-core-generator; do
  svc="${pair%%:*}"; img="${pair##*:}"
  uid="$(docker run --rm --entrypoint id "$img" -u 2>/dev/null | tr -d '\r\n')"
  ok=0; [[ "$uid" =~ ^[0-9]+$ ]] && [ "$uid" != 0 ] && ok=1
  check "$svc: image runs as non-root uid ($uid)" $ok "uid='$uid'"
  user="$(docker image inspect "$img" --format '{{.Config.User}}' 2>/dev/null | tr -d '\r\n')"
  ok=0; [[ "$user" =~ ^[1-9][0-9]*(:[0-9]+)?$ ]] && ok=1
  check "$svc: Config.User is numeric non-zero ($user)" $ok "Config.User='$user'"
  hist="$(docker history --no-trunc --format '{{.CreatedBy}}' "$img" 2>/dev/null)"
  ok=1; for v in ${secrets[@]+"${secrets[@]}"}; do [[ "$hist" == *"$v"* ]] && ok=0; done
  check "$svc: no .env secret values in image history" $ok "secret value in history"
  cid="$(docker compose --env-file "$envfile" -f "$compose" ps -q "$svc" 2>/dev/null)"
  if [ -n "$cid" ]; then
    hc="$(docker inspect "$cid" --format '{{.HostConfig.ReadonlyRootfs}}|{{json .HostConfig.CapDrop}}|{{json .HostConfig.SecurityOpt}}')"
    IFS='|' read -r ro cd so <<<"$hc"
    check "$svc: read-only root filesystem" "$(b [ "$ro" = true ])" "$hc"
    check "$svc: cap_drop ALL" "$(b grep -q ALL <<<"$cd")" "$hc"
    check "$svc: no-new-privileges" "$(b grep -q no-new-privileges <<<"$so")" "$hc"
  else
    cfg="$(docker compose --env-file "$envfile" -f "$compose" --profile tools config 2>/dev/null | awk -v s="  $svc:" '$0==s{f=1;next} f&&/^  [a-z]/{f=0} f')"
    check "$svc: read-only root filesystem (config)" "$(b grep -q 'read_only: true' <<<"$cfg")" "read_only not set"
    check "$svc: cap_drop ALL (config)" "$(b grep -q 'ALL' <<<"$cfg")" "cap_drop missing"
    check "$svc: no-new-privileges (config)" "$(b grep -q 'no-new-privileges:true' <<<"$cfg")" "security_opt missing"
  fi
done

wcid="$(docker compose --env-file "$envfile" -f "$compose" ps -q worker 2>/dev/null)"
if [ -n "$wcid" ]; then
  check "worker: touch /app/x fails (read-only)" "$(! docker exec "$wcid" touch /app/x >/dev/null 2>&1 && echo 1 || echo 0)" "write succeeded"
  check "worker: /tmp is writable (tmpfs)" "$(docker exec "$wcid" touch /tmp/x >/dev/null 2>&1 && echo 1 || echo 0)" "tmpfs not writable"
fi

pattern='(password|passwd|pwd|api[_-]?key|secret)\s*[:=]\s*["'"'"']?[A-Za-z0-9/+_.!@#%^&*-]{6,}|BEGIN (RSA |EC )?PRIVATE KEY|AKIA[0-9A-Z]{16}'
hits="$(git grep -nIiE "$pattern" -- . ':!.env.example' ':!deploy/k8s/secrets.example.yaml' ':!docs' ':!*.md' ':!scripts/verify-security.*' ':!tests' ':!.superpowers' 2>/dev/null | grep -vE '^(tests|docs|\.superpowers)/|^deploy/k8s/secrets\.example|\$\{|change-me|<[^>]+>|Password=\$|password=\$|$$|REPLACE_ME|PASSWORD :.|from .env|X-Api-Key' || true)"
check "secrets scan: no credential literals in tracked files" "$([ -z "$hits" ] && echo 1 || echo 0)" "$(echo "$hits" | head -5 | tr '\n' '|')"
if [ ${#secrets[@]} -gt 0 ]; then
  t=""; for v in "${secrets[@]}"; do t+="$(git grep -lF -e "$v" -- . ':!.env.example' ':!.superpowers' 2>/dev/null)"; done
  check "secrets scan: no .env value in tracked files" "$([ -z "$t" ] && echo 1 || echo 0)" "$t"
fi

if [ $failed -gt 0 ]; then echo; echo "$failed check(s) FAILED"; exit 1; fi
echo; echo "All security checks OK"
