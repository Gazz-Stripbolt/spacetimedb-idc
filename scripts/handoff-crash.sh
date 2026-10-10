#!/usr/bin/env bash
# Crash test: kill -9 the whole server in the middle of a burst of handoffs, restart it from
# disk, and check that every character is live on exactly one shard afterwards.
#
#   scripts/handoff-crash.sh [rounds] [characters]
#
# Starts its own on-disk dev server on PORT (default 3100), so it doesn't disturb the
# in-memory one the other tests use. Shard languages come from A_LANG / B_LANG.
set -uo pipefail
cd "$(dirname "$0")/.."
ROUNDS=${1:-5}
N=${2:-30}
PORT=${PORT:-3100}
export HOST=http://127.0.0.1:$PORT SERVER=http://127.0.0.1:$PORT
LOG=$(mktemp)
server=""

start() {
  PERSIST=1 LISTEN=127.0.0.1:$PORT scripts/dev-server.sh start >>"$LOG" 2>&1 &
  server=$!
  for _ in $(seq 120); do curl -sf -o /dev/null "$HOST/v1/ping" && return 0; sleep 0.25; done
  echo "server didn't come up"; tail -20 "$LOG"; exit 1
}
crash() { kill -9 "$server" 2>/dev/null; wait "$server" 2>/dev/null; }
trap crash EXIT

rm -rf ".dev-server/data-$PORT"
start
scripts/deploy-shards.sh >/dev/null 2>&1 || { echo "deploy failed"; exit 1; }

call() { curl -s -o /dev/null -w '%{http_code}' -X POST -H 'content-type: application/json' -d "$3" "$HOST/v1/database/$1/call/$2"; }
# Prints "<live on a> <live on b> <problems>" where problems lists characters that aren't live
# exactly once, plus anything still locked, pending or queued.
census() {
  python3 - "$HOST" "$N" <<'PY'
import json, sys, urllib.request
host, n = sys.argv[1], int(sys.argv[2])
a, b = (json.load(urllib.request.urlopen(f"{host}/v1/database/{d}/route/api/state", timeout=5)) for d in ("shard-a", "shard-b"))
names = [f"c{i}" for i in range(n)]
copies = {x: [(s, c) for s in ("a", "b") for c in (a if s == "a" else b)["characters"] if c["name"] == x] for x in names}
problems = [f"{x}:{[(s, c['state'], c['locked']) for s, c in cs]}" for x, cs in copies.items() if len(cs) != 1 or cs[0][1]["state"] != "live" or cs[0][1]["locked"]]
queued = a["idc"]["outbox_pending"] + b["idc"]["outbox_pending"]
dead = a["idc"]["outbox_dead"] + b["idc"]["outbox_dead"]
if queued: problems.append(f"queued={queued}")
if dead: problems.append(f"dead={dead}")
hs = [h["status"] for s in (a, b) for h in s["handoffs"]]
statuses = ",".join(f"{k}:{hs.count(k)}" for k in sorted(set(hs)))
print(sum(1 for cs in copies.values() if cs and cs[0][0] == "a"), sum(1 for cs in copies.values() if cs and cs[0][0] == "b"), statuses, " ".join(problems) or "ok")
PY
}

for i in $(seq 0 $((N - 1))); do call shard-a spawn "[\"c$i\"]" >/dev/null; done
fail=0
for round in $(seq "$ROUNDS"); do
  # Everyone heads for the other shard at once; the server dies a random 10-200 ms in.
  for i in $(seq 0 $((N - 1))); do
    for s in shard-a shard-b; do call $s transfer "[\"c$i\", 0]" >/dev/null & done
  done
  delay="0.$(printf %03d $((RANDOM % 191 + 10)))"
  sleep "$delay"
  crash
  wait 2>/dev/null
  mid="killed after ${delay}s"
  start
  # Leases on messages claimed before the crash expire after 60 s; give it time to settle.
  result=""
  for _ in $(seq 150); do
    result=$(census 2>/dev/null)
    [[ $result == *" ok" ]] && break
    sleep 0.5
  done
  read -r on_a on_b statuses verdict <<<"$result"
  printf '  round %d (%s): a=%s b=%s  [%s]  %s\n' "$round" "$mid" "$on_a" "$on_b" "$statuses" "$verdict"
  [[ $result == *" ok" ]] || fail=1
done
((fail == 0)) && echo "all rounds: every character live on exactly one shard" || { echo "INVARIANT BROKEN"; tail -30 "$LOG"; }
((fail == 0))
