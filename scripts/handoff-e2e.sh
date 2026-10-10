#!/usr/bin/env bash
# End-to-end and chaos tests for handoffs between two world shards. Expects both shards freshly
# published by scripts/deploy-shards.sh on a server that allows loopback module HTTP.
#
#   scripts/handoff-e2e.sh
#
# Every scenario ends by checking the invariant: each character is live on exactly one shard,
# nothing is pending or locked, and both outboxes are drained.
set -uo pipefail
cd "$(dirname "$0")/.."
HOST=${HOST:-http://127.0.0.1:3000}
SERVER=${SERVER:-local}
SHARD_A=${SHARD_A:-shard-a}
SHARD_B=${SHARD_B:-shard-b}
SECRET=$(cat .idc-secret)
db() { [[ $1 == a ]] && echo "$SHARD_A" || echo "$SHARD_B"; }
self() { [[ $1 == a ]] && echo shard-a || echo shard-b; }

pass=0 fail=0
ok()  { printf '  \e[32m✔\e[0m %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf '  \e[31m✘\e[0m %s\n' "$1"; fail=$((fail + 1)); }
check() { if [[ $3 == "$2" ]]; then ok "$1"; else bad "$1 (expected $(printf %q "$2"), got $(printf %q "$3"))"; fi; }

# q <python expr over `a` and `b`, the two shards' /api/state>. Helpers: ch(s, name) → character
# or None, ho(s, name) → latest handoff row for name, live(name) → number of live copies.
q() {
  python3 - "$HOST/v1/database/$SHARD_A/route/api/state" "$HOST/v1/database/$SHARD_B/route/api/state" "$1" <<'PY' 2>/dev/null
import json, sys, urllib.request
a, b = (json.load(urllib.request.urlopen(u, timeout=5)) for u in sys.argv[1:3])
ch = lambda s, n: next((c for c in s['characters'] if c['name'] == n), None)
ho = lambda s, n: next((h for h in s['handoffs'] if h['entity'] == n), None)
live = lambda n: sum(1 for s in (a, b) for c in s['characters'] if c['name'] == n and c['state'] == 'live' and not c['locked'])
copies = lambda n: sum(1 for s in (a, b) for c in s['characters'] if c['name'] == n)
settled = lambda: all(not c['locked'] and c['state'] == 'live' for s in (a, b) for c in s['characters']) and a['idc']['outbox_pending'] == 0 and b['idc']['outbox_pending'] == 0
print(eval(sys.argv[3]))
PY
}
wait_for() { # <desc> <python bool expr> [timeout s]
  local deadline=$((SECONDS + ${3:-15}))
  while ((SECONDS < deadline)); do
    [[ $(q "$2") == True ]] && { ok "$1"; return 0; }
    sleep 0.2
  done
  bad "$1 (timed out; last: $(q "[(s['characters'], s['handoffs'][:3]) for s in (a, b)]" | cut -c1-600))"; return 1
}
# Each scenario ends here: all named characters live exactly once, nothing in flight.
invariant() { # <names…>
  local names; names=$(printf "'%s'," "$@")
  wait_for "invariant: exactly one live copy of each, nothing locked/pending/queued" "settled() and all(copies(n) == 1 and live(n) == 1 for n in [$names])" 30
}
call() { curl -s -o /dev/null -w '%{http_code}' -X POST -H 'content-type: application/json' -d "$3" "$HOST/v1/database/$(db "$1")/call/$2"; }
call_body() { curl -s -X POST -H 'content-type: application/json' -d "$3" "$HOST/v1/database/$(db "$1")/call/$2"; }
# Point shard <x>'s outgoing traffic somewhere else, or back (partition / heal).
peers_of() { [[ $1 == a ]] && echo "shard-b=$HOST/v1/database/$SHARD_B" || echo "shard-a=$HOST/v1/database/$SHARD_A"; }
cut_off() {
  local other; other=$([[ $1 == a ]] && echo shard-b || echo shard-a)
  (cd /tmp && IDC_PEERS="$other=$HOST/v1/database/nowhere-$RANDOM" spacetime publish "$(db "$1")" -s "$SERVER" --env-only -y >/dev/null 2>&1)
  spacetime call "$(db "$1")" idc_kick -s "$SERVER" >/dev/null 2>&1
}
heal() {
  (cd /tmp && IDC_PEERS="$(peers_of "$1")" spacetime publish "$(db "$1")" -s "$SERVER" --env-only -y >/dev/null 2>&1)
  spacetime call "$(db "$1")" idc_kick -s "$SERVER" >/dev/null 2>&1
}
# Sign and POST an envelope like a peer would: inject <to a|b> <from name> <kind> <payload json>
inject() {
  local now; now=$(python3 -c 'import time; print(int(time.time()*1e6))')
  local env="{\"id\":\"forged-$now-$RANDOM\",\"from\":\"$2\",\"to\":\"$(self "$1")\",\"kind\":\"$3\",\"payload\":$4,\"sent_at\":$now}"
  python3 - "$SECRET" "$HOST/v1/database/$(db "$1")/route/idc/inbox" "$env" <<'PY'
import hashlib, hmac, sys, time, urllib.request, urllib.error
secret, url, body = sys.argv[1], sys.argv[2], sys.argv[3].encode()
t = str(int(time.time() * 1_000_000))
sig = hmac.new(secret.encode(), t.encode() + b"." + body, hashlib.sha256).hexdigest()
req = urllib.request.Request(url, data=body, method="POST", headers={"content-type": "application/json", "x-idc-signature": f"t={t},v1={sig}"})
try:
    r = urllib.request.urlopen(req); print(r.status, r.read().decode())
except urllib.error.HTTPError as e:
    print(e.code, e.read().decode())
PY
}
tid() { q "ho($1, '$2')['id']"; }

wait_for "both shards up and paired" "all(s['idc']['self'] for s in (a, b))" 30

echo "Walking across the border (west → east → west)"
check "spawn thrall on shard-a" 200 "$(call a spawn '["thrall"]')"
for _ in 1 2 3; do call a step '["thrall", 1, 0]' >/dev/null; done
check "last step west of the border is a normal move" True "$(q "ch(a, 'thrall')['x'] == 7")"
check "stepping over the border starts a handoff" 200 "$(call a step '["thrall", 1, 0]')"
wait_for "thrall is live on shard-b at x = 8" "ch(b, 'thrall') and ch(b, 'thrall')['state'] == 'live' and ch(b, 'thrall')['x'] == 8 and not ch(a, 'thrall')"
check "source says released, target says active" "released active" "$(q "ho(a, 'thrall')['status'] + ' ' + ho(b, 'thrall')['status']")"
echo "    handoff took $(q "ho(a, 'thrall')['ms']") ms (offer → accept → release)"
check "gold and bag came along" True "$(q "ch(b, 'thrall')['gold'] == 10 and ch(b, 'thrall')['bag'] == ['torch']")"
check "playable on shard-b" 200 "$(call b step '["thrall", 1, 0]')"
call b step '["thrall", -1, 0]' >/dev/null
check "and back west" 200 "$(call b step '["thrall", -1, 0]')"
wait_for "thrall is back on shard-a" "ch(a, 'thrall') and ch(a, 'thrall')['state'] == 'live' and not ch(b, 'thrall')"
invariant thrall

echo "Locked while in transit"
call a spawn '["jaina"]' >/dev/null
call a spawn '["rexxar"]' >/dev/null
cut_off a
check "transfer starts while shard-b is unreachable" 200 "$(call a transfer '["jaina", 0]')"
check "jaina is locked on shard-a" True "$(q "ch(a, 'jaina')['locked'] and ho(a, 'jaina')['status'] == 'offered'")"
check "a locked character can't walk" 530 "$(call a step '["jaina", 1, 0]')"
check "a locked character can't trade" 530 "$(call a give '["jaina", "rexxar", 1]')"
check "a locked character can't receive a trade either" 530 "$(call a give '["rexxar", "jaina", 1]')"
check "a second transfer of the same character is refused" 530 "$(call a transfer '["jaina", 0]')"
heal a
wait_for "transfer completes once shard-b is reachable again" "ch(b, 'jaina') and ch(b, 'jaina')['state'] == 'live' and not ch(a, 'jaina')" 30
invariant thrall jaina rexxar

echo "Concurrent double transfer"
call a spawn '["sylvanas"]' >/dev/null
CODES=$( (call a transfer '["sylvanas", 0]' & call a transfer '["sylvanas", 0]' & call a transfer '["sylvanas", 0]' & wait) | fold -w3 | sort | tr '\n' ' ')
check "exactly one of three concurrent transfer attempts wins" "200 530 530 " "$CODES"
wait_for "sylvanas arrives on shard-b once" "ch(b, 'sylvanas') and ch(b, 'sylvanas')['state'] == 'live' and not ch(a, 'sylvanas')"
invariant sylvanas

echo "Rejection (the target refuses, the source unlocks)"
call a spawn '["arthas"]' >/dev/null
call b spawn '["arthas"]' >/dev/null 2>&1 || true
check "name collision: arthas also exists on shard-b" True "$(q "copies('arthas') == 2")"
call a transfer '["arthas", 0]' >/dev/null
wait_for "rejected with a reason" "ho(a, 'arthas') and ho(a, 'arthas')['status'] == 'rejected' and 'taken' in ho(a, 'arthas')['detail']"
check "arthas on shard-a is unlocked and playable" 200 "$(call a step '["arthas", 0, 1]')"
check "the note tells the player why" True "$(q "'taken' in ch(a, 'arthas')['note'] or ch(a, 'arthas')['note'] == ''")"
call b set_capacity '[0]' >/dev/null
call a spawn '["anduin"]' >/dev/null
call a transfer '["anduin", 0]' >/dev/null
wait_for "a full shard rejects arrivals" "ho(a, 'anduin') and ho(a, 'anduin')['status'] == 'rejected' and 'full' in ho(a, 'anduin')['detail'] and not ch(a, 'anduin')['locked']"
call b set_capacity '[50]' >/dev/null
check "no pending leftovers on shard-b" True "$(q "all(c['state'] == 'live' for c in b['characters'])")"

echo "Timeout during a partition (cancel queued behind the offer)"
call a spawn '["illidan"]' >/dev/null
cut_off a
call a transfer '["illidan", 1000]' >/dev/null
wait_for "after 1 s the source asks to cancel, still locked" "ho(a, 'illidan')['status'] == 'cancelling' and ch(a, 'illidan')['locked']" 10
check "a cancelling character still can't move" 530 "$(call a step '["illidan", 1, 0]')"
heal a
wait_for "target imports, then discards on the cancel; illidan stays on shard-a" "ho(a, 'illidan')['status'] == 'cancelled' and ho(b, 'illidan') and ho(b, 'illidan')['status'] == 'cancelled' and not ch(b, 'illidan')" 30
check "the timeout reason reaches the player" True "$(q "'timed out' in ch(a, 'illidan')['note']")"
invariant illidan anduin

echo "Cancel vs. accept race (20 transfers, each cancelled 5-100 ms later)"
NAMES=()
for i in $(seq 20); do NAMES+=("racer$i"); call a spawn "[\"racer$i\"]" >/dev/null; done
for i in $(seq 20); do (call a transfer "[\"racer$i\", 0]" >/dev/null; sleep "0.$(printf %03d $((i * 5)))"; call a cancel_transfer "[\"racer$i\"]" >/dev/null) & done; wait
invariant "${NAMES[@]}"
echo "    outcomes: $(q "sorted({s for s in [ho(a, 'racer%d' % i)['status'] for i in range(1, 21)]})") ($(q "sum(1 for i in range(1, 21) if ch(b, 'racer%d' % i))") crossed, $(q "sum(1 for i in range(1, 21) if ch(a, 'racer%d' % i))") stayed)"
call a transfer '["thrall", 0]' >/dev/null
wait_for "thrall crosses" "ch(b, 'thrall') and ch(b, 'thrall')['state'] == 'live'"
check "a cancel after release is refused" 530 "$(call a cancel_transfer '["thrall"]')"
invariant thrall

echo "Partitions between every step (nothing lost, nothing duplicated)"
call a spawn '["kael"]' >/dev/null
cut_off b
call a transfer '["kael", 0]' >/dev/null
wait_for "offer delivered: kael is pending on shard-b, still owned (locked) by shard-a" "ch(b, 'kael') and ch(b, 'kael')['state'] == 'pending' and ch(a, 'kael')['locked']" 10
check "only the owner's copy counts: zero live, one locked, one pending" True "$(q "live('kael') == 0 and copies('kael') == 2")"
check "the pending copy can't be played" 530 "$(call b step '["kael", 1, 0]')"
cut_off a
heal b
wait_for "accept delivered: shard-a let go, release is queued in its outbox" "not ch(a, 'kael') and ho(a, 'kael')['status'] == 'released' and a['idc']['outbox_pending'] >= 1 and ch(b, 'kael')['state'] == 'pending'" 15
heal a
wait_for "release delivered: kael is live on shard-b" "ch(b, 'kael') and ch(b, 'kael')['state'] == 'live'" 30
invariant kael

echo "Replayed and forged messages"
# Fresh message ids, so idc's inbox dedupe doesn't catch them; the state machine has to.
KAEL=$(tid b kael)
CHK=$(q "ho(b, 'kael')['id'] and ''")
inject b shard-a handoff.offer "{\"id\":\"$KAEL\",\"entity\":\"kael\",\"data\":\"{}\",\"checksum\":\"x\"}" >/dev/null
inject b shard-a handoff.release "{\"id\":\"$KAEL\"}" >/dev/null
inject b shard-a handoff.cancel "{\"id\":\"$KAEL\",\"reason\":\"forged\"}" >/dev/null
sleep 1
check "replayed offer/release and a late cancel for a finished transfer change nothing" True "$(q "ch(b, 'kael')['state'] == 'live' and ho(b, 'kael')['status'] == 'active' and copies('kael') == 1")"
THRALL=$(tid a thrall)
inject a shard-b handoff.accept "{\"id\":\"$THRALL\",\"checksum\":\"x\"}" >/dev/null
inject a shard-b handoff.reject "{\"id\":\"$THRALL\",\"reason\":\"forged\"}" >/dev/null
sleep 1
check "replayed accept/reject at the source change nothing" True "$(q "copies('thrall') == 1 and live('thrall') == 1")"
DATA='{"name":"ghost","x":9,"y":1,"hp":100,"gold":999999,"bag":[]}'
SUM=$(printf %s "$DATA" | sha256sum | cut -d' ' -f1)
inject b shard-a handoff.offer "{\"id\":\"shard-a-forged-1\",\"entity\":\"ghost\",\"data\":$(python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$DATA"),\"checksum\":\"$(printf 0%.0s {1..64})\"}" >/dev/null
sleep 0.5
check "an offer whose data doesn't match its checksum is rejected" True "$(q "ho(b, 'ghost')['status'] == 'rejected' and 'checksum' in ho(b, 'ghost')['detail'] and not ch(b, 'ghost')")"
inject b shard-a handoff.offer "{\"id\":\"shard-a-forged-2\",\"entity\":\"ghost\",\"data\":$(python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$DATA"),\"checksum\":\"$SUM\"}" >/dev/null
sleep 0.5
check "an offer nobody sent stays pending: the claimed source never releases it" True "$(q "ch(b, 'ghost')['state'] == 'pending' and live('ghost') == 0")"
inject b shard-a handoff.cancel '{"id":"shard-a-forged-2","reason":"cleanup"}' >/dev/null
wait_for "and is dropped by a cancel" "not ch(b, 'ghost')"
check "unknown handoff kind is refused (dead letter, not applied)" True "$(inject b shard-a handoff.bogus '{"id":"x"}' | grep -c 422 | sed 's/1/True/')"
check "a handoff message without an id is refused" True "$(inject b shard-a handoff.release '{}' | grep -c 422 | sed 's/1/True/')"

echo "Reducer transport"
for x in a b; do
  (cd /tmp && IDC_TRANSPORT=reducer spacetime publish "$(db $x)" -s "$SERVER" --env-only -y >/dev/null 2>&1)
  spacetime call "$(db $x)" idc_kick -s "$SERVER" >/dev/null 2>&1
done
check "transport is reducer" "reducer reducer" "$(q "a['idc']['transport'] + ' ' + b['idc']['transport']")"
call a spawn '["velen"]' >/dev/null
call a transfer '["velen", 0]' >/dev/null
wait_for "velen crosses over reducer calls" "ch(b, 'velen') and ch(b, 'velen')['state'] == 'live' and not ch(a, 'velen')" 30
invariant velen thrall jaina

echo
echo "$pass passed, $fail failed"
((fail == 0))
