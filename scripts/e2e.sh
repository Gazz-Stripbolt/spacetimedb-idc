#!/usr/bin/env bash
# End-to-end tests for the shop ⇄ warehouse demo. Expects both databases freshly
# published by scripts/deploy.sh on a server that allows loopback module HTTP.
#
#   scripts/e2e.sh
set -uo pipefail
cd "$(dirname "$0")/.."
HOST=${HOST:-http://127.0.0.1:3000}
SERVER=${SERVER:-local}
SHOP=${SHOP:-shop}
WAREHOUSE=${WAREHOUSE:-warehouse}
SECRET=$(cat .idc-secret)
db() { [[ $1 == shop ]] && echo "$SHOP" || echo "$WAREHOUSE"; }

pass=0 fail=0
ok()  { printf '  \e[32m✔\e[0m %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf '  \e[31m✘\e[0m %s\n' "$1"; fail=$((fail + 1)); }
check() { if [[ $3 == "$2" ]]; then ok "$1"; else bad "$1 (expected $(printf %q "$2"), got $(printf %q "$3"))"; fi; }

state() { curl -s "$HOST/v1/database/$(db "$1")/route/api/state"; }
# q <db> <python expr over `s`>: evaluate against the db's /api/state
q() { state "$1" | python3 -c "import json,sys; s=json.load(sys.stdin); print($2)" 2>/dev/null; }
# wait_for <desc> <db> <python bool expr> [timeout s]
wait_for() {
  local deadline=$((SECONDS + ${4:-15}))
  while ((SECONDS < deadline)); do
    [[ $(q "$2" "$3") == True ]] && { ok "$1"; return 0; }
    sleep 0.2
  done
  bad "$1 (timed out)"; return 1
}
call() { curl -s -o /dev/null -w '%{http_code}' -X POST -H 'content-type: application/json' -d "$3" "$HOST/v1/database/$(db "$1")/call/$2"; }
call_body() { curl -s -X POST -H 'content-type: application/json' -d "$3" "$HOST/v1/database/$(db "$1")/call/$2"; }
last_order() { q shop "s['orders'][0]['$1']"; }

# Sign and POST an envelope like a peer would. sign_post <db> <path> <body> [timestamp offset s]
sign_post() {
  python3 - "$SECRET" "$HOST/v1/database/$(db "$1")/route$2" "$3" "${4:-0}" <<'PY'
import hashlib, hmac, sys, time, urllib.request, urllib.error
secret, url, body, offset = sys.argv[1], sys.argv[2], sys.argv[3].encode(), int(sys.argv[4])
t = str(int((time.time() + offset) * 1_000_000))
sig = hmac.new(secret.encode(), t.encode() + b"." + body, hashlib.sha256).hexdigest()
req = urllib.request.Request(url, data=body, method="POST", headers={"content-type": "application/json", "x-idc-signature": f"t={t},v1={sig}"})
try:
    r = urllib.request.urlopen(req); print(r.status, r.read().decode())
except urllib.error.HTTPError as e:
    print(e.code, e.read().decode())
PY
}

echo "Pairing (automatic token + known-identity exchange, both ways)"
wait_for "shop holds a token for warehouse and trusts it" shop "'warehouse' in s['idc']['has_token_for'] and 'warehouse' in s['idc']['trusts']" 30
wait_for "warehouse holds a token for shop and trusts it" warehouse "'shop' in s['idc']['has_token_for'] and 'shop' in s['idc']['trusts']" 30

echo "Event-driven replication (warehouse → shop)"
call warehouse sync_all '[]' >/dev/null
wait_for "shop's stock mirror filled by pushes" shop "len(s['stock_mirror']) == 3"
call warehouse restock '["sprocket", 7]' >/dev/null
wait_for "restock shows up in the shop's mirror" shop "{r['sku']: r['qty'] for r in s['stock_mirror']}.get('sprocket') == 17" 5

echo "Request → reaction → reply (shop → warehouse → shop), route transport"
check "transport is route" route "$(q shop "s['idc']['transport']")"
check "place_order accepted" 200 "$(call shop place_order '["gear", 2]')"
wait_for "order confirmed by the warehouse" shop "s['orders'][0]['status'] == 'confirmed'" 5
echo "    round trip: $(last_order round_trip_ms) ms"
wait_for "warehouse stock decremented" warehouse "{r['sku']: r['qty'] for r in s['stock']}['gear'] == 23"
wait_for "shop mirror follows the reservation" shop "{r['sku']: r['qty'] for r in s['stock_mirror']}['gear'] == 23"
call shop place_order '["rocket-boots", 5]' >/dev/null
wait_for "too-large order rejected with a reason" shop "s['orders'][0]['status'] == 'rejected' and 'only 1 left' in s['orders'][0]['note']"
call shop place_order '["unobtainium", 1]' >/dev/null
wait_for "unknown sku rejected" shop "s['orders'][0]['status'] == 'rejected' and 'unknown sku' in s['orders'][0]['note']"

echo "Request/response and SQL pulls (from procedures)"
QUOTE=$(call_body shop quote '["gear"]')
check "quote() RPC returns warehouse's answer" 23 "$(python3 -c "import json,sys; v=json.loads(json.loads(sys.argv[1])); print(v['qty'])" "$QUOTE" 2>/dev/null)"
PEEK=$(call_body shop peek_warehouse '[]')
check "peek_warehouse() SQL pull returns rows" True "$(python3 -c "import json,sys; v=json.loads(json.loads(sys.argv[1])); print(len(v[0]['rows']) == 3)" "$PEEK" 2>/dev/null)"

echo "Security"
check "unsigned inbox POST rejected" 401 "$(curl -s -o /dev/null -w '%{http_code}' -X POST -d '{}' "$HOST/v1/database/$SHOP/route/idc/inbox")"
check "wrong signature rejected" 401 "$(curl -s -o /dev/null -w '%{http_code}' -X POST -H 'x-idc-signature: t=1,v1=00' -d '{}' "$HOST/v1/database/$SHOP/route/idc/inbox")"
STALE='{"id":"stale-1","from":"warehouse","to":"shop","kind":"stock","payload":{"sku":"x","qty":1},"sent_at":0}'
check "stale timestamp rejected" 401 "$(sign_post shop /idc/inbox "$STALE" -600 | cut -d' ' -f1)"
check "pairing request without signature rejected" 401 "$(curl -s -o /dev/null -w '%{http_code}' -X POST -d '{"from":"warehouse","identity":"00"}' "$HOST/v1/database/$SHOP/route/idc/pair")"
check "pairing as a non-peer name refused" 403 "$(sign_post shop /idc/pair '{"from":"mallory","identity":"c200000000000000000000000000000000000000000000000000000000000000"}' | cut -d' ' -f1)"
OUT=$(spacetime call "$SHOP" idc_receive '"{}"' -s "$SERVER" 2>&1)
check "idc_receive reducer refuses unknown identities" 1 "$(grep -c 'not a known peer' <<<"$OUT")"

echo "Idempotency (at-least-once delivery, exactly-once effect)"
NOW=$(python3 -c 'import time; print(int(time.time()*1e6))')
REPLAY="{\"id\":\"replay-$NOW\",\"from\":\"warehouse\",\"to\":\"shop\",\"kind\":\"stock\",\"payload\":{\"sku\":\"replay-test\",\"qty\":42},\"sent_at\":$NOW}"
check "first delivery applied" '200 {"duplicate":false,"ok":true}' "$(sign_post shop /idc/inbox "$REPLAY")"
check "same message again is a no-op" '200 {"duplicate":true,"ok":true}' "$(sign_post shop /idc/inbox "$REPLAY")"

echo "Dead letters (peer permanently refuses a message)"
FORGED="{\"id\":\"forged-$NOW\",\"from\":\"shop\",\"to\":\"warehouse\",\"kind\":\"reserve\",\"payload\":{\"order_id\":999999,\"sku\":\"gear\",\"qty\":1},\"sent_at\":$NOW}"
sign_post warehouse /idc/inbox "$FORGED" >/dev/null
wait_for "reply for a nonexistent order is parked as dead, not retried forever" warehouse "s['idc']['outbox_dead'] == 1"

echo "Outage and recovery (retries with backoff, no lost messages)"
IDC_PEERS="warehouse=$HOST/v1/database/does-not-exist" spacetime publish "$SHOP" -s "$SERVER" --env-only -y >/dev/null 2>&1
spacetime call "$SHOP" idc_kick -s "$SERVER" >/dev/null 2>&1   # TS submodules re-read config from env here
call shop place_order '["gear", 1]' >/dev/null
wait_for "order waits while the warehouse is unreachable" shop "s['orders'][0]['status'] == 'pending' and s['idc']['outbox_pending'] == 1 and any(l['event'] == 'retry' for l in s['idc']['log'])" 10
IDC_PEERS="warehouse=$HOST/v1/database/$WAREHOUSE" spacetime publish "$SHOP" -s "$SERVER" --env-only -y >/dev/null 2>&1
spacetime call "$SHOP" idc_kick -s "$SERVER" >/dev/null 2>&1   # TS submodules re-read config from env here
wait_for "delivered automatically once it's back" shop "s['orders'][0]['status'] == 'confirmed' and s['idc']['outbox_pending'] == 0" 30

echo "Ordering under a burst (20 concurrent orders)"
FIRST=$(($(q shop "s['orders'][0]['id']") + 1))
for i in $(seq 20); do call shop place_order '["gear", 1]' >/dev/null & done; wait
wait_for "all 20 confirmed" shop "sum(1 for o in s['orders'] if o['id'] >= $FIRST and o['status'] == 'confirmed') == 20 and s['idc']['outbox_pending'] == 0" 30
check "warehouse saw them in order" True "$(q warehouse "(lambda ids: ids == sorted(ids))([r['order_id'] for r in reversed(s['reservations']) if $FIRST <= r['order_id'] < 999999])")"
echo "    round trips (ms): $(q shop "sorted(round(o['round_trip_ms'],1) for o in s['orders'] if o['id'] >= $FIRST and o['round_trip_ms'])")"

echo "Reducer transport (identity token + known-identity check)"
IDC_TRANSPORT=reducer spacetime publish "$WAREHOUSE" -s "$SERVER" --env-only -y >/dev/null 2>&1
spacetime call "$WAREHOUSE" idc_kick -s "$SERVER" >/dev/null 2>&1   # TS submodules re-read config from env here
IDC_TRANSPORT=reducer spacetime publish "$SHOP" -s "$SERVER" --env-only -y >/dev/null 2>&1
spacetime call "$SHOP" idc_kick -s "$SERVER" >/dev/null 2>&1   # TS submodules re-read config from env here
check "transport is reducer" reducer "$(q shop "s['idc']['transport']")"
call shop place_order '["sprocket", 3]' >/dev/null
wait_for "order confirmed over reducer calls" shop "s['orders'][0]['status'] == 'confirmed'" 5
echo "    round trip: $(last_order round_trip_ms) ms"
wait_for "both hops went through idc_receive" shop "any(l['transport'] == 'reducer' and l['event'] == 'applied' for l in s['idc']['log'])"
check "warehouse applied via reducer too" True "$(q warehouse "any(l['transport'] == 'reducer' and l['event'] == 'applied' for l in s['idc']['log'])")"
spacetime sql "$WAREHOUSE" "DELETE FROM idc_known_peer" -s "$SERVER" >/dev/null 2>&1
call shop place_order '["gear", 1]' >/dev/null
wait_for "a peer that forgot our identity gets re-paired automatically" shop "s['orders'][0]['status'] == 'confirmed' and any(l['event'] == 'paired' for l in s['idc']['log'][:6])" 15

echo
echo "$pass passed, $fail failed"
((fail == 0))
