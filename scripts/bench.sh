#!/usr/bin/env bash
# Fire N orders at the shop and time how long until every one is settled by the warehouse.
#   scripts/bench.sh [N=300] [concurrency=16]
set -euo pipefail
cd "$(dirname "$0")/.."
HOST=${HOST:-http://127.0.0.1:3000}; SHOP=${SHOP:-shop}; WAREHOUSE=${WAREHOUSE:-warehouse}
N=${1:-300}; C=${2:-16}
curl -s -o /dev/null -X POST -H 'content-type: application/json' -d '["gear", 100000]' "$HOST/v1/database/$WAREHOUSE/call/restock"
sql() { spacetime sql "$SHOP" "$1" -s "${SERVER:-local}" 2>/dev/null; }
before=$(sql "SELECT COUNT(*) AS n FROM order WHERE status != 'pending'" | grep -Eo '[0-9]+' | tail -1)
start=$(date +%s.%N)
seq "$N" | xargs -P "$C" -I{} curl -s -o /dev/null -X POST -H 'content-type: application/json' -d '["gear", 1]' "$HOST/v1/database/$SHOP/call/place_order"
placed=$(date +%s.%N)
target=$((before + N))
while :; do
  done_n=$(sql "SELECT COUNT(*) AS n FROM order WHERE status != 'pending'" | grep -Eo '[0-9]+' | tail -1)
  ((done_n >= target)) && break
  sleep 0.1
done
end=$(date +%s.%N)
python3 - "$N" "$start" "$placed" "$end" "$(curl -s "$HOST/v1/database/$SHOP/route/api/state" | python3 -c 'import json,sys; print(json.load(sys.stdin)["idc"]["transport"])')" <<'PY'
import sys
n, s, p, e, transport = int(sys.argv[1]), *map(float, sys.argv[2:5]), sys.argv[5]
print(f"transport={transport}: {n} orders placed in {p-s:.2f}s, all settled after {e-s:.2f}s "
      f"→ {n/(e-s):.0f} orders/s end-to-end ({3*n/(e-s):.0f} cross-database messages/s: reserve, reply, stock push)")
PY
