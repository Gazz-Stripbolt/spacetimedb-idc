#!/usr/bin/env bash
# Publish the handoff demo: two world shards, `shard-a` (west) and `shard-b` (east), pointed at each other.
#
#   scripts/deploy-shards.sh                          # Rust ⇄ Rust, route transport
#   A_LANG=csharp B_LANG=typescript scripts/deploy-shards.sh   # rust | csharp | typescript
#   IDC_TRANSPORT=reducer scripts/deploy-shards.sh
set -euo pipefail
cd "$(dirname "$0")/.."
SERVER=${SERVER:-local}
HOST=${HOST:-http://127.0.0.1:3000}
SHARD_A=${SHARD_A:-shard-a}           # database names
SHARD_B=${SHARD_B:-shard-b}
A_LANG=${A_LANG:-rust}
B_LANG=${B_LANG:-rust}
export IDC_TRANSPORT=${IDC_TRANSPORT:-route}

for dir in "demo/$A_LANG/shard" "demo/$B_LANG/shard"; do
  [[ -d $dir ]] || { echo "no such module: $dir" >&2; exit 2; }
done

SECRET_FILE=.idc-secret
[[ -f $SECRET_FILE ]] || (umask 077; openssl rand -hex 32 > "$SECRET_FILE")
export IDC_SECRET=$(cat "$SECRET_FILE")

publish() { # db module self peers
  IDC_SELF=$3 IDC_PEERS=$4 spacetime publish "$1" --module-path "$2" -s "$SERVER" -y "${@:5}"
}
publish "$SHARD_A" "demo/$A_LANG/shard" shard-a "shard-b=$HOST/v1/database/$SHARD_B" "$@"
publish "$SHARD_B" "demo/$B_LANG/shard" shard-b "shard-a=$HOST/v1/database/$SHARD_A" "$@"
echo
echo "shard-a ($A_LANG) ⇄ shard-b ($B_LANG), transport: $IDC_TRANSPORT"
echo "Dashboard: $HOST/v1/database/$SHARD_A/route/"
