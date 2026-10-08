#!/usr/bin/env bash
# Publish the demo's `warehouse` and `shop` databases and point them at each other.
#
#   scripts/deploy.sh                                   # Rust shop + Rust warehouse, route transport
#   SHOP_LANG=typescript scripts/deploy.sh              # shop: rust | typescript | csharp
#   WAREHOUSE_LANG=csharp scripts/deploy.sh             # warehouse: rust | csharp
#   IDC_TRANSPORT=reducer scripts/deploy.sh             # route | reducer
#   SERVER=maincloud HOST=https://maincloud.spacetimedb.com SHOP=my-shop WAREHOUSE=my-warehouse scripts/deploy.sh
set -euo pipefail
cd "$(dirname "$0")/.."
SERVER=${SERVER:-local}
HOST=${HOST:-http://127.0.0.1:3000}
SHOP=${SHOP:-shop}                    # database names
WAREHOUSE=${WAREHOUSE:-warehouse}
SHOP_LANG=${SHOP_LANG:-rust}
WAREHOUSE_LANG=${WAREHOUSE_LANG:-rust}
export IDC_TRANSPORT=${IDC_TRANSPORT:-route}

for dir in "demo/$SHOP_LANG/shop" "demo/$WAREHOUSE_LANG/warehouse"; do
  [[ -d $dir ]] || { echo "no such module: $dir" >&2; exit 2; }
done

# One shared secret for the mesh. Keep it out of git.
SECRET_FILE=.idc-secret
[[ -f $SECRET_FILE ]] || (umask 077; openssl rand -hex 32 > "$SECRET_FILE")
export IDC_SECRET=$(cat "$SECRET_FILE")

publish() { # db module self peers
  IDC_SELF=$3 IDC_PEERS=$4 spacetime publish "$1" --module-path "$2" -s "$SERVER" -y "${@:5}"
}
publish "$WAREHOUSE" "demo/$WAREHOUSE_LANG/warehouse" warehouse "shop=$HOST/v1/database/$SHOP" "$@"
publish "$SHOP" "demo/$SHOP_LANG/shop" shop "warehouse=$HOST/v1/database/$WAREHOUSE" "$@"
echo
echo "shop ($SHOP_LANG) ⇄ warehouse ($WAREHOUSE_LANG), transport: $IDC_TRANSPORT"
[[ $WAREHOUSE_LANG == rust ]] && echo "Dashboard: $HOST/v1/database/$WAREHOUSE/route/"
[[ $SHOP_LANG == rust ]] && echo "Dashboard: $HOST/v1/database/$SHOP/route/"
true
