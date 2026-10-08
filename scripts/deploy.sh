#!/usr/bin/env bash
# Publish `warehouse` and `shop` and point them at each other.
#
#   scripts/deploy.sh                 # local server, route transport, TypeScript shop
#   IDC_TRANSPORT=reducer scripts/deploy.sh
#   SHOP_MODULE=shop-rs scripts/deploy.sh   # shop-ts (default) | shop-rs | shop-cs
#   WAREHOUSE_MODULE=warehouse-cs scripts/deploy.sh   # warehouse (Rust, default) | warehouse-cs
#   SERVER=maincloud HOST=https://maincloud.spacetimedb.com SHOP=my-shop WAREHOUSE=my-warehouse scripts/deploy.sh
set -euo pipefail
cd "$(dirname "$0")/.."
SERVER=${SERVER:-local}
HOST=${HOST:-http://127.0.0.1:3000}
SHOP=${SHOP:-shop}
WAREHOUSE=${WAREHOUSE:-warehouse}
SHOP_MODULE=${SHOP_MODULE:-shop-ts}
WAREHOUSE_MODULE=${WAREHOUSE_MODULE:-warehouse}
export IDC_TRANSPORT=${IDC_TRANSPORT:-route}

# One shared secret for the mesh. Keep it out of git.
SECRET_FILE=.idc-secret
[[ -f $SECRET_FILE ]] || (umask 077; openssl rand -hex 32 > "$SECRET_FILE")
export IDC_SECRET=$(cat "$SECRET_FILE")

publish() { # db module self peers
  IDC_SELF=$3 IDC_PEERS=$4 spacetime publish "$1" --module-path "$2" -s "$SERVER" -y "${@:5}"
}
publish "$WAREHOUSE" "$WAREHOUSE_MODULE" warehouse "shop=$HOST/v1/database/$SHOP" "$@"
publish "$SHOP" "$SHOP_MODULE" shop "warehouse=$HOST/v1/database/$WAREHOUSE" "$@"
echo
echo "Dashboard: $HOST/v1/database/$WAREHOUSE/route/"
