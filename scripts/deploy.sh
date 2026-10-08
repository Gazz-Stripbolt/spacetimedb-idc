#!/usr/bin/env bash
# Publish `warehouse` and `shop` and point them at each other.
#
#   scripts/deploy.sh                 # local server, route transport
#   IDC_TRANSPORT=reducer scripts/deploy.sh
#   SERVER=maincloud HOST=https://maincloud.spacetimedb.com SHOP=my-shop WAREHOUSE=my-warehouse scripts/deploy.sh
set -euo pipefail
cd "$(dirname "$0")/.."
SERVER=${SERVER:-local}
HOST=${HOST:-http://127.0.0.1:3000}
SHOP=${SHOP:-shop}
WAREHOUSE=${WAREHOUSE:-warehouse}
export IDC_TRANSPORT=${IDC_TRANSPORT:-route}

# One shared secret for the mesh. Keep it out of git.
SECRET_FILE=.idc-secret
[[ -f $SECRET_FILE ]] || (umask 077; openssl rand -hex 32 > "$SECRET_FILE")
export IDC_SECRET=$(cat "$SECRET_FILE")

publish() { # db module self peers
  IDC_SELF=$3 IDC_PEERS=$4 spacetime publish "$1" --module-path "$2" -s "$SERVER" -y "${@:5}"
}
publish "$WAREHOUSE" warehouse warehouse "shop=$HOST/v1/database/$SHOP" "$@"
publish "$SHOP" shop shop "warehouse=$HOST/v1/database/$WAREHOUSE" "$@"
echo
echo "Dashboard: $HOST/v1/database/$SHOP/route/"
