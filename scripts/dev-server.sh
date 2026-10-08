#!/usr/bin/env bash
# Build (once) and run a local SpacetimeDB standalone that allows module HTTP to loopback.
#
# Why: standalone refuses outbound HTTP from modules to loopback/private addresses
# (SSRF protection), so two databases on one local server can't reach each other.
# SpacetimeDB has a compile-time feature for its own tests, `allow_loopback_http_for_tests`,
# that lifts the loopback block. This script builds standalone with it.
# On Maincloud (public URLs) you don't need any of this.
#
#   scripts/dev-server.sh build    # ~20-40 min the first time
#   scripts/dev-server.sh start    # in-memory server on 127.0.0.1:3000
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
DEV="$ROOT/.dev-server"
VERSION=${STDB_VERSION:-$(spacetime --version | sed -n 's/.*tool version \([0-9.]*\);.*/\1/p')}
BIN="$DEV/spacetimedb-standalone-$VERSION"

case "${1:-start}" in
  build)
    if [[ -x $BIN ]]; then echo "already built: $BIN"; exit 0; fi
    mkdir -p "$DEV"
    SRC="$DEV/SpacetimeDB-$VERSION"
    [[ -d $SRC ]] || git clone --depth 1 --branch "v$VERSION" https://github.com/clockworklabs/SpacetimeDB "$SRC"
    (cd "$SRC" && CARGO_PROFILE_RELEASE_LTO=false cargo build --release -p spacetimedb-standalone --features allow_loopback_http_for_tests)
    cp "$SRC/target/release/spacetimedb-standalone" "$BIN"
    echo "built $BIN"
    ;;
  start)
    [[ -x $BIN ]] || { echo "run '$0 build' first" >&2; exit 1; }
    # Reuse the CLI's JWT keys so `spacetime publish/call/sql` are authorized as usual.
    rm -rf "$DEV/data"; mkdir -p "$DEV/data"
    exec "$BIN" start --in-memory --data-dir "$DEV/data" --listen-addr "${LISTEN:-127.0.0.1:3000}" \
      --jwt-key-dir "${SPACETIME_CONFIG_DIR:-$HOME/.config/spacetime}" --non-interactive
    ;;
  *) echo "usage: $0 build|start" >&2; exit 2 ;;
esac
