# Demo: shop ⇄ warehouse

Two databases with one job each:

- **shop** takes orders. It asks the warehouse to reserve stock, reacts to the answer, and keeps a live mirror of the
  warehouse's stock.
- **warehouse** owns the stock. It reserves on request, replies, and pushes every stock change to the shop.

Every arrow below is a push. Nothing polls.

```
place_order ─▶ shop ──"reserve"──────────▶ warehouse
               shop ◀─"reservation"─────── warehouse   (confirmed / rejected + reason)
               shop ◀─"stock"───────────── warehouse   (on every change, incl. restock)
               shop ──quote() RPC─────────▶ warehouse  (synchronous answer)
```

The **Rust** pair is the reference demo. The other languages implement the same behaviour so CI can test every
pairing:

| Folder | Language | Uses |
|---|---|---|
| [`rust/shop`](rust/shop/src/lib.rs), [`rust/warehouse`](rust/warehouse/src/lib.rs) | Rust | [`rust/idc.rs`](../rust/idc.rs) |
| [`typescript/shop`](typescript/shop/src/index.ts) | TypeScript | the [`spacetimedb-idc` submodule](../typescript) |
| [`csharp/shop`](csharp/shop/Lib.cs), [`csharp/warehouse`](csharp/warehouse/Lib.cs) | C# | [`csharp/Idc.cs`](../csharp/Idc.cs) |
| [`dashboard/`](dashboard) | HTML + Rust | Live dashboard the Rust modules serve at `/route/` |

## Run it

You need the [SpacetimeDB CLI](https://spacetimedb.com/install) 2.11+, Rust with `wasm32-unknown-unknown`, and
`python3` for the tests. For the other variants you also need Node 22+ (TypeScript) or .NET 8 with the
`wasi-experimental` workload (C#).

> **Why a custom local server?** Standalone refuses outbound HTTP from modules to loopback and private addresses
> (SSRF protection), so two databases on one local server can't reach each other. [`scripts/dev-server.sh`](../scripts/dev-server.sh)
> builds standalone with SpacetimeDB's own test feature `allow_loopback_http_for_tests`. On Maincloud the URLs are
> public and none of this is needed.

```bash
scripts/dev-server.sh build      # once: builds standalone with loopback allowed (~15-40 min)
scripts/dev-server.sh start &    # in-memory server on 127.0.0.1:3000

scripts/deploy.sh                # Rust shop + Rust warehouse, pointed at each other
open http://127.0.0.1:3000/v1/database/warehouse/route/   # live dashboard

scripts/e2e.sh                   # 31 end-to-end checks
scripts/bench.sh 500 16          # throughput
```

Mix and match:

```bash
npm install                                          # once, for the TypeScript shop
SHOP_LANG=typescript scripts/deploy.sh               # TS submodule shop ⇄ Rust warehouse
SHOP_LANG=csharp WAREHOUSE_LANG=csharp scripts/deploy.sh
IDC_TRANSPORT=reducer scripts/deploy.sh              # identity-token transport instead of signed routes
```

Or poke it from the CLI:

```bash
spacetime call shop place_order gear 2        # → confirmed ~20 ms later
spacetime call warehouse restock sprocket 7   # → shop's stock_mirror updates by push
spacetime call shop quote gear                # synchronous RPC
spacetime call shop peek_warehouse            # SQL pull
spacetime sql shop "SELECT * FROM idc_log"    # what happened, with latencies
```

## What the tests check

[`scripts/e2e.sh`](../scripts/e2e.sh), run in CI for every shop × warehouse language pair:

- **Pairing:** both sides hold a token for the other *and* trust the other's identity, with no manual steps.
- **Event-driven replication:** a restock appears in the shop's mirror by push.
- **Request → reaction → reply:** orders confirmed, or rejected with the reason.
- **RPC and SQL pulls.**
- **Security:**
  - unsigned, badly signed and stale requests are rejected;
  - pairing as a non-peer name is refused;
  - `idc_receive` refuses identities it doesn't know.
- **Idempotency:** the same signed message twice → applied once, then `duplicate: true`.
- **Dead letters:** a message the peer permanently refuses is parked, not retried forever.
- **Outage:** while the warehouse is unreachable the order waits, and it's delivered automatically once it's back.
- **Ordering:** 20 concurrent orders arrive in commit order.
- **Reducer transport and re-pairing:** the full round trip through `/call`, and automatic re-pairing after a peer
  forgets us.
