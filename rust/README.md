# idc.rs: inter-database communication for Rust modules

One file you drop into a Rust SpacetimeDB module. Your database can then push messages to other databases (Rust, C#
or TypeScript) and react to theirs, with no polling. Rust submodules aren't supported by SpacetimeDB yet, so it's a
source file rather than a crate.

Source: [`idc.rs`](idc.rs) · Complete example: [`demo/rust`](../demo/rust) · Protocol: [`docs/PROTOCOL.md`](../docs/PROTOCOL.md)

## Add it

1. Copy `idc.rs` to your module's `src/idc.rs` and add `pub mod idc;` to `lib.rs`.
2. Dependencies:
   ```toml
   spacetimedb = { version = "2.11.*", features = ["unstable"] }
   http = "1"
   serde = { version = "1", features = ["derive"] }
   serde_json = "1"
   sha2 = "0.10"
   hmac = "0.12"
   ```
3. Wire it up in `lib.rs`:
   ```rust
   pub mod idc;
   use serde_json::{json, Value};

   #[spacetimedb::reducer(init)]
   pub fn init(ctx: &ReducerContext) {
       idc::init(ctx);
   }

   #[spacetimedb::http::router]
   fn routes() -> Router {
       idc::router()            // adds POST /idc/inbox and /idc/pair
           .get("/api/state", state)
   }

   /// Called for every incoming message, inside the receiving transaction.
   pub fn on_idc_message(ctx: &ReducerContext, from: &str, kind: &str, payload: &Value) -> Result<(), String> {
       match kind {
           "reservation" => { /* update your tables */ Ok(()) }
           other => Err(format!("unknown kind {other}")), // Err = permanent refusal (dead letter)
       }
   }

   #[spacetimedb::reducer]
   pub fn place_order(ctx: &ReducerContext, sku: String, qty: u32) {
       // ... insert the order ...
       idc::send(ctx, "warehouse", "reserve", json!({ "order_id": id, "sku": sku, "qty": qty }));
   }
   ```
4. Publish with the four env vars (the file declares them with `#[spacetimedb::env]`):
   ```bash
   IDC_SELF=shop IDC_SECRET=$SECRET IDC_TRANSPORT=route \
   IDC_PEERS=warehouse=https://maincloud.spacetimedb.com/v1/database/my-warehouse \
   spacetime publish my-shop
   ```

If your module already has a `#[spacetimedb::env]` struct, move the four `IDC_*` fields into it and delete the one in
`idc.rs` (a module has one env declaration).

## API

| Item | What it does |
|---|---|
| `idc::init(ctx)` | Idempotent setup: message-id epoch, cleanup schedule, pairing. Call it from `init`. |
| `idc::send(ctx, peer, kind, payload)` | Queues a message in the current transaction; delivery starts right after commit |
| `idc::router()` | The `/idc/inbox` and `/idc/pair` routes, to merge into your router |
| `idc::rpc(ctx, peer, path, payload)` | Signed synchronous call to a peer's route, from a procedure |
| `idc::verify_rpc(ctx, req)` / `idc::rpc_reply(value)` | For your RPC handlers |
| `idc::sql(ctx, peer, query)` | SQL against the peer's `/sql`, from a procedure |
| `idc_kick` (reducer) | Re-run setup, pairing and flushing, e.g. after `publish --env-only`, or once after adding idc to a live database |
| `idc_receive` (reducer) | Receiving end of the reducer transport |

Tables: `idc_outbox`, `idc_seen`, `idc_known_peer`, `idc_peer_token`, `idc_state` (private), `idc_log` (public:
metadata only), plus three schedule tables.
