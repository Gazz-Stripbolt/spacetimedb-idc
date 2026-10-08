//! **warehouse**: owns the stock. Reacts to `reserve` requests from the shop the
//! moment they arrive, and pushes every stock change back to the shop.

#[path = "../../idc/dashboard.rs"]
mod dashboard;
#[path = "../../idc/idc.rs"]
pub mod idc;

use serde_json::{Value, json};
use spacetimedb::http::{HandlerContext, Request, Response, Router, handler, router};
use spacetimedb::{ReducerContext, Table, Timestamp};

const SHOP: &str = "shop";

#[spacetimedb::table(accessor = stock, public)]
pub struct Stock {
    #[primary_key]
    pub sku: String,
    pub qty: u32,
}

#[spacetimedb::table(accessor = reservation, public)]
pub struct Reservation {
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    /// Which database asked, and its order id there.
    pub peer: String,
    pub order_id: u64,
    pub sku: String,
    pub qty: u32,
    pub at: Timestamp,
}

#[spacetimedb::reducer(init)]
pub fn init(ctx: &ReducerContext) {
    for (sku, qty) in [("gear", 25), ("sprocket", 10), ("rocket-boots", 1)] {
        ctx.db.stock().insert(Stock {
            sku: sku.into(),
            qty,
        });
    }
    idc::init(ctx);
}

fn set_stock(ctx: &ReducerContext, sku: &str, qty: u32) {
    let row = Stock {
        sku: sku.into(),
        qty,
    };
    if ctx.db.stock().sku().find(sku.to_string()).is_some() {
        ctx.db.stock().sku().update(row);
    } else {
        ctx.db.stock().insert(row);
    }
    // Replicate the change to the shop: same transaction, pushed right after commit.
    idc::send(ctx, SHOP, "stock", json!({ "sku": sku, "qty": qty }));
}

/// Warehouse staff add stock. The shop's mirror updates within milliseconds, with no polling.
#[spacetimedb::reducer]
pub fn restock(ctx: &ReducerContext, sku: String, qty: u32) -> Result<(), String> {
    if sku.is_empty() || qty == 0 {
        return Err("need a sku and qty > 0".into());
    }
    let current = ctx.db.stock().sku().find(&sku).map_or(0, |s| s.qty);
    set_stock(ctx, &sku, current + qty);
    Ok(())
}

/// Send the full stock list to the shop (useful after first pairing).
#[spacetimedb::reducer]
pub fn sync_all(ctx: &ReducerContext) {
    for s in ctx.db.stock().iter() {
        idc::send(ctx, SHOP, "stock", json!({ "sku": s.sku, "qty": s.qty }));
    }
}

/// Called by `idc` for every message, inside the receiving transaction.
pub fn on_idc_message(
    ctx: &ReducerContext,
    from: &str,
    kind: &str,
    payload: &Value,
) -> Result<(), String> {
    match kind {
        "reserve" => {
            let order_id = payload["order_id"]
                .as_u64()
                .ok_or("reserve: missing order_id")?;
            let sku = payload["sku"]
                .as_str()
                .ok_or("reserve: missing sku")?
                .to_string();
            let qty = payload["qty"].as_u64().ok_or("reserve: missing qty")? as u32;
            let available = ctx.db.stock().sku().find(&sku).map(|s| s.qty);
            let (ok, note) = match available {
                None => (false, format!("unknown sku `{sku}`")),
                Some(have) if have < qty => (false, format!("only {have} left")),
                Some(have) => {
                    set_stock(ctx, &sku, have - qty);
                    ctx.db.reservation().insert(Reservation {
                        id: 0,
                        peer: from.to_string(),
                        order_id,
                        sku: sku.clone(),
                        qty,
                        at: ctx.timestamp,
                    });
                    (true, format!("reserved {qty} × {sku}"))
                }
            };
            // A business "no" is a normal reply, not an error: errors mean "retry me".
            idc::send(
                ctx,
                from,
                "reservation",
                json!({ "order_id": order_id, "ok": ok, "note": note }),
            );
            Ok(())
        }
        other => Err(format!("warehouse doesn't handle `{other}` messages")),
    }
}

/// Synchronous, signed request/response: the shop asks "how many X do you have?".
#[handler]
fn rpc_stock(ctx: &mut HandlerContext, req: Request) -> Response {
    let (_from, payload) = match idc::verify_rpc(ctx, req) {
        Ok(v) => v,
        Err(response) => return response,
    };
    let sku = payload["sku"].as_str().unwrap_or_default().to_string();
    let qty = ctx.with_tx(|tx| tx.db.stock().sku().find(&sku).map(|s| s.qty));
    idc::rpc_reply(json!({ "sku": sku, "qty": qty, "answered_by": "warehouse" }))
}

#[handler]
fn state(ctx: &mut HandlerContext, _req: Request) -> Response {
    let (stock, reservations) = ctx.with_tx(|tx| {
        let mut stock: Vec<Value> = tx.db.stock().iter().map(|s| json!({ "sku": s.sku, "qty": s.qty })).collect();
        stock.sort_by(|a, b| a["sku"].as_str().cmp(&b["sku"].as_str()));
        let mut res: Vec<_> = tx.db.reservation().iter().collect();
        res.sort_by_key(|r| std::cmp::Reverse(r.id));
        let res: Vec<Value> = res
            .into_iter()
            .take(40)
            .map(|r| json!({ "id": r.id, "peer": r.peer, "order_id": r.order_id, "sku": r.sku, "qty": r.qty }))
            .collect();
        (stock, res)
    });
    dashboard::state_response(ctx, json!({ "stock": stock, "reservations": reservations }))
}

#[router]
fn routes() -> Router {
    idc::router()
        .merge(dashboard::router())
        .post("/rpc/stock", rpc_stock)
        .get("/api/state", state)
}
