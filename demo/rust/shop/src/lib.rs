//! **shop**: takes orders. It asks the warehouse to reserve stock and reacts
//! to the answer as soon as it arrives. It also keeps a live mirror of the
//! warehouse's stock, fed by pushes rather than polling.

#[path = "../../../dashboard/dashboard.rs"]
mod dashboard;
#[path = "../../../../rust/idc.rs"]
pub mod idc;

use serde_json::{Value, json};
use spacetimedb::http::{HandlerContext, Request, Response, Router, handler, router};
use spacetimedb::{ProcedureContext, ReducerContext, Table, Timestamp};

const WAREHOUSE: &str = "warehouse";

#[spacetimedb::table(accessor = order, public)]
pub struct Order {
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    pub sku: String,
    pub qty: u32,
    /// pending → confirmed | rejected
    pub status: String,
    pub note: String,
    pub placed: Timestamp,
    pub settled: Option<Timestamp>,
}

/// The warehouse's stock as last pushed to us. Never polled.
#[spacetimedb::table(accessor = stock_mirror, public)]
pub struct StockMirror {
    #[primary_key]
    pub sku: String,
    pub qty: u32,
    pub updated: Timestamp,
}

#[spacetimedb::reducer(init)]
pub fn init(ctx: &ReducerContext) {
    idc::init(ctx);
}

/// A customer places an order. The order row and the `reserve` message commit
/// together; the warehouse sees the request moments later.
#[spacetimedb::reducer]
pub fn place_order(ctx: &ReducerContext, sku: String, qty: u32) -> Result<(), String> {
    if sku.is_empty() || qty == 0 || qty > 1000 {
        return Err("need a sku and 1..=1000 qty".into());
    }
    let order = ctx.db.order().insert(Order {
        id: 0,
        sku: sku.clone(),
        qty,
        status: "pending".into(),
        note: String::new(),
        placed: ctx.timestamp,
        settled: None,
    });
    idc::send(
        ctx,
        WAREHOUSE,
        "reserve",
        json!({ "order_id": order.id, "sku": sku, "qty": qty }),
    );
    Ok(())
}

/// Called by `idc` for every message, inside the receiving transaction.
pub fn on_idc_message(
    ctx: &ReducerContext,
    _from: &str,
    kind: &str,
    payload: &Value,
) -> Result<(), String> {
    match kind {
        "reservation" => {
            let id = payload["order_id"]
                .as_u64()
                .ok_or("reservation: missing order_id")?;
            let Some(mut order) = ctx.db.order().id().find(id) else {
                return Err(format!("no order {id}"));
            };
            order.status = if payload["ok"].as_bool() == Some(true) {
                "confirmed"
            } else {
                "rejected"
            }
            .into();
            order.note = payload["note"].as_str().unwrap_or_default().into();
            order.settled = Some(ctx.timestamp);
            ctx.db.order().id().update(order);
            Ok(())
        }
        "stock" => {
            let sku = payload["sku"]
                .as_str()
                .ok_or("stock: missing sku")?
                .to_string();
            let qty = payload["qty"].as_u64().ok_or("stock: missing qty")? as u32;
            let row = StockMirror {
                sku: sku.clone(),
                qty,
                updated: ctx.timestamp,
            };
            if ctx.db.stock_mirror().sku().find(&sku).is_some() {
                ctx.db.stock_mirror().sku().update(row);
            } else {
                ctx.db.stock_mirror().insert(row);
            }
            Ok(())
        }
        other => Err(format!("shop doesn't handle `{other}` messages")),
    }
}

/// Request/response across databases: ask the warehouse directly and return its answer.
/// `spacetime call shop quote gear`
#[spacetimedb::procedure]
pub fn quote(ctx: &mut ProcedureContext, sku: String) -> String {
    match idc::rpc(ctx, WAREHOUSE, "/rpc/stock", json!({ "sku": sku })) {
        Ok(v) => v.to_string(),
        Err(e) => json!({ "error": e }).to_string(),
    }
}

/// SQL pull from the other database, the pre-HTTP-handler way. Works for public tables.
/// `spacetime call shop peek_warehouse`
#[spacetimedb::procedure]
pub fn peek_warehouse(ctx: &mut ProcedureContext) -> String {
    match idc::sql(ctx, WAREHOUSE, "SELECT * FROM stock") {
        Ok(v) => v.to_string(),
        Err(e) => json!({ "error": e }).to_string(),
    }
}

#[handler]
fn state(ctx: &mut HandlerContext, _req: Request) -> Response {
    let (orders, mirror) = ctx.with_tx(|tx| {
        let mut orders: Vec<_> = tx.db.order().iter().collect();
        orders.sort_by_key(|o| std::cmp::Reverse(o.id));
        let orders: Vec<Value> = orders
            .into_iter()
            .take(40)
            .map(|o| {
                let ms = o.settled.map(|s| (s.to_micros_since_unix_epoch() - o.placed.to_micros_since_unix_epoch()) as f64 / 1000.0);
                json!({ "id": o.id, "sku": o.sku, "qty": o.qty, "status": o.status, "note": o.note, "round_trip_ms": ms })
            })
            .collect();
        let mut mirror: Vec<Value> = tx.db.stock_mirror().iter().map(|s| json!({ "sku": s.sku, "qty": s.qty })).collect();
        mirror.sort_by(|a, b| a["sku"].as_str().cmp(&b["sku"].as_str()));
        (orders, mirror)
    });
    dashboard::state_response(ctx, json!({ "orders": orders, "stock_mirror": mirror }))
}

#[router]
fn routes() -> Router {
    idc::router()
        .merge(dashboard::router())
        .get("/api/state", state)
}
