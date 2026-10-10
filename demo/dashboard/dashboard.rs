//! A small live dashboard both modules serve from their own HTTP routes. It shows
//! both databases side by side and the messages flowing between them.

use crate::idc::{self, *};
use http::{StatusCode, header};
use serde_json::{Value, json};
use spacetimedb::Table;
use spacetimedb::http::{Body, HandlerContext, Request, Response, Router, handler};

const PAGE: &str = include_str!("dashboard.html");
const SHARDS_PAGE: &str = include_str!("shards.html");

fn base_path(req: &Request) -> String {
    let path = req.uri().path();
    path.find("/route")
        .map_or(String::new(), |i| path[..i].to_string())
}

#[handler]
fn page(ctx: &mut HandlerContext, req: Request) -> Response {
    render(ctx, &req, PAGE)
}

#[handler]
fn shards_page(ctx: &mut HandlerContext, req: Request) -> Response {
    render(ctx, &req, SHARDS_PAGE)
}

fn render(ctx: &mut HandlerContext, req: &Request, html: &str) -> Response {
    let me = ctx.env.IDC_SELF();
    let mut dbs = vec![json!({ "name": me, "base": base_path(req) })];
    for p in idc::peers(&ctx.env.IDC_PEERS()) {
        dbs.push(json!({ "name": p.name, "base": p.base }));
    }
    // Keep the order stable (shop first) no matter which database served the page.
    dbs.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
    let config = json!({ "self": me, "dbs": dbs })
        .to_string()
        .replace('<', "\\u003c");
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Body::from_bytes(html.replace("/*CONFIG*/{}", &config)))
        .unwrap()
}

/// App state plus this database's IDC view, as JSON for the dashboard.
pub fn state_response(ctx: &mut HandlerContext, app: Value) -> Response {
    let idc_state = ctx.with_tx(|tx| {
        let mut log: Vec<_> = tx.db.idc_log().iter().collect();
        log.sort_by_key(|l| std::cmp::Reverse(l.id));
        let log: Vec<Value> = log
            .into_iter()
            .take(40)
            .map(|l| {
                json!({
                    "id": l.id, "at_us": l.at.to_micros_since_unix_epoch(), "direction": l.direction,
                    "peer": l.peer, "kind": l.kind, "transport": l.transport, "msg_id": l.msg_id,
                    "event": l.event, "detail": l.detail, "latency_us": l.latency_us,
                })
            })
            .collect();
        let pending = tx.db.idc_outbox().iter().filter(|o| !o.dead).count();
        let dead = tx.db.idc_outbox().iter().filter(|o| o.dead).count();
        let paired_with: Vec<String> = tx.db.idc_peer_token().iter().map(|t| t.peer).collect();
        let known: Vec<String> = tx.db.idc_known_peer().iter().map(|k| k.name).collect();
        json!({
            "self": tx.env.IDC_SELF(), "transport": tx.env.IDC_TRANSPORT(),
            "outbox_pending": pending, "outbox_dead": dead,
            "has_token_for": paired_with, "trusts": known, "log": log,
        })
    });
    let mut body = app;
    body["idc"] = idc_state;
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::from_bytes(body.to_string()))
        .unwrap()
}

#[allow(dead_code)]
pub fn router() -> Router {
    Router::new().get("", page).get("/", page)
}

/// The two-shard handoff demo page.
#[allow(dead_code)]
pub fn shards_router() -> Router {
    Router::new().get("", shards_page).get("/", shards_page)
}
