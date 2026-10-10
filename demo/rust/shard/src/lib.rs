//! **shard**: one strip of a game world. Publish it twice (e.g. `shard-a` and `shard-b`) and
//! point each at the other. Characters walk across the border with a [`handoff`]: the source
//! locks the character, the target imports it as pending, the source lets go, and the target
//! makes it live. A character is never on both shards at once, and never on neither.

#[path = "../../../dashboard/dashboard.rs"]
mod dashboard;
#[path = "../../../../rust/handoff.rs"]
pub mod handoff;
#[path = "../../../../rust/idc.rs"]
pub mod idc;

use handoff::handoff as _;
use idc::*;
use serde_json::{Value, json};
use spacetimedb::http::{HandlerContext, Request, Response, Router, handler, router};
use spacetimedb::{ReducerContext, Table, Timestamp};
use std::time::Duration;

/// Each shard is `WIDTH` columns of the world. The shard whose name sorts first is the west
/// half (x 0..8), the other one the east half (x 8..16).
const WIDTH: i32 = 8;
const HEIGHT: i32 = 5;
const LIVE: &str = "live";
const PENDING: &str = "pending";

#[derive(Clone)]
#[spacetimedb::table(accessor = character, public)]
pub struct Character {
    #[primary_key]
    pub name: String,
    /// World coordinates, not shard-local ones.
    pub x: i32,
    pub y: i32,
    pub hp: u32,
    pub gold: u32,
    pub bag: Vec<String>,
    /// `live`, or `pending` while it's arriving and the source hasn't let go yet.
    pub state: String,
    pub note: String,
    pub updated: Timestamp,
}

#[spacetimedb::table(accessor = shard_config)]
pub struct ShardConfig {
    #[primary_key]
    pub key: u8,
    /// Arrivals are refused once this many characters are here.
    pub capacity: u32,
}

#[spacetimedb::reducer(init)]
pub fn init(ctx: &ReducerContext) {
    ctx.db.shard_config().insert(ShardConfig {
        key: 0,
        capacity: 50,
    });
    idc::init(ctx);
    handoff::init(ctx);
}

/// The other shard: the first entry in `IDC_PEERS`.
fn neighbour(ctx: &ReducerContext) -> Result<String, String> {
    idc::peers(&ctx.env.IDC_PEERS())
        .into_iter()
        .next()
        .map(|p| p.name)
        .ok_or_else(|| "no neighbouring shard configured".into())
}

/// The world columns this shard owns: `lo..lo + WIDTH`.
fn lo(ctx: &ReducerContext) -> i32 {
    let me = ctx.env.IDC_SELF();
    match neighbour(ctx) {
        Ok(other) if other < me => WIDTH,
        _ => 0,
    }
}

fn owns(ctx: &ReducerContext, x: i32) -> bool {
    (lo(ctx)..lo(ctx) + WIDTH).contains(&x)
}

fn live(ctx: &ReducerContext, name: &str) -> Result<Character, String> {
    let c = ctx
        .db
        .character()
        .name()
        .find(name.to_string())
        .ok_or_else(|| format!("no character `{name}` here"))?;
    if c.state != LIVE || handoff::is_locked(ctx, name) {
        return Err(format!("`{name}` is in transit"));
    }
    Ok(c)
}

fn data(c: &Character, x: i32) -> Value {
    json!({ "name": c.name, "x": x, "y": c.y, "hp": c.hp, "gold": c.gold, "bag": c.bag })
}

#[spacetimedb::reducer]
pub fn spawn(ctx: &ReducerContext, name: String) -> Result<(), String> {
    if name.is_empty() || name.len() > 24 {
        return Err("name must be 1-24 characters".into());
    }
    if ctx.db.character().name().find(&name).is_some() {
        return Err(format!("`{name}` already exists here"));
    }
    ctx.db.character().insert(Character {
        name,
        x: lo(ctx) + WIDTH / 2,
        y: HEIGHT / 2,
        hp: 100,
        gold: 10,
        bag: vec!["torch".into()],
        state: LIVE.into(),
        note: String::new(),
        updated: ctx.timestamp,
    });
    Ok(())
}

/// Walk one square. Stepping over the border starts a handoff to the neighbouring shard;
/// the character waits at the border, locked, until the neighbour has it.
#[spacetimedb::reducer]
pub fn step(ctx: &ReducerContext, name: String, dx: i32, dy: i32) -> Result<(), String> {
    if dx.abs() > 1 || dy.abs() > 1 {
        return Err("one square at a time".into());
    }
    let mut c = live(ctx, &name)?;
    let (x, y) = (c.x + dx, (c.y + dy).clamp(0, HEIGHT - 1));
    if owns(ctx, x) {
        c.x = x;
        c.y = y;
        c.note = String::new();
        c.updated = ctx.timestamp;
        ctx.db.character().name().update(c);
        return Ok(());
    }
    if !(0..2 * WIDTH).contains(&x) {
        return Err("that's the edge of the world".into());
    }
    c.y = y;
    let peer = neighbour(ctx)?;
    handoff::start(ctx, &peer, &name, &data(&c, x))?;
    c.note = format!("crossing to {peer}…");
    c.updated = ctx.timestamp;
    ctx.db.character().name().update(c);
    Ok(())
}

/// Send a character to the neighbour without walking (it lands on the nearest column there).
/// `timeout_ms` = 0 uses the default.
#[spacetimedb::reducer]
pub fn transfer(ctx: &ReducerContext, name: String, timeout_ms: u64) -> Result<(), String> {
    let c = live(ctx, &name)?;
    let peer = neighbour(ctx)?;
    let x = if lo(ctx) == 0 { WIDTH } else { WIDTH - 1 };
    let timeout = match timeout_ms {
        0 => handoff::DEFAULT_TIMEOUT,
        ms => Duration::from_millis(ms),
    };
    handoff::start_with_timeout(ctx, &peer, &name, &data(&c, x), timeout)?;
    Ok(())
}

#[spacetimedb::reducer]
pub fn cancel_transfer(ctx: &ReducerContext, name: String) -> Result<(), String> {
    let h = handoff::latest(ctx, &name)
        .filter(|h| h.role == "out")
        .ok_or_else(|| format!("`{name}` isn't leaving"))?;
    handoff::cancel(ctx, &h.id, "cancelled by player")
}

/// Trading needs both characters live and here. Locked ones can't trade.
#[spacetimedb::reducer]
pub fn give(ctx: &ReducerContext, from: String, to: String, gold: u32) -> Result<(), String> {
    let mut a = live(ctx, &from)?;
    let mut b = live(ctx, &to)?;
    if from == to || a.gold < gold {
        return Err("not enough gold".into());
    }
    a.gold -= gold;
    b.gold += gold;
    ctx.db.character().name().update(a);
    ctx.db.character().name().update(b);
    Ok(())
}

#[spacetimedb::reducer]
pub fn set_capacity(ctx: &ReducerContext, capacity: u32) {
    ctx.db
        .shard_config()
        .key()
        .update(ShardConfig { key: 0, capacity });
}

// ---------------------------------------------------------------------------
// Handoff hooks
// ---------------------------------------------------------------------------

pub fn handoff_validate(
    ctx: &ReducerContext,
    _from: &str,
    entity: &str,
    data: &Value,
) -> Result<(), String> {
    let capacity = ctx
        .db
        .shard_config()
        .key()
        .find(0)
        .map_or(0, |c| c.capacity);
    if ctx.db.character().count() >= capacity as u64 {
        return Err(format!("{} is full", ctx.env.IDC_SELF()));
    }
    if ctx.db.character().name().find(entity.to_string()).is_some() {
        return Err(format!(
            "the name `{entity}` is taken on {}",
            ctx.env.IDC_SELF()
        ));
    }
    if data["name"].as_str() != Some(entity) {
        return Err("data doesn't match the entity id".into());
    }
    let x = data["x"].as_i64().ok_or("missing x")? as i32;
    if !owns(ctx, x) {
        return Err(format!("x = {x} isn't on {}", ctx.env.IDC_SELF()));
    }
    if data["hp"].as_u64() == Some(0) {
        return Err("fallen characters can't travel".into());
    }
    Ok(())
}

pub fn handoff_import(ctx: &ReducerContext, from: &str, entity: &str, data: &Value) {
    ctx.db.character().insert(Character {
        name: entity.to_string(),
        x: data["x"].as_i64().unwrap_or_default() as i32,
        y: data["y"].as_i64().unwrap_or_default() as i32,
        hp: data["hp"].as_u64().unwrap_or_default() as u32,
        gold: data["gold"].as_u64().unwrap_or_default() as u32,
        bag: data["bag"]
            .as_array()
            .map(|b| {
                b.iter()
                    .filter_map(|i| i.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default(),
        state: PENDING.into(),
        note: format!("arriving from {from}…"),
        updated: ctx.timestamp,
    });
}

pub fn handoff_activate(ctx: &ReducerContext, entity: &str) {
    if let Some(mut c) = ctx.db.character().name().find(entity.to_string()) {
        c.state = LIVE.into();
        c.note = String::new();
        c.updated = ctx.timestamp;
        ctx.db.character().name().update(c);
    }
}

pub fn handoff_discard(ctx: &ReducerContext, entity: &str) {
    if ctx
        .db
        .character()
        .name()
        .find(entity.to_string())
        .is_some_and(|c| c.state == PENDING)
    {
        ctx.db.character().name().delete(entity.to_string());
    }
}

pub fn handoff_remove(ctx: &ReducerContext, entity: &str) {
    ctx.db.character().name().delete(entity.to_string());
}

pub fn handoff_returned(ctx: &ReducerContext, entity: &str, reason: &str) {
    if let Some(mut c) = ctx.db.character().name().find(entity.to_string()) {
        c.note = format!("stayed: {reason}");
        c.updated = ctx.timestamp;
        ctx.db.character().name().update(c);
    }
}

pub fn on_idc_message(
    ctx: &ReducerContext,
    from: &str,
    kind: &str,
    payload: &Value,
) -> Result<(), String> {
    if let Some(result) = handoff::on_idc_message(ctx, from, kind, payload) {
        return result;
    }
    Err(format!("shard doesn't handle `{kind}` messages"))
}

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

#[handler]
fn api_state(ctx: &mut HandlerContext, _req: Request) -> Response {
    let app = ctx.with_tx(|tx| {
        let mut chars: Vec<Character> = tx.db.character().iter().collect();
        chars.sort_by(|a, b| a.name.cmp(&b.name));
        let chars: Vec<Value> = chars
            .into_iter()
            .map(|c| {
                json!({
                    "name": c.name, "x": c.x, "y": c.y, "hp": c.hp, "gold": c.gold, "bag": c.bag,
                    "state": c.state, "note": c.note, "locked": handoff::is_locked(tx, &c.name),
                })
            })
            .collect();
        let mut handoffs: Vec<handoff::Handoff> = tx.db.handoff().iter().collect();
        handoffs.sort_by_key(|h| std::cmp::Reverse(h.updated));
        let handoffs: Vec<Value> = handoffs
            .into_iter()
            .map(|h| {
                json!({
                    "id": h.id, "entity": h.entity, "role": h.role, "peer": h.peer, "status": h.status,
                    "detail": h.detail, "ms": (h.updated.to_micros_since_unix_epoch() - h.started.to_micros_since_unix_epoch()) as f64 / 1000.0,
                })
            })
            .collect();
        let lo = lo(tx);
        json!({
            "shard": { "lo": lo, "hi": lo + WIDTH, "height": HEIGHT,
                       "capacity": tx.db.shard_config().key().find(0).map_or(0, |c| c.capacity) },
            "characters": chars, "handoffs": handoffs,
        })
    });
    dashboard::state_response(ctx, app)
}

#[router]
fn routes() -> Router {
    idc::router()
        .merge(dashboard::shards_router())
        .get("/api/state", api_state)
}
