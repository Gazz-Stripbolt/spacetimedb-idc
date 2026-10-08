//! # idc.rs: inter-database communication for SpacetimeDB, today
//!
//! Drop this file into a module with `#[path = "../../idc/idc.rs"] pub mod idc;`
//! and implement one function in the crate root:
//!
//! ```ignore
//! pub fn on_idc_message(ctx: &ReducerContext, from: &str, kind: &str, payload: &serde_json::Value) -> Result<(), String>
//! ```
//!
//! What it gives you:
//!
//! - **Transactional outbox.** [`send`] writes the message in the *same transaction*
//!   as your business change, so a message exists if and only if the change committed.
//! - **Push, not poll.** Every send schedules a one-shot procedure for "now" that
//!   delivers over HTTP. The peer reacts the moment the request lands.
//! - **Two transports**, picked with the `IDC_TRANSPORT` env var:
//!   - `route`: POST to the peer's HTTP handler `/route/idc/inbox`, signed with HMAC-SHA256.
//!   - `reducer`: call the peer's `idc_receive` reducer through `/call`, as an identity
//!     the peer knows. This is the "secrets table + known identities" approach, with the
//!     pairing handshake that fills both tables automated.
//! - **At-least-once delivery, exactly-once effect.** Retries with exponential backoff,
//!   per-peer ordering, and an idempotent inbox that drops duplicates.
//! - **Request/response** with [`rpc`] and **SQL pulls** with [`sql`] for the times you
//!   need an answer right away.
//!
//! Configuration is owner-only by construction: it all comes from environment variables.

use hmac::{Hmac, Mac};
use http::{StatusCode, header};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::Sha256;
use spacetimedb::http::{Body, HandlerContext, Request, Response, Router, Timeout, handler};
use spacetimedb::{
    Identity, ProcedureContext, ReducerContext, ScheduleAt, Table, TimeDuration, Timestamp,
};
use std::time::Duration;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#[spacetimedb::env]
pub struct Env {
    /// This database's name in the mesh, e.g. `shop`.
    pub IDC_SELF: String,
    /// Shared HMAC secret for the mesh (same value on every peer).
    pub IDC_SECRET: String,
    /// Comma-separated `name=base_url` pairs, where `base_url` is the peer's
    /// `https://host/v1/database/<name_or_identity>`.
    pub IDC_PEERS: String,
    /// `route` (signed HTTP handler) or `reducer` (identity token + `/call`).
    #[env(values("route", "reducer"))]
    pub IDC_TRANSPORT: String,
}

/// How long a signed request stays valid. Replays inside the window are caught by the inbox.
const MAX_CLOCK_SKEW_MICROS: i64 = 5 * 60 * 1_000_000;
/// How long a claimed outbox message stays reserved by one flusher.
const LEASE: Duration = Duration::from_secs(60);
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);
/// Max messages per HTTP request on the `route` transport.
const BATCH: usize = 64;
const MAX_BACKOFF_MS: u64 = 60_000;
const LOG_KEEP: u64 = 300;
const SEEN_RETENTION: Duration = Duration::from_secs(7 * 24 * 3600);

#[derive(Clone, Debug)]
pub struct Peer {
    pub name: String,
    /// `https://host/v1/database/<db>`
    pub base: String,
}

impl Peer {
    /// `https://host`: where `/v1/identity` lives.
    pub fn host(&self) -> &str {
        self.base
            .find("/v1/")
            .map_or(self.base.as_str(), |i| &self.base[..i])
    }
}

pub fn peers(raw: &str) -> Vec<Peer> {
    raw.split(',')
        .filter_map(|entry| {
            let (name, base) = entry.trim().split_once('=')?;
            Some(Peer {
                name: name.trim().to_string(),
                base: base.trim().trim_end_matches('/').to_string(),
            })
        })
        .collect()
}

fn find_peer(raw: &str, name: &str) -> Result<Peer, String> {
    peers(raw)
        .into_iter()
        .find(|p| p.name == name)
        .ok_or_else(|| format!("unknown peer `{name}`"))
}

// ---------------------------------------------------------------------------
// Tables
// ---------------------------------------------------------------------------

/// Messages waiting to be delivered. Private: payloads never leak to subscribers.
#[derive(Clone)]
#[spacetimedb::table(accessor = idc_outbox)]
pub struct IdcOutbox {
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    #[index(btree)]
    pub peer: String,
    pub kind: String,
    pub payload: String,
    pub created: Timestamp,
    pub attempts: u32,
    pub next_attempt: Timestamp,
    /// Set while one flusher is delivering this message.
    pub in_flight_until: Option<Timestamp>,
    /// Permanently rejected by the peer (4xx). Kept for inspection, never retried.
    pub dead: bool,
    pub last_error: String,
}

/// Message ids we've already applied: the idempotent inbox.
#[spacetimedb::table(accessor = idc_seen)]
pub struct IdcSeen {
    #[primary_key]
    pub key: String,
    #[index(btree)]
    pub at: Timestamp,
}

/// Identities allowed to call `idc_receive`, filled in by the pairing handshake.
#[spacetimedb::table(accessor = idc_known_peer)]
pub struct IdcKnownPeer {
    #[primary_key]
    pub identity: Identity,
    #[index(btree)]
    pub name: String,
    pub paired_at: Timestamp,
}

/// Our credentials on each peer, filled in by the pairing handshake.
#[spacetimedb::table(accessor = idc_peer_token)]
pub struct IdcPeerToken {
    #[primary_key]
    pub peer: String,
    pub identity: Identity,
    pub token: String,
    pub paired_at: Timestamp,
}

/// Random-ish per-database epoch, so message ids stay unique even if the
/// database is wiped and its auto-increment ids start over.
#[spacetimedb::table(accessor = idc_state)]
pub struct IdcState {
    #[primary_key]
    pub key: u8,
    pub epoch: String,
}

/// Public activity feed: what went where, when, and how fast. Payloads aren't included.
#[spacetimedb::table(accessor = idc_log, public)]
pub struct IdcLog {
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    pub at: Timestamp,
    /// `out` or `in`
    pub direction: String,
    pub peer: String,
    pub kind: String,
    pub transport: String,
    pub msg_id: String,
    /// queued / delivered / retry / dead / applied / duplicate / paired / ...
    pub event: String,
    pub detail: String,
    /// Queue-to-delivery (out) or send-to-apply (in), in microseconds. 0 when not applicable.
    pub latency_us: i64,
}

#[spacetimedb::table(accessor = idc_flush_job, scheduled(idc_flush))]
pub struct IdcFlushJob {
    #[primary_key]
    #[auto_inc]
    pub scheduled_id: u64,
    pub scheduled_at: ScheduleAt,
}

#[spacetimedb::table(accessor = idc_pair_job, scheduled(idc_pair))]
pub struct IdcPairJob {
    #[primary_key]
    #[auto_inc]
    pub scheduled_id: u64,
    pub scheduled_at: ScheduleAt,
    pub attempt: u32,
}

#[spacetimedb::table(accessor = idc_prune_job, scheduled(idc_prune))]
pub struct IdcPruneJob {
    #[primary_key]
    #[auto_inc]
    pub scheduled_id: u64,
    pub scheduled_at: ScheduleAt,
}

// ---------------------------------------------------------------------------
// Wire format
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Envelope {
    /// Globally unique: `<sender epoch>-<outbox id>`.
    pub id: String,
    pub from: String,
    pub to: String,
    pub kind: String,
    pub payload: Value,
    /// Sender clock, microseconds since the Unix epoch.
    pub sent_at: i64,
}

type HmacSha256 = Hmac<Sha256>;

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    s.len()
        .is_multiple_of(2)
        .then(|| {
            (0..s.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
                .collect()
        })
        .flatten()
}

/// `t=<micros>,v1=<hex hmac-sha256(secret, "<micros>.<body>")>`
pub fn sign(secret: &str, now: Timestamp, body: &[u8]) -> String {
    let t = now.to_micros_since_unix_epoch();
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("any key length works");
    mac.update(t.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);
    format!("t={t},v1={}", hex(&mac.finalize().into_bytes()))
}

/// Checks the signature (constant time) and the timestamp window.
pub fn verify(
    secret: &str,
    signature: Option<&str>,
    body: &[u8],
    now: Timestamp,
) -> Result<(), String> {
    let signature = signature.ok_or("missing X-IDC-Signature")?;
    let mut t = None;
    let mut v1 = None;
    for part in signature.split(',') {
        match part.split_once('=') {
            Some(("t", v)) => t = v.parse::<i64>().ok(),
            Some(("v1", v)) => v1 = unhex(v),
            _ => {}
        }
    }
    let (t, v1) = t.zip(v1).ok_or("malformed X-IDC-Signature")?;
    if (now.to_micros_since_unix_epoch() - t).abs() > MAX_CLOCK_SKEW_MICROS {
        return Err("signature timestamp outside the allowed window".into());
    }
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("any key length works");
    mac.update(t.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);
    mac.verify_slice(&v1)
        .map_err(|_| "bad signature".to_string())
}

const SIGNATURE_HEADER: &str = "x-idc-signature";

// ---------------------------------------------------------------------------
// Sending
// ---------------------------------------------------------------------------

/// Call from your module's `init` reducer.
pub fn init(ctx: &ReducerContext) {
    if ctx.db.idc_state().key().find(0).is_none() {
        let epoch = format!("{:x}", ctx.timestamp.to_micros_since_unix_epoch());
        ctx.db.idc_state().insert(IdcState { key: 0, epoch });
    }
    ctx.db.idc_pair_job().insert(IdcPairJob {
        scheduled_id: 0,
        scheduled_at: ScheduleAt::Time(ctx.timestamp),
        attempt: 0,
    });
    ctx.db.idc_prune_job().insert(IdcPruneJob {
        scheduled_id: 0,
        scheduled_at: ScheduleAt::Interval(TimeDuration::from_duration(Duration::from_secs(3600))),
    });
}

/// Queue `payload` for `peer` in the current transaction. Delivery starts as soon as it commits.
pub fn send(ctx: &ReducerContext, peer: &str, kind: &str, payload: Value) -> String {
    let row = ctx.db.idc_outbox().insert(IdcOutbox {
        id: 0,
        peer: peer.to_string(),
        kind: kind.to_string(),
        payload: payload.to_string(),
        created: ctx.timestamp,
        attempts: 0,
        next_attempt: ctx.timestamp,
        in_flight_until: None,
        dead: false,
        last_error: String::new(),
    });
    let msg_id = msg_id(ctx, row.id);
    log(
        ctx,
        "out",
        peer,
        kind,
        &transport(ctx),
        &msg_id,
        "queued",
        "",
        0,
    );
    schedule_flush(ctx, ctx.timestamp);
    msg_id
}

fn msg_id(ctx: &ReducerContext, outbox_id: u64) -> String {
    let epoch = ctx
        .db
        .idc_state()
        .key()
        .find(0)
        .map(|s| s.epoch)
        .unwrap_or_default();
    format!("{epoch}-{outbox_id}")
}

fn transport(ctx: &ReducerContext) -> String {
    ctx.env.IDC_TRANSPORT()
}

/// Make sure a flush is scheduled at or before `at`, without piling up duplicate jobs.
fn schedule_flush(ctx: &ReducerContext, at: Timestamp) {
    let covered = ctx
        .db
        .idc_flush_job()
        .iter()
        .any(|j| matches!(j.scheduled_at, ScheduleAt::Time(t) if t <= at));
    if !covered {
        ctx.db.idc_flush_job().insert(IdcFlushJob {
            scheduled_id: 0,
            scheduled_at: ScheduleAt::Time(at),
        });
    }
}

/// Re-run pairing and flushing, e.g. after changing `IDC_PEERS` with `spacetime publish --env-only`.
#[spacetimedb::reducer]
pub fn idc_kick(ctx: &ReducerContext) {
    ctx.db.idc_pair_job().insert(IdcPairJob {
        scheduled_id: 0,
        scheduled_at: ScheduleAt::Time(ctx.timestamp),
        attempt: 0,
    });
    schedule_flush(ctx, ctx.timestamp);
}

struct Claimed {
    row: IdcOutbox,
    msg_id: String,
    sent_at: Timestamp,
}

/// Claim the next batch: consecutive due messages from the head of one peer's queue.
/// Per-peer order is kept because a peer's head message blocks the rest of its
/// queue until it's delivered or dead.
fn claim_batch(ctx: &ReducerContext, max: usize) -> Vec<Claimed> {
    let now = ctx.timestamp;
    let mut rows: Vec<IdcOutbox> = ctx.db.idc_outbox().iter().filter(|r| !r.dead).collect();
    rows.sort_by_key(|r| r.id);
    let eligible =
        |r: &IdcOutbox| !r.in_flight_until.is_some_and(|t| t > now) && r.next_attempt <= now;
    let mut seen_peers: Vec<&str> = Vec::new();
    let mut peer: Option<String> = None;
    for row in &rows {
        if seen_peers.contains(&row.peer.as_str()) {
            continue;
        }
        seen_peers.push(&row.peer);
        if eligible(row) {
            peer = Some(row.peer.clone());
            break;
        }
    }
    let Some(peer) = peer else { return Vec::new() };
    let mut batch = Vec::new();
    for row in rows.into_iter().filter(|r| r.peer == peer) {
        if batch.len() == max || !eligible(&row) {
            break;
        }
        let mut leased = row.clone();
        leased.in_flight_until = Some(now + LEASE);
        ctx.db.idc_outbox().id().update(leased);
        batch.push(Claimed {
            msg_id: msg_id(ctx, row.id),
            row,
            sent_at: now,
        });
    }
    batch
}

enum Delivery {
    Ok(String),
    Retry(String),
    Dead(String),
}

/// Scheduled procedure: deliver everything that's due, then reschedule for the next retry.
#[spacetimedb::procedure]
pub fn idc_flush(ctx: &mut ProcedureContext, _job: IdcFlushJob) {
    let max = if ctx.with_tx(|tx| tx.env.IDC_TRANSPORT()) == "reducer" {
        1
    } else {
        BATCH
    };
    for _ in 0..200 {
        let batch = ctx.with_tx(|tx| claim_batch(tx, max));
        if batch.is_empty() {
            break;
        }
        let results = deliver(ctx, &batch);
        ctx.with_tx(|tx| {
            for (claimed, result) in batch.iter().zip(&results) {
                finish(tx, claimed, result);
            }
        });
    }
    ctx.with_tx(|tx| {
        let next = tx
            .db
            .idc_outbox()
            .iter()
            .filter(|r| !r.dead)
            .map(|r| {
                r.in_flight_until
                    .map_or(r.next_attempt, |t| t.max(r.next_attempt))
            })
            .min();
        if let Some(at) = next {
            schedule_flush(tx, at.max(tx.timestamp));
        }
    });
}

fn finish(ctx: &ReducerContext, claimed: &Claimed, result: &Delivery) {
    let Some(mut row) = ctx.db.idc_outbox().id().find(claimed.row.id) else {
        return;
    };
    let transport = transport(ctx);
    let latency =
        ctx.timestamp.to_micros_since_unix_epoch() - row.created.to_micros_since_unix_epoch();
    match result {
        Delivery::Ok(detail) => {
            ctx.db.idc_outbox().id().delete(row.id);
            log(
                ctx,
                "out",
                &row.peer,
                &row.kind,
                &transport,
                &claimed.msg_id,
                "delivered",
                detail,
                latency,
            );
        }
        Delivery::Retry(err) => {
            if err.contains("is not a known peer") {
                // The peer forgot us (e.g. its data was reset): pair again.
                ctx.db.idc_peer_token().peer().delete(&row.peer);
                ctx.db.idc_pair_job().insert(IdcPairJob {
                    scheduled_id: 0,
                    scheduled_at: ScheduleAt::Time(ctx.timestamp),
                    attempt: 0,
                });
            }
            row.attempts += 1;
            let backoff_ms = (250u64 << row.attempts.min(16)).min(MAX_BACKOFF_MS);
            row.next_attempt = ctx.timestamp + Duration::from_millis(backoff_ms);
            row.in_flight_until = None;
            row.last_error = err.clone();
            let detail = format!(
                "attempt {} failed, retrying in {backoff_ms} ms: {err}",
                row.attempts
            );
            log(
                ctx,
                "out",
                &row.peer,
                &row.kind,
                &transport,
                &claimed.msg_id,
                "retry",
                &detail,
                0,
            );
            ctx.db.idc_outbox().id().update(row);
        }
        Delivery::Dead(err) => {
            row.dead = true;
            row.in_flight_until = None;
            row.last_error = err.clone();
            log(
                ctx,
                "out",
                &row.peer,
                &row.kind,
                &transport,
                &claimed.msg_id,
                "dead",
                err,
                0,
            );
            ctx.db.idc_outbox().id().update(row);
        }
    }
}

/// Deliver a batch (all for the same peer). Returns one outcome per message.
fn deliver(ctx: &mut ProcedureContext, batch: &[Claimed]) -> Vec<Delivery> {
    let peer_name = batch[0].row.peer.clone();
    let (me, secret, raw_peers, transport, token) = ctx.with_tx(|tx| {
        let token = tx
            .db
            .idc_peer_token()
            .peer()
            .find(&peer_name)
            .map(|t| t.token);
        (
            tx.env.IDC_SELF(),
            tx.env.IDC_SECRET(),
            tx.env.IDC_PEERS(),
            tx.env.IDC_TRANSPORT(),
            token,
        )
    });
    let all = |d: fn(String) -> Delivery, msg: String| {
        batch.iter().map(|_| d(msg.clone())).collect::<Vec<_>>()
    };
    let peer = match find_peer(&raw_peers, &peer_name) {
        Ok(p) => p,
        Err(e) => return all(Delivery::Retry, e),
    };
    let envelopes: Vec<Envelope> = batch
        .iter()
        .map(|c| Envelope {
            id: c.msg_id.clone(),
            from: me.clone(),
            to: peer.name.clone(),
            kind: c.row.kind.clone(),
            payload: serde_json::from_str(&c.row.payload).unwrap_or(Value::Null),
            sent_at: c.sent_at.to_micros_since_unix_epoch(),
        })
        .collect();
    let sent_at = batch[0].sent_at;

    let request = match transport.as_str() {
        "reducer" => {
            let Some(token) = token else {
                return all(Delivery::Retry, "not paired with peer yet".into());
            };
            // /call takes a JSON array of reducer arguments: here, one string.
            let envelope = serde_json::to_string(&envelopes[0]).expect("envelope serializes");
            let args = serde_json::to_string(&[envelope]).expect("args serialize");
            Request::builder()
                .method("POST")
                .uri(format!("{}/call/idc_receive", peer.base))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .extension(Timeout::from(TimeDuration::from_duration(HTTP_TIMEOUT)))
                .body(args)
        }
        _ => {
            let body = serde_json::to_string(&envelopes).expect("envelopes serialize");
            Request::builder()
                .method("POST")
                .uri(format!("{}/route/idc/inbox", peer.base))
                .header(header::CONTENT_TYPE, "application/json")
                .header(SIGNATURE_HEADER, sign(&secret, sent_at, body.as_bytes()))
                .extension(Timeout::from(TimeDuration::from_duration(HTTP_TIMEOUT)))
                .body(body)
        }
    };
    let request = match request {
        Ok(r) => r,
        Err(e) => return all(Delivery::Dead, format!("bad request: {e}")),
    };
    let response = match ctx.http.send(request) {
        Ok(r) => r,
        Err(e) => return all(Delivery::Retry, format!("transport error: {e}")),
    };
    let status = response.status();
    let text = response.into_body().into_string_lossy();
    if !status.is_success() {
        let short: String = text.chars().take(300).collect();
        let detail = format!("{} {short}", status.as_u16());
        let permanent = status.is_client_error()
            && ![
                StatusCode::UNAUTHORIZED,
                StatusCode::NOT_FOUND,
                StatusCode::REQUEST_TIMEOUT,
                StatusCode::TOO_MANY_REQUESTS,
            ]
            .contains(&status);
        // 4xx means the peer understood and refused: retrying won't help.
        // 401/404 stay retryable (not paired yet, or the peer isn't published yet).
        return if permanent {
            all(Delivery::Dead, detail)
        } else {
            all(Delivery::Retry, detail)
        };
    }
    if transport == "reducer" {
        return all(Delivery::Ok, format!("{} via idc_receive", status.as_u16()));
    }
    // Route transport: one result per envelope, in order.
    let results: Vec<Value> = serde_json::from_str::<Value>(&text)
        .ok()
        .and_then(|v| v["results"].as_array().cloned())
        .unwrap_or_default();
    batch
        .iter()
        .enumerate()
        .map(|(i, _)| match results.get(i) {
            Some(r) if r["ok"].as_bool() == Some(true) => {
                let dup = if r["duplicate"].as_bool() == Some(true) {
                    " (duplicate)"
                } else {
                    ""
                };
                Delivery::Ok(format!("{} batch of {}{dup}", status.as_u16(), batch.len()))
            }
            Some(r) => Delivery::Dead(format!("rejected: {}", r["error"].as_str().unwrap_or("?"))),
            None => Delivery::Retry("peer returned no result for this message".into()),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Receiving
// ---------------------------------------------------------------------------

/// Apply an incoming message exactly once. Runs inside the caller's transaction, so
/// the dedupe record and your business change commit (or roll back) together.
/// Returns `true` for a duplicate.
fn receive(ctx: &ReducerContext, env: &Envelope, transport: &str) -> Result<bool, String> {
    let me = ctx.env.IDC_SELF();
    if env.to != me {
        return Err(format!("message addressed to `{}`, this is `{me}`", env.to));
    }
    let key = format!("{}|{}", env.from, env.id);
    if ctx.db.idc_seen().key().find(&key).is_some() {
        log(
            ctx,
            "in",
            &env.from,
            &env.kind,
            transport,
            &env.id,
            "duplicate",
            "already applied",
            0,
        );
        return Ok(true);
    }
    ctx.db.idc_seen().insert(IdcSeen {
        key,
        at: ctx.timestamp,
    });
    crate::on_idc_message(ctx, &env.from, &env.kind, &env.payload)?;
    let latency = ctx.timestamp.to_micros_since_unix_epoch() - env.sent_at;
    log(
        ctx, "in", &env.from, &env.kind, transport, &env.id, "applied", "", latency,
    );
    Ok(false)
}

fn json_response(status: StatusCode, value: Value) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from_bytes(value.to_string()))
        .expect("valid response")
}

fn signature(req: &Request) -> Option<String> {
    req.headers()
        .get(SIGNATURE_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

/// Transport `route`: signed POST from a peer, either one envelope or a batch (array).
/// Each envelope is applied in its own transaction, so one bad message doesn't
/// roll back the others; the response has one result per envelope.
#[handler]
pub fn idc_inbox(ctx: &mut HandlerContext, req: Request) -> Response {
    let secret = ctx.env.IDC_SECRET();
    let sig = signature(&req);
    let body = req.into_body().into_bytes();
    if let Err(e) = verify(&secret, sig.as_deref(), &body, ctx.timestamp) {
        return json_response(StatusCode::UNAUTHORIZED, json!({ "error": e }));
    }
    let parsed: Result<Value, _> = serde_json::from_slice(&body);
    let (envelopes, single) = match parsed {
        Ok(Value::Array(items)) => (items, false),
        Ok(item) => (vec![item], true),
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                json!({ "error": format!("bad JSON: {e}") }),
            );
        }
    };
    let mut results = Vec::with_capacity(envelopes.len());
    for raw in envelopes {
        let result = match serde_json::from_value::<Envelope>(raw) {
            Err(e) => json!({ "ok": false, "error": format!("bad envelope: {e}") }),
            Ok(env) => match ctx.try_with_tx(|tx| receive(tx, &env, "route")) {
                Ok(duplicate) => json!({ "id": env.id, "ok": true, "duplicate": duplicate }),
                Err(e) => json!({ "id": env.id, "ok": false, "error": e }),
            },
        };
        results.push(result);
    }
    if single {
        let r = results.pop().unwrap();
        let status = if r["ok"] == true {
            StatusCode::OK
        } else {
            StatusCode::UNPROCESSABLE_ENTITY
        };
        let body = if r["ok"] == true {
            json!({ "ok": true, "duplicate": r["duplicate"] })
        } else {
            json!({ "error": r["error"] })
        };
        return json_response(status, body);
    }
    json_response(StatusCode::OK, json!({ "results": results }))
}

/// Transport `reducer`: the caller must be an identity we paired with.
#[spacetimedb::reducer]
pub fn idc_receive(ctx: &ReducerContext, envelope: String) -> Result<(), String> {
    let known = ctx
        .db
        .idc_known_peer()
        .identity()
        .find(ctx.sender())
        .ok_or_else(|| format!("{} is not a known peer", ctx.sender()))?;
    let env: Envelope =
        serde_json::from_str(&envelope).map_err(|e| format!("bad envelope: {e}"))?;
    if env.from != known.name {
        return Err(format!(
            "identity belongs to `{}`, envelope claims `{}`",
            known.name, env.from
        ));
    }
    receive(ctx, &env, "reducer").map(|_| ())
}

// ---------------------------------------------------------------------------
// Pairing: automates "secrets table + known identities" in both directions
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct PairRequest {
    from: String,
    identity: String,
}

/// Scheduled procedure, run at init and by `idc_kick`. For each peer we don't
/// hold a token for yet:
/// 1. mint a fresh identity + token on the peer's host (`POST /v1/identity`),
/// 2. send the identity to the peer's `/route/idc/pair`, signed with the mesh secret,
/// 3. keep the token. The peer now knows that identity as us.
#[spacetimedb::procedure]
pub fn idc_pair(ctx: &mut ProcedureContext, job: IdcPairJob) {
    let (me, secret, raw_peers) =
        ctx.with_tx(|tx| (tx.env.IDC_SELF(), tx.env.IDC_SECRET(), tx.env.IDC_PEERS()));
    let mut pending = false;
    for peer in peers(&raw_peers) {
        if ctx.with_tx(|tx| tx.db.idc_peer_token().peer().find(&peer.name).is_some()) {
            continue;
        }
        match pair_with(ctx, &me, &secret, &peer) {
            Ok((identity, token)) => ctx.with_tx(|tx| {
                tx.db.idc_peer_token().peer().delete(&peer.name);
                tx.db.idc_peer_token().insert(IdcPeerToken {
                    peer: peer.name.clone(),
                    identity,
                    token: token.clone(),
                    paired_at: tx.timestamp,
                });
                let detail = format!("we are {} on {}", identity.to_abbreviated_hex(), peer.name);
                log(
                    tx, "out", &peer.name, "pair", "reducer", "", "paired", &detail, 0,
                );
            }),
            Err(e) => {
                pending = true;
                ctx.with_tx(|tx| log(tx, "out", &peer.name, "pair", "reducer", "", "retry", &e, 0));
            }
        }
    }
    if pending {
        let attempt = job.attempt + 1;
        let backoff = Duration::from_millis((500u64 << attempt.min(7)).min(MAX_BACKOFF_MS));
        ctx.with_tx(|tx| {
            tx.db.idc_pair_job().insert(IdcPairJob {
                scheduled_id: 0,
                scheduled_at: ScheduleAt::Time(tx.timestamp + backoff),
                attempt,
            });
        });
    }
}

fn pair_with(
    ctx: &mut ProcedureContext,
    me: &str,
    secret: &str,
    peer: &Peer,
) -> Result<(Identity, String), String> {
    #[derive(Deserialize)]
    struct Minted {
        identity: String,
        token: String,
    }
    let mint = Request::builder()
        .method("POST")
        .uri(format!("{}/v1/identity", peer.host()))
        .extension(Timeout::from(TimeDuration::from_duration(HTTP_TIMEOUT)))
        .body(Body::empty())
        .map_err(|e| e.to_string())?;
    let response = ctx
        .http
        .send(mint)
        .map_err(|e| format!("mint identity: {e}"))?;
    if !response.status().is_success() {
        return Err(format!("mint identity: HTTP {}", response.status()));
    }
    let minted: Minted =
        serde_json::from_slice(&response.into_body().into_bytes()).map_err(|e| e.to_string())?;
    let identity = Identity::from_hex(&minted.identity).map_err(|e| e.to_string())?;

    let body = serde_json::to_string(&PairRequest {
        from: me.to_string(),
        identity: minted.identity,
    })
    .unwrap();
    let request = Request::builder()
        .method("POST")
        .uri(format!("{}/route/idc/pair", peer.base))
        .header(header::CONTENT_TYPE, "application/json")
        .header(
            SIGNATURE_HEADER,
            sign(secret, ctx.timestamp_now(), body.as_bytes()),
        )
        .extension(Timeout::from(TimeDuration::from_duration(HTTP_TIMEOUT)))
        .body(body)
        .map_err(|e| e.to_string())?;
    let response = ctx.http.send(request).map_err(|e| format!("pair: {e}"))?;
    if !response.status().is_success() {
        let status = response.status();
        return Err(format!(
            "pair: HTTP {status} {}",
            response.into_body().into_string_lossy()
        ));
    }
    Ok((identity, minted.token))
}

/// Peer side of pairing: trust `identity` as peer `from`, replacing any older identity.
#[handler]
pub fn idc_pair_route(ctx: &mut HandlerContext, req: Request) -> Response {
    let secret = ctx.env.IDC_SECRET();
    let sig = signature(&req);
    let body = req.into_body().into_bytes();
    if let Err(e) = verify(&secret, sig.as_deref(), &body, ctx.timestamp) {
        return json_response(StatusCode::UNAUTHORIZED, json!({ "error": e }));
    }
    let pair: PairRequest = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => return json_response(StatusCode::BAD_REQUEST, json!({ "error": e.to_string() })),
    };
    let Ok(identity) = Identity::from_hex(&pair.identity) else {
        return json_response(StatusCode::BAD_REQUEST, json!({ "error": "bad identity" }));
    };
    let result = ctx.try_with_tx(|tx| {
        find_peer(&tx.env.IDC_PEERS(), &pair.from)?;
        let old: Vec<Identity> = tx
            .db
            .idc_known_peer()
            .name()
            .filter(&pair.from)
            .map(|k| k.identity)
            .collect();
        for id in old {
            tx.db.idc_known_peer().identity().delete(id);
        }
        tx.db.idc_known_peer().insert(IdcKnownPeer {
            identity,
            name: pair.from.clone(),
            paired_at: tx.timestamp,
        });
        let detail = format!("{} is {}", identity.to_abbreviated_hex(), pair.from);
        log(
            tx, "in", &pair.from, "pair", "reducer", "", "paired", &detail, 0,
        );
        Ok::<_, String>(())
    });
    match result {
        Ok(()) => json_response(StatusCode::OK, json!({ "ok": true })),
        Err(e) => json_response(StatusCode::FORBIDDEN, json!({ "error": e })),
    }
}

// ---------------------------------------------------------------------------
// Request/response and SQL pulls (from procedures)
// ---------------------------------------------------------------------------

/// Signed synchronous call to a peer's HTTP route, e.g. `rpc(ctx, "warehouse", "/rpc/stock", json!({...}))`.
/// The peer's handler checks it with [`verify_rpc`].
pub fn rpc(
    ctx: &mut ProcedureContext,
    peer: &str,
    path: &str,
    payload: Value,
) -> Result<Value, String> {
    let (me, secret, raw_peers) =
        ctx.with_tx(|tx| (tx.env.IDC_SELF(), tx.env.IDC_SECRET(), tx.env.IDC_PEERS()));
    let peer = find_peer(&raw_peers, peer)?;
    let body = json!({ "from": me, "payload": payload }).to_string();
    let request = Request::builder()
        .method("POST")
        .uri(format!("{}/route{path}", peer.base))
        .header(header::CONTENT_TYPE, "application/json")
        .header(
            SIGNATURE_HEADER,
            sign(&secret, ctx.timestamp_now(), body.as_bytes()),
        )
        .extension(Timeout::from(TimeDuration::from_duration(HTTP_TIMEOUT)))
        .body(body)
        .map_err(|e| e.to_string())?;
    let response = ctx.http.send(request).map_err(|e| e.to_string())?;
    let status = response.status();
    let value: Value =
        serde_json::from_slice(&response.into_body().into_bytes()).unwrap_or(Value::Null);
    if status.is_success() {
        Ok(value)
    } else {
        Err(format!("HTTP {status}: {value}"))
    }
}

/// For RPC handlers: verify the signature and return `(from, payload)`.
pub fn verify_rpc(ctx: &HandlerContext, req: Request) -> Result<(String, Value), Response> {
    let secret = ctx.env.IDC_SECRET();
    let sig = signature(&req);
    let body = req.into_body().into_bytes();
    verify(&secret, sig.as_deref(), &body, ctx.timestamp)
        .map_err(|e| json_response(StatusCode::UNAUTHORIZED, json!({ "error": e })))?;
    let v: Value = serde_json::from_slice(&body)
        .map_err(|e| json_response(StatusCode::BAD_REQUEST, json!({ "error": e.to_string() })))?;
    let from = v["from"].as_str().unwrap_or_default().to_string();
    Ok((from, v["payload"].clone()))
}

pub fn rpc_reply(value: Value) -> Response {
    json_response(StatusCode::OK, value)
}

/// Run SQL against a peer through its `/sql` endpoint, using our paired identity if we have one.
/// Reads of public tables work for anyone; private tables and DML need the database owner's token.
pub fn sql(ctx: &mut ProcedureContext, peer: &str, query: &str) -> Result<Value, String> {
    let (raw_peers, token) = ctx.with_tx(|tx| {
        (
            tx.env.IDC_PEERS(),
            tx.db
                .idc_peer_token()
                .peer()
                .find(peer.to_string())
                .map(|t| t.token),
        )
    });
    let peer = find_peer(&raw_peers, peer)?;
    let mut builder = Request::builder()
        .method("POST")
        .uri(format!("{}/sql", peer.base))
        .header(header::CONTENT_TYPE, "text/plain")
        .extension(Timeout::from(TimeDuration::from_duration(HTTP_TIMEOUT)));
    if let Some(token) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let response = ctx
        .http
        .send(builder.body(query.to_string()).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    let status = response.status();
    let text = response.into_body().into_string_lossy();
    if status.is_success() {
        serde_json::from_str(&text).map_err(|e| e.to_string())
    } else {
        Err(format!("HTTP {status}: {text}"))
    }
}

// ---------------------------------------------------------------------------
// Housekeeping
// ---------------------------------------------------------------------------

#[spacetimedb::reducer]
pub fn idc_prune(ctx: &ReducerContext, _job: IdcPruneJob) {
    let cutoff = ctx.timestamp - SEEN_RETENTION;
    let old: Vec<String> = ctx
        .db
        .idc_seen()
        .at()
        .filter(..cutoff)
        .map(|s| s.key)
        .collect();
    for key in old {
        ctx.db.idc_seen().key().delete(key);
    }
}

#[allow(clippy::too_many_arguments)]
fn log(
    ctx: &ReducerContext,
    direction: &str,
    peer: &str,
    kind: &str,
    transport: &str,
    msg_id: &str,
    event: &str,
    detail: &str,
    latency_us: i64,
) {
    let row = ctx.db.idc_log().insert(IdcLog {
        id: 0,
        at: ctx.timestamp,
        direction: direction.into(),
        peer: peer.into(),
        kind: kind.into(),
        transport: transport.into(),
        msg_id: msg_id.into(),
        event: event.into(),
        detail: detail.into(),
        latency_us,
    });
    if row.id > LOG_KEEP {
        ctx.db.idc_log().id().delete(row.id - LOG_KEEP);
    }
}

/// The routes every IDC peer exposes. Merge into your module's router.
pub fn router() -> Router {
    Router::new()
        .post("/idc/inbox", idc_inbox)
        .post("/idc/pair", idc_pair_route)
}

/// Small helper so procedures can stamp fresh signatures mid-run.
trait Now {
    fn timestamp_now(&mut self) -> Timestamp;
}

impl Now for ProcedureContext {
    fn timestamp_now(&mut self) -> Timestamp {
        self.with_tx(|tx| tx.timestamp)
    }
}
