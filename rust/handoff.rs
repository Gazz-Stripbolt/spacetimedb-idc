//! # handoff.rs: move an entity between databases safely, on top of idc
//!
//! The classic case is a game character walking from one world shard to the next. A plain
//! "copy to B, then delete on A" can't be made safe without distributed transactions: crash
//! between the two steps and you get a duplicate (B committed, A didn't delete) or a lost
//! character (A deleted, B never committed). SpacetimeDB has no transaction that spans two
//! databases, and synchronous IDC wouldn't add one.
//!
//! What *can* be made safe is an ownership state machine where every step is a transaction
//! on one side plus an idc message, and every message is idempotent:
//!
//! ```text
//!  source A                                  target B
//!  start(): lock entity ──── offer ─────────▶ validate → import as *pending*
//!                                             (or ◀── reject: A unlocks)
//!  remove our copy ◀──────── accept ──────── (checksum echoed back)
//!           └─────────────── release ───────▶ activate: now live on B
//! ```
//!
//! Invariant: **at most one live copy, and never zero copies**. Before A sees `accept`, A owns
//! the entity (locked, so nobody can play or trade it). After A removes its copy, the `release`
//! message sits in A's transactional outbox and is retried until B applies it. So a crash at any
//! point just resumes. Cancels and timeouts are messages in the same ordered queue as the offer,
//! so the target always sees `offer` before `cancel` and the race resolves the same way every time.
//!
//! ## Setup
//!
//! Copy this file next to `idc.rs`, add `pub mod handoff;`, call [`init`] from your `init`
//! reducer, route handoff messages first in `on_idc_message`:
//!
//! ```ignore
//! pub fn on_idc_message(ctx: &ReducerContext, from: &str, kind: &str, payload: &Value) -> Result<(), String> {
//!     if let Some(result) = handoff::on_idc_message(ctx, from, kind, payload) {
//!         return result;
//!     }
//!     // ... your own kinds
//! }
//! ```
//!
//! and implement these in the crate root:
//!
//! ```ignore
//! // Target: may we take this entity? Read-only; an Err becomes a `reject` (the source unlocks).
//! pub fn handoff_validate(ctx: &ReducerContext, from: &str, entity: &str, data: &Value) -> Result<(), String>;
//! // Target: store it as pending (not playable yet). Must not fail: validate first.
//! pub fn handoff_import(ctx: &ReducerContext, from: &str, entity: &str, data: &Value);
//! // Target: the source has let go, make it live.
//! pub fn handoff_activate(ctx: &ReducerContext, entity: &str);
//! // Target: the transfer was cancelled after import, drop the pending copy.
//! pub fn handoff_discard(ctx: &ReducerContext, entity: &str);
//! // Source: the target has it, delete our copy.
//! pub fn handoff_remove(ctx: &ReducerContext, entity: &str);
//! // Source: the transfer failed (rejected, cancelled, timed out), the entity is ours and unlocked again.
//! pub fn handoff_returned(ctx: &ReducerContext, entity: &str, reason: &str);
//! ```
//!
//! Use [`is_locked`] in your own reducers to refuse moves, trades and edits while an entity is
//! in transit.

use crate::idc::{self, *};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use spacetimedb::{ReducerContext, ScheduleAt, Table, Timestamp};
use std::time::Duration;

/// How long the source waits for an answer before it asks the target to cancel.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

// Statuses. Source side: offered → released, or offered → cancelling → cancelled, or offered → rejected.
// Target side: accepted → active, or accepted → cancelled, or rejected.
pub const OFFERED: &str = "offered";
pub const CANCELLING: &str = "cancelling";
pub const CANCELLED: &str = "cancelled";
pub const RELEASED: &str = "released";
pub const REJECTED: &str = "rejected";
pub const ACCEPTED: &str = "accepted";
pub const ACTIVE: &str = "active";

/// One row per transfer, on each side. No payloads, so it's public: players can watch their
/// own transfers (add RLS if entity ids are sensitive).
#[derive(Clone)]
#[spacetimedb::table(accessor = handoff, public)]
pub struct Handoff {
    #[primary_key]
    pub id: String,
    #[index(btree)]
    pub entity: String,
    /// `out` (we're the source) or `in` (we're the target).
    pub role: String,
    pub peer: String,
    pub status: String,
    /// Lowercase hex SHA-256 of the data text, as sent.
    pub checksum: String,
    pub started: Timestamp,
    pub updated: Timestamp,
    /// Why it ended the way it did, for people reading the table.
    pub detail: String,
}

#[spacetimedb::table(accessor = handoff_seq)]
pub struct HandoffSeq {
    #[primary_key]
    pub key: u8,
    pub next: u64,
}

#[spacetimedb::table(accessor = handoff_timeout_job, scheduled(handoff_timeout))]
pub struct HandoffTimeoutJob {
    #[primary_key]
    #[auto_inc]
    pub scheduled_id: u64,
    pub scheduled_at: ScheduleAt,
    pub transfer_id: String,
}

/// Call from your `init` reducer (or `idc_kick`). Idempotent.
pub fn init(ctx: &ReducerContext) {
    if ctx.db.handoff_seq().key().find(0).is_none() {
        ctx.db.handoff_seq().insert(HandoffSeq { key: 0, next: 1 });
    }
}

pub fn checksum(data: &str) -> String {
    Sha256::digest(data.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Is `entity` in transit (locked on the source, or pending on the target)?
pub fn is_locked(ctx: &ReducerContext, entity: &str) -> bool {
    ctx.db
        .handoff()
        .entity()
        .filter(entity)
        .any(|h| matches!(h.status.as_str(), OFFERED | CANCELLING | ACCEPTED))
}

/// The most recent transfer of `entity` this database took part in.
pub fn latest(ctx: &ReducerContext, entity: &str) -> Option<Handoff> {
    ctx.db
        .handoff()
        .entity()
        .filter(entity)
        .max_by_key(|h| h.started)
}

/// Offer `entity` to `peer` with [`DEFAULT_TIMEOUT`]. See [`start_with_timeout`].
pub fn start(
    ctx: &ReducerContext,
    peer: &str,
    entity: &str,
    data: &Value,
) -> Result<String, String> {
    start_with_timeout(ctx, peer, entity, data, DEFAULT_TIMEOUT)
}

/// Lock `entity` and offer it, with `data`, to `peer`. Returns the transfer id. Runs in your
/// reducer's transaction: if your reducer fails afterwards, nothing was locked or sent.
/// If the target hasn't answered within `timeout`, the transfer is cancelled.
pub fn start_with_timeout(
    ctx: &ReducerContext,
    peer: &str,
    entity: &str,
    data: &Value,
    timeout: Duration,
) -> Result<String, String> {
    if entity.is_empty() {
        return Err("entity id is empty".into());
    }
    let me = ctx.env.IDC_SELF();
    if peer == me {
        return Err("can't hand off to ourselves".into());
    }
    if !idc::peers(&ctx.env.IDC_PEERS())
        .iter()
        .any(|p| p.name == peer)
    {
        return Err(format!("unknown peer `{peer}`"));
    }
    if is_locked(ctx, entity) {
        return Err(format!("`{entity}` is already in transit"));
    }
    forget_finished(ctx, entity);
    let mut seq = ctx
        .db
        .handoff_seq()
        .key()
        .find(0)
        .ok_or("handoff::init wasn't called")?;
    let epoch = ctx
        .db
        .idc_state()
        .key()
        .find(0)
        .map(|s| s.epoch)
        .unwrap_or_default();
    let id = format!("{me}-{epoch}-{}", seq.next);
    seq.next += 1;
    ctx.db.handoff_seq().key().update(seq);

    let text = data.to_string();
    let sum = checksum(&text);
    ctx.db.handoff().insert(Handoff {
        id: id.clone(),
        entity: entity.to_string(),
        role: "out".into(),
        peer: peer.to_string(),
        status: OFFERED.into(),
        checksum: sum.clone(),
        started: ctx.timestamp,
        updated: ctx.timestamp,
        detail: String::new(),
    });
    idc::send(
        ctx,
        peer,
        "handoff.offer",
        json!({ "id": id, "entity": entity, "data": text, "checksum": sum }),
    );
    ctx.db.handoff_timeout_job().insert(HandoffTimeoutJob {
        scheduled_id: 0,
        scheduled_at: ScheduleAt::Time(ctx.timestamp + timeout),
        transfer_id: id.clone(),
    });
    Ok(id)
}

/// Ask the target to drop an outgoing transfer. Only possible until we've seen its `accept`;
/// after that the entity already belongs to the target. The entity stays locked until the
/// target confirms, then [`handoff_returned`](crate) runs.
pub fn cancel(ctx: &ReducerContext, transfer_id: &str, reason: &str) -> Result<(), String> {
    let mut h = ctx
        .db
        .handoff()
        .id()
        .find(transfer_id.to_string())
        .filter(|h| h.role == "out")
        .ok_or_else(|| format!("no outgoing transfer `{transfer_id}`"))?;
    match h.status.as_str() {
        OFFERED => {
            h.status = CANCELLING.into();
            h.detail = reason.to_string();
            h.updated = ctx.timestamp;
            idc::send(
                ctx,
                &h.peer,
                "handoff.cancel",
                json!({ "id": h.id, "reason": reason }),
            );
            ctx.db.handoff().id().update(h);
            Ok(())
        }
        CANCELLING => Ok(()),
        RELEASED => Err("too late: the target already has it".into()),
        other => Err(format!("transfer is already {other}")),
    }
}

/// Fires when an offer's timeout is up. A no-op unless the target still hasn't answered.
#[spacetimedb::reducer]
pub fn handoff_timeout(ctx: &ReducerContext, job: HandoffTimeoutJob) -> Result<(), String> {
    if ctx.sender() != ctx.database_identity() {
        return Err("scheduled only".into());
    }
    let pending = ctx
        .db
        .handoff()
        .id()
        .find(&job.transfer_id)
        .is_some_and(|h| h.status == OFFERED);
    if pending {
        cancel(ctx, &job.transfer_id, "timed out")?;
    }
    Ok(())
}

/// Route `handoff.*` messages here first. Returns `None` for kinds that aren't ours.
pub fn on_idc_message(
    ctx: &ReducerContext,
    from: &str,
    kind: &str,
    payload: &Value,
) -> Option<Result<(), String>> {
    let action = kind.strip_prefix("handoff.")?;
    let Some(id) = payload["id"].as_str() else {
        return Some(Err(format!("{kind}: missing id")));
    };
    Some(match action {
        "offer" => on_offer(ctx, from, id, payload),
        "accept" => on_accept(ctx, from, id, payload),
        "reject" => {
            let reason = payload["reason"].as_str().unwrap_or("rejected");
            on_returned(ctx, from, id, REJECTED, reason)
        }
        "cancelled" => on_returned(ctx, from, id, CANCELLED, "cancelled"),
        "release" => on_release(ctx, from, id),
        "cancel" => on_cancel(ctx, from, id, payload),
        other => Err(format!("unknown handoff message `{other}`")),
    })
}

// Every handler below is idempotent and refuses with a *message*, never an Err, unless the
// message itself is malformed. An Err would turn into a dead letter on the sender, and a dead
// `release` would leave an entity with no live copy.

fn find(ctx: &ReducerContext, id: &str, role: &str, peer: &str) -> Option<Handoff> {
    ctx.db
        .handoff()
        .id()
        .find(id.to_string())
        .filter(|h| h.role == role && h.peer == peer)
}

fn set_status(ctx: &ReducerContext, mut h: Handoff, status: &str, detail: &str) {
    h.status = status.to_string();
    h.detail = detail.to_string();
    h.updated = ctx.timestamp;
    ctx.db.handoff().id().update(h);
}

/// Drop finished transfers of `entity`, so the table stays one row per entity in the long run.
fn forget_finished(ctx: &ReducerContext, entity: &str) {
    let done: Vec<String> = ctx
        .db
        .handoff()
        .entity()
        .filter(entity)
        .filter(|h| matches!(h.status.as_str(), RELEASED | REJECTED | CANCELLED | ACTIVE))
        .map(|h| h.id)
        .collect();
    for id in done {
        ctx.db.handoff().id().delete(id);
    }
}

/// Target: an entity is offered to us.
fn on_offer(ctx: &ReducerContext, from: &str, id: &str, p: &Value) -> Result<(), String> {
    let entity = p["entity"]
        .as_str()
        .ok_or("handoff.offer: missing entity")?;
    let text = p["data"].as_str().ok_or("handoff.offer: missing data")?;
    let sum = p["checksum"]
        .as_str()
        .ok_or("handoff.offer: missing checksum")?;
    if ctx.db.handoff().id().find(id.to_string()).is_some() {
        // Seen it (or a cancel got here first and left a tombstone): the answer was already sent.
        return Ok(());
    }
    let record = |status: &str, detail: &str| Handoff {
        id: id.to_string(),
        entity: entity.to_string(),
        role: "in".into(),
        peer: from.to_string(),
        status: status.to_string(),
        checksum: sum.to_string(),
        started: ctx.timestamp,
        updated: ctx.timestamp,
        detail: detail.to_string(),
    };
    let refusal = if checksum(text) != sum {
        Some("checksum mismatch".to_string())
    } else if is_locked(ctx, entity) {
        Some(format!("`{entity}` is already in transit here"))
    } else {
        match serde_json::from_str::<Value>(text) {
            Err(e) => Some(format!("bad data: {e}")),
            Ok(data) => match crate::handoff_validate(ctx, from, entity, &data) {
                Err(reason) => Some(reason),
                Ok(()) => {
                    forget_finished(ctx, entity);
                    crate::handoff_import(ctx, from, entity, &data);
                    ctx.db.handoff().insert(record(ACCEPTED, ""));
                    idc::send(
                        ctx,
                        from,
                        "handoff.accept",
                        json!({ "id": id, "checksum": sum }),
                    );
                    None
                }
            },
        }
    };
    if let Some(reason) = refusal {
        forget_finished(ctx, entity);
        ctx.db.handoff().insert(record(REJECTED, &reason));
        idc::send(
            ctx,
            from,
            "handoff.reject",
            json!({ "id": id, "reason": reason }),
        );
    }
    Ok(())
}

/// Source: the target has a pending copy. Let go of ours.
fn on_accept(ctx: &ReducerContext, from: &str, id: &str, p: &Value) -> Result<(), String> {
    let Some(h) = find(ctx, id, "out", from) else {
        return Ok(());
    };
    if h.status != OFFERED {
        // cancelling: the target will discard its copy and confirm. Anything else: a replay.
        return Ok(());
    }
    if p["checksum"].as_str() != Some(h.checksum.as_str()) {
        return cancel(ctx, id, "checksum mismatch on accept");
    }
    crate::handoff_remove(ctx, &h.entity);
    idc::send(ctx, from, "handoff.release", json!({ "id": id }));
    set_status(ctx, h, RELEASED, "");
    Ok(())
}

/// Source: the transfer failed (`reject`) or the target confirmed our cancel (`cancelled`).
fn on_returned(
    ctx: &ReducerContext,
    from: &str,
    id: &str,
    status: &str,
    reason: &str,
) -> Result<(), String> {
    let Some(h) = find(ctx, id, "out", from) else {
        return Ok(());
    };
    if !matches!(h.status.as_str(), OFFERED | CANCELLING) {
        return Ok(());
    }
    let entity = h.entity.clone();
    let reason = if h.status == CANCELLING && !h.detail.is_empty() {
        h.detail.clone()
    } else {
        reason.to_string()
    };
    set_status(ctx, h, status, &reason);
    crate::handoff_returned(ctx, &entity, &reason);
    Ok(())
}

/// Target: the source has deleted its copy. Ours is the only one now.
fn on_release(ctx: &ReducerContext, from: &str, id: &str) -> Result<(), String> {
    let Some(h) = find(ctx, id, "in", from) else {
        return Ok(());
    };
    if h.status != ACCEPTED {
        return Ok(());
    }
    crate::handoff_activate(ctx, &h.entity);
    set_status(ctx, h, ACTIVE, "");
    Ok(())
}

/// Target: the source wants to call it off. It only asks before it has seen our `accept`,
/// so whatever we hold for this transfer is still pending and can be dropped.
fn on_cancel(ctx: &ReducerContext, from: &str, id: &str, p: &Value) -> Result<(), String> {
    let reason = p["reason"].as_str().unwrap_or("cancelled");
    match ctx.db.handoff().id().find(id.to_string()) {
        Some(h) if h.role != "in" || h.peer != from => return Ok(()),
        Some(h) if h.status == ACCEPTED => {
            crate::handoff_discard(ctx, &h.entity);
            set_status(ctx, h, CANCELLED, reason);
        }
        // Already active can't happen (release only follows accept, and the source never
        // sends both release and cancel). Rejected/cancelled: just confirm again.
        Some(h) if h.status == ACTIVE => {
            log::warn!("handoff {id}: cancel after activation ignored");
            return Ok(());
        }
        Some(_) => {}
        None => {
            // The offer never got here (e.g. it was refused as malformed). Leave a tombstone so
            // a late offer with this id is ignored.
            ctx.db.handoff().insert(Handoff {
                id: id.to_string(),
                entity: String::new(),
                role: "in".into(),
                peer: from.to_string(),
                status: CANCELLED.into(),
                checksum: String::new(),
                started: ctx.timestamp,
                updated: ctx.timestamp,
                detail: reason.to_string(),
            });
        }
    }
    idc::send(ctx, from, "handoff.cancelled", json!({ "id": id }));
    Ok(())
}
