/**
 * # spacetimedb-idc/handoff: move an entity between databases safely
 *
 * The TypeScript twin of `rust/handoff.rs`, on the same wire protocol. The classic case is a game
 * character walking from one world shard to the next. "Copy to B, then delete on A" can't be made
 * safe without distributed transactions: crash in between and you get a duplicate or a lost
 * character. What *can* be made safe is an ownership state machine where every step is one
 * transaction plus an idc message, and every message is idempotent:
 *
 * ```text
 *  source A                                  target B
 *  start(): lock entity ──── offer ─────────▶ validate → import as *pending*
 *                                             (or ◀── reject: A unlocks)
 *  remove our copy ◀──────── accept ──────── (checksum echoed back)
 *           └─────────────── release ───────▶ activate: now live on B
 * ```
 *
 * Invariant: **at most one live copy, and never zero copies**. Cancels and timeouts travel in the
 * same ordered queue as the offer, so the target always sees `offer` before `cancel`.
 *
 * The tables (`idc.handoff`, public) and the timeout job are part of the idc submodule, so there's
 * nothing extra to mount. Route `handoff.*` messages here first in your `onMessage`, and pass your
 * hooks. Use `isLocked()` in your own reducers to refuse moves, trades and edits while in transit.
 */
import { SenderError } from 'spacetimedb/server';
import { ScheduleAt } from 'spacetimedb';
import { sha256 } from '@noble/hashes/sha2';
import { send, cancelHandoff, parsePeers, type IdcCtx, type Message } from './index';

/** How long the source waits for an answer before it asks the target to cancel. */
export const DEFAULT_TIMEOUT_MS = 30_000;

// Statuses. Source side: offered → released, or offered → cancelling → cancelled, or offered → rejected.
// Target side: accepted → active, or accepted → cancelled, or rejected.
export const OFFERED = 'offered';
export const CANCELLING = 'cancelling';
export const CANCELLED = 'cancelled';
export const RELEASED = 'released';
export const REJECTED = 'rejected';
export const ACCEPTED = 'accepted';
export const ACTIVE = 'active';

export type Handoff = NonNullable<ReturnType<IdcCtx['db']['handoff']['id']['find']>>;

/** Your side of the protocol. `Tx` is your module's transaction context. */
export interface Hooks<Tx> {
  /** Target: may we take this entity? Read-only; throw to refuse (becomes a `reject`, the source unlocks). */
  validate(tx: Tx, from: string, entity: string, data: any): void;
  /** Target: store it as pending (not playable yet). Must not throw: validate first. */
  import(tx: Tx, from: string, entity: string, data: any): void;
  /** Target: the source has let go, make it live. */
  activate(tx: Tx, entity: string): void;
  /** Target: the transfer was cancelled after import, drop the pending copy. */
  discard(tx: Tx, entity: string): void;
  /** Source: the target has it, delete our copy. */
  remove(tx: Tx, entity: string): void;
  /** Source: the transfer failed (rejected, cancelled, timed out); the entity is ours and unlocked again. */
  returned(tx: Tx, entity: string, reason: string): void;
}

export interface HandoffOptions<Tx> {
  /** Narrow your transaction context to the idc submodule, e.g. `tx => tx.as.idc`. */
  scope: (tx: Tx) => IdcCtx;
  hooks: Hooks<Tx>;
}

const enc = new TextEncoder();
export const checksum = (data: string) => Array.from(sha256(enc.encode(data)), (x) => x.toString(16).padStart(2, '0')).join('');

const IN_TRANSIT = [OFFERED, CANCELLING, ACCEPTED];
const FINISHED = [RELEASED, REJECTED, CANCELLED, ACTIVE];

/** Is `entity` in transit (locked on the source, or pending on the target)? */
export function isLocked(ctx: IdcCtx, entity: string): boolean {
  for (const h of ctx.db.handoff.entity.filter(entity)) if (IN_TRANSIT.includes(h.status)) return true;
  return false;
}

/** The most recent transfer of `entity` this database took part in. */
export function latest(ctx: IdcCtx, entity: string): Handoff | undefined {
  let best: Handoff | undefined;
  for (const h of ctx.db.handoff.entity.filter(entity)) {
    if (!best || h.started.microsSinceUnixEpoch > best.started.microsSinceUnixEpoch) best = h;
  }
  return best;
}

/**
 * Lock `entity` and offer it, with `data`, to `peer`. Returns the transfer id. Runs in your
 * reducer's transaction: if your reducer throws afterwards, nothing was locked or sent. If the
 * target hasn't answered within `timeoutMs`, the transfer is cancelled.
 */
export function start(ctx: IdcCtx, peer: string, entity: string, data: unknown, timeoutMs = DEFAULT_TIMEOUT_MS): string {
  if (!entity) throw new SenderError('entity id is empty');
  const cfg = ctx.db.config.key.find(0);
  if (!cfg) throw new SenderError('idc is not configured');
  if (peer === cfg.self) throw new SenderError("can't hand off to ourselves");
  if (!parsePeers(cfg.peers).some((p) => p.name === peer)) throw new SenderError(`unknown peer \`${peer}\``);
  if (isLocked(ctx, entity)) throw new SenderError(`\`${entity}\` is already in transit`);
  forgetFinished(ctx, entity);
  const seq = ctx.db.handoffSeq.key.find(0);
  if (!seq) throw new SenderError('idc.configure() wasn\'t called');
  const id = `${cfg.self}-${ctx.db.state.key.find(0)?.epoch ?? ''}-${seq.next}`;
  ctx.db.handoffSeq.key.update({ ...seq, next: seq.next + 1n });

  const text = JSON.stringify(data);
  const sum = checksum(text);
  ctx.db.handoff.insert({
    id, entity, role: 'out', peer, status: OFFERED, checksum: sum, started: ctx.timestamp, updated: ctx.timestamp, detail: '',
  });
  send(ctx, peer, 'handoff.offer', { id, entity, data: text, checksum: sum });
  ctx.db.handoffTimeoutJob.insert({
    scheduledId: 0n,
    scheduledAt: ScheduleAt.time(ctx.timestamp.microsSinceUnixEpoch + BigInt(Math.trunc(timeoutMs)) * 1000n),
    transferId: id,
  });
  return id;
}

/**
 * Ask the target to drop an outgoing transfer. Only possible until we've seen its `accept`.
 * The entity stays locked until the target confirms, then your `returned` hook runs.
 */
export const cancel = cancelHandoff;

/**
 * Route `handoff.*` messages here first in your `onMessage`. Returns false for kinds that aren't ours.
 * `if (handoff.onIdcMessage(tx, msg, opts)) return;`
 */
export function onIdcMessage<Tx>(tx: Tx, msg: Message, opts: HandoffOptions<Tx>): boolean {
  if (!msg.kind.startsWith('handoff.')) return false;
  const action = msg.kind.slice('handoff.'.length);
  const p = msg.payload ?? {};
  const id = p.id;
  if (typeof id !== 'string') throw new Error(`${msg.kind}: missing id`);
  switch (action) {
    case 'offer':
      onOffer(tx, msg.from, id, p, opts);
      break;
    case 'accept':
      onAccept(tx, msg.from, id, p, opts);
      break;
    case 'reject':
      onReturned(tx, msg.from, id, REJECTED, typeof p.reason === 'string' ? p.reason : 'rejected', opts);
      break;
    case 'cancelled':
      onReturned(tx, msg.from, id, CANCELLED, 'cancelled', opts);
      break;
    case 'release':
      onRelease(tx, msg.from, id, opts);
      break;
    case 'cancel':
      onCancel(tx, msg.from, id, p, opts);
      break;
    default:
      throw new Error(`unknown handoff message \`${action}\``);
  }
  return true;
}

// Every handler below is idempotent and refuses with a *message*, never a throw, unless the
// message itself is malformed. A throw would turn into a dead letter on the sender, and a dead
// `release` would leave an entity with no live copy.

function find(ctx: IdcCtx, id: string, role: string, peer: string): Handoff | undefined {
  const h = ctx.db.handoff.id.find(id);
  return h && h.role === role && h.peer === peer ? h : undefined;
}

function setStatus(ctx: IdcCtx, h: Handoff, status: string, detail: string): void {
  ctx.db.handoff.id.update({ ...h, status, detail, updated: ctx.timestamp });
}

/** Drop finished transfers of `entity`, so the table stays one row per entity in the long run. */
function forgetFinished(ctx: IdcCtx, entity: string): void {
  const done = [...ctx.db.handoff.entity.filter(entity)].filter((h) => FINISHED.includes(h.status));
  for (const h of done) ctx.db.handoff.id.delete(h.id);
}

/** Target: an entity is offered to us. */
function onOffer<Tx>(tx: Tx, from: string, id: string, p: any, { scope, hooks }: HandoffOptions<Tx>): void {
  const ctx = scope(tx);
  const { entity, data: text, checksum: sum } = p;
  if (typeof entity !== 'string') throw new Error('handoff.offer: missing entity');
  if (typeof text !== 'string') throw new Error('handoff.offer: missing data');
  if (typeof sum !== 'string') throw new Error('handoff.offer: missing checksum');
  // Seen it (or a cancel got here first and left a tombstone): the answer was already sent.
  if (ctx.db.handoff.id.find(id)) return;
  const record = (status: string, detail: string) => ({
    id, entity, role: 'in', peer: from, status, checksum: sum, started: ctx.timestamp, updated: ctx.timestamp, detail,
  });
  let refusal: string | undefined;
  if (checksum(text) !== sum) refusal = 'checksum mismatch';
  else if (isLocked(ctx, entity)) refusal = `\`${entity}\` is already in transit here`;
  else {
    let data: any;
    try {
      data = JSON.parse(text);
    } catch (e) {
      refusal = `bad data: ${(e as Error).message}`;
    }
    if (refusal === undefined) {
      try {
        hooks.validate(tx, from, entity, data);
      } catch (e) {
        refusal = (e as Error).message ?? String(e);
      }
    }
    if (refusal === undefined) {
      forgetFinished(ctx, entity);
      hooks.import(tx, from, entity, data);
      ctx.db.handoff.insert(record(ACCEPTED, ''));
      send(ctx, from, 'handoff.accept', { id, checksum: sum });
      return;
    }
  }
  forgetFinished(ctx, entity);
  ctx.db.handoff.insert(record(REJECTED, refusal));
  send(ctx, from, 'handoff.reject', { id, reason: refusal });
}

/** Source: the target has a pending copy. Let go of ours. */
function onAccept<Tx>(tx: Tx, from: string, id: string, p: any, { scope, hooks }: HandoffOptions<Tx>): void {
  const ctx = scope(tx);
  const h = find(ctx, id, 'out', from);
  // cancelling: the target will discard its copy and confirm. Anything else: a replay.
  if (!h || h.status !== OFFERED) return;
  if (p.checksum !== h.checksum) {
    cancelHandoff(ctx, id, 'checksum mismatch on accept');
    return;
  }
  hooks.remove(tx, h.entity);
  send(ctx, from, 'handoff.release', { id });
  setStatus(ctx, h, RELEASED, '');
}

/** Source: the transfer failed (`reject`) or the target confirmed our cancel (`cancelled`). */
function onReturned<Tx>(tx: Tx, from: string, id: string, status: string, reason: string, { scope, hooks }: HandoffOptions<Tx>): void {
  const ctx = scope(tx);
  const h = find(ctx, id, 'out', from);
  if (!h || (h.status !== OFFERED && h.status !== CANCELLING)) return;
  const why = h.status === CANCELLING && h.detail ? h.detail : reason;
  setStatus(ctx, h, status, why);
  hooks.returned(tx, h.entity, why);
}

/** Target: the source has deleted its copy. Ours is the only one now. */
function onRelease<Tx>(tx: Tx, from: string, id: string, { scope, hooks }: HandoffOptions<Tx>): void {
  const ctx = scope(tx);
  const h = find(ctx, id, 'in', from);
  if (!h || h.status !== ACCEPTED) return;
  hooks.activate(tx, h.entity);
  setStatus(ctx, h, ACTIVE, '');
}

/**
 * Target: the source wants to call it off. It only asks before it has seen our `accept`,
 * so whatever we hold for this transfer is still pending and can be dropped.
 */
function onCancel<Tx>(tx: Tx, from: string, id: string, p: any, { scope, hooks }: HandoffOptions<Tx>): void {
  const ctx = scope(tx);
  const reason = typeof p.reason === 'string' ? p.reason : 'cancelled';
  const h = ctx.db.handoff.id.find(id);
  if (h && (h.role !== 'in' || h.peer !== from)) return;
  if (h && h.status === ACCEPTED) {
    hooks.discard(tx, h.entity);
    setStatus(ctx, h, CANCELLED, reason);
  } else if (h && h.status === ACTIVE) {
    // Can't happen (release only follows accept, and the source never sends both release and cancel).
    console.warn(`handoff ${id}: cancel after activation ignored`);
    return;
  } else if (!h) {
    // The offer never got here (e.g. it was refused as malformed). Leave a tombstone so a late
    // offer with this id is ignored.
    ctx.db.handoff.insert({
      id, entity: '', role: 'in', peer: from, status: CANCELLED, checksum: '', started: ctx.timestamp, updated: ctx.timestamp, detail: reason,
    });
  }
  // Rejected/cancelled: just confirm again.
  send(ctx, from, 'handoff.cancelled', { id });
}
