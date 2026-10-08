/**
 * # spacetimedb-idc: inter-database communication, as a SpacetimeDB submodule
 *
 * The TypeScript twin of `rust/idc.rs` and `csharp/Idc.cs`. Same wire protocol, so TypeScript,
 * Rust and C# databases all talk to each other.
 *
 * - **Transactional outbox:** `send()` queues a message in your transaction; it
 *   exists if and only if your change commits.
 * - **Push, not poll:** every send schedules a one-shot procedure for "now" that
 *   POSTs to the peer, so the peer reacts the moment the request lands.
 * - **Two transports:** `route` (HMAC-signed HTTP handler, batched) or `reducer`
 *   (`/call/idc_receive` as an identity the peer knows, with automatic pairing).
 * - **At-least-once delivery, exactly-once effect:** retries with backoff,
 *   per-peer order, an idempotent inbox and dead letters.
 *
 * Mount it in your module with `schema({ ..., idc })`, then see `README.md` for
 * the handful of wrappers a consumer adds. Submodules can't read environment
 * variables or own routes and lifecycle reducers, so the consumer passes config in
 * through `configure()` and registers the routes.
 */
import { schema, table, t, SyncResponse, SenderError } from 'spacetimedb/server';
import type { ReducerCtx, ProcedureCtx, HandlerContext, Request } from 'spacetimedb/server';
import { Identity, ScheduleAt, TimeDuration, Timestamp } from 'spacetimedb';
import { hmac } from '@noble/hashes/hmac';
import { sha256 } from '@noble/hashes/sha2';

// ---------------------------------------------------------------------------
// Tables (they live under the consumer's namespace, e.g. `idc.outbox`)
// ---------------------------------------------------------------------------

const config = table(
  { name: 'config' },
  {
    key: t.u8().primaryKey(),
    self: t.string(),
    secret: t.string(),
    peers: t.string(),
    transport: t.string(),
  }
);

const outbox = table(
  { name: 'outbox' },
  {
    id: t.u64().primaryKey().autoInc(),
    peer: t.string().index(),
    kind: t.string(),
    payload: t.string(),
    created: t.timestamp(),
    attempts: t.u32(),
    nextAttempt: t.timestamp(),
    inFlightUntil: t.option(t.timestamp()),
    dead: t.bool(),
    lastError: t.string(),
  }
);

const seen = table({ name: 'seen' }, { key: t.string().primaryKey(), at: t.timestamp().index() });

const knownPeer = table(
  { name: 'known_peer' },
  { identity: t.identity().primaryKey(), name: t.string().index(), pairedAt: t.timestamp() }
);

const peerToken = table(
  { name: 'peer_token' },
  { peer: t.string().primaryKey(), identity: t.identity(), token: t.string(), pairedAt: t.timestamp() }
);

const state = table({ name: 'state' }, { key: t.u8().primaryKey(), epoch: t.string() });

/** Public activity feed: metadata only, never payloads. */
const log = table(
  { name: 'log', public: true },
  {
    id: t.u64().primaryKey().autoInc(),
    at: t.timestamp(),
    direction: t.string(),
    peer: t.string(),
    kind: t.string(),
    transport: t.string(),
    msgId: t.string(),
    event: t.string(),
    detail: t.string(),
    latencyUs: t.i64(),
  }
);

const flushJob = table({ name: 'flush_job' }, { scheduledId: t.u64().primaryKey().autoInc(), scheduledAt: t.scheduleAt() });
const pairJob = table(
  { name: 'pair_job' },
  { scheduledId: t.u64().primaryKey().autoInc(), scheduledAt: t.scheduleAt(), attempt: t.u32() }
);
const pruneJob = table({ name: 'prune_job' }, { scheduledId: t.u64().primaryKey().autoInc(), scheduledAt: t.scheduleAt() });

const spacetimedb = schema({ config, outbox, seen, knownPeer, peerToken, state, log, flushJob, pairJob, pruneJob });
export default spacetimedb;

type S = typeof spacetimedb.schemaType;
/** A context narrowed to this submodule: pass `ctx.as.idc` (or whatever you named it). */
export type IdcCtx = ReducerCtx<S>;
export type IdcProcedureCtx = ProcedureCtx<S>;
export type IdcHandlerCtx = HandlerContext<S>;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

export interface Config {
  /** This database's name in the mesh, e.g. `shop`. */
  self: string;
  /** Shared HMAC secret (same on every peer). */
  secret: string;
  /** `name=https://host/v1/database/<db>,other=...` */
  peers: string;
  /** `route` or `reducer` */
  transport: string;
}

const MAX_CLOCK_SKEW_US = 5n * 60n * 1_000_000n;
const LEASE_US = 60n * 1_000_000n;
const HTTP_TIMEOUT = TimeDuration.fromMillis(10_000);
const MAX_BACKOFF_MS = 60_000;
const BATCH = 64;
const LOG_KEEP = 300n;
const SEEN_RETENTION_US = 7n * 24n * 3600n * 1_000_000n;
const SIGNATURE_HEADER = 'x-idc-signature';

interface Peer {
  name: string;
  base: string;
}

export function parsePeers(raw: string): Peer[] {
  return raw
    .split(',')
    .map((entry) => entry.trim())
    .filter((entry) => entry.includes('='))
    .map((entry) => {
      const i = entry.indexOf('=');
      return { name: entry.slice(0, i).trim(), base: entry.slice(i + 1).trim().replace(/\/+$/, '') };
    });
}

const hostOf = (p: Peer) => (p.base.includes('/v1/') ? p.base.slice(0, p.base.indexOf('/v1/')) : p.base);

function findPeer(cfg: Config, name: string): Peer {
  const p = parsePeers(cfg.peers).find((x) => x.name === name);
  if (!p) throw new Error(`unknown peer \`${name}\``);
  return p;
}

function getConfig(ctx: IdcCtx): Config {
  const c = ctx.db.config.key.find(0);
  if (!c) throw new Error('idc is not configured: call idc.configure() from your init and idc_kick reducers');
  return c;
}

/**
 * Store the config and do the one-time setup. Call it from your `init` and from an
 * `idc_kick` reducer (run that after `spacetime publish --env-only`, or once after
 * adding idc to a database that's already live). Idempotent.
 */
export function configure(ctx: IdcCtx, cfg: Config): void {
  const row = { key: 0, ...cfg };
  if (ctx.db.config.key.find(0)) ctx.db.config.key.update(row);
  else ctx.db.config.insert(row);
  if (!ctx.db.state.key.find(0)) {
    ctx.db.state.insert({ key: 0, epoch: ctx.timestamp.microsSinceUnixEpoch.toString(16) });
  }
  if (ctx.db.pruneJob.count() === 0n) {
    ctx.db.pruneJob.insert({ scheduledId: 0n, scheduledAt: ScheduleAt.interval(3600n * 1_000_000n) });
  }
  ctx.db.pairJob.insert({ scheduledId: 0n, scheduledAt: ScheduleAt.time(ctx.timestamp.microsSinceUnixEpoch), attempt: 0 });
  scheduleFlush(ctx, ctx.timestamp.microsSinceUnixEpoch);
}

// ---------------------------------------------------------------------------
// Signing
// ---------------------------------------------------------------------------

const enc = new TextEncoder();
const hex = (b: Uint8Array) => Array.from(b, (x) => x.toString(16).padStart(2, '0')).join('');

function mac(secret: string, t: bigint, body: Uint8Array): Uint8Array {
  const prefix = enc.encode(`${t}.`);
  const msg = new Uint8Array(prefix.length + body.length);
  msg.set(prefix, 0);
  msg.set(body, prefix.length);
  return hmac(sha256, enc.encode(secret), msg);
}

/** `t=<micros>,v1=<hex hmac-sha256(secret, "<micros>.<body>")>`, same as idc.rs. */
export function sign(secret: string, nowUs: bigint, body: Uint8Array): string {
  return `t=${nowUs},v1=${hex(mac(secret, nowUs, body))}`;
}

export function verify(secret: string, header: string | null, body: Uint8Array, nowUs: bigint): void {
  if (!header) throw new Error('missing X-IDC-Signature');
  let ts: bigint | undefined;
  let v1: string | undefined;
  for (const part of header.split(',')) {
    const [k, v] = part.trim().split('=', 2);
    if (k === 't' && /^-?\d+$/.test(v ?? '')) ts = BigInt(v);
    if (k === 'v1') v1 = v;
  }
  if (ts === undefined || !v1) throw new Error('malformed X-IDC-Signature');
  const skew = nowUs > ts ? nowUs - ts : ts - nowUs;
  if (skew > MAX_CLOCK_SKEW_US) throw new Error('signature timestamp outside the allowed window');
  const expected = hex(mac(secret, ts, body));
  let diff = expected.length ^ v1.length;
  for (let i = 0; i < expected.length; i++) diff |= expected.charCodeAt(i) ^ (v1.charCodeAt(i) || 0);
  if (diff !== 0) throw new Error('bad signature');
}

// ---------------------------------------------------------------------------
// Sending
// ---------------------------------------------------------------------------

export interface Envelope {
  id: string;
  from: string;
  to: string;
  kind: string;
  payload: unknown;
  /** Sender clock, microseconds since the Unix epoch (a JSON number). */
  sent_at: number;
}

const msgId = (ctx: IdcCtx, outboxId: bigint) => `${ctx.db.state.key.find(0)?.epoch ?? ''}-${outboxId}`;

function writeLog(
  ctx: IdcCtx,
  e: { direction: string; peer: string; kind: string; transport: string; msgId: string; event: string; detail?: string; latencyUs?: bigint }
): void {
  const row = ctx.db.log.insert({ id: 0n, at: ctx.timestamp, detail: '', latencyUs: 0n, ...e });
  if (row.id > LOG_KEEP) ctx.db.log.id.delete(row.id - LOG_KEEP);
}

function scheduleFlush(ctx: IdcCtx, atUs: bigint): void {
  for (const j of ctx.db.flushJob.iter()) {
    if (j.scheduledAt.tag === 'Time' && j.scheduledAt.value.microsSinceUnixEpoch <= atUs) return;
  }
  ctx.db.flushJob.insert({ scheduledId: 0n, scheduledAt: ScheduleAt.time(atUs) });
}

/** Queue `payload` for `peer` in the current transaction. Delivery starts as soon as it commits. */
export function send(ctx: IdcCtx, peer: string, kind: string, payload: unknown): string {
  const row = ctx.db.outbox.insert({
    id: 0n,
    peer,
    kind,
    payload: JSON.stringify(payload ?? null),
    created: ctx.timestamp,
    attempts: 0,
    nextAttempt: ctx.timestamp,
    inFlightUntil: undefined,
    dead: false,
    lastError: '',
  });
  const id = msgId(ctx, row.id);
  writeLog(ctx, { direction: 'out', peer, kind, transport: getConfig(ctx).transport, msgId: id, event: 'queued' });
  scheduleFlush(ctx, ctx.timestamp.microsSinceUnixEpoch);
  return id;
}

type OutboxRow = NonNullable<ReturnType<IdcCtx['db']['outbox']['id']['find']>>;
interface Claimed {
  row: OutboxRow;
  msgId: string;
  sentAtUs: bigint;
}

function claimBatch(ctx: IdcCtx, max: number): Claimed[] {
  const now = ctx.timestamp.microsSinceUnixEpoch;
  const rows = [...ctx.db.outbox.iter()].filter((r) => !r.dead).sort((a, b) => (a.id < b.id ? -1 : 1));
  const eligible = (r: OutboxRow) =>
    !(r.inFlightUntil && r.inFlightUntil.microsSinceUnixEpoch > now) && r.nextAttempt.microsSinceUnixEpoch <= now;
  const seenPeers = new Set<string>();
  let peer: string | undefined;
  for (const r of rows) {
    if (seenPeers.has(r.peer)) continue;
    seenPeers.add(r.peer);
    if (eligible(r)) {
      peer = r.peer;
      break;
    }
  }
  if (peer === undefined) return [];
  const batch: Claimed[] = [];
  for (const r of rows.filter((x) => x.peer === peer)) {
    if (batch.length === max || !eligible(r)) break;
    ctx.db.outbox.id.update({ ...r, inFlightUntil: new Timestamp(now + LEASE_US) });
    batch.push({ row: r, msgId: msgId(ctx, r.id), sentAtUs: now });
  }
  return batch;
}

type Delivery = { ok: true; detail: string } | { ok: false; dead: boolean; detail: string; repair?: boolean };

/** Scheduled procedure: deliver everything that's due, then reschedule for the next retry. */
export const flush = spacetimedb.procedure({ onSchedule: flushJob }, { arg: flushJob.rowType }, t.unit(), (ctx) => {
  const max = ctx.withTx((tx) => (getConfig(tx).transport === 'reducer' ? 1 : BATCH));
  for (let i = 0; i < 200; i++) {
    const batch = ctx.withTx((tx) => claimBatch(tx, max));
    if (batch.length === 0) break;
    const results = deliver(ctx, batch);
    ctx.withTx((tx) => batch.forEach((c, j) => finish(tx, c, results[j])));
  }
  ctx.withTx((tx) => {
    let next: bigint | undefined;
    for (const r of tx.db.outbox.iter()) {
      if (r.dead) continue;
      let at = r.nextAttempt.microsSinceUnixEpoch;
      if (r.inFlightUntil && r.inFlightUntil.microsSinceUnixEpoch > at) at = r.inFlightUntil.microsSinceUnixEpoch;
      if (next === undefined || at < next) next = at;
    }
    if (next !== undefined) {
      const now = tx.timestamp.microsSinceUnixEpoch;
      scheduleFlush(tx, next > now ? next : now);
    }
  });
  return {};
});

function finish(ctx: IdcCtx, c: Claimed, result: Delivery): void {
  const row = ctx.db.outbox.id.find(c.row.id);
  if (!row) return;
  const transport = getConfig(ctx).transport;
  const base = { direction: 'out', peer: row.peer, kind: row.kind, transport, msgId: c.msgId };
  if (result.ok) {
    ctx.db.outbox.id.delete(row.id);
    writeLog(ctx, { ...base, event: 'delivered', detail: result.detail, latencyUs: ctx.timestamp.microsSinceUnixEpoch - row.created.microsSinceUnixEpoch });
  } else if (result.dead) {
    ctx.db.outbox.id.update({ ...row, dead: true, inFlightUntil: undefined, lastError: result.detail });
    writeLog(ctx, { ...base, event: 'dead', detail: result.detail });
  } else {
    if (result.repair || result.detail.includes('is not a known peer')) {
      // The peer forgot us (e.g. its data was reset): pair again.
      ctx.db.peerToken.peer.delete(row.peer);
      ctx.db.pairJob.insert({ scheduledId: 0n, scheduledAt: ScheduleAt.time(ctx.timestamp.microsSinceUnixEpoch), attempt: 0 });
    }
    const attempts = row.attempts + 1;
    const backoffMs = Math.min(250 * 2 ** Math.min(attempts, 16), MAX_BACKOFF_MS);
    ctx.db.outbox.id.update({
      ...row,
      attempts,
      nextAttempt: new Timestamp(ctx.timestamp.microsSinceUnixEpoch + BigInt(backoffMs) * 1000n),
      inFlightUntil: undefined,
      lastError: result.detail,
    });
    writeLog(ctx, { ...base, event: 'retry', detail: `attempt ${attempts} failed, retrying in ${backoffMs} ms: ${result.detail}` });
  }
}

function deliver(ctx: IdcProcedureCtx, batch: Claimed[]): Delivery[] {
  const peerName = batch[0].row.peer;
  const { cfg, token } = ctx.withTx((tx) => ({ cfg: getConfig(tx), token: tx.db.peerToken.peer.find(peerName)?.token }));
  const all = (d: Delivery) => batch.map(() => d);
  let peer: Peer;
  try {
    peer = findPeer(cfg, peerName);
  } catch (e) {
    return all({ ok: false, dead: false, detail: String((e as Error).message) });
  }
  const envelopes: Envelope[] = batch.map((c) => ({
    id: c.msgId,
    from: cfg.self,
    to: peer.name,
    kind: c.row.kind,
    payload: JSON.parse(c.row.payload),
    sent_at: Number(c.sentAtUs),
  }));

  let url: string;
  let headers: Record<string, string>;
  let body: string;
  if (cfg.transport === 'reducer') {
    if (!token) return all({ ok: false, dead: false, detail: 'not paired with peer yet' });
    url = `${peer.base}/call/idc_receive`;
    body = JSON.stringify([JSON.stringify(envelopes[0])]);
    headers = { 'content-type': 'application/json', authorization: `Bearer ${token}` };
  } else {
    url = `${peer.base}/route/idc/inbox`;
    body = JSON.stringify(envelopes);
    headers = { 'content-type': 'application/json', [SIGNATURE_HEADER]: sign(cfg.secret, batch[0].sentAtUs, enc.encode(body)) };
  }

  let status: number;
  let text: string;
  try {
    const res = ctx.http.fetch(url, { method: 'POST', headers, body, timeout: HTTP_TIMEOUT });
    status = res.status;
    text = res.text();
  } catch (e) {
    // The TS SDK's fetch throws on non-standard status codes, including 530, which is how
    // `/call` reports a reducer error. The body (the reason) is lost, so assume the peer
    // forgot us: re-pair and retry once, then treat it as a permanent refusal.
    const m = /invalid status code: (\d+)/.exec(String(e));
    if (m && m[1] === '530') {
      return batch.map((c) =>
        c.row.attempts === 0
          ? { ok: false, dead: false, repair: true, detail: '530 from idc_receive (reason unreadable); re-pairing' }
          : { ok: false, dead: true, detail: '530 from idc_receive again after re-pairing: peer refused the message' }
      );
    }
    return all({ ok: false, dead: false, detail: `transport error: ${String(e)}` });
  }
  if (status < 200 || status >= 300) {
    const detail = `${status} ${text.slice(0, 300)}`;
    // 4xx = the peer understood and refused. 401/404 stay retryable (not paired / not published yet).
    const permanent = status >= 400 && status < 500 && ![401, 404, 408, 429].includes(status);
    return all({ ok: false, dead: permanent, detail });
  }
  if (cfg.transport === 'reducer') return all({ ok: true, detail: `${status} via idc_receive` });
  let results: any[] = [];
  try {
    results = JSON.parse(text).results ?? [];
  } catch {}
  return batch.map((_, i) => {
    const r = results[i];
    if (!r) return { ok: false, dead: false, detail: 'peer returned no result for this message' };
    if (r.ok === true) return { ok: true, detail: `${status} batch of ${batch.length}${r.duplicate ? ' (duplicate)' : ''}` };
    return { ok: false, dead: true, detail: `rejected: ${r.error ?? '?'}` };
  });
}

// ---------------------------------------------------------------------------
// Receiving
// ---------------------------------------------------------------------------

export interface Message {
  id: string;
  from: string;
  kind: string;
  payload: any;
}

export interface ReceiveOptions<Tx> {
  /** Narrow the consumer's transaction context to this submodule, e.g. `tx => tx.as.idc`. */
  scope: (tx: Tx) => IdcCtx;
  /** Apply a message in the receiving transaction. Throw to refuse it permanently (dead letter). */
  onMessage: (tx: Tx, msg: Message) => void;
}

/** Dedupe + apply, inside the caller's transaction. Returns true for a duplicate. */
function receiveIn<Tx>(tx: Tx, env: Envelope, transport: string, opts: ReceiveOptions<Tx>): boolean {
  const idc = opts.scope(tx);
  const me = getConfig(idc).self;
  if (env.to !== me) throw new Error(`message addressed to \`${env.to}\`, this is \`${me}\``);
  const key = `${env.from}|${env.id}`;
  if (idc.db.seen.key.find(key)) {
    writeLog(idc, { direction: 'in', peer: env.from, kind: env.kind, transport, msgId: env.id, event: 'duplicate', detail: 'already applied' });
    return true;
  }
  idc.db.seen.insert({ key, at: idc.timestamp });
  opts.onMessage(tx, { id: env.id, from: env.from, kind: env.kind, payload: env.payload });
  const latencyUs = idc.timestamp.microsSinceUnixEpoch - BigInt(Math.trunc(env.sent_at));
  writeLog(idc, { direction: 'in', peer: env.from, kind: env.kind, transport, msgId: env.id, event: 'applied', latencyUs });
  return false;
}

const json = (status: number, value: unknown) =>
  new SyncResponse(JSON.stringify(value), { status, headers: { 'content-type': 'application/json' } });

interface TxRunner<Tx> {
  withTx<T>(body: (tx: Tx) => T): T;
  timestamp: Timestamp;
}

/**
 * Transport `route`. Register on your router as `/idc/inbox`:
 * `spacetimedb.httpHandler((ctx, req) => idc.inbox(ctx, req, { scope: tx => tx.as.idc, onMessage }))`
 */
export function inbox<Tx>(ctx: TxRunner<Tx>, req: Request, opts: ReceiveOptions<Tx>): SyncResponse {
  const body = req.bytes();
  const secret = ctx.withTx((tx) => getConfig(opts.scope(tx)).secret);
  try {
    verify(secret, req.headers.get(SIGNATURE_HEADER), body, ctx.timestamp.microsSinceUnixEpoch);
  } catch (e) {
    return json(401, { error: (e as Error).message });
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(new TextDecoder().decode(body));
  } catch (e) {
    return json(400, { error: `bad JSON: ${(e as Error).message}` });
  }
  const single = !Array.isArray(parsed);
  const envelopes = (single ? [parsed] : parsed) as Envelope[];
  const results = envelopes.map((env) => {
    try {
      const duplicate = ctx.withTx((tx) => receiveIn(tx, env, 'route', opts));
      return { id: env.id, ok: true, duplicate };
    } catch (e) {
      return { id: env?.id, ok: false, error: (e as Error).message };
    }
  });
  if (single) {
    const r = results[0];
    return r.ok ? json(200, { duplicate: r.duplicate, ok: true }) : json(422, { error: r.error });
  }
  return json(200, { results });
}

interface ReducerLike {
  sender: Identity;
}

/**
 * Transport `reducer`. Expose it from your module as a reducer named `idc_receive`:
 * `spacetimedb.reducer({ name: 'idc_receive' }, { envelope: t.string() }, (ctx, { envelope }) => idc.receive(ctx, envelope, { scope: c => c.as.idc, onMessage }))`
 */
export function receive<Ctx extends ReducerLike>(ctx: Ctx, envelope: string, opts: ReceiveOptions<Ctx>): void {
  const idc = opts.scope(ctx);
  const known = idc.db.knownPeer.identity.find(ctx.sender);
  if (!known) throw new SenderError(`${ctx.sender.toHexString()} is not a known peer`);
  let env: Envelope;
  try {
    env = JSON.parse(envelope);
  } catch (e) {
    throw new SenderError(`bad envelope: ${(e as Error).message}`);
  }
  if (env.from !== known.name) throw new SenderError(`identity belongs to \`${known.name}\`, envelope claims \`${env.from}\``);
  receiveIn(ctx, env, 'reducer', opts);
}

// ---------------------------------------------------------------------------
// Pairing
// ---------------------------------------------------------------------------

/** Scheduled procedure: mint an identity on each peer's host and introduce it, signed. */
export const pair = spacetimedb.procedure({ onSchedule: pairJob }, { arg: pairJob.rowType }, t.unit(), (ctx, { arg }) => {
  const cfg = ctx.withTx((tx) => tx.db.config.key.find(0));
  if (!cfg) return {};
  let pending = false;
  for (const peer of parsePeers(cfg.peers)) {
    if (ctx.withTx((tx) => tx.db.peerToken.peer.find(peer.name))) continue;
    try {
      const minted = ctx.http.fetch(`${hostOf(peer)}/v1/identity`, { method: 'POST', timeout: HTTP_TIMEOUT });
      if (!minted.ok) throw new Error(`mint identity: HTTP ${minted.status}`);
      const { identity, token } = minted.json() as { identity: string; token: string };
      const body = JSON.stringify({ from: cfg.self, identity });
      const nowUs = ctx.withTx((tx) => tx.timestamp.microsSinceUnixEpoch);
      const res = ctx.http.fetch(`${peer.base}/route/idc/pair`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', [SIGNATURE_HEADER]: sign(cfg.secret, nowUs, enc.encode(body)) },
        body,
        timeout: HTTP_TIMEOUT,
      });
      if (!res.ok) throw new Error(`pair: HTTP ${res.status} ${res.text()}`);
      ctx.withTx((tx) => {
        tx.db.peerToken.peer.delete(peer.name);
        tx.db.peerToken.insert({ peer: peer.name, identity: Identity.fromString(identity), token, pairedAt: tx.timestamp });
        writeLog(tx, { direction: 'out', peer: peer.name, kind: 'pair', transport: 'reducer', msgId: '', event: 'paired', detail: `we are ${identity.slice(0, 16)} on ${peer.name}` });
      });
    } catch (e) {
      pending = true;
      ctx.withTx((tx) => writeLog(tx, { direction: 'out', peer: peer.name, kind: 'pair', transport: 'reducer', msgId: '', event: 'retry', detail: String((e as Error).message ?? e) }));
    }
  }
  if (pending) {
    const attempt = arg.attempt + 1;
    const backoffUs = BigInt(Math.min(500 * 2 ** Math.min(attempt, 7), MAX_BACKOFF_MS)) * 1000n;
    ctx.withTx((tx) => {
      tx.db.pairJob.insert({ scheduledId: 0n, scheduledAt: ScheduleAt.time(tx.timestamp.microsSinceUnixEpoch + backoffUs), attempt });
    });
  }
  return {};
});

/**
 * Peer side of pairing. Register on your router as `/idc/pair`:
 * `spacetimedb.httpHandler((ctx, req) => idc.pairRoute(ctx.as.idc, req))`
 */
export function pairRoute(ctx: IdcHandlerCtx, req: Request): SyncResponse {
  const body = req.bytes();
  const cfg = ctx.withTx((tx) => getConfig(tx));
  try {
    verify(cfg.secret, req.headers.get(SIGNATURE_HEADER), body, ctx.timestamp.microsSinceUnixEpoch);
  } catch (e) {
    return json(401, { error: (e as Error).message });
  }
  let from: string;
  let identity: Identity;
  try {
    const p = JSON.parse(new TextDecoder().decode(body));
    from = String(p.from);
    identity = Identity.fromString(String(p.identity));
  } catch (e) {
    return json(400, { error: (e as Error).message });
  }
  if (!parsePeers(cfg.peers).some((p) => p.name === from)) return json(403, { error: `unknown peer \`${from}\`` });
  ctx.withTx((tx) => {
    for (const k of [...tx.db.knownPeer.name.filter(from)]) tx.db.knownPeer.identity.delete(k.identity);
    tx.db.knownPeer.insert({ identity, name: from, pairedAt: tx.timestamp });
    writeLog(tx, { direction: 'in', peer: from, kind: 'pair', transport: 'reducer', msgId: '', event: 'paired', detail: `${identity.toHexString().slice(0, 16)} is ${from}` });
  });
  return json(200, { ok: true });
}

// ---------------------------------------------------------------------------
// Request/response and SQL pulls (from procedures)
// ---------------------------------------------------------------------------

/** Signed synchronous call to a peer's route, e.g. `idc.rpc(ctx.as.idc, 'warehouse', '/rpc/stock', { sku })`. */
export function rpc(ctx: IdcProcedureCtx, peerName: string, path: string, payload: unknown): any {
  const { cfg, nowUs } = ctx.withTx((tx) => ({ cfg: getConfig(tx), nowUs: tx.timestamp.microsSinceUnixEpoch }));
  const peer = findPeer(cfg, peerName);
  const body = JSON.stringify({ from: cfg.self, payload });
  const res = ctx.http.fetch(`${peer.base}/route${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', [SIGNATURE_HEADER]: sign(cfg.secret, nowUs, enc.encode(body)) },
    body,
    timeout: HTTP_TIMEOUT,
  });
  const text = res.text();
  if (!res.ok) throw new Error(`HTTP ${res.status}: ${text}`);
  return JSON.parse(text);
}

/** For RPC handlers: verify and return `{ from, payload }`, or a 401/400 response to return as-is. */
export function verifyRpc(ctx: IdcHandlerCtx, req: Request): { from: string; payload: any } | SyncResponse {
  const body = req.bytes();
  const secret = ctx.withTx((tx) => getConfig(tx).secret);
  try {
    verify(secret, req.headers.get(SIGNATURE_HEADER), body, ctx.timestamp.microsSinceUnixEpoch);
    const v = JSON.parse(new TextDecoder().decode(body));
    return { from: String(v.from ?? ''), payload: v.payload };
  } catch (e) {
    return json(401, { error: (e as Error).message });
  }
}

export const rpcReply = (value: unknown) => json(200, value);

/** SQL against a peer's `/sql`, as our paired identity when we have one. */
export function sql(ctx: IdcProcedureCtx, peerName: string, query: string): any {
  const { cfg, token } = ctx.withTx((tx) => ({ cfg: getConfig(tx), token: tx.db.peerToken.peer.find(peerName)?.token }));
  const peer = findPeer(cfg, peerName);
  const headers: Record<string, string> = { 'content-type': 'text/plain' };
  if (token) headers.authorization = `Bearer ${token}`;
  const res = ctx.http.fetch(`${peer.base}/sql`, { method: 'POST', headers, body: query, timeout: HTTP_TIMEOUT });
  const text = res.text();
  if (!res.ok) throw new Error(`HTTP ${res.status}: ${text}`);
  return JSON.parse(text);
}

// ---------------------------------------------------------------------------
// Housekeeping and introspection
// ---------------------------------------------------------------------------

export const prune = spacetimedb.reducer({ onSchedule: pruneJob }, { arg: pruneJob.rowType }, (ctx) => {
  const cutoff = ctx.timestamp.microsSinceUnixEpoch - SEEN_RETENTION_US;
  for (const s of [...ctx.db.seen.iter()]) {
    if (s.at.microsSinceUnixEpoch < cutoff) ctx.db.seen.key.delete(s.key);
  }
});

/** The same IDC summary the Rust side serves, for dashboards. */
export function stateJson(ctx: IdcCtx) {
  const cfg = ctx.db.config.key.find(0);
  const rows = [...ctx.db.log.iter()].sort((a, b) => (a.id > b.id ? -1 : 1)).slice(0, 40);
  const outboxRows = [...ctx.db.outbox.iter()];
  return {
    self: cfg?.self ?? '',
    transport: cfg?.transport ?? '',
    outbox_pending: outboxRows.filter((r) => !r.dead).length,
    outbox_dead: outboxRows.filter((r) => r.dead).length,
    has_token_for: [...ctx.db.peerToken.iter()].map((r) => r.peer),
    trusts: [...ctx.db.knownPeer.iter()].map((r) => r.name),
    log: rows.map((l) => ({
      id: Number(l.id),
      at_us: Number(l.at.microsSinceUnixEpoch),
      direction: l.direction,
      peer: l.peer,
      kind: l.kind,
      transport: l.transport,
      msg_id: l.msgId,
      event: l.event,
      detail: l.detail,
      latency_us: Number(l.latencyUs),
    })),
  };
}
