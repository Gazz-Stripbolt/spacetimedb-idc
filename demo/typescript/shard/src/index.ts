/**
 * **shard**, in TypeScript: one strip of a game world, using `spacetimedb-idc` and its handoffs.
 *
 * Same behaviour as the Rust shard (`demo/rust/shard`). Publish it twice (e.g. `shard-a` and `shard-b`)
 * and point each at the other; characters walk across the border with a handoff, and any pairing of
 * TypeScript, Rust and C# shards works.
 */
import { schema, table, t, Router, SyncResponse, SenderError, type ReducerCtx } from 'spacetimedb/server';
import * as idc from 'spacetimedb-idc';
import * as handoff from 'spacetimedb-idc/handoff';

/**
 * Each shard is `WIDTH` columns of the world. The shard whose name sorts first is the west
 * half (x 0..8), the other one the east half (x 8..16).
 */
const WIDTH = 8;
const HEIGHT = 5;
const LIVE = 'live';
const PENDING = 'pending';

const character = table(
  { name: 'character', public: true },
  {
    name: t.string().primaryKey(),
    /** World coordinates, not shard-local ones. */
    x: t.i32(),
    y: t.i32(),
    hp: t.u32(),
    gold: t.u32(),
    bag: t.array(t.string()),
    /** `live`, or `pending` while it's arriving and the source hasn't let go yet. */
    state: t.string(),
    note: t.string(),
    updated: t.timestamp(),
  }
);

/** Arrivals are refused once `capacity` characters are here. */
const shardConfig = table({ name: 'shard_config' }, { key: t.u8().primaryKey(), capacity: t.u32() });

const spacetimedb = schema(
  { character, shardConfig, idc },
  {
    env: {
      IDC_SELF: t.string(),
      IDC_SECRET: t.string(),
      IDC_PEERS: t.string(),
      IDC_TRANSPORT: t.enum('IdcTransport', ['route', 'reducer']),
      IDC_DURABILITY: t.option(t.enum('IdcDurability', ['confirmed', 'unsafe'])),
    },
  }
);
export default spacetimedb;

// ---------------------------------------------------------------------------
// idc wiring: config in, routes registered, messages handled
// ---------------------------------------------------------------------------

type Ctx = ReducerCtx<typeof spacetimedb.schemaType>;
type Character = NonNullable<ReturnType<Ctx['db']['character']['name']['find']>>;

const idcConfig = (ctx: { env: { IDC_SELF: string; IDC_SECRET: string; IDC_PEERS: string; IDC_TRANSPORT: string; IDC_DURABILITY?: string } }) => ({
  self: ctx.env.IDC_SELF,
  secret: ctx.env.IDC_SECRET,
  peers: ctx.env.IDC_PEERS,
  transport: ctx.env.IDC_TRANSPORT,
  durability: ctx.env.IDC_DURABILITY,
});

export const init = spacetimedb.init((ctx) => {
  ctx.db.shardConfig.insert({ key: 0, capacity: 50 });
  idc.configure(ctx.as.idc, idcConfig(ctx));
});

/** Re-read config after `spacetime publish --env-only` (submodules can't read env themselves). */
export const idcKick = spacetimedb.reducer((ctx) => idc.configure(ctx.as.idc, idcConfig(ctx)));

const handlers = { scope: (tx: Ctx) => tx.as.idc, onMessage };

export const idcInbox = spacetimedb.httpHandler((ctx, req) => idc.inbox(ctx, req, handlers));
export const idcPair = spacetimedb.httpHandler((ctx, req) => idc.pairRoute(ctx.as.idc, req));
export const idcReceive = spacetimedb.reducer({ envelope: t.string() }, (ctx, { envelope }) =>
  idc.receive(ctx, envelope, handlers)
);

/** Called for every incoming message, inside the receiving transaction. */
function onMessage(tx: Ctx, msg: idc.Message): void {
  if (handoff.onIdcMessage(tx, msg, { scope: (c: Ctx) => c.as.idc, hooks })) return;
  throw new Error(`shard doesn't handle \`${msg.kind}\` messages`);
}

// ---------------------------------------------------------------------------
// The world
// ---------------------------------------------------------------------------

const self = (ctx: Ctx) => ctx.env.IDC_SELF;

/** The other shard: the first entry in `IDC_PEERS`. */
function neighbour(ctx: Ctx): string {
  const p = idc.parsePeers(ctx.env.IDC_PEERS)[0];
  if (!p) throw new SenderError('no neighbouring shard configured');
  return p.name;
}

/** The world columns this shard owns: `lo..lo + WIDTH`. */
function lo(ctx: Ctx): number {
  const other = idc.parsePeers(ctx.env.IDC_PEERS)[0]?.name;
  return other !== undefined && other < self(ctx) ? WIDTH : 0;
}

const owns = (ctx: Ctx, x: number) => x >= lo(ctx) && x < lo(ctx) + WIDTH;

function live(ctx: Ctx, name: string): Character {
  const c = ctx.db.character.name.find(name);
  if (!c) throw new SenderError(`no character \`${name}\` here`);
  if (c.state !== LIVE || handoff.isLocked(ctx.as.idc, name)) throw new SenderError(`\`${name}\` is in transit`);
  return c;
}

const data = (c: Character, x: number) => ({ name: c.name, x, y: c.y, hp: c.hp, gold: c.gold, bag: c.bag });

export const spawn = spacetimedb.reducer({ name: t.string() }, (ctx, { name }) => {
  if (!name || name.length > 24) throw new SenderError('name must be 1-24 characters');
  if (ctx.db.character.name.find(name)) throw new SenderError(`\`${name}\` already exists here`);
  ctx.db.character.insert({
    name, x: lo(ctx) + WIDTH / 2, y: Math.floor(HEIGHT / 2), hp: 100, gold: 10, bag: ['torch'], state: LIVE, note: '', updated: ctx.timestamp,
  });
});

/**
 * Walk one square. Stepping over the border starts a handoff to the neighbouring shard;
 * the character waits at the border, locked, until the neighbour has it.
 */
export const step = spacetimedb.reducer({ name: t.string(), dx: t.i32(), dy: t.i32() }, (ctx, { name, dx, dy }) => {
  if (Math.abs(dx) > 1 || Math.abs(dy) > 1) throw new SenderError('one square at a time');
  const c = live(ctx, name);
  const x = c.x + dx;
  const y = Math.min(Math.max(c.y + dy, 0), HEIGHT - 1);
  if (owns(ctx, x)) {
    ctx.db.character.name.update({ ...c, x, y, note: '', updated: ctx.timestamp });
    return;
  }
  if (x < 0 || x >= 2 * WIDTH) throw new SenderError("that's the edge of the world");
  const peer = neighbour(ctx);
  handoff.start(ctx.as.idc, peer, name, data({ ...c, y }, x));
  ctx.db.character.name.update({ ...c, y, note: `crossing to ${peer}…`, updated: ctx.timestamp });
});

/**
 * Send a character to the neighbour without walking (it lands on the nearest column there).
 * `timeout_ms` = 0 uses the default.
 */
export const transfer = spacetimedb.reducer({ name: t.string(), timeoutMs: t.u64() }, (ctx, { name, timeoutMs }) => {
  const c = live(ctx, name);
  const peer = neighbour(ctx);
  const x = lo(ctx) === 0 ? WIDTH : WIDTH - 1;
  handoff.start(ctx.as.idc, peer, name, data(c, x), timeoutMs === 0n ? handoff.DEFAULT_TIMEOUT_MS : Number(timeoutMs));
});

export const cancelTransfer = spacetimedb.reducer({ name: t.string() }, (ctx, { name }) => {
  const h = handoff.latest(ctx.as.idc, name);
  if (!h || h.role !== 'out') throw new SenderError(`\`${name}\` isn't leaving`);
  handoff.cancel(ctx.as.idc, h.id, 'cancelled by player');
});

/** Trading needs both characters live and here. Locked ones can't trade. */
export const give = spacetimedb.reducer({ from: t.string(), to: t.string(), gold: t.u32() }, (ctx, { from, to, gold }) => {
  const a = live(ctx, from);
  const b = live(ctx, to);
  if (from === to || a.gold < gold) throw new SenderError('not enough gold');
  ctx.db.character.name.update({ ...a, gold: a.gold - gold });
  ctx.db.character.name.update({ ...b, gold: b.gold + gold });
});

export const setCapacity = spacetimedb.reducer({ capacity: t.u32() }, (ctx, { capacity }) => {
  ctx.db.shardConfig.key.update({ key: 0, capacity });
});

// ---------------------------------------------------------------------------
// Handoff hooks
// ---------------------------------------------------------------------------

const hooks: handoff.Hooks<Ctx> = {
  validate(tx, _from, entity, d) {
    const capacity = tx.db.shardConfig.key.find(0)?.capacity ?? 0;
    if (tx.db.character.count() >= BigInt(capacity)) throw new Error(`${self(tx)} is full`);
    if (tx.db.character.name.find(entity)) throw new Error(`the name \`${entity}\` is taken on ${self(tx)}`);
    if (d?.name !== entity) throw new Error("data doesn't match the entity id");
    if (!Number.isInteger(d.x)) throw new Error('missing x');
    if (!owns(tx, d.x)) throw new Error(`x = ${d.x} isn't on ${self(tx)}`);
    if (d.hp === 0) throw new Error("fallen characters can't travel");
  },
  import(tx, from, entity, d) {
    const int = (v: unknown) => (Number.isInteger(v) ? (v as number) : 0);
    tx.db.character.insert({
      name: entity,
      x: int(d.x),
      y: int(d.y),
      hp: int(d.hp),
      gold: int(d.gold),
      bag: Array.isArray(d.bag) ? d.bag.filter((i: unknown) => typeof i === 'string') : [],
      state: PENDING,
      note: `arriving from ${from}…`,
      updated: tx.timestamp,
    });
  },
  activate(tx, entity) {
    const c = tx.db.character.name.find(entity);
    if (c) tx.db.character.name.update({ ...c, state: LIVE, note: '', updated: tx.timestamp });
  },
  discard(tx, entity) {
    if (tx.db.character.name.find(entity)?.state === PENDING) tx.db.character.name.delete(entity);
  },
  remove(tx, entity) {
    tx.db.character.name.delete(entity);
  },
  returned(tx, entity, reason) {
    const c = tx.db.character.name.find(entity);
    if (c) tx.db.character.name.update({ ...c, note: `stayed: ${reason}`, updated: tx.timestamp });
  },
};

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

/** JSON for the dashboard and the tests; same shape as the Rust shard's. */
export const state = spacetimedb.httpHandler((ctx) => {
  const body = ctx.withTx((tx) => {
    const characters = [...tx.db.character.iter()]
      .sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0))
      .map((c) => ({
        name: c.name, x: c.x, y: c.y, hp: c.hp, gold: c.gold, bag: c.bag, state: c.state, note: c.note,
        locked: handoff.isLocked(tx.as.idc, c.name),
      }));
    const handoffs = [...tx.as.idc.db.handoff.iter()]
      .sort((a, b) => (a.updated.microsSinceUnixEpoch > b.updated.microsSinceUnixEpoch ? -1 : a.updated.microsSinceUnixEpoch < b.updated.microsSinceUnixEpoch ? 1 : 0))
      .map((h) => ({
        id: h.id, entity: h.entity, role: h.role, peer: h.peer, status: h.status, detail: h.detail,
        ms: Number(h.updated.microsSinceUnixEpoch - h.started.microsSinceUnixEpoch) / 1000,
      }));
    const l = lo(tx);
    return {
      shard: { lo: l, hi: l + WIDTH, height: HEIGHT, capacity: tx.db.shardConfig.key.find(0)?.capacity ?? 0 },
      characters,
      handoffs,
      idc: idc.stateJson(tx.as.idc),
    };
  });
  return new SyncResponse(JSON.stringify(body), { headers: { 'content-type': 'application/json', 'cache-control': 'no-store' } });
});

export const router = spacetimedb.httpRouter(
  new Router().post('/idc/inbox', idcInbox).post('/idc/pair', idcPair).get('/api/state', state)
);
