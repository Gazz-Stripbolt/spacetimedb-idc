/**
 * **shop**, in TypeScript, using `spacetimedb-idc` as a submodule.
 *
 * Same behaviour as `shop-rs`, and it talks to the Rust `warehouse` over the same
 * wire protocol. Everything IDC-specific is in the "idc wiring" block: about 25 lines.
 */
import { schema, table, t, Router, SyncResponse, type ReducerCtx } from 'spacetimedb/server';
import * as idc from 'spacetimedb-idc';

const WAREHOUSE = 'warehouse';

const order = table(
  { name: 'order', public: true },
  {
    id: t.u64().primaryKey().autoInc(),
    sku: t.string(),
    qty: t.u32(),
    /** pending → confirmed | rejected */
    status: t.string(),
    note: t.string(),
    placed: t.timestamp(),
    settled: t.option(t.timestamp()),
  }
);

/** The warehouse's stock as last pushed to us. Never polled. */
const stockMirror = table(
  { name: 'stock_mirror', public: true },
  { sku: t.string().primaryKey(), qty: t.u32(), updated: t.timestamp() }
);

const spacetimedb = schema(
  { order, stockMirror, idc },
  {
    env: {
      IDC_SELF: t.string(),
      IDC_SECRET: t.string(),
      IDC_PEERS: t.string(),
      IDC_TRANSPORT: t.enum('IdcTransport', ['route', 'reducer']),
    },
  }
);
export default spacetimedb;

// ---------------------------------------------------------------------------
// idc wiring: config in, routes registered, messages handled
// ---------------------------------------------------------------------------

type Ctx = ReducerCtx<typeof spacetimedb.schemaType>;

const idcConfig = (ctx: { env: { IDC_SELF: string; IDC_SECRET: string; IDC_PEERS: string; IDC_TRANSPORT: string } }) => ({
  self: ctx.env.IDC_SELF,
  secret: ctx.env.IDC_SECRET,
  peers: ctx.env.IDC_PEERS,
  transport: ctx.env.IDC_TRANSPORT,
});

export const init = spacetimedb.init((ctx) => idc.configure(ctx.as.idc, idcConfig(ctx)));

/** Re-read config after `spacetime publish --env-only` (submodules can't read env themselves). */
export const idcKick = spacetimedb.reducer((ctx) => idc.configure(ctx.as.idc, idcConfig(ctx)));

const handlers = { scope: (tx: Ctx) => tx.as.idc, onMessage };

export const idcInbox = spacetimedb.httpHandler((ctx, req) => idc.inbox(ctx, req, handlers));
export const idcPair = spacetimedb.httpHandler((ctx, req) => idc.pairRoute(ctx.as.idc, req));
export const idcReceive = spacetimedb.reducer({ envelope: t.string() }, (ctx, { envelope }) =>
  idc.receive(ctx, envelope, handlers)
);

// ---------------------------------------------------------------------------
// The app
// ---------------------------------------------------------------------------

/** Order row and `reserve` message commit together; the warehouse sees it moments later. */
export const placeOrder = spacetimedb.reducer({ sku: t.string(), qty: t.u32() }, (ctx, { sku, qty }) => {
  if (!sku || qty === 0 || qty > 1000) throw new Error('need a sku and 1..=1000 qty');
  const o = ctx.db.order.insert({ id: 0n, sku, qty, status: 'pending', note: '', placed: ctx.timestamp, settled: undefined });
  idc.send(ctx.as.idc, WAREHOUSE, 'reserve', { order_id: Number(o.id), sku, qty });
});

/** Called for every incoming message, inside the receiving transaction. */
function onMessage(tx: Ctx, msg: idc.Message): void {
  const p = msg.payload;
  switch (msg.kind) {
    case 'reservation': {
      const o = tx.db.order.id.find(BigInt(p.order_id));
      if (!o) throw new Error(`no order ${p.order_id}`);
      tx.db.order.id.update({ ...o, status: p.ok === true ? 'confirmed' : 'rejected', note: String(p.note ?? ''), settled: tx.timestamp });
      return;
    }
    case 'stock': {
      const row = { sku: String(p.sku), qty: Number(p.qty), updated: tx.timestamp };
      if (tx.db.stockMirror.sku.find(row.sku)) tx.db.stockMirror.sku.update(row);
      else tx.db.stockMirror.insert(row);
      return;
    }
    default:
      throw new Error(`shop doesn't handle \`${msg.kind}\` messages`);
  }
}

/** Request/response across databases. `spacetime call shop quote gear` */
export const quote = spacetimedb.procedure({ sku: t.string() }, t.string(), (ctx, { sku }) => {
  try {
    return JSON.stringify(idc.rpc(ctx.as.idc, WAREHOUSE, '/rpc/stock', { sku }));
  } catch (e) {
    return JSON.stringify({ error: String((e as Error).message) });
  }
});

/** SQL pull from the other database. `spacetime call shop peek_warehouse` */
export const peekWarehouse = spacetimedb.procedure(t.string(), (ctx) => {
  try {
    return JSON.stringify(idc.sql(ctx.as.idc, WAREHOUSE, 'SELECT * FROM stock'));
  } catch (e) {
    return JSON.stringify({ error: String((e as Error).message) });
  }
});

/** JSON for the dashboard (served by the Rust warehouse at /route/). */
export const state = spacetimedb.httpHandler((ctx) => {
  const body = ctx.withTx((tx) => {
    const orders = [...tx.db.order.iter()]
      .sort((a, b) => (a.id > b.id ? -1 : 1))
      .slice(0, 40)
      .map((o) => ({
        id: Number(o.id),
        sku: o.sku,
        qty: o.qty,
        status: o.status,
        note: o.note,
        round_trip_ms: o.settled ? Number(o.settled.microsSinceUnixEpoch - o.placed.microsSinceUnixEpoch) / 1000 : null,
      }));
    const stock_mirror = [...tx.db.stockMirror.iter()].map((s) => ({ sku: s.sku, qty: s.qty })).sort((a, b) => a.sku.localeCompare(b.sku));
    return { orders, stock_mirror, idc: idc.stateJson(tx.as.idc) };
  });
  return new SyncResponse(JSON.stringify(body), { headers: { 'content-type': 'application/json', 'cache-control': 'no-store' } });
});

export const router = spacetimedb.httpRouter(
  new Router().post('/idc/inbox', idcInbox).post('/idc/pair', idcPair).get('/api/state', state)
);
