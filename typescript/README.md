# spacetimedb-idc for TypeScript (submodule)

Inter-database communication as a **SpacetimeDB submodule**. Mount it in your module and your database can push
messages to other databases (in TypeScript, Rust or C#) and react to theirs, with no polling.

Source: [`src/index.ts`](src/index.ts) · Complete example: [`demo/typescript/shop`](../demo/typescript/shop/src/index.ts) ·
Protocol: [`docs/PROTOCOL.md`](../docs/PROTOCOL.md)

## Install

Not on npm yet. Copy this folder into your repo and add it as an npm workspace package, so there's a single copy of
the `spacetimedb` package:

```jsonc
// package.json at your repo root
{ "private": true, "workspaces": ["spacetimedb-idc", "my-module"] }
```

```jsonc
// my-module/package.json
{ "dependencies": { "spacetimedb": "2.11.*", "spacetimedb-idc": "0.1.0" } }
```

> With two copies of `spacetimedb` (e.g. a `node_modules` inside this folder), `spacetime publish` fails with
> *"Local module schema inspection failed"*.

## Wire it up

Submodules can't read env vars, have lifecycle reducers or contribute routes, so your module adds a few lines:

```typescript
import { schema, table, t, Router, type ReducerCtx } from 'spacetimedb/server';
import * as idc from 'spacetimedb-idc';                 // `import * as`, not a default import

const spacetimedb = schema(
  { order, idc },                                       // mounted under the namespace "idc"
  { env: { IDC_SELF: t.string(), IDC_SECRET: t.string(), IDC_PEERS: t.string(),
           IDC_TRANSPORT: t.enum('IdcTransport', ['route', 'reducer']) } }
);
export default spacetimedb;
type Ctx = ReducerCtx<typeof spacetimedb.schemaType>;

// 1. Hand the config in (from init, and from idc_kick after `publish --env-only`).
const cfg = (ctx: Ctx) => ({ self: ctx.env.IDC_SELF, secret: ctx.env.IDC_SECRET,
                             peers: ctx.env.IDC_PEERS, transport: ctx.env.IDC_TRANSPORT });
export const init    = spacetimedb.init((ctx) => idc.configure(ctx.as.idc, cfg(ctx)));
export const idcKick = spacetimedb.reducer((ctx) => idc.configure(ctx.as.idc, cfg(ctx)));

// 2. Register the routes (and the reducer, if you use the reducer transport).
const handlers = { scope: (tx: Ctx) => tx.as.idc, onMessage };
export const idcInbox   = spacetimedb.httpHandler((ctx, req) => idc.inbox(ctx, req, handlers));
export const idcPair    = spacetimedb.httpHandler((ctx, req) => idc.pairRoute(ctx.as.idc, req));
export const idcReceive = spacetimedb.reducer({ envelope: t.string() },
  (ctx, { envelope }) => idc.receive(ctx, envelope, handlers));
export const router = spacetimedb.httpRouter(
  new Router().post('/idc/inbox', idcInbox).post('/idc/pair', idcPair));

// 3. Handle messages. This runs in the receiving transaction, together with the dedupe record.
function onMessage(tx: Ctx, msg: idc.Message) {
  if (msg.kind === 'reservation') { /* update tx.db.order ... */ return; }
  throw new Error(`unknown kind ${msg.kind}`);   // throw = permanent refusal (dead letter)
}

// 4. Send from any reducer. The message commits with your transaction and is pushed right after.
export const placeOrder = spacetimedb.reducer({ sku: t.string(), qty: t.u32() }, (ctx, { sku, qty }) => {
  const o = ctx.db.order.insert({ /* ... */ });
  idc.send(ctx.as.idc, 'warehouse', 'reserve', { order_id: Number(o.id), sku, qty });
});
```

Publish with the four env vars:

```bash
IDC_SELF=shop IDC_SECRET=$SECRET IDC_TRANSPORT=route \
IDC_PEERS=warehouse=https://maincloud.spacetimedb.com/v1/database/my-warehouse \
spacetime publish my-shop
```

## API

| Function | Use it from | What it does |
|---|---|---|
| `configure(ctx.as.idc, cfg)` | `init`, `idc_kick` | Stores config and does the idempotent setup (epoch, cleanup schedule, pairing) |
| `send(ctx.as.idc, peer, kind, payload)` | reducers, `withTx` | Queues a message in the current transaction |
| `inbox(ctx, req, { scope, onMessage })` | your `/idc/inbox` handler | Route transport: verifies, dedupes and applies (single or batch) |
| `receive(ctx, envelope, { scope, onMessage })` | your `idc_receive` reducer | Reducer transport: checks the caller is a paired peer |
| `pairRoute(ctx.as.idc, req)` | your `/idc/pair` handler | Peer side of the pairing handshake |
| `rpc(ctx.as.idc, peer, path, payload)` | procedures | Signed synchronous call to a peer's route; returns its JSON |
| `verifyRpc(ctx.as.idc, req)` / `rpcReply(value)` | RPC handlers | Check an incoming RPC and answer it |
| `sql(ctx.as.idc, peer, query)` | procedures | SQL against the peer's `/sql` |
| `stateJson(tx.as.idc)` | anywhere | Summary for dashboards: pending, dead, paired, recent log |

Tables (under your namespace): `idc.config`, `idc.outbox`, `idc.seen`, `idc.known_peer`, `idc.peer_token`, `idc.state`
(all private), `idc.log` (public: metadata only), plus three schedule tables. The scheduled `idc.flush` / `idc.pair`
procedures run without any wiring.

## Handoffs

`spacetimedb-idc/handoff` moves an entity (say, a character) to another database with no window where it's duplicated
or lost. Its tables (`idc.handoff`, public, plus a sequence and a timeout schedule) are part of the idc submodule, so
there's nothing extra to mount:

```typescript
import * as handoff from 'spacetimedb-idc/handoff';

function onMessage(tx: Ctx, msg: idc.Message) {
  if (handoff.onIdcMessage(tx, msg, { scope: (c: Ctx) => c.as.idc, hooks })) return;   // hooks: handoff.Hooks<Ctx>
  // ...
}
handoff.start(ctx.as.idc, 'shard-b', name, data);    // lock + offer, in this transaction
handoff.isLocked(ctx.as.idc, name);                   // refuse moves/trades while in transit
```

See [`docs/HANDOFF.md`](../docs/HANDOFF.md) and the [shard demo](../demo/typescript/shard/src/index.ts).

## Notes

- `ctx.http.fetch` in TypeScript throws on HTTP 530, which is how `/call` reports a reducer error, so the reason is
  lost. On the reducer transport the library re-pairs once, then treats a repeat 530 as a refusal. The route
  transport isn't affected.
- Delivery waits for durability on both ends (`/sql?confirmed=true` on itself before sending, and on the peer before
  dropping the message), because SpacetimeDB acknowledges commits before they're on disk. It assumes this database
  lives on the same host as its first peer.
- HMAC comes from `@noble/hashes`, because the module runtime has no WebCrypto.
- u64 columns are `bigint`. The library converts ids to numbers in JSON.
