# Findings: database-to-database communication on SpacetimeDB 2.11

**Tested on:** standalone 2.11.0 (built with `allow_loopback_http_for_tests`), Rust modules, a 2-vCPU Linux VM.
**Source read:** `clockworklabs/SpacetimeDB` at `v2.11.0` / master `f0d2ad4`.

## What exists today

| Building block | Can it do HTTP? | Runs when | Notes |
|---|---|---|---|
| Reducer | ❌ | A client or the scheduler calls it | Transactional and deterministic. Can insert schedule rows. |
| Procedure | ✅ `ctx.http` | A client calls it, or a **schedule row** fires | Not a transaction. Use `with_tx` for DB access, and **no HTTP inside `with_tx`**. |
| HTTP handler | ✅ `ctx.http` | An HTTP request hits `/route/...` | No caller identity. Use `with_tx` for DB access. |
| Schedule table | — | `ScheduleAt::Time(t)` once, or `Interval` | One-shot rows for procedures are deleted *before* the procedure runs. |
| `/v1/database/:db/call/:fn` | — | — | Reducer **or procedure**. Bearer token → `ctx.sender()`. Procedure return values come back in the body. |
| `/v1/database/:db/sql` | — | — | Reads of public tables need no auth. Private tables and DML need the **owner**. |
| `/v1/identity` | — | — | Mints a fresh identity + token. The token has no expiry. |

There's no native database-to-database channel and no way for a module to subscribe to another database. Everything
cross-database goes over HTTP, started by a procedure or a handler.

## Event-driven without polling: how

`reducer → outbox row + one-shot schedule row (ScheduleAt::Time(now)) → commit → procedure fires → HTTP → peer's handler/reducer → peer's transaction`.

- The one-shot schedule row fires immediately after the commit. Measured queue-to-delivered for a single hop: **~7–12 ms**.
  Full round trip (shop → warehouse → shop, three transactions and two HTTP hops): **~15–33 ms**.
- The receiving side is a normal transaction, so it can queue its own replies, and those get pushed the same way.
- Idle cost is **zero**: no interval timers. Retries schedule a single one-shot row at the next backoff time.

## Gotchas we hit (so you don't have to)

1. **Standalone blocks module HTTP to loopback and private IPs** (`refusing to connect to private or special-purpose
   addresses`). It's SSRF protection and can't be configured at runtime. The only way to run two databases on one local
   server and have them talk is the compile-time feature `allow_loopback_http_for_tests` (see `scripts/dev-server.sh`).
   CI builds and caches that binary. Your LAN IP doesn't help either: 10/8, 172.16/12 and 192.168/16 are all blocked too.
2. **No HTTP inside `with_tx`.** Delivery has to be "claim in tx 1 → HTTP → settle in tx 2". Without a lease, two
   overlapping flush procedures would send the same message twice. Procedures on wasm share one thread but interleave
   while they wait on HTTP, so the lease (`in_flight_until`) matters.
3. **`with_tx` closures must be `Fn` and may re-run.** Keep side effects (HTTP, logging outside the DB) out of them.
4. **A business "no" must not be an error.** If `on_idc_message` returns `Err`, the receiving transaction rolls back and
   the sender marks the message **dead**. Model refusals ("out of stock") as reply messages, and save `Err` for "this
   message can never be processed".
5. **Status codes decide retry vs. dead.** 2xx = done. 401/404/408/429/5xx/transport errors = retry. 401/404 stay
   retryable because they usually mean "not paired yet" or "peer not published yet". Other 4xx = dead letter.
6. **A reducer `Err` over `/call` comes back as HTTP 530** with the error text in the body. That's how the reducer
   transport notices "not a known peer" and re-pairs.
7. **Message ids need an epoch.** Auto-increment ids restart if a database is wiped, and the peer's inbox would then
   silently drop "new" messages as duplicates. Ids are `<epoch>-<outbox id>`, with the epoch fixed at `init`.
8. **Per-peer ordering costs a little throughput.** The head of a peer's queue blocks the rest until it's delivered or
   dead. That's fine for correctness-sensitive flows; drop the rule if you don't need ordering.
9. **Batching is the big throughput lever.** With one message per HTTP request, a 20-order burst settled one order every ~13 ms (55 → 304 ms).
   Batching up to 64 envelopes per request let delivery keep up with everything our load generator could produce
   (~900 msgs/s). Each envelope is still applied in its own transaction, so one bad message can't roll back its neighbours.
10. **`spacetime publish --env-only` refuses `--module-path`.** Run it from a directory without a `spacetime.json`, with
    only the changed variables set in the shell environment.
11. **The handler RNG is timestamp-seeded.** Nothing here relies on randomness for security: authentication is the HMAC
    secret or a server-issued token.

## Security model

- **Route transport:** HMAC-SHA256 over `"<micros>.<body>"` with a mesh-wide secret from an env var, ±5 minute window,
  constant-time comparison. Replays inside the window are absorbed by the inbox.
- **Reducer transport:** a SpacetimeDB token, issued by the peer's host, for an identity the peer has explicitly stored
  in `idc_known_peer`. The reducer also checks that the identity's name matches the envelope's `from`.
- **Pairing** is itself HMAC-signed, and only names listed in the receiver's `IDC_PEERS` can be paired.
- **All config lives in env vars** (owner-controlled). Tokens and payloads live in private tables. `idc_log` is public,
  but holds only metadata.
- **Not covered:** per-peer secrets, secret rotation without downtime (you could accept two secrets during a rotation),
  and rate limiting on the inbox.

## Benchmarks (local, 2 vCPU, client on the same box)

| Scenario | Result |
|---|---|
| Single order, route transport | 15–30 ms round trip |
| Single order, reducer transport | ~29–33 ms round trip |
| 20 concurrent orders, route (batched) | all settled in 26–33 ms |
| 20 concurrent orders, route, *before batching* | 55 → 304 ms (sequential) |
| 1000 orders at ~300/s, route | every order settled within ~30 ms of the last one placed |
| 300 orders at ~280/s, reducer | 47 orders/s sustained (one `/call` per message) |
| Synchronous RPC (`quote`) | ~15 ms |

## What native IDC could make easier

These are the bits the workaround has to build by hand:

- A trusted **database identity** for callers (no token minting or pairing) and a way to check "is the caller database X?".
- **Delivery guarantees** (outbox, retry, dedupe) built into the runtime.
- **Calling a reducer in another database from a reducer**, with the send committed atomically with the transaction.
- **Cross-database subscriptions**, so a module can react to another database's table changes without a push protocol.
