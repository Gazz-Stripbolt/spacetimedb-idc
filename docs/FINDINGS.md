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
10. **Don't install the latest `wasm-opt`.** The CLI suggests installing binaryen's `wasm-opt`, but with binaryen
    **version_133** every publish (Rust and C#) fails with *"Local module schema inspection failed"*. version_123 works.
11. **`spacetime publish --env-only` refuses `--module-path`.** Run it from a directory without a `spacetime.json`, with
    only the changed variables set in the shell environment.
12. **The handler RNG is timestamp-seeded.** Nothing here relies on randomness for security: authentication is the HMAC
    secret or a server-issued token.
13. **A commit is acknowledged before it's durable, and procedures can leak it.** Standalone hands each commit to a
    background durability actor that writes and fsyncs in batches. The commit is visible to the next transaction right
    away. Clients are protected (subscriptions and `/sql` default to *confirmed reads* and wait for disk), but a
    procedure that reads a fresh outbox row and POSTs it to a peer is not. Crash before the fsync and the transaction
    is gone, yet the peer already acted on it. The reverse also happens: the peer answers 200, crashes, and forgets.
    Our `kill -9` crash test hit both in CI: duplicated characters, and (with fsync slowed down via `strace`) lost
    ones. The fix: before sending, the flush procedure calls its own `/sql?confirmed=true` (a read of `st_table`,
    which works in any language without auth), and it only drops a delivered message after the same call against the
    peer succeeds. On an in-memory server the wait is a no-op HTTP hop. Worth an upstream API: "wait until my commits are
    durable" from a procedure, or confirmed-only reads inside `with_tx`. `IDC_DURABILITY=unsafe` turns the waits off
    for meshes that can tolerate lost or phantom messages.

## Packaging it as a TypeScript submodule

Submodules (2.11, TypeScript only for now) are a good fit, but they come with rules that shaped the API:

| Rule | Consequence for idc |
|---|---|
| Submodules can't declare env vars, and their host-dispatched entry points can't read the root's | Config lives in a private `idc.config` table. The consumer copies it from its own env in `init` and `idc_kick` (`idc.configure(ctx.as.idc, …)`). Run `idc_kick` after `publish --env-only`. |
| No lifecycle reducers in submodules | `configure()` is a helper the consumer's `init` calls. |
| Submodule routers are ignored | The consumer registers `/idc/inbox` and `/idc/pair` on its own router, delegating to `idc.inbox` / `idc.pairRoute`. |
| A submodule can't call into its consumer | `idc.inbox` / `idc.receive` take the **root** context plus `{ scope: tx => tx.as.idc, onMessage }`, so the dedupe record (submodule table) and your effect (consumer tables) commit in one transaction. |
| Scheduled procedures in a submodule *are* dispatched | `idc.flush` and `idc.pair` run on their own, with no consumer wiring. |

TypeScript-specific gotchas:

- **`ctx.http.fetch` throws `invalid status code: 530`.** The SDK maps status codes through a list of standard ones, and
  530 (how `/call` reports a reducer error) isn't on it, so the response body is lost. The TS library re-pairs once on a
  530, then dead-letters a repeat. Worth an upstream fix.
- **One copy of the `spacetimedb` package.** If the submodule package carries its own `node_modules/spacetimedb`, the CLI
  fails with an unhelpful *"Local module schema inspection failed"*. Use npm workspaces, as this repo does.
- **u64 columns are `bigint`.** Convert them before `JSON.stringify`. Timestamps are `microsSinceUnixEpoch: bigint`.
- **No WebCrypto in the module runtime,** so HMAC comes from `@noble/hashes` (pure JS, bundled by `spacetime build`).

Interop: every shop language (TS, Rust, C#) passes the full idc suite (now 32 checks) against every warehouse language (Rust, C#),
on both transports. That's six pairings, all in CI.

## The C# port

`csharp/Idc.cs` is a drop-in partial `Module` class (C# submodules aren't supported yet). What it took:

- **No `System.Security.Cryptography` on wasi**, so the file carries a small managed SHA-256 / HMAC.
- **System.Text.Json reflection is trimmed away.** `JsonNode` / `JsonObject` with primitive values work, but
  `JsonArray.Add<T>(string)` (including the collection-initializer form) throws
  `NoMetadataForType … EmptyJsonTypeInfoResolver`. Add `JsonValue.Create(...)` explicitly.
- **`HttpMethod` is ambiguous** between `SpacetimeDB.HttpMethod` and `System.Net.Http.HttpMethod` under implicit usings,
  so alias it.
- **The message handler is a required partial method** (`public static partial void OnIdcMessage(IdcTx, IdcMessage)`),
  so forgetting it is a compile error, not silently dropped messages.
- `IdcTx(Local Db, Timestamp)` bridges reducer, procedure-transaction and handler-transaction contexts, which all expose
  the same generated `Local`.
- **Performance: use NativeAOT-LLVM.** The default Mono JIT build (`.NET 8` + `wasi-experimental`) interprets IL inside
  wasm: ~60–90 ms round trips, ~154 orders/s C# ⇄ C# on the route transport, ~21/s on the reducer transport.
  NativeAOT-LLVM compiles to native wasm: **~24–32 ms, ~270 orders/s and ~43/s**, on par with Rust. The CLI uses it
  automatically for `net10.0` projects. For .NET 8 it's `--native-aot`, but only on Windows (`nativeaot_unsupported_on_host`
  is macOS, or Linux + .NET 8).
- **AOT trimming warnings:** the generic `JsonArray.Add<T>` is flagged (IL2026/IL3050), so use `Add((JsonNode)x)`.

## Handoffs (moving ownership between databases)

- **Sync IDC doesn't make transfers safe; an ownership state machine does.** HTTP from a procedure runs outside any
  transaction, so "copy to B, then delete on A" always has a crash window. Locking on the source, importing as pending
  on the target, and releasing only after the target confirms closes it, provided every message is durable and
  idempotent. Details: [HANDOFF.md](HANDOFF.md).
- **Per-peer ordering makes the cancel race deterministic.** A cancel is queued behind its offer, so the target has
  always seen the offer first, and only holds a pending copy it can drop. Without ordering, a tombstone for "cancel
  before offer" covers the gap.
- **Idempotency has to be per transfer, not just per message.** idc dedupes message ids, but a replay with a fresh id
  (or a resend after a crash) must also be a no-op, so every handler checks the transfer's status first.
- **Protocol handlers must never fail.** A failed receive dead-letters the message on the sender, and a dead `release`
  would strand an entity with zero live copies. Refusals are messages, and hook code that can fail runs in `validate`,
  before anything is written.
- **The state machine is only as safe as the commits under it.** The first version passed every chaos test and 8 local
  crash rounds, then duplicated characters in CI's crash test: the CI disk was slow enough that `kill -9` rolled back
  commits whose messages had already been delivered (gotcha 13). With idc waiting for durability on both ends, the crash
  test holds on a normal disk and with every fsync delayed by 200 ms.
- **Crash recovery is bounded by the outbox lease.** After `kill -9`, messages that were mid-delivery stay leased for
  60 s, so transfers with a shorter timeout get cancelled rather than completed. Still exactly one live copy, just
  slower.
- **Checksum the exact text, not re-serialized JSON.** C#'s `JsonNode.ToJsonString()` escapes non-ASCII by default and
  key order differs between serializers, so `data` travels as a string and the checksum covers those bytes.
- **TS submodules: scheduled reducers must be exported from the submodule's entry module.** Splitting the handoff code
  into a second file that registered its own schedule table created an import cycle that crashed at startup depending
  on import order. So the tables and the timeout reducer live in `index.ts`, and `handoff.ts` imports one way only.
- **`ctx.identity()` / `ctx.Identity` are deprecated in 2.11.** Use `database_identity()` / `DatabaseIdentity` for the
  "scheduler only" check on scheduled reducers.

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

Measured before the durability waits (gotcha 13). With them, Rust ⇄ Rust is ~35–40 ms per round trip and ~245 orders/s
on the route transport, and ~55 ms / ~18 orders/s on the reducer transport (one message per call, so every message
pays both waits).

| Scenario | Result |
|---|---|
| Single order, route transport | 15–30 ms round trip (Rust shop), 26–30 ms (TS shop) |
| Single order, reducer transport | ~29–33 ms round trip |
| 20 concurrent orders, route (batched) | all settled in 26–33 ms |
| 20 concurrent orders, route, *before batching* | 55 → 304 ms (sequential) |
| 1000 orders at ~300/s, route | every order settled within ~30 ms of the last one placed |
| 500 orders, route, TS shop | 233 orders/s; all settled ~180 ms after the last one placed |
| 500 orders, route, C# ⇄ C#, NativeAOT-LLVM | 269 orders/s (Mono JIT: 154) |
| 300 orders, reducer, C# ⇄ C#, NativeAOT-LLVM | 43 orders/s (Mono JIT: 21) |
| 300 orders at ~280/s, reducer | 47 orders/s sustained (one `/call` per message) |
| Synchronous RPC (`quote`) | ~15 ms |

## What native IDC could make easier

These are the bits the workaround has to build by hand:

- A trusted **database identity** for callers (no token minting or pairing) and a way to check "is the caller database X?".
- **Delivery guarantees** (outbox, retry, dedupe) built into the runtime.
- **Calling a reducer in another database from a reducer**, with the send committed atomically with the transaction.
- **Cross-database subscriptions**, so a module can react to another database's table changes without a push protocol.
