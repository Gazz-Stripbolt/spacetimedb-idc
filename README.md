<div align="center">

# 🔧 Spacetime IDC

**Inter-database communication for SpacetimeDB, today.**

Databases that push messages to each other: delivered in about 10 ms per hop, retried until delivered, applied
exactly once, with **no polling**. Built from parts SpacetimeDB already ships: procedures, schedule tables and
HTTP handlers. Available for **Rust**, **C#** and **TypeScript (as a submodule)**, all speaking one protocol.

[![CI](https://github.com/Gazz-Stripbolt/spacetimedb-idc/actions/workflows/ci.yml/badge.svg)](https://github.com/Gazz-Stripbolt/spacetimedb-idc/actions/workflows/ci.yml)
![SpacetimeDB 2.11](https://img.shields.io/badge/SpacetimeDB-2.11-e8730c)
![TypeScript submodule](https://img.shields.io/badge/TypeScript-submodule-3178c6)
![Rust](https://img.shields.io/badge/Rust-drop--in-b7410e)
![C#](https://img.shields.io/badge/C%23-drop--in-512bd4)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/idc-dark.png">
  <img alt="Dashboard: the shop's orders and stock mirror next to the warehouse's stock and reservations, with a live timeline of messages between them" src="docs/idc-light.png" width="860">
</picture>

</div>

---

> [!NOTE]
> **A stopgap until native IDC ships.** SpacetimeDB has native inter-database communication on the way: async IDC is
> planned first (the work is already landing upstream), and sync IDC later. When it arrives, prefer it. This repo
> stays useful as a reference for outbox, retry and idempotency patterns.

## Where to go

| I want… | Go to |
|---|---|
| **Rust**: add IDC to a Rust module | [`rust/`](rust): `idc.rs`, one drop-in file |
| **C#**: add IDC to a C# module | [`csharp/`](csharp): `Idc.cs`, one drop-in file |
| **TypeScript**: add IDC to a TS module, as a **submodule** | [`typescript/`](typescript): the `spacetimedb-idc` submodule |
| **See it working**: two databases talking | [`demo/`](demo): shop ⇄ warehouse in Rust (plus TS and C# variants) |
| **Move a character between shards** (or anything that must never be duplicated or lost) | [`docs/HANDOFF.md`](docs/HANDOFF.md): handoffs, all three languages |
| **The wire format**, to port it or debug it | [`docs/PROTOCOL.md`](docs/PROTOCOL.md) |
| **Everything we learned**: limits, gotchas, numbers | [`docs/FINDINGS.md`](docs/FINDINGS.md) |

Every language talks to every other. CI runs the full end-to-end suite on each combination:

| shop ↓ · warehouse → | Rust | C# |
|---|---|---|
| **TypeScript** (submodule) | ✅ | ✅ |
| **Rust** | ✅ | ✅ |
| **C#** | ✅ | ✅ |

## The problem

SpacetimeDB databases can't talk to each other natively yet. Native inter-database communication is coming, but until it
lands the usual workaround is to call the other database's HTTP API (`/call`, `/sql`) from a procedure. That has three sore spots:

1. **You hand-maintain a secrets table** with a token for the other database.
2. **You hand-maintain a "known identities" table** on the other side to check who's calling. If you want traffic both
   ways, *both* databases need *both* tables.
3. **Nothing is event-driven.** If database A changes something B cares about, B finds out by polling on a schedule.

Spacetime IDC fixes all three.

## How it works: transactional outbox → scheduled procedure → peer's HTTP route

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant S as shop (database)
    participant W as warehouse (database)
    C->>S: place_order("gear", 2)  (reducer)
    Note over S: same transaction:<br/>insert order (pending)<br/>insert outbox row "reserve"<br/>schedule flush for *now*
    S-->>S: idc_flush (procedure) wakes immediately
    S->>W: POST /route/idc/inbox  (HMAC-signed batch)
    Note over W: same transaction:<br/>dedupe by message id<br/>reserve stock<br/>queue "reservation" + "stock" replies
    W-->>S: 200 {results:[ok]}
    W-->>W: idc_flush wakes immediately
    W->>S: POST /route/idc/inbox  (reservation, stock)
    Note over S: order → confirmed<br/>stock mirror updated
    S-->>C: subscription update (≈20 ms after step 1)
```

- **Reducers can't make HTTP calls, but procedures can.** So `send()` writes the message to an **outbox table in the same
  transaction** as your change, and inserts a one-shot schedule row for *now*. The scheduled **procedure** runs right
  after the commit and POSTs the message to the peer.
- **The peer reacts the moment the request lands.** Its handler applies the message in a transaction, and that
  transaction can queue replies of its own. That's event-driven in both directions with zero polling. The only timers
  are retry backoffs, and they only exist while something is failing.
- **Reliable delivery:** no message without its commit and no commit without its message. Retries back off from 500 ms
  to 60 s, each peer's messages stay in order, and anything the peer permanently refuses becomes a dead letter.
- **Idempotent apply:** every message has a globally unique id, recorded in the *same* transaction as its effect. So
  at-least-once delivery has an exactly-once effect.
- **Durable before visible:** SpacetimeDB acknowledges a commit before it's on disk, so a crash can undo a transaction
  whose message already went out. idc sends only once its own commits are durable, and drops a message only once the
  peer's commit is durable (both via `/sql?confirmed=true`). A `kill -9` crash test checks this in CI.

## Two transports, plus RPC and SQL

| | **Route** (default) | **Reducer** |
|---|---|---|
| How | POST to the peer's HTTP handler `/route/idc/inbox` | POST `/call/idc_receive` as an identity the peer knows |
| Auth | HMAC-SHA256 over `timestamp.body` with a shared secret; the secret never travels | SpacetimeDB identity token + known-identity table |
| Setup | One env var (`IDC_SECRET`) on each side | **Pairing**, fully automatic (below) |
| Batching | Up to 64 messages per request | One message per call |
| Throughput* | ~245 orders/s Rust (~270–300 before the durability waits) | ~18 orders/s (was ~43–49) |
| Round trip* | ~35–40 ms (was ~15–30) | ~55 ms (was ~30) |

<sub>*Local standalone 2.11.0 on a 2-vCPU VM. Each order is three cross-database messages. The durability waits add two confirmed `/sql` calls per hop; the older numbers are from before them and still hold for C# and TS relative to Rust. C# numbers are with NativeAOT-LLVM; the Mono JIT build is 2–3× slower (see [csharp/](csharp#performance-use-nativeaot-llvm)). Run `scripts/bench.sh` yourself.</sub>

**Pairing** (reducer transport) automates the token and known-identity chore in both directions. Each database mints
its own identity on the peer's host (`POST /v1/identity`), keeps the token privately, and introduces the identity to
the peer with a signed `POST /route/idc/pair`. The peer then trusts it in `idc_receive`. This runs right after publish,
retries until the peer is up, pairs back the moment a peer introduces itself, and re-runs by itself if a peer
forgets us.

**RPC:** a signed synchronous call from a procedure to a peer's route, e.g. `quote(sku)` returns the warehouse's answer
in ~15 ms. **SQL pulls:** `/sql` from a procedure, the pre-HTTP-handler way.

## Handoffs: moving ownership between databases

Some things have to live in exactly one database at a time: a character walking from one world shard to the next, an
item going to an auction house, a tenant changing region. "Copy to B, then delete on A" can't be made safe, even with
synchronous calls: crash between the two steps and you get a duplicate or a loss, and there's no transaction that
spans two databases.

**Handoffs** ([`rust/handoff.rs`](rust/handoff.rs), [`csharp/Handoff.cs`](csharp/Handoff.cs),
[`spacetimedb-idc/handoff`](typescript/src/handoff.ts)) are an ownership state machine on top of idc:

```
source:  start() locks it ── offer ──▶ target: validate, import as pending
source:  deletes its copy ◀── accept ──
                          ── release ─▶ target: activate (now live)
```

Every step is one local transaction plus a durable, idempotent message, so a crash anywhere just resumes. Rejections,
cancels and timeouts unlock the entity on the source. **At most one live copy, never zero.** A crossing takes ~40 ms (Rust).
The chaos suite (partitions between every step, races, replays, forged messages) passes on every language pairing,
and a `kill -9` crash test runs in CI.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/handoff-dark.png">
  <img alt="Two-shard demo: one character locked at the border while its pending copy waits on the other shard" src="docs/handoff-light.png" width="860">
</picture>

Details, API for all three languages, and the failure table: [`docs/HANDOFF.md`](docs/HANDOFF.md).

## Configuration (all languages)

Four environment variables, controlled by the database owner:

| Variable | Example |
|---|---|
| `IDC_SELF` | `shop` |
| `IDC_PEERS` | `warehouse=https://maincloud.spacetimedb.com/v1/database/my-warehouse` (comma-separated) |
| `IDC_SECRET` | `openssl rand -hex 32`, the same value on every peer |
| `IDC_TRANSPORT` | `route` or `reducer` |

Adding IDC to a database that's already live is a normal publish: the new tables are added automatically and existing
data isn't touched. Then run `spacetime call <db> idc_kick` once, because `init` doesn't re-run on updates.

## Good to know

- **Local development needs a dev server.** Standalone blocks module HTTP to loopback and private addresses, so two
  databases on one local server can't reach each other. [`scripts/dev-server.sh`](scripts/dev-server.sh) builds
  standalone with SpacetimeDB's own `allow_loopback_http_for_tests` feature. Maincloud uses public URLs and doesn't need it.
- **Procedures and HTTP handlers are beta** in SpacetimeDB. APIs may move between releases.
- **Tokens from `/v1/identity` don't expire.** Treat the peer-token table as a secret.
- **The secret is shared across the mesh.** Per-peer secrets would be an easy extension.
- **Maincloud:** not tested from this repo. It's the same mechanism (module → public HTTPS → module) apps already use
  to reach Maincloud's HTTP API from procedures.

## Repository layout

```
rust/idc.rs, rust/handoff.rs         Rust drop-ins
csharp/Idc.cs, csharp/Handoff.cs     C# drop-ins
typescript/                          TypeScript submodule (spacetimedb-idc, spacetimedb-idc/handoff)
demo/rust/{shop,warehouse}           the reference demo
demo/typescript/shop                 the shop on the TS submodule
demo/csharp/{shop,warehouse}         the demo in C#
demo/{rust,csharp,typescript}/shard  the handoff demo: two world shards
demo/dashboard/                      live dashboards served by the Rust modules
scripts/                             dev-server.sh · deploy.sh · e2e.sh · bench.sh
                                     deploy-shards.sh · handoff-e2e.sh · handoff-crash.sh
docs/                                PROTOCOL.md · FINDINGS.md · HANDOFF.md · screenshots
```

## Credits

Built by **Tinker** ([@Gazz-Stripbolt](https://github.com/Gazz-Stripbolt)), the resident gadgeteer for the
[Pogly](https://pogly.gg) team: collaborative stream overlays, powered by SpacetimeDB. Pogly's own cross-database
setup, and the wish to make it event-driven, inspired this repo.

More SpacetimeDB building blocks from this workshop: **[github.com/Gazz-Stripbolt](https://github.com/Gazz-Stripbolt)**.

🚀 **New to SpacetimeDB?** If you sign up through **[this referral link](https://spacetimedb.com/?referral=Lethalchip)**,
Pogly gets free recurring energy. Thank you!

## License

[MIT](LICENSE)
