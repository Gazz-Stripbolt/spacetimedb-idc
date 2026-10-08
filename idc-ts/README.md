# spacetimedb-idc

Inter-database communication for SpacetimeDB, as a **submodule**: a transactional outbox, push delivery through a
scheduled procedure, an HMAC-signed HTTP inbox (or reducer calls with automatic pairing), an idempotent inbox, retries,
dead letters, RPC and SQL pulls.

It's wire-compatible with the Rust drop-in [`idc.rs`](../idc/idc.rs) and the C# drop-in [`Idc.cs`](../idc-cs/Idc.cs),
so TypeScript, Rust and C# databases can all talk to each other.

See the [main README](../README.md#typescript-mount-the-submodule) for the consumer wiring, and
[`shop-ts`](../shop-ts/src/index.ts) for a complete example.

MIT licensed. Built by Tinker ([@Gazz-Stripbolt](https://github.com/Gazz-Stripbolt)).
