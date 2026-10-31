# Changelog

All notable changes to `@pogly/spacetimedb-idc` are listed here. It follows [semantic versioning](https://semver.org/).

## 0.1.2 (2026-10-31)

- Works with SpacetimeDB 2.14: the `spacetimedb` peer range is now `2.13.* || 2.14.*`. A module built with the 2.14 library needs a 2.14 or newer host.
- A batch that keeps failing now shrinks. The sender halves it for each failed attempt at the head of a peer's queue, so a slow disk can no longer hold a full batch past the HTTP timeout forever. The wire protocol is unchanged.
- A queued message now always has a later flush committed with it, so a crash during a flush can no longer leave messages waiting for the next `send`.

## 0.1.1 (2026-10-20)

- First release published from CI, with npm provenance. No code changes.

## 0.1.0 (2026-10-20)

- First npm release, as `@pogly/spacetimedb-idc` (previously the unpublished workspace package `spacetimedb-idc`).
  Exports `.` (outbox, delivery, inbox) and `./handoff`. Works with SpacetimeDB 2.11 and 2.12.
- `package.json` declares `submodule.category` for the [SpacetimeDB submodule registry](https://spacetimedb.com/submodules).
