# ADR-014: Layered sync core with port traits and async edges

- **Status:** Accepted
- **Date:** 2026-08-23

## Context

PulseDB's guiding principle is "Storage, not Intelligence" with a "sync core, async edges" concurrency posture. The public surface (`PulseDB` struct, `SubstrateProvider`) must not leak runtime or storage coupling into consumers, and the storage/vector engines must stay swappable so ADR-001/ADR-005 decisions can be revisited without a public-API rewrite.

## Decision

The crate is a layered library: **public API** (`lib.rs`, `db.rs`) → **core services** (experience, search, relation, insight, activity, watch, substrate, sync) → **storage** (redb) and **vector index** (hnsw_rs), both behind internal port traits (`StorageEngine`; `VectorIndex` for the index). The interior is synchronous; async exists only at the edges — the watch streams, the `#[async_trait] SubstrateProvider`, and the (feature-gated, off-by-default) `sync` transport/manager, which is async end to end and counts as an edge when enabled. **Runtime coupling:** those async edges call `tokio::task::spawn_blocking` / `tokio::spawn` directly (`src/substrate/impl.rs`, `src/sync/manager.rs`), so Tokio is the required edge runtime — awaiting the async surface from executor-agnostic code without a Tokio context can panic. Executor-neutral spawning is a revisit condition, not shipped. Dependency direction points inward: services never depend on a concrete **storage** engine, only on the port. (The vector index is *currently* wired concretely — `PulseDB` holds `HnswIndex` directly rather than `dyn VectorIndex`; the port exists but full dyn wiring is an aspiration, not shipped.).

## Consequences

- Storage-engine swaps are interior changes (the `StorageEngine` port is fully dyn-wired); a vector-engine swap additionally requires reworking the concrete `HnswIndex` wiring in `src/db.rs` (see Decision gap). The public API and `SubstrateProvider` contract hold either way.
- Adding a second consumer runtime (Python bindings, a server) extends an edge rather than piercing the core.
- Any new async in the interior is a boundary violation, not a convenience.

## Revisit trigger

Revisit when a second consumer runtime (bindings or server) forces a seam rethink.

### Verified claims

- `StorageEngine` port and layering exist in the shipped crate (`src/storage`, `src/substrate`).
- Known gap (honest record): vector access is concrete today (`HnswIndex` held directly in `src/db.rs`); the `sync` feature is an async edge; and the async edges require a Tokio runtime context — see Decision.

### Unverified claims

- None.

## Amendment 2026-10-09 — sync protocol v5

### Context

The r1.s1 recovery approved on 2026-09-08 replaced protocol-v4 compatibility and estimated byte floors; its completion contract added transport conformance and quiescent one-shot status.

### Decision

- Use sync protocol **v5** with framing **v4**. Reject v4 peers explicitly; there is no v4 interoperability or fallback, so both replicas must upgrade (`src/sync/mod.rs:167–180`, `src/sync/mod.rs:231`, `src/sync/wire.rs:90–110`).
- Carry embeddings beside the record in wire-only `SyncExperience`; the on-disk `Experience` encoding still omits embeddings. Receiving restores the transmitted vector without implicit re-embedding (`src/sync/types.rs:186–224`).
- Bind push and pull to the intended receiver identity; reject a wrong target before applying or serving changes. Replies identify the responder separately from the acknowledged WAL owner (`src/sync/server.rs:195–228`, `src/sync/server.rs:278–299`).
- Pack a fitting ordered prefix using exact postcard sizes including the complete frame and preamble; count limits are ceilings, not byte estimates. Excluded eligible changes remain owed (`src/sync/wire.rs:112–149`, `src/sync/pusher.rs:127–183`).
- Report noncompletion explicitly: `MissingDependency` for absent update targets, `CatchUpIncomplete` for unfinished initial catch-up, and typed terminal size refusals (`ChangeTooLarge`, `RequestTooLarge`, `PeerRejectedSize`). Returned one-shot operations leave current activity quiescent (`src/sync/error.rs:134–249`, `src/sync/error.rs:261–297`, `src/sync/manager.rs:1170–1176`; `tests/sync_engine.rs:4110–4170`, `tests/sync_http.rs:3974–4064`).

### Consequences

Protocol and disk-schema versions are independent; this wire change does not change the stored Experience format. The hard upgrade deliberately deviates from the sync-convergence gate's “capability negotiation” control (operator-approved 2026-09-08; r1.s1 retrospective C2). Capability advertisements remain informational, with compatibility determined by protocol and framing versions (`src/sync/mod.rs:182–190`). This amendment records the deviation rather than claiming the control is met. See [ADR-006](ADR-006-serializer-replacement.md) for the postcard decision.
