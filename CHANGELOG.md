# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.5.0] - 2026-06-20

> **Sprint 3.5 — Temporal Dynamics.** Three vertical slices: decay core + schema v3 (VS-3.5.1), energy-weighted recall (VS-3.5.2), lifecycle surfacing + 1M bench guard (VS-3.5.3).

### Added

#### Temporal energy & decay (VS-3.5.1)
- `PulseDB::energy(id) -> f32` — temporal-energy diagnostic for an experience, derived-on-read (never stored): `E = clamp(importance · (1 + freq_weight · ln(1 + applications)) · exp(−ln2 · Δt / half_life), 0, 1)`.
- `PulseDB::reinforce_experience(id) -> u32` — reinforcement now increments the local instance's bucket in a per-instance G-counter and returns the new total application count (CRDT-safe across instances).
- `Experience::applications() -> u32` — total application count summed across all instance buckets.
- `DecayConfig` — per-collective decay configuration: `half_life` (default 30 days), `freq_weight` (`k` in `1 + k·ln(1 + applications)`, default 0.25), `floor` (cold threshold, default 0.05), `auto_archive_below_floor` (default `false`), `default_recall_weights` (default `None`). Configured via `Config.decay`.
- `SubstrateProvider::reinforce_experience()` and `SubstrateProvider::energy()` — async substrate surfaces (trait default returns unsupported-operation; `PulseDBSubstrate` delegates to the blocking core; backward compatible).
- Exact G-counter merge for `applications` under sync — bidirectional replication converges via per-instance max, with no lost or doubled increments (assumes a distinct `InstanceId` per replica; see #10).

#### Energy-weighted recall (VS-3.5.2)
- `RecallWeights { similarity, energy }` (with `RecallWeights::new`) — blend weights for energy-aware ranking.
- `SearchOptions.weights: Option<RecallWeights>` — opt-in energy-weighted ranking on `PulseDB::search`. Default `None` preserves the legacy pure-similarity path **byte-for-byte** (as does `{ similarity: 1.0, energy: 0.0 }`).
- `DecayConfig.default_recall_weights: Option<RecallWeights>` — per-collective default blend, resolved per query (invalid stored weights are ignored; see #16).
- `get_context_candidates` honors recall weights for energy-aware context retrieval.
- Ranking blends `similarity·sim + energy·E` over the cosine top-`k′` candidate frontier (over-fetch-then-re-rank): energy reorders admitted candidates but does not itself retrieve high-energy/low-similarity records (known limitation; see #15).

#### Temporal lifecycle surface (VS-3.5.3)
- `PulseDB::list_cold_experiences(collective_id, below, limit)` — read-only, coldest-first surfacing of prune-eligible cold experiences. Returns lightweight `(ExperienceId, energy)` pairs (not full `Experience` records) for experiences whose current temporal energy is `< below` and that are not already archived. A human/agent-triggered review tool: it surfaces candidates a consumer may choose to archive, but never mutates storage. `below` ∈ `[0.0, 1.0]`, `limit` ∈ `1..=1000`; deliberate `O(n)` full-collective scan.
- `SubstrateProvider::list_cold_experiences()` — async substrate mirror of the cold-experience surfacing API, with a trait default (unsupported-operation) and a `PulseDBSubstrate` override that delegates to the blocking core (backward compatible).

#### Performance guard (VS-3.5.3)
- NFR-018 1M P99 search-latency criterion bench guard (`cargo bench search`) — the 1M-experience P99 search latency is measured and recorded against the 50 ms budget. Verdict: **MET @ 9.35 ms** (~5.3x headroom). The guard prints the measured P99 and does not panic on regression (records the verdict for review; no forward CI enforcement yet — see #19).

### Changed
- **BREAKING — schema v3.** The on-disk schema bumps to v3 with an automatic, one-time `v1/v2 → v3` migration on `open()` (a `.pre-v3.bak` sidecar is retained; read-only databases refuse the migration). `Experience` is reshaped: the former scalar reinforcement counter is replaced by a per-instance G-counter `applications: BTreeMap<InstanceId, u32>` (totalled via `Experience::applications()`), and a `last_reinforced: Timestamp` field is added. Code that constructed or pattern-matched `Experience` directly, or read the old scalar counter, must migrate to the new fields.

### Notes
- `auto_archive_below_floor` ships **inert** (default OFF): the flag round-trips through config but wires **no** automatic archive trigger. `list_cold_experiences` only surfaces candidates; no auto-archive actuator exists (rustdoc follow-up: #22).
- Per-collective `DecayConfig` is **local and unsynced** by design — energy is advisory/derived-on-read and may legitimately differ across replicas (DECAY_SPEC D4).

## [0.4.0] - 2026-03-26

### Added

#### PulseVision-ready APIs (Issue #8)
- `Config::read_only()` constructor and `read_only` field — opens database in read-only mode where all mutations return `PulseDBError::ReadOnly`
- `PulseDB::is_read_only()` method
- `PulseDBError::ReadOnly` variant with `is_read_only()` predicate
- `PulseDB::list_experiences(collective_id, limit, offset)` — paginated experience enumeration with embeddings
- `PulseDB::list_relations(collective_id, limit, offset)` — paginated relation listing
- `PulseDB::list_insights(collective_id, limit, offset)` — paginated insight listing
- `SubstrateProvider::list_experiences()`, `list_relations()`, `list_insights()` with default implementations (backward compatible)
- `WatchEvent.experience: Option<Experience>` — enriched events include full experience data for Created/Updated events (embeddings, importance, domain)

### Changed
- `WatchEvent` struct now has an `experience` field (`Option<Experience>`) — set to `Some` for Created/Updated events via in-process watch, `None` for Deleted and WAL-reconstructed events

## [0.3.0] - 2026-03-26

### Added

#### Native Sync Protocol
- `SyncManager` for orchestrating sync between PulseDB instances (start/stop/sync_once/initial_sync)
- `SyncTransport` pluggable trait for transport abstraction
- `HttpSyncTransport` for HTTP/HTTPS sync via reqwest (`sync-http` feature)
- `SyncServer` framework-agnostic server handler for Axum/other consumers (`sync-http` feature)
- `InMemorySyncTransport` for testing
- `SyncConfig` with direction (push/pull/bidirectional), conflict resolution (ServerWins/LastWriteWins), retry with exponential backoff
- `SyncApplyGuard` thread-local echo prevention (prevents infinite sync loops)
- `SyncProgressCallback` trait for initial sync UI feedback
- WAL extension: all entity types (experiences, relations, insights, collectives) now tracked in WAL
- Schema v2 migration (automatic on open)
- `PulseDB::compact_wal()` for WAL compaction using min-cursor strategy
- Per-peer sync cursor persistence in redb
- Stable `InstanceId` per database (UUID v7, persisted in metadata)
- `PulseDBError::Sync` variant (feature-gated)

#### Feature Flags
- `sync` — Core sync protocol, types, engine, in-memory transport
- `sync-http` — HTTP transport (reqwest) + server handler
- `sync-websocket` — WebSocket transport placeholder (tokio-tungstenite)

#### Testing & Benchmarks
- 65+ sync-specific integration tests (foundation, engine, HTTP)
- 6 Criterion benchmarks for sync operations (serialization, echo prevention, WAL poll, compaction)

### Changed
- WAL schema version 1 → 2 (entity_type field added to WatchEventRecord, auto-migration on open)
- `WatchEventRecord.experience_id` renamed to `entity_id` with new `entity_type` discriminant
- `poll_changes()` now filters to Experience-only events (backward compatible)
- WAL sequence now increments for relation, insight, and collective mutations

## [0.2.1] - 2026-03-19

### Fixed
- Race condition in builtin embedding model auto-download when multiple PulseDB instances open concurrently (file lock with double-check pattern)

## [0.2.0] - 2026-03-18

### Added
- `SubstrateProvider::create_collective()` for creating collectives through the async trait
- `SubstrateProvider::get_or_create_collective()` for idempotent collective creation (recommended for SDK consumers)
- `SubstrateProvider::list_collectives()` for listing all collectives
- Auto-download of builtin embedding model when missing (no manual download step needed)

### Breaking
- `SubstrateProvider` trait has 3 new required methods — implementors must add them

## [0.1.1] - 2026-03-15

### Changed
- Improved public documentation for docs.rs readability
- Added docs.rs build configuration for feature-gated items
- Added Feature Flags documentation table to crate-level docs

## [0.1.0] - 2026-03-15

### Added

#### Core
- Database open/close lifecycle with ACID guarantees via redb
- redb storage layer with schema versioning and corruption detection
- Collective CRUD operations for project-level isolation
- Experience CRUD (record, get, update, archive, delete, reinforce)
- Comprehensive input validation for all public APIs
- Built-in ONNX embedding service (all-MiniLM-L6-v2, 384d) with atomic model download (`builtin-embeddings` feature)

#### Search & Retrieval
- HNSW vector index integration for approximate nearest neighbor search (hnsw_rs)
- Similarity search API with cosine distance scoring and domain/type/importance filtering
- Recent experiences API with timestamp-ordered retrieval
- Unified context candidates API aggregating similar, recent, insights, relations, and active agents

#### Knowledge Graph
- Typed experience relations (Supports, Contradicts, Elaborates, Supersedes, Implies, RelatedTo)
- Direction-based relation querying (Outgoing, Incoming, Both)
- Derived insight storage with vector search
- Agent activity tracking with heartbeat and stale detection

#### Real-time & Integration
- In-process watch system for real-time experience notifications via crossbeam channels
- Cross-process change detection via WAL sequence tracking and file lock coordination
- Configurable watch behavior (WatchConfig: in_process toggle, poll interval, buffer size)
- SubstrateProvider async trait and PulseDBSubstrate adapter for agent framework integration

#### Quality
- Error handling audit: comprehensive PulseDBError hierarchy with actionable messages
- All public APIs documented with examples (50 doc tests passing)
- Property-based tests with proptest (7 invariant tests)
- Fuzz testing infrastructure with 3 cargo-fuzz targets
- Test coverage at 89.56% (2033/2270 lines)
- Criterion benchmarks for core operations, mixed workloads, and scaling (1K-100K)
- CI pipeline: 6 jobs (lint, test, MSRV, coverage, security audit, benchmarks)
- CI regression detection with critcmp (10% threshold)

### Performance Targets

| Operation | Target | Measured (1K) |
|-----------|--------|---------------|
| `record_experience` | < 10 ms | 5.5 ms |
| `search_similar` (k=20) | < 50 ms | 95 us |
| `get_context_candidates` | < 100 ms | 189 us |
| `open()` | < 100 ms | < 5 ms |
