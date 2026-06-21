# PulseDB-ai — Roadmap

> Derived from MASTER-SPEC.md by `/plan-roadmap` on 2026-05-30.
> Co-edited by user + scaffold-dev orchestrator over time.

## Roadmap overview

_Expand into a 3-paragraph project-shape summary using the 3-timelines framing: visionary horizon (Phases) → value-building windows (Sprints) → visibility cycles (Vertical Slices)._

## Phase 3.5: Temporal Dynamics — MVP completion — v0.5.0 (mid-2026)

MVP-completion half-step inserted ahead of Phase 4 hardening. Give every experience a decaying, reinforcement-driven *energy* (recency × frequency, ACT-R-shaped) and blend it into recall (score = α·sim + β·E) so the substrate surfaces what is both relevant and warm. Energy is computed lazily at read time (closed-form exp(), never stored, no daemon), preserving the "sync core, async edges" and "Storage, not Intelligence" principles — PulseDB computes heat; consumers decide what counts as usage. These two capabilities are MVP-blocking for the Claude-Code-plugin consumer (identified 2026-06-02, decisions locked 2026-06-10) and are sequenced AHEAD of Phase 4 production hardening. Full D1–D7 fork-decision rationale: DECAY_SPEC.md.

### Sprint 3.5: Temporal Dynamics

Ship temporal dynamics end-to-end: schema-v3 last_reinforced + closed-form energy + DecayConfig (VS-3.5.1), energy-weighted recall via HNSW over-fetch + in-substrate re-rank (VS-3.5.2), and conservative lifecycle + the <50ms bench guard (VS-3.5.3). Demoable: reinforce→decay matches the closed form; weighted recall ranks fresh-over-stale while RecallWeights{1,0} reproduces legacy order bit-for-bit; search stays within the <50ms P99 @1M budget.

#### VS-3.5.1: Decay core + schema v3

Add last_reinforced (Timestamp) to Experience and convert applications to a per-instance G-counter ({InstanceId→u32}, sum accessor) via schema-v3 auto-migration (legacy counts → {LEGACY} sentinel bucket to avoid sync double-count; backup-before-migrate). Includes sync-applier merge rules (per-key max ⇒ exact total), the read-only closed-form energy(id) diagnostic, and per-collective DecayConfig. reinforce_experience() increments the local-instance key and resets last_reinforced = now atomically.

##### Traceability

- FR: FR-030, FR-031, FR-033, FR-035
- NFR: None
- Backlog: E7-S01

##### Demo criteria

- [ ] auto: cargo test --lib --features sync -- experience::decay sync::applier → expected: exit code 0
- [ ] user: reinforce the same experience on two instances while diverged, then sync → expected: exact total (no lost or doubled counts); energy rises on reinforce then decays per the closed-form E(t)

#### VS-3.5.2: Energy-weighted recall

Optional RecallWeights{similarity, energy} on search options; HNSW over-fetch (k′ = max(4k, k+16)) then in-substrate re-rank by score = α·clamp₀₁(sim) + β·E, applied to search_similar and the similarity component of get_context_candidates. Absent weights ⇒ legacy pure-similarity ranking (backward compatible).

##### Traceability

- FR: FR-032
- NFR: NFR-018, NFR-019
- Backlog: E7-S02

##### Demo criteria

- [ ] auto: cargo test --lib search:: → expected: exit code 0
- [ ] user: query a stale-but-similar vs a fresh-reinforced experience → expected: weighted recall ranks the fresh one first; RecallWeights{1.0, 0.0} reproduces legacy order bit-for-bit

#### VS-3.5.3: Lifecycle + bench guard

Conservative lifecycle: floor config + list_cold_experiences(below) helper surfacing prune-eligible (E < floor) candidates; auto_archive_below_floor defaults OFF until dogfood data exists. Criterion bench guard proving energy-weighted search (over-fetch + k′ exp() calls) stays within budget.

##### Traceability

- FR: FR-034
- NFR: NFR-018
- Backlog: E7-S03

##### Demo criteria

- [ ] auto: cargo bench search → expected: <50ms P99 @1M with energy re-rank, criterion regression <10%
- [ ] user: a cold experience with E < floor → expected: appears in list_cold_experiences; auto-archive stays OFF by default

## Phase 4: Production Reach — ~12 months (2026–2027)

Harden PulseDB to production-grade reliability and broaden its reach. By the end of Phase 4 the core has a modernized on-disk substrate behind a tested upgrade path, fault-injection-tested error and recovery paths at high coverage, a real-time WebSocket sync transport alongside HTTP, and Python bindings so non-Rust consumers can adopt the substrate.

### Sprint 4.0: Storage-Format Modernization

Adopt the redb `2.x→4.x` file-format major and **replace the unmaintained `bincode` serializer with a maintained one** (e.g. `postcard` / `bitcode` / `rkyv`), both behind a tested upgrade-on-open path, so databases created by prior releases survive the upgrade. Replacing bincode is what actually clears the `RUSTSEC-2025-0141` advisory: the whole crate is unmaintained (all versions; `3.0.0` is a non-functional tombstone), so a 1.x→2.x/3.x bump would not clear it. Sequenced at the head of Phase 4 (substrate-first) because every later sprint sits on this on-disk format and Python bindings (Sprint 4.2) would add consumers of it. Demoable: a v0.5.1-created database opens and reads back identically under the new redb file format + replacement serializer. Tracking issues #40 (redb) + #30 (serializer replacement); detailed compat specs live in the paired AI workspace (`PulseDB-ai/docs/specs/`).

#### VS-4.0.1: On-disk-format compatibility analysis & migration design

Determine redb `2.x→4.x` file-format compatibility (auto-upgrade vs hard-refuse) and **select the replacement serializer** (`postcard` / `bitcode` / `rkyv` — evaluate wire-format stability, performance, and `serde` compatibility), then produce the upgrade-on-open migration design (detect prior format → read via bincode 1.3 → re-write via redb 4.x + the new serializer → backup sidecar) that slices 4.0.2–4.0.4 implement against.

##### Traceability

- FR: FR-001
- NFR: NFR-020
- Backlog: None

##### Demo criteria

- [ ] auto: the manifest pins redb 4.x and `cargo tree -i redb | grep -q 'redb v4'` (assert the resolved major, not just that redb is present); the selected serializer resolves with our feature set → expected: exit code 0
- [ ] user: review the compatibility analysis (redb 2→4 auto-upgrade-vs-refuse) + the serializer selection (postcard/bitcode/rkyv tradeoffs) + the read-old/re-write-new upgrade-on-open + backup-sidecar design → expected: a complete migration plan with a backup/rollback path

#### VS-4.0.2: redb 2→4 migration implementation

Adopt redb 4.x: migrate the breaking API surface and implement upgrade-on-open for the 2.x file format (read-or-migrate behind a backup sidecar, reusing the existing backup-before-migrate machinery), so existing databases open under redb 4.x.

##### Traceability

- FR: FR-001
- NFR: NFR-020
- Backlog: None

##### Demo criteria

- [ ] auto: cargo test --lib storage:: → expected: exit code 0
- [ ] user: open a v0.5.1 (redb-2.x) database under redb 4.x → expected: opens (read unchanged or migrated) with a backup sidecar, no data loss

#### VS-4.0.3: Replace bincode with a maintained serializer

Replace the unmaintained `bincode` (1.3.3) with the serializer selected in VS-4.0.1 at all ~14 serialization call sites **and the `sync-http` wire format** (the sync server deserializes the handshake body before it can check `SYNC_PROTOCOL_VERSION`, so cross-version sync wire compatibility is in scope). Migrate existing data on open via a **legacy read path that carries no maintained-crate dependency** (a vendored/minimal bincode-1.3 decoder, or a sequenced removal — decided in VS-4.0.1), then re-write via the new serializer. Dropping the `bincode` *crate* dependency is what lets the `RUSTSEC-2025-0141` ignore be removed from `deny.toml`.

##### Traceability

- FR: None
- NFR: NFR-020
- Backlog: None

##### Demo criteria

- [ ] auto: cargo deny check --all-features → expected: exit code 0 with the `RUSTSEC-2025-0141` ignore removed (no `bincode` *crate* dependency remains — the legacy decode path is vendored, not a crate dep)
- [ ] user: a prior-release fixture (bincode 1.x bytes) migrates and reads back value-identical under the new serializer → expected: identical reads after on-open re-serialization

#### VS-4.0.4: Golden-fixture / real-upgrade test harness

Check in a **full v0.5.1-created redb-2.x database file** (a real prior-release on-disk file — not just serialized value bytes, so redb's file-format upgrade is actually exercised), open it under the new redb file format + replacement serializer in CI, and assert every entity reads back identically — closing the fresh-DB-only CI gap (MIGRATE-020).

##### Traceability

- FR: FR-001
- NFR: NFR-020
- Backlog: None

##### Demo criteria

- [ ] auto: cargo test --test storage_format_upgrade → expected: exit code 0 (prior-release fixture opens + reads identically)
- [ ] user: confirm the upgrade test is wired as a required CI gate → expected: CI fails if a prior-release fixture fails to open/migrate

### Sprint 4.1: Production Hardening

Close the highest-risk coverage gaps with fault-injection tests for error/recovery, vector-index rebuild, and watch poll/timeout paths. Demoable: green fault-injection suites and raised coverage on error.rs, vector/hnsw.rs, and watch.

#### VS-4.1.1: Fault-injection coverage for error & recovery paths

Fault-injection tests that trigger every PulseDBError variant and exercise crash/recovery paths in error.rs, raising its coverage to production-grade.

##### Traceability

- FR: None
- NFR: None
- Backlog: None

##### Demo criteria

- [ ] auto: cargo test --lib error:: → expected: exit code 0
- [ ] user: review fault-injection tests trigger each PulseDBError variant (Storage / Concurrency / Embedding / ResourceLimit) → expected: every variant has a covering test

#### VS-4.1.2: Vector index rebuild & error-path coverage

Coverage for HNSW vector-index rebuild and error paths: recovery after corruption, dimension-mismatch handling, and concurrent rebuild.

##### Traceability

- FR: None
- NFR: None
- Backlog: None

##### Demo criteria

- [ ] auto: cargo test --lib vector:: → expected: exit code 0
- [ ] user: rebuild the HNSW index after simulated corruption → expected: search recall unchanged post-rebuild

#### VS-4.1.3: Watch poll/timeout path coverage

Coverage for watch poll/timeout paths: stale-agent thresholds, poll timeouts, and no missed or duplicate events under load.

##### Traceability

- FR: None
- NFR: None
- Backlog: None

##### Demo criteria

- [ ] auto: cargo test --lib watch:: → expected: exit code 0
- [ ] user: watch poll honors timeout and stale-agent thresholds → expected: no missed or duplicate events under timeout

### Sprint 4.2: Transport & Bindings Reach

Implement the WebSocket SyncTransport (replacing the sync-websocket placeholder) and Python (PyO3) bindings. Demoable: a working WebSocket sync round-trip and a Python package exercising core PulseDB ops.

## Phase 5: Collective Intelligence — Year 2

Ship the differentiating intelligence layer. By the end of Phase 5, PulseDB abstracts experiences across collectives (Wisdom / cross-collective transfer), exposes an entity-relationship graph over experiences, and supports KV-cache (REFRAG) storage for context reuse.

### Sprint 5.1: Wisdom & Cross-Collective Transfer

Implement Wisdom: cross-collective knowledge transfer that abstracts repeated experiences across collectives. (Slice decomposition deferred — add via /plan-roadmap --add-slice 5.1.)

### Sprint 5.2: Graph & KV-Cache

Add the entity-relationship graph over experiences and KV-cache (REFRAG) storage. (Slice decomposition deferred.)

## Phase 6: Scale & Performance — Year 3+

Scale the substrate. By the end of Phase 6, PulseDB has SIMD-accelerated HNSW search, richer multi-process replication, and point-in-time recovery, validated against large-scale benchmarks (1M+ experiences per collective).

### Sprint 6.1: SIMD & Performance

SIMD-accelerated HNSW behind a feature flag, plus large-scale performance benchmarks. (Slice decomposition deferred.)

### Sprint 6.2: Replication & Recovery

Multi-process replication and point-in-time recovery. (Slice decomposition deferred.)

