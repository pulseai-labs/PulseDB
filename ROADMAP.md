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

Harden PulseDB to production-grade reliability and broaden its reach. By the end of Phase 4 the core has fault-injection-tested error and recovery paths at high coverage, a real-time WebSocket sync transport alongside HTTP, and Python bindings so non-Rust consumers can adopt the substrate.

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

