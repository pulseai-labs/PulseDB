# PulseDB-ai — Roadmap

> Derived from MASTER-SPEC.md by `/plan-roadmap` on 2026-05-30.
> Co-edited by user + scaffold-dev orchestrator over time.

## Roadmap overview

_Expand into a 3-paragraph project-shape summary using the 3-timelines framing: visionary horizon (Phases) → value-building windows (Sprints) → visibility cycles (Vertical Slices)._

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

