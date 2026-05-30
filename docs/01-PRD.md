# PulseDB: Product Requirements Document

> **Version:** 1.0.0  
> **Status:** Approved  
> **Last Updated:** February 2026  
> **Owner:** PulseDB Team

---

## 1. Executive Summary

PulseDB is an embedded database purpose-built for agentic AI systems that require shared memory, collective learning, and efficient context retrieval. Unlike traditional databases that store data for retrieval, PulseDB enables a **shared consciousness model** where multiple AI agents can instantly perceive what other agents have learned—without message passing, without polling, without coordination overhead.

**Elevator Pitch**: *The database that gives your agents shared consciousness. Not message passing. Not RAG. Actual collective memory.*

**Target Release**: Q2 2026 (MVP)

---

## 2. Problem Statement

### 2.1 The Coordination Problem in Multi-Agent Systems

Modern AI development increasingly relies on multi-agent architectures where specialized agents collaborate on complex tasks. However, current approaches suffer from fundamental coordination problems:

**Message Passing Overhead**
```
Agent A: "I found a bug"
Agent B: "What bug?"
Agent A: "In the auth module"
Agent B: "Which file?"
Agent A: "login.tsx, line 42"
...
```

Each exchange adds latency, loses context, and creates coordination overhead that scales O(n²) with agent count.

**RAG Limitations**
Traditional Retrieval-Augmented Generation treats agent knowledge as static documents:
- No semantic understanding of what experiences mean
- No real-time awareness of what other agents are doing
- No cross-document reasoning
- No learning accumulation across sessions

**Existing Database Limitations**

| Solution | Limitation |
|----------|------------|
| PostgreSQL + pgvector | Requires server, no native experience semantics |
| SQLite + sqlite-vss | No graph, manual context assembly, extension complexity |
| Qdrant/Pinecone | Vector-only, requires server, no experience model |
| Neo4j | Graph-only, no vector, heavyweight server |
| LanceDB | Vector-focused, no graph, no experience operations |
| ChromaDB | Vector-only, Python-native, no embedded Rust |

### 2.2 The Opportunity

There is no embedded database that provides:
1. **Experience-native storage** (not just documents/vectors)
2. **Integrated vector + graph** capabilities
3. **Real-time awareness** of agent activities
4. **Context assembly** as a first-class primitive
5. **Scoped isolation** with safe cross-project knowledge transfer

PulseDB fills this gap as a purpose-built substrate for hive mind architectures.

---

## 3. Target Users

### 3.1 Primary Users

#### PulseHive Integration
- **Description**: PulseHive is an AI-powered development platform using shared consciousness for agent coordination
- **Integration**: PulseDB as the `SubstrateProvider` implementation
- **Requirements**: Maximum performance, full API surface, Rust-native

#### Custom Agent System Builders
- **Description**: Developers building multi-agent systems (AutoGPT, CrewAI, LangGraph integrations)
- **Use Case**: Need shared memory without building infrastructure
- **Requirements**: Simple API, embedded deployment, good documentation

### 3.2 Secondary Users

#### AI Assistant Developers
- **Description**: Building long-running assistants with persistent memory
- **Use Case**: Cross-session learning, user preference tracking
- **Requirements**: Low latency, reliable persistence

#### RAG System Developers
- **Description**: Building retrieval systems that need reasoning, not just retrieval
- **Use Case**: Cross-document reasoning, relationship tracking
- **Requirements**: Vector search + relationship storage

---

## 4. User Stories

### 4.1 Core Stories (MVP)

| ID | As a... | I want to... | So that... | Priority |
|----|---------|--------------|------------|----------|
| US-001 | Agent developer | Record experiences from my agents | Other agents can learn from them | Must |
| US-002 | Agent developer | Retrieve relevant context for a task | My agent has the knowledge it needs | Must |
| US-003 | Agent developer | Create isolated collectives per project | Projects don't leak knowledge | Must |
| US-004 | Agent developer | Track what other agents are doing | Agents can coordinate without conflicts | Must |
| US-005 | Agent developer | Store relationships between experiences | Agents understand how knowledge connects | Must |
| US-006 | Agent developer | Subscribe to new experiences | Agents react to new learnings in real-time | Must |
| US-007 | Agent developer | Store derived insights | Synthesized knowledge is preserved | Must |
| US-008 | System integrator | Use PulseDB as a SubstrateProvider | PulseHive can use PulseDB as its storage layer | Must |

### 4.2 Post-MVP Stories

| ID | As a... | I want to... | So that... | Priority |
|----|---------|--------------|------------|----------|
| US-009 | Agent developer | Store cross-project wisdom | Safe patterns transfer between projects | Should |
| US-010 | Agent developer | Store entity-relationship graphs | Complex knowledge structures are preserved | Should |
| US-011 | Agent developer | Store pre-computed KV cache | Context retrieval is faster | Could |
| US-012 | ML engineer | Collect training data from experiences | I can fine-tune models on agent learnings | Could |

---

## 5. Success Metrics

### 5.1 Performance Metrics

| Metric | Target | Rationale |
|--------|--------|-----------|
| Binary size | < 20MB | Embedded deployment, reasonable download |
| Startup time | < 100ms | Fast agent initialization |
| `record_experience()` | < 10ms | Real-time experience recording |
| `get_context_candidates()` | < 100ms | Interactive context retrieval |
| `search_similar()` | < 50ms | Fast semantic search |
| Concurrent readers | Unlimited | MVCC enables parallel reads |
| Experiences per collective | 1M+ | Scale for large projects |

### 5.2 Adoption Metrics (Post-Launch)

| Metric | 3-Month Target | 6-Month Target |
|--------|----------------|----------------|
| crates.io downloads | 1,000 | 5,000 |
| GitHub stars | 500 | 2,000 |
| PulseHive integration | Complete | Production |
| Community contributions | 5 PRs | 20 PRs |

### 5.3 Quality Metrics

| Metric | Target |
|--------|--------|
| Test coverage | > 80% |
| Documentation coverage | 100% public API |
| Zero critical bugs | Maintained |
| MSRV (Minimum Supported Rust Version) | 1.75+ |

---

## 6. Competitive Analysis

### 6.1 Direct Competitors

| Product | Strengths | Weaknesses vs PulseDB |
|---------|-----------|----------------------|
| **LanceDB** | Fast vector search, embedded, Rust | No graph, no experience model, no real-time |
| **ChromaDB** | Easy Python API, good docs | Python-only, no embedded Rust, no graph |
| **Qdrant** | Production vector DB, good performance | Server required, no graph, no experience model |

### 6.2 Indirect Competitors

| Product | Overlap | Differentiation |
|---------|---------|-----------------|
| **PostgreSQL + pgvector** | Vector search | PulseDB: embedded, experience-native, real-time |
| **SQLite + sqlite-vss** | Embedded + vector | PulseDB: graph, experience model, context assembly |
| **Redis + RediSearch** | Fast, real-time | PulseDB: embedded, persistent, vector + graph |

### 6.3 Competitive Positioning

```
                    Experience-Native
                          ▲
                          │
                    PulseDB ●
                          │
        Embedded ◄────────┼────────► Server-Based
                          │
              LanceDB ●   │   ● Qdrant
                          │   ● Pinecone
                          │
                          ▼
                    Document/Vector-Only
```

**PulseDB's Unique Position**: Only embedded database with experience-native storage, integrated vector + graph, and real-time awareness primitives.

---

## 7. Product Scope

### 7.1 In Scope (MVP)

| Category | Features |
|----------|----------|
| **Storage** | Experience CRUD, Collective management, Activity tracking |
| **Search** | Vector similarity search, Recency retrieval, Domain filtering |
| **Relationships** | Experience relations storage, Derived insights storage |
| **Real-time** | Watch/subscription for new experiences |
| **Integration** | SubstrateProvider trait implementation |
| **Embedding** | Built-in ONNX model (384d), External embedding support |

### 7.2 Out of Scope (MVP)

| Feature | Reason | Timeline |
|---------|--------|----------|
| Entity-Relationship Graph | Complexity, not needed for PulseHive MVP | Post-MVP |
| Cross-Collective Wisdom | Requires abstraction algorithms in PulseHive | Post-MVP |
| KV Cache Storage (REFRAG) | Performance optimization, not core | Post-MVP |
| Training Data Collection | Nice-to-have for ML pipelines | Post-MVP |
| Python Bindings | Rust-first, PulseHive is Rust | 3-6 months post-MVP |
| Distributed Deployment | Single-node embedded is the value prop | Not planned |
| Cloud-Hosted Service | Against embedded philosophy | Not planned |
| SQL Query Language | Not the right abstraction | Not planned |
| GUI/Dashboard | Separate project if needed | Not planned |

### 7.3 Non-Goals

These are explicitly NOT goals for PulseDB:

1. **Not a general-purpose database** — Optimized for agentic workloads only
2. **Not a server** — Embedded single-binary deployment is core value
3. **Not an intelligence layer** — PulseDB stores, consumers (PulseHive) think
4. **Not cross-machine sync** — Single-node, local-first

---

## 8. Technical Constraints

### 8.1 Language & Runtime

| Constraint | Value | Rationale |
|------------|-------|-----------|
| Primary language | Rust | Performance, safety, PulseHive compatibility |
| MSRV | 1.75+ | Modern async, stable features |
| Target platforms | Linux, macOS, Windows | Developer machines, CI/CD |
| Architecture | x86_64, aarch64 | Modern systems, Apple Silicon |

### 8.2 Dependencies

| Dependency | Purpose | Risk |
|------------|---------|------|
| redb | Key-value storage | Low (pure Rust, mature) |
| hnsw_rs | HNSW vector index | Low (pure Rust, battle-tested) |
| ort (ONNX Runtime) | Embedding generation | Medium (large, but optional) |
| crossbeam-channel | In-process notifications | Low (standard choice) |
| bincode/postcard | Serialization | Low (pure Rust) |

### 8.3 Performance Constraints

| Constraint | Requirement |
|------------|-------------|
| Memory usage | < 100MB base + data |
| Disk I/O | Optimized for SSD |
| CPU | Single-threaded writes, parallel reads |
| Startup | Cold start < 100ms |

---

## 9. Architecture Overview

### 9.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        CONSUMER APPLICATIONS                     │
│  (PulseHive, Custom Agent Systems, RAG Systems)                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                    PulseDB Public API                        ││
│  │  record_experience()  get_context_candidates()  watch()     ││
│  │  create_collective()  store_relation()  store_insight()     ││
│  └─────────────────────────────────────────────────────────────┘│
│                              │                                   │
│  ┌───────────────────────────┼───────────────────────────────┐  │
│  │                     PULSEDB CORE                           │  │
│  │                           │                                │  │
│  │  ┌─────────────┐  ┌───────┴───────┐  ┌─────────────────┐  │  │
│  │  │  Embedding  │  │  Query Engine │  │  Watch System   │  │  │
│  │  │  Provider   │  │  (candidates) │  │  (crossbeam)    │  │  │
│  │  └─────────────┘  └───────────────┘  └─────────────────┘  │  │
│  │         │                 │                   │            │  │
│  │  ┌──────┴─────────────────┴───────────────────┴──────────┐│  │
│  │  │                   Storage Layer                        ││  │
│  │  │  ┌─────────────┐              ┌─────────────────────┐ ││  │
│  │  │  │    redb     │              │    HNSW Index       │ ││  │
│  │  │  │  (KV store) │              │   (hnsw_rs)         │ ││  │
│  │  │  └─────────────┘              └─────────────────────┘ ││  │
│  │  └────────────────────────────────────────────────────────┘│  │
│  └────────────────────────────────────────────────────────────┘  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 9.2 Key Architectural Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Storage engine | redb | Pure Rust, embedded, ACID, MVCC |
| Vector index | hnsw_rs (pure Rust) | Native filtered search, no FFI risks (ADR-005) |
| Embedding | ONNX (optional) | Zero runtime dependencies when external |
| Serialization | bincode/postcard | Fast, compact, Rust-native |
| Concurrency | Single-writer, multi-reader | Matches redb, simple mental model |

### 9.3 Architectural Boundary

**PulseDB is a pure storage layer.** All intelligence lives in consumers:

| PulseDB Provides | Consumer (PulseHive) Provides |
|------------------|-------------------------------|
| `store_experience()` | When to record, what to record |
| `get_context_candidates()` | Context assembly, token budgeting |
| `store_relation()` | Relationship inference algorithm |
| `store_insight()` | Insight synthesis algorithm |
| `store_wisdom()` | Wisdom abstraction algorithm |
| Embedding generation (optional) | Embedding model selection |

---

## 10. Release Criteria

### 10.1 MVP Release Criteria

| Category | Criteria | Verification |
|----------|----------|--------------|
| **Functionality** | All US-001 through US-008 complete | Acceptance tests |
| **Performance** | All performance metrics met | Benchmark suite |
| **Quality** | > 80% test coverage | Coverage report |
| **Quality** | Zero critical bugs | Issue tracker |
| **Documentation** | All public API documented | rustdoc review |
| **Documentation** | README with examples | Manual review |
| **Integration** | PulseHive integration working | Integration test |
| **Packaging** | Published to crates.io | Manual verification |

### 10.2 Release Checklist

```
Pre-Release:
□ All tests passing
□ Benchmarks run and documented
□ CHANGELOG updated
□ Version bumped
□ rustdoc generated and reviewed
□ README examples tested
□ MSRV verified

Release:
□ Tag created
□ crates.io published
□ GitHub release created
□ Announcement prepared

Post-Release:
□ Monitor for issues
□ Community feedback collected
```

---

## 11. Risks and Mitigations

### 11.1 Technical Risks

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| hnsw_rs integration | Low | Low | Pure Rust, wrapped behind VectorIndex trait |
| ONNX model size bloats binary | Medium | Medium | Make optional, lazy loading |
| Performance targets not met | Low | High | Early benchmarking, profiling |
| redb limitations discovered | Low | High | Evaluate early, have backup plan |

### 11.2 Resource Risks

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Solo developer bottleneck | High | Medium | Prioritize ruthlessly, MVP only |
| Scope creep | Medium | High | Strict out-of-scope list |
| Timeline slip | Medium | Medium | Buffer in schedule |

### 11.3 Market Risks

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Competitor releases similar product | Low | Medium | First-mover advantage, PulseHive integration |
| No adoption | Medium | High | Strong docs, compelling demo |

---

## 12. Timeline Overview

### 12.1 Phase Summary

| Phase | Duration | Focus |
|-------|----------|-------|
| Phase 1: Foundation | 4 weeks | Core storage, basic CRUD |
| Phase 2: Substrate | 3 weeks | Hive mind primitives |
| Phase 3: Polish | 3 weeks | Quality, performance, release |
| **Total MVP** | **10 weeks** | |

### 12.2 Milestones

| Milestone | Target | Deliverable |
|-----------|--------|-------------|
| M1: Storage Working | Week 4 | redb + HNSW integration |
| M2: Primitives Complete | Week 7 | All MVP API implemented |
| M3: PulseHive Ready | Week 9 | SubstrateProvider working |
| M4: Release | Week 10 | crates.io publication |

---

## 13. Open Questions

| ID | Question | Status | Resolution |
|----|----------|--------|------------|
| OQ-001 | Should we support custom embedding dimensions beyond 384/768? | Resolved | Yes, via `EmbeddingDimension::Custom(usize)` |
| OQ-002 | Should watch() be async or sync with callback? | Resolved | Async stream (`impl Stream<Item = Experience>`) |
| OQ-003 | Should we support multiple HNSW indexes per collective? | Deferred | Single index for MVP, evaluate post-MVP |

---

## 14. Appendix

### 14.1 Glossary

| Term | Definition |
|------|------------|
| **Experience** | A unit of learning that can benefit other agents |
| **Collective** | An isolated hive mind, typically one per project |
| **Wisdom** | Abstracted patterns safe to transfer across collectives |
| **Context Candidates** | Raw retrieval results before assembly |
| **Substrate** | The shared storage layer agents read/write |
| **SubstrateProvider** | Trait defining the storage interface for PulseHive |

### 14.2 References

- [SPEC.md](../SPEC.md) — Technical specification
- [PulseHive SPEC](../../PulseHive/SPEC.md) — Consumer application specification

---

## Changelog

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0.0 | February 2026 | PulseDB Team | Initial PRD |
