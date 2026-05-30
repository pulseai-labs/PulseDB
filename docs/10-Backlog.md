# PulseDB: Product Backlog

> **Version:** 1.0.0  
> **Status:** Approved  
> **Last Updated:** February 2026  
> **Owner:** PulseDB Team

---

## 1. Overview

This document contains the prioritized product backlog for PulseDB, organized by epic with user stories and acceptance criteria.

### 1.1 Priority Legend (MoSCoW)

| Priority | Meaning | MVP |
|----------|---------|-----|
| **Must** | Critical for release | ✓ |
| **Should** | Important but not critical | Partial |
| **Could** | Desirable if time permits | ✗ |
| **Won't** | Out of scope for this release | ✗ |

### 1.2 Story Point Scale

| Points | Complexity | Time Estimate |
|--------|------------|---------------|
| 1 | Trivial | < 2 hours |
| 2 | Simple | 2-4 hours |
| 3 | Medium | 4-8 hours |
| 5 | Complex | 1-2 days |
| 8 | Very Complex | 2-3 days |
| 13 | Epic-level | 3-5 days |

---

## 2. Epic Overview

| Epic | Description | Priority | Points |
|------|-------------|----------|--------|
| E1 | Core Storage | Must | 34 |
| E2 | Vector Search | Must | 21 |
| E3 | Substrate Primitives | Must | 26 |
| E4 | Real-Time Features | Must | 13 |
| E5 | Polish & Release | Must | 18 |
| E6 | Post-MVP Features | Won't (v1) | 55 |
| **Total MVP** | | | **112** |

---

## 3. Epic 1: Core Storage

**Goal:** Implement fundamental database operations with redb.

### E1-S01: Database Lifecycle

| Field | Value |
|-------|-------|
| **ID** | E1-S01 |
| **Title** | Database open/close lifecycle |
| **Priority** | Must |
| **Points** | 5 |
| **Depends On** | - |

**User Story:**
As a developer, I want to open and close a PulseDB database so that I can persist agent experiences.

**Acceptance Criteria:**
- [ ] `PulseDB::open(path, config)` creates new database if not exists
- [ ] `PulseDB::open(path, config)` opens existing database
- [ ] `db.close()` flushes all pending writes
- [ ] Database files created at specified path
- [ ] Config validation (dimension mismatch returns error)
- [ ] Startup time < 100ms for 100K experiences

---

### E1-S02: Collective CRUD

| Field | Value |
|-------|-------|
| **ID** | E1-S02 |
| **Title** | Collective management |
| **Priority** | Must |
| **Points** | 5 |
| **Depends On** | E1-S01 |

**User Story:**
As a developer, I want to create and manage collectives so that I can isolate experiences per project.

**Acceptance Criteria:**
- [ ] `create_collective(name)` returns CollectiveId
- [ ] `create_collective_with_owner(name, owner_id)` supports multi-tenancy
- [ ] `list_collectives()` returns all collectives
- [ ] `list_collectives_by_owner(owner_id)` filters by owner
- [ ] `get_collective(id)` returns Option<Collective>
- [ ] `get_collective_stats(id)` returns experience count, storage size
- [ ] `delete_collective(id)` removes collective and all data
- [ ] Embedding dimension locked on collective creation

---

### E1-S03: Experience Storage

| Field | Value |
|-------|-------|
| **ID** | E1-S03 |
| **Title** | Experience CRUD operations |
| **Priority** | Must |
| **Points** | 8 |
| **Depends On** | E1-S02 |

**User Story:**
As an agent developer, I want to record and retrieve experiences so that agents can learn from each other.

**Acceptance Criteria:**
- [ ] `record_experience(NewExperience)` stores experience
- [ ] `get_experience(id)` retrieves by ID
- [ ] `update_experience(id, update)` modifies mutable fields
- [ ] `archive_experience(id)` soft-deletes (excludes from search)
- [ ] `unarchive_experience(id)` restores archived
- [ ] `delete_experience(id)` permanently removes
- [ ] `reinforce_experience(id)` increments application count
- [ ] Experience content validated (non-empty, < 100KB)
- [ ] Importance/confidence validated (0.0-1.0)
- [ ] Latency < 10ms for record_experience

---

### E1-S04: Embedding Generation

| Field | Value |
|-------|-------|
| **ID** | E1-S04 |
| **Title** | Built-in embedding service |
| **Priority** | Must |
| **Points** | 8 |
| **Depends On** | E1-S03 |

**User Story:**
As a developer, I want PulseDB to generate embeddings automatically so that I don't need external embedding services.

**Acceptance Criteria:**
- [ ] `EmbeddingProvider::Builtin` uses ONNX runtime
- [ ] Default model: all-MiniLM-L6-v2 (384d)
- [ ] Alternative model: bge-base-en-v1.5 (768d) via config
- [ ] Custom model path supported
- [ ] `EmbeddingProvider::External` validates dimension only
- [ ] Embedding generated on record_experience if Builtin
- [ ] Embedding required if External mode
- [ ] Batch embedding for efficiency

---

### E1-S05: redb Integration

| Field | Value |
|-------|-------|
| **ID** | E1-S05 |
| **Title** | redb storage layer |
| **Priority** | Must |
| **Points** | 8 |
| **Depends On** | E1-S01 |

**User Story:**
As a developer, I want reliable storage with ACID transactions so that data is never corrupted.

**Acceptance Criteria:**
- [ ] All tables created per schema (04-DataModel.md)
- [ ] Write transactions are atomic
- [ ] Read transactions use MVCC snapshots
- [ ] Crash recovery works (no data loss on restart)
- [ ] Secondary indexes for efficient queries
- [ ] Serialization with bincode

---

## 4. Epic 2: Vector Search

**Goal:** Implement HNSW-based semantic search.

### E2-S01: HNSW Index

| Field | Value |
|-------|-------|
| **ID** | E2-S01 |
| **Title** | HNSW index integration |
| **Priority** | Must |
| **Points** | 8 |
| **Depends On** | E1-S03 |

**User Story:**
As an agent developer, I want fast semantic search so that agents can find relevant experiences.

**Acceptance Criteria:**
- [ ] hnsw_rs integration via VectorIndex trait
- [ ] Index created per collective
- [ ] Index persisted to disk
- [ ] Index loaded on database open
- [ ] Add/remove vectors on experience create/delete
- [ ] Configurable HNSW parameters (M, ef_construction, ef_search)
- [ ] Search latency < 50ms for k=20, 100K experiences

---

### E2-S02: Similarity Search

| Field | Value |
|-------|-------|
| **ID** | E2-S02 |
| **Title** | search_similar API |
| **Priority** | Must |
| **Points** | 5 |
| **Depends On** | E2-S01 |

**User Story:**
As an agent developer, I want to search for similar experiences so that agents have relevant context.

**Acceptance Criteria:**
- [ ] `search_similar(collective_id, embedding, k)` returns Vec<(Experience, f32)>
- [ ] Results sorted by similarity descending
- [ ] Archived experiences excluded
- [ ] Collective isolation enforced
- [ ] Filter by domain supported
- [ ] Filter by min_importance supported
- [ ] Filter by experience_type supported

---

### E2-S03: Recent Experiences

| Field | Value |
|-------|-------|
| **ID** | E2-S03 |
| **Title** | Recency-based retrieval |
| **Priority** | Must |
| **Points** | 3 |
| **Depends On** | E1-S03 |

**User Story:**
As an agent developer, I want to get recent experiences so that agents have current context.

**Acceptance Criteria:**
- [ ] `get_recent_experiences(collective_id, limit)` returns newest first
- [ ] Uses timestamp index for efficiency
- [ ] Archived experiences excluded
- [ ] Filter options supported

---

### E2-S04: Context Candidates

| Field | Value |
|-------|-------|
| **ID** | E2-S04 |
| **Title** | get_context_candidates API |
| **Priority** | Must |
| **Points** | 5 |
| **Depends On** | E2-S02, E2-S03 |

**User Story:**
As an agent developer, I want a single call to get all context candidates so that context assembly is simple.

**Acceptance Criteria:**
- [ ] `get_context_candidates(request)` returns ContextCandidates
- [ ] Includes recent_experiences
- [ ] Includes similar_experiences with scores
- [ ] Includes stored_insights (if requested)
- [ ] Includes active_agents (if requested)
- [ ] Includes relations (if requested)
- [ ] Filters applied correctly
- [ ] Latency < 100ms

---

## 5. Epic 3: Substrate Primitives

**Goal:** Implement hive mind primitives beyond basic storage.

### E3-S01: Relation Storage

| Field | Value |
|-------|-------|
| **ID** | E3-S01 |
| **Title** | Experience relationship storage |
| **Priority** | Must |
| **Points** | 5 |
| **Depends On** | E1-S03 |

**User Story:**
As an agent developer, I want to store relationships between experiences so that agents understand how knowledge connects.

**Acceptance Criteria:**
- [ ] `store_relation(NewExperienceRelation)` persists relation
- [ ] `get_related_experiences(exp_id, direction)` retrieves related
- [ ] `delete_relation(id)` removes relation
- [ ] RelationType enum (Supports, Contradicts, Elaborates, Supersedes, Implies, RelatedTo)
- [ ] Strength score (0.0-1.0)
- [ ] Self-referential relations prevented
- [ ] Cross-collective relations prevented
- [ ] Cascade delete on experience delete

---

### E3-S02: Insight Storage

| Field | Value |
|-------|-------|
| **ID** | E3-S02 |
| **Title** | Derived insight storage |
| **Priority** | Must |
| **Points** | 5 |
| **Depends On** | E1-S03, E2-S01 |

**User Story:**
As an agent developer, I want to store synthesized insights so that derived knowledge is preserved.

**Acceptance Criteria:**
- [ ] `store_insight(NewDerivedInsight)` persists insight
- [ ] `get_insights(collective_id, embedding, k)` retrieves similar
- [ ] `delete_insight(id)` removes insight
- [ ] Source experiences tracked
- [ ] Insight embedding indexed in HNSW
- [ ] Included in context_candidates

---

### E3-S03: Activity Tracking

| Field | Value |
|-------|-------|
| **ID** | E3-S03 |
| **Title** | Agent activity tracking |
| **Priority** | Must |
| **Points** | 5 |
| **Depends On** | E1-S02 |

**User Story:**
As an agent developer, I want to track what agents are doing so that agents can coordinate.

**Acceptance Criteria:**
- [ ] `register_activity(NewActivity)` creates/updates activity
- [ ] `update_heartbeat(agent_id, collective_id)` updates timestamp
- [ ] `end_activity(agent_id, collective_id)` removes activity
- [ ] `get_active_agents(collective_id)` returns active agents
- [ ] Stale threshold configurable (default 5 min)
- [ ] One activity per agent per collective

---

### E3-S04: SubstrateProvider Trait

| Field | Value |
|-------|-------|
| **ID** | E3-S04 |
| **Title** | SubstrateProvider implementation |
| **Priority** | Must |
| **Points** | 8 |
| **Depends On** | E3-S01, E3-S02, E3-S03 |

**User Story:**
As PulseHive, I want PulseDB to implement SubstrateProvider so that I can use it as my storage layer.

**Acceptance Criteria:**
- [ ] `SubstrateProvider` trait defined in PulseDB
- [ ] `PulseDBSubstrate` implements trait
- [ ] Async wrappers over sync core
- [ ] All trait methods implemented
- [ ] Works with PulseHive HiveMind
- [ ] Re-exported for consumers

---

### E3-S05: Input Validation

| Field | Value |
|-------|-------|
| **ID** | E3-S05 |
| **Title** | Comprehensive input validation |
| **Priority** | Must |
| **Points** | 3 |
| **Depends On** | E1-S03 |

**User Story:**
As a developer, I want clear validation errors so that I can fix incorrect input.

**Acceptance Criteria:**
- [ ] All NewExperience fields validated
- [ ] Content size limit (100KB)
- [ ] Importance/confidence range (0.0-1.0)
- [ ] Embedding dimension validated
- [ ] Domain tag limits
- [ ] File path limits
- [ ] Descriptive error messages
- [ ] ValidationError enum with variants

---

## 6. Epic 4: Real-Time Features

**Goal:** Implement watch and notification system.

### E4-S01: In-Process Watch

| Field | Value |
|-------|-------|
| **ID** | E4-S01 |
| **Title** | In-process watch notifications |
| **Priority** | Must |
| **Points** | 5 |
| **Depends On** | E1-S03 |

**User Story:**
As an agent developer, I want to subscribe to new experiences so that agents react in real-time.

**Acceptance Criteria:**
- [ ] `watch_experiences(collective_id)` returns async Stream
- [ ] New experiences emitted to stream
- [ ] crossbeam-channel for low latency (<100ns)
- [ ] Multiple subscribers supported
- [ ] Filter by domain/type supported
- [ ] Stream ends when dropped
- [ ] No memory leak on drop

---

### E4-S02: Cross-Process Watch

| Field | Value |
|-------|-------|
| **ID** | E4-S02 |
| **Title** | Cross-process change detection |
| **Priority** | Should |
| **Points** | 5 |
| **Depends On** | E4-S01 |

**User Story:**
As a developer, I want multiple processes to detect changes so that distributed agents can coordinate.

**Acceptance Criteria:**
- [ ] WAL sequence number tracked
- [ ] Configurable poll interval
- [ ] Detect new experiences since last check
- [ ] File lock for writer coordination

---

### E4-S03: Watch Configuration

| Field | Value |
|-------|-------|
| **ID** | E4-S03 |
| **Title** | Watch system configuration |
| **Priority** | Should |
| **Points** | 3 |
| **Depends On** | E4-S01 |

**User Story:**
As a developer, I want to configure watch behavior so that I can tune for my use case.

**Acceptance Criteria:**
- [ ] `WatchConfig` struct in Config
- [ ] `in_process` flag (default: true)
- [ ] `poll_interval_ms` for cross-process
- [ ] `buffer_size` for channel

---

## 7. Epic 5: Polish & Release

**Goal:** Quality, documentation, and release preparation.

### E5-S01: Error Handling

| Field | Value |
|-------|-------|
| **ID** | E5-S01 |
| **Title** | Comprehensive error handling |
| **Priority** | Must |
| **Points** | 3 |
| **Depends On** | E1-S03 |

**User Story:**
As a developer, I want clear error types so that I can handle errors appropriately.

**Acceptance Criteria:**
- [ ] `PulseDBError` enum with categories
- [ ] `StorageError` for database issues
- [ ] `ValidationError` for input issues
- [ ] `NotFoundError` for missing resources
- [ ] `ConcurrencyError` for lock issues
- [ ] `EmbeddingError` for embedding issues
- [ ] thiserror for error derivation
- [ ] Actionable error messages

---

### E5-S02: Documentation

| Field | Value |
|-------|-------|
| **ID** | E5-S02 |
| **Title** | API documentation |
| **Priority** | Must |
| **Points** | 5 |
| **Depends On** | All |

**User Story:**
As a developer, I want comprehensive documentation so that I can use PulseDB effectively.

**Acceptance Criteria:**
- [ ] All public types documented
- [ ] All public functions documented
- [ ] Examples for common operations
- [ ] README with quick start
- [ ] rustdoc generated and verified
- [ ] Examples compile and run

---

### E5-S03: Test Suite

| Field | Value |
|-------|-------|
| **ID** | E5-S03 |
| **Title** | Comprehensive test coverage |
| **Priority** | Must |
| **Points** | 5 |
| **Depends On** | All |

**User Story:**
As a developer, I want high test coverage so that I can trust PulseDB works correctly.

**Acceptance Criteria:**
- [ ] Unit tests for all modules
- [ ] Integration tests for workflows
- [ ] Property-based tests for invariants
- [ ] Fuzz tests for crash resistance
- [ ] Coverage > 80%
- [ ] CI pipeline running all tests

---

### E5-S04: Benchmarks

| Field | Value |
|-------|-------|
| **ID** | E5-S04 |
| **Title** | Performance benchmarks |
| **Priority** | Must |
| **Points** | 3 |
| **Depends On** | E2-S04 |

**User Story:**
As a developer, I want performance benchmarks so that I can verify PulseDB meets requirements.

**Acceptance Criteria:**
- [ ] Criterion benchmarks for core operations
- [ ] record_experience < 10ms
- [ ] search_similar < 50ms
- [ ] get_context_candidates < 100ms
- [ ] Scaling benchmarks (1K to 1M)
- [ ] CI regression detection

---

### E5-S05: Release

| Field | Value |
|-------|-------|
| **ID** | E5-S05 |
| **Title** | crates.io release |
| **Priority** | Must |
| **Points** | 2 |
| **Depends On** | All |

**User Story:**
As a developer, I want PulseDB on crates.io so that I can add it as a dependency.

**Acceptance Criteria:**
- [ ] Cargo.toml metadata complete
- [ ] CHANGELOG updated
- [ ] Version 0.1.0
- [ ] `cargo publish` succeeds
- [ ] GitHub release created
- [ ] Demo example published

---

## 8. Epic 6: Post-MVP Features

**Goal:** Future enhancements after MVP release.

### E6-S01: Entity Graph (Post-MVP)

| Points | 13 |
|--------|-----|

- Entity CRUD operations
- Relation CRUD operations
- Graph traversal queries
- Graph-enhanced context candidates

### E6-S02: Cross-Collective Wisdom (Post-MVP)

| Points | 8 |
|--------|---|

- Wisdom storage
- get_applicable_wisdom API
- Source collective tracking
- Confidence scores

### E6-S03: KV Cache Storage (Post-MVP)

| Points | 13 |
|--------|-----|

- KVCacheEntry storage
- Memory-mapped file for KV data
- LZ4 compression
- Model version management
- Cache invalidation

### E6-S04: Training Data Collection (Post-MVP)

| Points | 8 |
|--------|---|

- TrainingExample storage
- Batch retrieval for fine-tuning
- Quality scoring
- Export format

### E6-S05: Python Bindings (Post-MVP)

| Points | 13 |
|--------|-----|

- PyO3 bindings
- Python package
- Async support
- Documentation

---

## 9. Dependency Graph

```
E1-S01 (DB Lifecycle)
    │
    ├── E1-S05 (redb Integration)
    │
    └── E1-S02 (Collective CRUD)
            │
            ├── E1-S03 (Experience Storage)
            │       │
            │       ├── E1-S04 (Embedding)
            │       │
            │       ├── E2-S01 (HNSW Index)
            │       │       │
            │       │       └── E2-S02 (Search Similar)
            │       │               │
            │       │               └── E2-S04 (Context Candidates)
            │       │
            │       ├── E2-S03 (Recent)
            │       │
            │       ├── E3-S01 (Relations)
            │       │
            │       ├── E3-S02 (Insights)
            │       │
            │       ├── E3-S05 (Validation)
            │       │
            │       └── E4-S01 (Watch)
            │               │
            │               └── E4-S02 (Cross-Process)
            │
            └── E3-S03 (Activities)

E3-S04 (SubstrateProvider) depends on E3-S01, E3-S02, E3-S03
E5-* (Polish) depends on all above
```

---

## 10. Sprint Allocation

### Sprint 1 (Week 1-2): Foundation
- E1-S01: Database Lifecycle (5 pts)
- E1-S05: redb Integration (8 pts)
- E1-S02: Collective CRUD (5 pts)
- **Total: 18 points**

### Sprint 2 (Week 3-4): Core Storage
- E1-S03: Experience Storage (8 pts)
- E1-S04: Embedding Generation (8 pts)
- E3-S05: Input Validation (3 pts)
- **Total: 19 points**

### Sprint 3 (Week 5-6): Vector Search
- E2-S01: HNSW Index (8 pts)
- E2-S02: Similarity Search (5 pts)
- E2-S03: Recent Experiences (3 pts)
- **Total: 16 points**

### Sprint 4 (Week 7-8): Substrate Primitives
- E3-S01: Relation Storage (5 pts)
- E3-S02: Insight Storage (5 pts)
- E3-S03: Activity Tracking (5 pts)
- E2-S04: Context Candidates (5 pts)
- **Total: 20 points**

### Sprint 5 (Week 9): Real-Time & Integration
- E4-S01: In-Process Watch (5 pts)
- E4-S02: Cross-Process Watch (5 pts)
- E4-S03: Watch Configuration (3 pts)
- E3-S04: SubstrateProvider (8 pts)
- **Total: 21 points**

### Sprint 6 (Week 10): Polish & Release
- E5-S01: Error Handling (3 pts)
- E5-S02: Documentation (5 pts)
- E5-S03: Test Suite (5 pts)
- E5-S04: Benchmarks (3 pts)
- E5-S05: Release (2 pts)
- **Total: 18 points**

---

## Changelog

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0.0 | February 2026 | PulseDB Team | Initial backlog |
