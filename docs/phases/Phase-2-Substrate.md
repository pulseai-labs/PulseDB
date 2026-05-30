# Phase 2: Substrate

> **Weeks:** 5-8  
> **Sprints:** 3-4  
> **Story Points:** 36  
> **Milestones:** M3 (Vector Search), M4 (Hive Mind Primitives)

---

## 1. Overview

Phase 2 builds the substrate primitives: HNSW vector search, similarity/recency retrieval, relations, insights, activities, and the unified context candidates API.

```
Week 5          Week 6          Week 7          Week 8
┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
│ HNSW Index  │ │ Search Ops  │ │ Hive Prims  │ │ Context API │
│ Integration │ │ Filters     │ │ Rel/Ins/Act │ │ Substrate   │
└──────┬──────┘ └──────┬──────┘ └──────┬──────┘ └──────┬──────┘
       │               │               │               │
       └───────────────┴───────┬───────┴───────────────┘
                               ▼
                      M3: Search    M4: Hive
                      Working       Primitives
```

**Prerequisites:** Phase 1 complete (DB lifecycle, experiences, embeddings)

---

## 2. Stories & Acceptance Criteria

### Sprint 3 (Weeks 5-6): 16 points

#### E2-S01: HNSW Index Integration (8 pts)

**User Story:** As an agent developer, I want fast semantic search so that agents can find relevant experiences.

**Acceptance Criteria:**
- [ ] hnsw_rs integration via VectorIndex trait
- [ ] Index created per collective
- [ ] Index persisted to disk (`collective_id.hnsw`)
- [ ] Index loaded on database open
- [ ] Add vector on experience create
- [ ] Remove vector on experience delete
- [ ] Configurable HNSW parameters (M, ef_construction, ef_search)
- [ ] Search latency < 50ms for k=20, 100K experiences

---

#### E2-S02: Similarity Search (5 pts)

**User Story:** As an agent developer, I want to search for similar experiences so that agents have relevant context.

**Acceptance Criteria:**
- [ ] `search_similar(collective_id, embedding, k)` returns `Vec<(Experience, f32)>`
- [ ] Results sorted by similarity descending
- [ ] Archived experiences excluded
- [ ] Collective isolation enforced
- [ ] Filter by domain supported
- [ ] Filter by min_importance supported
- [ ] Filter by experience_type supported

---

#### E2-S03: Recent Experiences (3 pts)

**User Story:** As an agent developer, I want to get recent experiences so that agents have current context.

**Acceptance Criteria:**
- [ ] `get_recent_experiences(collective_id, limit)` returns newest first
- [ ] Uses timestamp index for efficiency
- [ ] Archived experiences excluded
- [ ] Filter by domain, type supported
- [ ] Latency < 5ms for limit=50

---

### Sprint 4 (Weeks 7-8): 20 points

#### E3-S01: Relation Storage (5 pts)

**User Story:** As an agent developer, I want to store relationships between experiences so that agents understand how knowledge connects.

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

#### E3-S02: Insight Storage (5 pts)

**User Story:** As an agent developer, I want to store synthesized insights so that derived knowledge is preserved.

**Acceptance Criteria:**
- [ ] `store_insight(NewDerivedInsight)` persists insight
- [ ] `get_insights(collective_id, embedding, k)` retrieves similar insights
- [ ] `delete_insight(id)` removes insight
- [ ] Source experiences tracked
- [ ] Insight embedding indexed in HNSW
- [ ] Included in context_candidates

---

#### E3-S03: Activity Tracking (5 pts)

**User Story:** As an agent developer, I want to track what agents are doing so that agents can coordinate.

**Acceptance Criteria:**
- [ ] `register_activity(NewActivity)` creates/updates activity
- [ ] `update_heartbeat(agent_id, collective_id)` updates timestamp
- [ ] `end_activity(agent_id, collective_id)` removes activity
- [ ] `get_active_agents(collective_id)` returns active agents
- [ ] Stale threshold configurable (default 5 min)
- [ ] One activity per agent per collective

---

#### E2-S04: Context Candidates (5 pts)

**User Story:** As an agent developer, I want a single call to get all context candidates so that context assembly is simple.

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

## 3. Dependency Graph

```
Phase 1 Complete
       │
       ├── E2-S01 (HNSW Index)
       │       │
       │       ├── E2-S02 (Search Similar)
       │       │
       │       └── E3-S02 (Insights) ──────┐
       │                                    │
       ├── E2-S03 (Recent)                 │
       │                                    │
       ├── E3-S01 (Relations)              │
       │                                    │
       └── E3-S03 (Activities)             │
               │                            │
               └────────────────────────────┴──► E2-S04 (Context Candidates)
```

---

## 4. Architecture Context

### 4.1 Components to Implement

```
┌─────────────────────────────────────────────────────────────┐
│                      PulseDB                                │
│  ┌─────────────────────────────────────────────────────┐   │
│  │                   Public API Layer                   │   │
│  │  search_similar(), get_context_candidates(), etc.   │   │
│  └───────────────────────────┬─────────────────────────┘   │
│                              │                              │
│  ┌───────────────────────────▼─────────────────────────┐   │
│  │                   Core Services                      │   │
│  │  ┌──────────────┐  ┌──────────────┐                 │   │
│  │  │ Search       │  │ Relation     │                 │   │
│  │  │ Engine       │  │ Manager      │                 │   │
│  │  └──────────────┘  └──────────────┘                 │   │
│  │  ┌──────────────┐  ┌──────────────┐                 │   │
│  │  │ Insight      │  │ Activity     │                 │   │
│  │  │ Manager      │  │ Tracker      │                 │   │
│  │  └──────────────┘  └──────────────┘                 │   │
│  └───────────────────────────┬─────────────────────────┘   │
│                              │                              │
│  ┌───────────────────────────▼─────────────────────────┐   │
│  │                   Storage Layer                      │   │
│  │  ┌──────────────────┐  ┌────────────────────────┐   │   │
│  │  │    redb          │  │      HNSW Index        │   │   │
│  │  │    (data)        │  │      (vectors)         │   │   │
│  │  └──────────────────┘  └────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### 4.2 File Structure Additions

```
src/
├── search/
│   ├── mod.rs          # SearchEngine
│   ├── query.rs        # Query building
│   ├── filter.rs       # SearchFilter
│   └── tests.rs
│
├── vector/
│   ├── mod.rs          # VectorIndex trait
│   ├── hnsw.rs         # HNSW wrapper (hnsw_rs, pure Rust)
│   └── tests.rs
│
├── relation/
│   ├── mod.rs          # store, get_related, delete
│   ├── types.rs        # ExperienceRelation, RelationType
│   └── tests.rs
│
├── insight/
│   ├── mod.rs          # store, get_insights, delete
│   ├── types.rs        # DerivedInsight, NewDerivedInsight
│   └── tests.rs
│
└── activity/
    ├── mod.rs          # register, heartbeat, end, get_active
    ├── types.rs        # Activity, NewActivity
    └── tests.rs
```

---

## 5. Data Model

### 5.1 Search Types

```rust
pub struct SearchFilter {
    pub domains: Option<Vec<String>>,
    pub experience_types: Option<Vec<ExperienceType>>,
    pub min_importance: Option<f32>,
    pub min_confidence: Option<f32>,
    pub since: Option<Timestamp>,
    pub exclude_archived: bool,  // default: true
}

pub struct SearchResult {
    pub experience: Experience,
    pub similarity: f32,
}
```

### 5.2 Relation Types

```rust
pub struct RelationId(pub Uuid);

pub struct ExperienceRelation {
    pub id: RelationId,
    pub source_id: ExperienceId,
    pub target_id: ExperienceId,
    pub relation_type: RelationType,
    pub strength: f32,
    pub created_at: Timestamp,
}

pub enum RelationType {
    Supports,     // Source supports target
    Contradicts,  // Source contradicts target
    Elaborates,   // Source elaborates on target
    Supersedes,   // Source supersedes target
    Implies,      // Source implies target
    RelatedTo,    // Generic relation
}

pub struct NewExperienceRelation {
    pub source_id: ExperienceId,
    pub target_id: ExperienceId,
    pub relation_type: RelationType,
    pub strength: f32,
}

pub enum RelationDirection {
    Outgoing,  // Source → Target
    Incoming,  // Target ← Source
    Both,
}
```

### 5.3 Insight Types

```rust
pub struct InsightId(pub Uuid);

pub struct DerivedInsight {
    pub id: InsightId,
    pub collective_id: CollectiveId,
    pub content: String,
    pub embedding: Embedding,
    pub source_experience_ids: Vec<ExperienceId>,
    pub insight_type: InsightType,
    pub confidence: f32,
    pub created_at: Timestamp,
}

pub enum InsightType {
    Pattern,      // Recurring pattern detected
    Synthesis,    // Combined from multiple experiences
    Abstraction,  // Generalized from specifics
    Correlation,  // Correlation between experiences
}

pub struct NewDerivedInsight {
    pub collective_id: CollectiveId,
    pub content: String,
    pub embedding: Option<Embedding>,
    pub source_experience_ids: Vec<ExperienceId>,
    pub insight_type: InsightType,
    pub confidence: f32,
}
```

### 5.4 Activity Types

```rust
pub struct Activity {
    pub agent_id: String,
    pub collective_id: CollectiveId,
    pub current_task: Option<String>,
    pub context_summary: Option<String>,
    pub started_at: Timestamp,
    pub last_heartbeat: Timestamp,
}

pub struct NewActivity {
    pub agent_id: String,
    pub collective_id: CollectiveId,
    pub current_task: Option<String>,
    pub context_summary: Option<String>,
}
```

### 5.5 Context Candidates

```rust
pub struct ContextRequest {
    pub collective_id: CollectiveId,
    pub query_embedding: Embedding,
    pub max_similar: usize,
    pub max_recent: usize,
    pub include_insights: bool,
    pub include_relations: bool,
    pub include_active_agents: bool,
    pub filter: SearchFilter,
}

pub struct ContextCandidates {
    pub similar_experiences: Vec<SearchResult>,
    pub recent_experiences: Vec<Experience>,
    pub insights: Vec<DerivedInsight>,
    pub relations: Vec<ExperienceRelation>,
    pub active_agents: Vec<Activity>,
}
```

### 5.6 Storage Tables (redb additions)

| Table | Key | Value |
|-------|-----|-------|
| `relations` | `RelationId` | `ExperienceRelation` |
| `rel_by_source` | `(ExperienceId, RelationId)` | `()` |
| `rel_by_target` | `(ExperienceId, RelationId)` | `()` |
| `insights` | `InsightId` | `DerivedInsight` |
| `ins_by_collective` | `(CollectiveId, InsightId)` | `()` |
| `activities` | `(CollectiveId, AgentId)` | `Activity` |

### 5.7 HNSW Index Files

```
pulse.db.hnsw/
├── collective_abc123.hnsw       # Experience vectors
├── collective_abc123.hnsw.meta  # Index metadata (dimension, count)
├── collective_abc123_insights.hnsw  # Insight vectors
└── ...
```

---

## 6. API Signatures

### 6.1 Vector Search

```rust
impl PulseDB {
    pub fn search_similar(
        &self,
        collective_id: CollectiveId,
        query: &Embedding,
        k: usize,
    ) -> Result<Vec<SearchResult>, PulseDBError>;

    pub fn search_similar_filtered(
        &self,
        collective_id: CollectiveId,
        query: &Embedding,
        k: usize,
        filter: SearchFilter,
    ) -> Result<Vec<SearchResult>, PulseDBError>;

    pub fn get_recent_experiences(
        &self,
        collective_id: CollectiveId,
        limit: usize,
    ) -> Result<Vec<Experience>, PulseDBError>;

    pub fn get_recent_experiences_filtered(
        &self,
        collective_id: CollectiveId,
        limit: usize,
        filter: SearchFilter,
    ) -> Result<Vec<Experience>, PulseDBError>;
}
```

### 6.2 Relation Management

```rust
impl PulseDB {
    pub fn store_relation(
        &self,
        relation: NewExperienceRelation,
    ) -> Result<RelationId, PulseDBError>;

    pub fn get_related_experiences(
        &self,
        experience_id: ExperienceId,
        direction: RelationDirection,
    ) -> Result<Vec<(Experience, ExperienceRelation)>, PulseDBError>;

    pub fn get_relation(&self, id: RelationId) -> Result<Option<ExperienceRelation>, PulseDBError>;

    pub fn delete_relation(&self, id: RelationId) -> Result<(), PulseDBError>;
}
```

### 6.3 Insight Management

```rust
impl PulseDB {
    pub fn store_insight(
        &self,
        insight: NewDerivedInsight,
    ) -> Result<InsightId, PulseDBError>;

    pub fn get_insights(
        &self,
        collective_id: CollectiveId,
        query: &Embedding,
        k: usize,
    ) -> Result<Vec<DerivedInsight>, PulseDBError>;

    pub fn get_insight(&self, id: InsightId) -> Result<Option<DerivedInsight>, PulseDBError>;

    pub fn delete_insight(&self, id: InsightId) -> Result<(), PulseDBError>;
}
```

### 6.4 Activity Tracking

```rust
impl PulseDB {
    pub fn register_activity(&self, activity: NewActivity) -> Result<(), PulseDBError>;

    pub fn update_heartbeat(
        &self,
        agent_id: &str,
        collective_id: CollectiveId,
    ) -> Result<(), PulseDBError>;

    pub fn end_activity(
        &self,
        agent_id: &str,
        collective_id: CollectiveId,
    ) -> Result<(), PulseDBError>;

    pub fn get_active_agents(
        &self,
        collective_id: CollectiveId,
    ) -> Result<Vec<Activity>, PulseDBError>;
}
```

### 6.5 Context Candidates

```rust
impl PulseDB {
    pub fn get_context_candidates(
        &self,
        request: ContextRequest,
    ) -> Result<ContextCandidates, PulseDBError>;
}
```

---

## 7. HNSW Configuration

### 7.1 Default Parameters

```rust
pub struct HnswConfig {
    pub m: usize,              // Max connections per node (default: 16)
    pub ef_construction: usize, // Construction time accuracy (default: 200)
    pub ef_search: usize,       // Query time accuracy (default: 50)
    pub max_elements: usize,    // Pre-allocated capacity (default: 100_000)
}

impl Default for HnswConfig {
    fn default() -> Self {
        Self {
            m: 16,
            ef_construction: 200,
            ef_search: 50,
            max_elements: 100_000,
        }
    }
}
```

### 7.2 Tuning Guidelines

| Use Case | M | ef_construction | ef_search |
|----------|---|-----------------|-----------|
| Low memory | 8 | 100 | 30 |
| Balanced | 16 | 200 | 50 |
| High recall | 32 | 400 | 100 |

---

## 8. Performance Targets

| Operation | Target | Dataset |
|-----------|--------|---------|
| `search_similar(k=20)` | < 50ms | 100K experiences |
| `search_similar(k=20)` | < 100ms | 1M experiences |
| `get_recent(limit=50)` | < 5ms | Any size |
| `get_context_candidates()` | < 100ms | 100K experiences |
| HNSW index load | < 500ms | 100K vectors |
| HNSW index save | < 1s | 100K vectors |
| `store_relation()` | < 5ms | Single write |
| `store_insight()` | < 10ms | With embedding |

---

## 9. Security & Validation

### 9.1 Input Validation

| Field | Constraint |
|-------|------------|
| `query_embedding` | Matches collective dimension |
| `k` / `limit` | 1 - 1000 |
| `relation.strength` | 0.0 - 1.0 |
| `insight.confidence` | 0.0 - 1.0 |
| `insight.content` | Non-empty, ≤ 100KB |
| `activity.agent_id` | Non-empty, ≤ 255 chars |

### 9.2 Isolation Rules

- Relations cannot cross collectives
- Insights cannot reference experiences from other collectives
- Activities are scoped to collective
- Search results always filtered by collective

---

## 10. Testing Requirements

### 10.1 Unit Tests

```rust
#[cfg(test)]
mod tests {
    // HNSW
    #[test] fn test_hnsw_add_search() { }
    #[test] fn test_hnsw_remove() { }
    #[test] fn test_hnsw_persist_reload() { }
    
    // Search
    #[test] fn test_search_similar_returns_sorted() { }
    #[test] fn test_search_excludes_archived() { }
    #[test] fn test_search_respects_filters() { }
    #[test] fn test_get_recent_ordered_by_time() { }
    
    // Relations
    #[test] fn test_store_relation() { }
    #[test] fn test_get_related_outgoing() { }
    #[test] fn test_get_related_incoming() { }
    #[test] fn test_relation_cascade_delete() { }
    #[test] fn test_self_relation_rejected() { }
    #[test] fn test_cross_collective_rejected() { }
    
    // Insights
    #[test] fn test_store_insight() { }
    #[test] fn test_get_insights_similar() { }
    #[test] fn test_insight_in_context_candidates() { }
    
    // Activities
    #[test] fn test_register_activity() { }
    #[test] fn test_heartbeat_updates_timestamp() { }
    #[test] fn test_stale_activity_excluded() { }
    
    // Context Candidates
    #[test] fn test_context_includes_similar() { }
    #[test] fn test_context_includes_recent() { }
    #[test] fn test_context_respects_flags() { }
}
```

### 10.2 Integration Tests

```rust
// tests/search_integration.rs
#[test]
fn test_semantic_search_workflow() {
    let db = setup_db_with_experiences(100);
    
    let query = db.embed("debugging memory leak")?;
    let results = db.search_similar(collective, &query, 10)?;
    
    assert!(results.len() <= 10);
    assert!(results.windows(2).all(|w| w[0].similarity >= w[1].similarity));
}

// tests/context_integration.rs
#[test]
fn test_full_context_assembly() {
    let db = setup_db_with_all_primitives();
    
    let candidates = db.get_context_candidates(ContextRequest {
        collective_id: collective,
        query_embedding: query,
        max_similar: 10,
        max_recent: 5,
        include_insights: true,
        include_relations: true,
        include_active_agents: true,
        filter: SearchFilter::default(),
    })?;
    
    assert!(!candidates.similar_experiences.is_empty());
    assert!(!candidates.recent_experiences.is_empty());
}
```

---

## 11. Milestones Checklist

### M3: Vector Search (End Week 6)

| Criteria | Status |
|----------|--------|
| HNSW index integration | ☐ |
| `search_similar()` working | ☐ |
| `get_recent_experiences()` working | ☐ |
| Filters implemented | ☐ |
| Search latency < 50ms @ 100K | ☐ |

### M4: Hive Mind Primitives (End Week 8)

| Criteria | Status |
|----------|--------|
| Relation storage | ☐ |
| Insight storage | ☐ |
| Activity tracking | ☐ |
| `get_context_candidates()` | ☐ |
| All primitives tested | ☐ |

---

## 12. Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| `hnsw_rs` | 0.3+ | HNSW vector index (pure Rust, ADR-005) |

---

## 13. References

- [Phase-1-Foundation.md](./Phase-1-Foundation.md) — Prerequisites
- [03-Architecture.md](../03-Architecture.md) — Full architecture
- [04-DataModel.md](../04-DataModel.md) — Complete data model
- [05-API-Reference.md](../05-API-Reference.md) — Full API docs
- [Phase-3-Release.md](./Phase-3-Release.md) — Next phase
