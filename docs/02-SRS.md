# PulseDB: Software Requirements Specification

> **Version:** 1.0.0  
> **Status:** Approved  
> **Last Updated:** February 2026  
> **Owner:** PulseDB Team

---

## 1. Introduction

### 1.1 Purpose

This Software Requirements Specification (SRS) defines the functional and non-functional requirements for PulseDB, an embedded database for agentic AI systems. This document serves as the authoritative reference for implementation and testing.

### 1.2 Scope

PulseDB provides:
- Experience-native storage for AI agent learnings
- Vector similarity search for semantic retrieval
- Experience relationship storage
- Real-time change notifications
- SubstrateProvider interface for PulseHive integration

### 1.3 Definitions

| Term | Definition |
|------|------------|
| Experience | A unit of learning recorded by an agent |
| Collective | An isolated hive mind (typically per project) |
| Wisdom | Abstracted cross-collective knowledge (post-MVP) |
| Context Candidates | Raw retrieval results before consumer assembly |
| SubstrateProvider | Trait interface for PulseHive compatibility |

### 1.4 References

- [01-PRD.md](./01-PRD.md) — Product Requirements Document
- [SPEC.md](../SPEC.md) — Technical Specification

---

## 2. Overall Description

### 2.1 Product Perspective

PulseDB is an embedded database library consumed by:
1. **PulseHive** — As the `SubstrateProvider` implementation
2. **Custom agent systems** — Direct API usage
3. **RAG systems** — For cross-document reasoning

### 2.2 Product Functions

```
┌─────────────────────────────────────────────────────────────┐
│                      PulseDB Functions                       │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Database Lifecycle          Collective Management           │
│  ├── open()                  ├── create_collective()        │
│  ├── close()                 ├── list_collectives()         │
│  └── config                  ├── get_collective_stats()     │
│                              └── delete_collective()         │
│                                                              │
│  Experience Operations       Search & Retrieval              │
│  ├── record_experience()     ├── get_context_candidates()   │
│  ├── get_experience()        ├── search_similar()           │
│  ├── update_experience()     └── get_recent()               │
│  ├── archive_experience()                                    │
│  └── delete_experience()     Real-Time                       │
│                              ├── watch_experiences()         │
│  Relationship Storage        └── get_activities()            │
│  ├── store_relation()                                        │
│  ├── get_related()           Insight Storage                 │
│  └── delete_relation()       ├── store_insight()            │
│                              ├── get_insights()              │
│  Activity Tracking           └── delete_insight()            │
│  ├── register_activity()                                     │
│  ├── update_heartbeat()                                      │
│  └── end_activity()                                          │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### 2.3 User Characteristics

| User Type | Expertise | Usage Pattern |
|-----------|-----------|---------------|
| PulseHive | Expert Rust | Full API, high volume |
| Agent developers | Intermediate Rust | Core CRUD, search |
| RAG developers | Beginner-Intermediate | Simple storage, search |

### 2.4 Constraints

| Constraint | Description |
|------------|-------------|
| Language | Rust (no Python bindings in MVP) |
| Deployment | Embedded only (no server mode) |
| Platform | Linux, macOS, Windows (x86_64, aarch64) |
| Concurrency | Single-writer, multi-reader |

### 2.5 Assumptions and Dependencies

**Assumptions:**
- Consumer handles authentication/authorization
- Consumer provides user ID for multi-tenancy
- Consumer manages embedding model selection (if external)

**Dependencies:**
- redb >= 1.0 (key-value storage)
- hnsw_rs (pure Rust HNSW vector index)
- ort (optional, for built-in embeddings)

---

## 3. Functional Requirements

### 3.1 Database Lifecycle

#### FR-001: Database Open

| Attribute | Value |
|-----------|-------|
| ID | FR-001 |
| Priority | Must |
| Description | Open a PulseDB instance from a file path |

**Input:**
```rust
path: impl AsRef<Path>  // Database file path
config: Config          // Configuration options
```

**Output:**
```rust
Result<PulseDB, PulseDBError>
```

**Behavior:**
1. If file does not exist, create new database
2. If file exists, open existing database
3. Validate configuration against existing database (e.g., embedding dimension)
4. Initialize HNSW index
5. Return PulseDB instance or error

**Acceptance Criteria:**
- [ ] New database created when path doesn't exist
- [ ] Existing database opened when path exists
- [ ] Error returned if config incompatible with existing database
- [ ] Database usable immediately after open

#### FR-002: Database Close

| Attribute | Value |
|-----------|-------|
| ID | FR-002 |
| Priority | Must |
| Description | Close database and flush all pending writes |

**Input:**
```rust
self: PulseDB  // Consumes the instance
```

**Output:**
```rust
Result<(), PulseDBError>
```

**Behavior:**
1. Flush all pending writes to disk
2. Close HNSW index
3. Release file locks
4. Return success or error

**Acceptance Criteria:**
- [ ] All data persisted after close
- [ ] File locks released
- [ ] Instance cannot be used after close (consumed)

#### FR-003: Configuration

| Attribute | Value |
|-----------|-------|
| ID | FR-003 |
| Priority | Must |
| Description | Configure database behavior |

**Configuration Options:**
```rust
pub struct Config {
    pub embedding_provider: EmbeddingProvider,
    pub embedding_dimension: EmbeddingDimension,
    pub default_collective: Option<CollectiveId>,
    pub cache_size_mb: usize,
    pub sync_mode: SyncMode,
}

pub enum EmbeddingProvider {
    Builtin { model_path: Option<PathBuf> },
    External,
}

pub enum EmbeddingDimension {
    D384,           // Default: all-MiniLM-L6-v2
    D768,           // Optional: bge-base-en-v1.5
    Custom(usize),  // For external providers
}

pub enum SyncMode {
    Normal,     // Sync on commit
    Fast,       // Async sync (risk data loss on crash)
    Paranoid,   // Sync every write
}
```

**Acceptance Criteria:**
- [ ] Default config works out of box
- [ ] Builtin embedding generates embeddings
- [ ] External embedding validates provided dimensions
- [ ] Dimension mismatch returns error

---

### 3.2 Collective Management

#### FR-004: Create Collective

| Attribute | Value |
|-----------|-------|
| ID | FR-004 |
| Priority | Must |
| Description | Create a new isolated collective (hive mind) |

**Input:**
```rust
name: &str              // Human-readable name
owner_id: Option<UserId> // Owner identifier (opaque)
```

**Output:**
```rust
Result<CollectiveId, PulseDBError>
```

**Behavior:**
1. Generate unique CollectiveId
2. Lock embedding dimension to current config
3. Initialize empty HNSW index for collective
4. Store collective metadata
5. Return CollectiveId

**Acceptance Criteria:**
- [ ] Unique ID generated
- [ ] Embedding dimension locked
- [ ] Collective immediately usable
- [ ] Name can be retrieved later

#### FR-005: List Collectives

| Attribute | Value |
|-----------|-------|
| ID | FR-005 |
| Priority | Must |
| Description | List all collectives, optionally filtered by owner |

**Input:**
```rust
owner_id: Option<UserId>  // Filter by owner (None = all)
```

**Output:**
```rust
Result<Vec<Collective>, PulseDBError>
```

**Acceptance Criteria:**
- [ ] Returns all collectives when owner_id is None
- [ ] Filters by owner when provided
- [ ] Returns empty vec if no collectives

#### FR-006: Get Collective Stats

| Attribute | Value |
|-----------|-------|
| ID | FR-006 |
| Priority | Should |
| Description | Get statistics for a collective |

**Output:**
```rust
pub struct CollectiveStats {
    pub experience_count: u64,
    pub relation_count: u64,
    pub insight_count: u64,
    pub active_agent_count: u32,
    pub storage_bytes: u64,
    pub created_at: Timestamp,
    pub last_activity: Timestamp,
}
```

**Acceptance Criteria:**
- [ ] Counts are accurate
- [ ] Storage estimate reasonable
- [ ] Timestamps correct

#### FR-007: Delete Collective

| Attribute | Value |
|-----------|-------|
| ID | FR-007 |
| Priority | Must |
| Description | Delete a collective and all its data |

**Behavior:**
1. Delete all experiences in collective
2. Delete all relations in collective
3. Delete all insights in collective
4. Delete HNSW index for collective
5. Delete collective metadata

**Acceptance Criteria:**
- [ ] All data removed
- [ ] Storage reclaimed
- [ ] CollectiveId no longer valid

---

### 3.3 Experience Operations

#### FR-008: Record Experience

| Attribute | Value |
|-----------|-------|
| ID | FR-008 |
| Priority | Must |
| Description | Record a new experience to the collective |

**Input:**
```rust
pub struct NewExperience {
    pub collective_id: CollectiveId,
    pub content: String,
    pub experience_type: ExperienceType,
    pub embedding: Option<Vec<f32>>,  // Required if External provider
    pub importance: f32,              // 0.0 - 1.0
    pub confidence: f32,              // 0.0 - 1.0
    pub domain: Vec<String>,
    pub related_files: Vec<String>,
    pub source_agent: AgentId,
    pub source_task: Option<TaskId>,
}
```

**Output:**
```rust
Result<ExperienceId, PulseDBError>
```

**Behavior:**
1. Validate collective exists
2. Generate embedding if Builtin provider and not provided
3. Validate embedding dimension matches collective
4. Generate unique ExperienceId
5. Store experience in redb
6. Add to HNSW index
7. Notify watchers
8. Return ExperienceId

**Acceptance Criteria:**
- [ ] Experience persisted
- [ ] Embedding generated (Builtin) or validated (External)
- [ ] Searchable immediately
- [ ] Watchers notified

#### FR-009: Get Experience

| Attribute | Value |
|-----------|-------|
| ID | FR-009 |
| Priority | Must |
| Description | Retrieve an experience by ID |

**Input:**
```rust
id: ExperienceId
```

**Output:**
```rust
Result<Option<Experience>, PulseDBError>
```

**Acceptance Criteria:**
- [ ] Returns experience if exists
- [ ] Returns None if not found
- [ ] Returns error only on database failure

#### FR-010: Update Experience

| Attribute | Value |
|-----------|-------|
| ID | FR-010 |
| Priority | Must |
| Description | Update mutable fields of an experience |

**Mutable Fields:**
```rust
pub struct ExperienceUpdate {
    pub importance: Option<f32>,
    pub confidence: Option<f32>,
    pub applications: Option<u32>,  // Increment count
    pub domain: Option<Vec<String>>,
    pub related_files: Option<Vec<String>>,
}
```

**Note:** Content and embedding are immutable. Create new experience if content changes.

**Acceptance Criteria:**
- [ ] Only specified fields updated
- [ ] Embedding NOT re-indexed (immutable)
- [ ] Updated timestamp set

#### FR-011: Archive Experience

| Attribute | Value |
|-----------|-------|
| ID | FR-011 |
| Priority | Must |
| Description | Soft-delete an experience (exclude from search) |

**Behavior:**
1. Set `archived = true`
2. Remove from HNSW index
3. Keep in storage (can be unarchived)

**Acceptance Criteria:**
- [ ] Not returned in search results
- [ ] Can be retrieved by ID
- [ ] Can be unarchived

#### FR-012: Unarchive Experience

| Attribute | Value |
|-----------|-------|
| ID | FR-012 |
| Priority | Must |
| Description | Restore an archived experience |

**Behavior:**
1. Set `archived = false`
2. Re-add to HNSW index

**Acceptance Criteria:**
- [ ] Appears in search results again
- [ ] Applications count preserved

#### FR-013: Delete Experience

| Attribute | Value |
|-----------|-------|
| ID | FR-013 |
| Priority | Must |
| Description | Permanently delete an experience |

**Behavior:**
1. Remove from HNSW index
2. Delete from storage
3. Delete related relations (cascade)

**Acceptance Criteria:**
- [ ] Cannot be retrieved
- [ ] Relations cleaned up
- [ ] Storage reclaimed

#### FR-014: Reinforce Experience

| Attribute | Value |
|-----------|-------|
| ID | FR-014 |
| Priority | Should |
| Description | Increment applications count (experience was useful) |

**Input:**
```rust
id: ExperienceId
boost: Option<f32>  // Optional importance boost
```

**Behavior:**
1. Increment `applications` counter
2. Optionally boost `importance` (capped at 1.0)

**Acceptance Criteria:**
- [ ] Counter incremented atomically
- [ ] Importance capped at 1.0

---

### 3.4 Search & Retrieval

#### FR-015: Get Context Candidates

| Attribute | Value |
|-----------|-------|
| ID | FR-015 |
| Priority | Must |
| Description | Retrieve raw context candidates for a task |

**Input:**
```rust
pub struct ContextCandidatesRequest {
    pub collective_id: CollectiveId,
    pub query_embedding: Vec<f32>,
    pub max_recent: usize,           // Default: 10
    pub max_similar: usize,          // Default: 20
    pub include_activities: bool,    // Default: true
    pub include_insights: bool,      // Default: true
    pub include_relations: bool,     // Default: true
    pub domain_filter: Option<Vec<String>>,
    pub min_importance: Option<f32>,
}
```

**Output:**
```rust
pub struct ContextCandidates {
    pub recent_experiences: Vec<Experience>,
    pub similar_experiences: Vec<(Experience, f32)>,  // With similarity score
    pub stored_insights: Vec<DerivedInsight>,
    pub active_agents: Vec<Activity>,
    pub relations: Vec<ExperienceRelation>,
}
```

**Behavior:**
1. Retrieve most recent experiences (by timestamp)
2. Retrieve semantically similar experiences (HNSW search)
3. Retrieve stored insights (if requested)
4. Retrieve active agent activities (if requested)
5. Retrieve relations for returned experiences (if requested)
6. Apply filters (domain, importance)
7. Return combined candidates

**Acceptance Criteria:**
- [ ] Recent experiences sorted by timestamp desc
- [ ] Similar experiences sorted by similarity desc
- [ ] Filters applied correctly
- [ ] Archived experiences excluded
- [ ] Performance < 100ms

#### FR-016: Search Similar

| Attribute | Value |
|-----------|-------|
| ID | FR-016 |
| Priority | Must |
| Description | Vector similarity search for experiences |

**Input:**
```rust
collective_id: CollectiveId,
query_embedding: Vec<f32>,
k: usize,                          // Number of results
filter: Option<SearchFilter>,
```

**Output:**
```rust
Vec<(Experience, f32)>  // Experience with similarity score
```

**Acceptance Criteria:**
- [ ] Returns up to k results
- [ ] Sorted by similarity descending
- [ ] Filters applied correctly
- [ ] Performance < 50ms for k=20

#### FR-017: Get Recent Experiences

| Attribute | Value |
|-----------|-------|
| ID | FR-017 |
| Priority | Should |
| Description | Get most recent experiences by timestamp |

**Input:**
```rust
collective_id: CollectiveId,
limit: usize,
filter: Option<SearchFilter>,
```

**Output:**
```rust
Vec<Experience>
```

**Acceptance Criteria:**
- [ ] Sorted by timestamp descending
- [ ] Filters applied
- [ ] Archived excluded

---

### 3.5 Relationship Storage

#### FR-018: Store Relation

| Attribute | Value |
|-----------|-------|
| ID | FR-018 |
| Priority | Must |
| Description | Store a relationship between experiences |

**Input:**
```rust
pub struct NewExperienceRelation {
    pub source_id: ExperienceId,
    pub target_id: ExperienceId,
    pub relation_type: RelationType,
    pub strength: f32,              // 0.0 - 1.0
    pub metadata: Option<String>,   // JSON for extensibility
}

pub enum RelationType {
    Supports,       // Source supports/confirms target
    Contradicts,    // Source contradicts target
    Elaborates,     // Source adds detail to target
    Supersedes,     // Source replaces/updates target
    Implies,        // Source implies target should apply
    RelatedTo,      // General semantic relationship
}
```

**Output:**
```rust
Result<RelationId, PulseDBError>
```

**Acceptance Criteria:**
- [ ] Relation persisted
- [ ] Both experiences must exist
- [ ] Duplicate relations prevented

#### FR-019: Get Related Experiences

| Attribute | Value |
|-----------|-------|
| ID | FR-019 |
| Priority | Must |
| Description | Get experiences related to a given experience |

**Input:**
```rust
experience_id: ExperienceId,
relation_type: Option<RelationType>,  // Filter by type
direction: RelationDirection,         // Outgoing, Incoming, Both
```

**Output:**
```rust
Vec<(Experience, ExperienceRelation)>
```

**Acceptance Criteria:**
- [ ] Returns related experiences with relation metadata
- [ ] Direction filtering works
- [ ] Type filtering works

#### FR-020: Delete Relation

| Attribute | Value |
|-----------|-------|
| ID | FR-020 |
| Priority | Should |
| Description | Delete a specific relation |

**Acceptance Criteria:**
- [ ] Relation removed
- [ ] Experiences not affected

---

### 3.6 Real-Time Features

#### FR-021: Watch Experiences

| Attribute | Value |
|-----------|-------|
| ID | FR-021 |
| Priority | Must |
| Description | Subscribe to new experiences in a collective |

**Input:**
```rust
collective_id: CollectiveId,
filter: Option<WatchFilter>,
```

**Output:**
```rust
impl Stream<Item = Experience>  // Async stream
```

**Behavior:**
1. Create subscription for collective
2. On new experience matching filter, emit to stream
3. Stream continues until dropped

**Acceptance Criteria:**
- [ ] New experiences emitted within 100ms
- [ ] Filter applied correctly
- [ ] Multiple subscribers supported
- [ ] No memory leak on drop

#### FR-022: Register Activity

| Attribute | Value |
|-----------|-------|
| ID | FR-022 |
| Priority | Must |
| Description | Register an agent's current activity |

**Input:**
```rust
pub struct NewActivity {
    pub agent_id: AgentId,
    pub collective_id: CollectiveId,
    pub task_description: String,
    pub working_on: Vec<String>,  // Files, entities
}
```

**Output:**
```rust
Result<ActivityId, PulseDBError>
```

**Behavior:**
1. Create or update activity record
2. Set `started_at` and `last_heartbeat`
3. Return ActivityId

**Acceptance Criteria:**
- [ ] Activity visible to other agents
- [ ] Previous activity for same agent replaced

#### FR-023: Update Heartbeat

| Attribute | Value |
|-----------|-------|
| ID | FR-023 |
| Priority | Must |
| Description | Update activity heartbeat to indicate still active |

**Behavior:**
1. Update `last_heartbeat` timestamp
2. Optionally update `working_on`

**Acceptance Criteria:**
- [ ] Heartbeat updated
- [ ] Activity remains visible

#### FR-024: End Activity

| Attribute | Value |
|-----------|-------|
| ID | FR-024 |
| Priority | Must |
| Description | End an agent's activity |

**Behavior:**
1. Remove activity from active list
2. Activity no longer returned in queries

**Acceptance Criteria:**
- [ ] Activity no longer visible
- [ ] Stale activities auto-expire (configurable timeout)

#### FR-025: Get Active Agents

| Attribute | Value |
|-----------|-------|
| ID | FR-025 |
| Priority | Must |
| Description | Get currently active agents in a collective |

**Input:**
```rust
collective_id: CollectiveId,
stale_threshold: Option<Duration>,  // Default: 5 minutes
```

**Output:**
```rust
Vec<Activity>
```

**Acceptance Criteria:**
- [ ] Only non-stale activities returned
- [ ] Sorted by last_heartbeat

---

### 3.7 Insight Storage

#### FR-026: Store Insight

| Attribute | Value |
|-----------|-------|
| ID | FR-026 |
| Priority | Must |
| Description | Store a derived insight (synthesized by consumer) |

**Input:**
```rust
pub struct NewDerivedInsight {
    pub collective_id: CollectiveId,
    pub content: String,
    pub embedding: Vec<f32>,
    pub source_experiences: Vec<ExperienceId>,
    pub confidence: f32,
    pub domain: Vec<String>,
}
```

**Output:**
```rust
Result<InsightId, PulseDBError>
```

**Acceptance Criteria:**
- [ ] Insight persisted
- [ ] Linked to source experiences
- [ ] Searchable by embedding

#### FR-027: Get Insights

| Attribute | Value |
|-----------|-------|
| ID | FR-027 |
| Priority | Must |
| Description | Get insights similar to a query |

**Input:**
```rust
collective_id: CollectiveId,
query_embedding: Vec<f32>,
limit: usize,
```

**Output:**
```rust
Vec<(DerivedInsight, f32)>  // With similarity score
```

**Acceptance Criteria:**
- [ ] Returns relevant insights
- [ ] Sorted by similarity

#### FR-028: Delete Insight

| Attribute | Value |
|-----------|-------|
| ID | FR-028 |
| Priority | Should |
| Description | Delete an insight (e.g., when stale) |

**Acceptance Criteria:**
- [ ] Insight removed
- [ ] Source experience links cleaned up

---

### 3.8 SubstrateProvider Interface

#### FR-029: Implement SubstrateProvider Trait

| Attribute | Value |
|-----------|-------|
| ID | FR-029 |
| Priority | Must |
| Description | Implement SubstrateProvider for PulseHive compatibility |

**Trait Definition:**
```rust
#[async_trait]
pub trait SubstrateProvider: Send + Sync {
    // Experience operations
    async fn store_experience(&self, exp: Experience) -> Result<ExperienceId>;
    async fn get_experience(&self, id: ExperienceId) -> Result<Option<Experience>>;
    async fn search_similar(&self, collective: CollectiveId, embedding: &[f32], k: usize) -> Result<Vec<(Experience, f32)>>;
    
    // Relation operations
    async fn store_relation(&self, rel: ExperienceRelation) -> Result<RelationId>;
    async fn get_related(&self, exp_id: ExperienceId) -> Result<Vec<(Experience, ExperienceRelation)>>;
    
    // Insight operations
    async fn store_insight(&self, insight: DerivedInsight) -> Result<InsightId>;
    async fn get_insights(&self, collective: CollectiveId, embedding: &[f32], k: usize) -> Result<Vec<DerivedInsight>>;
    
    // Activity operations
    async fn get_activities(&self, collective: CollectiveId) -> Result<Vec<Activity>>;
}
```

**Acceptance Criteria:**
- [ ] All trait methods implemented
- [ ] Works with PulseHive HiveMind
- [ ] Async wrapper over sync core

---

## 4. Non-Functional Requirements

### 4.1 Performance Requirements

#### NFR-001: Startup Time

| Attribute | Value |
|-----------|-------|
| ID | NFR-001 |
| Requirement | Database opens in < 100ms |
| Measurement | Time from `open()` call to returned PulseDB |
| Conditions | Existing database, 100K experiences |

#### NFR-002: Record Experience Latency

| Attribute | Value |
|-----------|-------|
| ID | NFR-002 |
| Requirement | `record_experience()` completes in < 10ms |
| Measurement | P99 latency |
| Conditions | Collective with 100K existing experiences |

#### NFR-003: Context Retrieval Latency

| Attribute | Value |
|-----------|-------|
| ID | NFR-003 |
| Requirement | `get_context_candidates()` completes in < 100ms |
| Measurement | P99 latency |
| Conditions | 100K experiences, k=20 similar |

#### NFR-004: Vector Search Latency

| Attribute | Value |
|-----------|-------|
| ID | NFR-004 |
| Requirement | `search_similar()` completes in < 50ms |
| Measurement | P99 latency |
| Conditions | 1M experiences, k=20 |

#### NFR-005: Concurrent Readers

| Attribute | Value |
|-----------|-------|
| ID | NFR-005 |
| Requirement | Support unlimited concurrent readers |
| Measurement | No degradation with 100 concurrent reads |
| Conditions | MVCC isolation |

#### NFR-006: Scale

| Attribute | Value |
|-----------|-------|
| ID | NFR-006 |
| Requirement | Support 1M+ experiences per collective |
| Measurement | All latency requirements still met |

### 4.2 Storage Requirements

#### NFR-007: Binary Size

| Attribute | Value |
|-----------|-------|
| ID | NFR-007 |
| Requirement | Binary size < 20MB (with ONNX model) |
| Measurement | Release build size |

#### NFR-008: Memory Usage

| Attribute | Value |
|-----------|-------|
| ID | NFR-008 |
| Requirement | Base memory < 100MB + proportional to data |
| Measurement | RSS after open with 100K experiences |

#### NFR-009: Disk Efficiency

| Attribute | Value |
|-----------|-------|
| ID | NFR-009 |
| Requirement | < 2KB per experience (excluding embedding) |
| Measurement | Database size / experience count |

### 4.3 Reliability Requirements

#### NFR-010: Data Durability

| Attribute | Value |
|-----------|-------|
| ID | NFR-010 |
| Requirement | No data loss on normal shutdown |
| Measurement | All writes visible after restart |

#### NFR-011: Crash Recovery

| Attribute | Value |
|-----------|-------|
| ID | NFR-011 |
| Requirement | Recover from crash with at most last write lost |
| Measurement | Database opens after kill -9 |

#### NFR-012: Corruption Detection

| Attribute | Value |
|-----------|-------|
| ID | NFR-012 |
| Requirement | Detect and report database corruption |
| Measurement | Error returned, not silent corruption |

### 4.4 Usability Requirements

#### NFR-013: API Ergonomics

| Attribute | Value |
|-----------|-------|
| ID | NFR-013 |
| Requirement | Common operations require < 5 lines of code |
| Measurement | Code review of examples |

#### NFR-014: Error Messages

| Attribute | Value |
|-----------|-------|
| ID | NFR-014 |
| Requirement | All errors provide actionable information |
| Measurement | Error message review |

#### NFR-015: Documentation

| Attribute | Value |
|-----------|-------|
| ID | NFR-015 |
| Requirement | 100% public API documented with examples |
| Measurement | rustdoc coverage |

### 4.5 Compatibility Requirements

#### NFR-016: Platform Support

| Attribute | Value |
|-----------|-------|
| ID | NFR-016 |
| Requirement | Linux, macOS, Windows support |
| Platforms | x86_64, aarch64 |

#### NFR-017: Rust Version

| Attribute | Value |
|-----------|-------|
| ID | NFR-017 |
| Requirement | MSRV 1.75+ |
| Measurement | CI builds with specified MSRV |

---

## 5. Interface Requirements

### 5.1 External Interfaces

#### 5.1.1 File System Interface

**Database Files:**
```
{path}.db       # Main redb database
{path}.db.hnsw  # HNSW index files (per collective)
{path}.db.kv    # KV cache (post-MVP, optional)
{path}.db.lock  # Lock file for cross-process safety
```

#### 5.1.2 Embedding Model Interface

**Builtin Mode:**
- Model file: ONNX format
- Default path: Bundled in binary
- Custom path: User-provided via config

**External Mode:**
- Consumer provides pre-computed embeddings
- PulseDB validates dimension only

### 5.2 Internal Interfaces

#### 5.2.1 Storage Layer Interface

```rust
trait StorageEngine {
    fn get(&self, table: &str, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn put(&self, table: &str, key: &[u8], value: &[u8]) -> Result<()>;
    fn delete(&self, table: &str, key: &[u8]) -> Result<()>;
    fn scan(&self, table: &str, prefix: &[u8]) -> Result<impl Iterator<Item = (Vec<u8>, Vec<u8>)>>;
}
```

#### 5.2.2 Vector Index Interface

```rust
trait VectorIndex {
    fn add(&mut self, id: u64, vector: &[f32]) -> Result<()>;
    fn remove(&mut self, id: u64) -> Result<()>;
    fn search(&self, query: &[f32], k: usize) -> Result<Vec<(u64, f32)>>;
    fn save(&self, path: &Path) -> Result<()>;
    fn load(path: &Path) -> Result<Self>;
}
```

---

## 6. Data Requirements

### 6.1 Data Entities

See [04-DataModel.md](./04-DataModel.md) for complete data model specification.

### 6.2 Data Validation Rules

| Entity | Field | Validation |
|--------|-------|------------|
| Experience | content | Non-empty, max 100KB |
| Experience | importance | 0.0 - 1.0 |
| Experience | confidence | 0.0 - 1.0 |
| Experience | embedding | Dimension matches collective |
| Collective | name | Non-empty, max 256 chars |
| Relation | strength | 0.0 - 1.0 |
| Insight | source_experiences | All must exist |

---

## 7. Traceability Matrix

| Requirement | User Story | Test Case |
|-------------|------------|-----------|
| FR-001 | US-001 | TC-001 |
| FR-004 | US-003 | TC-004 |
| FR-008 | US-001 | TC-008 |
| FR-015 | US-002 | TC-015 |
| FR-018 | US-005 | TC-018 |
| FR-021 | US-006 | TC-021 |
| FR-026 | US-007 | TC-026 |
| FR-029 | US-008 | TC-029 |
| NFR-002 | - | PERF-002 |
| NFR-003 | - | PERF-003 |
| NFR-004 | - | PERF-004 |

---

## Changelog

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0.0 | February 2026 | PulseDB Team | Initial SRS |
