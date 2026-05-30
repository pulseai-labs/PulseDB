# PulseDB: API Reference

> **Version:** 1.0.0  
> **Status:** Approved  
> **Last Updated:** February 2026  
> **Owner:** PulseDB Team

---

## 1. Overview

This document provides the complete API reference for PulseDB. All public types, functions, and traits are documented with signatures, parameters, return types, and usage examples.

### 1.1 Crate Structure

```
pulsedb
├── lib.rs              // Re-exports
├── db.rs               // PulseDB struct
├── collective.rs       // Collective management
├── experience.rs       // Experience CRUD
├── search.rs           // Search and retrieval
├── relation.rs         // Relationship storage
├── insight.rs          // Derived insights
├── activity.rs         // Activity tracking
├── watch.rs            // Real-time notifications
├── embedding.rs        // Embedding service
├── substrate.rs        // SubstrateProvider trait
├── types.rs            // Core types
└── error.rs            // Error types
```

### 1.2 Feature Flags

```toml
[features]
default = ["builtin-embeddings"]
builtin-embeddings = ["ort"]  # ONNX runtime for embedding generation
```

---

## 2. Database Lifecycle

### 2.1 PulseDB

The main entry point for all database operations.

```rust
pub struct PulseDB { /* private */ }
```

#### `PulseDB::open`

Opens or creates a PulseDB database.

```rust
pub fn open(path: impl AsRef<Path>, config: Config) -> Result<PulseDB, PulseDBError>
```

**Parameters:**
| Name | Type | Description |
|------|------|-------------|
| `path` | `impl AsRef<Path>` | Path to database file |
| `config` | `Config` | Configuration options |

**Returns:** `Result<PulseDB, PulseDBError>`

**Errors:**
| Error | Condition |
|-------|-----------|
| `StorageError::Io` | File system error |
| `StorageError::Corrupted` | Database file corrupted |
| `ValidationError::DimensionMismatch` | Config dimension doesn't match existing |

**Example:**
```rust
use pulsedb::{PulseDB, Config};

// Open with defaults
let db = PulseDB::open("./pulse.db", Config::default())?;

// Open with custom config
let db = PulseDB::open("./pulse.db", Config {
    embedding_provider: EmbeddingProvider::External,
    embedding_dimension: EmbeddingDimension::D768,
    ..Default::default()
})?;
```

---

#### `PulseDB::close`

Closes the database, flushing all pending writes.

```rust
pub fn close(self) -> Result<(), PulseDBError>
```

**Parameters:** None (consumes self)

**Returns:** `Result<(), PulseDBError>`

**Example:**
```rust
let db = PulseDB::open("./pulse.db", Config::default())?;
// ... use database ...
db.close()?;  // Explicit close
// db is consumed, cannot be used after this
```

---

### 2.2 Config

Database configuration options.

```rust
#[derive(Clone, Debug)]
pub struct Config {
    /// Embedding provider configuration
    pub embedding_provider: EmbeddingProvider,
    
    /// Embedding vector dimension
    pub embedding_dimension: EmbeddingDimension,
    
    /// Default collective for operations (optional)
    pub default_collective: Option<CollectiveId>,
    
    /// Cache size in megabytes
    pub cache_size_mb: usize,
    
    /// Sync mode for durability
    pub sync_mode: SyncMode,
    
    /// Watch system configuration
    pub watch_config: WatchConfig,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            embedding_provider: EmbeddingProvider::Builtin { model_path: None },
            embedding_dimension: EmbeddingDimension::D384,
            default_collective: None,
            cache_size_mb: 64,
            sync_mode: SyncMode::Normal,
            watch_config: WatchConfig::default(),
        }
    }
}
```

---

### 2.3 EmbeddingProvider

```rust
#[derive(Clone, Debug)]
pub enum EmbeddingProvider {
    /// PulseDB computes embeddings using built-in ONNX model
    Builtin {
        /// Custom model path (None = use bundled model)
        model_path: Option<PathBuf>,
    },
    
    /// Consumer provides pre-computed embeddings
    External,
}
```

**Usage Notes:**
- `Builtin`: PulseDB generates embeddings automatically. No embedding field needed in `NewExperience`.
- `External`: Consumer must provide `embedding` field in `NewExperience`. PulseDB only validates dimension.

---

### 2.4 EmbeddingDimension

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EmbeddingDimension {
    /// 384 dimensions (all-MiniLM-L6-v2)
    D384,
    
    /// 768 dimensions (bge-base-en-v1.5)
    D768,
    
    /// Custom dimension for external providers
    Custom(usize),
}

impl EmbeddingDimension {
    pub fn size(&self) -> usize {
        match self {
            Self::D384 => 384,
            Self::D768 => 768,
            Self::Custom(n) => *n,
        }
    }
}
```

---

### 2.5 SyncMode

```rust
#[derive(Clone, Copy, Debug, Default)]
pub enum SyncMode {
    /// Sync on commit (default, safe)
    #[default]
    Normal,
    
    /// Async sync (faster, risk data loss on crash)
    Fast,
    
    /// Sync every write (slowest, maximum durability)
    Paranoid,
}
```

---

## 3. Collective Management

### 3.1 create_collective

Creates a new isolated collective (hive mind).

```rust
impl PulseDB {
    pub fn create_collective(&self, name: &str) -> Result<CollectiveId, PulseDBError>;
    
    pub fn create_collective_with_owner(
        &self,
        name: &str,
        owner_id: UserId,
    ) -> Result<CollectiveId, PulseDBError>;
}
```

**Parameters:**
| Name | Type | Description |
|------|------|-------------|
| `name` | `&str` | Human-readable name (max 256 chars) |
| `owner_id` | `UserId` | Optional owner identifier |

**Returns:** `Result<CollectiveId, PulseDBError>`

**Example:**
```rust
// Simple creation
let collective_id = db.create_collective("my-project")?;

// With owner
let collective_id = db.create_collective_with_owner(
    "my-project",
    UserId("user_123".into()),
)?;
```

---

### 3.2 list_collectives

Lists all collectives, optionally filtered by owner.

```rust
impl PulseDB {
    pub fn list_collectives(&self) -> Result<Vec<Collective>, PulseDBError>;
    
    pub fn list_collectives_by_owner(
        &self,
        owner_id: &UserId,
    ) -> Result<Vec<Collective>, PulseDBError>;
}
```

**Returns:** `Result<Vec<Collective>, PulseDBError>`

**Example:**
```rust
// List all
let collectives = db.list_collectives()?;

// Filter by owner
let my_collectives = db.list_collectives_by_owner(&UserId("user_123".into()))?;
```

---

### 3.3 get_collective

Gets a collective by ID.

```rust
impl PulseDB {
    pub fn get_collective(&self, id: CollectiveId) -> Result<Option<Collective>, PulseDBError>;
}
```

**Returns:** `Result<Option<Collective>, PulseDBError>`
- `Ok(Some(collective))` if found
- `Ok(None)` if not found
- `Err(...)` on database error

---

### 3.4 get_collective_stats

Gets statistics for a collective.

```rust
impl PulseDB {
    pub fn get_collective_stats(&self, id: CollectiveId) -> Result<CollectiveStats, PulseDBError>;
}

#[derive(Clone, Debug)]
pub struct CollectiveStats {
    pub experience_count: u64,
    pub relation_count: u64,
    pub insight_count: u64,
    pub active_agent_count: u32,
    pub storage_bytes: u64,
    pub created_at: Timestamp,
    pub last_activity: Option<Timestamp>,
}
```

---

### 3.5 delete_collective

Deletes a collective and all its data.

```rust
impl PulseDB {
    pub fn delete_collective(&self, id: CollectiveId) -> Result<(), PulseDBError>;
}
```

**⚠️ Warning:** This permanently deletes all experiences, relations, insights, and activities in the collective.

---

## 4. Experience Operations

### 4.1 record_experience

Records a new experience to the collective.

```rust
impl PulseDB {
    pub fn record_experience(&self, experience: NewExperience) -> Result<ExperienceId, PulseDBError>;
}

#[derive(Clone, Debug, Default)]
pub struct NewExperience {
    /// Target collective (required)
    pub collective_id: CollectiveId,
    
    /// Experience content (required)
    pub content: String,
    
    /// Experience type (required)
    pub experience_type: ExperienceType,
    
    /// Pre-computed embedding (required if External provider)
    pub embedding: Option<Vec<f32>>,
    
    /// Importance score (0.0 - 1.0)
    pub importance: f32,
    
    /// Confidence score (0.0 - 1.0)
    pub confidence: f32,
    
    /// Domain tags
    pub domain: Vec<String>,
    
    /// Related file paths
    pub related_files: Vec<String>,
    
    /// Source agent ID
    pub source_agent: AgentId,
    
    /// Source task ID (optional)
    pub source_task: Option<TaskId>,
}
```

**Example:**
```rust
use pulsedb::{NewExperience, ExperienceType, Severity};

let exp_id = db.record_experience(NewExperience {
    collective_id,
    content: "Prisma client not available in edge runtime".into(),
    experience_type: ExperienceType::Difficulty {
        description: "Next.js middleware runs in edge runtime".into(),
        severity: Severity::High,
    },
    importance: 0.9,
    confidence: 1.0,
    domain: vec!["prisma".into(), "nextjs".into(), "edge".into()],
    source_agent: AgentId("agent_1".into()),
    ..Default::default()
})?;
```

---

### 4.2 get_experience

Retrieves an experience by ID.

```rust
impl PulseDB {
    pub fn get_experience(&self, id: ExperienceId) -> Result<Option<Experience>, PulseDBError>;
}
```

**Returns:** `Result<Option<Experience>, PulseDBError>`

---

### 4.3 update_experience

Updates mutable fields of an experience.

```rust
impl PulseDB {
    pub fn update_experience(
        &self,
        id: ExperienceId,
        update: ExperienceUpdate,
    ) -> Result<(), PulseDBError>;
}

#[derive(Clone, Debug, Default)]
pub struct ExperienceUpdate {
    pub importance: Option<f32>,
    pub confidence: Option<f32>,
    pub domain: Option<Vec<String>>,
    pub related_files: Option<Vec<String>>,
}
```

**Note:** Content and embedding are immutable. Create a new experience if content changes.

---

### 4.4 archive_experience

Soft-deletes an experience (excludes from search).

```rust
impl PulseDB {
    pub fn archive_experience(&self, id: ExperienceId) -> Result<(), PulseDBError>;
}
```

---

### 4.5 unarchive_experience

Restores an archived experience.

```rust
impl PulseDB {
    pub fn unarchive_experience(&self, id: ExperienceId) -> Result<(), PulseDBError>;
}
```

---

### 4.6 delete_experience

Permanently deletes an experience.

```rust
impl PulseDB {
    pub fn delete_experience(&self, id: ExperienceId) -> Result<(), PulseDBError>;
}
```

**⚠️ Warning:** This also deletes all relations involving this experience.

---

### 4.7 reinforce_experience

Increments the application count (experience was useful).

```rust
impl PulseDB {
    pub fn reinforce_experience(
        &self,
        id: ExperienceId,
        importance_boost: Option<f32>,
    ) -> Result<(), PulseDBError>;
}
```

**Parameters:**
| Name | Type | Description |
|------|------|-------------|
| `id` | `ExperienceId` | Experience to reinforce |
| `importance_boost` | `Option<f32>` | Optional importance increase (capped at 1.0) |

---

## 5. Search & Retrieval

### 5.1 get_context_candidates

Retrieves raw context candidates for a task.

```rust
impl PulseDB {
    pub fn get_context_candidates(
        &self,
        request: ContextCandidatesRequest,
    ) -> Result<ContextCandidates, PulseDBError>;
}

#[derive(Clone, Debug)]
pub struct ContextCandidatesRequest {
    /// Target collective
    pub collective_id: CollectiveId,
    
    /// Query embedding for similarity search
    pub query_embedding: Vec<f32>,
    
    /// Max recent experiences to retrieve
    pub max_recent: usize,
    
    /// Max similar experiences to retrieve
    pub max_similar: usize,
    
    /// Include active agent activities
    pub include_activities: bool,
    
    /// Include derived insights
    pub include_insights: bool,
    
    /// Include experience relations
    pub include_relations: bool,
    
    /// Filter by domains (optional)
    pub domain_filter: Option<Vec<String>>,
    
    /// Minimum importance threshold (optional)
    pub min_importance: Option<f32>,
}

impl Default for ContextCandidatesRequest {
    fn default() -> Self {
        Self {
            collective_id: CollectiveId::nil(),
            query_embedding: vec![],
            max_recent: 10,
            max_similar: 20,
            include_activities: true,
            include_insights: true,
            include_relations: true,
            domain_filter: None,
            min_importance: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ContextCandidates {
    /// Recent experiences sorted by timestamp descending
    pub recent_experiences: Vec<Experience>,
    
    /// Similar experiences with similarity scores
    pub similar_experiences: Vec<(Experience, f32)>,
    
    /// Stored derived insights
    pub stored_insights: Vec<DerivedInsight>,
    
    /// Currently active agents
    pub active_agents: Vec<Activity>,
    
    /// Relations for returned experiences
    pub relations: Vec<ExperienceRelation>,
}
```

**Example:**
```rust
let candidates = db.get_context_candidates(ContextCandidatesRequest {
    collective_id,
    query_embedding: embedding_model.embed("Help user with auth")?,
    max_recent: 5,
    max_similar: 20,
    include_activities: true,
    domain_filter: Some(vec!["auth".into()]),
    ..Default::default()
})?;

for (exp, score) in candidates.similar_experiences {
    println!("Score {:.3}: {}", score, exp.content);
}
```

---

### 5.2 search_similar

Vector similarity search for experiences.

```rust
impl PulseDB {
    pub fn search_similar(
        &self,
        collective_id: CollectiveId,
        query_embedding: &[f32],
        k: usize,
    ) -> Result<Vec<(Experience, f32)>, PulseDBError>;
    
    pub fn search_similar_filtered(
        &self,
        collective_id: CollectiveId,
        query_embedding: &[f32],
        k: usize,
        filter: SearchFilter,
    ) -> Result<Vec<(Experience, f32)>, PulseDBError>;
}

#[derive(Clone, Debug, Default)]
pub struct SearchFilter {
    /// Filter by domains (experience must have at least one)
    pub domains: Option<Vec<String>>,
    
    /// Minimum importance threshold
    pub min_importance: Option<f32>,
    
    /// Minimum confidence threshold
    pub min_confidence: Option<f32>,
    
    /// Experience types to include
    pub experience_types: Option<Vec<ExperienceTypeFilter>>,
    
    /// Include archived experiences
    pub include_archived: bool,
}
```

**Returns:** `Vec<(Experience, f32)>` sorted by similarity descending.

---

### 5.3 get_recent_experiences

Gets most recent experiences by timestamp.

```rust
impl PulseDB {
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

---

## 6. Relationship Storage

### 6.1 store_relation

Stores a relationship between experiences.

```rust
impl PulseDB {
    pub fn store_relation(&self, relation: NewExperienceRelation) -> Result<RelationId, PulseDBError>;
}

#[derive(Clone, Debug)]
pub struct NewExperienceRelation {
    pub source_id: ExperienceId,
    pub target_id: ExperienceId,
    pub relation_type: RelationType,
    pub strength: f32,
    pub metadata: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RelationType {
    Supports,
    Contradicts,
    Elaborates,
    Supersedes,
    Implies,
    RelatedTo,
}
```

**Example:**
```rust
let relation_id = db.store_relation(NewExperienceRelation {
    source_id: solution_exp_id,
    target_id: problem_exp_id,
    relation_type: RelationType::Elaborates,
    strength: 0.9,
    metadata: None,
})?;
```

---

### 6.2 get_related_experiences

Gets experiences related to a given experience.

```rust
impl PulseDB {
    pub fn get_related_experiences(
        &self,
        experience_id: ExperienceId,
        direction: RelationDirection,
    ) -> Result<Vec<(Experience, ExperienceRelation)>, PulseDBError>;
    
    pub fn get_related_experiences_by_type(
        &self,
        experience_id: ExperienceId,
        relation_type: RelationType,
        direction: RelationDirection,
    ) -> Result<Vec<(Experience, ExperienceRelation)>, PulseDBError>;
}

#[derive(Clone, Copy, Debug)]
pub enum RelationDirection {
    Outgoing,  // source_id = experience_id
    Incoming,  // target_id = experience_id
    Both,
}
```

---

### 6.3 delete_relation

Deletes a specific relation.

```rust
impl PulseDB {
    pub fn delete_relation(&self, id: RelationId) -> Result<(), PulseDBError>;
}
```

---

## 7. Insight Storage

### 7.1 store_insight

Stores a derived insight (synthesized by consumer).

```rust
impl PulseDB {
    pub fn store_insight(&self, insight: NewDerivedInsight) -> Result<InsightId, PulseDBError>;
}

#[derive(Clone, Debug)]
pub struct NewDerivedInsight {
    pub collective_id: CollectiveId,
    pub content: String,
    pub embedding: Vec<f32>,
    pub source_experiences: Vec<ExperienceId>,
    pub confidence: f32,
    pub domain: Vec<String>,
}
```

---

### 7.2 get_insights

Gets insights similar to a query.

```rust
impl PulseDB {
    pub fn get_insights(
        &self,
        collective_id: CollectiveId,
        query_embedding: &[f32],
        limit: usize,
    ) -> Result<Vec<(DerivedInsight, f32)>, PulseDBError>;
}
```

---

### 7.3 delete_insight

Deletes an insight.

```rust
impl PulseDB {
    pub fn delete_insight(&self, id: InsightId) -> Result<(), PulseDBError>;
}
```

---

## 8. Activity Tracking

### 8.1 register_activity

Registers an agent's current activity.

```rust
impl PulseDB {
    pub fn register_activity(&self, activity: NewActivity) -> Result<(), PulseDBError>;
}

#[derive(Clone, Debug)]
pub struct NewActivity {
    pub agent_id: AgentId,
    pub collective_id: CollectiveId,
    pub task_description: String,
    pub working_on: Vec<String>,
}
```

---

### 8.2 update_heartbeat

Updates activity heartbeat.

```rust
impl PulseDB {
    pub fn update_heartbeat(&self, agent_id: &AgentId, collective_id: CollectiveId) -> Result<(), PulseDBError>;
    
    pub fn update_heartbeat_with_files(
        &self,
        agent_id: &AgentId,
        collective_id: CollectiveId,
        working_on: Vec<String>,
    ) -> Result<(), PulseDBError>;
}
```

---

### 8.3 end_activity

Ends an agent's activity.

```rust
impl PulseDB {
    pub fn end_activity(&self, agent_id: &AgentId, collective_id: CollectiveId) -> Result<(), PulseDBError>;
}
```

---

### 8.4 get_active_agents

Gets currently active agents.

```rust
impl PulseDB {
    pub fn get_active_agents(&self, collective_id: CollectiveId) -> Result<Vec<Activity>, PulseDBError>;
    
    pub fn get_active_agents_with_threshold(
        &self,
        collective_id: CollectiveId,
        stale_threshold: Duration,
    ) -> Result<Vec<Activity>, PulseDBError>;
}
```

---

## 9. Real-Time Watch

### 9.1 watch_experiences

Subscribes to new experiences.

```rust
impl PulseDB {
    pub async fn watch_experiences(
        &self,
        collective_id: CollectiveId,
    ) -> Result<impl Stream<Item = Experience>, PulseDBError>;
    
    pub async fn watch_experiences_filtered(
        &self,
        collective_id: CollectiveId,
        filter: WatchFilter,
    ) -> Result<impl Stream<Item = Experience>, PulseDBError>;
}

#[derive(Clone, Debug, Default)]
pub struct WatchFilter {
    pub domains: Option<Vec<String>>,
    pub experience_types: Option<Vec<ExperienceTypeFilter>>,
    pub min_importance: Option<f32>,
}
```

**Example:**
```rust
use futures::StreamExt;

let mut stream = db.watch_experiences(collective_id).await?;

while let Some(experience) = stream.next().await {
    println!("New experience: {}", experience.content);
}
```

---

### 9.2 WatchConfig

```rust
#[derive(Clone, Debug)]
pub struct WatchConfig {
    /// Use in-process channels (crossbeam)
    pub in_process: bool,
    
    /// Poll interval for cross-process (ms)
    pub poll_interval_ms: u64,
    
    /// Buffer size for watch channel
    pub buffer_size: usize,
}

impl Default for WatchConfig {
    fn default() -> Self {
        Self {
            in_process: true,
            poll_interval_ms: 100,
            buffer_size: 1000,
        }
    }
}
```

---

## 10. SubstrateProvider Trait

The trait for PulseHive integration.

```rust
#[async_trait]
pub trait SubstrateProvider: Send + Sync {
    // ─────────────────────────────────────────────────────────────
    // Experience Operations
    // ─────────────────────────────────────────────────────────────
    
    async fn store_experience(&self, exp: Experience) -> Result<ExperienceId, PulseDBError>;
    
    async fn get_experience(&self, id: ExperienceId) -> Result<Option<Experience>, PulseDBError>;
    
    async fn search_similar(
        &self,
        collective: CollectiveId,
        embedding: &[f32],
        k: usize,
    ) -> Result<Vec<(Experience, f32)>, PulseDBError>;
    
    async fn get_recent(
        &self,
        collective: CollectiveId,
        limit: usize,
    ) -> Result<Vec<Experience>, PulseDBError>;
    
    // ─────────────────────────────────────────────────────────────
    // Relation Operations
    // ─────────────────────────────────────────────────────────────
    
    async fn store_relation(&self, rel: ExperienceRelation) -> Result<RelationId, PulseDBError>;
    
    async fn get_related(
        &self,
        exp_id: ExperienceId,
    ) -> Result<Vec<(Experience, ExperienceRelation)>, PulseDBError>;
    
    // ─────────────────────────────────────────────────────────────
    // Insight Operations
    // ─────────────────────────────────────────────────────────────
    
    async fn store_insight(&self, insight: DerivedInsight) -> Result<InsightId, PulseDBError>;
    
    async fn get_insights(
        &self,
        collective: CollectiveId,
        embedding: &[f32],
        k: usize,
    ) -> Result<Vec<DerivedInsight>, PulseDBError>;
    
    // ─────────────────────────────────────────────────────────────
    // Activity Operations
    // ─────────────────────────────────────────────────────────────
    
    async fn get_activities(&self, collective: CollectiveId) -> Result<Vec<Activity>, PulseDBError>;
    
    // ─────────────────────────────────────────────────────────────
    // Watch Operations
    // ─────────────────────────────────────────────────────────────
    
    async fn watch(
        &self,
        collective: CollectiveId,
    ) -> Result<Pin<Box<dyn Stream<Item = Experience> + Send>>, PulseDBError>;
}
```

**Implementation:**
```rust
pub struct PulseDBSubstrate {
    db: PulseDB,
}

impl PulseDBSubstrate {
    pub fn new(path: impl AsRef<Path>, config: Config) -> Result<Self, PulseDBError> {
        Ok(Self {
            db: PulseDB::open(path, config)?,
        })
    }
}

#[async_trait]
impl SubstrateProvider for PulseDBSubstrate {
    // Async wrappers over sync core
    async fn store_experience(&self, exp: Experience) -> Result<ExperienceId, PulseDBError> {
        // Sync operation, wrapped for async trait
        self.db.record_experience(exp.into())
    }
    // ... other implementations
}
```

---

## 11. Error Types

### 11.1 PulseDBError

```rust
#[derive(Debug, thiserror::Error)]
pub enum PulseDBError {
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
    
    #[error("Validation error: {0}")]
    Validation(#[from] ValidationError),
    
    #[error("Not found: {0}")]
    NotFound(NotFoundError),
    
    #[error("Concurrency error: {0}")]
    Concurrency(#[from] ConcurrencyError),
    
    #[error("Embedding error: {0}")]
    Embedding(#[from] EmbeddingError),
}
```

### 11.2 StorageError

```rust
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("Database corrupted: {0}")]
    Corrupted(String),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Transaction failed: {0}")]
    Transaction(String),
    
    #[error("Serialization error: {0}")]
    Serialization(String),
}
```

### 11.3 ValidationError

```rust
#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error("Embedding dimension mismatch: expected {expected}, got {got}")]
    DimensionMismatch { expected: usize, got: usize },
    
    #[error("Invalid {field}: {reason}")]
    InvalidField { field: String, reason: String },
    
    #[error("Content too large: {size} bytes (max: {max})")]
    ContentTooLarge { size: usize, max: usize },
    
    #[error("Collective not found: {0}")]
    CollectiveNotFound(CollectiveId),
    
    #[error("Experience not found: {0}")]
    ExperienceNotFound(ExperienceId),
}
```

### 11.4 Error Handling Example

```rust
use pulsedb::{PulseDB, PulseDBError, ValidationError};

match db.record_experience(exp) {
    Ok(id) => println!("Recorded: {:?}", id),
    Err(PulseDBError::Validation(ValidationError::DimensionMismatch { expected, got })) => {
        eprintln!("Wrong embedding dimension: expected {}, got {}", expected, got);
    }
    Err(PulseDBError::Validation(ValidationError::CollectiveNotFound(id))) => {
        eprintln!("Collective {:?} does not exist", id);
    }
    Err(e) => eprintln!("Unexpected error: {}", e),
}
```

---

## 12. Type Reference

### 12.1 Identifiers

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CollectiveId(pub Uuid);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExperienceId(pub Uuid);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RelationId(pub Uuid);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InsightId(pub Uuid);

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UserId(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AgentId(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskId(pub String);
```

### 12.2 Timestamp

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Timestamp(pub i64);  // Unix millis

impl Timestamp {
    pub fn now() -> Self {
        Self(SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64)
    }
}
```

---

## 13. Complete Example

```rust
use pulsedb::{
    PulseDB, Config, EmbeddingProvider, EmbeddingDimension,
    NewExperience, ExperienceType, Severity,
    ContextCandidatesRequest, NewExperienceRelation, RelationType,
    AgentId,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ─────────────────────────────────────────────────────────────
    // 1. Open database
    // ─────────────────────────────────────────────────────────────
    let db = PulseDB::open("./pulse.db", Config::default())?;
    
    // ─────────────────────────────────────────────────────────────
    // 2. Create collective
    // ─────────────────────────────────────────────────────────────
    let collective_id = db.create_collective("my-project")?;
    
    // ─────────────────────────────────────────────────────────────
    // 3. Record experiences
    // ─────────────────────────────────────────────────────────────
    let problem_id = db.record_experience(NewExperience {
        collective_id,
        content: "Prisma client fails in edge runtime".into(),
        experience_type: ExperienceType::Difficulty {
            description: "Next.js middleware uses edge".into(),
            severity: Severity::High,
        },
        importance: 0.9,
        domain: vec!["prisma".into(), "nextjs".into()],
        source_agent: AgentId("agent_1".into()),
        ..Default::default()
    })?;
    
    let solution_id = db.record_experience(NewExperience {
        collective_id,
        content: "Use Prisma adapter pattern for edge".into(),
        experience_type: ExperienceType::Solution {
            problem_ref: Some(problem_id),
            approach: "adapter pattern".into(),
            worked: true,
        },
        importance: 0.95,
        domain: vec!["prisma".into(), "nextjs".into()],
        source_agent: AgentId("agent_1".into()),
        ..Default::default()
    })?;
    
    // ─────────────────────────────────────────────────────────────
    // 4. Store relation
    // ─────────────────────────────────────────────────────────────
    db.store_relation(NewExperienceRelation {
        source_id: solution_id,
        target_id: problem_id,
        relation_type: RelationType::Elaborates,
        strength: 1.0,
        metadata: None,
    })?;
    
    // ─────────────────────────────────────────────────────────────
    // 5. Get context candidates (another agent)
    // ─────────────────────────────────────────────────────────────
    let candidates = db.get_context_candidates(ContextCandidatesRequest {
        collective_id,
        query_embedding: vec![0.0; 384],  // Would be real embedding
        max_similar: 10,
        domain_filter: Some(vec!["prisma".into()]),
        ..Default::default()
    })?;
    
    println!("Found {} similar experiences", candidates.similar_experiences.len());
    
    // ─────────────────────────────────────────────────────────────
    // 6. Clean up
    // ─────────────────────────────────────────────────────────────
    db.close()?;
    
    Ok(())
}
```

---

## Changelog

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0.0 | February 2026 | PulseDB Team | Initial API reference |
