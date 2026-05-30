# Phase 1: Foundation

> **Weeks:** 1-4  
> **Sprints:** 1-2  
> **Story Points:** 37  
> **Milestones:** M1 (Database Opens), M2 (Core Storage)

---

## 1. Overview

Phase 1 establishes the core storage foundation: database lifecycle, redb integration, collective management, experience CRUD, and embedding service.

```
Week 1          Week 2          Week 3          Week 4
┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
│ DB Lifecycle│ │ Collective  │ │ Experience  │ │ Embedding   │
│ redb Setup  │ │ CRUD        │ │ Storage     │ │ Service     │
└──────┬──────┘ └──────┬──────┘ └──────┬──────┘ └──────┬──────┘
       │               │               │               │
       ▼               │               │               ▼
      M1: DB          ─┴───────────────┴─             M2: Core
      Opens                                          Storage
```

---

## 2. Stories & Acceptance Criteria

### Sprint 1 (Weeks 1-2): 18 points

#### E1-S01: Database Lifecycle (5 pts)

**User Story:** As a developer, I want to open and close a PulseDB database so that I can persist agent experiences.

**Acceptance Criteria:**
- [ ] `PulseDB::open(path, config)` creates new database if not exists
- [ ] `PulseDB::open(path, config)` opens existing database
- [ ] `db.close()` flushes all pending writes
- [ ] Database files created at specified path
- [ ] Config validation (dimension mismatch returns error)
- [ ] Startup time < 100ms for 100K experiences

---

#### E1-S05: redb Integration (8 pts)

**User Story:** As a developer, I want reliable storage with ACID transactions so that data is never corrupted.

**Acceptance Criteria:**
- [ ] All tables created per schema (see Data Model section)
- [ ] Write transactions are atomic
- [ ] Read transactions use MVCC snapshots
- [ ] Crash recovery works (no data loss on restart)
- [ ] Secondary indexes for efficient queries
- [ ] Serialization with bincode

---

#### E1-S02: Collective CRUD (5 pts)

**User Story:** As a developer, I want to create and manage collectives so that I can isolate experiences per project.

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

### Sprint 2 (Weeks 3-4): 19 points

#### E1-S03: Experience Storage (8 pts)

**User Story:** As an agent developer, I want to record and retrieve experiences so that agents can learn from each other.

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

#### E1-S04: Embedding Generation (8 pts)

**User Story:** As a developer, I want PulseDB to generate embeddings automatically so that I don't need external embedding services.

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

#### E3-S05: Input Validation (3 pts)

**User Story:** As a developer, I want clear validation errors so that I can fix incorrect input.

**Acceptance Criteria:**
- [ ] All NewExperience fields validated
- [ ] Content size limit (100KB)
- [ ] Importance/confidence range (0.0-1.0)
- [ ] Embedding dimension validated
- [ ] Domain tag limits (max 10 tags, 100 chars each)
- [ ] File path limits (max 10 paths, 500 chars each)
- [ ] Descriptive error messages
- [ ] ValidationError enum with variants

---

## 3. Dependency Graph

```
E1-S01 (DB Lifecycle)
    │
    ├── E1-S05 (redb Integration)
    │
    └── E1-S02 (Collective CRUD)
            │
            └── E1-S03 (Experience Storage)
                    │
                    ├── E1-S04 (Embedding)
                    │
                    └── E3-S05 (Validation)
```

---

## 4. Architecture Context

### 4.1 Components to Implement

```
┌─────────────────────────────────────────────────────────────┐
│                      PulseDB (lib.rs)                       │
│  ┌─────────────────────────────────────────────────────┐   │
│  │                   Public API Layer                   │   │
│  │  PulseDB::open(), close(), record_experience(), etc │   │
│  └───────────────────────────┬─────────────────────────┘   │
│                              │                              │
│  ┌───────────────────────────▼─────────────────────────┐   │
│  │                   Core Services                      │   │
│  │  ┌──────────────┐  ┌──────────────┐                 │   │
│  │  │ Collective   │  │ Experience   │                 │   │
│  │  │ Manager      │  │ Manager      │                 │   │
│  │  └──────────────┘  └──────────────┘                 │   │
│  │  ┌──────────────┐  ┌──────────────┐                 │   │
│  │  │ Embedding    │  │ Validation   │                 │   │
│  │  │ Service      │  │ Service      │                 │   │
│  │  └──────────────┘  └──────────────┘                 │   │
│  └───────────────────────────┬─────────────────────────┘   │
│                              │                              │
│  ┌───────────────────────────▼─────────────────────────┐   │
│  │                   Storage Layer                      │   │
│  │  ┌──────────────────────────────────────────────┐   │   │
│  │  │              redb Wrapper                     │   │   │
│  │  │  Tables, Transactions, Serialization         │   │   │
│  │  └──────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### 4.2 File Structure

```
src/
├── lib.rs              # Re-exports, PulseDB struct
├── db.rs               # PulseDB lifecycle (open, close)
├── config.rs           # Config, EmbeddingProvider
├── types.rs            # CollectiveId, ExperienceId, timestamps
├── error.rs            # PulseDBError enum
│
├── collective/
│   ├── mod.rs          # create, list, get, delete, stats
│   └── tests.rs
│
├── experience/
│   ├── mod.rs          # record, get, update, archive, delete
│   ├── types.rs        # Experience, NewExperience, ExperienceUpdate
│   ├── validation.rs   # Input validation
│   └── tests.rs
│
├── embedding/
│   ├── mod.rs          # EmbeddingService trait
│   ├── onnx.rs         # ONNX provider implementation
│   └── tests.rs
│
└── storage/
    ├── mod.rs          # Storage abstraction
    ├── redb.rs         # redb implementation
    ├── tables.rs       # Table definitions
    └── tests.rs
```

---

## 5. Data Model

### 5.1 Core Types

```rust
pub struct CollectiveId(pub Uuid);
pub struct ExperienceId(pub Uuid);
pub type Timestamp = i64;  // Unix millis
pub type Embedding = Vec<f32>;
```

### 5.2 Collective

```rust
pub struct Collective {
    pub id: CollectiveId,
    pub name: String,
    pub owner_id: Option<String>,
    pub embedding_dimension: u16,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

pub struct CollectiveStats {
    pub experience_count: u64,
    pub storage_bytes: u64,
    pub oldest_experience: Option<Timestamp>,
    pub newest_experience: Option<Timestamp>,
}
```

### 5.3 Experience

```rust
pub struct Experience {
    pub id: ExperienceId,
    pub collective_id: CollectiveId,
    pub content: String,
    pub embedding: Embedding,
    pub experience_type: ExperienceType,
    pub importance: f32,
    pub confidence: f32,
    pub domain_tags: Vec<String>,
    pub source_files: Vec<String>,
    pub agent_id: Option<String>,
    pub application_count: u32,
    pub archived: bool,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

pub enum ExperienceType {
    Observation,
    Decision,
    Outcome,
    Lesson,
    Pattern,
    Preference,
}

pub struct NewExperience {
    pub collective_id: CollectiveId,
    pub content: String,
    pub embedding: Option<Embedding>,  // Required if External mode
    pub experience_type: ExperienceType,
    pub importance: f32,
    pub confidence: f32,
    pub domain_tags: Vec<String>,
    pub source_files: Vec<String>,
    pub agent_id: Option<String>,
}

pub struct ExperienceUpdate {
    pub importance: Option<f32>,
    pub confidence: Option<f32>,
    pub domain_tags: Option<Vec<String>>,
}
```

### 5.4 Configuration

```rust
pub struct Config {
    pub embedding_provider: EmbeddingProvider,
    pub embedding_dimension: EmbeddingDimension,
    pub cache_size_mb: usize,
    pub sync_mode: SyncMode,
}

pub enum EmbeddingProvider {
    Builtin { model_path: Option<PathBuf> },
    External,
}

pub enum EmbeddingDimension {
    D384,   // all-MiniLM-L6-v2
    D768,   // bge-base-en-v1.5
    Custom(u16),
}

pub enum SyncMode {
    Fast,    // Async writes
    Normal,  // Sync on commit
    Paranoid, // Sync every write
}
```

### 5.5 Storage Tables (redb)

| Table | Key | Value |
|-------|-----|-------|
| `collectives` | `CollectiveId` | `Collective` |
| `experiences` | `ExperienceId` | `Experience` |
| `exp_by_collective` | `(CollectiveId, Timestamp, ExperienceId)` | `()` |
| `exp_by_type` | `(CollectiveId, ExperienceType, ExperienceId)` | `()` |

---

## 6. API Signatures

### 6.1 Database Lifecycle

```rust
impl PulseDB {
    pub fn open(path: impl AsRef<Path>, config: Config) -> Result<Self, PulseDBError>;
    pub fn close(self) -> Result<(), PulseDBError>;
}
```

### 6.2 Collective Management

```rust
impl PulseDB {
    pub fn create_collective(&self, name: &str) -> Result<CollectiveId, PulseDBError>;
    pub fn create_collective_with_owner(&self, name: &str, owner_id: &str) -> Result<CollectiveId, PulseDBError>;
    pub fn get_collective(&self, id: CollectiveId) -> Result<Option<Collective>, PulseDBError>;
    pub fn list_collectives(&self) -> Result<Vec<Collective>, PulseDBError>;
    pub fn list_collectives_by_owner(&self, owner_id: &str) -> Result<Vec<Collective>, PulseDBError>;
    pub fn get_collective_stats(&self, id: CollectiveId) -> Result<CollectiveStats, PulseDBError>;
    pub fn delete_collective(&self, id: CollectiveId) -> Result<(), PulseDBError>;
}
```

### 6.3 Experience CRUD

```rust
impl PulseDB {
    pub fn record_experience(&self, experience: NewExperience) -> Result<ExperienceId, PulseDBError>;
    pub fn get_experience(&self, id: ExperienceId) -> Result<Option<Experience>, PulseDBError>;
    pub fn update_experience(&self, id: ExperienceId, update: ExperienceUpdate) -> Result<(), PulseDBError>;
    pub fn archive_experience(&self, id: ExperienceId) -> Result<(), PulseDBError>;
    pub fn unarchive_experience(&self, id: ExperienceId) -> Result<(), PulseDBError>;
    pub fn delete_experience(&self, id: ExperienceId) -> Result<(), PulseDBError>;
    pub fn reinforce_experience(&self, id: ExperienceId) -> Result<u32, PulseDBError>;
}
```

### 6.4 Embedding Service

```rust
pub trait EmbeddingService: Send + Sync {
    fn embed(&self, text: &str) -> Result<Embedding, PulseDBError>;
    fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Embedding>, PulseDBError>;
    fn dimension(&self) -> u16;
}

pub struct OnnxEmbedding { /* ... */ }
impl EmbeddingService for OnnxEmbedding { /* ... */ }
```

---

## 7. Error Types

```rust
#[derive(Debug, thiserror::Error)]
pub enum PulseDBError {
    #[error("Storage error: {0}")]
    Storage(#[from] redb::Error),
    
    #[error("Validation error: {0}")]
    Validation(#[from] ValidationError),
    
    #[error("Not found: {entity} with id {id}")]
    NotFound { entity: &'static str, id: String },
    
    #[error("Embedding error: {0}")]
    Embedding(String),
    
    #[error("Configuration error: {0}")]
    Config(String),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error("Field '{field}' is required")]
    Required { field: &'static str },
    
    #[error("Field '{field}' exceeds maximum length of {max}")]
    TooLong { field: &'static str, max: usize },
    
    #[error("Field '{field}' must be between {min} and {max}")]
    OutOfRange { field: &'static str, min: f32, max: f32 },
    
    #[error("Embedding dimension mismatch: expected {expected}, got {actual}")]
    DimensionMismatch { expected: u16, actual: u16 },
    
    #[error("Field '{field}' has too many items: max {max}")]
    TooManyItems { field: &'static str, max: usize },
}
```

---

## 8. Performance Targets

| Operation | Target | Measured At |
|-----------|--------|-------------|
| `PulseDB::open()` | < 100ms | 100K experiences |
| `record_experience()` | < 10ms | Single write |
| `get_experience()` | < 1ms | By ID |
| `list_collectives()` | < 5ms | 100 collectives |
| Embedding generation | < 50ms | Single text |
| Batch embedding (10) | < 200ms | 10 texts |

---

## 9. Security & Validation

### 9.1 Input Validation Rules

| Field | Constraint |
|-------|------------|
| `content` | Non-empty, ≤ 100KB |
| `importance` | 0.0 - 1.0 |
| `confidence` | 0.0 - 1.0 |
| `domain_tags` | ≤ 10 items, ≤ 100 chars each |
| `source_files` | ≤ 10 items, ≤ 500 chars each |
| `collective.name` | Non-empty, ≤ 255 chars |
| `embedding` | Matches collective dimension |

### 9.2 Validation Implementation

```rust
impl NewExperience {
    pub fn validate(&self, expected_dim: u16) -> Result<(), ValidationError> {
        // Content
        if self.content.is_empty() {
            return Err(ValidationError::Required { field: "content" });
        }
        if self.content.len() > 100 * 1024 {
            return Err(ValidationError::TooLong { field: "content", max: 102400 });
        }
        
        // Importance
        if self.importance < 0.0 || self.importance > 1.0 {
            return Err(ValidationError::OutOfRange { 
                field: "importance", min: 0.0, max: 1.0 
            });
        }
        
        // Embedding dimension
        if let Some(ref emb) = self.embedding {
            if emb.len() as u16 != expected_dim {
                return Err(ValidationError::DimensionMismatch {
                    expected: expected_dim,
                    actual: emb.len() as u16,
                });
            }
        }
        
        Ok(())
    }
}
```

---

## 10. Testing Requirements

### 10.1 Unit Tests

```rust
#[cfg(test)]
mod tests {
    // Database lifecycle
    #[test] fn test_open_creates_new_db() { }
    #[test] fn test_open_existing_db() { }
    #[test] fn test_close_flushes_data() { }
    
    // Collective CRUD
    #[test] fn test_create_collective() { }
    #[test] fn test_create_collective_with_owner() { }
    #[test] fn test_list_collectives() { }
    #[test] fn test_delete_collective_cascades() { }
    
    // Experience CRUD
    #[test] fn test_record_experience() { }
    #[test] fn test_get_experience() { }
    #[test] fn test_update_experience() { }
    #[test] fn test_archive_excludes_from_results() { }
    #[test] fn test_reinforce_increments_count() { }
    
    // Validation
    #[test] fn test_empty_content_rejected() { }
    #[test] fn test_importance_range_validated() { }
    #[test] fn test_dimension_mismatch_rejected() { }
    
    // Embedding
    #[test] fn test_builtin_embedding_generates() { }
    #[test] fn test_external_requires_embedding() { }
}
```

### 10.2 Integration Tests

```rust
// tests/experience_lifecycle.rs
#[test]
fn test_full_experience_lifecycle() {
    let db = PulseDB::open(temp_dir(), Config::default())?;
    let collective = db.create_collective("test")?;
    
    // Record
    let id = db.record_experience(NewExperience {
        collective_id: collective,
        content: "Test experience".into(),
        ..Default::default()
    })?;
    
    // Read
    let exp = db.get_experience(id)?.unwrap();
    assert_eq!(exp.content, "Test experience");
    
    // Update
    db.update_experience(id, ExperienceUpdate {
        importance: Some(0.9),
        ..Default::default()
    })?;
    
    // Archive
    db.archive_experience(id)?;
    
    // Delete
    db.delete_experience(id)?;
    assert!(db.get_experience(id)?.is_none());
}
```

---

## 11. Milestones Checklist

### M1: Database Opens (End Week 1)

| Criteria | Status |
|----------|--------|
| `PulseDB::open()` creates new database | ☐ |
| `PulseDB::open()` opens existing database | ☐ |
| `PulseDB::close()` flushes data | ☐ |
| Config validation working | ☐ |
| Basic smoke test passing | ☐ |

### M2: Core Storage (End Week 4)

| Criteria | Status |
|----------|--------|
| Collective CRUD operations | ☐ |
| Experience CRUD operations | ☐ |
| Embedding generation (builtin) | ☐ |
| Embedding validation (external) | ☐ |
| Input validation complete | ☐ |
| Unit tests for all operations | ☐ |

---

## 12. Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| `redb` | 2.0+ | Key-value storage |
| `ort` | 2.0+ | ONNX runtime (optional) |
| `bincode` | 1.3+ | Serialization |
| `uuid` | 1.0+ | ID generation |
| `thiserror` | 1.0+ | Error derivation |
| `tracing` | 0.1+ | Logging |

---

## 13. References

- [03-Architecture.md](../03-Architecture.md) — Full architecture
- [04-DataModel.md](../04-DataModel.md) — Complete data model
- [05-API-Reference.md](../05-API-Reference.md) — Full API docs
- [Phase-2-Substrate.md](./Phase-2-Substrate.md) — Next phase
