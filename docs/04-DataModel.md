# PulseDB: Data Model Document

> **Version:** 1.0.0  
> **Status:** Approved  
> **Last Updated:** February 2026  
> **Owner:** PulseDB Team

---

## 1. Overview

This document defines the complete data model for PulseDB, including entity definitions, relationships, storage layout, and invariants.

---

## 2. Entity-Relationship Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        ENTITY-RELATIONSHIP DIAGRAM                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────┐                                                         │
│  │    Collective   │                                                         │
│  │─────────────────│                                                         │
│  │ id (PK)         │                                                         │
│  │ owner_id        │                                                         │
│  │ name            │                                                         │
│  │ embedding_dim   │                                                         │
│  │ created_at      │                                                         │
│  └────────┬────────┘                                                         │
│           │                                                                  │
│           │ 1:N                                                              │
│           │                                                                  │
│           ▼                                                                  │
│  ┌─────────────────┐         ┌─────────────────┐                            │
│  │   Experience    │◄───────►│ ExperienceRelation│                          │
│  │─────────────────│   N:M   │─────────────────│                            │
│  │ id (PK)         │         │ id (PK)         │                            │
│  │ collective_id(FK)│        │ source_id (FK)  │                            │
│  │ content         │         │ target_id (FK)  │                            │
│  │ embedding       │         │ relation_type   │                            │
│  │ experience_type │         │ strength        │                            │
│  │ importance      │         └─────────────────┘                            │
│  │ confidence      │                                                         │
│  │ applications    │                                                         │
│  │ domain[]        │                                                         │
│  │ related_files[] │                                                         │
│  │ source_agent    │                                                         │
│  │ source_task     │                                                         │
│  │ timestamp       │                                                         │
│  │ archived        │                                                         │
│  └────────┬────────┘                                                         │
│           │                                                                  │
│           │ N:M (via source_experiences)                                     │
│           │                                                                  │
│           ▼                                                                  │
│  ┌─────────────────┐                                                         │
│  │ DerivedInsight  │                                                         │
│  │─────────────────│                                                         │
│  │ id (PK)         │                                                         │
│  │ collective_id(FK)│                                                        │
│  │ content         │                                                         │
│  │ embedding       │                                                         │
│  │ source_exp_ids[]│                                                         │
│  │ confidence      │                                                         │
│  │ domain[]        │                                                         │
│  │ created_at      │                                                         │
│  └─────────────────┘                                                         │
│                                                                              │
│  ┌─────────────────┐                                                         │
│  │    Activity     │                                                         │
│  │─────────────────│                                                         │
│  │ agent_id (PK)   │                                                         │
│  │ collective_id(FK)│                                                        │
│  │ task_description│                                                         │
│  │ working_on[]    │                                                         │
│  │ started_at      │                                                         │
│  │ last_heartbeat  │                                                         │
│  └─────────────────┘                                                         │
│                                                                              │
│  POST-MVP ENTITIES (dotted lines):                                           │
│  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ┐                                                        │
│      Wisdom                                                                  │
│  │─────────────────│                                                         │
│   id (PK)                                                                    │
│  │ owner_id        │                                                         │
│   category                                                                   │
│  │ pattern         │                                                         │
│   confidence                                                                 │
│  │ embedding       │                                                         │
│   source_collectives[]                                                       │
│  └ ─ ─ ─ ─ ─ ─ ─ ─ ┘                                                        │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Entity Definitions

### 3.1 Collective

The root entity representing an isolated hive mind (typically one per project).

```rust
pub struct Collective {
    /// Unique identifier (UUID v7 for time-ordering)
    pub id: CollectiveId,
    
    /// Owner identifier (opaque, provided by consumer)
    /// PulseDB does NOT authenticate - just uses for filtering
    pub owner_id: UserId,
    
    /// Human-readable name
    pub name: String,
    
    /// Embedding dimension (locked on creation)
    /// All experiences in this collective must use this dimension
    pub embedding_dimension: usize,
    
    /// Statistics (computed on read, not stored)
    pub experience_count: u64,
    pub active_agents: u32,
    
    /// Timestamps
    pub created_at: Timestamp,
}

/// Opaque user identifier
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UserId(pub String);

/// Collective identifier (UUID v7)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CollectiveId(pub Uuid);
```

**Constraints:**
| Field | Constraint |
|-------|------------|
| id | Unique, immutable |
| owner_id | Non-empty string, max 256 chars |
| name | Non-empty, max 256 chars |
| embedding_dimension | 1-4096, immutable after creation |

---

### 3.2 Experience

The core entity representing a unit of learning recorded by an agent.

```rust
pub struct Experience {
    /// Unique identifier (UUID v7)
    pub id: ExperienceId,
    
    /// Parent collective
    pub collective_id: CollectiveId,
    
    // ─────────────────────────────────────────────────────────────
    // Content
    // ─────────────────────────────────────────────────────────────
    
    /// The learning content
    pub content: String,
    
    /// Semantic embedding vector
    /// Dimension must match collective.embedding_dimension
    pub embedding: Vec<f32>,
    
    /// Structured type information
    pub experience_type: ExperienceType,
    
    // ─────────────────────────────────────────────────────────────
    // Relevance Signals
    // ─────────────────────────────────────────────────────────────
    
    /// Importance score (0.0 - 1.0)
    /// Raw value, consumer computes decay
    pub importance: f32,
    
    /// Confidence score (0.0 - 1.0)
    /// How reliable is this learning
    pub confidence: f32,
    
    /// Application count
    /// Times this experience helped other agents
    pub applications: u32,
    
    // ─────────────────────────────────────────────────────────────
    // Context for Retrieval
    // ─────────────────────────────────────────────────────────────
    
    /// Domain tags for filtering
    pub domain: Vec<String>,
    
    /// Related file paths
    pub related_files: Vec<String>,
    
    // ─────────────────────────────────────────────────────────────
    // Provenance
    // ─────────────────────────────────────────────────────────────
    
    /// Agent that created this experience
    pub source_agent: AgentId,
    
    /// Task context (optional)
    pub source_task: Option<TaskId>,
    
    /// Creation timestamp
    pub timestamp: Timestamp,
    
    // ─────────────────────────────────────────────────────────────
    // Lifecycle
    // ─────────────────────────────────────────────────────────────
    
    /// Soft-delete flag
    /// Archived experiences are excluded from search
    pub archived: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExperienceId(pub Uuid);

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AgentId(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskId(pub String);
```

**Constraints:**
| Field | Constraint |
|-------|------------|
| id | Unique, immutable |
| collective_id | Must exist, immutable |
| content | Non-empty, max 100KB |
| embedding | Length == collective.embedding_dimension |
| importance | 0.0 - 1.0 |
| confidence | 0.0 - 1.0 |
| applications | >= 0 |
| domain | Each tag max 100 chars, max 50 tags |
| related_files | Each path max 500 chars, max 100 files |
| source_agent | Non-empty, max 256 chars |
| timestamp | Immutable after creation |

---

### 3.3 ExperienceType

Enumeration of experience categories with structured data.

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ExperienceType {
    // ─────────────────────────────────────────────────────────────
    // Problems and Solutions
    // ─────────────────────────────────────────────────────────────
    
    Difficulty {
        description: String,
        severity: Severity,
    },
    
    Solution {
        problem_ref: Option<ExperienceId>,
        approach: String,
        worked: bool,
    },
    
    // ─────────────────────────────────────────────────────────────
    // Patterns
    // ─────────────────────────────────────────────────────────────
    
    ErrorPattern {
        signature: String,
        fix: String,
        prevention: String,
    },
    
    SuccessPattern {
        task_type: String,
        approach: String,
        quality: f32,
    },
    
    // ─────────────────────────────────────────────────────────────
    // Preferences and Decisions
    // ─────────────────────────────────────────────────────────────
    
    UserPreference {
        category: String,
        preference: String,
        strength: f32,
    },
    
    ArchitecturalDecision {
        decision: String,
        rationale: String,
    },
    
    // ─────────────────────────────────────────────────────────────
    // Knowledge
    // ─────────────────────────────────────────────────────────────
    
    TechInsight {
        technology: String,
        insight: String,
    },
    
    Fact {
        statement: String,
        source: String,
    },
    
    // ─────────────────────────────────────────────────────────────
    // Generic
    // ─────────────────────────────────────────────────────────────
    
    /// Catch-all for unstructured experiences
    Generic {
        category: Option<String>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Severity {
    Low,
    Medium,
    High,
    Critical,
}
```

---

### 3.4 ExperienceRelation

Represents a relationship between two experiences.

```rust
pub struct ExperienceRelation {
    /// Unique identifier
    pub id: RelationId,
    
    /// Source experience (from)
    pub source_id: ExperienceId,
    
    /// Target experience (to)
    pub target_id: ExperienceId,
    
    /// Relationship type
    pub relation_type: RelationType,
    
    /// Relationship strength (0.0 - 1.0)
    pub strength: f32,
    
    /// Optional metadata (JSON for extensibility)
    pub metadata: Option<String>,
    
    /// Creation timestamp
    pub created_at: Timestamp,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RelationId(pub Uuid);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RelationType {
    /// Source supports/confirms target
    Supports,
    
    /// Source contradicts target
    Contradicts,
    
    /// Source adds detail to target
    Elaborates,
    
    /// Source replaces/updates target
    Supersedes,
    
    /// Source implies target should apply
    Implies,
    
    /// General semantic relationship
    RelatedTo,
}
```

**Constraints:**
| Field | Constraint |
|-------|------------|
| id | Unique, immutable |
| source_id | Must exist |
| target_id | Must exist, != source_id |
| strength | 0.0 - 1.0 |
| metadata | Max 10KB if present |
| (source_id, target_id, relation_type) | Unique combination |

---

### 3.5 DerivedInsight

Represents a synthesized insight from multiple experiences (computed by consumer, stored by PulseDB).

```rust
pub struct DerivedInsight {
    /// Unique identifier
    pub id: InsightId,
    
    /// Parent collective
    pub collective_id: CollectiveId,
    
    /// Synthesized content
    pub content: String,
    
    /// Semantic embedding
    pub embedding: Vec<f32>,
    
    /// Source experiences this was derived from
    pub source_experiences: Vec<ExperienceId>,
    
    /// Confidence score (0.0 - 1.0)
    pub confidence: f32,
    
    /// Domain tags
    pub domain: Vec<String>,
    
    /// Creation timestamp
    pub created_at: Timestamp,
    
    /// Last updated timestamp
    pub updated_at: Timestamp,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InsightId(pub Uuid);
```

**Constraints:**
| Field | Constraint |
|-------|------------|
| id | Unique, immutable |
| collective_id | Must exist |
| content | Non-empty, max 50KB |
| embedding | Length == collective.embedding_dimension |
| source_experiences | All must exist, max 100 |
| confidence | 0.0 - 1.0 |

---

### 3.6 Activity

Represents an agent's current activity for real-time awareness.

```rust
pub struct Activity {
    /// Agent identifier (unique per collective)
    pub agent_id: AgentId,
    
    /// Parent collective
    pub collective_id: CollectiveId,
    
    /// Current task description
    pub task_description: String,
    
    /// Files/entities being worked on
    pub working_on: Vec<String>,
    
    /// Activity start time
    pub started_at: Timestamp,
    
    /// Last heartbeat (for stale detection)
    pub last_heartbeat: Timestamp,
}
```

**Constraints:**
| Field | Constraint |
|-------|------------|
| (agent_id, collective_id) | Unique combination (PK) |
| task_description | Max 1KB |
| working_on | Max 100 items, each max 500 chars |
| Stale threshold | Configurable, default 5 minutes |

---

### 3.7 Wisdom (Post-MVP)

Represents abstracted cross-collective knowledge.

```rust
pub struct Wisdom {
    /// Unique identifier
    pub id: WisdomId,
    
    /// Owner (user, not collective)
    pub owner_id: UserId,
    
    /// Category of wisdom
    pub category: WisdomCategory,
    
    /// The abstracted pattern
    pub pattern: String,
    
    /// Confidence score (computed by consumer, stored here)
    pub confidence: f32,
    
    /// Semantic embedding
    pub embedding: Vec<f32>,
    
    /// Source collectives (provenance, not access)
    pub source_collectives: Vec<CollectiveId>,
    
    /// Number of source experiences
    pub source_experience_count: u32,
    
    /// Applicable domains
    pub applicable_domains: Vec<String>,
    
    /// Timestamps
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WisdomId(pub Uuid);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WisdomCategory {
    CodingStyle,
    ArchitecturalPreference,
    TechStackPreference,
    QualityStandard,
    WorkflowPreference,
}
```

---

## 4. Physical Storage Layout

### 4.1 redb Tables

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            REDB TABLES                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  TABLE: collectives                                                          │
│  ─────────────────                                                           │
│  Key:   CollectiveId (16 bytes)                                              │
│  Value: Collective (bincode serialized)                                      │
│  Index: None (small table, scan OK)                                          │
│                                                                              │
│  TABLE: experiences                                                          │
│  ────────────────                                                            │
│  Key:   ExperienceId (16 bytes)                                              │
│  Value: Experience (bincode serialized, embedding separate)                  │
│  Index: experiences_by_collective                                            │
│                                                                              │
│  TABLE: experiences_by_collective                                            │
│  ───────────────────────────────                                             │
│  Key:   (CollectiveId, Timestamp, ExperienceId) - 40 bytes                  │
│  Value: () - empty, used for range scans by time                            │
│  Purpose: Efficient "get recent experiences for collective"                  │
│                                                                              │
│  TABLE: embeddings                                                           │
│  ───────────────                                                             │
│  Key:   ExperienceId (16 bytes)                                              │
│  Value: Vec<f32> (raw bytes, 384*4=1536 or 768*4=3072 bytes)                │
│  Purpose: Separate table for large embeddings                                │
│                                                                              │
│  TABLE: relations                                                            │
│  ───────────────                                                             │
│  Key:   RelationId (16 bytes)                                                │
│  Value: ExperienceRelation (bincode serialized)                              │
│  Index: relations_by_source, relations_by_target                             │
│                                                                              │
│  TABLE: relations_by_source                                                  │
│  ─────────────────────────                                                   │
│  Key:   (ExperienceId, RelationId) - 32 bytes                               │
│  Value: () - empty                                                           │
│  Purpose: Find outgoing relations for an experience                          │
│                                                                              │
│  TABLE: relations_by_target                                                  │
│  ─────────────────────────                                                   │
│  Key:   (ExperienceId, RelationId) - 32 bytes                               │
│  Value: () - empty                                                           │
│  Purpose: Find incoming relations for an experience                          │
│                                                                              │
│  TABLE: insights                                                             │
│  ──────────────                                                              │
│  Key:   InsightId (16 bytes)                                                 │
│  Value: DerivedInsight (bincode serialized)                                  │
│  Index: insights_by_collective                                               │
│                                                                              │
│  TABLE: insights_by_collective                                               │
│  ───────────────────────────                                                 │
│  Key:   (CollectiveId, InsightId) - 32 bytes                                │
│  Value: () - empty                                                           │
│                                                                              │
│  TABLE: activities                                                           │
│  ────────────────                                                            │
│  Key:   (CollectiveId, AgentId) - variable                                  │
│  Value: Activity (bincode serialized)                                        │
│  Purpose: One active activity per agent per collective                       │
│                                                                              │
│  TABLE: metadata                                                             │
│  ──────────────                                                              │
│  Key:   String                                                               │
│  Value: Bytes                                                                │
│  Contents:                                                                   │
│    - "schema_version": u32                                                   │
│    - "created_at": Timestamp                                                 │
│    - "wal_sequence": u64                                                     │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Key Encoding

```rust
/// Compound key encoding for experiences_by_collective
fn encode_experience_index_key(
    collective_id: CollectiveId,
    timestamp: Timestamp,
    experience_id: ExperienceId,
) -> [u8; 40] {
    let mut key = [0u8; 40];
    key[0..16].copy_from_slice(collective_id.0.as_bytes());
    key[16..24].copy_from_slice(&timestamp.to_be_bytes());  // Big-endian for ordering
    key[24..40].copy_from_slice(experience_id.0.as_bytes());
    key
}

/// Prefix for range scans
fn collective_prefix(collective_id: CollectiveId) -> [u8; 16] {
    let mut prefix = [0u8; 16];
    prefix.copy_from_slice(collective_id.0.as_bytes());
    prefix
}
```

### 4.3 HNSW Index Files

```
pulse.db.hnsw/
├── collective_{uuid}.hnsw       # HNSW index for collective
├── collective_{uuid}.hnsw.meta  # Index metadata (count, config)
└── ...
```

**Index Metadata:**
```rust
struct HnswMeta {
    collective_id: CollectiveId,
    element_count: u64,
    dimension: usize,
    m: usize,
    ef_construction: usize,
    created_at: Timestamp,
    last_modified: Timestamp,
}
```

---

## 5. Serialization

### 5.1 Format Selection

| Data Type | Format | Size | Rationale |
|-----------|--------|------|-----------|
| Experience | bincode | ~200-500 bytes + content | Fast, compact |
| Embedding | raw f32 | dim * 4 bytes | Zero overhead |
| Relation | bincode | ~100 bytes | Consistent |
| Insight | bincode | ~200-500 bytes + content | Consistent |
| Activity | bincode | ~200 bytes | Consistent |
| Metadata | JSON | Variable | Human-readable |

### 5.2 Serialization Examples

```rust
use bincode::{serialize, deserialize};

// Serialize experience (without embedding)
let exp_bytes = serialize(&experience)?;

// Serialize embedding separately
let emb_bytes: &[u8] = bytemuck::cast_slice(&embedding);

// Deserialize
let experience: Experience = deserialize(&exp_bytes)?;
let embedding: Vec<f32> = bytemuck::cast_slice(emb_bytes).to_vec();
```

---

## 6. Size Estimates

### 6.1 Per-Entity Sizes

| Entity | Fixed Fields | Variable Fields | Typical Total |
|--------|--------------|-----------------|---------------|
| Collective | 50 bytes | name ~50 bytes | ~100 bytes |
| Experience | 100 bytes | content ~500 bytes | ~600 bytes |
| Embedding (384d) | - | 1,536 bytes | 1,536 bytes |
| Embedding (768d) | - | 3,072 bytes | 3,072 bytes |
| Relation | 80 bytes | metadata ~100 bytes | ~180 bytes |
| Insight | 100 bytes | content ~500 bytes | ~600 bytes |
| Activity | 80 bytes | task ~200 bytes | ~280 bytes |

### 6.2 Scale Projections

| Scale | Experiences | Estimated Size (384d) | Estimated Size (768d) |
|-------|-------------|----------------------|----------------------|
| Small | 10K | ~20 MB | ~35 MB |
| Medium | 100K | ~200 MB | ~350 MB |
| Large | 1M | ~2 GB | ~3.5 GB |
| XLarge | 10M | ~20 GB | ~35 GB |

---

## 7. Invariants

### 7.1 Referential Integrity

| Constraint | Enforcement |
|------------|-------------|
| Experience.collective_id must exist | Validated on insert |
| Relation.source_id must exist | Validated on insert |
| Relation.target_id must exist | Validated on insert |
| Insight.source_experiences must exist | Validated on insert |
| Activity.collective_id must exist | Validated on insert |

### 7.2 Uniqueness Constraints

| Constraint | Scope |
|------------|-------|
| CollectiveId | Global |
| ExperienceId | Global |
| RelationId | Global |
| InsightId | Global |
| (source_id, target_id, relation_type) | Per-relation |
| (collective_id, agent_id) | Per-activity |

### 7.3 Consistency Rules

```
1. Embedding Dimension Lock
   ∀ experience ∈ collective:
     experience.embedding.len() == collective.embedding_dimension

2. No Self-Relations
   ∀ relation:
     relation.source_id ≠ relation.target_id

3. Same-Collective Relations
   ∀ relation:
     experience(relation.source_id).collective_id ==
     experience(relation.target_id).collective_id

4. Bounded Scores
   ∀ entity with importance, confidence, strength:
     0.0 ≤ score ≤ 1.0

5. Archived Exclusion
   ∀ search operation:
     archived == false (unless explicitly included)
```

---

## 8. Migration Strategy

### 8.1 Schema Versioning

```rust
const SCHEMA_VERSION: u32 = 1;

fn check_schema_version(db: &Database) -> Result<()> {
    let stored_version = db.get_metadata("schema_version")?;
    
    match stored_version.cmp(&SCHEMA_VERSION) {
        Ordering::Equal => Ok(()),
        Ordering::Less => migrate_schema(db, stored_version, SCHEMA_VERSION),
        Ordering::Greater => Err(PulseDBError::IncompatibleVersion {
            stored: stored_version,
            expected: SCHEMA_VERSION,
        }),
    }
}
```

### 8.2 Migration Approach

| Version Change | Strategy |
|----------------|----------|
| Add new field with default | Lazy migration (deserialize old, add default) |
| Add new table | Create on first access |
| Remove field | Ignore in deserialization |
| Change field type | Full migration with backup |
| Change key encoding | Full migration with backup |

### 8.3 Backup Before Migration

```rust
fn backup_database(path: &Path) -> Result<PathBuf> {
    let backup_path = path.with_extension("db.backup");
    std::fs::copy(path, &backup_path)?;
    Ok(backup_path)
}
```

---

## 9. Query Patterns

### 9.1 Common Queries

| Query | Tables Accessed | Expected Latency |
|-------|-----------------|------------------|
| Get experience by ID | experiences, embeddings | < 1ms |
| Get recent experiences | experiences_by_collective, experiences | < 10ms |
| Search similar (k=20) | HNSW index, experiences | < 50ms |
| Get context candidates | All of the above | < 100ms |
| Get related experiences | relations_by_source, experiences | < 10ms |
| List active agents | activities | < 5ms |

### 9.2 Query Examples

```rust
// Get recent experiences for collective
fn get_recent(collective_id: CollectiveId, limit: usize) -> Result<Vec<Experience>> {
    let prefix = collective_prefix(collective_id);
    let iter = db.table::<experiences_by_collective>()
        .range(prefix..)?
        .rev()  // Newest first (timestamp in key)
        .take(limit);
    
    let mut results = Vec::with_capacity(limit);
    for (key, _) in iter {
        let exp_id = ExperienceId::from_bytes(&key[24..40]);
        if let Some(exp) = db.table::<experiences>().get(exp_id)? {
            if !exp.archived {
                results.push(exp);
            }
        }
    }
    Ok(results)
}

// Get outgoing relations
fn get_outgoing_relations(exp_id: ExperienceId) -> Result<Vec<ExperienceRelation>> {
    let prefix = exp_id.as_bytes();
    let iter = db.table::<relations_by_source>()
        .range(prefix..)?;
    
    let mut results = Vec::new();
    for (key, _) in iter {
        if !key.starts_with(prefix) { break; }
        let rel_id = RelationId::from_bytes(&key[16..32]);
        if let Some(rel) = db.table::<relations>().get(rel_id)? {
            results.push(rel);
        }
    }
    Ok(results)
}
```

---

## 10. References

- [02-SRS.md](./02-SRS.md) — Software Requirements
- [03-Architecture.md](./03-Architecture.md) — Architecture
- [SPEC.md](../SPEC.md) — Technical Specification

---

## Changelog

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0.0 | February 2026 | PulseDB Team | Initial data model document |
