# PulseDB: Architecture Document

> **Version:** 1.0.0  
> **Status:** Approved  
> **Last Updated:** February 2026  
> **Owner:** PulseDB Team

---

## 1. Overview

This document describes the internal architecture of PulseDB, an embedded database for agentic AI systems. It covers system components, data flow, concurrency model, and extension points.

### 1.1 Architectural Goals

| Goal | Description |
|------|-------------|
| **Embedded** | Single binary, no external server dependencies |
| **Fast** | Sub-100ms context retrieval at scale |
| **Safe** | Rust's memory safety, ACID transactions |
| **Simple** | Clear mental model, predictable behavior |
| **Extensible** | SubstrateProvider trait for integration |

### 1.2 Architectural Principles

1. **Storage, Not Intelligence** — PulseDB stores what consumers compute. No inference, synthesis, or decay algorithms.
2. **Single-Writer, Multi-Reader** — Simplifies concurrency, matches redb semantics.
3. **Sync Core, Async Edges** — Synchronous internals, async only for streams.
4. **Fail Fast** — Return errors early, never silently corrupt.

---

## 2. System Context

### 2.1 Context Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              SYSTEM CONTEXT                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐        │
│  │    PulseHive    │     │  Custom Agents  │     │   RAG Systems   │        │
│  │  (Primary User) │     │   (Direct API)  │     │  (Vector+Graph) │        │
│  └────────┬────────┘     └────────┬────────┘     └────────┬────────┘        │
│           │                       │                       │                  │
│           │   SubstrateProvider   │   PulseDB API         │   PulseDB API   │
│           │                       │                       │                  │
│           └───────────────────────┼───────────────────────┘                  │
│                                   │                                          │
│                                   ▼                                          │
│                    ┌──────────────────────────────┐                          │
│                    │           PulseDB            │                          │
│                    │   Embedded Agentic Database  │                          │
│                    └──────────────┬───────────────┘                          │
│                                   │                                          │
│                    ┌──────────────┴───────────────┐                          │
│                    │                              │                          │
│                    ▼                              ▼                          │
│           ┌────────────────┐            ┌────────────────┐                   │
│           │   File System  │            │  ONNX Runtime  │                   │
│           │  (redb, HNSW)  │            │  (Optional)    │                   │
│           └────────────────┘            └────────────────┘                   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Deployment Model

```
┌─────────────────────────────────────────────────────────────────┐
│                     DEPLOYMENT OPTIONS                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Option A: In-Process (Primary)                                  │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │  Application Process                                         ││
│  │  ┌─────────────────┐  ┌─────────────────────────────────┐   ││
│  │  │  Your Agent     │  │        PulseDB Library          │   ││
│  │  │  Application    │──│  (linked via Cargo dependency)  │   ││
│  │  └─────────────────┘  └─────────────────────────────────┘   ││
│  └─────────────────────────────────────────────────────────────┘│
│                                                                  │
│  Option B: Multi-Process (Shared Database)                       │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │  Process A              Process B              Process C     ││
│  │  ┌──────────┐          ┌──────────┐          ┌──────────┐   ││
│  │  │ PulseDB  │          │ PulseDB  │          │ PulseDB  │   ││
│  │  │ (Writer) │          │ (Reader) │          │ (Reader) │   ││
│  │  └────┬─────┘          └────┬─────┘          └────┬─────┘   ││
│  │       │                     │                     │          ││
│  │       └─────────────────────┼─────────────────────┘          ││
│  │                             │                                ││
│  │                      ┌──────▼──────┐                         ││
│  │                      │  pulse.db   │                         ││
│  │                      │  (shared)   │                         ││
│  │                      └─────────────┘                         ││
│  └─────────────────────────────────────────────────────────────┘│
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. Component Architecture

### 3.1 High-Level Components

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          PULSEDB COMPONENTS                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                           PUBLIC API LAYER                              │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌────────────┐  │ │
│  │  │  PulseDB     │  │  Collective  │  │  Experience  │  │  Substrate │  │ │
│  │  │  (Entry)     │  │  Manager     │  │  Manager     │  │  Provider  │  │ │
│  │  └──────────────┘  └──────────────┘  └──────────────┘  └────────────┘  │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                      │                                       │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                           CORE SERVICES                                 │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌────────────┐  │ │
│  │  │  Query       │  │  Watch       │  │  Embedding   │  │  Activity  │  │ │
│  │  │  Engine      │  │  System      │  │  Service     │  │  Tracker   │  │ │
│  │  └──────────────┘  └──────────────┘  └──────────────┘  └────────────┘  │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                      │                                       │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                           STORAGE LAYER                                 │ │
│  │  ┌──────────────────────────────┐  ┌──────────────────────────────┐    │ │
│  │  │      KV Store (redb)         │  │    Vector Index (HNSW)       │    │ │
│  │  │  ┌─────────┐  ┌───────────┐  │  │  ┌─────────┐  ┌───────────┐  │    │ │
│  │  │  │ Tables  │  │ Txn Mgr   │  │  │  │ Index   │  │ Search    │  │    │ │
│  │  │  └─────────┘  └───────────┘  │  │  └─────────┘  └───────────┘  │    │ │
│  │  └──────────────────────────────┘  └──────────────────────────────┘    │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Component Descriptions

#### 3.2.1 Public API Layer

| Component | Responsibility |
|-----------|---------------|
| **PulseDB** | Entry point, lifecycle management, configuration |
| **CollectiveManager** | Create/list/delete collectives, stats |
| **ExperienceManager** | CRUD operations for experiences |
| **SubstrateProvider** | Trait implementation for PulseHive |

#### 3.2.2 Core Services

| Component | Responsibility |
|-----------|---------------|
| **QueryEngine** | Context candidates assembly, filtering, search |
| **WatchSystem** | Real-time notifications via crossbeam channels |
| **EmbeddingService** | ONNX embedding generation (optional) |
| **ActivityTracker** | Agent activity registration and heartbeats |

#### 3.2.3 Storage Layer

| Component | Responsibility |
|-----------|---------------|
| **KVStore** | redb wrapper for structured data storage |
| **VectorIndex** | HNSW index management via hnsw_rs (pure Rust) |

### 3.3 Component Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        COMPONENT INTERACTIONS                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│                              ┌─────────────┐                                 │
│                              │   PulseDB   │                                 │
│                              │   (entry)   │                                 │
│                              └──────┬──────┘                                 │
│                                     │                                        │
│               ┌─────────────────────┼─────────────────────┐                  │
│               │                     │                     │                  │
│               ▼                     ▼                     ▼                  │
│       ┌───────────────┐     ┌───────────────┐     ┌───────────────┐         │
│       │  Collective   │     │  Experience   │     │   Activity    │         │
│       │   Manager     │     │   Manager     │     │   Tracker     │         │
│       └───────┬───────┘     └───────┬───────┘     └───────┬───────┘         │
│               │                     │                     │                  │
│               │              ┌──────┴──────┐              │                  │
│               │              │             │              │                  │
│               │              ▼             ▼              │                  │
│               │      ┌─────────────┐ ┌──────────┐        │                  │
│               │      │   Query     │ │  Watch   │        │                  │
│               │      │   Engine    │ │  System  │        │                  │
│               │      └──────┬──────┘ └────┬─────┘        │                  │
│               │             │             │               │                  │
│               │      ┌──────┴─────────────┴──────┐       │                  │
│               │      │                           │       │                  │
│               ▼      ▼                           ▼       ▼                  │
│       ┌───────────────────────┐       ┌───────────────────────┐             │
│       │      KV Store         │       │    Vector Index       │             │
│       │       (redb)          │◄─────►│      (HNSW)           │             │
│       └───────────────────────┘       └───────────────────────┘             │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Data Flow

### 4.1 Write Path: Record Experience

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     WRITE PATH: record_experience()                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Consumer                                                                    │
│     │                                                                        │
│     │  1. record_experience(NewExperience)                                   │
│     ▼                                                                        │
│  ┌──────────────────┐                                                        │
│  │  ExperienceManager│                                                       │
│  └────────┬─────────┘                                                        │
│           │                                                                  │
│           │  2. Validate collective exists                                   │
│           │  3. Generate ExperienceId                                        │
│           ▼                                                                  │
│  ┌──────────────────┐  4. Generate embedding (if Builtin)                   │
│  │ EmbeddingService │─────────────────────────────────────┐                 │
│  └────────┬─────────┘                                     │                 │
│           │                                               │                 │
│           │  5. Validate embedding dimension              │                 │
│           ▼                                               ▼                 │
│  ┌──────────────────┐                           ┌──────────────────┐        │
│  │    KV Store      │  6. Store experience      │   Vector Index   │        │
│  │     (redb)       │◄─────────────────────────►│     (HNSW)       │        │
│  └────────┬─────────┘  7. Add to index          └──────────────────┘        │
│           │                                                                  │
│           │  8. Begin write transaction                                      │
│           │  9. Insert to experiences table                                  │
│           │  10. Commit transaction                                          │
│           ▼                                                                  │
│  ┌──────────────────┐                                                        │
│  │   Watch System   │  11. Notify watchers                                   │
│  └────────┬─────────┘                                                        │
│           │                                                                  │
│           │  12. Return ExperienceId                                         │
│           ▼                                                                  │
│  Consumer                                                                    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Read Path: Get Context Candidates

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                  READ PATH: get_context_candidates()                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Consumer                                                                    │
│     │                                                                        │
│     │  1. get_context_candidates(request)                                    │
│     ▼                                                                        │
│  ┌──────────────────┐                                                        │
│  │   Query Engine   │                                                        │
│  └────────┬─────────┘                                                        │
│           │                                                                  │
│           ├──────────────────────────────────────────────────────┐           │
│           │                                                      │           │
│           ▼                                                      ▼           │
│  ┌──────────────────┐                               ┌──────────────────┐    │
│  │   Vector Index   │  2. HNSW search               │    KV Store      │    │
│  │     (HNSW)       │     (k=max_similar)           │     (redb)       │    │
│  └────────┬─────────┘                               └────────┬─────────┘    │
│           │                                                  │              │
│           │  3. Return (id, score) pairs                     │              │
│           ▼                                                  │              │
│  ┌──────────────────┐                                        │              │
│  │   Query Engine   │◄───────────────────────────────────────┘              │
│  └────────┬─────────┘  4. Fetch experience records                          │
│           │            5. Fetch recent experiences                          │
│           │            6. Fetch stored insights                             │
│           │            7. Fetch active agents                               │
│           │            8. Fetch relations                                   │
│           │                                                                  │
│           │  9. Apply filters (domain, importance)                          │
│           │  10. Assemble ContextCandidates                                 │
│           ▼                                                                  │
│  Consumer ◄─── 11. Return ContextCandidates                                 │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4.3 Search Path: search_similar()

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      SEARCH PATH: search_similar()                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  1. Consumer calls search_similar(collective_id, query_embedding, k)         │
│                                                                              │
│  2. Query Engine validates:                                                  │
│     - Collective exists                                                      │
│     - Embedding dimension matches                                            │
│                                                                              │
│  3. Vector Index performs HNSW search:                                       │
│     ┌─────────────────────────────────────────────────────────────────┐     │
│     │  HNSW Algorithm                                                  │     │
│     │  ├── Start at entry point (layer L)                             │     │
│     │  ├── Greedy search to find closest node at each layer           │     │
│     │  ├── Descend to layer L-1, repeat                               │     │
│     │  ├── At layer 0, collect k nearest neighbors                    │     │
│     │  └── Return (node_id, distance) pairs                           │     │
│     └─────────────────────────────────────────────────────────────────┘     │
│                                                                              │
│  4. Query Engine fetches full Experience records from KV Store              │
│                                                                              │
│  5. Apply post-filters (archived=false, domain match, etc.)                 │
│                                                                              │
│  6. Return Vec<(Experience, f32)> sorted by similarity                      │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 5. Storage Architecture

### 5.1 File Layout

```
project_database/
├── pulse.db              # Main redb database (all structured data)
├── pulse.db.lock         # Lock file for cross-process coordination
├── pulse.db.hnsw/        # HNSW index directory
│   ├── collective_abc123.hnsw    # Per-collective HNSW index
│   ├── collective_def456.hnsw
│   └── ...
└── pulse.db.kv           # KV cache file (post-MVP, optional)
```

### 5.2 redb Table Schema

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           REDB TABLE SCHEMA                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Table: collectives                                                          │
│  Key:   CollectiveId (16 bytes, UUID)                                        │
│  Value: Collective (serialized)                                              │
│                                                                              │
│  Table: experiences                                                          │
│  Key:   ExperienceId (16 bytes, UUID)                                        │
│  Value: Experience (serialized)                                              │
│                                                                              │
│  Table: experiences_by_collective                                            │
│  Key:   (CollectiveId, Timestamp, ExperienceId) - compound key              │
│  Value: () - empty, used for range scans                                    │
│                                                                              │
│  Table: relations                                                            │
│  Key:   RelationId (16 bytes, UUID)                                          │
│  Value: ExperienceRelation (serialized)                                      │
│                                                                              │
│  Table: relations_by_source                                                  │
│  Key:   (ExperienceId, RelationId) - compound key                           │
│  Value: () - empty, for lookups by source                                   │
│                                                                              │
│  Table: relations_by_target                                                  │
│  Key:   (ExperienceId, RelationId) - compound key                           │
│  Value: () - empty, for lookups by target                                   │
│                                                                              │
│  Table: insights                                                             │
│  Key:   InsightId (16 bytes, UUID)                                           │
│  Value: DerivedInsight (serialized)                                          │
│                                                                              │
│  Table: activities                                                           │
│  Key:   (CollectiveId, AgentId) - compound key                              │
│  Value: Activity (serialized)                                                │
│                                                                              │
│  Table: metadata                                                             │
│  Key:   String (e.g., "schema_version", "created_at")                       │
│  Value: Bytes                                                                │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 5.3 HNSW Index Configuration

```rust
pub struct HnswConfig {
    /// Max connections per node (higher = better recall, more memory)
    pub m: usize,                    // Default: 16
    
    /// Size of dynamic candidate list during construction
    pub ef_construction: usize,      // Default: 200
    
    /// Size of dynamic candidate list during search
    pub ef_search: usize,            // Default: 50
    
    /// Distance metric
    pub metric: DistanceMetric,      // Default: Cosine
}

pub enum DistanceMetric {
    Cosine,      // Default, normalized vectors
    Euclidean,   // L2 distance
    InnerProduct, // Dot product (for pre-normalized)
}
```

### 5.4 Serialization

| Data Type | Format | Rationale |
|-----------|--------|-----------|
| Experiences | bincode | Fast, compact, Rust-native |
| Relations | bincode | Consistent with experiences |
| Insights | bincode | Consistent with experiences |
| Embeddings | raw f32 | No overhead, HNSW expects raw |
| Metadata | JSON | Human-readable for debugging |

---

## 6. Concurrency Model

### 6.1 Single-Writer, Multi-Reader (SWMR)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         CONCURRENCY MODEL                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  WITHIN SINGLE PROCESS                                                       │
│  ─────────────────────                                                       │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                          PulseDB Instance                            │    │
│  │                                                                      │    │
│  │  Writer Thread              Reader Threads (unlimited)               │    │
│  │  ┌──────────────┐          ┌──────────┐ ┌──────────┐ ┌──────────┐   │    │
│  │  │ Write Txn    │          │ Read Txn │ │ Read Txn │ │ Read Txn │   │    │
│  │  │ (exclusive)  │          │ (MVCC)   │ │ (MVCC)   │ │ (MVCC)   │   │    │
│  │  └──────────────┘          └──────────┘ └──────────┘ └──────────┘   │    │
│  │         │                        │           │           │          │    │
│  │         │                        └───────────┴───────────┘          │    │
│  │         ▼                                    │                      │    │
│  │  ┌─────────────────────────────────────────────────────────────┐   │    │
│  │  │                        redb                                  │   │    │
│  │  │  • Write transaction holds exclusive lock                    │   │    │
│  │  │  • Read transactions see consistent snapshot (MVCC)          │   │    │
│  │  │  • No read blocking during writes                            │   │    │
│  │  └─────────────────────────────────────────────────────────────┘   │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  ACROSS MULTIPLE PROCESSES                                                   │
│  ─────────────────────────                                                   │
│                                                                              │
│  Process A (Writer)          Process B (Reader)        Process C (Reader)   │
│  ┌──────────────────┐       ┌──────────────────┐      ┌──────────────────┐  │
│  │ Holds file lock  │       │ MVCC read view   │      │ MVCC read view   │  │
│  │ during writes    │       │ (may be stale)   │      │ (may be stale)   │  │
│  └────────┬─────────┘       └────────┬─────────┘      └────────┬─────────┘  │
│           │                          │                         │            │
│           └──────────────────────────┼─────────────────────────┘            │
│                                      │                                      │
│                               ┌──────▼──────┐                               │
│                               │  pulse.db   │                               │
│                               │  (shared)   │                               │
│                               └─────────────┘                               │
│                                                                              │
│  Cross-process coordination:                                                 │
│  • File lock (pulse.db.lock) serializes writers                             │
│  • Readers poll for changes (configurable interval)                         │
│  • WAL sequence number for change detection                                 │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 6.2 Transaction Semantics

| Operation | Isolation Level | Blocking |
|-----------|-----------------|----------|
| Read single experience | Snapshot | Non-blocking |
| Read multiple (scan) | Snapshot | Non-blocking |
| Write single experience | Serializable | Blocks other writes |
| Write batch | Serializable | Blocks other writes |

### 6.3 Lock Hierarchy

```
1. Database lock (file-level)
   └── 2. Write transaction lock (redb)
       └── 3. HNSW index lock (per-collective)
```

**Deadlock Prevention**: Always acquire locks in order. Never acquire database lock while holding HNSW lock.

---

## 7. Watch System

### 7.1 In-Process Notifications

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      IN-PROCESS WATCH SYSTEM                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                          Watch Registry                              │    │
│  │                                                                      │    │
│  │  HashMap<CollectiveId, Vec<Sender<Experience>>>                      │    │
│  │                                                                      │    │
│  │  collective_abc123 ──► [Sender1, Sender2, Sender3]                  │    │
│  │  collective_def456 ──► [Sender4]                                    │    │
│  │                                                                      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  On record_experience():                                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  1. Write experience to storage                                      │    │
│  │  2. Look up senders for collective                                   │    │
│  │  3. For each sender: sender.try_send(experience.clone())            │    │
│  │     - Non-blocking (try_send)                                        │    │
│  │     - If channel full, log warning but don't block                   │    │
│  │  4. Return ExperienceId                                              │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Subscriber side:                                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  let stream = db.watch_experiences(collective_id).await;            │    │
│  │  while let Some(exp) = stream.next().await {                        │    │
│  │      // Process new experience                                       │    │
│  │  }                                                                   │    │
│  │  // Stream ends when dropped, removes sender from registry           │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Implementation: crossbeam-channel (bounded, lock-free)                      │
│  Latency: < 100ns                                                           │
│  Buffer: Configurable (default: 1000 messages)                              │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 7.2 Cross-Process Notifications

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    CROSS-PROCESS WATCH (WAL Tailing)                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Writer Process                       Reader Process                         │
│  ┌───────────────────────┐           ┌───────────────────────┐              │
│  │  1. Write experience   │           │  Polling loop:        │              │
│  │  2. Increment WAL seq  │           │  1. Read current seq  │              │
│  │  3. Commit             │           │  2. If seq > last_seen│              │
│  └───────────┬────────────┘           │     fetch new records │              │
│              │                        │  3. Sleep(poll_interval)│            │
│              ▼                        └───────────┬───────────┘              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                          pulse.db                                    │    │
│  │                                                                      │    │
│  │  metadata table: { "wal_sequence": 12345 }                          │    │
│  │                                                                      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Poll interval: Configurable (default: 100ms)                               │
│  Trade-off: Lower interval = fresher data, higher CPU                       │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 8. Embedding Service

### 8.1 Embedding Provider Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        EMBEDDING SERVICE                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                      EmbeddingService                                │    │
│  │                                                                      │    │
│  │  pub fn embed(&self, text: &str) -> Result<Vec<f32>>                │    │
│  │  pub fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>>│    │
│  │                                                                      │    │
│  └───────────────────────────────┬─────────────────────────────────────┘    │
│                                  │                                          │
│                    ┌─────────────┴─────────────┐                            │
│                    │                           │                            │
│                    ▼                           ▼                            │
│  ┌─────────────────────────────┐ ┌─────────────────────────────┐           │
│  │      Builtin Provider       │ │     External Provider       │           │
│  │                             │ │                             │           │
│  │  • ONNX Runtime             │ │  • Validate dimension only  │           │
│  │  • all-MiniLM-L6-v2 (384d)  │ │  • Consumer provides vectors│           │
│  │  • bge-base-en-v1.5 (768d)  │ │  • Supports any model       │           │
│  │  • Zero runtime deps        │ │                             │           │
│  │                             │ │                             │           │
│  └─────────────────────────────┘ └─────────────────────────────┘           │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 8.2 ONNX Model Loading

```rust
// Lazy loading on first embed() call
pub struct OnnxEmbedder {
    session: OnceCell<ort::Session>,
    model_path: PathBuf,
    dimension: usize,
}

impl OnnxEmbedder {
    fn get_session(&self) -> Result<&ort::Session> {
        self.session.get_or_try_init(|| {
            ort::Session::builder()?
                .with_optimization_level(GraphOptimizationLevel::Level3)?
                .with_intra_threads(1)?
                .commit_from_file(&self.model_path)
        })
    }
}
```

---

## 9. Error Handling

### 9.1 Error Hierarchy

```rust
#[derive(Debug, thiserror::Error)]
pub enum PulseDBError {
    // Storage errors
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
    
    // Validation errors
    #[error("Validation error: {0}")]
    Validation(#[from] ValidationError),
    
    // Not found errors
    #[error("Not found: {0}")]
    NotFound(NotFoundError),
    
    // Concurrency errors
    #[error("Concurrency error: {0}")]
    Concurrency(#[from] ConcurrencyError),
    
    // Embedding errors
    #[error("Embedding error: {0}")]
    Embedding(#[from] EmbeddingError),
}

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("Database corrupted: {0}")]
    Corrupted(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Transaction failed: {0}")]
    Transaction(String),
}

#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error("Embedding dimension mismatch: expected {expected}, got {got}")]
    DimensionMismatch { expected: usize, got: usize },
    #[error("Invalid {field}: {reason}")]
    InvalidField { field: String, reason: String },
    #[error("Content too large: {size} bytes (max: {max})")]
    ContentTooLarge { size: usize, max: usize },
}
```

### 9.2 Error Handling Strategy

| Error Type | Recovery Strategy |
|------------|-------------------|
| NotFound | Return `None` or empty result |
| Validation | Return error, no state change |
| Storage | Return error, transaction rolled back |
| Corruption | Return error, suggest recovery |
| Concurrency | Retry with backoff (caller) |

---

## 10. Extension Points

### 10.1 SubstrateProvider Trait

```rust
/// Trait for PulseHive integration
/// Defined in PulseDB, implemented by PulseDB, re-exported by PulseHive
#[async_trait]
pub trait SubstrateProvider: Send + Sync {
    // Core operations
    async fn store_experience(&self, exp: Experience) -> Result<ExperienceId>;
    async fn get_experience(&self, id: ExperienceId) -> Result<Option<Experience>>;
    async fn search_similar(&self, collective: CollectiveId, embedding: &[f32], k: usize) 
        -> Result<Vec<(Experience, f32)>>;
    
    // Relationship operations
    async fn store_relation(&self, rel: ExperienceRelation) -> Result<RelationId>;
    async fn get_related(&self, exp_id: ExperienceId) -> Result<Vec<(Experience, ExperienceRelation)>>;
    
    // Insight operations
    async fn store_insight(&self, insight: DerivedInsight) -> Result<InsightId>;
    async fn get_insights(&self, collective: CollectiveId, embedding: &[f32], k: usize) 
        -> Result<Vec<DerivedInsight>>;
    
    // Activity operations
    async fn get_activities(&self, collective: CollectiveId) -> Result<Vec<Activity>>;
    
    // Watch operations
    async fn watch(&self, collective: CollectiveId) -> Result<impl Stream<Item = Experience>>;
}
```

### 10.2 Custom Storage Backends (Future)

```rust
/// Internal trait for storage abstraction (not public in MVP)
trait StorageBackend {
    fn get(&self, table: &str, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn put(&self, table: &str, key: &[u8], value: &[u8]) -> Result<()>;
    fn delete(&self, table: &str, key: &[u8]) -> Result<()>;
    fn scan_prefix(&self, table: &str, prefix: &[u8]) -> Result<impl Iterator<Item = (Vec<u8>, Vec<u8>)>>;
}

// MVP: Only RedbBackend
// Future: Could add RocksDB, SQLite, etc.
```

---

## 11. Security Considerations

### 11.1 Trust Boundaries

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          TRUST BOUNDARIES                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  TRUSTED (PulseDB responsibility):                                          │
│  ├── Data integrity (no corruption)                                         │
│  ├── Isolation between collectives                                          │
│  ├── Embedding dimension validation                                         │
│  └── Input size limits                                                       │
│                                                                              │
│  UNTRUSTED (Consumer responsibility):                                        │
│  ├── Authentication (who is calling)                                        │
│  ├── Authorization (can they access this collective)                        │
│  ├── User ID management                                                      │
│  ├── Content sanitization                                                    │
│  └── Rate limiting                                                           │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 11.2 Data Protection

| Threat | Mitigation |
|--------|------------|
| Unauthorized file access | File system permissions (consumer) |
| Data tampering | Checksums in redb |
| Resource exhaustion | Content size limits, configurable |
| SQL injection | N/A (no SQL) |
| Cross-collective leakage | CollectiveId in all queries |

---

## 12. Deployment Considerations

### 12.1 Build Configuration

```toml
[features]
default = ["builtin-embeddings"]
builtin-embeddings = ["ort"]
full = ["builtin-embeddings"]

[dependencies]
redb = "2.0"
hnsw_rs = "0.3"  # Pure Rust HNSW (see ADR-005)
ort = { version = "2.0", optional = true }
```

### 12.2 Binary Size Optimization

| Configuration | Size | Trade-off |
|---------------|------|-----------|
| `default` (with ONNX) | ~18MB | Full functionality |
| Without `builtin-embeddings` | ~5MB | Consumer provides embeddings |
| Release + LTO | -20% | Longer build time |

---

## 13. References

- [01-PRD.md](./01-PRD.md) — Product Requirements
- [02-SRS.md](./02-SRS.md) — Software Requirements
- [04-DataModel.md](./04-DataModel.md) — Data Model
- [SPEC.md](../SPEC.md) — Technical Specification

---

## Changelog

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0.0 | February 2026 | PulseDB Team | Initial architecture document |
