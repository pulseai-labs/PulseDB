# PulseDB

An embedded database purpose-built for agentic AI systems.

[![CI](https://github.com/pulsehive/pulsedb/actions/workflows/ci.yml/badge.svg)](https://github.com/pulsehive/pulsedb/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/pulsehive-db)](https://crates.io/crates/pulsehive-db)
[![docs.rs](https://docs.rs/pulsehive-db/badge.svg)](https://docs.rs/pulsehive-db)
[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL--3.0-blue.svg)](LICENSE)
[![MSRV](https://img.shields.io/badge/MSRV-1.89-blue)](Cargo.toml)

**Collective memory for AI agents.** Not message passing. Not RAG. A purpose-built embedded database for multi-agent coordination.

PulseDB gives your AI agents persistent, shared memory. Record what agents learn, search by semantic similarity, track relationships between experiences, and get notified in real-time when knowledge changes — all from a single embedded database with zero external dependencies.

## Why PulseDB?

- **Experience-native** — Not just vectors. Experiences carry importance, confidence, domain tags, typed variants (insights, errors, patterns, decisions), and relationships to other experiences.
- **Embedded** — No server, no Docker, no network. A single Rust crate that compiles into your binary. Open a file, start storing.
- **Real-time** — Watch streams notify agents of new experiences as they happen (<100ns overhead). No polling.
- **Context-aware** — One API call assembles context from similar experiences, recent activity, insights, relations, and active agents. Not just "find the nearest vector."
- **Fast** — Sub-millisecond reads, <6ms writes, <100us vector search at 1K experiences. Built on redb (ACID) + HNSW (approximate nearest neighbor).

## Quick Start

```rust
use pulsedb::{PulseDB, Config, NewExperience};

// Open or create a database
let db = PulseDB::open("my-agents.db", Config::default())?;

// Create a collective (isolated namespace for your project)
let collective = db.create_collective("my-project")?;

// Record an experience
db.record_experience(NewExperience {
    collective_id: collective,
    content: "Always validate user input before processing".to_string(),
    importance: 0.8,
    embedding: Some(vec![0.1f32; 384]),
    ..Default::default()
})?;

// Search for semantically similar experiences
let query = vec![0.1f32; 384];
let results = db.search_similar(collective, &query, 10)?;
for result in &results {
    println!("[{:.3}] {}", result.similarity, result.experience.content);
}

db.close()?;
```

## Installation

```toml
[dependencies]
pulsehive-db = "0.3"
```

With built-in embedding generation:

```toml
[dependencies]
pulsehive-db = { version = "0.3", features = ["builtin-embeddings"] }
```

With distributed sync (HTTP transport):

```toml
[dependencies]
pulsehive-db = { version = "0.3", features = ["sync-http"] }
```

> **Note:** The crate is published as `pulsehive-db` on crates.io but imported as `use pulsedb::...` in Rust code.

## Features

- **Experience storage** — Record, retrieve, update, archive, and delete agent experiences with full CRUD operations
- **Temporal lifecycle** — Experiences accrue/decay energy over time; `list_cold_experiences()` surfaces coldest-first prune-eligible candidates for human/agent-triggered review (read-only — auto-archive is OFF by default)
- **Vector search** — HNSW approximate nearest neighbor search for semantic similarity (384-dimensional embeddings by default)
- **Knowledge graph** — Typed relations between experiences (Supports, Contradicts, Elaborates, Supersedes, Implies, RelatedTo)
- **Real-time watch** — In-process notification streams via crossbeam channels and cross-process change detection via WAL sequence tracking
- **Context assembly** — Single `get_context_candidates()` call retrieves similar experiences, recent activity, insights, relations, and active agents
- **Derived insights** — Store synthesized knowledge with source experience tracking
- **Activity tracking** — Monitor which agents are active with heartbeat and staleness detection
- **Optional ONNX embeddings** — Built-in all-MiniLM-L6-v2 (384d) with automatic model download (`builtin-embeddings` feature)
- **ACID transactions** — redb-backed storage with crash safety via shadow paging
- **Async integration** — `SubstrateProvider` trait with `tokio::spawn_blocking` wrappers for async agent frameworks
- **Distributed sync** — Native sync protocol for multi-instance PulseDB (push/pull/bidirectional, HTTP transport, conflict resolution)

## Distributed Sync

PulseDB instances can sync knowledge across a network — a desktop PulseDB syncing with a server-side PulseDB. Requires the `sync` feature.

```text
Desktop (Tauri)                    Server (Axum)
┌──────────────────┐              ┌──────────────────┐
│  PulseDB (local) │              │  PulseDB (server)│
│  ┌─────────────┐ │   push/pull  │  ┌─────────────┐ │
│  │ SyncManager │◄├─────────────►├──│ SyncServer  │ │
│  │ (background)│ │  HTTP/bincode│  │ (Axum)      │ │
│  └─────────────┘ │              │  └─────────────┘ │
└──────────────────┘              └──────────────────┘
```

**Feature flags:** `sync` (core engine), `sync-http` (HTTP transport + server helper)

**Key capabilities:**
- Background push/pull loops with configurable intervals
- Conflict resolution: `ServerWins` or `LastWriteWins`
- Echo prevention (synced changes don't loop back)
- WAL compaction to reclaim disk space
- Initial sync catchup with progress callback
- Pluggable transport trait (HTTP, in-memory for testing, custom)

## Performance

Measured on Apple Silicon (M-series), single-threaded:

| Operation | 1K experiences | Target (100K) |
|-----------|---------------|---------------|
| `record_experience` | 5.5 ms | < 10 ms |
| `search_similar` (k=20) | 95 us | < 50 ms |
| `get_context_candidates` | 189 us | < 100 ms |
| `get_experience` by ID | 1.3 us | — |

Run benchmarks yourself: `cargo bench`

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    CONSUMER APPLICATIONS                     │
│         (Agent Frameworks, Custom AI Systems, RAG)           │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌────────────────────────────────────────────────────────┐  │
│  │                  PulseDB Public API                     │  │
│  │  record_experience()  search_similar()  watch()        │  │
│  │  create_collective()  store_relation()  store_insight() │  │
│  │  get_context_candidates()  get_active_agents()         │  │
│  └───────────────────────┬────────────────────────────────┘  │
│                          │                                    │
│  ┌───────────────────────┼────────────────────────────────┐  │
│  │                  PULSEDB CORE                           │  │
│  │                       │                                 │  │
│  │  ┌──────────┐  ┌─────┴───────┐  ┌───────────────────┐  │  │
│  │  │Embedding │  │Query Engine │  │  Watch System     │  │  │
│  │  │Provider  │  │(context)    │  │  (crossbeam)      │  │  │
│  │  └──────────┘  └─────────────┘  └───────────────────┘  │  │
│  │       │               │                  │              │  │
│  │  ┌────┴───────────────┴──────────────────┴───────────┐  │  │
│  │  │                Storage Layer                       │  │  │
│  │  │  ┌──────────┐              ┌────────────────────┐  │  │  │
│  │  │  │   redb   │              │   HNSW Index      │  │  │  │
│  │  │  │(KV store)│              │   (hnsw_rs)       │  │  │  │
│  │  │  └──────────┘              └────────────────────┘  │  │  │
│  │  └────────────────────────────────────────────────────┘  │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

## Comparison

| Feature | PulseDB | pgvector | sqlite-vss | Qdrant | ChromaDB | LanceDB |
|---------|---------|----------|------------|--------|----------|---------|
| Embedded (no server) | Yes | No | Yes | No | No | Yes |
| Experience-native model | Yes | No | No | No | No | No |
| Vector search | Yes | Yes | Yes | Yes | Yes | Yes |
| Knowledge graph | Yes | No | No | No | No | No |
| Real-time watch | Yes | No | No | No | No | No |
| Context assembly | Yes | No | No | No | No | No |
| ACID transactions | Yes | Yes | Yes | No | No | No |
| Native sync protocol | Yes | No | No | No | No | No |
| Language | Rust | SQL | C/SQL | Rust | Python | Rust |

## Key Concepts

### Collective

A **collective** is an isolated namespace for experiences, typically one per project. Each collective has its own vector index and can have different embedding dimensions.

### Experience

An **experience** is a unit of learned knowledge. It contains content (text), an embedding (vector), importance and confidence scores, domain tags, and a typed variant — `TechInsight`, `ErrorPattern`, `SuccessPattern`, `ArchitecturalDecision`, and more.

### Temporal lifecycle

Each experience has a temporal **energy** that decays over time and is boosted only by an explicit `reinforce_experience()` call — reads (`get_experience`, `search`, `list_cold_experiences`) do **not** reinforce, so a memory keeps decaying unless a consumer reinforces it. `list_cold_experiences()` surfaces prune-eligible candidates (energy below a threshold, not yet archived), coldest-first, as lightweight `(id, energy)` pairs for a human/agent to review:

```rust
let collective = db.create_collective("my-project")?;
// Surface up to 100 candidates with energy < 0.05, coldest-first (read-only):
for (id, energy) in db.list_cold_experiences(collective, 0.05, 100)? {
    println!("cold candidate {id} @ energy {energy}");
}
```

It is a **read-only review tool** — it never archives anything. Auto-archive is **OFF by default** (`auto_archive_below_floor` is inert; no automatic prune trigger is wired).

## Minimum Supported Rust Version

The current MSRV is **1.89**. This is verified in CI on every commit.

## Contributing

Contributions are welcome! Please open an issue to discuss your idea before submitting a pull request.

## Documentation

- [API Reference (docs.rs)](https://docs.rs/pulsehive-db)
- [CHANGELOG](CHANGELOG.md)

## License

PulseDB is dual-licensed:

- **Open Source**: [GNU Affero General Public License v3.0 (AGPL-3.0)](LICENSE) — free for open-source projects and internal use. If you modify PulseDB and offer it as a network service, you must release your source code under AGPL-3.0.
- **Commercial License**: For proprietary use without AGPL obligations, contact us for a commercial license.

This ensures PulseDB remains free for the community while protecting against unauthorized commercial hosting.
