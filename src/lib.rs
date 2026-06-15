//! # PulseDB
//!
//! Distributed database for agentic AI systems - collective memory for multi-agent coordination.
//!
//! PulseDB provides persistent storage for AI agent experiences, enabling semantic
//! search, context retrieval, and knowledge sharing between agents. Supports native
//! sync between instances for multi-device and client-server deployments.
//!
//! ## Quick Start
//!
//! ```rust
//! # fn main() -> pulsedb::Result<()> {
//! # let dir = tempfile::tempdir().unwrap();
//! use pulsedb::{PulseDB, Config, NewExperience};
//!
//! // Open or create a database
//! let db = PulseDB::open(dir.path().join("test.db"), Config::default())?;
//!
//! // Create a collective (isolated namespace)
//! let collective = db.create_collective("my-project")?;
//!
//! // Record an experience
//! db.record_experience(NewExperience {
//!     collective_id: collective,
//!     content: "Always validate user input before processing".to_string(),
//!     importance: 0.8,
//!     embedding: Some(vec![0.1f32; 384]),
//!     ..Default::default()
//! })?;
//!
//! // Search for relevant experiences
//! let query_embedding = vec![0.1f32; 384];
//! let results = db.search_similar(collective, &query_embedding, 10)?;
//!
//! // Clean up
//! db.close()?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Key Concepts
//!
//! ### Collective
//!
//! A **collective** is an isolated namespace for experiences, typically one per project.
//! Each collective has its own vector index and can have different embedding dimensions.
//!
//! ### Experience
//!
//! An **experience** is a unit of learned knowledge. It contains:
//! - Content (text description of the experience)
//! - Embedding (vector representation for semantic search)
//! - Metadata (type, importance, confidence, tags)
//!
//! ### Embedding Providers
//!
//! PulseDB supports two modes for embeddings:
//!
//! - **External** (default): You provide pre-computed embeddings from your own service
//!   (OpenAI, Cohere, etc.)
//! - **Builtin**: PulseDB generates embeddings using a bundled ONNX model
//!   (requires `builtin-embeddings` feature)
//!
//! ## Distributed Sync
//!
//! With the `sync` feature, PulseDB instances can synchronize data across a
//! network. See the [`sync`] module for full documentation.
//!
//! Key components:
//! - `SyncManager` — Orchestrates sync lifecycle (start/stop/sync_once)
//! - `SyncTransport` — Pluggable transport trait (HTTP, in-memory, custom)
//! - `SyncServer` — Server-side handler for Axum consumers (`sync-http`)
//! - `PulseDB::compact_wal()` — WAL compaction for disk space reclamation
//!
//! ## Thread Safety
//!
//! `PulseDB` is `Send + Sync` and can be shared across threads using `Arc`.
//! The database uses MVCC for concurrent reads with exclusive write locking.
//!
//! ## Feature Flags
//!
//! | Feature | Description |
//! |---------|-------------|
//! | `builtin-embeddings` | Bundles ONNX runtime with all-MiniLM-L6-v2 for local embedding generation. Without this feature, you must supply pre-computed embeddings. |
//! | `sync` | Core sync protocol: types, transport trait, in-memory transport, echo prevention guard. |
//! | `sync-http` | HTTP sync transport via reqwest (implies `sync`). |
//! | `sync-websocket` | WebSocket sync transport via tokio-tungstenite (implies `sync`). |

#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]
#![warn(rustdoc::missing_crate_level_docs)]
#![deny(unsafe_op_in_unsafe_fn)]

// ============================================================================
// Module declarations
// ============================================================================

mod config;
mod db;
mod error;
mod types;

pub mod embedding;
pub mod storage;

// Domain modules
mod activity;
mod collective;
mod experience;
mod insight;
mod relation;
mod search;
mod watch;

/// SubstrateProvider async trait for agent framework integration.
pub mod substrate;

/// Native sync protocol for distributed PulseDB instances.
///
/// Requires the `sync` feature flag. Provides types, transport trait,
/// and echo prevention for synchronizing data between PulseDB instances.
#[cfg(feature = "sync")]
#[cfg_attr(docsrs, doc(cfg(feature = "sync")))]
pub mod sync;

/// Vector index module for HNSW-based approximate nearest neighbor search.
pub mod vector;

// ============================================================================
// Public API re-exports
// ============================================================================

// Main database interface
pub use db::PulseDB;

// Configuration
pub use config::{
    ActivityConfig, Config, DecayConfig, EmbeddingDimension, EmbeddingProvider, HnswConfig,
    RecallWeights, SyncMode, WatchConfig,
};

// Error handling
pub use error::{NotFoundError, PulseDBError, Result, StorageError, ValidationError};

// Core types
pub use types::{
    AgentId, CollectiveId, Embedding, ExperienceId, InsightId, InstanceId, RelationId, TaskId,
    Timestamp, UserId,
};

// Domain types
pub use collective::{Collective, CollectiveStats};
pub use experience::{
    energy, Experience, ExperienceType, ExperienceUpdate, NewExperience, Severity,
};

// Relations
pub use relation::{ExperienceRelation, NewExperienceRelation, RelationDirection, RelationType};

// Insights
pub use insight::{DerivedInsight, InsightType, NewDerivedInsight};

// Activities
pub use activity::{Activity, NewActivity};

// Search & Context
pub use search::{ContextCandidates, ContextRequest, SearchFilter, SearchOptions, SearchResult};

// Watch (real-time notifications + cross-process change detection)
pub use watch::{ChangePoller, WatchEvent, WatchEventType, WatchFilter, WatchLock, WatchStream};

// Substrate (async agent framework integration)
pub use substrate::{PulseDBSubstrate, SubstrateProvider};

// Storage (for advanced users)
pub use storage::DatabaseMetadata;

// ============================================================================
// Prelude module for convenient imports
// ============================================================================

/// Convenient imports for common PulseDB usage.
///
/// ```rust
/// use pulsedb::prelude::*;
/// ```
pub mod prelude {
    pub use crate::config::{Config, EmbeddingDimension, SyncMode};
    pub use crate::db::PulseDB;
    pub use crate::error::{PulseDBError, Result};
    pub use crate::experience::{Experience, ExperienceType, NewExperience};
    pub use crate::search::{
        ContextCandidates, ContextRequest, SearchFilter, SearchOptions, SearchResult,
    };
    pub use crate::substrate::{PulseDBSubstrate, SubstrateProvider};
    pub use crate::types::{CollectiveId, ExperienceId, Timestamp};
    pub use crate::watch::{ChangePoller, WatchEvent, WatchEventType, WatchFilter, WatchLock};
}
