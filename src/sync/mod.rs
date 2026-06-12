//! Native sync protocol for distributed PulseDB instances.
//!
//! This module enables synchronizing data between PulseDB instances
//! across a network — PulseDB's evolution from embedded-only to
//! distributed agentic database.
//!
//! # Architecture
//!
//! ```text
//! Desktop (Tauri)                    Server (Axum)
//! ┌──────────────────┐              ┌──────────────────┐
//! │  PulseDB (local) │              │  PulseDB (server)│
//! │  ┌─────────────┐ │   push/pull  │  ┌─────────────┐ │
//! │  │ SyncManager │◄├─────────────►├──│ SyncManager │ │
//! │  │ (background)│ │  HTTP / WS   │  │ (background)│ │
//! │  └─────────────┘ │              │  └─────────────┘ │
//! └──────────────────┘              └──────────────────┘
//! ```
//!
//! # Feature Flags
//!
//! | Feature | Description |
//! |---------|-------------|
//! | `sync` | Core types, transport trait, sync engine, in-memory transport |
//! | `sync-http` | HTTP transport (reqwest) + server helper for Axum consumers |
//! | `sync-websocket` | WebSocket transport (tokio-tungstenite, future) |
//!
//! # Module Overview
//!
//! **Core** (always with `sync` feature):
//! - `types` — Wire types: `SyncChange`, `SyncPayload`, `InstanceId`, `SyncCursor`
//! - `config` — `SyncConfig`, `SyncDirection`, `ConflictResolution`, `RetryConfig`
//! - `error` — `SyncError` enum (Transport, Timeout, ProtocolVersion, etc.)
//! - `transport` — `SyncTransport` pluggable trait
//! - `transport_mem` — `InMemorySyncTransport` for testing
//! - `guard` — `SyncApplyGuard` thread-local echo prevention
//!
//! **Engine**:
//! - `manager` — `SyncManager`: start/stop/sync_once/initial_sync lifecycle
//! - `applier` — `RemoteChangeApplier`: applies remote changes with idempotency
//! - `progress` — `SyncProgressCallback` for initial sync UI feedback
//!
//! **HTTP** (with `sync-http` feature):
//! - `server` — `SyncServer`: framework-agnostic server handler
//! - `transport_http` — `HttpSyncTransport`: reqwest-based client
//!
//! # WAL Compaction
//!
//! The WAL grows unboundedly as entities are created/updated/deleted.
//! Call [`PulseDB::compact_wal()`](crate::PulseDB::compact_wal) periodically
//! to trim events that all peers have already synced. Compaction uses the
//! min-cursor strategy: only events below the oldest peer's cursor are removed.

pub mod applier;
pub mod config;
pub mod error;
pub mod guard;
pub mod manager;
pub mod progress;
pub(crate) mod pusher;
#[cfg(feature = "sync-http")]
#[cfg_attr(docsrs, doc(cfg(feature = "sync-http")))]
pub mod server;
pub mod transport;
#[cfg(feature = "sync-http")]
#[cfg_attr(docsrs, doc(cfg(feature = "sync-http")))]
pub mod transport_http;
pub mod transport_mem;
pub mod types;

/// Sync protocol version.
///
/// Exchanged during handshake to ensure compatibility between peers.
/// Increment when making breaking changes to the wire format.
pub const SYNC_PROTOCOL_VERSION: u32 = 2;

/// Capability advertised by peers that sync reinforcement G-counter fields.
pub const SYNC_CAPABILITY_GCOUNTER_APPLICATIONS: &str = "gcounter-applications";

// Re-exports for ergonomic access
pub use config::SyncConfig;
pub use error::SyncError;
pub use guard::{is_sync_applying, SyncApplyGuard};
pub use manager::SyncManager;
pub use progress::SyncProgressCallback;
#[cfg(feature = "sync-http")]
pub use server::SyncServer;
pub use transport::SyncTransport;
#[cfg(feature = "sync-http")]
pub use transport_http::HttpSyncTransport;
pub use transport_mem::InMemorySyncTransport;
pub use types::{
    HandshakeRequest, HandshakeResponse, InstanceId, PullRequest, PullResponse, PushResponse,
    SerializableExperienceUpdate, SyncChange, SyncCursor, SyncEntityType, SyncPayload, SyncStatus,
};
