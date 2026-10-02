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
//! - `types` — Wire types: `SyncChange`, `SyncPayload`, `SyncExperience`, `PushRequest`, `PullRequest`, `WireReply` (+ the persisted `SyncCursor` record)
//! - `wire` — versioned frame codec: operation framing, exact frame sizing, bounded encode/decode
//! - `config` — `SyncConfig`, `SyncDirection`, `ConflictResolution`, `RetryConfig`
//! - `error` — `SyncError` enum (Transport, Timeout, ProtocolVersion, etc.)
//! - `transport` — `SyncTransport` pluggable trait
//! - `transport_mem` — `InMemorySyncTransport` for testing
//! - `server` — `SyncServer`: framework-agnostic server handler (no web framework dependency, so it is available wherever `sync` is)
//! - `guard` — `SyncApplyGuard` thread-local echo prevention
//!
//! **Engine**:
//! - `manager` — `SyncManager`: start/stop/sync_once/initial_sync lifecycle
//! - `applier` — `RemoteChangeApplier`: applies remote changes with idempotency
//! - `progress` — `SyncProgressCallback` and the shared cursor-advance policy
//!
//! **HTTP** (with `sync-http` feature):
//! - `transport_http` — `HttpSyncTransport`: reqwest-based client
//!
//! # WAL Compaction
//!
//! The WAL grows unboundedly as entities are created/updated/deleted.
//! Call [`PulseDB::compact_wal()`](crate::PulseDB::compact_wal) periodically
//! to trim events that all peers have already synced. Compaction uses the
//! min-push-position strategy: only events every peer has acknowledged receiving
//! are removed (pull positions never feed compaction — issue #9).
//!
//! # Wire hygiene
//!
//! The sync server is the network edge of PulseDB's trust boundary (ADR-009),
//! and PulseDB builds no router of its own — so the edge checks below are the
//! ones it owns, and a consumer's framework limits stack on top of them.
//!
//! **Framing (protocol v5).** Every body — handshake, push and pull, request
//! and reply — carries the [`SYNC_WIRE_PREAMBLE_LEN`]-byte frame header, and
//! every byte-level handler validates it by raw byte-slicing before any decode:
//! byte cap, then magic, then [`WIRE_FORMAT_VERSION`], then the operation
//! discriminator, then an exact postcard decode that refuses trailing bytes.
//! Route and metadata validation runs next, and only then may anything be
//! applied or any cursor persisted. A protocol-v4 body is unframed on the data
//! endpoints and is refused as [`SyncError::WireFormatMismatch`]; v5 offers no
//! fallback.
//!
//! **Request byte cap (#26, #98).** Every byte-level server handler
//! (`SyncServer::handle_{handshake,push,pull}_bytes`) compares `bytes.len()`
//! against [`SyncConfig::max_request_bytes`] (default 64 MiB) **before** the
//! frame header is read and before any postcard decode. An oversized body is
//! refused with the typed [`SyncError::PayloadTooLarge`]`{ size, max }` — never
//! a decode error, never a partial decode; [`SyncError::is_payload_too_large`]
//! is the hook for a `413 Payload Too Large` mapping. The HTTP transport client
//! applies the same cap to response bodies (a `Content-Length` above the cap is
//! refused unread, a chunked body is read bounded).
//!
//! Senders no longer *guess* whether a body will fit. The estimated
//! per-experience byte floor that `SyncConfig::validate` used to impose is
//! gone: both packers size the **complete candidate frame** with pinned
//! postcard's own `serialized_size` ([`wire::encoded_len`]) and send the
//! longest ordered prefix that fits the effective cap — `min(local policy, peer
//! inbound limit)` on a push, `min(request limit, server policy)` on a pull
//! reply. `SyncConfig::batch_size` stays a ceiling on the batch's **count**; it
//! makes no claim about encoded bytes. A single change that cannot fit a body
//! on its own is a deterministic dead end, so it is reported as the typed
//! [`SyncError::ChangeTooLarge`] with its cursor unadvanced, and the background
//! loop stops retrying it instead of rebuilding the same refused body forever.
//!
//! **Send and receive budgets are different numbers.**
//! [`SyncTransport::receive_limit_bytes`](crate::sync::SyncTransport::receive_limit_bytes) is inbound only — the body this
//! transport will read — and nothing about it bounds what a peer will accept.
//! The outbound direction arrives per call as `send_budget_bytes` on
//! `handshake`, `push_changes` and `pull_changes`: the cap the request's own
//! frame is encoded against, supplied by whoever packed it, so the encode cap
//! and the packing cap are the same value by construction. It is a local
//! parameter and is never serialized; a budget field inside a request would
//! change that request's own encoded length. Each producer supplies what the
//! reader on the other side will actually take — `min(local policy, peer
//! inbound limit)` for a push, `min(local policy, the bound peer's inbound
//! limit)` for a pull, and local policy alone for a handshake, which has no
//! binding yet and therefore rests on peer conformance: a bounded control frame
//! fits [`wire::MIN_CONTROL_FRAME_BYTES`](crate::sync::wire::MIN_CONTROL_FRAME_BYTES), which every conforming v5 server
//! enforces as its own floor at construction. A request that exceeds its send
//! budget is refused locally as the typed [`SyncError::RequestTooLarge`](crate::sync::SyncError::RequestTooLarge),
//! before transmission — deterministic and terminal like
//! [`SyncError::ChangeTooLarge`](crate::sync::SyncError::ChangeTooLarge), and distinct from it, because no WAL sequence
//! is at fault and no cursor is involved. An oversized body arriving from the
//! wire keeps [`SyncError::PayloadTooLarge`](crate::sync::SyncError::PayloadTooLarge); the outbound mapping never
//! reclassifies it.
//!
//! **What the byte cap does and does not bound.** It bounds encoded request and
//! reply bodies, and the accumulation of a bounded response. It does **not**
//! bound every decoded object, nor WAL and payload allocations behind the
//! decode, and it is not a 64 MiB ceiling on the process. A framework body
//! limit still has to be installed upstream before `Bytes` is buffered — see
//! the example adapter in `tests/sync_http.rs`; a byte handler that is handed
//! an already-buffered body cannot un-allocate it.
//!
//! **Protocol version and capabilities (#12).** The handshake carries a
//! `protocol_version` that is *checked*: a mismatch reaches the client as the
//! typed [`SyncError::ProtocolVersion`]`{ local, remote }`, never as a reason
//! string inside `SyncError::Handshake`. The handshake's capability list, by
//! contrast, is **informational only** — see
//! [`SYNC_CAPABILITY_GCOUNTER_APPLICATIONS`]. Peers advertise capabilities;
//! nothing is negotiated from them and nothing is refused because of them.
//!
//! **Reinforcement clock skew (#13).** An incoming `last_reinforced` beyond
//! `now + `[`SyncConfig::max_clock_skew_ms`] (default 5 minutes) is logged at
//! `warn` with the peer, the experience id and the skew, and counted in the
//! local-only [`SyncStats::skewed_timestamps`] (`SyncManager::stats()`,
//! `SyncServer::stats()`). It is **never** clamped, rejected or re-timestamped:
//! FR-031's max-merge stores the value byte-for-byte, so convergence is
//! untouched. The bound stays **advisory**: correcting a skewed value needs a
//! record-level time reference to converge on, and protocol v5 deliberately did
//! not add one — that work was kept out of this repair and is assigned to a
//! later protocol version.

pub mod applier;
pub mod config;
pub mod error;
pub mod guard;
pub mod manager;
pub mod progress;
pub(crate) mod pusher;
pub mod server;
pub mod transport;
#[cfg(feature = "sync-http")]
#[cfg_attr(docsrs, doc(cfg(feature = "sync-http")))]
pub mod transport_http;
pub mod transport_mem;
pub mod types;
pub mod wire;

/// Sync protocol version.
///
/// Exchanged during handshake to ensure compatibility between peers.
/// Increment when making breaking changes to the wire format.
///
/// Bumped 2 → 3 in VS-4.0.3 (bincode→postcard serializer swap + wire preamble).
/// Bumped 3 → 4 in VS-4.3.2 (key-value `tags` field on `Experience` +
/// `SerializableExperienceUpdate`).
/// Bumped 4 → 5 in 0.8.0 (this repair): wire-carried embeddings, routed
/// requests, responder-named replies and exact body budgets.
///
/// # v5 does not interoperate with v4
///
/// There is no compatibility fallback and no legacy-size accommodation: both
/// replicas upgrade. A v4 peer's bodies are unframed on the push and pull
/// endpoints, so they are refused by the frame header before any decode
/// ([`SyncError::WireFormatMismatch`]); a v4 handshake carries the v3 wire
/// version byte and is refused the same way. A v5 client sees a typed
/// incompatibility ([`SyncError::is_protocol_incompatible`]). Nothing here
/// promises an *old* client understands a *new* error type — a v4 peer
/// predates all of them.
pub const SYNC_PROTOCOL_VERSION: u32 = 5;

/// Capability advertised by peers that sync reinforcement G-counter fields.
///
/// The handshake capability list is **informational and not negotiated**: a
/// peer advertises what it speaks so that operators and logs can see it, but
/// no capability is required, matched, or used to refuse a handshake, and no
/// behaviour is switched on its presence. Compatibility is decided solely by
/// [`SYNC_PROTOCOL_VERSION`] (checked, typed) and the wire preamble
/// ([`WIRE_FORMAT_VERSION`], checked, typed). A capability-driven
/// negotiation would be a protocol change.
pub const SYNC_CAPABILITY_GCOUNTER_APPLICATIONS: &str = "gcounter-applications";

// ============================================================================
// Wire-format frame header (serializer-independent fail-loud — VS-4.0.3 / C5,
// extended to every operation in protocol v5)
// ============================================================================
//
// EVERY sync body — handshake, push and pull, request AND reply — is framed
// with a fixed-layout 4-byte header parsed by *raw byte-slicing* BEFORE any
// deserialize:
//
//     [ SYNC_WIRE_MAGIC[0], SYNC_WIRE_MAGIC[1], wire_format_version, operation ] ++ <body>
//
// Protocol v4 framed only the handshake, on the reasoning that push and pull
// were reached only after a handshake had pinned the version. That reasoning
// does not hold at a network edge: PulseDB builds no router, the byte handlers
// are individually addressable, and a stateless direct push must not bypass the
// checks a handshake would have made. So under v5 the header is on every body,
// and it carries an operation discriminator as well — a push body delivered to
// the pull endpoint is refused before the decoder sees it.
//
// The codec that reads and writes these frames lives in [`wire`].

/// Fixed magic bytes leading every sync wire frame.
///
/// Two distinctive non-ASCII bytes (`0xFE 0xED`, "feed") chosen to be unlikely
/// to collide with the first bytes of a serialized body: postcard frames a
/// `HandshakeRequest` starting with the 16-byte `InstanceId`, whose leading
/// byte is effectively random but very rarely `0xFE`, and a `0xFE 0xED` pair is
/// rarer still — so the magic cheaply catches "this isn't a PulseDB sync frame
/// at all" (a protocol-v4 peer's unframed push body, say) before any version
/// check.
pub const SYNC_WIRE_MAGIC: [u8; 2] = [0xFE, 0xED];

/// Current wire-format version carried in the frame header.
///
/// Moves in lockstep with [`SYNC_PROTOCOL_VERSION`]; a mismatch here is caught
/// pre-deserialize and surfaced as [`error::SyncError::WireFormatMismatch`].
/// Bumped 3 → 4 with protocol v5: the header grew the operation byte, so a v4
/// frame is not a v5 frame even where the body would have decoded.
pub const WIRE_FORMAT_VERSION: u8 = 4;

/// Length in bytes of the wire frame header
/// (`magic[2] ++ wire_format_version[1] ++ operation[1]`).
pub const SYNC_WIRE_PREAMBLE_LEN: usize = SYNC_WIRE_MAGIC.len() + 2;

/// Prepends the frame header for `operation` to a serialized `body`.
///
/// Thin wrapper over [`wire::write_header`], kept as the name the transports
/// and server have always used. Every body goes through it under v5 — there is
/// no unframed leg left.
pub fn write_wire_preamble(operation: wire::WireOperation, body: &[u8]) -> Vec<u8> {
    let mut framed = Vec::with_capacity(SYNC_WIRE_PREAMBLE_LEN + body.len());
    framed.extend_from_slice(&wire::write_header(operation));
    framed.extend_from_slice(body);
    framed
}

/// Parses + validates the frame header by **raw byte-slicing of
/// `framed[..4]`** and returns the post-header body slice on success.
///
/// This MUST run BEFORE any `deserialize(...)` — that ordering is the whole
/// point of the fail-loud design (C5): a serializer or version mismatch is
/// caught here as a typed [`error::SyncError::WireFormatMismatch`], and a
/// misrouted body as [`error::SyncError::WireOperationMismatch`], never as a
/// generic decode error.
///
/// # Errors
/// - [`error::SyncError::WireFormatMismatch`] with `got: None` when the body is
///   shorter than the header or the magic bytes don't match — which is what a
///   protocol-v4 unframed push or pull body looks like.
/// - [`error::SyncError::WireFormatMismatch`] with `got: Some(v)` when the magic
///   matches but the `wire_format_version` byte is not [`WIRE_FORMAT_VERSION`].
/// - [`error::SyncError::WireOperationMismatch`] when the frame is well-formed
///   but names a different operation than this endpoint serves.
pub fn read_wire_preamble(
    operation: wire::WireOperation,
    framed: &[u8],
) -> Result<&[u8], error::SyncError> {
    wire::read_header(operation, framed)
}

// Re-exports for ergonomic access
pub use config::SyncConfig;
pub use error::SyncError;
pub use guard::{is_sync_applying, SyncApplyGuard};
pub use manager::SyncManager;
pub use progress::SyncProgressCallback;
pub use server::SyncServer;
pub use transport::SyncTransport;
#[cfg(feature = "sync-http")]
pub use transport_http::HttpSyncTransport;
pub use transport_mem::InMemorySyncTransport;
pub use types::{
    HandshakeRequest, HandshakeResponse, InstanceId, PullPage, PullRequest, PushAck, PushRequest,
    SerializableExperienceUpdate, SyncChange, SyncCursor, SyncEntityType, SyncExperience,
    SyncPayload, SyncPosition, SyncStats, SyncStatus, WireErrorCode, WireReply, WireResult,
};
pub use wire::{WireOperation, MIN_CONTROL_FRAME_BYTES};
