//! Pluggable transport trait for the sync protocol.
//!
//! Consumers choose their transport: in-memory for testing,
//! HTTP for production, WebSocket for real-time sync.
//! PulseDB provides the trait; consumers (or feature-gated modules)
//! provide implementations.

use async_trait::async_trait;

use super::error::SyncError;
use super::types::{
    HandshakeRequest, HandshakeResponse, PullPage, PullRequest, PushAck, PushRequest, WireReply,
};

/// Transport layer for the sync protocol.
///
/// Implementations handle the wire protocol for exchanging sync data
/// between PulseDB instances. The sync engine calls these methods;
/// the transport handles serialization, networking, and authentication.
///
/// # Protocol v5 shape
///
/// Push and pull both take a **routed request** naming the peer the exchange
/// is for, and both return a [`WireReply`] naming the peer that actually
/// answered. An expected failure — the endpoint was replaced, a change is too
/// large, the request was refused — arrives inside that reply as a compact
/// machine-readable result, so it survives an HTTP hop instead of degrading
/// into a status code and a string. A `SyncError` from these methods is a
/// transport or framing failure, not a protocol answer.
///
/// # Direction-specific budgets
///
/// The three request methods take `send_budget_bytes`: the **outbound** cap the
/// request's own frame is encoded against, supplied per call by whoever packed
/// it. It is a local parameter and is never serialized — a budget field inside
/// a request would change that request's own encoded length, which is the
/// self-referential sizing problem the exact-size packer exists to avoid.
///
/// [`receive_limit_bytes`](SyncTransport::receive_limit_bytes) is the opposite
/// direction and is unrelated to it: what this transport will READ. Nothing
/// about a reader bounds what a peer will accept, so the two are legitimately
/// unequal and neither is derived from the other. An over-budget outbound
/// encode is [`SyncError::RequestTooLarge`]; an oversized inbound body stays
/// [`SyncError::PayloadTooLarge`].
///
/// # Implementations
///
/// - [`super::transport_mem::InMemorySyncTransport`] — in-process double that
///   still frames and decodes every message
/// - `HttpSyncTransport` — HTTP/HTTPS (behind `sync-http`)
/// - `WebSocketSyncTransport` — not implemented; the `sync-websocket` feature
///   maintains its compilation surface only
///
/// # Example
///
/// ```rust
/// use pulsedb::sync::transport::SyncTransport;
/// use pulsedb::sync::transport_mem::InMemorySyncTransport;
///
/// let (local, remote) = InMemorySyncTransport::new_pair();
/// // `local` answers as `remote`'s peer and vice versa; each side serves only
/// // its own lane, so neither can read the other's WAL by accident.
/// ```
#[async_trait]
pub trait SyncTransport: Send + Sync {
    /// Perform a handshake with the remote peer.
    ///
    /// Called when establishing a sync connection, and again whenever a reply
    /// reveals that the endpoint's identity changed. Exchanges instance ids,
    /// protocol versions, capabilities, and the responder's inbound body cap.
    ///
    /// No binding exists yet, so `send_budget_bytes` is local policy alone.
    async fn handshake(
        &self,
        request: HandshakeRequest,
        send_budget_bytes: usize,
    ) -> Result<HandshakeResponse, SyncError>;

    /// Push local changes to the remote peer.
    ///
    /// The request names the sender (the WAL owner of its changes), the
    /// intended target, and the sender's inbound budget for the reply.
    ///
    /// `send_budget_bytes` must be the very cap the batch was packed against,
    /// so a body that fits the packer is never refused by the encoder.
    async fn push_changes(
        &self,
        request: PushRequest,
        send_budget_bytes: usize,
    ) -> Result<WireReply<PushAck>, SyncError>;

    /// Pull changes from the remote peer.
    ///
    /// The request names the intended target — whose WAL is scanned — and the
    /// requester's inbound budget for the reply.
    ///
    /// `send_budget_bytes` bounds the request this side sends, which is a
    /// different quantity from `reply_limit_bytes` inside it.
    async fn pull_changes(
        &self,
        request: PullRequest,
        send_budget_bytes: usize,
    ) -> Result<WireReply<PullPage>, SyncError>;

    /// Check if the remote peer is reachable.
    ///
    /// Liveness only. It is **not** an identity check: a health check that
    /// answers says something is listening, not that the peer behind the
    /// address is still the one this session is bound to.
    async fn health_check(&self) -> Result<(), SyncError>;

    /// The largest reply body, in bytes, this transport will actually read.
    ///
    /// The **actual** bounded-reader limit, not a configured guess: the manager
    /// advertises `min(its own policy, this)` as its inbound budget, and a
    /// number larger than the reader would accept turns a fitting reply into an
    /// unreadable one.
    fn receive_limit_bytes(&self) -> usize;
}
