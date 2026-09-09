//! Sync-specific error types.
//!
//! [`SyncError`] covers all failure modes in the sync protocol:
//! transport failures, handshake rejections, serialization issues,
//! and shutdown coordination.

use thiserror::Error;

use super::types::InstanceId;

/// Errors specific to the sync protocol.
///
/// These are wrapped into [`crate::PulseDBError::Sync`] when propagated
/// through the public API.
#[derive(Debug, Error)]
pub enum SyncError {
    /// Transport-level failure (network I/O, connection refused, etc.).
    #[error("Sync transport error: {0}")]
    Transport(String),

    /// Handshake was rejected by the remote peer.
    #[error("Sync handshake failed: {0}")]
    Handshake(String),

    /// Failed to serialize or deserialize a sync message.
    #[error("Sync serialization error: {0}")]
    Serialization(String),

    /// Operation timed out waiting for a response.
    #[error("Sync operation timed out")]
    Timeout,

    /// Connection to the remote peer was lost.
    #[error("Connection to sync peer lost")]
    ConnectionLost,

    /// Protocol version mismatch between peers.
    #[error("Sync protocol version mismatch: local v{local}, remote v{remote}")]
    ProtocolVersion {
        /// Local protocol version.
        local: u32,
        /// Remote protocol version.
        remote: u32,
    },

    /// Wire-format preamble mismatch — caught by raw-byte inspection of the
    /// 3-byte preamble *before* any deserialize, so a serializer mismatch
    /// (e.g. a bincode-era peer vs a postcard-era peer) fails loud with a
    /// typed error instead of yielding garbage through the decoder.
    ///
    /// This is distinct from [`SyncError::ProtocolVersion`]: that variant is
    /// protocol-*semantics* (negotiated in-band after a successful decode);
    /// this variant is wire-*format* (the bytes can't be trusted to decode at
    /// all). Two failure shapes feed it:
    ///
    /// - **bad magic** (`got == None`): the leading bytes are not a PulseDB
    ///   sync preamble at all (truncated body, a non-PulseDB POST, or a
    ///   pre-4.0 no-preamble peer's body) — reported with `got: None`.
    /// - **wrong version** (`got == Some(v)`): a valid magic but a
    ///   `wire_format_version` byte this peer does not speak.
    #[error("Sync wire-format mismatch: expected wire format v{expected}, got {}", match got { Some(g) => format!("v{g}"), None => "bad/absent magic".to_string() })]
    WireFormatMismatch {
        /// The wire-format version this peer speaks.
        expected: u8,
        /// The wire-format version observed in the preamble, or `None` when the
        /// magic bytes were absent/wrong (so no trustworthy version was read).
        got: Option<u8>,
    },

    /// A request or response body exceeded the configured byte cap and was
    /// refused **before** any decode.
    ///
    /// Raised by the server-side byte handlers (`SyncServer::handle_*_bytes`)
    /// when `body.len()` exceeds
    /// [`SyncConfig::max_request_bytes`](super::config::SyncConfig::max_request_bytes),
    /// and by the HTTP transport client when a response body exceeds its cap.
    /// `size` is the observed body length — for a bounded read without a
    /// `Content-Length`, the byte count at which the cap was crossed — and
    /// `max` is the cap in force. The body never reaches postcard, so this is
    /// distinct from a decode-side [`SyncError::Serialization`].
    #[error("Sync payload too large: {size} bytes exceeds the {max}-byte cap")]
    PayloadTooLarge {
        /// Observed body length in bytes (or the length at which a bounded
        /// read crossed the cap).
        size: usize,
        /// The byte cap in force.
        max: usize,
    },

    /// Received an invalid or unrecognized payload.
    #[error("Invalid sync payload: {0}")]
    InvalidPayload(String),

    /// A frame carried the right magic and wire-format version but the wrong
    /// **operation** discriminator — a push body delivered to the pull
    /// endpoint, say.
    ///
    /// Caught by raw byte inspection of the frame header, like
    /// [`SyncError::WireFormatMismatch`], so a misrouted body never reaches the
    /// decoder. Protocol v4 framed only the handshake and carried no operation
    /// byte at all, which is why an unframed v4 push or pull body surfaces as a
    /// wire-format mismatch rather than this variant.
    #[error("Sync wire operation mismatch: this endpoint serves operation {expected}, got {got}")]
    WireOperationMismatch {
        /// The operation discriminator this endpoint serves.
        expected: u8,
        /// The operation discriminator the frame carried.
        got: u8,
    },

    /// The endpoint answered under an instance id other than the one the
    /// request was addressed to — a remint, a restored snapshot, or a different
    /// instance behind the same address.
    ///
    /// Raised on both legs. A **server** returns it (as
    /// `WireResult::PeerChanged`) when a routed request names a
    /// `target_instance` that is not its own, and applies nothing: no storage
    /// write, no statistic, no WAL event, no cursor movement. A **client**
    /// raises it when a reply's `responder` is not the identity it is bound to.
    ///
    /// This is identity consistency, not authentication: it says the exchange
    /// reached a different peer, not that the peer is untrusted.
    #[error("Sync peer changed: expected instance {expected}, endpoint answered as {responder}")]
    PeerChanged {
        /// The identity the exchange was addressed to.
        expected: InstanceId,
        /// The identity that actually answered.
        responder: InstanceId,
    },

    /// One individual change cannot fit a sync body on its own, so no batch
    /// containing it can ever be sent.
    ///
    /// This is **deterministic and terminal**, not transient: the same change
    /// rebuilt on the next cycle is the same size against the same cap. The
    /// background loop therefore records
    /// [`SyncStatus::Error`](super::types::SyncStatus::Error) and stops
    /// retrying rather than resending a body it already knows will not fit, and
    /// a one-shot call returns this error. The change's cursor is left
    /// unadvanced, so nothing is acknowledged over it.
    ///
    /// Correcting it is an operator action — raise the peer's
    /// `max_request_bytes`, or the transport's receive limit — after which
    /// [`SyncManager::start`](super::manager::SyncManager::start) runs again.
    #[error(
        "Sync change {sequence} needs {needed} bytes on its own, over the {cap}-byte body cap"
    )]
    ChangeTooLarge {
        /// The WAL sequence of the change that cannot fit.
        sequence: u64,
        /// The exact frame size that one change alone requires.
        needed: u64,
        /// The effective body cap it was measured against.
        cap: u64,
    },

    /// A request this instance built cannot fit the budget the party that must
    /// read it will actually accept.
    ///
    /// Raised at exactly one boundary — the **outbound** encode, where the
    /// request's own frame is measured against the send budget its producer
    /// supplied. It is never a reclassification of an inbound
    /// [`SyncError::PayloadTooLarge`]: an oversized body arriving from the wire
    /// keeps that variant, because there the fault is the sender's.
    ///
    /// **Deterministic and terminal**, for the same reason
    /// [`SyncError::ChangeTooLarge`] is: the same request rebuilt next cycle is
    /// the same size against the same cap, so retrying sends a body already
    /// known not to fit. The background loop records
    /// [`SyncStatus::Error`](super::types::SyncStatus::Error) and stops; a
    /// one-shot call returns it.
    ///
    /// Distinct from [`SyncError::ChangeTooLarge`], and not interchangeable
    /// with it: no WAL sequence is at fault here and no cursor is involved —
    /// the request's own shape is what does not fit. A single oversized WAL
    /// change is still `ChangeTooLarge`, caught by the packer before any
    /// transport is invoked, and still leaves its cursor unadvanced.
    ///
    /// Correcting it is an operator action — narrow
    /// [`SyncConfig::collectives`](super::config::SyncConfig::collectives), or
    /// raise the peer's `max_request_bytes` — after which
    /// [`SyncManager::start`](super::manager::SyncManager::start) runs again.
    #[error("Sync {operation:?} request needs {needed} bytes, over the {cap}-byte send budget")]
    RequestTooLarge {
        /// Which request could not fit.
        operation: super::wire::WireOperation,
        /// The exact frame size the request requires.
        needed: u64,
        /// The effective send budget it was measured against.
        cap: u64,
    },

    /// The peer refused the request with a compact structured reason.
    ///
    /// The detail is bounded on the wire
    /// ([`MAX_WIRE_DETAIL_BYTES`](super::types::MAX_WIRE_DETAIL_BYTES)) — a
    /// reply never carries an unbounded message or a per-change failure vector.
    #[error("Sync request rejected by peer: {code:?}: {detail}")]
    RemoteRejected {
        /// The machine-readable rejection code.
        code: super::types::WireErrorCode,
        /// A bounded human-readable detail.
        detail: String,
    },

    /// An apply could not run because the record it depends on is absent.
    ///
    /// Returned for an `ExperienceUpdated` whose target does not exist locally,
    /// and for a storage update that reported no row changed. It is a
    /// **failure**, never an idempotent skip: acknowledging it would let the
    /// sender compact away the create this update needs. Recovering the missing
    /// dependency is outside this repair — the point of the variant is that the
    /// non-completion is explicit rather than papered over.
    ///
    /// Already-absent deletes and archives keep their existing idempotent-skip
    /// behaviour; they need no record to be correct.
    #[error("Sync change depends on {entity} {id}, which is absent locally")]
    MissingDependency {
        /// The kind of record the change depends on.
        entity: &'static str,
        /// Its id, as text.
        id: String,
    },

    /// A [`SyncConfig`](super::config::SyncConfig) or transport pairing was
    /// refused at construction.
    ///
    /// [`SyncManager::new`](super::manager::SyncManager::new) and
    /// `SyncServer::new` validate before anything is built, so an unusable
    /// configuration fails at the call that made it rather than on the first
    /// cycle. 0.8.0 source break: both return `Result`.
    #[error("Invalid sync configuration: {0}")]
    Config(String),

    /// A one-shot catch-up ([`SyncManager::initial_sync`]) stopped before it had
    /// pulled everything the peer holds, so its completion cannot be claimed.
    ///
    /// Raised **only** by `initial_sync`, which is a "catch me up" call: a
    /// caller that gets `Ok(())` from it is entitled to believe the catch-up
    /// finished. The background loop and `sync_once` are unaffected — a failed
    /// change there is correctly left for the next cycle to retry, with the
    /// pull position deliberately not advanced past it. Two shapes reach this:
    ///
    /// - **stalled**: the peer answered `has_more: true` while handing back a
    ///   cursor that did not advance, which is what `SyncServer::handle_pull`
    ///   returns when a `collectives` filter (or an entity deleted since its WAL
    ///   event) emptied the whole page it polled. The loop must stop — the next
    ///   request would be byte-identical — but the changes beyond that page were
    ///   never reached. Repairing the server's pagination so the cursor advances
    ///   over a fully-filtered page is tracked in issue #90.
    /// - **apply failure**: a change in the run was still unapplied when the
    ///   loop stopped, so the store is not caught up even if the last page was
    ///   exhausted. Only failures left OUTSTANDING count: the catch-up loop
    ///   retries, so a change that errored on one page and applied on a later
    ///   attempt in the same run is not one of these.
    ///
    /// `position` is the pull position the run stopped at — where the next
    /// attempt resumes. Retrying is reasonable for the second shape (the
    /// failure may be transient); the first repeats until the peer changes.
    ///
    /// [`SyncManager::initial_sync`]: super::manager::SyncManager::initial_sync
    #[error(
        "Initial sync with peer {peer} stopped at position {position} without completing: {reason}"
    )]
    CatchUpIncomplete {
        /// The peer being caught up with.
        peer: InstanceId,
        /// The pull position the catch-up stopped at (and resumes from).
        position: u64,
        /// What stopped it — see the variant docs for the two shapes.
        reason: String,
    },

    /// No cursor found for the specified peer instance.
    #[error("No sync cursor found for instance {instance}")]
    CursorNotFound {
        /// The peer instance whose cursor was not found.
        instance: InstanceId,
    },

    /// The sync system is shutting down.
    #[error("Sync system is shutting down")]
    Shutdown,
}

impl SyncError {
    /// Creates a transport error with the given message.
    pub fn transport(msg: impl Into<String>) -> Self {
        Self::Transport(msg.into())
    }

    /// Creates a handshake error with the given message.
    pub fn handshake(msg: impl Into<String>) -> Self {
        Self::Handshake(msg.into())
    }

    /// Creates a serialization error with the given message.
    pub fn serialization(msg: impl Into<String>) -> Self {
        Self::Serialization(msg.into())
    }

    /// Creates an invalid payload error with the given message.
    pub fn invalid_payload(msg: impl Into<String>) -> Self {
        Self::InvalidPayload(msg.into())
    }

    /// Creates a wire-format mismatch for a **bad / absent magic** preamble.
    ///
    /// Used when the leading bytes are not a recognizable PulseDB sync
    /// preamble (truncated body, wrong magic, or a pre-preamble peer's body).
    pub fn wire_format_bad_magic(expected: u8) -> Self {
        Self::WireFormatMismatch {
            expected,
            got: None,
        }
    }

    /// Creates a wire-format mismatch for a **wrong wire-format version**.
    ///
    /// Used when the magic is valid but the `wire_format_version` byte names a
    /// version this peer does not speak.
    pub fn wire_format_version(expected: u8, got: u8) -> Self {
        Self::WireFormatMismatch {
            expected,
            got: Some(got),
        }
    }

    /// Creates a [`SyncError::CatchUpIncomplete`] for a **stalled** peer: it
    /// reported more changes while handing back a cursor that did not advance.
    pub fn catch_up_stalled(peer: InstanceId, position: u64) -> Self {
        Self::CatchUpIncomplete {
            peer,
            position,
            reason: "the peer reported more changes but did not advance the cursor \
                     (a fully-filtered page; see issue #90)"
                .to_string(),
        }
    }

    /// Creates a [`SyncError::CatchUpIncomplete`] for a catch-up that left
    /// `failed` changes unapplied, whatever the peer said about more pages.
    ///
    /// `failed` counts the changes STILL unapplied when the run stopped, not
    /// the attempts it made: a change that errored once and applied on a later
    /// retry within the same run is not counted.
    pub fn catch_up_apply_failed(peer: InstanceId, position: u64, failed: usize) -> Self {
        Self::CatchUpIncomplete {
            peer,
            position,
            reason: format!("{failed} change(s) failed to apply"),
        }
    }

    /// Creates a configuration error with the given message.
    pub fn config(msg: impl Into<String>) -> Self {
        Self::Config(msg.into())
    }

    /// Creates a [`SyncError::MissingDependency`] for an absent experience.
    pub fn missing_experience(id: impl std::fmt::Display) -> Self {
        Self::MissingDependency {
            entity: "experience",
            id: id.to_string(),
        }
    }

    /// Returns true if the exchange reached a different peer identity.
    pub fn is_peer_changed(&self) -> bool {
        matches!(self, Self::PeerChanged { .. })
    }

    /// Returns true if a single change is too large to ever be sent — the
    /// deterministic, terminal failure the background loop stops retrying on.
    pub fn is_change_too_large(&self) -> bool {
        matches!(self, Self::ChangeTooLarge { .. })
    }

    /// Returns true if an outbound request could not fit the budget the reader
    /// on the other side will accept — the deterministic, terminal failure the
    /// background loop stops retrying on, alongside
    /// [`is_change_too_large`](Self::is_change_too_large).
    pub fn is_request_too_large(&self) -> bool {
        matches!(self, Self::RequestTooLarge { .. })
    }

    /// Returns true if a frame carried the wrong operation discriminator.
    pub fn is_wire_operation_mismatch(&self) -> bool {
        matches!(self, Self::WireOperationMismatch { .. })
    }

    /// Returns true if an apply was refused because a record it depends on is
    /// absent locally.
    pub fn is_missing_dependency(&self) -> bool {
        matches!(self, Self::MissingDependency { .. })
    }

    /// Returns true if a constructor refused the configuration.
    pub fn is_config(&self) -> bool {
        matches!(self, Self::Config(_))
    }

    /// Returns true if the incompatibility is one a **protocol v5 peer reports
    /// about a peer that is not v5** — a wire-format mismatch (an unframed or
    /// wrong-version body, which is what protocol v4 produces), a wrong
    /// operation discriminator, or a negotiated protocol-version mismatch.
    ///
    /// v5 does not interoperate with v4 and offers no fallback: both replicas
    /// upgrade. This predicate is the typed hook a v5 client matches on. It
    /// makes no promise about what an *old* client understands — a v4 peer
    /// predates every error type here.
    pub fn is_protocol_incompatible(&self) -> bool {
        matches!(
            self,
            Self::WireFormatMismatch { .. }
                | Self::WireOperationMismatch { .. }
                | Self::ProtocolVersion { .. }
        )
    }

    /// Returns true if this is a transport error.
    pub fn is_transport(&self) -> bool {
        matches!(self, Self::Transport(_))
    }

    /// Returns true if this is a timeout error.
    pub fn is_timeout(&self) -> bool {
        matches!(self, Self::Timeout)
    }

    /// Returns true if this is a connection lost error.
    pub fn is_connection_lost(&self) -> bool {
        matches!(self, Self::ConnectionLost)
    }

    /// Returns true if this is a shutdown error.
    pub fn is_shutdown(&self) -> bool {
        matches!(self, Self::Shutdown)
    }

    /// Returns true if this is a wire-format mismatch (bad magic OR wrong
    /// wire-format version) — the typed fail-loud signal for cross-version
    /// sync, distinct from a generic [`SyncError::Serialization`].
    pub fn is_wire_format_mismatch(&self) -> bool {
        matches!(self, Self::WireFormatMismatch { .. })
    }

    /// Returns true if a body was refused for exceeding the byte cap before
    /// decode — the signal a consumer maps to `413 Payload Too Large`.
    pub fn is_payload_too_large(&self) -> bool {
        matches!(self, Self::PayloadTooLarge { .. })
    }

    /// Returns true if a one-shot catch-up stopped short (stalled peer OR a
    /// change that failed to apply) — the typed signal that
    /// [`SyncManager::initial_sync`](super::manager::SyncManager::initial_sync)
    /// did not finish, as distinct from a transport failure.
    pub fn is_catch_up_incomplete(&self) -> bool {
        matches!(self, Self::CatchUpIncomplete { .. })
    }
}

impl From<postcard::Error> for SyncError {
    fn from(err: postcard::Error) -> Self {
        SyncError::Serialization(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_error_display() {
        let err = SyncError::transport("connection refused");
        assert_eq!(err.to_string(), "Sync transport error: connection refused");
    }

    #[test]
    fn test_protocol_version_display() {
        let err = SyncError::ProtocolVersion {
            local: 1,
            remote: 2,
        };
        assert_eq!(
            err.to_string(),
            "Sync protocol version mismatch: local v1, remote v2"
        );
    }

    #[test]
    fn test_sync_error_is_checks() {
        assert!(SyncError::transport("x").is_transport());
        assert!(SyncError::Timeout.is_timeout());
        assert!(SyncError::ConnectionLost.is_connection_lost());
        assert!(SyncError::Shutdown.is_shutdown());
    }

    #[test]
    fn test_catch_up_incomplete_typed_and_distinct() {
        let peer = InstanceId::new();
        let stalled = SyncError::catch_up_stalled(peer, 42);
        let failed = SyncError::catch_up_apply_failed(peer, 42, 3);

        // Both shapes are the typed catch-up signal, not a transport failure.
        assert!(stalled.is_catch_up_incomplete());
        assert!(failed.is_catch_up_incomplete());
        assert!(!stalled.is_transport());
        assert!(!SyncError::transport("x").is_catch_up_incomplete());

        // Each names the peer, where it stopped, and what stopped it.
        for error in [&stalled, &failed] {
            let text = error.to_string();
            assert!(text.contains(&peer.to_string()), "{text}");
            assert!(text.contains("42"), "{text}");
        }
        assert!(stalled.to_string().contains("did not advance the cursor"));
        assert!(failed.to_string().contains("3 change(s) failed to apply"));
    }

    #[test]
    fn test_postcard_error_conversion() {
        // Deserializing truncated bytes triggers a postcard error.
        let bad_bytes = vec![0u8; 0]; // too short for a (u64, u64)
        let postcard_err = postcard::from_bytes::<(u64, u64)>(&bad_bytes).unwrap_err();
        let sync_err: SyncError = postcard_err.into();
        assert!(matches!(sync_err, SyncError::Serialization(_)));
    }

    #[test]
    fn test_payload_too_large_typed_and_distinct() {
        let err = SyncError::PayloadTooLarge { size: 65, max: 64 };
        assert!(err.is_payload_too_large());
        assert!(!err.is_wire_format_mismatch());
        assert!(!SyncError::serialization("x").is_payload_too_large());
        assert_eq!(
            err.to_string(),
            "Sync payload too large: 65 bytes exceeds the 64-byte cap"
        );
    }

    #[test]
    fn test_wire_format_mismatch_typed_and_distinct() {
        let bad_magic = SyncError::wire_format_bad_magic(3);
        let wrong_ver = SyncError::wire_format_version(3, 2);

        // Both are the typed wire-format signal, NOT a generic Serialization.
        assert!(bad_magic.is_wire_format_mismatch());
        assert!(wrong_ver.is_wire_format_mismatch());
        assert!(!SyncError::serialization("x").is_wire_format_mismatch());

        // Distinct from protocol-semantics ProtocolVersion.
        assert!(matches!(
            bad_magic,
            SyncError::WireFormatMismatch { got: None, .. }
        ));
        assert!(matches!(
            wrong_ver,
            SyncError::WireFormatMismatch {
                expected: 3,
                got: Some(2)
            }
        ));
    }
}
