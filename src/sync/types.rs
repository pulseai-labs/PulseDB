//! Core types for the PulseDB sync protocol.
//!
//! This module defines the wire types used for synchronizing data between
//! PulseDB instances: change payloads, cursors, handshake messages, and
//! the instance identity type.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::collective::Collective;
use crate::experience::Experience;
use crate::insight::DerivedInsight;
use crate::relation::ExperienceRelation;
use crate::types::{CollectiveId, ExperienceId, InsightId, RelationId, Timestamp};

pub use crate::types::InstanceId;

// ============================================================================
// SyncCursor — Persisted per-peer sync positions (storage record, schema v5)
// ============================================================================

/// Persisted sync positions for a specific peer instance.
///
/// Push and pull are tracked **separately** (issue #9): `push_sequence` is the
/// *local* WAL position the peer has acknowledged receiving, and
/// `pull_sequence` is the *remote* WAL position this instance has applied from
/// the peer. The two live in different sequence spaces and must never
/// overwrite each other — [`PulseDB::compact_wal`](crate::PulseDB::compact_wal)
/// trusts only `push_sequence`.
///
/// This is the **storage** record (postcard-encoded in the `sync_cursors`
/// table; schema v5 split the pre-0.8.0 single `last_sequence`). It is not a
/// wire type: pull and push messages carry a single-direction
/// [`SyncPosition`] so the protocol v4 bytes are unchanged.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncCursor {
    /// The peer instance this cursor tracks.
    pub instance_id: InstanceId,

    /// Highest **local** WAL sequence the peer has acknowledged (push side).
    ///
    /// WAL compaction never deletes above the minimum of this value over all
    /// known peers; `0` means "nothing pushed yet" and blocks compaction.
    pub push_sequence: u64,

    /// Highest **remote** WAL sequence applied locally from this peer (pull
    /// side). Never feeds compaction.
    pub pull_sequence: u64,
}

impl SyncCursor {
    /// Creates a new cursor with both positions at 0 (beginning of WAL).
    pub fn new(instance_id: InstanceId) -> Self {
        Self {
            instance_id,
            push_sequence: 0,
            pull_sequence: 0,
        }
    }
}

// ============================================================================
// SyncPosition — Single-direction wire position (protocol v4 bytes)
// ============================================================================

/// A single-direction sync position as carried on the wire.
///
/// Rides in [`PullRequest::cursor`] and [`PullPage::scan_position`].
/// `sequence` is the position **for the direction of the message it rides
/// in**: a pull message carries a position in the *remote* WAL (persisted as
/// [`SyncCursor::pull_sequence`]), while a push acknowledgement's position is
/// [`PushAck::safe_through`] in the *sender's* WAL (persisted as
/// [`SyncCursor::push_sequence`]).
///
/// Field order and types are pinned — `{ instance_id: InstanceId, sequence:
/// u64 }` — and unchanged from protocol v4, so the persisted schema-v5 cursor
/// migration is untouched. What v5 changed is the *envelope*: an
/// acknowledgement now names the responder and the WAL owner separately,
/// instead of overloading one position with both meanings.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncPosition {
    /// The instance the position refers to.
    pub instance_id: InstanceId,

    /// The WAL sequence for the message's direction.
    pub sequence: u64,
}

impl SyncPosition {
    /// Creates a position at `sequence` for `instance_id`.
    pub fn new(instance_id: InstanceId, sequence: u64) -> Self {
        Self {
            instance_id,
            sequence,
        }
    }
}

// ============================================================================
// SyncEntityType — What kind of entity changed
// ============================================================================

/// Discriminant for the type of entity in a sync change.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum SyncEntityType {
    /// An experience was created, updated, archived, or deleted.
    Experience = 0,
    /// A relation was created or deleted.
    Relation = 1,
    /// An insight was created or deleted.
    Insight = 2,
    /// A collective was created.
    Collective = 3,
}

// ============================================================================
// SerializableExperienceUpdate — Wire-safe mirror of ExperienceUpdate
// ============================================================================

/// Wire-safe version of [`crate::ExperienceUpdate`] for sync payloads.
///
/// The original `ExperienceUpdate` does not derive `Serialize`/`Deserialize`,
/// so this struct mirrors its fields with full serde support. Use the `From`
/// impls to convert between the two.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SerializableExperienceUpdate {
    /// New importance score (0.0–1.0).
    pub importance: Option<f32>,

    /// New confidence score (0.0–1.0).
    pub confidence: Option<f32>,

    /// Replace domain tags entirely.
    pub domain: Option<Vec<String>>,

    /// Replace key-value tags entirely (VS-4.3.2).
    pub tags: Option<BTreeMap<String, String>>,

    /// Replace related files entirely.
    pub related_files: Option<Vec<String>>,

    /// Set archived status.
    pub archived: Option<bool>,

    /// Full G-counter applications map for CRDT merge.
    pub applications: Option<BTreeMap<InstanceId, u32>>,

    /// Last reinforcement timestamp for max-timestamp merge.
    pub last_reinforced: Option<Timestamp>,
}

impl From<crate::experience::ExperienceUpdate> for SerializableExperienceUpdate {
    fn from(update: crate::experience::ExperienceUpdate) -> Self {
        Self {
            importance: update.importance,
            confidence: update.confidence,
            domain: update.domain,
            tags: update.tags,
            related_files: update.related_files,
            archived: update.archived,
            applications: None,
            last_reinforced: None,
        }
    }
}

impl From<SerializableExperienceUpdate> for crate::experience::ExperienceUpdate {
    fn from(update: SerializableExperienceUpdate) -> Self {
        Self {
            importance: update.importance,
            confidence: update.confidence,
            domain: update.domain,
            tags: update.tags,
            related_files: update.related_files,
            archived: update.archived,
        }
    }
}

// ============================================================================
// SyncExperience — Wire-only experience carrier (issue #96)
// ============================================================================

/// An [`Experience`] together with the embedding vector that its **disk**
/// encoding deliberately omits.
///
/// [`Experience::embedding`] is `#[serde(skip)]` because embeddings live in
/// their own storage table and the storage layer rejoins them on read. That is
/// right for the record and wrong for the wire: a create crossing any
/// serializing transport arrived with a zero-length vector, failed the
/// collective's dimension check, and the experience never landed at all (#96).
///
/// This type is the wire's own shape, so the fix costs the disk format nothing:
/// the record still serializes without its vector, and the vector travels
/// beside it in a field only the sync protocol knows about.
///
/// The conversions **move** the vector rather than copying it: outbound takes
/// it out of the owned record, inbound puts it back. Nothing re-embeds
/// implicitly — a vector that did not cross the wire does not reappear.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SyncExperience {
    /// The experience record, whose own `embedding` field is not serialized.
    pub experience: Experience,
    /// The embedding vector, carried explicitly so it survives the wire.
    pub embedding: Vec<f32>,
}

impl From<Experience> for SyncExperience {
    fn from(mut experience: Experience) -> Self {
        let embedding = std::mem::take(&mut experience.embedding);
        Self {
            experience,
            embedding,
        }
    }
}

impl From<SyncExperience> for Experience {
    fn from(value: SyncExperience) -> Self {
        let mut experience = value.experience;
        experience.embedding = value.embedding;
        experience
    }
}

// ============================================================================
// SyncPayload — Full data for each mutation type
// ============================================================================

/// The payload of a sync change, containing all data needed to apply
/// the change on the receiving end.
///
/// Uses full payloads (not deltas) so the receiver has everything needed
/// including embeddings for HNSW insertion.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SyncPayload {
    /// A new experience was created.
    ///
    /// Carries the wire-only [`SyncExperience`], **not** a bare
    /// [`Experience`]: the record's `embedding` is `#[serde(skip)]` for disk
    /// serialization, so a bare record crossing a serializing transport arrives
    /// with an empty vector and cannot be indexed (#96). The wrapper moves the
    /// vector out of the loaded record on the way out and restores it on the
    /// way in; the stored encoding is untouched.
    ExperienceCreated(SyncExperience),

    /// An existing experience was updated.
    ExperienceUpdated {
        /// The experience that was updated.
        id: ExperienceId,
        /// The fields that changed.
        update: SerializableExperienceUpdate,
        /// When the update occurred.
        timestamp: Timestamp,
    },

    /// An experience was soft-deleted (archived).
    ExperienceArchived {
        /// The archived experience.
        id: ExperienceId,
        /// When the archive occurred.
        timestamp: Timestamp,
    },

    /// An experience was permanently deleted.
    ExperienceDeleted {
        /// The deleted experience.
        id: ExperienceId,
        /// When the deletion occurred.
        timestamp: Timestamp,
    },

    /// A new relation was created.
    RelationCreated(ExperienceRelation),

    /// A relation was deleted.
    RelationDeleted {
        /// The deleted relation.
        id: RelationId,
        /// When the deletion occurred.
        timestamp: Timestamp,
    },

    /// A new insight was created.
    InsightCreated(DerivedInsight),

    /// An insight was deleted.
    InsightDeleted {
        /// The deleted insight.
        id: InsightId,
        /// When the deletion occurred.
        timestamp: Timestamp,
    },

    /// A new collective was created.
    CollectiveCreated(Collective),
}

// ============================================================================
// SyncChange — A single change to sync
// ============================================================================

/// A single change event to be synchronized between PulseDB instances.
///
/// Contains the full payload needed to apply the change, plus metadata
/// about the source instance and WAL position.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SyncChange {
    /// Source WAL sequence number.
    pub sequence: u64,

    /// The instance that produced this change.
    pub source_instance: InstanceId,

    /// Which collective this change belongs to.
    pub collective_id: CollectiveId,

    /// What kind of entity changed.
    pub entity_type: SyncEntityType,

    /// The full change data.
    pub payload: SyncPayload,

    /// When the change occurred.
    pub timestamp: Timestamp,
}

// ============================================================================
// SyncStatus — Current state of the sync system
// ============================================================================

/// Current operational status of the sync system.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncStatus {
    /// Sync is idle, waiting for the next poll interval.
    Idle,
    /// Sync is actively transferring data.
    Syncing,
    /// Sync encountered an error.
    Error(String),
    /// Disconnected from the remote peer.
    Disconnected,
}

// ============================================================================
// SyncStats — Local-only counters (never on the wire)
// ============================================================================

/// Local-only counters accumulated over the changes a peer has applied —
/// by a [`SyncManager`](super::SyncManager) on the pull side and by a
/// `SyncServer` on the push side.
///
/// This type is **not** a wire type: it deliberately derives neither
/// `Serialize` nor `Deserialize`, so a new counter never changes the shape of
/// `PushResponse`/`PullResponse` or moves
/// [`SYNC_PROTOCOL_VERSION`](super::SYNC_PROTOCOL_VERSION).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SyncStats {
    /// Number of applied changes whose incoming `last_reinforced` lay beyond
    /// `now + SyncConfig::max_clock_skew_ms` (#13).
    ///
    /// Every one is merged **unchanged** — FR-031's max-merge is never clamped,
    /// rejected or re-timestamped (r1 veto fold C2) — and a batch that carried
    /// any is logged ONCE at `warn` with the peer, the count and the largest
    /// skew seen. The log is per batch rather than per change because the
    /// condition never self-clears while the peer's clock is wrong. The bound
    /// is advisory: no protocol version carries a record-level time reference
    /// yet, and v5 deliberately did not add one.
    pub skewed_timestamps: u64,
}

// ============================================================================
// Wire bounds — what a control frame may carry
// ============================================================================

/// Maximum number of capability strings a handshake may carry.
///
/// The list is informational (see
/// [`SYNC_CAPABILITY_GCOUNTER_APPLICATIONS`](super::SYNC_CAPABILITY_GCOUNTER_APPLICATIONS)),
/// but it is still attacker-controlled input, and the 1 KiB control-frame
/// minimum is only a real guarantee if the largest legal handshake is bounded.
/// Both bounds are enforced on decode, and
/// `wire::tests::recovery_v5_bounded_control_frames_fit_the_minimum_budget`
/// certifies the maximum-sized frame against the minimum.
pub const MAX_HANDSHAKE_CAPABILITIES: usize = 8;

/// Maximum byte length of one handshake capability string.
pub const MAX_HANDSHAKE_CAPABILITY_BYTES: usize = 64;

/// Maximum byte length of any human-readable detail carried on the wire — a
/// handshake rejection reason, a [`WireResult::Rejected`] detail.
///
/// A reply never carries an unbounded message and never carries a per-change
/// failure vector: the counts in [`PushAck`] say how many failed, and the
/// sender already knows which sequences it sent.
pub const MAX_WIRE_DETAIL_BYTES: usize = 256;

/// Truncates `detail` to [`MAX_WIRE_DETAIL_BYTES`] on a char boundary.
pub fn bound_wire_detail(detail: impl Into<String>) -> String {
    let mut detail = detail.into();
    if detail.len() > MAX_WIRE_DETAIL_BYTES {
        let mut cut = MAX_WIRE_DETAIL_BYTES;
        while cut > 0 && !detail.is_char_boundary(cut) {
            cut -= 1;
        }
        detail.truncate(cut);
    }
    detail
}

// ============================================================================
// Handshake messages
// ============================================================================

/// Request sent during sync handshake to establish a connection.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HandshakeRequest {
    /// The local instance ID.
    pub instance_id: InstanceId,
    /// The sync protocol version.
    pub protocol_version: u32,
    /// Capabilities advertised by this instance.
    ///
    /// Informational and not negotiated, but bounded: at most
    /// [`MAX_HANDSHAKE_CAPABILITIES`] entries of at most
    /// [`MAX_HANDSHAKE_CAPABILITY_BYTES`] each.
    pub capabilities: Vec<String>,
}

impl HandshakeRequest {
    /// Refuses a handshake whose capability list is outside the wire bounds.
    ///
    /// Called by the server before anything else looks at the request: the
    /// control-frame budget is a guarantee only over bounded messages.
    pub fn check_bounds(&self) -> Result<(), super::error::SyncError> {
        if self.capabilities.len() > MAX_HANDSHAKE_CAPABILITIES {
            return Err(super::error::SyncError::invalid_payload(format!(
                "handshake advertises {} capabilities (max {MAX_HANDSHAKE_CAPABILITIES})",
                self.capabilities.len()
            )));
        }
        if let Some(oversized) = self
            .capabilities
            .iter()
            .find(|c| c.len() > MAX_HANDSHAKE_CAPABILITY_BYTES)
        {
            return Err(super::error::SyncError::invalid_payload(format!(
                "handshake capability of {} bytes (max {MAX_HANDSHAKE_CAPABILITY_BYTES})",
                oversized.len()
            )));
        }
        Ok(())
    }
}

/// Response to a handshake request.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HandshakeResponse {
    /// The remote instance ID.
    pub instance_id: InstanceId,
    /// The remote's protocol version.
    pub protocol_version: u32,
    /// Whether the handshake was accepted.
    pub accepted: bool,
    /// Reason for rejection, if not accepted. Bounded by
    /// [`MAX_WIRE_DETAIL_BYTES`].
    pub reason: Option<String>,
    /// The **responder's own inbound body cap**, in bytes.
    ///
    /// This is what the peer will accept on a later push, so the sender packs
    /// against `min(local policy, this)`. It is the server's actual
    /// `SyncConfig::max_request_bytes`, not a guess: a sender that assumed its
    /// own cap would build bodies the peer refuses forever.
    pub receive_limit_bytes: u64,
}

// ============================================================================
// Routed requests — every exchange names the peer it is for
// ============================================================================

/// Request to push local changes to a remote peer.
///
/// # Ownership, spelled out
///
/// - `source_instance` is the **sender**, and therefore the WAL owner of every
///   sequence in `changes`. A change claiming a different `source_instance` is
///   invalid payload.
/// - `target_instance` is the identity the sender believes it is talking to. A
///   server whose own id differs applies **nothing** and answers
///   [`WireResult::PeerChanged`]. This is what makes an endpoint replaced
///   *between* the pull and the push of one cycle safe: the pull cannot vouch
///   for the push.
/// - `reply_limit_bytes` is the sender's effective inbound budget, so the
///   responder can preflight its acknowledgement before applying anything.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PushRequest {
    /// The sender's protocol version.
    pub protocol_version: u32,
    /// The sender's instance id — the WAL owner of `changes`.
    pub source_instance: InstanceId,
    /// The instance this request is addressed to.
    pub target_instance: InstanceId,
    /// The sender's inbound body cap for the reply, in bytes.
    pub reply_limit_bytes: u64,
    /// The changes being pushed, in ascending sequence order.
    pub changes: Vec<SyncChange>,
}

/// Request to pull changes from a remote peer.
///
/// `target_instance` is the WAL owner being scanned as well as the expected
/// responder: a pull position is a position in the *peer's* WAL and is
/// meaningless without saying whose.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PullRequest {
    /// The requester's protocol version.
    pub protocol_version: u32,
    /// The requester's instance id.
    pub source_instance: InstanceId,
    /// The instance this request is addressed to, and whose WAL is scanned.
    pub target_instance: InstanceId,
    /// The remote-WAL position to pull changes from (the client's persisted
    /// `pull_sequence` for this peer).
    pub cursor: SyncPosition,
    /// Maximum number of changes to return in this batch. Zero is refused.
    pub batch_size: u64,
    /// The requester's inbound body cap for the reply, in bytes. The server
    /// packs against `min(this, its own policy)`.
    pub reply_limit_bytes: u64,
    /// Optional filter: only pull changes for these collectives.
    pub collectives: Option<Vec<CollectiveId>>,
}

// ============================================================================
// Bounded replies
// ============================================================================

/// Machine-readable rejection code carried in [`WireResult::Rejected`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum WireErrorCode {
    /// The request was malformed, out of bounds, or internally inconsistent.
    InvalidRequest = 0,
    /// The request named a protocol version this peer does not speak.
    ProtocolVersion = 1,
    /// The responder failed while serving the request.
    Internal = 2,
}

/// Every reply on the v5 wire, whatever the operation.
///
/// It always names the **responder** — the identity that actually answered —
/// so a client can tell a reply from its bound peer apart from one produced by
/// a replacement, on every message rather than only on a pull.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WireReply<T> {
    /// The responder's protocol version.
    pub protocol_version: u32,
    /// The identity that produced this reply.
    pub responder: InstanceId,
    /// Success, or a compact structured failure.
    pub result: WireResult<T>,
}

impl<T> WireReply<T> {
    /// A successful reply from `responder`.
    pub fn ok(responder: InstanceId, value: T) -> Self {
        Self {
            protocol_version: super::SYNC_PROTOCOL_VERSION,
            responder,
            result: WireResult::Ok(value),
        }
    }

    /// A reply refusing a request addressed to `expected`, because this
    /// responder is somebody else.
    pub fn peer_changed(responder: InstanceId, expected: InstanceId) -> Self {
        Self {
            protocol_version: super::SYNC_PROTOCOL_VERSION,
            responder,
            result: WireResult::PeerChanged { expected },
        }
    }

    /// A compact structured rejection; `detail` is truncated to
    /// [`MAX_WIRE_DETAIL_BYTES`].
    pub fn rejected(responder: InstanceId, code: WireErrorCode, detail: impl Into<String>) -> Self {
        Self {
            protocol_version: super::SYNC_PROTOCOL_VERSION,
            responder,
            result: WireResult::Rejected {
                code,
                detail: bound_wire_detail(detail),
            },
        }
    }

    /// Turns a non-success result into the typed client-side error, or hands
    /// back the value.
    ///
    /// `expected` is the identity the caller is bound to; a reply from anyone
    /// else is [`SyncError::PeerChanged`](super::error::SyncError::PeerChanged)
    /// whatever its result says, because a success produced by the wrong peer
    /// is not a success.
    pub fn into_result(self, expected: InstanceId) -> Result<T, super::error::SyncError> {
        use super::error::SyncError;
        if self.protocol_version != super::SYNC_PROTOCOL_VERSION {
            return Err(SyncError::ProtocolVersion {
                local: super::SYNC_PROTOCOL_VERSION,
                remote: self.protocol_version,
            });
        }
        if self.responder != expected {
            return Err(SyncError::PeerChanged {
                expected,
                responder: self.responder,
            });
        }
        match self.result {
            WireResult::Ok(value) => Ok(value),
            WireResult::PeerChanged { expected: named } => Err(SyncError::PeerChanged {
                expected: named,
                responder: self.responder,
            }),
            WireResult::ChangeTooLarge {
                sequence,
                needed,
                cap,
            } => Err(SyncError::ChangeTooLarge {
                sequence,
                needed,
                cap,
            }),
            WireResult::Rejected { code, detail } => {
                Err(SyncError::RemoteRejected { code, detail })
            }
        }
    }
}

/// The body of a [`WireReply`]: success, or one of the expected, bounded,
/// machine-readable failures.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WireResult<T> {
    /// The request succeeded.
    Ok(T),
    /// The request was addressed to `expected`, which is not this responder.
    /// Nothing was applied, no statistic moved and no cursor advanced.
    PeerChanged {
        /// The identity the request named.
        expected: InstanceId,
    },
    /// One change cannot fit a body on its own, so no batch containing it can
    /// be sent. Deterministic and terminal — see
    /// [`SyncError::ChangeTooLarge`](super::error::SyncError::ChangeTooLarge).
    ChangeTooLarge {
        /// The WAL sequence of the change that cannot fit.
        sequence: u64,
        /// The exact frame size that one change alone requires.
        needed: u64,
        /// The effective body cap it was measured against.
        cap: u64,
    },
    /// A compact structured rejection.
    Rejected {
        /// The machine-readable code.
        code: WireErrorCode,
        /// A bounded detail — never an unbounded message or failure vector.
        detail: String,
    },
}

// ============================================================================
// Reply payloads
// ============================================================================

/// A successful push acknowledgement.
///
/// # Two identities, two meanings
///
/// [`WireReply::responder`] is the peer that answered. `wal_owner` is whose WAL
/// `safe_through` indexes — the **sender's**, because a push acknowledges a
/// position in the sender's WAL. Protocol v4 collapsed these into one
/// `SyncPosition` and the two transports disagreed about which id belonged in
/// it, which is why identity could only be checked on a pull.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PushAck {
    /// The instance whose WAL `safe_through` indexes: the sender.
    pub wal_owner: InstanceId,
    /// Changes applied plus changes genuinely idempotently skipped.
    pub accepted: u64,
    /// Changes that FAILED to apply.
    pub rejected: u64,
    /// Total changes the responder received; `accepted + rejected`.
    pub total: u64,
    /// Highest sender-WAL sequence at or below which every change in this
    /// batch was applied or idempotently skipped, or `None` when none was.
    ///
    /// This is an **actual-success** position, never a filtered tail and never
    /// `failure_sequence - 1`. The sender may not advance past it.
    pub safe_through: Option<u64>,
}

/// One page of pulled changes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PullPage {
    /// The changes at or after the requested cursor, in ascending sequence
    /// order.
    pub changes: Vec<SyncChange>,
    /// Whether the responder may hold more beyond `scan_position`.
    ///
    /// A claim about the responder's WAL, not about this batch: false only when
    /// this scan proved the WAL exhausted.
    pub has_more: bool,
    /// How far the responder actually **scanned** in its own WAL — the last
    /// event it read before the first eligible change it could not include.
    ///
    /// Filtered events and events that no longer resolve to an entity advance
    /// it; an omitted eligible change does not. It belongs to the prefix that
    /// was emitted, not to the poller's eager end position.
    pub scan_position: SyncPosition,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_instance_id_new_is_unique() {
        let a = InstanceId::new();
        let b = InstanceId::new();
        assert_ne!(a, b);
    }

    #[test]
    fn test_instance_id_nil() {
        let id = InstanceId::nil();
        assert_eq!(id, InstanceId::default());
        assert_eq!(id, InstanceId::nil());
    }

    #[test]
    fn test_instance_id_bytes_roundtrip() {
        let id = InstanceId::new();
        let bytes = *id.as_bytes();
        let restored = InstanceId::from_bytes(bytes);
        assert_eq!(id, restored);
    }

    #[test]
    fn test_instance_id_display() {
        let id = InstanceId::nil();
        assert_eq!(id.to_string(), "00000000-0000-0000-0000-000000000000");
    }

    #[test]
    fn test_instance_id_postcard_roundtrip() {
        let id = InstanceId::new();
        let bytes = postcard::to_allocvec(&id).unwrap();
        let restored: InstanceId = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(id, restored);
    }

    #[test]
    fn test_sync_cursor_new() {
        let id = InstanceId::new();
        let cursor = SyncCursor::new(id);
        assert_eq!(cursor.instance_id, id);
        assert_eq!(cursor.push_sequence, 0);
        assert_eq!(cursor.pull_sequence, 0);
    }

    #[test]
    fn test_sync_cursor_postcard_roundtrip() {
        let cursor = SyncCursor {
            instance_id: InstanceId::new(),
            push_sequence: 42,
            pull_sequence: 7,
        };
        let bytes = postcard::to_allocvec(&cursor).unwrap();
        let restored: SyncCursor = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(cursor, restored);
    }

    /// r1.s1.w1: `SyncPosition` must encode byte-for-byte like the 0.7.0 wire
    /// `SyncCursor { instance_id, last_sequence }`, so protocol v4 bytes are
    /// unchanged. Pinned literally against postcard's encoding: a uuid is
    /// `varint(16) ‖ 16 raw bytes` and the sequence a LEB128 varint.
    #[test]
    fn sync_position_wire_bytes_match_v4_cursor_encoding() {
        let id_bytes = [0x5A; 16];
        let position = SyncPosition::new(InstanceId::from_bytes(id_bytes), 7);
        let mut expected = vec![0x10];
        expected.extend_from_slice(&id_bytes);
        expected.push(0x07);
        assert_eq!(postcard::to_stdvec(&position).unwrap(), expected);
        // A 0.7.0-encoded cursor decodes as a SyncPosition.
        let decoded: SyncPosition = postcard::from_bytes(&expected).unwrap();
        assert_eq!(decoded, position);
        // Multi-byte varint sequence (300 = 0xAC 0x02): no fixed-width assumption.
        let big = SyncPosition::new(InstanceId::from_bytes(id_bytes), 300);
        let mut expected_big = vec![0x10];
        expected_big.extend_from_slice(&id_bytes);
        expected_big.extend_from_slice(&[0xAC, 0x02]);
        assert_eq!(postcard::to_stdvec(&big).unwrap(), expected_big);
    }

    #[test]
    fn test_sync_entity_type_repr() {
        assert_eq!(SyncEntityType::Experience as u8, 0);
        assert_eq!(SyncEntityType::Relation as u8, 1);
        assert_eq!(SyncEntityType::Insight as u8, 2);
        assert_eq!(SyncEntityType::Collective as u8, 3);
    }

    #[test]
    fn test_serializable_experience_update_from_conversion() {
        let update = crate::experience::ExperienceUpdate {
            importance: Some(0.9),
            confidence: None,
            domain: Some(vec!["rust".to_string()]),
            tags: None,
            related_files: None,
            archived: Some(false),
        };
        let serializable: SerializableExperienceUpdate = update.into();
        assert_eq!(serializable.importance, Some(0.9));
        assert_eq!(serializable.confidence, None);
        assert_eq!(serializable.domain, Some(vec!["rust".to_string()]));
        assert_eq!(serializable.archived, Some(false));
    }

    #[test]
    fn test_serializable_experience_update_into_conversion() {
        let serializable = SerializableExperienceUpdate {
            importance: Some(0.5),
            confidence: Some(0.8),
            domain: None,
            tags: None,
            related_files: Some(vec!["main.rs".to_string()]),
            archived: None,
            applications: None,
            last_reinforced: None,
        };
        let update: crate::experience::ExperienceUpdate = serializable.into();
        assert_eq!(update.importance, Some(0.5));
        assert_eq!(update.confidence, Some(0.8));
        assert_eq!(update.related_files, Some(vec!["main.rs".to_string()]));
    }

    #[test]
    fn test_serializable_experience_update_postcard_roundtrip() {
        let update = SerializableExperienceUpdate {
            importance: Some(0.7),
            confidence: Some(0.9),
            domain: Some(vec!["test".to_string()]),
            tags: Some(BTreeMap::from([(
                "entity.type".to_string(),
                "person".to_string(),
            )])),
            related_files: None,
            archived: Some(true),
            applications: Some(std::collections::BTreeMap::from([(InstanceId::new(), 2)])),
            last_reinforced: Some(Timestamp::now()),
        };
        let bytes = postcard::to_allocvec(&update).unwrap();
        let restored: SerializableExperienceUpdate = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(update.importance, restored.importance);
        assert_eq!(update.confidence, restored.confidence);
        assert_eq!(update.domain, restored.domain);
        assert_eq!(update.archived, restored.archived);
        assert_eq!(update.applications, restored.applications);
        assert_eq!(update.last_reinforced, restored.last_reinforced);
    }

    #[test]
    fn test_sync_stats_default_is_zero() {
        let stats = SyncStats::default();
        assert_eq!(stats.skewed_timestamps, 0);
        assert_eq!(
            stats,
            SyncStats {
                skewed_timestamps: 0
            }
        );
    }

    #[test]
    fn test_sync_status_equality() {
        assert_eq!(SyncStatus::Idle, SyncStatus::Idle);
        assert_eq!(SyncStatus::Error("x".into()), SyncStatus::Error("x".into()));
        assert_ne!(SyncStatus::Idle, SyncStatus::Syncing);
    }

    #[test]
    fn recovery_v5_handshake_response_carries_the_inbound_limit() {
        let response = HandshakeResponse {
            instance_id: InstanceId::new(),
            protocol_version: crate::sync::SYNC_PROTOCOL_VERSION,
            accepted: true,
            reason: None,
            receive_limit_bytes: 8 * 1024 * 1024,
        };
        let bytes = postcard::to_allocvec(&response).unwrap();
        let restored: HandshakeResponse = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(restored.receive_limit_bytes, 8 * 1024 * 1024);
        assert_eq!(restored.instance_id, response.instance_id);
    }

    #[test]
    fn test_handshake_request_postcard_roundtrip() {
        let req = HandshakeRequest {
            instance_id: InstanceId::new(),
            protocol_version: crate::sync::SYNC_PROTOCOL_VERSION,
            capabilities: vec!["push".to_string(), "pull".to_string()],
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let restored: HandshakeRequest = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req.instance_id, restored.instance_id);
        assert_eq!(req.protocol_version, restored.protocol_version);
        assert_eq!(req.capabilities, restored.capabilities);
    }

    #[test]
    fn test_pull_request_postcard_roundtrip() {
        let peer = InstanceId::new();
        let req = PullRequest {
            protocol_version: crate::sync::SYNC_PROTOCOL_VERSION,
            source_instance: InstanceId::new(),
            target_instance: peer,
            cursor: SyncPosition::new(peer, 0),
            batch_size: 500,
            reply_limit_bytes: 4096,
            collectives: Some(vec![CollectiveId::new()]),
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let restored: PullRequest = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(req.cursor, restored.cursor);
        assert_eq!(req.batch_size, restored.batch_size);
        assert_eq!(req.target_instance, restored.target_instance);
        assert_eq!(req.reply_limit_bytes, restored.reply_limit_bytes);
    }

    #[test]
    fn recovery_v5_push_ack_reply_postcard_roundtrip() {
        let responder = InstanceId::new();
        let sender = InstanceId::new();
        let reply = WireReply::ok(
            responder,
            PushAck {
                wal_owner: sender,
                accepted: 10,
                rejected: 2,
                total: 12,
                safe_through: Some(100),
            },
        );
        let bytes = postcard::to_allocvec(&reply).unwrap();
        let restored: WireReply<PushAck> = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(restored.responder, responder);
        let ack = restored.into_result(responder).unwrap();
        assert_eq!(ack.wal_owner, sender);
        assert_eq!(ack.accepted, 10);
        assert_eq!(ack.rejected, 2);
        assert_eq!(ack.total, 12);
        assert_eq!(ack.safe_through, Some(100));
    }

    /// The wire wrapper MOVES the vector out of the record and back in, so a
    /// serialized round trip preserves the embedding the record's own encoding
    /// deliberately drops (#96).
    #[test]
    fn recovery_v5_sync_experience_carries_the_embedding_across_postcard() {
        use crate::experience::{Experience, ExperienceType};
        use crate::types::{AgentId, ExperienceId};

        let vector: Vec<f32> = (0..8).map(|i| i as f32 * 0.25).collect();
        let experience = Experience {
            id: ExperienceId::new(),
            collective_id: CollectiveId::new(),
            content: "wire vector".to_string(),
            embedding: vector.clone(),
            experience_type: ExperienceType::Generic { category: None },
            importance: 0.5,
            confidence: 0.5,
            applications: BTreeMap::new(),
            domain: Vec::new(),
            tags: BTreeMap::new(),
            related_files: Vec::new(),
            source_agent: AgentId::new("wire-test"),
            source_task: None,
            timestamp: Timestamp::now(),
            last_reinforced: Timestamp::now(),
            archived: false,
        };

        let carried: SyncExperience = experience.clone().into();
        assert!(
            carried.experience.embedding.is_empty(),
            "the vector is MOVED out of the record, not copied beside it"
        );
        assert_eq!(carried.embedding, vector);

        let bytes = postcard::to_allocvec(&carried).unwrap();
        let restored: SyncExperience = postcard::from_bytes(&bytes).unwrap();
        let rebuilt: Experience = restored.into();
        assert_eq!(rebuilt.id, experience.id);
        assert_eq!(
            rebuilt.embedding, vector,
            "the record's own encoding still skips `embedding`; the wire type is what carries it"
        );

        // And the bare record still loses it, which is why the wrapper exists.
        let bare = postcard::to_allocvec(&experience).unwrap();
        let bare_back: Experience = postcard::from_bytes(&bare).unwrap();
        assert!(bare_back.embedding.is_empty());
    }

    /// A rejection detail never grows past the wire bound.
    #[test]
    fn recovery_v5_wire_detail_is_bounded() {
        let long = "x".repeat(MAX_WIRE_DETAIL_BYTES * 4);
        let reply: WireReply<PushAck> =
            WireReply::rejected(InstanceId::new(), WireErrorCode::InvalidRequest, long);
        match reply.result {
            WireResult::Rejected { ref detail, .. } => {
                assert_eq!(detail.len(), MAX_WIRE_DETAIL_BYTES)
            }
            ref other => panic!("expected a rejection, got {other:?}"),
        }
    }

    /// A reply from a responder other than the one addressed is a peer change,
    /// whatever its result says — a success produced by the wrong peer is not
    /// a success.
    #[test]
    fn recovery_v5_reply_from_another_responder_is_peer_changed() {
        let expected = InstanceId::new();
        let actual = InstanceId::new();
        let reply = WireReply::ok(
            actual,
            PushAck {
                wal_owner: expected,
                accepted: 1,
                rejected: 0,
                total: 1,
                safe_through: Some(1),
            },
        );
        let err = reply.into_result(expected).unwrap_err();
        assert!(err.is_peer_changed(), "got {err}");
    }
}
