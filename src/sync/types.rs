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
// SyncCursor — Tracks sync position per peer
// ============================================================================

/// Tracks the sync position for a specific peer instance.
///
/// Each peer maintains a cursor recording the last WAL sequence number
/// successfully synced. This enables incremental sync — only changes
/// after the cursor position are transferred.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncCursor {
    /// The peer instance this cursor tracks.
    pub instance_id: InstanceId,

    /// The last WAL sequence number successfully synced from/to this peer.
    pub last_sequence: u64,
}

impl SyncCursor {
    /// Creates a new cursor at sequence 0 (beginning of WAL).
    pub fn new(instance_id: InstanceId) -> Self {
        Self {
            instance_id,
            last_sequence: 0,
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
            related_files: update.related_files,
            archived: update.archived,
        }
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
    ExperienceCreated(Experience),

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
    pub capabilities: Vec<String>,
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
    /// Reason for rejection, if not accepted.
    pub reason: Option<String>,
}

// ============================================================================
// Pull request/response
// ============================================================================

/// Request to pull changes from a remote peer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PullRequest {
    /// The cursor position to pull changes from.
    pub cursor: SyncCursor,
    /// Maximum number of changes to return in this batch.
    pub batch_size: usize,
    /// Optional filter: only pull changes for these collectives.
    pub collectives: Option<Vec<CollectiveId>>,
}

/// Response containing pulled changes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PullResponse {
    /// The changes since the cursor position.
    pub changes: Vec<SyncChange>,
    /// Whether there are more changes available.
    pub has_more: bool,
    /// The updated cursor position after this batch.
    pub new_cursor: SyncCursor,
}

// ============================================================================
// Push response
// ============================================================================

/// Response after pushing changes to a remote peer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PushResponse {
    /// Number of changes accepted by the remote.
    pub accepted: usize,
    /// Number of changes rejected by the remote.
    pub rejected: usize,
    /// The remote's updated cursor position.
    pub new_cursor: SyncCursor,
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
    fn test_instance_id_bincode_roundtrip() {
        let id = InstanceId::new();
        let bytes = bincode::serialize(&id).unwrap();
        let restored: InstanceId = bincode::deserialize(&bytes).unwrap();
        assert_eq!(id, restored);
    }

    #[test]
    fn test_sync_cursor_new() {
        let id = InstanceId::new();
        let cursor = SyncCursor::new(id);
        assert_eq!(cursor.instance_id, id);
        assert_eq!(cursor.last_sequence, 0);
    }

    #[test]
    fn test_sync_cursor_bincode_roundtrip() {
        let cursor = SyncCursor {
            instance_id: InstanceId::new(),
            last_sequence: 42,
        };
        let bytes = bincode::serialize(&cursor).unwrap();
        let restored: SyncCursor = bincode::deserialize(&bytes).unwrap();
        assert_eq!(cursor, restored);
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
    fn test_serializable_experience_update_bincode_roundtrip() {
        let update = SerializableExperienceUpdate {
            importance: Some(0.7),
            confidence: Some(0.9),
            domain: Some(vec!["test".to_string()]),
            related_files: None,
            archived: Some(true),
            applications: Some(std::collections::BTreeMap::from([(InstanceId::new(), 2)])),
            last_reinforced: Some(Timestamp::now()),
        };
        let bytes = bincode::serialize(&update).unwrap();
        let restored: SerializableExperienceUpdate = bincode::deserialize(&bytes).unwrap();
        assert_eq!(update.importance, restored.importance);
        assert_eq!(update.confidence, restored.confidence);
        assert_eq!(update.domain, restored.domain);
        assert_eq!(update.archived, restored.archived);
        assert_eq!(update.applications, restored.applications);
        assert_eq!(update.last_reinforced, restored.last_reinforced);
    }

    #[test]
    fn test_sync_status_equality() {
        assert_eq!(SyncStatus::Idle, SyncStatus::Idle);
        assert_eq!(SyncStatus::Error("x".into()), SyncStatus::Error("x".into()));
        assert_ne!(SyncStatus::Idle, SyncStatus::Syncing);
    }

    #[test]
    fn test_handshake_request_bincode_roundtrip() {
        let req = HandshakeRequest {
            instance_id: InstanceId::new(),
            protocol_version: crate::sync::SYNC_PROTOCOL_VERSION,
            capabilities: vec!["push".to_string(), "pull".to_string()],
        };
        let bytes = bincode::serialize(&req).unwrap();
        let restored: HandshakeRequest = bincode::deserialize(&bytes).unwrap();
        assert_eq!(req.instance_id, restored.instance_id);
        assert_eq!(req.protocol_version, restored.protocol_version);
        assert_eq!(req.capabilities, restored.capabilities);
    }

    #[test]
    fn test_pull_request_bincode_roundtrip() {
        let req = PullRequest {
            cursor: SyncCursor::new(InstanceId::new()),
            batch_size: 500,
            collectives: Some(vec![CollectiveId::new()]),
        };
        let bytes = bincode::serialize(&req).unwrap();
        let restored: PullRequest = bincode::deserialize(&bytes).unwrap();
        assert_eq!(req.cursor, restored.cursor);
        assert_eq!(req.batch_size, restored.batch_size);
    }

    #[test]
    fn test_push_response_bincode_roundtrip() {
        let resp = PushResponse {
            accepted: 10,
            rejected: 2,
            new_cursor: SyncCursor {
                instance_id: InstanceId::new(),
                last_sequence: 100,
            },
        };
        let bytes = bincode::serialize(&resp).unwrap();
        let restored: PushResponse = bincode::deserialize(&bytes).unwrap();
        assert_eq!(resp.accepted, restored.accepted);
        assert_eq!(resp.rejected, restored.rejected);
        assert_eq!(resp.new_cursor, restored.new_cursor);
    }
}
