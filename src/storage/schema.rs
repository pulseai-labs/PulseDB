//! Database schema definitions and versioning.
//!
//! This module defines the table structure for the redb storage engine.
//! All table definitions are compile-time constants to ensure consistency.
//!
//! # Schema Versioning
//!
//! The schema version is stored in the metadata table. When opening an
//! existing database, we check the version and fail if it doesn't match.
//! Migration support will be added in a future release.
//!
//! # Table Layout
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │ METADATA_TABLE                                               │
//! │   Key: &str                                                  │
//! │   Value: &[u8] (JSON for human-readable, bincode for data)  │
//! │   Entries: "db_metadata" -> DatabaseMetadata                 │
//! └─────────────────────────────────────────────────────────────┘
//!
//! ┌─────────────────────────────────────────────────────────────┐
//! │ COLLECTIVES_TABLE                                            │
//! │   Key: &[u8; 16] (CollectiveId as UUID bytes)               │
//! │   Value: &[u8] (bincode-serialized Collective)              │
//! └─────────────────────────────────────────────────────────────┘
//!
//! ┌─────────────────────────────────────────────────────────────┐
//! │ EXPERIENCES_TABLE                                            │
//! │   Key: &[u8; 16] (ExperienceId as UUID bytes)               │
//! │   Value: &[u8] (bincode-serialized Experience)              │
//! └─────────────────────────────────────────────────────────────┘
//! ```

use redb::{MultimapTableDefinition, TableDefinition};
use serde::{Deserialize, Serialize};

use crate::config::EmbeddingDimension;
use crate::types::Timestamp;

/// Current schema version.
///
/// Increment this when making breaking changes to the schema.
/// Version 2 adds `entity_type` to `WatchEventRecord` for sync protocol support.
pub const SCHEMA_VERSION: u32 = 2;

/// Maximum content size in bytes (100 KB).
pub const MAX_CONTENT_SIZE: usize = 100 * 1024;

/// Maximum number of domain tags per experience.
pub const MAX_DOMAIN_TAGS: usize = 50;

/// Maximum length of a single domain tag.
pub const MAX_TAG_LENGTH: usize = 100;

/// Maximum number of source files per experience.
pub const MAX_SOURCE_FILES: usize = 100;

/// Maximum length of a single source file path.
pub const MAX_FILE_PATH_LENGTH: usize = 500;

/// Maximum length of a source agent identifier.
pub const MAX_SOURCE_AGENT_LENGTH: usize = 256;

/// Maximum relation metadata size in bytes (10 KB).
pub const MAX_RELATION_METADATA_SIZE: usize = 10 * 1024;

/// Maximum insight content size in bytes (50 KB).
pub const MAX_INSIGHT_CONTENT_SIZE: usize = 50 * 1024;

/// Maximum number of source experiences per insight.
pub const MAX_INSIGHT_SOURCES: usize = 100;

/// Maximum agent ID length in bytes.
///
/// Agent IDs are UTF-8 strings identifying a specific AI agent instance.
/// 255 bytes is generous for identifiers like "claude-opus-4" or UUIDs.
pub const MAX_ACTIVITY_AGENT_ID_LENGTH: usize = 255;

/// Maximum size for activity optional fields (current_task, context_summary) in bytes (1 KB).
///
/// These fields are short descriptions, not full content — 1KB is sufficient
/// for a task name or brief context summary.
pub const MAX_ACTIVITY_FIELD_SIZE: usize = 1024;

// ============================================================================
// Table Definitions
// ============================================================================

/// Metadata table for database-level information.
///
/// Stores schema version, creation time, and other database-wide settings.
/// Key is a string identifier, value is serialized data.
pub const METADATA_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("metadata");

/// Collectives table.
///
/// Key: CollectiveId as 16-byte UUID
/// Value: bincode-serialized Collective struct
pub const COLLECTIVES_TABLE: TableDefinition<&[u8; 16], &[u8]> =
    TableDefinition::new("collectives");

/// Decay configuration table.
///
/// Key: CollectiveId as 16-byte UUID
/// Value: bincode-serialized per-collective decay configuration
pub const DECAY_CONFIGS_TABLE: TableDefinition<&[u8; 16], &[u8]> =
    TableDefinition::new("decay_configs");

/// Experiences table.
///
/// Key: ExperienceId as 16-byte UUID
/// Value: bincode-serialized Experience struct (without embedding)
pub const EXPERIENCES_TABLE: TableDefinition<&[u8; 16], &[u8]> =
    TableDefinition::new("experiences");

/// Index: Experiences by collective and timestamp.
///
/// Enables efficient queries like "recent experiences in collective X".
/// Key: CollectiveId as 16-byte UUID
/// Value (multimap): (Timestamp big-endian 8 bytes, ExperienceId 16 bytes) = 24 bytes
///
/// Using a multimap allows multiple experiences per collective. Values are
/// sorted lexicographically, so big-endian timestamps ensure time ordering.
pub const EXPERIENCES_BY_COLLECTIVE_TABLE: MultimapTableDefinition<&[u8; 16], &[u8; 24]> =
    MultimapTableDefinition::new("experiences_by_collective");

/// Index: Experiences by collective and type.
///
/// Enables efficient queries like "all ErrorPattern experiences in collective X".
/// Key: (CollectiveId bytes, ExperienceTypeTag byte) = 17 bytes
/// Value: ExperienceId as 16-byte UUID
///
/// Using a multimap allows multiple experiences of the same type.
pub const EXPERIENCES_BY_TYPE_TABLE: MultimapTableDefinition<&[u8; 17], &[u8; 16]> =
    MultimapTableDefinition::new("experiences_by_type");

/// Embeddings table.
///
/// Stored separately from experiences to keep the main table compact.
/// Key: ExperienceId as 16-byte UUID
/// Value: raw f32 bytes (dimension * 4 bytes)
pub const EMBEDDINGS_TABLE: TableDefinition<&[u8; 16], &[u8]> = TableDefinition::new("embeddings");

// ============================================================================
// Relation Tables (E3-S01)
// ============================================================================

/// Relations table.
///
/// Primary storage for experience relations.
/// Key: RelationId as 16-byte UUID
/// Value: bincode-serialized ExperienceRelation struct
pub const RELATIONS_TABLE: TableDefinition<&[u8; 16], &[u8]> = TableDefinition::new("relations");

/// Index: Relations by source experience.
///
/// Enables efficient queries like "find all outgoing relations from experience X".
/// Key: ExperienceId (source) as 16-byte UUID
/// Value (multimap): RelationId as 16-byte UUID
///
/// Multiple relations per source experience. Iterate values with
/// `table.get(source_id)?` to find all outgoing relation IDs.
pub const RELATIONS_BY_SOURCE_TABLE: MultimapTableDefinition<&[u8; 16], &[u8; 16]> =
    MultimapTableDefinition::new("relations_by_source");

/// Index: Relations by target experience.
///
/// Enables efficient queries like "find all incoming relations to experience X".
/// Key: ExperienceId (target) as 16-byte UUID
/// Value (multimap): RelationId as 16-byte UUID
pub const RELATIONS_BY_TARGET_TABLE: MultimapTableDefinition<&[u8; 16], &[u8; 16]> =
    MultimapTableDefinition::new("relations_by_target");

// ============================================================================
// Insight Tables (E3-S02)
// ============================================================================

/// Insights table.
///
/// Primary storage for derived insights.
/// Key: InsightId as 16-byte UUID
/// Value: bincode-serialized DerivedInsight struct (with inline embedding)
pub const INSIGHTS_TABLE: TableDefinition<&[u8; 16], &[u8]> = TableDefinition::new("insights");

/// Index: Insights by collective.
///
/// Enables efficient queries like "find all insights in collective X".
/// Key: CollectiveId as 16-byte UUID
/// Value (multimap): InsightId as 16-byte UUID
pub const INSIGHTS_BY_COLLECTIVE_TABLE: MultimapTableDefinition<&[u8; 16], &[u8; 16]> =
    MultimapTableDefinition::new("insights_by_collective");

// ============================================================================
// Activity Tables (E3-S03)
// ============================================================================

/// Activities table — agent presence tracking.
///
/// First PulseDB table using variable-length keys. Activities are keyed by
/// a composite `(collective_id, agent_id)` rather than a UUID, since each
/// agent can have at most one active session per collective.
///
/// Key: `[collective_id: 16B][agent_id_len: 2B BE][agent_id: NB]`
/// Value: bincode-serialized Activity struct
pub const ACTIVITIES_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("activities");

// ============================================================================
// Watch Events Tables (E4-S02)
// ============================================================================

// ============================================================================
// Sync Metadata (feature: sync)
// ============================================================================

/// Metadata key for the instance ID (16-byte UUID v7).
///
/// Stored in `METADATA_TABLE` as raw 16 bytes. Generated on first open
/// and persisted for the lifetime of the database. Used by the sync
/// protocol to identify this PulseDB instance.
#[cfg(feature = "sync")]
pub const INSTANCE_ID_KEY: &str = "instance_id";

/// Sync cursors table — per-peer sync position tracking.
///
/// Each entry records the last WAL sequence number successfully synced
/// with a specific peer instance. Key is the peer's InstanceId (16 bytes),
/// value is bincode-serialized `SyncCursor`.
#[cfg(feature = "sync")]
pub const SYNC_CURSORS_TABLE: TableDefinition<&[u8; 16], &[u8]> =
    TableDefinition::new("sync_cursors");

// ============================================================================
// Watch Events Tables (E4-S02)
// ============================================================================

/// Metadata key for the current WAL sequence number.
///
/// Stored in `METADATA_TABLE` as 8-byte big-endian `u64`.
/// Starts at 0 (no writes yet), incremented atomically within each
/// experience write transaction.
pub const WAL_SEQUENCE_KEY: &str = "wal_sequence";

/// Watch events table — cross-process change detection log.
///
/// Each experience mutation (create, update, archive, delete) records an
/// entry here with a monotonically increasing sequence number as the key.
/// Reader processes poll this table to discover changes made by the writer.
///
/// Key: u64 sequence number as 8-byte big-endian (lexicographic = numeric order)
/// Value: bincode-serialized `WatchEventRecord`
///
/// The table grows unboundedly; a future compaction feature will allow
/// trimming old entries.
pub const WATCH_EVENTS_TABLE: TableDefinition<&[u8; 8], &[u8]> =
    TableDefinition::new("watch_events");

/// A persisted watch event for cross-process change detection (schema v2).
///
/// This is the on-disk representation — compact and self-contained.
/// Converted to the public `WatchEvent` type when returned to callers.
///
/// Uses raw byte arrays for IDs (not UUID wrappers) to keep serialization
/// simple and avoid coupling the storage format to the public type system.
///
/// # Schema v2 Changes
///
/// In v1, this struct only tracked experiences (`experience_id`). In v2,
/// the field is renamed to `entity_id` and an `entity_type` discriminant
/// is added to track all entity types (relations, insights, collectives).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WatchEventRecord {
    /// The entity that changed (16-byte UUID).
    ///
    /// For experiences this is an ExperienceId, for relations a RelationId, etc.
    pub entity_id: [u8; 16],

    /// The collective this entity belongs to (16-byte UUID).
    pub collective_id: [u8; 16],

    /// What kind of change occurred.
    pub event_type: WatchEventTypeTag,

    /// When the change occurred (milliseconds since Unix epoch).
    pub timestamp_ms: i64,

    /// What kind of entity changed (schema v2).
    pub entity_type: EntityTypeTag,
}

/// Schema v1 watch event record (for migration deserialization only).
///
/// In v1, the WAL only tracked experience mutations. This struct matches
/// the v1 bincode layout for reading old records during migration.
#[derive(Deserialize)]
pub(crate) struct WatchEventRecordV1 {
    pub experience_id: [u8; 16],
    pub collective_id: [u8; 16],
    pub event_type: WatchEventTypeTag,
    pub timestamp_ms: i64,
}

/// Compact tag for watch event types stored on disk.
///
/// Mirrors `WatchEventType` from `watch/types.rs` but uses `repr(u8)` for
/// minimal storage footprint. Derives `Serialize`/`Deserialize` since it's
/// part of the bincode-serialized `WatchEventRecord`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum WatchEventTypeTag {
    /// A new experience was recorded.
    Created = 0,
    /// An existing experience was modified or reinforced.
    Updated = 1,
    /// An experience was soft-deleted (archived).
    Archived = 2,
    /// An experience was permanently deleted.
    Deleted = 3,
}

impl WatchEventTypeTag {
    /// Converts a raw byte to a WatchEventTypeTag.
    ///
    /// Returns `None` if the byte doesn't correspond to a known variant.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Created),
            1 => Some(Self::Updated),
            2 => Some(Self::Archived),
            3 => Some(Self::Deleted),
            _ => None,
        }
    }
}

// ============================================================================
// Entity Type Tag (schema v2)
// ============================================================================

/// Compact discriminant for entity types in WAL records.
///
/// Identifies what kind of entity a WAL event refers to. Added in schema v2
/// to extend WAL tracking beyond just experiences.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum EntityTypeTag {
    /// An experience entity.
    #[default]
    Experience = 0,
    /// A relation between experiences.
    Relation = 1,
    /// A derived insight.
    Insight = 2,
    /// A collective.
    Collective = 3,
}

impl EntityTypeTag {
    /// Converts a raw byte to an EntityTypeTag.
    ///
    /// Returns `None` if the byte doesn't correspond to a known variant.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Experience),
            1 => Some(Self::Relation),
            2 => Some(Self::Insight),
            3 => Some(Self::Collective),
            _ => None,
        }
    }
}

// ============================================================================
// Experience Type Tag
// ============================================================================

/// Compact discriminant for experience types, used in secondary index keys.
///
/// Each variant maps to a single byte (`repr(u8)`), making index keys small
/// and comparison fast. The full `ExperienceType` enum (with associated data)
/// lives in `experience/types.rs` and bridges to this tag via `type_tag()`.
///
/// # Variants (9, per ADR-004 / Data Model spec)
///
/// - `Difficulty` — Problem encountered by the agent
/// - `Solution` — Fix for a problem (can link to Difficulty)
/// - `ErrorPattern` — Reusable error signature + fix + prevention
/// - `SuccessPattern` — Proven approach with quality rating
/// - `UserPreference` — User preference with strength
/// - `ArchitecturalDecision` — Design decision with rationale
/// - `TechInsight` — Technical knowledge about a technology
/// - `Fact` — Verified factual statement with source
/// - `Generic` — Catch-all for uncategorized experiences
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum ExperienceTypeTag {
    /// Problem encountered by the agent.
    Difficulty = 0,
    /// Fix for a problem (can reference a Difficulty).
    Solution = 1,
    /// Reusable error signature with fix and prevention.
    ErrorPattern = 2,
    /// Proven approach with quality rating.
    SuccessPattern = 3,
    /// User preference with strength.
    UserPreference = 4,
    /// Design decision with rationale.
    ArchitecturalDecision = 5,
    /// Technical knowledge about a technology.
    TechInsight = 6,
    /// Verified factual statement with source.
    Fact = 7,
    /// Catch-all for uncategorized experiences.
    Generic = 8,
}

impl ExperienceTypeTag {
    /// Converts a raw byte to an ExperienceTypeTag.
    ///
    /// Returns `None` if the byte doesn't correspond to a known variant.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Difficulty),
            1 => Some(Self::Solution),
            2 => Some(Self::ErrorPattern),
            3 => Some(Self::SuccessPattern),
            4 => Some(Self::UserPreference),
            5 => Some(Self::ArchitecturalDecision),
            6 => Some(Self::TechInsight),
            7 => Some(Self::Fact),
            8 => Some(Self::Generic),
            _ => None,
        }
    }

    /// Returns all variants in discriminant order.
    pub fn all() -> &'static [Self] {
        &[
            Self::Difficulty,
            Self::Solution,
            Self::ErrorPattern,
            Self::SuccessPattern,
            Self::UserPreference,
            Self::ArchitecturalDecision,
            Self::TechInsight,
            Self::Fact,
            Self::Generic,
        ]
    }
}

// ============================================================================
// Database Metadata
// ============================================================================

/// Database metadata stored in the metadata table.
///
/// This is serialized with bincode and stored under the key "db_metadata".
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DatabaseMetadata {
    /// Schema version for compatibility checking.
    pub schema_version: u32,

    /// Embedding dimension configured for this database.
    ///
    /// Once set, this cannot be changed without recreating the database.
    pub embedding_dimension: EmbeddingDimension,

    /// Timestamp when the database was created.
    pub created_at: Timestamp,

    /// Last time the database was opened (updated on each open).
    pub last_opened_at: Timestamp,
}

impl DatabaseMetadata {
    /// Creates new metadata for a fresh database.
    pub fn new(embedding_dimension: EmbeddingDimension) -> Self {
        let now = Timestamp::now();
        Self {
            schema_version: SCHEMA_VERSION,
            embedding_dimension,
            created_at: now,
            last_opened_at: now,
        }
    }

    /// Updates the last_opened_at timestamp.
    pub fn touch(&mut self) {
        self.last_opened_at = Timestamp::now();
    }

    /// Checks if this metadata is compatible with the current schema.
    pub fn is_compatible(&self) -> bool {
        self.schema_version == SCHEMA_VERSION
    }
}

// ============================================================================
// Key Encoding Helpers
// ============================================================================

/// Encodes a (CollectiveId, Timestamp, ExperienceId) tuple for the index.
///
/// Format: [collective_id: 16 bytes][timestamp_be: 8 bytes] = 24 bytes
/// (ExperienceId is the multimap value, not part of the key)
///
/// Big-endian timestamp ensures lexicographic ordering matches time ordering.
#[inline]
pub fn encode_collective_timestamp_key(collective_id: &[u8; 16], timestamp: Timestamp) -> [u8; 24] {
    let mut key = [0u8; 24];
    key[..16].copy_from_slice(collective_id);
    key[16..24].copy_from_slice(&timestamp.to_be_bytes());
    key
}

/// Decodes the timestamp from a collective index key.
#[inline]
pub fn decode_timestamp_from_key(key: &[u8; 24]) -> Timestamp {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&key[16..24]);
    Timestamp::from_millis(i64::from_be_bytes(bytes))
}

/// Creates a range start key for querying experiences in a collective.
///
/// Uses timestamp 0 (Unix epoch) as the start. We don't support timestamps
/// before 1970 since that predates computers being useful for AI agents.
#[inline]
pub fn collective_range_start(collective_id: &[u8; 16]) -> [u8; 24] {
    encode_collective_timestamp_key(collective_id, Timestamp::from_millis(0))
}

/// Creates a range end key for querying experiences in a collective.
///
/// Uses maximum positive timestamp to include all experiences.
#[inline]
pub fn collective_range_end(collective_id: &[u8; 16]) -> [u8; 24] {
    encode_collective_timestamp_key(collective_id, Timestamp::from_millis(i64::MAX))
}

// ============================================================================
// Type Index Key Encoding
// ============================================================================

/// Encodes a (CollectiveId, ExperienceTypeTag) key for the type index.
///
/// Format: [collective_id: 16 bytes][type_tag: 1 byte] = 17 bytes
///
/// This key design allows efficient range queries: to find all experiences
/// of a given type in a collective, we do a point lookup on this 17-byte key
/// and iterate the multimap values (ExperienceIds).
#[inline]
pub fn encode_type_index_key(collective_id: &[u8; 16], type_tag: ExperienceTypeTag) -> [u8; 17] {
    let mut key = [0u8; 17];
    key[..16].copy_from_slice(collective_id);
    key[16] = type_tag as u8;
    key
}

/// Decodes the ExperienceTypeTag from a type index key.
///
/// Returns `None` if the tag byte doesn't correspond to a known variant.
#[inline]
pub fn decode_type_tag_from_key(key: &[u8; 17]) -> Option<ExperienceTypeTag> {
    ExperienceTypeTag::from_u8(key[16])
}

/// Decodes the CollectiveId bytes from a type index key.
#[inline]
pub fn decode_collective_from_type_key(key: &[u8; 17]) -> [u8; 16] {
    let mut id = [0u8; 16];
    id.copy_from_slice(&key[..16]);
    id
}

// ============================================================================
// Activity Key Encoding (E3-S03)
// ============================================================================

/// Encodes a `(collective_id, agent_id)` composite key for the activities table.
///
/// Format: `[collective_id: 16 bytes][agent_id_len: 2 bytes BE u16][agent_id: N bytes]`
///
/// The collective_id prefix allows efficient filtering by collective (prefix scan).
/// The 2-byte length field enables safe decoding of the variable-length agent_id.
#[inline]
pub fn encode_activity_key(collective_id: &[u8; 16], agent_id: &str) -> Vec<u8> {
    let agent_bytes = agent_id.as_bytes();
    let len = agent_bytes.len() as u16;
    let mut key = Vec::with_capacity(16 + 2 + agent_bytes.len());
    key.extend_from_slice(collective_id);
    key.extend_from_slice(&len.to_be_bytes());
    key.extend_from_slice(agent_bytes);
    key
}

/// Extracts the 16-byte CollectiveId from an activity key.
///
/// # Panics
///
/// Panics if the key is shorter than 16 bytes (should never happen with
/// properly encoded keys from `encode_activity_key`).
#[inline]
pub fn decode_collective_from_activity_key(key: &[u8]) -> [u8; 16] {
    let mut id = [0u8; 16];
    id.copy_from_slice(&key[..16]);
    id
}

/// Extracts the agent_id string from an activity key.
///
/// Reads the 2-byte length at offset 16, then slices the UTF-8 agent_id.
///
/// # Panics
///
/// Panics if the key is malformed (insufficient length or invalid UTF-8).
/// This should never happen with keys created by `encode_activity_key`.
#[inline]
pub fn decode_agent_id_from_activity_key(key: &[u8]) -> &str {
    let len = u16::from_be_bytes([key[16], key[17]]) as usize;
    std::str::from_utf8(&key[18..18 + len]).expect("activity key contains invalid UTF-8")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_version() {
        assert_eq!(SCHEMA_VERSION, 2);
    }

    #[test]
    fn test_database_metadata_new() {
        let meta = DatabaseMetadata::new(EmbeddingDimension::D384);
        assert_eq!(meta.schema_version, SCHEMA_VERSION);
        assert_eq!(meta.embedding_dimension, EmbeddingDimension::D384);
        assert!(meta.is_compatible());
    }

    #[test]
    fn test_database_metadata_touch() {
        let mut meta = DatabaseMetadata::new(EmbeddingDimension::D384);
        let original = meta.last_opened_at;
        std::thread::sleep(std::time::Duration::from_millis(1));
        meta.touch();
        assert!(meta.last_opened_at > original);
    }

    #[test]
    fn test_database_metadata_serialization() {
        let meta = DatabaseMetadata::new(EmbeddingDimension::D768);
        let bytes = bincode::serialize(&meta).unwrap();
        let restored: DatabaseMetadata = bincode::deserialize(&bytes).unwrap();
        assert_eq!(meta.schema_version, restored.schema_version);
        assert_eq!(meta.embedding_dimension, restored.embedding_dimension);
    }

    #[test]
    fn test_encode_collective_timestamp_key() {
        let collective_id = [1u8; 16];
        let timestamp = Timestamp::from_millis(1234567890);

        let key = encode_collective_timestamp_key(&collective_id, timestamp);

        assert_eq!(&key[..16], &collective_id);
        assert_eq!(decode_timestamp_from_key(&key), timestamp);
    }

    #[test]
    fn test_key_ordering() {
        let collective_id = [1u8; 16];
        let t1 = Timestamp::from_millis(1000);
        let t2 = Timestamp::from_millis(2000);

        let key1 = encode_collective_timestamp_key(&collective_id, t1);
        let key2 = encode_collective_timestamp_key(&collective_id, t2);

        // Lexicographic ordering should match timestamp ordering
        assert!(key1 < key2);
    }

    #[test]
    fn test_collective_range() {
        let collective_id = [42u8; 16];
        let start = collective_range_start(&collective_id);
        let end = collective_range_end(&collective_id);

        // Any timestamp should fall within this range
        let mid = encode_collective_timestamp_key(&collective_id, Timestamp::now());
        assert!(start <= mid);
        assert!(mid <= end);
    }

    // ====================================================================
    // ExperienceTypeTag tests
    // ====================================================================

    #[test]
    fn test_experience_type_tag_from_u8_roundtrip() {
        for tag in ExperienceTypeTag::all() {
            let byte = *tag as u8;
            let restored = ExperienceTypeTag::from_u8(byte).unwrap();
            assert_eq!(*tag, restored);
        }
    }

    #[test]
    fn test_experience_type_tag_from_u8_invalid() {
        assert!(ExperienceTypeTag::from_u8(255).is_none());
        assert!(ExperienceTypeTag::from_u8(9).is_none());
    }

    #[test]
    fn test_experience_type_tag_all_variants() {
        let all = ExperienceTypeTag::all();
        assert_eq!(all.len(), 9);
        assert_eq!(all[0], ExperienceTypeTag::Difficulty);
        assert_eq!(all[5], ExperienceTypeTag::ArchitecturalDecision);
        assert_eq!(all[8], ExperienceTypeTag::Generic);
    }

    #[test]
    fn test_experience_type_tag_bincode_roundtrip() {
        for tag in ExperienceTypeTag::all() {
            let bytes = bincode::serialize(tag).unwrap();
            let restored: ExperienceTypeTag = bincode::deserialize(&bytes).unwrap();
            assert_eq!(*tag, restored);
        }
    }

    // ====================================================================
    // Type index key encoding tests
    // ====================================================================

    #[test]
    fn test_encode_type_index_key_roundtrip() {
        let collective_id = [7u8; 16];
        let tag = ExperienceTypeTag::SuccessPattern;

        let key = encode_type_index_key(&collective_id, tag);

        assert_eq!(decode_collective_from_type_key(&key), collective_id);
        assert_eq!(decode_type_tag_from_key(&key), Some(tag));
    }

    #[test]
    fn test_type_index_key_different_types_produce_different_keys() {
        let collective_id = [1u8; 16];

        let key_obs = encode_type_index_key(&collective_id, ExperienceTypeTag::Difficulty);
        let key_les = encode_type_index_key(&collective_id, ExperienceTypeTag::SuccessPattern);

        assert_ne!(key_obs, key_les);
        // Same collective prefix
        assert_eq!(&key_obs[..16], &key_les[..16]);
        // Different type byte
        assert_ne!(key_obs[16], key_les[16]);
    }

    #[test]
    fn test_type_index_key_different_collectives_produce_different_keys() {
        let id_a = [1u8; 16];
        let id_b = [2u8; 16];
        let tag = ExperienceTypeTag::Solution;

        let key_a = encode_type_index_key(&id_a, tag);
        let key_b = encode_type_index_key(&id_b, tag);

        assert_ne!(key_a, key_b);
        // Same type byte
        assert_eq!(key_a[16], key_b[16]);
    }

    // ====================================================================
    // Activity key encoding tests (E3-S03)
    // ====================================================================

    #[test]
    fn test_activity_key_encode_decode_roundtrip() {
        let collective_id = [42u8; 16];
        let agent_id = "claude-opus";

        let key = encode_activity_key(&collective_id, agent_id);

        assert_eq!(decode_collective_from_activity_key(&key), collective_id);
        assert_eq!(decode_agent_id_from_activity_key(&key), agent_id);
    }

    #[test]
    fn test_activity_key_different_agents_produce_different_keys() {
        let collective_id = [1u8; 16];

        let key_a = encode_activity_key(&collective_id, "agent-alpha");
        let key_b = encode_activity_key(&collective_id, "agent-beta");

        assert_ne!(key_a, key_b);
        // Same collective prefix
        assert_eq!(&key_a[..16], &key_b[..16]);
    }

    #[test]
    fn test_activity_key_different_collectives_produce_different_keys() {
        let id_a = [1u8; 16];
        let id_b = [2u8; 16];

        let key_a = encode_activity_key(&id_a, "same-agent");
        let key_b = encode_activity_key(&id_b, "same-agent");

        assert_ne!(key_a, key_b);
        // Same agent_id suffix
        assert_eq!(
            decode_agent_id_from_activity_key(&key_a),
            decode_agent_id_from_activity_key(&key_b)
        );
    }

    #[test]
    fn test_activity_key_format() {
        let collective_id = [0xAB; 16];
        let agent_id = "hi";

        let key = encode_activity_key(&collective_id, agent_id);

        // 16 (collective) + 2 (len) + 2 (agent "hi") = 20 bytes
        assert_eq!(key.len(), 20);
        // Collective prefix
        assert_eq!(&key[..16], &[0xAB; 16]);
        // Length field (big-endian u16 = 2)
        assert_eq!(&key[16..18], &[0, 2]);
        // Agent ID bytes
        assert_eq!(&key[18..], b"hi");
    }
}
