//! Storage layer abstractions for PulseDB.
//!
//! This module provides a trait-based abstraction over the storage engine,
//! allowing different backends to be used (e.g., redb, mock for testing).
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                      PulseDB                                 │
//! │                         │                                    │
//! │                         ▼                                    │
//! │              ┌─────────────────────┐                        │
//! │              │   StorageEngine     │  ← Trait               │
//! │              └─────────────────────┘                        │
//! │                    ▲         ▲                              │
//! │                    │         │                              │
//! │         ┌─────────┴─┐   ┌───┴─────────┐                    │
//! │         │RedbStorage│   │ MockStorage │                    │
//! │         └───────────┘   └─────────────┘                    │
//! │           (prod)           (test)                          │
//! └─────────────────────────────────────────────────────────────┘
//! ```

pub mod redb;
pub mod schema;

pub use self::redb::RedbStorage;
pub use schema::{DatabaseMetadata, SCHEMA_VERSION};

use std::collections::BTreeMap;
use std::path::Path;

use crate::activity::Activity;
use crate::collective::Collective;
use crate::config::{Config, DecayConfig};
use crate::error::Result;
use crate::experience::{Experience, ExperienceUpdate};
use crate::insight::DerivedInsight;
use crate::relation::{ExperienceRelation, RelationType};
use crate::types::{CollectiveId, ExperienceId, InsightId, InstanceId, RelationId, Timestamp};

/// Storage engine trait for PulseDB.
///
/// This trait defines the contract that any storage backend must implement.
/// The primary implementation is [`RedbStorage`], but other implementations
/// can be created for testing or alternative backends.
///
/// # Thread Safety
///
/// Implementations must be `Send + Sync` to allow the database to be shared
/// across threads. The engine handles internal synchronization.
///
/// # Example
///
/// ```rust
/// # fn main() -> pulsedb::Result<()> {
/// # let dir = tempfile::tempdir().unwrap();
/// use pulsedb::{Config, storage::{StorageEngine, RedbStorage}};
///
/// let config = Config::default();
/// let storage = RedbStorage::open(dir.path().join("test.db"), &config)?;
/// let metadata = storage.metadata();
/// println!("Schema version: {}", metadata.schema_version);
/// # Ok(())
/// # }
/// ```
pub trait StorageEngine: Send + Sync {
    // =========================================================================
    // Lifecycle
    // =========================================================================

    /// Returns the database metadata.
    ///
    /// The metadata includes schema version, embedding dimension, and timestamps.
    fn metadata(&self) -> &DatabaseMetadata;

    /// Closes the storage engine, flushing any pending writes.
    ///
    /// This method consumes the storage engine. After calling `close()`,
    /// the engine cannot be used.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend supports reporting flush failures.
    /// Note: the current redb backend flushes on drop (infallible), so
    /// this always returns `Ok(())` for [`RedbStorage`].
    fn close(self: Box<Self>) -> Result<()>;

    /// Returns the path to the database file, if applicable.
    ///
    /// Some storage implementations (like in-memory) may not have a path.
    fn path(&self) -> Option<&Path>;

    // =========================================================================
    // Collective Storage Operations
    // =========================================================================

    /// Saves a collective to storage.
    ///
    /// If a collective with the same ID already exists, it is overwritten.
    /// Each call opens and commits its own write transaction.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction or serialization fails.
    fn save_collective(&self, collective: &Collective) -> Result<()>;

    /// Retrieves a collective by ID.
    ///
    /// Returns `None` if no collective with the given ID exists.
    ///
    /// # Errors
    ///
    /// Returns an error if the read transaction or deserialization fails.
    fn get_collective(&self, id: CollectiveId) -> Result<Option<Collective>>;

    /// Lists all collectives in the database.
    ///
    /// Returns an empty vector if no collectives exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the read transaction or deserialization fails.
    fn list_collectives(&self) -> Result<Vec<Collective>>;

    /// Deletes a collective by ID.
    ///
    /// Returns `true` if the collective existed and was deleted,
    /// `false` if no collective with the given ID was found.
    ///
    /// # Errors
    ///
    /// Returns an error if the write transaction fails.
    fn delete_collective(&self, id: CollectiveId) -> Result<bool>;

    /// Retrieves the stored decay configuration for a collective.
    ///
    /// Returns `None` when no per-collective override has been stored; callers
    /// should fall back to [`DecayConfig::default`] or their global config.
    ///
    /// # Errors
    ///
    /// Returns an error if the read transaction or deserialization fails.
    fn get_decay_config(&self, collective_id: CollectiveId) -> Result<Option<DecayConfig>>;

    /// Saves the decay configuration override for a collective.
    ///
    /// # Errors
    ///
    /// Returns an error if the write transaction or serialization fails.
    fn set_decay_config(&self, collective_id: CollectiveId, config: DecayConfig) -> Result<()>;

    // =========================================================================
    // Experience Index Operations (for collective stats & cascade delete)
    // =========================================================================

    /// Counts experiences belonging to a collective.
    ///
    /// Queries the `experiences_by_collective` multimap index.
    /// Returns 0 if no experiences exist for the collective.
    ///
    /// # Errors
    ///
    /// Returns an error if the read transaction fails.
    fn count_experiences_in_collective(&self, id: CollectiveId) -> Result<u64>;

    /// Deletes all experiences and related index entries for a collective.
    ///
    /// Used for cascade deletion when a collective is removed. Cleans up:
    /// - Experience records
    /// - Embedding vectors
    /// - By-collective index entries
    /// - By-type index entries
    ///
    /// Returns the number of experiences deleted.
    ///
    /// # Errors
    ///
    /// Returns an error if the write transaction fails.
    fn delete_experiences_by_collective(&self, id: CollectiveId) -> Result<u64>;

    /// Lists all experience IDs belonging to a collective.
    ///
    /// Used to rebuild HNSW indexes from redb embeddings on startup.
    /// Iterates the `experiences_by_collective` multimap index.
    fn list_experience_ids_in_collective(&self, id: CollectiveId) -> Result<Vec<ExperienceId>>;

    /// Retrieves the most recent experience IDs in a collective.
    ///
    /// Performs a reverse iteration on `EXPERIENCES_BY_COLLECTIVE_TABLE`
    /// to get IDs ordered by timestamp descending (newest first).
    /// The multimap values are `[timestamp_be: 8 bytes][experience_id: 16 bytes]`,
    /// and since timestamps are big-endian, reverse lexicographic order = newest first.
    ///
    /// Returns `(ExperienceId, Timestamp)` pairs for the caller to fetch full
    /// records and apply post-filters.
    ///
    /// # Arguments
    ///
    /// * `collective_id` - The collective to query
    /// * `limit` - Maximum number of entries to return
    fn get_recent_experience_ids(
        &self,
        collective_id: CollectiveId,
        limit: usize,
    ) -> Result<Vec<(ExperienceId, Timestamp)>>;

    // =========================================================================
    // Experience Storage Operations
    // =========================================================================

    /// Saves an experience and its embedding to storage.
    ///
    /// Writes atomically to 4 tables in a single transaction:
    /// - `EXPERIENCES_TABLE` — the experience record (without embedding)
    /// - `EMBEDDINGS_TABLE` — the embedding vector as raw f32 bytes
    /// - `EXPERIENCES_BY_COLLECTIVE_TABLE` — secondary index by collective+timestamp
    /// - `EXPERIENCES_BY_TYPE_TABLE` — secondary index by collective+type
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction or serialization fails.
    fn save_experience(&self, experience: &Experience) -> Result<()>;

    /// Retrieves an experience by ID, including its embedding.
    ///
    /// Reads from both `EXPERIENCES_TABLE` and `EMBEDDINGS_TABLE` to
    /// reconstitute the full experience with embedding.
    ///
    /// Returns `None` if no experience with the given ID exists.
    fn get_experience(&self, id: ExperienceId) -> Result<Option<Experience>>;

    /// Updates mutable fields of an experience.
    ///
    /// Applies only the `Some` fields from the update. Immutable fields
    /// (content, embedding, collective_id, type) are not affected.
    ///
    /// Returns `true` if the experience existed and was updated,
    /// `false` if not found.
    fn update_experience(&self, id: ExperienceId, update: &ExperienceUpdate) -> Result<bool>;

    /// Merges synced G-counter fields into an experience.
    ///
    /// Each incoming applications bucket is merged with per-key max semantics.
    /// `last_reinforced`, when supplied, is merged by max timestamp. Returns
    /// `true` when the experience existed and was merged.
    #[cfg(feature = "sync")]
    fn merge_experience_applications(
        &self,
        id: ExperienceId,
        applications: &BTreeMap<InstanceId, u32>,
        last_reinforced: Option<Timestamp>,
    ) -> Result<bool>;

    /// Permanently deletes an experience and its embedding.
    ///
    /// Removes from all 4 tables in a single transaction.
    ///
    /// Returns `true` if the experience existed and was deleted,
    /// `false` if not found.
    fn delete_experience(&self, id: ExperienceId) -> Result<bool>;

    /// Atomically increments the applications counter for an experience.
    ///
    /// Performs a read-modify-write in a single write transaction to prevent
    /// lost updates under concurrent access. Uses saturating arithmetic
    /// (caps at `u32::MAX`, never panics).
    ///
    /// Returns `Some(new_count)` if the experience was found and updated,
    /// `None` if no experience with the given ID exists.
    fn reinforce_experience(&self, id: ExperienceId) -> Result<Option<u32>>;

    /// Saves an embedding vector to storage.
    ///
    /// The embedding is stored as raw little-endian f32 bytes.
    fn save_embedding(&self, id: ExperienceId, embedding: &[f32]) -> Result<()>;

    /// Retrieves an embedding vector by experience ID.
    ///
    /// Returns `None` if no embedding exists for the given ID.
    fn get_embedding(&self, id: ExperienceId) -> Result<Option<Vec<f32>>>;

    // =========================================================================
    // Relation Storage Operations (E3-S01)
    // =========================================================================

    /// Saves a relation and its index entries atomically.
    ///
    /// Writes to 3 tables in a single transaction:
    /// - `RELATIONS_TABLE` — the relation record
    /// - `RELATIONS_BY_SOURCE_TABLE` — index by source experience
    /// - `RELATIONS_BY_TARGET_TABLE` — index by target experience
    fn save_relation(&self, relation: &ExperienceRelation) -> Result<()>;

    /// Retrieves a relation by ID.
    ///
    /// Returns `None` if no relation with the given ID exists.
    fn get_relation(&self, id: RelationId) -> Result<Option<ExperienceRelation>>;

    /// Deletes a relation and its index entries atomically.
    ///
    /// Returns `true` if the relation existed and was deleted,
    /// `false` if not found.
    fn delete_relation(&self, id: RelationId) -> Result<bool>;

    /// Finds all relation IDs where the given experience is the source.
    ///
    /// Iterates the `RELATIONS_BY_SOURCE_TABLE` multimap for the experience.
    fn get_relation_ids_by_source(&self, experience_id: ExperienceId) -> Result<Vec<RelationId>>;

    /// Finds all relation IDs where the given experience is the target.
    ///
    /// Iterates the `RELATIONS_BY_TARGET_TABLE` multimap for the experience.
    fn get_relation_ids_by_target(&self, experience_id: ExperienceId) -> Result<Vec<RelationId>>;

    /// Deletes all relations where the given experience is source or target.
    ///
    /// Used for cascade deletion when an experience is removed.
    /// Returns the count of deleted relations.
    fn delete_relations_for_experience(&self, experience_id: ExperienceId) -> Result<u64>;

    /// Checks if a relation with the same (source, target, type) already exists.
    ///
    /// Scans the source index, loads each relation, and checks for a matching
    /// target and type. Efficient for the expected cardinality (few relations
    /// per experience).
    fn relation_exists(
        &self,
        source_id: ExperienceId,
        target_id: ExperienceId,
        relation_type: RelationType,
    ) -> Result<bool>;

    // =========================================================================
    // Insight Storage Operations (E3-S02)
    // =========================================================================

    /// Saves a derived insight and its index entries atomically.
    ///
    /// Writes to 2 tables in a single transaction:
    /// - `INSIGHTS_TABLE` — the insight record (with inline embedding)
    /// - `INSIGHTS_BY_COLLECTIVE_TABLE` — index by collective
    fn save_insight(&self, insight: &DerivedInsight) -> Result<()>;

    /// Retrieves a derived insight by ID.
    ///
    /// Returns `None` if no insight with the given ID exists.
    fn get_insight(&self, id: InsightId) -> Result<Option<DerivedInsight>>;

    /// Deletes a derived insight and its index entries atomically.
    ///
    /// Returns `true` if the insight existed and was deleted,
    /// `false` if not found.
    fn delete_insight(&self, id: InsightId) -> Result<bool>;

    /// Lists all insight IDs belonging to a collective.
    ///
    /// Used to rebuild HNSW indexes from stored insights on startup.
    /// Iterates the `INSIGHTS_BY_COLLECTIVE_TABLE` multimap.
    fn list_insight_ids_in_collective(&self, id: CollectiveId) -> Result<Vec<InsightId>>;

    /// Deletes all insights belonging to a collective.
    ///
    /// Used for cascade deletion when a collective is removed.
    /// Returns the count of deleted insights.
    fn delete_insights_by_collective(&self, id: CollectiveId) -> Result<u64>;

    // =========================================================================
    // Activity Storage Operations (E3-S03)
    // =========================================================================

    /// Saves an agent activity to storage (upsert).
    ///
    /// If an activity for the same `(collective_id, agent_id)` already exists,
    /// it is replaced. Uses the composite key encoding from `schema::encode_activity_key`.
    fn save_activity(&self, activity: &Activity) -> Result<()>;

    /// Retrieves an agent activity by agent ID and collective.
    ///
    /// Returns `None` if no activity exists for the given pair.
    fn get_activity(&self, agent_id: &str, collective_id: CollectiveId)
        -> Result<Option<Activity>>;

    /// Deletes an agent activity.
    ///
    /// Returns `true` if the activity existed and was deleted,
    /// `false` if no activity was found for the given pair.
    fn delete_activity(&self, agent_id: &str, collective_id: CollectiveId) -> Result<bool>;

    /// Lists all activities in a collective.
    ///
    /// Iterates the `ACTIVITIES_TABLE` and filters entries whose key
    /// starts with the collective's 16-byte ID. Returns activities in
    /// no guaranteed order.
    fn list_activities_in_collective(&self, collective_id: CollectiveId) -> Result<Vec<Activity>>;

    /// Deletes all activities belonging to a collective.
    ///
    /// Used for cascade deletion when a collective is removed.
    /// Returns the count of deleted activities.
    fn delete_activities_by_collective(&self, collective_id: CollectiveId) -> Result<u64>;

    // =========================================================================
    // Paginated List Operations (PulseVision)
    // =========================================================================

    /// Lists experience IDs in a collective with pagination.
    ///
    /// Returns IDs ordered by timestamp (oldest first). Use `offset` to skip
    /// previously fetched pages and `limit` to control page size.
    fn list_experience_ids_paginated(
        &self,
        collective_id: CollectiveId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ExperienceId>>;

    /// Lists all relations in a collective with pagination.
    ///
    /// Scans relations whose source experience belongs to the collective.
    fn list_relations_in_collective(
        &self,
        collective_id: CollectiveId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<crate::relation::ExperienceRelation>>;

    /// Lists insight IDs in a collective with pagination.
    fn list_insight_ids_paginated(
        &self,
        collective_id: CollectiveId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<InsightId>>;

    // =========================================================================
    // Watch Event Operations (E4-S02) — Cross-Process Change Detection
    // =========================================================================

    /// Returns the current WAL sequence number.
    ///
    /// Every experience write (create, update, delete, reinforce) atomically
    /// increments the sequence. Returns 0 if no writes have occurred yet.
    ///
    /// This is a read-only operation — the write-side logic is internal to the
    /// storage implementation to maintain transactional atomicity.
    fn get_wal_sequence(&self) -> Result<u64>;

    /// Retrieves watch events with sequence numbers greater than `since_seq`.
    ///
    /// Returns events in ascending sequence order and the highest sequence
    /// number seen. If no new events exist, returns an empty vec and `since_seq`.
    ///
    /// # Arguments
    ///
    /// * `since_seq` - Return events with sequence > this value (0 = all events)
    /// * `limit` - Maximum number of events to return per call
    fn poll_watch_events(
        &self,
        since_seq: u64,
        limit: usize,
    ) -> Result<(Vec<schema::WatchEventRecord>, u64)>;

    // =========================================================================
    // Sync Operations (feature: sync)
    // =========================================================================

    /// Retrieves ALL watch events (all entity types) with their sequence numbers.
    ///
    /// Unlike `poll_watch_events()` which returns records without sequences,
    /// this method returns `(sequence, record)` pairs needed by the sync
    /// pusher to construct `SyncChange` objects.
    #[cfg(feature = "sync")]
    fn poll_sync_events(
        &self,
        since_seq: u64,
        limit: usize,
    ) -> Result<Vec<(u64, schema::WatchEventRecord)>>;

    /// Returns the persistent instance ID for this database.
    ///
    /// Generated on first open and stable across restarts.
    /// Used by the sync protocol to identify this PulseDB instance.
    #[cfg(feature = "sync")]
    fn instance_id(&self) -> crate::sync::InstanceId;

    /// Saves a sync cursor for a peer instance.
    ///
    /// Upserts the cursor in the `SYNC_CURSORS_TABLE`.
    #[cfg(feature = "sync")]
    fn save_sync_cursor(&self, cursor: &crate::sync::SyncCursor) -> Result<()>;

    /// Loads the sync cursor for a specific peer instance.
    ///
    /// Returns `None` if no cursor has been saved for this peer.
    #[cfg(feature = "sync")]
    fn load_sync_cursor(
        &self,
        instance_id: &crate::sync::InstanceId,
    ) -> Result<Option<crate::sync::SyncCursor>>;

    /// Lists all saved sync cursors.
    ///
    /// Returns cursors for all known peer instances.
    #[cfg(feature = "sync")]
    fn list_sync_cursors(&self) -> Result<Vec<crate::sync::SyncCursor>>;

    /// Compacts the WAL by deleting events with sequence <= `up_to_seq`.
    ///
    /// Returns the number of events deleted. This is a write operation
    /// that permanently removes old WAL entries to reclaim disk space.
    ///
    /// # Safety
    ///
    /// Only compact up to the minimum cursor across all peers — otherwise
    /// peers that haven't synced yet will miss events.
    #[cfg(feature = "sync")]
    fn compact_wal_events(&self, up_to_seq: u64) -> Result<u64>;
}

/// Opens a storage engine at the given path.
///
/// This is a convenience function that creates a [`RedbStorage`] instance.
/// For more control, use `RedbStorage::open()` directly.
///
/// # Arguments
///
/// * `path` - Path to the database file (created if it doesn't exist)
/// * `config` - Database configuration
///
/// # Errors
///
/// Returns an error if:
/// - The database file is corrupted
/// - The database is locked by another process
/// - Schema version doesn't match
/// - Embedding dimension doesn't match (for existing databases)
pub fn open_storage(path: impl AsRef<Path>, config: &Config) -> Result<Box<dyn StorageEngine>> {
    let storage = RedbStorage::open(path, config)?;
    Ok(Box::new(storage))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::EmbeddingDimension;
    use tempfile::tempdir;

    #[test]
    fn test_open_storage() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let config = Config::default();
        let storage = open_storage(&path, &config).unwrap();

        assert_eq!(
            storage.metadata().embedding_dimension,
            EmbeddingDimension::D384
        );
        assert!(storage.path().is_some());

        storage.close().unwrap();
    }

    #[test]
    fn test_storage_engine_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<RedbStorage>();
    }
}
