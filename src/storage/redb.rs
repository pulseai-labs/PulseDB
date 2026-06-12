//! redb storage engine implementation.
//!
//! This module provides the primary storage backend for PulseDB using
//! [redb](https://docs.rs/redb), a pure Rust embedded key-value store.
//!
//! # Features
//!
//! - ACID transactions with MVCC
//! - Single-writer, multiple-reader concurrency
//! - Automatic crash recovery
//! - Zero external dependencies (pure Rust)
//!
//! # File Layout
//!
//! When you open a database at `./pulse.db`, redb creates:
//! - `./pulse.db` - Main database file
//! - `./pulse.db.lock` - Lock file for writer coordination (may not be visible)

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use ::redb::{Database, ReadableTable};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, instrument, warn};

use crate::activity::Activity;
use crate::collective::Collective;
use crate::config::DecayConfig;
use crate::experience::{Experience, ExperienceUpdate};
use crate::insight::DerivedInsight;
use crate::relation::{ExperienceRelation, RelationType};
use crate::types::{CollectiveId, ExperienceId, InsightId, InstanceId, RelationId, Timestamp};

#[cfg(feature = "sync")]
use super::schema::SYNC_CURSORS_TABLE;
use super::schema::{
    decode_collective_from_activity_key, encode_activity_key, encode_type_index_key,
    DatabaseMetadata, EntityTypeTag, ExperienceTypeTag, ExperienceV2, WatchEventRecord,
    WatchEventTypeTag, ACTIVITIES_TABLE, COLLECTIVES_TABLE, DECAY_CONFIGS_TABLE, EMBEDDINGS_TABLE,
    EXPERIENCES_BY_COLLECTIVE_TABLE, EXPERIENCES_BY_TYPE_TABLE, EXPERIENCES_TABLE,
    INSIGHTS_BY_COLLECTIVE_TABLE, INSIGHTS_TABLE, INSTANCE_ID_KEY, METADATA_TABLE,
    RELATIONS_BY_SOURCE_TABLE, RELATIONS_BY_TARGET_TABLE, RELATIONS_TABLE, SCHEMA_VERSION,
    WAL_SEQUENCE_KEY, WATCH_EVENTS_TABLE,
};
use super::StorageEngine;
use crate::config::{Config, EmbeddingDimension, RecallWeights};
use crate::error::{PulseDBError, Result, StorageError, ValidationError};

/// Metadata key in the metadata table.
const METADATA_KEY: &str = "db_metadata";

/// Deterministic sibling path retained before schema-v3 migrations.
fn pre_v3_backup_path(path: &Path) -> PathBuf {
    let mut backup = path.to_path_buf();
    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_else(|| "pulsedb.redb".into());
    backup.set_file_name(format!("{file_name}.pre-v3.bak"));
    backup
}

/// Reserved legacy bucket for scalar v2 application counts.
///
/// The bytes spell `PULSEDB_LEGACY__`; freshly minted UUIDv7 instance ids
/// cannot collide with this fixed non-v7 sentinel.
fn legacy_applications_instance_id() -> InstanceId {
    InstanceId::from_bytes(*b"PULSEDB_LEGACY__")
}

#[derive(Debug, Deserialize, Serialize)]
struct StoredDecayConfig {
    half_life_secs: u64,
    freq_weight: f32,
    floor: f32,
    auto_archive_below_floor: bool,
    default_recall_weights: Option<RecallWeights>,
}

impl From<&DecayConfig> for StoredDecayConfig {
    fn from(config: &DecayConfig) -> Self {
        Self {
            half_life_secs: config.half_life.as_secs(),
            freq_weight: config.freq_weight,
            floor: config.floor,
            auto_archive_below_floor: config.auto_archive_below_floor,
            default_recall_weights: config.default_recall_weights,
        }
    }
}

impl From<StoredDecayConfig> for DecayConfig {
    fn from(config: StoredDecayConfig) -> Self {
        Self {
            half_life: std::time::Duration::from_secs(config.half_life_secs),
            freq_weight: config.freq_weight,
            floor: config.floor,
            auto_archive_below_floor: config.auto_archive_below_floor,
            default_recall_weights: config.default_recall_weights,
        }
    }
}

/// redb storage engine wrapper.
///
/// This struct holds the redb database handle and cached metadata.
/// It implements [`StorageEngine`] for use with PulseDB.
///
/// # Thread Safety
///
/// `RedbStorage` is `Send + Sync`. redb handles internal synchronization
/// using MVCC for readers and exclusive locking for writers.
#[derive(Debug)]
pub struct RedbStorage {
    /// The redb database handle.
    db: Database,

    /// Cached database metadata.
    metadata: DatabaseMetadata,

    /// Path to the database file.
    path: PathBuf,

    /// Persistent instance ID for local G-counter buckets and sync protocol.
    instance_id: InstanceId,
}

impl RedbStorage {
    /// Opens or creates a database at the given path.
    ///
    /// If the database doesn't exist, it will be created and initialized
    /// with the configuration settings. If it exists, the configuration
    /// will be validated against the stored metadata.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the database file
    /// * `config` - Database configuration
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The database file is corrupted
    /// - The database is locked by another process
    /// - Schema version doesn't match
    /// - Embedding dimension doesn't match (for existing databases)
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> pulsedb::Result<()> {
    /// # let dir = tempfile::tempdir().unwrap();
    /// use pulsedb::{Config, storage::RedbStorage};
    ///
    /// let storage = RedbStorage::open(dir.path().join("test.db"), &Config::default())?;
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(skip(config), fields(path = %path.as_ref().display()))]
    pub fn open(path: impl AsRef<Path>, config: &Config) -> Result<Self> {
        let path = path.as_ref();
        let db_exists = path.exists();

        debug!(db_exists = db_exists, "Opening storage engine");

        // Create or open the database
        let db = Self::create_database(path, config)?;

        if db_exists {
            // Validate existing database
            Self::open_existing(db, path.to_path_buf(), config)
        } else {
            // Initialize new database
            Self::initialize_new(db, path.to_path_buf(), config)
        }
    }

    /// Creates the redb database with appropriate settings.
    fn create_database(path: &Path, _config: &Config) -> Result<Database> {
        let builder = Database::builder();

        // Note: redb 2.x doesn't have set_cache_size, it manages memory internally
        // The cache_size_mb config will be used for future optimizations

        // Note: redb doesn't expose a typed error variant for lock conflicts,
        // so we detect them via error message string matching. This may need
        // updating if redb changes its error messages in a future version.
        let db = builder.create(path).map_err(|e| {
            if e.to_string().contains("locked") {
                StorageError::DatabaseLocked
            } else {
                StorageError::Redb(e.to_string())
            }
        })?;

        debug!("Database file opened successfully");
        Ok(db)
    }

    /// Initializes a new database with tables and metadata.
    #[instrument(skip(db, config), fields(path = %path.display()))]
    fn initialize_new(db: Database, path: PathBuf, config: &Config) -> Result<Self> {
        info!("Initializing new database");

        let metadata = DatabaseMetadata::new(config.embedding_dimension);

        // Create all tables and write metadata in a single transaction
        let write_txn = db.begin_write().map_err(StorageError::from)?;

        {
            // Create the metadata table and write metadata
            let mut meta_table = write_txn.open_table(METADATA_TABLE)?;
            let metadata_bytes = bincode::serialize(&metadata)
                .map_err(|e| StorageError::serialization(e.to_string()))?;
            meta_table.insert(METADATA_KEY, metadata_bytes.as_slice())?;

            // Create other tables (they're created on first access)
            let _ = write_txn.open_table(COLLECTIVES_TABLE)?;
            let _ = write_txn.open_table(DECAY_CONFIGS_TABLE)?;
            let _ = write_txn.open_table(EXPERIENCES_TABLE)?;
            let _ = write_txn.open_table(EMBEDDINGS_TABLE)?;
            let _ = write_txn.open_multimap_table(EXPERIENCES_BY_COLLECTIVE_TABLE)?;
            let _ = write_txn.open_multimap_table(EXPERIENCES_BY_TYPE_TABLE)?;
            let _ = write_txn.open_table(RELATIONS_TABLE)?;
            let _ = write_txn.open_multimap_table(RELATIONS_BY_SOURCE_TABLE)?;
            let _ = write_txn.open_multimap_table(RELATIONS_BY_TARGET_TABLE)?;
            let _ = write_txn.open_table(INSIGHTS_TABLE)?;
            let _ = write_txn.open_multimap_table(INSIGHTS_BY_COLLECTIVE_TABLE)?;
            let _ = write_txn.open_table(ACTIVITIES_TABLE)?;
            let _ = write_txn.open_table(WATCH_EVENTS_TABLE)?;

            let instance_id = InstanceId::new();
            meta_table.insert(INSTANCE_ID_KEY, instance_id.as_bytes().as_slice())?;

            #[cfg(feature = "sync")]
            {
                let _ = write_txn.open_table(SYNC_CURSORS_TABLE)?;
            }
        }

        write_txn.commit().map_err(StorageError::from)?;

        let instance_id = {
            let read_txn = db.begin_read().map_err(StorageError::from)?;
            let meta_table = read_txn.open_table(METADATA_TABLE)?;
            let entry = meta_table
                .get(INSTANCE_ID_KEY)?
                .ok_or_else(|| StorageError::corrupted("Missing instance_id after init"))?;
            let bytes: [u8; 16] = entry
                .value()
                .try_into()
                .map_err(|_| StorageError::corrupted("invalid instance_id bytes"))?;
            InstanceId::from_bytes(bytes)
        };

        info!(
            schema_version = SCHEMA_VERSION,
            dimension = config.embedding_dimension.size(),
            "Database initialized"
        );

        Ok(Self {
            db,
            metadata,
            path,
            instance_id,
        })
    }

    /// Opens and validates an existing database.
    #[instrument(skip(db, config), fields(path = %path.display()))]
    fn open_existing(db: Database, path: PathBuf, config: &Config) -> Result<Self> {
        info!("Opening existing database");

        // Read metadata from the database
        let read_txn = db.begin_read().map_err(StorageError::from)?;

        let metadata = {
            let meta_table = read_txn.open_table(METADATA_TABLE).map_err(|e| {
                StorageError::corrupted(format!("Cannot open metadata table: {}", e))
            })?;

            let metadata_bytes = meta_table
                .get(METADATA_KEY)
                .map_err(StorageError::from)?
                .ok_or_else(|| StorageError::corrupted("Missing database metadata"))?;

            bincode::deserialize::<DatabaseMetadata>(metadata_bytes.value())
                .map_err(|e| StorageError::corrupted(format!("Invalid metadata format: {}", e)))?
        };

        drop(read_txn);

        // Validate schema version (allow migration from v1 → v2 → v3).
        if !matches!(metadata.schema_version, 1..=SCHEMA_VERSION) {
            warn!(
                expected = SCHEMA_VERSION,
                found = metadata.schema_version,
                "Schema version mismatch"
            );
            return Err(PulseDBError::Storage(StorageError::SchemaVersionMismatch {
                expected: SCHEMA_VERSION,
                found: metadata.schema_version,
            }));
        }
        let needs_v2_migration = metadata.schema_version == 1;
        let needs_v3_migration = metadata.schema_version <= 2;

        // Validate embedding dimension
        if metadata.embedding_dimension != config.embedding_dimension {
            warn!(
                expected = config.embedding_dimension.size(),
                found = metadata.embedding_dimension.size(),
                "Embedding dimension mismatch"
            );
            return Err(PulseDBError::Validation(
                ValidationError::DimensionMismatch {
                    expected: config.embedding_dimension.size(),
                    got: metadata.embedding_dimension.size(),
                },
            ));
        }

        if needs_v3_migration && config.read_only {
            return Err(PulseDBError::ReadOnly);
        }

        if needs_v3_migration {
            std::fs::copy(&path, pre_v3_backup_path(&path)).map_err(PulseDBError::Io)?;
        }

        // Update last_opened_at timestamp and bump schema version if migrating.
        let mut metadata = metadata;
        metadata.touch();
        if needs_v2_migration || needs_v3_migration {
            metadata.schema_version = SCHEMA_VERSION;
        }

        let write_txn = db.begin_write().map_err(StorageError::from)?;
        {
            // Ensure watch_events table exists (migration for pre-E4-S02 databases)
            let _ = write_txn.open_table(WATCH_EVENTS_TABLE)?;
            let _ = write_txn.open_table(DECAY_CONFIGS_TABLE)?;

            // Migrate WAL records from v1 → v2 (add entity_type field)
            if needs_v2_migration {
                Self::migrate_wal_v1_to_v2(&write_txn)?;
                info!("Migrated WAL records from schema v1 to v2");
            }

            let mut meta_table = write_txn.open_table(METADATA_TABLE)?;

            if meta_table.get(INSTANCE_ID_KEY)?.is_none() {
                let instance_id = InstanceId::new();
                meta_table.insert(INSTANCE_ID_KEY, instance_id.as_bytes().as_slice())?;
                debug!("Generated new instance_id for existing database");
            }

            #[cfg(feature = "sync")]
            {
                let _ = write_txn.open_table(SYNC_CURSORS_TABLE)?;
            }

            drop(meta_table);

            if needs_v3_migration {
                Self::migrate_experiences_v2_to_v3(&write_txn)?;
                info!("Migrated experiences from schema v2 to v3");
            }

            let mut meta_table = write_txn.open_table(METADATA_TABLE)?;
            let metadata_bytes = bincode::serialize(&metadata)
                .map_err(|e| StorageError::serialization(e.to_string()))?;
            meta_table.insert(METADATA_KEY, metadata_bytes.as_slice())?;
        }
        write_txn.commit().map_err(StorageError::from)?;

        let instance_id = {
            let read_txn = db.begin_read().map_err(StorageError::from)?;
            let meta_table = read_txn.open_table(METADATA_TABLE)?;
            let entry = meta_table
                .get(INSTANCE_ID_KEY)?
                .ok_or_else(|| StorageError::corrupted("Missing instance_id"))?;
            let bytes: [u8; 16] = entry
                .value()
                .try_into()
                .map_err(|_| StorageError::corrupted("invalid instance_id bytes"))?;
            InstanceId::from_bytes(bytes)
        };

        info!(
            schema_version = metadata.schema_version,
            dimension = metadata.embedding_dimension.size(),
            "Database opened successfully"
        );

        Ok(Self {
            db,
            metadata,
            path,
            instance_id,
        })
    }

    /// Returns a reference to the underlying redb database.
    ///
    /// This is for internal use by other PulseDB modules.
    #[inline]
    #[allow(dead_code)] // Used by Collective CRUD (E1-S02) and Experience CRUD (E1-S03)
    pub(crate) fn database(&self) -> &Database {
        &self.db
    }

    /// Increments the WAL sequence and records a watch event within an existing write transaction.
    ///
    /// This is the core of cross-process change detection. By executing within the caller's
    /// transaction, the sequence increment and event record are atomic with the data mutation:
    /// if the transaction commits, both are durable; if it rolls back, neither is visible.
    ///
    /// # Arguments
    ///
    /// * `write_txn` - The caller's open write transaction
    /// * `entity_id` - The entity that changed (16-byte UUID)
    /// * `collective_id` - The collective it belongs to
    /// * `entity_type` - What kind of entity changed (Experience, Relation, etc.)
    /// * `event_type` - What kind of change occurred (Created, Updated, etc.)
    /// * `timestamp` - When the change occurred
    fn increment_wal_and_record(
        &self,
        write_txn: &::redb::WriteTransaction,
        entity_id: &[u8; 16],
        collective_id: CollectiveId,
        entity_type: EntityTypeTag,
        event_type: WatchEventTypeTag,
        timestamp: Timestamp,
    ) -> Result<u64> {
        // Echo prevention: skip WAL recording when applying sync changes
        #[cfg(feature = "sync")]
        if crate::sync::guard::is_sync_applying() {
            return Ok(0);
        }

        // Read current sequence (0 if key doesn't exist yet)
        let mut meta_table = write_txn.open_table(METADATA_TABLE)?;
        let current_seq = match meta_table.get(WAL_SEQUENCE_KEY)? {
            Some(entry) => {
                let bytes: [u8; 8] = entry
                    .value()
                    .try_into()
                    .map_err(|_| StorageError::corrupted("invalid wal_sequence bytes"))?;
                u64::from_be_bytes(bytes)
            }
            None => 0,
        };
        let new_seq = current_seq + 1;

        // Write new sequence number
        let seq_bytes = new_seq.to_be_bytes();
        meta_table.insert(WAL_SEQUENCE_KEY, seq_bytes.as_slice())?;

        // Record the event (schema v2 with entity_type)
        let record = WatchEventRecord {
            entity_id: *entity_id,
            collective_id: *collective_id.as_bytes(),
            event_type,
            timestamp_ms: timestamp.as_millis(),
            entity_type,
        };
        let record_bytes =
            bincode::serialize(&record).map_err(|e| StorageError::serialization(e.to_string()))?;

        let mut events_table = write_txn.open_table(WATCH_EVENTS_TABLE)?;
        events_table.insert(&seq_bytes, record_bytes.as_slice())?;

        Ok(new_seq)
    }

    /// Migrates WAL records from schema v1 to v2.
    ///
    /// V1 records have 4 fields: experience_id, collective_id, event_type, timestamp_ms.
    /// V2 adds entity_type (defaults to Experience for existing records).
    fn migrate_wal_v1_to_v2(write_txn: &::redb::WriteTransaction) -> Result<()> {
        use super::schema::WatchEventRecordV1;

        let events_table = write_txn.open_table(WATCH_EVENTS_TABLE)?;

        // Collect all (key, v1_record) pairs
        let mut entries: Vec<([u8; 8], WatchEventRecordV1)> = Vec::new();
        for entry in events_table.iter()? {
            let (key, value) = entry.map_err(StorageError::from)?;
            let seq_bytes: [u8; 8] = *key.value();
            let v1_record: WatchEventRecordV1 = bincode::deserialize(value.value())
                .map_err(|e| StorageError::serialization(format!("v1 WAL record: {}", e)))?;
            entries.push((seq_bytes, v1_record));
        }
        drop(events_table);

        // Rewrite as v2 records
        let mut events_table = write_txn.open_table(WATCH_EVENTS_TABLE)?;
        for (seq_bytes, v1) in &entries {
            let v2 = WatchEventRecord {
                entity_id: v1.experience_id,
                collective_id: v1.collective_id,
                event_type: v1.event_type,
                timestamp_ms: v1.timestamp_ms,
                entity_type: EntityTypeTag::Experience,
            };
            let v2_bytes =
                bincode::serialize(&v2).map_err(|e| StorageError::serialization(e.to_string()))?;
            events_table.insert(seq_bytes, v2_bytes.as_slice())?;
        }

        debug!(count = entries.len(), "Migrated WAL records to v2");
        Ok(())
    }

    /// Migrates experience records from schema v2 to v3.
    ///
    /// V2 stores scalar application counts. V3 stores a per-instance G-counter
    /// and maps every legacy scalar into the reserved LEGACY bucket to avoid
    /// double-counting already-synced replicas in later merge logic.
    fn migrate_experiences_v2_to_v3(write_txn: &::redb::WriteTransaction) -> Result<()> {
        let experiences_table = write_txn.open_table(EXPERIENCES_TABLE)?;

        let mut entries: Vec<([u8; 16], ExperienceV2)> = Vec::new();
        for entry in experiences_table.iter()? {
            let (key, value) = entry.map_err(StorageError::from)?;
            let experience_id = *key.value();
            let experience: ExperienceV2 = bincode::deserialize(value.value())
                .map_err(|e| StorageError::serialization(format!("v2 experience record: {}", e)))?;
            entries.push((experience_id, experience));
        }
        drop(experiences_table);

        let mut experiences_table = write_txn.open_table(EXPERIENCES_TABLE)?;
        for (experience_id, v2) in entries {
            let mut applications = BTreeMap::new();
            applications.insert(legacy_applications_instance_id(), v2.applications);

            let v3 = Experience {
                id: v2.id,
                collective_id: v2.collective_id,
                content: v2.content,
                embedding: v2.embedding,
                experience_type: v2.experience_type,
                importance: v2.importance,
                confidence: v2.confidence,
                applications,
                domain: v2.domain,
                related_files: v2.related_files,
                source_agent: v2.source_agent,
                source_task: v2.source_task,
                timestamp: v2.timestamp,
                last_reinforced: v2.timestamp,
                archived: v2.archived,
            };
            let bytes =
                bincode::serialize(&v3).map_err(|e| StorageError::serialization(e.to_string()))?;
            experiences_table.insert(&experience_id, bytes.as_slice())?;
        }

        debug!("Migrated experience records to v3");
        Ok(())
    }

    /// Returns the embedding dimension configured for this database.
    #[inline]
    pub fn embedding_dimension(&self) -> EmbeddingDimension {
        self.metadata.embedding_dimension
    }
}

impl StorageEngine for RedbStorage {
    // =========================================================================
    // Lifecycle
    // =========================================================================

    fn metadata(&self) -> &DatabaseMetadata {
        &self.metadata
    }

    #[instrument(skip(self))]
    fn close(self: Box<Self>) -> Result<()> {
        info!("Closing storage engine");

        // redb flushes all data durably on drop. Since `Database::drop` is
        // infallible, this method currently always returns Ok(()). The Result
        // return type is retained for API forward-compatibility if a future
        // storage backend can report flush errors.
        drop(self.db);

        info!("Storage engine closed");
        Ok(())
    }

    fn path(&self) -> Option<&Path> {
        Some(&self.path)
    }

    // =========================================================================
    // Collective Storage Operations
    // =========================================================================

    fn save_collective(&self, collective: &Collective) -> Result<()> {
        let bytes = bincode::serialize(collective)
            .map_err(|e| StorageError::serialization(e.to_string()))?;

        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        {
            let mut table = write_txn.open_table(COLLECTIVES_TABLE)?;
            table.insert(collective.id.as_bytes(), bytes.as_slice())?;
        }
        self.increment_wal_and_record(
            &write_txn,
            collective.id.as_bytes(),
            collective.id,
            EntityTypeTag::Collective,
            WatchEventTypeTag::Created,
            collective.created_at,
        )?;
        write_txn.commit().map_err(StorageError::from)?;

        debug!(id = %collective.id, name = %collective.name, "Collective saved");
        Ok(())
    }

    fn get_collective(&self, id: CollectiveId) -> Result<Option<Collective>> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_table(COLLECTIVES_TABLE)?;

        match table.get(id.as_bytes())? {
            Some(value) => {
                let collective: Collective = bincode::deserialize(value.value())
                    .map_err(|e| StorageError::serialization(e.to_string()))?;
                Ok(Some(collective))
            }
            None => Ok(None),
        }
    }

    fn list_collectives(&self) -> Result<Vec<Collective>> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_table(COLLECTIVES_TABLE)?;

        let mut collectives = Vec::new();
        for result in table.iter()? {
            let (_, value) = result.map_err(StorageError::from)?;
            let collective: Collective = bincode::deserialize(value.value())
                .map_err(|e| StorageError::serialization(e.to_string()))?;
            collectives.push(collective);
        }

        Ok(collectives)
    }

    fn delete_collective(&self, id: CollectiveId) -> Result<bool> {
        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        let existed;
        {
            let mut table = write_txn.open_table(COLLECTIVES_TABLE)?;
            existed = table.remove(id.as_bytes())?.is_some();
        }
        write_txn.commit().map_err(StorageError::from)?;

        if existed {
            debug!(id = %id, "Collective deleted");
        }
        Ok(existed)
    }

    fn get_decay_config(&self, collective_id: CollectiveId) -> Result<Option<DecayConfig>> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_table(DECAY_CONFIGS_TABLE)?;

        match table.get(collective_id.as_bytes())? {
            Some(value) => {
                let stored: StoredDecayConfig = bincode::deserialize(value.value())
                    .map_err(|e| StorageError::serialization(e.to_string()))?;
                Ok(Some(stored.into()))
            }
            None => Ok(None),
        }
    }

    fn set_decay_config(&self, collective_id: CollectiveId, config: DecayConfig) -> Result<()> {
        let stored = StoredDecayConfig::from(&config);
        let bytes =
            bincode::serialize(&stored).map_err(|e| StorageError::serialization(e.to_string()))?;

        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        {
            let mut table = write_txn.open_table(DECAY_CONFIGS_TABLE)?;
            table.insert(collective_id.as_bytes(), bytes.as_slice())?;
        }
        write_txn.commit().map_err(StorageError::from)?;

        debug!(collective_id = %collective_id, "Decay config saved");
        Ok(())
    }

    // =========================================================================
    // Experience Index Operations
    // =========================================================================

    fn count_experiences_in_collective(&self, id: CollectiveId) -> Result<u64> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_multimap_table(EXPERIENCES_BY_COLLECTIVE_TABLE)?;

        let count = table.get(id.as_bytes())?.count() as u64;

        Ok(count)
    }

    fn delete_experiences_by_collective(&self, id: CollectiveId) -> Result<u64> {
        // Phase 1: Read — collect experience IDs and relation IDs to delete
        let (exp_ids, relation_ids): (Vec<[u8; 16]>, Vec<[u8; 16]>) = {
            let read_txn = self.db.begin_read().map_err(StorageError::from)?;
            let table = read_txn.open_multimap_table(EXPERIENCES_BY_COLLECTIVE_TABLE)?;

            let mut ids = Vec::new();
            for result in table.get(id.as_bytes())? {
                let value = result.map_err(StorageError::from)?;
                let entry = value.value();
                // Entry is [timestamp: 8 bytes][experience_id: 16 bytes]
                let mut exp_id = [0u8; 16];
                exp_id.copy_from_slice(&entry[8..24]);
                ids.push(exp_id);
            }

            // Collect all relation IDs for these experiences (deduplicated)
            let mut rel_ids = std::collections::HashSet::new();
            let source_table = read_txn.open_multimap_table(RELATIONS_BY_SOURCE_TABLE)?;
            let target_table = read_txn.open_multimap_table(RELATIONS_BY_TARGET_TABLE)?;
            for exp_id in &ids {
                for result in source_table.get(exp_id)? {
                    let value = result.map_err(StorageError::from)?;
                    rel_ids.insert(*value.value());
                }
                for result in target_table.get(exp_id)? {
                    let value = result.map_err(StorageError::from)?;
                    rel_ids.insert(*value.value());
                }
            }

            (ids, rel_ids.into_iter().collect())
        };

        let count = exp_ids.len() as u64;
        if count == 0 {
            return Ok(0);
        }

        // Phase 2: Write — delete from all tables in a single transaction
        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        {
            // Delete experience records
            let mut exp_table = write_txn.open_table(EXPERIENCES_TABLE)?;
            for exp_id in &exp_ids {
                exp_table.remove(exp_id)?;
            }
        }
        {
            // Delete embedding vectors
            let mut emb_table = write_txn.open_table(EMBEDDINGS_TABLE)?;
            for exp_id in &exp_ids {
                emb_table.remove(exp_id)?;
            }
        }
        {
            // Clear the by-collective index for this collective
            let mut idx_table = write_txn.open_multimap_table(EXPERIENCES_BY_COLLECTIVE_TABLE)?;
            idx_table.remove_all(id.as_bytes())?;
        }
        {
            // Clear the by-type index for all type variants of this collective
            let mut type_table = write_txn.open_multimap_table(EXPERIENCES_BY_TYPE_TABLE)?;
            for tag in ExperienceTypeTag::all() {
                let key = encode_type_index_key(id.as_bytes(), *tag);
                type_table.remove_all(&key)?;
            }
        }
        {
            // Delete relations and their index entries
            if !relation_ids.is_empty() {
                let mut rel_table = write_txn.open_table(RELATIONS_TABLE)?;
                let mut source_idx = write_txn.open_multimap_table(RELATIONS_BY_SOURCE_TABLE)?;
                let mut target_idx = write_txn.open_multimap_table(RELATIONS_BY_TARGET_TABLE)?;

                // Clear relation indexes for all affected experiences
                for exp_id in &exp_ids {
                    source_idx.remove_all(exp_id)?;
                    target_idx.remove_all(exp_id)?;
                }
                // Delete the relation records themselves
                for rel_id in &relation_ids {
                    rel_table.remove(rel_id)?;
                }

                debug!(
                    count = relation_ids.len(),
                    "Cascade-deleted relations for collective"
                );
            }
        }
        write_txn.commit().map_err(StorageError::from)?;

        debug!(id = %id, count = count, "Cascade-deleted experiences for collective");
        Ok(count)
    }

    fn list_experience_ids_in_collective(&self, id: CollectiveId) -> Result<Vec<ExperienceId>> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_multimap_table(EXPERIENCES_BY_COLLECTIVE_TABLE)?;

        let mut ids = Vec::new();
        for result in table.get(id.as_bytes())? {
            let value = result.map_err(StorageError::from)?;
            let entry = value.value();
            // Entry is [timestamp: 8 bytes][experience_id: 16 bytes]
            let mut exp_bytes = [0u8; 16];
            exp_bytes.copy_from_slice(&entry[8..24]);
            ids.push(ExperienceId::from_bytes(exp_bytes));
        }

        Ok(ids)
    }

    fn get_recent_experience_ids(
        &self,
        collective_id: CollectiveId,
        limit: usize,
    ) -> Result<Vec<(ExperienceId, Timestamp)>> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_multimap_table(EXPERIENCES_BY_COLLECTIVE_TABLE)?;

        // Collect all (ExperienceId, Timestamp) pairs for this collective.
        // Multimap values are sorted ascending by [timestamp_be][exp_id],
        // so we collect all and then take from the end for newest-first.
        let mut entries = Vec::new();
        for result in table.get(collective_id.as_bytes())? {
            let value = result.map_err(StorageError::from)?;
            let entry = value.value();
            // Entry layout: [timestamp_be: 8 bytes][experience_id: 16 bytes]
            let mut ts_bytes = [0u8; 8];
            ts_bytes.copy_from_slice(&entry[..8]);
            let timestamp = Timestamp::from_millis(i64::from_be_bytes(ts_bytes));

            let mut exp_bytes = [0u8; 16];
            exp_bytes.copy_from_slice(&entry[8..24]);
            entries.push((ExperienceId::from_bytes(exp_bytes), timestamp));
        }

        // Take the last `limit` entries (newest) and reverse to get descending order
        let start = entries.len().saturating_sub(limit);
        let mut recent = entries.split_off(start);
        recent.reverse();

        Ok(recent)
    }

    // =========================================================================
    // Experience Storage Operations
    // =========================================================================

    fn save_experience(&self, experience: &Experience) -> Result<()> {
        // Serialize experience (embedding is #[serde(skip)], excluded automatically)
        let exp_bytes = bincode::serialize(experience)
            .map_err(|e| StorageError::serialization(e.to_string()))?;

        // Convert embedding to raw little-endian bytes
        let emb_bytes = f32_slice_to_bytes(&experience.embedding);

        // Build index keys
        let type_key = encode_type_index_key(
            experience.collective_id.as_bytes(),
            experience.experience_type.type_tag(),
        );

        // Write to all 4 tables in a single atomic transaction
        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        {
            // Main experience record
            let mut exp_table = write_txn.open_table(EXPERIENCES_TABLE)?;
            exp_table.insert(experience.id.as_bytes(), exp_bytes.as_slice())?;
        }
        {
            // Embedding vector (stored separately for compactness)
            let mut emb_table = write_txn.open_table(EMBEDDINGS_TABLE)?;
            emb_table.insert(experience.id.as_bytes(), emb_bytes.as_slice())?;
        }
        {
            // By-collective index: key=collective_id, value=timestamp+experience_id
            let mut idx_table = write_txn.open_multimap_table(EXPERIENCES_BY_COLLECTIVE_TABLE)?;
            // Value is [timestamp_be: 8 bytes][experience_id: 16 bytes] = 24 bytes
            let mut value = [0u8; 24];
            value[..8].copy_from_slice(&experience.timestamp.to_be_bytes());
            value[8..24].copy_from_slice(experience.id.as_bytes());
            idx_table.insert(experience.collective_id.as_bytes(), &value)?;
        }
        {
            // By-type index: key=collective_id+type_tag, value=experience_id
            let mut type_table = write_txn.open_multimap_table(EXPERIENCES_BY_TYPE_TABLE)?;
            type_table.insert(&type_key, experience.id.as_bytes())?;
        }
        // Record WAL event for cross-process change detection
        self.increment_wal_and_record(
            &write_txn,
            experience.id.as_bytes(),
            experience.collective_id,
            EntityTypeTag::Experience,
            WatchEventTypeTag::Created,
            experience.timestamp,
        )?;
        write_txn.commit().map_err(StorageError::from)?;

        debug!(
            id = %experience.id,
            collective_id = %experience.collective_id,
            "Experience saved"
        );
        Ok(())
    }

    fn get_experience(&self, id: ExperienceId) -> Result<Option<Experience>> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;

        // Read main experience record
        let exp_table = read_txn.open_table(EXPERIENCES_TABLE)?;
        let exp_entry = match exp_table.get(id.as_bytes())? {
            Some(v) => v,
            None => return Ok(None),
        };

        let mut experience: Experience = bincode::deserialize(exp_entry.value())
            .map_err(|e| StorageError::serialization(e.to_string()))?;

        // Read embedding from separate table and reconstitute
        let emb_table = read_txn.open_table(EMBEDDINGS_TABLE)?;
        if let Some(emb_entry) = emb_table.get(id.as_bytes())? {
            experience.embedding = bytes_to_f32_vec(emb_entry.value());
        }

        Ok(Some(experience))
    }

    fn update_experience(&self, id: ExperienceId, update: &ExperienceUpdate) -> Result<bool> {
        // Read-modify-write: read the current record, apply updates, write back
        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        let collective_id;
        let timestamp;
        let is_archive;
        {
            let mut exp_table = write_txn.open_table(EXPERIENCES_TABLE)?;

            let entry = match exp_table.get(id.as_bytes())? {
                Some(v) => v,
                None => return Ok(false),
            };

            let mut experience: Experience = bincode::deserialize(entry.value())
                .map_err(|e| StorageError::serialization(e.to_string()))?;

            // Drop the borrow on entry before mutating the table
            drop(entry);

            // Capture metadata for WAL event before applying updates
            collective_id = experience.collective_id;
            timestamp = experience.timestamp;
            is_archive = update.archived == Some(true);

            // Apply updates (only Some fields)
            if let Some(importance) = update.importance {
                experience.importance = importance;
            }
            if let Some(confidence) = update.confidence {
                experience.confidence = confidence;
            }
            if let Some(ref domain) = update.domain {
                experience.domain = domain.clone();
            }
            if let Some(ref related_files) = update.related_files {
                experience.related_files = related_files.clone();
            }
            if let Some(archived) = update.archived {
                experience.archived = archived;
            }

            // Re-serialize and write back
            let bytes = bincode::serialize(&experience)
                .map_err(|e| StorageError::serialization(e.to_string()))?;
            exp_table.insert(id.as_bytes(), bytes.as_slice())?;
        }
        // Record WAL event for cross-process change detection
        let event_type = if is_archive {
            WatchEventTypeTag::Archived
        } else {
            WatchEventTypeTag::Updated
        };
        self.increment_wal_and_record(
            &write_txn,
            id.as_bytes(),
            collective_id,
            EntityTypeTag::Experience,
            event_type,
            timestamp,
        )?;
        write_txn.commit().map_err(StorageError::from)?;

        debug!(id = %id, "Experience updated");
        Ok(true)
    }

    #[cfg(feature = "sync")]
    fn merge_experience_applications(
        &self,
        id: ExperienceId,
        applications: &BTreeMap<InstanceId, u32>,
        last_reinforced: Option<Timestamp>,
    ) -> Result<bool> {
        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        let found = {
            let mut exp_table = write_txn.open_table(EXPERIENCES_TABLE)?;
            let entry = match exp_table.get(id.as_bytes())? {
                Some(v) => v,
                None => return Ok(false),
            };

            let mut experience: Experience = bincode::deserialize(entry.value())
                .map_err(|e| StorageError::serialization(e.to_string()))?;
            drop(entry);

            for (instance_id, count) in applications {
                let bucket = experience.applications.entry(*instance_id).or_insert(0);
                *bucket = (*bucket).max(*count);
            }

            if let Some(incoming) = last_reinforced {
                experience.last_reinforced = experience.last_reinforced.max(incoming);
            }

            let bytes = bincode::serialize(&experience)
                .map_err(|e| StorageError::serialization(e.to_string()))?;
            exp_table.insert(id.as_bytes(), bytes.as_slice())?;
            true
        };

        write_txn.commit().map_err(StorageError::from)?;
        debug!(id = %id, "Experience applications merged");
        Ok(found)
    }

    fn delete_experience(&self, id: ExperienceId) -> Result<bool> {
        // First read the experience to get collective_id, timestamp, and type_tag
        // (needed for cleaning up secondary indices and WAL event)
        let (collective_id, timestamp, type_tag) = {
            let read_txn = self.db.begin_read().map_err(StorageError::from)?;
            let exp_table = read_txn.open_table(EXPERIENCES_TABLE)?;

            match exp_table.get(id.as_bytes())? {
                Some(entry) => {
                    let exp: Experience = bincode::deserialize(entry.value())
                        .map_err(|e| StorageError::serialization(e.to_string()))?;
                    (
                        exp.collective_id,
                        exp.timestamp,
                        exp.experience_type.type_tag(),
                    )
                }
                None => return Ok(false),
            }
        };

        // Delete from all 4 tables in a single transaction
        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        {
            let mut exp_table = write_txn.open_table(EXPERIENCES_TABLE)?;
            exp_table.remove(id.as_bytes())?;
        }
        {
            let mut emb_table = write_txn.open_table(EMBEDDINGS_TABLE)?;
            emb_table.remove(id.as_bytes())?;
        }
        {
            // Remove specific entry from by-collective multimap
            let mut idx_table = write_txn.open_multimap_table(EXPERIENCES_BY_COLLECTIVE_TABLE)?;
            let mut value = [0u8; 24];
            value[..8].copy_from_slice(&timestamp.to_be_bytes());
            value[8..24].copy_from_slice(id.as_bytes());
            idx_table.remove(collective_id.as_bytes(), &value)?;
        }
        {
            // Remove specific entry from by-type multimap
            let mut type_table = write_txn.open_multimap_table(EXPERIENCES_BY_TYPE_TABLE)?;
            let type_key = encode_type_index_key(collective_id.as_bytes(), type_tag);
            type_table.remove(&type_key, id.as_bytes())?;
        }
        // Record WAL event for cross-process change detection
        self.increment_wal_and_record(
            &write_txn,
            id.as_bytes(),
            collective_id,
            EntityTypeTag::Experience,
            WatchEventTypeTag::Deleted,
            timestamp,
        )?;
        write_txn.commit().map_err(StorageError::from)?;

        debug!(id = %id, "Experience deleted");
        Ok(true)
    }

    fn reinforce_experience(&self, id: ExperienceId) -> Result<Option<u32>> {
        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        let (new_count, collective_id, timestamp) = {
            let mut exp_table = write_txn.open_table(EXPERIENCES_TABLE)?;

            let entry = match exp_table.get(id.as_bytes())? {
                Some(v) => v,
                None => return Ok(None),
            };

            let mut experience: Experience = bincode::deserialize(entry.value())
                .map_err(|e| StorageError::serialization(e.to_string()))?;
            drop(entry);

            let bucket = experience.applications.entry(self.instance_id).or_insert(0);
            *bucket = bucket.saturating_add(1);
            experience.last_reinforced = Timestamp::now();
            let new_count = experience.applications();
            let collective_id = experience.collective_id;
            let timestamp = experience.timestamp;

            let bytes = bincode::serialize(&experience)
                .map_err(|e| StorageError::serialization(e.to_string()))?;
            exp_table.insert(id.as_bytes(), bytes.as_slice())?;
            (new_count, collective_id, timestamp)
        };
        // Record WAL event for cross-process change detection
        self.increment_wal_and_record(
            &write_txn,
            id.as_bytes(),
            collective_id,
            EntityTypeTag::Experience,
            WatchEventTypeTag::Updated,
            timestamp,
        )?;
        write_txn.commit().map_err(StorageError::from)?;

        debug!(id = %id, applications = new_count, "Experience reinforced");
        Ok(Some(new_count))
    }

    fn save_embedding(&self, id: ExperienceId, embedding: &[f32]) -> Result<()> {
        let bytes = f32_slice_to_bytes(embedding);

        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        {
            let mut table = write_txn.open_table(EMBEDDINGS_TABLE)?;
            table.insert(id.as_bytes(), bytes.as_slice())?;
        }
        write_txn.commit().map_err(StorageError::from)?;

        debug!(id = %id, dim = embedding.len(), "Embedding saved");
        Ok(())
    }

    fn get_embedding(&self, id: ExperienceId) -> Result<Option<Vec<f32>>> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_table(EMBEDDINGS_TABLE)?;

        match table.get(id.as_bytes())? {
            Some(entry) => Ok(Some(bytes_to_f32_vec(entry.value()))),
            None => Ok(None),
        }
    }

    // =========================================================================
    // Relation Storage Operations (E3-S01)
    // =========================================================================

    fn save_relation(&self, relation: &ExperienceRelation) -> Result<()> {
        let bytes =
            bincode::serialize(relation).map_err(|e| StorageError::serialization(e.to_string()))?;

        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        {
            let mut table = write_txn.open_table(RELATIONS_TABLE)?;
            table.insert(relation.id.as_bytes(), bytes.as_slice())?;
        }
        {
            let mut table = write_txn.open_multimap_table(RELATIONS_BY_SOURCE_TABLE)?;
            table.insert(relation.source_id.as_bytes(), relation.id.as_bytes())?;
        }
        {
            let mut table = write_txn.open_multimap_table(RELATIONS_BY_TARGET_TABLE)?;
            table.insert(relation.target_id.as_bytes(), relation.id.as_bytes())?;
        }
        // Look up collective_id from source experience for WAL record
        let collective_id = {
            let exp_table = write_txn.open_table(EXPERIENCES_TABLE)?;
            let entry = exp_table
                .get(relation.source_id.as_bytes())?
                .ok_or_else(|| {
                    StorageError::corrupted("relation source experience not found for WAL record")
                })?;
            let exp: Experience = bincode::deserialize(entry.value())
                .map_err(|e| StorageError::serialization(e.to_string()))?;
            exp.collective_id
        };
        self.increment_wal_and_record(
            &write_txn,
            relation.id.as_bytes(),
            collective_id,
            EntityTypeTag::Relation,
            WatchEventTypeTag::Created,
            relation.created_at,
        )?;
        write_txn.commit().map_err(StorageError::from)?;

        debug!(id = %relation.id, "Relation saved");
        Ok(())
    }

    fn get_relation(&self, id: RelationId) -> Result<Option<ExperienceRelation>> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_table(RELATIONS_TABLE)?;

        match table.get(id.as_bytes())? {
            Some(value) => {
                let relation: ExperienceRelation = bincode::deserialize(value.value())
                    .map_err(|e| StorageError::serialization(e.to_string()))?;
                Ok(Some(relation))
            }
            None => Ok(None),
        }
    }

    fn delete_relation(&self, id: RelationId) -> Result<bool> {
        // Read the relation first to get source/target IDs for index cleanup
        // and source experience's collective_id for WAL record
        let (source_id, target_id, collective_id) = {
            let read_txn = self.db.begin_read().map_err(StorageError::from)?;
            let rel_table = read_txn.open_table(RELATIONS_TABLE)?;

            match rel_table.get(id.as_bytes())? {
                Some(entry) => {
                    let rel: ExperienceRelation = bincode::deserialize(entry.value())
                        .map_err(|e| StorageError::serialization(e.to_string()))?;
                    // Look up collective_id from source experience
                    let exp_table = read_txn.open_table(EXPERIENCES_TABLE)?;
                    let cid = match exp_table.get(rel.source_id.as_bytes())? {
                        Some(exp_entry) => {
                            let exp: Experience = bincode::deserialize(exp_entry.value())
                                .map_err(|e| StorageError::serialization(e.to_string()))?;
                            exp.collective_id
                        }
                        // Source experience may have been deleted; use nil collective
                        None => CollectiveId::nil(),
                    };
                    (rel.source_id, rel.target_id, cid)
                }
                None => return Ok(false),
            }
        };

        // Delete from all 3 tables atomically
        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        {
            let mut table = write_txn.open_table(RELATIONS_TABLE)?;
            table.remove(id.as_bytes())?;
        }
        {
            let mut table = write_txn.open_multimap_table(RELATIONS_BY_SOURCE_TABLE)?;
            table.remove(source_id.as_bytes(), id.as_bytes())?;
        }
        {
            let mut table = write_txn.open_multimap_table(RELATIONS_BY_TARGET_TABLE)?;
            table.remove(target_id.as_bytes(), id.as_bytes())?;
        }
        self.increment_wal_and_record(
            &write_txn,
            id.as_bytes(),
            collective_id,
            EntityTypeTag::Relation,
            WatchEventTypeTag::Deleted,
            Timestamp::now(),
        )?;
        write_txn.commit().map_err(StorageError::from)?;

        debug!(id = %id, "Relation deleted");
        Ok(true)
    }

    fn get_relation_ids_by_source(&self, experience_id: ExperienceId) -> Result<Vec<RelationId>> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_multimap_table(RELATIONS_BY_SOURCE_TABLE)?;

        let mut ids = Vec::new();
        for result in table.get(experience_id.as_bytes())? {
            let value = result.map_err(StorageError::from)?;
            let bytes = value.value();
            ids.push(RelationId::from_bytes(*bytes));
        }

        Ok(ids)
    }

    fn get_relation_ids_by_target(&self, experience_id: ExperienceId) -> Result<Vec<RelationId>> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_multimap_table(RELATIONS_BY_TARGET_TABLE)?;

        let mut ids = Vec::new();
        for result in table.get(experience_id.as_bytes())? {
            let value = result.map_err(StorageError::from)?;
            let bytes = value.value();
            ids.push(RelationId::from_bytes(*bytes));
        }

        Ok(ids)
    }

    fn delete_relations_for_experience(&self, experience_id: ExperienceId) -> Result<u64> {
        // Phase 1: Read — collect all relation IDs from both indexes
        let relation_ids: Vec<RelationId> = {
            let read_txn = self.db.begin_read().map_err(StorageError::from)?;
            let source_table = read_txn.open_multimap_table(RELATIONS_BY_SOURCE_TABLE)?;
            let target_table = read_txn.open_multimap_table(RELATIONS_BY_TARGET_TABLE)?;

            let mut ids = std::collections::HashSet::new();

            // Outgoing relations (this experience is source)
            for result in source_table.get(experience_id.as_bytes())? {
                let value = result.map_err(StorageError::from)?;
                ids.insert(RelationId::from_bytes(*value.value()));
            }

            // Incoming relations (this experience is target)
            for result in target_table.get(experience_id.as_bytes())? {
                let value = result.map_err(StorageError::from)?;
                ids.insert(RelationId::from_bytes(*value.value()));
            }

            ids.into_iter().collect()
        };

        let count = relation_ids.len() as u64;
        if count == 0 {
            return Ok(0);
        }

        // Phase 2: Read each relation to get source/target IDs for index cleanup
        let relations: Vec<ExperienceRelation> = {
            let read_txn = self.db.begin_read().map_err(StorageError::from)?;
            let table = read_txn.open_table(RELATIONS_TABLE)?;

            let mut rels = Vec::with_capacity(relation_ids.len());
            for rel_id in &relation_ids {
                if let Some(entry) = table.get(rel_id.as_bytes())? {
                    let rel: ExperienceRelation = bincode::deserialize(entry.value())
                        .map_err(|e| StorageError::serialization(e.to_string()))?;
                    rels.push(rel);
                }
            }
            rels
        };

        // Phase 3: Write — delete from all 3 tables atomically
        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        {
            let mut rel_table = write_txn.open_table(RELATIONS_TABLE)?;
            for rel in &relations {
                rel_table.remove(rel.id.as_bytes())?;
            }
        }
        {
            let mut source_table = write_txn.open_multimap_table(RELATIONS_BY_SOURCE_TABLE)?;
            for rel in &relations {
                source_table.remove(rel.source_id.as_bytes(), rel.id.as_bytes())?;
            }
        }
        {
            let mut target_table = write_txn.open_multimap_table(RELATIONS_BY_TARGET_TABLE)?;
            for rel in &relations {
                target_table.remove(rel.target_id.as_bytes(), rel.id.as_bytes())?;
            }
        }
        write_txn.commit().map_err(StorageError::from)?;

        debug!(
            experience_id = %experience_id,
            count = count,
            "Cascade-deleted relations for experience"
        );
        Ok(count)
    }

    fn relation_exists(
        &self,
        source_id: ExperienceId,
        target_id: ExperienceId,
        relation_type: RelationType,
    ) -> Result<bool> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let index_table = read_txn.open_multimap_table(RELATIONS_BY_SOURCE_TABLE)?;
        let rel_table = read_txn.open_table(RELATIONS_TABLE)?;

        // Scan all relations for this source and check each
        for result in index_table.get(source_id.as_bytes())? {
            let value = result.map_err(StorageError::from)?;
            let rel_id = RelationId::from_bytes(*value.value());

            if let Some(entry) = rel_table.get(rel_id.as_bytes())? {
                let rel: ExperienceRelation = bincode::deserialize(entry.value())
                    .map_err(|e| StorageError::serialization(e.to_string()))?;
                if rel.target_id == target_id && rel.relation_type == relation_type {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    // =========================================================================
    // Insight Storage Operations (E3-S02)
    // =========================================================================

    fn save_insight(&self, insight: &DerivedInsight) -> Result<()> {
        let bytes =
            bincode::serialize(insight).map_err(|e| StorageError::serialization(e.to_string()))?;

        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        {
            let mut table = write_txn.open_table(INSIGHTS_TABLE)?;
            table.insert(insight.id.as_bytes(), bytes.as_slice())?;
        }
        {
            let mut table = write_txn.open_multimap_table(INSIGHTS_BY_COLLECTIVE_TABLE)?;
            table.insert(insight.collective_id.as_bytes(), insight.id.as_bytes())?;
        }
        self.increment_wal_and_record(
            &write_txn,
            insight.id.as_bytes(),
            insight.collective_id,
            EntityTypeTag::Insight,
            WatchEventTypeTag::Created,
            insight.created_at,
        )?;
        write_txn.commit().map_err(StorageError::from)?;

        debug!(id = %insight.id, collective_id = %insight.collective_id, "Insight saved");
        Ok(())
    }

    fn get_insight(&self, id: InsightId) -> Result<Option<DerivedInsight>> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_table(INSIGHTS_TABLE)?;

        match table.get(id.as_bytes())? {
            Some(value) => {
                let insight: DerivedInsight = bincode::deserialize(value.value())
                    .map_err(|e| StorageError::serialization(e.to_string()))?;
                Ok(Some(insight))
            }
            None => Ok(None),
        }
    }

    fn delete_insight(&self, id: InsightId) -> Result<bool> {
        // Read the insight first to get collective_id for index cleanup
        let collective_id = {
            let read_txn = self.db.begin_read().map_err(StorageError::from)?;
            let table = read_txn.open_table(INSIGHTS_TABLE)?;

            match table.get(id.as_bytes())? {
                Some(entry) => {
                    let insight: DerivedInsight = bincode::deserialize(entry.value())
                        .map_err(|e| StorageError::serialization(e.to_string()))?;
                    insight.collective_id
                }
                None => return Ok(false),
            }
        };

        // Delete from both tables atomically
        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        {
            let mut table = write_txn.open_table(INSIGHTS_TABLE)?;
            table.remove(id.as_bytes())?;
        }
        {
            let mut table = write_txn.open_multimap_table(INSIGHTS_BY_COLLECTIVE_TABLE)?;
            table.remove(collective_id.as_bytes(), id.as_bytes())?;
        }
        self.increment_wal_and_record(
            &write_txn,
            id.as_bytes(),
            collective_id,
            EntityTypeTag::Insight,
            WatchEventTypeTag::Deleted,
            Timestamp::now(),
        )?;
        write_txn.commit().map_err(StorageError::from)?;

        debug!(id = %id, "Insight deleted");
        Ok(true)
    }

    fn list_insight_ids_in_collective(&self, id: CollectiveId) -> Result<Vec<InsightId>> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_multimap_table(INSIGHTS_BY_COLLECTIVE_TABLE)?;

        let mut ids = Vec::new();
        for result in table.get(id.as_bytes())? {
            let value = result.map_err(StorageError::from)?;
            ids.push(InsightId::from_bytes(*value.value()));
        }

        Ok(ids)
    }

    fn delete_insights_by_collective(&self, id: CollectiveId) -> Result<u64> {
        // Phase 1: Read — collect insight IDs
        let insight_ids: Vec<[u8; 16]> = {
            let read_txn = self.db.begin_read().map_err(StorageError::from)?;
            let table = read_txn.open_multimap_table(INSIGHTS_BY_COLLECTIVE_TABLE)?;

            let mut ids = Vec::new();
            for result in table.get(id.as_bytes())? {
                let value = result.map_err(StorageError::from)?;
                ids.push(*value.value());
            }
            ids
        };

        let count = insight_ids.len() as u64;
        if count == 0 {
            return Ok(0);
        }

        // Phase 2: Write — delete from both tables atomically
        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        {
            let mut table = write_txn.open_table(INSIGHTS_TABLE)?;
            for insight_id in &insight_ids {
                table.remove(insight_id)?;
            }
        }
        {
            let mut table = write_txn.open_multimap_table(INSIGHTS_BY_COLLECTIVE_TABLE)?;
            table.remove_all(id.as_bytes())?;
        }
        write_txn.commit().map_err(StorageError::from)?;

        debug!(id = %id, count = count, "Cascade-deleted insights for collective");
        Ok(count)
    }

    // =========================================================================
    // Activity Storage Operations (E3-S03)
    // =========================================================================

    fn save_activity(&self, activity: &Activity) -> Result<()> {
        let key = encode_activity_key(activity.collective_id.as_bytes(), &activity.agent_id);
        let bytes =
            bincode::serialize(activity).map_err(|e| StorageError::serialization(e.to_string()))?;

        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        {
            let mut table = write_txn.open_table(ACTIVITIES_TABLE)?;
            table.insert(key.as_slice(), bytes.as_slice())?;
        }
        write_txn.commit().map_err(StorageError::from)?;

        debug!(
            agent_id = %activity.agent_id,
            collective_id = %activity.collective_id,
            "Activity saved"
        );
        Ok(())
    }

    fn get_activity(
        &self,
        agent_id: &str,
        collective_id: CollectiveId,
    ) -> Result<Option<Activity>> {
        let key = encode_activity_key(collective_id.as_bytes(), agent_id);

        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_table(ACTIVITIES_TABLE)?;

        match table.get(key.as_slice())? {
            Some(value) => {
                let activity: Activity = bincode::deserialize(value.value())
                    .map_err(|e| StorageError::serialization(e.to_string()))?;
                Ok(Some(activity))
            }
            None => Ok(None),
        }
    }

    fn delete_activity(&self, agent_id: &str, collective_id: CollectiveId) -> Result<bool> {
        let key = encode_activity_key(collective_id.as_bytes(), agent_id);

        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        let existed = {
            let mut table = write_txn.open_table(ACTIVITIES_TABLE)?;
            let removed = table.remove(key.as_slice())?;
            removed.is_some()
        };
        write_txn.commit().map_err(StorageError::from)?;

        if existed {
            debug!(agent_id = %agent_id, collective_id = %collective_id, "Activity deleted");
        }
        Ok(existed)
    }

    fn list_activities_in_collective(&self, collective_id: CollectiveId) -> Result<Vec<Activity>> {
        let prefix = collective_id.as_bytes();

        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_table(ACTIVITIES_TABLE)?;

        let mut activities = Vec::new();
        for result in table.iter()? {
            let (key, value) = result.map_err(StorageError::from)?;
            let key_bytes = key.value();

            // Check if this key belongs to the requested collective (16-byte prefix)
            if key_bytes.len() >= 16 && decode_collective_from_activity_key(key_bytes) == *prefix {
                let activity: Activity = bincode::deserialize(value.value())
                    .map_err(|e| StorageError::serialization(e.to_string()))?;
                activities.push(activity);
            }
        }

        Ok(activities)
    }

    fn delete_activities_by_collective(&self, collective_id: CollectiveId) -> Result<u64> {
        let prefix = collective_id.as_bytes();

        // Phase 1: Read — collect matching keys
        let keys_to_delete: Vec<Vec<u8>> = {
            let read_txn = self.db.begin_read().map_err(StorageError::from)?;
            let table = read_txn.open_table(ACTIVITIES_TABLE)?;

            let mut keys = Vec::new();
            for result in table.iter()? {
                let (key, _) = result.map_err(StorageError::from)?;
                let key_bytes = key.value();

                if key_bytes.len() >= 16
                    && decode_collective_from_activity_key(key_bytes) == *prefix
                {
                    keys.push(key_bytes.to_vec());
                }
            }
            keys
        };

        let count = keys_to_delete.len() as u64;
        if count == 0 {
            return Ok(0);
        }

        // Phase 2: Write — delete all collected keys
        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        {
            let mut table = write_txn.open_table(ACTIVITIES_TABLE)?;
            for key in &keys_to_delete {
                table.remove(key.as_slice())?;
            }
        }
        write_txn.commit().map_err(StorageError::from)?;

        debug!(
            collective_id = %collective_id,
            count = count,
            "Cascade-deleted activities for collective"
        );
        Ok(count)
    }

    // =========================================================================
    // Paginated List Operations (PulseVision)
    // =========================================================================

    fn list_experience_ids_paginated(
        &self,
        collective_id: CollectiveId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ExperienceId>> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_multimap_table(EXPERIENCES_BY_COLLECTIVE_TABLE)?;

        let mut ids = Vec::new();
        let mut skipped = 0usize;

        // Multimap: key=collective_id (16 bytes), values=[timestamp_be:8][exp_id:16] (24 bytes)
        for result in table.get(collective_id.as_bytes())? {
            let value = result.map_err(StorageError::from)?;
            if skipped < offset {
                skipped += 1;
                continue;
            }
            let entry = value.value();
            let mut id_bytes = [0u8; 16];
            id_bytes.copy_from_slice(&entry[8..24]);
            ids.push(ExperienceId::from_bytes(id_bytes));
            if ids.len() >= limit {
                return Ok(ids);
            }
        }

        Ok(ids)
    }

    fn list_relations_in_collective(
        &self,
        collective_id: CollectiveId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<crate::relation::ExperienceRelation>> {
        // Get all experience IDs in this collective first
        let exp_ids = self.list_experience_ids_in_collective(collective_id)?;

        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let source_table = read_txn.open_multimap_table(RELATIONS_BY_SOURCE_TABLE)?;
        let rel_table = read_txn.open_table(RELATIONS_TABLE)?;

        let mut relations = Vec::new();
        let mut skipped = 0usize;

        for exp_id in &exp_ids {
            for result in source_table.get(exp_id.as_bytes())? {
                let rel_id_value = result.map_err(StorageError::from)?;
                let rel_id = RelationId::from_bytes(*rel_id_value.value());

                if skipped < offset {
                    skipped += 1;
                    continue;
                }

                if let Some(entry) = rel_table.get(rel_id.as_bytes())? {
                    let relation: crate::relation::ExperienceRelation =
                        bincode::deserialize(entry.value())
                            .map_err(|e| StorageError::serialization(e.to_string()))?;
                    relations.push(relation);
                    if relations.len() >= limit {
                        return Ok(relations);
                    }
                }
            }
        }

        Ok(relations)
    }

    fn list_insight_ids_paginated(
        &self,
        collective_id: CollectiveId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<InsightId>> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_multimap_table(INSIGHTS_BY_COLLECTIVE_TABLE)?;

        let mut ids = Vec::new();
        let mut skipped = 0usize;

        for result in table.get(collective_id.as_bytes())? {
            let value = result.map_err(StorageError::from)?;
            if skipped < offset {
                skipped += 1;
                continue;
            }
            ids.push(InsightId::from_bytes(*value.value()));
            if ids.len() >= limit {
                return Ok(ids);
            }
        }

        Ok(ids)
    }

    // =========================================================================
    // Watch Event Operations (E4-S02)
    // =========================================================================

    fn get_wal_sequence(&self) -> Result<u64> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let meta_table = read_txn.open_table(METADATA_TABLE)?;
        match meta_table.get(WAL_SEQUENCE_KEY)? {
            Some(entry) => {
                let bytes: [u8; 8] = entry
                    .value()
                    .try_into()
                    .map_err(|_| StorageError::corrupted("invalid wal_sequence bytes"))?;
                Ok(u64::from_be_bytes(bytes))
            }
            None => Ok(0),
        }
    }

    fn poll_watch_events(
        &self,
        since_seq: u64,
        limit: usize,
    ) -> Result<(Vec<WatchEventRecord>, u64)> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let events_table = read_txn.open_table(WATCH_EVENTS_TABLE)?;

        let start_key = (since_seq + 1).to_be_bytes();
        let end_key = u64::MAX.to_be_bytes();
        let mut events = Vec::new();
        let mut max_seq = since_seq;

        for entry in events_table.range::<&[u8; 8]>(&start_key..=&end_key)? {
            let (key, value) = entry.map_err(StorageError::from)?;
            let seq = u64::from_be_bytes(*key.value());
            let record: WatchEventRecord = bincode::deserialize(value.value())
                .map_err(|e| StorageError::serialization(e.to_string()))?;
            events.push(record);
            max_seq = seq;
            if events.len() >= limit {
                break;
            }
        }

        Ok((events, max_seq))
    }

    // =========================================================================
    // Sync Operations (feature: sync)
    // =========================================================================

    #[cfg(feature = "sync")]
    fn poll_sync_events(
        &self,
        since_seq: u64,
        limit: usize,
    ) -> Result<Vec<(u64, WatchEventRecord)>> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let events_table = read_txn.open_table(WATCH_EVENTS_TABLE)?;

        let start_key = (since_seq + 1).to_be_bytes();
        let end_key = u64::MAX.to_be_bytes();
        let mut events = Vec::new();

        for entry in events_table.range::<&[u8; 8]>(&start_key..=&end_key)? {
            let (key, value) = entry.map_err(StorageError::from)?;
            let seq = u64::from_be_bytes(*key.value());
            let record: WatchEventRecord = bincode::deserialize(value.value())
                .map_err(|e| StorageError::serialization(e.to_string()))?;
            events.push((seq, record));
            if events.len() >= limit {
                break;
            }
        }

        Ok(events)
    }

    #[cfg(feature = "sync")]
    fn instance_id(&self) -> crate::sync::InstanceId {
        self.instance_id
    }

    #[cfg(feature = "sync")]
    fn save_sync_cursor(&self, cursor: &crate::sync::SyncCursor) -> Result<()> {
        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        {
            let mut table = write_txn.open_table(SYNC_CURSORS_TABLE)?;
            let bytes = bincode::serialize(cursor)
                .map_err(|e| StorageError::serialization(e.to_string()))?;
            table.insert(cursor.instance_id.as_bytes(), bytes.as_slice())?;
        }
        write_txn.commit().map_err(StorageError::from)?;
        debug!(
            peer = %cursor.instance_id,
            last_sequence = cursor.last_sequence,
            "Saved sync cursor"
        );
        Ok(())
    }

    #[cfg(feature = "sync")]
    fn load_sync_cursor(
        &self,
        instance_id: &crate::sync::InstanceId,
    ) -> Result<Option<crate::sync::SyncCursor>> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_table(SYNC_CURSORS_TABLE)?;
        match table.get(instance_id.as_bytes())? {
            Some(entry) => {
                let cursor: crate::sync::SyncCursor = bincode::deserialize(entry.value())
                    .map_err(|e| StorageError::serialization(e.to_string()))?;
                Ok(Some(cursor))
            }
            None => Ok(None),
        }
    }

    #[cfg(feature = "sync")]
    fn list_sync_cursors(&self) -> Result<Vec<crate::sync::SyncCursor>> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_table(SYNC_CURSORS_TABLE)?;
        let mut cursors = Vec::new();
        for entry in table.iter()? {
            let (_, value) = entry.map_err(StorageError::from)?;
            let cursor: crate::sync::SyncCursor = bincode::deserialize(value.value())
                .map_err(|e| StorageError::serialization(e.to_string()))?;
            cursors.push(cursor);
        }
        Ok(cursors)
    }

    #[cfg(feature = "sync")]
    fn compact_wal_events(&self, up_to_seq: u64) -> Result<u64> {
        if up_to_seq == 0 {
            return Ok(0);
        }

        // Collect keys to delete in a read pass
        let keys_to_delete: Vec<[u8; 8]> = {
            let read_txn = self.db.begin_read().map_err(StorageError::from)?;
            let events_table = read_txn.open_table(WATCH_EVENTS_TABLE)?;

            let start_key = 1u64.to_be_bytes();
            let end_key = up_to_seq.to_be_bytes();
            let mut keys = Vec::new();

            for entry in events_table.range::<&[u8; 8]>(&start_key..=&end_key)? {
                let (key, _) = entry.map_err(StorageError::from)?;
                keys.push(*key.value());
            }
            keys
        };

        if keys_to_delete.is_empty() {
            return Ok(0);
        }

        let count = keys_to_delete.len() as u64;

        // Delete in a write transaction
        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        {
            let mut events_table = write_txn.open_table(WATCH_EVENTS_TABLE)?;
            for key in &keys_to_delete {
                events_table.remove(key)?;
            }
        }
        write_txn.commit().map_err(StorageError::from)?;

        debug!(count, up_to_seq, "Compacted WAL events");
        Ok(count)
    }
}

// ============================================================================
// Embedding byte conversion helpers
// ============================================================================

/// Converts a slice of f32 values to raw little-endian bytes.
#[inline]
fn f32_slice_to_bytes(data: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(data.len() * 4);
    for &val in data {
        bytes.extend_from_slice(&val.to_le_bytes());
    }
    bytes
}

/// Converts raw little-endian bytes back to a Vec<f32>.
#[inline]
fn bytes_to_f32_vec(data: &[u8]) -> Vec<f32> {
    data.chunks_exact(4)
        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect()
}

// RedbStorage is auto Send + Sync: Database, DatabaseMetadata, and PathBuf
// are all Send + Sync.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PulseDB;
    use tempfile::tempdir;

    #[derive(Serialize)]
    struct SeedExperienceV2 {
        id: ExperienceId,
        collective_id: CollectiveId,
        content: String,
        experience_type: ExperienceType,
        importance: f32,
        confidence: f32,
        applications: u32,
        domain: Vec<String>,
        related_files: Vec<String>,
        source_agent: AgentId,
        source_task: Option<crate::types::TaskId>,
        timestamp: Timestamp,
        archived: bool,
    }

    fn default_config() -> Config {
        Config::default()
    }

    fn seed_schema_v2_store(
        path: &Path,
        applications: u32,
    ) -> (ExperienceId, CollectiveId, Timestamp) {
        let db = Database::builder().create(path).unwrap();
        let collective = Collective::new("migrated-collective", 384);
        let experience_id = ExperienceId::new();
        let timestamp = Timestamp::from_millis(1_717_171_717_000);
        let experience = SeedExperienceV2 {
            id: experience_id,
            collective_id: collective.id,
            content: "legacy v2 experience".into(),
            experience_type: ExperienceType::Fact {
                statement: "legacy fact".into(),
                source: "fixture".into(),
            },
            importance: 0.7,
            confidence: 0.8,
            applications,
            domain: vec!["migration".into()],
            related_files: vec!["src/storage/redb.rs".into()],
            source_agent: AgentId::new("legacy-agent"),
            source_task: None,
            timestamp,
            archived: false,
        };

        let mut metadata = DatabaseMetadata::new(EmbeddingDimension::D384);
        metadata.schema_version = 2;
        let metadata_bytes = bincode::serialize(&metadata).unwrap();
        let collective_bytes = bincode::serialize(&collective).unwrap();
        let experience_bytes = bincode::serialize(&experience).unwrap();
        let embedding = vec![0.25_f32; 384];
        let embedding_bytes = f32_slice_to_bytes(&embedding);

        let write_txn = db.begin_write().unwrap();
        {
            let mut meta_table = write_txn.open_table(METADATA_TABLE).unwrap();
            meta_table
                .insert(METADATA_KEY, metadata_bytes.as_slice())
                .unwrap();
            let mut collectives = write_txn.open_table(COLLECTIVES_TABLE).unwrap();
            collectives
                .insert(collective.id.as_bytes(), collective_bytes.as_slice())
                .unwrap();
            let mut experiences = write_txn.open_table(EXPERIENCES_TABLE).unwrap();
            experiences
                .insert(experience_id.as_bytes(), experience_bytes.as_slice())
                .unwrap();
            let mut embeddings = write_txn.open_table(EMBEDDINGS_TABLE).unwrap();
            embeddings
                .insert(experience_id.as_bytes(), embedding_bytes.as_slice())
                .unwrap();

            let mut by_collective = write_txn
                .open_multimap_table(EXPERIENCES_BY_COLLECTIVE_TABLE)
                .unwrap();
            let mut value = [0u8; 24];
            value[..8].copy_from_slice(&timestamp.to_be_bytes());
            value[8..24].copy_from_slice(experience_id.as_bytes());
            by_collective
                .insert(collective.id.as_bytes(), &value)
                .unwrap();

            let mut by_type = write_txn
                .open_multimap_table(EXPERIENCES_BY_TYPE_TABLE)
                .unwrap();
            let type_key = encode_type_index_key(collective.id.as_bytes(), ExperienceTypeTag::Fact);
            by_type.insert(&type_key, experience_id.as_bytes()).unwrap();

            let _ = write_txn.open_table(DECAY_CONFIGS_TABLE).unwrap();
            let _ = write_txn.open_table(WATCH_EVENTS_TABLE).unwrap();
            let _ = write_txn.open_table(RELATIONS_TABLE).unwrap();
            let _ = write_txn
                .open_multimap_table(RELATIONS_BY_SOURCE_TABLE)
                .unwrap();
            let _ = write_txn
                .open_multimap_table(RELATIONS_BY_TARGET_TABLE)
                .unwrap();
            let _ = write_txn.open_table(INSIGHTS_TABLE).unwrap();
            let _ = write_txn
                .open_multimap_table(INSIGHTS_BY_COLLECTIVE_TABLE)
                .unwrap();
            let _ = write_txn.open_table(ACTIVITIES_TABLE).unwrap();
        }
        write_txn.commit().unwrap();

        drop(db);

        (experience_id, collective.id, timestamp)
    }

    #[test]
    fn test_schema_v2_experience_migration_writes_legacy_bucket_backup_and_preserves_queries() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let (experience_id, collective_id, timestamp) = seed_schema_v2_store(&path, 7);

        let backup_path = pre_v3_backup_path(&path);
        assert!(!backup_path.exists());

        let db = PulseDB::open(&path, default_config()).unwrap();

        assert!(backup_path.exists(), "v2 migration must retain a backup");
        let experience = db.get_experience(experience_id).unwrap().unwrap();
        assert_eq!(experience.last_reinforced, timestamp);
        assert_eq!(experience.applications(), 7);
        assert_eq!(
            experience
                .applications
                .get(&legacy_applications_instance_id()),
            Some(&7)
        );

        let query = vec![0.25_f32; 384];
        let search_results = db.search_similar(collective_id, &query, 10).unwrap();
        assert!(
            search_results
                .iter()
                .any(|result| result.experience.id == experience_id),
            "migrated experience must remain searchable"
        );

        let recent = db.get_recent_experiences(collective_id, 10).unwrap();
        assert!(
            recent
                .iter()
                .any(|experience| experience.id == experience_id),
            "migrated experience must remain in the by-collective index"
        );

        db.close().unwrap();
    }

    #[test]
    fn test_read_only_open_refuses_unmigrated_schema_v2_store() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        seed_schema_v2_store(&path, 3);

        let err = RedbStorage::open(&path, &Config::read_only()).unwrap_err();
        assert!(matches!(err, PulseDBError::ReadOnly));
        assert!(
            !pre_v3_backup_path(&path).exists(),
            "read-only refusal must happen before migration backup/write work"
        );
    }

    #[test]
    fn test_open_creates_new_database() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        assert!(!path.exists());

        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        assert!(path.exists());
        assert_eq!(storage.metadata().schema_version, SCHEMA_VERSION);
        assert_eq!(
            storage.metadata().embedding_dimension,
            EmbeddingDimension::D384
        );

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_open_existing_database() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        // Create database
        let storage = RedbStorage::open(&path, &default_config()).unwrap();
        let created_at = storage.metadata().created_at;
        Box::new(storage).close().unwrap();

        // Reopen
        std::thread::sleep(std::time::Duration::from_millis(10));
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        // created_at should be preserved
        assert_eq!(storage.metadata().created_at, created_at);
        // last_opened_at should be updated
        assert!(storage.metadata().last_opened_at > created_at);

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_dimension_mismatch_returns_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        // Create with D384
        let config_384 = Config {
            embedding_dimension: EmbeddingDimension::D384,
            ..Default::default()
        };
        let storage = RedbStorage::open(&path, &config_384).unwrap();
        Box::new(storage).close().unwrap();

        // Try to reopen with D768
        let config_768 = Config {
            embedding_dimension: EmbeddingDimension::D768,
            ..Default::default()
        };
        let result = RedbStorage::open(&path, &config_768);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(
            err,
            PulseDBError::Validation(ValidationError::DimensionMismatch { .. })
        ));
    }

    #[test]
    fn test_database_files_created() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("pulse.db");

        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        // Main database file should exist
        assert!(path.exists());
        assert!(storage.path().is_some());
        assert_eq!(storage.path().unwrap(), path);

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_metadata_preserved_across_opens() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let config = Config {
            embedding_dimension: EmbeddingDimension::Custom(512),
            ..Default::default()
        };

        // Create
        let storage = RedbStorage::open(&path, &config).unwrap();
        assert_eq!(
            storage.metadata().embedding_dimension,
            EmbeddingDimension::Custom(512)
        );
        Box::new(storage).close().unwrap();

        // Reopen
        let storage = RedbStorage::open(&path, &config).unwrap();
        assert_eq!(
            storage.metadata().embedding_dimension,
            EmbeddingDimension::Custom(512)
        );
        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_embedding_dimension_accessor() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let config = Config {
            embedding_dimension: EmbeddingDimension::D768,
            ..Default::default()
        };

        let storage = RedbStorage::open(&path, &config).unwrap();
        assert_eq!(storage.embedding_dimension(), EmbeddingDimension::D768);

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_all_six_tables_created() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        // Verify all 6 tables exist by opening each in a read transaction.
        // If any table wasn't created during initialize_new(), this would
        // return a TableDoesNotExist error.
        let read_txn = storage.database().begin_read().unwrap();

        read_txn.open_table(METADATA_TABLE).unwrap();
        read_txn.open_table(COLLECTIVES_TABLE).unwrap();
        read_txn.open_table(EXPERIENCES_TABLE).unwrap();
        read_txn.open_table(EMBEDDINGS_TABLE).unwrap();
        read_txn
            .open_multimap_table(EXPERIENCES_BY_COLLECTIVE_TABLE)
            .unwrap();
        read_txn
            .open_multimap_table(EXPERIENCES_BY_TYPE_TABLE)
            .unwrap();
        read_txn.open_table(DECAY_CONFIGS_TABLE).unwrap();

        Box::new(storage).close().unwrap();
    }

    // ====================================================================
    // Collective CRUD tests
    // ====================================================================

    #[test]
    fn test_save_and_get_collective() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test-project", 384);
        let id = collective.id;

        storage.save_collective(&collective).unwrap();

        let retrieved = storage.get_collective(id).unwrap().unwrap();
        assert_eq!(retrieved.id, id);
        assert_eq!(retrieved.name, "test-project");
        assert_eq!(retrieved.embedding_dimension, 384);
        assert!(retrieved.owner_id.is_none());

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_get_nonexistent_collective_returns_none() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let result = storage.get_collective(CollectiveId::new()).unwrap();
        assert!(result.is_none());

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_decay_config_absent_then_round_trips() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();
        let collective_id = CollectiveId::new();

        assert!(storage.get_decay_config(collective_id).unwrap().is_none());

        let config = crate::config::DecayConfig {
            half_life: std::time::Duration::from_secs(7 * 24 * 60 * 60),
            freq_weight: 0.5,
            floor: 0.2,
            auto_archive_below_floor: true,
            default_recall_weights: None,
        };

        storage
            .set_decay_config(collective_id, config.clone())
            .unwrap();

        let restored = storage.get_decay_config(collective_id).unwrap();
        assert_eq!(restored, Some(config));

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_save_collective_overwrites_existing() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let mut collective = Collective::new("original-name", 384);
        let id = collective.id;
        storage.save_collective(&collective).unwrap();

        // Overwrite with updated name
        collective.name = "updated-name".to_string();
        storage.save_collective(&collective).unwrap();

        let retrieved = storage.get_collective(id).unwrap().unwrap();
        assert_eq!(retrieved.name, "updated-name");

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_list_collectives_empty() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collectives = storage.list_collectives().unwrap();
        assert!(collectives.is_empty());

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_list_collectives_returns_all() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let c1 = Collective::new("project-alpha", 384);
        let c2 = Collective::new("project-beta", 384);
        let c3 = Collective::new("project-gamma", 384);

        storage.save_collective(&c1).unwrap();
        storage.save_collective(&c2).unwrap();
        storage.save_collective(&c3).unwrap();

        let collectives = storage.list_collectives().unwrap();
        assert_eq!(collectives.len(), 3);

        // Verify all IDs are present
        let ids: Vec<CollectiveId> = collectives.iter().map(|c| c.id).collect();
        assert!(ids.contains(&c1.id));
        assert!(ids.contains(&c2.id));
        assert!(ids.contains(&c3.id));

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_delete_collective_existing() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("to-delete", 384);
        let id = collective.id;
        storage.save_collective(&collective).unwrap();

        // Delete it
        let deleted = storage.delete_collective(id).unwrap();
        assert!(deleted);

        // Verify it's gone
        assert!(storage.get_collective(id).unwrap().is_none());

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_delete_collective_nonexistent() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let deleted = storage.delete_collective(CollectiveId::new()).unwrap();
        assert!(!deleted);

        Box::new(storage).close().unwrap();
    }

    // ====================================================================
    // ACID Guarantee Tests
    // ====================================================================

    #[test]
    fn test_uncommitted_transaction_is_invisible() {
        // ATOMICITY: If we don't commit a write transaction, the data
        // must not be visible to subsequent reads.
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("phantom", 384);
        let id = collective.id;
        let bytes = bincode::serialize(&collective).unwrap();

        // Open a write transaction, insert data, but DON'T commit -- just drop
        {
            let write_txn = storage.database().begin_write().unwrap();
            {
                let mut table = write_txn.open_table(COLLECTIVES_TABLE).unwrap();
                table.insert(id.as_bytes(), bytes.as_slice()).unwrap();
            }
            // write_txn is dropped here without commit() -- rolled back
        }

        // The collective should NOT be visible
        let result = storage.get_collective(id).unwrap();
        assert!(result.is_none(), "Uncommitted data must not be visible");

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_committed_transaction_is_visible() {
        // DURABILITY (within session): committed data must be immediately
        // visible to subsequent reads.
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("committed", 384);
        let id = collective.id;

        storage.save_collective(&collective).unwrap();

        let result = storage.get_collective(id).unwrap();
        assert!(result.is_some(), "Committed data must be visible");

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_multi_table_atomicity() {
        // ATOMICITY: A single transaction writing to multiple tables
        // is all-or-nothing. Here we write to both COLLECTIVES and METADATA
        // in one transaction and verify both are visible after commit.
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("multi-table", 384);
        let id = collective.id;
        let collective_bytes = bincode::serialize(&collective).unwrap();

        // Write to TWO tables in a single transaction
        let write_txn = storage.database().begin_write().unwrap();
        {
            let mut coll_table = write_txn.open_table(COLLECTIVES_TABLE).unwrap();
            coll_table
                .insert(id.as_bytes(), collective_bytes.as_slice())
                .unwrap();
        }
        {
            let mut meta_table = write_txn.open_table(METADATA_TABLE).unwrap();
            meta_table
                .insert("test_marker", b"multi_table_test".as_slice())
                .unwrap();
        }
        write_txn.commit().unwrap();

        // Verify BOTH writes are visible
        let coll = storage.get_collective(id).unwrap();
        assert!(coll.is_some(), "Collective from multi-table txn must exist");

        let read_txn = storage.database().begin_read().unwrap();
        let meta_table = read_txn.open_table(METADATA_TABLE).unwrap();
        let marker = meta_table.get("test_marker").unwrap();
        assert!(marker.is_some(), "Metadata from multi-table txn must exist");

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_mvcc_read_consistency() {
        // ISOLATION (MVCC): A single read transaction sees a consistent
        // snapshot reflecting all committed writes up to the moment the
        // read was opened, and none of the uncommitted or subsequent ones.
        //
        // We write across multiple separate transactions, then verify a
        // read sees the expected consistent state. Combined with
        // test_uncommitted_transaction_is_invisible (atomicity), this
        // covers the key ACID isolation properties.
        //
        // Note: redb 2.6.3 has a page allocation constraint that prevents
        // holding a read transaction open while a write commits on the
        // same Database handle. redb guarantees MVCC isolation internally
        // via shadow paging; this test verifies our usage is correct.
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        // Write 3 collectives across separate transactions
        let c1 = Collective::new("alpha", 384);
        let c2 = Collective::new("beta", 384);
        let c3 = Collective::new("gamma", 384);

        storage.save_collective(&c1).unwrap();
        storage.save_collective(&c2).unwrap();
        storage.save_collective(&c3).unwrap();

        // Delete c2 (another transaction)
        storage.delete_collective(c2.id).unwrap();

        // A read transaction must see the consistent state:
        // c1 and c3 present, c2 absent
        let read_txn = storage.database().begin_read().unwrap();
        let table = read_txn.open_table(COLLECTIVES_TABLE).unwrap();

        assert!(
            table.get(c1.id.as_bytes()).unwrap().is_some(),
            "c1 must be visible (committed)"
        );
        assert!(
            table.get(c2.id.as_bytes()).unwrap().is_none(),
            "c2 must be absent (deleted)"
        );
        assert!(
            table.get(c3.id.as_bytes()).unwrap().is_some(),
            "c3 must be visible (committed)"
        );

        // Count should be exactly 2
        let count = table.iter().unwrap().count();
        // +1 for the metadata entry? No -- COLLECTIVES_TABLE is separate.
        assert_eq!(count, 2, "Exactly 2 collectives should exist");

        drop(table);
        drop(read_txn);

        Box::new(storage).close().unwrap();
    }

    // ====================================================================
    // Corruption Detection Tests
    // ====================================================================

    #[test]
    fn test_corruption_detection_invalid_metadata_bytes() {
        // Opening a database whose metadata contains garbage bytes
        // must return a Corrupted error, not a panic or deserialization UB.
        let dir = tempdir().unwrap();
        let path = dir.path().join("corrupt.db");

        // Create a valid database, then corrupt the metadata
        let storage = RedbStorage::open(&path, &default_config()).unwrap();
        let write_txn = storage.database().begin_write().unwrap();
        {
            let mut meta = write_txn.open_table(METADATA_TABLE).unwrap();
            meta.insert(METADATA_KEY, b"not-valid-bincode-data".as_slice())
                .unwrap();
        }
        write_txn.commit().unwrap();
        Box::new(storage).close().unwrap();

        // Reopen must detect the corruption
        let result = RedbStorage::open(&path, &default_config());
        assert!(result.is_err(), "Corrupted metadata must be rejected");
        let err = result.unwrap_err();
        match err {
            PulseDBError::Storage(StorageError::Corrupted(msg)) => {
                assert!(
                    msg.contains("Invalid metadata format"),
                    "Error should mention invalid format, got: {}",
                    msg
                );
            }
            other => panic!("Expected StorageError::Corrupted, got: {:?}", other),
        }
    }

    #[test]
    fn test_corruption_detection_missing_metadata_key() {
        // If the metadata table exists but the "db_metadata" key is absent,
        // open_existing must return a Corrupted error.
        let dir = tempdir().unwrap();
        let path = dir.path().join("no_key.db");

        // Create a valid database, then delete the metadata key
        let storage = RedbStorage::open(&path, &default_config()).unwrap();
        let write_txn = storage.database().begin_write().unwrap();
        {
            let mut meta = write_txn.open_table(METADATA_TABLE).unwrap();
            meta.remove(METADATA_KEY).unwrap();
        }
        write_txn.commit().unwrap();
        Box::new(storage).close().unwrap();

        // Reopen must detect the missing key
        let result = RedbStorage::open(&path, &default_config());
        assert!(result.is_err(), "Missing metadata key must be rejected");
        let err = result.unwrap_err();
        match err {
            PulseDBError::Storage(StorageError::Corrupted(msg)) => {
                assert!(
                    msg.contains("Missing database metadata"),
                    "Error should mention missing metadata, got: {}",
                    msg
                );
            }
            other => panic!("Expected StorageError::Corrupted, got: {:?}", other),
        }
    }

    #[test]
    fn test_corruption_detection_missing_metadata_table() {
        // If the metadata table doesn't exist at all, open_existing must
        // return a Corrupted error. We simulate this by creating a raw
        // redb database without our schema tables.
        let dir = tempdir().unwrap();
        let path = dir.path().join("no_table.db");

        // Create a raw redb database with a dummy table (not our schema)
        {
            let db = ::redb::Database::create(&path).unwrap();
            let write_txn = db.begin_write().unwrap();
            {
                let dummy: ::redb::TableDefinition<&str, &str> =
                    ::redb::TableDefinition::new("dummy");
                let mut table = write_txn.open_table(dummy).unwrap();
                table.insert("key", "value").unwrap();
            }
            write_txn.commit().unwrap();
        }

        // Opening this as a PulseDB must detect the missing metadata table
        let result = RedbStorage::open(&path, &default_config());
        assert!(result.is_err(), "Missing metadata table must be rejected");
        let err = result.unwrap_err();
        match err {
            PulseDBError::Storage(StorageError::Corrupted(msg)) => {
                assert!(
                    msg.contains("Cannot open metadata table"),
                    "Error should mention metadata table, got: {}",
                    msg
                );
            }
            other => panic!("Expected StorageError::Corrupted, got: {:?}", other),
        }
    }

    // ====================================================================
    // Experience CRUD tests
    // ====================================================================

    use crate::experience::{Experience, ExperienceType, ExperienceUpdate, Severity};
    use crate::types::{AgentId, ExperienceId, Timestamp};

    /// Creates a test experience with a given collective_id and embedding dimension.
    fn test_experience(collective_id: CollectiveId, dim: usize) -> Experience {
        let timestamp = Timestamp::now();
        Experience {
            id: ExperienceId::new(),
            collective_id,
            content: "Test experience content".into(),
            embedding: vec![0.42; dim],
            experience_type: ExperienceType::Fact {
                statement: "redb uses shadow paging".into(),
                source: "docs".into(),
            },
            importance: 0.8,
            confidence: 0.7,
            applications: BTreeMap::new(),
            domain: vec!["rust".into(), "databases".into()],
            related_files: vec!["src/storage/redb.rs".into()],
            source_agent: AgentId::new("test-agent"),
            source_task: None,
            timestamp,
            last_reinforced: timestamp,
            archived: false,
        }
    }

    #[test]
    fn test_save_and_get_experience() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test", 384);
        storage.save_collective(&collective).unwrap();

        let exp = test_experience(collective.id, 384);
        let exp_id = exp.id;

        storage.save_experience(&exp).unwrap();

        let retrieved = storage.get_experience(exp_id).unwrap().unwrap();
        assert_eq!(retrieved.id, exp_id);
        assert_eq!(retrieved.collective_id, collective.id);
        assert_eq!(retrieved.content, "Test experience content");
        assert_eq!(retrieved.importance, 0.8);
        assert_eq!(retrieved.confidence, 0.7);
        assert_eq!(retrieved.applications(), 0);
        assert_eq!(retrieved.domain, vec!["rust", "databases"]);
        assert!(!retrieved.archived);
        // Embedding should be reconstituted from EMBEDDINGS_TABLE
        assert_eq!(retrieved.embedding.len(), 384);
        assert_eq!(retrieved.embedding[0], 0.42);

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_get_nonexistent_experience_returns_none() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let result = storage.get_experience(ExperienceId::new()).unwrap();
        assert!(result.is_none());

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_update_experience_fields() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test", 384);
        storage.save_collective(&collective).unwrap();

        let exp = test_experience(collective.id, 384);
        let exp_id = exp.id;
        storage.save_experience(&exp).unwrap();

        // Update importance and domain
        let update = ExperienceUpdate {
            importance: Some(0.95),
            domain: Some(vec!["updated-tag".into()]),
            ..Default::default()
        };
        let updated = storage.update_experience(exp_id, &update).unwrap();
        assert!(updated);

        let retrieved = storage.get_experience(exp_id).unwrap().unwrap();
        assert_eq!(retrieved.importance, 0.95);
        assert_eq!(retrieved.domain, vec!["updated-tag"]);
        // Unchanged fields
        assert_eq!(retrieved.confidence, 0.7);

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_update_nonexistent_experience_returns_false() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let update = ExperienceUpdate {
            importance: Some(0.5),
            ..Default::default()
        };
        let result = storage
            .update_experience(ExperienceId::new(), &update)
            .unwrap();
        assert!(!result);

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_delete_experience() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test", 384);
        storage.save_collective(&collective).unwrap();

        let exp = test_experience(collective.id, 384);
        let exp_id = exp.id;
        storage.save_experience(&exp).unwrap();

        // Verify exists
        assert!(storage.get_experience(exp_id).unwrap().is_some());

        // Delete
        let deleted = storage.delete_experience(exp_id).unwrap();
        assert!(deleted);

        // Verify gone
        assert!(storage.get_experience(exp_id).unwrap().is_none());
        assert!(storage.get_embedding(exp_id).unwrap().is_none());

        // Verify index cleaned up
        assert_eq!(
            storage
                .count_experiences_in_collective(collective.id)
                .unwrap(),
            0
        );

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_delete_nonexistent_experience_returns_false() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let result = storage.delete_experience(ExperienceId::new()).unwrap();
        assert!(!result);

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_save_and_get_embedding() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let id = ExperienceId::new();
        let embedding = vec![0.1, 0.2, 0.3, -0.5, 1.0, f32::MIN_POSITIVE];

        storage.save_embedding(id, &embedding).unwrap();

        let retrieved = storage.get_embedding(id).unwrap().unwrap();
        assert_eq!(retrieved, embedding);

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_experience_by_collective_index() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test", 384);
        storage.save_collective(&collective).unwrap();

        // Add 3 experiences
        for _ in 0..3 {
            let exp = test_experience(collective.id, 384);
            storage.save_experience(&exp).unwrap();
        }

        // Count should be 3
        assert_eq!(
            storage
                .count_experiences_in_collective(collective.id)
                .unwrap(),
            3
        );

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_cascade_delete_includes_experiences() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test", 384);
        storage.save_collective(&collective).unwrap();

        let exp1 = test_experience(collective.id, 384);
        let exp2 = test_experience(collective.id, 384);
        let id1 = exp1.id;
        let id2 = exp2.id;
        storage.save_experience(&exp1).unwrap();
        storage.save_experience(&exp2).unwrap();

        // Cascade delete
        let count = storage
            .delete_experiences_by_collective(collective.id)
            .unwrap();
        assert_eq!(count, 2);

        // Verify experiences are gone
        assert!(storage.get_experience(id1).unwrap().is_none());
        assert!(storage.get_experience(id2).unwrap().is_none());
        assert!(storage.get_embedding(id1).unwrap().is_none());
        assert!(storage.get_embedding(id2).unwrap().is_none());

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_update_experience_archived_flag() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test", 384);
        storage.save_collective(&collective).unwrap();

        let exp = test_experience(collective.id, 384);
        let exp_id = exp.id;
        storage.save_experience(&exp).unwrap();

        // Archive
        let update = ExperienceUpdate {
            archived: Some(true),
            ..Default::default()
        };
        storage.update_experience(exp_id, &update).unwrap();

        let retrieved = storage.get_experience(exp_id).unwrap().unwrap();
        assert!(retrieved.archived);

        // Unarchive
        let update = ExperienceUpdate {
            archived: Some(false),
            ..Default::default()
        };
        storage.update_experience(exp_id, &update).unwrap();

        let retrieved = storage.get_experience(exp_id).unwrap().unwrap();
        assert!(!retrieved.archived);

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_f32_byte_conversion_roundtrip() {
        let original = vec![0.0, 1.0, -1.0, f32::MAX, f32::MIN, std::f32::consts::PI];
        let bytes = f32_slice_to_bytes(&original);
        assert_eq!(bytes.len(), original.len() * 4);

        let restored = bytes_to_f32_vec(&bytes);
        assert_eq!(original, restored);
    }

    #[test]
    fn test_experience_with_all_type_variants() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test", 384);
        storage.save_collective(&collective).unwrap();

        // Save one experience per type variant
        let types = vec![
            ExperienceType::Difficulty {
                description: "test".into(),
                severity: Severity::High,
            },
            ExperienceType::Solution {
                problem_ref: None,
                approach: "test".into(),
                worked: true,
            },
            ExperienceType::ErrorPattern {
                signature: "E0308".into(),
                fix: "check types".into(),
                prevention: "use clippy".into(),
            },
            ExperienceType::SuccessPattern {
                task_type: "refactor".into(),
                approach: "extract method".into(),
                quality: 0.9,
            },
            ExperienceType::UserPreference {
                category: "style".into(),
                preference: "snake_case".into(),
                strength: 1.0,
            },
            ExperienceType::ArchitecturalDecision {
                decision: "use redb".into(),
                rationale: "pure Rust".into(),
            },
            ExperienceType::TechInsight {
                technology: "tokio".into(),
                insight: "spawn_blocking".into(),
            },
            ExperienceType::Fact {
                statement: "Rust is safe".into(),
                source: "docs".into(),
            },
            ExperienceType::Generic { category: None },
        ];

        for experience_type in types {
            let mut exp = test_experience(collective.id, 384);
            exp.experience_type = experience_type;
            storage.save_experience(&exp).unwrap();

            // Verify roundtrip
            let retrieved = storage.get_experience(exp.id).unwrap().unwrap();
            assert_eq!(
                retrieved.experience_type.type_tag(),
                exp.experience_type.type_tag()
            );
        }

        assert_eq!(
            storage
                .count_experiences_in_collective(collective.id)
                .unwrap(),
            9
        );

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_reinforce_experience_atomic() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test", 384);
        storage.save_collective(&collective).unwrap();

        let exp = test_experience(collective.id, 384);
        let exp_id = exp.id;
        storage.save_experience(&exp).unwrap();

        // Reinforce 3 times
        assert_eq!(storage.reinforce_experience(exp_id).unwrap(), Some(1));
        assert_eq!(storage.reinforce_experience(exp_id).unwrap(), Some(2));
        assert_eq!(storage.reinforce_experience(exp_id).unwrap(), Some(3));

        // Verify the stored value
        let retrieved = storage.get_experience(exp_id).unwrap().unwrap();
        assert_eq!(retrieved.applications(), 3);

        // Verify embedding was NOT re-written (still intact)
        let emb = storage.get_embedding(exp_id).unwrap().unwrap();
        assert_eq!(emb.len(), 384);

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_reinforce_experience_nonexistent() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let result = storage.reinforce_experience(ExperienceId::new()).unwrap();
        assert!(result.is_none());

        Box::new(storage).close().unwrap();
    }

    // ====================================================================
    // WAL Sequence Tracking Tests (E4-S02)
    // ====================================================================

    #[test]
    fn test_wal_sequence_starts_at_zero() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        assert_eq!(storage.get_wal_sequence().unwrap(), 0);

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_save_experience_increments_wal_sequence() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test", 384);
        storage.save_collective(&collective).unwrap();
        // save_collective now records WAL event #1
        assert_eq!(storage.get_wal_sequence().unwrap(), 1);

        let exp1 = test_experience(collective.id, 384);
        storage.save_experience(&exp1).unwrap();
        assert_eq!(storage.get_wal_sequence().unwrap(), 2);

        let exp2 = test_experience(collective.id, 384);
        storage.save_experience(&exp2).unwrap();
        assert_eq!(storage.get_wal_sequence().unwrap(), 3);

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_poll_watch_events_returns_correct_events() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test", 384);
        storage.save_collective(&collective).unwrap();
        // Collective creates WAL event #1

        let exp1 = test_experience(collective.id, 384);
        let exp2 = test_experience(collective.id, 384);
        let exp3 = test_experience(collective.id, 384);
        storage.save_experience(&exp1).unwrap();
        storage.save_experience(&exp2).unwrap();
        storage.save_experience(&exp3).unwrap();

        // Poll all events (collective + 3 experiences = 4 total)
        let (events, max_seq) = storage.poll_watch_events(0, 100).unwrap();
        assert_eq!(events.len(), 4);
        assert_eq!(max_seq, 4);
        // First event is collective creation, rest are experience creations
        assert!(events
            .iter()
            .all(|e| e.event_type == WatchEventTypeTag::Created));
        // Skip collective event (index 0), experience IDs should match
        assert_eq!(events[1].entity_id, *exp1.id.as_bytes());
        assert_eq!(events[2].entity_id, *exp2.id.as_bytes());
        assert_eq!(events[3].entity_id, *exp3.id.as_bytes());

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_poll_watch_events_since_midpoint() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test", 384);
        storage.save_collective(&collective).unwrap();

        // Create 5 experiences
        for _ in 0..5 {
            let exp = test_experience(collective.id, 384);
            storage.save_experience(&exp).unwrap();
        }

        // Collective = seq 1, 5 experiences = seq 2-6. Total = 6.
        // Poll from seq 4 — should get 2 events (seq 5 and 6)
        let (events, max_seq) = storage.poll_watch_events(4, 100).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(max_seq, 6);

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_poll_watch_events_empty_when_caught_up() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test", 384);
        storage.save_collective(&collective).unwrap();

        let exp = test_experience(collective.id, 384);
        storage.save_experience(&exp).unwrap();

        // Poll everything (collective + experience = 2 events)
        let (events, max_seq) = storage.poll_watch_events(0, 100).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(max_seq, 2);

        // Poll again from same position — empty
        let (events, max_seq) = storage.poll_watch_events(2, 100).unwrap();
        assert_eq!(events.len(), 0);
        assert_eq!(max_seq, 2); // stays the same

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_delete_records_watch_event() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test", 384);
        storage.save_collective(&collective).unwrap();

        let exp = test_experience(collective.id, 384);
        storage.save_experience(&exp).unwrap();
        storage.delete_experience(exp.id).unwrap();

        // Collective(1) + Created(2) + Deleted(3) = 3 events
        let (events, max_seq) = storage.poll_watch_events(0, 100).unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(max_seq, 3);
        assert_eq!(events[0].event_type, WatchEventTypeTag::Created); // collective
        assert_eq!(events[1].event_type, WatchEventTypeTag::Created); // experience
        assert_eq!(events[2].event_type, WatchEventTypeTag::Deleted); // experience deleted
        assert_eq!(events[2].entity_id, *exp.id.as_bytes());

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_update_records_watch_event() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test", 384);
        storage.save_collective(&collective).unwrap();

        let exp = test_experience(collective.id, 384);
        storage.save_experience(&exp).unwrap();

        let update = ExperienceUpdate {
            importance: Some(0.99),
            ..Default::default()
        };
        storage.update_experience(exp.id, &update).unwrap();

        // Collective(1) + Created(2) + Updated(3) = 3 events
        let (events, _) = storage.poll_watch_events(0, 100).unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[2].event_type, WatchEventTypeTag::Updated);

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_reinforce_records_watch_event() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test", 384);
        storage.save_collective(&collective).unwrap();

        let exp = test_experience(collective.id, 384);
        storage.save_experience(&exp).unwrap();
        storage.reinforce_experience(exp.id).unwrap();

        // Collective(1) + Created(2) + Updated(3) = 3 events
        let (events, _) = storage.poll_watch_events(0, 100).unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[1].event_type, WatchEventTypeTag::Created);
        assert_eq!(events[2].event_type, WatchEventTypeTag::Updated);

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_archive_records_archived_event() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test", 384);
        storage.save_collective(&collective).unwrap();

        let exp = test_experience(collective.id, 384);
        storage.save_experience(&exp).unwrap();

        let update = ExperienceUpdate {
            archived: Some(true),
            ..Default::default()
        };
        storage.update_experience(exp.id, &update).unwrap();

        // Collective(1) + Created(2) + Archived(3) = 3 events
        let (events, _) = storage.poll_watch_events(0, 100).unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[2].event_type, WatchEventTypeTag::Archived);

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_poll_watch_events_batch_limit() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test", 384);
        storage.save_collective(&collective).unwrap();

        // Create 10 experiences
        for _ in 0..10 {
            let exp = test_experience(collective.id, 384);
            storage.save_experience(&exp).unwrap();
        }

        // Poll with limit of 3
        let (events, max_seq) = storage.poll_watch_events(0, 3).unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(max_seq, 3);

        // Continue from where we left off
        let (events, max_seq) = storage.poll_watch_events(3, 3).unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(max_seq, 6);

        Box::new(storage).close().unwrap();
    }
}
