//! PulseDB main struct and lifecycle operations.
//!
//! The [`PulseDB`] struct is the primary interface for interacting with
//! the database. It provides methods for:
//!
//! - Opening and closing the database
//! - Managing collectives (isolation units)
//! - Recording and querying experiences
//! - Semantic search and context retrieval
//!
//! # Quick Start
//!
//! ```rust
//! # fn main() -> pulsedb::Result<()> {
//! # let dir = tempfile::tempdir().unwrap();
//! use pulsedb::{PulseDB, Config, NewExperience};
//!
//! // Open or create a database
//! let db = PulseDB::open(dir.path().join("test.db"), Config::default())?;
//!
//! // Create a collective for your project
//! let collective = db.create_collective("my-project")?;
//!
//! // Record an experience
//! db.record_experience(NewExperience {
//!     collective_id: collective,
//!     content: "Always validate user input".to_string(),
//!     embedding: Some(vec![0.1f32; 384]),
//!     ..Default::default()
//! })?;
//!
//! // Close when done
//! db.close()?;
//! # Ok(())
//! # }
//! ```
//!
//! # Thread Safety
//!
//! `PulseDB` is `Send + Sync` and can be shared across threads using `Arc`.
//! The underlying storage uses MVCC for concurrent reads with exclusive
//! write locking.
//!
//! ```rust
//! # fn main() -> pulsedb::Result<()> {
//! # let dir = tempfile::tempdir().unwrap();
//! use std::sync::Arc;
//! use pulsedb::{PulseDB, Config};
//!
//! let db = Arc::new(PulseDB::open(dir.path().join("test.db"), Config::default())?);
//!
//! // Clone Arc for use in another thread
//! let db_clone = Arc::clone(&db);
//! std::thread::spawn(move || {
//!     // Safe to use db_clone here
//! });
//! # Ok(())
//! # }
//! ```

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

#[cfg(feature = "sync")]
use tracing::debug;
use tracing::{info, instrument, warn};

use crate::activity::{validate_new_activity, Activity, NewActivity};
use crate::collective::types::CollectiveStats;
use crate::collective::{validate_collective_name, Collective};
use crate::config::{Config, EmbeddingProvider};
use crate::embedding::{create_embedding_service, EmbeddingService};
use crate::error::{NotFoundError, PulseDBError, Result, ValidationError};
use crate::experience::{
    energy as experience_energy, validate_experience_update, validate_new_experience, Experience,
    ExperienceUpdate, NewExperience,
};
use crate::insight::{validate_new_insight, DerivedInsight, NewDerivedInsight};
#[cfg(feature = "sync")]
use crate::relation::ExperienceRelation;
use crate::search::{ContextCandidates, ContextRequest, SearchFilter, SearchResult};
use crate::storage::{open_storage, DatabaseMetadata, StorageEngine};
#[cfg(feature = "sync")]
use crate::types::RelationId;
use crate::types::{CollectiveId, ExperienceId, InsightId, Timestamp};
use crate::vector::HnswIndex;
use crate::watch::{WatchEvent, WatchEventType, WatchFilter, WatchService, WatchStream};

/// The main PulseDB database handle.
///
/// This is the primary interface for all database operations. Create an
/// instance with [`PulseDB::open()`] and close it with [`PulseDB::close()`].
///
/// # Ownership
///
/// `PulseDB` owns its storage and embedding service. When you call `close()`,
/// the database is consumed and cannot be used afterward. This ensures
/// resources are properly released.
pub struct PulseDB {
    /// Storage engine (redb or mock for testing).
    storage: Box<dyn StorageEngine>,

    /// Embedding service (external or ONNX).
    embedding: Box<dyn EmbeddingService>,

    /// Configuration used to open this database.
    config: Config,

    /// Per-collective HNSW vector indexes for experience semantic search.
    ///
    /// Outer RwLock protects the HashMap (add/remove collectives).
    /// Each HnswIndex has its own internal RwLock for concurrent search+insert.
    vectors: RwLock<HashMap<CollectiveId, HnswIndex>>,

    /// Per-collective HNSW vector indexes for insight semantic search.
    ///
    /// Separate from `vectors` to prevent ID collisions between experiences
    /// and insights. Uses InsightId→ExperienceId byte conversion for the
    /// HNSW API (safe because indexes are isolated per collective).
    insight_vectors: RwLock<HashMap<CollectiveId, HnswIndex>>,

    /// Watch service for real-time experience change notifications.
    ///
    /// Arc-wrapped because [`WatchStream`] holds a weak reference for
    /// cleanup on drop.
    watch: Arc<WatchService>,
}

impl std::fmt::Debug for PulseDB {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let vector_count = self.vectors.read().map(|v| v.len()).unwrap_or(0);
        let insight_vector_count = self.insight_vectors.read().map(|v| v.len()).unwrap_or(0);
        f.debug_struct("PulseDB")
            .field("config", &self.config)
            .field("embedding_dimension", &self.embedding_dimension())
            .field("vector_indexes", &vector_count)
            .field("insight_vector_indexes", &insight_vector_count)
            .finish_non_exhaustive()
    }
}

impl PulseDB {
    /// Opens or creates a PulseDB database at the specified path.
    ///
    /// If the database doesn't exist, it will be created with the given
    /// configuration. If it exists, the configuration will be validated
    /// against the stored settings (e.g., embedding dimension must match).
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the database file (created if it doesn't exist)
    /// * `config` - Configuration options for the database
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Configuration is invalid (see [`Config::validate`])
    /// - Database file is corrupted
    /// - Database is locked by another process
    /// - Schema version doesn't match (needs migration)
    /// - Embedding dimension doesn't match existing database
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> pulsedb::Result<()> {
    /// # let dir = tempfile::tempdir().unwrap();
    /// use pulsedb::{PulseDB, Config, EmbeddingDimension};
    ///
    /// // Open with default configuration
    /// let db = PulseDB::open(dir.path().join("default.db"), Config::default())?;
    /// # drop(db);
    ///
    /// // Open with custom embedding dimension
    /// let db = PulseDB::open(dir.path().join("custom.db"), Config {
    ///     embedding_dimension: EmbeddingDimension::D768,
    ///     ..Default::default()
    /// })?;
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(skip(config), fields(path = %path.as_ref().display()))]
    pub fn open(path: impl AsRef<Path>, config: Config) -> Result<Self> {
        // Validate configuration first
        config.validate().map_err(PulseDBError::from)?;

        info!("Opening PulseDB");

        // Open storage engine
        let storage = open_storage(&path, &config)?;

        // Create embedding service
        let embedding = create_embedding_service(&config)?;

        // Load or rebuild HNSW indexes for all existing collectives
        let vectors = Self::load_all_indexes(&*storage, &config)?;
        let insight_vectors = Self::load_all_insight_indexes(&*storage, &config)?;

        info!(
            dimension = config.embedding_dimension.size(),
            sync_mode = ?config.sync_mode,
            collectives = vectors.len(),
            "PulseDB opened successfully"
        );

        let watch = Arc::new(WatchService::new(
            config.watch.buffer_size,
            config.watch.in_process,
        ));

        Ok(Self {
            storage,
            embedding,
            config,
            vectors: RwLock::new(vectors),
            insight_vectors: RwLock::new(insight_vectors),
            watch,
        })
    }

    /// Closes the database, flushing all pending writes.
    ///
    /// This method consumes the `PulseDB` instance, ensuring it cannot
    /// be used after closing. The underlying storage engine flushes all
    /// buffered data to disk.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage backend reports a flush failure.
    /// Note: the current redb backend flushes durably on drop, so this
    /// always returns `Ok(())` in practice.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> pulsedb::Result<()> {
    /// # let dir = tempfile::tempdir().unwrap();
    /// use pulsedb::{PulseDB, Config};
    ///
    /// let db = PulseDB::open(dir.path().join("test.db"), Config::default())?;
    /// // ... use the database ...
    /// db.close()?;  // db is consumed here
    /// // db.something() // Compile error: db was moved
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(skip(self))]
    pub fn close(self) -> Result<()> {
        info!("Closing PulseDB");

        // Persist HNSW indexes BEFORE closing storage.
        // If HNSW save fails, storage is still open for potential recovery.
        // On next open(), stale/missing HNSW files trigger a rebuild from redb.
        if let Some(hnsw_dir) = self.hnsw_dir() {
            // Experience HNSW indexes
            let vectors = self
                .vectors
                .read()
                .map_err(|_| PulseDBError::vector("Vectors lock poisoned during close"))?;
            for (collective_id, index) in vectors.iter() {
                if let Err(e) = index.save_to_dir(&hnsw_dir, &collective_id.to_string()) {
                    warn!(
                        collective = %collective_id,
                        error = %e,
                        "Failed to save HNSW index (will rebuild on next open)"
                    );
                }
            }
            drop(vectors);

            // Insight HNSW indexes (separate files with _insights suffix)
            let insight_vectors = self
                .insight_vectors
                .read()
                .map_err(|_| PulseDBError::vector("Insight vectors lock poisoned during close"))?;
            for (collective_id, index) in insight_vectors.iter() {
                let name = format!("{}_insights", collective_id);
                if let Err(e) = index.save_to_dir(&hnsw_dir, &name) {
                    warn!(
                        collective = %collective_id,
                        error = %e,
                        "Failed to save insight HNSW index (will rebuild on next open)"
                    );
                }
            }
        }

        // Close storage (flushes pending writes)
        self.storage.close()?;

        info!("PulseDB closed successfully");
        Ok(())
    }

    /// Returns a reference to the database configuration.
    ///
    /// This is the configuration that was used to open the database.
    /// Note that some settings (like embedding dimension) are locked
    /// on database creation and cannot be changed.
    #[inline]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Returns the database metadata.
    ///
    /// Metadata includes schema version, embedding dimension, and timestamps
    /// for when the database was created and last opened.
    #[inline]
    pub fn metadata(&self) -> &DatabaseMetadata {
        self.storage.metadata()
    }

    /// Returns the embedding dimension configured for this database.
    ///
    /// All embeddings stored in this database must have exactly this
    /// many dimensions.
    #[inline]
    pub fn embedding_dimension(&self) -> usize {
        self.config.embedding_dimension.size()
    }

    // =========================================================================
    // Internal Accessors (for use by feature modules)
    // =========================================================================

    /// Returns a reference to the storage engine.
    ///
    /// This is for internal use by other PulseDB modules.
    #[inline]
    #[allow(dead_code)] // Will be used by search (Phase 2) and other modules
    pub(crate) fn storage(&self) -> &dyn StorageEngine {
        self.storage.as_ref()
    }

    /// Returns a reference to the embedding service.
    ///
    /// This is for internal use by other PulseDB modules.
    #[inline]
    #[allow(dead_code)] // Will be used by search (Phase 2) and other modules
    pub(crate) fn embedding(&self) -> &dyn EmbeddingService {
        self.embedding.as_ref()
    }

    // =========================================================================
    // HNSW Index Lifecycle
    // =========================================================================

    /// Returns the directory for HNSW index files.
    ///
    /// Derives `{db_path}.hnsw/` from the storage path. Returns `None` if
    /// the storage has no file path (e.g., in-memory tests).
    fn hnsw_dir(&self) -> Option<PathBuf> {
        self.storage.path().map(|p| {
            let mut hnsw_path = p.as_os_str().to_owned();
            hnsw_path.push(".hnsw");
            PathBuf::from(hnsw_path)
        })
    }

    /// Loads or rebuilds HNSW indexes for all existing collectives.
    ///
    /// For each collective in storage:
    /// 1. Try loading metadata from `.hnsw.meta` file
    /// 2. Rebuild the graph from redb embeddings (always, since we can't
    ///    load the graph due to hnsw_rs lifetime constraints)
    /// 3. Restore deleted set from metadata if available
    fn load_all_indexes(
        storage: &dyn StorageEngine,
        config: &Config,
    ) -> Result<HashMap<CollectiveId, HnswIndex>> {
        let collectives = storage.list_collectives()?;
        let mut vectors = HashMap::with_capacity(collectives.len());

        let hnsw_dir = storage.path().map(|p| {
            let mut hnsw_path = p.as_os_str().to_owned();
            hnsw_path.push(".hnsw");
            PathBuf::from(hnsw_path)
        });

        for collective in &collectives {
            let dimension = collective.embedding_dimension as usize;

            // List all experience IDs in this collective
            let exp_ids = storage.list_experience_ids_in_collective(collective.id)?;

            // Load embeddings from redb (source of truth)
            let mut embeddings = Vec::with_capacity(exp_ids.len());
            for exp_id in &exp_ids {
                if let Some(embedding) = storage.get_embedding(*exp_id)? {
                    embeddings.push((*exp_id, embedding));
                }
            }

            // Try loading metadata (for deleted set and ID mappings)
            let metadata = hnsw_dir
                .as_ref()
                .and_then(|dir| HnswIndex::load_metadata(dir, &collective.id.to_string()).ok())
                .flatten();

            // Rebuild the HNSW graph from embeddings
            let index = if embeddings.is_empty() {
                HnswIndex::new(dimension, &config.hnsw)
            } else {
                let start = std::time::Instant::now();
                let idx = HnswIndex::rebuild_from_embeddings(dimension, &config.hnsw, embeddings)?;
                info!(
                    collective = %collective.id,
                    vectors = idx.active_count(),
                    elapsed_ms = start.elapsed().as_millis() as u64,
                    "Rebuilt HNSW index from redb embeddings"
                );
                idx
            };

            // Restore deleted set from metadata if available
            if let Some(meta) = metadata {
                index.restore_deleted_set(&meta.deleted)?;
            }

            vectors.insert(collective.id, index);
        }

        Ok(vectors)
    }

    /// Loads or rebuilds insight HNSW indexes for all existing collectives.
    ///
    /// For each collective, loads all insights from storage and rebuilds
    /// the HNSW graph from their inline embeddings. Uses InsightId→ExperienceId
    /// byte conversion for the HNSW API.
    fn load_all_insight_indexes(
        storage: &dyn StorageEngine,
        config: &Config,
    ) -> Result<HashMap<CollectiveId, HnswIndex>> {
        let collectives = storage.list_collectives()?;
        let mut insight_vectors = HashMap::with_capacity(collectives.len());

        let hnsw_dir = storage.path().map(|p| {
            let mut hnsw_path = p.as_os_str().to_owned();
            hnsw_path.push(".hnsw");
            PathBuf::from(hnsw_path)
        });

        for collective in &collectives {
            let dimension = collective.embedding_dimension as usize;

            // List all insight IDs in this collective
            let insight_ids = storage.list_insight_ids_in_collective(collective.id)?;

            // Load insights and extract embeddings (converting InsightId → ExperienceId)
            let mut embeddings = Vec::with_capacity(insight_ids.len());
            for insight_id in &insight_ids {
                if let Some(insight) = storage.get_insight(*insight_id)? {
                    let exp_id = ExperienceId::from_bytes(*insight_id.as_bytes());
                    embeddings.push((exp_id, insight.embedding));
                }
            }

            // Try loading metadata (for deleted set)
            let name = format!("{}_insights", collective.id);
            let metadata = hnsw_dir
                .as_ref()
                .and_then(|dir| HnswIndex::load_metadata(dir, &name).ok())
                .flatten();

            // Rebuild HNSW graph from embeddings
            let index = if embeddings.is_empty() {
                HnswIndex::new(dimension, &config.hnsw)
            } else {
                let start = std::time::Instant::now();
                let idx = HnswIndex::rebuild_from_embeddings(dimension, &config.hnsw, embeddings)?;
                info!(
                    collective = %collective.id,
                    insights = idx.active_count(),
                    elapsed_ms = start.elapsed().as_millis() as u64,
                    "Rebuilt insight HNSW index from stored insights"
                );
                idx
            };

            // Restore deleted set from metadata if available
            if let Some(meta) = metadata {
                index.restore_deleted_set(&meta.deleted)?;
            }

            insight_vectors.insert(collective.id, index);
        }

        Ok(insight_vectors)
    }

    /// Executes a closure with the HNSW index for a collective.
    ///
    /// This is the primary accessor for vector search operations (used by
    /// `search_similar()`). The closure runs while the outer RwLock guard
    /// is held (read lock), so the HnswIndex reference stays valid.
    /// Returns `None` if no index exists for the collective.
    #[doc(hidden)]
    pub fn with_vector_index<F, R>(&self, collective_id: CollectiveId, f: F) -> Result<Option<R>>
    where
        F: FnOnce(&HnswIndex) -> Result<R>,
    {
        let vectors = self
            .vectors
            .read()
            .map_err(|_| PulseDBError::vector("Vectors lock poisoned"))?;
        match vectors.get(&collective_id) {
            Some(index) => Ok(Some(f(index)?)),
            None => Ok(None),
        }
    }

    // =========================================================================
    // Test Helpers
    // =========================================================================

    /// Returns a reference to the storage engine for integration testing.
    ///
    /// This method is intentionally hidden from documentation. It provides
    /// test-only access to the storage layer for verifying ACID guarantees
    /// and crash recovery. Production code should use the public PulseDB API.
    #[doc(hidden)]
    #[inline]
    pub fn storage_for_test(&self) -> &dyn StorageEngine {
        self.storage.as_ref()
    }

    /// Returns true if this database is in read-only mode.
    pub fn is_read_only(&self) -> bool {
        self.config.read_only
    }

    /// Checks if the database is read-only and returns an error if so.
    #[inline]
    fn check_writable(&self) -> Result<()> {
        if self.config.read_only {
            return Err(PulseDBError::ReadOnly);
        }
        Ok(())
    }

    // =========================================================================
    // Collective Management (E1-S02)
    // =========================================================================

    /// Creates a new collective with the given name.
    ///
    /// The collective's embedding dimension is locked to the database's
    /// configured dimension at creation time.
    ///
    /// # Arguments
    ///
    /// * `name` - Human-readable name (1-255 characters, not whitespace-only)
    ///
    /// # Errors
    ///
    /// Returns a validation error if the name is empty, whitespace-only,
    /// or exceeds 255 characters.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> pulsedb::Result<()> {
    /// # let dir = tempfile::tempdir().unwrap();
    /// # let db = pulsedb::PulseDB::open(dir.path().join("test.db"), pulsedb::Config::default())?;
    /// let id = db.create_collective("my-project")?;
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(skip(self))]
    pub fn create_collective(&self, name: &str) -> Result<CollectiveId> {
        self.check_writable()?;
        validate_collective_name(name)?;

        let dimension = self.config.embedding_dimension.size() as u16;
        let collective = Collective::new(name, dimension);
        let id = collective.id;

        // Persist to redb first (source of truth)
        self.storage.save_collective(&collective)?;

        // Create empty HNSW indexes for this collective
        let exp_index = HnswIndex::new(dimension as usize, &self.config.hnsw);
        let insight_index = HnswIndex::new(dimension as usize, &self.config.hnsw);
        self.vectors
            .write()
            .map_err(|_| PulseDBError::vector("Vectors lock poisoned"))?
            .insert(id, exp_index);
        self.insight_vectors
            .write()
            .map_err(|_| PulseDBError::vector("Insight vectors lock poisoned"))?
            .insert(id, insight_index);

        info!(id = %id, name = %name, "Collective created");
        Ok(id)
    }

    /// Creates a new collective with an owner for multi-tenancy.
    ///
    /// Same as [`create_collective`](Self::create_collective) but assigns
    /// an owner ID, enabling filtering with
    /// [`list_collectives_by_owner`](Self::list_collectives_by_owner).
    ///
    /// # Arguments
    ///
    /// * `name` - Human-readable name (1-255 characters)
    /// * `owner_id` - Owner identifier (must not be empty)
    ///
    /// # Errors
    ///
    /// Returns a validation error if the name or owner_id is invalid.
    #[instrument(skip(self))]
    pub fn create_collective_with_owner(&self, name: &str, owner_id: &str) -> Result<CollectiveId> {
        self.check_writable()?;
        validate_collective_name(name)?;

        if owner_id.is_empty() {
            return Err(PulseDBError::from(
                crate::error::ValidationError::required_field("owner_id"),
            ));
        }

        let dimension = self.config.embedding_dimension.size() as u16;
        let collective = Collective::with_owner(name, owner_id, dimension);
        let id = collective.id;

        // Persist to redb first (source of truth)
        self.storage.save_collective(&collective)?;

        // Create empty HNSW indexes for this collective
        let exp_index = HnswIndex::new(dimension as usize, &self.config.hnsw);
        let insight_index = HnswIndex::new(dimension as usize, &self.config.hnsw);
        self.vectors
            .write()
            .map_err(|_| PulseDBError::vector("Vectors lock poisoned"))?
            .insert(id, exp_index);
        self.insight_vectors
            .write()
            .map_err(|_| PulseDBError::vector("Insight vectors lock poisoned"))?
            .insert(id, insight_index);

        info!(id = %id, name = %name, owner = %owner_id, "Collective created with owner");
        Ok(id)
    }

    /// Returns a collective by ID, or `None` if not found.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> pulsedb::Result<()> {
    /// # let dir = tempfile::tempdir().unwrap();
    /// # let db = pulsedb::PulseDB::open(dir.path().join("test.db"), pulsedb::Config::default())?;
    /// # let id = db.create_collective("example")?;
    /// if let Some(collective) = db.get_collective(id)? {
    ///     println!("Found: {}", collective.name);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(skip(self))]
    pub fn get_collective(&self, id: CollectiveId) -> Result<Option<Collective>> {
        self.storage.get_collective(id)
    }

    /// Lists all collectives in the database.
    ///
    /// Returns an empty vector if no collectives exist.
    pub fn list_collectives(&self) -> Result<Vec<Collective>> {
        self.storage.list_collectives()
    }

    /// Lists collectives filtered by owner ID.
    ///
    /// Returns only collectives whose `owner_id` matches the given value.
    /// Returns an empty vector if no matching collectives exist.
    pub fn list_collectives_by_owner(&self, owner_id: &str) -> Result<Vec<Collective>> {
        let all = self.storage.list_collectives()?;
        Ok(all
            .into_iter()
            .filter(|c| c.owner_id.as_deref() == Some(owner_id))
            .collect())
    }

    /// Returns statistics for a collective.
    ///
    /// # Errors
    ///
    /// Returns [`NotFoundError::Collective`] if the collective doesn't exist.
    #[instrument(skip(self))]
    pub fn get_collective_stats(&self, id: CollectiveId) -> Result<CollectiveStats> {
        // Verify collective exists
        self.storage
            .get_collective(id)?
            .ok_or_else(|| PulseDBError::from(NotFoundError::collective(id)))?;

        let experience_count = self.storage.count_experiences_in_collective(id)?;

        Ok(CollectiveStats {
            experience_count,
            storage_bytes: 0,
            oldest_experience: None,
            newest_experience: None,
        })
    }

    /// Deletes a collective and all its associated data.
    ///
    /// Performs cascade deletion: removes all experiences belonging to the
    /// collective before removing the collective record itself.
    ///
    /// # Errors
    ///
    /// Returns [`NotFoundError::Collective`] if the collective doesn't exist.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> pulsedb::Result<()> {
    /// # let dir = tempfile::tempdir().unwrap();
    /// # let db = pulsedb::PulseDB::open(dir.path().join("test.db"), pulsedb::Config::default())?;
    /// # let collective_id = db.create_collective("to-delete")?;
    /// db.delete_collective(collective_id)?;
    /// assert!(db.get_collective(collective_id)?.is_none());
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(skip(self))]
    pub fn delete_collective(&self, id: CollectiveId) -> Result<()> {
        self.check_writable()?;
        // Verify collective exists
        self.storage
            .get_collective(id)?
            .ok_or_else(|| PulseDBError::from(NotFoundError::collective(id)))?;

        // Cascade: delete all experiences for this collective
        let deleted_count = self.storage.delete_experiences_by_collective(id)?;
        if deleted_count > 0 {
            info!(count = deleted_count, "Cascade-deleted experiences");
        }

        // Cascade: delete all insights for this collective
        let deleted_insights = self.storage.delete_insights_by_collective(id)?;
        if deleted_insights > 0 {
            info!(count = deleted_insights, "Cascade-deleted insights");
        }

        // Cascade: delete all activities for this collective
        let deleted_activities = self.storage.delete_activities_by_collective(id)?;
        if deleted_activities > 0 {
            info!(count = deleted_activities, "Cascade-deleted activities");
        }

        // Delete the collective record from storage
        self.storage.delete_collective(id)?;

        // Remove HNSW indexes from memory
        self.vectors
            .write()
            .map_err(|_| PulseDBError::vector("Vectors lock poisoned"))?
            .remove(&id);
        self.insight_vectors
            .write()
            .map_err(|_| PulseDBError::vector("Insight vectors lock poisoned"))?
            .remove(&id);

        // Remove HNSW files from disk (non-fatal if fails)
        if let Some(hnsw_dir) = self.hnsw_dir() {
            if let Err(e) = HnswIndex::remove_files(&hnsw_dir, &id.to_string()) {
                warn!(
                    collective = %id,
                    error = %e,
                    "Failed to remove experience HNSW files (non-fatal)"
                );
            }
            let insight_name = format!("{}_insights", id);
            if let Err(e) = HnswIndex::remove_files(&hnsw_dir, &insight_name) {
                warn!(
                    collective = %id,
                    error = %e,
                    "Failed to remove insight HNSW files (non-fatal)"
                );
            }
        }

        info!(id = %id, "Collective deleted");
        Ok(())
    }

    // =========================================================================
    // Experience CRUD (E1-S03)
    // =========================================================================

    /// Records a new experience in the database.
    ///
    /// This is the primary method for storing agent-learned knowledge. The method:
    /// 1. Validates the input (content, scores, tags, embedding)
    /// 2. Verifies the collective exists
    /// 3. Resolves the embedding (generates if Builtin, requires if External)
    /// 4. Stores the experience atomically across 4 tables
    ///
    /// # Arguments
    ///
    /// * `exp` - The experience to record (see [`NewExperience`])
    ///
    /// # Errors
    ///
    /// - [`ValidationError`](crate::ValidationError) if input is invalid
    /// - [`NotFoundError::Collective`] if the collective doesn't exist
    /// - [`PulseDBError::Embedding`] if embedding generation fails (Builtin mode)
    #[instrument(skip(self, exp), fields(collective_id = %exp.collective_id))]
    pub fn record_experience(&self, exp: NewExperience) -> Result<ExperienceId> {
        self.check_writable()?;
        let is_external = matches!(self.config.embedding_provider, EmbeddingProvider::External);

        // Verify collective exists and get its dimension
        let collective = self
            .storage
            .get_collective(exp.collective_id)?
            .ok_or_else(|| PulseDBError::from(NotFoundError::collective(exp.collective_id)))?;

        // Validate input
        validate_new_experience(&exp, collective.embedding_dimension, is_external)?;

        // Resolve embedding
        let embedding = match exp.embedding {
            Some(emb) => emb,
            None => {
                // Builtin mode: generate embedding from content
                self.embedding.embed(&exp.content)?
            }
        };

        // Clone embedding for HNSW insertion (~1.5KB for 384d, negligible vs I/O)
        let embedding_for_hnsw = embedding.clone();
        let collective_id = exp.collective_id;

        let timestamp = Timestamp::now();

        // Construct the full experience record
        let experience = Experience {
            id: ExperienceId::new(),
            collective_id,
            content: exp.content,
            embedding,
            experience_type: exp.experience_type,
            importance: exp.importance,
            confidence: exp.confidence,
            applications: BTreeMap::new(),
            domain: exp.domain,
            related_files: exp.related_files,
            source_agent: exp.source_agent,
            source_task: exp.source_task,
            timestamp,
            last_reinforced: timestamp,
            archived: false,
        };

        let id = experience.id;

        // Write to redb FIRST (source of truth). If crash happens after
        // this but before HNSW insert, rebuild on next open will include it.
        self.storage.save_experience(&experience)?;

        // Insert into HNSW index (derived structure)
        let vectors = self
            .vectors
            .read()
            .map_err(|_| PulseDBError::vector("Vectors lock poisoned"))?;
        if let Some(index) = vectors.get(&collective_id) {
            index.insert_experience(id, &embedding_for_hnsw)?;
        }

        // Emit watch event after both storage and HNSW succeed
        self.watch.emit(
            WatchEvent {
                experience_id: id,
                collective_id,
                event_type: WatchEventType::Created,
                timestamp: experience.timestamp,
                experience: Some(experience.clone()),
            },
            &experience,
        )?;

        info!(id = %id, "Experience recorded");
        Ok(id)
    }

    /// Retrieves an experience by ID, including its embedding.
    ///
    /// Returns `None` if no experience with the given ID exists.
    #[instrument(skip(self))]
    pub fn get_experience(&self, id: ExperienceId) -> Result<Option<Experience>> {
        self.storage.get_experience(id)
    }

    /// Updates mutable fields of an experience.
    ///
    /// Only fields set to `Some(...)` in the update are changed.
    /// Content and embedding are immutable — create a new experience instead.
    ///
    /// # Errors
    ///
    /// - [`ValidationError`](crate::ValidationError) if updated values are invalid
    /// - [`NotFoundError::Experience`] if the experience doesn't exist
    #[instrument(skip(self, update))]
    pub fn update_experience(&self, id: ExperienceId, update: ExperienceUpdate) -> Result<()> {
        self.check_writable()?;
        validate_experience_update(&update)?;

        let updated = self.storage.update_experience(id, &update)?;
        if !updated {
            return Err(PulseDBError::from(NotFoundError::experience(id)));
        }

        // Emit watch event (fetch experience for collective_id + filter matching)
        if self.watch.has_subscribers() {
            if let Ok(Some(exp)) = self.storage.get_experience(id) {
                let event_type = if update.archived == Some(true) {
                    WatchEventType::Archived
                } else {
                    WatchEventType::Updated
                };
                self.watch.emit(
                    WatchEvent {
                        experience_id: id,
                        collective_id: exp.collective_id,
                        event_type,
                        timestamp: Timestamp::now(),
                        experience: Some(exp.clone()),
                    },
                    &exp,
                )?;
            }
        }

        info!(id = %id, "Experience updated");
        Ok(())
    }

    /// Archives an experience (soft-delete).
    ///
    /// Archived experiences remain in storage but are excluded from search
    /// results. Use [`unarchive_experience`](Self::unarchive_experience) to restore.
    ///
    /// # Errors
    ///
    /// Returns [`NotFoundError::Experience`] if the experience doesn't exist.
    #[instrument(skip(self))]
    pub fn archive_experience(&self, id: ExperienceId) -> Result<()> {
        self.check_writable()?;
        self.update_experience(
            id,
            ExperienceUpdate {
                archived: Some(true),
                ..Default::default()
            },
        )
    }

    /// Restores an archived experience.
    ///
    /// The experience will once again appear in search results.
    ///
    /// # Errors
    ///
    /// Returns [`NotFoundError::Experience`] if the experience doesn't exist.
    #[instrument(skip(self))]
    pub fn unarchive_experience(&self, id: ExperienceId) -> Result<()> {
        self.check_writable()?;
        self.update_experience(
            id,
            ExperienceUpdate {
                archived: Some(false),
                ..Default::default()
            },
        )
    }

    /// Permanently deletes an experience and its embedding.
    ///
    /// This removes the experience from all tables and indices.
    /// Unlike archiving, this is irreversible.
    ///
    /// # Errors
    ///
    /// Returns [`NotFoundError::Experience`] if the experience doesn't exist.
    #[instrument(skip(self))]
    pub fn delete_experience(&self, id: ExperienceId) -> Result<()> {
        self.check_writable()?;
        // Read experience first to get collective_id for HNSW lookup.
        // This adds one extra read, but delete is not a hot path.
        let experience = self
            .storage
            .get_experience(id)?
            .ok_or_else(|| PulseDBError::from(NotFoundError::experience(id)))?;

        // Cascade-delete any relations involving this experience.
        // Done before experience deletion so we can still look up relation data.
        let rel_count = self.storage.delete_relations_for_experience(id)?;
        if rel_count > 0 {
            info!(
                count = rel_count,
                "Cascade-deleted relations for experience"
            );
        }

        // Delete from redb FIRST (source of truth). If crash happens after
        // this but before HNSW soft-delete, on reopen the experience won't be
        // loaded from redb, so it's automatically excluded from the rebuilt index.
        self.storage.delete_experience(id)?;

        // Soft-delete from HNSW index (mark as deleted, not removed from graph).
        // This takes effect immediately for the current session's searches.
        let vectors = self
            .vectors
            .read()
            .map_err(|_| PulseDBError::vector("Vectors lock poisoned"))?;
        if let Some(index) = vectors.get(&experience.collective_id) {
            index.delete_experience(id)?;
        }

        // Emit watch event after storage + HNSW deletion
        self.watch.emit(
            WatchEvent {
                experience_id: id,
                collective_id: experience.collective_id,
                event_type: WatchEventType::Deleted,
                timestamp: Timestamp::now(),
                experience: None, // Deleted — no data to include
            },
            &experience,
        )?;

        info!(id = %id, "Experience deleted");
        Ok(())
    }

    /// Reinforces an experience by incrementing its application count.
    ///
    /// Each call atomically increments the `applications` counter by 1.
    /// Returns the new application count.
    ///
    /// # Errors
    ///
    /// Returns [`NotFoundError::Experience`] if the experience doesn't exist.
    #[instrument(skip(self))]
    pub fn reinforce_experience(&self, id: ExperienceId) -> Result<u32> {
        self.check_writable()?;
        let new_count = self
            .storage
            .reinforce_experience(id)?
            .ok_or_else(|| PulseDBError::from(NotFoundError::experience(id)))?;

        // Emit watch event (fetch experience for collective_id + filter matching)
        if self.watch.has_subscribers() {
            if let Ok(Some(exp)) = self.storage.get_experience(id) {
                self.watch.emit(
                    WatchEvent {
                        experience_id: id,
                        collective_id: exp.collective_id,
                        event_type: WatchEventType::Updated,
                        timestamp: Timestamp::now(),
                        experience: Some(exp.clone()),
                    },
                    &exp,
                )?;
            }
        }

        info!(id = %id, applications = new_count, "Experience reinforced");
        Ok(new_count)
    }

    /// Computes the current temporal energy for an experience.
    ///
    /// This is a read-only diagnostic: it never writes to storage and does not
    /// require a writable database handle. Per-collective decay configuration
    /// takes precedence over the database's global default.
    ///
    /// # Errors
    ///
    /// Returns [`NotFoundError::Experience`] if the experience doesn't exist.
    #[instrument(skip(self))]
    pub fn energy(&self, id: ExperienceId) -> Result<f32> {
        let experience = self
            .storage
            .get_experience(id)?
            .ok_or_else(|| PulseDBError::from(NotFoundError::experience(id)))?;
        let decay_config = self
            .storage
            .get_decay_config(experience.collective_id)?
            .unwrap_or_else(|| self.config.decay.clone());

        Ok(experience_energy(
            experience.importance,
            experience.applications(),
            experience.last_reinforced,
            Timestamp::now(),
            &decay_config,
        ))
    }

    // =========================================================================
    // Recent Experiences
    // =========================================================================

    // =========================================================================
    // Paginated List Operations (PulseVision)
    // =========================================================================

    /// Lists experiences in a collective with pagination.
    ///
    /// Returns full `Experience` records (including embeddings) ordered by
    /// timestamp. Use `offset` and `limit` for pagination.
    ///
    /// Designed for visualization tools (PulseVision) that need to enumerate
    /// the entire embedding space of a collective.
    #[instrument(skip(self))]
    pub fn list_experiences(
        &self,
        collective_id: CollectiveId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Experience>> {
        let ids = self
            .storage
            .list_experience_ids_paginated(collective_id, limit, offset)?;
        let mut experiences = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(exp) = self.storage.get_experience(id)? {
                experiences.push(exp);
            }
        }
        Ok(experiences)
    }

    /// Lists relations in a collective with pagination.
    #[instrument(skip(self))]
    pub fn list_relations(
        &self,
        collective_id: CollectiveId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<crate::relation::ExperienceRelation>> {
        self.storage
            .list_relations_in_collective(collective_id, limit, offset)
    }

    /// Lists insights in a collective with pagination.
    ///
    /// Returns full `DerivedInsight` records including embeddings.
    #[instrument(skip(self))]
    pub fn list_insights(
        &self,
        collective_id: CollectiveId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<DerivedInsight>> {
        let ids = self
            .storage
            .list_insight_ids_paginated(collective_id, limit, offset)?;
        let mut insights = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(insight) = self.storage.get_insight(id)? {
                insights.push(insight);
            }
        }
        Ok(insights)
    }

    /// Retrieves the most recent experiences in a collective.
    ///
    /// Returns full experiences ordered by timestamp (newest first).
    #[instrument(skip(self))]
    pub fn get_recent_experiences(
        &self,
        collective_id: CollectiveId,
        limit: usize,
    ) -> Result<Vec<Experience>> {
        self.get_recent_experiences_filtered(collective_id, limit, SearchFilter::default())
    }

    /// Retrieves the most recent experiences in a collective with filtering.
    ///
    /// Like [`get_recent_experiences()`](Self::get_recent_experiences), but
    /// applies additional filters on domain, experience type, importance,
    /// confidence, and timestamp.
    ///
    /// Over-fetches from storage (2x `limit`) to account for entries removed
    /// by post-filtering, then truncates to the requested `limit`.
    ///
    /// # Arguments
    ///
    /// * `collective_id` - The collective to query
    /// * `limit` - Maximum number of experiences to return (1-1000)
    /// * `filter` - Filter criteria to apply
    ///
    /// # Errors
    ///
    /// - [`ValidationError::InvalidField`] if `limit` is 0 or > 1000
    /// - [`NotFoundError::Collective`] if the collective doesn't exist
    #[instrument(skip(self, filter))]
    pub fn get_recent_experiences_filtered(
        &self,
        collective_id: CollectiveId,
        limit: usize,
        filter: SearchFilter,
    ) -> Result<Vec<Experience>> {
        // Validate limit
        if limit == 0 || limit > 1000 {
            return Err(
                ValidationError::invalid_field("limit", "must be between 1 and 1000").into(),
            );
        }

        // Verify collective exists
        self.storage
            .get_collective(collective_id)?
            .ok_or_else(|| PulseDBError::from(NotFoundError::collective(collective_id)))?;

        // Over-fetch IDs to account for post-filtering losses
        let over_fetch = limit.saturating_mul(2).min(2000);
        let recent_ids = self
            .storage
            .get_recent_experience_ids(collective_id, over_fetch)?;

        // Load full experiences and apply filter
        let mut results = Vec::with_capacity(limit);
        for (exp_id, _timestamp) in recent_ids {
            if results.len() >= limit {
                break;
            }

            if let Some(experience) = self.storage.get_experience(exp_id)? {
                if filter.matches(&experience) {
                    results.push(experience);
                }
            }
        }

        Ok(results)
    }

    // =========================================================================
    // Similarity Search (E2-S02)
    // =========================================================================

    /// Searches for experiences semantically similar to the query embedding.
    ///
    /// Uses the HNSW vector index for approximate nearest neighbor search,
    /// then fetches full experience records from storage. Archived experiences
    /// are excluded by default.
    ///
    /// Results are sorted by similarity descending (most similar first).
    /// Similarity is computed as `1.0 - cosine_distance`.
    ///
    /// # Arguments
    ///
    /// * `collective_id` - The collective to search within
    /// * `query` - Query embedding vector (must match collective's dimension)
    /// * `k` - Maximum number of results to return (1-1000)
    ///
    /// # Errors
    ///
    /// - [`ValidationError::InvalidField`] if `k` is 0 or > 1000
    /// - [`ValidationError::DimensionMismatch`] if `query.len()` doesn't match
    ///   the collective's embedding dimension
    /// - [`NotFoundError::Collective`] if the collective doesn't exist
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> pulsedb::Result<()> {
    /// # let dir = tempfile::tempdir().unwrap();
    /// # let db = pulsedb::PulseDB::open(dir.path().join("test.db"), pulsedb::Config::default())?;
    /// # let collective_id = db.create_collective("example")?;
    /// let query = vec![0.1f32; 384]; // Your query embedding
    /// let results = db.search_similar(collective_id, &query, 10)?;
    /// for result in &results {
    ///     println!(
    ///         "[{:.3}] {}",
    ///         result.similarity, result.experience.content
    ///     );
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(skip(self, query))]
    pub fn search_similar(
        &self,
        collective_id: CollectiveId,
        query: &[f32],
        k: usize,
    ) -> Result<Vec<SearchResult>> {
        self.search_similar_filtered(collective_id, query, k, SearchFilter::default())
    }

    /// Searches for semantically similar experiences with additional filtering.
    ///
    /// Like [`search_similar()`](Self::search_similar), but applies additional
    /// filters on domain, experience type, importance, confidence, and timestamp.
    ///
    /// Over-fetches from the HNSW index (2x `k`) to account for entries removed
    /// by post-filtering, then truncates to the requested `k`.
    ///
    /// # Arguments
    ///
    /// * `collective_id` - The collective to search within
    /// * `query` - Query embedding vector (must match collective's dimension)
    /// * `k` - Maximum number of results to return (1-1000)
    /// * `filter` - Filter criteria to apply after vector search
    ///
    /// # Errors
    ///
    /// - [`ValidationError::InvalidField`] if `k` is 0 or > 1000
    /// - [`ValidationError::DimensionMismatch`] if `query.len()` doesn't match
    ///   the collective's embedding dimension
    /// - [`NotFoundError::Collective`] if the collective doesn't exist
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> pulsedb::Result<()> {
    /// # let dir = tempfile::tempdir().unwrap();
    /// # let db = pulsedb::PulseDB::open(dir.path().join("test.db"), pulsedb::Config::default())?;
    /// # let collective_id = db.create_collective("example")?;
    /// # let query_embedding = vec![0.1f32; 384];
    /// use pulsedb::SearchFilter;
    ///
    /// let filter = SearchFilter {
    ///     domains: Some(vec!["rust".to_string()]),
    ///     min_importance: Some(0.5),
    ///     ..SearchFilter::default()
    /// };
    /// let results = db.search_similar_filtered(
    ///     collective_id,
    ///     &query_embedding,
    ///     10,
    ///     filter,
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(skip(self, query, filter))]
    pub fn search_similar_filtered(
        &self,
        collective_id: CollectiveId,
        query: &[f32],
        k: usize,
        filter: SearchFilter,
    ) -> Result<Vec<SearchResult>> {
        // Validate k
        if k == 0 || k > 1000 {
            return Err(ValidationError::invalid_field("k", "must be between 1 and 1000").into());
        }

        // Verify collective exists and check embedding dimension
        let collective = self
            .storage
            .get_collective(collective_id)?
            .ok_or_else(|| PulseDBError::from(NotFoundError::collective(collective_id)))?;

        let expected_dim = collective.embedding_dimension as usize;
        if query.len() != expected_dim {
            return Err(ValidationError::dimension_mismatch(expected_dim, query.len()).into());
        }

        // Over-fetch from HNSW to compensate for post-filtering losses
        let over_fetch = k.saturating_mul(2).min(2000);
        let ef_search = self.config.hnsw.ef_search;

        // Search HNSW index — returns (ExperienceId, cosine_distance) sorted
        // by distance ascending (closest first)
        let candidates = self
            .with_vector_index(collective_id, |index| {
                index.search_experiences(query, over_fetch, ef_search)
            })?
            .unwrap_or_default();

        // Fetch full experiences, apply filter, convert distance → similarity
        let mut results = Vec::with_capacity(k);
        for (exp_id, distance) in candidates {
            if results.len() >= k {
                break;
            }

            if let Some(experience) = self.storage.get_experience(exp_id)? {
                if filter.matches(&experience) {
                    results.push(SearchResult {
                        experience,
                        similarity: 1.0 - distance,
                    });
                }
            }
        }

        Ok(results)
    }

    // =========================================================================
    // Experience Relations (E3-S01)
    // =========================================================================

    /// Stores a new relation between two experiences.
    ///
    /// Relations are typed, directed edges connecting a source experience to a
    /// target experience. Both experiences must exist and belong to the same
    /// collective. Duplicate relations (same source, target, and type) are
    /// rejected.
    ///
    /// # Arguments
    ///
    /// * `relation` - The relation to create (source, target, type, strength)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Source or target experience doesn't exist ([`NotFoundError::Experience`])
    /// - Experiences belong to different collectives ([`ValidationError::InvalidField`])
    /// - A relation with the same (source, target, type) already exists
    /// - Self-relation attempted (source == target)
    /// - Strength is out of range `[0.0, 1.0]`
    #[instrument(skip(self, relation))]
    pub fn store_relation(
        &self,
        relation: crate::relation::NewExperienceRelation,
    ) -> Result<crate::types::RelationId> {
        self.check_writable()?;
        use crate::relation::{validate_new_relation, ExperienceRelation};
        use crate::types::RelationId;

        // Validate input fields (self-relation, strength bounds, metadata size)
        validate_new_relation(&relation)?;

        // Load source and target experiences to verify existence
        let source = self
            .storage
            .get_experience(relation.source_id)?
            .ok_or_else(|| PulseDBError::from(NotFoundError::experience(relation.source_id)))?;
        let target = self
            .storage
            .get_experience(relation.target_id)?
            .ok_or_else(|| PulseDBError::from(NotFoundError::experience(relation.target_id)))?;

        // Verify same collective
        if source.collective_id != target.collective_id {
            return Err(PulseDBError::from(ValidationError::invalid_field(
                "target_id",
                "source and target experiences must belong to the same collective",
            )));
        }

        // Check for duplicate (same source, target, type)
        if self.storage.relation_exists(
            relation.source_id,
            relation.target_id,
            relation.relation_type,
        )? {
            return Err(PulseDBError::from(ValidationError::invalid_field(
                "relation_type",
                "a relation with this source, target, and type already exists",
            )));
        }

        // Construct the full relation
        let id = RelationId::new();
        let full_relation = ExperienceRelation {
            id,
            source_id: relation.source_id,
            target_id: relation.target_id,
            relation_type: relation.relation_type,
            strength: relation.strength,
            metadata: relation.metadata,
            created_at: Timestamp::now(),
        };

        self.storage.save_relation(&full_relation)?;

        info!(
            id = %id,
            source = %relation.source_id,
            target = %relation.target_id,
            relation_type = ?full_relation.relation_type,
            "Relation stored"
        );
        Ok(id)
    }

    /// Retrieves experiences related to the given experience.
    ///
    /// Returns pairs of `(Experience, ExperienceRelation)` based on the
    /// requested direction:
    /// - `Outgoing`: experiences that this experience points TO (as source)
    /// - `Incoming`: experiences that point TO this experience (as target)
    /// - `Both`: union of outgoing and incoming
    ///
    /// To filter by relation type, use
    /// [`get_related_experiences_filtered`](Self::get_related_experiences_filtered).
    ///
    /// Silently skips relations where the related experience no longer exists
    /// (orphan tolerance).
    ///
    /// # Errors
    ///
    /// Returns a storage error if the read transaction fails.
    #[instrument(skip(self))]
    pub fn get_related_experiences(
        &self,
        experience_id: ExperienceId,
        direction: crate::relation::RelationDirection,
    ) -> Result<Vec<(Experience, crate::relation::ExperienceRelation)>> {
        self.get_related_experiences_filtered(experience_id, direction, None)
    }

    /// Retrieves experiences related to the given experience, with optional
    /// type filtering.
    ///
    /// Like [`get_related_experiences()`](Self::get_related_experiences), but
    /// accepts an optional [`RelationType`](crate::RelationType) filter.
    /// When `Some(rt)`, only relations matching that type are returned.
    ///
    /// # Arguments
    ///
    /// * `experience_id` - The experience to query relations for
    /// * `direction` - Which direction(s) to traverse
    /// * `relation_type` - If `Some`, only return relations of this type
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> pulsedb::Result<()> {
    /// # let dir = tempfile::tempdir().unwrap();
    /// # let db = pulsedb::PulseDB::open(dir.path().join("test.db"), pulsedb::Config::default())?;
    /// # let cid = db.create_collective("example")?;
    /// # let exp_a = db.record_experience(pulsedb::NewExperience {
    /// #     collective_id: cid,
    /// #     content: "a".into(),
    /// #     embedding: Some(vec![0.1f32; 384]),
    /// #     ..Default::default()
    /// # })?;
    /// use pulsedb::{RelationType, RelationDirection};
    ///
    /// // Only "Supports" relations outgoing from exp_a
    /// let supports = db.get_related_experiences_filtered(
    ///     exp_a,
    ///     RelationDirection::Outgoing,
    ///     Some(RelationType::Supports),
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(skip(self))]
    pub fn get_related_experiences_filtered(
        &self,
        experience_id: ExperienceId,
        direction: crate::relation::RelationDirection,
        relation_type: Option<crate::relation::RelationType>,
    ) -> Result<Vec<(Experience, crate::relation::ExperienceRelation)>> {
        use crate::relation::RelationDirection;

        let mut results = Vec::new();

        // Outgoing: this experience is the source → fetch target experiences
        if matches!(
            direction,
            RelationDirection::Outgoing | RelationDirection::Both
        ) {
            let rel_ids = self.storage.get_relation_ids_by_source(experience_id)?;
            for rel_id in rel_ids {
                if let Some(relation) = self.storage.get_relation(rel_id)? {
                    if relation_type.is_some_and(|rt| rt != relation.relation_type) {
                        continue;
                    }
                    if let Some(experience) = self.storage.get_experience(relation.target_id)? {
                        results.push((experience, relation));
                    }
                }
            }
        }

        // Incoming: this experience is the target → fetch source experiences
        if matches!(
            direction,
            RelationDirection::Incoming | RelationDirection::Both
        ) {
            let rel_ids = self.storage.get_relation_ids_by_target(experience_id)?;
            for rel_id in rel_ids {
                if let Some(relation) = self.storage.get_relation(rel_id)? {
                    if relation_type.is_some_and(|rt| rt != relation.relation_type) {
                        continue;
                    }
                    if let Some(experience) = self.storage.get_experience(relation.source_id)? {
                        results.push((experience, relation));
                    }
                }
            }
        }

        Ok(results)
    }

    /// Retrieves a relation by ID.
    ///
    /// Returns `None` if no relation with the given ID exists.
    pub fn get_relation(
        &self,
        id: crate::types::RelationId,
    ) -> Result<Option<crate::relation::ExperienceRelation>> {
        self.storage.get_relation(id)
    }

    /// Deletes a relation by ID.
    ///
    /// # Errors
    ///
    /// Returns [`NotFoundError::Relation`] if no relation with the given ID exists.
    #[instrument(skip(self))]
    pub fn delete_relation(&self, id: crate::types::RelationId) -> Result<()> {
        self.check_writable()?;
        let deleted = self.storage.delete_relation(id)?;
        if !deleted {
            return Err(PulseDBError::from(NotFoundError::relation(id)));
        }
        info!(id = %id, "Relation deleted");
        Ok(())
    }

    // =========================================================================
    // Derived Insights (E3-S02)
    // =========================================================================

    /// Stores a new derived insight.
    ///
    /// Creates a synthesized knowledge record from multiple source experiences.
    /// The method:
    /// 1. Validates the input (content, confidence, sources)
    /// 2. Verifies the collective exists
    /// 3. Verifies all source experiences exist and belong to the same collective
    /// 4. Resolves the embedding (generates if Builtin, requires if External)
    /// 5. Stores the insight with inline embedding
    /// 6. Inserts into the insight HNSW index
    ///
    /// # Arguments
    ///
    /// * `insight` - The insight to store (see [`NewDerivedInsight`])
    ///
    /// # Errors
    ///
    /// - [`ValidationError`](crate::ValidationError) if input is invalid
    /// - [`NotFoundError::Collective`] if the collective doesn't exist
    /// - [`NotFoundError::Experience`] if any source experience doesn't exist
    /// - [`ValidationError::InvalidField`] if source experiences belong to
    ///   different collectives
    /// - [`ValidationError::DimensionMismatch`] if embedding dimension is wrong
    #[instrument(skip(self, insight), fields(collective_id = %insight.collective_id))]
    pub fn store_insight(&self, insight: NewDerivedInsight) -> Result<InsightId> {
        self.check_writable()?;
        let is_external = matches!(self.config.embedding_provider, EmbeddingProvider::External);

        // Validate input fields
        validate_new_insight(&insight)?;

        // Verify collective exists
        let collective = self
            .storage
            .get_collective(insight.collective_id)?
            .ok_or_else(|| PulseDBError::from(NotFoundError::collective(insight.collective_id)))?;

        // Verify all source experiences exist and belong to this collective
        for source_id in &insight.source_experience_ids {
            let source_exp = self
                .storage
                .get_experience(*source_id)?
                .ok_or_else(|| PulseDBError::from(NotFoundError::experience(*source_id)))?;
            if source_exp.collective_id != insight.collective_id {
                return Err(PulseDBError::from(ValidationError::invalid_field(
                    "source_experience_ids",
                    format!(
                        "experience {} belongs to collective {}, not {}",
                        source_id, source_exp.collective_id, insight.collective_id
                    ),
                )));
            }
        }

        // Resolve embedding
        let embedding = match insight.embedding {
            Some(ref emb) => {
                // Validate dimension
                let expected_dim = collective.embedding_dimension as usize;
                if emb.len() != expected_dim {
                    return Err(ValidationError::dimension_mismatch(expected_dim, emb.len()).into());
                }
                emb.clone()
            }
            None => {
                if is_external {
                    return Err(PulseDBError::embedding(
                        "embedding is required when using External embedding provider",
                    ));
                }
                self.embedding.embed(&insight.content)?
            }
        };

        let embedding_for_hnsw = embedding.clone();
        let now = Timestamp::now();
        let id = InsightId::new();

        // Construct the full insight record
        let derived_insight = DerivedInsight {
            id,
            collective_id: insight.collective_id,
            content: insight.content,
            embedding,
            source_experience_ids: insight.source_experience_ids,
            insight_type: insight.insight_type,
            confidence: insight.confidence,
            domain: insight.domain,
            created_at: now,
            updated_at: now,
        };

        // Write to redb FIRST (source of truth)
        self.storage.save_insight(&derived_insight)?;

        // Insert into insight HNSW index (using InsightId→ExperienceId byte conversion)
        let exp_id = ExperienceId::from_bytes(*id.as_bytes());
        let insight_vectors = self
            .insight_vectors
            .read()
            .map_err(|_| PulseDBError::vector("Insight vectors lock poisoned"))?;
        if let Some(index) = insight_vectors.get(&insight.collective_id) {
            index.insert_experience(exp_id, &embedding_for_hnsw)?;
        }

        info!(id = %id, "Insight stored");
        Ok(id)
    }

    /// Retrieves a derived insight by ID.
    ///
    /// Returns `None` if no insight with the given ID exists.
    #[instrument(skip(self))]
    pub fn get_insight(&self, id: InsightId) -> Result<Option<DerivedInsight>> {
        self.storage.get_insight(id)
    }

    /// Searches for insights semantically similar to the query embedding.
    ///
    /// Uses the insight-specific HNSW index for approximate nearest neighbor
    /// search, then fetches full insight records from storage.
    ///
    /// # Arguments
    ///
    /// * `collective_id` - The collective to search within
    /// * `query` - Query embedding vector (must match collective's dimension)
    /// * `k` - Maximum number of results to return
    ///
    /// # Errors
    ///
    /// - [`ValidationError::DimensionMismatch`] if `query.len()` doesn't match
    /// - [`NotFoundError::Collective`] if the collective doesn't exist
    #[instrument(skip(self, query))]
    pub fn get_insights(
        &self,
        collective_id: CollectiveId,
        query: &[f32],
        k: usize,
    ) -> Result<Vec<(DerivedInsight, f32)>> {
        // Verify collective exists and check embedding dimension
        let collective = self
            .storage
            .get_collective(collective_id)?
            .ok_or_else(|| PulseDBError::from(NotFoundError::collective(collective_id)))?;

        let expected_dim = collective.embedding_dimension as usize;
        if query.len() != expected_dim {
            return Err(ValidationError::dimension_mismatch(expected_dim, query.len()).into());
        }

        let ef_search = self.config.hnsw.ef_search;

        // Search insight HNSW — returns (ExperienceId, distance) pairs
        let insight_vectors = self
            .insight_vectors
            .read()
            .map_err(|_| PulseDBError::vector("Insight vectors lock poisoned"))?;

        let candidates = match insight_vectors.get(&collective_id) {
            Some(index) => index.search_experiences(query, k, ef_search)?,
            None => return Ok(vec![]),
        };
        drop(insight_vectors);

        // Convert ExperienceId back to InsightId and fetch records
        let mut results = Vec::with_capacity(candidates.len());
        for (exp_id, distance) in candidates {
            let insight_id = InsightId::from_bytes(*exp_id.as_bytes());
            if let Some(insight) = self.storage.get_insight(insight_id)? {
                // Convert HNSW distance to similarity (1.0 - distance), matching search_similar pattern
                results.push((insight, 1.0 - distance));
            }
        }

        Ok(results)
    }

    /// Deletes a derived insight by ID.
    ///
    /// Removes the insight from storage and soft-deletes it from the HNSW index.
    ///
    /// # Errors
    ///
    /// Returns [`NotFoundError::Insight`] if no insight with the given ID exists.
    #[instrument(skip(self))]
    pub fn delete_insight(&self, id: InsightId) -> Result<()> {
        self.check_writable()?;
        // Read insight first to get collective_id for HNSW lookup
        let insight = self
            .storage
            .get_insight(id)?
            .ok_or_else(|| PulseDBError::from(NotFoundError::insight(id)))?;

        // Delete from redb FIRST (source of truth)
        self.storage.delete_insight(id)?;

        // Soft-delete from insight HNSW (using InsightId→ExperienceId byte conversion)
        let exp_id = ExperienceId::from_bytes(*id.as_bytes());
        let insight_vectors = self
            .insight_vectors
            .read()
            .map_err(|_| PulseDBError::vector("Insight vectors lock poisoned"))?;
        if let Some(index) = insight_vectors.get(&insight.collective_id) {
            index.delete_experience(exp_id)?;
        }

        info!(id = %id, "Insight deleted");
        Ok(())
    }

    // =========================================================================
    // Activity Tracking (E3-S03)
    // =========================================================================

    /// Registers an agent's presence in a collective.
    ///
    /// Creates a new activity record or replaces an existing one for the
    /// same `(collective_id, agent_id)` pair (upsert semantics). Both
    /// `started_at` and `last_heartbeat` are set to `Timestamp::now()`.
    ///
    /// # Arguments
    ///
    /// * `activity` - The activity registration (see [`NewActivity`])
    ///
    /// # Errors
    ///
    /// - [`ValidationError`] if agent_id is empty or fields exceed size limits
    /// - [`NotFoundError::Collective`] if the collective doesn't exist
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> pulsedb::Result<()> {
    /// # let dir = tempfile::tempdir().unwrap();
    /// # let db = pulsedb::PulseDB::open(dir.path().join("test.db"), pulsedb::Config::default())?;
    /// # let collective_id = db.create_collective("example")?;
    /// use pulsedb::NewActivity;
    ///
    /// db.register_activity(NewActivity {
    ///     agent_id: "claude-opus".to_string(),
    ///     collective_id,
    ///     current_task: Some("Reviewing pull request".to_string()),
    ///     context_summary: None,
    /// })?;
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(skip(self, activity), fields(agent_id = %activity.agent_id, collective_id = %activity.collective_id))]
    pub fn register_activity(&self, activity: NewActivity) -> Result<()> {
        // Validate input
        validate_new_activity(&activity)?;

        // Verify collective exists
        self.storage
            .get_collective(activity.collective_id)?
            .ok_or_else(|| PulseDBError::from(NotFoundError::collective(activity.collective_id)))?;

        // Build stored activity with timestamps
        let now = Timestamp::now();
        let stored = Activity {
            agent_id: activity.agent_id,
            collective_id: activity.collective_id,
            current_task: activity.current_task,
            context_summary: activity.context_summary,
            started_at: now,
            last_heartbeat: now,
        };

        self.storage.save_activity(&stored)?;

        info!(
            agent_id = %stored.agent_id,
            collective_id = %stored.collective_id,
            "Activity registered"
        );
        Ok(())
    }

    /// Updates an agent's heartbeat timestamp.
    ///
    /// Refreshes the `last_heartbeat` to `Timestamp::now()` without changing
    /// any other fields. The agent must have an existing activity registered.
    ///
    /// # Errors
    ///
    /// - [`NotFoundError::Activity`] if no activity exists for the agent/collective pair
    #[instrument(skip(self))]
    pub fn update_heartbeat(&self, agent_id: &str, collective_id: CollectiveId) -> Result<()> {
        self.check_writable()?;
        let mut activity = self
            .storage
            .get_activity(agent_id, collective_id)?
            .ok_or_else(|| {
                PulseDBError::from(NotFoundError::activity(format!(
                    "{} in {}",
                    agent_id, collective_id
                )))
            })?;

        activity.last_heartbeat = Timestamp::now();
        self.storage.save_activity(&activity)?;

        info!(agent_id = %agent_id, collective_id = %collective_id, "Heartbeat updated");
        Ok(())
    }

    /// Ends an agent's activity in a collective.
    ///
    /// Removes the activity record. After calling this, the agent will no
    /// longer appear in `get_active_agents()` results.
    ///
    /// # Errors
    ///
    /// - [`NotFoundError::Activity`] if no activity exists for the agent/collective pair
    #[instrument(skip(self))]
    pub fn end_activity(&self, agent_id: &str, collective_id: CollectiveId) -> Result<()> {
        let deleted = self.storage.delete_activity(agent_id, collective_id)?;

        if !deleted {
            return Err(PulseDBError::from(NotFoundError::activity(format!(
                "{} in {}",
                agent_id, collective_id
            ))));
        }

        info!(agent_id = %agent_id, collective_id = %collective_id, "Activity ended");
        Ok(())
    }

    /// Returns all active (non-stale) agents in a collective.
    ///
    /// Fetches all activities, filters out those whose `last_heartbeat` is
    /// older than `config.activity.stale_threshold`, and returns the rest
    /// sorted by `last_heartbeat` descending (most recently active first).
    ///
    /// # Errors
    ///
    /// - [`NotFoundError::Collective`] if the collective doesn't exist
    #[instrument(skip(self))]
    pub fn get_active_agents(&self, collective_id: CollectiveId) -> Result<Vec<Activity>> {
        // Verify collective exists
        self.storage
            .get_collective(collective_id)?
            .ok_or_else(|| PulseDBError::from(NotFoundError::collective(collective_id)))?;

        let all_activities = self.storage.list_activities_in_collective(collective_id)?;

        // Filter stale activities
        let now = Timestamp::now();
        let threshold_ms = self.config.activity.stale_threshold.as_millis() as i64;
        let cutoff = now.as_millis() - threshold_ms;

        let mut active: Vec<Activity> = all_activities
            .into_iter()
            .filter(|a| a.last_heartbeat.as_millis() >= cutoff)
            .collect();

        // Sort by last_heartbeat descending (most recently active first)
        active.sort_by(|a, b| b.last_heartbeat.cmp(&a.last_heartbeat));

        Ok(active)
    }

    // =========================================================================
    // Context Candidates (E2-S04)
    // =========================================================================

    /// Retrieves unified context candidates from all retrieval primitives.
    ///
    /// This is the primary API for context assembly. It orchestrates:
    /// 1. Similarity search ([`search_similar_filtered`](Self::search_similar_filtered))
    /// 2. Recent experiences ([`get_recent_experiences_filtered`](Self::get_recent_experiences_filtered))
    /// 3. Insight search ([`get_insights`](Self::get_insights)) — if requested
    /// 4. Relation collection ([`get_related_experiences`](Self::get_related_experiences)) — if requested
    /// 5. Active agents ([`get_active_agents`](Self::get_active_agents)) — if requested
    ///
    /// # Arguments
    ///
    /// * `request` - Configuration for which primitives to query and limits
    ///
    /// # Errors
    ///
    /// - [`ValidationError::InvalidField`] if `max_similar` or `max_recent` is 0 or > 1000
    /// - [`ValidationError::DimensionMismatch`] if `query_embedding.len()` doesn't match
    ///   the collective's embedding dimension
    /// - [`NotFoundError::Collective`] if the collective doesn't exist
    ///
    /// # Performance
    ///
    /// Target: < 100ms at 100K experiences. The similarity search (~50ms) dominates;
    /// all other sub-calls are < 10ms each.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> pulsedb::Result<()> {
    /// # let dir = tempfile::tempdir().unwrap();
    /// # let db = pulsedb::PulseDB::open(dir.path().join("test.db"), pulsedb::Config::default())?;
    /// # let collective_id = db.create_collective("example")?;
    /// # let query_vec = vec![0.1f32; 384];
    /// use pulsedb::{ContextRequest, SearchFilter};
    ///
    /// let candidates = db.get_context_candidates(ContextRequest {
    ///     collective_id,
    ///     query_embedding: query_vec,
    ///     max_similar: 10,
    ///     max_recent: 5,
    ///     include_insights: true,
    ///     include_relations: true,
    ///     include_active_agents: true,
    ///     filter: SearchFilter {
    ///         domains: Some(vec!["rust".to_string()]),
    ///         ..SearchFilter::default()
    ///     },
    ///     ..ContextRequest::default()
    /// })?;
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(skip(self, request), fields(collective_id = %request.collective_id))]
    pub fn get_context_candidates(&self, request: ContextRequest) -> Result<ContextCandidates> {
        // ── Validate limits ──────────────────────────────────────
        if request.max_similar == 0 || request.max_similar > 1000 {
            return Err(ValidationError::invalid_field(
                "max_similar",
                "must be between 1 and 1000",
            )
            .into());
        }
        if request.max_recent == 0 || request.max_recent > 1000 {
            return Err(
                ValidationError::invalid_field("max_recent", "must be between 1 and 1000").into(),
            );
        }

        // ── Verify collective exists and check dimension ─────────
        let collective = self
            .storage
            .get_collective(request.collective_id)?
            .ok_or_else(|| PulseDBError::from(NotFoundError::collective(request.collective_id)))?;

        let expected_dim = collective.embedding_dimension as usize;
        if request.query_embedding.len() != expected_dim {
            return Err(ValidationError::dimension_mismatch(
                expected_dim,
                request.query_embedding.len(),
            )
            .into());
        }

        // ── 1. Similar experiences (HNSW vector search) ──────────
        let similar_experiences = self.search_similar_filtered(
            request.collective_id,
            &request.query_embedding,
            request.max_similar,
            request.filter.clone(),
        )?;

        // ── 2. Recent experiences (timestamp index scan) ─────────
        let recent_experiences = self.get_recent_experiences_filtered(
            request.collective_id,
            request.max_recent,
            request.filter,
        )?;

        // ── 3. Insights (HNSW vector search on insight index) ────
        let insights = if request.include_insights {
            self.get_insights(
                request.collective_id,
                &request.query_embedding,
                request.max_similar,
            )?
            .into_iter()
            .map(|(insight, _score)| insight)
            .collect()
        } else {
            vec![]
        };

        // ── 4. Relations (graph traversal from result experiences) ─
        let relations = if request.include_relations {
            use std::collections::HashSet;

            let mut seen = HashSet::new();
            let mut all_relations = Vec::new();

            // Collect unique experience IDs from both result sets
            let exp_ids: Vec<_> = similar_experiences
                .iter()
                .map(|r| r.experience.id)
                .chain(recent_experiences.iter().map(|e| e.id))
                .collect();

            for exp_id in exp_ids {
                let related =
                    self.get_related_experiences(exp_id, crate::relation::RelationDirection::Both)?;

                for (_experience, relation) in related {
                    if seen.insert(relation.id) {
                        all_relations.push(relation);
                    }
                }
            }

            all_relations
        } else {
            vec![]
        };

        // ── 5. Active agents (staleness-filtered activity records) ─
        let active_agents = if request.include_active_agents {
            self.get_active_agents(request.collective_id)?
        } else {
            vec![]
        };

        Ok(ContextCandidates {
            similar_experiences,
            recent_experiences,
            insights,
            relations,
            active_agents,
        })
    }

    // =========================================================================
    // Watch System (E4-S01)
    // =========================================================================

    /// Subscribes to all experience changes in a collective.
    ///
    /// Returns a [`WatchStream`] that yields [`WatchEvent`] values for every
    /// create, update, archive, and delete operation. The stream ends when
    /// dropped or when the `PulseDB` instance is closed.
    ///
    /// Multiple subscribers per collective are supported. Each gets an
    /// independent copy of every event.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # #[tokio::main]
    /// # async fn main() -> pulsedb::Result<()> {
    /// # let dir = tempfile::tempdir().unwrap();
    /// # let db = pulsedb::PulseDB::open(dir.path().join("test.db"), pulsedb::Config::default())?;
    /// # let collective_id = db.create_collective("example")?;
    /// use futures::StreamExt;
    ///
    /// let mut stream = db.watch_experiences(collective_id)?;
    /// while let Some(event) = stream.next().await {
    ///     println!("{:?}: {}", event.event_type, event.experience_id);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn watch_experiences(&self, collective_id: CollectiveId) -> Result<WatchStream> {
        self.watch.subscribe(collective_id, None)
    }

    /// Subscribes to filtered experience changes in a collective.
    ///
    /// Like [`watch_experiences`](Self::watch_experiences), but only delivers
    /// events that match the filter criteria. Filters are applied on the
    /// sender side before channel delivery.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> pulsedb::Result<()> {
    /// # let dir = tempfile::tempdir().unwrap();
    /// # let db = pulsedb::PulseDB::open(dir.path().join("test.db"), pulsedb::Config::default())?;
    /// # let collective_id = db.create_collective("example")?;
    /// use pulsedb::WatchFilter;
    ///
    /// let filter = WatchFilter {
    ///     domains: Some(vec!["security".to_string()]),
    ///     min_importance: Some(0.7),
    ///     ..Default::default()
    /// };
    /// let mut stream = db.watch_experiences_filtered(collective_id, filter)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn watch_experiences_filtered(
        &self,
        collective_id: CollectiveId,
        filter: WatchFilter,
    ) -> Result<WatchStream> {
        self.watch.subscribe(collective_id, Some(filter))
    }

    // =========================================================================
    // Cross-Process Watch (E4-S02)
    // =========================================================================

    /// Returns the current WAL sequence number.
    ///
    /// Use this to establish a baseline before starting to poll for changes.
    /// Returns 0 if no experience writes have occurred yet.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> pulsedb::Result<()> {
    /// # let dir = tempfile::tempdir().unwrap();
    /// # let db = pulsedb::PulseDB::open(dir.path().join("test.db"), pulsedb::Config::default())?;
    /// let seq = db.get_current_sequence()?;
    /// // ... later ...
    /// let (events, new_seq) = db.poll_changes(seq)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_current_sequence(&self) -> Result<u64> {
        self.storage.get_wal_sequence()
    }

    /// Polls for experience changes since the given sequence number.
    ///
    /// Returns a tuple of `(events, new_sequence)`:
    /// - `events`: New [`WatchEvent`]s in sequence order
    /// - `new_sequence`: Pass this value back on the next call
    ///
    /// Returns an empty vec and the same sequence if no changes exist.
    ///
    /// # Arguments
    ///
    /// * `since_seq` - The last sequence number you received (0 for first call)
    ///
    /// # Performance
    ///
    /// Target: < 10ms per call. Internally performs a range scan on the
    /// watch_events table, O(k) where k is the number of new events.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # fn main() -> pulsedb::Result<()> {
    /// # let dir = tempfile::tempdir().unwrap();
    /// # let db = pulsedb::PulseDB::open(dir.path().join("test.db"), pulsedb::Config::default())?;
    /// use std::time::Duration;
    ///
    /// let mut seq = 0u64;
    /// loop {
    ///     let (events, new_seq) = db.poll_changes(seq)?;
    ///     seq = new_seq;
    ///     for event in events {
    ///         println!("{:?}: {}", event.event_type, event.experience_id);
    ///     }
    ///     std::thread::sleep(Duration::from_millis(100));
    /// }
    /// # }
    /// ```
    pub fn poll_changes(&self, since_seq: u64) -> Result<(Vec<WatchEvent>, u64)> {
        use crate::storage::schema::EntityTypeTag;
        let (records, new_seq) = self.storage.poll_watch_events(since_seq, 1000)?;
        let events = records
            .into_iter()
            .filter(|r| r.entity_type == EntityTypeTag::Experience)
            .map(WatchEvent::from)
            .collect();
        Ok((events, new_seq))
    }

    /// Polls for changes with a custom batch size limit.
    ///
    /// Same as [`poll_changes`](Self::poll_changes) but returns at most
    /// `limit` events per call. Use this for backpressure control.
    pub fn poll_changes_batch(
        &self,
        since_seq: u64,
        limit: usize,
    ) -> Result<(Vec<WatchEvent>, u64)> {
        use crate::storage::schema::EntityTypeTag;
        let (records, new_seq) = self.storage.poll_watch_events(since_seq, limit)?;
        let events = records
            .into_iter()
            .filter(|r| r.entity_type == EntityTypeTag::Experience)
            .map(WatchEvent::from)
            .collect();
        Ok((events, new_seq))
    }

    // =========================================================================
    // Sync WAL Compaction (feature: sync)
    // =========================================================================

    /// Compacts the WAL by removing events that all peers have already synced.
    ///
    /// Finds the minimum cursor across all known peers and deletes WAL events
    /// up to that sequence. If no peers exist, no compaction occurs (events
    /// may be needed when a peer connects later).
    ///
    /// Call this periodically (e.g., daily) to reclaim disk space.
    /// Returns the number of WAL events deleted.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # fn main() -> pulsedb::Result<()> {
    /// # let dir = tempfile::tempdir().unwrap();
    /// # let db = pulsedb::PulseDB::open(dir.path().join("test.db"), pulsedb::Config::default())?;
    /// let deleted = db.compact_wal()?;
    /// println!("Compacted {} WAL events", deleted);
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "sync")]
    pub fn compact_wal(&self) -> Result<u64> {
        let cursors = self
            .storage
            .list_sync_cursors()
            .map_err(|e| PulseDBError::internal(format!("Failed to list sync cursors: {}", e)))?;

        if cursors.is_empty() {
            // No peers — don't compact (events may be needed later)
            return Ok(0);
        }

        let min_seq = cursors.iter().map(|c| c.last_sequence).min().unwrap_or(0);

        if min_seq == 0 {
            return Ok(0);
        }

        let deleted = self.storage.compact_wal_events(min_seq)?;
        info!(deleted, min_seq, "WAL compacted");
        Ok(deleted)
    }

    // =========================================================================
    // Sync Apply Methods (feature: sync)
    // =========================================================================
    //
    // These methods apply remote changes received via sync. They bypass
    // validation and embedding generation (data was validated on the source).
    // WAL recording is suppressed by the SyncApplyGuard (entered by the caller).
    // Watch emit is skipped (no in-process notifications for sync changes).
    //
    // These are pub(crate) and will be called by the sync applier in Phase 3.

    /// Applies a synced experience from a remote peer.
    ///
    /// Writes the full experience to storage and inserts into HNSW.
    /// Caller must hold `SyncApplyGuard` to suppress WAL recording.
    #[cfg(feature = "sync")]
    #[allow(dead_code)] // Called by sync applier (Phase 3)
    pub fn apply_synced_experience(&self, experience: Experience) -> Result<()> {
        let collective_id = experience.collective_id;
        let id = experience.id;
        let embedding = experience.embedding.clone();

        self.storage.save_experience(&experience)?;

        // Insert into HNSW index
        let vectors = self
            .vectors
            .read()
            .map_err(|_| PulseDBError::vector("Vectors lock poisoned"))?;
        if let Some(index) = vectors.get(&collective_id) {
            index.insert_experience(id, &embedding)?;
        }

        debug!(id = %id, "Synced experience applied");
        Ok(())
    }

    /// Applies a synced experience update from a remote peer.
    ///
    /// Caller must hold `SyncApplyGuard` to suppress WAL recording.
    #[cfg(feature = "sync")]
    #[allow(dead_code)] // Called by sync applier (Phase 3)
    pub fn apply_synced_experience_update(
        &self,
        id: ExperienceId,
        update: ExperienceUpdate,
    ) -> Result<()> {
        self.storage.update_experience(id, &update)?;
        debug!(id = %id, "Synced experience update applied");
        Ok(())
    }

    /// Applies a synced experience deletion from a remote peer.
    ///
    /// Removes from storage and soft-deletes from HNSW.
    /// Caller must hold `SyncApplyGuard` to suppress WAL recording.
    #[cfg(feature = "sync")]
    #[allow(dead_code)] // Called by sync applier (Phase 3)
    pub fn apply_synced_experience_delete(&self, id: ExperienceId) -> Result<()> {
        // Get collective_id for HNSW lookup before deleting
        if let Some(exp) = self.storage.get_experience(id)? {
            let collective_id = exp.collective_id;

            // Cascade delete relations
            self.storage.delete_relations_for_experience(id)?;

            self.storage.delete_experience(id)?;

            // Soft-delete from HNSW
            let vectors = self
                .vectors
                .read()
                .map_err(|_| PulseDBError::vector("Vectors lock poisoned"))?;
            if let Some(index) = vectors.get(&collective_id) {
                index.delete_experience(id)?;
            }
        }

        debug!(id = %id, "Synced experience delete applied");
        Ok(())
    }

    /// Applies a synced relation from a remote peer.
    ///
    /// Caller must hold `SyncApplyGuard` to suppress WAL recording.
    #[cfg(feature = "sync")]
    #[allow(dead_code)] // Called by sync applier (Phase 3)
    pub fn apply_synced_relation(&self, relation: ExperienceRelation) -> Result<()> {
        let id = relation.id;
        self.storage.save_relation(&relation)?;
        debug!(id = %id, "Synced relation applied");
        Ok(())
    }

    /// Applies a synced relation deletion from a remote peer.
    ///
    /// Caller must hold `SyncApplyGuard` to suppress WAL recording.
    #[cfg(feature = "sync")]
    #[allow(dead_code)] // Called by sync applier (Phase 3)
    pub fn apply_synced_relation_delete(&self, id: RelationId) -> Result<()> {
        self.storage.delete_relation(id)?;
        debug!(id = %id, "Synced relation delete applied");
        Ok(())
    }

    /// Applies a synced insight from a remote peer.
    ///
    /// Writes to storage and inserts into insight HNSW index.
    /// Caller must hold `SyncApplyGuard` to suppress WAL recording.
    #[cfg(feature = "sync")]
    #[allow(dead_code)] // Called by sync applier (Phase 3)
    pub fn apply_synced_insight(&self, insight: DerivedInsight) -> Result<()> {
        let id = insight.id;
        let collective_id = insight.collective_id;
        let embedding = insight.embedding.clone();

        self.storage.save_insight(&insight)?;

        // Insert into insight HNSW (using InsightId→ExperienceId byte conversion)
        let exp_id = ExperienceId::from_bytes(*id.as_bytes());
        let insight_vectors = self
            .insight_vectors
            .read()
            .map_err(|_| PulseDBError::vector("Insight vectors lock poisoned"))?;
        if let Some(index) = insight_vectors.get(&collective_id) {
            index.insert_experience(exp_id, &embedding)?;
        }

        debug!(id = %id, "Synced insight applied");
        Ok(())
    }

    /// Applies a synced insight deletion from a remote peer.
    ///
    /// Removes from storage and soft-deletes from insight HNSW.
    /// Caller must hold `SyncApplyGuard` to suppress WAL recording.
    #[cfg(feature = "sync")]
    #[allow(dead_code)] // Called by sync applier (Phase 3)
    pub fn apply_synced_insight_delete(&self, id: InsightId) -> Result<()> {
        if let Some(insight) = self.storage.get_insight(id)? {
            self.storage.delete_insight(id)?;

            // Soft-delete from insight HNSW
            let exp_id = ExperienceId::from_bytes(*id.as_bytes());
            let insight_vectors = self
                .insight_vectors
                .read()
                .map_err(|_| PulseDBError::vector("Insight vectors lock poisoned"))?;
            if let Some(index) = insight_vectors.get(&insight.collective_id) {
                index.delete_experience(exp_id)?;
            }
        }

        debug!(id = %id, "Synced insight delete applied");
        Ok(())
    }

    /// Applies a synced collective from a remote peer.
    ///
    /// Writes to storage and creates HNSW indexes for the collective.
    /// Caller must hold `SyncApplyGuard` to suppress WAL recording.
    #[cfg(feature = "sync")]
    #[allow(dead_code)] // Called by sync applier (Phase 3)
    pub fn apply_synced_collective(&self, collective: Collective) -> Result<()> {
        let id = collective.id;
        let dimension = collective.embedding_dimension as usize;

        self.storage.save_collective(&collective)?;

        // Create HNSW indexes (same as create_collective)
        let exp_index = crate::vector::HnswIndex::new(dimension, &self.config.hnsw);
        let insight_index = crate::vector::HnswIndex::new(dimension, &self.config.hnsw);
        self.vectors
            .write()
            .map_err(|_| PulseDBError::vector("Vectors lock poisoned"))?
            .insert(id, exp_index);
        self.insight_vectors
            .write()
            .map_err(|_| PulseDBError::vector("Insight vectors lock poisoned"))?
            .insert(id, insight_index);

        debug!(id = %id, "Synced collective applied");
        Ok(())
    }
}

// PulseDB is auto Send + Sync: Box<dyn StorageEngine + Send + Sync>,
// Box<dyn EmbeddingService + Send + Sync>, and Config are all Send + Sync.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::EmbeddingDimension;
    use tempfile::tempdir;

    #[test]
    fn test_open_creates_database() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let db = PulseDB::open(&path, Config::default()).unwrap();

        assert!(path.exists());
        assert_eq!(db.embedding_dimension(), 384);

        db.close().unwrap();
    }

    #[test]
    fn test_open_existing_database() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        // Create
        let db = PulseDB::open(&path, Config::default()).unwrap();
        db.close().unwrap();

        // Reopen
        let db = PulseDB::open(&path, Config::default()).unwrap();
        assert_eq!(db.embedding_dimension(), 384);
        db.close().unwrap();
    }

    #[test]
    fn test_config_validation() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let invalid_config = Config {
            cache_size_mb: 0, // Invalid
            ..Default::default()
        };

        let result = PulseDB::open(&path, invalid_config);
        assert!(result.is_err());
    }

    #[test]
    fn test_dimension_mismatch() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        // Create with D384
        let db = PulseDB::open(
            &path,
            Config {
                embedding_dimension: EmbeddingDimension::D384,
                ..Default::default()
            },
        )
        .unwrap();
        db.close().unwrap();

        // Try to reopen with D768
        let result = PulseDB::open(
            &path,
            Config {
                embedding_dimension: EmbeddingDimension::D768,
                ..Default::default()
            },
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_metadata_access() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let db = PulseDB::open(&path, Config::default()).unwrap();

        let metadata = db.metadata();
        assert_eq!(metadata.embedding_dimension, EmbeddingDimension::D384);

        db.close().unwrap();
    }

    #[test]
    fn test_pulsedb_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<PulseDB>();
    }
}
