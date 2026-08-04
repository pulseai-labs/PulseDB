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
use crate::config::{Config, DecayConfig, EmbeddingProvider, RecallWeights};
use crate::embedding::{create_embedding_service, EmbeddingService};
use crate::error::{NotFoundError, PulseDBError, Result, StorageError, ValidationError};
use crate::experience::{
    energy as experience_energy, validate_experience_update, validate_new_experience, Experience,
    ExperienceUpdate, NewExperience,
};
use crate::insight::{validate_new_insight, DerivedInsight, NewDerivedInsight};
#[cfg(feature = "sync")]
use crate::relation::ExperienceRelation;
use crate::search::rerank::{self, is_legacy_recall, resolve_recall_weights};
use crate::search::{ContextCandidates, ContextRequest, SearchFilter, SearchOptions, SearchResult};
use crate::storage::{open_storage, DatabaseMetadata, StorageEngine};
#[cfg(feature = "sync")]
use crate::types::InstanceId;
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

    /// Embedding service (external or ONNX), or a caller-injected instance.
    ///
    /// Held as `Arc<dyn>` (was `Box<dyn>`) so a caller that retains a handle
    /// to the embedder — e.g. PulseBase wiring its candle stack into other
    /// subsystems — shares the same instance that backs this open `PulseDB`.
    /// Both embed call sites (`record_experience` and `store_insight`) reach
    /// the injected instance through this field.
    embedding: Arc<dyn EmbeddingService>,

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

    /// Whether this `PulseDB` was opened via [`open_with_embedder`] (true) or
    /// [`open`] (false). Set at the divergence point in each constructor —
    /// NOT inside [`open_parts`] (the shared helper does not know which path
    /// called it).
    ///
    /// When true, the validation gate in [`record_experience`] and
    /// [`store_insight`] treats `embedding: None` as always valid: an injected
    /// embedder handles it regardless of what [`EmbeddingProvider`](crate::config::EmbeddingProvider)
    /// the config carries. When false, the pre-1.04 behavior is preserved
    /// exactly (`External` still requires pre-computed embeddings).
    ///
    /// Private + instance-level: not part of [`Config`](crate::config::Config),
    /// does not serialize, does not affect any public signature.
    has_injected_embedder: bool,
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
        // Validate config BEFORE constructing the embedder (Codex review):
        // `create_embedding_service` for `Builtin` downloads/loads the ONNX
        // model (expensive), so an invalid config (e.g. `cache_size_mb == 0`)
        // should fail fast here, not after the model download. `open_parts`
        // keeps its own `config.validate()` as defense-in-depth.
        config.validate().map_err(PulseDBError::from)?;

        // Build the embedder internally (unchanged contract) and delegate to
        // the shared open helper. The `Box`→`Arc` conversion happens here so
        // `create_embedding_service` keeps its existing `Box<dyn>` return type.
        let embedding: Arc<dyn EmbeddingService> = Arc::from(create_embedding_service(&config)?);

        // VS-4.3.3 work 1.01 — the `open` path now stamps its config-derived
        // identity AND runs the cross-provider-mismatch guard, mirroring
        // [`open_with_embedder`]. This closes `pulsedb-internal` #7: a store
        // stamped for provider A could previously be reopened via `open` with
        // provider B and silently mixed in one HNSW index. The two paths now
        // diverge only in *which* identity they stamp (config-derived here vs.
        // injected in [`open_with_embedder`]), not in *whether* they stamp.
        //
        // The 4-state match on `(persisted identity, era marker)`:
        //   - (None, false)         → genuine pre-0.7.0 store, BOTH keys absent;
        //                             lenient adoption (stamp the requested).
        //   - (None, true)          → post-0.7.0 store whose stamp was LOST or
        //                             corrupted (era present, identity absent);
        //                             typed corruption error, NOT silent adoption.
        //   - (Some(persisted), _)  → a stamp exists; run the mismatch guard
        //                             (with a one-time legacy `main_graph`
        //                             migration). Re-stamp only on adoption.
        let (storage, vectors, insight_vectors, watch) = Self::open_parts(&path, &config)?;
        let requested = Self::config_derived_identity(&*embedding);

        let persisted = storage.provider_identity()?;
        let era = storage.provider_identity_era_marker()?;
        let should_stamp: Option<crate::embedding::ProviderIdentity> = match (persisted, era) {
            (None, false) => {
                // Genuine pre-0.7.0 store: BOTH keys absent. Lenient adoption.
                tracing::debug!(
                    "open: no provider-identity stamp and no era marker — \
                     lenient adoption (genuine pre-0.7.0 store)"
                );
                Some(requested.clone())
            }
            (None, true) => {
                // Post-0.7.0 store whose stamp was LOST/CORRUPTED (era marker
                // present, identity absent). NOT silent re-adoption — typed
                // corruption error (closes `pulsedb-internal` #17).
                return Err(PulseDBError::Storage(StorageError::corrupted(
                    "provider identity stamp missing but era marker present — \
                     the stamp was lost or corrupted; refusing silent re-adoption",
                )));
            }
            (Some(persisted), _) => {
                // A stamp exists. Check for the VS-4.3.1-era
                // {builtin-onnx, main_graph} legacy marker FIRST (one-time
                // migration), then the mismatch guard.
                if persisted.provider == "builtin-onnx"
                    && persisted.model_id == "main_graph"
                    && requested.provider == "builtin-onnx"
                    && requested.model_id
                        == format!("onnx-{}", crate::embedding::BUNDLED_MINILM_FINGERPRINT)
                {
                    tracing::info!(
                        persisted_model_id = %persisted.model_id,
                        "open: migrating legacy {{builtin-onnx, main_graph}} stamp to \
                         the loaded model's onnx-<hash> identity"
                    );
                    // Re-stamp with whatever the LOADED model actually is
                    // (requested), not an assumed fingerprint — feature-agnostic
                    // and correct even if a non-MiniLM builtin is loaded.
                    Some(requested.clone())
                } else if persisted.provider != requested.provider
                    || persisted.model_id != requested.model_id
                {
                    return Err(PulseDBError::EmbeddingProviderMismatch {
                        persisted,
                        requested,
                    });
                } else {
                    // Match — the existing stamp is authoritative; skip the
                    // redundant re-stamp (mirrors the open_with_embedder
                    // audit-challenge-3 optimization).
                    None
                }
            }
        };

        // STAMP — last successful step (mirrors open_with_embedder). Read-only
        // guard: the lenient-adoption/migration path (a write) must not fire
        // under a read-only config.
        //
        // Concurrency (pulsedb-internal #11): the check-then-set shape (read
        // `provider_identity` + era marker → compare → stamp) is safe under
        // redb 4.1's exclusive writable file lock — on supported platforms two
        // processes cannot hold writable handles to the same store
        // concurrently, so the read-then-write sequence is serialized by the
        // lock. The "check-then-set race" only exists on platforms without
        // file locking, where concurrent writable opens are explicitly the
        // caller's responsibility per redb's contract. No CAS
        // (compare-and-set) is needed; the lock is the serialization
        // mechanism (Codex #11 closed the race; #12 dropped the trait break).
        if let Some(to_stamp) = should_stamp {
            if config.read_only {
                return Err(PulseDBError::ReadOnly);
            }
            // VS-4.3.3/1.05 (#10): validate the embedder's dimension matches
            // the configured dimension BEFORE stamping. Catches a 384-config +
            // 768-embedder mismatch that would stamp successfully then corrupt
            // the HNSW on the first record.
            let expected = config.embedding_dimension.size();
            let actual = embedding.dimension();
            if actual != expected {
                return Err(PulseDBError::Validation(
                    ValidationError::dimension_mismatch(expected, actual),
                ));
            }
            storage.stamp_provider_identity(&to_stamp)?;
        }

        Ok(Self {
            storage,
            embedding,
            config,
            vectors: RwLock::new(vectors),
            insight_vectors: RwLock::new(insight_vectors),
            watch,
            has_injected_embedder: false,
        })
    }

    /// Shared open prefix common to both [`open`] and [`open_with_embedder`]:
    /// validate config, open storage, load HNSW indexes, build watch.
    ///
    /// Returns the four owned pieces a `PulseDB` is assembled from. Both
    /// constructors run their own post-`open_parts` tail: the mismatch guard +
    /// provider-identity stamp (`open_with_embedder` stamps the injected
    /// identity; [`open`] stamps the config-derived identity — VS-4.3.3/1.01
    /// closed the asymmetry so both post-0.7.0 opens stamp). `open_parts`
    /// itself performs NO stamping, so it is also the seam the lenient-path
    /// tests use to synthesize a genuine unstamped store.
    ///
    /// Split out in work 1.03 so that `open` no longer delegates through
    /// `open_with_embedder` (which stamps).
    #[allow(clippy::type_complexity)] // one-off private helper; a type alias adds friction.
    fn open_parts(
        path: impl AsRef<Path>,
        config: &Config,
    ) -> Result<(
        Box<dyn StorageEngine>,
        HashMap<CollectiveId, HnswIndex>,
        HashMap<CollectiveId, HnswIndex>,
        Arc<WatchService>,
    )> {
        config.validate().map_err(PulseDBError::from)?;

        let storage = open_storage(&path, config)?;
        let vectors = Self::load_all_indexes(&*storage, config)?;
        let insight_vectors = Self::load_all_insight_indexes(&*storage, config)?;

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

        Ok((storage, vectors, insight_vectors, watch))
    }

    /// Config-derived identity for the [`open`] path (VS-4.3.3/1.01).
    ///
    /// Routes to the embedder's [`identity`](EmbeddingService::identity) — for
    /// `Builtin` this is `OnnxEmbedding`'s construction-time fingerprint
    /// (`onnx-<hash>` post-1.04); for `External` it is `ExternalEmbedding`'s
    /// `{external, external-{dim}}`. This does NOT introduce a third identity
    /// source; it merely routes the config-built embedder's identity into the
    /// stamp + mismatch-check path that previously only `open_with_embedder`
    /// ran.
    fn config_derived_identity(
        embedder: &dyn EmbeddingService,
    ) -> crate::embedding::ProviderIdentity {
        embedder.identity()
    }

    /// Opens or creates a PulseDB database with a **caller-supplied** embedding
    /// service (VS-4.3.1, work 1.02 — embedding injection seam).
    ///
    /// This is the Phase-0-unblocking constructor for downstream consumers
    /// (e.g. PulseBase) that own their embedding stack and want
    /// `record_experience` / `store_insight` to embed-on-write through their
    /// own provider. The same `Arc<dyn>` instance backs the open `PulseDB`
    /// and the caller's retained handle.
    ///
    /// The injected embedder bypasses [`EmbeddingProvider`] / [`Config`]
    /// entirely — it is passed in, not encoded in `Config`. Validation,
    /// storage open, and HNSW index load behave identically to [`open`].
    ///
    /// **Work 1.03 safety guard (the cross-provider-mixing refusal for
    /// `pulseai-labs/PulseDB#61`):** this constructor persists the injected
    /// embedder's identity into redb metadata on the fully-successful open
    /// path, and refuses a reopen whose persisted identity differs from the
    /// injected embedder's on `(provider, model_id)`. The comparison is
    /// READ-only and runs BEFORE the stamp write; if the persisted identity is
    /// absent (a pre-1.03 store, or a store opened via [`open`] which does not
    /// stamp), the first `open_with_embedder` silently adopts the injected
    /// identity — safe because no production users carry pre-existing stores,
    /// and documented as the 0.7.0 release-notes caveat.
    ///
    /// **Stamp-write ordering (audit challenge 4):** the stamp is the **last
    /// successful step** of this constructor, after storage open, after the
    /// mismatch check, after HNSW index load. A failure in any prior step
    /// leaves the store unstamped (zero writes from this open) — preserving
    /// the "failed open leaves no trace" property.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the database file (created if it doesn't exist)
    /// * `config` - Configuration options for the database
    /// * `embedder` - Caller-supplied embedding service (shared ownership)
    ///
    /// # Errors
    ///
    /// Same failure modes as [`open`] (invalid config, corrupted file, lock
    /// contention, schema mismatch) plus
    /// [`PulseDBError::EmbeddingProviderMismatch`] when the persisted identity
    /// does not match the injected embedder on `(provider, model_id)`.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use pulsedb::{PulseDB, Config, Result, embedding::EmbeddingService};
    /// # use std::sync::Arc;
    /// # struct MyEmbedder;
    /// # impl EmbeddingService for MyEmbedder {
    /// #     fn embed(&self, _: &str) -> pulsedb::Result<Vec<f32>> { Ok(vec![0.0; 384]) }
    /// #     fn embed_batch(&self, _: &[&str]) -> pulsedb::Result<Vec<Vec<f32>>> { Ok(vec![]) }
    /// #     fn dimension(&self) -> usize { 384 }
    /// #     fn identity(&self) -> pulsedb::embedding::ProviderIdentity {
    /// #         pulsedb::embedding::ProviderIdentity { provider: "mine".into(), model_id: "m-1".into() }
    /// #     }
    /// # }
    /// # fn main() -> Result<()> {
    /// # let dir = tempfile::tempdir().unwrap();
    /// let embedder: Arc<dyn EmbeddingService> = Arc::new(MyEmbedder);
    /// let db = PulseDB::open_with_embedder(
    ///     dir.path().join("injected.db"),
    ///     Config::default(),
    ///     embedder,
    /// )?;
    /// # drop(db);
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(skip(config, embedder), fields(path = %path.as_ref().display()))]
    pub fn open_with_embedder(
        path: impl AsRef<Path>,
        config: Config,
        embedder: Arc<dyn EmbeddingService>,
    ) -> Result<Self> {
        info!("Opening PulseDB with injected embedder");

        // Shared open prefix (validate → open storage → load HNSW indexes →
        // watch). A failure here leaves the store unstamped — the stamp write
        // happens only on the fully-successful path below.
        let (storage, vectors, insight_vectors, watch) = Self::open_parts(&path, &config)?;

        // Cross-provider-mismatch guard (audit challenge 3 — comparison fields).
        // READ-only: reads `PROVIDER_IDENTITY_KEY` and compares on
        // `(provider, model_id)` ONLY — `ProviderIdentity` carries no
        // `dimension` field (dimension mismatch is caught separately by
        // `validate_embedding`). Runs BEFORE the stamp write so a refused
        // reopen writes nothing.
        let injected = embedder.identity();
        // `should_stamp` threads the match's intent out to the (post-match)
        // stamp write. Audit challenge 3 (gap — redundant I/O on reopen): the
        // matching reopen previously re-stamped identical bytes on every open,
        // issuing a write txn + fsync for no semantic gain. Now only the
        // lenient-adoption path (no prior stamp) writes; the Match arm
        // preserves the existing stamp untouched. The mismatch arm returns
        // before reaching `should_stamp`'s use site.
        // VS-4.3.3/1.06 (slice-close fix-up): mirror the `open` path's era-
        // marker check. A post-0.7.0 store whose stamp was lost (era present,
        // identity absent) must NOT be silently re-adopted via
        // open_with_embedder — that bypasses the cross-provider-mismatch guard.
        let persisted_identity = storage.provider_identity()?;
        let era = storage.provider_identity_era_marker()?;
        let should_stamp: bool = match (persisted_identity, era) {
            (None, false) => {
                // Genuine pre-0.7.0 store: BOTH keys absent. Lenient adoption.
                tracing::debug!(
                    provider = %injected.provider,
                    model_id = %injected.model_id,
                    "No persisted provider identity; adopting injected (lenient path)"
                );
                true
            }
            (None, true) => {
                // Post-0.7.0 store whose stamp was LOST/CORRUPTED (era marker
                // present, identity absent). NOT silent re-adoption — typed
                // corruption error (closes Codex #17 on BOTH constructors).
                return Err(PulseDBError::Storage(StorageError::corrupted(
                    "provider identity stamp missing but era marker present — \
                     the stamp was lost or corrupted; refusing silent re-adoption",
                )));
            }
            (Some(persisted), _) => {
                // Check for the VS-4.3.1-era {builtin-onnx, main_graph}
                // legacy marker FIRST (one-time migration), then the mismatch
                // guard. Mirrors the `open` path's migration — both
                // constructors must handle it consistently.
                if persisted.provider == "builtin-onnx"
                    && persisted.model_id == "main_graph"
                    && injected.provider == "builtin-onnx"
                    && injected.model_id
                        == format!("onnx-{}", crate::embedding::BUNDLED_MINILM_FINGERPRINT)
                {
                    tracing::info!(
                        persisted_model_id = %persisted.model_id,
                        "open_with_embedder: migrating legacy {{builtin-onnx, main_graph}} \
                         stamp to the injected model's onnx-<hash> identity"
                    );
                    // Re-stamp with the injected identity (the loaded MiniLM).
                    true
                } else if persisted.provider != injected.provider
                    || persisted.model_id != injected.model_id
                {
                    return Err(PulseDBError::EmbeddingProviderMismatch {
                        persisted,
                        requested: injected,
                    });
                } else {
                    // Match — the persisted stamp already records this identity.
                    // No stamp write needed: re-stamping identical bytes is a
                    // write txn + fsync for no semantic gain (audit challenge 3).
                    false
                }
            }
        };

        // STAMP — last successful step (audit challenge 4). After this point
        // the only remaining work is the zero-failure `PulseDB` struct
        // assembly, so a stamp written here survives. Any failure above (config
        // validation, storage open, HNSW load, mismatch refusal) returned
        // before reaching this line — leaving the store unstamped. Guarded by
        // `should_stamp` so a matching reopen skips the redundant write.
        //
        // Read-only guard (Codex review): the lenient-adoption path (no prior
        // stamp → `should_stamp = true`) must NOT write under a read-only
        // config. `open_parts` respects read-only (storage opens read-only,
        // no schema migration), but the stamp write is a separate write txn
        // that would violate the read-only contract, contend with a writer,
        // and mutate stores used by read-only observers. Refuse the unstamped
        // adoption in read-only mode — the caller can reopen writable to
        // stamp, then reopen read-only.
        //
        // Concurrency (pulsedb-internal #11): the check-then-set shape (read
        // `provider_identity` → compare → stamp) is safe under redb 4.1's
        // exclusive writable file lock — on supported platforms two processes
        // cannot hold writable handles to the same store concurrently, so the
        // read-then-write sequence is serialized by the lock. The
        // "check-then-set race" only exists on platforms without file locking,
        // where concurrent writable opens are explicitly the caller's
        // responsibility per redb's contract. No CAS (compare-and-set) is
        // needed; the lock is the serialization mechanism (Codex #11 closed
        // the race; #12 dropped the trait break).
        if should_stamp {
            if config.read_only {
                return Err(PulseDBError::ReadOnly);
            }
            // VS-4.3.3/1.05 (#10): validate the injected embedder's dimension
            // against the configured dimension BEFORE stamping. Catches a
            // 384-config + 768-embedder mismatch that would stamp successfully
            // then corrupt the HNSW on the first record.
            let expected = config.embedding_dimension.size();
            let actual = embedder.dimension();
            if actual != expected {
                return Err(PulseDBError::Validation(
                    ValidationError::dimension_mismatch(expected, actual),
                ));
            }
            storage.stamp_provider_identity(&injected)?;
        }

        Ok(Self {
            storage,
            embedding: embedder,
            config,
            vectors: RwLock::new(vectors),
            insight_vectors: RwLock::new(insight_vectors),
            watch,
            has_injected_embedder: true,
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

    /// Returns the persistable identity of this database's embedding provider
    /// (VS-4.3.1 — embedding injection seam).
    ///
    /// Reads the **persisted** identity stamped into redb metadata by
    /// [`open_with_embedder`] (work item 1.03), so the value reflects whatever
    /// provider *actually* embedded the store's contents and survives a
    /// process restart — not whatever embedder happens to be wired this
    /// session. The signature is stable across the 1.02→1.03 swap; only the
    /// read site changed (1.02 read the in-memory embedder, 1.03 reads the
    /// persisted stamp).
    ///
    /// For a database opened via [`open`] (which does NOT stamp, per audit
    /// challenge 5), this falls back to the in-memory embedder's identity —
    /// `open`'s provider is config-derived and reconstructible, so the
    /// in-memory value IS the authoritative identity in that case.
    ///
    /// # Errors
    ///
    /// Returns the provider identity recorded for this store.
    ///
    /// For a store opened via [`open`], no stamp is written, so the in-memory
    /// identity (config-derived, reconstructible) is authoritative.
    ///
    /// For a store opened via [`open_with_embedder`], the persisted stamp is
    /// the source of truth and SHOULD always be present (it's the last
    /// successful step of the constructor). A missing stamp on such a store
    /// means the stamp was LOST — a silent integrity regression — and is
    /// surfaced as a `StorageError::Corrupted` rather than silently falling
    /// back to the in-memory embedder identity. Failing loud here prevents a
    /// torn-write or fsync failure from masquerading as "lenient adoption"
    /// (audit challenge 1, premise-level).
    ///
    /// Returns a storage error if the persisted identity cannot be read or
    /// decoded (corruption).
    #[inline]
    pub fn provider_identity(&self) -> Result<crate::embedding::ProviderIdentity> {
        match self.storage.provider_identity()? {
            Some(stamped) => Ok(stamped),
            None => {
                if self.has_injected_embedder {
                    // The stamp should ALWAYS be present on an
                    // `open_with_embedder` store (it's the last successful
                    // step of the constructor). Its absence means the stamp
                    // was lost — a silent integrity regression.
                    Err(StorageError::corrupted(
                        "provider identity stamp missing on an open_with_embedder store",
                    )
                    .into())
                } else {
                    // `open` path: identity is config-derived and
                    // reconstructible, so the in-memory value IS authoritative.
                    Ok(self.embedding.identity())
                }
            }
        }
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
        let is_external = !self.has_injected_embedder
            && matches!(self.config.embedding_provider, EmbeddingProvider::External);

        // VS-4.3.3/1.03 (pulsedb-internal #8): refuse `embedding: Some(vec)`
        // under `open_with_embedder`. The injected embedder's contract is "I
        // embed everything"; a caller-supplied vector bypasses it, so the
        // stamped identity could no longer truthfully describe who embedded the
        // stored vectors. The `open` + `Some(vec)` legacy API stays legal (its
        // identity is config-derived; `External`-via-`open` is the
        // caller-controlled path). Placed before the collective fetch + dim
        // validation so the gate fires first — catches `Some(vec![])` as
        // misuse, not as a dim-0 error.
        if self.has_injected_embedder && exp.embedding.is_some() {
            return Err(PulseDBError::InjectedEmbedderPresent {
                record_kind: "experience",
            });
        }

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

    /// Surfaces prune-eligible cold experiences in a collective, coldest-first.
    ///
    /// Returns lightweight `(ExperienceId, energy)` pairs — **never** full
    /// [`Experience`] records — for every experience whose current temporal
    /// energy is `< below` **and** that is **not already archived**
    /// (`energy < below && !archived`). Results are sorted ascending by energy
    /// (coldest first) and truncated to `limit`.
    ///
    /// This is a **human-triggered review tool**, not an automatic actuator: it
    /// merely *surfaces* candidates a consumer may choose to archive/prune. It
    /// **does not archive** anything and never mutates storage — the
    /// `auto_archive_below_floor` flag is inert and read by no actuator.
    ///
    /// # Archived exclusion
    ///
    /// Already-archived experiences are excluded even when their energy is
    /// `< below`: re-listing them is noise that would double-count a consumer's
    /// prune loop. Only *cold AND not-yet-archived* experiences are returned.
    ///
    /// # Performance
    ///
    /// This is a **deliberate `O(n)` single-pass full-collective scan** (enumerate
    /// all experience IDs → load each → compute scalar energy → filter). There is
    /// **no energy index**; the scan is acceptable precisely because this is a
    /// human-triggered review tool invoked rarely, not a hot query path. The
    /// `DecayConfig` is resolved once and `Timestamp::now()` is captured once for
    /// the whole scan (scalar `experience_energy` per candidate), mirroring
    /// [`energy()`](Self::energy) — never a per-item `self.energy(id)` re-resolve.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # fn main() -> pulsedb::Result<()> {
    /// # let dir = tempfile::tempdir().unwrap();
    /// # let db = pulsedb::PulseDB::open(dir.path().join("cold.db"), pulsedb::Config::default())?;
    /// let collective = db.create_collective("my-project")?;
    ///
    /// // Surface up to 100 prune-eligible candidates with energy < 0.05,
    /// // coldest-first. Returns lightweight (ExperienceId, energy) pairs —
    /// // not full Experience records. Read-only: nothing is archived.
    /// for (id, energy) in db.list_cold_experiences(collective, 0.05, 100)? {
    ///     println!("cold candidate {id} @ energy {energy}");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Arguments
    ///
    /// * `collective_id` - The collective to scan.
    /// * `below` - Energy threshold in `[0.0, 1.0]`; experiences with current
    ///   energy strictly below this are surfaced.
    /// * `limit` - Maximum number of pairs to return (1-1000).
    ///
    /// # Errors
    ///
    /// - [`ValidationError::InvalidField`] if `limit` is 0 or > 1000, or if
    ///   `below` is NaN or outside `[0.0, 1.0]`.
    /// - [`NotFoundError::Collective`] if the collective doesn't exist.
    #[instrument(skip(self))]
    pub fn list_cold_experiences(
        &self,
        collective_id: CollectiveId,
        below: f32,
        limit: usize,
    ) -> Result<Vec<(ExperienceId, f32)>> {
        // Validate limit (mirror get_recent_experiences_filtered).
        if limit == 0 || limit > 1000 {
            return Err(
                ValidationError::invalid_field("limit", "must be between 1 and 1000").into(),
            );
        }

        // Validate threshold: reject NaN and out-of-range.
        if below.is_nan() || !(0.0..=1.0).contains(&below) {
            return Err(
                ValidationError::invalid_field("below", "must be between 0.0 and 1.0").into(),
            );
        }

        // Verify collective exists.
        self.storage
            .get_collective(collective_id)?
            .ok_or_else(|| PulseDBError::from(NotFoundError::collective(collective_id)))?;

        // Resolve decay config ONCE and capture `now` ONCE for the whole scan
        // (hot-path rule: never re-resolve per item via self.energy(id)).
        let decay_config = self
            .storage
            .get_decay_config(collective_id)?
            .unwrap_or_else(|| self.config.decay.clone());
        let now = Timestamp::now();

        // Single-pass full-collective scan. Stream every experience ID in ONE
        // index iteration (limit = usize::MAX, offset = 0) — offset-restart
        // pagination was quadratic (each page re-skipped from the start). The
        // remaining per-row-txn / embedding-hauling / snapshot-safety rework is
        // tracked in #21. Load each, compute scalar energy, keep
        // `energy < below && !archived`.
        let mut cold: Vec<(ExperienceId, f32)> = Vec::new();
        let ids = self
            .storage
            .list_experience_ids_paginated(collective_id, usize::MAX, 0)?;
        for id in ids {
            let Some(experience) = self.storage.get_experience(id)? else {
                continue;
            };
            if experience.archived {
                continue;
            }
            let energy = experience_energy(
                experience.importance,
                experience.applications(),
                experience.last_reinforced,
                now,
                &decay_config,
            );
            if energy < below {
                cold.push((id, energy));
            }
        }

        // Coldest-first (ascending energy), then truncate to limit.
        cold.sort_by(|a, b| a.1.total_cmp(&b.1));
        cold.truncate(limit);
        Ok(cold)
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

    /// Searches for experiences with optional recall weighting.
    ///
    /// This is the forward-compatible search entry for VS-3.5.2. When the
    /// resolved energy weight is zero, it delegates to the unchanged legacy
    /// similarity path. Positive energy weights over-fetch vector candidates,
    /// blend similarity with temporal energy, then sort and truncate.
    ///
    /// # Arguments
    ///
    /// * `collective_id` - The collective to search within
    /// * `query` - Query embedding vector (must match collective's dimension)
    /// * `options` - Result limit, filter, and optional recall weights
    ///
    /// # Errors
    ///
    /// - [`ValidationError::InvalidField`] if request weights are invalid
    /// - [`ValidationError::DimensionMismatch`] if `query.len()` doesn't match
    ///   the collective's embedding dimension
    /// - Legacy search errors from [`search_similar_filtered`](Self::search_similar_filtered)
    #[instrument(skip(self, query, options))]
    pub fn search(
        &self,
        collective_id: CollectiveId,
        query: &[f32],
        options: SearchOptions,
    ) -> Result<Vec<SearchResult>> {
        // Effective per-collective decay config: a stored per-collective override
        // wins; otherwise fall back to the global `Config.decay` — matching the
        // record/energy reads elsewhere. This also honors the documented global
        // `Config.decay.default_recall_weights` for collectives with no stored
        // override (PR #23 review; precedence stored > global > none, relates to #16).
        let decay_config = self
            .storage
            .get_decay_config(collective_id)?
            .unwrap_or_else(|| self.config.decay.clone());
        let collective_default =
            decay_config.default_recall_weights.filter(|weights| {
                match weights.validate("decay.default_recall_weights") {
                    Ok(()) => true,
                    Err(error) => {
                        warn!(
                            ?error,
                            "ignoring invalid default_recall_weights (issue #14)"
                        );
                        false
                    }
                }
            });

        let effective = resolve_recall_weights(options.weights, collective_default)?;
        if is_legacy_recall(effective) {
            return self.search_similar_filtered(collective_id, query, options.k, options.filter);
        }

        let weights = effective.expect("non-legacy recall implies weights are present");
        self.search_similar_weighted(
            collective_id,
            query,
            options.k,
            options.filter,
            weights,
            decay_config,
        )
    }

    fn search_similar_weighted(
        &self,
        collective_id: CollectiveId,
        query: &[f32],
        k: usize,
        filter: SearchFilter,
        weights: RecallWeights,
        decay_config: DecayConfig,
    ) -> Result<Vec<SearchResult>> {
        if k == 0 || k > 1000 {
            return Err(ValidationError::invalid_field("k", "must be between 1 and 1000").into());
        }

        let collective = self
            .storage
            .get_collective(collective_id)?
            .ok_or_else(|| PulseDBError::from(NotFoundError::collective(collective_id)))?;

        let expected_dim = collective.embedding_dimension as usize;
        if query.len() != expected_dim {
            return Err(ValidationError::dimension_mismatch(expected_dim, query.len()).into());
        }

        let over_fetch = std::cmp::max(k.saturating_mul(4), k.saturating_add(16)).min(2000);
        let ef_search = self.config.hnsw.ef_search.max(over_fetch);
        let now = Timestamp::now();

        let candidates = self
            .with_vector_index(collective_id, |index| {
                index.search_experiences(query, over_fetch, ef_search)
            })?
            .unwrap_or_default();

        let mut scored = Vec::with_capacity(candidates.len());
        for (exp_id, distance) in candidates {
            if let Some(experience) = self.storage.get_experience(exp_id)? {
                if filter.matches(&experience) {
                    let similarity = 1.0 - distance;
                    let energy = experience_energy(
                        experience.importance,
                        experience.applications(),
                        experience.last_reinforced,
                        now,
                        &decay_config,
                    );
                    let score = rerank::blend_score(similarity, energy, weights);
                    scored.push((
                        SearchResult {
                            experience,
                            similarity,
                        },
                        score,
                    ));
                }
            }
        }

        Ok(rerank::rerank(scored, k))
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
        // Structurally distinct from `record_experience` (which folds this
        // check into `validate_new_experience(..., is_external)`): here the
        // external-embedder gate is applied inline at the resolution site
        // below (`if is_external { return Err(...) }`). Both behaviors are
        // equivalent; this asymmetry is intentional, not a bug to flatten.
        let is_external = !self.has_injected_embedder
            && matches!(self.config.embedding_provider, EmbeddingProvider::External);

        // VS-4.3.3/1.03 (pulsedb-internal #8): refuse `embedding: Some(vec)`
        // under `open_with_embedder`. Same gate as `record_experience` above
        // (the asymmetry comment just above explains why the *external-embedder*
        // gate is shaped differently here; THIS gate is identical at both sites
        // — same condition, different `record_kind`). Placed before the
        // collective fetch + dim validation so the gate fires first.
        if self.has_injected_embedder && insight.embedding.is_some() {
            return Err(PulseDBError::InjectedEmbedderPresent {
                record_kind: "insight",
            });
        }

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
        active.sort_by_key(|a| std::cmp::Reverse(a.last_heartbeat));

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
        let similar_experiences = self.search(
            request.collective_id,
            &request.query_embedding,
            SearchOptions {
                k: request.max_similar,
                filter: request.filter.clone(),
                weights: request.recall_weights,
            },
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

    /// Inserts a backdated experience fixture into storage and the vector index.
    #[cfg(test)]
    pub(crate) fn insert_experience_backdated(
        &self,
        collective_id: CollectiveId,
        content: &str,
        embedding: Vec<f32>,
        importance: f32,
        applications: BTreeMap<crate::types::InstanceId, u32>,
        last_reinforced: Timestamp,
    ) -> Result<ExperienceId> {
        self.check_writable()?;
        let embedding_for_hnsw = embedding.clone();
        let now = Timestamp::now();
        let experience = Experience {
            id: ExperienceId::new(),
            collective_id,
            content: content.to_string(),
            embedding,
            experience_type: crate::experience::ExperienceType::default(),
            importance,
            confidence: 0.8,
            applications,
            domain: vec![],
            related_files: vec![],
            source_agent: crate::types::AgentId::new("test"),
            source_task: None,
            timestamp: now,
            last_reinforced,
            archived: false,
        };
        let id = experience.id;

        self.storage.save_experience(&experience)?;
        let vectors = self
            .vectors
            .read()
            .map_err(|_| PulseDBError::vector("Vectors lock poisoned"))?;
        let index = vectors
            .get(&collective_id)
            .ok_or_else(|| PulseDBError::vector("HNSW index missing for collective"))?;
        index.insert_experience(id, &embedding_for_hnsw)?;

        Ok(id)
    }

    /// Stores a collective decay config fixture for tests.
    #[cfg(test)]
    pub(crate) fn set_decay_config_for_test(
        &self,
        collective_id: CollectiveId,
        config: DecayConfig,
    ) -> Result<()> {
        self.storage.set_decay_config(collective_id, config)
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

    /// Merges synced G-counter reinforcement fields from a remote peer.
    ///
    /// Caller must hold `SyncApplyGuard` to suppress WAL recording.
    #[cfg(feature = "sync")]
    pub(crate) fn apply_synced_experience_counter_merge(
        &self,
        id: ExperienceId,
        applications: &BTreeMap<InstanceId, u32>,
        last_reinforced: Option<Timestamp>,
    ) -> Result<bool> {
        let merged =
            self.storage
                .merge_experience_applications(id, applications, last_reinforced)?;
        if merged {
            debug!(id = %id, "Synced experience counter merge applied");
        }
        Ok(merged)
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

    // =========================================================================
    // list_cold_experiences — conservative-lifecycle surfacing (VS-3.5.3 / FR-034)
    // =========================================================================

    /// A 384-d embedding fixture (dimension must match the default D384 index).
    fn cold_test_embedding() -> Vec<f32> {
        let mut embedding = vec![0.0f32; 384];
        embedding[0] = 1.0;
        embedding
    }

    /// Backdates `last_reinforced` by `days` so the fixture decays well below
    /// the default `floor` (0.05) under a 30-day half-life.
    fn days_ago(days: i64) -> Timestamp {
        Timestamp::from_millis(Timestamp::now().as_millis() - days * 24 * 60 * 60 * 1000)
    }

    /// Opens a default-config db with a single collective.
    fn open_cold_fixture(name: &str) -> (tempfile::TempDir, PulseDB, CollectiveId) {
        let dir = tempdir().unwrap();
        let db = PulseDB::open(dir.path().join(format!("{name}.db")), Config::default()).unwrap();
        let collective_id = db.create_collective(name).unwrap();
        (dir, db, collective_id)
    }

    #[test]
    fn list_cold_experiences_surfaces_below_floor() {
        let (_dir, db, collective_id) = open_cold_fixture("cold-surfaces");
        let floor = Config::default().decay.floor; // 0.05

        // A cold experience: importance 0.9 last reinforced ~365 days ago decays
        // far below the 0.05 floor under the default 30-day half-life.
        let cold_id = db
            .insert_experience_backdated(
                collective_id,
                "cold memory",
                cold_test_embedding(),
                0.9,
                std::collections::BTreeMap::new(),
                days_ago(365),
            )
            .unwrap();

        // A warm experience: importance 0.9 reinforced now stays at ~0.9 (> floor).
        let warm_id = db
            .insert_experience_backdated(
                collective_id,
                "warm memory",
                cold_test_embedding(),
                0.9,
                std::collections::BTreeMap::new(),
                Timestamp::now(),
            )
            .unwrap();

        let cold = db.list_cold_experiences(collective_id, floor, 100).unwrap();

        // Only the cold experience is surfaced, with its energy reported.
        assert_eq!(cold.len(), 1, "exactly one experience is below the floor");
        assert_eq!(cold[0].0, cold_id, "the cold experience is surfaced");
        assert!(
            cold[0].1 < floor,
            "reported energy {} is below floor {floor}",
            cold[0].1
        );
        assert!(
            !cold.iter().any(|(id, _)| *id == warm_id),
            "the warm experience is not surfaced"
        );

        // Coldest-first ordering: add a second, even-colder experience and assert
        // the result is sorted ascending by energy.
        let colder_id = db
            .insert_experience_backdated(
                collective_id,
                "even colder memory",
                cold_test_embedding(),
                0.1,
                std::collections::BTreeMap::new(),
                days_ago(365),
            )
            .unwrap();
        let cold = db.list_cold_experiences(collective_id, floor, 100).unwrap();
        assert_eq!(cold.len(), 2, "both cold experiences are surfaced");
        assert!(
            cold[0].1 <= cold[1].1,
            "results are coldest-first (ascending energy): {:?}",
            cold
        );
        assert_eq!(cold[0].0, colder_id, "the coldest experience comes first");

        // limit/below validation: limit 0 and out-of-range `below` are rejected.
        assert!(db.list_cold_experiences(collective_id, floor, 0).is_err());
        assert!(db.list_cold_experiences(collective_id, 1.5, 100).is_err());
        assert!(db
            .list_cold_experiences(collective_id, f32::NAN, 100)
            .is_err());

        db.close().unwrap();
    }

    #[test]
    fn cold_experience_not_auto_archived_by_default() {
        // D3 invariant: under DEFAULT config, recording → searching → listing a
        // cold experience NEVER flips `archived` — auto_archive_below_floor is
        // inert (read by no actuator).
        let (_dir, db, collective_id) = open_cold_fixture("auto-archive-off");
        let floor = Config::default().decay.floor;

        let cold_id = db
            .insert_experience_backdated(
                collective_id,
                "cold-but-not-archived",
                cold_test_embedding(),
                0.9,
                std::collections::BTreeMap::new(),
                days_ago(365),
            )
            .unwrap();

        // Freshly recorded: archived must be false.
        assert!(
            !db.storage
                .get_experience(cold_id)
                .unwrap()
                .unwrap()
                .archived,
            "archived is false immediately after record"
        );

        // search: a query touching the collective must not flip archived.
        let _ = db
            .search_similar(collective_id, &cold_test_embedding(), 10)
            .unwrap();
        assert!(
            !db.storage
                .get_experience(cold_id)
                .unwrap()
                .unwrap()
                .archived,
            "archived is false after search"
        );

        // list_cold_experiences surfaces it but must NOT archive it.
        let cold = db.list_cold_experiences(collective_id, floor, 100).unwrap();
        assert!(
            cold.iter().any(|(id, _)| *id == cold_id),
            "the cold experience is surfaced"
        );
        assert!(
            !db.storage
                .get_experience(cold_id)
                .unwrap()
                .unwrap()
                .archived,
            "archived is STILL false after list_cold_experiences (no auto-archive)"
        );

        db.close().unwrap();
    }

    #[test]
    fn list_cold_excludes_archived_experiences() {
        // C5: an experience that is cold (E < below) AND already archived is
        // EXCLUDED from the result (prune-eligible = cold and not yet archived).
        let (_dir, db, collective_id) = open_cold_fixture("cold-excludes-archived");
        let floor = Config::default().decay.floor;

        // Two genuinely-cold experiences (both would match E < below).
        let surfaced_id = db
            .insert_experience_backdated(
                collective_id,
                "cold not archived",
                cold_test_embedding(),
                0.9,
                std::collections::BTreeMap::new(),
                days_ago(365),
            )
            .unwrap();
        let archived_id = db
            .insert_experience_backdated(
                collective_id,
                "cold already archived",
                cold_test_embedding(),
                0.9,
                std::collections::BTreeMap::new(),
                days_ago(365),
            )
            .unwrap();

        // Non-vacuity guard: confirm BOTH are below the floor BEFORE archiving —
        // so the exclusion below is genuinely the !archived filter at work, not a
        // side-effect of the archived experience being warm.
        let before = db.list_cold_experiences(collective_id, floor, 100).unwrap();
        assert_eq!(
            before.len(),
            2,
            "both cold experiences match E < below before archiving"
        );

        // Archive one of them — it now matches E < below but is archived.
        db.archive_experience(archived_id).unwrap();

        let after = db.list_cold_experiences(collective_id, floor, 100).unwrap();
        assert_eq!(
            after.len(),
            1,
            "the archived cold experience is excluded by the !archived filter"
        );
        assert_eq!(
            after[0].0, surfaced_id,
            "only the non-archived cold exp remains"
        );
        assert!(
            !after.iter().any(|(id, _)| *id == archived_id),
            "the archived cold experience does NOT appear"
        );

        db.close().unwrap();
    }
}

// ============================================================================
// open_with_embedder — embedding injection seam (VS-4.3.1, work 1.02)
// ============================================================================
//
// Isolated test module so the AC filter `cargo test --lib db::open_with_embedder`
// matches by full path (`db::open_with_embedder::*`).
//
// Audit challenge 2 (load-bearing): these tests exercise BOTH `record_experience`
// AND `store_insight` routing through the injected embedder — not just the
// experience path. Both reach the injected embedder via the `Box`→`Arc` field
// swap (db.rs `self.embedding`), and the recording-stub asserts both contents
// landed in the embedder, so coverage is tested rather than accidental.
#[cfg(test)]
mod open_with_embedder {
    use super::*;
    use crate::config::EmbeddingDimension;
    use crate::embedding::{EmbeddingService, ProviderIdentity};
    use crate::experience::NewExperience;
    use crate::insight::{InsightType, NewDerivedInsight};
    use crate::Embedding;
    use std::sync::Mutex;
    use tempfile::tempdir;

    /// A stub `EmbeddingService` that records every text it is asked to embed
    /// and emits a deterministic, content-tagged vector. Used to prove that
    /// `record_experience` and `store_insight` both route through the
    /// injected embedder (audit challenge 2).
    struct RecordingEmbedding {
        dimension: usize,
        embedded: Mutex<Vec<String>>,
        identity: ProviderIdentity,
    }

    impl RecordingEmbedding {
        fn new(dimension: usize) -> Self {
            Self {
                dimension,
                embedded: Mutex::new(Vec::new()),
                identity: ProviderIdentity {
                    provider: "stub-injected".to_string(),
                    model_id: format!("recording-{dimension}"),
                },
            }
        }

        fn embedded_texts(&self) -> Vec<String> {
            self.embedded.lock().expect("embedded lock").clone()
        }
    }

    impl EmbeddingService for RecordingEmbedding {
        fn embed(&self, text: &str) -> Result<Embedding> {
            self.embedded
                .lock()
                .expect("embedded lock")
                .push(text.to_string());
            // Deterministic non-zero vector so HNSW insertion accepts it.
            Ok(vec![0.5f32; self.dimension])
        }

        fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Embedding>> {
            let mut guard = self.embedded.lock().expect("embedded lock");
            let mut out = Vec::with_capacity(texts.len());
            for t in texts {
                guard.push((*t).to_string());
                out.push(vec![0.5f32; self.dimension]);
            }
            Ok(out)
        }

        fn dimension(&self) -> usize {
            self.dimension
        }

        fn identity(&self) -> ProviderIdentity {
            self.identity.clone()
        }
    }

    /// AC-1, AC-3: `open_with_embedder` exists and routes BOTH record paths
    /// through the injected embedder. Records an experience AND an insight
    /// (both with `embedding: None`) and asserts the stub saw both contents —
    /// proving `record_experience` (db.rs ~836) and `store_insight`
    /// (db.rs ~1963) reach the injected instance via the shared field.
    ///
    /// Uses `Config::with_builtin_embeddings()` because the `None`-embedding
    /// branch (embed-on-write) is only taken when the configured provider is
    /// not `External`. The injected embedder bypasses `EmbeddingProvider`
    /// (per spec §3), and the `Builtin` config value is the one that admits
    /// `embedding: None` on record.
    #[test]
    fn routes_both_record_paths_through_injected_embedder() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("injected.db");

        let embedder = Arc::new(RecordingEmbedding::new(384));
        let config = Config::with_builtin_embeddings();
        let db = PulseDB::open_with_embedder(
            &path,
            config,
            embedder.clone() as Arc<dyn EmbeddingService>,
        )
        .expect("open_with_embedder succeeds");

        let collective_id = db
            .create_collective("injected-seam")
            .expect("create collective");

        // record_experience with embedding: None -> must hit self.embedding.embed
        let exp_content = "experience routed through injected embedder";
        let exp = NewExperience {
            collective_id,
            content: exp_content.to_string(),
            embedding: None,
            ..Default::default()
        };
        let exp_id = db.record_experience(exp).expect("record_experience");

        // store_insight with embedding: None -> must ALSO hit self.embedding.embed
        let insight_content = "insight routed through injected embedder";
        let insight = NewDerivedInsight {
            collective_id,
            content: insight_content.to_string(),
            embedding: None,
            source_experience_ids: vec![exp_id],
            insight_type: InsightType::Pattern,
            confidence: 0.8,
            domain: Vec::new(),
        };
        db.store_insight(insight).expect("store_insight");

        let embedded = embedder.embedded_texts();
        assert!(
            embedded.iter().any(|c| c == exp_content),
            "record_experience must route content through the injected embedder; saw: {embedded:?}"
        );
        assert!(
            embedded.iter().any(|c| c == insight_content),
            "store_insight must route content through the injected embedder; saw: {embedded:?}"
        );

        db.close().unwrap();
    }

    /// Regression for the VS-4.3.1 slice-close manual-demo defect (work 1.04).
    ///
    /// `open_with_embedder` + `EmbeddingProvider::External` (the natural config
    /// for a consumer like PulseBase that supplies its own embedder) MUST accept
    /// `record_experience { embedding: None }` and route the content through the
    /// injected embedder. Before 1.04, `record_experience` derived `is_external`
    /// purely from `config.embedding_provider`, so the `External` config tripped
    /// the "embedding required" gate even though an injected embedder was
    /// present — `Validation(RequiredField { field: "embedding (required when
    /// using External embedding provider)" })`.
    ///
    /// This test uses `Config::default()` (whose `embedding_provider` is
    /// `EmbeddingProvider::External`) on purpose: a `Builtin`-config test would
    /// pass before AND after the fix and prove nothing (the 1.02 test missed the
    /// bug for exactly this reason). The end-to-end manual-demo prototype
    /// surfaced it; this unit test is the durable guard.
    #[test]
    fn open_with_embedder_with_external_config_accepts_embedding_none() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("external-injected.db");

        let embedder = Arc::new(RecordingEmbedding::new(384));
        // Config::default() carries EmbeddingProvider::External — the bug only
        // manifests under this variant.
        let config = Config::default();
        assert!(
            config.embedding_provider.is_external(),
            "test setup invariant: Config::default() must use EmbeddingProvider::External"
        );
        let db = PulseDB::open_with_embedder(
            &path,
            config,
            embedder.clone() as Arc<dyn EmbeddingService>,
        )
        .expect("open_with_embedder succeeds");

        let collective_id = db
            .create_collective("external-injected-seam")
            .expect("create collective");

        // record_experience with embedding: None under External config — must
        // succeed and route through the injected embedder, NOT trip the
        // RequiredField { embedding } gate.
        let exp_content = "experience recorded via injected embedder under External config";
        let exp = NewExperience {
            collective_id,
            content: exp_content.to_string(),
            embedding: None,
            ..Default::default()
        };
        let exp_id = db
            .record_experience(exp)
            .expect("record_experience accepts embedding: None when an embedder is injected");

        // Symmetric check on store_insight — the same derivation lives there
        // (db.rs:2125) and must be fixed in lockstep.
        let insight_content = "insight recorded via injected embedder under External config";
        let insight = NewDerivedInsight {
            collective_id,
            content: insight_content.to_string(),
            embedding: None,
            source_experience_ids: vec![exp_id],
            insight_type: InsightType::Pattern,
            confidence: 0.8,
            domain: Vec::new(),
        };
        db.store_insight(insight)
            .expect("store_insight accepts embedding: None when an embedder is injected");

        let embedded = embedder.embedded_texts();
        assert!(
            embedded.iter().any(|c| c == exp_content),
            "record_experience must route content through the injected embedder under External config; saw: {embedded:?}"
        );
        assert!(
            embedded.iter().any(|c| c == insight_content),
            "store_insight must route content through the injected embedder under External config; saw: {embedded:?}"
        );

        db.close().unwrap();
    }

    /// `provider_identity()` reads the persisted stamp (1.03 swap). After
    /// `open_with_embedder` stamps, the getter returns the persisted value
    /// even though 1.02 read it from the in-memory embedder.
    #[test]
    fn provider_identity_reads_from_injected_embedder() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("identity.db");

        let embedder = Arc::new(RecordingEmbedding::new(384));
        let db = PulseDB::open_with_embedder(
            &path,
            Config::default(),
            embedder as Arc<dyn EmbeddingService>,
        )
        .expect("open_with_embedder succeeds");

        let id = db
            .provider_identity()
            .expect("provider_identity reads stamp");
        assert_eq!(id.provider, "stub-injected");
        assert_eq!(id.model_id, "recording-384");

        db.close().unwrap();
    }

    /// The existing `open` path still works after the 1.03 refactor that split
    /// the shared open prefix out of `open_with_embedder`. The `open` path
    /// intentionally does NOT stamp (audit challenge 5 — deferred); the
    /// regression here is only that it opens and reports the right dimension.
    #[test]
    fn open_delegates_and_still_works() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("delegate.db");

        let db = PulseDB::open(&path, Config::default()).expect("open succeeds via shared helper");
        assert_eq!(db.embedding_dimension(), EmbeddingDimension::D384.size());
        db.close().unwrap();
    }

    // ---- VS-4.3.3 work 1.03: forbid `embedding: Some(vec)` under
    //      `open_with_embedder` (pulsedb-internal #8). The injected embedder's
    //      contract is "I embed everything"; a caller-supplied vector bypasses
    //      it. The `open` + `Some(vec)` legacy API stays legal. ----

    /// Opens a store via `open_with_embedder` with a 384-d `RecordingEmbedder`
    /// and `Config::with_builtin_embeddings()` (the config that admits
    /// `embedding: None` on record), then creates a single collective. Shared
    /// setup for the 1.03 refusal/regression tests.
    fn injected_store_with_collective(
        dir: &tempfile::TempDir,
        db_name: &str,
    ) -> (PulseDB, Arc<RecordingEmbedding>, CollectiveId) {
        let path = dir.path().join(db_name);
        let embedder = Arc::new(RecordingEmbedding::new(384));
        let db = PulseDB::open_with_embedder(
            &path,
            Config::with_builtin_embeddings(),
            embedder.clone() as Arc<dyn EmbeddingService>,
        )
        .expect("open_with_embedder succeeds");
        let collective_id = db
            .create_collective("injected-1.03")
            .expect("create collective");
        (db, embedder, collective_id)
    }

    /// AC-1 (a): `record_experience { embedding: Some(vec) }` under
    /// `open_with_embedder` is refused with `InjectedEmbedderPresent {
    /// record_kind: "experience" }`.
    #[test]
    fn record_experience_with_some_vec_under_injected_is_refused() {
        let dir = tempdir().unwrap();
        let (db, _embedder, collective_id) = injected_store_with_collective(&dir, "refused-exp.db");

        let exp = NewExperience {
            collective_id,
            content: "caller-supplied vector under injected embedder".to_string(),
            embedding: Some(vec![0.5f32; 384]),
            ..Default::default()
        };
        match db.record_experience(exp) {
            Err(PulseDBError::InjectedEmbedderPresent {
                record_kind: "experience",
            }) => {}
            other => panic!(
                "expected InjectedEmbedderPresent{{record_kind: \"experience\"}}, got {other:?}"
            ),
        }

        db.close().unwrap();
    }

    /// AC-1 (b): `store_insight { embedding: Some(vec) }` under
    /// `open_with_embedder` is refused with `InjectedEmbedderPresent {
    /// record_kind: "insight" }`. The source experience is created first via
    /// `record_experience { embedding: None }` (which succeeds — the injected
    /// embedder embeds it).
    #[test]
    fn store_insight_with_some_vec_under_injected_is_refused() {
        let dir = tempdir().unwrap();
        let (db, _embedder, collective_id) =
            injected_store_with_collective(&dir, "refused-insight.db");

        // Source experience via the injected embedder (embedding: None) — the
        // path this gate must NOT over-fire on.
        let exp = NewExperience {
            collective_id,
            content: "source experience for the refused insight".to_string(),
            embedding: None,
            ..Default::default()
        };
        let exp_id = db
            .record_experience(exp)
            .expect("record_experience with None routes through the injected embedder");

        let insight = NewDerivedInsight {
            collective_id,
            content: "caller-supplied insight vector under injected embedder".to_string(),
            embedding: Some(vec![0.5f32; 384]),
            source_experience_ids: vec![exp_id],
            insight_type: InsightType::Pattern,
            confidence: 0.8,
            domain: Vec::new(),
        };
        match db.store_insight(insight) {
            Err(PulseDBError::InjectedEmbedderPresent {
                record_kind: "insight",
            }) => {}
            other => panic!(
                "expected InjectedEmbedderPresent{{record_kind: \"insight\"}}, got {other:?}"
            ),
        }

        db.close().unwrap();
    }

    /// AC-1 (c): regression guard — `record_experience { embedding: None }`
    /// under `open_with_embedder` still routes through the injected embedder
    /// (the gate must not over-fire on `None`). Preserves the VS-4.3.1/1.04
    /// behavior.
    #[test]
    fn record_experience_with_none_under_injected_routes_through_embedder() {
        let dir = tempdir().unwrap();
        let (db, embedder, collective_id) = injected_store_with_collective(&dir, "none-routes.db");

        let content = "embedding None must route through the injected embedder";
        let exp = NewExperience {
            collective_id,
            content: content.to_string(),
            embedding: None,
            ..Default::default()
        };
        db.record_experience(exp)
            .expect("record_experience with None succeeds under open_with_embedder");

        let embedded = embedder.embedded_texts();
        assert!(
            embedded.iter().any(|c| c == content),
            "embedding: None must route content through the injected embedder; saw: {embedded:?}"
        );

        db.close().unwrap();
    }

    /// AC-1 (d): regression guard — `record_experience { embedding: Some(vec) }`
    /// under the LEGACY `PulseDB::open` (NOT `open_with_embedder`) still
    /// succeeds. Proves the gate fires only under the injected-embedder
    /// constructor; the `open` + `Some(vec)` API (since v0.1.0) is unchanged.
    /// Uses `Config::default()` (`EmbeddingProvider::External`) — the
    /// caller-controlled path this slice intentionally keeps legal.
    #[test]
    fn record_experience_with_some_vec_under_open_legacy_succeeds() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("legacy-open.db");

        // Config::default() -> External; under `open` (has_injected_embedder =
        // false) this is the caller-controlled per-record-vector path.
        let db = PulseDB::open(&path, Config::default()).expect("open succeeds");
        let collective_id = db
            .create_collective("legacy-open")
            .expect("create collective");

        let exp = NewExperience {
            collective_id,
            content: "caller-supplied vector under the legacy open path".to_string(),
            embedding: Some(vec![0.5f32; 384]),
            ..Default::default()
        };
        db.record_experience(exp)
            .expect("record_experience with Some(vec) succeeds under the legacy open path");

        db.close().unwrap();
    }

    /// AC-1 (e): empty-vec ordering pin — `record_experience { embedding:
    /// Some(vec![]) }` under `open_with_embedder` is refused by the gate as
    /// misuse, NOT by `validate_new_experience`'s dim check (dim 0 != 384).
    /// Proves the gate fires BEFORE dim validation, so an empty vector reads as
    /// misuse rather than a dimension-mismatch error.
    #[test]
    fn record_experience_with_empty_vec_under_injected_refused_by_gate_not_dim() {
        let dir = tempdir().unwrap();
        let (db, _embedder, collective_id) = injected_store_with_collective(&dir, "empty-vec.db");

        let exp = NewExperience {
            collective_id,
            content: "empty caller-supplied vector under injected embedder".to_string(),
            embedding: Some(vec![]),
            ..Default::default()
        };
        match db.record_experience(exp) {
            Err(PulseDBError::InjectedEmbedderPresent {
                record_kind: "experience",
            }) => {}
            other => panic!("expected InjectedEmbedderPresent (gate-before-dim), got {other:?}"),
        }

        db.close().unwrap();
    }
}

/// AC-1 for work 1.03: persisted provider identity stamp, mismatch refusal,
/// lenient no-stamp path, and failed-open-leaves-no-stamp ordering (audit
/// challenge 4). Covers the cross-provider-mixing safety guard for
/// `pulseai-labs/PulseDB#61`.
#[cfg(test)]
mod provider_identity_persistence {
    use super::*;
    use crate::embedding::{EmbeddingService, ProviderIdentity};
    use crate::error::{PulseDBError, ValidationError};
    use crate::Embedding;
    use std::sync::Arc;
    use tempfile::tempdir;

    /// Configurable-identity stub embedder. Distinct from
    /// `RecordingEmbedding` (above) so each test in this module can stamp a
    /// *specific* `(provider, model_id)` pair — the mismatch-refusal and
    /// failed-open tests need identities they control.
    struct StubEmbedder {
        dimension: usize,
        identity: ProviderIdentity,
    }

    impl StubEmbedder {
        fn new(dimension: usize, identity: ProviderIdentity) -> Self {
            Self {
                dimension,
                identity,
            }
        }
    }

    impl EmbeddingService for StubEmbedder {
        fn embed(&self, _text: &str) -> Result<Embedding> {
            Ok(vec![0.5f32; self.dimension])
        }
        fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Embedding>> {
            (0..texts.len())
                .map(|_| Ok(vec![0.5f32; self.dimension]))
                .collect()
        }
        fn dimension(&self) -> usize {
            self.dimension
        }
        fn identity(&self) -> ProviderIdentity {
            self.identity.clone()
        }
    }

    fn injected(identity: ProviderIdentity) -> Arc<dyn EmbeddingService> {
        Arc::new(StubEmbedder::new(384, identity))
    }

    fn id(provider: &str, model_id: &str) -> ProviderIdentity {
        ProviderIdentity {
            provider: provider.to_string(),
            model_id: model_id.to_string(),
        }
    }

    /// Creates an UNSTAMPED store on disk — no `PROVIDER_IDENTITY_KEY`, no era
    /// marker (`PROVIDER_IDENTITY_STAMPED_AT_KEY`) — by calling `open_parts`
    /// directly. `open_parts` is the shared open prefix that performs NO
    /// stamping; the stamping tail lives in the public `open` /
    /// `open_with_embedder` constructors. After VS-4.3.3/1.01 the public `open`
    /// stamps, so the unstamped (genuine pre-0.7.0, BOTH keys absent) state can
    /// no longer be produced through `open` — this helper is the lower-level
    /// synthesis the lenient-path tests now need.
    fn unstamped_store(path: &Path) {
        let (storage, _vectors, _insight_vectors, _watch) =
            PulseDB::open_parts(path, &Config::default()).expect("open_parts creates the store");
        // redb flushes on drop; the store persists with NEITHER metadata key
        // written (open_parts performs no provider-identity stamp).
        storage.close().expect("close flushes the unstamped store");
    }

    /// (a) Stamp round-trips across a reopen with the same provider. The
    /// persisted identity survives `close()` + a second `open_with_embedder`,
    /// and `provider_identity()` returns it.
    #[test]
    fn stamp_round_trips_across_reopen_with_same_provider() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("roundtrip.db");

        let original = id("external", "ada-002");
        let db = PulseDB::open_with_embedder(&path, Config::default(), injected(original.clone()))
            .expect("first open stamps");
        assert_eq!(db.provider_identity().expect("read stamp"), original);
        db.close().expect("close flushes");

        let db2 = PulseDB::open_with_embedder(&path, Config::default(), injected(original.clone()))
            .expect("reopen with same provider succeeds");
        assert_eq!(
            db2.provider_identity().expect("read stamp after reopen"),
            original,
            "stamp survived close+reopen"
        );
        db2.close().unwrap();
    }

    /// (b) Mismatched reopen is refused with the typed error and the persisted
    /// stamp is left intact (no partial stamp / no overwrite from the refused
    /// open). Comparison is on `(provider, model_id)` ONLY.
    #[test]
    fn mismatched_reopen_refused_with_typed_error_leaves_stamp_intact() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("mismatch.db");

        let persisted = id("external", "ada-002");
        let requested = id("external", "cohere-embed-v3");

        let db = PulseDB::open_with_embedder(&path, Config::default(), injected(persisted.clone()))
            .expect("first open stamps");
        db.close().unwrap();

        let err =
            PulseDB::open_with_embedder(&path, Config::default(), injected(requested.clone()))
                .expect_err("mismatched reopen must be refused");

        match err {
            PulseDBError::EmbeddingProviderMismatch {
                persisted: p,
                requested: r,
            } => {
                assert_eq!(p, persisted, "error names the persisted identity");
                assert_eq!(r, requested, "error names the requested identity");
            }
            other => panic!("expected EmbeddingProviderMismatch, got {other:?}"),
        }

        // The refused open must NOT have overwritten the stamp. Reopen with the
        // original identity and confirm the persisted value is intact.
        let db2 =
            PulseDB::open_with_embedder(&path, Config::default(), injected(persisted.clone()))
                .expect("reopen with original identity still works");
        assert_eq!(
            db2.provider_identity().expect("read stamp"),
            persisted,
            "the refused (failed) open wrote nothing — stamp intact"
        );
        db2.close().unwrap();
    }

    /// (c) Lenient no-stamp path: a store created via `open` (which does NOT
    /// stamp, per audit challenge 5) then opened via `open_with_embedder`
    /// silently adopts the injected identity and proceeds.
    #[test]
    fn lenient_no_stamp_path_adopts_injected_identity() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("lenient.db");

        // Create a genuine pre-0.7.0 store — BOTH keys absent. After
        // VS-4.3.3/1.01 the public `open` stamps, so synthesize the unstamped
        // state via `open_parts` (which performs no stamping).
        unstamped_store(&path);

        // Reopen via `open_with_embedder` with an arbitrary identity. The
        // absent key triggers the lenient adoption path — the open succeeds and
        // stamps the injected identity.
        let adopted = id("external", "ada-002");
        let db2 = PulseDB::open_with_embedder(&path, Config::default(), injected(adopted.clone()))
            .expect("lenient path adopts the injected identity");
        assert_eq!(
            db2.provider_identity().expect("read stamp"),
            adopted,
            "lenient path stamped the injected identity"
        );
        db2.close().unwrap();
    }

    /// (g) Read-only guard (Codex review): a read-only `open_with_embedder` of
    /// an unstamped store must NOT stamp — the lenient-adoption path requires
    /// a write, which violates the read-only contract. Refuse with the typed
    /// `ReadOnly` error. The caller can reopen writable to stamp, then reopen
    /// read-only. Regression guard for the db.rs:419 read-only bug.
    #[test]
    fn read_only_open_with_embedder_does_not_stamp_unstamped_store() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("readonly-unstamped.db");

        // Create a genuine pre-0.7.0 store — BOTH keys absent. After
        // VS-4.3.3/1.01 the public `open` stamps, so synthesize the unstamped
        // state via `open_parts` (which performs no stamping).
        unstamped_store(&path);

        // Reopen via `open_with_embedder` in READ-ONLY mode. The lenient
        // adoption path would stamp, but read-only must refuse the write.
        let read_only_config = Config {
            read_only: true,
            ..Config::default()
        };
        let err = PulseDB::open_with_embedder(
            &path,
            read_only_config,
            injected(id("external", "ada-002")),
        )
        .expect_err("read-only + unstamped + injected must refuse the stamp write");
        assert!(err.is_read_only(), "expected ReadOnly error, got {err:?}");

        // The refused read-only open wrote nothing. Reopen WRITABLE with the
        // same identity to confirm the lenient path still works (the store
        // remained unstamped, so writable adoption stamps now).
        let db2 = PulseDB::open_with_embedder(
            &path,
            Config::default(),
            injected(id("external", "ada-002")),
        )
        .expect("writable reopen adopts the identity");
        assert_eq!(
            db2.provider_identity().expect("read stamp"),
            id("external", "ada-002"),
            "writable reopen stamped; read-only open left no trace"
        );
        db2.close().unwrap();
    }

    /// (d) `provider_identity()` returns the persisted value, not the in-memory
    /// embedder. Proved by stamping an identity, reopening with the SAME
    /// identity, then confirming the getter returns the persisted stamp and
    /// that the storage layer holds it (not None) — i.e. the getter consulted
    /// storage rather than the in-memory embedder.
    #[test]
    fn provider_identity_returns_persisted_value_not_in_memory() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("authoritative.db");

        let stamped = id("external", "ada-002");
        let db = PulseDB::open_with_embedder(&path, Config::default(), injected(stamped.clone()))
            .expect("first open stamps");
        db.close().unwrap();

        // Reopen with a stub whose in-memory identity is the SAME (so the
        // mismatch guard passes) but constructed independently — the getter
        // must read the persisted stamp, not call the embedder.
        let db2 = PulseDB::open_with_embedder(&path, Config::default(), injected(stamped.clone()))
            .expect("reopen");
        let read = db2.provider_identity().expect("getter reads stamp");
        assert_eq!(read, stamped);

        // Cross-check at the storage layer: the persisted key holds the value
        // (not None), proving the getter consulted storage rather than the
        // in-memory embedder.
        assert_eq!(
            db2.storage.provider_identity().expect("storage read"),
            Some(stamped),
            "PROVIDER_IDENTITY_KEY is present and authoritative"
        );
        db2.close().unwrap();
    }

    /// (e) Stamp-write ordering (audit challenge 4 — load-bearing): a FAILED
    /// `open_with_embedder` leaves the store with no NEW writes from that open.
    /// The mismatch refusal is a deterministic failed-open: it runs the
    /// READ-only check and returns BEFORE the stamp write. Asserted by stamping
    /// provider A, attempting a failed reopen with provider B, then confirming
    /// the persisted value is still A — the failed open wrote nothing.
    ///
    /// This is the literal "failed open leaves no trace" property: any later
    /// successful open with the original identity sees the original stamp, and
    /// no failed-open ever overwrote it.
    #[test]
    fn failed_open_leaves_no_stamp_overwrite() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ordering.db");

        let original = id("external", "ada-002");
        let impostor = id("external", "cohere-embed-v3");

        let db = PulseDB::open_with_embedder(&path, Config::default(), injected(original.clone()))
            .expect("first open stamps original");
        db.close().unwrap();

        // Failed open: mismatched reopen refuses. This MUST run the read-only
        // mismatch check and return before any stamp write — leaving the
        // persisted value as `original`, NOT overwritten by `impostor`.
        let _ = PulseDB::open_with_embedder(&path, Config::default(), injected(impostor.clone()))
            .expect_err("mismatched reopen refuses (failed open)");

        // The failed open wrote nothing. Confirm at the storage layer.
        let probe =
            PulseDB::open_with_embedder(&path, Config::default(), injected(original.clone()))
                .expect("reopen with original succeeds");
        assert_eq!(
            probe.storage.provider_identity().expect("storage read"),
            Some(original),
            "failed open left the stamp intact — no overwrite from the refused reopen"
        );
        probe.close().unwrap();
    }

    /// (f) Audit challenge 1 (premise-level — silent corruption). For a store
    /// opened via `open_with_embedder`, the stamp should ALWAYS be present
    /// (it's the last successful step of the constructor). If
    /// `storage.provider_identity()` returns `None` on such a store, the stamp
    /// was LOST — a silent integrity regression, not a "lenient adoption."
    ///
    /// Pre-fix: the getter silently fell back to `self.embedding.identity()`,
    /// hiding the corruption. Post-fix: the getter returns a
    /// `StorageError::Corrupted` (surfaced as `PulseDBError::Storage`).
    ///
    /// The missing-stamp state is constructed in-scope by replaying
    /// `open_with_embedder`'s tail WITHOUT the stamp write — i.e. assemble a
    /// `PulseDB` from `open_parts` whose `has_injected_embedder` is true but
    /// whose storage holds no `PROVIDER_IDENTITY_KEY`. This is exactly the
    /// shape "constructor finished all pre-stamp steps, but the stamp write
    /// was later lost" (torn write, fsync failure, manual metadata edit).
    #[test]
    fn provider_identity_errors_on_missing_stamp_for_injected_store() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("missing-stamp.db");

        let embedder = injected(id("external", "ada-002"));
        let config = Config::default();

        // Replay `open_with_embedder`'s successful open path, but deliberately
        // SKIP `stamp_provider_identity` — simulating a lost stamp on an
        // `open_with_embedder` store. `open_parts` is the shared helper both
        // constructors use; calling it here mirrors the real constructor tail
        // minus the stamp. `has_injected_embedder: true` is the load-bearing
        // bit — it tells the getter this store OUGHT to have a stamp.
        let (storage, vectors, insight_vectors, watch) =
            PulseDB::open_parts(&path, &config).expect("open_parts succeeds");

        let unstamped = PulseDB {
            storage,
            embedding: embedder,
            config,
            vectors: RwLock::new(vectors),
            insight_vectors: RwLock::new(insight_vectors),
            watch,
            has_injected_embedder: true,
        };

        // Sanity: the storage layer genuinely has no stamp (the precondition).
        assert_eq!(
            unstamped.storage.provider_identity().expect("storage read"),
            None,
            "test precondition: stamp is absent"
        );

        // The getter MUST surface this as corruption, NOT silently fall back
        // to the in-memory embedder identity.
        let err = unstamped
            .provider_identity()
            .expect_err("missing stamp on an injected store is corruption, not a fallback");
        match err {
            PulseDBError::Storage(storage_err) => {
                let msg = storage_err.to_string();
                assert!(
                    msg.to_lowercase().contains("corrupt"),
                    "expected a Corrupted-class storage error, got: {msg}"
                );
                assert!(
                    msg.contains("provider identity stamp missing"),
                    "expected the corruption message to name the missing stamp, got: {msg}"
                );
            }
            other => panic!("expected PulseDBError::Storage(Corrupted(..)), got {other:?}"),
        }

        unstamped.close().unwrap();
    }

    // ========================================================================
    // VS-4.3.3 work 1.01 — `open` path stamps + mismatch check + era marker
    // ========================================================================

    /// AC-4 (VS-4.3.3/1.01): opening via `open` with a `Builtin` config stamps
    /// the config-derived identity — `OnnxEmbedding`'s construction-time
    /// fingerprint (`{builtin-onnx, onnx-<hash>}` post-1.04). The stamp is
    /// readable back via `provider_identity()`, proving the `open` path now
    /// stamps instead of leaving the store identity-less (the #7 closure).
    #[test]
    #[cfg(feature = "builtin-embeddings")]
    fn open_path_stamps_config_derived_identity() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("open-builtin-stamp.db");

        let db = PulseDB::open(&path, Config::with_builtin_embeddings())
            .expect("open with Builtin config stamps the onnx identity");
        let stamped = db.provider_identity().expect("stamp is readable");
        assert_eq!(stamped.provider, "builtin-onnx");
        assert!(
            stamped.model_id.starts_with("onnx-"),
            "expected the onnx fingerprint model_id, got: {}",
            stamped.model_id
        );
        db.close().unwrap();
    }

    /// Opening via `open` with the default `External` config stamps the
    /// config-derived identity `{external, external-384}`. Runs without the
    /// `builtin-embeddings` feature.
    #[test]
    fn open_path_external_stamps_external_identity() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("open-external-stamp.db");

        let db = PulseDB::open(&path, Config::default())
            .expect("open with External config stamps the external identity");
        assert_eq!(
            db.provider_identity().expect("stamp is readable"),
            id("external", "external-384"),
            "open stamped the config-derived External identity"
        );
        db.close().unwrap();
    }

    /// Reopening a Builtin-stamped store via `open` with an `External` config
    /// is refused with `EmbeddingProviderMismatch` — the `open`-path mismatch
    /// guard (the headline #7 closure: the `open` path can no longer be used
    /// to silently mix providers in one HNSW index).
    #[test]
    #[cfg(feature = "builtin-embeddings")]
    fn open_path_mismatched_builtin_reopen_refused() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("open-mismatch.db");

        // Stamp via `open` with Builtin → {builtin-onnx, onnx-<hash>}.
        let db = PulseDB::open(&path, Config::with_builtin_embeddings())
            .expect("first open stamps the builtin identity");
        db.close().unwrap();

        // Reopen via `open` with External → requested {external, external-384}.
        let err = PulseDB::open(&path, Config::default())
            .expect_err("cross-provider reopen via open must be refused");
        match err {
            PulseDBError::EmbeddingProviderMismatch {
                persisted,
                requested,
            } => {
                assert_eq!(persisted.provider, "builtin-onnx");
                assert_eq!(requested.provider, "external");
            }
            other => panic!("expected EmbeddingProviderMismatch, got {other:?}"),
        }
    }

    /// Reopening a store via `open` with the SAME config succeeds and leaves
    /// the stamp unchanged — the Match arm skips the redundant re-stamp
    /// (mirrors the open_with_embedder audit-challenge-3 optimization).
    #[test]
    fn open_path_same_config_reopen_skips_restamp() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("open-same-config.db");

        let db = PulseDB::open(&path, Config::default()).expect("first open stamps");
        let first = db.provider_identity().expect("stamp readable");
        db.close().unwrap();

        let db2 = PulseDB::open(&path, Config::default())
            .expect("reopen with same config succeeds (Match arm)");
        assert_eq!(
            db2.provider_identity().expect("stamp readable"),
            first,
            "stamp unchanged across a matching reopen"
        );
        db2.close().unwrap();
    }

    /// The lenient-adoption path fires ONLY for a genuine pre-0.7.0 store
    /// (BOTH keys absent). Synthesized via `open_parts` (no stamping); reopened
    /// via `open`, it leniently adopts + stamps the config-derived identity.
    #[test]
    fn lenient_path_fires_only_when_both_keys_absent() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("open-lenient-both-absent.db");

        // Genuine pre-0.7.0 store: BOTH keys absent.
        unstamped_store(&path);

        // Reopen via `open` → (None, false) → lenient adoption.
        let db = PulseDB::open(&path, Config::default())
            .expect("lenient adoption stamps the config-derived identity");
        assert_eq!(
            db.provider_identity().expect("stamp readable"),
            id("external", "external-384"),
            "lenient path stamped the config-derived identity"
        );
        db.close().unwrap();
    }

    /// Codex #17 closure: a post-0.7.0 store whose identity stamp was LOST or
    /// CORRUPTED (era marker present, identity absent) is refused with a typed
    /// corruption error — NOT silently re-adopted. The corruption shape is
    /// synthesized by removing ONLY `PROVIDER_IDENTITY_KEY` directly through
    /// redb, leaving `PROVIDER_IDENTITY_STAMPED_AT_KEY` behind.
    #[test]
    fn era_marker_present_identity_absent_is_corruption() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("open-era-without-identity.db");

        // Stamp both keys via a real `open`.
        let db = PulseDB::open(&path, Config::default()).expect("open stamps both keys");
        db.close().unwrap();

        // Remove ONLY the identity key, leaving the era marker behind.
        let redb_db = redb::Database::open(&path).expect("open redb directly to corrupt");
        {
            let wtxn = redb_db.begin_write().unwrap();
            {
                let mut meta = wtxn
                    .open_table(crate::storage::schema::METADATA_TABLE)
                    .unwrap();
                meta.remove(crate::storage::schema::PROVIDER_IDENTITY_KEY)
                    .unwrap();
            }
            wtxn.commit().unwrap();
        }
        drop(redb_db);

        // Reopen via `open` → (None, true) → corruption error (not adoption).
        let err = PulseDB::open(&path, Config::default())
            .expect_err("era-present + identity-absent must be refused as corruption");
        match err {
            PulseDBError::Storage(storage_err) => {
                let msg = storage_err.to_string();
                assert!(
                    msg.to_lowercase().contains("corrupt"),
                    "expected a Corrupted-class error, got: {msg}"
                );
                assert!(
                    msg.contains("era marker present"),
                    "expected the message to name the era-marker-present state, got: {msg}"
                );
            }
            other => panic!("expected PulseDBError::Storage(Corrupted(..)), got {other:?}"),
        }
    }

    /// VS-4.3.3/1.06 fix-up (close-depth challenge 1): `open_with_embedder`
    /// must ALSO refuse an era-present/identity-absent store — not silently
    /// adopt the injected identity. Mirrors the `open` path test above.
    #[test]
    fn open_with_embedder_era_marker_present_identity_absent_is_corruption() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("injected-era-without-identity.db");

        // Stamp both keys via `open_with_embedder`.
        let db = PulseDB::open_with_embedder(
            &path,
            Config::default(),
            injected(id("external", "ada-002")),
        )
        .expect("open_with_embedder stamps both keys");
        db.close().unwrap();

        // Remove ONLY the identity key, leaving the era marker behind.
        let redb_db = redb::Database::open(&path).expect("open redb directly to corrupt");
        {
            let wtxn = redb_db.begin_write().unwrap();
            {
                let mut meta = wtxn
                    .open_table(crate::storage::schema::METADATA_TABLE)
                    .unwrap();
                meta.remove(crate::storage::schema::PROVIDER_IDENTITY_KEY)
                    .unwrap();
            }
            wtxn.commit().unwrap();
        }
        drop(redb_db);

        // Reopen via `open_with_embedder` with a DIFFERENT identity → (None,
        // true) → corruption error (not silent adoption of the new identity).
        let err = PulseDB::open_with_embedder(
            &path,
            Config::default(),
            injected(id("external", "different-model")),
        )
        .expect_err("open_with_embedder must refuse era-present + identity-absent as corruption");
        let msg = err.to_string();
        assert!(
            msg.to_lowercase().contains("corrupt"),
            "expected a Corrupted-class error, got: {msg}"
        );
        assert!(
            msg.contains("era marker present"),
            "expected the message to name the era-marker-present state, got: {msg}"
        );
    }

    /// VS-4.3.3/1.06 fix-up (close-depth challenge 2): the `main_graph`
    /// migration must fire ONLY for the bundled MiniLM, not for an arbitrary
    /// builtin model. A non-MiniLM `main_graph` reopen must be refused (the
    /// user must re-embed), not silently re-stamped.
    #[test]
    fn main_graph_migration_refuses_non_bundled_minilm() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("main-graph-non-minilm.db");

        // Create a store through open_with_embedder (sets up all tables +
        // stamps both keys), then overwrite the identity key with
        // {builtin-onnx, main_graph} to simulate a VS-4.3.1-era stamp.
        let db = PulseDB::open_with_embedder(
            &path,
            Config::default(),
            injected(id("external", "ada-002")),
        )
        .expect("create store");
        db.close().unwrap();

        let redb_db = redb::Database::open(&path).expect("open redb to overwrite identity");
        {
            let wtxn = redb_db.begin_write().unwrap();
            {
                let mut meta = wtxn
                    .open_table(crate::storage::schema::METADATA_TABLE)
                    .unwrap();
                let legacy = crate::embedding::ProviderIdentity {
                    provider: "builtin-onnx".to_string(),
                    model_id: "main_graph".to_string(),
                };
                let bytes = postcard::to_allocvec(&legacy).unwrap();
                meta.insert(
                    crate::storage::schema::PROVIDER_IDENTITY_KEY,
                    bytes.as_slice(),
                )
                .unwrap();
            }
            wtxn.commit().unwrap();
        }
        drop(redb_db);

        // Reopen via `open` with External config → config-derived identity is
        // {external, external-384}, NOT {builtin-onnx, onnx-<minilm>}. The
        // migration must NOT fire; the mismatch guard must refuse.
        let err = PulseDB::open(&path, Config::default())
            .expect_err("non-MiniLM main_graph reopen must be refused");
        assert!(
            matches!(err, PulseDBError::EmbeddingProviderMismatch { .. }),
            "expected EmbeddingProviderMismatch (migration should NOT fire for non-MiniLM), got: {err:?}"
        );
    }
    /// reopen whose injected identity MATCHES the persisted stamp should NOT
    /// re-stamp — the bytes are identical and the write txn + fsync is pure
    /// overhead on the hot open path. The structural guard (`should_stamp`)
    /// is in place and grep-verified (AC-3); this BEHAVIORAL assertion is
    /// the harder half.
    ///
    /// DEFERRED (handoff §4 item 5: "do not skip silently"). Two witness
    /// strategies were attempted, both infeasible within the `src/db.rs`-only
    /// scope of this work item:
    ///
    /// 1. **mtime-unchanged observation.** Fails because `open_existing`
    ///    (redb.rs ~1171-1256) ALWAYS issues a write txn on reopen when
    ///    `!read_only` — it calls `metadata.touch()` (bumps `last_opened_at`)
    ///    and rewrites `METADATA_KEY` unconditionally. That write pollutes
    ///    the store file's mtime regardless of whether `stamp_provider_identity`
    ///    fires, so mtime cannot distinguish "stamped" from "not stamped."
    ///    (Verified empirically: the assertion fails by ~88ms even with the
    ///    `should_stamp` guard in place.)
    ///
    /// 2. **content-spy on `stamp_provider_identity` call count.** Would
    ///    require a delegating `impl StorageEngine for StampCountingSpy` that
    ///    forwards all 53 trait methods to an inner `Box<dyn StorageEngine>`
    ///    while counting the stamp call. `StorageEngine` has no `as_any` /
    ///    downcast path, and adding `clear_provider_identity` or `as_any` to
    ///    the trait would edit `src/storage/mod.rs` (out of scope for 1.05).
    ///    A 53-method spy is the "non-trivial new test infra" the handoff
    ///    explicitly allows deferring.
    ///
    /// The fix itself (`should_stamp` threading) is sound and low-risk: the
    /// Match arm sets `should_stamp = false` and the stamp call is guarded by
    /// `if should_stamp { ... }`. Reopen with a matching identity provably
    /// takes the Match arm (the existing `stamp_round_trips_across_reopen_with_same_provider`
    /// test confirms the persisted value is read back identically). What's
    /// missing is only the negative behavioral witness.
    ///
    /// RESUME: a follow-up work item should either (a) add a test-only
    /// `stamp_call_count()` accessor to `StorageEngine` (or an `as_any`
    /// downcast path) and convert this to a real assertion, or (b) extract a
    /// minimal `ProviderIdentityStore` sub-trait that `open_with_embedder`
    /// depends on, so a focused spy can be implemented over the small surface.
    #[test]
    #[ignore = "AC-3 behavioral witness deferred — see report §6 Deferrals; \
                structural guard is in place and grep-verified (AC-3)"]
    fn matching_reopen_skips_redundant_stamp() {
        // Skipped body kept as living documentation of the intended assertion.
        // When the spy infra lands, restore the mtime-independent witness here.
    }

    /// (h) VS-4.3.3/1.05 (#10): a dimension mismatch between the injected
    /// embedder and the configured dimension is refused BEFORE the stamp
    /// fires. Catches the 384-config + 768-embedder case that would stamp
    /// successfully then corrupt the HNSW on the first record. The "no stamp
    /// written" half is proved by reopening with a CORRECT-dim embedder
    /// carrying a DIFFERENT identity: if the failed open had left a stamp, the
    /// different identity would trip the mismatch guard; instead the lenient
    /// adoption path fires (persisted identity is None), proving nothing was
    /// stamped.
    #[test]
    fn open_with_embedder_dim_mismatch_refused_before_stamp() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("dim-mismatch.db");

        // D384 config (Config::default()) but inject a 768-dim embedder.
        let too_big = Arc::new(StubEmbedder::new(768, id("external", "ada-002")));
        let err = PulseDB::open_with_embedder(&path, Config::default(), too_big)
            .expect_err("768-dim embedder under D384 config must be refused");

        match err {
            PulseDBError::Validation(ValidationError::DimensionMismatch { expected, got }) => {
                assert_eq!(expected, 384, "expected the configured D384");
                assert_eq!(got, 768, "got the injected 768");
            }
            other => panic!("expected DimensionMismatch, got {other:?}"),
        }

        // No stamp was written: reopen with a CORRECT-dim (384) embedder
        // carrying a DIFFERENT identity. If the failed open had left a stamp,
        // this reopen would hit the EmbeddingProviderMismatch guard; instead
        // the lenient adoption path fires (persisted is None), proving the
        // dim-mismatch wrote nothing.
        let correct = Arc::new(StubEmbedder::new(384, id("external", "cohere-embed-v3")));
        let db = PulseDB::open_with_embedder(&path, Config::default(), correct)
            .expect("no stamp left → lenient adoption of the new identity");
        assert_eq!(
            db.provider_identity().unwrap(),
            id("external", "cohere-embed-v3"),
            "lenient path stamped the correct-dim identity"
        );
        db.close().unwrap();
    }

    /// (i) VS-4.3.3/1.05 (#10) regression guard: when the injected embedder's
    /// dimension MATCHES the configured dimension, the dim check does not
    /// over-fire — the open succeeds and the stamp is written normally.
    #[test]
    fn open_with_embedder_matching_dim_stamps_normally() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("dim-match.db");

        let original = id("external", "ada-002");
        // `injected()` builds a 384-dim StubEmbedder; Config::default() is D384.
        let db = PulseDB::open_with_embedder(&path, Config::default(), injected(original.clone()))
            .expect("matching dimension stamps normally");
        assert_eq!(
            db.provider_identity().expect("stamp written"),
            original,
            "stamp carries the injected identity"
        );
        db.close().unwrap();
    }
}
