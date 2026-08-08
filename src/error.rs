//! Error types for PulseDB.
//!
//! PulseDB uses a hierarchical error system:
//! - `PulseDBError` is the top-level error returned by all public APIs
//! - Specific error types (`StorageError`, `ValidationError`) provide detail
//!
//! # Error Handling Pattern
//! ```rust
//! use pulsedb::{PulseDB, Config, Result};
//!
//! fn example() -> Result<()> {
//!     let dir = tempfile::tempdir().unwrap();
//!     let db = PulseDB::open(dir.path().join("test.db"), Config::default())?;
//!     // ... operations that may fail ...
//!     db.close()?;
//!     Ok(())
//! }
//! ```

use std::path::PathBuf;
use thiserror::Error;

#[cfg(feature = "sync")]
use crate::sync::SyncError;

use crate::embedding::ProviderIdentity;

/// Result type alias for PulseDB operations.
pub type Result<T> = std::result::Result<T, PulseDBError>;

/// Top-level error enum for all PulseDB operations.
///
/// This is the only error type returned by public APIs.
/// Use pattern matching to handle specific error cases.
#[derive(Debug, Error)]
pub enum PulseDBError {
    /// Storage layer error (I/O, corruption, transactions).
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    /// Input validation error.
    #[error("Validation error: {0}")]
    Validation(#[from] ValidationError),

    /// Configuration error.
    #[error("Configuration error: {reason}")]
    Config {
        /// Description of what's wrong with the configuration.
        reason: String,
    },

    /// Requested entity not found.
    #[error("{0}")]
    NotFound(#[from] NotFoundError),

    /// General I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Embedding generation/validation error.
    #[error("Embedding error: {0}")]
    Embedding(String),

    /// Refusing to reopen a database whose persisted provider identity does not
    /// match the injected embedder (VS-4.3.1 work 1.03 — the cross-provider-mixing
    /// safety guard for `pulseai-labs/PulseDB#61`).
    ///
    /// The substrate persists the identity that *actually* embedded the store's
    /// vectors under `PROVIDER_IDENTITY_KEY` (redb metadata). On reopen via
    /// [`open_with_embedder`](crate::PulseDB::open_with_embedder), that persisted
    /// identity is compared against the injected embedder's identity on
    /// `(provider, model_id)` only — `ProviderIdentity` carries no `dimension`
    /// field (dimension mismatch is caught separately by `validate_embedding`).
    /// A mismatch is refused with this typed error rather than silently mixing
    /// vectors from incompatible models into one HNSW index.
    ///
    /// The consumer (PulseBase) ships a `re-embed --to <provider>` migration that
    /// resolves the mismatch intentionally; the substrate's job is to make the
    /// refusal *detectable*, which this typed variant satisfies.
    #[error(
        "embedding provider mismatch: the database was embedded by {persisted:?} \
         (provider + model_id), but the injected embedder is {requested:?}; \
         reopen with the original provider or run the re-embed migration"
    )]
    EmbeddingProviderMismatch {
        /// The persisted identity read from `PROVIDER_IDENTITY_KEY`.
        persisted: ProviderIdentity,
        /// The injected embedder's identity that mismatched it.
        requested: ProviderIdentity,
    },

    /// Refusing `embedding: Some(vec)` under `open_with_embedder`
    /// (VS-4.3.3 work 1.03 — cross-provider-mixing safety, API-surface half).
    ///
    /// The injected-embedder constructor's contract is "I embed everything":
    /// every record is embedded through the injected service. A caller-supplied
    /// vector bypasses that service, so the store's stamped identity can no
    /// longer truthfully describe who embedded its vectors. Refused with this
    /// typed error.
    ///
    /// The `PulseDB::open` + `Some(vec)` legacy API (since v0.1.0) stays legal —
    /// its identity is config-derived, and `External`-via-`open` is explicitly
    /// the caller-controlled path. Use `open` if you need the per-record vector
    /// API.
    #[error(
        "cannot pass embedding: Some(vector) to {record_kind} under open_with_embedder \
         (the injected embedder must embed everything); pass embedding: None to route \
         through it, or use PulseDB::open to retain the per-record vector API"
    )]
    InjectedEmbedderPresent {
        /// `"experience"` or `"insight"` — which write path was refused.
        record_kind: &'static str,
    },

    /// Vector index error (HNSW operations).
    #[error("Vector index error: {0}")]
    Vector(String),

    /// Watch system error (subscription or event delivery).
    #[error("Watch error: {0}")]
    Watch(String),

    /// Internal error (e.g., async runtime failure, task join error).
    #[error("Internal error: {0}")]
    Internal(String),

    /// Database is in read-only mode.
    ///
    /// Returned when a mutation method is called on a database opened
    /// with `Config::read_only()`.
    #[error("Database is in read-only mode")]
    ReadOnly,

    /// Sync protocol error.
    ///
    /// Only available when the `sync` feature is enabled.
    #[cfg(feature = "sync")]
    #[cfg_attr(docsrs, doc(cfg(feature = "sync")))]
    #[error("Sync error: {0}")]
    Sync(#[from] SyncError),
}

impl PulseDBError {
    /// Creates a configuration error with the given reason.
    pub fn config(reason: impl Into<String>) -> Self {
        Self::Config {
            reason: reason.into(),
        }
    }

    /// Creates an embedding error with the given message.
    pub fn embedding(msg: impl Into<String>) -> Self {
        Self::Embedding(msg.into())
    }

    /// Creates a vector index error with the given message.
    pub fn vector(msg: impl Into<String>) -> Self {
        Self::Vector(msg.into())
    }

    /// Creates a watch system error with the given message.
    pub fn watch(msg: impl Into<String>) -> Self {
        Self::Watch(msg.into())
    }

    /// Creates an internal error with the given message.
    pub fn internal(msg: impl Into<String>) -> Self {
        Self::Internal(msg.into())
    }

    /// Returns true if this is a "not found" error.
    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound(_))
    }

    /// Returns true if this is a validation error.
    pub fn is_validation(&self) -> bool {
        matches!(self, Self::Validation(_))
    }

    /// Returns true if this is a storage error.
    pub fn is_storage(&self) -> bool {
        matches!(self, Self::Storage(_))
    }

    /// Returns true if this is a vector index error.
    pub fn is_vector(&self) -> bool {
        matches!(self, Self::Vector(_))
    }

    /// Returns true if this is a watch system error.
    pub fn is_watch(&self) -> bool {
        matches!(self, Self::Watch(_))
    }

    /// Returns true if this is an embedding error.
    pub fn is_embedding(&self) -> bool {
        matches!(self, Self::Embedding(_))
    }

    /// Returns true if this is an internal error.
    pub fn is_internal(&self) -> bool {
        matches!(self, Self::Internal(_))
    }

    /// Returns true if this is a configuration error.
    pub fn is_config(&self) -> bool {
        matches!(self, Self::Config { .. })
    }

    /// Returns true if this is an I/O error.
    pub fn is_io(&self) -> bool {
        matches!(self, Self::Io(_))
    }

    /// Returns true if this is a read-only error.
    pub fn is_read_only(&self) -> bool {
        matches!(self, Self::ReadOnly)
    }

    /// Returns true if this is a sync error.
    ///
    /// Only available when the `sync` feature is enabled.
    #[cfg(feature = "sync")]
    #[cfg_attr(docsrs, doc(cfg(feature = "sync")))]
    pub fn is_sync(&self) -> bool {
        matches!(self, Self::Sync(_))
    }
}

/// Storage-related errors.
///
/// These errors indicate problems with the underlying storage layer.
#[derive(Debug, Error)]
pub enum StorageError {
    /// Database file or data is corrupted.
    #[error("Database corrupted: {0}")]
    Corrupted(String),

    /// Database file not found at expected path.
    #[error("Database not found: {0}")]
    DatabaseNotFound(PathBuf),

    /// Database is locked by another process.
    #[error("Database is locked by another writer")]
    DatabaseLocked,

    /// Transaction failed (commit, rollback, etc.).
    #[error("Transaction failed: {0}")]
    Transaction(String),

    /// Serialization/deserialization error.
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Error from the redb storage engine.
    #[error("Storage engine error: {0}")]
    Redb(String),

    /// Database schema version doesn't match expected version.
    #[error("Schema version mismatch: expected {expected}, found {found}")]
    SchemaVersionMismatch {
        /// Expected schema version.
        expected: u32,
        /// Actual schema version found in database.
        found: u32,
    },

    /// Table not found in database.
    #[error("Table not found: {0}")]
    TableNotFound(String),

    /// The database was written by an **older** substrate format and must be
    /// migrated before it can be opened.
    ///
    /// The substrate format is the storage-substrate axis (redb file format +
    /// value serializer), distinct from the logical `schema_version`. This is the
    /// *typed, actionable* signal a guided migration keys off — it must never be a
    /// raw redb `UpgradeRequired` or a bincode decode panic leaking through.
    ///
    /// A **writable** open of such a store migrates automatically (the migration
    /// gate is wired in a later work item); this error is surfaced only when
    /// migration cannot proceed (e.g. a read-only open).
    #[error(
        "Database substrate format {found} is older than the current format {current}; \
         open the database writable once to migrate it (read-only opens of an \
         un-migrated store cannot upgrade)"
    )]
    SubstrateUpgradeRequired {
        /// Substrate format found in the database (0 = pre-4.0 / bincode era).
        found: u8,
        /// Current substrate format this build writes and reads.
        current: u8,
    },

    /// The database was written by a **newer** PulseDB whose substrate format is
    /// ahead of this build — a forward-incompatibility.
    ///
    /// This build must not touch the file: doing so risks silent corruption.
    /// Upgrade PulseDB to a version that understands substrate format `found`.
    #[error(
        "Database substrate format {found} is newer than this build's format {current}; \
         upgrade PulseDB to open this database (do not modify it with an older build)"
    )]
    SubstrateFormatTooNew {
        /// Substrate format found in the database.
        found: u8,
        /// Current substrate format this build supports.
        current: u8,
    },

    /// The bincode→postcard codec migration cannot run as a single transaction
    /// because the store is larger than the safe single-txn ceiling, and no
    /// declared available-memory budget (or a too-small one) authorizes it.
    ///
    /// This is the **fail-closed-above-floor** valve (VS-4.0.3 work-1.04 §6.4):
    /// rather than risk an OOM mid-migration (which would leave the migration
    /// unfinishable) or ship a half-correct phased path, the codec pass refuses
    /// with **zero destructive writes**. The caller can either declare an
    /// available-memory budget via `Config` to raise the single-txn ceiling, or
    /// use the offline `pulsedb migrate` tool (VS-4.0.4) for very large stores.
    ///
    /// `store_size` is the on-disk file size at open; `projected_peak` is the
    /// conservative peak-RSS estimate (`store_size × coefficient`); `budget` is
    /// the single-txn ceiling that was exceeded.
    #[error(
        "store too large for a single-transaction codec migration: store size {store_size} bytes \
         (projected peak ~{projected_peak} bytes) exceeds the single-txn budget of {budget} bytes; \
         declare available memory via Config to raise the ceiling, or run the offline migration tool"
    )]
    SubstrateMigrationTooLarge {
        /// On-disk redb file size at open, in bytes.
        store_size: u64,
        /// Conservative peak-RSS estimate for a single-txn re-encode, in bytes.
        projected_peak: u64,
        /// The single-txn budget (ceiling) that `projected_peak` exceeded, in bytes.
        budget: u64,
    },

    /// The bincode→postcard codec migration cannot run because there is not enough
    /// free disk space to hold the pristine `.pre-substrate.bak` backup plus the
    /// migrated file plus a redb transaction-growth margin.
    ///
    /// This is the **disk axis** of the unified headroom preflight (VS-4.0.3
    /// work-1.05 / audit C3), the companion of [`Self::SubstrateMigrationTooLarge`]
    /// (the memory axis). The pristine backup taken before any destructive write
    /// (`.pre-substrate.bak`) roughly **doubles** the on-disk footprint, and the
    /// postcard re-encode does not shrink the size-dominant `Vec<f32>` embedding
    /// table, so the migrated file is conservatively assumed to be ~1× the store
    /// size. The preflight fails here with **zero destructive writes** (no
    /// half-migration that runs the disk out mid-pass) rather than risk an
    /// unfinishable migration. Free up disk, or run the offline `pulsedb migrate`
    /// tool (VS-4.0.4) for very large stores.
    ///
    /// `store_size` is the on-disk file size at open; `required` is the conservative
    /// free-space estimate (`backup ≈ store_size` + migrated ≈ store_size + redb
    /// txn-growth margin); `available` is the free space observed on the store's
    /// filesystem at open.
    #[error(
        "insufficient free disk space for the codec migration: store size {store_size} bytes \
         needs ~{required} bytes free (pristine backup + migrated file + transaction margin) \
         but only {available} bytes are available; free up disk or run the offline migration tool"
    )]
    SubstrateMigrationInsufficientDisk {
        /// On-disk redb file size at open, in bytes.
        store_size: u64,
        /// Conservative free-space estimate the migration needs, in bytes.
        required: u64,
        /// Free space observed on the store's filesystem at open, in bytes.
        available: u64,
    },

    /// The bincode→postcard codec migration was attempted by a build **without** the
    /// `sync` feature, but the database contains `sync_cursors` rows written by a
    /// prior sync-enabled build. Those rows can only be re-encoded by a build that
    /// knows the `SyncCursor` type, so completing the migration here would leave them
    /// bincode-encoded under a postcard marker — silent corruption a later sync build
    /// would hit reading them as postcard. The migration fails closed with **no marker
    /// bump** (the store stays re-migratable) so a `sync`-enabled build can finish it.
    #[error(
        "cannot migrate this database's sync state without the `sync` feature: it \
         contains sync_cursors rows that require a sync-enabled PulseDB build to \
         re-encode; rebuild or run PulseDB with the `sync` feature to migrate it"
    )]
    SubstrateMigrationRequiresSync,
}

impl StorageError {
    /// Creates a corruption error with the given message.
    pub fn corrupted(msg: impl Into<String>) -> Self {
        Self::Corrupted(msg.into())
    }

    /// Creates a transaction error with the given message.
    pub fn transaction(msg: impl Into<String>) -> Self {
        Self::Transaction(msg.into())
    }

    /// Creates a serialization error with the given message.
    pub fn serialization(msg: impl Into<String>) -> Self {
        Self::Serialization(msg.into())
    }

    /// Creates a redb error with the given message.
    pub fn redb(msg: impl Into<String>) -> Self {
        Self::Redb(msg.into())
    }

    /// Creates a substrate-upgrade-required error.
    ///
    /// `found` is the substrate format stored in the database (0 = pre-4.0
    /// bincode era), `current` is the format this build writes.
    pub fn substrate_upgrade_required(found: u8, current: u8) -> Self {
        Self::SubstrateUpgradeRequired { found, current }
    }

    /// Creates a substrate-format-too-new error (forward-incompatibility).
    pub fn substrate_format_too_new(found: u8, current: u8) -> Self {
        Self::SubstrateFormatTooNew { found, current }
    }

    /// Creates a substrate-migration-too-large error (single-txn ceiling exceeded).
    pub fn substrate_migration_too_large(
        store_size: u64,
        projected_peak: u64,
        budget: u64,
    ) -> Self {
        Self::SubstrateMigrationTooLarge {
            store_size,
            projected_peak,
            budget,
        }
    }

    /// Creates a substrate-migration-insufficient-disk error (disk-headroom axis).
    pub fn substrate_migration_insufficient_disk(
        store_size: u64,
        required: u64,
        available: u64,
    ) -> Self {
        Self::SubstrateMigrationInsufficientDisk {
            store_size,
            required,
            available,
        }
    }
}

// Conversions from redb error types
impl From<redb::Error> for StorageError {
    fn from(err: redb::Error) -> Self {
        StorageError::Redb(err.to_string())
    }
}

impl From<redb::DatabaseError> for StorageError {
    fn from(err: redb::DatabaseError) -> Self {
        StorageError::Redb(err.to_string())
    }
}

impl From<redb::TransactionError> for StorageError {
    fn from(err: redb::TransactionError) -> Self {
        StorageError::Transaction(err.to_string())
    }
}

impl From<redb::CommitError> for StorageError {
    fn from(err: redb::CommitError) -> Self {
        StorageError::Transaction(format!("Commit failed: {}", err))
    }
}

impl From<redb::TableError> for StorageError {
    fn from(err: redb::TableError) -> Self {
        StorageError::Redb(format!("Table error: {}", err))
    }
}

impl From<redb::StorageError> for StorageError {
    fn from(err: redb::StorageError) -> Self {
        StorageError::Redb(format!("Storage error: {}", err))
    }
}

// Convert postcard errors to StorageError
impl From<postcard::Error> for StorageError {
    fn from(err: postcard::Error) -> Self {
        StorageError::Serialization(err.to_string())
    }
}

// Also allow direct conversion to PulseDBError for convenience
impl From<redb::Error> for PulseDBError {
    fn from(err: redb::Error) -> Self {
        PulseDBError::Storage(StorageError::from(err))
    }
}

impl From<redb::DatabaseError> for PulseDBError {
    fn from(err: redb::DatabaseError) -> Self {
        PulseDBError::Storage(StorageError::from(err))
    }
}

impl From<redb::TransactionError> for PulseDBError {
    fn from(err: redb::TransactionError) -> Self {
        PulseDBError::Storage(StorageError::from(err))
    }
}

impl From<redb::CommitError> for PulseDBError {
    fn from(err: redb::CommitError) -> Self {
        PulseDBError::Storage(StorageError::from(err))
    }
}

impl From<redb::TableError> for PulseDBError {
    fn from(err: redb::TableError) -> Self {
        PulseDBError::Storage(StorageError::from(err))
    }
}

impl From<redb::StorageError> for PulseDBError {
    fn from(err: redb::StorageError) -> Self {
        PulseDBError::Storage(StorageError::from(err))
    }
}

impl From<postcard::Error> for PulseDBError {
    fn from(err: postcard::Error) -> Self {
        PulseDBError::Storage(StorageError::from(err))
    }
}

/// Validation errors for input data.
///
/// These errors indicate problems with data provided by the caller.
#[derive(Debug, Error)]
pub enum ValidationError {
    /// Embedding dimension doesn't match collective's configured dimension.
    #[error("Embedding dimension mismatch: expected {expected}, got {got}")]
    DimensionMismatch {
        /// Expected dimension from collective configuration.
        expected: usize,
        /// Actual dimension provided.
        got: usize,
    },

    /// A field has an invalid value.
    #[error("Invalid field '{field}': {reason}")]
    InvalidField {
        /// Name of the invalid field.
        field: String,
        /// Why the value is invalid.
        reason: String,
    },

    /// Content exceeds maximum allowed size.
    #[error("Content too large: {size} bytes (max: {max} bytes)")]
    ContentTooLarge {
        /// Actual content size in bytes.
        size: usize,
        /// Maximum allowed size in bytes.
        max: usize,
    },

    /// A required field is missing or empty.
    #[error("Required field missing: {field}")]
    RequiredField {
        /// Name of the missing field.
        field: String,
    },

    /// Too many items in a collection field.
    #[error("Too many items in '{field}': {count} (max: {max})")]
    TooManyItems {
        /// Name of the field.
        field: String,
        /// Actual count.
        count: usize,
        /// Maximum allowed.
        max: usize,
    },
}

impl ValidationError {
    /// Creates a dimension mismatch error.
    pub fn dimension_mismatch(expected: usize, got: usize) -> Self {
        Self::DimensionMismatch { expected, got }
    }

    /// Creates an invalid field error.
    pub fn invalid_field(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::InvalidField {
            field: field.into(),
            reason: reason.into(),
        }
    }

    /// Creates a content too large error.
    pub fn content_too_large(size: usize, max: usize) -> Self {
        Self::ContentTooLarge { size, max }
    }

    /// Creates a required field error.
    pub fn required_field(field: impl Into<String>) -> Self {
        Self::RequiredField {
            field: field.into(),
        }
    }

    /// Creates a too many items error.
    pub fn too_many_items(field: impl Into<String>, count: usize, max: usize) -> Self {
        Self::TooManyItems {
            field: field.into(),
            count,
            max,
        }
    }
}

/// Not found errors for specific entity types.
#[derive(Debug, Error)]
pub enum NotFoundError {
    /// Collective with given ID not found.
    #[error("Collective not found: {0}")]
    Collective(String),

    /// Experience with given ID not found.
    #[error("Experience not found: {0}")]
    Experience(String),

    /// Relation with given ID not found.
    #[error("Relation not found: {0}")]
    Relation(String),

    /// Insight with given ID not found.
    #[error("Insight not found: {0}")]
    Insight(String),

    /// Activity not found for given agent/collective pair.
    #[error("Activity not found: {0}")]
    Activity(String),
}

impl NotFoundError {
    /// Creates a collective not found error.
    pub fn collective(id: impl ToString) -> Self {
        Self::Collective(id.to_string())
    }

    /// Creates an experience not found error.
    pub fn experience(id: impl ToString) -> Self {
        Self::Experience(id.to_string())
    }

    /// Creates a relation not found error.
    pub fn relation(id: impl ToString) -> Self {
        Self::Relation(id.to_string())
    }

    /// Creates an insight not found error.
    pub fn insight(id: impl ToString) -> Self {
        Self::Insight(id.to_string())
    }

    /// Creates an activity not found error.
    pub fn activity(id: impl ToString) -> Self {
        Self::Activity(id.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = PulseDBError::config("Invalid dimension");
        assert_eq!(err.to_string(), "Configuration error: Invalid dimension");
    }

    #[test]
    fn test_storage_error_display() {
        let err = StorageError::SchemaVersionMismatch {
            expected: 2,
            found: 1,
        };
        assert_eq!(
            err.to_string(),
            "Schema version mismatch: expected 2, found 1"
        );
    }

    #[test]
    fn test_validation_error_display() {
        let err = ValidationError::dimension_mismatch(384, 768);
        assert_eq!(
            err.to_string(),
            "Embedding dimension mismatch: expected 384, got 768"
        );
    }

    #[test]
    fn test_not_found_error_display() {
        let err = NotFoundError::collective("abc-123");
        assert_eq!(err.to_string(), "Collective not found: abc-123");
    }

    #[test]
    fn test_is_not_found() {
        let err: PulseDBError = NotFoundError::collective("test").into();
        assert!(err.is_not_found());
        assert!(!err.is_validation());
    }

    #[test]
    fn test_is_validation() {
        let err: PulseDBError = ValidationError::required_field("content").into();
        assert!(err.is_validation());
        assert!(!err.is_not_found());
    }

    #[test]
    fn test_vector_error_display() {
        let err = PulseDBError::vector("HNSW insert failed");
        assert_eq!(err.to_string(), "Vector index error: HNSW insert failed");
        assert!(err.is_vector());
        assert!(!err.is_storage());
    }

    #[test]
    fn test_error_conversion_chain() {
        // Simulate a storage error propagating up
        fn inner() -> Result<()> {
            Err(StorageError::corrupted("test corruption"))?
        }

        let result = inner();
        assert!(result.is_err());
        assert!(result.unwrap_err().is_storage());
    }

    #[test]
    fn test_watch_error_display() {
        let err = PulseDBError::watch("subscribers lock poisoned");
        assert_eq!(err.to_string(), "Watch error: subscribers lock poisoned");
    }

    #[test]
    fn test_watch_constructor() {
        let err = PulseDBError::watch("test");
        assert!(err.is_watch());
        assert!(!err.is_storage());
    }

    #[test]
    fn test_is_watch() {
        let err = PulseDBError::watch("test");
        assert!(err.is_watch());
        assert!(!err.is_not_found());
    }

    #[test]
    fn test_is_embedding() {
        let err = PulseDBError::embedding("model load failed");
        assert!(err.is_embedding());
        assert!(!err.is_vector());
    }

    #[test]
    fn test_is_internal() {
        let err = PulseDBError::internal("task join failed");
        assert!(err.is_internal());
        assert!(!err.is_storage());
    }

    #[test]
    fn test_is_config() {
        let err = PulseDBError::config("invalid dimension");
        assert!(err.is_config());
        assert!(!err.is_validation());
    }

    #[test]
    fn test_is_io() {
        let err = PulseDBError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "file missing",
        ));
        assert!(err.is_io());
        assert!(!err.is_storage());
    }

    #[test]
    fn test_substrate_upgrade_required_display_is_actionable() {
        let err = StorageError::substrate_upgrade_required(0, 1);
        assert!(matches!(
            err,
            StorageError::SubstrateUpgradeRequired {
                found: 0,
                current: 1
            }
        ));
        let msg = err.to_string();
        // Names both versions and tells the operator HOW to recover.
        assert!(msg.contains("substrate format 0"), "msg: {msg}");
        assert!(msg.contains("current format 1"), "msg: {msg}");
        assert!(msg.contains("writable"), "msg: {msg}");
    }

    #[test]
    fn test_substrate_format_too_new_display_is_actionable() {
        let err = StorageError::substrate_format_too_new(7, 1);
        assert!(matches!(
            err,
            StorageError::SubstrateFormatTooNew {
                found: 7,
                current: 1
            }
        ));
        let msg = err.to_string();
        assert!(msg.contains("substrate format 7"), "msg: {msg}");
        assert!(msg.contains("format 1"), "msg: {msg}");
        // Tells the operator to upgrade rather than touch the file.
        assert!(msg.contains("upgrade PulseDB"), "msg: {msg}");
    }

    #[test]
    fn test_substrate_errors_propagate_as_storage() {
        let err: PulseDBError = StorageError::substrate_upgrade_required(0, 1).into();
        assert!(err.is_storage());
        let err: PulseDBError = StorageError::substrate_format_too_new(2, 1).into();
        assert!(err.is_storage());
    }
}
