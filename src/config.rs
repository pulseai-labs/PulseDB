//! Configuration types for PulseDB.
//!
//! The [`Config`] struct controls database behavior including:
//! - Embedding provider (builtin ONNX or external)
//! - Embedding dimension (384, 768, or custom)
//! - Cache size and durability settings
//!
//! # Example
//! ```rust
//! use pulsedb::{Config, EmbeddingProvider, EmbeddingDimension, SyncMode};
//!
//! // Use defaults (External provider, 384 dimensions)
//! let config = Config::default();
//!
//! // Customize for production
//! let config = Config {
//!     embedding_dimension: EmbeddingDimension::D768,
//!     cache_size_mb: 128,
//!     sync_mode: SyncMode::Normal,
//!     ..Default::default()
//! };
//! ```

use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::ValidationError;
use crate::types::CollectiveId;

/// Database configuration options.
///
/// All fields have sensible defaults. Use struct update syntax to override
/// specific settings:
///
/// ```rust
/// use pulsedb::Config;
///
/// let config = Config {
///     cache_size_mb: 256,
///     ..Default::default()
/// };
/// ```
#[derive(Clone, Debug)]
pub struct Config {
    /// How embeddings are generated or provided.
    pub embedding_provider: EmbeddingProvider,

    /// Embedding vector dimension (must match provider output).
    pub embedding_dimension: EmbeddingDimension,

    /// Default collective for operations when none specified.
    pub default_collective: Option<CollectiveId>,

    /// Cache size in megabytes for the storage engine.
    ///
    /// Higher values improve read performance but use more memory.
    /// Default: 64 MB
    pub cache_size_mb: usize,

    /// Durability mode for write operations.
    pub sync_mode: SyncMode,

    /// HNSW vector index parameters.
    ///
    /// Controls the quality and performance of semantic search.
    /// See [`HnswConfig`] for tuning guidelines.
    pub hnsw: HnswConfig,

    /// Agent activity tracking parameters.
    ///
    /// Controls staleness detection for agent heartbeats.
    /// See [`ActivityConfig`] for details.
    pub activity: ActivityConfig,

    /// Watch system parameters.
    ///
    /// Controls the in-process event notification channel.
    /// See [`WatchConfig`] for details.
    pub watch: WatchConfig,

    /// Temporal decay parameters.
    ///
    /// Controls closed-form energy decay for experiences. See [`DecayConfig`]
    /// for defaults and tuning guidance.
    pub decay: DecayConfig,

    /// Read-only mode.
    ///
    /// When `true`, all mutation methods (`record_experience`, `store_relation`,
    /// etc.) return `PulseDBError::ReadOnly`. Read operations work normally.
    ///
    /// Use this for read-only consumers like PulseVision that open the same
    /// database file a writer is using.
    ///
    /// Default: false
    pub read_only: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            // External is the safe default - no ONNX dependency required
            embedding_provider: EmbeddingProvider::External,
            // 384 matches all-MiniLM-L6-v2, the default builtin model
            embedding_dimension: EmbeddingDimension::D384,
            default_collective: None,
            cache_size_mb: 64,
            sync_mode: SyncMode::Normal,
            hnsw: HnswConfig::default(),
            activity: ActivityConfig::default(),
            watch: WatchConfig::default(),
            decay: DecayConfig::default(),
            read_only: false,
        }
    }
}

impl Config {
    /// Creates a new Config with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a Config for read-only access.
    ///
    /// All mutation methods will return `PulseDBError::ReadOnly`.
    /// Use this for read-only consumers like visualization tools that
    /// open the same database file a writer is using.
    ///
    /// # Example
    /// ```rust
    /// use pulsedb::Config;
    ///
    /// let config = Config::read_only();
    /// assert!(config.read_only);
    /// ```
    pub fn read_only() -> Self {
        Self {
            read_only: true,
            ..Default::default()
        }
    }

    /// Creates a Config for builtin embedding generation.
    ///
    /// This requires the `builtin-embeddings` feature to be enabled.
    ///
    /// # Example
    /// ```rust
    /// use pulsedb::Config;
    ///
    /// let config = Config::with_builtin_embeddings();
    /// ```
    pub fn with_builtin_embeddings() -> Self {
        Self {
            embedding_provider: EmbeddingProvider::Builtin { model_path: None },
            ..Default::default()
        }
    }

    /// Creates a Config for external embedding provider.
    ///
    /// When using external embeddings, you must provide pre-computed
    /// embedding vectors when recording experiences.
    ///
    /// # Example
    /// ```rust
    /// use pulsedb::{Config, EmbeddingDimension};
    ///
    /// // OpenAI ada-002 uses 1536 dimensions
    /// let config = Config::with_external_embeddings(EmbeddingDimension::Custom(1536));
    /// ```
    pub fn with_external_embeddings(dimension: EmbeddingDimension) -> Self {
        Self {
            embedding_provider: EmbeddingProvider::External,
            embedding_dimension: dimension,
            ..Default::default()
        }
    }

    /// Validates the configuration.
    ///
    /// Called automatically by `PulseDB::open()`. You can also call this
    /// explicitly to check configuration before attempting to open.
    ///
    /// # Errors
    /// Returns `ValidationError` if:
    /// - `cache_size_mb` is 0
    /// - Custom dimension is 0 or > 4096
    pub fn validate(&self) -> Result<(), ValidationError> {
        // Cache size must be positive
        if self.cache_size_mb == 0 {
            return Err(ValidationError::invalid_field(
                "cache_size_mb",
                "must be greater than 0",
            ));
        }

        // Validate HNSW parameters
        if self.hnsw.max_nb_connection == 0 {
            return Err(ValidationError::invalid_field(
                "hnsw.max_nb_connection",
                "must be greater than 0",
            ));
        }
        if self.hnsw.ef_construction == 0 {
            return Err(ValidationError::invalid_field(
                "hnsw.ef_construction",
                "must be greater than 0",
            ));
        }
        if self.hnsw.ef_search == 0 {
            return Err(ValidationError::invalid_field(
                "hnsw.ef_search",
                "must be greater than 0",
            ));
        }

        // Validate watch buffer size
        if self.watch.buffer_size == 0 {
            return Err(ValidationError::invalid_field(
                "watch.buffer_size",
                "must be greater than 0",
            ));
        }
        if self.watch.poll_interval_ms == 0 {
            return Err(ValidationError::invalid_field(
                "watch.poll_interval_ms",
                "must be greater than 0",
            ));
        }

        // Validate temporal decay parameters
        if self.decay.half_life.is_zero() {
            return Err(ValidationError::invalid_field(
                "decay.half_life",
                "must be greater than 0",
            ));
        }
        if !self.decay.freq_weight.is_finite() || self.decay.freq_weight < 0.0 {
            return Err(ValidationError::invalid_field(
                "decay.freq_weight",
                "must be finite and non-negative",
            ));
        }
        if !self.decay.floor.is_finite() || !(0.0..=1.0).contains(&self.decay.floor) {
            return Err(ValidationError::invalid_field(
                "decay.floor",
                "must be finite and between 0 and 1",
            ));
        }
        if let Some(weights) = self.decay.default_recall_weights {
            weights.validate("decay.default_recall_weights")?;
        }

        // Validate custom dimension bounds
        if let EmbeddingDimension::Custom(dim) = self.embedding_dimension {
            if dim == 0 {
                return Err(ValidationError::invalid_field(
                    "embedding_dimension",
                    "custom dimension must be greater than 0",
                ));
            }
            if dim > 4096 {
                return Err(ValidationError::invalid_field(
                    "embedding_dimension",
                    "custom dimension must not exceed 4096",
                ));
            }
        }

        Ok(())
    }

    /// Returns the embedding dimension as a numeric value.
    pub fn dimension(&self) -> usize {
        self.embedding_dimension.size()
    }
}

/// Embedding provider configuration.
///
/// Determines how embedding vectors are generated for experiences.
#[derive(Clone, Debug)]
pub enum EmbeddingProvider {
    /// PulseDB generates embeddings using a built-in ONNX model.
    ///
    /// Requires the `builtin-embeddings` feature. The default model is
    /// all-MiniLM-L6-v2 (384 dimensions).
    Builtin {
        /// Custom ONNX model path. If `None`, uses the bundled model.
        model_path: Option<PathBuf>,
    },

    /// Caller provides pre-computed embedding vectors.
    ///
    /// Use this when you have your own embedding service (OpenAI, Cohere, etc.)
    /// or want to use a model not bundled with PulseDB.
    External,
}

impl EmbeddingProvider {
    /// Returns true if this is the builtin provider.
    pub fn is_builtin(&self) -> bool {
        matches!(self, Self::Builtin { .. })
    }

    /// Returns true if this is the external provider.
    pub fn is_external(&self) -> bool {
        matches!(self, Self::External)
    }
}

/// Embedding vector dimensions.
///
/// Standard dimensions are provided for common models. Use `Custom` for
/// other embedding services.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum EmbeddingDimension {
    /// 384 dimensions (all-MiniLM-L6-v2, default builtin model).
    #[default]
    D384,

    /// 768 dimensions (bge-base-en-v1.5, BERT-base).
    D768,

    /// Custom dimension for other embedding models.
    ///
    /// Must be between 1 and 4096.
    Custom(usize),
}

impl EmbeddingDimension {
    /// Returns the numeric size of this dimension.
    ///
    /// # Example
    /// ```rust
    /// use pulsedb::EmbeddingDimension;
    ///
    /// assert_eq!(EmbeddingDimension::D384.size(), 384);
    /// assert_eq!(EmbeddingDimension::D768.size(), 768);
    /// assert_eq!(EmbeddingDimension::Custom(1536).size(), 1536);
    /// ```
    #[inline]
    pub const fn size(&self) -> usize {
        match self {
            Self::D384 => 384,
            Self::D768 => 768,
            Self::Custom(n) => *n,
        }
    }
}

/// Durability mode for write operations.
///
/// Controls the trade-off between write performance and crash safety.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncMode {
    /// Sync to disk on transaction commit.
    ///
    /// This is the default and recommended setting. Provides good performance
    /// while ensuring committed data survives crashes.
    #[default]
    Normal,

    /// Async sync (faster writes, may lose recent data on crash).
    ///
    /// Use for development or when you can tolerate losing the last few
    /// seconds of writes. Significantly faster than `Normal`.
    Fast,

    /// Sync every write operation (slowest, maximum durability).
    ///
    /// Use when data loss is absolutely unacceptable. Very slow for
    /// high write volumes.
    Paranoid,
}

impl SyncMode {
    /// Returns true if this mode syncs on every write.
    pub fn is_paranoid(&self) -> bool {
        matches!(self, Self::Paranoid)
    }

    /// Returns true if this mode is async (may lose data on crash).
    pub fn is_fast(&self) -> bool {
        matches!(self, Self::Fast)
    }
}

/// Configuration for the HNSW vector index.
///
/// Controls the trade-off between index build time, memory usage,
/// and search accuracy. The defaults are tuned for PulseDB's target
/// scale (10K-500K experiences per collective).
///
/// # Tuning Guide
///
/// | Use Case     | M  | ef_construction | ef_search |
/// |--------------|----|-----------------|-----------|
/// | Low memory   |  8 |             100 |        30 |
/// | Balanced     | 16 |             200 |        50 |
/// | High recall  | 32 |             400 |       100 |
#[derive(Clone, Debug)]
pub struct HnswConfig {
    /// Maximum bidirectional connections per node (M parameter).
    ///
    /// Higher values improve recall but increase memory and build time.
    /// Each node stores up to M links, so memory per node is O(M).
    /// Default: 16
    pub max_nb_connection: usize,

    /// Number of candidates tracked during index construction.
    ///
    /// Higher values produce a better quality graph but slow down insertion.
    /// Rule of thumb: ef_construction >= 2 * max_nb_connection.
    /// Default: 200
    pub ef_construction: usize,

    /// Number of candidates tracked during search.
    ///
    /// Higher values improve recall but increase search latency.
    /// Must be >= k (the number of results requested).
    /// Default: 50
    pub ef_search: usize,

    /// Maximum number of layers in the skip-list structure.
    ///
    /// Lower layers are dense, upper layers are sparse "express lanes."
    /// Default 16 handles datasets up to ~1M vectors with M=16.
    /// Default: 16
    pub max_layer: usize,

    /// Initial pre-allocated capacity (number of vectors).
    ///
    /// The index grows beyond this automatically, but pre-allocation
    /// avoids reallocations for known workloads.
    /// Default: 10_000
    pub max_elements: usize,
}

impl Default for HnswConfig {
    fn default() -> Self {
        Self {
            max_nb_connection: 16,
            ef_construction: 200,
            ef_search: 50,
            max_layer: 16,
            max_elements: 10_000,
        }
    }
}

/// Configuration for agent activity tracking.
///
/// Controls how stale activities are detected and filtered.
///
/// # Example
/// ```rust
/// use std::time::Duration;
/// use pulsedb::Config;
///
/// let config = Config {
///     activity: pulsedb::ActivityConfig {
///         stale_threshold: Duration::from_secs(120), // 2 minutes
///     },
///     ..Default::default()
/// };
/// ```
#[derive(Clone, Debug)]
pub struct ActivityConfig {
    /// Duration after which an activity with no heartbeat is considered stale.
    ///
    /// Activities whose `last_heartbeat` is older than `now - stale_threshold`
    /// are excluded from `get_active_agents()` results. They remain in storage
    /// until explicitly ended or the collective is deleted.
    ///
    /// Default: 5 minutes (300 seconds)
    pub stale_threshold: Duration,
}

impl Default for ActivityConfig {
    fn default() -> Self {
        Self {
            stale_threshold: Duration::from_secs(300),
        }
    }
}

/// Configuration for the watch system (in-process and cross-process).
///
/// Controls whether in-process channel subscriptions are enabled, the
/// channel buffer size for real-time experience notifications, and the
/// poll interval for cross-process change detection.
///
/// # Example
/// ```rust
/// use pulsedb::Config;
///
/// let config = Config {
///     watch: pulsedb::WatchConfig {
///         in_process: true,
///         buffer_size: 500,
///         poll_interval_ms: 200,
///     },
///     ..Default::default()
/// };
/// ```
#[derive(Clone, Debug)]
pub struct WatchConfig {
    /// Enable in-process watch subscriptions via crossbeam channels.
    ///
    /// When `true` (default), [`watch_experiences()`](crate::PulseDB::watch_experiences)
    /// streams receive real-time events. When `false`, in-process event
    /// dispatch is skipped entirely — only cross-process
    /// [`poll_changes()`](crate::PulseDB::poll_changes) remains available.
    ///
    /// Default: true
    pub in_process: bool,

    /// Maximum number of events buffered per subscriber (in-process).
    ///
    /// When a subscriber's channel is full, new events are dropped for
    /// that subscriber (with a warning log). The publisher never blocks.
    ///
    /// Default: 1000
    pub buffer_size: usize,

    /// Poll interval in milliseconds for cross-process change detection.
    ///
    /// Reader processes call `poll_changes()` at this interval to check
    /// for new experiences written by the writer process.
    ///
    /// Default: 100
    pub poll_interval_ms: u64,
}

impl Default for WatchConfig {
    fn default() -> Self {
        Self {
            in_process: true,
            buffer_size: 1000,
            poll_interval_ms: 100,
        }
    }
}

/// Weighting factors for energy-aware recall.
///
/// This config type is stored as part of [`DecayConfig`], but recall ranking
/// uses it only once the energy-weighted search surface is enabled.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct RecallWeights {
    /// Similarity-score contribution.
    pub similarity: f32,

    /// Energy-score contribution.
    pub energy: f32,
}

impl RecallWeights {
    /// Creates recall weights.
    pub const fn new(similarity: f32, energy: f32) -> Self {
        Self { similarity, energy }
    }

    pub(crate) fn validate(&self, field: &'static str) -> Result<(), ValidationError> {
        if !self.similarity.is_finite()
            || !self.energy.is_finite()
            || self.similarity < 0.0
            || self.energy < 0.0
        {
            return Err(ValidationError::invalid_field(
                field,
                "weights must be finite and non-negative",
            ));
        }

        let sum = self.similarity + self.energy;
        if (sum - 1.0).abs() > 0.000_1 {
            return Err(ValidationError::invalid_field(
                field,
                "similarity and energy weights must sum to 1",
            ));
        }

        Ok(())
    }
}

impl Default for RecallWeights {
    fn default() -> Self {
        Self {
            similarity: 0.7,
            energy: 0.3,
        }
    }
}

/// Temporal decay configuration.
///
/// The defaults model a 30-day half-life, logarithmic reinforcement boost,
/// and a conservative cold-memory floor without automatic archiving.
#[derive(Clone, Debug, PartialEq)]
pub struct DecayConfig {
    /// Half-life for exponential energy decay.
    ///
    /// Default: 30 days.
    pub half_life: Duration,

    /// Logarithmic frequency weight `k` in `1 + k * ln(1 + applications)`.
    ///
    /// Default: `0.25`.
    pub freq_weight: f32,

    /// Energy floor below which experiences are cold.
    ///
    /// Default: `0.05`.
    pub floor: f32,

    /// Whether cold experiences should be archived automatically.
    ///
    /// Default: `false`.
    pub auto_archive_below_floor: bool,

    /// Default recall weights for energy-aware ranking.
    ///
    /// `None` preserves legacy pure-similarity ranking.
    pub default_recall_weights: Option<RecallWeights>,
}

impl Default for DecayConfig {
    fn default() -> Self {
        Self {
            half_life: Duration::from_secs(30 * 24 * 60 * 60),
            freq_weight: 0.25,
            floor: 0.05,
            auto_archive_below_floor: false,
            default_recall_weights: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert!(config.embedding_provider.is_external());
        assert_eq!(config.embedding_dimension, EmbeddingDimension::D384);
        assert_eq!(config.cache_size_mb, 64);
        assert_eq!(config.sync_mode, SyncMode::Normal);
        assert!(config.default_collective.is_none());
    }

    #[test]
    fn test_with_builtin_embeddings() {
        let config = Config::with_builtin_embeddings();
        assert!(config.embedding_provider.is_builtin());
    }

    #[test]
    fn test_with_external_embeddings() {
        let config = Config::with_external_embeddings(EmbeddingDimension::Custom(1536));
        assert!(config.embedding_provider.is_external());
        assert_eq!(config.dimension(), 1536);
    }

    #[test]
    fn test_validate_success() {
        let config = Config::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_cache_size_zero() {
        let config = Config {
            cache_size_mb: 0,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(
            matches!(err, ValidationError::InvalidField { field, .. } if field == "cache_size_mb")
        );
    }

    #[test]
    fn test_validate_custom_dimension_zero() {
        let config = Config {
            embedding_dimension: EmbeddingDimension::Custom(0),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_custom_dimension_too_large() {
        let config = Config {
            embedding_dimension: EmbeddingDimension::Custom(5000),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_custom_dimension_valid() {
        let config = Config {
            embedding_dimension: EmbeddingDimension::Custom(1536),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_embedding_dimension_sizes() {
        assert_eq!(EmbeddingDimension::D384.size(), 384);
        assert_eq!(EmbeddingDimension::D768.size(), 768);
        assert_eq!(EmbeddingDimension::Custom(512).size(), 512);
    }

    #[test]
    fn test_sync_mode_checks() {
        assert!(!SyncMode::Normal.is_fast());
        assert!(!SyncMode::Normal.is_paranoid());
        assert!(SyncMode::Fast.is_fast());
        assert!(SyncMode::Paranoid.is_paranoid());
    }

    #[test]
    fn test_hnsw_config_defaults() {
        let config = HnswConfig::default();
        assert_eq!(config.max_nb_connection, 16);
        assert_eq!(config.ef_construction, 200);
        assert_eq!(config.ef_search, 50);
        assert_eq!(config.max_layer, 16);
        assert_eq!(config.max_elements, 10_000);
    }

    #[test]
    fn test_config_includes_hnsw() {
        let config = Config::default();
        assert_eq!(config.hnsw.max_nb_connection, 16);
    }

    #[test]
    fn test_decay_config_defaults() {
        let config = DecayConfig::default();
        assert_eq!(config.half_life, Duration::from_secs(30 * 24 * 60 * 60));
        assert_eq!(config.freq_weight, 0.25);
        assert_eq!(config.floor, 0.05);
        assert!(!config.auto_archive_below_floor);
        assert!(config.default_recall_weights.is_none());
    }

    #[test]
    fn test_config_includes_decay() {
        let config = Config::default();
        assert_eq!(
            config.decay.half_life,
            Duration::from_secs(30 * 24 * 60 * 60)
        );
    }

    #[test]
    fn test_validate_decay_zero_half_life() {
        let config = Config {
            decay: DecayConfig {
                half_life: Duration::ZERO,
                ..Default::default()
            },
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(matches!(
            err,
            ValidationError::InvalidField { field, .. } if field == "decay.half_life"
        ));
    }

    #[test]
    fn test_validate_decay_negative_freq_weight() {
        let config = Config {
            decay: DecayConfig {
                freq_weight: -0.01,
                ..Default::default()
            },
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(matches!(
            err,
            ValidationError::InvalidField { field, .. } if field == "decay.freq_weight"
        ));
    }

    #[test]
    fn test_validate_decay_floor_out_of_range() {
        let config = Config {
            decay: DecayConfig {
                floor: 1.01,
                ..Default::default()
            },
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(matches!(
            err,
            ValidationError::InvalidField { field, .. } if field == "decay.floor"
        ));
    }

    #[test]
    fn test_validate_hnsw_zero_max_nb_connection() {
        let config = Config {
            hnsw: HnswConfig {
                max_nb_connection: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(matches!(
            err,
            ValidationError::InvalidField { field, .. } if field == "hnsw.max_nb_connection"
        ));
    }

    #[test]
    fn test_validate_hnsw_zero_ef_construction() {
        let config = Config {
            hnsw: HnswConfig {
                ef_construction: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_hnsw_zero_ef_search() {
        let config = Config {
            hnsw: HnswConfig {
                ef_search: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_embedding_dimension_serialization() {
        let dim = EmbeddingDimension::D768;
        let bytes = bincode::serialize(&dim).unwrap();
        let restored: EmbeddingDimension = bincode::deserialize(&bytes).unwrap();
        assert_eq!(dim, restored);
    }

    #[test]
    fn test_watch_config_defaults() {
        let config = WatchConfig::default();
        assert!(config.in_process);
        assert_eq!(config.buffer_size, 1000);
        assert_eq!(config.poll_interval_ms, 100);
    }

    #[test]
    fn test_validate_watch_zero_buffer_size() {
        let config = Config {
            watch: WatchConfig {
                buffer_size: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(matches!(
            err,
            ValidationError::InvalidField { field, .. } if field == "watch.buffer_size"
        ));
    }

    #[test]
    fn test_validate_watch_zero_poll_interval() {
        let config = Config {
            watch: WatchConfig {
                poll_interval_ms: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(matches!(
            err,
            ValidationError::InvalidField { field, .. } if field == "watch.poll_interval_ms"
        ));
    }
}
