//! Embedding service abstractions for PulseDB.
//!
//! This module provides the trait and implementations for embedding generation.
//! Embeddings are dense vector representations of text used for semantic search.
//!
//! # Providers
//!
//! - [`ExternalEmbedding`] - For pre-computed embeddings (e.g., OpenAI, Cohere)
//! - `OnnxEmbedding` - Built-in ONNX model (requires `builtin-embeddings` feature)
//!
//! # Example
//!
//! ```rust
//! use pulsedb::embedding::{EmbeddingService, ExternalEmbedding};
//!
//! // External mode - user provides embeddings
//! let service = ExternalEmbedding::new(384);
//! assert_eq!(service.dimension(), 384);
//!
//! // Validation only - cannot generate embeddings
//! let result = service.embed("hello");
//! assert!(result.is_err());
//! ```

#[cfg(feature = "builtin-embeddings")]
#[cfg_attr(docsrs, doc(cfg(feature = "builtin-embeddings")))]
pub mod onnx;

use serde::{Deserialize, Serialize};

use crate::error::{PulseDBError, Result};
use crate::types::Embedding;

/// Persistable identity of an `EmbeddingService` impl.
///
/// Carries the opaque tokens the embedding-injection seam (VS-4.3.1) stamps
/// into persisted metadata so a later re-open can detect that a *different*
/// provider embedded the existing vectors and refuse the mismatch (work item
/// 1.03). Fields are deliberately `provider` + `model_id` ONLY — **no
/// `dimension`**. Dimension already has a single source of truth in
/// [`EmbeddingService::dimension`]; carrying it here would create two values
/// that can drift. (Audit challenge 3 accepted.)
///
/// The exact string values are opaque identity tokens: they need not match any
/// external registry, but MUST be stable across runs of the same impl and
/// across machines (derived from deterministic inputs, never runtime state
/// like a timestamp or absolute path).
///
/// `Serialize` + `Deserialize` (via postcard) is the shape 1.03 persists into
/// redb metadata; see `test_provider_identity_postcard_roundtrip`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderIdentity {
    /// Opaque provider name, e.g. `"external"` or `"builtin-onnx"`.
    pub provider: String,
    /// Opaque, stable model identifier derived deterministically by the impl.
    pub model_id: String,
}

/// The construction-time fingerprint of the bundled all-MiniLM-L6-v2
/// (length-framed `model.onnx ‖ tokenizer.json` SHA-256). Used by the one-time
/// `{builtin-onnx, main_graph}` migration (VS-4.3.3/1.04) to recognize the
/// bundled MiniLM — NOT feature-gated so the migration check in `db.rs` can
/// reference it without `--features builtin-embeddings`.
/// Verified 2026-07-31 against HF commit `1110a24…`.
pub(crate) const BUNDLED_MINILM_FINGERPRINT: &str =
    "589318e079c05f6ccede875658e0eeeb179945698317711efedd77c0111cacba";

/// Embedding service trait for generating vector representations of text.
///
/// This trait defines the contract for any embedding provider. Implementations
/// must be thread-safe (`Send + Sync`) to allow concurrent embedding operations.
///
/// # Implementing a Custom Provider
///
/// ```rust,no_run
/// use pulsedb::embedding::EmbeddingService;
/// use pulsedb::{Embedding, Result};
///
/// # struct MyApiClient;
/// # impl MyApiClient {
/// #     fn get_embedding(&self, _: &str) -> Result<Embedding> { Ok(vec![0.0; 384]) }
/// #     fn get_embeddings(&self, _: &[&str]) -> Result<Vec<Embedding>> { Ok(vec![]) }
/// # }
/// struct MyEmbeddingService {
///     client: MyApiClient,
///     dimension: usize,
/// }
///
/// impl EmbeddingService for MyEmbeddingService {
///     fn embed(&self, text: &str) -> Result<Embedding> {
///         Ok(self.client.get_embedding(text)?)
///     }
///
///     fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Embedding>> {
///         Ok(self.client.get_embeddings(texts)?)
///     }
///
///     fn dimension(&self) -> usize {
///         self.dimension
///     }
///
///     fn identity(&self) -> pulsedb::embedding::ProviderIdentity {
///         pulsedb::embedding::ProviderIdentity {
///             provider: "my-api".to_string(),
///             model_id: format!("my-model-{}", self.dimension),
///         }
///     }
/// }
/// ```
pub trait EmbeddingService: Send + Sync {
    /// Generates an embedding for a single text.
    ///
    /// # Arguments
    ///
    /// * `text` - The text to embed
    ///
    /// # Returns
    ///
    /// A vector of f32 values with length equal to `dimension()`.
    ///
    /// # Errors
    ///
    /// Returns `PulseDBError::Embedding` if embedding generation fails.
    fn embed(&self, text: &str) -> Result<Embedding>;

    /// Generates embeddings for multiple texts in a batch.
    ///
    /// Batch processing is typically more efficient than individual calls
    /// due to reduced API overhead and better GPU utilization.
    ///
    /// # Arguments
    ///
    /// * `texts` - Slice of texts to embed
    ///
    /// # Returns
    ///
    /// A vector of embeddings in the same order as the input texts.
    ///
    /// # Errors
    ///
    /// Returns `PulseDBError::Embedding` if any embedding generation fails.
    fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Embedding>>;

    /// Returns the dimension of embeddings produced by this service.
    ///
    /// All embeddings from this service will have exactly this many dimensions.
    fn dimension(&self) -> usize;

    /// Returns the persistable identity of this provider.
    ///
    /// This is a **required** method (no default impl): every `EmbeddingService`
    /// must declare its identity to be usable, so the compiler — not convention
    /// — enforces "identity cannot drift from the impl" (spec §3, audit
    /// challenge locked). The returned [`ProviderIdentity`] is the token the
    /// embedding-injection seam stamps into persisted metadata (work item
    /// 1.03). Implementations MUST derive `model_id` from deterministic inputs
    /// so it is stable across runs and across machines.
    fn identity(&self) -> ProviderIdentity;

    /// Validates that an embedding has the correct dimension.
    ///
    /// # Errors
    ///
    /// Returns `ValidationError::DimensionMismatch` if dimensions don't match.
    fn validate_embedding(&self, embedding: &Embedding) -> Result<()> {
        let expected = self.dimension();
        let actual = embedding.len();

        if actual != expected {
            return Err(PulseDBError::Validation(
                crate::error::ValidationError::dimension_mismatch(expected, actual),
            ));
        }

        Ok(())
    }
}

/// External embedding provider.
///
/// This provider is used when embeddings are generated externally (e.g., by
/// OpenAI, Cohere, or a custom service). It validates embedding dimensions
/// but cannot generate embeddings itself.
///
/// # Usage
///
/// When using `ExternalEmbedding`, you must provide pre-computed embedding
/// vectors when recording experiences. Attempting to call `embed()` or
/// `embed_batch()` will return an error.
///
/// # Example
///
/// ```rust
/// use pulsedb::embedding::{EmbeddingService, ExternalEmbedding};
///
/// // Create for OpenAI ada-002 (1536 dimensions)
/// let service = ExternalEmbedding::new(1536);
/// assert_eq!(service.dimension(), 1536);
/// ```
#[derive(Clone, Debug)]
pub struct ExternalEmbedding {
    dimension: usize,
}

impl ExternalEmbedding {
    /// Creates a new external embedding provider with the given dimension.
    ///
    /// # Arguments
    ///
    /// * `dimension` - The expected embedding dimension
    ///
    /// # Example
    ///
    /// ```rust
    /// use pulsedb::embedding::ExternalEmbedding;
    ///
    /// // all-MiniLM-L6-v2
    /// let service = ExternalEmbedding::new(384);
    ///
    /// // OpenAI text-embedding-3-small
    /// let service = ExternalEmbedding::new(1536);
    /// ```
    pub fn new(dimension: usize) -> Self {
        Self { dimension }
    }
}

impl EmbeddingService for ExternalEmbedding {
    fn embed(&self, _text: &str) -> Result<Embedding> {
        Err(PulseDBError::embedding(
            "External embedding mode: embeddings must be provided by the caller",
        ))
    }

    fn embed_batch(&self, _texts: &[&str]) -> Result<Vec<Embedding>> {
        Err(PulseDBError::embedding(
            "External embedding mode: embeddings must be provided by the caller",
        ))
    }

    fn dimension(&self) -> usize {
        self.dimension
    }

    fn identity(&self) -> ProviderIdentity {
        // `external` is the closed EmbeddingProvider::External variant's
        // identity token. model_id pins the dimension the caller configured,
        // so two external services configured for different dims are distinct
        // identities (1.03's mismatch guard keys on this).
        ProviderIdentity {
            provider: "external".to_string(),
            model_id: format!("external-{}", self.dimension),
        }
    }
}

/// Creates an embedding service based on the configuration.
///
/// # Arguments
///
/// * `config` - Database configuration specifying the embedding provider
///
/// # Returns
///
/// A boxed embedding service ready for use.
///
/// # Errors
///
/// Returns an error if:
/// - Builtin embeddings requested but feature not enabled
/// - ONNX model loading fails (for builtin provider)
pub fn create_embedding_service(
    config: &crate::config::Config,
) -> Result<Box<dyn EmbeddingService>> {
    use crate::config::EmbeddingProvider;

    match &config.embedding_provider {
        EmbeddingProvider::External => {
            let dimension = config.embedding_dimension.size();
            Ok(Box::new(ExternalEmbedding::new(dimension)))
        }

        #[cfg(feature = "builtin-embeddings")]
        EmbeddingProvider::Builtin { model_path } => {
            let dim = config.embedding_dimension.size();
            match onnx::OnnxEmbedding::with_dimension(model_path.clone(), dim) {
                Ok(service) => Ok(Box::new(service)),
                Err(ref e) if e.to_string().contains("Model not found") => {
                    tracing::info!(
                        "Builtin embedding model not found, downloading (dimension: {dim})..."
                    );
                    let _path = onnx::OnnxEmbedding::download_default_model(dim)?;
                    let service = onnx::OnnxEmbedding::with_dimension(model_path.clone(), dim)?;
                    Ok(Box::new(service))
                }
                Err(e) => Err(e),
            }
        }

        #[cfg(not(feature = "builtin-embeddings"))]
        EmbeddingProvider::Builtin { .. } => Err(PulseDBError::embedding(
            "Builtin embeddings require the 'builtin-embeddings' feature",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_external_embedding_dimension() {
        let service = ExternalEmbedding::new(384);
        assert_eq!(service.dimension(), 384);
    }

    #[test]
    fn test_external_embedding_embed_returns_error() {
        let service = ExternalEmbedding::new(384);
        let result = service.embed("hello world");
        assert!(result.is_err());
    }

    #[test]
    fn test_external_embedding_embed_batch_returns_error() {
        let service = ExternalEmbedding::new(384);
        let result = service.embed_batch(&["hello", "world"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_embedding_correct_dimension() {
        let service = ExternalEmbedding::new(3);
        let embedding = vec![1.0, 2.0, 3.0];
        assert!(service.validate_embedding(&embedding).is_ok());
    }

    #[test]
    fn test_validate_embedding_wrong_dimension() {
        let service = ExternalEmbedding::new(3);
        let embedding = vec![1.0, 2.0]; // Only 2 dimensions
        let result = service.validate_embedding(&embedding);
        assert!(result.is_err());
    }

    #[test]
    fn test_external_embedding_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ExternalEmbedding>();
    }

    #[test]
    fn test_create_embedding_service_external() {
        let config = crate::config::Config::default();
        let service = create_embedding_service(&config).unwrap();
        assert_eq!(service.dimension(), 384);
    }

    #[test]
    #[ignore] // Requires network access for auto-download
    fn test_create_embedding_service_builtin_auto_downloads() {
        let config = crate::config::Config::with_builtin_embeddings();
        let result = create_embedding_service(&config);
        // With auto-download, this should succeed if network is available
        assert!(result.is_ok());
    }
}

/// Identity-surface tests for `ProviderIdentity` + `EmbeddingService::identity()`.
///
/// Lives in its own module so the AC-1 filter `cargo test --lib embedding::identity`
/// matches these tests by their full path (`embedding::identity::…`).
#[cfg(test)]
mod identity {
    use super::*;

    #[test]
    fn provider_identity_postcard_roundtrip() {
        // 1.03 will persist ProviderIdentity via postcard into redb metadata.
        // This test pins the Serialize/Deserialize shape that persistence relies on.
        let id = ProviderIdentity {
            provider: "external".to_string(),
            model_id: "external-384".to_string(),
        };
        let bytes = postcard::to_stdvec(&id).expect("serialize");
        let restored: ProviderIdentity = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(id, restored);
    }

    #[test]
    fn external_embedding_identity_is_stable_and_nonempty() {
        // Identity must be deterministic across calls of the same impl and
        // carry only provider + model_id (no dimension field — see spec §3).
        let service = ExternalEmbedding::new(384);
        let id = service.identity();
        assert_eq!(id.provider, "external");
        assert_eq!(id.model_id, "external-384");
        assert!(!id.provider.is_empty());
        assert!(!id.model_id.is_empty());
        // Stable across calls.
        assert_eq!(service.identity(), id);
        // Dimension varies with config -> model_id varies with it.
        let big = ExternalEmbedding::new(1536).identity();
        assert_eq!(big.model_id, "external-1536");
    }

    #[test]
    fn identity_dispatches_through_trait_object() {
        // Spec §3: identity() is a required trait method (no default impl),
        // so the compiler enforces "identity must be declared to be usable".
        // This is a compile-time guarantee; this test just exercises the call
        // through the trait object to confirm it dispatches to the impl.
        let service = ExternalEmbedding::new(384);
        let dyn_ref: &dyn EmbeddingService = &service;
        let id = dyn_ref.identity();
        assert_eq!(id.provider, "external");
    }
}
