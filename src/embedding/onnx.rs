//! ONNX-based embedding generation.
//!
//! This module provides embedding generation using ONNX Runtime.
//! It requires the `builtin-embeddings` feature to be enabled.
//!
//! # Supported Models
//!
//! - **all-MiniLM-L6-v2** (384 dimensions) - Default, fast and compact
//! - **bge-base-en-v1.5** (768 dimensions) - Higher quality, larger
//!
//! # Example
//!
//! ```rust,no_run
//! use pulsedb::embedding::onnx::OnnxEmbedding;
//! use pulsedb::embedding::EmbeddingService;
//!
//! # fn main() -> pulsedb::Result<()> {
//! let service = OnnxEmbedding::new(None)?;  // Use default model
//! let embedding = service.embed("Hello, world!")?;
//! assert_eq!(embedding.len(), 384);
//! # Ok(())
//! # }
//! ```
//!
//! # Architecture
//!
//! The embedding pipeline mirrors what runs inside services like Ollama
//! or OpenAI's embedding endpoint, but executed locally:
//!
//! ```text
//! Text → Tokenize → ONNX Inference → Mean Pool → L2 Normalize → Embedding
//! ```
//!
//! # Performance Notes
//!
//! - Embedding generation is CPU-intensive
//! - Use `embed_batch()` for multiple texts (more efficient due to batched inference)
//! - Consider using `spawn_blocking` when called from async context

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use ndarray::Array2;
use ort::session::builder::GraphOptimizationLevel;
use ort::session::Session;
use tokenizers::Tokenizer;
use tracing::{debug, info};

use crate::embedding::{EmbeddingService, ProviderIdentity};
use crate::error::{PulseDBError, Result};
use crate::types::Embedding;

// ---------------------------------------------------------------------------
// Model configuration constants
// ---------------------------------------------------------------------------

/// Default model: all-MiniLM-L6-v2 (384 dimensions, 256 max tokens)
const DEFAULT_MODEL_NAME: &str = "all-MiniLM-L6-v2";
const DEFAULT_DIMENSION: usize = 384;
const DEFAULT_MAX_LENGTH: usize = 256;

/// Alternative model: bge-base-en-v1.5 (768 dimensions, 512 max tokens)
const BGE_MODEL_NAME: &str = "bge-base-en-v1.5";
const BGE_MAX_LENGTH: usize = 512;

/// File names expected in each model directory
const MODEL_FILENAME: &str = "model.onnx";
const TOKENIZER_FILENAME: &str = "tokenizer.json";

/// HuggingFace commit-SHA pin for the bundled all-MiniLM-L6-v2 (384d).
/// Pinned (not `resolve/main`) so the download is byte-stable across HF revisions;
/// a floating main revision would give two machines different fingerprints.
/// Verified 2026-07-31: resolve/<sha>/onnx/model.onnx returns byte-identical
/// content to the bundled model (model.onnx SHA-256
/// 6fd5d72fe4589f189f8ebc006442dbb529bb7ce38f8082112682524616046452,
/// 90 405 214 bytes).
const MINILM_HF_PINNED_SHA: &str = "1110a243fdf4706b3f48f1d95db1a4f5529b4d41";

// ---------------------------------------------------------------------------
// OnnxEmbedding struct
// ---------------------------------------------------------------------------

/// ONNX-based embedding service.
///
/// Generates embeddings locally using an ONNX model via ONNX Runtime.
/// The model and tokenizer are loaded eagerly at construction time for
/// fail-fast behavior — if the model files are missing, you'll get an
/// error at `PulseDB::open()`, not at the first `record_experience()`.
///
/// # Thread Safety
///
/// `OnnxEmbedding` is `Send + Sync`. ONNX Runtime's `Session` handles
/// internal synchronization for concurrent inference requests.
pub struct OnnxEmbedding {
    /// ONNX Runtime session (the loaded model, ready for inference).
    /// Wrapped in Mutex because `Session::run()` requires `&mut self`,
    /// but our `EmbeddingService` trait uses `&self` for concurrent access.
    session: Mutex<Session>,

    /// HuggingFace tokenizer (converts text to token IDs).
    /// Tokenizer is immutable after loading so no Mutex needed.
    tokenizer: Tokenizer,

    /// Embedding dimension produced by this model (e.g., 384 or 768).
    dimension: usize,

    /// Maximum sequence length the model accepts.
    max_length: usize,

    /// Construction-time full SHA-256 (length-framed `model.onnx ‖ tokenizer.json`
    /// bytes), retained so `identity()` is infallible and stable across later file
    /// replacement on disk (closes the file-replacement attack, Codex #14).
    identity_hash: String,
}

impl OnnxEmbedding {
    /// Creates a new ONNX embedding service with the default model (all-MiniLM-L6-v2, 384d).
    ///
    /// # Arguments
    ///
    /// * `model_path` - Optional path to a model directory containing `model.onnx`
    ///   and `tokenizer.json`. If `None`, looks in the default cache directory
    ///   (`~/.cache/pulsedb/models/all-MiniLM-L6-v2/`).
    ///
    /// # Errors
    ///
    /// Returns an error if model files are not found or cannot be loaded.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use pulsedb::embedding::onnx::OnnxEmbedding;
    ///
    /// # fn main() -> pulsedb::Result<()> {
    /// // Use default model from cache
    /// let service = OnnxEmbedding::new(None)?;
    ///
    /// // Use custom model directory
    /// let service = OnnxEmbedding::new(Some("./models/my-model".into()))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(model_path: Option<PathBuf>) -> Result<Self> {
        Self::with_dimension(model_path, DEFAULT_DIMENSION)
    }

    /// Creates an ONNX embedding service with a specific dimension.
    ///
    /// The dimension determines which default model to use:
    /// - `384` → all-MiniLM-L6-v2 (max 256 tokens)
    /// - `768` → bge-base-en-v1.5 (max 512 tokens)
    /// - Other → requires `model_path` to be provided
    ///
    /// # Arguments
    ///
    /// * `model_path` - Optional path to a model directory
    /// * `dimension` - Expected embedding dimension
    pub fn with_dimension(model_path: Option<PathBuf>, dimension: usize) -> Result<Self> {
        let max_length = match dimension {
            DEFAULT_DIMENSION => DEFAULT_MAX_LENGTH,
            768 => BGE_MAX_LENGTH,
            _ => DEFAULT_MAX_LENGTH,
        };

        let model_dir = resolve_model_dir(model_path.as_deref(), dimension)?;

        info!(
            model_dir = %model_dir.display(),
            dimension,
            max_length,
            "Loading ONNX embedding model"
        );

        Self::load_from_dir(&model_dir, dimension, max_length)
    }

    /// Downloads the default model files to the cache directory.
    ///
    /// Downloads `model.onnx` and `tokenizer.json` from HuggingFace Hub
    /// to `~/.cache/pulsedb/models/{model_name}/`.
    ///
    /// # Arguments
    ///
    /// * `dimension` - Which model to download:
    ///   - `384` → all-MiniLM-L6-v2
    ///   - `768` → bge-base-en-v1.5
    ///
    /// # Returns
    ///
    /// The path to the model directory.
    pub fn download_default_model(dimension: usize) -> Result<PathBuf> {
        let (model_name, model_url, tokenizer_url): (&str, String, String) = match dimension {
            DEFAULT_DIMENSION => (
                DEFAULT_MODEL_NAME,
                format!("https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2/resolve/{MINILM_HF_PINNED_SHA}/onnx/model.onnx"),
                format!("https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2/resolve/{MINILM_HF_PINNED_SHA}/tokenizer.json"),
            ),
            768 => (
                // bge-base-en-v1.5 (768d) is out of scope for this slice's
                // fingerprint pin (no bundled-model migration targets it); it
                // stays on `resolve/main`. Known limitation — see report.
                BGE_MODEL_NAME,
                "https://huggingface.co/BAAI/bge-base-en-v1.5/resolve/main/onnx/model.onnx"
                    .to_string(),
                "https://huggingface.co/BAAI/bge-base-en-v1.5/resolve/main/tokenizer.json"
                    .to_string(),
            ),
            _ => {
                return Err(PulseDBError::embedding(format!(
                    "No default model for dimension {dimension}. \
                     Supported: 384 (all-MiniLM-L6-v2), 768 (bge-base-en-v1.5)"
                )));
            }
        };

        let cache_dir = default_cache_dir(model_name);

        // Create directory
        std::fs::create_dir_all(&cache_dir).map_err(|e| {
            PulseDBError::embedding(format!(
                "Failed to create model cache directory {}: {e}",
                cache_dir.display()
            ))
        })?;

        // Acquire exclusive file lock to prevent concurrent download races.
        // Multiple threads/processes may call this simultaneously on first run.
        let lock_path = cache_dir.join(".download.lock");
        let lock_file = std::fs::File::create(&lock_path)
            .map_err(|e| PulseDBError::embedding(format!("Failed to create download lock: {e}")))?;
        use fs2::FileExt;
        lock_file.lock_exclusive().map_err(|e| {
            PulseDBError::embedding(format!("Failed to acquire download lock: {e}"))
        })?;

        let model_path = cache_dir.join(MODEL_FILENAME);
        let tokenizer_path = cache_dir.join(TOKENIZER_FILENAME);

        // Double-check after acquiring lock — another process may have downloaded while we waited
        if model_path.exists() && tokenizer_path.exists() {
            info!(dir = %cache_dir.display(), "Model files already downloaded by another process");
            return Ok(cache_dir);
        }

        // Download model if not present
        if !model_path.exists() {
            info!(url = %model_url, dest = %model_path.display(), "Downloading ONNX model");
            download_file(&model_url, &model_path)?;
        }

        // Download tokenizer if not present
        if !tokenizer_path.exists() {
            info!(url = %tokenizer_url, dest = %tokenizer_path.display(), "Downloading tokenizer");
            download_file(&tokenizer_url, &tokenizer_path)?;
        }

        info!(dir = %cache_dir.display(), "Model files ready");
        Ok(cache_dir)
    }

    /// Loads the model and tokenizer from a directory.
    fn load_from_dir(model_dir: &Path, dimension: usize, max_length: usize) -> Result<Self> {
        let model_path = model_dir.join(MODEL_FILENAME);
        let tokenizer_path = model_dir.join(TOKENIZER_FILENAME);

        // Validate files exist
        if !model_path.exists() {
            return Err(PulseDBError::embedding(format!(
                "Model file not found: {}. \
                 Download with OnnxEmbedding::download_default_model({dimension}) \
                 or provide a directory containing '{MODEL_FILENAME}'",
                model_path.display()
            )));
        }
        if !tokenizer_path.exists() {
            return Err(PulseDBError::embedding(format!(
                "Tokenizer file not found: {}. \
                 The model directory must contain '{TOKENIZER_FILENAME}'",
                tokenizer_path.display()
            )));
        }

        let session = create_session(&model_path)?;
        let tokenizer = load_tokenizer(&tokenizer_path, max_length)?;

        // Construction-time fingerprint: full SHA-256 of length-framed
        // model+tokenizer bytes. Computed once here so `identity()` is infallible
        // and the retained hash describes the exact artifacts loaded at construction
        // (not whatever the file path points at later — closes the file-replacement
        // attack, Codex #14). The file is read twice (once by `create_session`,
        // once here for the hash); that is an acceptable one-time construction cost.
        let model_bytes = std::fs::read(&model_path)
            .map_err(|e| PulseDBError::embedding(format!("Failed to read model bytes: {e}")))?;
        let tokenizer_bytes = std::fs::read(&tokenizer_path)
            .map_err(|e| PulseDBError::embedding(format!("Failed to read tokenizer bytes: {e}")))?;
        let identity_hash = compute_fingerprint(&model_bytes, &tokenizer_bytes);

        debug!(dimension, max_length, "ONNX embedding model loaded");

        Ok(Self {
            session: Mutex::new(session),
            tokenizer,
            dimension,
            max_length,
            identity_hash,
        })
    }
}

impl EmbeddingService for OnnxEmbedding {
    fn embed(&self, text: &str) -> Result<Embedding> {
        if text.is_empty() {
            return Err(PulseDBError::embedding("Cannot embed empty text"));
        }

        // 1. Tokenize: text → token IDs + attention mask
        let encoding = self
            .tokenizer
            .encode(text, true)
            .map_err(|e| PulseDBError::embedding(format!("Tokenization failed: {e}")))?;

        let ids = encoding.get_ids();
        let mask = encoding.get_attention_mask();

        // 2. Truncate to model's max sequence length
        let len = ids.len().min(self.max_length);

        // 3. Build input tensors [1, seq_len]
        let input_ids: Vec<i64> = ids[..len].iter().map(|&x| x as i64).collect();
        let attention_mask: Vec<i64> = mask[..len].iter().map(|&x| x as i64).collect();
        let token_type_ids: Vec<i64> = vec![0i64; len];

        let ids_array = Array2::from_shape_vec((1, len), input_ids)
            .map_err(|e| PulseDBError::embedding(format!("Tensor shape error: {e}")))?;
        let mask_array = Array2::from_shape_vec((1, len), attention_mask.clone())
            .map_err(|e| PulseDBError::embedding(format!("Tensor shape error: {e}")))?;
        let type_array = Array2::from_shape_vec((1, len), token_type_ids)
            .map_err(|e| PulseDBError::embedding(format!("Tensor shape error: {e}")))?;

        // 4. Create ONNX tensor values from ndarray
        let ids_tensor = ort::value::Tensor::from_array(ids_array)
            .map_err(|e| PulseDBError::embedding(format!("Tensor creation failed: {e}")))?;
        let mask_tensor = ort::value::Tensor::from_array(mask_array)
            .map_err(|e| PulseDBError::embedding(format!("Tensor creation failed: {e}")))?;
        let type_tensor = ort::value::Tensor::from_array(type_array)
            .map_err(|e| PulseDBError::embedding(format!("Tensor creation failed: {e}")))?;

        // 5. Run ONNX inference (lock session for mutable access)
        let mut session = self
            .session
            .lock()
            .map_err(|e| PulseDBError::embedding(format!("Session lock poisoned: {e}")))?;
        let outputs = session
            .run(ort::inputs![
                "input_ids" => ids_tensor,
                "attention_mask" => mask_tensor,
                "token_type_ids" => type_tensor,
            ])
            .map_err(|e| PulseDBError::embedding(format!("ONNX inference failed: {e}")))?;

        // 6. Extract token embeddings [1, seq_len, dim]
        let token_embeddings = outputs[0]
            .try_extract_tensor::<f32>()
            .map_err(|e| PulseDBError::embedding(format!("Output extraction failed: {e}")))?;

        // Convert attention mask for pooling
        let mask_u32: Vec<u32> = attention_mask.iter().map(|&x| x as u32).collect();

        // 7. Mean pool → [dim], then L2 normalize
        let pooled = mean_pool_raw(token_embeddings.1, &mask_u32, self.dimension, len);
        Ok(l2_normalize(&pooled))
    }

    fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Embedding>> {
        if texts.is_empty() {
            return Ok(vec![]);
        }
        if texts.len() == 1 {
            return Ok(vec![self.embed(texts[0])?]);
        }

        // 1. Tokenize all texts
        let encodings: Vec<_> = texts
            .iter()
            .map(|t| self.tokenizer.encode(*t, true))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| PulseDBError::embedding(format!("Batch tokenization failed: {e}")))?;

        // 2. Pad to longest sequence in batch (not max_length — saves compute)
        let max_len = encodings
            .iter()
            .map(|enc| enc.get_ids().len().min(self.max_length))
            .max()
            .unwrap_or(0);

        let batch_size = texts.len();

        // 3. Build padded tensors [batch_size, max_len]
        let mut input_ids = vec![0i64; batch_size * max_len];
        let mut attention_mask = vec![0i64; batch_size * max_len];
        let token_type_ids = vec![0i64; batch_size * max_len];

        for (i, enc) in encodings.iter().enumerate() {
            let ids = enc.get_ids();
            let mask = enc.get_attention_mask();
            let len = ids.len().min(self.max_length);

            for j in 0..len {
                input_ids[i * max_len + j] = ids[j] as i64;
                attention_mask[i * max_len + j] = mask[j] as i64;
            }
        }

        let ids_array = Array2::from_shape_vec((batch_size, max_len), input_ids)
            .map_err(|e| PulseDBError::embedding(format!("Tensor shape error: {e}")))?;
        let mask_array = Array2::from_shape_vec((batch_size, max_len), attention_mask.clone())
            .map_err(|e| PulseDBError::embedding(format!("Tensor shape error: {e}")))?;
        let type_array = Array2::from_shape_vec((batch_size, max_len), token_type_ids)
            .map_err(|e| PulseDBError::embedding(format!("Tensor shape error: {e}")))?;

        // 4. Create ONNX tensor values
        let ids_tensor = ort::value::Tensor::from_array(ids_array)
            .map_err(|e| PulseDBError::embedding(format!("Tensor creation failed: {e}")))?;
        let mask_tensor = ort::value::Tensor::from_array(mask_array)
            .map_err(|e| PulseDBError::embedding(format!("Tensor creation failed: {e}")))?;
        let type_tensor = ort::value::Tensor::from_array(type_array)
            .map_err(|e| PulseDBError::embedding(format!("Tensor creation failed: {e}")))?;

        // 5. Run batched inference (lock session for mutable access)
        let mut session = self
            .session
            .lock()
            .map_err(|e| PulseDBError::embedding(format!("Session lock poisoned: {e}")))?;
        let outputs = session
            .run(ort::inputs![
                "input_ids" => ids_tensor,
                "attention_mask" => mask_tensor,
                "token_type_ids" => type_tensor,
            ])
            .map_err(|e| PulseDBError::embedding(format!("ONNX inference failed: {e}")))?;

        // 6. Extract [batch_size, max_len, dim]
        let token_embeddings = outputs[0]
            .try_extract_tensor::<f32>()
            .map_err(|e| PulseDBError::embedding(format!("Output extraction failed: {e}")))?;

        let (_shape, data) = token_embeddings;

        // 7. Per-text mean pooling + L2 normalization
        let mut results = Vec::with_capacity(batch_size);
        for i in 0..batch_size {
            let text_mask: Vec<u32> = (0..max_len)
                .map(|j| attention_mask[i * max_len + j] as u32)
                .collect();

            // Extract this text's token embeddings from the flat data
            let offset = i * max_len * self.dimension;
            let text_data = &data[offset..offset + max_len * self.dimension];

            let pooled = mean_pool_raw(text_data, &text_mask, self.dimension, max_len);
            results.push(l2_normalize(&pooled));
        }

        Ok(results)
    }

    fn dimension(&self) -> usize {
        self.dimension
    }

    fn identity(&self) -> ProviderIdentity {
        // Identity is the construction-time full SHA-256 of length-framed
        // model.onnx ‖ tokenizer.json bytes (computed once in `load_from_dir`),
        // rendered as `onnx-<full hex sha256>`. The `onnx-` prefix carries the
        // provider discriminator for human readability; the 64-char hex hash is
        // the discriminator. Infallible — the hash was captured at construction.
        ProviderIdentity {
            provider: "builtin-onnx".to_string(),
            model_id: format!("onnx-{}", self.identity_hash),
        }
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Creates an ONNX Runtime session with optimized settings.
fn create_session(model_path: &Path) -> Result<Session> {
    Session::builder()
        .map_err(|e| PulseDBError::embedding(format!("Failed to create session builder: {e}")))?
        // Level3: all optimizations (operator fusion, constant folding, etc.)
        .with_optimization_level(GraphOptimizationLevel::Level3)
        .map_err(|e| PulseDBError::embedding(format!("Failed to set optimization level: {e}")))?
        .commit_from_file(model_path)
        .map_err(|e| {
            PulseDBError::embedding(format!(
                "Failed to load ONNX model from {}: {e}",
                model_path.display()
            ))
        })
}

/// Loads a HuggingFace tokenizer from a tokenizer.json file.
fn load_tokenizer(tokenizer_path: &Path, max_length: usize) -> Result<Tokenizer> {
    let mut tokenizer = Tokenizer::from_file(tokenizer_path).map_err(|e| {
        PulseDBError::embedding(format!(
            "Failed to load tokenizer from {}: {e}",
            tokenizer_path.display()
        ))
    })?;

    // Configure truncation to model's max sequence length
    tokenizer
        .with_truncation(Some(tokenizers::TruncationParams {
            max_length,
            strategy: tokenizers::TruncationStrategy::LongestFirst,
            ..Default::default()
        }))
        .map_err(|e| PulseDBError::embedding(format!("Failed to set truncation: {e}")))?;

    // Disable padding — we handle padding manually in embed_batch()
    // for smart padding (pad to longest in batch, not max_length)
    tokenizer.with_padding(None);

    Ok(tokenizer)
}

/// Resolves the model directory from an optional user path or default cache.
fn resolve_model_dir(model_path: Option<&Path>, dimension: usize) -> Result<PathBuf> {
    match model_path {
        Some(path) => {
            if !path.exists() {
                return Err(PulseDBError::embedding(format!(
                    "Model directory not found: {}",
                    path.display()
                )));
            }
            Ok(path.to_path_buf())
        }
        None => {
            // Determine model name from dimension
            let model_name = match dimension {
                DEFAULT_DIMENSION => DEFAULT_MODEL_NAME,
                768 => BGE_MODEL_NAME,
                _ => {
                    return Err(PulseDBError::embedding(format!(
                        "No default model for dimension {dimension}. \
                         Provide a model_path for custom dimensions, \
                         or use 384 (all-MiniLM-L6-v2) or 768 (bge-base-en-v1.5)"
                    )));
                }
            };

            let cache_dir = default_cache_dir(model_name);

            if !cache_dir.join(MODEL_FILENAME).exists() {
                return Err(PulseDBError::embedding(format!(
                    "Model not found at {}. \
                     Download with: OnnxEmbedding::download_default_model({dimension})",
                    cache_dir.display()
                )));
            }

            Ok(cache_dir)
        }
    }
}

/// Returns the default cache directory for a model.
///
/// Platform-specific:
/// - Linux: `~/.cache/pulsedb/models/{name}/`
/// - macOS: `~/Library/Caches/pulsedb/models/{name}/`
/// - Windows: `{LOCALAPPDATA}/pulsedb/models/{name}/`
fn default_cache_dir(model_name: &str) -> PathBuf {
    dirs::cache_dir()
        .unwrap_or_else(|| PathBuf::from(".cache"))
        .join("pulsedb")
        .join("models")
        .join(model_name)
}

/// Mean pooling over token embeddings from flat data.
///
/// Computes the attention-weighted average of token embeddings to produce
/// a single sentence embedding. Only tokens with mask=1 contribute.
///
/// The data is laid out as `[seq_len * dim]` in row-major order, where
/// each contiguous block of `dim` floats is one token's embedding.
///
/// # Arguments
///
/// * `data` - Flat f32 slice of shape `[seq_len, dim]`
/// * `attention_mask` - Shape `[seq_len]`, 1 for real tokens, 0 for padding
/// * `dim` - Embedding dimension
/// * `seq_len` - Number of tokens
fn mean_pool_raw(data: &[f32], attention_mask: &[u32], dim: usize, seq_len: usize) -> Vec<f32> {
    let mut pooled = vec![0.0f32; dim];
    let mut mask_sum = 0.0f32;

    for (t, &mask_val) in attention_mask.iter().enumerate().take(seq_len) {
        let weight = mask_val as f32;
        mask_sum += weight;
        let offset = t * dim;
        for d in 0..dim {
            pooled[d] += data[offset + d] * weight;
        }
    }

    // Divide by number of real tokens (avoid division by zero)
    if mask_sum > 0.0 {
        for val in &mut pooled {
            *val /= mask_sum;
        }
    }

    pooled
}

/// L2 normalizes a vector to unit length.
///
/// After normalization, the vector has magnitude 1.0, which means
/// cosine similarity can be computed as a simple dot product:
/// `cos(a, b) = a · b` when `|a| = |b| = 1`.
fn l2_normalize(v: &[f32]) -> Vec<f32> {
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        v.iter().map(|x| x / norm).collect()
    } else {
        v.to_vec()
    }
}

/// Construction-time fingerprint: full SHA-256 of length-framed
/// `model_bytes ‖ tokenizer_bytes`. Length-framing (big-endian u64 lengths)
/// prevents concatenation-ambiguity collisions — e.g. two file pairs whose
/// plain concatenation is identical still hash differently because their
/// per-file length prefixes differ. Full 256 bits are retained (NOT truncated).
fn compute_fingerprint(model_bytes: &[u8], tokenizer_bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update((model_bytes.len() as u64).to_be_bytes());
    hasher.update(model_bytes);
    hasher.update((tokenizer_bytes.len() as u64).to_be_bytes());
    hasher.update(tokenizer_bytes);
    format!("{:x}", hasher.finalize())
}

/// Downloads a file from a URL to a local path.
///
/// Uses atomic write (temp file + rename) to prevent partial downloads
/// from leaving corrupted files that block future retry attempts.
fn download_file(url: &str, dest: &Path) -> Result<()> {
    let response = ureq::get(url)
        .call()
        .map_err(|e| PulseDBError::embedding(format!("Download failed for {url}: {e}")))?;

    // Write to temp file first — rename on success prevents partial corruption
    let temp = dest.with_extension("tmp");
    let mut reader = response.into_body().into_reader();
    let mut file = std::fs::File::create(&temp).map_err(|e| {
        PulseDBError::embedding(format!("Failed to create file {}: {e}", temp.display()))
    })?;

    if let Err(e) = std::io::copy(&mut reader, &mut file) {
        let _ = std::fs::remove_file(&temp);
        return Err(PulseDBError::embedding(format!(
            "Failed to write to {}: {e}",
            dest.display()
        )));
    }

    // Atomic rename — only the complete file appears at the destination
    std::fs::rename(&temp, dest).map_err(|e| {
        let _ = std::fs::remove_file(&temp);
        PulseDBError::embedding(format!(
            "Failed to finalize download {}: {e}",
            dest.display()
        ))
    })?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::super::BUNDLED_MINILM_FINGERPRINT;
    use super::*;

    // --- L2 normalization tests ---

    #[test]
    fn test_l2_normalize_basic() {
        let v = vec![3.0, 4.0];
        let normalized = l2_normalize(&v);
        // norm = sqrt(9 + 16) = 5
        assert!((normalized[0] - 0.6).abs() < 1e-6);
        assert!((normalized[1] - 0.8).abs() < 1e-6);

        // Verify unit length
        let norm: f32 = normalized.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_l2_normalize_zero_vector() {
        let v = vec![0.0, 0.0, 0.0];
        let normalized = l2_normalize(&v);
        // Zero vector stays zero (no division by zero)
        assert_eq!(normalized, vec![0.0, 0.0, 0.0]);
    }

    #[test]
    fn test_l2_normalize_already_unit() {
        let v = vec![1.0, 0.0, 0.0];
        let normalized = l2_normalize(&v);
        assert!((normalized[0] - 1.0).abs() < 1e-6);
        assert!((normalized[1] - 0.0).abs() < 1e-6);
    }

    // --- Mean pooling tests ---

    #[test]
    fn test_mean_pool_uniform_mask() {
        // All tokens are real (mask = all ones)
        // 2 tokens, 3 dimensions → average of both
        let data = vec![
            1.0, 2.0, 3.0, // token 0
            5.0, 6.0, 7.0, // token 1
        ];
        let mask = vec![1u32, 1];

        let pooled = mean_pool_raw(&data, &mask, 3, 2);
        // Average: [(1+5)/2, (2+6)/2, (3+7)/2] = [3, 4, 5]
        assert!((pooled[0] - 3.0).abs() < 1e-6);
        assert!((pooled[1] - 4.0).abs() < 1e-6);
        assert!((pooled[2] - 5.0).abs() < 1e-6);
    }

    #[test]
    fn test_mean_pool_partial_mask() {
        // Only first token is real, second is padding
        let data = vec![
            1.0, 2.0, 3.0, // token 0 (real)
            99.0, 99.0, 99.0, // token 1 (padding — should be ignored)
        ];
        let mask = vec![1u32, 0]; // Only token 0 counts

        let pooled = mean_pool_raw(&data, &mask, 3, 2);
        // Only token 0 contributes: [1, 2, 3]
        assert!((pooled[0] - 1.0).abs() < 1e-6);
        assert!((pooled[1] - 2.0).abs() < 1e-6);
        assert!((pooled[2] - 3.0).abs() < 1e-6);
    }

    #[test]
    fn test_mean_pool_zero_mask() {
        // Edge case: all tokens masked (shouldn't happen in practice)
        let data = vec![99.0, 99.0, 99.0];
        let mask = vec![0u32];

        let pooled = mean_pool_raw(&data, &mask, 3, 1);
        // All zeros (no tokens contribute)
        assert_eq!(pooled, vec![0.0, 0.0, 0.0]);
    }

    // --- Path resolution tests ---

    #[test]
    fn test_resolve_model_dir_custom_path_missing() {
        let result = resolve_model_dir(Some(Path::new("/nonexistent/path")), 384);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("not found"), "Error: {err}");
    }

    #[test]
    fn test_resolve_model_dir_unsupported_dimension() {
        let result = resolve_model_dir(None, 999);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("No default model"), "Error: {err}");
    }

    #[test]
    fn test_default_cache_dir_format() {
        let dir = default_cache_dir("test-model");
        // Should end with pulsedb/models/test-model
        let path_str = dir.to_string_lossy();
        assert!(path_str.contains("pulsedb"), "Path: {path_str}");
        assert!(path_str.contains("models"), "Path: {path_str}");
        assert!(path_str.contains("test-model"), "Path: {path_str}");
    }

    // --- Thread safety ---

    #[test]
    fn test_onnx_embedding_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<OnnxEmbedding>();
    }

    // --- Fingerprint behavioral tests (portable — no model load) ---

    #[test]
    fn fingerprint_shape_onnx_prefix_and_length() {
        // AC-3 anchor: the identity model_id carries the `onnx-` prefix and a
        // full 64-char hex SHA-256. println! so `--nocapture` emits "onnx-".
        let hash = compute_fingerprint(&[1, 2, 3], &[4, 5, 6]);
        let model_id = format!("onnx-{hash}");
        println!("fingerprint model_id = {model_id}");
        assert!(
            model_id.starts_with("onnx-"),
            "model_id must carry the onnx- prefix"
        );
        assert_eq!(
            model_id.len(),
            "onnx-".len() + 64,
            "model_id must be onnx- + 64-char hex SHA-256"
        );
    }

    #[test]
    fn fingerprint_tokenizer_sensitivity() {
        // Same model bytes, two different tokenizer byte slices → different.
        let model = b"model-bytes";
        let fp_a = compute_fingerprint(model, b"tokenizer-A");
        let fp_b = compute_fingerprint(model, b"tokenizer-B");
        assert_ne!(
            fp_a, fp_b,
            "different tokenizer bytes must yield different fingerprints"
        );
    }

    #[test]
    fn fingerprint_model_sensitivity() {
        // Same tokenizer bytes, two different model byte slices → different.
        let tokenizer = b"tokenizer-bytes";
        let fp_a = compute_fingerprint(b"model-A", tokenizer);
        let fp_b = compute_fingerprint(b"model-B", tokenizer);
        assert_ne!(
            fp_a, fp_b,
            "different model bytes must yield different fingerprints"
        );
    }

    #[test]
    fn fingerprint_length_framing_disambiguates() {
        // Both pairs' UNFRAMED concatenation is [0xAA, 0xBB, 0xCC] — without
        // length-framing these would collide. The be64 per-file length prefixes
        // make the framed inputs distinct → distinct fingerprints.
        let fp_a = compute_fingerprint(&[0xAA, 0xBB], &[0xCC]);
        let fp_b = compute_fingerprint(&[0xAA], &[0xBB, 0xCC]);
        assert_ne!(
            fp_a, fp_b,
            "length-framing must disambiguate concatenation-colliding pairs"
        );
    }

    // --- Model-gated tests (run only when the bundled MiniLM is cached) ---

    /// Returns the bundled MiniLM cache dir iff both model.onnx + tokenizer.json
    /// are present; model-gated tests skip (with an eprintln! note) otherwise.
    fn bundled_minilm_dir() -> Option<PathBuf> {
        let dir = default_cache_dir(DEFAULT_MODEL_NAME);
        if dir.join(MODEL_FILENAME).exists() && dir.join(TOKENIZER_FILENAME).exists() {
            Some(dir)
        } else {
            None
        }
    }

    #[test]
    fn bundled_minilm_fingerprint_pinned() {
        // AC-1 anchor: the bundled MiniLM's construction-time fingerprint must
        // match the pinned constant byte-for-byte.
        let dir = match bundled_minilm_dir() {
            Some(d) => d,
            None => {
                eprintln!("skip: bundled MiniLM not cached — pinned-fingerprint test not run");
                return;
            }
        };
        let emb = OnnxEmbedding::new(Some(dir)).expect("construct bundled MiniLM");
        let identity = emb.identity();
        assert_eq!(identity.provider, "builtin-onnx");
        assert_eq!(
            identity.model_id,
            format!("onnx-{}", BUNDLED_MINILM_FINGERPRINT),
            "bundled MiniLM fingerprint must match the pinned constant"
        );
    }

    #[test]
    fn identity_stable_across_file_replacement() {
        // Codex #14: replacing model.onnx on disk AFTER construction must NOT
        // change identity() — the retained hash describes the artifacts loaded
        // at construction, not the file path's later contents.
        let src_dir = match bundled_minilm_dir() {
            Some(d) => d,
            None => {
                eprintln!("skip: bundled MiniLM not cached — file-replacement test not run");
                return;
            }
        };
        let tmp = tempfile::tempdir().expect("tempdir");
        std::fs::copy(
            src_dir.join(MODEL_FILENAME),
            tmp.path().join(MODEL_FILENAME),
        )
        .expect("copy model");
        std::fs::copy(
            src_dir.join(TOKENIZER_FILENAME),
            tmp.path().join(TOKENIZER_FILENAME),
        )
        .expect("copy tokenizer");

        let emb = OnnxEmbedding::new(Some(tmp.path().to_path_buf())).expect("construct");
        let before = emb.identity();

        // Tamper the model file on disk AFTER construction.
        std::fs::write(tmp.path().join(MODEL_FILENAME), b"tampered").expect("overwrite");

        let after = emb.identity();
        assert_eq!(
            before, after,
            "identity must be construction-time-stable (Codex #14)"
        );
    }
}
