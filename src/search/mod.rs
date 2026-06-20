//! Search operations for PulseDB.
//!
//! This module provides search filtering and query building for experience
//! retrieval operations (recent, similarity, context candidates).

mod context;
mod filter;
pub(crate) mod rerank;

pub use context::{ContextCandidates, ContextRequest};
pub use filter::SearchFilter;

use crate::config::RecallWeights;
use crate::experience::Experience;

/// Options for recall search.
///
/// `SearchOptions` is the call-time configuration for [`PulseDB::search()`].
/// Recall-weight precedence is: explicit `weights` here > the collective's
/// configured `DecayConfig.default_recall_weights` (per-collective stored config,
/// else the global `Config.decay`) > legacy pure-similarity ranking. So
/// `weights: None` preserves legacy ranking only when no default is configured.
///
/// [`PulseDB::search()`]: crate::PulseDB::search
#[derive(Clone, Debug)]
pub struct SearchOptions {
    /// Maximum number of results to return.
    pub k: usize,

    /// Filter criteria applied after vector retrieval.
    pub filter: SearchFilter,

    /// Optional recall weights for similarity and temporal energy. `None` falls
    /// back to the configured `default_recall_weights`, then to legacy ranking.
    pub weights: Option<RecallWeights>,
}

impl Default for SearchOptions {
    fn default() -> Self {
        Self {
            k: 10,
            filter: SearchFilter::default(),
            weights: None,
        }
    }
}

/// A search result pairing an experience with its similarity score.
///
/// Returned by [`PulseDB::search_similar()`](crate::PulseDB::search_similar) and
/// [`PulseDB::search_similar_filtered()`](crate::PulseDB::search_similar_filtered).
/// Results are sorted by `similarity` descending (most similar first).
///
/// # Similarity Score
///
/// The `similarity` field is computed as `1.0 - cosine_distance`, where
/// cosine distance ranges from 0.0 (identical) to 2.0 (opposite). This
/// gives a similarity range of [-1.0, 1.0], where:
/// - `1.0` = identical vectors
/// - `0.0` = orthogonal vectors
/// - `-1.0` = opposite vectors
///
/// In practice, experience embeddings from transformer models (e.g.,
/// all-MiniLM-L6-v2) produce non-negative values, so similarity is
/// typically in [0.0, 1.0].
///
/// # Example
///
/// ```rust
/// # fn main() -> pulsedb::Result<()> {
/// # let dir = tempfile::tempdir().unwrap();
/// # let db = pulsedb::PulseDB::open(dir.path().join("test.db"), pulsedb::Config::default())?;
/// # let collective_id = db.create_collective("example")?;
/// # let query_embedding = vec![0.1f32; 384];
/// let results = db.search_similar(collective_id, &query_embedding, 10)?;
/// for result in &results {
///     println!(
///         "similarity={:.3}: {}",
///         result.similarity, result.experience.content
///     );
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct SearchResult {
    /// The full experience record.
    pub experience: Experience,

    /// Similarity score (1.0 - cosine_distance).
    ///
    /// Higher is more similar. Typically in [0.0, 1.0] for transformer
    /// embeddings. Theoretical range is [-1.0, 1.0].
    pub similarity: f32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::experience::ExperienceType;
    use crate::types::{AgentId, CollectiveId, ExperienceId, Timestamp};

    /// Helper to create a SearchResult with a given similarity.
    fn make_result(similarity: f32) -> SearchResult {
        let timestamp = Timestamp::now();
        SearchResult {
            experience: Experience {
                id: ExperienceId::new(),
                collective_id: CollectiveId::new(),
                content: format!("test sim={}", similarity),
                embedding: vec![0.1; 384],
                experience_type: ExperienceType::default(),
                importance: 0.5,
                confidence: 0.8,
                applications: std::collections::BTreeMap::new(),
                domain: vec!["test".to_string()],
                related_files: vec![],
                source_agent: AgentId::new("agent-1"),
                source_task: None,
                timestamp,
                last_reinforced: timestamp,
                archived: false,
            },
            similarity,
        }
    }

    #[test]
    fn test_search_result_clone_and_debug() {
        let result = make_result(0.95);
        let cloned = result.clone();
        assert_eq!(cloned.similarity, 0.95);
        // Debug should not panic
        let debug = format!("{:?}", result);
        assert!(!debug.is_empty());
    }

    #[test]
    fn test_search_result_similarity_identity() {
        // 1.0 - 0.0 distance = 1.0 similarity (identical vectors)
        let result = make_result(1.0);
        assert_eq!(result.similarity, 1.0);
    }

    #[test]
    fn test_search_result_similarity_can_be_negative() {
        // 1.0 - 2.0 distance = -1.0 similarity (opposite vectors)
        let result = make_result(-1.0);
        assert!(result.similarity < 0.0);
    }
}
