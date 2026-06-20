//! Context candidates retrieval types.
//!
//! [`ContextRequest`] and [`ContextCandidates`] enable a single call to
//! [`PulseDB::get_context_candidates()`](crate::PulseDB::get_context_candidates)
//! that orchestrates all retrieval primitives (similarity search, recent
//! experiences, insights, relations, active agents) into one response.

use crate::activity::Activity;
use crate::config::RecallWeights;
use crate::experience::Experience;
use crate::insight::DerivedInsight;
use crate::relation::ExperienceRelation;
use crate::search::{SearchFilter, SearchResult};
use crate::types::CollectiveId;

/// Request for unified context retrieval.
///
/// Configures which primitives to query and how many results to return.
/// Pass this to [`PulseDB::get_context_candidates()`](crate::PulseDB::get_context_candidates).
///
/// # Required Fields
///
/// - `collective_id` - The collective to search within (must exist)
/// - `query_embedding` - Embedding vector for similarity and insight search
///   (must match the collective's embedding dimension)
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
///     filter: SearchFilter {
///         domains: Some(vec!["rust".to_string()]),
///         ..SearchFilter::default()
///     },
///     ..ContextRequest::default()
/// })?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct ContextRequest {
    /// The collective to search within (must exist).
    pub collective_id: CollectiveId,

    /// Query embedding vector for similarity search and insight retrieval.
    ///
    /// Must match the collective's configured embedding dimension.
    pub query_embedding: Vec<f32>,

    /// Maximum number of similar experiences to return (1-1000, default: 20).
    pub max_similar: usize,

    /// Maximum number of recent experiences to return (1-1000, default: 10).
    pub max_recent: usize,

    /// Whether to include derived insights in the response (default: true).
    pub include_insights: bool,

    /// Whether to include relations for returned experiences (default: true).
    pub include_relations: bool,

    /// Whether to include active agent activities (default: true).
    pub include_active_agents: bool,

    /// Filter criteria applied to similar and recent experience queries.
    pub filter: SearchFilter,

    /// Optional recall weights for similarity and temporal energy.
    ///
    /// This field is reserved for weighted context-candidate ranking in
    /// VS-3.5.2 work item 1.03. `None` preserves legacy ranking.
    pub recall_weights: Option<RecallWeights>,
}

impl Default for ContextRequest {
    fn default() -> Self {
        Self {
            collective_id: CollectiveId::nil(),
            query_embedding: vec![],
            max_similar: 20,
            max_recent: 10,
            include_insights: true,
            include_relations: true,
            include_active_agents: true,
            filter: SearchFilter::default(),
            recall_weights: None,
        }
    }
}

/// Aggregated context candidates from all retrieval primitives.
///
/// Returned by [`PulseDB::get_context_candidates()`](crate::PulseDB::get_context_candidates).
/// Each field may be empty if no results were found or the corresponding
/// feature was disabled in the [`ContextRequest`].
///
/// # Field Semantics
///
/// - `similar_experiences` - Sorted by similarity descending (most similar first)
/// - `recent_experiences` - Sorted by timestamp descending (newest first)
/// - `insights` - Similar insights found via HNSW vector search
/// - `relations` - Relations involving any returned experience (deduplicated)
/// - `active_agents` - Non-stale agents in the collective
#[derive(Clone, Debug)]
pub struct ContextCandidates {
    /// Semantically similar experiences. Ordered by descending cosine similarity
    /// for legacy recall; when recall weights are active, ordered by the blended
    /// similarity+energy score — so `SearchResult.similarity` (always the raw
    /// cosine value) is not necessarily monotonically descending.
    pub similar_experiences: Vec<SearchResult>,

    /// Most recent experiences, sorted by timestamp descending.
    pub recent_experiences: Vec<Experience>,

    /// Derived insights similar to the query embedding.
    ///
    /// Empty if `include_insights` was `false` in the request.
    pub insights: Vec<DerivedInsight>,

    /// Relations involving the returned similar and recent experiences.
    ///
    /// Deduplicated by `RelationId`. Empty if `include_relations` was `false`.
    pub relations: Vec<ExperienceRelation>,

    /// Currently active (non-stale) agents in the collective.
    ///
    /// Empty if `include_active_agents` was `false` in the request.
    pub active_agents: Vec<Activity>,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::config::{Config, RecallWeights};
    use crate::types::{InstanceId, Timestamp};
    use crate::PulseDB;

    fn embedding_with_query_cosine(cosine: f32) -> Vec<f32> {
        let mut embedding = vec![0.0; 384];
        embedding[0] = cosine;
        embedding[1] = (1.0 - cosine.powi(2)).sqrt();
        embedding
    }

    #[test]
    fn test_context_request_default_values() {
        let req = ContextRequest::default();
        assert_eq!(req.collective_id, CollectiveId::nil());
        assert!(req.query_embedding.is_empty());
        assert_eq!(req.max_similar, 20);
        assert_eq!(req.max_recent, 10);
        assert!(req.include_insights);
        assert!(req.include_relations);
        assert!(req.include_active_agents);
        assert!(req.filter.exclude_archived);
    }

    #[test]
    fn test_context_request_clone_and_debug() {
        let req = ContextRequest {
            collective_id: CollectiveId::new(),
            query_embedding: vec![0.1; 384],
            max_similar: 5,
            ..ContextRequest::default()
        };
        let cloned = req.clone();
        assert_eq!(cloned.max_similar, 5);
        assert_eq!(cloned.query_embedding.len(), 384);

        let debug = format!("{:?}", req);
        assert!(debug.contains("ContextRequest"));
    }

    #[test]
    fn test_context_candidates_clone_and_debug() {
        let candidates = ContextCandidates {
            similar_experiences: vec![],
            recent_experiences: vec![],
            insights: vec![],
            relations: vec![],
            active_agents: vec![],
        };
        let cloned = candidates.clone();
        assert!(cloned.similar_experiences.is_empty());

        let debug = format!("{:?}", candidates);
        assert!(debug.contains("ContextCandidates"));
    }

    #[test]
    fn context_candidates_energy_reranks_fresh_ahead() {
        let dir = tempfile::tempdir().unwrap();
        let db = PulseDB::open(dir.path().join("context-rerank.db"), Config::default()).unwrap();
        let collective_id = db.create_collective("context-rerank").unwrap();
        let query_embedding = embedding_with_query_cosine(1.0);
        let now = Timestamp::now();
        let stale_last_reinforced =
            Timestamp::from_millis(now.as_millis() - 90 * 24 * 60 * 60 * 1000);
        let fresh_last_reinforced = now;

        let stale_id = db
            .insert_experience_backdated(
                collective_id,
                "stale but most similar",
                embedding_with_query_cosine(0.95),
                0.8,
                BTreeMap::new(),
                stale_last_reinforced,
            )
            .unwrap();
        let fresh_id = db
            .insert_experience_backdated(
                collective_id,
                "fresh reinforced",
                embedding_with_query_cosine(0.90),
                0.8,
                BTreeMap::from([(InstanceId::new(), 24)]),
                fresh_last_reinforced,
            )
            .unwrap();

        let weighted = db
            .get_context_candidates(ContextRequest {
                collective_id,
                query_embedding: query_embedding.clone(),
                max_similar: 2,
                max_recent: 2,
                include_insights: false,
                include_relations: false,
                include_active_agents: false,
                recall_weights: Some(RecallWeights::new(0.5, 0.5)),
                ..ContextRequest::default()
            })
            .unwrap();
        let legacy = db
            .get_context_candidates(ContextRequest {
                collective_id,
                query_embedding,
                max_similar: 2,
                max_recent: 2,
                include_insights: false,
                include_relations: false,
                include_active_agents: false,
                recall_weights: None,
                ..ContextRequest::default()
            })
            .unwrap();

        assert_eq!(weighted.similar_experiences[0].experience.id, fresh_id);
        assert_eq!(legacy.similar_experiences[0].experience.id, stale_id);
    }
}
