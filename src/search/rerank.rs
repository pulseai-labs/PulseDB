//! Weight resolution helpers for recall ranking.

use std::cmp::Ordering;

use crate::config::RecallWeights;
use crate::error::Result;
use crate::search::SearchResult;

/// Resolves effective recall weights from request and collective defaults.
///
/// Explicit request weights take precedence over stored collective defaults.
/// Request weights are validated so bad caller input fails loudly instead of
/// silently falling back to legacy ranking.
pub(crate) fn resolve_recall_weights(
    request: Option<RecallWeights>,
    collective_default: Option<RecallWeights>,
) -> Result<Option<RecallWeights>> {
    if let Some(weights) = request {
        weights.validate("weights")?;
        Ok(Some(weights))
    } else {
        Ok(collective_default)
    }
}

/// Returns true when effective weights should use the legacy search path.
pub(crate) fn is_legacy_recall(weights: Option<RecallWeights>) -> bool {
    match weights {
        None => true,
        Some(weights) => weights.energy == 0.0,
    }
}

/// Clamps a value into the `[0, 1]` interval.
pub(crate) fn clamp01(value: f32) -> f32 {
    value.clamp(0.0, 1.0)
}

/// Computes the linear recall blend from raw similarity and pre-computed energy.
pub(crate) fn blend_score(similarity: f32, energy: f32, weights: RecallWeights) -> f32 {
    weights.similarity * clamp01(similarity) + weights.energy * energy
}

/// Sorts scored results by score descending and truncates to `k`.
pub(crate) fn rerank(scored: Vec<(SearchResult, f32)>, k: usize) -> Vec<SearchResult> {
    let mut indexed: Vec<_> = scored.into_iter().enumerate().collect();
    indexed.sort_by(
        |(left_index, (_, left_score)), (right_index, (_, right_score))| {
            right_score
                .partial_cmp(left_score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| left_index.cmp(right_index))
        },
    );
    indexed.truncate(k);
    indexed.into_iter().map(|(_, (result, _))| result).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::experience::{Experience, ExperienceType};
    use crate::types::{AgentId, CollectiveId, ExperienceId, Timestamp};

    fn make_result(content: &str, similarity: f32) -> SearchResult {
        let timestamp = Timestamp::now();
        SearchResult {
            experience: Experience {
                id: ExperienceId::new(),
                collective_id: CollectiveId::new(),
                content: content.to_string(),
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
    fn resolve_absent_request_uses_collective_default() {
        let collective_default = RecallWeights::new(0.7, 0.3);

        let resolved = resolve_recall_weights(None, Some(collective_default)).unwrap();

        assert_eq!(resolved, Some(collective_default));
    }

    #[test]
    fn resolve_request_overrides_collective_default() {
        let request = RecallWeights::new(1.0, 0.0);
        let collective_default = RecallWeights::new(0.7, 0.3);

        let resolved = resolve_recall_weights(Some(request), Some(collective_default)).unwrap();

        assert_eq!(resolved, Some(request));
        assert!(is_legacy_recall(resolved));
    }

    #[test]
    fn beta_zero_predicate_covers_none_and_explicit_legacy() {
        assert!(is_legacy_recall(None));
        assert!(is_legacy_recall(Some(RecallWeights::new(1.0, 0.0))));
        assert!(!is_legacy_recall(Some(RecallWeights::new(0.7, 0.3))));
    }

    #[test]
    fn invalid_request_weights_err() {
        let err = resolve_recall_weights(Some(RecallWeights::new(0.5, 0.9)), None);

        assert!(err.is_err());
    }

    #[test]
    fn blend_identity_similarity() {
        let weights = RecallWeights::new(1.0, 0.0);

        assert_eq!(blend_score(0.42, 0.9, weights), 0.42);
    }

    #[test]
    fn blend_identity_energy() {
        let weights = RecallWeights::new(0.0, 1.0);

        assert_eq!(blend_score(0.42, 0.9, weights), 0.9);
    }

    #[test]
    fn blend_clamps_negative_similarity() {
        let weights = RecallWeights::new(1.0, 0.0);

        assert_eq!(blend_score(-0.2, 0.9, weights), 0.0);
    }

    #[test]
    fn rerank_sorts_desc_truncates_and_preserves_ties() {
        let scored = vec![
            (make_result("low", 0.2), 0.2),
            (make_result("tie-a", 0.7), 0.7),
            (make_result("high", 0.9), 0.9),
            (make_result("tie-b", 0.7), 0.7),
        ];

        let reranked = rerank(scored, 3);

        assert_eq!(reranked.len(), 3);
        assert_eq!(reranked[0].experience.content, "high");
        assert_eq!(reranked[1].experience.content, "tie-a");
        assert_eq!(reranked[2].experience.content, "tie-b");
    }
}
