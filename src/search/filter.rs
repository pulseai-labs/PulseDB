//! Search filtering for experience queries.
//!
//! [`SearchFilter`] provides a composable way to filter experiences across
//! different query types (recent, similarity, context candidates). Filters
//! are applied as post-filters after the primary retrieval (timestamp scan
//! or HNSW search).

use crate::experience::{Experience, ExperienceType};
use crate::types::Timestamp;

/// Filter criteria for experience search operations.
///
/// Used by `get_recent_experiences_filtered()` and `search_similar_filtered()`.
/// Fields set to `None` are not filtered on. The `exclude_archived` field
/// defaults to `true` since archived experiences are rarely wanted in queries.
///
/// # Example
///
/// ```rust
/// use pulsedb::SearchFilter;
///
/// // Filter to only "Solution" experiences with importance >= 0.5
/// let filter = SearchFilter {
///     min_importance: Some(0.5),
///     experience_types: Some(vec![pulsedb::ExperienceType::Solution {
///         problem_ref: None,
///         approach: String::new(),
///         worked: true,
///     }]),
///     ..SearchFilter::default()
/// };
/// ```
#[derive(Clone, Debug)]
pub struct SearchFilter {
    /// Only include experiences with at least one matching domain tag.
    ///
    /// `None` means no domain filtering. An empty `Some(vec![])` matches nothing.
    pub domains: Option<Vec<String>>,

    /// Only include experiences of these types.
    ///
    /// Matching is done on the type discriminant (tag), not the associated data.
    /// For example, any `Solution { .. }` matches if `Solution` is in the list.
    pub experience_types: Option<Vec<ExperienceType>>,

    /// Only include experiences with importance >= this threshold.
    pub min_importance: Option<f32>,

    /// Only include experiences with confidence >= this threshold.
    pub min_confidence: Option<f32>,

    /// Only include experiences created at or after this timestamp.
    pub since: Option<Timestamp>,

    /// Whether to exclude archived experiences (default: `true`).
    pub exclude_archived: bool,
}

impl Default for SearchFilter {
    fn default() -> Self {
        Self {
            domains: None,
            experience_types: None,
            min_importance: None,
            min_confidence: None,
            since: None,
            exclude_archived: true,
        }
    }
}

impl SearchFilter {
    /// Returns `true` if the given experience passes all filter criteria.
    pub fn matches(&self, experience: &Experience) -> bool {
        // Check archived status
        if self.exclude_archived && experience.archived {
            return false;
        }

        // Check domain overlap
        if let Some(ref domains) = self.domains {
            let has_match = experience
                .domain
                .iter()
                .any(|d| domains.iter().any(|f| f == d));
            if !has_match {
                return false;
            }
        }

        // Check experience type (compare by discriminant tag, not associated data)
        if let Some(ref types) = self.experience_types {
            let exp_tag = experience.experience_type.type_tag();
            let has_match = types.iter().any(|t| t.type_tag() == exp_tag);
            if !has_match {
                return false;
            }
        }

        // Check importance threshold
        if let Some(min) = self.min_importance {
            if experience.importance < min {
                return false;
            }
        }

        // Check confidence threshold
        if let Some(min) = self.min_confidence {
            if experience.confidence < min {
                return false;
            }
        }

        // Check timestamp
        if let Some(since) = self.since {
            if experience.timestamp < since {
                return false;
            }
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{AgentId, CollectiveId, ExperienceId};

    /// Helper to create a minimal test experience.
    fn test_experience() -> Experience {
        let timestamp = Timestamp::now();
        Experience {
            id: ExperienceId::new(),
            collective_id: CollectiveId::new(),
            content: "test content".to_string(),
            embedding: vec![0.1; 384],
            experience_type: ExperienceType::Fact {
                statement: "test".to_string(),
                source: String::new(),
            },
            importance: 0.5,
            confidence: 0.8,
            applications: std::collections::BTreeMap::new(),
            domain: vec!["rust".to_string(), "testing".to_string()],
            related_files: vec![],
            source_agent: AgentId::new("agent-1"),
            source_task: None,
            timestamp,
            last_reinforced: timestamp,
            archived: false,
        }
    }

    #[test]
    fn test_default_filter_excludes_archived() {
        let filter = SearchFilter::default();
        assert!(filter.exclude_archived);

        let mut exp = test_experience();
        assert!(filter.matches(&exp));

        exp.archived = true;
        assert!(!filter.matches(&exp));
    }

    #[test]
    fn test_default_filter_matches_all_non_archived() {
        let filter = SearchFilter::default();
        let exp = test_experience();
        assert!(filter.matches(&exp));
    }

    #[test]
    fn test_domain_filter() {
        let filter = SearchFilter {
            domains: Some(vec!["rust".to_string()]),
            ..SearchFilter::default()
        };

        let exp = test_experience(); // has domain: ["rust", "testing"]
        assert!(filter.matches(&exp));

        let filter_no_match = SearchFilter {
            domains: Some(vec!["python".to_string()]),
            ..SearchFilter::default()
        };
        assert!(!filter_no_match.matches(&exp));
    }

    #[test]
    fn test_experience_type_filter() {
        let filter = SearchFilter {
            experience_types: Some(vec![ExperienceType::Fact {
                statement: String::new(),
                source: String::new(),
            }]),
            ..SearchFilter::default()
        };

        let exp = test_experience(); // Fact type
        assert!(filter.matches(&exp));

        let filter_no_match = SearchFilter {
            experience_types: Some(vec![ExperienceType::Generic { category: None }]),
            ..SearchFilter::default()
        };
        assert!(!filter_no_match.matches(&exp));
    }

    #[test]
    fn test_importance_filter() {
        let filter = SearchFilter {
            min_importance: Some(0.7),
            ..SearchFilter::default()
        };

        let mut exp = test_experience(); // importance: 0.5
        assert!(!filter.matches(&exp));

        exp.importance = 0.8;
        assert!(filter.matches(&exp));
    }

    #[test]
    fn test_confidence_filter() {
        let filter = SearchFilter {
            min_confidence: Some(0.9),
            ..SearchFilter::default()
        };

        let exp = test_experience(); // confidence: 0.8
        assert!(!filter.matches(&exp));
    }

    #[test]
    fn test_since_filter() {
        let before = Timestamp::now();
        let exp = test_experience();

        // Filter for experiences after "before" — exp was created after, should match
        let filter = SearchFilter {
            since: Some(before),
            ..SearchFilter::default()
        };
        assert!(filter.matches(&exp));
    }

    #[test]
    fn test_combined_filters() {
        let filter = SearchFilter {
            domains: Some(vec!["rust".to_string()]),
            min_importance: Some(0.3),
            min_confidence: Some(0.5),
            exclude_archived: true,
            ..SearchFilter::default()
        };

        let exp = test_experience(); // domain: ["rust", "testing"], importance: 0.5, confidence: 0.8
        assert!(filter.matches(&exp));
    }
}
