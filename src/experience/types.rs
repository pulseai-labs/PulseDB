//! Type definitions for experiences.
//!
//! An **experience** is the core data type in PulseDB — a unit of learned knowledge
//! that agents share through collectives. Each experience has content, an embedding
//! vector for semantic search, a rich type, and metadata.
//!
//! # Type Hierarchy
//!
//! ```text
//! ExperienceType (rich, with associated data)
//!     ↓ type_tag()
//! ExperienceTypeTag (compact 1-byte discriminant for index keys)
//! ```

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::storage::schema::ExperienceTypeTag;
use crate::types::{AgentId, CollectiveId, ExperienceId, InstanceId, TaskId, Timestamp};

// ============================================================================
// Severity
// ============================================================================

/// Severity level for difficulty experiences.
///
/// Used as associated data in [`ExperienceType::Difficulty`] to indicate
/// how impactful a problem was.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Severity {
    /// Minor impact, easily worked around.
    Low,
    /// Noticeable impact, workaround available.
    Medium,
    /// Significant impact, blocks progress.
    High,
    /// Showstopper, must be resolved immediately.
    Critical,
}

// ============================================================================
// ExperienceType — Rich enum with 9 variants (ADR-004)
// ============================================================================

/// Rich experience type with associated data per variant.
///
/// This is the full type stored in the experience record. For index keys,
/// use [`type_tag()`](Self::type_tag) to get the compact
/// [`ExperienceTypeTag`] discriminant.
///
/// # Variants
///
/// Each variant carries structured data specific to that kind of experience:
/// - **Difficulty** — A problem the agent encountered
/// - **Solution** — A fix for a problem, optionally linked to a Difficulty
/// - **ErrorPattern** — A reusable error signature with fix and prevention
/// - **SuccessPattern** — A proven approach with quality rating
/// - **UserPreference** — A user preference with strength
/// - **ArchitecturalDecision** — A design decision with rationale
/// - **TechInsight** — Technical knowledge about a technology
/// - **Fact** — A verified factual statement with source
/// - **Generic** — Catch-all for uncategorized experiences
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ExperienceType {
    /// Problem encountered by the agent.
    Difficulty {
        /// What the problem is.
        description: String,
        /// How severe the problem is.
        severity: Severity,
    },

    /// Fix for a problem, optionally linked to a Difficulty experience.
    Solution {
        /// Reference to the Difficulty experience this solves, if any.
        problem_ref: Option<ExperienceId>,
        /// The approach taken to solve the problem.
        approach: String,
        /// Whether the solution worked.
        worked: bool,
    },

    /// Reusable error signature with fix and prevention strategy.
    ErrorPattern {
        /// The error signature (e.g., error code, message pattern).
        signature: String,
        /// How to fix occurrences of this error.
        fix: String,
        /// How to prevent this error from occurring.
        prevention: String,
    },

    /// Proven approach with quality rating (0.0–1.0).
    SuccessPattern {
        /// The type of task this pattern applies to.
        task_type: String,
        /// The approach that works.
        approach: String,
        /// Quality rating of the outcome (0.0–1.0).
        quality: f32,
    },

    /// User preference with strength (0.0–1.0).
    UserPreference {
        /// The preference category (e.g., "style", "tooling").
        category: String,
        /// The specific preference.
        preference: String,
        /// How strongly the user feels about this (0.0–1.0).
        strength: f32,
    },

    /// Design decision with rationale.
    ArchitecturalDecision {
        /// The decision made.
        decision: String,
        /// Why this decision was made.
        rationale: String,
    },

    /// Technical knowledge about a specific technology.
    TechInsight {
        /// The technology this insight is about.
        technology: String,
        /// The insight or knowledge.
        insight: String,
    },

    /// Verified factual statement with source attribution.
    Fact {
        /// The factual statement.
        statement: String,
        /// Where this fact was verified.
        source: String,
    },

    /// Catch-all for uncategorized experiences.
    Generic {
        /// Optional category label.
        category: Option<String>,
    },
}

impl ExperienceType {
    /// Returns the compact [`ExperienceTypeTag`] for use in index keys.
    ///
    /// This bridges the rich type (with data) to the 1-byte discriminant
    /// stored in secondary index keys.
    pub fn type_tag(&self) -> ExperienceTypeTag {
        match self {
            Self::Difficulty { .. } => ExperienceTypeTag::Difficulty,
            Self::Solution { .. } => ExperienceTypeTag::Solution,
            Self::ErrorPattern { .. } => ExperienceTypeTag::ErrorPattern,
            Self::SuccessPattern { .. } => ExperienceTypeTag::SuccessPattern,
            Self::UserPreference { .. } => ExperienceTypeTag::UserPreference,
            Self::ArchitecturalDecision { .. } => ExperienceTypeTag::ArchitecturalDecision,
            Self::TechInsight { .. } => ExperienceTypeTag::TechInsight,
            Self::Fact { .. } => ExperienceTypeTag::Fact,
            Self::Generic { .. } => ExperienceTypeTag::Generic,
        }
    }
}

impl Default for ExperienceType {
    fn default() -> Self {
        Self::Generic { category: None }
    }
}

// ============================================================================
// Experience — The full stored record
// ============================================================================

/// A stored experience — the core data type in PulseDB.
///
/// Experiences are agent-learned knowledge units stored in collectives.
/// Each experience has content, a semantic embedding for vector search,
/// a rich type, and metadata for filtering and ranking.
///
/// # Serialization Note
///
/// The `embedding` field is marked `#[serde(skip)]` because embeddings are
/// stored in a separate `EMBEDDINGS_TABLE` for performance. The storage
/// layer reconstitutes the full struct by joining both tables on read.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Experience {
    /// Unique identifier (UUID v7, time-ordered).
    pub id: ExperienceId,

    /// The collective this experience belongs to.
    pub collective_id: CollectiveId,

    /// The experience content (text). Immutable after creation.
    pub content: String,

    /// Semantic embedding vector. Immutable after creation.
    ///
    /// Stored separately in EMBEDDINGS_TABLE; skipped during bincode
    /// serialization of the main experience record.
    #[serde(skip)]
    pub embedding: Vec<f32>,

    /// Rich experience type with associated data.
    pub experience_type: ExperienceType,

    /// Importance score (0.0–1.0). Higher = more important.
    pub importance: f32,

    /// Confidence score (0.0–1.0). Higher = more confident.
    pub confidence: f32,

    /// Per-instance application/reinforcement counters.
    ///
    /// The total application count is available via [`applications()`](Self::applications).
    pub applications: BTreeMap<InstanceId, u32>,

    /// Domain tags for categorical filtering (e.g., ["rust", "async"]).
    pub domain: Vec<String>,

    /// Related source file paths.
    pub related_files: Vec<String>,

    /// The agent that created this experience.
    pub source_agent: AgentId,

    /// Optional task context where this experience was created.
    pub source_task: Option<TaskId>,

    /// When this experience was recorded.
    pub timestamp: Timestamp,

    /// Last time this experience was explicitly reinforced.
    pub last_reinforced: Timestamp,

    /// Whether this experience is archived (soft-deleted).
    ///
    /// Archived experiences are excluded from search results but remain
    /// in storage and can be restored via `unarchive_experience()`.
    pub archived: bool,
}

impl Experience {
    /// Returns the total application count across all instance buckets.
    pub fn applications(&self) -> u32 {
        self.applications
            .values()
            .copied()
            .fold(0u32, u32::saturating_add)
    }
}

// ============================================================================
// NewExperience — Input for record_experience()
// ============================================================================

/// Input for creating a new experience via [`PulseDB::record_experience()`](crate::PulseDB).
///
/// Only the mutable fields are set here. The `id`, `timestamp`, `last_reinforced`,
/// `applications`, and `archived` fields are set automatically by the storage layer.
///
/// # Embedding
///
/// - **External provider**: `embedding` is required (must be `Some`)
/// - **Builtin provider**: `embedding` is optional; if `None`, PulseDB generates it
#[derive(Clone, Debug)]
pub struct NewExperience {
    /// The collective to store this experience in.
    pub collective_id: CollectiveId,

    /// The experience content (text).
    pub content: String,

    /// Rich experience type.
    pub experience_type: ExperienceType,

    /// Pre-computed embedding vector. Required for External provider.
    pub embedding: Option<Vec<f32>>,

    /// Importance score (0.0–1.0).
    pub importance: f32,

    /// Confidence score (0.0–1.0).
    pub confidence: f32,

    /// Domain tags for categorical filtering.
    pub domain: Vec<String>,

    /// Related source file paths.
    pub related_files: Vec<String>,

    /// The agent creating this experience.
    pub source_agent: AgentId,

    /// Optional task context.
    pub source_task: Option<TaskId>,
}

impl Default for NewExperience {
    fn default() -> Self {
        Self {
            collective_id: CollectiveId::nil(),
            content: String::new(),
            experience_type: ExperienceType::default(),
            embedding: None,
            importance: 0.5,
            confidence: 0.5,
            domain: Vec::new(),
            related_files: Vec::new(),
            source_agent: AgentId::new("anonymous"),
            source_task: None,
        }
    }
}

// ============================================================================
// ExperienceUpdate — Partial update for mutable fields
// ============================================================================

/// Partial update for an experience's mutable fields.
///
/// Only fields set to `Some(...)` will be updated. Content and embedding
/// are immutable — create a new experience if content changes.
#[derive(Clone, Debug, Default)]
pub struct ExperienceUpdate {
    /// New importance score (0.0–1.0).
    pub importance: Option<f32>,

    /// New confidence score (0.0–1.0).
    pub confidence: Option<f32>,

    /// Replace domain tags entirely.
    pub domain: Option<Vec<String>>,

    /// Replace related files entirely.
    pub related_files: Option<Vec<String>>,

    /// Set archived status (used internally by archive/unarchive).
    pub archived: Option<bool>,
}

#[cfg(test)]
mod tests {
    use super::*;

    // ====================================================================
    // Severity tests
    // ====================================================================

    #[test]
    fn test_severity_bincode_roundtrip() {
        for severity in [
            Severity::Low,
            Severity::Medium,
            Severity::High,
            Severity::Critical,
        ] {
            let bytes = bincode::serialize(&severity).unwrap();
            let restored: Severity = bincode::deserialize(&bytes).unwrap();
            assert_eq!(severity, restored);
        }
    }

    // ====================================================================
    // ExperienceType tests
    // ====================================================================

    #[test]
    fn test_experience_type_default() {
        let et = ExperienceType::default();
        assert!(matches!(et, ExperienceType::Generic { category: None }));
    }

    #[test]
    fn test_experience_type_tag_mapping() {
        let cases: Vec<(ExperienceType, ExperienceTypeTag)> = vec![
            (
                ExperienceType::Difficulty {
                    description: "test".into(),
                    severity: Severity::High,
                },
                ExperienceTypeTag::Difficulty,
            ),
            (
                ExperienceType::Solution {
                    problem_ref: None,
                    approach: "test".into(),
                    worked: true,
                },
                ExperienceTypeTag::Solution,
            ),
            (
                ExperienceType::ErrorPattern {
                    signature: "test".into(),
                    fix: "test".into(),
                    prevention: "test".into(),
                },
                ExperienceTypeTag::ErrorPattern,
            ),
            (
                ExperienceType::SuccessPattern {
                    task_type: "test".into(),
                    approach: "test".into(),
                    quality: 0.9,
                },
                ExperienceTypeTag::SuccessPattern,
            ),
            (
                ExperienceType::UserPreference {
                    category: "test".into(),
                    preference: "test".into(),
                    strength: 0.8,
                },
                ExperienceTypeTag::UserPreference,
            ),
            (
                ExperienceType::ArchitecturalDecision {
                    decision: "test".into(),
                    rationale: "test".into(),
                },
                ExperienceTypeTag::ArchitecturalDecision,
            ),
            (
                ExperienceType::TechInsight {
                    technology: "test".into(),
                    insight: "test".into(),
                },
                ExperienceTypeTag::TechInsight,
            ),
            (
                ExperienceType::Fact {
                    statement: "test".into(),
                    source: "test".into(),
                },
                ExperienceTypeTag::Fact,
            ),
            (
                ExperienceType::Generic {
                    category: Some("test".into()),
                },
                ExperienceTypeTag::Generic,
            ),
        ];

        for (experience_type, expected_tag) in cases {
            assert_eq!(
                experience_type.type_tag(),
                expected_tag,
                "Tag mismatch for {:?}",
                experience_type,
            );
        }
    }

    #[test]
    fn test_experience_type_bincode_roundtrip_all_variants() {
        let variants = vec![
            ExperienceType::Difficulty {
                description: "compile error".into(),
                severity: Severity::High,
            },
            ExperienceType::Solution {
                problem_ref: Some(ExperienceId::new()),
                approach: "added lifetime annotation".into(),
                worked: true,
            },
            ExperienceType::ErrorPattern {
                signature: "E0308 mismatched types".into(),
                fix: "check return type".into(),
                prevention: "use clippy".into(),
            },
            ExperienceType::SuccessPattern {
                task_type: "refactoring".into(),
                approach: "extract method".into(),
                quality: 0.95,
            },
            ExperienceType::UserPreference {
                category: "style".into(),
                preference: "snake_case".into(),
                strength: 0.9,
            },
            ExperienceType::ArchitecturalDecision {
                decision: "use redb over SQLite".into(),
                rationale: "pure Rust, ACID, no FFI".into(),
            },
            ExperienceType::TechInsight {
                technology: "tokio".into(),
                insight: "spawn_blocking for CPU-bound work".into(),
            },
            ExperienceType::Fact {
                statement: "redb uses shadow paging".into(),
                source: "redb docs".into(),
            },
            ExperienceType::Generic { category: None },
        ];

        for variant in variants {
            let bytes = bincode::serialize(&variant).unwrap();
            let restored: ExperienceType = bincode::deserialize(&bytes).unwrap();
            // Compare tags as a proxy (associated data is different types per variant)
            assert_eq!(variant.type_tag(), restored.type_tag());
        }
    }

    // ====================================================================
    // Experience tests
    // ====================================================================

    #[test]
    fn test_experience_bincode_roundtrip() {
        let timestamp = Timestamp::now();
        let exp = Experience {
            id: ExperienceId::new(),
            collective_id: CollectiveId::new(),
            content: "Test experience content".into(),
            embedding: vec![0.1, 0.2, 0.3], // will be skipped by serde
            experience_type: ExperienceType::Fact {
                statement: "Rust is memory-safe".into(),
                source: "docs".into(),
            },
            importance: 0.8,
            confidence: 0.9,
            applications: BTreeMap::from([(InstanceId::new(), 5)]),
            domain: vec!["rust".into(), "safety".into()],
            related_files: vec!["src/main.rs".into()],
            source_agent: AgentId::new("agent-1"),
            source_task: Some(TaskId::new("task-42")),
            timestamp,
            last_reinforced: timestamp,
            archived: false,
        };

        let bytes = bincode::serialize(&exp).unwrap();
        let restored: Experience = bincode::deserialize(&bytes).unwrap();

        assert_eq!(exp.id, restored.id);
        assert_eq!(exp.collective_id, restored.collective_id);
        assert_eq!(exp.content, restored.content);
        // Embedding is skipped — restored should be empty
        assert!(restored.embedding.is_empty());
        assert_eq!(
            exp.experience_type.type_tag(),
            restored.experience_type.type_tag()
        );
        assert_eq!(exp.importance, restored.importance);
        assert_eq!(exp.confidence, restored.confidence);
        assert_eq!(exp.applications, restored.applications);
        assert_eq!(exp.applications(), restored.applications());
        assert_eq!(exp.domain, restored.domain);
        assert_eq!(exp.related_files, restored.related_files);
        assert_eq!(exp.source_agent, restored.source_agent);
        assert_eq!(exp.source_task, restored.source_task);
        assert_eq!(exp.timestamp, restored.timestamp);
        assert_eq!(exp.archived, restored.archived);
    }

    #[test]
    fn test_experience_embedding_skipped_in_serialization() {
        let timestamp = Timestamp::now();
        let exp = Experience {
            id: ExperienceId::new(),
            collective_id: CollectiveId::new(),
            content: "test".into(),
            embedding: vec![1.0; 384], // 384 floats = 1,536 bytes
            experience_type: ExperienceType::default(),
            importance: 0.5,
            confidence: 0.5,
            applications: BTreeMap::new(),
            domain: vec![],
            related_files: vec![],
            source_agent: AgentId::new("a"),
            source_task: None,
            timestamp,
            last_reinforced: timestamp,
            archived: false,
        };

        let bytes = bincode::serialize(&exp).unwrap();
        // If embedding were included, size would be > 1,536 bytes.
        // With skip, it should be much smaller.
        assert!(
            bytes.len() < 500,
            "Serialized size {} suggests embedding was not skipped",
            bytes.len()
        );
    }

    // ====================================================================
    // NewExperience tests
    // ====================================================================

    #[test]
    fn test_new_experience_default() {
        let ne = NewExperience::default();
        assert_eq!(ne.collective_id, CollectiveId::nil());
        assert!(ne.content.is_empty());
        assert!(matches!(
            ne.experience_type,
            ExperienceType::Generic { category: None }
        ));
        assert!(ne.embedding.is_none());
        assert_eq!(ne.importance, 0.5);
        assert_eq!(ne.confidence, 0.5);
        assert!(ne.domain.is_empty());
        assert!(ne.related_files.is_empty());
        assert_eq!(ne.source_agent.as_str(), "anonymous");
        assert!(ne.source_task.is_none());
    }

    // ====================================================================
    // ExperienceUpdate tests
    // ====================================================================

    #[test]
    fn test_experience_update_default() {
        let update = ExperienceUpdate::default();
        assert!(update.importance.is_none());
        assert!(update.confidence.is_none());
        assert!(update.domain.is_none());
        assert!(update.related_files.is_none());
        assert!(update.archived.is_none());
    }
}
