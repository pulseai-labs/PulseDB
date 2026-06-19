//! Input validation for experiences.
//!
//! Validates [`NewExperience`] and [`ExperienceUpdate`] fields before
//! they reach the storage layer. All size/count constraints are defined
//! as constants in [`crate::storage::schema`].
//!
//! # Validation Layers
//!
//! ```text
//! PulseDB::record_experience()
//!     ├── validate_new_experience()      ← top-level fields
//!     │       └── validate_experience_type()  ← variant-specific fields
//!     └── storage.save_experience()      ← only reached if valid
//! ```

use crate::error::{PulseDBError, ValidationError};
use crate::experience::types::{ExperienceType, ExperienceUpdate, NewExperience};
use crate::storage::schema::{
    MAX_CONTENT_SIZE, MAX_DOMAIN_TAGS, MAX_FILE_PATH_LENGTH, MAX_SOURCE_AGENT_LENGTH,
    MAX_SOURCE_FILES, MAX_TAG_LENGTH,
};

/// Validates a [`NewExperience`] before storage.
///
/// # Rules
///
/// | Field | Constraint |
/// |-------|------------|
/// | `content` | Non-empty, max 100 KB |
/// | `importance` | 0.0–1.0 |
/// | `confidence` | 0.0–1.0 |
/// | `domain` | Max 50 tags, each max 100 chars |
/// | `related_files` | Max 100 paths, each max 500 chars |
/// | `embedding` | Required if `is_external_provider`; dimension must match collective |
/// | `source_agent` | Non-empty, max 256 chars |
/// | `experience_type` | Variant-specific field validation (quality, strength) |
pub(crate) fn validate_new_experience(
    exp: &NewExperience,
    collective_dimension: u16,
    is_external_provider: bool,
) -> Result<(), PulseDBError> {
    // Content: non-empty
    if exp.content.is_empty() {
        return Err(ValidationError::required_field("content").into());
    }

    // Content: max size
    if exp.content.len() > MAX_CONTENT_SIZE {
        return Err(ValidationError::content_too_large(exp.content.len(), MAX_CONTENT_SIZE).into());
    }

    // Importance: 0.0–1.0
    if !(0.0..=1.0).contains(&exp.importance) {
        return Err(ValidationError::invalid_field(
            "importance",
            format!("must be between 0.0 and 1.0, got {}", exp.importance),
        )
        .into());
    }

    // Confidence: 0.0–1.0
    if !(0.0..=1.0).contains(&exp.confidence) {
        return Err(ValidationError::invalid_field(
            "confidence",
            format!("must be between 0.0 and 1.0, got {}", exp.confidence),
        )
        .into());
    }

    // Domain tags: count limit
    if exp.domain.len() > MAX_DOMAIN_TAGS {
        return Err(
            ValidationError::too_many_items("domain", exp.domain.len(), MAX_DOMAIN_TAGS).into(),
        );
    }

    // Domain tags: individual length limit
    for (i, tag) in exp.domain.iter().enumerate() {
        if tag.len() > MAX_TAG_LENGTH {
            return Err(ValidationError::invalid_field(
                "domain",
                format!(
                    "tag at index {} exceeds max length of {} chars (got {})",
                    i,
                    MAX_TAG_LENGTH,
                    tag.len()
                ),
            )
            .into());
        }
    }

    // Related files: count limit
    if exp.related_files.len() > MAX_SOURCE_FILES {
        return Err(ValidationError::too_many_items(
            "related_files",
            exp.related_files.len(),
            MAX_SOURCE_FILES,
        )
        .into());
    }

    // Related files: individual length limit
    for (i, path) in exp.related_files.iter().enumerate() {
        if path.len() > MAX_FILE_PATH_LENGTH {
            return Err(ValidationError::invalid_field(
                "related_files",
                format!(
                    "path at index {} exceeds max length of {} chars (got {})",
                    i,
                    MAX_FILE_PATH_LENGTH,
                    path.len()
                ),
            )
            .into());
        }
    }

    // Embedding: required for external provider
    if is_external_provider && exp.embedding.is_none() {
        return Err(ValidationError::required_field(
            "embedding (required when using External embedding provider)",
        )
        .into());
    }

    // Embedding: dimension check
    if let Some(ref emb) = exp.embedding {
        if emb.len() != collective_dimension as usize {
            return Err(ValidationError::dimension_mismatch(
                collective_dimension as usize,
                emb.len(),
            )
            .into());
        }
    }

    // Source agent: non-empty
    if exp.source_agent.as_str().is_empty() {
        return Err(ValidationError::required_field("source_agent").into());
    }

    // Source agent: max length
    if exp.source_agent.as_str().len() > MAX_SOURCE_AGENT_LENGTH {
        return Err(ValidationError::invalid_field(
            "source_agent",
            format!(
                "exceeds max length of {} chars (got {})",
                MAX_SOURCE_AGENT_LENGTH,
                exp.source_agent.as_str().len()
            ),
        )
        .into());
    }

    // Experience type: variant-specific validation
    validate_experience_type(&exp.experience_type)?;

    Ok(())
}

/// Validates an [`ExperienceUpdate`] before applying.
///
/// Only validates fields that are `Some(...)`.
pub(crate) fn validate_experience_update(update: &ExperienceUpdate) -> Result<(), PulseDBError> {
    // Importance: 0.0–1.0
    if let Some(importance) = update.importance {
        if !(0.0..=1.0).contains(&importance) {
            return Err(ValidationError::invalid_field(
                "importance",
                format!("must be between 0.0 and 1.0, got {}", importance),
            )
            .into());
        }
    }

    // Confidence: 0.0–1.0
    if let Some(confidence) = update.confidence {
        if !(0.0..=1.0).contains(&confidence) {
            return Err(ValidationError::invalid_field(
                "confidence",
                format!("must be between 0.0 and 1.0, got {}", confidence),
            )
            .into());
        }
    }

    // Domain tags
    if let Some(ref domain) = update.domain {
        if domain.len() > MAX_DOMAIN_TAGS {
            return Err(
                ValidationError::too_many_items("domain", domain.len(), MAX_DOMAIN_TAGS).into(),
            );
        }
        for (i, tag) in domain.iter().enumerate() {
            if tag.len() > MAX_TAG_LENGTH {
                return Err(ValidationError::invalid_field(
                    "domain",
                    format!(
                        "tag at index {} exceeds max length of {} chars (got {})",
                        i,
                        MAX_TAG_LENGTH,
                        tag.len()
                    ),
                )
                .into());
            }
        }
    }

    // Related files
    if let Some(ref files) = update.related_files {
        if files.len() > MAX_SOURCE_FILES {
            return Err(ValidationError::too_many_items(
                "related_files",
                files.len(),
                MAX_SOURCE_FILES,
            )
            .into());
        }
        for (i, path) in files.iter().enumerate() {
            if path.len() > MAX_FILE_PATH_LENGTH {
                return Err(ValidationError::invalid_field(
                    "related_files",
                    format!(
                        "path at index {} exceeds max length of {} chars (got {})",
                        i,
                        MAX_FILE_PATH_LENGTH,
                        path.len()
                    ),
                )
                .into());
            }
        }
    }

    Ok(())
}

/// Validates variant-specific fields of an [`ExperienceType`].
///
/// Currently validates:
/// - [`ExperienceType::SuccessPattern::quality`]: must be 0.0–1.0
/// - [`ExperienceType::UserPreference::strength`]: must be 0.0–1.0
///
/// Other variants have no additional numeric constraints beyond what
/// Rust's type system enforces.
fn validate_experience_type(et: &ExperienceType) -> Result<(), PulseDBError> {
    match et {
        ExperienceType::SuccessPattern { quality, .. } if !(0.0..=1.0).contains(quality) => {
            return Err(ValidationError::invalid_field(
                "experience_type.quality",
                format!("must be between 0.0 and 1.0, got {}", quality),
            )
            .into());
        }
        ExperienceType::UserPreference { strength, .. } if !(0.0..=1.0).contains(strength) => {
            return Err(ValidationError::invalid_field(
                "experience_type.strength",
                format!("must be between 0.0 and 1.0, got {}", strength),
            )
            .into());
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::schema::{
        MAX_CONTENT_SIZE, MAX_FILE_PATH_LENGTH, MAX_SOURCE_AGENT_LENGTH, MAX_TAG_LENGTH,
    };
    use crate::types::{AgentId, CollectiveId};

    fn valid_new_experience() -> NewExperience {
        NewExperience {
            collective_id: CollectiveId::new(),
            content: "Test experience content".into(),
            experience_type: ExperienceType::default(),
            embedding: Some(vec![0.1; 384]),
            importance: 0.5,
            confidence: 0.5,
            domain: vec!["rust".into()],
            related_files: vec!["src/main.rs".into()],
            source_agent: AgentId::new("agent-1"),
            source_task: None,
        }
    }

    // ====================================================================
    // validate_new_experience — moved from mod.rs
    // ====================================================================

    #[test]
    fn test_valid_experience_passes() {
        assert!(validate_new_experience(&valid_new_experience(), 384, true).is_ok());
    }

    #[test]
    fn test_empty_content_rejected() {
        let mut exp = valid_new_experience();
        exp.content = String::new();
        let err = validate_new_experience(&exp, 384, true).unwrap_err();
        assert!(err.is_validation());
    }

    #[test]
    fn test_content_too_large_rejected() {
        let mut exp = valid_new_experience();
        exp.content = "x".repeat(MAX_CONTENT_SIZE + 1);
        let err = validate_new_experience(&exp, 384, true).unwrap_err();
        assert!(err.is_validation());
    }

    #[test]
    fn test_importance_negative_rejected() {
        let mut exp = valid_new_experience();
        exp.importance = -0.1;
        let err = validate_new_experience(&exp, 384, true).unwrap_err();
        assert!(err.is_validation());
    }

    #[test]
    fn test_importance_above_one_rejected() {
        let mut exp = valid_new_experience();
        exp.importance = 1.1;
        let err = validate_new_experience(&exp, 384, true).unwrap_err();
        assert!(err.is_validation());
    }

    #[test]
    fn test_confidence_out_of_range_rejected() {
        let mut exp = valid_new_experience();
        exp.confidence = -0.5;
        assert!(validate_new_experience(&exp, 384, true).is_err());

        exp.confidence = 2.0;
        assert!(validate_new_experience(&exp, 384, true).is_err());
    }

    #[test]
    fn test_too_many_domain_tags_rejected() {
        let mut exp = valid_new_experience();
        exp.domain = (0..MAX_DOMAIN_TAGS + 1)
            .map(|i| format!("tag-{}", i))
            .collect();
        let err = validate_new_experience(&exp, 384, true).unwrap_err();
        assert!(err.is_validation());
    }

    #[test]
    fn test_domain_tag_too_long_rejected() {
        let mut exp = valid_new_experience();
        exp.domain = vec!["x".repeat(MAX_TAG_LENGTH + 1)];
        let err = validate_new_experience(&exp, 384, true).unwrap_err();
        assert!(err.is_validation());
    }

    #[test]
    fn test_too_many_related_files_rejected() {
        let mut exp = valid_new_experience();
        exp.related_files = (0..MAX_SOURCE_FILES + 1)
            .map(|i| format!("file-{}.rs", i))
            .collect();
        let err = validate_new_experience(&exp, 384, true).unwrap_err();
        assert!(err.is_validation());
    }

    #[test]
    fn test_file_path_too_long_rejected() {
        let mut exp = valid_new_experience();
        exp.related_files = vec!["x".repeat(MAX_FILE_PATH_LENGTH + 1)];
        let err = validate_new_experience(&exp, 384, true).unwrap_err();
        assert!(err.is_validation());
    }

    #[test]
    fn test_embedding_required_for_external_provider() {
        let mut exp = valid_new_experience();
        exp.embedding = None;
        let err = validate_new_experience(&exp, 384, true).unwrap_err();
        assert!(err.is_validation());
    }

    #[test]
    fn test_embedding_optional_for_builtin_provider() {
        let mut exp = valid_new_experience();
        exp.embedding = None;
        assert!(validate_new_experience(&exp, 384, false).is_ok());
    }

    #[test]
    fn test_embedding_dimension_mismatch_rejected() {
        let mut exp = valid_new_experience();
        exp.embedding = Some(vec![0.1; 768]); // Expect 384
        let err = validate_new_experience(&exp, 384, true).unwrap_err();
        assert!(err.is_validation());
    }

    #[test]
    fn test_empty_source_agent_rejected() {
        let mut exp = valid_new_experience();
        exp.source_agent = AgentId::new("");
        let err = validate_new_experience(&exp, 384, true).unwrap_err();
        assert!(err.is_validation());
    }

    // ====================================================================
    // validate_experience_update — moved from mod.rs
    // ====================================================================

    #[test]
    fn test_empty_update_passes() {
        assert!(validate_experience_update(&ExperienceUpdate::default()).is_ok());
    }

    #[test]
    fn test_update_valid_importance_passes() {
        let update = ExperienceUpdate {
            importance: Some(0.9),
            ..Default::default()
        };
        assert!(validate_experience_update(&update).is_ok());
    }

    #[test]
    fn test_update_invalid_importance_rejected() {
        let update = ExperienceUpdate {
            importance: Some(1.5),
            ..Default::default()
        };
        assert!(validate_experience_update(&update).is_err());
    }

    #[test]
    fn test_update_invalid_confidence_rejected() {
        let update = ExperienceUpdate {
            confidence: Some(-0.1),
            ..Default::default()
        };
        assert!(validate_experience_update(&update).is_err());
    }

    #[test]
    fn test_update_too_many_domain_tags_rejected() {
        let update = ExperienceUpdate {
            domain: Some(
                (0..MAX_DOMAIN_TAGS + 1)
                    .map(|i| format!("tag-{}", i))
                    .collect(),
            ),
            ..Default::default()
        };
        assert!(validate_experience_update(&update).is_err());
    }

    #[test]
    fn test_update_domain_tag_too_long_rejected() {
        let update = ExperienceUpdate {
            domain: Some(vec!["x".repeat(MAX_TAG_LENGTH + 1)]),
            ..Default::default()
        };
        assert!(validate_experience_update(&update).is_err());
    }

    // ====================================================================
    // NEW: Boundary tests — exactly at limits
    // ====================================================================

    #[test]
    fn test_importance_exactly_zero_passes() {
        let mut exp = valid_new_experience();
        exp.importance = 0.0;
        assert!(validate_new_experience(&exp, 384, true).is_ok());
    }

    #[test]
    fn test_importance_exactly_one_passes() {
        let mut exp = valid_new_experience();
        exp.importance = 1.0;
        assert!(validate_new_experience(&exp, 384, true).is_ok());
    }

    #[test]
    fn test_confidence_exactly_zero_passes() {
        let mut exp = valid_new_experience();
        exp.confidence = 0.0;
        assert!(validate_new_experience(&exp, 384, true).is_ok());
    }

    #[test]
    fn test_confidence_exactly_one_passes() {
        let mut exp = valid_new_experience();
        exp.confidence = 1.0;
        assert!(validate_new_experience(&exp, 384, true).is_ok());
    }

    #[test]
    fn test_content_exactly_at_max_size_passes() {
        let mut exp = valid_new_experience();
        exp.content = "x".repeat(MAX_CONTENT_SIZE);
        assert!(validate_new_experience(&exp, 384, true).is_ok());
    }

    #[test]
    fn test_exactly_max_domain_tags_passes() {
        let mut exp = valid_new_experience();
        exp.domain = (0..MAX_DOMAIN_TAGS).map(|i| format!("tag-{}", i)).collect();
        assert!(validate_new_experience(&exp, 384, true).is_ok());
    }

    #[test]
    fn test_exactly_max_source_files_passes() {
        let mut exp = valid_new_experience();
        exp.related_files = (0..MAX_SOURCE_FILES)
            .map(|i| format!("file-{}.rs", i))
            .collect();
        assert!(validate_new_experience(&exp, 384, true).is_ok());
    }

    #[test]
    fn test_domain_tag_exactly_at_max_length_passes() {
        let mut exp = valid_new_experience();
        exp.domain = vec!["x".repeat(MAX_TAG_LENGTH)];
        assert!(validate_new_experience(&exp, 384, true).is_ok());
    }

    #[test]
    fn test_file_path_exactly_at_max_length_passes() {
        let mut exp = valid_new_experience();
        exp.related_files = vec!["x".repeat(MAX_FILE_PATH_LENGTH)];
        assert!(validate_new_experience(&exp, 384, true).is_ok());
    }

    // ====================================================================
    // NEW: source_agent max length
    // ====================================================================

    #[test]
    fn test_source_agent_exactly_max_length_passes() {
        let mut exp = valid_new_experience();
        exp.source_agent = AgentId::new("a".repeat(MAX_SOURCE_AGENT_LENGTH));
        assert!(validate_new_experience(&exp, 384, true).is_ok());
    }

    #[test]
    fn test_source_agent_too_long_rejected() {
        let mut exp = valid_new_experience();
        exp.source_agent = AgentId::new("a".repeat(MAX_SOURCE_AGENT_LENGTH + 1));
        let err = validate_new_experience(&exp, 384, true).unwrap_err();
        assert!(err.is_validation());
    }

    #[test]
    fn test_source_agent_too_long_error_message() {
        let mut exp = valid_new_experience();
        exp.source_agent = AgentId::new("a".repeat(MAX_SOURCE_AGENT_LENGTH + 1));
        let err = validate_new_experience(&exp, 384, true).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("source_agent"),
            "Error should mention field name: {}",
            msg
        );
        assert!(
            msg.contains("256"),
            "Error should mention max length: {}",
            msg
        );
    }

    // ====================================================================
    // NEW: ExperienceType variant validation
    // ====================================================================

    #[test]
    fn test_success_pattern_valid_quality_passes() {
        let mut exp = valid_new_experience();
        exp.experience_type = ExperienceType::SuccessPattern {
            task_type: "refactoring".into(),
            approach: "extract method".into(),
            quality: 0.95,
        };
        assert!(validate_new_experience(&exp, 384, true).is_ok());
    }

    #[test]
    fn test_success_pattern_quality_zero_passes() {
        let mut exp = valid_new_experience();
        exp.experience_type = ExperienceType::SuccessPattern {
            task_type: "test".into(),
            approach: "test".into(),
            quality: 0.0,
        };
        assert!(validate_new_experience(&exp, 384, true).is_ok());
    }

    #[test]
    fn test_success_pattern_quality_one_passes() {
        let mut exp = valid_new_experience();
        exp.experience_type = ExperienceType::SuccessPattern {
            task_type: "test".into(),
            approach: "test".into(),
            quality: 1.0,
        };
        assert!(validate_new_experience(&exp, 384, true).is_ok());
    }

    #[test]
    fn test_success_pattern_quality_above_one_rejected() {
        let mut exp = valid_new_experience();
        exp.experience_type = ExperienceType::SuccessPattern {
            task_type: "test".into(),
            approach: "test".into(),
            quality: 1.1,
        };
        let err = validate_new_experience(&exp, 384, true).unwrap_err();
        assert!(err.is_validation());
        assert!(err.to_string().contains("quality"));
    }

    #[test]
    fn test_success_pattern_quality_negative_rejected() {
        let mut exp = valid_new_experience();
        exp.experience_type = ExperienceType::SuccessPattern {
            task_type: "test".into(),
            approach: "test".into(),
            quality: -0.1,
        };
        assert!(validate_new_experience(&exp, 384, true).is_err());
    }

    #[test]
    fn test_user_preference_valid_strength_passes() {
        let mut exp = valid_new_experience();
        exp.experience_type = ExperienceType::UserPreference {
            category: "style".into(),
            preference: "dark mode".into(),
            strength: 0.8,
        };
        assert!(validate_new_experience(&exp, 384, true).is_ok());
    }

    #[test]
    fn test_user_preference_strength_above_one_rejected() {
        let mut exp = valid_new_experience();
        exp.experience_type = ExperienceType::UserPreference {
            category: "style".into(),
            preference: "dark mode".into(),
            strength: 1.5,
        };
        let err = validate_new_experience(&exp, 384, true).unwrap_err();
        assert!(err.is_validation());
        assert!(err.to_string().contains("strength"));
    }

    #[test]
    fn test_user_preference_strength_negative_rejected() {
        let mut exp = valid_new_experience();
        exp.experience_type = ExperienceType::UserPreference {
            category: "style".into(),
            preference: "dark mode".into(),
            strength: -0.5,
        };
        assert!(validate_new_experience(&exp, 384, true).is_err());
    }

    #[test]
    fn test_generic_experience_type_passes() {
        assert!(validate_experience_type(&ExperienceType::default()).is_ok());
    }

    #[test]
    fn test_difficulty_experience_type_passes() {
        assert!(validate_experience_type(&ExperienceType::Difficulty {
            description: "test".into(),
            severity: crate::experience::types::Severity::High,
        })
        .is_ok());
    }
}
