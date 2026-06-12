//! Integration tests for experience CRUD operations (E1-S03).
//!
//! Tests the full stack: PulseDB facade → validation → StorageEngine → redb.
//! Uses External embedding provider (default), so all experiences must provide
//! pre-computed embeddings of the correct dimension (384 for D384).

use pulsedb::{
    AgentId, CollectiveId, Config, ExperienceId, ExperienceType, ExperienceUpdate, NewExperience,
    PulseDB, Severity,
};
use tempfile::tempdir;

/// Default embedding dimension for tests (D384).
const DIM: usize = 384;

/// Creates a dummy embedding of the correct dimension.
fn dummy_embedding() -> Vec<f32> {
    vec![0.1; DIM]
}

/// Helper to open a fresh database with default config (External provider, D384).
fn open_db() -> (PulseDB, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let db = PulseDB::open(&path, Config::default()).unwrap();
    (db, dir)
}

/// Helper: open DB, create a collective, return both IDs.
fn open_db_with_collective() -> (PulseDB, CollectiveId, tempfile::TempDir) {
    let (db, dir) = open_db();
    let cid = db.create_collective("test-collective").unwrap();
    (db, cid, dir)
}

/// Helper to build a minimal valid NewExperience for a given collective.
fn minimal_experience(collective_id: CollectiveId) -> NewExperience {
    NewExperience {
        collective_id,
        content: "Always validate user input before processing".to_string(),
        embedding: Some(dummy_embedding()),
        ..Default::default()
    }
}

// ============================================================================
// Record + Get Roundtrip
// ============================================================================

#[test]
fn test_record_and_get_experience() {
    let (db, cid, _dir) = open_db_with_collective();

    let id = db
        .record_experience(NewExperience {
            collective_id: cid,
            content: "Use Arc for shared ownership across threads".to_string(),
            experience_type: ExperienceType::TechInsight {
                technology: "rust".into(),
                insight: "Arc enables shared ownership".into(),
            },
            embedding: Some(dummy_embedding()),
            importance: 0.9,
            confidence: 0.85,
            domain: vec!["rust".into(), "concurrency".into()],
            related_files: vec!["src/main.rs".into()],
            ..Default::default()
        })
        .unwrap();

    let exp = db.get_experience(id).unwrap().unwrap();

    assert_eq!(exp.id, id);
    assert_eq!(exp.collective_id, cid);
    assert_eq!(exp.content, "Use Arc for shared ownership across threads");
    assert_eq!(exp.embedding.len(), DIM);
    assert_eq!(exp.importance, 0.9);
    assert_eq!(exp.confidence, 0.85);
    assert_eq!(exp.applications(), 0);
    assert_eq!(exp.domain, vec!["rust", "concurrency"]);
    assert_eq!(exp.related_files, vec!["src/main.rs"]);
    assert!(!exp.archived);
    assert!(
        matches!(&exp.experience_type, ExperienceType::TechInsight { technology, .. } if technology == "rust")
    );

    db.close().unwrap();
}

#[test]
fn test_record_experience_all_type_variants() {
    let (db, cid, _dir) = open_db_with_collective();

    let types: Vec<ExperienceType> = vec![
        ExperienceType::Difficulty {
            description: "borrow checker error".into(),
            severity: Severity::High,
        },
        ExperienceType::Solution {
            problem_ref: None,
            approach: "clone the value".into(),
            worked: true,
        },
        ExperienceType::ErrorPattern {
            signature: "E0308".into(),
            fix: "check types".into(),
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
            decision: "use redb".into(),
            rationale: "pure Rust, ACID".into(),
        },
        ExperienceType::TechInsight {
            technology: "tokio".into(),
            insight: "spawn_blocking for CPU work".into(),
        },
        ExperienceType::Fact {
            statement: "redb uses shadow paging".into(),
            source: "redb docs".into(),
        },
        ExperienceType::Generic {
            category: Some("misc".into()),
        },
    ];

    let mut ids = Vec::new();
    for exp_type in &types {
        let id = db
            .record_experience(NewExperience {
                collective_id: cid,
                content: format!("Experience for {:?}", exp_type.type_tag()),
                experience_type: exp_type.clone(),
                embedding: Some(dummy_embedding()),
                ..Default::default()
            })
            .unwrap();
        ids.push(id);
    }

    // Verify each was stored with correct type tag
    for (i, id) in ids.iter().enumerate() {
        let exp = db.get_experience(*id).unwrap().unwrap();
        assert_eq!(
            exp.experience_type.type_tag(),
            types[i].type_tag(),
            "Type tag mismatch for variant {}",
            i
        );
    }

    db.close().unwrap();
}

#[test]
fn test_get_experience_nonexistent() {
    let (db, _dir) = open_db();

    let result = db.get_experience(ExperienceId::new()).unwrap();
    assert!(result.is_none());

    db.close().unwrap();
}

#[test]
fn test_record_experience_default_fields() {
    let (db, cid, _dir) = open_db_with_collective();

    let id = db.record_experience(minimal_experience(cid)).unwrap();

    let exp = db.get_experience(id).unwrap().unwrap();
    assert_eq!(exp.importance, 0.5); // default
    assert_eq!(exp.confidence, 0.5); // default
    assert_eq!(exp.applications(), 0);
    assert!(!exp.archived);
    assert!(matches!(
        exp.experience_type,
        ExperienceType::Generic { category: None }
    ));
    assert_eq!(exp.source_agent.as_str(), "anonymous");
    assert!(exp.source_task.is_none());

    db.close().unwrap();
}

// ============================================================================
// Update Experience
// ============================================================================

#[test]
fn test_update_experience_importance() {
    let (db, cid, _dir) = open_db_with_collective();

    let id = db.record_experience(minimal_experience(cid)).unwrap();

    db.update_experience(
        id,
        ExperienceUpdate {
            importance: Some(0.95),
            ..Default::default()
        },
    )
    .unwrap();

    let exp = db.get_experience(id).unwrap().unwrap();
    assert_eq!(exp.importance, 0.95);
    // Other fields unchanged
    assert_eq!(exp.confidence, 0.5);

    db.close().unwrap();
}

#[test]
fn test_update_experience_confidence() {
    let (db, cid, _dir) = open_db_with_collective();

    let id = db.record_experience(minimal_experience(cid)).unwrap();

    db.update_experience(
        id,
        ExperienceUpdate {
            confidence: Some(0.1),
            ..Default::default()
        },
    )
    .unwrap();

    let exp = db.get_experience(id).unwrap().unwrap();
    assert_eq!(exp.confidence, 0.1);

    db.close().unwrap();
}

#[test]
fn test_update_experience_domain() {
    let (db, cid, _dir) = open_db_with_collective();

    let id = db.record_experience(minimal_experience(cid)).unwrap();

    db.update_experience(
        id,
        ExperienceUpdate {
            domain: Some(vec!["rust".into(), "testing".into()]),
            ..Default::default()
        },
    )
    .unwrap();

    let exp = db.get_experience(id).unwrap().unwrap();
    assert_eq!(exp.domain, vec!["rust", "testing"]);

    db.close().unwrap();
}

#[test]
fn test_update_experience_related_files() {
    let (db, cid, _dir) = open_db_with_collective();

    let id = db.record_experience(minimal_experience(cid)).unwrap();

    db.update_experience(
        id,
        ExperienceUpdate {
            related_files: Some(vec!["src/lib.rs".into(), "tests/integration.rs".into()]),
            ..Default::default()
        },
    )
    .unwrap();

    let exp = db.get_experience(id).unwrap().unwrap();
    assert_eq!(
        exp.related_files,
        vec!["src/lib.rs", "tests/integration.rs"]
    );

    db.close().unwrap();
}

#[test]
fn test_update_experience_multiple_fields() {
    let (db, cid, _dir) = open_db_with_collective();

    let id = db.record_experience(minimal_experience(cid)).unwrap();

    db.update_experience(
        id,
        ExperienceUpdate {
            importance: Some(1.0),
            confidence: Some(0.99),
            domain: Some(vec!["critical".into()]),
            related_files: Some(vec!["deploy.yaml".into()]),
            ..Default::default()
        },
    )
    .unwrap();

    let exp = db.get_experience(id).unwrap().unwrap();
    assert_eq!(exp.importance, 1.0);
    assert_eq!(exp.confidence, 0.99);
    assert_eq!(exp.domain, vec!["critical"]);
    assert_eq!(exp.related_files, vec!["deploy.yaml"]);

    db.close().unwrap();
}

#[test]
fn test_update_experience_nonexistent() {
    let (db, _dir) = open_db();

    let result = db.update_experience(
        ExperienceId::new(),
        ExperienceUpdate {
            importance: Some(0.5),
            ..Default::default()
        },
    );

    assert!(result.is_err());
    assert!(result.unwrap_err().is_not_found());

    db.close().unwrap();
}

#[test]
fn test_update_preserves_content_and_embedding() {
    let (db, cid, _dir) = open_db_with_collective();

    let id = db
        .record_experience(NewExperience {
            collective_id: cid,
            content: "original content".to_string(),
            embedding: Some(vec![0.42; DIM]),
            ..Default::default()
        })
        .unwrap();

    // Update importance — content and embedding should be unchanged
    db.update_experience(
        id,
        ExperienceUpdate {
            importance: Some(0.99),
            ..Default::default()
        },
    )
    .unwrap();

    let exp = db.get_experience(id).unwrap().unwrap();
    assert_eq!(exp.content, "original content");
    assert_eq!(exp.embedding.len(), DIM);
    assert!((exp.embedding[0] - 0.42).abs() < f32::EPSILON);

    db.close().unwrap();
}

// ============================================================================
// Archive / Unarchive
// ============================================================================

#[test]
fn test_archive_experience() {
    let (db, cid, _dir) = open_db_with_collective();

    let id = db.record_experience(minimal_experience(cid)).unwrap();
    assert!(!db.get_experience(id).unwrap().unwrap().archived);

    db.archive_experience(id).unwrap();
    assert!(db.get_experience(id).unwrap().unwrap().archived);

    db.close().unwrap();
}

#[test]
fn test_unarchive_experience() {
    let (db, cid, _dir) = open_db_with_collective();

    let id = db.record_experience(minimal_experience(cid)).unwrap();

    db.archive_experience(id).unwrap();
    assert!(db.get_experience(id).unwrap().unwrap().archived);

    db.unarchive_experience(id).unwrap();
    assert!(!db.get_experience(id).unwrap().unwrap().archived);

    db.close().unwrap();
}

#[test]
fn test_archive_nonexistent() {
    let (db, _dir) = open_db();

    let result = db.archive_experience(ExperienceId::new());
    assert!(result.is_err());
    assert!(result.unwrap_err().is_not_found());

    db.close().unwrap();
}

#[test]
fn test_archive_idempotent() {
    let (db, cid, _dir) = open_db_with_collective();

    let id = db.record_experience(minimal_experience(cid)).unwrap();

    // Archive twice — should succeed both times
    db.archive_experience(id).unwrap();
    db.archive_experience(id).unwrap();

    assert!(db.get_experience(id).unwrap().unwrap().archived);

    db.close().unwrap();
}

// ============================================================================
// Delete Experience
// ============================================================================

#[test]
fn test_delete_experience() {
    let (db, cid, _dir) = open_db_with_collective();

    let id = db.record_experience(minimal_experience(cid)).unwrap();
    assert!(db.get_experience(id).unwrap().is_some());

    db.delete_experience(id).unwrap();
    assert!(db.get_experience(id).unwrap().is_none());

    db.close().unwrap();
}

#[test]
fn test_delete_experience_nonexistent() {
    let (db, _dir) = open_db();

    let result = db.delete_experience(ExperienceId::new());
    assert!(result.is_err());
    assert!(result.unwrap_err().is_not_found());

    db.close().unwrap();
}

#[test]
fn test_delete_experience_removes_from_stats() {
    let (db, cid, _dir) = open_db_with_collective();

    let id = db.record_experience(minimal_experience(cid)).unwrap();
    assert_eq!(db.get_collective_stats(cid).unwrap().experience_count, 1);

    db.delete_experience(id).unwrap();
    assert_eq!(db.get_collective_stats(cid).unwrap().experience_count, 0);

    db.close().unwrap();
}

// ============================================================================
// Reinforce Experience
// ============================================================================

#[test]
fn test_reinforce_experience() {
    let (db, cid, _dir) = open_db_with_collective();

    let id = db.record_experience(minimal_experience(cid)).unwrap();
    assert_eq!(db.get_experience(id).unwrap().unwrap().applications(), 0);

    let count = db.reinforce_experience(id).unwrap();
    assert_eq!(count, 1);

    let count = db.reinforce_experience(id).unwrap();
    assert_eq!(count, 2);

    let count = db.reinforce_experience(id).unwrap();
    assert_eq!(count, 3);

    let exp = db.get_experience(id).unwrap().unwrap();
    assert_eq!(exp.applications(), 3);

    db.close().unwrap();
}

#[test]
fn test_reinforce_nonexistent() {
    let (db, _dir) = open_db();

    let result = db.reinforce_experience(ExperienceId::new());
    assert!(result.is_err());
    assert!(result.unwrap_err().is_not_found());

    db.close().unwrap();
}

#[test]
fn test_reinforce_preserves_other_fields() {
    let (db, cid, _dir) = open_db_with_collective();

    let id = db
        .record_experience(NewExperience {
            collective_id: cid,
            content: "reinforce test content".to_string(),
            embedding: Some(dummy_embedding()),
            importance: 0.7,
            domain: vec!["testing".into()],
            ..Default::default()
        })
        .unwrap();

    db.reinforce_experience(id).unwrap();

    let exp = db.get_experience(id).unwrap().unwrap();
    assert_eq!(exp.content, "reinforce test content");
    assert_eq!(exp.importance, 0.7);
    assert_eq!(exp.domain, vec!["testing"]);
    assert_eq!(exp.embedding.len(), DIM);

    db.close().unwrap();
}

// ============================================================================
// Validation Rejections
// ============================================================================

#[test]
fn test_record_experience_empty_content_rejected() {
    let (db, cid, _dir) = open_db_with_collective();

    let result = db.record_experience(NewExperience {
        collective_id: cid,
        content: "".to_string(),
        embedding: Some(dummy_embedding()),
        ..Default::default()
    });

    assert!(result.is_err());
    assert!(result.unwrap_err().is_validation());

    db.close().unwrap();
}

#[test]
fn test_record_experience_content_too_large_rejected() {
    let (db, cid, _dir) = open_db_with_collective();

    let large_content = "x".repeat(102_401); // MAX_CONTENT_SIZE = 102_400

    let result = db.record_experience(NewExperience {
        collective_id: cid,
        content: large_content,
        embedding: Some(dummy_embedding()),
        ..Default::default()
    });

    assert!(result.is_err());
    assert!(result.unwrap_err().is_validation());

    db.close().unwrap();
}

#[test]
fn test_record_experience_importance_out_of_range_rejected() {
    let (db, cid, _dir) = open_db_with_collective();

    let result = db.record_experience(NewExperience {
        collective_id: cid,
        content: "valid content".to_string(),
        embedding: Some(dummy_embedding()),
        importance: 1.5, // out of range
        ..Default::default()
    });

    assert!(result.is_err());
    assert!(result.unwrap_err().is_validation());

    db.close().unwrap();
}

#[test]
fn test_record_experience_confidence_out_of_range_rejected() {
    let (db, cid, _dir) = open_db_with_collective();

    let result = db.record_experience(NewExperience {
        collective_id: cid,
        content: "valid content".to_string(),
        embedding: Some(dummy_embedding()),
        confidence: -0.1, // out of range
        ..Default::default()
    });

    assert!(result.is_err());
    assert!(result.unwrap_err().is_validation());

    db.close().unwrap();
}

#[test]
fn test_record_experience_wrong_embedding_dimension_rejected() {
    let (db, cid, _dir) = open_db_with_collective();

    let result = db.record_experience(NewExperience {
        collective_id: cid,
        content: "valid content".to_string(),
        embedding: Some(vec![0.1; 768]), // D384 expected, got 768
        ..Default::default()
    });

    assert!(result.is_err());
    assert!(result.unwrap_err().is_validation());

    db.close().unwrap();
}

#[test]
fn test_record_experience_missing_embedding_rejected_for_external() {
    let (db, cid, _dir) = open_db_with_collective();

    // Default config is External provider — embedding is required
    let result = db.record_experience(NewExperience {
        collective_id: cid,
        content: "valid content".to_string(),
        embedding: None, // missing!
        ..Default::default()
    });

    assert!(result.is_err());
    assert!(result.unwrap_err().is_validation());

    db.close().unwrap();
}

#[test]
fn test_record_experience_nonexistent_collective_rejected() {
    let (db, _dir) = open_db();

    let result = db.record_experience(NewExperience {
        collective_id: CollectiveId::new(), // doesn't exist
        content: "valid content".to_string(),
        embedding: Some(dummy_embedding()),
        ..Default::default()
    });

    assert!(result.is_err());
    assert!(result.unwrap_err().is_not_found());

    db.close().unwrap();
}

#[test]
fn test_record_experience_too_many_domain_tags_rejected() {
    let (db, cid, _dir) = open_db_with_collective();

    let result = db.record_experience(NewExperience {
        collective_id: cid,
        content: "valid content".to_string(),
        embedding: Some(dummy_embedding()),
        domain: (0..51).map(|i| format!("tag-{i}")).collect(), // > 50
        ..Default::default()
    });

    assert!(result.is_err());
    assert!(result.unwrap_err().is_validation());

    db.close().unwrap();
}

#[test]
fn test_record_experience_domain_tag_too_long_rejected() {
    let (db, cid, _dir) = open_db_with_collective();

    let result = db.record_experience(NewExperience {
        collective_id: cid,
        content: "valid content".to_string(),
        embedding: Some(dummy_embedding()),
        domain: vec!["x".repeat(101)], // > 100 chars
        ..Default::default()
    });

    assert!(result.is_err());
    assert!(result.unwrap_err().is_validation());

    db.close().unwrap();
}

#[test]
fn test_update_experience_importance_out_of_range_rejected() {
    let (db, cid, _dir) = open_db_with_collective();

    let id = db.record_experience(minimal_experience(cid)).unwrap();

    let result = db.update_experience(
        id,
        ExperienceUpdate {
            importance: Some(2.0),
            ..Default::default()
        },
    );

    assert!(result.is_err());
    assert!(result.unwrap_err().is_validation());

    // Verify original value unchanged
    let exp = db.get_experience(id).unwrap().unwrap();
    assert_eq!(exp.importance, 0.5);

    db.close().unwrap();
}

// ============================================================================
// ExperienceType Variant Validation (ticket #5)
// ============================================================================

#[test]
fn test_record_experience_success_pattern_quality_out_of_range_rejected() {
    let (db, cid, _dir) = open_db_with_collective();

    let result = db.record_experience(NewExperience {
        collective_id: cid,
        content: "valid content".to_string(),
        embedding: Some(dummy_embedding()),
        experience_type: ExperienceType::SuccessPattern {
            task_type: "test".into(),
            approach: "test".into(),
            quality: 1.5,
        },
        ..Default::default()
    });

    assert!(result.is_err());
    assert!(result.unwrap_err().is_validation());

    db.close().unwrap();
}

#[test]
fn test_record_experience_user_preference_strength_out_of_range_rejected() {
    let (db, cid, _dir) = open_db_with_collective();

    let result = db.record_experience(NewExperience {
        collective_id: cid,
        content: "valid content".to_string(),
        embedding: Some(dummy_embedding()),
        experience_type: ExperienceType::UserPreference {
            category: "style".into(),
            preference: "dark mode".into(),
            strength: -0.1,
        },
        ..Default::default()
    });

    assert!(result.is_err());
    assert!(result.unwrap_err().is_validation());

    db.close().unwrap();
}

#[test]
fn test_record_experience_source_agent_too_long_rejected() {
    let (db, cid, _dir) = open_db_with_collective();

    let result = db.record_experience(NewExperience {
        collective_id: cid,
        content: "valid content".to_string(),
        embedding: Some(dummy_embedding()),
        source_agent: AgentId::new("a".repeat(257)),
        ..Default::default()
    });

    assert!(result.is_err());
    assert!(result.unwrap_err().is_validation());

    db.close().unwrap();
}

#[test]
fn test_record_experience_success_pattern_valid_quality_accepted() {
    let (db, cid, _dir) = open_db_with_collective();

    let id = db
        .record_experience(NewExperience {
            collective_id: cid,
            content: "valid content".to_string(),
            embedding: Some(dummy_embedding()),
            experience_type: ExperienceType::SuccessPattern {
                task_type: "refactoring".into(),
                approach: "extract method".into(),
                quality: 0.95,
            },
            ..Default::default()
        })
        .unwrap();

    let exp = db.get_experience(id).unwrap().unwrap();
    assert!(matches!(
        exp.experience_type,
        ExperienceType::SuccessPattern { quality, .. } if (quality - 0.95).abs() < f32::EPSILON
    ));

    db.close().unwrap();
}

// ============================================================================
// Persistence Across Reopen
// ============================================================================

#[test]
fn test_experience_persists_across_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");

    // Session 1: create collective and record experience
    let (cid, exp_id) = {
        let db = PulseDB::open(&path, Config::default()).unwrap();
        let cid = db.create_collective("persist-test").unwrap();
        let exp_id = db
            .record_experience(NewExperience {
                collective_id: cid,
                content: "persisted knowledge".to_string(),
                experience_type: ExperienceType::Fact {
                    statement: "data persists".into(),
                    source: "integration test".into(),
                },
                embedding: Some(dummy_embedding()),
                importance: 0.8,
                domain: vec!["persistence".into()],
                ..Default::default()
            })
            .unwrap();
        db.close().unwrap();
        (cid, exp_id)
    };

    // Session 2: reopen and verify
    {
        let db = PulseDB::open(&path, Config::default()).unwrap();
        let exp = db.get_experience(exp_id).unwrap().unwrap();

        assert_eq!(exp.id, exp_id);
        assert_eq!(exp.collective_id, cid);
        assert_eq!(exp.content, "persisted knowledge");
        assert_eq!(exp.importance, 0.8);
        assert_eq!(exp.domain, vec!["persistence"]);
        assert_eq!(exp.embedding.len(), DIM);
        assert!(matches!(exp.experience_type, ExperienceType::Fact { .. }));

        db.close().unwrap();
    }
}

#[test]
fn test_updated_experience_persists_across_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");

    // Session 1: create, record, update, reinforce
    let (exp_id,) = {
        let db = PulseDB::open(&path, Config::default()).unwrap();
        let cid = db.create_collective("update-persist").unwrap();
        let exp_id = db.record_experience(minimal_experience(cid)).unwrap();

        db.update_experience(
            exp_id,
            ExperienceUpdate {
                importance: Some(0.99),
                domain: Some(vec!["updated".into()]),
                ..Default::default()
            },
        )
        .unwrap();
        db.archive_experience(exp_id).unwrap();
        db.reinforce_experience(exp_id).unwrap();
        db.reinforce_experience(exp_id).unwrap();

        db.close().unwrap();
        (exp_id,)
    };

    // Session 2: verify all changes persisted
    {
        let db = PulseDB::open(&path, Config::default()).unwrap();
        let exp = db.get_experience(exp_id).unwrap().unwrap();

        assert_eq!(exp.importance, 0.99);
        assert_eq!(exp.domain, vec!["updated"]);
        assert!(exp.archived);
        assert_eq!(exp.applications(), 2);

        db.close().unwrap();
    }
}

// ============================================================================
// Collective Cascade Delete
// ============================================================================

#[test]
fn test_delete_collective_cascades_experiences() {
    let (db, cid, _dir) = open_db_with_collective();

    // Record 3 experiences in the collective
    let ids: Vec<ExperienceId> = (0..3)
        .map(|i| {
            db.record_experience(NewExperience {
                collective_id: cid,
                content: format!("experience {i}"),
                embedding: Some(dummy_embedding()),
                ..Default::default()
            })
            .unwrap()
        })
        .collect();

    assert_eq!(db.get_collective_stats(cid).unwrap().experience_count, 3);

    // Delete the collective — should cascade-delete all experiences
    db.delete_collective(cid).unwrap();

    // All experiences should be gone
    for id in &ids {
        assert!(db.get_experience(*id).unwrap().is_none());
    }

    db.close().unwrap();
}

#[test]
fn test_cascade_delete_does_not_affect_other_collectives() {
    let (db, _dir) = open_db();

    let cid_a = db.create_collective("collective-a").unwrap();
    let cid_b = db.create_collective("collective-b").unwrap();

    let exp_a = db.record_experience(minimal_experience(cid_a)).unwrap();
    let exp_b = db.record_experience(minimal_experience(cid_b)).unwrap();

    // Delete collective A
    db.delete_collective(cid_a).unwrap();

    // A's experience gone, B's still there
    assert!(db.get_experience(exp_a).unwrap().is_none());
    assert!(db.get_experience(exp_b).unwrap().is_some());

    db.close().unwrap();
}

// ============================================================================
// Collective Stats
// ============================================================================

#[test]
fn test_collective_stats_reflect_experience_count() {
    let (db, cid, _dir) = open_db_with_collective();

    assert_eq!(db.get_collective_stats(cid).unwrap().experience_count, 0);

    db.record_experience(minimal_experience(cid)).unwrap();
    assert_eq!(db.get_collective_stats(cid).unwrap().experience_count, 1);

    db.record_experience(minimal_experience(cid)).unwrap();
    assert_eq!(db.get_collective_stats(cid).unwrap().experience_count, 2);

    db.close().unwrap();
}

// ============================================================================
// Multiple Experiences
// ============================================================================

#[test]
fn test_multiple_experiences_in_same_collective() {
    let (db, cid, _dir) = open_db_with_collective();

    let mut ids = Vec::new();
    for i in 0..10 {
        let id = db
            .record_experience(NewExperience {
                collective_id: cid,
                content: format!("lesson number {i}"),
                embedding: Some(dummy_embedding()),
                importance: (i as f32) / 10.0,
                ..Default::default()
            })
            .unwrap();
        ids.push(id);
    }

    // Each has a unique ID and correct content
    for (i, id) in ids.iter().enumerate() {
        let exp = db.get_experience(*id).unwrap().unwrap();
        assert_eq!(exp.content, format!("lesson number {i}"));
        assert!((exp.importance - (i as f32) / 10.0).abs() < f32::EPSILON);
    }

    assert_eq!(db.get_collective_stats(cid).unwrap().experience_count, 10);

    db.close().unwrap();
}

#[test]
fn test_experiences_across_multiple_collectives() {
    let (db, _dir) = open_db();

    let cid1 = db.create_collective("project-alpha").unwrap();
    let cid2 = db.create_collective("project-beta").unwrap();

    let exp1 = db.record_experience(minimal_experience(cid1)).unwrap();
    let exp2 = db.record_experience(minimal_experience(cid2)).unwrap();

    // Each experience is in its own collective
    assert_eq!(
        db.get_experience(exp1).unwrap().unwrap().collective_id,
        cid1
    );
    assert_eq!(
        db.get_experience(exp2).unwrap().unwrap().collective_id,
        cid2
    );

    // Stats reflect per-collective counts
    assert_eq!(db.get_collective_stats(cid1).unwrap().experience_count, 1);
    assert_eq!(db.get_collective_stats(cid2).unwrap().experience_count, 1);

    db.close().unwrap();
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn test_record_experience_boundary_importance_values() {
    let (db, cid, _dir) = open_db_with_collective();

    // 0.0 and 1.0 are valid boundary values
    let id_zero = db
        .record_experience(NewExperience {
            collective_id: cid,
            content: "zero importance".to_string(),
            embedding: Some(dummy_embedding()),
            importance: 0.0,
            ..Default::default()
        })
        .unwrap();

    let id_one = db
        .record_experience(NewExperience {
            collective_id: cid,
            content: "max importance".to_string(),
            embedding: Some(dummy_embedding()),
            importance: 1.0,
            ..Default::default()
        })
        .unwrap();

    assert_eq!(db.get_experience(id_zero).unwrap().unwrap().importance, 0.0);
    assert_eq!(db.get_experience(id_one).unwrap().unwrap().importance, 1.0);

    db.close().unwrap();
}

#[test]
fn test_record_experience_max_content_size_accepted() {
    let (db, cid, _dir) = open_db_with_collective();

    let content = "x".repeat(102_400); // exactly MAX_CONTENT_SIZE

    let id = db
        .record_experience(NewExperience {
            collective_id: cid,
            content,
            embedding: Some(dummy_embedding()),
            ..Default::default()
        })
        .unwrap();

    let exp = db.get_experience(id).unwrap().unwrap();
    assert_eq!(exp.content.len(), 102_400);

    db.close().unwrap();
}

#[test]
fn test_record_experience_max_domain_tags_accepted() {
    let (db, cid, _dir) = open_db_with_collective();

    let id = db
        .record_experience(NewExperience {
            collective_id: cid,
            content: "valid content".to_string(),
            embedding: Some(dummy_embedding()),
            domain: (0..50).map(|i| format!("tag-{i}")).collect(), // exactly 50
            ..Default::default()
        })
        .unwrap();

    let exp = db.get_experience(id).unwrap().unwrap();
    assert_eq!(exp.domain.len(), 50);

    db.close().unwrap();
}

#[test]
fn test_delete_then_rerecord() {
    let (db, cid, _dir) = open_db_with_collective();

    let id = db.record_experience(minimal_experience(cid)).unwrap();
    db.delete_experience(id).unwrap();

    // Can record a new experience after deletion
    let id2 = db.record_experience(minimal_experience(cid)).unwrap();
    assert_ne!(id, id2);
    assert!(db.get_experience(id2).unwrap().is_some());

    db.close().unwrap();
}
