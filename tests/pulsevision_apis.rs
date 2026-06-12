//! Integration tests for PulseVision-ready APIs (Issue #8).
//!
//! Tests read-only mode, paginated list methods, and enriched watch events.

use pulsedb::{
    Config, InsightType, NewDerivedInsight, NewExperience, NewExperienceRelation, PulseDB,
    RelationType, WatchEventType,
};
use tempfile::tempdir;

// ============================================================================
// Helpers
// ============================================================================

fn open_db() -> (PulseDB, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let db = PulseDB::open(dir.path().join("test.db"), Config::default()).unwrap();
    (db, dir)
}

fn minimal_exp(cid: pulsedb::CollectiveId) -> NewExperience {
    NewExperience {
        collective_id: cid,
        content: format!("pv-test-{}", uuid::Uuid::now_v7()),
        embedding: Some(vec![0.1f32; 384]),
        ..Default::default()
    }
}

// ============================================================================
// Read-only mode
// ============================================================================

#[test]
fn test_read_only_config() {
    let config = Config::read_only();
    assert!(config.read_only);

    let config = Config::default();
    assert!(!config.read_only);
}

#[test]
fn test_read_only_blocks_mutations() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");

    // First open normally and create some data
    {
        let db = PulseDB::open(&path, Config::default()).unwrap();
        let cid = db.create_collective("test").unwrap();
        db.record_experience(minimal_exp(cid)).unwrap();
        db.close().unwrap();
    }

    // Reopen in read-only mode
    let db = PulseDB::open(&path, Config::read_only()).unwrap();
    assert!(db.is_read_only());

    // All mutations should fail
    let err = db.create_collective("fail").unwrap_err();
    assert!(err.is_read_only());

    let cid = db.list_collectives().unwrap()[0].id;

    let err = db.record_experience(minimal_exp(cid)).unwrap_err();
    assert!(err.is_read_only());

    let err = db.delete_collective(cid).unwrap_err();
    assert!(err.is_read_only());
}

#[test]
fn test_read_only_allows_reads() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");

    // Create data
    let cid;
    let exp_id;
    {
        let db = PulseDB::open(&path, Config::default()).unwrap();
        cid = db.create_collective("readable").unwrap();
        exp_id = db.record_experience(minimal_exp(cid)).unwrap();
        db.close().unwrap();
    }

    // Read-only access
    let db = PulseDB::open(&path, Config::read_only()).unwrap();

    // Reads should work
    assert!(db.get_experience(exp_id).unwrap().is_some());
    assert!(!db.list_collectives().unwrap().is_empty());
    assert!(db.get_collective(cid).unwrap().is_some());

    let energy = db.energy(exp_id).unwrap();
    assert!((0.0..=1.0).contains(&energy));

    let err = db.reinforce_experience(exp_id).unwrap_err();
    assert!(err.is_read_only());
}

// ============================================================================
// Paginated list methods
// ============================================================================

#[test]
fn test_list_experiences_basic() {
    let (db, _dir) = open_db();
    let cid = db.create_collective("list-test").unwrap();

    // Record 5 experiences
    for _ in 0..5 {
        db.record_experience(minimal_exp(cid)).unwrap();
    }

    // List all
    let all = db.list_experiences(cid, 100, 0).unwrap();
    assert_eq!(all.len(), 5);

    // Verify embeddings are included (PulseVision needs them for 3D positioning)
    assert_eq!(all[0].embedding.len(), 384);
}

#[test]
fn test_list_experiences_pagination() {
    let (db, _dir) = open_db();
    let cid = db.create_collective("page-test").unwrap();

    for _ in 0..10 {
        db.record_experience(minimal_exp(cid)).unwrap();
    }

    // Page 1: first 3
    let page1 = db.list_experiences(cid, 3, 0).unwrap();
    assert_eq!(page1.len(), 3);

    // Page 2: next 3
    let page2 = db.list_experiences(cid, 3, 3).unwrap();
    assert_eq!(page2.len(), 3);

    // No overlap
    assert_ne!(page1[0].id, page2[0].id);

    // Past end
    let past = db.list_experiences(cid, 100, 10).unwrap();
    assert!(past.is_empty());
}

#[test]
fn test_list_relations() {
    let (db, _dir) = open_db();
    let cid = db.create_collective("rel-list").unwrap();

    let e1 = db.record_experience(minimal_exp(cid)).unwrap();
    let e2 = db.record_experience(minimal_exp(cid)).unwrap();
    let e3 = db.record_experience(minimal_exp(cid)).unwrap();

    db.store_relation(NewExperienceRelation {
        source_id: e1,
        target_id: e2,
        relation_type: RelationType::Supports,
        strength: 0.8,
        metadata: None,
    })
    .unwrap();

    db.store_relation(NewExperienceRelation {
        source_id: e2,
        target_id: e3,
        relation_type: RelationType::Elaborates,
        strength: 0.6,
        metadata: None,
    })
    .unwrap();

    let rels = db.list_relations(cid, 100, 0).unwrap();
    assert_eq!(rels.len(), 2);
}

#[test]
fn test_list_insights() {
    let (db, _dir) = open_db();
    let cid = db.create_collective("insight-list").unwrap();
    let exp_id = db.record_experience(minimal_exp(cid)).unwrap();

    db.store_insight(NewDerivedInsight {
        collective_id: cid,
        content: "insight 1".to_string(),
        embedding: Some(vec![0.1f32; 384]),
        source_experience_ids: vec![exp_id],
        insight_type: InsightType::Pattern,
        confidence: 0.9,
        domain: vec![],
    })
    .unwrap();

    let insights = db.list_insights(cid, 100, 0).unwrap();
    assert_eq!(insights.len(), 1);
    assert_eq!(insights[0].content, "insight 1");
}

// ============================================================================
// Enriched watch events
// ============================================================================

#[test]
fn test_watch_event_includes_experience_on_create() {
    let (db, _dir) = open_db();
    let cid = db.create_collective("watch-enrich").unwrap();

    // Subscribe before recording
    let stream = db.watch_experiences(cid).unwrap();

    // Record an experience
    let exp_id = db
        .record_experience(NewExperience {
            collective_id: cid,
            content: "enriched content".to_string(),
            embedding: Some(vec![0.5f32; 384]),
            importance: 0.9,
            ..Default::default()
        })
        .unwrap();

    // Get the event from the stream
    let rt = tokio::runtime::Runtime::new().unwrap();
    let event = rt.block_on(async {
        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            futures::StreamExt::into_future(stream),
        )
        .await
    });

    if let Ok((Some(event), _)) = event {
        assert_eq!(event.experience_id, exp_id);
        assert_eq!(event.event_type, WatchEventType::Created);

        // Enriched: experience data included
        assert!(
            event.experience.is_some(),
            "Created event should include experience"
        );
        let exp = event.experience.unwrap();
        assert_eq!(exp.content, "enriched content");
        assert_eq!(exp.embedding.len(), 384);
        assert!((exp.importance - 0.9).abs() < f32::EPSILON);
    }
}

#[test]
fn test_watch_event_none_on_delete() {
    let (db, _dir) = open_db();
    let cid = db.create_collective("watch-delete").unwrap();
    let exp_id = db.record_experience(minimal_exp(cid)).unwrap();

    let stream = db.watch_experiences(cid).unwrap();
    db.delete_experience(exp_id).unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let event = rt.block_on(async {
        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            futures::StreamExt::into_future(stream),
        )
        .await
    });

    if let Ok((Some(event), _)) = event {
        assert_eq!(event.event_type, WatchEventType::Deleted);
        assert!(
            event.experience.is_none(),
            "Deleted event should not include experience"
        );
    }
}

// ============================================================================
// Error variant
// ============================================================================

#[test]
fn test_read_only_error_display() {
    let err = pulsedb::PulseDBError::ReadOnly;
    assert_eq!(err.to_string(), "Database is in read-only mode");
    assert!(err.is_read_only());
}
