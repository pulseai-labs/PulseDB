//! Integration tests for Phase 3: Sync Engine.
//!
//! Tests two real PulseDB instances syncing via InMemorySyncTransport.
//! Covers push, pull, bidirectional sync, conflict resolution, echo
//! prevention, incremental sync, and SyncManager lifecycle.

#![cfg(feature = "sync")]

use std::sync::Arc;

use pulsedb::sync::config::{ConflictResolution, SyncConfig, SyncDirection};
use pulsedb::sync::guard::SyncApplyGuard;
use pulsedb::sync::manager::SyncManager;
use pulsedb::sync::transport_mem::InMemorySyncTransport;
use pulsedb::sync::SyncStatus;
use pulsedb::{
    CollectiveId, Config, ExperienceUpdate, InsightType, NewDerivedInsight, NewExperience,
    NewExperienceRelation, PulseDB, RelationType,
};
use tempfile::tempdir;

// ============================================================================
// Helpers
// ============================================================================

fn open_db() -> (Arc<PulseDB>, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let db = Arc::new(PulseDB::open(dir.path().join("test.db"), Config::default()).unwrap());
    (db, dir)
}

fn minimal_exp(cid: CollectiveId) -> NewExperience {
    NewExperience {
        collective_id: cid,
        content: format!("experience-{}", uuid::Uuid::now_v7()),
        embedding: Some(vec![0.1f32; 384]),
        ..Default::default()
    }
}

fn sync_config() -> SyncConfig {
    SyncConfig {
        direction: SyncDirection::Bidirectional,
        batch_size: 500,
        ..Default::default()
    }
}

/// Create two PulseDB instances with paired transports and SyncManagers.
fn setup_sync_pair() -> SyncPair {
    let (db_a, dir_a) = open_db();
    let (db_b, dir_b) = open_db();
    let (transport_a, transport_b) = InMemorySyncTransport::new_pair();

    let manager_a = SyncManager::new(Arc::clone(&db_a), Box::new(transport_a), sync_config());
    let manager_b = SyncManager::new(Arc::clone(&db_b), Box::new(transport_b), sync_config());

    SyncPair {
        db_a,
        db_b,
        manager_a,
        manager_b,
        _dir_a: dir_a,
        _dir_b: dir_b,
    }
}

struct SyncPair {
    db_a: Arc<PulseDB>,
    db_b: Arc<PulseDB>,
    manager_a: SyncManager,
    manager_b: SyncManager,
    _dir_a: tempfile::TempDir,
    _dir_b: tempfile::TempDir,
}

// ============================================================================
// Basic push + pull
// ============================================================================

#[tokio::test]
async fn test_basic_experience_sync() {
    let mut pair = setup_sync_pair();

    // Create collective + experience on A
    let cid = pair.db_a.create_collective("test").unwrap();
    let exp_id = pair.db_a.record_experience(minimal_exp(cid)).unwrap();

    // Sync A → shared buffer
    pair.manager_a.sync_once().await.unwrap();

    // Create same collective on B (needed for HNSW indexes)
    // In real usage, collective sync handles this. Here we create it manually
    // since B needs the collective before it can receive experiences.
    pair.db_b.create_collective("test").unwrap();

    // Sync B ← shared buffer
    pair.manager_b.sync_once().await.unwrap();

    // Verify B has the experience
    let exp = pair.db_b.get_experience(exp_id).unwrap();
    assert!(exp.is_some(), "Experience should have synced to DB-B");
    assert_eq!(exp.unwrap().content.starts_with("experience-"), true);
}

#[tokio::test]
async fn test_collective_sync() {
    let mut pair = setup_sync_pair();

    // Create collective on A
    let cid = pair.db_a.create_collective("synced-collective").unwrap();

    // Sync A → buffer → B
    pair.manager_a.sync_once().await.unwrap();
    pair.manager_b.sync_once().await.unwrap();

    // B should have the collective
    let collective = pair.db_b.get_collective(cid).unwrap();
    assert!(collective.is_some(), "Collective should sync to DB-B");
    assert_eq!(collective.unwrap().name, "synced-collective");

    // B should be able to record experiences in the synced collective
    let exp_id = pair.db_b.record_experience(minimal_exp(cid)).unwrap();
    assert!(pair.db_b.get_experience(exp_id).unwrap().is_some());
}

#[tokio::test]
async fn test_experience_with_collective_sync() {
    let mut pair = setup_sync_pair();

    // Create collective + experience on A
    let cid = pair.db_a.create_collective("proj").unwrap();
    let exp_id = pair.db_a.record_experience(minimal_exp(cid)).unwrap();

    // Sync A → buffer
    pair.manager_a.sync_once().await.unwrap();

    // Sync B ← buffer (collective + experience arrive together)
    pair.manager_b.sync_once().await.unwrap();

    // B should have both
    assert!(pair.db_b.get_collective(cid).unwrap().is_some());
    assert!(pair.db_b.get_experience(exp_id).unwrap().is_some());
}

#[tokio::test]
async fn test_relation_sync() {
    let mut pair = setup_sync_pair();

    let cid = pair.db_a.create_collective("rel-test").unwrap();
    let exp1 = pair.db_a.record_experience(minimal_exp(cid)).unwrap();
    let exp2 = pair.db_a.record_experience(minimal_exp(cid)).unwrap();
    let rel_id = pair
        .db_a
        .store_relation(NewExperienceRelation {
            source_id: exp1,
            target_id: exp2,
            relation_type: RelationType::Supports,
            strength: 0.9,
            metadata: None,
        })
        .unwrap();

    // Sync A → B
    pair.manager_a.sync_once().await.unwrap();
    pair.manager_b.sync_once().await.unwrap();

    // B should have the relation
    let rel = pair.db_b.get_relation(rel_id).unwrap();
    assert!(rel.is_some(), "Relation should sync to DB-B");
    let rel = rel.unwrap();
    assert_eq!(rel.source_id, exp1);
    assert_eq!(rel.target_id, exp2);
}

#[tokio::test]
async fn test_insight_sync() {
    let mut pair = setup_sync_pair();

    let cid = pair.db_a.create_collective("insight-test").unwrap();
    let exp_id = pair.db_a.record_experience(minimal_exp(cid)).unwrap();
    let insight_id = pair
        .db_a
        .store_insight(NewDerivedInsight {
            collective_id: cid,
            content: "synced insight".to_string(),
            embedding: Some(vec![0.2f32; 384]),
            source_experience_ids: vec![exp_id],
            insight_type: InsightType::Pattern,
            confidence: 0.8,
            domain: vec!["test".to_string()],
        })
        .unwrap();

    // Sync A → B
    pair.manager_a.sync_once().await.unwrap();
    pair.manager_b.sync_once().await.unwrap();

    let insight = pair.db_b.get_insight(insight_id).unwrap();
    assert!(insight.is_some(), "Insight should sync to DB-B");
    assert_eq!(insight.unwrap().content, "synced insight");
}

// ============================================================================
// Delete sync
// ============================================================================

#[tokio::test]
async fn test_experience_delete_sync() {
    let mut pair = setup_sync_pair();

    let cid = pair.db_a.create_collective("del-test").unwrap();
    let exp_id = pair.db_a.record_experience(minimal_exp(cid)).unwrap();

    // Sync creation
    pair.manager_a.sync_once().await.unwrap();
    pair.manager_b.sync_once().await.unwrap();
    assert!(pair.db_b.get_experience(exp_id).unwrap().is_some());

    // Delete on A
    pair.db_a.delete_experience(exp_id).unwrap();

    // Sync deletion
    pair.manager_a.sync_once().await.unwrap();
    pair.manager_b.sync_once().await.unwrap();

    // B should no longer have it
    assert!(
        pair.db_b.get_experience(exp_id).unwrap().is_none(),
        "Deleted experience should be gone on DB-B"
    );
}

// ============================================================================
// Update sync
// ============================================================================

#[tokio::test]
async fn test_experience_update_sync() {
    let mut pair = setup_sync_pair();

    let cid = pair.db_a.create_collective("upd-test").unwrap();
    let exp_id = pair.db_a.record_experience(minimal_exp(cid)).unwrap();

    // Sync creation
    pair.manager_a.sync_once().await.unwrap();
    pair.manager_b.sync_once().await.unwrap();

    // Update on A
    pair.db_a
        .update_experience(
            exp_id,
            ExperienceUpdate {
                importance: Some(0.99),
                ..Default::default()
            },
        )
        .unwrap();

    // Sync update
    pair.manager_a.sync_once().await.unwrap();
    pair.manager_b.sync_once().await.unwrap();

    let exp = pair.db_b.get_experience(exp_id).unwrap().unwrap();
    assert!(
        (exp.importance - 0.99).abs() < f32::EPSILON,
        "Updated importance should sync"
    );
}

// ============================================================================
// Incremental sync
// ============================================================================

#[tokio::test]
async fn test_incremental_sync() {
    let mut pair = setup_sync_pair();

    let cid = pair.db_a.create_collective("inc-test").unwrap();

    // First batch
    let id1 = pair.db_a.record_experience(minimal_exp(cid)).unwrap();
    pair.manager_a.sync_once().await.unwrap();
    pair.manager_b.sync_once().await.unwrap();
    assert!(pair.db_b.get_experience(id1).unwrap().is_some());

    // Second batch (only new changes should sync)
    let id2 = pair.db_a.record_experience(minimal_exp(cid)).unwrap();
    pair.manager_a.sync_once().await.unwrap();
    pair.manager_b.sync_once().await.unwrap();
    assert!(pair.db_b.get_experience(id2).unwrap().is_some());

    // Third sync with no new changes
    let status = pair.manager_a.sync_once().await.unwrap();
    assert_eq!(status, SyncStatus::Idle);
}

// ============================================================================
// Echo prevention
// ============================================================================

#[tokio::test]
async fn test_echo_prevention() {
    let mut pair = setup_sync_pair();

    let cid = pair.db_a.create_collective("echo-test").unwrap();
    let exp_id = pair.db_a.record_experience(minimal_exp(cid)).unwrap();

    // Sync A → B
    pair.manager_a.sync_once().await.unwrap();
    pair.manager_b.sync_once().await.unwrap();
    assert!(pair.db_b.get_experience(exp_id).unwrap().is_some());

    // B syncs back to shared buffer — the synced experience should NOT
    // be pushed back (echo prevention)
    let seq_before = pair.db_b.get_current_sequence().unwrap();
    pair.manager_b.sync_once().await.unwrap();
    assert_eq!(pair.db_b.get_current_sequence().unwrap(), seq_before);

    // A syncs again — should have NO new changes from B
    pair.manager_a.sync_once().await.unwrap();

    // The experience on A should still be the original (not duplicated)
    let exp = pair.db_a.get_experience(exp_id).unwrap().unwrap();
    assert_eq!(exp.applications(), 0); // Not modified
}

// ============================================================================
// Conflict resolution
// ============================================================================

#[tokio::test]
async fn test_conflict_resolution_server_wins() {
    let (db_a, _dir_a) = open_db();
    let (db_b, _dir_b) = open_db();
    let (transport_a, transport_b) = InMemorySyncTransport::new_pair();

    let config = SyncConfig {
        conflict_resolution: ConflictResolution::ServerWins,
        ..sync_config()
    };

    let mut manager_a = SyncManager::new(Arc::clone(&db_a), Box::new(transport_a), config.clone());
    let mut manager_b = SyncManager::new(Arc::clone(&db_b), Box::new(transport_b), config);

    // Create on A, sync to B
    let cid = db_a.create_collective("conflict").unwrap();
    let exp_id = db_a.record_experience(minimal_exp(cid)).unwrap();

    manager_a.sync_once().await.unwrap();
    manager_b.sync_once().await.unwrap();

    // Update on A (remote/server)
    db_a.update_experience(
        exp_id,
        ExperienceUpdate {
            importance: Some(0.1),
            ..Default::default()
        },
    )
    .unwrap();

    // Sync update A → B (ServerWins: remote always wins)
    manager_a.sync_once().await.unwrap();
    manager_b.sync_once().await.unwrap();

    let exp_b = db_b.get_experience(exp_id).unwrap().unwrap();
    assert!(
        (exp_b.importance - 0.1).abs() < f32::EPSILON,
        "ServerWins: remote update should be applied"
    );
}

// ============================================================================
// Bidirectional sync
// ============================================================================

#[tokio::test]
async fn test_bidirectional_sync() {
    // Bidirectional sync uses two separate transport pairs:
    // A→B transport and B→A transport. The InMemorySyncTransport
    // shares a single buffer, so both directions need separate pairs.
    let (db_a, _dir_a) = open_db();
    let (db_b, _dir_b) = open_db();

    // A→B direction
    let (transport_a_push, transport_b_pull) = InMemorySyncTransport::new_pair();
    // B→A direction
    let (transport_b_push, transport_a_pull) = InMemorySyncTransport::new_pair();

    let config_a_push = SyncConfig {
        direction: SyncDirection::PushOnly,
        ..sync_config()
    };
    let config_b_pull = SyncConfig {
        direction: SyncDirection::PullOnly,
        ..sync_config()
    };
    let config_b_push = SyncConfig {
        direction: SyncDirection::PushOnly,
        ..sync_config()
    };
    let config_a_pull = SyncConfig {
        direction: SyncDirection::PullOnly,
        ..sync_config()
    };

    let mut mgr_a_push =
        SyncManager::new(Arc::clone(&db_a), Box::new(transport_a_push), config_a_push);
    let mut mgr_b_pull =
        SyncManager::new(Arc::clone(&db_b), Box::new(transport_b_pull), config_b_pull);
    let mut mgr_b_push =
        SyncManager::new(Arc::clone(&db_b), Box::new(transport_b_push), config_b_push);
    let mut mgr_a_pull =
        SyncManager::new(Arc::clone(&db_a), Box::new(transport_a_pull), config_a_pull);

    // Create collective on A, push to B
    let cid = db_a.create_collective("bidi").unwrap();
    mgr_a_push.sync_once().await.unwrap();
    mgr_b_pull.sync_once().await.unwrap();

    // Create experiences on both sides
    let id_a = db_a.record_experience(minimal_exp(cid)).unwrap();
    let id_b = db_b.record_experience(minimal_exp(cid)).unwrap();

    // Push A→B, Pull B←A
    mgr_a_push.sync_once().await.unwrap();
    mgr_b_pull.sync_once().await.unwrap();

    // Push B→A, Pull A←B
    mgr_b_push.sync_once().await.unwrap();
    mgr_a_pull.sync_once().await.unwrap();

    // Both should have both experiences
    assert!(db_a.get_experience(id_a).unwrap().is_some());
    assert!(
        db_a.get_experience(id_b).unwrap().is_some(),
        "A should have B's experience"
    );
    assert!(db_b.get_experience(id_a).unwrap().is_some());
    assert!(db_b.get_experience(id_b).unwrap().is_some());
}

#[tokio::test]
async fn test_bidirectional_reinforcement_gcounter_converges_exact_total() {
    let (db_a, _dir_a) = open_db();
    let (db_b, _dir_b) = open_db();

    let (transport_a_push, transport_b_pull) = InMemorySyncTransport::new_pair();
    let (transport_b_push, transport_a_pull) = InMemorySyncTransport::new_pair();

    let mut mgr_a_push = SyncManager::new(
        Arc::clone(&db_a),
        Box::new(transport_a_push),
        SyncConfig {
            direction: SyncDirection::PushOnly,
            ..sync_config()
        },
    );
    let mut mgr_b_pull = SyncManager::new(
        Arc::clone(&db_b),
        Box::new(transport_b_pull),
        SyncConfig {
            direction: SyncDirection::PullOnly,
            ..sync_config()
        },
    );
    let mut mgr_b_push = SyncManager::new(
        Arc::clone(&db_b),
        Box::new(transport_b_push),
        SyncConfig {
            direction: SyncDirection::PushOnly,
            ..sync_config()
        },
    );
    let mut mgr_a_pull = SyncManager::new(
        Arc::clone(&db_a),
        Box::new(transport_a_pull),
        SyncConfig {
            direction: SyncDirection::PullOnly,
            ..sync_config()
        },
    );

    let cid = db_a.create_collective("reinforce-gcounter").unwrap();
    let exp_id = db_a.record_experience(minimal_exp(cid)).unwrap();
    mgr_a_push.sync_once().await.unwrap();
    mgr_b_pull.sync_once().await.unwrap();

    db_a.reinforce_experience(exp_id).unwrap();
    db_b.reinforce_experience(exp_id).unwrap();
    db_b.reinforce_experience(exp_id).unwrap();

    mgr_a_push.sync_once().await.unwrap();
    mgr_b_pull.sync_once().await.unwrap();
    mgr_b_push.sync_once().await.unwrap();
    mgr_a_pull.sync_once().await.unwrap();

    let exp_a = db_a.get_experience(exp_id).unwrap().unwrap();
    let exp_b = db_b.get_experience(exp_id).unwrap().unwrap();
    assert_eq!(exp_a.applications(), 3);
    assert_eq!(exp_b.applications(), 3);
    assert_eq!(exp_a.applications, exp_b.applications);
}

#[tokio::test]
async fn test_create_collision_sentinel_merge_does_not_double_count() {
    use std::collections::BTreeMap;

    let (db_a, _dir_a) = open_db();
    let (db_b, _dir_b) = open_db();
    let (transport_a, transport_b) = InMemorySyncTransport::new_pair();

    let mut manager_a = SyncManager::new(
        Arc::clone(&db_a),
        Box::new(transport_a),
        SyncConfig {
            direction: SyncDirection::PushOnly,
            ..sync_config()
        },
    );
    let mut manager_b = SyncManager::new(
        Arc::clone(&db_b),
        Box::new(transport_b),
        SyncConfig {
            direction: SyncDirection::PullOnly,
            ..sync_config()
        },
    );

    let cid = db_a.create_collective("sentinel-collision").unwrap();
    let exp_id = db_a.record_experience(minimal_exp(cid)).unwrap();
    manager_a.sync_once().await.unwrap();
    manager_b.sync_once().await.unwrap();

    let legacy_key = pulsedb::InstanceId::nil();
    let remote_key = pulsedb::InstanceId::new();
    let mut remote = db_a.get_experience(exp_id).unwrap().unwrap();
    remote.applications = BTreeMap::from([(legacy_key, 5), (remote_key, 7)]);
    let mut local = db_b.get_experience(exp_id).unwrap().unwrap();
    local.applications = BTreeMap::from([(legacy_key, 5)]);

    let guard = SyncApplyGuard::enter();
    db_a.apply_synced_experience(remote).unwrap();
    db_b.apply_synced_experience(local).unwrap();
    drop(guard);

    let (collision_push, collision_pull) = InMemorySyncTransport::new_pair();
    let mut collision_sender = SyncManager::new(
        Arc::clone(&db_a),
        Box::new(collision_push),
        SyncConfig {
            direction: SyncDirection::PushOnly,
            ..sync_config()
        },
    );
    let mut collision_receiver = SyncManager::new(
        Arc::clone(&db_b),
        Box::new(collision_pull),
        SyncConfig {
            direction: SyncDirection::PullOnly,
            ..sync_config()
        },
    );

    collision_sender.sync_once().await.unwrap();
    collision_receiver.sync_once().await.unwrap();

    let merged = db_b.get_experience(exp_id).unwrap().unwrap();
    assert_eq!(merged.applications.get(&legacy_key), Some(&5));
    assert_eq!(merged.applications.get(&remote_key), Some(&7));
    assert_eq!(merged.applications(), 12);
}

// ============================================================================
// Initial sync
// ============================================================================

#[tokio::test]
async fn test_initial_sync_catchup() {
    let (db_a, _dir_a) = open_db();
    let (db_b, _dir_b) = open_db();
    let (transport_a, transport_b) = InMemorySyncTransport::new_pair();

    let config = SyncConfig {
        batch_size: 5, // Small batches to test pagination
        ..sync_config()
    };

    let mut manager_a = SyncManager::new(Arc::clone(&db_a), Box::new(transport_a), config.clone());
    let mut manager_b = SyncManager::new(Arc::clone(&db_b), Box::new(transport_b), config);

    // Create a bunch of data on A
    let cid = db_a.create_collective("catchup").unwrap();
    let mut exp_ids = Vec::new();
    for _ in 0..12 {
        exp_ids.push(db_a.record_experience(minimal_exp(cid)).unwrap());
    }

    // Push all from A
    manager_a.sync_once().await.unwrap();

    // B does initial sync (catches up all changes)
    manager_b.initial_sync(None).await.unwrap();

    // B should have everything
    assert!(db_b.get_collective(cid).unwrap().is_some());
    for id in &exp_ids {
        assert!(
            db_b.get_experience(*id).unwrap().is_some(),
            "Experience {} should be synced",
            id
        );
    }
}

// ============================================================================
// SyncManager lifecycle
// ============================================================================

#[tokio::test]
async fn test_sync_manager_status() {
    let pair = setup_sync_pair();
    assert_eq!(pair.manager_a.status(), SyncStatus::Idle);
}

#[tokio::test]
async fn test_sync_manager_start_stop() {
    let mut pair = setup_sync_pair();

    pair.manager_a.start().await.unwrap();
    // Give the background loop a moment
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    pair.manager_a.stop().await.unwrap();
    assert_eq!(pair.manager_a.status(), SyncStatus::Idle);
}

// ============================================================================
// Selective sync (collective filter)
// ============================================================================

#[tokio::test]
async fn test_selective_collective_sync() {
    let (db_a, _dir_a) = open_db();
    let (db_b, _dir_b) = open_db();
    let (transport_a, transport_b) = InMemorySyncTransport::new_pair();

    let cid_yes = db_a.create_collective("yes").unwrap();
    let cid_no = db_a.create_collective("no").unwrap();

    let exp_yes = db_a.record_experience(minimal_exp(cid_yes)).unwrap();
    let exp_no = db_a.record_experience(minimal_exp(cid_no)).unwrap();

    // Only sync cid_yes
    let config = SyncConfig {
        collectives: Some(vec![cid_yes]),
        ..sync_config()
    };

    let mut manager_a = SyncManager::new(Arc::clone(&db_a), Box::new(transport_a), config.clone());
    let mut manager_b = SyncManager::new(Arc::clone(&db_b), Box::new(transport_b), config);

    manager_a.sync_once().await.unwrap();
    manager_b.sync_once().await.unwrap();

    // B should have the filtered collective's experience
    assert!(db_b.get_collective(cid_yes).unwrap().is_some());
    assert!(db_b.get_experience(exp_yes).unwrap().is_some());

    // B should NOT have the excluded collective
    assert!(
        db_b.get_collective(cid_no).unwrap().is_none(),
        "Excluded collective should not sync"
    );
    assert!(
        db_b.get_experience(exp_no).unwrap().is_none(),
        "Excluded experience should not sync"
    );
}
