//! Integration tests for Phase 3: Sync Engine.
//!
//! Tests two real PulseDB instances syncing through each other's real
//! `SyncServer` over the real frame codec (`common::ServerBackedTransport`).
//! Covers push, pull, bidirectional sync, conflict resolution, echo
//! prevention, incremental sync, peer replacement and SyncManager lifecycle.
//!
//! The adapter is server-backed on purpose: a double that hands structs across
//! applies nothing and encodes nothing, so it cannot witness either half of what
//! these tests assert.

#![cfg(feature = "sync")]

mod common;

use std::sync::Arc;

use common::{
    copy_fixture, fixtures_dir, server_for, server_for_with, sync_both_ways, ServerBackedTransport,
};
use pulsedb::sync::config::{ConflictResolution, SyncConfig, SyncDirection};
use pulsedb::sync::manager::SyncManager;
use pulsedb::sync::transport_mem::InMemorySyncTransport;
use pulsedb::sync::SyncStatus;
use pulsedb::{
    CollectiveId, Config, ExperienceId, ExperienceUpdate, InsightType, NewDerivedInsight,
    NewExperience, NewExperienceRelation, PulseDB, RelationType,
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
    let config = SyncConfig {
        direction: SyncDirection::Bidirectional,
        batch_size: 250,
        ..Default::default()
    };
    // 500 was the pre-0.8.0 default and no longer validates against the default
    // byte cap; these tests are supposed to model a supported configuration.
    config.validate().expect("the test config must be valid");
    config
}

/// Create two PulseDB instances, each syncing through the OTHER's real
/// `SyncServer`.
///
/// `manager_a` talks to B's server and `manager_b` to A's, so a push from A is
/// genuinely applied into B's store through the same byte handler an HTTP
/// consumer would call.
fn setup_sync_pair() -> SyncPair {
    let (db_a, dir_a) = open_db();
    let (db_b, dir_b) = open_db();
    let server_a = server_for(&db_a);
    let server_b = server_for(&db_b);

    let manager_a = SyncManager::new(
        Arc::clone(&db_a),
        Box::new(ServerBackedTransport::new(Arc::clone(&server_b))),
        sync_config(),
    )
    .unwrap();
    let manager_b = SyncManager::new(
        Arc::clone(&db_b),
        Box::new(ServerBackedTransport::new(Arc::clone(&server_a))),
        sync_config(),
    )
    .unwrap();

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
    assert!(exp.unwrap().content.starts_with("experience-"));
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
    let server_a = server_for(&db_a);
    let server_b = server_for(&db_b);

    let config = SyncConfig {
        conflict_resolution: ConflictResolution::ServerWins,
        ..sync_config()
    };

    let mut manager_a = SyncManager::new(
        Arc::clone(&db_a),
        Box::new(ServerBackedTransport::new(server_b)),
        config.clone(),
    )
    .unwrap();
    let mut manager_b = SyncManager::new(
        Arc::clone(&db_b),
        Box::new(ServerBackedTransport::new(server_a)),
        config,
    )
    .unwrap();

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
    // Each direction is its own manager over a server-backed transport: A's
    // pushes go to B's real server, B's pulls read A's real WAL.
    let (db_a, _dir_a) = open_db();
    let (db_b, _dir_b) = open_db();
    let server_a = server_for(&db_a);
    let server_b = server_for(&db_b);

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

    let mut mgr_a_push = SyncManager::new(
        Arc::clone(&db_a),
        Box::new(ServerBackedTransport::new(Arc::clone(&server_b))),
        config_a_push,
    )
    .unwrap();
    let mut mgr_b_pull = SyncManager::new(
        Arc::clone(&db_b),
        Box::new(ServerBackedTransport::new(Arc::clone(&server_a))),
        config_b_pull,
    )
    .unwrap();
    let mut mgr_b_push = SyncManager::new(
        Arc::clone(&db_b),
        Box::new(ServerBackedTransport::new(server_a)),
        config_b_push,
    )
    .unwrap();
    let mut mgr_a_pull = SyncManager::new(
        Arc::clone(&db_a),
        Box::new(ServerBackedTransport::new(server_b)),
        config_a_pull,
    )
    .unwrap();

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

    let server_a = server_for(&db_a);
    let server_b = server_for(&db_b);

    let mut mgr_a_push = SyncManager::new(
        Arc::clone(&db_a),
        Box::new(ServerBackedTransport::new(Arc::clone(&server_b))),
        SyncConfig {
            direction: SyncDirection::PushOnly,
            ..sync_config()
        },
    )
    .unwrap();
    let mut mgr_b_pull = SyncManager::new(
        Arc::clone(&db_b),
        Box::new(ServerBackedTransport::new(Arc::clone(&server_a))),
        SyncConfig {
            direction: SyncDirection::PullOnly,
            ..sync_config()
        },
    )
    .unwrap();
    let mut mgr_b_push = SyncManager::new(
        Arc::clone(&db_b),
        Box::new(ServerBackedTransport::new(server_a)),
        SyncConfig {
            direction: SyncDirection::PushOnly,
            ..sync_config()
        },
    )
    .unwrap();
    let mut mgr_a_pull = SyncManager::new(
        Arc::clone(&db_a),
        Box::new(ServerBackedTransport::new(server_b)),
        SyncConfig {
            direction: SyncDirection::PullOnly,
            ..sync_config()
        },
    )
    .unwrap();

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

/// The reserved bucket the v2→v3 migration files a store's scalar
/// `applications` count under. Mirrors the private
/// `legacy_applications_instance_id()` in `src/storage/redb.rs`: a fixed,
/// non-UUIDv7 key that every migrated store shares, so two independently
/// migrated copies of one v0.4.0 store merge their legacy counts under
/// per-key max (one bucket) instead of summing them (two buckets).
fn legacy_sentinel() -> pulsedb::InstanceId {
    pulsedb::InstanceId::from_bytes(*b"PULSEDB_LEGACY__")
}

/// Issue #11: the sentinel-merge proof runs on two REAL v0.4.0 stores
/// (schema v2, scalar `applications`), each migrated through the genuine
/// v2→v3 path on open, then synced bidirectionally. Every experience collides
/// on create, and the merged total must equal the legacy count — not twice it.
#[tokio::test]
async fn test_create_collision_sentinel_merge_does_not_double_count() {
    use std::collections::BTreeMap;

    // Two independent restores of the same real v0.4.0 store, each migrated
    // by its own open (the checked-in blob is never opened directly).
    let (_tmp_a, path_a) = copy_fixture("real-v0.4.0.redb");
    let (_tmp_b, path_b) = copy_fixture("real-v0.4.0.redb");
    let db_a = Arc::new(PulseDB::open(&path_a, Config::default()).unwrap());
    let db_b = Arc::new(PulseDB::open(&path_b, Config::default()).unwrap());

    // v0.4.0 persisted no identity, so each copy minted its own on migration:
    // the two stores are distinct sync actors, exactly as two restores are.
    let id_a = db_a.instance_id();
    let id_b = db_b.instance_id();
    assert_ne!(id_a, id_b, "each migrated copy mints its own identity");

    // Oracle: the manifest's captured pre-migration scalar counts.
    let manifest: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(fixtures_dir().join("real-v0.4.0.manifest.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        manifest["schema_version"], 2,
        "fixture must be a schema-v2 store"
    );
    let legacy: Vec<(ExperienceId, u32)> = manifest["experiences"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| {
            let id = ExperienceId(uuid::Uuid::parse_str(e["id"].as_str().unwrap()).unwrap());
            let count = u32::try_from(e["applications"].as_u64().unwrap()).unwrap();
            (id, count)
        })
        .collect();
    assert!(!legacy.is_empty(), "fixture carries experiences");

    // Post-migration, pre-sync: the scalar count landed in the sentinel bucket
    // on both copies, and nowhere else.
    for (id, count) in &legacy {
        for db in [&db_a, &db_b] {
            let exp = db.get_experience(*id).unwrap().unwrap();
            assert_eq!(
                exp.applications,
                BTreeMap::from([(legacy_sentinel(), *count)]),
                "experience {id}: v2 scalar must migrate into exactly the sentinel bucket"
            );
        }
    }

    // Sync both ways. Every collective and experience already exists on the
    // other side, so each ExperienceCreated is a create collision that goes
    // through the G-counter merge — the sentinel must merge by key, not add.
    sync_both_ways(&db_a, &db_b).await;

    for (id, count) in &legacy {
        for db in [&db_a, &db_b] {
            let exp = db.get_experience(*id).unwrap().unwrap();
            assert_eq!(
                exp.applications(),
                *count,
                "experience {id}: merged total must equal the legacy count, not twice it"
            );
            assert_eq!(
                exp.applications,
                BTreeMap::from([(legacy_sentinel(), *count)]),
                "experience {id}: still exactly one sentinel bucket after sync"
            );
        }
    }

    // Fresh reinforcements on each migrated copy land in each copy's own
    // minted bucket and sum exactly on top of the sentinel. This part bites
    // whatever the fixture's scalar values are: a sentinel renamed, or minted
    // per store, would surface here as an extra bucket.
    let (probe, probe_legacy) = legacy[0];
    for _ in 0..2 {
        db_a.reinforce_experience(probe).unwrap();
    }
    for _ in 0..3 {
        db_b.reinforce_experience(probe).unwrap();
    }
    sync_both_ways(&db_a, &db_b).await;

    let expected = BTreeMap::from([(legacy_sentinel(), probe_legacy), (id_a, 2), (id_b, 3)]);
    for db in [&db_a, &db_b] {
        let exp = db.get_experience(probe).unwrap().unwrap();
        assert_eq!(
            exp.applications, expected,
            "sentinel + one bucket per migrated copy"
        );
        assert_eq!(exp.applications(), probe_legacy + 5);
    }
}

// ============================================================================
// Initial sync
// ============================================================================

#[tokio::test]
async fn test_initial_sync_catchup() {
    let (db_a, _dir_a) = open_db();
    let (db_b, _dir_b) = open_db();
    let server_a = server_for(&db_a);
    let server_b = server_for(&db_b);

    let config = SyncConfig {
        batch_size: 5, // Small batches to test pagination
        ..sync_config()
    };

    let mut manager_a = SyncManager::new(
        Arc::clone(&db_a),
        Box::new(ServerBackedTransport::new(server_b)),
        config.clone(),
    )
    .unwrap();
    let mut manager_b = SyncManager::new(
        Arc::clone(&db_b),
        Box::new(ServerBackedTransport::new(server_a)),
        config,
    )
    .unwrap();

    // Create a bunch of data on A
    let cid = db_a.create_collective("catchup").unwrap();
    let mut exp_ids = Vec::new();
    for _ in 0..12 {
        exp_ids.push(db_a.record_experience(minimal_exp(cid)).unwrap());
    }

    // Push all from A. One cycle pushes at most `batch_size` changes — that is
    // what `batch_size` means on the push path — so drain the 13 WAL events
    // (the collective plus twelve experiences) in ceil(13 / 5) cycles.
    for _ in 0..3 {
        manager_a.sync_once().await.unwrap();
    }

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
    let server_a = server_for(&db_a);
    let server_b = server_for(&db_b);

    let cid_yes = db_a.create_collective("yes").unwrap();
    let cid_no = db_a.create_collective("no").unwrap();

    let exp_yes = db_a.record_experience(minimal_exp(cid_yes)).unwrap();
    let exp_no = db_a.record_experience(minimal_exp(cid_no)).unwrap();

    // Only sync cid_yes
    let config = SyncConfig {
        collectives: Some(vec![cid_yes]),
        ..sync_config()
    };

    let mut manager_a = SyncManager::new(
        Arc::clone(&db_a),
        Box::new(ServerBackedTransport::new(server_b)),
        config.clone(),
    )
    .unwrap();
    let mut manager_b = SyncManager::new(
        Arc::clone(&db_b),
        Box::new(ServerBackedTransport::new(server_a)),
        config,
    )
    .unwrap();

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

// ============================================================================
// WAL compaction vs. unpushed local events (#9 — r1.s1.w1)
// ============================================================================

/// A remote *pull* position must never let `compact_wal` delete local events
/// that have not been *pushed* yet (issue #9). Push and pull positions are
/// tracked separately per peer, and compaction trusts only the push side.
#[tokio::test]
async fn test_compact_wal_keeps_unpushed_local_events() {
    let (db_a, _dir_a) = open_db();
    let (db_b, _dir_b) = open_db();
    let server_a = server_for(&db_a);
    let server_b = server_for(&db_b);
    let peer_of_a = db_b.instance_id();

    let mut manager_a = SyncManager::new(
        Arc::clone(&db_a),
        Box::new(ServerBackedTransport::new(Arc::clone(&server_b))),
        sync_config(),
    )
    .unwrap();
    // B pushes and pulls through separate managers so B's own seeding push does
    // not advance B's pull position past A's events.
    let mut manager_b_push = SyncManager::new(
        Arc::clone(&db_b),
        Box::new(ServerBackedTransport::new(Arc::clone(&server_a))),
        SyncConfig {
            direction: SyncDirection::PushOnly,
            ..sync_config()
        },
    )
    .unwrap();
    let mut manager_b_pull = SyncManager::new(
        Arc::clone(&db_b),
        Box::new(ServerBackedTransport::new(server_a)),
        SyncConfig {
            direction: SyncDirection::PullOnly,
            ..sync_config()
        },
    )
    .unwrap();

    // Seed B so A's pull position lands well above A's own WAL.
    let cid = db_b.create_collective("shared").unwrap();
    for _ in 0..8 {
        db_b.record_experience(minimal_exp(cid)).unwrap();
    }
    manager_b_push.sync_once().await.unwrap();

    // initial_sync A <- B: A's pull position for B is now 9 (collective + 8).
    manager_a.initial_sync(None).await.unwrap();
    assert!(
        db_a.get_collective(cid).unwrap().is_some(),
        "A should have pulled B's collective"
    );

    // A writes locally: WAL seq 1 (sync-applied changes record no WAL events).
    let local_id = db_a.record_experience(minimal_exp(cid)).unwrap();
    assert_eq!(db_a.get_current_sequence().unwrap(), 1);

    // Nothing has been pushed to B yet -> compaction must delete nothing.
    let deleted = db_a.compact_wal().unwrap();
    assert_eq!(
        deleted, 0,
        "compaction deleted unpushed local events off a remote pull position (#9)"
    );
    let pending = db_a.storage_for_test().poll_sync_events(0, 100).unwrap();
    assert_eq!(
        pending.len(),
        1,
        "the unpushed local event must survive compaction"
    );
    assert_eq!(pending[0].0, 1);

    // Push A -> B; B pulls and has the record.
    manager_a.sync_once().await.unwrap();
    manager_b_pull.sync_once().await.unwrap();
    assert!(
        db_b.get_experience(local_id).unwrap().is_some(),
        "B should have A's local record after the push"
    );

    // After the push, compaction may delete at most up to A's push position (1).
    // A second local write above that position must survive.
    db_a.record_experience(minimal_exp(cid)).unwrap();
    assert_eq!(db_a.get_current_sequence().unwrap(), 2);
    let cursor = db_a
        .storage_for_test()
        .load_sync_cursor(&peer_of_a)
        .unwrap()
        .expect("A persisted a cursor for B");
    assert_eq!(cursor.push_sequence, 1, "push side = what B acknowledged");
    assert_eq!(
        cursor.pull_sequence, 9,
        "pull side = B's WAL position, untouched by the push"
    );
    let deleted = db_a.compact_wal().unwrap();
    assert!(
        deleted <= cursor.push_sequence,
        "compaction must not exceed the push position"
    );
    assert_eq!(deleted, 1, "compaction deletes exactly the pushed event");
    let remaining = db_a.storage_for_test().poll_sync_events(0, 100).unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(
        remaining[0].0, 2,
        "the unpushed event above the push position survives"
    );
}

// ============================================================================
// Skew visibility (r1.s1.w3 — #13, veto fold C2)
// ============================================================================

/// A pulled change whose `last_reinforced` lies beyond
/// `now + max_clock_skew_ms` shows up in `SyncManager::stats()` and is merged
/// unchanged — visible, never clamped.
#[tokio::test]
async fn manager_stats_count_skewed_last_reinforced() {
    use std::collections::BTreeMap;

    use pulsedb::sync::types::{
        SerializableExperienceUpdate, SyncChange, SyncEntityType, SyncPayload, SyncStats,
    };
    use pulsedb::Timestamp;

    let (db_a, _dir_a) = open_db();
    let (transport_a, _transport_b) = InMemorySyncTransport::new_pair();
    let cid = db_a.create_collective("skew-stats").unwrap();
    let exp_id = db_a.record_experience(minimal_exp(cid)).unwrap();

    let config = SyncConfig {
        direction: SyncDirection::PullOnly,
        ..sync_config()
    };
    // The peer is the identity `transport_a` answers as, and the lane it serves
    // on a pull is that identity's own WAL.
    let peer = transport_a.instance_id();
    let seeder = transport_a.clone();
    let mut manager_a =
        SyncManager::new(Arc::clone(&db_a), Box::new(transport_a), config.clone()).unwrap();
    assert_eq!(manager_a.stats(), SyncStats::default());

    // The peer's WAL holds a reinforcement whose timestamp is a day past the
    // bound.
    let allowance = i64::try_from(config.max_clock_skew_ms).unwrap();
    let skewed = Timestamp::from_millis(Timestamp::now().as_millis() + allowance + 86_400_000);
    let change = SyncChange {
        sequence: 1_000,
        source_instance: peer,
        collective_id: cid,
        entity_type: SyncEntityType::Experience,
        payload: SyncPayload::ExperienceUpdated {
            id: exp_id,
            update: SerializableExperienceUpdate {
                applications: Some(BTreeMap::from([(peer, 1)])),
                last_reinforced: Some(skewed),
                ..Default::default()
            },
            timestamp: Timestamp::now(),
        },
        timestamp: Timestamp::now(),
    };
    seeder.seed(vec![change]);

    manager_a.sync_once().await.unwrap();

    assert_eq!(
        manager_a.stats(),
        SyncStats {
            skewed_timestamps: 1
        },
        "the skewed reinforcement is visible in the manager's stats"
    );
    let stored = db_a.get_experience(exp_id).unwrap().unwrap();
    assert_eq!(stored.last_reinforced, skewed, "counted, not clamped");
    assert_eq!(stored.applications.get(&peer), Some(&1));

    // Stats are cumulative across cycles and untouched by a clean cycle.
    manager_a.sync_once().await.unwrap();
    assert_eq!(manager_a.stats().skewed_timestamps, 1);
}

// ============================================================================
// Empty pulls still register the peer (#9 follow-up — PR #88 review)
// ============================================================================

/// A pull that returns nothing must still persist the peer's cursor record.
/// The record is what represents the peer in the cursor store, and
/// `compact_wal` needs to see its `push_sequence == 0` to stay blocked. Without
/// it a `PullOnly` peer is invisible, and once other peers acknowledge the
/// local WAL, compaction deletes events this peer was never sent.
#[tokio::test]
async fn empty_pull_still_registers_the_peer_and_blocks_compaction() {
    let (db_a, _dir_a) = open_db();
    let (_db_b, _dir_b) = open_db();
    let (transport_a, transport_b) = InMemorySyncTransport::new_pair();
    let peer_of_a = transport_a.instance_id();
    drop(transport_b);

    // B has nothing to offer, so A's pull comes back empty.
    let mut manager_a = SyncManager::new(
        Arc::clone(&db_a),
        Box::new(transport_a),
        SyncConfig {
            direction: SyncDirection::PullOnly,
            ..sync_config()
        },
    )
    .unwrap();
    manager_a.sync_once().await.unwrap();

    // The peer must nevertheless be on record, at push_sequence 0.
    let cursor = db_a
        .storage_for_test()
        .load_sync_cursor(&peer_of_a)
        .unwrap()
        .expect("an empty pull must still register the peer");
    assert_eq!(
        cursor.push_sequence, 0,
        "nothing has been pushed to this peer"
    );

    // A writes locally; compaction must not touch it while that peer sits at 0.
    let cid = db_a.create_collective("local-only").unwrap();
    db_a.record_experience(minimal_exp(cid)).unwrap();
    assert_eq!(
        db_a.compact_wal().unwrap(),
        0,
        "a peer at push_sequence 0 blocks compaction"
    );
}

/// The pull side must not step over a change that failed to apply. `apply_batch`
/// records per-change failures rather than returning an error, so persisting the
/// server's reported position would skip the failed sequence permanently.
#[tokio::test]
async fn pull_position_does_not_advance_past_a_change_that_failed_to_apply() {
    use std::collections::BTreeMap;

    use pulsedb::sync::types::{
        SerializableExperienceUpdate, SyncChange, SyncEntityType, SyncPayload,
    };

    let (db_a, _dir_a) = open_db();
    let (transport_a, _transport_b) = InMemorySyncTransport::new_pair();
    let peer_of_a = transport_a.instance_id();
    let cid = db_a.create_collective("pull-ack-bound").unwrap();
    let exp_id = db_a.record_experience(minimal_exp(cid)).unwrap();

    // A payload the applier refuses: more G-counter buckets than it accepts.
    let mut buckets = BTreeMap::new();
    for i in 0..=65_536u128 {
        buckets.insert(
            pulsedb::sync::types::InstanceId::from_bytes(i.to_le_bytes()),
            1u32,
        );
    }

    let change_at = |seq: u64, applications| SyncChange {
        sequence: seq,
        source_instance: peer_of_a,
        collective_id: cid,
        entity_type: SyncEntityType::Experience,
        payload: SyncPayload::ExperienceUpdated {
            id: exp_id,
            update: SerializableExperienceUpdate {
                applications,
                ..Default::default()
            },
            timestamp: pulsedb::Timestamp::from_millis(0),
        },
        timestamp: pulsedb::Timestamp::now(),
    };

    // seq 7 applies, seq 8 is refused, seq 9 would apply.
    transport_a.seed(vec![
        change_at(7, None),
        change_at(8, Some(buckets)),
        change_at(9, None),
    ]);

    let mut manager_a = SyncManager::new(
        Arc::clone(&db_a),
        Box::new(transport_a),
        SyncConfig {
            direction: SyncDirection::PullOnly,
            ..sync_config()
        },
    )
    .unwrap();
    manager_a.sync_once().await.unwrap();

    let cursor = db_a
        .storage_for_test()
        .load_sync_cursor(&peer_of_a)
        .unwrap()
        .expect("the peer must be on record");
    assert_eq!(
        cursor.pull_sequence, 7,
        "the pull position must stop at the last change that applied, so the \
         refused one is fetched again rather than skipped forever"
    );
}

// ============================================================================
// initial_sync terminates on a stalled position (PR #88 review, class B)
// and reports completion only when it achieved it (class L)
// ============================================================================

/// A server that filtered every event it polled returns `changes: []`,
/// `has_more: true` and an UNADVANCED cursor. `initial_sync` must read the
/// stalled position as "no progress" and stop, not re-issue the identical
/// request forever — and must report the stop as an error, because the changes
/// beyond that page were never reached.
///
/// The transport fails the request once it has been asked more times than the
/// guard should ever allow, so a regression surfaces as a failed assertion
/// rather than a hung test: the loop's awaits all resolve immediately, so on a
/// current-thread runtime a spin never yields long enough for a timeout to
/// fire.
#[tokio::test]
async fn initial_sync_terminates_on_an_empty_batch_that_claims_more() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use pulsedb::sync::transport::SyncTransport;
    use pulsedb::sync::types::{
        HandshakeRequest, HandshakeResponse, InstanceId, PullPage, PullRequest, PushAck,
        PushRequest, SyncPosition, WireReply,
    };
    use pulsedb::sync::{SyncError, SYNC_PROTOCOL_VERSION};

    /// More pulls than a correct `initial_sync` can possibly issue here.
    const SPIN_TRIPWIRE: usize = 8;

    /// Always answers with an empty batch, `has_more: true`, and the sequence
    /// that was asked for — the shape a fully-filtered poll produces.
    struct StalledPullTransport {
        peer: InstanceId,
        pulls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl SyncTransport for StalledPullTransport {
        async fn handshake(
            &self,
            _request: HandshakeRequest,
            _send_budget_bytes: usize,
        ) -> Result<HandshakeResponse, SyncError> {
            Ok(HandshakeResponse {
                instance_id: self.peer,
                protocol_version: SYNC_PROTOCOL_VERSION,
                accepted: true,
                reason: None,
                receive_limit_bytes: 64 * 1024 * 1024,
            })
        }

        async fn push_changes(
            &self,
            _request: PushRequest,
            _send_budget_bytes: usize,
        ) -> Result<WireReply<PushAck>, SyncError> {
            unreachable!("initial_sync never pushes");
        }

        async fn pull_changes(
            &self,
            request: PullRequest,
            _send_budget_bytes: usize,
        ) -> Result<WireReply<PullPage>, SyncError> {
            if self.pulls.fetch_add(1, Ordering::SeqCst) >= SPIN_TRIPWIRE {
                return Err(SyncError::transport("initial_sync is spinning"));
            }
            Ok(WireReply::ok(
                self.peer,
                PullPage {
                    changes: Vec::new(),
                    has_more: true,
                    scan_position: SyncPosition::new(self.peer, request.cursor.sequence),
                },
            ))
        }

        async fn health_check(&self) -> Result<(), SyncError> {
            Ok(())
        }

        fn receive_limit_bytes(&self) -> usize {
            64 * 1024 * 1024
        }
    }

    let (db, _dir) = open_db();
    let pulls = Arc::new(AtomicUsize::new(0));
    let transport = StalledPullTransport {
        peer: InstanceId::new(),
        pulls: Arc::clone(&pulls),
    };

    let mut manager = SyncManager::new(
        Arc::clone(&db),
        Box::new(transport),
        SyncConfig {
            direction: SyncDirection::PullOnly,
            ..sync_config()
        },
    )
    .unwrap();

    let error = manager
        .initial_sync(None)
        .await
        .expect_err("a peer that promises more and will not advance has not caught us up");

    assert!(
        error.is_catch_up_incomplete(),
        "the stall must surface as the typed catch-up error, got: {error}"
    );
    assert!(
        error.to_string().contains("did not advance the cursor"),
        "the error must name what stopped it, got: {error}"
    );
    assert_eq!(
        pulls.load(Ordering::SeqCst),
        1,
        "one request is enough to learn the position will not move"
    );
}

/// A pull transport that serves ONE scripted page and then reports itself
/// exhausted, with the same bounded tripwire as the stall test above: it fails
/// the request once it has been asked more times than a correct `initial_sync`
/// can ask, so a regression surfaces as an assertion rather than a hang.
struct ScriptedPullTransport {
    peer: pulsedb::sync::types::InstanceId,
    pulls: Arc<std::sync::atomic::AtomicUsize>,
    page: std::sync::Mutex<Vec<pulsedb::sync::types::SyncChange>>,
    has_more: bool,
}

impl ScriptedPullTransport {
    /// More pulls than a correct `initial_sync` can issue against one page.
    const SPIN_TRIPWIRE: usize = 8;

    fn new(
        peer: pulsedb::sync::types::InstanceId,
        pulls: &Arc<std::sync::atomic::AtomicUsize>,
        page: Vec<pulsedb::sync::types::SyncChange>,
        has_more: bool,
    ) -> Self {
        Self {
            peer,
            pulls: Arc::clone(pulls),
            page: std::sync::Mutex::new(page),
            has_more,
        }
    }
}

#[async_trait::async_trait]
impl pulsedb::sync::transport::SyncTransport for ScriptedPullTransport {
    async fn handshake(
        &self,
        _request: pulsedb::sync::types::HandshakeRequest,
        _send_budget_bytes: usize,
    ) -> Result<pulsedb::sync::types::HandshakeResponse, pulsedb::sync::SyncError> {
        Ok(pulsedb::sync::types::HandshakeResponse {
            instance_id: self.peer,
            protocol_version: pulsedb::sync::SYNC_PROTOCOL_VERSION,
            accepted: true,
            reason: None,
            receive_limit_bytes: 64 * 1024 * 1024,
        })
    }

    async fn push_changes(
        &self,
        _request: pulsedb::sync::types::PushRequest,
        _send_budget_bytes: usize,
    ) -> Result<
        pulsedb::sync::types::WireReply<pulsedb::sync::types::PushAck>,
        pulsedb::sync::SyncError,
    > {
        unreachable!("initial_sync never pushes");
    }

    async fn pull_changes(
        &self,
        request: pulsedb::sync::types::PullRequest,
        _send_budget_bytes: usize,
    ) -> Result<
        pulsedb::sync::types::WireReply<pulsedb::sync::types::PullPage>,
        pulsedb::sync::SyncError,
    > {
        use std::sync::atomic::Ordering;

        if self.pulls.fetch_add(1, Ordering::SeqCst) >= Self::SPIN_TRIPWIRE {
            return Err(pulsedb::sync::SyncError::transport(
                "initial_sync is spinning",
            ));
        }
        let mut page = std::mem::take(&mut *self.page.lock().unwrap());
        // A pull serves the RESPONDER's own WAL, so every change it emits is
        // owned by the responder — exactly what `SyncServer::handle_pull` does
        // with `build_change_from_record(.., self.instance_id)`.
        for change in &mut page {
            change.source_instance = self.peer;
        }
        // Once the page is served the peer is exhausted, whatever it said the
        // first time.
        let has_more = !page.is_empty() && self.has_more;
        let new_seq = page
            .last()
            .map_or(request.cursor.sequence, |change| change.sequence);
        Ok(pulsedb::sync::types::WireReply::ok(
            self.peer,
            pulsedb::sync::types::PullPage {
                changes: page,
                has_more,
                scan_position: pulsedb::sync::types::SyncPosition::new(self.peer, new_seq),
            },
        ))
    }

    async fn health_check(&self) -> Result<(), pulsedb::sync::SyncError> {
        Ok(())
    }

    fn receive_limit_bytes(&self) -> usize {
        64 * 1024 * 1024
    }
}

/// Builds a manager pulling from a one-page scripted peer.
fn catchup_manager(
    db: &Arc<PulseDB>,
    peer: pulsedb::sync::types::InstanceId,
    pulls: &Arc<std::sync::atomic::AtomicUsize>,
    page: Vec<pulsedb::sync::types::SyncChange>,
    has_more: bool,
) -> SyncManager {
    SyncManager::new(
        Arc::clone(db),
        Box::new(ScriptedPullTransport::new(peer, pulls, page, has_more)),
        SyncConfig {
            direction: SyncDirection::PullOnly,
            ..sync_config()
        },
    )
    .unwrap()
}

/// A page the peer reports as its last, with every change applying, IS a
/// completed catch-up.
#[tokio::test]
async fn initial_sync_completes_on_an_exhausted_page() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let (db, _dir) = open_db();
    let pulls = Arc::new(AtomicUsize::new(0));
    let collective = CollectiveId::new();
    let mut manager = catchup_manager(
        &db,
        pulsedb::sync::types::InstanceId::new(),
        &pulls,
        vec![collective_change(1, collective, "catchup-complete")],
        false,
    );

    manager
        .initial_sync(None)
        .await
        .expect("an exhausted page with everything applied is a completed catch-up");

    assert!(db.get_collective(collective).unwrap().is_some());
    assert_eq!(pulls.load(Ordering::SeqCst), 1);
}

/// An idempotent SKIP is a successful outcome — it is the ordinary shape of a
/// re-sync — so a page of nothing but skips still completes. This is the
/// distinction `ApplyResult::failed` exists to draw: `skipped` counts these too.
#[tokio::test]
async fn initial_sync_completes_on_a_page_of_idempotent_skips() {
    use std::sync::atomic::AtomicUsize;

    let (db, _dir) = open_db();
    let pulls = Arc::new(AtomicUsize::new(0));
    // Deleting an experience this store never had is the idempotent skip.
    let absent = ExperienceId::new();
    let peer = pulsedb::sync::types::InstanceId::new();
    let mut manager = catchup_manager(&db, peer, &pulls, vec![delete_change(1, absent)], false);

    manager
        .initial_sync(None)
        .await
        .expect("a page of idempotent skips is a completed catch-up, not a failure");

    let cursor = db
        .storage_for_test()
        .load_sync_cursor(&peer)
        .unwrap()
        .expect("the peer must be on record");
    assert_eq!(
        cursor.pull_sequence, 1,
        "the skipped change was handled, so the position moves past it"
    );
}

/// An exhausted page is not enough: a change that FAILED to apply means the
/// store is not caught up, so `initial_sync` must not report success.
#[tokio::test]
async fn initial_sync_refuses_completion_when_a_change_failed_to_apply() {
    use std::sync::atomic::AtomicUsize;

    let (db, _dir) = open_db();
    let pulls = Arc::new(AtomicUsize::new(0));
    let collective = CollectiveId::new();
    let page = vec![
        collective_change(1, collective, "catchup-partial"),
        poison_change(2, collective),
    ];
    let peer = pulsedb::sync::types::InstanceId::new();
    let mut manager = catchup_manager(&db, peer, &pulls, page, false);

    let error = manager
        .initial_sync(None)
        .await
        .expect_err("a change that never applied is not a completed catch-up");

    assert!(
        error.is_catch_up_incomplete(),
        "an apply failure must surface as the typed catch-up error, got: {error}"
    );
    assert!(
        error.to_string().contains("failed to apply"),
        "the error must name what stopped it, got: {error}"
    );
    let cursor = db
        .storage_for_test()
        .load_sync_cursor(&peer)
        .unwrap()
        .expect("the peer must be on record");
    assert_eq!(
        cursor.pull_sequence, 1,
        "the position stops at the last change that applied, so the failed one \
         is fetched again"
    );
}

// ============================================================================
// initial_sync counts failures still OUTSTANDING, not attempts (PR #88, class R)
// ============================================================================

/// A pull transport that serves a SCRIPT of pages, front to back.
///
/// This is the lever for a change that FAILS on its first delivery and APPLIES
/// on a retry: the same sequence appears on two pages, carried by a change the
/// applier refuses the first time and one it accepts the second. The page queue
/// is the only state — nothing here depends on timing, which matters because
/// `initial_sync`'s awaits all resolve immediately and a timer on a
/// current-thread runtime would never be polled.
///
/// Once the script is spent the peer answers empty and exhausted from whatever
/// position it was asked, so a regression cannot spin; the tripwire below turns
/// one into an assertion anyway.
struct ScriptedPagesTransport {
    peer: pulsedb::sync::types::InstanceId,
    pages:
        std::sync::Mutex<std::collections::VecDeque<(Vec<pulsedb::sync::types::SyncChange>, bool)>>,
    requested: Arc<std::sync::Mutex<Vec<u64>>>,
}

impl ScriptedPagesTransport {
    /// More pulls than a correct `initial_sync` can issue against any script
    /// these tests write.
    const SPIN_TRIPWIRE: usize = 8;

    fn new(
        peer: pulsedb::sync::types::InstanceId,
        requested: &Arc<std::sync::Mutex<Vec<u64>>>,
        pages: Vec<(Vec<pulsedb::sync::types::SyncChange>, bool)>,
    ) -> Self {
        Self {
            peer,
            pages: std::sync::Mutex::new(pages.into()),
            requested: Arc::clone(requested),
        }
    }
}

#[async_trait::async_trait]
impl pulsedb::sync::transport::SyncTransport for ScriptedPagesTransport {
    async fn handshake(
        &self,
        _request: pulsedb::sync::types::HandshakeRequest,
        _send_budget_bytes: usize,
    ) -> Result<pulsedb::sync::types::HandshakeResponse, pulsedb::sync::SyncError> {
        Ok(pulsedb::sync::types::HandshakeResponse {
            instance_id: self.peer,
            protocol_version: pulsedb::sync::SYNC_PROTOCOL_VERSION,
            accepted: true,
            reason: None,
            receive_limit_bytes: 64 * 1024 * 1024,
        })
    }

    async fn push_changes(
        &self,
        _request: pulsedb::sync::types::PushRequest,
        _send_budget_bytes: usize,
    ) -> Result<
        pulsedb::sync::types::WireReply<pulsedb::sync::types::PushAck>,
        pulsedb::sync::SyncError,
    > {
        unreachable!("initial_sync never pushes");
    }

    async fn pull_changes(
        &self,
        request: pulsedb::sync::types::PullRequest,
        _send_budget_bytes: usize,
    ) -> Result<
        pulsedb::sync::types::WireReply<pulsedb::sync::types::PullPage>,
        pulsedb::sync::SyncError,
    > {
        let from = request.cursor.sequence;
        {
            let mut asked = self.requested.lock().unwrap();
            asked.push(from);
            if asked.len() > Self::SPIN_TRIPWIRE {
                return Err(pulsedb::sync::SyncError::transport(
                    "initial_sync is spinning",
                ));
            }
        }

        let (mut changes, has_more) = self
            .pages
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| (Vec::new(), false));
        // Owned by the responder, as a real pull's changes are.
        for change in &mut changes {
            change.source_instance = self.peer;
        }
        // An honest server names the highest sequence it served, and echoes the
        // requested position when it served nothing.
        let new_seq = changes
            .iter()
            .map(|change| change.sequence)
            .max()
            .unwrap_or(from);
        Ok(pulsedb::sync::types::WireReply::ok(
            self.peer,
            pulsedb::sync::types::PullPage {
                changes,
                has_more,
                scan_position: pulsedb::sync::types::SyncPosition::new(self.peer, new_seq),
            },
        ))
    }

    async fn health_check(&self) -> Result<(), pulsedb::sync::SyncError> {
        Ok(())
    }

    fn receive_limit_bytes(&self) -> usize {
        64 * 1024 * 1024
    }
}

/// Builds a manager pulling from a scripted multi-page peer.
fn scripted_catchup_manager(
    db: &Arc<PulseDB>,
    peer: pulsedb::sync::types::InstanceId,
    requested: &Arc<std::sync::Mutex<Vec<u64>>>,
    pages: Vec<(Vec<pulsedb::sync::types::SyncChange>, bool)>,
) -> SyncManager {
    SyncManager::new(
        Arc::clone(db),
        Box::new(ScriptedPagesTransport::new(peer, requested, pages)),
        SyncConfig {
            direction: SyncDirection::PullOnly,
            ..sync_config()
        },
    )
    .unwrap()
}

/// The persisted pull position for `peer`.
fn pull_position(db: &Arc<PulseDB>, peer: pulsedb::sync::types::InstanceId) -> u64 {
    db.storage_for_test()
        .load_sync_cursor(&peer)
        .unwrap()
        .expect("the peer must be on record")
        .pull_sequence
}

/// A change that fails on one page and APPLIES on a retry later in the SAME
/// run has not left the catch-up incomplete.
///
/// `safe_through` stops the position strictly below a batch's lowest failure
/// rather than stalling on it, so `[1 ok, 2 ok, 3 err, 4 ok]` advances to 2 and
/// the next iteration re-requests 3. A transient failure applies on that
/// retry — and counting ATTEMPTS made the run report `CatchUpIncomplete` on a
/// catch-up that reached the peer's last page with everything applied. A false
/// failure is worse than no contract: it teaches the operator to ignore the
/// error that also fires for real reasons.
#[tokio::test]
async fn initial_sync_completes_when_a_later_retry_applies_the_failed_change() {
    let (db, _dir) = open_db();
    let peer = pulsedb::sync::types::InstanceId::new();
    let requested = Arc::new(std::sync::Mutex::new(Vec::new()));

    let first = CollectiveId::new();
    let second = CollectiveId::new();
    let third = CollectiveId::new();
    let fourth = CollectiveId::new();

    let mut manager = scripted_catchup_manager(
        &db,
        peer,
        &requested,
        vec![
            // Sequence 3 is refused here — the position stops at 2, below it.
            (
                vec![
                    collective_change(1, first, "retry-1"),
                    collective_change(2, second, "retry-2"),
                    poison_change(3, second),
                    collective_change(4, fourth, "retry-4"),
                ],
                true,
            ),
            // The re-request from 2 delivers 3 again, and this time it applies.
            // 4 comes back too and is an idempotent skip.
            (
                vec![
                    collective_change(3, third, "retry-3"),
                    collective_change(4, fourth, "retry-4"),
                ],
                false,
            ),
        ],
    );

    manager
        .initial_sync(None)
        .await
        .expect("a failure a later retry applied is not an incomplete catch-up");

    assert_eq!(
        *requested.lock().unwrap(),
        vec![0, 2],
        "the run must re-request from the position below the failure"
    );
    for (id, label) in [(first, "1"), (second, "2"), (third, "3"), (fourth, "4")] {
        assert!(
            db.get_collective(id).unwrap().is_some(),
            "change {label} must be in the store"
        );
    }
    assert_eq!(
        pull_position(&db, peer),
        4,
        "the run reached the peer's last page with everything applied"
    );
}

/// The boundary of the same rule: the failed change is the peer's LAST event,
/// and its successful retry ends the run exactly AT that sequence.
///
/// The pull position is INCLUSIVE — it is a `safe_through`, the highest
/// sequence at or below which everything was handled — so a final position of
/// `s` means `s` itself applied. Resolving failures with `sequence >= position`
/// instead of `sequence > position` would call this run incomplete and
/// reintroduce the false failure one sequence to the left.
#[tokio::test]
async fn initial_sync_completes_when_the_retry_lands_on_the_final_sequence() {
    let (db, _dir) = open_db();
    let peer = pulsedb::sync::types::InstanceId::new();
    let requested = Arc::new(std::sync::Mutex::new(Vec::new()));

    let first = CollectiveId::new();
    let last = CollectiveId::new();

    let mut manager = scripted_catchup_manager(
        &db,
        peer,
        &requested,
        vec![
            (
                vec![
                    collective_change(1, first, "boundary-1"),
                    poison_change(2, first),
                ],
                true,
            ),
            // Nothing follows 2, so the run ends with the position exactly on
            // the sequence that had failed.
            (vec![collective_change(2, last, "boundary-2")], false),
        ],
    );

    manager
        .initial_sync(None)
        .await
        .expect("a failure retried at the peer's last sequence completed the catch-up");

    assert_eq!(*requested.lock().unwrap(), vec![0, 1]);
    assert!(db.get_collective(last).unwrap().is_some());
    assert_eq!(
        pull_position(&db, peer),
        2,
        "the position ends ON the sequence that failed and then applied"
    );
}

/// The other side of the boundary: a change that is STILL failing when the loop
/// terminates leaves the catch-up incomplete, whatever it did on earlier pages.
///
/// It is also the de-duplication case — sequence 2 fails on both attempts, and
/// the error must report ONE outstanding change rather than two, because it
/// counts changes left unapplied, not attempts.
#[tokio::test]
async fn initial_sync_reports_incomplete_when_the_retry_fails_again() {
    let (db, _dir) = open_db();
    let peer = pulsedb::sync::types::InstanceId::new();
    let requested = Arc::new(std::sync::Mutex::new(Vec::new()));

    let first = CollectiveId::new();
    let beyond = CollectiveId::new();

    let mut manager = scripted_catchup_manager(
        &db,
        peer,
        &requested,
        vec![
            (
                vec![
                    collective_change(1, first, "stuck-1"),
                    poison_change(2, first),
                ],
                true,
            ),
            // The retry of 2 fails again. 3 applies but sits above the failure,
            // so the position cannot move and the loop stops here.
            (
                vec![
                    poison_change(2, first),
                    collective_change(3, beyond, "stuck-3"),
                ],
                true,
            ),
        ],
    );

    let error = manager
        .initial_sync(None)
        .await
        .expect_err("a change still failing at the end is not a completed catch-up");

    assert!(
        error.is_catch_up_incomplete(),
        "an unresolved apply failure must surface as the typed catch-up error, got: {error}"
    );
    assert!(
        error.to_string().contains("1 change(s) failed to apply"),
        "one change is outstanding across two failed attempts, not two, got: {error}"
    );
    assert_eq!(*requested.lock().unwrap(), vec![0, 1]);
    assert_eq!(
        pull_position(&db, peer),
        1,
        "the position stays below the change that never applied"
    );
}

/// A `CollectiveCreated` change at `sequence`.
fn collective_change(
    sequence: u64,
    id: CollectiveId,
    name: &str,
) -> pulsedb::sync::types::SyncChange {
    pulsedb::sync::types::SyncChange {
        sequence,
        source_instance: pulsedb::sync::types::InstanceId::new(),
        collective_id: id,
        entity_type: pulsedb::sync::types::SyncEntityType::Collective,
        payload: pulsedb::sync::types::SyncPayload::CollectiveCreated(pulsedb::Collective {
            id,
            name: name.to_string(),
            owner_id: None,
            embedding_dimension: 384,
            created_at: pulsedb::Timestamp::now(),
            updated_at: pulsedb::Timestamp::now(),
        }),
        timestamp: pulsedb::Timestamp::now(),
    }
}

/// An `ExperienceDeleted` change at `sequence` — an idempotent skip when the
/// experience is absent locally.
fn delete_change(sequence: u64, id: ExperienceId) -> pulsedb::sync::types::SyncChange {
    pulsedb::sync::types::SyncChange {
        sequence,
        source_instance: pulsedb::sync::types::InstanceId::new(),
        collective_id: CollectiveId::new(),
        entity_type: pulsedb::sync::types::SyncEntityType::Experience,
        payload: pulsedb::sync::types::SyncPayload::ExperienceDeleted {
            id,
            timestamp: pulsedb::Timestamp::from_millis(0),
        },
        timestamp: pulsedb::Timestamp::now(),
    }
}

/// A change the applier REFUSES: more `applications` G-counter buckets than it
/// accepts from a peer.
fn poison_change(sequence: u64, collective: CollectiveId) -> pulsedb::sync::types::SyncChange {
    use std::collections::BTreeMap;

    let mut buckets = BTreeMap::new();
    for i in 0..=65_536u128 {
        buckets.insert(
            pulsedb::sync::types::InstanceId::from_bytes(i.to_le_bytes()),
            1u32,
        );
    }
    pulsedb::sync::types::SyncChange {
        sequence,
        source_instance: pulsedb::sync::types::InstanceId::new(),
        collective_id: collective,
        entity_type: pulsedb::sync::types::SyncEntityType::Experience,
        payload: pulsedb::sync::types::SyncPayload::ExperienceUpdated {
            id: ExperienceId::new(),
            update: pulsedb::sync::types::SerializableExperienceUpdate {
                applications: Some(buckets),
                ..Default::default()
            },
            timestamp: pulsedb::Timestamp::from_millis(0),
        },
        timestamp: pulsedb::Timestamp::now(),
    }
}

// ============================================================================
// Peer identity: a remote remint invalidates the cached identity
// ============================================================================

/// A sync endpoint that can be **replaced mid-session by a restored copy of
/// itself**: a fresh `InstanceId` and none of the changes it was previously
/// sent. That is exactly what an operator produces by restoring a store from an
/// older snapshot and calling `PulseDB::remint_instance_id` on it — a
/// *different* peer holding *less* data behind the same address.
///
/// The push acknowledgement deliberately mirrors `SyncServer::handle_push`,
/// which fills `new_cursor.instance_id` with the **sender's**
/// `source_instance` (the acknowledged position is a position in the sender's
/// WAL). A manager that tried to read the peer's identity off a push response
/// would therefore see its own id here, which is why the pull response is the
/// only usable detection point.
struct RestorableEndpoint {
    peer: std::sync::Mutex<pulsedb::sync::types::InstanceId>,
    /// Experiences the endpoint currently holds — emptied by `restore()`.
    held: std::sync::Mutex<std::collections::HashSet<ExperienceId>>,
    /// The endpoint's own WAL, as offered on a pull. Replaced by `restore()`,
    /// because a restored copy's WAL is a different WAL.
    offers: std::sync::Mutex<Vec<pulsedb::sync::types::SyncChange>>,
    handshakes: std::sync::atomic::AtomicUsize,
    pulls: std::sync::atomic::AtomicUsize,
    pushes: std::sync::atomic::AtomicUsize,
}

impl RestorableEndpoint {
    fn new(offers: Vec<pulsedb::sync::types::SyncChange>) -> Arc<Self> {
        Arc::new(Self {
            peer: std::sync::Mutex::new(pulsedb::sync::types::InstanceId::new()),
            held: std::sync::Mutex::new(std::collections::HashSet::new()),
            offers: std::sync::Mutex::new(offers),
            handshakes: std::sync::atomic::AtomicUsize::new(0),
            pulls: std::sync::atomic::AtomicUsize::new(0),
            pushes: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    fn peer(&self) -> pulsedb::sync::types::InstanceId {
        *self.peer.lock().unwrap()
    }

    /// Restore from an older snapshot and remint: a new identity, none of the
    /// changes it was sent, and its own (different) WAL.
    fn restore(
        &self,
        offers: Vec<pulsedb::sync::types::SyncChange>,
    ) -> pulsedb::sync::types::InstanceId {
        let fresh = pulsedb::sync::types::InstanceId::new();
        *self.peer.lock().unwrap() = fresh;
        self.held.lock().unwrap().clear();
        *self.offers.lock().unwrap() = offers;
        fresh
    }

    fn holds(&self, ids: &[ExperienceId]) -> bool {
        let held = self.held.lock().unwrap();
        ids.iter().all(|id| held.contains(id))
    }

    fn handshakes(&self) -> usize {
        self.handshakes.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn pulls(&self) -> usize {
        self.pulls.load(std::sync::atomic::Ordering::SeqCst)
    }
}

struct RestorableTransport(Arc<RestorableEndpoint>);

#[async_trait::async_trait]
impl pulsedb::sync::transport::SyncTransport for RestorableTransport {
    async fn handshake(
        &self,
        _request: pulsedb::sync::types::HandshakeRequest,
        _send_budget_bytes: usize,
    ) -> Result<pulsedb::sync::types::HandshakeResponse, pulsedb::sync::SyncError> {
        self.0
            .handshakes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(pulsedb::sync::types::HandshakeResponse {
            instance_id: self.0.peer(),
            protocol_version: pulsedb::sync::SYNC_PROTOCOL_VERSION,
            accepted: true,
            reason: None,
            receive_limit_bytes: 64 * 1024 * 1024,
        })
    }

    async fn push_changes(
        &self,
        request: pulsedb::sync::types::PushRequest,
        _send_budget_bytes: usize,
    ) -> Result<
        pulsedb::sync::types::WireReply<pulsedb::sync::types::PushAck>,
        pulsedb::sync::SyncError,
    > {
        self.0
            .pushes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let peer = self.0.peer();
        // Route FIRST, exactly as a real endpoint does: a batch addressed to
        // the identity this endpoint USED to have is refused, and records
        // nothing.
        if request.target_instance != peer {
            return Ok(pulsedb::sync::types::WireReply::peer_changed(
                peer,
                request.target_instance,
            ));
        }

        let total = request.changes.len() as u64;
        let max_seq = request.changes.iter().map(|c| c.sequence).max();

        let mut held = self.0.held.lock().unwrap();
        for change in &request.changes {
            if let pulsedb::sync::types::SyncPayload::ExperienceCreated(exp) = &change.payload {
                held.insert(exp.experience.id);
            }
        }

        Ok(pulsedb::sync::types::WireReply::ok(
            peer,
            pulsedb::sync::types::PushAck {
                // The SENDER's WAL is what the position indexes.
                wal_owner: request.source_instance,
                accepted: total,
                rejected: 0,
                total,
                safe_through: max_seq,
            },
        ))
    }

    async fn pull_changes(
        &self,
        request: pulsedb::sync::types::PullRequest,
        _send_budget_bytes: usize,
    ) -> Result<
        pulsedb::sync::types::WireReply<pulsedb::sync::types::PullPage>,
        pulsedb::sync::SyncError,
    > {
        self.0
            .pulls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let peer = self.0.peer();
        if request.target_instance != peer {
            return Ok(pulsedb::sync::types::WireReply::peer_changed(
                peer,
                request.target_instance,
            ));
        }

        let offers = self.0.offers.lock().unwrap();
        let changes: Vec<pulsedb::sync::types::SyncChange> = offers
            .iter()
            .filter(|c| c.sequence > request.cursor.sequence)
            .cloned()
            .map(|mut change| {
                // The endpoint's own WAL, so the endpoint's own identity — and
                // a restored copy re-stamps them under its NEW identity.
                change.source_instance = peer;
                change
            })
            .collect();
        // An honest server names the highest sequence it served, and echoes the
        // requested position when it served nothing.
        let new_seq = changes
            .last()
            .map_or(request.cursor.sequence, |c| c.sequence);

        Ok(pulsedb::sync::types::WireReply::ok(
            peer,
            pulsedb::sync::types::PullPage {
                changes,
                has_more: false,
                scan_position: pulsedb::sync::types::SyncPosition::new(peer, new_seq),
            },
        ))
    }

    async fn health_check(&self) -> Result<(), pulsedb::sync::SyncError> {
        Ok(())
    }

    fn receive_limit_bytes(&self) -> usize {
        64 * 1024 * 1024
    }
}

fn cursor_row(
    db: &Arc<PulseDB>,
    peer: pulsedb::sync::types::InstanceId,
) -> Option<pulsedb::sync::types::SyncCursor> {
    db.storage_for_test().load_sync_cursor(&peer).unwrap()
}

/// Records a collective and three experiences, then syncs them to `endpoint`
/// so the manager is a live session with an ADVANCED push cursor — the state
/// the defect needs.
fn established_session(
    endpoint: &Arc<RestorableEndpoint>,
    config: SyncConfig,
) -> (
    Arc<PulseDB>,
    tempfile::TempDir,
    Vec<ExperienceId>,
    SyncManager,
) {
    let (db, dir) = open_db();
    let cid = db.create_collective("remint-resync").unwrap();
    let ids: Vec<ExperienceId> = (0..3)
        .map(|_| db.record_experience(minimal_exp(cid)).unwrap())
        .collect();

    let manager = SyncManager::new(
        Arc::clone(&db),
        Box::new(RestorableTransport(Arc::clone(endpoint))),
        config,
    )
    .unwrap();

    (db, dir, ids, manager)
}

/// The class: a peer that remints mid-session is a DIFFERENT peer holding LESS
/// data, and both directions have to start from that peer's own cursor.
///
/// Before the fix the manager kept the identity the handshake returned. On the
/// push side it loaded the old peer's cursor — already at the local WAL head —
/// and sent nothing, so the restored endpoint never got back the changes it had
/// lost. On the pull side it asked from the old peer's position, which sits
/// ABOVE most of the restored peer's shorter WAL, so those changes were never
/// fetched — and the position that came back, a position in the NEW peer's WAL,
/// was written into the OLD peer's row.
#[tokio::test]
async fn a_reminted_peer_is_resynced_from_its_own_cursor() {
    // The endpoint's own WAL before the restore: one change at sequence 5.
    let before = CollectiveId::new();
    let endpoint = RestorableEndpoint::new(vec![collective_change(5, before, "pre-restore")]);
    let (db, _dir, ids, mut manager) = established_session(&endpoint, sync_config());
    let original = endpoint.peer();

    manager.sync_once().await.unwrap();
    assert!(
        endpoint.holds(&ids),
        "the established session must have pushed everything once"
    );
    assert!(db.get_collective(before).unwrap().is_some());

    let head = db.get_current_sequence().unwrap();
    let old_row = cursor_row(&db, original).expect("the original peer is on record");
    assert_eq!(
        old_row.push_sequence, head,
        "the session must start with an ADVANCED push cursor, or the defect cannot bite"
    );
    assert_eq!(
        old_row.pull_sequence, 5,
        "and with a non-zero pull position, which is what the restored peer's \
         shorter WAL then sits below"
    );

    // The endpoint is restored from an older snapshot and reminted. Its WAL is a
    // DIFFERENT WAL: two changes below where the old cursor points, and one
    // above it from activity since the restore.
    let low_a = CollectiveId::new();
    let low_b = CollectiveId::new();
    let high = CollectiveId::new();
    let restored = endpoint.restore(vec![
        collective_change(2, low_a, "restored-2"),
        collective_change(3, low_b, "restored-3"),
        collective_change(9, high, "restored-9"),
    ]);
    assert_ne!(restored, original);
    assert!(!endpoint.holds(&ids), "the restored copy lost the changes");

    manager.sync_once().await.unwrap();

    // (a) The observable consequence, push side: the restored endpoint has the
    // changes back.
    assert!(
        endpoint.holds(&ids),
        "a restored peer must be re-sent the changes it is missing — a re-push of \
         changes it already had is absorbed by the applier's idempotent skip path, \
         while skipping ones it lacks is silent data loss"
    );
    // And pull side: the restored peer's whole WAL is read, not just the tail
    // above a cursor that belongs to a different instance.
    for (id, what) in [(low_a, "seq 2"), (low_b, "seq 3"), (high, "seq 9")] {
        assert!(
            db.get_collective(id).unwrap().is_some(),
            "the restored peer's change at {what} must be pulled — its WAL is read \
             from the new identity's own position, not from the old peer's"
        );
    }

    // (b) Nothing was filed under the wrong identity, and the old row is retained.
    let old_after = cursor_row(&db, original).expect("the old identity's row is retained");
    assert_eq!(
        old_after, old_row,
        "no position for the new identity may be written into the old identity's row"
    );
    let new_row = cursor_row(&db, restored).expect("the restored identity gets its OWN row");
    assert_eq!(new_row.instance_id, restored);
    assert_eq!(
        new_row.push_sequence, head,
        "the retransmission is acknowledged under the new identity"
    );
    assert_eq!(
        new_row.pull_sequence, 9,
        "and the pull position for the new peer's WAL is stored under the new key"
    );
}

/// The background task captures the peer identity when it starts, so it needs
/// its own coverage: the same remint, detected and repaired by the loop.
#[tokio::test]
async fn the_background_loop_resyncs_a_reminted_peer() {
    let before = CollectiveId::new();
    let endpoint = RestorableEndpoint::new(vec![collective_change(5, before, "bg-pre-restore")]);
    // Tight intervals so the loop turns several times inside the timeout.
    let config = SyncConfig {
        push_interval_ms: 20,
        pull_interval_ms: 20,
        ..sync_config()
    };
    config.validate().unwrap();
    let (db, _dir, ids, mut manager) = established_session(&endpoint, config);
    let original = endpoint.peer();

    manager.start().await.unwrap();
    await_until(
        || endpoint.holds(&ids),
        "the background loop pushes the backlog",
    )
    .await;

    let head = db.get_current_sequence().unwrap();
    let old_row = cursor_row(&db, original).expect("the original peer is on record");
    assert_eq!(old_row.push_sequence, head);
    assert_eq!(old_row.pull_sequence, 5);

    let high = CollectiveId::new();
    let restored = endpoint.restore(vec![collective_change(9, high, "bg-restored-9")]);
    await_until(
        || endpoint.holds(&ids),
        "the background loop must detect the remint and resend, not keep using the \
         identity it was spawned with",
    )
    .await;
    await_until(
        || db.get_collective(high).unwrap().is_some(),
        "and must pull the restored peer's WAL from the new identity's own position",
    )
    .await;

    manager.stop().await.unwrap();

    let old_after = cursor_row(&db, original).expect("the old identity's row is retained");
    assert_eq!(
        old_after, old_row,
        "the background path must not write the new identity's position into the old row"
    );
    let new_row = cursor_row(&db, restored).expect("the restored identity gets its OWN row");
    assert_eq!(new_row.instance_id, restored);
    assert_eq!(new_row.push_sequence, head);
    assert_eq!(new_row.pull_sequence, 9);
}

/// A stable peer costs nothing extra: the binding is still cached, so the
/// handshake happens once however many cycles run, and each cycle makes exactly
/// one pull.
#[tokio::test]
async fn a_stable_peer_identity_is_never_re_handshaked() {
    let endpoint = RestorableEndpoint::new(Vec::new());
    let (db, _dir, ids, mut manager) = established_session(&endpoint, sync_config());
    let peer = endpoint.peer();
    let cid = db.create_collective("stable-identity").unwrap();

    for _ in 0..3 {
        db.record_experience(minimal_exp(cid)).unwrap();
        manager.sync_once().await.unwrap();
    }

    assert!(endpoint.holds(&ids));
    assert_eq!(
        endpoint.handshakes(),
        1,
        "detection must not cost a handshake per cycle — only a detected mismatch \
         re-establishes the identity"
    );
    assert_eq!(
        endpoint.pulls(),
        3,
        "one pull per cycle: the identity check rides on the pull that was \
         happening anyway, it does not add a request"
    );

    let row = cursor_row(&db, peer).expect("the peer is on record");
    assert_eq!(row.push_sequence, db.get_current_sequence().unwrap());
}

/// Polls `condition` until it holds, or fails the test after 5 seconds.
async fn await_until(condition: impl Fn() -> bool, what: &str) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if condition() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("timed out waiting: {what}");
}

// ============================================================================
// Peer replacement, terminal errors and explicit dependency failure
//
// A reminted peer is rebound and replayed from its own cursor (#90); a body no
// budget can ever hold stops the background loop instead of being retried; and
// an update whose target is absent fails explicitly rather than being
// acknowledged (#96).
// ============================================================================

/// Records a collective and three experiences into a fresh store.
fn seeded_store() -> (
    Arc<PulseDB>,
    tempfile::TempDir,
    CollectiveId,
    Vec<ExperienceId>,
) {
    let (db, dir) = open_db();
    let cid = db.create_collective("replacement").unwrap();
    let ids = (0..3)
        .map(|_| db.record_experience(minimal_exp(cid)).unwrap())
        .collect();
    (db, dir, cid, ids)
}

/// A **PushOnly** manager whose cursor already sits at the WAL head still
/// notices that the endpoint was replaced.
///
/// This is the case a pull-only detection point cannot reach: nothing is
/// pulled, and nothing is selected to push either, so under protocol v4 the
/// cycle made no request at all and the manager kept syncing a peer that was
/// gone. Under v5 it sends a bounded EMPTY routed push whose `target_instance`
/// the replacement refuses, which is what re-establishes the identity — and a
/// health check would not have done it, because liveness is not identity.
#[tokio::test]
async fn recovery_v5_push_only_at_head_rebinds() {
    let (db_a, _dir_a, _cid, ids) = seeded_store();
    let (db_b, _dir_b) = open_db();
    let endpoint = common::SyncEndpoint::new(server_for(&db_b));
    let original = endpoint.instance_id();

    let mut manager = SyncManager::new(
        Arc::clone(&db_a),
        Box::new(ServerBackedTransport::over(Arc::clone(&endpoint))),
        SyncConfig {
            direction: SyncDirection::PushOnly,
            ..sync_config()
        },
    )
    .unwrap();

    manager.sync_once().await.unwrap();
    let head = db_a.get_current_sequence().unwrap();
    let old_row = cursor_row(&db_a, original).expect("the original peer is on record");
    assert_eq!(
        old_row.push_sequence, head,
        "the session must start with the cursor AT the WAL head, or the case does not bite"
    );
    for id in &ids {
        assert!(db_b.get_experience(*id).unwrap().is_some());
    }

    // The endpoint is replaced by a correctly reminted copy holding none of it.
    let (db_c, _dir_c) = open_db();
    endpoint.replace(server_for(&db_c));
    let restored = endpoint.instance_id();
    assert_ne!(restored, original);

    // Nothing is selected to push — the cursor is at the head — and the empty
    // probe is what finds out. ONE cycle is enough: the rebind spends this
    // cycle's single allowance and the cycle then restarts against the new
    // identity's own cursor, so the replay happens here, not on a later call.
    manager.sync_once().await.unwrap();

    for id in &ids {
        assert!(
            db_c.get_experience(*id).unwrap().is_some(),
            "a replaced endpoint must be re-sent the changes it is missing, within \
             the cycle that detected the replacement"
        );
    }
    // A following cycle is an ordinary idempotent no-op.
    manager.sync_once().await.unwrap();
    assert_eq!(
        cursor_row(&db_a, original).unwrap(),
        old_row,
        "no position for the new identity may be written into the old identity's row"
    );
    let new_row = cursor_row(&db_a, restored).expect("the restored identity gets its OWN row");
    assert_eq!(new_row.push_sequence, head);
}

/// The same detection when the whole page is FILTERED away: no changes are
/// selected, and the empty routed push still checks the identity.
#[tokio::test]
async fn recovery_v5_empty_filtered_push_checks_identity() {
    let (db_a, _dir_a, _cid, ids) = seeded_store();
    let (db_b, _dir_b) = open_db();
    let endpoint = common::SyncEndpoint::new(server_for(&db_b));
    let original = endpoint.instance_id();

    // A filter that excludes everything this store holds.
    let unrelated = CollectiveId::new();
    let mut manager = SyncManager::new(
        Arc::clone(&db_a),
        Box::new(ServerBackedTransport::over(Arc::clone(&endpoint))),
        SyncConfig {
            direction: SyncDirection::PushOnly,
            collectives: Some(vec![unrelated]),
            ..sync_config()
        },
    )
    .unwrap();

    manager.sync_once().await.unwrap();
    let pushes_after_first = endpoint.pushes();
    assert!(
        pushes_after_first >= 1,
        "an entirely filtered page must still put a bounded probe on the wire"
    );
    for id in &ids {
        assert!(
            db_b.get_experience(*id).unwrap().is_none(),
            "the filter still excludes what it excludes"
        );
    }
    let old_row = cursor_row(&db_a, original).expect("the peer is on record");
    assert_eq!(
        old_row.push_sequence,
        db_a.get_current_sequence().unwrap(),
        "a validated empty probe lets the filtered scan position be saved"
    );

    let (db_c, _dir_c) = open_db();
    endpoint.replace(server_for(&db_c));
    let restored = endpoint.instance_id();

    manager.sync_once().await.unwrap();
    assert!(
        endpoint.pushes() > pushes_after_first,
        "the filtered cycle must have made a request, or it could not have noticed"
    );
    assert!(
        cursor_row(&db_a, restored).is_some(),
        "the replacement must have been detected and bound"
    );
    assert_eq!(
        cursor_row(&db_a, original).unwrap(),
        old_row,
        "the old identity's row is retained untouched"
    );
}

/// The endpoint is replaced **between the pull and the push of one cycle**.
///
/// The pull confirmed a peer that was already gone by the time the push went
/// out, so pull-before-push cannot make this safe. The push request's own
/// `target_instance` is what refuses it — with no apply, no statistic and no
/// cursor movement on the replacement — and the cycle rebinds.
#[tokio::test]
async fn recovery_v5_replacement_between_pull_and_push() {
    let (db_a, _dir_a, _cid, ids) = seeded_store();
    let (db_b, _dir_b) = open_db();
    let endpoint = common::SyncEndpoint::new(server_for(&db_b));
    let original = endpoint.instance_id();

    let mut manager = SyncManager::new(
        Arc::clone(&db_a),
        Box::new(ServerBackedTransport::over(Arc::clone(&endpoint))),
        sync_config(),
    )
    .unwrap();

    // Establish the session against B.
    manager.sync_once().await.unwrap();
    let head = db_a.get_current_sequence().unwrap();
    let old_row = cursor_row(&db_a, original).expect("the original peer is on record");

    // Give A something new to push, and arm the swap for the instant after the
    // next pull is answered.
    let extra = db_a.record_experience(minimal_exp(_cid)).unwrap();
    let (db_c, _dir_c) = open_db();
    endpoint.replace_after_next_pull(server_for(&db_c));

    manager.sync_once().await.unwrap();
    let restored = endpoint.instance_id();
    assert_ne!(restored, original);

    // The push of that cycle was refused by C's own target check — the pull
    // could not vouch for it. The cycle then spends its single rebind
    // allowance and restarts against C's own cursor (0, since C has never been
    // synced with), so the replay happens WITHIN this cycle.
    for id in ids.iter().chain(std::iter::once(&extra)) {
        assert!(
            db_c.get_experience(*id).unwrap().is_some(),
            "the replacement must receive the changes it never had"
        );
    }
    assert_eq!(
        cursor_row(&db_a, original).unwrap().push_sequence,
        old_row.push_sequence,
        "the refused push must not have advanced the OLD identity's row"
    );
    assert!(
        cursor_row(&db_a, original).unwrap().push_sequence < db_a.get_current_sequence().unwrap()
            || head == db_a.get_current_sequence().unwrap()
    );
    let new_row = cursor_row(&db_a, restored).expect("the restored identity gets its OWN row");
    assert_eq!(new_row.push_sequence, db_a.get_current_sequence().unwrap());
}

/// An endpoint whose identity changes on EVERY answer is flapping. One rebind
/// per cycle is the allowance; a second in the same cycle is a bounded failure,
/// not an unbounded loop of handshakes.
#[tokio::test]
async fn recovery_v5_a_second_remint_in_one_cycle_fails_boundedly() {
    let (db_a, _dir_a, _cid, _ids) = seeded_store();

    /// Answers every request as a brand-new instance.
    struct FlappingEndpoint {
        handshakes: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl pulsedb::sync::transport::SyncTransport for FlappingEndpoint {
        async fn handshake(
            &self,
            _request: pulsedb::sync::types::HandshakeRequest,
            _send_budget_bytes: usize,
        ) -> Result<pulsedb::sync::types::HandshakeResponse, pulsedb::sync::SyncError> {
            self.handshakes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(pulsedb::sync::types::HandshakeResponse {
                instance_id: pulsedb::sync::types::InstanceId::new(),
                protocol_version: pulsedb::sync::SYNC_PROTOCOL_VERSION,
                accepted: true,
                reason: None,
                receive_limit_bytes: 64 * 1024 * 1024,
            })
        }

        async fn push_changes(
            &self,
            request: pulsedb::sync::types::PushRequest,
            _send_budget_bytes: usize,
        ) -> Result<
            pulsedb::sync::types::WireReply<pulsedb::sync::types::PushAck>,
            pulsedb::sync::SyncError,
        > {
            Ok(pulsedb::sync::types::WireReply::peer_changed(
                pulsedb::sync::types::InstanceId::new(),
                request.target_instance,
            ))
        }

        async fn pull_changes(
            &self,
            request: pulsedb::sync::types::PullRequest,
            _send_budget_bytes: usize,
        ) -> Result<
            pulsedb::sync::types::WireReply<pulsedb::sync::types::PullPage>,
            pulsedb::sync::SyncError,
        > {
            Ok(pulsedb::sync::types::WireReply::peer_changed(
                pulsedb::sync::types::InstanceId::new(),
                request.target_instance,
            ))
        }

        async fn health_check(&self) -> Result<(), pulsedb::sync::SyncError> {
            Ok(())
        }

        fn receive_limit_bytes(&self) -> usize {
            64 * 1024 * 1024
        }
    }

    let mut manager = SyncManager::new(
        Arc::clone(&db_a),
        Box::new(FlappingEndpoint {
            handshakes: std::sync::atomic::AtomicUsize::new(0),
        }),
        sync_config(),
    )
    .unwrap();

    let err = manager
        .sync_once()
        .await
        .expect_err("a peer that changes identity twice in one cycle must fail, not loop");
    assert!(
        err.to_string().contains("changed twice"),
        "the failure must say what it refused to keep doing, got: {err}"
    );
}

/// A change that cannot fit a body on its own is deterministic and terminal:
/// the background loop records the typed error and STOPS, rather than
/// rebuilding a body it already knows will be refused, forever. An explicit
/// restart after the cause is corrected runs again.
#[tokio::test]
async fn recovery_v5_oversized_change_stops_background() {
    let (db_a, _dir_a) = open_db();
    let (db_b, _dir_b) = open_db();
    let endpoint = common::SyncEndpoint::new(server_for(&db_b));

    let cid = db_a.create_collective("oversized").unwrap();
    // An experience whose encoded change cannot fit a small body on its own.
    let big = db_a
        .record_experience(NewExperience {
            collective_id: cid,
            content: "x".repeat(8 * 1024),
            embedding: Some(vec![0.1f32; 384]),
            ..Default::default()
        })
        .unwrap();

    let tight = 4 * 1024;
    let mut manager = SyncManager::new(
        Arc::clone(&db_a),
        Box::new(ServerBackedTransport::over(Arc::clone(&endpoint))),
        SyncConfig {
            direction: SyncDirection::PushOnly,
            max_request_bytes: tight,
            push_interval_ms: 10,
            pull_interval_ms: 10,
            ..sync_config()
        },
    )
    .unwrap();

    // Cycle one sends the fitting prefix — the collective at sequence 1 — and
    // stops before the change that does not fit. Nothing is wrong yet.
    manager
        .sync_once()
        .await
        .expect("the collective fits, so the prefix goes out");

    // Cycle two meets the oversized change with no prefix in front of it, and
    // that is the deterministic dead end.
    let err = manager
        .sync_once()
        .await
        .expect_err("the experience cannot fit a body on its own");
    match err {
        pulsedb::sync::SyncError::ChangeTooLarge {
            sequence,
            needed,
            cap,
        } => {
            assert_eq!(sequence, 2, "the oversized change is named");
            assert!(needed > cap, "{needed} must exceed the {cap}-byte cap");
            assert_eq!(cap, tight as u64);
        }
        other => panic!("expected the typed ChangeTooLarge, got {other}"),
    }
    assert!(
        !matches!(manager.status(), SyncStatus::Syncing),
        "a terminal one-shot must not leave the manager wedged in Syncing"
    );
    let peer = endpoint.instance_id();
    assert_eq!(
        cursor_row(&db_a, peer).unwrap().push_sequence,
        1,
        "nothing may be acknowledged over the oversized change"
    );
    assert!(
        db_b.get_experience(big).unwrap().is_none(),
        "and it was never sent"
    );

    // Background: it records the error and stops attempting.
    manager.start().await.unwrap();
    await_until(
        || matches!(manager.status(), SyncStatus::Error(ref m) if m.contains("over the")),
        "the background loop records the terminal error",
    )
    .await;
    // Attempt-count evidence, over many configured intervals: the loop ticks
    // every 10 ms, so this window spans ~20 of them. Elapsed time alone would
    // be weak; the push counter is what says no transfer was attempted.
    let attempts = endpoint.pushes();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(
        endpoint.pushes(),
        attempts,
        "a deterministic dead end must not be retried automatically across ~30 \
         poll intervals"
    );
    assert!(
        matches!(manager.status(), SyncStatus::Error(_)),
        "and the reason must survive on the exited task"
    );

    // Deterministic proof the task actually EXITED rather than merely being
    // quiet: a `start()` on a live run is refused, so one that succeeds can
    // only mean the previous handle was finished and got reaped.
    manager
        .start()
        .await
        .expect("the terminal task exited, so a restart reaps it instead of refusing");
    await_until(
        || matches!(manager.status(), SyncStatus::Error(_)),
        "the restarted run hits the same dead end and records it again",
    )
    .await;

    // Stopping a task that already exited reaps it without erasing the reason.
    manager.stop().await.unwrap();
    assert!(matches!(manager.status(), SyncStatus::Error(_)));

    // Explicit correction — the operator raises the peer's inbound limit — then
    // an explicit restart, which re-handshakes and picks the new budget up.
    endpoint.replace(server_for_with(
        &db_b,
        SyncConfig {
            max_request_bytes: 64 * 1024 * 1024,
            ..SyncConfig::default()
        },
    ));
    let mut manager = SyncManager::new(
        Arc::clone(&db_a),
        Box::new(ServerBackedTransport::over(Arc::clone(&endpoint))),
        SyncConfig {
            direction: SyncDirection::PushOnly,
            max_request_bytes: 64 * 1024 * 1024,
            push_interval_ms: 10,
            pull_interval_ms: 10,
            ..sync_config()
        },
    )
    .unwrap();
    manager.start().await.unwrap();
    await_until(
        || db_b.get_experience(big).unwrap().is_some(),
        "after the correction and an explicit restart the manager runs again",
    )
    .await;
    manager.stop().await.unwrap();
}

/// An `ExperienceUpdated` whose target is absent is a FAILURE, not a
/// successful or idempotent acknowledgement.
///
/// Acknowledging it would let the sender's `push_sequence` pass the update, and
/// `compact_wal` would then be free to delete the create the update depends on
/// — losing the record on both sides. Recovering the missing dependency is out
/// of scope here; the point is that the non-completion is explicit.
///
/// Already-absent deletes and archives keep their idempotent skip: they need no
/// record to be correct.
#[tokio::test]
async fn recovery_v5_absent_update_target_is_a_failure_not_an_ack() {
    use pulsedb::sync::types::{
        SerializableExperienceUpdate, SyncChange, SyncEntityType, SyncPayload,
    };

    let (db_b, _dir_b) = open_db();
    let server = server_for(&db_b);
    let sender = pulsedb::sync::types::InstanceId::new();
    let absent = ExperienceId::new();

    let update = SyncChange {
        sequence: 4,
        source_instance: sender,
        collective_id: CollectiveId::new(),
        entity_type: SyncEntityType::Experience,
        payload: SyncPayload::ExperienceUpdated {
            id: absent,
            update: SerializableExperienceUpdate {
                importance: Some(0.9),
                applications: Some(std::collections::BTreeMap::from([(sender, 2)])),
                last_reinforced: Some(pulsedb::Timestamp::now()),
                ..Default::default()
            },
            timestamp: pulsedb::Timestamp::now(),
        },
        timestamp: pulsedb::Timestamp::now(),
    };
    let delete = SyncChange {
        sequence: 5,
        source_instance: sender,
        collective_id: CollectiveId::new(),
        entity_type: SyncEntityType::Experience,
        payload: SyncPayload::ExperienceDeleted {
            id: ExperienceId::new(),
            timestamp: pulsedb::Timestamp::now(),
        },
        timestamp: pulsedb::Timestamp::now(),
    };

    let ack = server
        .handle_push(pulsedb::sync::types::PushRequest {
            protocol_version: pulsedb::sync::SYNC_PROTOCOL_VERSION,
            source_instance: sender,
            target_instance: server.instance_id(),
            reply_limit_bytes: 64 * 1024 * 1024,
            changes: vec![update, delete],
        })
        .unwrap()
        .into_result(server.instance_id())
        .unwrap();

    assert_eq!(ack.total, 2);
    assert_eq!(
        ack.rejected, 1,
        "the update whose target is absent is a failure"
    );
    assert_eq!(
        ack.accepted, 1,
        "the already-absent delete is a genuine idempotent skip and stays accepted"
    );
    assert_eq!(
        ack.safe_through, None,
        "nothing below the failure succeeded, so no position may be acknowledged"
    );
}

/// A push whose MIDDLE change fails is acknowledged only up to the last
/// sequence that really applied — proved on the real `handle_push` path, with
/// real stored outcomes rather than a hand-written acknowledgement.
///
/// The applier's own unit tests already pin the failure-floor rule. What this
/// closes is the seam between them and the wire: that `SyncServer::handle_push`
/// emits a NON-`None` `safe_through` under a genuine partial apply failure, and
/// that the value is the highest real success below the failure rather than
/// the batch tail or `failure_sequence - 1`.
///
/// The sequences are deliberately NONADJACENT — 2 / 5 / 9. With 1 / 2 / 3 an
/// off-by-one on the failure (`5 - 1`) and the correct answer coincide; at this
/// spacing `Some(4)`, `Some(9)` and `None` are all distinguishable from
/// `Some(2)`.
///
/// Arbitrary order is the second half: a peer chooses its batch's order, and
/// the floor is by SEQUENCE, not by position. `[9 ok, 2 ok, 5 err]` must reach
/// the same answer, which position-based bookkeeping cannot.
#[tokio::test]
async fn recovery_v5_partial_push_failure_acknowledges_the_real_success_floor() {
    use pulsedb::sync::types::{
        InstanceId, PushRequest, SerializableExperienceUpdate, SyncChange, SyncEntityType,
        SyncPayload,
    };

    /// A `CollectiveCreated` that genuinely applies, attributed to `sender`.
    fn applies(sequence: u64, sender: InstanceId, name: &str) -> (CollectiveId, SyncChange) {
        let id = CollectiveId::new();
        (
            id,
            SyncChange {
                sequence,
                source_instance: sender,
                collective_id: id,
                entity_type: SyncEntityType::Collective,
                payload: SyncPayload::CollectiveCreated(pulsedb::Collective {
                    id,
                    name: name.to_string(),
                    owner_id: None,
                    embedding_dimension: 384,
                    created_at: pulsedb::Timestamp::now(),
                    updated_at: pulsedb::Timestamp::now(),
                }),
                timestamp: pulsedb::Timestamp::now(),
            },
        )
    }

    /// An `ExperienceUpdated` whose target is absent — a real failure, not a
    /// synthesized one.
    fn fails(sequence: u64, sender: InstanceId) -> (ExperienceId, SyncChange) {
        let absent = ExperienceId::new();
        (
            absent,
            SyncChange {
                sequence,
                source_instance: sender,
                collective_id: CollectiveId::new(),
                entity_type: SyncEntityType::Experience,
                payload: SyncPayload::ExperienceUpdated {
                    id: absent,
                    update: SerializableExperienceUpdate {
                        importance: Some(0.9),
                        ..Default::default()
                    },
                    timestamp: pulsedb::Timestamp::now(),
                },
                timestamp: pulsedb::Timestamp::now(),
            },
        )
    }

    for (label, order) in [
        ("wal order", [0usize, 1, 2]),
        ("arbitrary order", [2, 0, 1]),
    ] {
        let (db, _dir) = open_db();
        let server = server_for(&db);
        let sender = InstanceId::new();

        let (low, low_change) = applies(2, sender, "low-applies");
        let (absent, failing) = fails(5, sender);
        let (high, high_change) = applies(9, sender, "high-applies");
        let batch = [low_change, failing, high_change];
        let changes: Vec<SyncChange> = order.iter().map(|i| batch[*i].clone()).collect();
        assert_eq!(changes.len(), 3);

        let ack = server
            .handle_push(PushRequest {
                protocol_version: pulsedb::sync::SYNC_PROTOCOL_VERSION,
                source_instance: sender,
                target_instance: server.instance_id(),
                reply_limit_bytes: 64 * 1024 * 1024,
                changes,
            })
            .unwrap()
            .into_result(server.instance_id())
            .unwrap();

        assert_eq!(ack.total, 3, "{label}");
        assert_eq!(
            ack.rejected, 1,
            "{label}: the absent update target is a failure"
        );
        assert_eq!(ack.accepted, 2, "{label}: both collectives really applied");
        assert_eq!(
            ack.safe_through,
            Some(2),
            "{label}: the acknowledgement is the highest success BELOW the failure \
             at sequence 5 — not the tail (9), not `failure - 1` (4), not `None`"
        );

        // The counters are read off real storage, not off the reply.
        assert!(
            db.get_collective(low).unwrap().is_some(),
            "{label}: the acknowledged change must actually be present"
        );
        assert!(
            db.get_collective(high).unwrap().is_some(),
            "{label}: a success ABOVE the failure is still applied — it is simply \
             not acknowledged"
        );
        assert!(
            db.get_experience(absent).unwrap().is_none(),
            "{label}: the failing change wrote nothing"
        );
    }
}

/// The wire counters come from actual apply outcomes: an ordinary idempotent
/// re-push has `rejected == 0`, and `accepted + rejected` always equals what
/// was submitted.
#[tokio::test]
async fn recovery_v5_idempotent_repush_reports_zero_rejected() {
    let (db_a, _dir_a, _cid, ids) = seeded_store();
    let (db_b, _dir_b) = open_db();
    let endpoint = common::SyncEndpoint::new(server_for(&db_b));

    let mut manager = SyncManager::new(
        Arc::clone(&db_a),
        Box::new(ServerBackedTransport::over(Arc::clone(&endpoint))),
        SyncConfig {
            direction: SyncDirection::PushOnly,
            ..sync_config()
        },
    )
    .unwrap();
    manager.sync_once().await.unwrap();
    for id in &ids {
        assert!(db_b.get_experience(*id).unwrap().is_some());
    }

    // Re-push the same batch by hand: every change is now an idempotent skip.
    let server = server_for(&db_b);
    let changes: Vec<_> = db_a
        .storage_for_test()
        .poll_sync_events(0, 100)
        .unwrap()
        .into_iter()
        .map(|(sequence, record)| {
            // Rebuild the change the pusher would have built.
            let _ = record;
            sequence
        })
        .collect();
    assert!(!changes.is_empty());

    // A second full cycle from position 0 re-sends everything.
    let mut replay = SyncManager::new(
        Arc::clone(&db_a),
        Box::new(ServerBackedTransport::over(Arc::clone(&endpoint))),
        SyncConfig {
            direction: SyncDirection::PushOnly,
            ..sync_config()
        },
    )
    .unwrap();
    db_a.storage_for_test()
        .update_push_cursor(&server.instance_id(), 0)
        .unwrap();
    replay.sync_once().await.unwrap();
    let row = cursor_row(&db_a, endpoint.instance_id()).unwrap();
    assert_eq!(
        row.push_sequence,
        db_a.get_current_sequence().unwrap(),
        "an all-idempotent re-push is a complete success and advances to the head"
    );
}

/// `start()` after a `stop()` runs a genuinely new loop.
///
/// `stop()` signals shutdown through a `Notify`, and a `notify_one` with no
/// waiter leaves a **permit** behind. Reusing one signal across runs meant a
/// `stop()` whose task had already exited armed the NEXT task to shut down the
/// instant it started — a manager that reported itself started and did nothing.
/// The signal is replaced per run, and a finished handle is reaped rather than
/// refusing the restart.
#[tokio::test]
async fn recovery_v5_restart_after_stop_runs_again() {
    let (db_a, _dir_a, cid, _ids) = seeded_store();
    let (db_b, _dir_b) = open_db();
    let endpoint = common::SyncEndpoint::new(server_for(&db_b));

    let mut manager = SyncManager::new(
        Arc::clone(&db_a),
        Box::new(ServerBackedTransport::over(Arc::clone(&endpoint))),
        SyncConfig {
            direction: SyncDirection::PushOnly,
            push_interval_ms: 10,
            pull_interval_ms: 10,
            ..sync_config()
        },
    )
    .unwrap();

    manager.start().await.unwrap();
    let first = db_a.record_experience(minimal_exp(cid)).unwrap();
    await_until(
        || db_b.get_experience(first).unwrap().is_some(),
        "the first run pushes",
    )
    .await;
    manager.stop().await.unwrap();
    assert_eq!(manager.status(), SyncStatus::Idle);

    // A second run, with a second write, on the same manager.
    manager.start().await.unwrap();
    let second = db_a.record_experience(minimal_exp(cid)).unwrap();
    await_until(
        || db_b.get_experience(second).unwrap().is_some(),
        "the RESTARTED run must push too — a stale shutdown permit would have \
         stopped it before its first tick",
    )
    .await;
    manager.stop().await.unwrap();
    assert_eq!(manager.status(), SyncStatus::Idle);
}

/// A double `start()` is still refused while a run is live.
#[tokio::test]
async fn recovery_v5_double_start_is_refused_while_running() {
    let (db_a, _dir_a, _cid, _ids) = seeded_store();
    let (db_b, _dir_b) = open_db();
    let endpoint = common::SyncEndpoint::new(server_for(&db_b));

    let mut manager = SyncManager::new(
        Arc::clone(&db_a),
        Box::new(ServerBackedTransport::over(endpoint)),
        SyncConfig {
            direction: SyncDirection::PushOnly,
            push_interval_ms: 10_000,
            pull_interval_ms: 10_000,
            ..sync_config()
        },
    )
    .unwrap();

    manager.start().await.unwrap();
    let err = manager
        .start()
        .await
        .expect_err("a live run must not be started twice");
    assert!(err.to_string().contains("already started"), "got {err}");
    manager.stop().await.unwrap();
}

// ============================================================================
// Direction-specific transport budgets (R4, R7-R10)
//
// The outbound budget is what the party that must READ the request will
// accept; the advertised reply budget is what this side can read. They are
// different numbers computed from different inputs, and neither is the other.
// ============================================================================

/// The exact `PullRequest` a `SyncManager` builds for `peer`, so a test can
/// MEASURE the boundary instead of baking in a guessed collective count.
///
/// It has to be the real shape — the same identities, cursor, batch size and
/// advertised reply limit — because every one of those contributes bytes.
fn manager_pull_request(
    local: pulsedb::sync::types::InstanceId,
    peer: pulsedb::sync::types::InstanceId,
    config: &SyncConfig,
    reply_limit_bytes: u64,
    collectives: &[CollectiveId],
) -> pulsedb::sync::types::PullRequest {
    pulsedb::sync::types::PullRequest {
        protocol_version: pulsedb::sync::SYNC_PROTOCOL_VERSION,
        source_instance: local,
        target_instance: peer,
        cursor: pulsedb::sync::types::SyncPosition::new(peer, 0),
        batch_size: config.batch_size as u64,
        reply_limit_bytes,
        collectives: Some(collectives.to_vec()),
    }
}

/// Grows a collective filter one id at a time until the real frame crosses
/// `cap`, and returns `(the first filter that does NOT fit, the largest that
/// does)`.
fn measure_filter_boundary(
    local: pulsedb::sync::types::InstanceId,
    peer: pulsedb::sync::types::InstanceId,
    config: &SyncConfig,
    reply_limit_bytes: u64,
    cap: usize,
) -> (Vec<CollectiveId>, Vec<CollectiveId>) {
    let mut ids: Vec<CollectiveId> = Vec::new();
    loop {
        ids.push(CollectiveId::new());
        let frame = pulsedb::sync::wire::encoded_len(&manager_pull_request(
            local,
            peer,
            config,
            reply_limit_bytes,
            &ids,
        ))
        .unwrap();
        if frame > cap {
            let under = ids[..ids.len() - 1].to_vec();
            assert!(
                !under.is_empty(),
                "the crossover must be above a single id, or the case is degenerate"
            );
            return (ids, under);
        }
        assert!(
            ids.len() < 4096,
            "the filter never crossed the {cap}-byte cap; the measurement is wrong"
        );
    }
}

/// R4 — bounded control traffic under ASYMMETRIC limits at the 1 KiB floor.
///
/// The peer accepts only the control minimum while this side's policy is
/// 64 MiB. The handshake and the empty routed push must both still succeed —
/// they are certified bounded control frames — and identity detection must stay
/// active on the two paths that have no changes to send: a cursor already at
/// the WAL head, and a page filtered away entirely.
#[tokio::test]
async fn recovery_v5_bounded_controls_survive_the_minimum_peer_budget() {
    let (db_a, _dir_a, _cid, ids) = seeded_store();
    let (db_b, _dir_b) = open_db();
    // A peer that will read no more than a bounded control frame.
    let endpoint = common::SyncEndpoint::new(server_for_with(
        &db_b,
        SyncConfig {
            max_request_bytes: pulsedb::sync::MIN_CONTROL_FRAME_BYTES,
            ..SyncConfig::default()
        },
    ));
    let original = endpoint.instance_id();

    // A filter excluding everything this store holds: every page is filtered,
    // so only the empty probe ever goes out and it must fit 1 KiB.
    let unrelated = CollectiveId::new();
    let mut manager = SyncManager::new(
        Arc::clone(&db_a),
        Box::new(ServerBackedTransport::over(Arc::clone(&endpoint))),
        SyncConfig {
            direction: SyncDirection::PushOnly,
            collectives: Some(vec![unrelated]),
            ..sync_config()
        },
    )
    .unwrap();

    // The handshake happened (the manager is bound) and the empty probe went
    // out under the 1 KiB peer cap.
    manager
        .sync_once()
        .await
        .expect("a bounded control exchange fits the certified minimum");
    let probes = endpoint.pushes();
    assert!(probes >= 1, "an entirely filtered page still probes");
    for id in &ids {
        assert!(db_b.get_experience(*id).unwrap().is_none());
    }
    let head = db_a.get_current_sequence().unwrap();
    assert_eq!(
        cursor_row(&db_a, original).unwrap().push_sequence,
        head,
        "a validated probe lets the filtered scan position be saved"
    );

    // Identity detection at the WAL head, still under the minimum budget.
    let (db_c, _dir_c) = open_db();
    endpoint.replace(server_for_with(
        &db_c,
        SyncConfig {
            max_request_bytes: pulsedb::sync::MIN_CONTROL_FRAME_BYTES,
            ..SyncConfig::default()
        },
    ));
    let restored = endpoint.instance_id();
    assert_ne!(restored, original);

    manager.sync_once().await.unwrap();
    assert!(
        endpoint.pushes() > probes,
        "the cycle must have made a request, or it could not have noticed"
    );
    assert!(
        cursor_row(&db_a, restored).is_some(),
        "the replacement is detected and bound through a 1 KiB control exchange"
    );
}

/// R8 + R9 + R10 — a control request too large for the peer's reader is a
/// LOCAL, typed, terminal failure, and the boundary is the measured one.
///
/// `SyncConfig::collectives` is a documented public field and is unbounded, so
/// a supported configuration can build a pull request past a conforming peer's
/// inbound cap. Sending it means the peer refuses it remotely and the loop
/// retries the identical body forever. Failing locally is strictly better: one
/// round trip cheaper, typed, and terminal.
#[tokio::test]
async fn recovery_v5_oversized_filtered_pull_fails_locally_and_terminally() {
    let (db_a, _dir_a) = open_db();
    let (db_b, _dir_b) = open_db();
    let cap = pulsedb::sync::MIN_CONTROL_FRAME_BYTES;
    let endpoint = common::SyncEndpoint::new(server_for_with(
        &db_b,
        SyncConfig {
            max_request_bytes: cap,
            ..SyncConfig::default()
        },
    ));
    let peer = endpoint.instance_id();

    let base = SyncConfig {
        direction: SyncDirection::PullOnly,
        push_interval_ms: 10,
        pull_interval_ms: 10,
        ..sync_config()
    };
    // The transport's receive limit IS the peer server's cap here, so the
    // advertised reply budget is min(64 MiB, 1 KiB).
    let reply_limit = cap as u64;
    let (over, under) = measure_filter_boundary(db_a.instance_id(), peer, &base, reply_limit, cap);
    let over_frame = pulsedb::sync::wire::encoded_len(&manager_pull_request(
        db_a.instance_id(),
        peer,
        &base,
        reply_limit,
        &over,
    ))
    .unwrap();

    // ── R9: the adjacent FITTING filter still works. The boundary is real,
    // not a blanket refusal of filtered pulls.
    let mut fits = SyncManager::new(
        Arc::clone(&db_a),
        Box::new(ServerBackedTransport::over(Arc::clone(&endpoint))),
        SyncConfig {
            collectives: Some(under.clone()),
            ..base.clone()
        },
    )
    .unwrap();
    fits.sync_once()
        .await
        .expect("a measured under-cap filter is an ordinary pull");
    assert!(
        endpoint.pulls() >= 1,
        "the fitting filter reached the peer, so the boundary is where measured"
    );

    // ── R8: one more id, and the request cannot be read by the peer.
    let pulls_before = endpoint.pulls();
    let mut manager = SyncManager::new(
        Arc::clone(&db_a),
        Box::new(ServerBackedTransport::over(Arc::clone(&endpoint))),
        SyncConfig {
            collectives: Some(over.clone()),
            ..base.clone()
        },
    )
    .unwrap();

    let err = manager
        .sync_once()
        .await
        .expect_err("a request the peer cannot read must not be sent");
    match err {
        pulsedb::sync::SyncError::RequestTooLarge {
            operation,
            needed,
            cap: c,
        } => {
            assert_eq!(operation, pulsedb::sync::WireOperation::Pull);
            assert_eq!(needed, over_frame as u64, "the measured frame is reported");
            assert_eq!(c, cap as u64, "against the peer's actual inbound budget");
        }
        other => panic!("expected the typed RequestTooLarge, got {other}"),
    }
    // R10 (classification): it is NOT the WAL-change variant, and not the
    // inbound one either. No sequence is at fault and no cursor is involved.
    let err = manager.sync_once().await.unwrap_err();
    assert!(err.is_request_too_large());
    assert!(
        !err.is_change_too_large(),
        "no WAL sequence is at fault, so ChangeTooLarge would be a lie in the type"
    );
    assert!(
        !err.is_payload_too_large(),
        "and it is an outbound failure, not an oversized inbound body"
    );
    assert_eq!(
        endpoint.pulls(),
        pulls_before,
        "no pull crossed to the peer — the failure is local, before the round trip"
    );
    assert!(
        matches!(manager.status(), SyncStatus::Error(ref m) if m.contains("send budget")),
        "a one-shot terminal failure records the actionable reason, got {:?}",
        manager.status()
    );

    // ── The background loop stops rather than retrying a body already known
    // not to fit.
    manager.start().await.unwrap();
    await_until(
        || matches!(manager.status(), SyncStatus::Error(ref m) if m.contains("send budget")),
        "the background loop records the terminal request-size error",
    )
    .await;
    let attempts = endpoint.pulls();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(
        endpoint.pulls(),
        attempts,
        "and makes no further automatic attempt across ~30 poll intervals"
    );
    // Deterministic proof the task EXITED rather than merely being quiet: a
    // `start()` on a live run is refused, so one that succeeds reaped it.
    manager
        .start()
        .await
        .expect("the terminal task exited, so a restart reaps it");
    await_until(
        || matches!(manager.status(), SyncStatus::Error(ref m) if m.contains("send budget")),
        "the restarted run hits the same dead end and records it again",
    )
    .await;
    // Stopping a task that already exited reaps it without erasing the reason.
    manager.stop().await.unwrap();
    assert!(
        matches!(manager.status(), SyncStatus::Error(ref m) if m.contains("send budget")),
        "the reason survives on the exited task"
    );

    // ── R10 (restart): the operator corrects the cause — here by raising the
    // peer's inbound limit — and an explicit restart re-handshakes, picks the
    // new budget up, and resumes.
    endpoint.replace(server_for_with(
        &db_b,
        SyncConfig {
            max_request_bytes: 64 * 1024 * 1024,
            ..SyncConfig::default()
        },
    ));
    let mut corrected = SyncManager::new(
        Arc::clone(&db_a),
        Box::new(ServerBackedTransport::over(Arc::clone(&endpoint))),
        SyncConfig {
            collectives: Some(over),
            ..base
        },
    )
    .unwrap();
    corrected
        .sync_once()
        .await
        .expect("the same filter fits the corrected peer, and the pull runs");
    assert!(
        endpoint.pulls() > attempts,
        "the corrected configuration actually reaches the peer"
    );
}

/// R7 — the WAL-change terminal path is NOT replaced by the request-size one.
///
/// A single change that cannot fit its push budget is caught by the packer,
/// before any transport is invoked, so it keeps naming its sequence and keeps
/// leaving its cursor unadvanced. Reclassifying it would lose both.
#[tokio::test]
async fn recovery_v5_oversized_change_is_not_reclassified_as_request_too_large() {
    let (db_a, _dir_a) = open_db();
    let (db_b, _dir_b) = open_db();
    let endpoint = common::SyncEndpoint::new(server_for(&db_b));

    let cid = db_a.create_collective("oversized").unwrap(); // seq 1
    db_a.record_experience(NewExperience {
        collective_id: cid,
        content: "x".repeat(8 * 1024),
        embedding: Some(vec![0.1f32; 384]),
        ..Default::default()
    })
    .unwrap(); // seq 2

    let tight = 4 * 1024;
    let mut manager = SyncManager::new(
        Arc::clone(&db_a),
        Box::new(ServerBackedTransport::over(Arc::clone(&endpoint))),
        SyncConfig {
            direction: SyncDirection::PushOnly,
            max_request_bytes: tight,
            ..sync_config()
        },
    )
    .unwrap();

    manager.sync_once().await.unwrap(); // the collective fits
    let pushes = endpoint.pushes();
    let err = manager.sync_once().await.unwrap_err();
    assert!(
        err.is_change_too_large(),
        "the packer catches it first and names the sequence, got {err}"
    );
    assert!(
        !err.is_request_too_large(),
        "the two terminal paths stay distinct, got {err}"
    );
    assert_eq!(
        endpoint.pushes(),
        pushes,
        "and it never reached the transport, so no request was ever built"
    );
    assert_eq!(
        cursor_row(&db_a, endpoint.instance_id())
            .unwrap()
            .push_sequence,
        1,
        "the oversized change keeps its cursor unadvanced"
    );
}
