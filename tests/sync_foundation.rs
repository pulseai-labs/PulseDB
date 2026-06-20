//! Integration tests for Phase 1: Sync Protocol Foundation.
//!
//! These tests verify the sync module's types, persistence, and transport
//! work correctly end-to-end with a real PulseDB instance.

#![cfg(feature = "sync")]

use pulsedb::storage::StorageEngine;
use pulsedb::sync::config::{ConflictResolution, SyncConfig, SyncDirection};
use pulsedb::sync::guard::{is_sync_applying, SyncApplyGuard};
use pulsedb::sync::transport::SyncTransport;
use pulsedb::sync::transport_mem::InMemorySyncTransport;
use pulsedb::sync::types::{
    HandshakeRequest, InstanceId, PullRequest, SyncChange, SyncCursor, SyncEntityType, SyncPayload,
    SyncStatus,
};
use pulsedb::sync::SYNC_PROTOCOL_VERSION;
use pulsedb::{Collective, CollectiveId, Config, Timestamp};
use tempfile::tempdir;

// ============================================================================
// InstanceId persistence
// ============================================================================

#[test]
fn test_instance_id_persists_across_reopens() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("sync_test.db");
    let config = Config::default();

    // Open, get instance_id, close
    let id1 = {
        let storage = pulsedb::storage::RedbStorage::open(&path, &config).unwrap();
        let id = storage.instance_id();
        assert_ne!(id, InstanceId::nil());
        drop(storage);
        id
    };

    // Reopen, get instance_id — must match
    let id2 = {
        let storage = pulsedb::storage::RedbStorage::open(&path, &config).unwrap();
        let id = storage.instance_id();
        drop(storage);
        id
    };

    assert_eq!(id1, id2, "InstanceId must be stable across reopens");
}

#[test]
fn test_instance_id_unique_per_database() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let config = Config::default();

    let storage1 = pulsedb::storage::RedbStorage::open(dir1.path().join("a.db"), &config).unwrap();
    let storage2 = pulsedb::storage::RedbStorage::open(dir2.path().join("b.db"), &config).unwrap();

    assert_ne!(
        storage1.instance_id(),
        storage2.instance_id(),
        "Different databases must have different InstanceIds"
    );
}

// ============================================================================
// SyncCursor persistence
// ============================================================================

#[test]
fn test_sync_cursor_save_and_load() {
    let dir = tempdir().unwrap();
    let config = Config::default();
    let storage =
        pulsedb::storage::RedbStorage::open(dir.path().join("cursor.db"), &config).unwrap();

    let peer_id = InstanceId::new();
    let cursor = SyncCursor {
        instance_id: peer_id,
        last_sequence: 42,
    };

    // Save
    storage.save_sync_cursor(&cursor).unwrap();

    // Load
    let loaded = storage.load_sync_cursor(&peer_id).unwrap();
    assert_eq!(loaded, Some(cursor));
}

#[test]
fn test_sync_cursor_load_missing_returns_none() {
    let dir = tempdir().unwrap();
    let config = Config::default();
    let storage =
        pulsedb::storage::RedbStorage::open(dir.path().join("cursor.db"), &config).unwrap();

    let unknown_peer = InstanceId::new();
    let loaded = storage.load_sync_cursor(&unknown_peer).unwrap();
    assert_eq!(loaded, None);
}

#[test]
fn test_sync_cursor_upsert() {
    let dir = tempdir().unwrap();
    let config = Config::default();
    let storage =
        pulsedb::storage::RedbStorage::open(dir.path().join("cursor.db"), &config).unwrap();

    let peer_id = InstanceId::new();

    // Save initial cursor
    storage
        .save_sync_cursor(&SyncCursor {
            instance_id: peer_id,
            last_sequence: 10,
        })
        .unwrap();

    // Update to higher sequence
    storage
        .save_sync_cursor(&SyncCursor {
            instance_id: peer_id,
            last_sequence: 50,
        })
        .unwrap();

    let loaded = storage.load_sync_cursor(&peer_id).unwrap().unwrap();
    assert_eq!(loaded.last_sequence, 50);
}

#[test]
fn test_sync_cursor_list_multiple_peers() {
    let dir = tempdir().unwrap();
    let config = Config::default();
    let storage =
        pulsedb::storage::RedbStorage::open(dir.path().join("cursor.db"), &config).unwrap();

    let peer_a = InstanceId::new();
    let peer_b = InstanceId::new();
    let peer_c = InstanceId::new();

    storage
        .save_sync_cursor(&SyncCursor {
            instance_id: peer_a,
            last_sequence: 10,
        })
        .unwrap();
    storage
        .save_sync_cursor(&SyncCursor {
            instance_id: peer_b,
            last_sequence: 20,
        })
        .unwrap();
    storage
        .save_sync_cursor(&SyncCursor {
            instance_id: peer_c,
            last_sequence: 30,
        })
        .unwrap();

    let cursors = storage.list_sync_cursors().unwrap();
    assert_eq!(cursors.len(), 3);

    // Verify all peers are present (order not guaranteed)
    let ids: Vec<InstanceId> = cursors.iter().map(|c| c.instance_id).collect();
    assert!(ids.contains(&peer_a));
    assert!(ids.contains(&peer_b));
    assert!(ids.contains(&peer_c));
}

#[test]
fn test_sync_cursor_persists_across_reopens() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("cursor.db");
    let config = Config::default();

    let peer_id = InstanceId::new();

    // Save cursor then close
    {
        let storage = pulsedb::storage::RedbStorage::open(&path, &config).unwrap();
        storage
            .save_sync_cursor(&SyncCursor {
                instance_id: peer_id,
                last_sequence: 99,
            })
            .unwrap();
        drop(storage);
    }

    // Reopen and verify
    {
        let storage = pulsedb::storage::RedbStorage::open(&path, &config).unwrap();
        let loaded = storage.load_sync_cursor(&peer_id).unwrap().unwrap();
        assert_eq!(loaded.last_sequence, 99);
    }
}

// ============================================================================
// SyncApplyGuard (thread-local echo prevention)
// ============================================================================

#[test]
fn test_sync_apply_guard_basic_lifecycle() {
    assert!(!is_sync_applying());

    {
        let _guard = SyncApplyGuard::enter();
        assert!(is_sync_applying());
    }

    assert!(!is_sync_applying());
}

#[test]
fn test_sync_apply_guard_resets_on_panic() {
    assert!(!is_sync_applying());

    let result = std::panic::catch_unwind(|| {
        let _guard = SyncApplyGuard::enter();
        assert!(is_sync_applying());
        panic!("intentional");
    });

    assert!(result.is_err());
    assert!(!is_sync_applying(), "Guard must reset on panic unwind");
}

// ============================================================================
// InMemorySyncTransport end-to-end
// ============================================================================

fn make_change(seq: u64, cid: CollectiveId) -> SyncChange {
    SyncChange {
        sequence: seq,
        source_instance: InstanceId::new(),
        collective_id: cid,
        entity_type: SyncEntityType::Collective,
        payload: SyncPayload::CollectiveCreated(Collective {
            id: cid,
            name: format!("collective-{}", seq),
            owner_id: None,
            embedding_dimension: 384,
            created_at: Timestamp::now(),
            updated_at: Timestamp::now(),
        }),
        timestamp: Timestamp::now(),
    }
}

#[tokio::test]
async fn test_memory_transport_full_roundtrip() {
    let (local, remote) = InMemorySyncTransport::new_pair();
    let cid = CollectiveId::new();

    // Handshake
    let hs_req = HandshakeRequest {
        instance_id: InstanceId::new(),
        protocol_version: SYNC_PROTOCOL_VERSION,
        capabilities: vec!["push".into(), "pull".into()],
    };
    let hs_resp = local.handshake(hs_req).await.unwrap();
    assert!(hs_resp.accepted);
    assert_eq!(hs_resp.protocol_version, SYNC_PROTOCOL_VERSION);

    // Health check
    assert!(local.health_check().await.is_ok());
    assert!(remote.health_check().await.is_ok());

    // Push changes from local
    let changes = vec![
        make_change(1, cid),
        make_change(2, cid),
        make_change(3, cid),
    ];
    let push_resp = local.push_changes(changes).await.unwrap();
    assert_eq!(push_resp.accepted, 3);
    assert_eq!(push_resp.rejected, 0);

    // Pull changes from remote (shared buffer)
    let pull_req = PullRequest {
        cursor: SyncCursor::new(remote.instance_id()),
        batch_size: 500,
        collectives: None,
    };
    let pull_resp = remote.pull_changes(pull_req).await.unwrap();
    assert_eq!(pull_resp.changes.len(), 3);
    assert!(!pull_resp.has_more);

    // Verify sequence ordering
    assert_eq!(pull_resp.changes[0].sequence, 1);
    assert_eq!(pull_resp.changes[1].sequence, 2);
    assert_eq!(pull_resp.changes[2].sequence, 3);
}

#[tokio::test]
async fn test_memory_transport_incremental_pull() {
    let (local, remote) = InMemorySyncTransport::new_pair();
    let cid = CollectiveId::new();

    // Push 5 changes
    let changes: Vec<SyncChange> = (1..=5).map(|s| make_change(s, cid)).collect();
    local.push_changes(changes).await.unwrap();

    // Pull first batch (size 2)
    let pull1 = PullRequest {
        cursor: SyncCursor::new(remote.instance_id()),
        batch_size: 2,
        collectives: None,
    };
    let resp1 = remote.pull_changes(pull1).await.unwrap();
    assert_eq!(resp1.changes.len(), 2);
    assert!(resp1.has_more);
    assert_eq!(resp1.new_cursor.last_sequence, 2);

    // Pull next batch from cursor
    let pull2 = PullRequest {
        cursor: resp1.new_cursor,
        batch_size: 2,
        collectives: None,
    };
    let resp2 = remote.pull_changes(pull2).await.unwrap();
    assert_eq!(resp2.changes.len(), 2);
    assert!(resp2.has_more);
    assert_eq!(resp2.new_cursor.last_sequence, 4);

    // Pull remainder
    let pull3 = PullRequest {
        cursor: resp2.new_cursor,
        batch_size: 100,
        collectives: None,
    };
    let resp3 = remote.pull_changes(pull3).await.unwrap();
    assert_eq!(resp3.changes.len(), 1);
    assert!(!resp3.has_more);
    assert_eq!(resp3.new_cursor.last_sequence, 5);
}

// ============================================================================
// SyncConfig validation
// ============================================================================

#[test]
fn test_sync_config_defaults_are_valid() {
    let config = SyncConfig::default();
    assert!(config.validate().is_ok());
    assert_eq!(config.direction, SyncDirection::Bidirectional);
    assert_eq!(config.conflict_resolution, ConflictResolution::ServerWins);
    assert_eq!(config.batch_size, 500);
}

#[test]
fn test_sync_config_custom_is_valid() {
    let config = SyncConfig {
        direction: SyncDirection::PushOnly,
        batch_size: 100,
        push_interval_ms: 500,
        pull_interval_ms: 2000,
        sync_relations: false,
        ..Default::default()
    };
    assert!(config.validate().is_ok());
}

// ============================================================================
// SyncStatus
// ============================================================================

#[test]
fn test_sync_status_variants() {
    assert_eq!(SyncStatus::Idle, SyncStatus::Idle);
    assert_ne!(SyncStatus::Idle, SyncStatus::Syncing);
    assert_ne!(SyncStatus::Idle, SyncStatus::Disconnected);
    assert_eq!(SyncStatus::Error("x".into()), SyncStatus::Error("x".into()));
    assert_ne!(SyncStatus::Error("x".into()), SyncStatus::Error("y".into()));
}

// ============================================================================
// PulseDBError::Sync variant
// ============================================================================

#[test]
fn test_pulsedb_error_sync_variant() {
    use pulsedb::sync::SyncError;
    use pulsedb::PulseDBError;

    let sync_err = SyncError::transport("connection refused");
    let db_err: PulseDBError = sync_err.into();
    assert!(db_err.is_sync());
    assert!(db_err.to_string().contains("connection refused"));
}

// ============================================================================
// Type serialization roundtrips (bincode)
// ============================================================================

#[test]
fn test_sync_change_bincode_roundtrip() {
    let cid = CollectiveId::new();
    let change = make_change(42, cid);
    let bytes = bincode::serialize(&change).unwrap();
    let restored: SyncChange = bincode::deserialize(&bytes).unwrap();
    assert_eq!(restored.sequence, 42);
    assert_eq!(restored.collective_id, cid);
}

#[test]
fn test_instance_id_bincode_roundtrip() {
    let id = InstanceId::new();
    let bytes = bincode::serialize(&id).unwrap();
    let restored: InstanceId = bincode::deserialize(&bytes).unwrap();
    assert_eq!(id, restored);
}

#[test]
fn test_sync_cursor_bincode_roundtrip() {
    let cursor = SyncCursor {
        instance_id: InstanceId::new(),
        last_sequence: 12345,
    };
    let bytes = bincode::serialize(&cursor).unwrap();
    let restored: SyncCursor = bincode::deserialize(&bytes).unwrap();
    assert_eq!(cursor, restored);
}

#[test]
fn test_sync_config_bincode_roundtrip() {
    let config = SyncConfig {
        direction: SyncDirection::PullOnly,
        batch_size: 250,
        collectives: Some(vec![CollectiveId::new()]),
        ..Default::default()
    };
    let bytes = bincode::serialize(&config).unwrap();
    let restored: SyncConfig = bincode::deserialize(&bytes).unwrap();
    assert_eq!(config.direction, restored.direction);
    assert_eq!(config.batch_size, restored.batch_size);
}

// ============================================================================
// Protocol version constant
// ============================================================================

#[test]
fn test_protocol_version_is_two() {
    assert_eq!(SYNC_PROTOCOL_VERSION, 2);
}

// ============================================================================
// Phase 2: WAL Extension Tests
// ============================================================================

use pulsedb::storage::schema::EntityTypeTag;
use pulsedb::{
    ExperienceType, InsightType, NewDerivedInsight, NewExperience, NewExperienceRelation, PulseDB,
    RelationType,
};

fn open_test_db() -> (PulseDB, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let db = PulseDB::open(dir.path().join("test.db"), Config::default()).unwrap();
    (db, dir)
}

fn minimal_exp(cid: CollectiveId) -> NewExperience {
    NewExperience {
        collective_id: cid,
        content: "test experience".to_string(),
        embedding: Some(vec![0.1f32; 384]),
        ..Default::default()
    }
}

#[test]
fn test_wal_records_collective_creation() {
    let (db, _dir) = open_test_db();

    let cid = db.create_collective("wal-test").unwrap();

    // poll_sync_events should return the collective creation event
    let storage = db.storage_for_test();
    let events = storage.poll_sync_events(0, 100).unwrap();
    assert_eq!(events.len(), 1);
    let (seq, record) = &events[0];
    assert_eq!(*seq, 1);
    assert_eq!(record.entity_type, EntityTypeTag::Collective);
    assert_eq!(record.entity_id, *cid.as_bytes());
}

#[test]
fn test_wal_records_relation_creation_and_deletion() {
    let (db, _dir) = open_test_db();
    let cid = db.create_collective("rel-test").unwrap();

    let exp1 = db.record_experience(minimal_exp(cid)).unwrap();
    let exp2 = db.record_experience(minimal_exp(cid)).unwrap();

    let rel_id = db
        .store_relation(NewExperienceRelation {
            source_id: exp1,
            target_id: exp2,
            relation_type: RelationType::Supports,
            strength: 0.8,
            metadata: None,
        })
        .unwrap();

    // Check WAL: collective(1) + exp(2) + exp(3) + relation(4) = 4 events
    let storage = db.storage_for_test();
    let events = storage.poll_sync_events(0, 100).unwrap();
    assert_eq!(events.len(), 4);
    assert_eq!(events[3].1.entity_type, EntityTypeTag::Relation);
    assert_eq!(events[3].1.entity_id, *rel_id.as_bytes());

    // Delete the relation
    db.delete_relation(rel_id).unwrap();

    let events = storage.poll_sync_events(4, 100).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].1.entity_type, EntityTypeTag::Relation);
}

#[test]
fn test_wal_records_insight_creation_and_deletion() {
    let (db, _dir) = open_test_db();
    let cid = db.create_collective("insight-test").unwrap();
    let exp_id = db.record_experience(minimal_exp(cid)).unwrap();

    let insight_id = db
        .store_insight(NewDerivedInsight {
            collective_id: cid,
            content: "test insight".to_string(),
            embedding: Some(vec![0.1f32; 384]),
            source_experience_ids: vec![exp_id],
            insight_type: InsightType::Pattern,
            confidence: 0.9,
            domain: vec![],
        })
        .unwrap();

    // Check WAL: collective(1) + exp(2) + insight(3) = 3 events
    let storage = db.storage_for_test();
    let events = storage.poll_sync_events(0, 100).unwrap();
    assert_eq!(events.len(), 3);
    assert_eq!(events[2].1.entity_type, EntityTypeTag::Insight);
    assert_eq!(events[2].1.entity_id, *insight_id.as_bytes());

    // Delete insight
    db.delete_insight(insight_id).unwrap();

    let events = storage.poll_sync_events(3, 100).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].1.entity_type, EntityTypeTag::Insight);
}

#[test]
fn test_poll_changes_backward_compat_filters_experience_only() {
    let (db, _dir) = open_test_db();
    let cid = db.create_collective("compat-test").unwrap();

    let exp1 = db.record_experience(minimal_exp(cid)).unwrap();
    let exp2 = db.record_experience(minimal_exp(cid)).unwrap();

    // Create a relation (non-experience WAL event)
    db.store_relation(NewExperienceRelation {
        source_id: exp1,
        target_id: exp2,
        relation_type: RelationType::Supports,
        strength: 0.5,
        metadata: None,
    })
    .unwrap();

    // poll_changes should only return experience events (backward compat)
    let (events, seq) = db.poll_changes(0).unwrap();
    assert_eq!(events.len(), 2); // Only 2 experiences, NOT collective or relation
    assert!(seq > 2); // Seq includes all events
}

#[test]
fn test_echo_prevention_suppresses_wal() {
    use pulsedb::sync::guard::SyncApplyGuard;

    let (db, _dir) = open_test_db();
    let cid = db.create_collective("echo-test").unwrap();

    let initial_seq = db.get_current_sequence().unwrap();

    // Enter sync apply mode — this should suppress WAL recording
    let _guard = SyncApplyGuard::enter();

    // Apply a synced experience (should NOT generate WAL event)
    let timestamp = Timestamp::now();
    let exp = pulsedb::Experience {
        id: pulsedb::ExperienceId::new(),
        collective_id: cid,
        content: "synced experience".to_string(),
        embedding: vec![0.1f32; 384],
        experience_type: ExperienceType::Generic { category: None },
        importance: 0.5,
        confidence: 0.8,
        applications: std::collections::BTreeMap::new(),
        domain: vec![],
        related_files: vec![],
        source_agent: pulsedb::AgentId::new("sync-test"),
        source_task: None,
        timestamp,
        last_reinforced: timestamp,
        archived: false,
    };
    db.apply_synced_experience(exp).unwrap();

    drop(_guard);

    // WAL sequence should NOT have increased
    let new_seq = db.get_current_sequence().unwrap();
    assert_eq!(
        new_seq, initial_seq,
        "WAL should not advance when SyncApplyGuard is active"
    );
}

#[test]
fn test_apply_synced_experience_writes_data() {
    use pulsedb::sync::guard::SyncApplyGuard;

    let (db, _dir) = open_test_db();
    let cid = db.create_collective("apply-test").unwrap();

    let exp_id = pulsedb::ExperienceId::new();
    let timestamp = Timestamp::now();
    let exp = pulsedb::Experience {
        id: exp_id,
        collective_id: cid,
        content: "synced content".to_string(),
        embedding: vec![0.2f32; 384],
        experience_type: ExperienceType::Generic { category: None },
        importance: 0.7,
        confidence: 0.9,
        applications: std::collections::BTreeMap::from([(pulsedb::InstanceId::new(), 5)]),
        domain: vec!["test".to_string()],
        related_files: vec![],
        source_agent: pulsedb::AgentId::new("sync-test"),
        source_task: None,
        timestamp,
        last_reinforced: timestamp,
        archived: false,
    };

    let _guard = SyncApplyGuard::enter();
    db.apply_synced_experience(exp).unwrap();
    drop(_guard);

    // Verify data is retrievable
    let retrieved = db.get_experience(exp_id).unwrap().unwrap();
    assert_eq!(retrieved.content, "synced content");
    assert_eq!(retrieved.applications(), 5);
}

#[test]
fn test_apply_synced_collective_creates_indexes() {
    use pulsedb::sync::guard::SyncApplyGuard;

    let (db, _dir) = open_test_db();

    let collective = Collective {
        id: CollectiveId::new(),
        name: "synced-collective".to_string(),
        owner_id: None,
        embedding_dimension: 384,
        created_at: Timestamp::now(),
        updated_at: Timestamp::now(),
    };

    let _guard = SyncApplyGuard::enter();
    db.apply_synced_collective(collective.clone()).unwrap();
    drop(_guard);

    // Verify collective exists via storage
    let retrieved = db
        .storage_for_test()
        .get_collective(collective.id)
        .unwrap()
        .unwrap();
    assert_eq!(retrieved.name, "synced-collective");

    // Verify we can record experiences in the synced collective (HNSW index exists)
    let exp_id = db.record_experience(minimal_exp(collective.id)).unwrap();
    assert!(db.get_experience(exp_id).unwrap().is_some());
}

#[test]
fn test_entity_type_tag_values() {
    assert_eq!(EntityTypeTag::Experience as u8, 0);
    assert_eq!(EntityTypeTag::Relation as u8, 1);
    assert_eq!(EntityTypeTag::Insight as u8, 2);
    assert_eq!(EntityTypeTag::Collective as u8, 3);
    assert_eq!(EntityTypeTag::default(), EntityTypeTag::Experience);
}

#[test]
fn test_change_poller_poll_sync_events() {
    use pulsedb::ChangePoller;

    let (db, _dir) = open_test_db();
    let cid = db.create_collective("poller-test").unwrap();
    db.record_experience(minimal_exp(cid)).unwrap();

    let storage = db.storage_for_test();
    let mut poller = ChangePoller::new();

    // poll_sync_events returns ALL entity types
    let events = poller.poll_sync_events(storage).unwrap();
    assert_eq!(events.len(), 2); // collective + experience
    assert_eq!(events[0].1.entity_type, EntityTypeTag::Collective);
    assert_eq!(events[1].1.entity_type, EntityTypeTag::Experience);

    // Cursor advanced — next poll returns nothing
    let events = poller.poll_sync_events(storage).unwrap();
    assert!(events.is_empty());
}

// ============================================================================
// Phase 5: WAL Compaction Tests
// ============================================================================

#[test]
fn test_compact_wal_no_peers() {
    let (db, _dir) = open_test_db();
    let cid = db.create_collective("compact-test").unwrap();
    db.record_experience(minimal_exp(cid)).unwrap();

    // No peers saved — compaction should be a no-op
    let deleted = db.compact_wal().unwrap();
    assert_eq!(deleted, 0, "Should not compact when no peers exist");

    // WAL events still exist
    let (events, _) = db.poll_changes(0).unwrap();
    assert_eq!(events.len(), 1); // experience event (collective filtered)
}

#[test]
fn test_compact_wal_single_peer() {
    let (db, _dir) = open_test_db();
    let cid = db.create_collective("compact-1").unwrap();

    // Create 5 experiences (WAL: collective=1, exp=2,3,4,5,6)
    for _ in 0..5 {
        db.record_experience(minimal_exp(cid)).unwrap();
    }
    assert_eq!(db.get_current_sequence().unwrap(), 6);

    // Save peer cursor at sequence 4
    let peer_id = InstanceId::new();
    let cursor = SyncCursor {
        instance_id: peer_id,
        last_sequence: 4,
    };
    db.storage_for_test().save_sync_cursor(&cursor).unwrap();

    // Compact — should delete events 1-4
    let deleted = db.compact_wal().unwrap();
    assert_eq!(deleted, 4, "Should delete events up to min cursor");

    // Remaining events (5 and 6) should still be pollable
    let storage = db.storage_for_test();
    let events = storage.poll_sync_events(4, 100).unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].0, 5);
    assert_eq!(events[1].0, 6);
}

#[test]
fn test_compact_wal_multiple_peers_uses_min_cursor() {
    let (db, _dir) = open_test_db();
    let cid = db.create_collective("compact-multi").unwrap();

    // Create 10 experiences (WAL: collective=1, exp=2..11)
    for _ in 0..10 {
        db.record_experience(minimal_exp(cid)).unwrap();
    }

    // Peer A at seq 3, Peer B at seq 7
    let peer_a = InstanceId::new();
    let peer_b = InstanceId::new();
    db.storage_for_test()
        .save_sync_cursor(&SyncCursor {
            instance_id: peer_a,
            last_sequence: 3,
        })
        .unwrap();
    db.storage_for_test()
        .save_sync_cursor(&SyncCursor {
            instance_id: peer_b,
            last_sequence: 7,
        })
        .unwrap();

    // Compact — uses min(3, 7) = 3
    let deleted = db.compact_wal().unwrap();
    assert_eq!(deleted, 3, "Should compact up to min cursor (3)");

    // Events 4+ should still exist
    let storage = db.storage_for_test();
    let events = storage.poll_sync_events(3, 100).unwrap();
    assert_eq!(events.len(), 8); // events 4-11
    assert_eq!(events[0].0, 4);
}

#[test]
fn test_compact_wal_preserves_events_above_cursor() {
    let (db, _dir) = open_test_db();
    let cid = db.create_collective("preserve-test").unwrap();

    db.record_experience(minimal_exp(cid)).unwrap();
    db.record_experience(minimal_exp(cid)).unwrap();
    db.record_experience(minimal_exp(cid)).unwrap();

    // Peer at seq 2 (collective=1, exp=2)
    db.storage_for_test()
        .save_sync_cursor(&SyncCursor {
            instance_id: InstanceId::new(),
            last_sequence: 2,
        })
        .unwrap();

    db.compact_wal().unwrap();

    // poll_changes should still work from after the compacted range
    let (events, _seq) = db.poll_changes(2).unwrap();
    assert!(
        !events.is_empty(),
        "Events above cursor should survive compaction"
    );
}

#[test]
fn test_compact_wal_idempotent() {
    let (db, _dir) = open_test_db();
    let cid = db.create_collective("idempotent").unwrap();
    db.record_experience(minimal_exp(cid)).unwrap();

    db.storage_for_test()
        .save_sync_cursor(&SyncCursor {
            instance_id: InstanceId::new(),
            last_sequence: 2,
        })
        .unwrap();

    let deleted1 = db.compact_wal().unwrap();
    assert_eq!(deleted1, 2);

    // Second compaction with same cursor — nothing to delete
    let deleted2 = db.compact_wal().unwrap();
    assert_eq!(deleted2, 0, "Second compaction should be a no-op");
}
