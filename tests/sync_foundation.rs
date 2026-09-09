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
    HandshakeRequest, InstanceId, PullRequest, PushRequest, SyncChange, SyncCursor, SyncEntityType,
    SyncPayload, SyncPosition, SyncStatus,
};
use pulsedb::sync::SYNC_PROTOCOL_VERSION;
use pulsedb::{Collective, CollectiveId, Config, Timestamp};
use tempfile::tempdir;

/// The outbound budget a DIRECT transport call supplies.
///
/// A direct caller is not the pusher, so nothing computed a packing cap for it;
/// it states its own budget explicitly. The encoder still enforces it — see the
/// direct-caller regression, which passes a budget smaller than the request and
/// gets `RequestTooLarge` back before anything is transmitted.
const DIRECT_SEND_BUDGET: usize = 64 * 1024 * 1024;

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
        push_sequence: 42,
        pull_sequence: 0,
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
            push_sequence: 10,
            pull_sequence: 0,
        })
        .unwrap();

    // Update to higher sequence
    storage
        .save_sync_cursor(&SyncCursor {
            instance_id: peer_id,
            push_sequence: 50,
            pull_sequence: 0,
        })
        .unwrap();

    let loaded = storage.load_sync_cursor(&peer_id).unwrap().unwrap();
    assert_eq!(loaded.push_sequence, 50);
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
            push_sequence: 10,
            pull_sequence: 0,
        })
        .unwrap();
    storage
        .save_sync_cursor(&SyncCursor {
            instance_id: peer_b,
            push_sequence: 20,
            pull_sequence: 0,
        })
        .unwrap();
    storage
        .save_sync_cursor(&SyncCursor {
            instance_id: peer_c,
            push_sequence: 30,
            pull_sequence: 0,
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
                push_sequence: 99,
                pull_sequence: 0,
            })
            .unwrap();
        drop(storage);
    }

    // Reopen and verify
    {
        let storage = pulsedb::storage::RedbStorage::open(&path, &config).unwrap();
        let loaded = storage.load_sync_cursor(&peer_id).unwrap().unwrap();
        assert_eq!(loaded.push_sequence, 99);
    }
}

// ============================================================================
// Cursor positions are monotonic non-decreasing (PR #88 review, class A)
// ============================================================================

/// A push whose FIRST change fails to apply is acknowledged as sequence 0.
/// Writing that 0 over an already-advanced `push_sequence` would permanently
/// wedge `compact_wal` — it blocks while any peer's push position is 0 — and
/// re-push the whole WAL every cycle forever. The zero ack must leave the
/// stored position exactly where it was.
#[test]
fn test_zero_push_acknowledgement_does_not_rewind_the_stored_position() {
    let dir = tempdir().unwrap();
    let config = Config::default();
    let storage =
        pulsedb::storage::RedbStorage::open(dir.path().join("cursor.db"), &config).unwrap();

    let peer_id = InstanceId::new();

    // The peer has acknowledged through 120 over previous cycles.
    storage.update_push_cursor(&peer_id, 120).unwrap();
    assert_eq!(
        storage
            .load_sync_cursor(&peer_id)
            .unwrap()
            .unwrap()
            .push_sequence,
        120
    );

    // The next batch's first change fails to apply: the peer acknowledges 0.
    storage.update_push_cursor(&peer_id, 0).unwrap();

    let loaded = storage.load_sync_cursor(&peer_id).unwrap().unwrap();
    assert_eq!(
        loaded.push_sequence, 120,
        "a zero acknowledgement must never move the push position backwards"
    );
    assert_eq!(loaded.pull_sequence, 0, "the pull side is untouched");
}

/// The same rule for every backwards acknowledgement, not just zero, and on
/// both sides of the record.
#[test]
fn test_cursor_positions_are_monotonic_non_decreasing() {
    let dir = tempdir().unwrap();
    let config = Config::default();
    let storage =
        pulsedb::storage::RedbStorage::open(dir.path().join("cursor.db"), &config).unwrap();

    let peer_id = InstanceId::new();

    storage.update_push_cursor(&peer_id, 50).unwrap();
    storage.update_pull_cursor(&peer_id, 70).unwrap();

    // Backwards acknowledgements are absorbed.
    storage.update_push_cursor(&peer_id, 20).unwrap();
    storage.update_pull_cursor(&peer_id, 0).unwrap();

    let loaded = storage.load_sync_cursor(&peer_id).unwrap().unwrap();
    assert_eq!(loaded.push_sequence, 50);
    assert_eq!(loaded.pull_sequence, 70);

    // Forward acknowledgements still advance.
    storage.update_push_cursor(&peer_id, 51).unwrap();
    storage.update_pull_cursor(&peer_id, 71).unwrap();

    let loaded = storage.load_sync_cursor(&peer_id).unwrap().unwrap();
    assert_eq!(loaded.push_sequence, 51);
    assert_eq!(loaded.pull_sequence, 71);
}

/// The monotonic clamp must not stop an absent record from being created — a
/// zero-valued update is how an empty pull registers a `PullOnly` peer.
#[test]
fn test_zero_update_still_registers_an_unknown_peer() {
    let dir = tempdir().unwrap();
    let config = Config::default();
    let storage =
        pulsedb::storage::RedbStorage::open(dir.path().join("cursor.db"), &config).unwrap();

    let peer_id = InstanceId::new();
    storage.update_pull_cursor(&peer_id, 0).unwrap();

    let loaded = storage
        .load_sync_cursor(&peer_id)
        .unwrap()
        .expect("an empty pull must still create the peer's record");
    assert_eq!(loaded.push_sequence, 0);
    assert_eq!(loaded.pull_sequence, 0);
}

/// The pull path persists its position on every cycle, empty pulls included,
/// and the intervals default to one second — so a no-op update must not pay a
/// durable write. An update that changes nothing about an existing record
/// leaves the store file untouched.
///
/// # Why the witness is `(len, mtime)` and not the bytes
///
/// redb takes an exclusive **whole-file** lock while the store is open, so
/// reading the file's bytes fails on Windows. `fs::metadata` does not touch the
/// locked byte range and is legal on every platform this ships to.
///
/// Both components are needed and neither alone would do: a commit need not
/// resize the file, so length alone can miss a real write, and a coarse
/// filesystem clock can miss an mtime change inside one tick. Dropping and
/// reopening the store to take a byte snapshot is not an alternative — that
/// operation itself makes redb flush and rewrite allocator state, so the
/// snapshot would witness the drop rather than the guard.
///
/// The positive control below is what keeps the no-op assertion from passing
/// vacuously: if this filesystem cannot show a real commit through either
/// component, the test FAILS rather than silently asserting nothing.
#[test]
fn test_no_op_cursor_update_does_not_touch_the_store() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("cursor.db");
    let config = Config::default();
    let storage = pulsedb::storage::RedbStorage::open(&path, &config).unwrap();

    let peer_id = InstanceId::new();
    storage.update_pull_cursor(&peer_id, 7).unwrap();

    let witness = || {
        let m = std::fs::metadata(&path).expect("the store file is always stat-able");
        (m.len(), m.modified().expect("mtime is available"))
    };
    let before = witness();

    // Ten idle cycles' worth of "persist the position we already have".
    for _ in 0..10 {
        storage.update_pull_cursor(&peer_id, 7).unwrap();
        storage.update_push_cursor(&peer_id, 0).unwrap();
    }

    assert_eq!(
        before,
        witness(),
        "a cursor update that changes nothing must not commit a write transaction"
    );

    // Positive control: a REAL advance must move the witness in this
    // environment, or the assertion above proves nothing. This is the
    // non-vacuity guard, not a claim about clock resolution in general.
    storage.update_pull_cursor(&peer_id, 8).unwrap();
    assert_ne!(
        before,
        witness(),
        "the witness did not move for a real cursor commit, so the no-op \
         assertion above is vacuous on this filesystem"
    );

    let loaded = storage.load_sync_cursor(&peer_id).unwrap().unwrap();
    assert_eq!(loaded.pull_sequence, 8);
    assert_eq!(loaded.push_sequence, 0);
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
    make_change_from(seq, cid, InstanceId::new())
}

fn make_change_from(seq: u64, cid: CollectiveId, source: InstanceId) -> SyncChange {
    SyncChange {
        sequence: seq,
        source_instance: source,
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

/// A routed pull addressed to `target`.
fn pull_request(target: InstanceId, from: u64, batch_size: u64) -> PullRequest {
    PullRequest {
        protocol_version: SYNC_PROTOCOL_VERSION,
        source_instance: InstanceId::new(),
        target_instance: target,
        cursor: SyncPosition::new(target, from),
        batch_size,
        reply_limit_bytes: 64 * 1024 * 1024,
        collectives: None,
    }
}

/// A routed push from `source` to `target`.
fn push_request(source: InstanceId, target: InstanceId, changes: Vec<SyncChange>) -> PushRequest {
    PushRequest {
        protocol_version: SYNC_PROTOCOL_VERSION,
        source_instance: source,
        target_instance: target,
        reply_limit_bytes: 64 * 1024 * 1024,
        changes,
    }
}

#[tokio::test]
async fn test_memory_transport_full_roundtrip() {
    let (local, remote) = InMemorySyncTransport::new_pair();
    let cid = CollectiveId::new();
    let sender = InstanceId::new();

    // Handshake
    let hs_req = HandshakeRequest {
        instance_id: sender,
        protocol_version: SYNC_PROTOCOL_VERSION,
        capabilities: vec!["push".into(), "pull".into()],
    };
    let hs_resp = local.handshake(hs_req, DIRECT_SEND_BUDGET).await.unwrap();
    assert!(hs_resp.accepted);
    assert_eq!(hs_resp.protocol_version, SYNC_PROTOCOL_VERSION);
    assert_eq!(
        hs_resp.receive_limit_bytes as usize,
        local.receive_limit_bytes(),
        "the handshake advertises the responder's ACTUAL inbound limit"
    );

    // Health check
    assert!(local.health_check().await.is_ok());
    assert!(remote.health_check().await.is_ok());

    // Push changes into the SENDER's lane.
    let changes = vec![
        make_change_from(1, cid, sender),
        make_change_from(2, cid, sender),
        make_change_from(3, cid, sender),
    ];
    let ack = local
        .push_changes(
            push_request(sender, local.instance_id(), changes),
            DIRECT_SEND_BUDGET,
        )
        .await
        .unwrap()
        .into_result(local.instance_id())
        .unwrap();
    assert_eq!(ack.accepted, 3);
    assert_eq!(ack.rejected, 0);
    assert_eq!(ack.total, 3);
    assert_eq!(
        ack.wal_owner, sender,
        "a push acknowledges a position in the SENDER's WAL"
    );

    // A pull reads the addressed peer's OWN lane, which the push did not touch.
    let page = remote
        .pull_changes(
            pull_request(remote.instance_id(), 0, 500),
            DIRECT_SEND_BUDGET,
        )
        .await
        .unwrap()
        .into_result(remote.instance_id())
        .unwrap();
    assert!(
        page.changes.is_empty(),
        "push and pull are separate lanes: one peer's WAL is not the other's"
    );

    // Seeded into the peer's own lane, the same changes come back in order.
    remote.seed(vec![
        make_change_from(1, cid, remote.instance_id()),
        make_change_from(2, cid, remote.instance_id()),
        make_change_from(3, cid, remote.instance_id()),
    ]);
    let page = remote
        .pull_changes(
            pull_request(remote.instance_id(), 0, 500),
            DIRECT_SEND_BUDGET,
        )
        .await
        .unwrap()
        .into_result(remote.instance_id())
        .unwrap();
    assert_eq!(page.changes.len(), 3);
    assert!(!page.has_more);
    assert_eq!(page.changes[0].sequence, 1);
    assert_eq!(page.changes[1].sequence, 2);
    assert_eq!(page.changes[2].sequence, 3);
    assert_eq!(page.scan_position.instance_id, remote.instance_id());
    assert_eq!(page.scan_position.sequence, 3);
}

#[tokio::test]
async fn test_memory_transport_incremental_pull() {
    let (_local, remote) = InMemorySyncTransport::new_pair();
    let cid = CollectiveId::new();
    let peer = remote.instance_id();

    remote.seed((1..=5).map(|s| make_change_from(s, cid, peer)).collect());

    let page1 = remote
        .pull_changes(pull_request(peer, 0, 2), DIRECT_SEND_BUDGET)
        .await
        .unwrap()
        .into_result(peer)
        .unwrap();
    assert_eq!(page1.changes.len(), 2);
    assert!(page1.has_more);
    assert_eq!(page1.scan_position.sequence, 2);

    let page2 = remote
        .pull_changes(
            pull_request(peer, page1.scan_position.sequence, 2),
            DIRECT_SEND_BUDGET,
        )
        .await
        .unwrap()
        .into_result(peer)
        .unwrap();
    assert_eq!(page2.changes.len(), 2);
    assert!(page2.has_more);
    assert_eq!(page2.scan_position.sequence, 4);

    let page3 = remote
        .pull_changes(
            pull_request(peer, page2.scan_position.sequence, 100),
            DIRECT_SEND_BUDGET,
        )
        .await
        .unwrap()
        .into_result(peer)
        .unwrap();
    assert_eq!(page3.changes.len(), 1);
    assert!(!page3.has_more);
    assert_eq!(page3.scan_position.sequence, 5);
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
    assert_eq!(config.batch_size, 250);
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
fn test_sync_change_postcard_roundtrip() {
    let cid = CollectiveId::new();
    let change = make_change(42, cid);
    let bytes = postcard::to_stdvec(&change).unwrap();
    let restored: SyncChange = postcard::from_bytes(&bytes).unwrap();
    assert_eq!(restored.sequence, 42);
    assert_eq!(restored.collective_id, cid);
}

#[test]
fn test_instance_id_postcard_roundtrip() {
    let id = InstanceId::new();
    let bytes = postcard::to_stdvec(&id).unwrap();
    let restored: InstanceId = postcard::from_bytes(&bytes).unwrap();
    assert_eq!(id, restored);
}

#[test]
fn test_sync_cursor_postcard_roundtrip() {
    let cursor = SyncCursor {
        instance_id: InstanceId::new(),
        push_sequence: 12345,
        pull_sequence: 0,
    };
    let bytes = postcard::to_stdvec(&cursor).unwrap();
    let restored: SyncCursor = postcard::from_bytes(&bytes).unwrap();
    assert_eq!(cursor, restored);
}

#[test]
fn test_sync_config_postcard_roundtrip() {
    let config = SyncConfig {
        direction: SyncDirection::PullOnly,
        batch_size: 250,
        collectives: Some(vec![CollectiveId::new()]),
        ..Default::default()
    };
    let bytes = postcard::to_stdvec(&config).unwrap();
    let restored: SyncConfig = postcard::from_bytes(&bytes).unwrap();
    assert_eq!(config.direction, restored.direction);
    assert_eq!(config.batch_size, restored.batch_size);
}

// ============================================================================
// Protocol version constant
// ============================================================================

#[test]
fn recovery_v5_protocol_and_wire_versions_are_five_and_four() {
    // Bumped 4→5 in 0.8.0: wire-carried embeddings, routed requests,
    // responder-named replies and exact body budgets. There is no v4
    // interoperability — both replicas upgrade.
    assert_eq!(SYNC_PROTOCOL_VERSION, 5);
    // The frame header moved in lockstep: it grew an operation byte, so a v4
    // frame is not a v5 frame even where the body would have decoded.
    assert_eq!(pulsedb::sync::WIRE_FORMAT_VERSION, 4);
    assert_eq!(pulsedb::sync::SYNC_WIRE_PREAMBLE_LEN, 4);
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
        tags: std::collections::BTreeMap::new(),
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
        tags: std::collections::BTreeMap::new(),
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

/// #96 (atomicity half): a sync create whose embedding cannot go into the
/// collective's vector index must leave NOTHING behind — not the experience
/// row, not its secondary index entries, not a WAL event.
///
/// The zero-length embedding is the real case: `Experience::embedding` is
/// `#[serde(skip)]`, so an `ExperienceCreated` crossing a serializing transport
/// arrives with an empty vector. Saving the record first and only then failing
/// the index insert left `get_experience` answering `Some` for something
/// `search_similar` could never find, and made the failure one-shot — the
/// applier's create arm short-circuits on an existing record, so every retry
/// resolved as applied and carried the cursors past it.
#[test]
fn apply_synced_experience_writes_nothing_when_the_embedding_cannot_be_indexed() {
    use pulsedb::sync::guard::SyncApplyGuard;

    let (db, _dir) = open_test_db();
    let cid = db.create_collective("apply-atomicity").unwrap();
    let seq_before = db.get_current_sequence().unwrap();

    let exp_id = pulsedb::ExperienceId::new();
    let timestamp = Timestamp::now();
    let exp = pulsedb::Experience {
        id: exp_id,
        collective_id: cid,
        content: "arrives without its vector".to_string(),
        // What a serializing transport delivers (#96): no embedding at all.
        embedding: vec![],
        experience_type: ExperienceType::Generic { category: None },
        importance: 0.5,
        confidence: 0.8,
        applications: std::collections::BTreeMap::new(),
        domain: vec![],
        tags: std::collections::BTreeMap::from([("k".to_string(), "v".to_string())]),
        related_files: vec![],
        source_agent: pulsedb::AgentId::new("sync-test"),
        source_task: None,
        timestamp,
        last_reinforced: timestamp,
        archived: false,
    };

    let guard = SyncApplyGuard::enter();
    let err = db
        .apply_synced_experience(exp)
        .expect_err("an embedding the index cannot take must not be applied");
    drop(guard);

    assert!(
        db.get_experience(exp_id).unwrap().is_none(),
        "the record must not be in the store after a rejected apply, got: {err}"
    );
    assert!(
        db.list_experiences(cid, 100, 0)
            .unwrap()
            .iter()
            .all(|e| e.id != exp_id),
        "the by-collective index must not carry the rejected experience either"
    );
    assert_eq!(
        db.get_current_sequence().unwrap(),
        seq_before,
        "the rejected apply must not record a WAL event (SyncApplyGuard held)"
    );
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
        push_sequence: 4,
        pull_sequence: 0,
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
            push_sequence: 3,
            pull_sequence: 0,
        })
        .unwrap();
    db.storage_for_test()
        .save_sync_cursor(&SyncCursor {
            instance_id: peer_b,
            push_sequence: 7,
            pull_sequence: 0,
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
            push_sequence: 2,
            pull_sequence: 0,
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
            push_sequence: 2,
            pull_sequence: 0,
        })
        .unwrap();

    let deleted1 = db.compact_wal().unwrap();
    assert_eq!(deleted1, 2);

    // Second compaction with same cursor — nothing to delete
    let deleted2 = db.compact_wal().unwrap();
    assert_eq!(deleted2, 0, "Second compaction should be a no-op");
}
