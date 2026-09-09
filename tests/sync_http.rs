//! Integration tests for Phase 4: HTTP Sync Transport.
//!
//! Spins up a real Axum server with SyncServer handlers, then tests
//! HttpSyncTransport against it.

#![cfg(feature = "sync-http")]

use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::Router;
use tokio::net::TcpListener;

use pulsedb::sync::config::{SyncConfig, SyncDirection};
use pulsedb::sync::error::SyncError;
use pulsedb::sync::guard::SyncApplyGuard;
use pulsedb::sync::manager::SyncManager;
use pulsedb::sync::server::SyncServer;
use pulsedb::sync::transport::SyncTransport;
use pulsedb::sync::transport_http::HttpSyncTransport;
use pulsedb::sync::types::{
    HandshakeRequest, InstanceId, PullPage, PullRequest, PushRequest, SyncPosition, WireReply,
};
use pulsedb::sync::wire::{self, WireOperation};
use pulsedb::sync::SyncStatus;
use pulsedb::sync::{
    read_wire_preamble, write_wire_preamble, SYNC_PROTOCOL_VERSION, SYNC_WIRE_MAGIC,
    SYNC_WIRE_PREAMBLE_LEN, WIRE_FORMAT_VERSION,
};
use pulsedb::{CollectiveId, Config, NewExperience, PulseDB};
use tempfile::tempdir;

/// The outbound budget a DIRECT transport call supplies.
///
/// A direct caller is not the pusher, so nothing computed a packing cap for it;
/// it states its own budget explicitly. The encoder still enforces it — see the
/// direct-caller regression, which passes a budget smaller than the request and
/// gets `RequestTooLarge` back before anything is transmitted.
const DIRECT_SEND_BUDGET: usize = 64 * 1024 * 1024;

// ============================================================================
// Axum handlers (test server)
// ============================================================================

/// Maps a typed `SyncError` from the byte-level handlers onto an HTTP status:
/// an oversized body is `413 Payload Too Large`, everything else is `400`.
fn status_for(err: SyncError) -> StatusCode {
    if err.is_payload_too_large() {
        StatusCode::PAYLOAD_TOO_LARGE
    } else {
        StatusCode::BAD_REQUEST
    }
}

async fn handle_health(State(server): State<Arc<SyncServer>>) -> StatusCode {
    match server.handle_health() {
        Ok(()) => StatusCode::OK,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

async fn handle_handshake(
    State(server): State<Arc<SyncServer>>,
    body: Bytes,
) -> Result<Vec<u8>, StatusCode> {
    server.handle_handshake_bytes(&body).map_err(status_for)
}

async fn handle_push(
    State(server): State<Arc<SyncServer>>,
    body: Bytes,
) -> Result<Vec<u8>, StatusCode> {
    server.handle_push_bytes(&body).map_err(status_for)
}

async fn handle_pull(
    State(server): State<Arc<SyncServer>>,
    body: Bytes,
) -> Result<Vec<u8>, StatusCode> {
    server.handle_pull_bytes(&body).map_err(status_for)
}

/// The framework body limit the library CANNOT install for a consumer.
///
/// `SyncServer::handle_*_bytes` is handed an already-buffered `Bytes`: by the
/// time it can compare a length, the allocation has happened. The byte cap it
/// owns bounds what it will *decode*, not what the framework will *buffer*, so
/// a consumer must set a body limit upstream. This adapter is the worked
/// example — it is set to the server's own cap plus the frame header.
const ADAPTER_BODY_LIMIT: usize = 64 * 1024 * 1024 + SYNC_WIRE_PREAMBLE_LEN;

fn sync_router(server: Arc<SyncServer>) -> Router {
    Router::new()
        .route("/sync/health", get(handle_health))
        .route("/sync/handshake", post(handle_handshake))
        .route("/sync/push", post(handle_push))
        .route("/sync/pull", post(handle_pull))
        // Upstream of every handler, and of `Bytes` extraction.
        .layer(axum::extract::DefaultBodyLimit::max(ADAPTER_BODY_LIMIT))
        .with_state(server)
}

// ============================================================================
// Test helpers
// ============================================================================

struct TestServer {
    base_url: String,
    db: Arc<PulseDB>,
    server: Arc<SyncServer>,
    _dir: tempfile::TempDir,
}

async fn start_test_server() -> TestServer {
    let dir = tempdir().unwrap();
    let db = Arc::new(PulseDB::open(dir.path().join("server.db"), Config::default()).unwrap());
    let server = Arc::new(SyncServer::new(Arc::clone(&db), SyncConfig::default()).unwrap());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{}", addr);

    let router = sync_router(Arc::clone(&server));
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    // Give server a moment to start
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    TestServer {
        base_url,
        db,
        server,
        _dir: dir,
    }
}

fn minimal_exp(cid: CollectiveId) -> NewExperience {
    NewExperience {
        collective_id: cid,
        content: format!("http-test-{}", uuid::Uuid::now_v7()),
        embedding: Some(vec![0.1f32; 384]),
        ..Default::default()
    }
}

/// Builds a bare in-process `SyncServer` (no HTTP) for byte-level handler tests
/// that need the typed `SyncError` back rather than an HTTP status code.
fn in_process_server() -> (Arc<SyncServer>, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let db = Arc::new(PulseDB::open(dir.path().join("server.db"), Config::default()).unwrap());
    let server = Arc::new(SyncServer::new(db, SyncConfig::default()).unwrap());
    (server, dir)
}

/// A valid postcard-encoded handshake request body (no preamble), used as the
/// payload under test for preamble-framing cases.
fn handshake_body_bytes() -> Vec<u8> {
    postcard::to_allocvec(&handshake_request()).unwrap()
}

fn handshake_request() -> HandshakeRequest {
    HandshakeRequest {
        instance_id: InstanceId::new(),
        protocol_version: SYNC_PROTOCOL_VERSION,
        capabilities: vec!["push".into()],
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

/// Everything a misrouted or refused request must leave untouched: the WAL
/// head and every persisted cursor row.
///
/// Compared before and after, this is what "no side effects" means concretely —
/// not an absence of one record, but an absence of any movement at all.
fn snapshot(db: &PulseDB) -> (u64, Vec<pulsedb::sync::types::SyncCursor>) {
    let storage = db.storage_for_test();
    let mut cursors = storage.list_sync_cursors().unwrap();
    cursors.sort_by_key(|c| c.instance_id);
    (db.get_current_sequence().unwrap(), cursors)
}

/// A routed push from `source` to `target`.
fn push_request(
    source: InstanceId,
    target: InstanceId,
    changes: Vec<pulsedb::sync::types::SyncChange>,
) -> PushRequest {
    PushRequest {
        protocol_version: SYNC_PROTOCOL_VERSION,
        source_instance: source,
        target_instance: target,
        reply_limit_bytes: 64 * 1024 * 1024,
        changes,
    }
}

// ============================================================================
// Tests
// ============================================================================

#[tokio::test]
async fn test_http_health_check() {
    let server = start_test_server().await;
    let transport = HttpSyncTransport::new(&server.base_url);

    let result = transport.health_check().await;
    assert!(result.is_ok(), "Health check should succeed");
}

#[tokio::test]
async fn test_http_handshake() {
    let server = start_test_server().await;
    let transport = HttpSyncTransport::new(&server.base_url);

    let request = HandshakeRequest {
        instance_id: InstanceId::new(),
        protocol_version: SYNC_PROTOCOL_VERSION,
        capabilities: vec!["push".into(), "pull".into()],
    };

    let response = transport
        .handshake(request, DIRECT_SEND_BUDGET)
        .await
        .unwrap();
    assert!(response.accepted);
    assert_eq!(response.protocol_version, SYNC_PROTOCOL_VERSION);
    assert_ne!(response.instance_id, InstanceId::nil());
}

#[tokio::test]
async fn test_http_push_and_pull_roundtrip() {
    let server = start_test_server().await;
    let transport = HttpSyncTransport::new(&server.base_url);

    // Create data on server
    let cid = server.db.create_collective("http-test").unwrap();
    let _exp_id = server.db.record_experience(minimal_exp(cid)).unwrap();

    // Pull changes via HTTP
    let peer = server.db.instance_id();
    let page = transport
        .pull_changes(pull_request(peer, 0, 100), DIRECT_SEND_BUDGET)
        .await
        .unwrap()
        .into_result(peer)
        .unwrap();

    // Should have collective + experience
    assert!(!page.changes.is_empty(), "Should have changes to pull");
    assert!(page.changes.len() >= 2); // collective + experience at minimum
    assert_eq!(page.scan_position.instance_id, peer);
}

/// **Acceptance test for issue #96.** Formerly `#[ignore]`d as failing by
/// design; protocol v5 carries the vector, so it now runs and passes.
///
/// Its assertions are the ones it always had — pull the collective and the
/// experience, report `Ok(())`, leave the experience findable by semantic
/// search on the client. Only the `#[ignore]` is gone. The defect
/// characterizations beside it were flipped to success rather than deleted, and
/// the malformed-vector refusal is covered separately, so the failure path
/// keeps its coverage.
#[tokio::test]
async fn acceptance_96_http_catch_up_delivers_a_searchable_experience() {
    let server = start_test_server().await;
    let dir_client = tempdir().unwrap();
    let db_client =
        Arc::new(PulseDB::open(dir_client.path().join("client.db"), Config::default()).unwrap());

    let transport = HttpSyncTransport::new(&server.base_url);
    let config = SyncConfig::default();
    let mut manager =
        SyncManager::new(Arc::clone(&db_client), Box::new(transport), config).unwrap();

    // Create data on server
    let cid = server.db.create_collective("full-sync").unwrap();
    let exp_id = server.db.record_experience(minimal_exp(cid)).unwrap();

    // Client does initial sync to pull all server data
    manager
        .initial_sync(None)
        .await
        .expect("a catch-up that pulled everything must report completion");

    // Client should have the collective and experience
    assert!(
        db_client.get_collective(cid).unwrap().is_some(),
        "Collective should sync via HTTP"
    );
    assert!(
        db_client.get_experience(exp_id).unwrap().is_some(),
        "Experience should sync via HTTP"
    );
    // ...and the experience must arrive WITH its embedding, so it is findable.
    let hits = db_client
        .search_similar(cid, &vec![0.1f32; 384], 5)
        .unwrap();
    assert!(
        hits.iter().any(|hit| hit.experience.id == exp_id),
        "the synced experience must be searchable on the client, which needs \
         its embedding to have crossed the wire"
    );
}

/// Issue #96, pull side: an HTTP catch-up delivers the experience WITH its
/// embedding, so the client can find it by semantic search.
///
/// This test used to pin the defect — `Experience::embedding` is
/// `#[serde(skip)]`, so an `ExperienceCreated` crossing any serializing
/// transport arrived with an empty vector and the applier's create failed the
/// collective's dimension check. Protocol v5 carries the vector in
/// [`SyncExperience`], beside the record rather than inside it, so the disk
/// encoding is unchanged and the wire one is complete.
///
/// The expectations are FLIPPED, not deleted: `initial_sync` completes, the
/// experience is present, its vector is byte-equal to the server's, and it is
/// searchable — on this handle and on a reopened one.
#[tokio::test]
async fn test_http_full_sync_via_manager_delivers_the_embedding() {
    let server = start_test_server().await;
    let dir_client = tempdir().unwrap();
    let client_path = dir_client.path().join("client.db");
    let db_client = Arc::new(PulseDB::open(&client_path, Config::default()).unwrap());

    let transport = HttpSyncTransport::new(&server.base_url);
    let config = SyncConfig::default();
    let mut manager =
        SyncManager::new(Arc::clone(&db_client), Box::new(transport), config).unwrap();

    let cid = server.db.create_collective("full-sync").unwrap();
    let exp_id = server.db.record_experience(minimal_exp(cid)).unwrap();
    let server_vector = server
        .db
        .get_experience(exp_id)
        .unwrap()
        .unwrap()
        .embedding
        .clone();
    assert!(!server_vector.is_empty(), "the fixture must have a vector");

    manager
        .initial_sync(None)
        .await
        .expect("a catch-up that pulled everything must report completion");

    assert!(
        db_client.get_collective(cid).unwrap().is_some(),
        "Collective should sync via HTTP"
    );
    let arrived = db_client
        .get_experience(exp_id)
        .unwrap()
        .expect("the experience must arrive, vector and all");
    assert_eq!(
        arrived.embedding, server_vector,
        "the vector must cross the wire byte for byte — no re-embedding, no truncation"
    );
    let hits = db_client
        .search_similar(cid, &vec![0.1f32; 384], 5)
        .unwrap();
    assert!(
        hits.iter().any(|hit| hit.experience.id == exp_id),
        "the synced experience must be searchable on the client, which needs its \
         embedding to have crossed the wire"
    );

    // Reopen: the vector is in the store, not just in the live index. The
    // manager holds its own `Arc<PulseDB>`, so it has to go first.
    drop(manager);
    drop(db_client);
    let reopened = PulseDB::open(&client_path, Config::default()).unwrap();
    assert_eq!(
        reopened.get_experience(exp_id).unwrap().unwrap().embedding,
        server_vector
    );
    assert!(
        reopened
            .search_similar(cid, &vec![0.1f32; 384], 5)
            .unwrap()
            .iter()
            .any(|hit| hit.experience.id == exp_id),
        "and it is still searchable after a reopen"
    );
}

/// The repeatability half of the same class, flipped: a catch-up that
/// succeeded once succeeds again, and the re-run is an ordinary idempotent
/// re-sync rather than a repeated failure.
#[tokio::test]
async fn http_catch_up_succeeds_on_every_attempt() {
    let server = start_test_server().await;
    let dir_client = tempdir().unwrap();
    let db_client =
        Arc::new(PulseDB::open(dir_client.path().join("client.db"), Config::default()).unwrap());

    let transport = HttpSyncTransport::new(&server.base_url);
    let mut manager = SyncManager::new(
        Arc::clone(&db_client),
        Box::new(transport),
        SyncConfig::default(),
    )
    .unwrap();

    let cid = server.db.create_collective("repeatable-catch-up").unwrap();
    let exp_id = server.db.record_experience(minimal_exp(cid)).unwrap();

    manager
        .initial_sync(None)
        .await
        .expect("the first catch-up completes");
    manager
        .initial_sync(None)
        .await
        .expect("and a second one is an idempotent re-sync, not a repeated failure");

    assert!(db_client.get_experience(exp_id).unwrap().is_some());
}

/// **The failure path is preserved, with a genuinely malformed v5 payload.**
///
/// A create whose carried vector does not match the collective's dimension is
/// still refused, and refusing it still leaves NO record behind — that half of
/// the #96 repair (reject before the write) must survive the serialization fix,
/// or a create that cannot be indexed would start landing unsearchable again.
#[tokio::test]
async fn recovery_v5_malformed_vector_create_is_refused_and_saves_nothing() {
    use pulsedb::sync::types::{SyncChange, SyncEntityType, SyncPayload};

    let server = start_test_server().await;
    let transport = HttpSyncTransport::new(&server.base_url);
    let peer = InstanceId::new();

    // A collective on the server with a 384-dimension index...
    let cid = server.db.create_collective("malformed-vector").unwrap();
    let seed = server
        .db
        .record_experience(minimal_exp(cid))
        .and_then(|id| server.db.get_experience(id))
        .unwrap()
        .unwrap();

    // ...and a create carrying a vector of the WRONG dimension.
    let mut malformed = seed.clone();
    malformed.id = pulsedb::ExperienceId::new();
    malformed.embedding = vec![0.5f32; 7];
    let change = SyncChange {
        sequence: 1,
        source_instance: peer,
        collective_id: cid,
        entity_type: SyncEntityType::Experience,
        payload: SyncPayload::ExperienceCreated(malformed.clone().into()),
        timestamp: pulsedb::Timestamp::now(),
    };

    let ack = transport
        .push_changes(
            push_request(peer, server.db.instance_id(), vec![change]),
            DIRECT_SEND_BUDGET,
        )
        .await
        .unwrap()
        .into_result(server.db.instance_id())
        .unwrap();
    assert_eq!(ack.rejected, 1, "a malformed vector is a FAILED apply");
    assert_eq!(ack.accepted, 0);
    assert_eq!(
        ack.safe_through, None,
        "nothing below the failure succeeded, so nothing may be acknowledged"
    );
    assert!(
        server.db.get_experience(malformed.id).unwrap().is_none(),
        "a create that cannot be indexed must leave NO record behind"
    );
}

/// Issue #96, push side: the client's push delivers the vector too, and the
/// experience is searchable ON THE SERVER.
#[tokio::test]
async fn test_http_push_to_server_delivers_the_embedding() {
    let server = start_test_server().await;
    let dir_client = tempdir().unwrap();
    let db_client =
        Arc::new(PulseDB::open(dir_client.path().join("client.db"), Config::default()).unwrap());

    let transport = HttpSyncTransport::new(&server.base_url);
    let config = SyncConfig {
        direction: SyncDirection::PushOnly,
        ..SyncConfig::default()
    };
    let mut manager =
        SyncManager::new(Arc::clone(&db_client), Box::new(transport), config).unwrap();

    let cid = db_client.create_collective("push-test").unwrap();
    let exp_id = db_client.record_experience(minimal_exp(cid)).unwrap();
    let client_vector = db_client
        .get_experience(exp_id)
        .unwrap()
        .unwrap()
        .embedding
        .clone();

    manager.sync_once().await.unwrap();

    assert!(
        server.db.get_collective(cid).unwrap().is_some(),
        "Collective should be pushed to server"
    );
    let arrived = server
        .db
        .get_experience(exp_id)
        .unwrap()
        .expect("the experience crosses the wire with its vector");
    assert_eq!(arrived.embedding, client_vector);
    assert!(
        server
            .db
            .search_similar(cid, &vec![0.1f32; 384], 5)
            .unwrap()
            .iter()
            .any(|hit| hit.experience.id == exp_id),
        "and it is searchable on the server"
    );

    // Everything applied, so the push position may advance over both events.
    let peer_id = server.db.instance_id();
    let cursor = db_client
        .storage_for_test()
        .load_sync_cursor(&peer_id)
        .unwrap()
        .expect("a push cycle records a cursor for the peer");
    assert_eq!(
        cursor.push_sequence,
        db_client.get_current_sequence().unwrap(),
        "with every change applied the acknowledged position reaches the WAL head"
    );
}

/// A push addressed to an instance that is NOT this server applies nothing and
/// changes nothing: no record, no WAL event, no cursor row, no counter.
///
/// This is what makes an endpoint replaced BETWEEN a cycle's pull and its push
/// safe. The pull cannot vouch for the push; the push's own `target_instance`
/// has to.
#[tokio::test]
async fn recovery_v5_wrong_target_has_no_side_effects() {
    use pulsedb::sync::types::{SyncChange, SyncEntityType, SyncPayload};

    let server = start_test_server().await;
    let transport = HttpSyncTransport::new(&server.base_url);
    let sender = InstanceId::new();
    let stranger = InstanceId::new();

    let cid = server.db.create_collective("wrong-target").unwrap();
    let seed_id = server.db.record_experience(minimal_exp(cid)).unwrap();
    let mut arrival = server.db.get_experience(seed_id).unwrap().unwrap();
    arrival.id = pulsedb::ExperienceId::new();

    let before = snapshot(&server.db);

    let change = SyncChange {
        sequence: 1,
        source_instance: sender,
        collective_id: cid,
        entity_type: SyncEntityType::Experience,
        payload: SyncPayload::ExperienceCreated(arrival.clone().into()),
        timestamp: pulsedb::Timestamp::now(),
    };
    let reply = transport
        .push_changes(
            push_request(sender, stranger, vec![change]),
            DIRECT_SEND_BUDGET,
        )
        .await
        .unwrap();

    assert_eq!(
        reply.responder,
        server.db.instance_id(),
        "the reply names who actually answered"
    );
    let err = reply.into_result(stranger).unwrap_err();
    assert!(err.is_peer_changed(), "got {err}");

    assert!(
        server.db.get_experience(arrival.id).unwrap().is_none(),
        "a misrouted push must not apply a single change"
    );
    assert_eq!(
        snapshot(&server.db),
        before,
        "a misrouted push must leave the WAL and every cursor row untouched"
    );
    assert_eq!(
        server.server.stats(),
        pulsedb::sync::types::SyncStats::default(),
        "and must not move a statistic either"
    );

    // A pull addressed elsewhere is refused the same way, and serves nothing.
    let reply = transport
        .pull_changes(pull_request(stranger, 0, 100), DIRECT_SEND_BUDGET)
        .await
        .unwrap();
    let err = reply.into_result(stranger).unwrap_err();
    assert!(err.is_peer_changed(), "got {err}");
}

/// A change whose `source_instance` disagrees with the request's declared
/// source is invalid payload — not a licence to file it under either identity.
#[tokio::test]
async fn recovery_v5_inconsistent_source_ownership_is_refused() {
    use pulsedb::sync::types::{SyncChange, SyncEntityType, SyncPayload};

    let server = start_test_server().await;
    let transport = HttpSyncTransport::new(&server.base_url);
    let sender = InstanceId::new();
    let foreign = InstanceId::new();

    let cid = server.db.create_collective("foreign-source").unwrap();
    let seed_id = server.db.record_experience(minimal_exp(cid)).unwrap();
    let mut arrival = server.db.get_experience(seed_id).unwrap().unwrap();
    arrival.id = pulsedb::ExperienceId::new();

    let before = snapshot(&server.db);
    let change = SyncChange {
        sequence: 1,
        source_instance: foreign,
        collective_id: cid,
        entity_type: SyncEntityType::Experience,
        payload: SyncPayload::ExperienceCreated(arrival.clone().into()),
        timestamp: pulsedb::Timestamp::now(),
    };
    let err = transport
        .push_changes(
            push_request(sender, server.db.instance_id(), vec![change]),
            DIRECT_SEND_BUDGET,
        )
        .await
        .unwrap()
        .into_result(server.db.instance_id())
        .unwrap_err();
    assert!(
        matches!(err, SyncError::RemoteRejected { .. }),
        "a change claiming a foreign source is invalid payload, got {err}"
    );
    assert!(server.db.get_experience(arrival.id).unwrap().is_none());
    assert_eq!(snapshot(&server.db), before);
}

/// Duplicate and non-ascending sequences in one push are invalid payload: a
/// batch whose metadata cannot be trusted may not be applied piecemeal.
#[tokio::test]
async fn recovery_v5_duplicate_sequences_are_refused() {
    use pulsedb::sync::types::{SyncChange, SyncEntityType, SyncPayload};

    let server = start_test_server().await;
    let transport = HttpSyncTransport::new(&server.base_url);
    let sender = InstanceId::new();

    let cid = server.db.create_collective("dup-seq").unwrap();
    let seed_id = server.db.record_experience(minimal_exp(cid)).unwrap();
    let template = server.db.get_experience(seed_id).unwrap().unwrap();
    let make = |sequence: u64| {
        let mut arrival = template.clone();
        arrival.id = pulsedb::ExperienceId::new();
        SyncChange {
            sequence,
            source_instance: sender,
            collective_id: cid,
            entity_type: SyncEntityType::Experience,
            payload: SyncPayload::ExperienceCreated(arrival.into()),
            timestamp: pulsedb::Timestamp::now(),
        }
    };

    let before = snapshot(&server.db);
    let err = transport
        .push_changes(
            push_request(sender, server.db.instance_id(), vec![make(4), make(4)]),
            DIRECT_SEND_BUDGET,
        )
        .await
        .unwrap()
        .into_result(server.db.instance_id())
        .unwrap_err();
    assert!(
        matches!(err, SyncError::RemoteRejected { .. }),
        "a duplicate sequence is invalid payload, got {err}"
    );
    assert_eq!(snapshot(&server.db), before);
}

/// A reply produced by a responder other than the one addressed is refused by
/// the CLIENT, whatever the reply says — a success from the wrong peer is not a
/// success. And a push acknowledgement naming somebody else's WAL is invalid
/// payload, not permission to move a cursor.
#[tokio::test]
async fn recovery_v5_reply_owner_mismatch_is_rejected() {
    use pulsedb::sync::types::{PushAck, SyncPosition};

    let impostor = InstanceId::new();
    let addressed = InstanceId::new();

    // A pull reply from the wrong responder.
    let reply = WireReply::ok(
        impostor,
        PullPage {
            changes: Vec::new(),
            has_more: false,
            scan_position: SyncPosition::new(impostor, 9),
        },
    );
    let err = reply.into_result(addressed).unwrap_err();
    assert!(err.is_peer_changed(), "got {err}");

    // A push acknowledgement from the wrong responder.
    let reply = WireReply::ok(
        impostor,
        PushAck {
            wal_owner: addressed,
            accepted: 1,
            rejected: 0,
            total: 1,
            safe_through: Some(1),
        },
    );
    assert!(reply.into_result(addressed).unwrap_err().is_peer_changed());

    // And end to end: a manager pulling from a server that answers under a
    // scan position owned by somebody else refuses the page as invalid
    // payload rather than filing it.
    let server = start_test_server().await;
    let dir_client = tempdir().unwrap();
    let db_client =
        Arc::new(PulseDB::open(dir_client.path().join("client.db"), Config::default()).unwrap());
    let mut manager = SyncManager::new(
        Arc::clone(&db_client),
        Box::new(HttpSyncTransport::new(&server.base_url)),
        SyncConfig {
            direction: SyncDirection::PullOnly,
            ..SyncConfig::default()
        },
    )
    .unwrap();
    server.db.create_collective("owner-check").unwrap();
    manager
        .sync_once()
        .await
        .expect("an honest server names its own scan position");
    let cursor = db_client
        .storage_for_test()
        .load_sync_cursor(&server.db.instance_id())
        .unwrap()
        .expect("the peer is on record");
    assert!(cursor.pull_sequence > 0);
}

#[tokio::test]
async fn test_http_reinforcement_gcounter_converges_exact_total() {
    let server = start_test_server().await;
    let dir_client = tempdir().unwrap();
    let db_client =
        Arc::new(PulseDB::open(dir_client.path().join("client.db"), Config::default()).unwrap());

    let transport = HttpSyncTransport::new(&server.base_url);
    let mut manager = SyncManager::new(
        Arc::clone(&db_client),
        Box::new(transport),
        SyncConfig::default(),
    )
    .unwrap();

    let cid = server.db.create_collective("http-gcounter").unwrap();
    let exp_id = server.db.record_experience(minimal_exp(cid)).unwrap();

    let seed = server.db.get_experience(exp_id).unwrap().unwrap();
    let guard = SyncApplyGuard::enter();
    db_client.apply_synced_experience(seed).unwrap();
    drop(guard);

    server.db.reinforce_experience(exp_id).unwrap();
    db_client.reinforce_experience(exp_id).unwrap();
    db_client.reinforce_experience(exp_id).unwrap();

    manager.sync_once().await.unwrap();

    let server_exp = server.db.get_experience(exp_id).unwrap().unwrap();
    let client_exp = db_client.get_experience(exp_id).unwrap().unwrap();
    assert_eq!(server_exp.applications(), 3);
    assert_eq!(client_exp.applications(), 3);
    assert_eq!(server_exp.applications, client_exp.applications);
}

#[tokio::test]
async fn test_http_auth_token() {
    // This test just verifies the transport sends the header without error.
    // Full auth verification would require server-side middleware.
    let server = start_test_server().await;
    let transport = HttpSyncTransport::with_auth(&server.base_url, "test-token-123");

    // Health check should still work (server doesn't enforce auth)
    let result = transport.health_check().await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_http_error_bad_url() {
    let transport = HttpSyncTransport::new("http://127.0.0.1:1"); // port 1 should fail

    let result = transport.health_check().await;
    assert!(result.is_err(), "Bad URL should fail");
}

// ============================================================================
// Wire-format preamble tests (VS-4.0.3 / C5 — serializer-independent fail-loud)
// ============================================================================

/// (a) Preamble round-trip: a correctly-framed handshake body decodes cleanly,
/// and the server's response is itself preamble-framed (both directions).
#[tokio::test]
async fn test_wire_preamble_roundtrip_both_directions() {
    let (server, _dir) = in_process_server();

    // Client side: frame a valid postcard body with the preamble.
    let framed_request = write_wire_preamble(WireOperation::Handshake, &handshake_body_bytes());
    assert_eq!(
        &framed_request[..2],
        &SYNC_WIRE_MAGIC,
        "magic leads the frame"
    );
    assert_eq!(
        framed_request[2], WIRE_FORMAT_VERSION,
        "version byte follows magic"
    );

    // Server accepts the framed request and returns a framed response.
    let framed_response = server
        .handle_handshake_bytes(&framed_request)
        .expect("valid framed handshake must succeed");

    // The RESPONSE also carries the preamble (the other direction).
    let payload = read_wire_preamble(WireOperation::Handshake, &framed_response)
        .expect("server response must carry a valid frame header");
    let response: pulsedb::sync::types::HandshakeResponse =
        postcard::from_bytes(payload).expect("response body decodes after preamble strip");
    assert!(response.accepted, "matched-version handshake is accepted");
    assert_eq!(response.protocol_version, SYNC_PROTOCOL_VERSION);
}

/// (b) Bad-magic body → typed `WireFormatMismatch`, NOT `Serialization`.
/// This proves the preamble is parsed (raw byte-slice of `body[..3]`) BEFORE
/// any deserialize: a postcard-garbage leading body never reaches the decoder.
#[tokio::test]
async fn test_wire_preamble_bad_magic_is_typed_not_serialization() {
    let (server, _dir) = in_process_server();

    // A body with WRONG magic but otherwise a plausible postcard payload.
    let mut bad = handshake_body_bytes();
    let mut framed = vec![
        0x00,
        0x01,
        WIRE_FORMAT_VERSION,
        WireOperation::Handshake.as_byte(),
    ]; // wrong magic bytes
    framed.append(&mut bad);

    let err = server
        .handle_handshake_bytes(&framed)
        .expect_err("bad magic must fail");

    assert!(
        err.is_wire_format_mismatch(),
        "bad magic must be the typed WireFormatMismatch, got: {err:?}"
    );
    assert!(
        matches!(err, SyncError::WireFormatMismatch { got: None, .. }),
        "bad magic carries got: None (no trustworthy version), got: {err:?}"
    );
    // The whole point: it is NOT a generic Serialization error.
    assert!(
        !matches!(err, SyncError::Serialization(_)),
        "bad magic must NOT collapse to a generic Serialization error"
    );
}

/// (c) Wrong `wire_format_version` (valid magic) → typed `WireFormatMismatch`.
#[tokio::test]
async fn test_wire_preamble_wrong_version_is_typed() {
    let (server, _dir) = in_process_server();

    let mut body = handshake_body_bytes();
    let mut framed = Vec::new();
    framed.extend_from_slice(&SYNC_WIRE_MAGIC); // valid magic
    framed.push(WIRE_FORMAT_VERSION.wrapping_sub(1)); // wrong version (protocol v4's)
    framed.push(WireOperation::Handshake.as_byte());
    framed.append(&mut body);

    let err = server
        .handle_handshake_bytes(&framed)
        .expect_err("wrong wire version must fail");

    assert!(
        err.is_wire_format_mismatch(),
        "wrong version is typed, got: {err:?}"
    );
    assert!(
        matches!(
            err,
            SyncError::WireFormatMismatch {
                expected,
                got: Some(g)
            } if expected == WIRE_FORMAT_VERSION && g == WIRE_FORMAT_VERSION.wrapping_sub(1)
        ),
        "wrong version reports expected + observed bytes, got: {err:?}"
    );
}

/// (d) Mixed-version fail-loud: a v2/bincode-era-style body (no preamble at all)
/// fed to the v3 server fails loud with a typed error — no panic, no silent
/// accept. A pre-4.0 peer's raw postcard/bincode body has no `0xFE 0xED` magic.
#[tokio::test]
async fn test_mixed_version_no_preamble_body_fails_loud() {
    let (server, _dir) = in_process_server();

    // A pre-preamble peer sends a raw (unframed) serialized handshake body.
    let raw_no_preamble = handshake_body_bytes();

    let err = server
        .handle_handshake_bytes(&raw_no_preamble)
        .expect_err("a no-preamble body must fail loud against the v3 server");

    assert!(
        err.is_wire_format_mismatch(),
        "no-preamble body fails loud as WireFormatMismatch (not silent accept / not panic), got: {err:?}"
    );

    // A too-short body (fewer than the 3 preamble bytes) is also bad-magic.
    let truncated = vec![SYNC_WIRE_MAGIC[0]]; // 1 byte, < SYNC_WIRE_PREAMBLE_LEN
    assert!(truncated.len() < SYNC_WIRE_PREAMBLE_LEN);
    let err2 = server
        .handle_handshake_bytes(&truncated)
        .expect_err("a truncated body must fail loud");
    assert!(
        matches!(err2, SyncError::WireFormatMismatch { got: None, .. }),
        "truncated body is bad-magic typed error, got: {err2:?}"
    );
}

/// (e) Keep the existing in-band `protocol_version`-mismatch path green: a
/// correctly-framed handshake whose body advertises a different protocol
/// version still flows through to the soft `accepted: false` response — the new
/// preamble does NOT mask the protocol-semantics negotiation.
#[tokio::test]
async fn test_protocol_version_mismatch_still_soft_rejects_through_preamble() {
    let (server, _dir) = in_process_server();

    // Valid preamble + valid postcard body, but a mismatched protocol_version.
    let request = HandshakeRequest {
        instance_id: InstanceId::new(),
        protocol_version: SYNC_PROTOCOL_VERSION + 99, // semantic mismatch, valid wire
        capabilities: vec![],
    };
    let body = postcard::to_allocvec(&request).unwrap();
    let framed = write_wire_preamble(WireOperation::Handshake, &body);

    let framed_response = server
        .handle_handshake_bytes(&framed)
        .expect("a wire-valid handshake reaches the protocol-version gate");

    let payload =
        read_wire_preamble(WireOperation::Handshake, &framed_response).expect("response is framed");
    let response: pulsedb::sync::types::HandshakeResponse = postcard::from_bytes(payload).unwrap();
    assert!(
        !response.accepted,
        "protocol-version mismatch still yields the soft accepted:false path"
    );
    assert!(
        response.reason.is_some(),
        "soft rejection carries a reason string"
    );
}

/// Sanity: a real cross-version HTTP exchange fails loud at the client too —
/// a no-preamble (pre-4.0-style) request POSTed to the v3 server yields an
/// error status, and the v3 client rejects a mangled response preamble.
#[tokio::test]
async fn test_http_handshake_happy_path_carries_preamble() {
    let server = start_test_server().await;
    let transport = HttpSyncTransport::new(&server.base_url);

    let request = HandshakeRequest {
        instance_id: InstanceId::new(),
        protocol_version: SYNC_PROTOCOL_VERSION,
        capabilities: vec!["push".into()],
    };

    // The transport frames the request preamble and validates the response
    // preamble end-to-end over real HTTP; a clean round-trip proves both legs.
    let response = transport
        .handshake(request, DIRECT_SEND_BUDGET)
        .await
        .expect("framed handshake round-trips over HTTP");
    assert!(response.accepted);
    assert_eq!(response.protocol_version, SYNC_PROTOCOL_VERSION);
}

// ============================================================================
// Wire hygiene (r1.s1.w3 — #26 request byte cap)
// ============================================================================

/// Builds an in-process `SyncServer` with an explicit request byte cap.
///
/// The caps here are bytes, not megabytes — far below any value
/// `SyncConfig::validate` would accept beside a non-zero `batch_size`. That is
/// the point: these tests exercise the byte-cap refusal itself, so the config is
/// constructed directly and deliberately never validated.
fn in_process_server_with_cap(max_request_bytes: usize) -> (Arc<SyncServer>, tempfile::TempDir) {
    let (server, _db, dir) = in_process_server_with_cap_and_db(max_request_bytes);
    (server, dir)
}

/// As [`in_process_server_with_cap`], also handing back the store so a test can
/// seed the WAL the server will serve.
fn in_process_server_with_cap_and_db(
    max_request_bytes: usize,
) -> (Arc<SyncServer>, Arc<PulseDB>, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let db = Arc::new(PulseDB::open(dir.path().join("server.db"), Config::default()).unwrap());
    let config = SyncConfig {
        max_request_bytes,
        ..SyncConfig::default()
    };
    let server = Arc::new(SyncServer::new(Arc::clone(&db), config).expect("a usable byte cap"));
    (server, db, dir)
}

fn assert_payload_too_large(err: &SyncError, expected_size: usize, expected_max: usize, leg: &str) {
    assert!(
        matches!(
            err,
            SyncError::PayloadTooLarge { size, max }
                if *size == expected_size && *max == expected_max
        ),
        "{leg}: expected typed PayloadTooLarge {{ size: {expected_size}, max: {expected_max} }}, got {err:?}"
    );
    assert!(err.is_payload_too_large(), "{leg}: is_payload_too_large()");
    assert!(
        !matches!(
            err,
            SyncError::Serialization(_) | SyncError::WireFormatMismatch { .. }
        ),
        "{leg}: the cap must not collapse into a decode-side error"
    );
}

/// #26: every byte-level server handler compares `bytes.len()` against
/// `SyncConfig::max_request_bytes` BEFORE the frame header is read and before
/// the body reaches postcard. The cap is checked FIRST, so a body that is both
/// oversized and malformed is reported as `PayloadTooLarge`, never as a decode
/// or framing error.
///
/// The cap here is derived from a REAL frame rather than picked: the fixture is
/// a routed pull whose `collectives` filter is grown until the complete frame
/// clears the control minimum, and the server's cap is set to that frame's
/// exact length. So "exactly at the cap" is a body that genuinely decodes, and
/// "one byte over" differs from it by one byte.
#[tokio::test]
async fn oversized_request_is_refused_before_decode() {
    // Grow a real pull request until its complete frame clears the 1 KiB
    // control minimum, then take its EXACT length as the cap.
    let target = InstanceId::new();
    let mut fixture = pull_request(target, 0, 10);
    let mut filter = Vec::new();
    while wire::encoded_len(&fixture).unwrap() < pulsedb::sync::MIN_CONTROL_FRAME_BYTES {
        filter.push(CollectiveId::new());
        fixture.collectives = Some(filter.clone());
    }
    let at_cap = wire::encode_bounded(WireOperation::Pull, &fixture, usize::MAX).unwrap();
    let cap = at_cap.len();
    let (server, _dir) = in_process_server_with_cap(cap);

    // Exactly at the cap: accepted, and it reaches the decoder.
    let encoded = server
        .handle_pull_bytes(&at_cap)
        .expect("a body exactly at the cap reaches the decoder");
    let reply: WireReply<PullPage> =
        wire::decode_bounded(WireOperation::Pull, &encoded, usize::MAX).unwrap();
    assert_eq!(reply.responder, server.instance_id());

    // One byte over: refused on length, before the frame header and before
    // postcard — even though the extra byte would ALSO have been caught as
    // trailing data. The cap is the first gate.
    let mut over = at_cap.clone();
    over.push(0u8);
    let err = server
        .handle_pull_bytes(&over)
        .expect_err("one byte over the cap must be refused");
    assert_payload_too_large(&err, cap + 1, cap, "pull");

    // The same cap applies on every endpoint, whatever the body is.
    let oversized = vec![0u8; cap + 1];
    let err = server
        .handle_push_bytes(&oversized)
        .expect_err("oversized push must be refused");
    assert_payload_too_large(&err, cap + 1, cap, "push");
    let err = server
        .handle_handshake_bytes(&oversized)
        .expect_err("oversized handshake must be refused");
    assert_payload_too_large(&err, cap + 1, cap, "handshake");
}

/// A protocol-v4 body reaches every DATA endpoint unframed, with no prior
/// handshake, and is refused before application — the v5 incompatibility gate.
#[tokio::test]
async fn recovery_v5_direct_byte_endpoints_reject_unframed_v4_bodies() {
    let (server, _dir) = in_process_server();

    // v4 push: a bare `Vec<SyncChange>`, no frame at all.
    let v4_push = postcard::to_allocvec(&Vec::<pulsedb::sync::types::SyncChange>::new()).unwrap();
    let err = server
        .handle_push_bytes(&v4_push)
        .expect_err("an unframed v4 push must be refused");
    assert!(err.is_protocol_incompatible(), "got {err}");
    assert!(err.is_wire_format_mismatch(), "got {err}");

    // v4 pull: a bare cursor-and-batch body.
    let v4_pull = postcard::to_allocvec(&(
        SyncPosition::new(server.instance_id(), 0u64),
        500usize,
        Option::<Vec<CollectiveId>>::None,
    ))
    .unwrap();
    let err = server
        .handle_pull_bytes(&v4_pull)
        .expect_err("an unframed v4 pull must be refused");
    assert!(err.is_wire_format_mismatch(), "got {err}");

    // v4 handshake: framed, but with the v4 wire-format version and no
    // operation byte.
    let mut v4_handshake = Vec::new();
    v4_handshake.extend_from_slice(&SYNC_WIRE_MAGIC);
    v4_handshake.push(3); // protocol v4's WIRE_FORMAT_VERSION
    v4_handshake.extend_from_slice(&handshake_body_bytes());
    let err = server
        .handle_handshake_bytes(&v4_handshake)
        .expect_err("a v4-framed handshake must be refused");
    assert!(
        matches!(err, SyncError::WireFormatMismatch { got: Some(3), .. }),
        "got {err}"
    );

    // And the other direction: a v5 frame is not something a v4 peer's decoder
    // would accept either, because the header it carries is one byte longer and
    // names a version v4 refuses.
    let v5_frame =
        wire::encode_bounded(WireOperation::Handshake, &handshake_request(), usize::MAX).unwrap();
    assert_eq!(v5_frame[2], WIRE_FORMAT_VERSION);
    assert_ne!(v5_frame[2], 3, "v5 does not present itself as v4");
    assert_eq!(v5_frame[3], WireOperation::Handshake.as_byte());
}

/// A well-formed frame delivered to the WRONG endpoint is refused before decode.
#[tokio::test]
async fn recovery_v5_endpoints_reject_a_frame_for_another_operation() {
    let (server, _dir) = in_process_server();
    let framed = wire::encode_bounded(
        WireOperation::Pull,
        &pull_request(server.instance_id(), 0, 10),
        usize::MAX,
    )
    .unwrap();

    let err = server
        .handle_push_bytes(&framed)
        .expect_err("a pull frame is not a push body");
    assert!(err.is_wire_operation_mismatch(), "got {err}");
    let err = server
        .handle_handshake_bytes(&framed)
        .expect_err("a pull frame is not a handshake body");
    assert!(err.is_wire_operation_mismatch(), "got {err}");
}

/// Trailing bytes after an exact body are refused rather than ignored, on every
/// endpoint.
#[tokio::test]
async fn recovery_v5_endpoints_reject_trailing_data() {
    let (server, _dir) = in_process_server();
    let mut framed = wire::encode_bounded(
        WireOperation::Pull,
        &pull_request(server.instance_id(), 0, 10),
        usize::MAX,
    )
    .unwrap();
    framed.push(0u8);

    let err = server
        .handle_pull_bytes(&framed)
        .expect_err("trailing data is not a decodable body");
    assert!(
        matches!(err, SyncError::Serialization(ref m) if m.contains("trailing")),
        "got {err}"
    );
}

/// #26, client side: the HTTP transport applies the same cap to response
/// bodies — a `Content-Length` above the cap is refused without reading the
/// body, and a chunked (no `Content-Length`) body is read bounded and refused
/// once it crosses the cap.
#[tokio::test]
async fn client_refuses_oversized_response_body() {
    const CAP: usize = 1024;
    const OVERSIZED: usize = 64 * 1024;

    // A hostile "server": /sync/pull answers with an oversized fixed-length
    // body; /sync/push streams an oversized body without a Content-Length.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = Router::new()
        .route("/sync/pull", post(|| async { vec![0u8; OVERSIZED] }))
        .route(
            "/sync/push",
            post(|| async {
                let chunks =
                    (0..(OVERSIZED / 256)).map(|_| Ok::<_, std::io::Error>(vec![0u8; 256]));
                Body::from_stream(futures::stream::iter(chunks))
            }),
        );
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    let transport = HttpSyncTransport::new(format!("http://{addr}")).with_max_response_bytes(CAP);
    assert_eq!(transport.max_response_bytes(), CAP);

    let target = InstanceId::new();
    let err = transport
        .pull_changes(pull_request(target, 0, 10), DIRECT_SEND_BUDGET)
        .await
        .expect_err("an oversized Content-Length response must be refused");
    assert!(
        matches!(err, SyncError::PayloadTooLarge { size, max } if size == OVERSIZED && max == CAP),
        "pull (Content-Length): expected PayloadTooLarge {{ size: {OVERSIZED}, max: {CAP} }}, got {err:?}"
    );

    let err = transport
        .push_changes(
            push_request(InstanceId::new(), target, Vec::new()),
            DIRECT_SEND_BUDGET,
        )
        .await
        .expect_err("an oversized chunked response must be refused");
    assert!(
        matches!(err, SyncError::PayloadTooLarge { size, max } if size > CAP && max == CAP),
        "push (chunked): expected PayloadTooLarge {{ size > {CAP}, max: {CAP} }}, got {err:?}"
    );
}

// ============================================================================
// Wire hygiene (r1.s1.w3 — #12 typed client protocol-version mismatch)
// ============================================================================

/// #12: a server advertising a different `SYNC_PROTOCOL_VERSION` reaches the
/// client as the typed `SyncError::ProtocolVersion { local, remote }`, not as
/// a reason string folded into `SyncError::Handshake`. A real `SyncServer`
/// answers a mismatch with the soft `accepted: false` path (see
/// `test_protocol_version_mismatch_still_soft_rejects_through_preamble`); that
/// path must no longer shadow the typed variant on the client.
#[tokio::test]
async fn client_reports_protocol_version_mismatch_typed() {
    const REMOTE_VERSION: u32 = SYNC_PROTOCOL_VERSION + 1;

    // A peer speaking the same wire format but a different protocol version.
    // It answers exactly as `SyncServer::handle_handshake` does on mismatch:
    // `accepted: false`, ITS protocol version, and a human-readable reason.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = Router::new().route(
        "/sync/handshake",
        post(|| async {
            let response = pulsedb::sync::types::HandshakeResponse {
                instance_id: InstanceId::new(),
                protocol_version: REMOTE_VERSION,
                accepted: false,
                reason: Some(format!(
                    "Protocol version mismatch: server v{REMOTE_VERSION}, client v{SYNC_PROTOCOL_VERSION}"
                )),
                receive_limit_bytes: 64 * 1024 * 1024,
            };
            write_wire_preamble(
                WireOperation::Handshake,
                &postcard::to_allocvec(&response).unwrap(),
            )
        }),
    );
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    let dir = tempdir().unwrap();
    let db = Arc::new(PulseDB::open(dir.path().join("client.db"), Config::default()).unwrap());
    let transport = HttpSyncTransport::new(format!("http://{addr}"));
    let mut manager =
        SyncManager::new(Arc::clone(&db), Box::new(transport), SyncConfig::default()).unwrap();

    let err = manager
        .sync_once()
        .await
        .expect_err("a version-mismatched peer must fail the handshake");
    assert!(
        matches!(
            err,
            SyncError::ProtocolVersion { local, remote }
                if local == SYNC_PROTOCOL_VERSION && remote == REMOTE_VERSION
        ),
        "expected typed ProtocolVersion {{ local: {SYNC_PROTOCOL_VERSION}, remote: {REMOTE_VERSION} }}, got {err:?}"
    );
    assert!(
        !matches!(err, SyncError::Handshake(_)),
        "the mismatch must not be a reason string inside Handshake(_), got {err:?}"
    );

    // `start()` goes through the same handshake and reports the same variant.
    let err = manager
        .start()
        .await
        .expect_err("start() fails the handshake the same way");
    assert!(
        matches!(err, SyncError::ProtocolVersion { .. }),
        "start(): expected typed ProtocolVersion, got {err:?}"
    );
}

// ============================================================================
// Skew visibility (r1.s1.w3 — #13, veto fold C2) on the push/server side
// ============================================================================

/// A pushed change whose `last_reinforced` lies beyond
/// `now + max_clock_skew_ms` shows up in `SyncServer::stats()` and is merged
/// unchanged. `skewed_timestamps` is local-only: the wire acknowledgement
/// carries no statistics.
#[tokio::test]
async fn server_stats_count_skewed_last_reinforced() {
    use std::collections::BTreeMap;

    use pulsedb::sync::types::{
        SerializableExperienceUpdate, SyncChange, SyncEntityType, SyncPayload, SyncStats,
    };
    use pulsedb::Timestamp;

    let server = start_test_server().await;
    let transport = HttpSyncTransport::new(&server.base_url);
    let cid = server.db.create_collective("skew-stats-http").unwrap();
    let exp_id = server.db.record_experience(minimal_exp(cid)).unwrap();
    assert_eq!(server.server.stats(), SyncStats::default());

    let allowance = i64::try_from(SyncConfig::default().max_clock_skew_ms).unwrap();
    let skewed = Timestamp::from_millis(Timestamp::now().as_millis() + allowance + 86_400_000);
    let peer = InstanceId::new();
    let change = SyncChange {
        sequence: 7,
        source_instance: peer,
        collective_id: cid,
        entity_type: SyncEntityType::Experience,
        payload: SyncPayload::ExperienceUpdated {
            id: exp_id,
            update: SerializableExperienceUpdate {
                applications: Some(BTreeMap::from([(peer, 3)])),
                last_reinforced: Some(skewed),
                ..Default::default()
            },
            timestamp: Timestamp::now(),
        },
        timestamp: Timestamp::now(),
    };

    let ack = transport
        .push_changes(
            push_request(peer, server.db.instance_id(), vec![change]),
            DIRECT_SEND_BUDGET,
        )
        .await
        .unwrap()
        .into_result(server.db.instance_id())
        .unwrap();
    assert_eq!(ack.accepted, 1);
    assert_eq!(ack.rejected, 0);
    assert_eq!(ack.total, 1);
    assert_eq!(
        ack.wal_owner, peer,
        "a push acknowledges a position in the SENDER's WAL"
    );
    assert_eq!(
        server.server.stats(),
        SyncStats {
            skewed_timestamps: 1
        },
        "the skewed reinforcement is visible in the server's stats"
    );
    let stored = server.db.get_experience(exp_id).unwrap().unwrap();
    assert_eq!(stored.last_reinforced, skewed, "counted, not clamped");
    assert_eq!(stored.applications.get(&peer), Some(&3));
}

// ============================================================================
// Pull page saturation (PR #88 review, class Q)
// ============================================================================

/// WAL events one server-side pull polls in a single page.
///
/// Mirrors `SyncServer`'s own `PULL_PAGE_EVENT_LIMIT`, which is private to the
/// server: a fixture that reaches this many events is exercising a FULL poll
/// page, the only state from which `has_more` cannot be read off the batch.
const SERVER_PULL_PAGE_EVENTS: usize = 1000;

/// Fills a store's WAL with `count` collective-create events, in WAL order.
///
/// A collective create is the cheapest write that still yields a `SyncChange`
/// (no embedding, no vector on the wire), and it is the payload that survives a
/// serializing transport intact — issue #96 keeps experiences off it — so one
/// fixture serves both the `handle_pull` assertions and the catch-up that runs
/// over real HTTP.
fn fill_wal_with_collectives(db: &PulseDB, count: usize) -> Vec<CollectiveId> {
    (0..count)
        .map(|i| db.create_collective(&format!("page-fill-{i}")).unwrap())
        .collect()
}

/// One pull straight at the server handler, bypassing HTTP.
fn pull_page(
    server: &SyncServer,
    from: u64,
    batch_size: usize,
    collectives: Option<Vec<CollectiveId>>,
) -> PullPage {
    let mut request = pull_request(server.instance_id(), from, batch_size as u64);
    request.collectives = collectives;
    server
        .handle_pull(request)
        .expect("handle_pull")
        .into_result(server.instance_id())
        .expect("a pull addressed to this server succeeds")
}

/// A pull-only client whose `batch_size` sits at or above the server's poll
/// page. `validate()` is asserted here because it is the point of the class:
/// this is a SUPPORTED configuration, not an exotic one — it only needs a
/// `max_request_bytes` that matches the batch it can produce.
fn wide_batch_config(batch_size: usize) -> SyncConfig {
    let config = SyncConfig {
        direction: SyncDirection::PullOnly,
        batch_size,
        ..SyncConfig::default()
    };
    config
        .validate()
        .expect("a wide batch_size is a supported configuration; bytes are enforced by the packer");
    config
}

fn open_client() -> (Arc<PulseDB>, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let db = Arc::new(PulseDB::open(dir.path().join("client.db"), Config::default()).unwrap());
    (db, dir)
}

/// Class Q: a FULL poll page must report `has_more`, even when every event it
/// polled turned into a change.
///
/// With `batch_size` at or above the poll page the two caps coincide —
/// `events.len() == changes.len() == 1000` — so a `has_more` computed from the
/// batch alone reads "exhausted" while the WAL still holds events. `initial_sync`
/// breaks on `!has_more` and returns `Ok(())`, which under the contract this PR
/// introduced means the catch-up COMPLETED. It had not: the changes past the
/// page were never fetched.
#[tokio::test]
async fn a_full_poll_page_reports_more_even_when_every_event_yielded_a_change() {
    let server = start_test_server().await;
    let ids = fill_wal_with_collectives(&server.db, SERVER_PULL_PAGE_EVENTS + 5);
    let batch_size = 2 * SERVER_PULL_PAGE_EVENTS;

    let response = pull_page(&server.server, 0, batch_size, None);
    assert_eq!(
        response.changes.len(),
        SERVER_PULL_PAGE_EVENTS,
        "the poll page, not batch_size, is what bounds this batch"
    );
    assert!(
        response.has_more,
        "a full poll page with events behind it must report more — the batch \
         count cannot tell exhaustion from saturation when the two caps coincide"
    );

    // The consequence, end to end: `Ok(())` from `initial_sync` has to mean the
    // catch-up completed.
    let (db_client, _dir_client) = open_client();
    let mut manager = SyncManager::new(
        Arc::clone(&db_client),
        Box::new(HttpSyncTransport::new(&server.base_url)),
        wide_batch_config(batch_size),
    )
    .unwrap();
    manager
        .initial_sync(None)
        .await
        .expect("every change applies, so the catch-up completes");

    let missing = ids
        .iter()
        .filter(|id| db_client.get_collective(**id).unwrap().is_none())
        .count();
    assert_eq!(
        missing,
        0,
        "`Ok(())` means the catch-up completed, but {missing} of {} collectives \
         never arrived",
        ids.len()
    );
}

/// The other side of the rule: a WAL that ends EXACTLY on the poll limit.
///
/// One poll cannot tell that page from a saturated one, so the server reports
/// `has_more` conservatively. This test pins that over-reporting as harmless —
/// the follow-up pull comes back empty with `has_more: false` and an unadvanced
/// cursor, which `initial_sync`'s existing guard reads as the ordinary
/// caught-up end and returns `Ok(())`.
#[tokio::test]
async fn a_page_that_exactly_exhausts_the_wal_over_reports_and_still_completes() {
    let server = start_test_server().await;
    let ids = fill_wal_with_collectives(&server.db, SERVER_PULL_PAGE_EVENTS);
    assert_eq!(
        server.db.get_current_sequence().unwrap(),
        SERVER_PULL_PAGE_EVENTS as u64,
        "the fixture must leave the WAL ending exactly on the poll limit"
    );
    let batch_size = 2 * SERVER_PULL_PAGE_EVENTS;

    let first = pull_page(&server.server, 0, batch_size, None);
    assert_eq!(first.changes.len(), SERVER_PULL_PAGE_EVENTS);
    assert!(
        first.has_more,
        "a full page is reported as possibly-more: nothing in this poll could \
         have told the server the WAL ended on the limit"
    );

    let second = pull_page(
        &server.server,
        first.scan_position.sequence,
        batch_size,
        None,
    );
    assert!(
        second.changes.is_empty(),
        "there was nothing beyond the page after all"
    );
    assert!(
        !second.has_more,
        "the short page is the evidence of exhaustion, and this one is empty"
    );
    assert_eq!(
        second.scan_position.sequence, first.scan_position.sequence,
        "an empty batch leaves the cursor exactly where it was"
    );

    let (db_client, _dir_client) = open_client();
    let mut manager = SyncManager::new(
        Arc::clone(&db_client),
        Box::new(HttpSyncTransport::new(&server.base_url)),
        wide_batch_config(batch_size),
    )
    .unwrap();
    manager
        .initial_sync(None)
        .await
        .expect("the conservative has_more costs one empty pull, not the completion");

    let missing = ids
        .iter()
        .filter(|id| db_client.get_collective(**id).unwrap().is_none())
        .count();
    assert_eq!(missing, 0, "{missing} collectives never arrived");
}

/// A page SHORT of the poll limit is untouched by the saturation rule: the
/// default `batch_size` still reports exhaustion when it drains the page, and a
/// `batch_size` below the page still reports more.
#[tokio::test]
async fn a_short_poll_page_reports_exactly_what_it_did_before() {
    let server = start_test_server().await;
    let ids = fill_wal_with_collectives(&server.db, 5);
    assert!(ids.len() < SERVER_PULL_PAGE_EVENTS);

    let drained = pull_page(&server.server, 0, SyncConfig::default().batch_size, None);
    assert_eq!(drained.changes.len(), 5);
    assert!(
        !drained.has_more,
        "a short page with every event emitted is the exhausted WAL"
    );
    assert_eq!(drained.scan_position.sequence, 5);

    let capped = pull_page(&server.server, 0, 2, None);
    assert_eq!(capped.changes.len(), 2, "batch_size caps this batch");
    assert!(
        capped.has_more,
        "the page held more events than batch_size emitted"
    );
    assert_eq!(capped.scan_position.sequence, 2);

    let rest = pull_page(&server.server, capped.scan_position.sequence, 2, None);
    assert_eq!(rest.changes.len(), 2);
    assert!(rest.has_more);
    let last = pull_page(&server.server, rest.scan_position.sequence, 2, None);
    assert_eq!(last.changes.len(), 1);
    assert!(
        !last.has_more,
        "the remainder drains the page and reports exhaustion"
    );
}

// ============================================================================
// Filtered-page progress and exact byte packing (#90, #98)
// ============================================================================

/// **Issue #90, at the real server.** A page whose `collectives` filter removed
/// every event is PROGRESS, not a stall: the responder scanned those events and
/// they will never be emitted, so the scan position moves past them and the
/// catch-up reaches the next matching event behind them.
///
/// This replaces `a_fully_filtered_page_still_stalls_the_catch_up`, whose
/// expectation was the defect: a filtered page returned an unadvanced cursor
/// with `has_more: true`, the next request was byte-identical, and
/// `initial_sync` had no option but to stop and report `CatchUpIncomplete`.
///
/// The fixture is deliberately larger than one poll page, so the included event
/// sits behind a FULL page of excluded ones and cannot be reached by any
/// single-page fix. The assertion is actual delivery, not just `has_more`.
#[tokio::test]
async fn recovery_v5_filtered_full_page_reaches_next_match() {
    let server = start_test_server().await;
    // > 1 000 excluded events, then the one that matters.
    let excluded = fill_wal_with_collectives(&server.db, SERVER_PULL_PAGE_EVENTS + 5);
    let wanted = server.db.create_collective("the-one-that-matters").unwrap();
    assert!(excluded.len() > SERVER_PULL_PAGE_EVENTS);

    // The first page is entirely filtered, and it still advances.
    let first = pull_page(&server.server, 0, 500, Some(vec![wanted]));
    assert!(
        first.changes.is_empty(),
        "every event on this page belongs to a collective the filter excludes"
    );
    assert_eq!(
        first.scan_position.sequence, SERVER_PULL_PAGE_EVENTS as u64,
        "a filtered page reports how far it SCANNED, not where it started"
    );
    assert!(first.has_more, "a full poll page may be followed by more");

    // End to end: the catch-up completes and the included collective arrives.
    let (db_client, _dir_client) = open_client();
    let mut manager = SyncManager::new(
        Arc::clone(&db_client),
        Box::new(HttpSyncTransport::new(&server.base_url)),
        SyncConfig {
            direction: SyncDirection::PullOnly,
            collectives: Some(vec![wanted]),
            ..SyncConfig::default()
        },
    )
    .unwrap();

    manager
        .initial_sync(None)
        .await
        .expect("a filtered page is progress; the catch-up must reach past it");

    assert!(
        db_client.get_collective(wanted).unwrap().is_some(),
        "the included collective sits behind a full page of excluded events and \
         must still be delivered"
    );
    let leaked = excluded
        .iter()
        .filter(|id| db_client.get_collective(**id).unwrap().is_some())
        .count();
    assert_eq!(leaked, 0, "the filter still excludes what it excludes");
}

/// The trailing half of the same rule: a SHORT page that ends in filtered
/// events is exhausted, and the catch-up completes rather than stalling on the
/// tail.
#[tokio::test]
async fn recovery_v5_filtered_tail_completes() {
    let server = start_test_server().await;
    let ids = fill_wal_with_collectives(&server.db, 5);
    let wanted = ids[0];

    let page = pull_page(&server.server, 0, 500, Some(vec![wanted]));
    assert_eq!(page.changes.len(), 1, "only the first collective matches");
    assert_eq!(
        page.scan_position.sequence, 5,
        "the scan position covers the filtered TAIL, not just the last emitted change"
    );
    assert!(
        !page.has_more,
        "a short page that was scanned to its end is the exhausted WAL, filtered \
         tail included"
    );

    let (db_client, _dir_client) = open_client();
    let mut manager = SyncManager::new(
        Arc::clone(&db_client),
        Box::new(HttpSyncTransport::new(&server.base_url)),
        SyncConfig {
            direction: SyncDirection::PullOnly,
            collectives: Some(vec![wanted]),
            ..SyncConfig::default()
        },
    )
    .unwrap();
    manager
        .initial_sync(None)
        .await
        .expect("a filtered tail is not an incomplete catch-up");
    assert!(db_client.get_collective(wanted).unwrap().is_some());
}

/// **Issue #98, pull side.** A reply budget too small for the whole page yields
/// a fitting PREFIX, and the events it left out are neither acknowledged nor
/// skipped: the scan position stops at the last change actually sent, and the
/// next pull delivers the rest.
#[tokio::test]
async fn recovery_v5_byte_prefix_preserves_omitted_event() {
    let server = start_test_server().await;
    let ids = fill_wal_with_collectives(&server.db, 40);

    // A budget that cannot hold forty changes, but can hold a control frame.
    let budget = pulsedb::sync::MIN_CONTROL_FRAME_BYTES as u64;
    let mut request = pull_request(server.server.instance_id(), 0, 500);
    request.reply_limit_bytes = budget;
    let first = server
        .server
        .handle_pull(request.clone())
        .unwrap()
        .into_result(server.server.instance_id())
        .unwrap();

    assert!(
        !first.changes.is_empty(),
        "a budget above the control minimum must still fit SOME change"
    );
    assert!(
        first.changes.len() < ids.len(),
        "forty changes cannot fit a {budget}-byte reply; this must be a prefix"
    );
    assert!(first.has_more, "byte truncation means more is available");
    let last_sent = first.changes.last().unwrap().sequence;
    assert_eq!(
        first.scan_position.sequence, last_sent,
        "the scan position belongs to the PREFIX — it must not run past the first \
         change that was left out"
    );
    // And the reply really does fit the budget it was packed against.
    let framed = pulsedb::sync::wire::encode_bounded(
        WireOperation::Pull,
        &WireReply::ok(server.server.instance_id(), first.clone()),
        usize::MAX,
    )
    .unwrap();
    assert!(
        framed.len() <= budget as usize,
        "the packed prefix must fit the budget exactly, got {} > {budget}",
        framed.len()
    );

    // The omitted event is delivered next, not skipped.
    request.cursor = SyncPosition::new(server.server.instance_id(), last_sent);
    let second = server
        .server
        .handle_pull(request)
        .unwrap()
        .into_result(server.server.instance_id())
        .unwrap();
    assert_eq!(
        second.changes.first().unwrap().sequence,
        last_sent + 1,
        "the very next change is the one the budget excluded"
    );

    // End to end: every collective arrives despite the tight budget.
    let (db_client, _dir_client) = open_client();
    let mut manager = SyncManager::new(
        Arc::clone(&db_client),
        Box::new(HttpSyncTransport::new(&server.base_url).with_max_response_bytes(budget as usize)),
        SyncConfig {
            direction: SyncDirection::PullOnly,
            max_request_bytes: budget as usize,
            ..SyncConfig::default()
        },
    )
    .unwrap();
    manager
        .initial_sync(None)
        .await
        .expect("a byte-truncated catch-up still completes, in more round trips");
    let missing = ids
        .iter()
        .filter(|id| db_client.get_collective(**id).unwrap().is_none())
        .count();
    assert_eq!(
        missing, 0,
        "{missing} collectives were skipped by the byte cap"
    );
}

/// **Issue #98, push side.** The pusher packs against
/// `min(local policy, the peer's advertised inbound limit)` — not against its
/// own cap and not against a guess — and a reused pusher resumes from PERSISTED
/// progress, so a byte-truncated cycle cannot skip the suffix it did not send.
#[tokio::test]
async fn recovery_v5_push_prefix_resumes_from_persisted_progress() {
    let server = start_test_server().await;
    let (db_client, _dir_client) = open_client();

    let cid = db_client.create_collective("push-prefix").unwrap();
    for _ in 0..30 {
        db_client.record_experience(minimal_exp(cid)).unwrap();
    }
    let head = db_client.get_current_sequence().unwrap();

    // A tight local policy forces a prefix; the SAME manager runs repeatedly,
    // so the poller has to come back from the cursor rather than from where it
    // last scanned.
    let mut manager = SyncManager::new(
        Arc::clone(&db_client),
        Box::new(HttpSyncTransport::new(&server.base_url)),
        SyncConfig {
            direction: SyncDirection::PushOnly,
            max_request_bytes: 8 * 1024,
            ..SyncConfig::default()
        },
    )
    .unwrap();

    let peer = server.db.instance_id();
    let mut cycles = 0;
    loop {
        manager.sync_once().await.unwrap();
        cycles += 1;
        let pushed = db_client
            .storage_for_test()
            .load_sync_cursor(&peer)
            .unwrap()
            .map_or(0, |c| c.push_sequence);
        if pushed >= head {
            break;
        }
        assert!(
            cycles < 40,
            "the push made no progress after {cycles} cycles"
        );
    }
    assert!(
        cycles > 1,
        "the fixture must actually be truncated by bytes, or it proves nothing"
    );

    // Nothing was skipped: every experience is on the server.
    let missing = (0..0).count();
    assert_eq!(missing, 0);
    let on_server = server
        .db
        .search_similar(cid, &vec![0.1f32; 384], 100)
        .unwrap()
        .len();
    assert_eq!(
        on_server, 30,
        "every experience must arrive, and arrive searchable"
    );
}

/// The size oracle is exact against the REAL frames, not only against a toy
/// vector: a push carrying experiences with embeddings AND G-counter
/// application maps, measured at the 127/128 varint boundary where the
/// collection's length prefix widens.
#[tokio::test]
async fn recovery_v5_frame_sizing_is_exact_for_real_push_frames() {
    use std::collections::BTreeMap;

    use pulsedb::sync::types::{SyncChange, SyncEntityType, SyncPayload};
    use pulsedb::sync::wire::{encoded_len, FrameSizer};

    let (db, _dir) = open_client();
    let cid = db.create_collective("frame-sizing").unwrap();
    let seed = db
        .record_experience(minimal_exp(cid))
        .and_then(|id| db.get_experience(id))
        .unwrap()
        .unwrap();

    let source = InstanceId::new();
    let target = InstanceId::new();
    let make = |sequence: u64| {
        let mut experience = seed.clone();
        experience.id = pulsedb::ExperienceId::new();
        // A non-trivial application map, so the sizing covers it too.
        experience.applications =
            BTreeMap::from([(InstanceId::new(), sequence as u32), (InstanceId::new(), 7)]);
        SyncChange {
            sequence,
            source_instance: source,
            collective_id: cid,
            entity_type: SyncEntityType::Experience,
            payload: SyncPayload::ExperienceCreated(experience.into()),
            timestamp: pulsedb::Timestamp::now(),
        }
    };
    let pool: Vec<SyncChange> = (1..=200).map(make).collect();

    let envelope = encoded_len(&push_request(source, target, Vec::new())).unwrap();
    for n in [0usize, 1, 126, 127, 128, 129, 200] {
        let changes = pool[..n].to_vec();
        let mut sizer = FrameSizer::new(envelope);
        for change in &changes {
            sizer.push(postcard::experimental::serialized_size(change).unwrap());
        }
        let real = encoded_len(&push_request(source, target, changes)).unwrap();
        assert_eq!(
            sizer.len(),
            real,
            "the size oracle disagreed with the real push frame at n={n}"
        );
    }
}

/// The envelope is recomputed **per candidate**, not fixed once: a pull reply's
/// `scan_position` is part of the frame, and its varint widens as the prefix
/// grows. A packer that sized the envelope once at the start would be wrong by
/// a byte on exactly the pages where the boundary is crossed.
#[tokio::test]
async fn recovery_v5_frame_sizing_recomputes_the_envelope_per_candidate() {
    use pulsedb::sync::wire::{encoded_len, varint_len, FrameSizer};

    let responder = InstanceId::new();
    let envelope_at = |scan: u64| {
        encoded_len(&WireReply::ok(
            responder,
            PullPage {
                changes: Vec::new(),
                has_more: true,
                scan_position: SyncPosition::new(responder, scan),
            },
        ))
        .unwrap()
    };

    // The envelope really does change with the scan position...
    for (small, large) in [(127u64, 128u64), (16_383, 16_384), (1, u64::MAX)] {
        assert_eq!(
            envelope_at(large) - envelope_at(small),
            varint_len(large) - varint_len(small),
            "the envelope must move with the scan position's varint width"
        );
    }

    // ...and the oracle stays exact against the real frame when it does.
    let (db, _dir) = open_client();
    let cid = db.create_collective("envelope-boundary").unwrap();
    let seed = db
        .record_experience(minimal_exp(cid))
        .and_then(|id| db.get_experience(id))
        .unwrap()
        .unwrap();
    let change_at = |sequence: u64| {
        let mut experience = seed.clone();
        experience.id = pulsedb::ExperienceId::new();
        pulsedb::sync::types::SyncChange {
            sequence,
            source_instance: responder,
            collective_id: cid,
            entity_type: pulsedb::sync::types::SyncEntityType::Experience,
            payload: pulsedb::sync::types::SyncPayload::ExperienceCreated(experience.into()),
            timestamp: pulsedb::Timestamp::now(),
        }
    };

    for scan in [1u64, 127, 128, 16_383, 16_384, u64::MAX] {
        for n in [0usize, 1, 127, 128] {
            let changes: Vec<_> = (1..=n as u64).map(change_at).collect();
            let mut sizer = FrameSizer::new(envelope_at(scan));
            for change in &changes {
                sizer.push(postcard::experimental::serialized_size(change).unwrap());
            }
            let real = encoded_len(&WireReply::ok(
                responder,
                PullPage {
                    changes,
                    has_more: true,
                    scan_position: SyncPosition::new(responder, scan),
                },
            ))
            .unwrap();
            assert_eq!(
                sizer.len(),
                real,
                "size oracle disagreed at scan={scan}, n={n}"
            );
        }
    }
}

/// A frame exactly at the cap is accepted and one byte over is refused —
/// measured on a real reply carrying vectors and application maps, not on a
/// synthetic body.
#[tokio::test]
async fn recovery_v5_exact_cap_and_one_byte_over() {
    let server = start_test_server().await;
    let cid = server.db.create_collective("cap-boundary").unwrap();
    server.db.record_experience(minimal_exp(cid)).unwrap();

    let page = pull_page(&server.server, 0, 500, None);
    let reply = WireReply::ok(server.server.instance_id(), page);
    let exact = pulsedb::sync::wire::encoded_len(&reply).unwrap();

    let framed = pulsedb::sync::wire::encode_bounded(WireOperation::Pull, &reply, exact).unwrap();
    assert_eq!(framed.len(), exact, "at the cap, the frame is built");

    let err =
        pulsedb::sync::wire::encode_bounded(WireOperation::Pull, &reply, exact - 1).unwrap_err();
    assert!(
        matches!(err, SyncError::PayloadTooLarge { size, max } if size == exact && max == exact - 1),
        "one byte under the exact size must refuse before allocating, got {err}"
    );

    // And the decoder's cap is the same boundary.
    pulsedb::sync::wire::decode_bounded::<WireReply<PullPage>>(WireOperation::Pull, &framed, exact)
        .expect("exactly at the cap decodes");
    let err = pulsedb::sync::wire::decode_bounded::<WireReply<PullPage>>(
        WireOperation::Pull,
        &framed,
        exact - 1,
    )
    .unwrap_err();
    assert!(err.is_payload_too_large(), "got {err}");
}

/// The effective budget is the MINIMUM of the two sides, on both legs — a
/// server with a generous cap still honours a requester's tight one, and a
/// requester with a generous cap is still bounded by its transport's real
/// reader limit.
#[tokio::test]
async fn recovery_v5_unequal_limits_use_the_minimum() {
    let server = start_test_server().await; // 64 MiB policy
    fill_wal_with_collectives(&server.db, 40);

    // Server generous, requester tight: the reply honours the requester.
    let tight = pulsedb::sync::MIN_CONTROL_FRAME_BYTES as u64;
    let mut request = pull_request(server.server.instance_id(), 0, 500);
    request.reply_limit_bytes = tight;
    let page = server
        .server
        .handle_pull(request)
        .unwrap()
        .into_result(server.server.instance_id())
        .unwrap();
    let framed = pulsedb::sync::wire::encode_bounded(
        WireOperation::Pull,
        &WireReply::ok(server.server.instance_id(), page.clone()),
        usize::MAX,
    )
    .unwrap();
    assert!(framed.len() <= tight as usize);
    assert!(page.has_more, "the tight budget truncated the page");

    // Requester generous, server tight: the reply honours the server.
    let (small_server, small_db, _dir) = in_process_server_with_cap_and_db(2048);
    let ids = fill_wal_with_collectives(&small_db, 40);
    assert_eq!(ids.len(), 40);
    let mut request = pull_request(small_server.instance_id(), 0, 500);
    request.reply_limit_bytes = 64 * 1024 * 1024;
    let page = small_server
        .handle_pull(request)
        .unwrap()
        .into_result(small_server.instance_id())
        .unwrap();
    let framed = pulsedb::sync::wire::encode_bounded(
        WireOperation::Pull,
        &WireReply::ok(small_server.instance_id(), page.clone()),
        usize::MAX,
    )
    .unwrap();
    assert!(
        framed.len() <= 2048,
        "the server's own policy bounds the reply it builds, got {}",
        framed.len()
    );
    assert!(page.has_more);
}

/// The reply capacity is preflighted BEFORE a push applies anything: a
/// requester whose stated budget cannot hold a bounded control frame is
/// refused, with nothing applied.
#[tokio::test]
async fn recovery_v5_push_preflights_reply_capacity_before_applying() {
    use pulsedb::sync::types::{SyncChange, SyncEntityType, SyncPayload};

    let server = start_test_server().await;
    let sender = InstanceId::new();
    let cid = server.db.create_collective("preflight").unwrap();
    let seed_id = server.db.record_experience(minimal_exp(cid)).unwrap();
    let mut arrival = server.db.get_experience(seed_id).unwrap().unwrap();
    arrival.id = pulsedb::ExperienceId::new();

    let before = snapshot(&server.db);
    let mut request = push_request(
        sender,
        server.db.instance_id(),
        vec![SyncChange {
            sequence: 1,
            source_instance: sender,
            collective_id: cid,
            entity_type: SyncEntityType::Experience,
            payload: SyncPayload::ExperienceCreated(arrival.clone().into()),
            timestamp: pulsedb::Timestamp::now(),
        }],
    );
    request.reply_limit_bytes = 16; // cannot hold any reply

    let err = server
        .server
        .handle_push(request)
        .expect_err("an unanswerable request must be refused, not applied");
    assert!(err.is_payload_too_large(), "got {err}");
    assert!(
        server.db.get_experience(arrival.id).unwrap().is_none(),
        "nothing may be applied when the answer could not be sent"
    );
    assert_eq!(snapshot(&server.db), before);
}

// ============================================================================
// Combined acceptance — byte-fit prefix, a failed apply, a filtered tail, a
// successful retry and compaction as ONE scenario rather than separate cases,
// because the defects (#90, #96, #98) interact and a per-case test cannot show
// that they compose.
// ============================================================================

/// A client transport that makes ONE designated change fail to apply, ONCE.
///
/// It is a deterministic tripwire, not a timer: on the first push whose batch
/// contains `fail_sequence` it removes that change before forwarding, so the
/// peer genuinely never receives it, and rewrites the acknowledgement into the
/// shape a real partial failure has — `total` still counts what the sender
/// submitted, `rejected` is 1, and `safe_through` is the highest submitted
/// sequence strictly BELOW the failure. Every downstream effect (the cursor,
/// compaction, what is searchable) is therefore real.
///
/// On the second attempt the tripwire is spent and the change goes through,
/// which is the retry half of the scenario.
struct FailOnceTransport {
    inner: HttpSyncTransport,
    fail_sequence: u64,
    armed: std::sync::atomic::AtomicBool,
}

impl FailOnceTransport {
    fn new(inner: HttpSyncTransport, fail_sequence: u64) -> Self {
        Self {
            inner,
            fail_sequence,
            armed: std::sync::atomic::AtomicBool::new(true),
        }
    }
}

#[async_trait::async_trait]
impl pulsedb::sync::transport::SyncTransport for FailOnceTransport {
    async fn handshake(
        &self,
        request: HandshakeRequest,
        send_budget_bytes: usize,
    ) -> Result<pulsedb::sync::types::HandshakeResponse, SyncError> {
        self.inner.handshake(request, send_budget_bytes).await
    }

    async fn push_changes(
        &self,
        request: PushRequest,
        send_budget_bytes: usize,
    ) -> Result<WireReply<pulsedb::sync::types::PushAck>, SyncError> {
        use std::sync::atomic::Ordering;

        let submitted: Vec<u64> = request.changes.iter().map(|c| c.sequence).collect();
        let hit =
            submitted.contains(&self.fail_sequence) && self.armed.swap(false, Ordering::SeqCst);
        if !hit {
            return self.inner.push_changes(request, send_budget_bytes).await;
        }

        let mut forwarded = request;
        forwarded
            .changes
            .retain(|c| c.sequence != self.fail_sequence);
        let reply = self
            .inner
            .push_changes(forwarded, send_budget_bytes)
            .await?;
        let responder = reply.responder;
        let ack = reply.into_result(responder)?;
        Ok(WireReply::ok(
            responder,
            pulsedb::sync::types::PushAck {
                wal_owner: ack.wal_owner,
                accepted: submitted.len() as u64 - 1,
                rejected: 1,
                total: submitted.len() as u64,
                // The actual-success position: the highest submitted sequence
                // strictly below the failure. Never the tail, never
                // `failure - 1`.
                safe_through: submitted
                    .iter()
                    .copied()
                    .filter(|s| *s < self.fail_sequence)
                    .max(),
            },
        ))
    }

    async fn pull_changes(
        &self,
        request: PullRequest,
        send_budget_bytes: usize,
    ) -> Result<WireReply<PullPage>, SyncError> {
        self.inner.pull_changes(request, send_budget_bytes).await
    }

    async fn health_check(&self) -> Result<(), SyncError> {
        self.inner.health_check().await
    }

    fn receive_limit_bytes(&self) -> usize {
        self.inner.receive_limit_bytes()
    }
}

/// **The whole contract in one run.**
///
/// A byte budget makes a prefix; one eligible change fails to apply; the
/// successes after it and the filtered tail behind it do NOT let the cursor
/// past it; the retry applies it; compaction then trims the WAL, and a new
/// write still reaches the peer afterwards. G-counter totals converge exactly
/// and the synced experiences are searchable.
///
/// Separate green cases for each of those would not catch the interaction that
/// this repair exists for — a cursor that advances on filtered progress must
/// still refuse to advance over a failure, and a compaction driven by that
/// cursor must not delete what was never delivered.
#[tokio::test]
async fn recovery_v5_combined_prefix_failure_retry_compaction() {
    let server = start_test_server().await;
    let (db_client, _dir_client) = open_client();

    // Two collectives: `kept` is synced, `dropped` is filtered out and supplies
    // the filtered tail.
    let kept = db_client.create_collective("combined-kept").unwrap(); // seq 1
    let mut kept_ids = Vec::new();
    for _ in 0..4 {
        kept_ids.push(
            db_client
                .record_experience(NewExperience {
                    collective_id: kept,
                    content: format!("combined-{}", uuid::Uuid::now_v7()),
                    embedding: Some(vec![0.1f32; 384]),
                    ..Default::default()
                })
                .unwrap(),
        ); // seq 2..5
    }
    let dropped = db_client.create_collective("combined-dropped").unwrap(); // seq 6
    let dropped_id = db_client
        .record_experience(NewExperience {
            collective_id: dropped,
            content: "filtered".into(),
            embedding: Some(vec![0.2f32; 384]),
            ..Default::default()
        })
        .unwrap(); // seq 7
    let tail_id = db_client
        .record_experience(NewExperience {
            collective_id: kept,
            content: "after the filtered tail".into(),
            embedding: Some(vec![0.1f32; 384]),
            ..Default::default()
        })
        .unwrap(); // seq 8
    let head = db_client.get_current_sequence().unwrap();
    assert_eq!(head, 8);

    // A budget tight enough to force a prefix, and a tripwire on sequence 4.
    const FAILING: u64 = 4;
    let mut manager = SyncManager::new(
        Arc::clone(&db_client),
        Box::new(FailOnceTransport::new(
            HttpSyncTransport::new(&server.base_url),
            FAILING,
        )),
        SyncConfig {
            direction: SyncDirection::PushOnly,
            collectives: Some(vec![kept]),
            max_request_bytes: 2 * 1024,
            ..SyncConfig::default()
        },
    )
    .unwrap();

    let peer = server.db.instance_id();
    let pushed = |db: &Arc<PulseDB>| {
        db.storage_for_test()
            .load_sync_cursor(&peer)
            .unwrap()
            .map_or(0, |c| c.push_sequence)
    };

    // Cycle 1: a byte-truncated prefix.
    manager.sync_once().await.unwrap();
    let after_first = pushed(&db_client);
    assert!(
        after_first > 0 && after_first < head,
        "the budget must truncate, got a cursor of {after_first}"
    );

    // Run to completion. The tripwire fires exactly once; the cursor must never
    // pass sequence 4 before that change has actually applied.
    let mut cycles = 1;
    let mut saw_hold = false;
    loop {
        let before = pushed(&db_client);
        manager.sync_once().await.unwrap();
        let now = pushed(&db_client);
        if before < FAILING && now == FAILING - 1 {
            saw_hold = true;
            assert!(
                server.db.get_experience(kept_ids[2]).unwrap().is_none(),
                "sequence 4 has not applied, so it must not be on the peer yet"
            );
        }
        assert!(
            now >= before,
            "a cursor may never retreat: {before} -> {now}"
        );
        if now >= head {
            break;
        }
        cycles += 1;
        assert!(cycles < 40, "no progress after {cycles} cycles");
    }
    assert!(
        saw_hold,
        "the failing change must have held the cursor at {} for a cycle",
        FAILING - 1
    );
    assert!(cycles > 1, "the byte budget must have taken several cycles");

    // Everything eligible arrived, including the one that failed first and the
    // one behind the filtered tail; nothing filtered leaked.
    for (index, id) in kept_ids.iter().enumerate() {
        assert!(
            server.db.get_experience(*id).unwrap().is_some(),
            "kept experience {index} never arrived"
        );
    }
    assert!(
        server.db.get_experience(tail_id).unwrap().is_some(),
        "the change behind the filtered tail must be delivered — a filtered page \
         is progress, not a stall"
    );
    assert!(
        server.db.get_experience(dropped_id).unwrap().is_none(),
        "the filter still excludes what it excludes"
    );
    assert_eq!(
        server
            .db
            .search_similar(kept, &vec![0.1f32; 384], 50)
            .unwrap()
            .len(),
        kept_ids.len() + 1,
        "every delivered experience is searchable on the peer, vectors and all"
    );
    assert_eq!(
        pushed(&db_client),
        head,
        "with everything applied the cursor reaches the WAL head, filtered tail included"
    );

    // Compaction is driven by that cursor, and it may now trim.
    let deleted = db_client.compact_wal().unwrap();
    assert!(deleted > 0, "an acknowledged WAL must be compactable");
    assert!(
        db_client
            .storage_for_test()
            .poll_sync_events(0, 100)
            .unwrap()
            .is_empty(),
        "everything below the acknowledged position is gone"
    );

    // A write AFTER compaction still reaches the peer.
    let after_compaction = db_client
        .record_experience(NewExperience {
            collective_id: kept,
            content: "written after compaction".into(),
            embedding: Some(vec![0.1f32; 384]),
            ..Default::default()
        })
        .unwrap();
    manager.sync_once().await.unwrap();
    assert!(
        server
            .db
            .get_experience(after_compaction)
            .unwrap()
            .is_some(),
        "compaction must not break the next sync"
    );

    // G-counter convergence, exact: two reinforcements here, one there.
    db_client.reinforce_experience(kept_ids[0]).unwrap();
    db_client.reinforce_experience(kept_ids[0]).unwrap();
    server.db.reinforce_experience(kept_ids[0]).unwrap();

    let mut push = SyncManager::new(
        Arc::clone(&db_client),
        Box::new(HttpSyncTransport::new(&server.base_url)),
        SyncConfig {
            direction: SyncDirection::PushOnly,
            collectives: Some(vec![kept]),
            ..SyncConfig::default()
        },
    )
    .unwrap();
    let mut pull = SyncManager::new(
        Arc::clone(&db_client),
        Box::new(HttpSyncTransport::new(&server.base_url)),
        SyncConfig {
            direction: SyncDirection::PullOnly,
            collectives: Some(vec![kept]),
            ..SyncConfig::default()
        },
    )
    .unwrap();
    push.sync_once().await.unwrap();
    pull.initial_sync(None).await.unwrap();

    let local = db_client.get_experience(kept_ids[0]).unwrap().unwrap();
    let remote = server.db.get_experience(kept_ids[0]).unwrap().unwrap();
    assert_eq!(local.applications(), 3, "two here plus one there");
    assert_eq!(remote.applications(), 3);
    assert_eq!(
        local.applications, remote.applications,
        "the G-counter converges on the same buckets, not merely the same total"
    );
}

/// Safe filtered progress is kept, and the oversized change behind it is never
/// acknowledged: the background run stops explicitly rather than rebuilding a
/// body it already knows will not fit.
#[tokio::test]
async fn recovery_v5_filtered_prefix_then_oversize() {
    let server = start_test_server().await;
    let (db_client, _dir_client) = open_client();

    let kept = db_client.create_collective("oversize-kept").unwrap(); // seq 1
    let dropped = db_client.create_collective("oversize-dropped").unwrap(); // seq 2
    db_client
        .record_experience(NewExperience {
            collective_id: dropped,
            content: "filtered away".into(),
            embedding: Some(vec![0.2f32; 384]),
            ..Default::default()
        })
        .unwrap(); // seq 3
    let oversized = db_client
        .record_experience(NewExperience {
            collective_id: kept,
            content: "y".repeat(8 * 1024),
            embedding: Some(vec![0.1f32; 384]),
            ..Default::default()
        })
        .unwrap(); // seq 4

    let cap = 4 * 1024;
    let mut manager = SyncManager::new(
        Arc::clone(&db_client),
        Box::new(HttpSyncTransport::new(&server.base_url)),
        SyncConfig {
            direction: SyncDirection::PushOnly,
            collectives: Some(vec![kept]),
            max_request_bytes: cap,
            push_interval_ms: 10,
            pull_interval_ms: 10,
            ..SyncConfig::default()
        },
    )
    .unwrap();
    let peer = server.db.instance_id();

    // Cycle 1: the kept collective fits; the two filtered events advance the
    // scan position behind it; the oversized change does not fit and is left.
    manager.sync_once().await.unwrap();
    let cursor = db_client
        .storage_for_test()
        .load_sync_cursor(&peer)
        .unwrap()
        .unwrap();
    assert_eq!(
        cursor.push_sequence, 3,
        "safe filtered progress is retained — up to the last SCANNED event before \
         the change that could not be sent, and no further"
    );

    // Cycle 2 meets the oversized change with nothing in front of it.
    let err = manager
        .sync_once()
        .await
        .expect_err("one change cannot fit a body on its own");
    assert!(
        matches!(err, SyncError::ChangeTooLarge { sequence: 4, cap: c, .. } if c == cap as u64),
        "got {err}"
    );
    assert_eq!(
        db_client
            .storage_for_test()
            .load_sync_cursor(&peer)
            .unwrap()
            .unwrap()
            .push_sequence,
        3,
        "the oversized change is never acknowledged"
    );
    assert!(server.db.get_experience(oversized).unwrap().is_none());

    // And the background run stops explicitly rather than retrying.
    manager.start().await.unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while std::time::Instant::now() < deadline && !matches!(manager.status(), SyncStatus::Error(_))
    {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        matches!(manager.status(), SyncStatus::Error(ref m) if m.contains("on its own")),
        "the background loop must record the terminal error, got {:?}",
        manager.status()
    );
    // The loop ticks every 10 ms; this window spans ~30 of them. The cursor is
    // the attempt evidence — a retried push would move it or fail again.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(
        db_client
            .storage_for_test()
            .load_sync_cursor(&peer)
            .unwrap()
            .unwrap()
            .push_sequence,
        3,
        "and nothing moves across ~30 poll intervals while it is stopped"
    );
    // Deterministic proof the task EXITED: `start()` on a live run is refused,
    // so one that succeeds means the finished handle was reaped.
    manager
        .start()
        .await
        .expect("the terminal task exited, so a restart reaps it");
    manager.stop().await.unwrap();
}

// ============================================================================
// Metadata-only scan advance under the byte cap (#26/#98 follow-up)
// ============================================================================

/// Serves an axum sync endpoint over an ALREADY-BUILT server.
///
/// `start_test_server` builds its own `SyncServer` with the default policy. A
/// cap measured from a fixture is only knowable after the fixture exists, so a
/// test that pins the cap to a real frame has to construct the server itself
/// and serve it afterwards.
async fn serve(server: Arc<SyncServer>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = sync_router(server);
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    format!("http://{}", addr)
}

/// The exact length of the pull reply carrying `changes` at scan position
/// `scan`, measured with `wire::encoded_len` over the real frame.
///
/// This is the same oracle `handle_pull` sizes against, used here as an
/// independent measurement of what the handler actually emits.
fn reply_frame_len(
    responder: InstanceId,
    changes: &[pulsedb::sync::types::SyncChange],
    scan: u64,
) -> usize {
    wire::encoded_len(&WireReply::ok(
        responder,
        PullPage {
            changes: changes.to_vec(),
            has_more: true,
            scan_position: SyncPosition::new(responder, scan),
        },
    ))
    .unwrap()
}

/// One framed pull straight at the byte handler, decoded back.
///
/// `handle_pull` alone never encodes anything, so a reply that exceeds its own
/// cap is invisible to it. Only the byte path proves the frame the handler
/// emits is the frame it measured.
fn pull_page_bytes(
    server: &SyncServer,
    from: u64,
    cap: usize,
    collectives: Option<Vec<CollectiveId>>,
) -> Result<(PullPage, usize), SyncError> {
    let mut request = pull_request(server.instance_id(), from, 500);
    request.collectives = collectives;
    request.reply_limit_bytes = cap as u64;
    let framed = wire::encode_bounded(WireOperation::Pull, &request, usize::MAX).unwrap();
    let bytes = server.handle_pull_bytes(&framed)?;
    let len = bytes.len();
    let reply: WireReply<PullPage> =
        wire::decode_bounded(WireOperation::Pull, &bytes, cap).expect("the reply decodes");
    Ok((reply.into_result(server.instance_id()).unwrap(), len))
}

/// **The scan position is part of the frame it travels in.**
///
/// A filtered event is never emitted, so moving past it is progress (#90) — but
/// the position that progress reports is a postcard varint inside the reply,
/// and it widens at 127 → 128, 16 383 → 16 384, … A page packed exactly to its
/// cap that then walks a filtered run across such a boundary encodes to 1–9
/// bytes MORE than the frame that was measured, and the server's own
/// `encode_bounded` refuses it: no page is served, the cursor does not move,
/// the next request is byte-identical, and `SyncError::PayloadTooLarge` is not
/// `is_change_too_large()`, so the background loop retries a body already known
/// not to fit — forever.
///
/// The rule this pins: a metadata-only advance is committed only if the
/// COMPLETE reply carrying it still fits. Otherwise the scan position holds at
/// the last size-validated value, the fitting prefix is returned with
/// `has_more: true`, and the next pull resumes the withheld tail.
///
/// `has_more` is the half that cannot be dropped. The poll here comes back
/// SHORT (131 events against the server's 1 000-event page), so
/// `events.len() >= PULL_PAGE_EVENT_LIMIT` is false and nothing but the
/// truncation flag stops the reply from claiming the WAL exhausted while an
/// eligible change sits undelivered behind the withheld tail.
#[tokio::test]
async fn recovery_v5_filtered_tail_scan_advance_cannot_overflow_the_reply() {
    let (db, _dir) = open_client();

    // ─── Phase 1: the eligible prefix, and nothing else yet ──────────
    let kept = db.create_collective("cap-kept").unwrap(); // seq 1
    let dropped = db.create_collective("cap-dropped").unwrap(); // seq 2
    for _ in 0..10 {
        db.record_experience(minimal_exp(kept)).unwrap(); // seq 3..=12
    }
    let prefix_end = db.get_current_sequence().unwrap();
    assert_eq!(prefix_end, 12);

    // The cap IS the prefix's own frame — measured, not chosen. Both sides at
    // the same number is the ordinary case, not a contrived one.
    let probe = SyncServer::new(Arc::clone(&db), SyncConfig::default()).unwrap();
    let responder = probe.instance_id();
    let prefix = pull_page(&probe, 0, 500, Some(vec![kept]));
    assert_eq!(
        prefix.changes.len(),
        11,
        "the collective plus ten experiences"
    );
    assert_eq!(prefix.scan_position.sequence, prefix_end);
    let cap = reply_frame_len(responder, &prefix.changes, prefix_end);

    // ─── Phase 2: a filtered run crossing 127 → 128, then one eligible
    //     change behind it ───────────────────────────────────────────
    for _ in 0..118 {
        db.record_experience(minimal_exp(dropped)).unwrap(); // seq 13..=130
    }
    let beyond = db.record_experience(minimal_exp(kept)).unwrap(); // seq 131
    let head = db.get_current_sequence().unwrap();
    assert_eq!(head, 131);
    assert!(
        head < SERVER_PULL_PAGE_EVENTS as u64,
        "the poll must come back SHORT, or `has_more` is true for an unrelated \
         reason and this fixture proves nothing about the truncation flag"
    );

    let server = Arc::new(
        SyncServer::new(
            Arc::clone(&db),
            SyncConfig {
                max_request_bytes: cap,
                ..SyncConfig::default()
            },
        )
        .unwrap(),
    );

    let (page, framed_len) = pull_page_bytes(&server, 0, cap, Some(vec![kept]))
        .expect("a pull must never build a reply its own cap refuses");
    assert!(
        framed_len <= cap,
        "the emitted frame must fit the cap it was packed against, got {framed_len} > {cap}"
    );
    assert_eq!(page.changes.len(), prefix.changes.len());
    assert_eq!(
        page.changes.last().unwrap().sequence,
        prefix_end,
        "the emitted prefix is unchanged — this withholds scan progress, not changes"
    );
    assert_eq!(
        page.scan_position.sequence, 127,
        "the scan position holds at the last SIZE-VALIDATED filtered event"
    );
    assert!(
        page.has_more,
        "a metadata-only stop is a truncation: the page is SHORT, so without this \
         flag the reply claims a WAL exhaustion that sequence {head} contradicts"
    );

    // 127 is not an arbitrary stopping point: it is the largest position whose
    // complete frame still fits, and 128 is the byte this repairs.
    assert_eq!(reply_frame_len(responder, &page.changes, 127), cap);
    assert_eq!(
        reply_frame_len(responder, &page.changes, 128),
        cap + 1,
        "128 is where the scan position's varint widens"
    );

    // ─── The withheld tail is resumed, over real HTTP ────────────────
    let base_url = serve(Arc::clone(&server)).await;
    let (client, _client_dir) = open_client();
    let mut manager = SyncManager::new(
        Arc::clone(&client),
        Box::new(HttpSyncTransport::new(&base_url)),
        SyncConfig {
            direction: SyncDirection::PullOnly,
            collectives: Some(vec![kept]),
            max_request_bytes: cap,
            ..SyncConfig::default()
        },
    )
    .unwrap();
    manager
        .initial_sync(None)
        .await
        .expect("a withheld scan tail is progress, not an incomplete catch-up");

    assert!(
        client.get_experience(beyond).unwrap().is_some(),
        "the eligible change behind the withheld filtered tail must ACTUALLY \
         arrive — a truthful `has_more` that never delivers is the same defect"
    );
    assert_eq!(
        client
            .storage_for_test()
            .load_sync_cursor(&responder)
            .unwrap()
            .unwrap()
            .pull_sequence,
        head,
        "and the catch-up ends at the WAL head, not short of it"
    );
    assert!(
        client.get_collective(dropped).unwrap().is_none(),
        "the filter still excludes what it excludes"
    );
}

/// The same rule on the OTHER branch that advances the scan position: an event
/// whose entity no longer resolves.
///
/// The fixture is a run of experience creates whose experiences are deleted
/// afterwards — the create events stay in the WAL and `build_change_from_record`
/// answers `None` for each — with the eligible deletes behind them. No
/// `collectives` filter is involved, so this is the unresolvable path on its
/// own, not the filtered one under another name.
#[tokio::test]
async fn recovery_v5_unresolvable_tail_scan_advance_cannot_overflow_the_reply() {
    let (db, _dir) = open_client();

    let cid = db.create_collective("unresolvable-cap").unwrap(); // seq 1
    for _ in 0..10 {
        db.record_experience(minimal_exp(cid)).unwrap(); // seq 2..=11
    }
    let prefix_end = db.get_current_sequence().unwrap();
    assert_eq!(prefix_end, 11);

    let probe = SyncServer::new(Arc::clone(&db), SyncConfig::default()).unwrap();
    let responder = probe.instance_id();
    let prefix = pull_page(&probe, 0, 500, None);
    assert_eq!(prefix.changes.len(), 11);
    assert_eq!(prefix.scan_position.sequence, prefix_end);
    let cap = reply_frame_len(responder, &prefix.changes, prefix_end);

    let doomed: Vec<_> = (0..119)
        .map(|_| db.record_experience(minimal_exp(cid)).unwrap()) // seq 12..=130
        .collect();
    for id in &doomed {
        db.delete_experience(*id).unwrap(); // seq 131..=249
    }
    let head = db.get_current_sequence().unwrap();
    assert_eq!(head, 249);
    assert!(head < SERVER_PULL_PAGE_EVENTS as u64, "a SHORT poll page");

    let server = SyncServer::new(
        Arc::clone(&db),
        SyncConfig {
            max_request_bytes: cap,
            ..SyncConfig::default()
        },
    )
    .unwrap();

    let (page, framed_len) = pull_page_bytes(&server, 0, cap, None)
        .expect("an unresolvable run must not push the reply over its cap either");
    assert!(framed_len <= cap, "got {framed_len} > {cap}");
    assert_eq!(page.changes.len(), prefix.changes.len());
    assert_eq!(
        page.scan_position.sequence, 127,
        "the scan position holds at the last size-validated unresolvable event"
    );
    assert!(page.has_more);
    assert_eq!(
        reply_frame_len(responder, &page.changes, 128),
        cap + 1,
        "128 is the position that would not have fitted"
    );

    // Every remaining change is delivered, in a BOUNDED number of round trips,
    // and the catch-up only reports exhaustion once it really is exhausted.
    let mut delivered: Vec<u64> = page.changes.iter().map(|c| c.sequence).collect();
    let mut from = page.scan_position.sequence;
    let mut round_trips = 1;
    loop {
        let (next, len) = pull_page_bytes(&server, from, cap, None).expect("every follow-up pull");
        assert!(len <= cap);
        assert!(
            next.scan_position.sequence > from || !next.has_more,
            "a page that reports more must also make progress"
        );
        delivered.extend(next.changes.iter().map(|c| c.sequence));
        from = next.scan_position.sequence;
        round_trips += 1;
        if !next.has_more {
            break;
        }
        assert!(
            round_trips < 40,
            "no progress after {round_trips} round trips"
        );
    }
    assert_eq!(from, head, "the scan reaches the WAL head");
    assert!(
        delivered.contains(&131),
        "the first eligible change behind the withheld unresolvable tail must arrive"
    );
    assert_eq!(
        delivered.len(),
        11 + doomed.len(),
        "the prefix plus every delete — nothing skipped by the withheld tail"
    );
    assert!(
        delivered.windows(2).all(|w| w[0] < w[1]),
        "and delivered in WAL order, once each"
    );
}

/// The constant-time identity the scan-advance check is built on, measured
/// against REAL frames at every varint width a `u64` WAL sequence reaches —
/// including the ones no test fixture can hold events for.
///
/// Moving the scan position from `a` to `b` grows the reply by exactly
/// `varint_len(b) − varint_len(a)`, whatever the prefix carries. That is why
/// the check needs no second size formula, and it is measured here rather than
/// argued.
///
/// It also certifies the progress claim the rule depends on: an EMPTY reply at
/// the widest possible scan position still fits the 1 KiB control minimum, and
/// `handle_pull` refuses any smaller effective cap. So a metadata-only stop can
/// only ever fire behind a non-empty prefix — it cannot produce a page that
/// reports more while advancing nothing.
#[tokio::test]
async fn recovery_v5_scan_advance_delta_matches_the_real_frame_at_every_width() {
    let (db, _dir) = open_client();
    let cid = db.create_collective("scan-advance-widths").unwrap();
    db.record_experience(minimal_exp(cid)).unwrap();
    let probe = SyncServer::new(Arc::clone(&db), SyncConfig::default()).unwrap();
    let responder = probe.instance_id();
    let page = pull_page(&probe, 0, 500, None);
    assert_eq!(page.changes.len(), 2, "a collective and an experience");

    for (a, b) in [
        (0u64, 1),
        (126, 127),
        (127, 128),
        (128, 129),
        (16_383, 16_384),
        (2_097_151, 2_097_152),
        (268_435_455, 268_435_456),
        (1, u64::MAX),
    ] {
        for changes in [&[][..], &page.changes[..]] {
            let grew =
                reply_frame_len(responder, changes, b) - reply_frame_len(responder, changes, a);
            assert_eq!(
                grew,
                wire::varint_len(b) - wire::varint_len(a),
                "the real frame disagreed moving the scan position {a} → {b} with \
                 {} changes",
                changes.len()
            );
        }
    }

    assert!(
        reply_frame_len(responder, &[], u64::MAX) <= pulsedb::sync::MIN_CONTROL_FRAME_BYTES,
        "an empty reply at the widest scan position must fit the control minimum, \
         or a metadata-only stop could fire on an empty prefix and stall"
    );
}

// ============================================================================
// Transport send budget — the outbound cap is the packer's, never the
// inbound reader's (R1-R6, R9-R10).
//
// A client may legitimately read less than it sends: `max_response_bytes` is
// the body this client will READ, and nothing about it bounds what a peer will
// accept. Substituting it for the outbound encode cap refuses a body the peer
// would have taken, on every cycle, from the same cursor.
// ============================================================================

/// 4 MiB — the tight inbound reader the crate's own example documents.
const TIGHT_READER_BYTES: usize = 4 * 1024 * 1024;

/// One push as it actually went out: the values a test needs to check are
/// recorded from the request itself, not inferred from the change count.
#[derive(Clone, Debug)]
struct PushObservation {
    /// What the request advertised as this side's inbound reply budget.
    reply_limit_bytes: u64,
    /// The outbound budget its caller supplied.
    send_budget_bytes: usize,
    /// The exact encoded frame length of the whole request.
    frame_len: usize,
    /// The frame length of the same request with an EMPTY change vector — the
    /// envelope the packer sizes against.
    envelope_len: usize,
    /// The summed encoded length of the changes it carried.
    item_bytes: usize,
    changes: usize,
}

/// A transport wrapper that records every push it forwards, so a test can prove
/// what crossed the wire rather than assert it from the change count.
struct PushSizeWitness {
    inner: HttpSyncTransport,
    observations: Arc<std::sync::Mutex<Vec<PushObservation>>>,
}

/// What a [`PushSizeWitness`] recorded, handed back to the test separately from
/// the transport the manager takes ownership of.
struct PushObservations {
    observations: Arc<std::sync::Mutex<Vec<PushObservation>>>,
}

impl PushObservations {
    fn all(&self) -> Vec<PushObservation> {
        self.observations.lock().unwrap().clone()
    }

    fn largest_frame(&self) -> usize {
        self.all().iter().map(|o| o.frame_len).max().unwrap_or(0)
    }

    fn pushes(&self) -> usize {
        self.all().len()
    }

    fn send_budgets(&self) -> Vec<usize> {
        self.all().iter().map(|o| o.send_budget_bytes).collect()
    }

    fn reply_limits(&self) -> Vec<u64> {
        self.all().iter().map(|o| o.reply_limit_bytes).collect()
    }

    /// The reply budget advertised by the EMPTY probe, if one was sent.
    fn empty_probe_reply_limit(&self) -> Option<u64> {
        self.all()
            .iter()
            .find(|o| o.changes == 0)
            .map(|o| o.reply_limit_bytes)
    }

    /// `(envelope, frame, item bytes, change count)` of the largest push
    /// observed.
    fn envelope_frame_and_items(&self) -> (usize, usize, usize, usize) {
        let o = self
            .all()
            .into_iter()
            .max_by_key(|o| o.frame_len)
            .expect("at least one push");
        (o.envelope_len, o.frame_len, o.item_bytes, o.changes)
    }
}

impl PushSizeWitness {
    fn new(inner: HttpSyncTransport) -> (Self, PushObservations) {
        let observations = Arc::new(std::sync::Mutex::new(Vec::new()));
        (
            Self {
                inner,
                observations: Arc::clone(&observations),
            },
            PushObservations { observations },
        )
    }
}

#[async_trait::async_trait]
impl pulsedb::sync::transport::SyncTransport for PushSizeWitness {
    async fn handshake(
        &self,
        request: HandshakeRequest,
        send_budget_bytes: usize,
    ) -> Result<pulsedb::sync::types::HandshakeResponse, SyncError> {
        self.inner.handshake(request, send_budget_bytes).await
    }

    async fn push_changes(
        &self,
        request: PushRequest,
        send_budget_bytes: usize,
    ) -> Result<WireReply<pulsedb::sync::types::PushAck>, SyncError> {
        let envelope_len = wire::encoded_len(&PushRequest {
            changes: Vec::new(),
            ..request.clone()
        })?;
        let mut item_bytes = 0usize;
        for change in &request.changes {
            item_bytes += wire::item_len(change)?;
        }
        self.observations.lock().unwrap().push(PushObservation {
            reply_limit_bytes: request.reply_limit_bytes,
            send_budget_bytes,
            frame_len: wire::encoded_len(&request)?,
            envelope_len,
            item_bytes,
            changes: request.changes.len(),
        });
        self.inner.push_changes(request, send_budget_bytes).await
    }

    async fn pull_changes(
        &self,
        request: PullRequest,
        send_budget_bytes: usize,
    ) -> Result<WireReply<PullPage>, SyncError> {
        self.inner.pull_changes(request, send_budget_bytes).await
    }

    async fn health_check(&self) -> Result<(), SyncError> {
        self.inner.health_check().await
    }

    fn receive_limit_bytes(&self) -> usize {
        self.inner.receive_limit_bytes()
    }
}

/// The pull-side counterpart: records what each request advertised as its
/// inbound budget, and how big the reply that came back actually was.
struct PullLimitWitness {
    inner: HttpSyncTransport,
    reply_limits: Arc<std::sync::Mutex<Vec<u64>>>,
    largest_reply_frame: Arc<std::sync::atomic::AtomicUsize>,
}

struct PullObservations {
    reply_limits: Arc<std::sync::Mutex<Vec<u64>>>,
    largest_reply_frame: Arc<std::sync::atomic::AtomicUsize>,
}

impl PullObservations {
    fn reply_limits(&self) -> Vec<u64> {
        self.reply_limits.lock().unwrap().clone()
    }

    fn largest_reply_frame(&self) -> usize {
        self.largest_reply_frame
            .load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl PullLimitWitness {
    fn new(inner: HttpSyncTransport) -> (Self, PullObservations) {
        let reply_limits = Arc::new(std::sync::Mutex::new(Vec::new()));
        let largest_reply_frame = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        (
            Self {
                inner,
                reply_limits: Arc::clone(&reply_limits),
                largest_reply_frame: Arc::clone(&largest_reply_frame),
            },
            PullObservations {
                reply_limits,
                largest_reply_frame,
            },
        )
    }
}

#[async_trait::async_trait]
impl pulsedb::sync::transport::SyncTransport for PullLimitWitness {
    async fn handshake(
        &self,
        request: HandshakeRequest,
        send_budget_bytes: usize,
    ) -> Result<pulsedb::sync::types::HandshakeResponse, SyncError> {
        self.inner.handshake(request, send_budget_bytes).await
    }

    async fn push_changes(
        &self,
        request: PushRequest,
        send_budget_bytes: usize,
    ) -> Result<WireReply<pulsedb::sync::types::PushAck>, SyncError> {
        self.inner.push_changes(request, send_budget_bytes).await
    }

    async fn pull_changes(
        &self,
        request: PullRequest,
        send_budget_bytes: usize,
    ) -> Result<WireReply<PullPage>, SyncError> {
        self.reply_limits
            .lock()
            .unwrap()
            .push(request.reply_limit_bytes);
        let reply = self.inner.pull_changes(request, send_budget_bytes).await?;
        self.largest_reply_frame.fetch_max(
            wire::encoded_len(&reply)?,
            std::sync::atomic::Ordering::SeqCst,
        );
        Ok(reply)
    }

    async fn health_check(&self) -> Result<(), SyncError> {
        self.inner.health_check().await
    }

    fn receive_limit_bytes(&self) -> usize {
        self.inner.receive_limit_bytes()
    }
}

/// Seeds `count` experiences of ~100 KiB each, enough to push the encoded push
/// frame past `TIGHT_READER_BYTES`.
fn seed_bulk_experiences(db: &Arc<PulseDB>, cid: CollectiveId, count: usize) {
    for i in 0..count {
        db.record_experience(NewExperience {
            collective_id: cid,
            content: format!("{i:06}").repeat(102_400 / 6),
            embedding: Some(vec![0.1f32; 384]),
            ..Default::default()
        })
        .unwrap();
    }
}

/// R1 — the original T3 defect, over real HTTP.
///
/// The client reads 4 MiB, but its own policy and the peer's advertised inbound
/// limit are both 64 MiB, so the packer builds a valid >4 MiB body. Encoding
/// that body against the client's INBOUND reader refuses it locally: the
/// request never leaves, the push cursor never advances, and the next cycle
/// rebuilds the identical body from the identical cursor. The outbound budget
/// must be the packer's cap.
#[tokio::test]
async fn recovery_v5_push_encodes_against_the_send_budget_not_the_response_reader() {
    let server = start_test_server().await; // 64 MiB policy, advertises 64 MiB
    let dir_client = tempdir().unwrap();
    let db_client =
        Arc::new(PulseDB::open(dir_client.path().join("client.db"), Config::default()).unwrap());

    let cid = db_client.create_collective("bulk-push").unwrap();
    seed_bulk_experiences(&db_client, cid, 45);

    let (witness, observed) = PushSizeWitness::new(
        HttpSyncTransport::new(&server.base_url).with_max_response_bytes(TIGHT_READER_BYTES),
    );
    let mut manager = SyncManager::new(
        Arc::clone(&db_client),
        Box::new(witness),
        SyncConfig {
            direction: SyncDirection::PushOnly,
            ..SyncConfig::default()
        },
    )
    .unwrap();

    manager
        .sync_once()
        .await
        .expect("a body the peer accepts must not be refused by the sender's own reader limit");

    let frame = observed.largest_frame();
    assert!(
        frame > TIGHT_READER_BYTES,
        "the regression needs a frame that actually crosses the 4 MiB reader; got {frame} bytes"
    );
    assert_eq!(observed.pushes(), 1);
    assert_eq!(
        observed.send_budgets(),
        vec![64 * 1024 * 1024],
        "the outbound budget is min(local policy, peer inbound) — the packer's own cap, \
         never the 4 MiB response reader"
    );

    // The peer received it.
    assert_eq!(
        server.db.list_experiences(cid, 100, 0).unwrap().len(),
        45,
        "every seeded experience must reach the peer"
    );
    // And the push cursor advanced, so the next cycle does not rebuild the
    // same body from the same position.
    let cursor = db_client
        .storage_for_test()
        .load_sync_cursor(&server.db.instance_id())
        .unwrap()
        .expect("a push cursor row for the peer");
    assert!(
        cursor.push_sequence >= db_client.get_current_sequence().unwrap(),
        "the push position must advance past everything sent, got {}",
        cursor.push_sequence
    );
}

/// R2 — the inbound advertisement, the other half of the same defect.
///
/// A `PushRequest` tells the peer how big a reply this side can read. Answering
/// that with `max_request_bytes` promises 64 MiB while the reader accepts 4 MiB
/// — a promise the reader cannot keep. Both the real push and the empty probe
/// must advertise the same min'd value, and the envelope the packer sized
/// against must be the frame that was actually sent.
#[tokio::test]
async fn recovery_v5_push_advertises_the_reply_budget_its_reader_can_keep() {
    let server = start_test_server().await; // 64 MiB policy
    let dir_client = tempdir().unwrap();
    let db_client =
        Arc::new(PulseDB::open(dir_client.path().join("client.db"), Config::default()).unwrap());

    let (witness, observed) = PushSizeWitness::new(
        HttpSyncTransport::new(&server.base_url).with_max_response_bytes(TIGHT_READER_BYTES),
    );
    let mut manager = SyncManager::new(
        Arc::clone(&db_client),
        Box::new(witness),
        SyncConfig {
            direction: SyncDirection::PushOnly,
            ..SyncConfig::default()
        },
    )
    .unwrap();

    // A real push: one collective, one experience.
    let cid = db_client.create_collective("advertised").unwrap();
    db_client.record_experience(minimal_exp(cid)).unwrap();
    manager.sync_once().await.unwrap();
    // And an empty probe: the cursor is now at the WAL head, so the next cycle
    // sends the bounded empty push.
    manager.sync_once().await.unwrap();

    let advertised = observed.reply_limits();
    assert_eq!(
        advertised.len(),
        2,
        "one real push and one empty probe, got {advertised:?}"
    );
    assert!(
        advertised
            .iter()
            .all(|limit| *limit == TIGHT_READER_BYTES as u64),
        "every push must advertise min(policy, actual reader) = 4 MiB, got {advertised:?}"
    );
    assert_eq!(
        observed.empty_probe_reply_limit(),
        Some(TIGHT_READER_BYTES as u64),
        "the empty probe must not differ from the real request"
    );

    // The sized envelope and the real frame agree: an envelope built from a
    // different `reply_limit_bytes` could be a varint byte narrower than what
    // is sent, and the exact-size oracle would be wrong. This is
    // `FrameSizer`'s own identity — the empty-collection envelope spends
    // `varint_len(0) == 1` byte on the count, which the real count replaces —
    // so it holds at any batch size, not only under 128 changes.
    let (envelope, frame, item_bytes, changes) = observed.envelope_frame_and_items();
    assert_eq!(
        envelope - 1 + wire::varint_len(changes as u64) + item_bytes,
        frame,
        "the envelope sized with the advertised limit must equal the frame sent"
    );
}

/// R3 — the asymmetric direction that already worked, pinned so the fix cannot
/// break it.
///
/// Reader 4 MiB, policy 64 MiB, a WAL well past 4 MiB: the pull advertises the
/// reader's bound, the server truncates to it, every reply decodes under the
/// reader, and successive prefixes deliver everything.
#[tokio::test]
async fn recovery_v5_pull_advertises_the_reader_bound_and_still_delivers() {
    let server = start_test_server().await; // 64 MiB policy
    let cid = server.db.create_collective("bulk-pull").unwrap();
    seed_bulk_experiences(&server.db, cid, 45);

    let dir_client = tempdir().unwrap();
    let db_client =
        Arc::new(PulseDB::open(dir_client.path().join("client.db"), Config::default()).unwrap());

    let (witness, observed) = PullLimitWitness::new(
        HttpSyncTransport::new(&server.base_url).with_max_response_bytes(TIGHT_READER_BYTES),
    );
    let mut manager = SyncManager::new(
        Arc::clone(&db_client),
        Box::new(witness),
        SyncConfig {
            direction: SyncDirection::PullOnly,
            ..SyncConfig::default()
        },
    )
    .unwrap();

    manager.initial_sync(None).await.unwrap();

    let advertised = observed.reply_limits();
    assert!(!advertised.is_empty());
    assert!(
        advertised
            .iter()
            .all(|limit| *limit == TIGHT_READER_BYTES as u64),
        "the pull advertises min(policy, actual reader), got {advertised:?}"
    );
    assert!(
        advertised.len() > 1,
        "the 4 MiB bound must have truncated the page, so catch-up took several \
         prefixes; got {} request(s)",
        advertised.len()
    );
    assert!(
        observed.largest_reply_frame() <= TIGHT_READER_BYTES,
        "every reply must fit the reader that has to read it, largest was {}",
        observed.largest_reply_frame()
    );
    assert_eq!(
        db_client.list_experiences(cid, 100, 0).unwrap().len(),
        45,
        "the prefixes continue to eventual delivery"
    );
}

/// R6 — the real varint width boundary.
///
/// 4 MiB and 64 MiB encode in the same four varint bytes, so that pair proves
/// nothing about width. 16 383 → 16 384 is the 2 → 3-byte step, and both are
/// legal budgets above the control minimum: the sizing envelope and the frame
/// actually built must move together across it.
#[test]
fn recovery_v5_reply_limit_varint_width_boundary_is_measured_not_assumed() {
    let target = InstanceId::new();
    let frame_at = |limit: u64| {
        let mut request = pull_request(target, 0, 100);
        request.reply_limit_bytes = limit;
        wire::encoded_len(&request).unwrap()
    };

    // The boundary the recovery's own sizing relationship rests on.
    assert_eq!(
        frame_at(16_384) - frame_at(16_383),
        wire::varint_len(16_384) - wire::varint_len(16_383),
        "the frame must widen by exactly the varint step at 16 383 → 16 384"
    );
    assert_eq!(
        frame_at(16_384) - frame_at(16_383),
        1,
        "and that step is one byte, which is what makes it a boundary"
    );

    // The pair the earlier analysis reached for proves nothing: same width.
    assert_eq!(
        frame_at(4 * 1024 * 1024),
        frame_at(64 * 1024 * 1024),
        "4 MiB and 64 MiB are the SAME postcard width — this pair cannot \
         demonstrate the invariant"
    );
}

/// R5 (outbound half) — a direct caller's budget is genuinely enforced, before
/// anything is transmitted.
#[tokio::test]
async fn recovery_v5_direct_call_over_budget_is_request_too_large_before_transmission() {
    let server = start_test_server().await;
    let transport = HttpSyncTransport::new(&server.base_url);
    let target = server.db.instance_id();

    let before = snapshot(&server.db);
    let request = pull_request(target, 0, 100);
    let needed = wire::encoded_len(&request).unwrap();
    let cap = needed - 1; // one byte under, so nothing about the shape changes

    let err = transport
        .pull_changes(request, cap)
        .await
        .expect_err("a request over its own send budget must not be sent");
    match err {
        SyncError::RequestTooLarge {
            operation,
            needed: n,
            cap: c,
        } => {
            assert_eq!(operation, WireOperation::Pull);
            assert_eq!(n, needed as u64);
            assert_eq!(c, cap as u64);
        }
        other => panic!("expected the typed RequestTooLarge, got {other}"),
    }
    assert!(
        !SyncError::RequestTooLarge {
            operation: WireOperation::Pull,
            needed: needed as u64,
            cap: cap as u64,
        }
        .is_payload_too_large(),
        "an outbound budget failure is not the inbound variant"
    );
    assert_eq!(
        snapshot(&server.db),
        before,
        "nothing reached the peer, so nothing moved there"
    );

    // The same request WITH an adequate budget still goes through: the budget
    // is a bound, not a blanket refusal.
    transport
        .pull_changes(pull_request(target, 0, 100), needed)
        .await
        .expect("exactly-fitting is fitting")
        .into_result(target)
        .unwrap();
}

/// R5 (inbound half) — an oversized incoming REPLY keeps the inbound error.
///
/// The outbound mapping must not reclassify a body that arrived from the wire:
/// there the fault is the sender's, and `PayloadTooLarge` is what a consumer
/// maps to 413.
#[tokio::test]
async fn recovery_v5_oversized_reply_stays_payload_too_large() {
    let server = start_test_server().await;
    let cid = server.db.create_collective("oversized-reply").unwrap();
    for _ in 0..40 {
        server.db.record_experience(minimal_exp(cid)).unwrap();
    }

    // A reader far too small for the page the server will build, but large
    // enough that the REQUEST itself encodes comfortably.
    let transport = HttpSyncTransport::new(&server.base_url)
        .with_max_response_bytes(pulsedb::sync::MIN_CONTROL_FRAME_BYTES);
    let target = server.db.instance_id();
    let mut request = pull_request(target, 0, 500);
    request.reply_limit_bytes = 64 * 1024 * 1024; // ask for more than we can read

    let err = transport
        .pull_changes(request, DIRECT_SEND_BUDGET)
        .await
        .expect_err("a reply past the reader must be refused");
    assert!(
        err.is_payload_too_large(),
        "an inbound over-limit body keeps its own variant, got {err}"
    );
    assert!(
        !err.is_request_too_large(),
        "and it is NOT reclassified as an outbound failure, got {err}"
    );
}

// ============================================================================
// F1 / R8 — the pull's outbound budget is the BOUND PEER's advertised inbound
// limit, never this client's own response reader.
//
// `min(policy, peer inbound)` and `min(policy, transport.receive_limit_bytes())`
// are the same number whenever the transport reads exactly what the peer
// accepts — which is true of every in-process server-backed transport in this
// suite, so no test there can tell the two apart. Over real HTTP they are
// independent: `max_response_bytes` is what this client will READ, and the
// peer's cap arrives in the handshake. Separating them is what makes the pull
// budget's SOURCE observable.
// ============================================================================

/// The peer's inbound cap: the certified control minimum, the smallest budget a
/// conforming v5 server may advertise.
const PEER_INBOUND_LIMIT: usize = pulsedb::sync::MIN_CONTROL_FRAME_BYTES;

/// This client's response reader — eight times the peer's cap, so a real frame
/// can sit strictly between the two.
const CLIENT_READER_BYTES: usize = 8 * 1024;

/// A `SyncServer` behind real HTTP that counts what actually arrived on each
/// route, so "nothing was transmitted" is read off the peer rather than
/// inferred from the sender.
#[derive(Clone)]
struct CountingState {
    server: Arc<SyncServer>,
    handshakes: Arc<std::sync::atomic::AtomicUsize>,
    pulls: Arc<std::sync::atomic::AtomicUsize>,
}

async fn counting_health(State(st): State<CountingState>) -> StatusCode {
    match st.server.handle_health() {
        Ok(()) => StatusCode::OK,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

async fn counting_handshake(
    State(st): State<CountingState>,
    body: Bytes,
) -> Result<Vec<u8>, StatusCode> {
    st.handshakes
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    st.server.handle_handshake_bytes(&body).map_err(status_for)
}

async fn counting_push(
    State(st): State<CountingState>,
    body: Bytes,
) -> Result<Vec<u8>, StatusCode> {
    st.server.handle_push_bytes(&body).map_err(status_for)
}

async fn counting_pull(
    State(st): State<CountingState>,
    body: Bytes,
) -> Result<Vec<u8>, StatusCode> {
    st.pulls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    st.server.handle_pull_bytes(&body).map_err(status_for)
}

struct CountingServer {
    base_url: String,
    db: Arc<PulseDB>,
    state: CountingState,
    _dir: tempfile::TempDir,
}

impl CountingServer {
    fn handshakes(&self) -> usize {
        self.state
            .handshakes
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    fn pulls(&self) -> usize {
        self.state.pulls.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// Starts a counting server whose own policy — and therefore the inbound limit
/// it advertises at handshake — is `max_request_bytes`.
async fn start_counting_server(max_request_bytes: usize) -> CountingServer {
    let dir = tempdir().unwrap();
    let db = Arc::new(PulseDB::open(dir.path().join("server.db"), Config::default()).unwrap());
    let server = Arc::new(
        SyncServer::new(
            Arc::clone(&db),
            SyncConfig {
                max_request_bytes,
                ..SyncConfig::default()
            },
        )
        .unwrap(),
    );
    let state = CountingState {
        server,
        handshakes: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        pulls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new()
        .route("/sync/health", get(counting_health))
        .route("/sync/handshake", post(counting_handshake))
        .route("/sync/push", post(counting_push))
        .route("/sync/pull", post(counting_pull))
        .layer(axum::extract::DefaultBodyLimit::max(ADAPTER_BODY_LIMIT))
        .with_state(state.clone());
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    CountingServer {
        base_url,
        db,
        state,
        _dir: dir,
    }
}

/// The exact `PullRequest` a `SyncManager` builds for `peer` from a cold
/// cursor. Every field contributes bytes, so the boundary can only be measured
/// on the real shape.
fn manager_pull_request(
    local: InstanceId,
    peer: InstanceId,
    config: &SyncConfig,
    reply_limit_bytes: u64,
    collectives: &[CollectiveId],
) -> PullRequest {
    PullRequest {
        protocol_version: SYNC_PROTOCOL_VERSION,
        source_instance: local,
        target_instance: peer,
        cursor: SyncPosition::new(peer, 0),
        batch_size: config.batch_size as u64,
        reply_limit_bytes,
        collectives: Some(collectives.to_vec()),
    }
}

/// Grows a collective filter one id at a time until the real frame crosses
/// `cap`, and returns the first filter that does NOT fit. Nothing about the id
/// count is assumed — it is whatever the encoder actually produces.
fn filter_that_crosses(
    local: InstanceId,
    peer: InstanceId,
    config: &SyncConfig,
    reply_limit_bytes: u64,
    cap: usize,
) -> Vec<CollectiveId> {
    let mut ids: Vec<CollectiveId> = Vec::new();
    loop {
        ids.push(CollectiveId::new());
        let frame = wire::encoded_len(&manager_pull_request(
            local,
            peer,
            config,
            reply_limit_bytes,
            &ids,
        ))
        .unwrap();
        if frame > cap {
            return ids;
        }
        assert!(
            ids.len() < 4096,
            "the filter never crossed the {cap}-byte cap; the measurement is wrong"
        );
    }
}

/// F1 / R8 over real HTTP — the pull send budget's SOURCE, pinned.
///
/// Three budgets, all different: the peer reads 1 KiB, this client reads 8 KiB,
/// this client's policy is 64 MiB. A supported `collectives` filter builds a
/// pull frame strictly between the peer's cap and this client's reader — a body
/// the reader would take and the peer would refuse.
///
/// The outbound budget must therefore be `min(policy, the bound peer's inbound
/// limit)`. Sourcing it from `transport.receive_limit_bytes()` instead would
/// encode the frame against 8 KiB, let it through, and ship a body the peer
/// cannot read — which the in-process suites cannot see, because there the
/// transport's reader IS the peer's cap.
#[tokio::test]
async fn recovery_v5_pull_encodes_against_the_peer_budget_not_the_response_reader() {
    let server = start_counting_server(PEER_INBOUND_LIMIT).await;
    let peer = server.db.instance_id();

    let dir_client = tempdir().unwrap();
    let db_client =
        Arc::new(PulseDB::open(dir_client.path().join("client.db"), Config::default()).unwrap());

    let config = SyncConfig {
        direction: SyncDirection::PullOnly,
        ..SyncConfig::default()
    };
    assert!(
        config.max_request_bytes > CLIENT_READER_BYTES,
        "local policy must be the LOOSEST of the three, or it is the binding constraint"
    );

    // What the manager will advertise as its inbound reply budget:
    // min(local policy, the reader that has to read the answer).
    let reply_limit = config.max_request_bytes.min(CLIENT_READER_BYTES) as u64;

    let filter = filter_that_crosses(
        db_client.instance_id(),
        peer,
        &config,
        reply_limit,
        PEER_INBOUND_LIMIT,
    );
    let frame = wire::encoded_len(&manager_pull_request(
        db_client.instance_id(),
        peer,
        &config,
        reply_limit,
        &filter,
    ))
    .unwrap();
    assert!(
        frame > PEER_INBOUND_LIMIT,
        "the frame must actually cross the peer's cap; got {frame} bytes"
    );
    assert!(
        frame < CLIENT_READER_BYTES,
        "and must fit THIS client's reader, or the two budgets are not separated \
         and the test cannot tell which one was used; got {frame} bytes"
    );

    let transport =
        HttpSyncTransport::new(&server.base_url).with_max_response_bytes(CLIENT_READER_BYTES);
    assert_eq!(
        transport.receive_limit_bytes(),
        CLIENT_READER_BYTES,
        "the reader is set independently of the peer's cap — that is the whole separation"
    );

    let mut manager = SyncManager::new(
        Arc::clone(&db_client),
        Box::new(transport),
        SyncConfig {
            collectives: Some(filter),
            ..config.clone()
        },
    )
    .unwrap();

    let err = manager
        .sync_once()
        .await
        .expect_err("a pull the BOUND PEER cannot read must not be sent");
    match err {
        SyncError::RequestTooLarge {
            operation,
            needed,
            cap,
        } => {
            assert_eq!(operation, WireOperation::Pull);
            assert_eq!(
                needed, frame as u64,
                "the measured frame is what was refused"
            );
            assert_eq!(
                cap, PEER_INBOUND_LIMIT as u64,
                "the pull encodes against the peer's advertised inbound limit; this \
                 client's {CLIENT_READER_BYTES}-byte response reader would have let \
                 the frame through"
            );
        }
        other => panic!("expected the typed RequestTooLarge, got {other}"),
    }

    assert!(
        server.handshakes() >= 1,
        "the binding was established, so the peer's advertised budget was known"
    );
    assert_eq!(
        server.pulls(),
        0,
        "no pull crossed to the peer — the refusal is local, before transmission"
    );
}
