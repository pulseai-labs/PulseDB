//! Integration tests for Phase 4: HTTP Sync Transport.
//!
//! Spins up a real Axum server with SyncServer handlers, then tests
//! HttpSyncTransport against it.

#![cfg(feature = "sync-http")]

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::Router;
use tokio::net::TcpListener;

use pulsedb::sync::config::{SyncConfig, SyncDirection};
use pulsedb::sync::guard::SyncApplyGuard;
use pulsedb::sync::manager::SyncManager;
use pulsedb::sync::server::SyncServer;
use pulsedb::sync::transport::SyncTransport;
use pulsedb::sync::transport_http::HttpSyncTransport;
use pulsedb::sync::types::{HandshakeRequest, InstanceId, PullRequest, SyncCursor};
use pulsedb::sync::SYNC_PROTOCOL_VERSION;
use pulsedb::{CollectiveId, Config, NewExperience, PulseDB};
use tempfile::tempdir;

// ============================================================================
// Axum handlers (test server)
// ============================================================================

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
    server
        .handle_handshake_bytes(&body)
        .map_err(|_| StatusCode::BAD_REQUEST)
}

async fn handle_push(
    State(server): State<Arc<SyncServer>>,
    body: Bytes,
) -> Result<Vec<u8>, StatusCode> {
    server
        .handle_push_bytes(&body)
        .map_err(|_| StatusCode::BAD_REQUEST)
}

async fn handle_pull(
    State(server): State<Arc<SyncServer>>,
    body: Bytes,
) -> Result<Vec<u8>, StatusCode> {
    server
        .handle_pull_bytes(&body)
        .map_err(|_| StatusCode::BAD_REQUEST)
}

fn sync_router(server: Arc<SyncServer>) -> Router {
    Router::new()
        .route("/sync/health", get(handle_health))
        .route("/sync/handshake", post(handle_handshake))
        .route("/sync/push", post(handle_push))
        .route("/sync/pull", post(handle_pull))
        .with_state(server)
}

// ============================================================================
// Test helpers
// ============================================================================

struct TestServer {
    base_url: String,
    db: Arc<PulseDB>,
    _dir: tempfile::TempDir,
}

async fn start_test_server() -> TestServer {
    let dir = tempdir().unwrap();
    let db = Arc::new(PulseDB::open(dir.path().join("server.db"), Config::default()).unwrap());
    let server = Arc::new(SyncServer::new(Arc::clone(&db), SyncConfig::default()));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{}", addr);

    tokio::spawn(async move {
        axum::serve(listener, sync_router(server)).await.unwrap();
    });

    // Give server a moment to start
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    TestServer {
        base_url,
        db,
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

    let response = transport.handshake(request).await.unwrap();
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
    let pull_request = PullRequest {
        cursor: SyncCursor::new(InstanceId::new()),
        batch_size: 100,
        collectives: None,
    };
    let pull_response = transport.pull_changes(pull_request).await.unwrap();

    // Should have collective + experience
    assert!(
        !pull_response.changes.is_empty(),
        "Should have changes to pull"
    );
    assert!(pull_response.changes.len() >= 2); // collective + experience at minimum
}

#[tokio::test]
async fn test_http_full_sync_via_manager() {
    let server = start_test_server().await;
    let dir_client = tempdir().unwrap();
    let db_client =
        Arc::new(PulseDB::open(dir_client.path().join("client.db"), Config::default()).unwrap());

    let transport = HttpSyncTransport::new(&server.base_url);
    let config = SyncConfig::default();
    let mut manager = SyncManager::new(Arc::clone(&db_client), Box::new(transport), config);

    // Create data on server
    let cid = server.db.create_collective("full-sync").unwrap();
    let exp_id = server.db.record_experience(minimal_exp(cid)).unwrap();

    // Client does initial sync to pull all server data
    manager.initial_sync(None).await.unwrap();

    // Client should have the collective and experience
    assert!(
        db_client.get_collective(cid).unwrap().is_some(),
        "Collective should sync via HTTP"
    );
    assert!(
        db_client.get_experience(exp_id).unwrap().is_some(),
        "Experience should sync via HTTP"
    );
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
    );

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

#[tokio::test]
async fn test_http_push_to_server() {
    let server = start_test_server().await;
    let dir_client = tempdir().unwrap();
    let db_client =
        Arc::new(PulseDB::open(dir_client.path().join("client.db"), Config::default()).unwrap());

    let transport = HttpSyncTransport::new(&server.base_url);
    let config = SyncConfig {
        direction: SyncDirection::PushOnly,
        ..SyncConfig::default()
    };
    let mut manager = SyncManager::new(Arc::clone(&db_client), Box::new(transport), config);

    // Create data on client
    let cid = db_client.create_collective("push-test").unwrap();
    let exp_id = db_client.record_experience(minimal_exp(cid)).unwrap();

    // Push to server
    manager.sync_once().await.unwrap();

    // Server should have the collective and experience
    assert!(
        server.db.get_collective(cid).unwrap().is_some(),
        "Collective should be pushed to server"
    );
    assert!(
        server.db.get_experience(exp_id).unwrap().is_some(),
        "Experience should be pushed to server"
    );
}
