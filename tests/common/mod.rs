//! Shared helpers for the integration-test binaries under `tests/`.
//!
//! Each `tests/*.rs` file is its own crate; a file opts in with `mod common;`.
//! Helpers that only some binaries use are expected to be dead code in the
//! others, hence the crate-level allow.

#![allow(dead_code)]

use std::path::PathBuf;

/// The committed golden-fixture directory (`tests/fixtures`).
pub fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// Copy the committed fixture to a fresh temp path and return the copy.
///
/// The on-open migration is destructive/in-place (redb's v2→v3 `upgrade()`
/// rewrites the file), so a test must never open the checked-in blob itself.
/// The returned `TempDir` owns the copy; keep it alive for as long as the
/// store is in use.
pub fn copy_fixture(name: &str) -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let dst = dir.path().join(name);
    std::fs::copy(fixtures_dir().join(name), &dst).unwrap_or_else(|e| panic!("copy {name}: {e}"));
    (dir, dst)
}

/// A sync endpoint a test can operate on: the [`SyncServer`] currently behind
/// the address, plus counters for what reached it.
///
/// The server is swappable, which is how a test stands an endpoint up again as
/// a **restored copy**: a different store, a different identity, and a
/// different WAL behind the same address. `swap_after_pull` arms a replacement
/// that lands the moment a pull has been answered — the narrow window in which
/// a cycle has confirmed the peer and is about to push to it.
#[cfg(feature = "sync")]
pub struct SyncEndpoint {
    server: std::sync::Mutex<std::sync::Arc<pulsedb::sync::server::SyncServer>>,
    swap_after_pull: std::sync::Mutex<Option<std::sync::Arc<pulsedb::sync::server::SyncServer>>>,
    handshakes: std::sync::atomic::AtomicUsize,
    pulls: std::sync::atomic::AtomicUsize,
    pushes: std::sync::atomic::AtomicUsize,
}

#[cfg(feature = "sync")]
impl SyncEndpoint {
    pub fn new(server: std::sync::Arc<pulsedb::sync::server::SyncServer>) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            server: std::sync::Mutex::new(server),
            swap_after_pull: std::sync::Mutex::new(None),
            handshakes: std::sync::atomic::AtomicUsize::new(0),
            pulls: std::sync::atomic::AtomicUsize::new(0),
            pushes: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    /// Replaces the endpoint behind this address now.
    pub fn replace(&self, server: std::sync::Arc<pulsedb::sync::server::SyncServer>) {
        *self.server.lock().unwrap() = server;
    }

    /// Arms a replacement that lands as soon as the next pull has been answered.
    pub fn replace_after_next_pull(
        &self,
        server: std::sync::Arc<pulsedb::sync::server::SyncServer>,
    ) {
        *self.swap_after_pull.lock().unwrap() = Some(server);
    }

    /// The identity the endpoint currently answers as.
    pub fn instance_id(&self) -> pulsedb::sync::types::InstanceId {
        self.server.lock().unwrap().instance_id()
    }

    pub fn handshakes(&self) -> usize {
        self.handshakes.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn pulls(&self) -> usize {
        self.pulls.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn pushes(&self) -> usize {
        self.pushes.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn server(&self) -> std::sync::Arc<pulsedb::sync::server::SyncServer> {
        std::sync::Arc::clone(&self.server.lock().unwrap())
    }
}

/// A [`SyncTransport`] backed by a real [`SyncServer`] over the real frame
/// codec, in process.
///
/// This is the adapter the engine and identity suites sync through. It is not a
/// convenience: an in-memory double that hands structs to the other side proves
/// nothing about **applying** a change, because nothing applies it, and it
/// proves nothing about the wire, because nothing is encoded. Every call here
/// frames the request exactly as HTTP would, hands the bytes to the server's own
/// byte handler, and decodes the framed reply — so a test that passes on this
/// adapter is a test the same exchange passes over HTTP.
///
/// Directions are kept apart the way the HTTP transport keeps them: the request
/// is encoded against the caller's `send_budget_bytes` (an over-budget one is
/// `RequestTooLarge`, raised before the server is handed anything), and the
/// reply is read under this client's own receive limit.
#[cfg(feature = "sync")]
pub struct ServerBackedTransport {
    endpoint: std::sync::Arc<SyncEndpoint>,
    receive_limit_bytes: usize,
}

/// Reclassifies an outbound over-budget encode, exactly as the HTTP transport
/// does, so a test on this adapter sees the same typed failure.
#[cfg(feature = "sync")]
fn encode_request<T: serde::Serialize>(
    operation: pulsedb::sync::wire::WireOperation,
    value: &T,
    send_budget_bytes: usize,
) -> Result<Vec<u8>, pulsedb::sync::SyncError> {
    use pulsedb::sync::SyncError;
    pulsedb::sync::wire::encode_bounded(operation, value, send_budget_bytes).map_err(|e| match e {
        SyncError::PayloadTooLarge { size, max } => SyncError::RequestTooLarge {
            operation,
            needed: size as u64,
            cap: max as u64,
        },
        other => other,
    })
}

#[cfg(feature = "sync")]
impl ServerBackedTransport {
    /// A transport over a fresh endpoint wrapping `server`.
    pub fn new(server: std::sync::Arc<pulsedb::sync::server::SyncServer>) -> Self {
        Self::over(SyncEndpoint::new(server))
    }

    /// A transport over an endpoint the test also holds.
    pub fn over(endpoint: std::sync::Arc<SyncEndpoint>) -> Self {
        let receive_limit_bytes = endpoint.server().receive_limit_bytes();
        Self {
            endpoint,
            receive_limit_bytes,
        }
    }

    /// The endpoint this transport talks to.
    pub fn endpoint(&self) -> std::sync::Arc<SyncEndpoint> {
        std::sync::Arc::clone(&self.endpoint)
    }
}

#[cfg(feature = "sync")]
#[async_trait::async_trait]
impl pulsedb::sync::transport::SyncTransport for ServerBackedTransport {
    async fn handshake(
        &self,
        request: pulsedb::sync::types::HandshakeRequest,
        send_budget_bytes: usize,
    ) -> Result<pulsedb::sync::types::HandshakeResponse, pulsedb::sync::SyncError> {
        use pulsedb::sync::wire::{self, WireOperation};
        // Counted only once the request actually leaves: an over-budget encode
        // is refused locally, and a test asserting "no round trip happened"
        // must be able to see that.
        let body = encode_request(WireOperation::Handshake, &request, send_budget_bytes)?;
        self.endpoint
            .handshakes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let reply = self.endpoint.server().handle_handshake_bytes(&body)?;
        wire::decode_bounded(WireOperation::Handshake, &reply, self.receive_limit_bytes)
    }

    async fn push_changes(
        &self,
        request: pulsedb::sync::types::PushRequest,
        send_budget_bytes: usize,
    ) -> Result<
        pulsedb::sync::types::WireReply<pulsedb::sync::types::PushAck>,
        pulsedb::sync::SyncError,
    > {
        use pulsedb::sync::wire::{self, WireOperation};
        // Counted only once the request actually leaves: an over-budget encode
        // is refused locally, and a test asserting "no round trip happened"
        // must be able to see that.
        let body = encode_request(WireOperation::Push, &request, send_budget_bytes)?;
        self.endpoint
            .pushes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let reply = self.endpoint.server().handle_push_bytes(&body)?;
        wire::decode_bounded(WireOperation::Push, &reply, self.receive_limit_bytes)
    }

    async fn pull_changes(
        &self,
        request: pulsedb::sync::types::PullRequest,
        send_budget_bytes: usize,
    ) -> Result<
        pulsedb::sync::types::WireReply<pulsedb::sync::types::PullPage>,
        pulsedb::sync::SyncError,
    > {
        use pulsedb::sync::wire::{self, WireOperation};
        // Counted only once the request actually leaves: an over-budget encode
        // is refused locally, and a test asserting "no round trip happened"
        // must be able to see that.
        let body = encode_request(WireOperation::Pull, &request, send_budget_bytes)?;
        self.endpoint
            .pulls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let reply = self.endpoint.server().handle_pull_bytes(&body)?;
        let decoded = wire::decode_bounded(WireOperation::Pull, &reply, self.receive_limit_bytes);
        // The replacement lands AFTER the pull is answered: the cycle has now
        // confirmed a peer that is already gone.
        if let Some(replacement) = self.endpoint.swap_after_pull.lock().unwrap().take() {
            self.endpoint.replace(replacement);
        }
        decoded
    }

    async fn health_check(&self) -> Result<(), pulsedb::sync::SyncError> {
        self.endpoint.server().handle_health()
    }

    fn receive_limit_bytes(&self) -> usize {
        self.receive_limit_bytes
    }
}

/// Builds a [`SyncServer`] over `db` with the default configuration.
#[cfg(feature = "sync")]
pub fn server_for(
    db: &std::sync::Arc<pulsedb::PulseDB>,
) -> std::sync::Arc<pulsedb::sync::server::SyncServer> {
    server_for_with(db, pulsedb::sync::config::SyncConfig::default())
}

/// Builds a [`SyncServer`] over `db` with `config`.
#[cfg(feature = "sync")]
pub fn server_for_with(
    db: &std::sync::Arc<pulsedb::PulseDB>,
    config: pulsedb::sync::config::SyncConfig,
) -> std::sync::Arc<pulsedb::sync::server::SyncServer> {
    std::sync::Arc::new(
        pulsedb::sync::server::SyncServer::new(std::sync::Arc::clone(db), config)
            .expect("a valid sync configuration"),
    )
}

/// One full bidirectional sync cycle between two stores, each side talking to
/// the other's real [`SyncServer`] over the real frame codec.
///
/// Each direction gets its own manager so the ordering stays explicit — A
/// pushes, B pulls, B pushes, A pulls — which is the sequencing the sync-engine
/// G-counter convergence test established.
///
/// Managers are constructed *inside* this helper, i.e. after any
/// `remint_instance_id` the caller performed: `SyncManager` and `SyncServer`
/// both read the store's identity once, at construction.
#[cfg(feature = "sync")]
pub async fn sync_both_ways(
    db_a: &std::sync::Arc<pulsedb::PulseDB>,
    db_b: &std::sync::Arc<pulsedb::PulseDB>,
) {
    use pulsedb::sync::config::{SyncConfig, SyncDirection};
    use pulsedb::sync::manager::SyncManager;
    use std::sync::Arc;

    let cfg = |direction| SyncConfig {
        direction,
        batch_size: 250,
        ..Default::default()
    };
    let server_a = server_for(db_a);
    let server_b = server_for(db_b);

    let mut mgr_a_push = SyncManager::new(
        Arc::clone(db_a),
        Box::new(ServerBackedTransport::new(Arc::clone(&server_b))),
        cfg(SyncDirection::PushOnly),
    )
    .unwrap();
    let mut mgr_b_pull = SyncManager::new(
        Arc::clone(db_b),
        Box::new(ServerBackedTransport::new(Arc::clone(&server_a))),
        cfg(SyncDirection::PullOnly),
    )
    .unwrap();
    let mut mgr_b_push = SyncManager::new(
        Arc::clone(db_b),
        Box::new(ServerBackedTransport::new(Arc::clone(&server_a))),
        cfg(SyncDirection::PushOnly),
    )
    .unwrap();
    let mut mgr_a_pull = SyncManager::new(
        Arc::clone(db_a),
        Box::new(ServerBackedTransport::new(server_b)),
        cfg(SyncDirection::PullOnly),
    )
    .unwrap();

    mgr_a_push.sync_once().await.unwrap();
    mgr_b_pull.sync_once().await.unwrap();
    mgr_b_push.sync_once().await.unwrap();
    mgr_a_pull.sync_once().await.unwrap();
}
