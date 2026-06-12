//! Sync manager — orchestrates sync lifecycle between PulseDB instances.
//!
//! [`SyncManager`] is the public API for sync. It manages:
//! - Handshake negotiation with remote peer
//! - Background push/pull loops on configured intervals
//! - Manual one-shot sync via [`sync_once()`](SyncManager::sync_once)
//! - Initial catchup sync with progress callback
//! - Error recovery with exponential backoff
//! - Graceful shutdown

use std::sync::{Arc, RwLock};

use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, instrument, warn};

use crate::db::PulseDB;

use super::applier::RemoteChangeApplier;
use super::config::{SyncConfig, SyncDirection};
use super::error::SyncError;
use super::progress::SyncProgressCallback;
use super::pusher::LocalChangePusher;
use super::transport::SyncTransport;
use super::types::{HandshakeRequest, InstanceId, PullRequest, SyncCursor, SyncStatus};
use super::{SYNC_CAPABILITY_GCOUNTER_APPLICATIONS, SYNC_PROTOCOL_VERSION};

/// Orchestrator for sync operations between two PulseDB instances.
///
/// The SyncManager is a **sidecar** — it holds `Arc<PulseDB>` but doesn't
/// wrap it. Local database operations are completely unaffected by sync state.
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use pulsedb::{PulseDB, Config};
/// use pulsedb::sync::{SyncManager, SyncConfig, InMemorySyncTransport};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let db = Arc::new(PulseDB::open("my.db", Config::default())?);
/// let (local_transport, _remote) = InMemorySyncTransport::new_pair();
/// let mut manager = SyncManager::new(db, Box::new(local_transport), SyncConfig::default());
/// manager.start().await?;
/// // ... sync runs in background ...
/// manager.stop().await?;
/// # Ok(())
/// # }
/// ```
pub struct SyncManager {
    db: Arc<PulseDB>,
    transport: Arc<dyn SyncTransport>,
    config: SyncConfig,
    local_instance_id: InstanceId,
    peer_instance_id: Option<InstanceId>,
    status: Arc<RwLock<SyncStatus>>,
    shutdown: Arc<Notify>,
    task_handle: Option<JoinHandle<()>>,
}

impl SyncManager {
    /// Creates a new SyncManager.
    ///
    /// Does NOT start sync — call [`start()`](Self::start) or
    /// [`sync_once()`](Self::sync_once) to begin.
    pub fn new(db: Arc<PulseDB>, transport: Box<dyn SyncTransport>, config: SyncConfig) -> Self {
        let local_instance_id = db.storage_for_test().instance_id();
        Self {
            db,
            transport: Arc::from(transport),
            config,
            local_instance_id,
            peer_instance_id: None,
            status: Arc::new(RwLock::new(SyncStatus::Idle)),
            shutdown: Arc::new(Notify::new()),
            task_handle: None,
        }
    }

    /// Starts the background sync loop.
    ///
    /// Performs a handshake with the remote peer, then spawns a background
    /// tokio task that pushes and pulls on the configured intervals.
    #[instrument(skip(self), fields(instance_id = %self.local_instance_id))]
    pub async fn start(&mut self) -> Result<(), SyncError> {
        if self.task_handle.is_some() {
            return Err(SyncError::transport("SyncManager already started"));
        }

        // Perform handshake
        let peer_id = self.perform_handshake().await?;
        self.peer_instance_id = Some(peer_id);

        self.set_status(SyncStatus::Syncing);

        // Clone everything needed for the background task
        let db = Arc::clone(&self.db);
        let transport = Arc::clone(&self.transport);
        let config = self.config.clone();
        let local_id = self.local_instance_id;
        let status = Arc::clone(&self.status);
        let shutdown = Arc::clone(&self.shutdown);

        let handle = tokio::spawn(async move {
            Self::background_loop(db, transport, config, local_id, peer_id, status, shutdown).await;
        });

        self.task_handle = Some(handle);
        info!("SyncManager started");
        Ok(())
    }

    /// Stops the background sync loop.
    #[instrument(skip(self))]
    pub async fn stop(&mut self) -> Result<(), SyncError> {
        if let Some(handle) = self.task_handle.take() {
            self.shutdown.notify_one();
            handle
                .await
                .map_err(|e| SyncError::transport(format!("Background task panicked: {}", e)))?;
            self.set_status(SyncStatus::Idle);
            info!("SyncManager stopped");
        }
        Ok(())
    }

    /// Performs a single push+pull sync cycle (no background task needed).
    ///
    /// Useful for testing or manual sync triggers.
    #[instrument(skip(self))]
    pub async fn sync_once(&mut self) -> Result<SyncStatus, SyncError> {
        // Handshake if we haven't yet
        if self.peer_instance_id.is_none() {
            let peer_id = self.perform_handshake().await?;
            self.peer_instance_id = Some(peer_id);
        }
        let peer_id = self.peer_instance_id.unwrap();

        self.set_status(SyncStatus::Syncing);

        // Load saved push cursor
        let push_seq = self.load_cursor_sequence(peer_id)?;
        let mut pusher = LocalChangePusher::new(
            Arc::clone(&self.db),
            Arc::clone(&self.transport),
            self.config.clone(),
            self.local_instance_id,
            peer_id,
            push_seq,
        );

        let applier = RemoteChangeApplier::new(Arc::clone(&self.db), self.config.clone());

        // Push if enabled
        let pushed = if matches!(
            self.config.direction,
            SyncDirection::PushOnly | SyncDirection::Bidirectional
        ) {
            pusher.push_pending().await?
        } else {
            0
        };

        // Pull if enabled
        let pulled = if matches!(
            self.config.direction,
            SyncDirection::PullOnly | SyncDirection::Bidirectional
        ) {
            self.pull_and_apply(&applier, peer_id).await?
        } else {
            0
        };

        self.set_status(SyncStatus::Idle);

        debug!(pushed, pulled, "sync_once complete");
        Ok(SyncStatus::Idle)
    }

    /// Performs initial sync — pulls all remote changes in batches.
    ///
    /// Call this before `start()` to catch up from a cold start.
    #[instrument(skip(self, progress))]
    pub async fn initial_sync(
        &mut self,
        progress: Option<Box<dyn SyncProgressCallback>>,
    ) -> Result<(), SyncError> {
        // Handshake if needed
        if self.peer_instance_id.is_none() {
            let peer_id = self.perform_handshake().await?;
            self.peer_instance_id = Some(peer_id);
        }
        let peer_id = self.peer_instance_id.unwrap();

        self.set_status(SyncStatus::Syncing);

        let applier = RemoteChangeApplier::new(Arc::clone(&self.db), self.config.clone());

        let mut total_pulled = 0usize;
        let mut cursor = SyncCursor {
            instance_id: peer_id,
            last_sequence: self.load_cursor_sequence(peer_id)?,
        };

        loop {
            let pull_request = PullRequest {
                cursor: cursor.clone(),
                batch_size: self.config.batch_size,
                collectives: self.config.collectives.clone(),
            };

            let response = self.transport.pull_changes(pull_request).await?;
            let batch_size = response.changes.len();

            if batch_size > 0 {
                applier.apply_batch(response.changes)?;
            }

            total_pulled += batch_size;
            cursor = response.new_cursor;

            // Save cursor after each batch (crash-safe)
            self.save_cursor(&cursor)?;

            if let Some(ref cb) = progress {
                cb.on_progress(batch_size, total_pulled, response.has_more);
            }

            if !response.has_more {
                break;
            }
        }

        self.set_status(SyncStatus::Idle);
        info!(total_pulled, "Initial sync complete");
        Ok(())
    }

    /// Returns the current sync status.
    pub fn status(&self) -> SyncStatus {
        self.status
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    // ─── Internal helpers ────────────────────────────────────────────

    #[instrument(skip(self))]
    async fn perform_handshake(&self) -> Result<InstanceId, SyncError> {
        let request = HandshakeRequest {
            instance_id: self.local_instance_id,
            protocol_version: SYNC_PROTOCOL_VERSION,
            capabilities: vec![
                "push".into(),
                "pull".into(),
                SYNC_CAPABILITY_GCOUNTER_APPLICATIONS.into(),
            ],
        };

        let response = self.transport.handshake(request).await?;

        if !response.accepted {
            return Err(SyncError::handshake(
                response.reason.unwrap_or_else(|| "rejected".into()),
            ));
        }

        if response.protocol_version != SYNC_PROTOCOL_VERSION {
            return Err(SyncError::ProtocolVersion {
                local: SYNC_PROTOCOL_VERSION,
                remote: response.protocol_version,
            });
        }

        debug!(peer = %response.instance_id, "Handshake accepted");
        Ok(response.instance_id)
    }

    fn set_status(&self, status: SyncStatus) {
        if let Ok(mut s) = self.status.write() {
            *s = status;
        }
    }

    fn load_cursor_sequence(&self, peer_id: InstanceId) -> Result<u64, SyncError> {
        self.db
            .storage_for_test()
            .load_sync_cursor(&peer_id)
            .map_err(|e| SyncError::transport(format!("Failed to load cursor: {}", e)))
            .map(|opt| opt.map_or(0, |c| c.last_sequence))
    }

    fn save_cursor(&self, cursor: &SyncCursor) -> Result<(), SyncError> {
        self.db
            .storage_for_test()
            .save_sync_cursor(cursor)
            .map_err(|e| SyncError::transport(format!("Failed to save cursor: {}", e)))
    }

    /// Pull changes from remote and apply them locally.
    async fn pull_and_apply(
        &self,
        applier: &RemoteChangeApplier,
        peer_id: InstanceId,
    ) -> Result<usize, SyncError> {
        let cursor_seq = self.load_cursor_sequence(peer_id)?;
        let pull_request = PullRequest {
            cursor: SyncCursor {
                instance_id: peer_id,
                last_sequence: cursor_seq,
            },
            batch_size: self.config.batch_size,
            collectives: self.config.collectives.clone(),
        };

        let response = self.transport.pull_changes(pull_request).await?;
        let count = response.changes.len();

        if count > 0 {
            applier.apply_batch(response.changes)?;
            self.save_cursor(&response.new_cursor)?;
        }

        Ok(count)
    }

    /// Background loop that runs push+pull on configured intervals.
    async fn background_loop(
        db: Arc<PulseDB>,
        transport: Arc<dyn SyncTransport>,
        config: SyncConfig,
        local_id: InstanceId,
        peer_id: InstanceId,
        status: Arc<RwLock<SyncStatus>>,
        shutdown: Arc<Notify>,
    ) {
        let interval_ms = std::cmp::max(config.push_interval_ms, config.pull_interval_ms);
        let interval = tokio::time::Duration::from_millis(interval_ms);

        let mut consecutive_failures = 0u32;
        let max_retries = config.retry.max_retries;
        let initial_backoff = config.retry.initial_backoff_ms;
        let max_backoff = config.retry.max_backoff_ms;
        let multiplier = config.retry.backoff_multiplier;

        loop {
            let sleep_duration = if consecutive_failures > 0 {
                // Exponential backoff
                let backoff = (initial_backoff as f64)
                    * multiplier.powi(consecutive_failures.saturating_sub(1) as i32);
                let backoff_ms = (backoff as u64).min(max_backoff);
                tokio::time::Duration::from_millis(backoff_ms)
            } else {
                interval
            };

            tokio::select! {
                _ = shutdown.notified() => {
                    debug!("Sync background loop shutting down");
                    break;
                }
                _ = tokio::time::sleep(sleep_duration) => {
                    // Build push cursor from saved state
                    let push_seq = db
                        .storage_for_test()
                        .load_sync_cursor(&peer_id)
                        .unwrap_or(None)
                        .map_or(0, |c| c.last_sequence);

                    let mut pusher = LocalChangePusher::new(
                        Arc::clone(&db),
                        Arc::clone(&transport),
                        config.clone(),
                        local_id,
                        peer_id,
                        push_seq,
                    );
                    let applier = RemoteChangeApplier::new(Arc::clone(&db), config.clone());

                    let result = Self::run_sync_cycle(&mut pusher, &applier, &transport, &db, &config, peer_id).await;

                    match result {
                        Ok(_) => {
                            if consecutive_failures > 0 {
                                info!("Sync recovered after {} failures", consecutive_failures);
                            }
                            consecutive_failures = 0;
                            if let Ok(mut s) = status.write() {
                                *s = SyncStatus::Syncing;
                            }
                        }
                        Err(e) => {
                            consecutive_failures += 1;
                            if consecutive_failures > max_retries {
                                warn!(
                                    failures = consecutive_failures,
                                    "Sync errors exceed max_retries, continuing with backoff"
                                );
                            }
                            error!("Sync cycle failed: {}", e);
                            if let Ok(mut s) = status.write() {
                                *s = SyncStatus::Error(e.to_string());
                            }
                        }
                    }
                }
            }
        }

        if let Ok(mut s) = status.write() {
            *s = SyncStatus::Idle;
        }
    }

    /// Execute one push+pull cycle. Used by background loop.
    async fn run_sync_cycle(
        pusher: &mut LocalChangePusher,
        applier: &RemoteChangeApplier,
        transport: &Arc<dyn SyncTransport>,
        db: &Arc<PulseDB>,
        config: &SyncConfig,
        peer_id: InstanceId,
    ) -> Result<(), SyncError> {
        // Push
        if matches!(
            config.direction,
            SyncDirection::PushOnly | SyncDirection::Bidirectional
        ) {
            pusher.push_pending().await?;
        }

        // Pull
        if matches!(
            config.direction,
            SyncDirection::PullOnly | SyncDirection::Bidirectional
        ) {
            let cursor_seq = db
                .storage_for_test()
                .load_sync_cursor(&peer_id)
                .map_err(|e| SyncError::transport(format!("cursor load: {}", e)))?
                .map_or(0, |c| c.last_sequence);

            let pull_request = PullRequest {
                cursor: SyncCursor {
                    instance_id: peer_id,
                    last_sequence: cursor_seq,
                },
                batch_size: config.batch_size,
                collectives: config.collectives.clone(),
            };

            let response = transport.pull_changes(pull_request).await?;
            let count = response.changes.len();

            if count > 0 {
                applier.apply_batch(response.changes)?;
                let cursor = response.new_cursor;
                db.storage_for_test()
                    .save_sync_cursor(&cursor)
                    .map_err(|e| SyncError::transport(format!("cursor save: {}", e)))?;
            }
        }

        Ok(())
    }
}
