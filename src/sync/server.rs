//! Server-side sync handler for HTTP consumers.
//!
//! [`SyncServer`] provides framework-agnostic methods for handling sync
//! requests. Consumers wire these into their web framework (Axum, Actix, etc.)
//! without PulseDB taking a dependency on any web framework.
//!
//! # Example (Axum)
//!
//! ```rust,ignore
//! use std::sync::Arc;
//! use axum::{Router, routing::{get, post}, extract::State, body::Bytes, http::StatusCode};
//! use pulsedb::sync::server::SyncServer;
//!
//! async fn handle_health(State(server): State<Arc<SyncServer>>) -> StatusCode {
//!     match server.handle_health() {
//!         Ok(()) => StatusCode::OK,
//!         Err(_) => StatusCode::SERVICE_UNAVAILABLE,
//!     }
//! }
//!
//! async fn handle_handshake(State(server): State<Arc<SyncServer>>, body: Bytes) -> Result<Vec<u8>, StatusCode> {
//!     server.handle_handshake_bytes(&body).map_err(|_| StatusCode::BAD_REQUEST)
//! }
//! ```

use std::sync::Arc;

use tracing::{debug, info, instrument};

use crate::db::PulseDB;
use crate::watch::ChangePoller;

use super::applier::RemoteChangeApplier;
use super::config::SyncConfig;
use super::error::SyncError;
use super::types::{
    HandshakeRequest, HandshakeResponse, InstanceId, PullRequest, PullResponse, PushResponse,
    SyncChange, SyncCursor,
};
use super::SYNC_PROTOCOL_VERSION;

/// Server-side sync handler.
///
/// Processes incoming sync requests from remote peers. Framework-agnostic —
/// consumers create web handlers that delegate to this struct's methods.
///
/// The server manages its own `ChangePoller` for serving pull requests and
/// delegates push handling to `RemoteChangeApplier`.
pub struct SyncServer {
    db: Arc<PulseDB>,
    instance_id: InstanceId,
    config: SyncConfig,
}

impl SyncServer {
    /// Creates a new SyncServer for the given database.
    pub fn new(db: Arc<PulseDB>, config: SyncConfig) -> Self {
        let instance_id = db.storage_for_test().instance_id();
        Self {
            db,
            instance_id,
            config,
        }
    }

    /// Returns the server's instance ID.
    pub fn instance_id(&self) -> InstanceId {
        self.instance_id
    }

    // ─── High-level handlers (typed) ─────────────────────────────────

    /// Handles a handshake request.
    #[instrument(skip(self, request), fields(peer = %request.instance_id))]
    pub fn handle_handshake(
        &self,
        request: HandshakeRequest,
    ) -> Result<HandshakeResponse, SyncError> {
        if request.protocol_version != SYNC_PROTOCOL_VERSION {
            return Ok(HandshakeResponse {
                instance_id: self.instance_id,
                protocol_version: SYNC_PROTOCOL_VERSION,
                accepted: false,
                reason: Some(format!(
                    "Protocol version mismatch: server v{}, client v{}",
                    SYNC_PROTOCOL_VERSION, request.protocol_version
                )),
            });
        }

        info!(peer = %request.instance_id, "Sync handshake accepted");
        Ok(HandshakeResponse {
            instance_id: self.instance_id,
            protocol_version: SYNC_PROTOCOL_VERSION,
            accepted: true,
            reason: None,
        })
    }

    /// Handles a push request — applies remote changes locally.
    #[instrument(skip(self, changes), fields(count = changes.len()))]
    pub fn handle_push(&self, changes: Vec<SyncChange>) -> Result<PushResponse, SyncError> {
        let max_seq = changes.iter().map(|c| c.sequence).max().unwrap_or(0);
        let source = changes
            .first()
            .map(|c| c.source_instance)
            .unwrap_or_else(InstanceId::nil);

        let applier = RemoteChangeApplier::new(Arc::clone(&self.db), self.config.clone());
        let result = applier.apply_batch(changes)?;

        debug!(
            accepted = result.applied,
            skipped = result.skipped,
            conflicts = result.conflicts,
            "Handled push"
        );

        Ok(PushResponse {
            accepted: result.applied,
            rejected: result.skipped,
            new_cursor: SyncCursor {
                instance_id: source,
                last_sequence: max_seq,
            },
        })
    }

    /// Handles a pull request — serves local changes to the remote peer.
    #[instrument(skip(self, request))]
    pub fn handle_pull(&self, request: PullRequest) -> Result<PullResponse, SyncError> {
        let storage = self.db.storage_for_test();
        let mut poller = ChangePoller::from_sequence(request.cursor.last_sequence);

        let events = poller
            .poll_sync_events(storage)
            .map_err(|e| SyncError::transport(format!("Failed to poll WAL for pull: {}", e)))?;

        // Build SyncChanges from WAL events (same logic as pusher)
        let mut changes = Vec::new();
        for (sequence, record) in &events {
            if let Some(change) =
                build_change_from_record(&self.db, *sequence, record, self.instance_id)?
            {
                // Apply collective filter
                if let Some(ref allowed) = request.collectives {
                    if !allowed.contains(&change.collective_id) {
                        continue;
                    }
                }
                changes.push(change);
                if changes.len() >= request.batch_size {
                    break;
                }
            }
        }

        let has_more = events.len() > changes.len();
        let new_seq = changes
            .last()
            .map(|c| c.sequence)
            .unwrap_or(request.cursor.last_sequence);

        Ok(PullResponse {
            changes,
            has_more,
            new_cursor: SyncCursor {
                instance_id: self.instance_id,
                last_sequence: new_seq,
            },
        })
    }

    /// Handles a health check.
    pub fn handle_health(&self) -> Result<(), SyncError> {
        // Verify DB is accessible by reading metadata
        let _seq = self
            .db
            .get_current_sequence()
            .map_err(|e| SyncError::transport(format!("Health check failed: {}", e)))?;
        Ok(())
    }

    // ─── Byte-level handlers (bincode in/out for HTTP) ───────────────

    /// Handles a handshake from raw bincode bytes.
    pub fn handle_handshake_bytes(&self, body: &[u8]) -> Result<Vec<u8>, SyncError> {
        let request: HandshakeRequest = bincode::deserialize(body).map_err(SyncError::from)?;
        let response = self.handle_handshake(request)?;
        bincode::serialize(&response).map_err(|e| SyncError::serialization(e.to_string()))
    }

    /// Handles a push from raw bincode bytes.
    pub fn handle_push_bytes(&self, body: &[u8]) -> Result<Vec<u8>, SyncError> {
        let changes: Vec<SyncChange> = bincode::deserialize(body).map_err(SyncError::from)?;
        let response = self.handle_push(changes)?;
        bincode::serialize(&response).map_err(|e| SyncError::serialization(e.to_string()))
    }

    /// Handles a pull from raw bincode bytes.
    pub fn handle_pull_bytes(&self, body: &[u8]) -> Result<Vec<u8>, SyncError> {
        let request: PullRequest = bincode::deserialize(body).map_err(SyncError::from)?;
        let response = self.handle_pull(request)?;
        bincode::serialize(&response).map_err(|e| SyncError::serialization(e.to_string()))
    }
}

/// Build a SyncChange from a WAL record by loading the full entity.
fn build_change_from_record(
    db: &PulseDB,
    sequence: u64,
    record: &crate::storage::schema::WatchEventRecord,
    source_instance: InstanceId,
) -> Result<Option<SyncChange>, SyncError> {
    use super::types::{SerializableExperienceUpdate, SyncEntityType, SyncPayload};
    use crate::storage::schema::{EntityTypeTag, WatchEventTypeTag};
    use crate::types::{CollectiveId, ExperienceId, InsightId, RelationId, Timestamp};

    let collective_id = CollectiveId::from_bytes(record.collective_id);
    let timestamp = Timestamp::from_millis(record.timestamp_ms);
    let map_err = |e: crate::error::PulseDBError| {
        SyncError::transport(format!("Failed to load entity: {}", e))
    };

    let entity_type = match record.entity_type {
        EntityTypeTag::Experience => SyncEntityType::Experience,
        EntityTypeTag::Relation => SyncEntityType::Relation,
        EntityTypeTag::Insight => SyncEntityType::Insight,
        EntityTypeTag::Collective => SyncEntityType::Collective,
    };

    let payload = match (record.entity_type, record.event_type) {
        (EntityTypeTag::Experience, WatchEventTypeTag::Created) => {
            let id = ExperienceId::from_bytes(record.entity_id);
            db.get_experience(id)
                .map_err(map_err)?
                .map(SyncPayload::ExperienceCreated)
        }
        (EntityTypeTag::Experience, WatchEventTypeTag::Updated) => {
            let id = ExperienceId::from_bytes(record.entity_id);
            db.get_experience(id)
                .map_err(map_err)?
                .map(|exp| SyncPayload::ExperienceUpdated {
                    id,
                    update: SerializableExperienceUpdate {
                        importance: Some(exp.importance),
                        confidence: Some(exp.confidence),
                        domain: Some(exp.domain.clone()),
                        related_files: Some(exp.related_files.clone()),
                        archived: Some(exp.archived),
                        applications: Some(exp.applications.clone()),
                        last_reinforced: Some(exp.last_reinforced),
                    },
                    timestamp,
                })
        }
        (EntityTypeTag::Experience, WatchEventTypeTag::Archived) => {
            let id = ExperienceId::from_bytes(record.entity_id);
            Some(SyncPayload::ExperienceArchived { id, timestamp })
        }
        (EntityTypeTag::Experience, WatchEventTypeTag::Deleted) => {
            let id = ExperienceId::from_bytes(record.entity_id);
            Some(SyncPayload::ExperienceDeleted { id, timestamp })
        }
        (EntityTypeTag::Relation, WatchEventTypeTag::Created) => {
            let id = RelationId::from_bytes(record.entity_id);
            db.get_relation(id)
                .map_err(map_err)?
                .map(SyncPayload::RelationCreated)
        }
        (EntityTypeTag::Relation, WatchEventTypeTag::Deleted) => {
            let id = RelationId::from_bytes(record.entity_id);
            Some(SyncPayload::RelationDeleted { id, timestamp })
        }
        (EntityTypeTag::Insight, WatchEventTypeTag::Created) => {
            let id = InsightId::from_bytes(record.entity_id);
            db.get_insight(id)
                .map_err(map_err)?
                .map(SyncPayload::InsightCreated)
        }
        (EntityTypeTag::Insight, WatchEventTypeTag::Deleted) => {
            let id = InsightId::from_bytes(record.entity_id);
            Some(SyncPayload::InsightDeleted { id, timestamp })
        }
        (EntityTypeTag::Collective, WatchEventTypeTag::Created) => {
            let id = CollectiveId::from_bytes(record.entity_id);
            db.get_collective(id)
                .map_err(map_err)?
                .map(SyncPayload::CollectiveCreated)
        }
        _ => None,
    };

    Ok(payload.map(|p| SyncChange {
        sequence,
        source_instance,
        collective_id,
        entity_type,
        payload: p,
        timestamp,
    }))
}

// SyncServer is Send + Sync (Arc<PulseDB> is Send + Sync)
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_server_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<SyncServer>();
    }
}
