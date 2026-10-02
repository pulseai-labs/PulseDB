//! Local change pusher — reads WAL events and pushes them to a remote peer.
//!
//! The [`LocalChangePusher`] polls the WAL for new events, loads full entity
//! data from storage, constructs [`SyncChange`] payloads, and pushes them
//! via the [`SyncTransport`].

use std::sync::Arc;

use tracing::{debug, instrument, trace, warn};

use crate::db::PulseDB;
use crate::storage::schema::{EntityTypeTag, WatchEventRecord, WatchEventTypeTag};
use crate::types::{CollectiveId, ExperienceId, InsightId, RelationId, Timestamp};
use crate::watch::ChangePoller;

use super::config::SyncConfig;
use super::error::SyncError;
use super::progress::next_progress;
use super::transport::SyncTransport;
use super::types::{
    InstanceId, PushRequest, SerializableExperienceUpdate, SyncChange, SyncEntityType, SyncPayload,
};
use super::wire;
use super::SYNC_PROTOCOL_VERSION;

/// What one [`LocalChangePusher::push_pending`] observed.
pub(crate) enum PushOutcome {
    /// The peer answered under the bound identity and acknowledged the batch;
    /// carries the number of changes sent.
    Pushed(usize),
    /// The endpoint answered under a **different** identity, carried here.
    /// Nothing was persisted — the acknowledgement belongs to a peer whose
    /// cursor row this pusher was not built from.
    PeerChanged(InstanceId),
}

/// Polls local WAL events and pushes them to a remote peer via transport.
pub(crate) struct LocalChangePusher {
    db: Arc<PulseDB>,
    transport: Arc<dyn SyncTransport>,
    config: SyncConfig,
    poller: ChangePoller,
    local_instance_id: InstanceId,
    peer_instance_id: InstanceId,
    /// The peer's advertised inbound body cap, from the handshake.
    peer_receive_limit_bytes: usize,
}

impl LocalChangePusher {
    /// Creates a new pusher.
    ///
    /// `start_sequence` is the WAL sequence to resume from (0 for fresh sync).
    ///
    /// The poller is built with [`SyncConfig::batch_size`], which is what makes
    /// that setting mean what its rustdoc says — "maximum number of changes per
    /// sync batch". Left on [`ChangePoller`]'s own default (1000, for non-sync
    /// callers) the pusher would scan up to 1000 events whatever `batch_size`
    /// said.
    pub fn new(
        db: Arc<PulseDB>,
        transport: Arc<dyn SyncTransport>,
        config: SyncConfig,
        local_instance_id: InstanceId,
        peer_instance_id: InstanceId,
        peer_receive_limit_bytes: usize,
        start_sequence: u64,
    ) -> Self {
        let poller =
            ChangePoller::from_sequence(start_sequence).with_batch_limit(config.batch_size);
        Self {
            db,
            transport,
            config,
            poller,
            local_instance_id,
            peer_instance_id,
            peer_receive_limit_bytes,
        }
    }

    /// The effective push body cap: the smaller of this instance's own policy
    /// and what the peer said it will accept.
    ///
    /// Packing against the local policy alone builds bodies a tighter peer
    /// refuses on every cycle; packing against the peer's alone ignores this
    /// instance's own budget.
    fn push_cap_bytes(&self) -> usize {
        self.config
            .max_request_bytes
            .min(self.peer_receive_limit_bytes)
    }

    /// The reply budget this pusher can actually receive: the smaller of this
    /// instance's own policy and what its transport will really read.
    ///
    /// The push side advertises the same quantity the pull side does
    /// (`SyncManager::reply_limit_bytes`); advertising `max_request_bytes`
    /// alone promises a peer a reply size the local reader would refuse. It is
    /// the exact opposite direction from [`push_cap_bytes`](Self::push_cap_bytes),
    /// and the two are computed from different inputs on purpose.
    ///
    /// Every `PushRequest` this pusher builds — the sized envelope, the real
    /// request and the empty probe — must use THIS expression, not merely the
    /// same value: `reply_limit_bytes` is a postcard varint, so a sizing
    /// envelope built from a different number can be a byte narrower than the
    /// frame actually sent.
    fn reply_limit_bytes(&self) -> u64 {
        self.config
            .max_request_bytes
            .min(self.transport.receive_limit_bytes()) as u64
    }

    /// Pushes all pending local changes to the remote peer.
    ///
    /// Returns the number of changes successfully pushed.
    #[instrument(skip(self), fields(peer = %self.peer_instance_id))]
    pub async fn push_pending(&mut self) -> Result<PushOutcome, SyncError> {
        // The poller is a pure function of persisted progress (see
        // `reset_to`), so where this scan starts is where the cursor is.
        let prior = self.poller.last_sequence();
        let storage = self.db.storage_for_test(); // pub accessor
        let events = self
            .poller
            .poll_sync_events(storage)
            .map_err(|e| SyncError::transport(format!("Failed to poll WAL events: {}", e)))?;

        // ─── The ordered scan, and its own position ──────────────────
        //
        // `scanned` is the last event actually read before the first eligible
        // change that could not be included. Filtered events and events whose
        // entity no longer exists advance it — they will never be pushed to
        // this peer, so compaction may reclaim them. A change excluded by the
        // count ceiling or the byte budget does NOT: it is still owed.
        let cap = self.push_cap_bytes();
        let envelope = wire::encoded_len(&PushRequest {
            protocol_version: SYNC_PROTOCOL_VERSION,
            source_instance: self.local_instance_id,
            target_instance: self.peer_instance_id,
            reply_limit_bytes: self.reply_limit_bytes(),
            changes: Vec::new(),
        })?;
        let mut sizer = wire::FrameSizer::new(envelope);
        let mut changes: Vec<SyncChange> = Vec::new();
        let mut scanned = prior;

        for (sequence, record) in &events {
            let change = match self.record_to_change(*sequence, record)? {
                Some(change) => change,
                None => {
                    scanned = *sequence;
                    continue;
                }
            };
            if changes.len() >= self.config.batch_size {
                break;
            }
            let item = wire::item_len(&change)?;
            if sizer.len_with(item) > cap {
                if changes.is_empty() && scanned <= prior {
                    // One change cannot fit a body on its own, and no safe
                    // filtered progress precedes it. This is deterministic: the
                    // same change rebuilt next cycle is the same size against
                    // the same cap. Fail closed, leave its cursor unadvanced,
                    // and let the background loop stop retrying.
                    let needed = sizer.len_with(item);
                    warn!(
                        sequence = change.sequence,
                        needed, cap, "A single change cannot fit the push body budget"
                    );
                    // A reused pusher must not carry a scan position past a
                    // change it never sent.
                    self.reset_to(prior);
                    return Err(SyncError::ChangeTooLarge {
                        sequence: change.sequence,
                        needed: needed as u64,
                        cap: cap as u64,
                    });
                }
                break;
            }
            sizer.push(item);
            changes.push(change);
            scanned = *sequence;
        }

        if changes.is_empty() {
            // Everything scanned was filtered, or the cursor was already at the
            // WAL head. Nothing is owed — but the peer's identity has still not
            // been checked this cycle, and a manager that made no request at
            // all would keep syncing an endpoint that was replaced.
            //
            // So send a bounded EMPTY routed push and validate its answer
            // before accepting scan-only progress. This is the only detection
            // point a PushOnly cycle has: a health check answers that something
            // is listening, never who, so it is not identity evidence.
            return self.probe_and_save(prior, scanned).await;
        }

        let count = changes.len();
        let sent: Vec<u64> = changes.iter().map(|c| c.sequence).collect();
        let reply = self
            .transport
            .push_changes(
                PushRequest {
                    protocol_version: SYNC_PROTOCOL_VERSION,
                    source_instance: self.local_instance_id,
                    target_instance: self.peer_instance_id,
                    reply_limit_bytes: self.reply_limit_bytes(),
                    changes,
                },
                // The very cap this batch was packed against, so a body the
                // packer built is never refused by the encoder.
                cap,
            )
            .await
            .inspect_err(|_| {
                // A transport failure leaves the fate of the batch unknown, so
                // resume from what is actually on record.
                self.reset_to(prior);
            })?;

        let responder = reply.responder;
        let ack = match reply.into_result(self.peer_instance_id) {
            Ok(ack) => ack,
            Err(SyncError::PeerChanged { .. }) => {
                // Nothing persisted: this acknowledgement, if any, belongs to a
                // peer whose cursor row this pusher was not built from.
                self.reset_to(prior);
                return Ok(PushOutcome::PeerChanged(responder));
            }
            Err(e) => {
                self.reset_to(prior);
                return Err(e);
            }
        };

        if let Err(e) = self.validate_ack(&ack, &sent) {
            self.reset_to(prior);
            return Err(e);
        }

        // The shared rule: with nothing rejected the position takes the scan
        // position, so a filtered tail inside this page is covered too; with
        // anything rejected it takes only the actual-success position, so a
        // change that failed to apply stays ahead of the cursor and is sent
        // again.
        let next = next_progress(prior, scanned, ack.rejected as usize, ack.safe_through);
        self.save_push_cursor(next)?;
        // Rebuild from what was PERSISTED, not from where the scan reached: a
        // partial acknowledgement, a byte-truncated prefix or a count-truncated
        // one all leave a suffix this peer has not seen, and a reused pusher
        // that resumed from its own scan position would never send it.
        self.reset_to(next);

        debug!(count, next, "Pushed local changes to remote");
        Ok(PushOutcome::Pushed(count))
    }

    /// Sends an empty routed push, validates the answer, and only then saves
    /// scan-only progress.
    ///
    /// The progress is real — the scanned events were filtered, or there were
    /// none — but it may only be filed under an identity this cycle actually
    /// reached. An unvalidated save would file it under a peer that is gone.
    async fn probe_and_save(&mut self, prior: u64, scanned: u64) -> Result<PushOutcome, SyncError> {
        let reply = self
            .transport
            .push_changes(
                PushRequest {
                    protocol_version: SYNC_PROTOCOL_VERSION,
                    source_instance: self.local_instance_id,
                    target_instance: self.peer_instance_id,
                    reply_limit_bytes: self.reply_limit_bytes(),
                    changes: Vec::new(),
                },
                // Same path, same budget as a real push.
                self.push_cap_bytes(),
            )
            .await
            .inspect_err(|_| self.reset_to(prior))?;

        let responder = reply.responder;
        let ack = match reply.into_result(self.peer_instance_id) {
            Ok(ack) => ack,
            Err(SyncError::PeerChanged { .. }) => {
                self.reset_to(prior);
                return Ok(PushOutcome::PeerChanged(responder));
            }
            Err(e) => {
                self.reset_to(prior);
                return Err(e);
            }
        };
        if let Err(e) = self.validate_ack(&ack, &[]) {
            self.reset_to(prior);
            return Err(e);
        }

        let next = next_progress(prior, scanned, 0, None);
        self.save_push_cursor(next)?;
        self.reset_to(next);
        trace!(next, "Empty push probe validated; scan-only progress saved");
        Ok(PushOutcome::Pushed(0))
    }

    /// Refuses an acknowledgement whose metadata does not match what was sent.
    ///
    /// A malformed acknowledgement is invalid payload, never permission to
    /// rebind or to move a cursor: the WAL owner must be this instance, the
    /// counts must add up to what was submitted, and any acknowledged position
    /// must name a sequence that was actually in the batch.
    fn validate_ack(&self, ack: &super::types::PushAck, sent: &[u64]) -> Result<(), SyncError> {
        if ack.wal_owner != self.local_instance_id {
            return Err(SyncError::invalid_payload(format!(
                "push acknowledgement names WAL owner {} but this instance is {}",
                ack.wal_owner, self.local_instance_id
            )));
        }
        let submitted = sent.len() as u64;
        if ack.total != submitted || ack.accepted.saturating_add(ack.rejected) != ack.total {
            return Err(SyncError::invalid_payload(format!(
                "push acknowledgement counts {}+{}={} do not match the {submitted} changes sent",
                ack.accepted, ack.rejected, ack.total
            )));
        }
        if let Some(position) = ack.safe_through {
            if !sent.contains(&position) {
                return Err(SyncError::invalid_payload(format!(
                    "push acknowledgement names position {position}, which was not in the batch"
                )));
            }
        }
        Ok(())
    }

    /// Rebuilds the poller from a persisted position.
    ///
    /// A pusher is reused across cycles, and its poller advances as it SCANS.
    /// After anything that leaves the scanned suffix unaccounted for — a
    /// transport failure, a partial acknowledgement, a rebind — resuming from
    /// the poller's own position would skip events that were never sent.
    fn reset_to(&mut self, sequence: u64) {
        self.poller =
            ChangePoller::from_sequence(sequence).with_batch_limit(self.config.batch_size);
    }

    /// Converts a WAL event record into a SyncChange, loading the full entity.
    ///
    /// Returns `None` if the entity should be skipped (filtered by config,
    /// or deleted between WAL event and push).
    fn record_to_change(
        &self,
        sequence: u64,
        record: &WatchEventRecord,
    ) -> Result<Option<SyncChange>, SyncError> {
        let collective_id = CollectiveId::from_bytes(record.collective_id);
        let timestamp = Timestamp::from_millis(record.timestamp_ms);

        // Filter by collective if configured
        if let Some(ref allowed) = self.config.collectives {
            if !allowed.contains(&collective_id) {
                trace!(seq = sequence, "Skipping change: collective filtered");
                return Ok(None);
            }
        }

        // Filter by entity type based on config
        match record.entity_type {
            EntityTypeTag::Relation if !self.config.sync_relations => {
                trace!(seq = sequence, "Skipping relation: sync_relations=false");
                return Ok(None);
            }
            EntityTypeTag::Insight if !self.config.sync_insights => {
                trace!(seq = sequence, "Skipping insight: sync_insights=false");
                return Ok(None);
            }
            _ => {}
        }

        let entity_type = match record.entity_type {
            EntityTypeTag::Experience => SyncEntityType::Experience,
            EntityTypeTag::Relation => SyncEntityType::Relation,
            EntityTypeTag::Insight => SyncEntityType::Insight,
            EntityTypeTag::Collective => SyncEntityType::Collective,
        };

        let payload = self.build_payload(record)?;
        let payload = match payload {
            Some(p) => p,
            None => {
                trace!(seq = sequence, "Skipping change: entity no longer exists");
                return Ok(None);
            }
        };

        Ok(Some(SyncChange {
            sequence,
            source_instance: self.local_instance_id,
            collective_id,
            entity_type,
            payload,
            timestamp,
        }))
    }

    /// Builds the SyncPayload by loading the full entity from storage.
    ///
    /// Returns `None` if the entity was deleted between WAL event and now.
    fn build_payload(&self, record: &WatchEventRecord) -> Result<Option<SyncPayload>, SyncError> {
        let map_err = |e: crate::error::PulseDBError| {
            SyncError::transport(format!("Failed to load entity for sync: {}", e))
        };

        match (record.entity_type, record.event_type) {
            // Experience events
            (EntityTypeTag::Experience, WatchEventTypeTag::Created) => {
                let id = ExperienceId::from_bytes(record.entity_id);
                match self.db.get_experience(id).map_err(map_err)? {
                    // `.into()` MOVES the loaded record's vector into the wire
                    // wrapper: the record still serializes without it, and the
                    // vector travels beside it (#96).
                    Some(exp) => Ok(Some(SyncPayload::ExperienceCreated(exp.into()))),
                    None => Ok(None), // Deleted before push
                }
            }
            (EntityTypeTag::Experience, WatchEventTypeTag::Updated) => {
                let id = ExperienceId::from_bytes(record.entity_id);
                match self.db.get_experience(id).map_err(map_err)? {
                    Some(exp) => {
                        // Send all current mutable field values
                        let update = SerializableExperienceUpdate {
                            importance: Some(exp.importance),
                            confidence: Some(exp.confidence),
                            domain: Some(exp.domain.clone()),
                            tags: Some(exp.tags.clone()),
                            related_files: Some(exp.related_files.clone()),
                            archived: Some(exp.archived),
                            applications: Some(exp.applications.clone()),
                            last_reinforced: Some(exp.last_reinforced),
                        };
                        Ok(Some(SyncPayload::ExperienceUpdated {
                            id,
                            update,
                            timestamp: Timestamp::from_millis(record.timestamp_ms),
                        }))
                    }
                    None => Ok(None),
                }
            }
            (EntityTypeTag::Experience, WatchEventTypeTag::Archived) => {
                let id = ExperienceId::from_bytes(record.entity_id);
                Ok(Some(SyncPayload::ExperienceArchived {
                    id,
                    timestamp: Timestamp::from_millis(record.timestamp_ms),
                }))
            }
            (EntityTypeTag::Experience, WatchEventTypeTag::Deleted) => {
                let id = ExperienceId::from_bytes(record.entity_id);
                Ok(Some(SyncPayload::ExperienceDeleted {
                    id,
                    timestamp: Timestamp::from_millis(record.timestamp_ms),
                }))
            }

            // Relation events
            (EntityTypeTag::Relation, WatchEventTypeTag::Created) => {
                let id = RelationId::from_bytes(record.entity_id);
                match self.db.get_relation(id).map_err(map_err)? {
                    Some(rel) => Ok(Some(SyncPayload::RelationCreated(rel))),
                    None => Ok(None),
                }
            }
            (EntityTypeTag::Relation, WatchEventTypeTag::Deleted) => {
                let id = RelationId::from_bytes(record.entity_id);
                Ok(Some(SyncPayload::RelationDeleted {
                    id,
                    timestamp: Timestamp::from_millis(record.timestamp_ms),
                }))
            }

            // Insight events
            (EntityTypeTag::Insight, WatchEventTypeTag::Created) => {
                let id = InsightId::from_bytes(record.entity_id);
                match self.db.get_insight(id).map_err(map_err)? {
                    Some(insight) => Ok(Some(SyncPayload::InsightCreated(insight))),
                    None => Ok(None),
                }
            }
            (EntityTypeTag::Insight, WatchEventTypeTag::Deleted) => {
                let id = InsightId::from_bytes(record.entity_id);
                Ok(Some(SyncPayload::InsightDeleted {
                    id,
                    timestamp: Timestamp::from_millis(record.timestamp_ms),
                }))
            }

            // Collective events
            (EntityTypeTag::Collective, WatchEventTypeTag::Created) => {
                let id = CollectiveId::from_bytes(record.entity_id);
                match self.db.get_collective(id).map_err(map_err)? {
                    Some(collective) => Ok(Some(SyncPayload::CollectiveCreated(collective))),
                    None => Ok(None),
                }
            }

            // Unexpected combinations (e.g., Collective + Deleted) — skip
            (entity_type, event_type) => {
                warn!(
                    ?entity_type,
                    ?event_type,
                    "Unexpected WAL event combination, skipping"
                );
                Ok(None)
            }
        }
    }

    /// Persists the push position for this peer (push side only — the pull
    /// position is owned by the manager's pull path and is never touched here).
    fn save_push_cursor(&self, sequence: u64) -> Result<(), SyncError> {
        self.db
            .storage_for_test()
            .update_push_cursor(&self.peer_instance_id, sequence)
            .map_err(|e| SyncError::transport(format!("Failed to save push cursor: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tempfile::tempdir;

    use super::*;
    use crate::sync::transport::SyncTransport;
    use crate::sync::transport_mem::InMemorySyncTransport;
    use crate::sync::types::{
        HandshakeRequest, HandshakeResponse, PullPage, PullRequest, WireReply,
    };
    use crate::{Config, ExperienceType, NewExperience, PulseDB};

    /// A peer that acknowledges only the FIRST change of every batch and
    /// reports the rest as failures — the partial-acknowledgement shape.
    struct PartialAckPeer {
        identity: InstanceId,
        batches: Mutex<Vec<Vec<u64>>>,
    }

    impl PartialAckPeer {
        fn new(identity: InstanceId) -> Arc<Self> {
            Arc::new(Self {
                identity,
                batches: Mutex::new(Vec::new()),
            })
        }

        fn batches(&self) -> Vec<Vec<u64>> {
            self.batches.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl SyncTransport for PartialAckPeer {
        async fn handshake(
            &self,
            _request: HandshakeRequest,
            _send_budget_bytes: usize,
        ) -> Result<HandshakeResponse, SyncError> {
            Ok(HandshakeResponse {
                instance_id: self.identity,
                protocol_version: SYNC_PROTOCOL_VERSION,
                accepted: true,
                reason: None,
                receive_limit_bytes: 64 * 1024 * 1024,
            })
        }

        async fn push_changes(
            &self,
            request: PushRequest,
            _send_budget_bytes: usize,
        ) -> Result<WireReply<super::super::types::PushAck>, SyncError> {
            let sent: Vec<u64> = request.changes.iter().map(|c| c.sequence).collect();
            self.batches.lock().unwrap().push(sent.clone());
            let total = sent.len() as u64;
            // The first change applies; everything after it fails.
            let (accepted, rejected, safe_through) = match sent.first() {
                Some(first) if total > 1 => (1, total - 1, Some(*first)),
                Some(first) => (1, 0, Some(*first)),
                None => (0, 0, None),
            };
            Ok(WireReply::ok(
                self.identity,
                super::super::types::PushAck {
                    wal_owner: request.source_instance,
                    accepted,
                    rejected,
                    total,
                    safe_through,
                },
            ))
        }

        async fn pull_changes(
            &self,
            _request: PullRequest,
            _send_budget_bytes: usize,
        ) -> Result<WireReply<PullPage>, SyncError> {
            unreachable!("this double is push-only");
        }

        async fn health_check(&self) -> Result<(), SyncError> {
            Ok(())
        }

        fn receive_limit_bytes(&self) -> usize {
            64 * 1024 * 1024
        }
    }

    /// A REUSED pusher resumes from PERSISTED progress, not from where its own
    /// poller last scanned.
    ///
    /// The poller advances as it SCANS, so after a partial acknowledgement the
    /// scanned position sits above the acknowledged one. A pusher that resumed
    /// from its own poller would never re-send the suffix the peer refused —
    /// silent loss, and only visible on the SECOND call, which is why this test
    /// reuses one pusher instead of building a fresh one per assertion.
    #[tokio::test]
    async fn recovery_v5_a_reused_pusher_resumes_from_persisted_progress() {
        let dir = tempdir().unwrap();
        let db = Arc::new(PulseDB::open(dir.path().join("resume.db"), Config::default()).unwrap());

        // WAL event 1 is the collective; 2..=5 are the experiences.
        let cid = db.create_collective("partial-ack").unwrap();
        for _ in 0..4 {
            db.record_experience(NewExperience {
                collective_id: cid,
                content: "partial ack resume".to_string(),
                experience_type: ExperienceType::Generic { category: None },
                embedding: Some(vec![0.1f32; 384]),
                importance: 0.5,
                ..Default::default()
            })
            .unwrap();
        }

        let peer = InstanceId::new();
        let endpoint = PartialAckPeer::new(peer);
        let mut pusher = LocalChangePusher::new(
            Arc::clone(&db),
            endpoint.clone(),
            SyncConfig::default(),
            db.instance_id(),
            peer,
            64 * 1024 * 1024,
            0,
        );

        // Cycle 1 sends 1..=5; only 1 is acknowledged.
        assert!(matches!(
            pusher.push_pending().await.unwrap(),
            PushOutcome::Pushed(5)
        ));
        let cursor = db
            .storage_for_test()
            .load_sync_cursor(&peer)
            .unwrap()
            .expect("the peer is on record");
        assert_eq!(
            cursor.push_sequence, 1,
            "with four changes rejected the cursor may only take the actual-success \
             position, never the scanned tail"
        );

        // Cycle 2 on the SAME pusher must re-send 2..=5.
        pusher.push_pending().await.unwrap();
        let batches = endpoint.batches();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0], vec![1, 2, 3, 4, 5]);
        assert_eq!(
            batches[1],
            vec![2, 3, 4, 5],
            "the unacknowledged suffix must be sent again, not skipped"
        );
    }

    /// `SyncConfig::batch_size` is documented as "maximum number of changes per
    /// sync batch", and `SyncConfig::validate` sizes `max_request_bytes` from
    /// it. Both are claims about the push path, so the push path's poller must
    /// be built with it: on [`ChangePoller`]'s own default (1000, for non-sync
    /// callers) a configured `batch_size` of 2 would still push everything the
    /// WAL had.
    #[tokio::test]
    async fn a_push_cycle_sends_at_most_batch_size_changes() {
        let dir = tempdir().unwrap();
        let db =
            Arc::new(PulseDB::open(dir.path().join("push-batch.db"), Config::default()).unwrap());

        // WAL event 1 is the collective; 2..=6 are the experiences.
        let cid = db.create_collective("push-batch-bound").unwrap();
        for _ in 0..5 {
            db.record_experience(NewExperience {
                collective_id: cid,
                content: "pusher batch bound".to_string(),
                experience_type: ExperienceType::Generic { category: None },
                embedding: Some(vec![0.1f32; 384]),
                importance: 0.5,
                ..Default::default()
            })
            .unwrap();
        }

        let (transport, _peer) = InMemorySyncTransport::new_pair();
        let peer_instance_id = transport.instance_id();
        let peer_limit = transport.receive_limit_bytes();
        let mut pusher = LocalChangePusher::new(
            Arc::clone(&db),
            Arc::new(transport),
            SyncConfig {
                batch_size: 2,
                ..SyncConfig::default()
            },
            db.instance_id(),
            peer_instance_id,
            peer_limit,
            0,
        );

        for cycle in 0..3 {
            assert!(
                matches!(pusher.push_pending().await.unwrap(), PushOutcome::Pushed(2)),
                "cycle {cycle} pushed more than the configured batch_size"
            );
        }
        assert!(
            matches!(pusher.push_pending().await.unwrap(), PushOutcome::Pushed(0)),
            "six WAL events in three batches of two leaves nothing pending"
        );
    }
}
