//! In-memory sync transport for testing.
//!
//! [`InMemorySyncTransport`] is an in-process double for the wire, not a
//! shortcut around it. Every request and reply is framed, encoded and decoded
//! through [`wire`](super::wire) exactly as an HTTP body would be, so a test
//! running on it exercises the same serialization, the same byte cap and the
//! same route checks. What it skips is the network, not the protocol.
//!
//! # Two lanes, not one shared buffer
//!
//! Each transport answers as **one** peer identity and serves **that peer's
//! own WAL lane**. A push writes into the lane of the change's
//! `source_instance`; a pull reads the lane of the identity being addressed.
//! The pre-v5 double kept a single buffer that both ends pushed into and pulled
//! out of, which made "whose WAL is this sequence in?" unanswerable — the
//! question every route and cursor check turns on. A test on the old double
//! could pass while the same exchange over HTTP misattributed the batch.
//!
//! It still holds no database: nothing here applies a change. Tests that assert
//! about **applying** need a server-backed adapter over
//! [`SyncServer`](super::server::SyncServer), which is what the engine and HTTP
//! suites use.
//!
//! # Conformance domain
//!
//! Within **framing, routing, protocol version and lane paging/byte progress**
//! the double answers exactly as a `SyncServer` over the same WAL would:
//!
//! - both data endpoints refuse a request whose `protocol_version` is not
//!   [`SYNC_PROTOCOL_VERSION`] with the server's `WireErrorCode::ProtocolVersion`
//!   reply, before any lane is read or written;
//! - a push runs the server's own batch-metadata checks (foreign source,
//!   sequence 0, repeated sequence) and reply-capacity preflight, in the
//!   server's order, before the lane is written;
//! - a pull reads at most the server's poll page of lane entries, packs the
//!   longest ordered prefix whose **complete** reply frame fits
//!   `min(request.reply_limit_bytes, this endpoint's receive limit)`, advances
//!   its scan position past filtered entries only while that complete reply
//!   still fits, and reports `has_more` from actual truncation or a full page —
//!   never from `matching.len() > batch_size`;
//! - a single change that cannot fit on its own is the server's typed
//!   `ChangeTooLarge` reply, with the scan position unadvanced.
//!
//! Storage resolution (an entity deleted since its WAL event) and apply
//! outcomes are outside that domain: the lane holds only what was seeded or
//! pushed, and nothing is applied.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use super::config::DEFAULT_MAX_REQUEST_BYTES;
use super::error::SyncError;
use super::server::{
    advance_scan_within_cap, preflight_reply_budget, validate_batch_metadata, PULL_PAGE_EVENT_LIMIT,
};
use super::transport::SyncTransport;
use super::types::{
    HandshakeRequest, HandshakeResponse, InstanceId, PullPage, PullRequest, PushAck, PushRequest,
    SyncChange, SyncPosition, WireErrorCode, WireReply, WireResult,
};
use super::wire::{self, WireOperation, MIN_CONTROL_FRAME_BYTES};
use super::SYNC_PROTOCOL_VERSION;

/// Per-owner WAL lanes shared between paired transports.
#[derive(Debug, Default)]
struct SharedLanes {
    lanes: BTreeMap<InstanceId, Vec<SyncChange>>,
}

/// In-process transport double for testing sync without network I/O.
///
/// Create a connected pair with [`new_pair()`](Self::new_pair). Each side
/// answers as its own identity; [`seed`](Self::seed) fills the lane a side will
/// serve on a pull, and [`received`](Self::received) reads back what a push put
/// into a lane.
///
/// # Example
///
/// ```rust
/// use pulsedb::sync::transport_mem::InMemorySyncTransport;
///
/// let (local, remote) = InMemorySyncTransport::new_pair();
/// assert_ne!(local.instance_id(), remote.instance_id());
/// ```
#[derive(Debug, Clone)]
pub struct InMemorySyncTransport {
    /// The identity this transport **answers as** — the peer, from the caller's
    /// point of view.
    peer_instance_id: InstanceId,
    /// Shared per-owner lanes.
    lanes: Arc<Mutex<SharedLanes>>,
    /// Inbound body cap this double will read, and advertise on a handshake.
    receive_limit_bytes: usize,
}

impl InMemorySyncTransport {
    /// Creates a pair of connected in-memory transports with distinct
    /// identities and a shared lane store.
    pub fn new_pair() -> (Self, Self) {
        let lanes = Arc::new(Mutex::new(SharedLanes::default()));
        let local = Self {
            peer_instance_id: InstanceId::new(),
            lanes: Arc::clone(&lanes),
            receive_limit_bytes: DEFAULT_MAX_REQUEST_BYTES,
        };
        let remote = Self {
            peer_instance_id: InstanceId::new(),
            lanes,
            receive_limit_bytes: DEFAULT_MAX_REQUEST_BYTES,
        };
        (local, remote)
    }

    /// Returns the instance ID this transport answers as.
    pub fn instance_id(&self) -> InstanceId {
        self.peer_instance_id
    }

    /// Sets the inbound body cap this double reads and advertises.
    pub fn with_receive_limit_bytes(mut self, receive_limit_bytes: usize) -> Self {
        self.receive_limit_bytes = receive_limit_bytes;
        self
    }

    /// Replaces the identity this transport answers as, as a remint or a
    /// restore-from-snapshot would.
    ///
    /// The old identity's lane is left in place — a restored copy is a
    /// different peer with a different WAL, and the previous one may
    /// legitimately come back.
    pub fn remint(&mut self) -> InstanceId {
        self.peer_instance_id = InstanceId::new();
        self.peer_instance_id
    }

    /// Appends `changes` to the lane this transport serves on a pull.
    pub fn seed(&self, changes: Vec<SyncChange>) {
        let mut lanes = self.lanes.lock().unwrap_or_else(|e| e.into_inner());
        lanes
            .lanes
            .entry(self.peer_instance_id)
            .or_default()
            .extend(changes);
    }

    /// Everything a push has written into `owner`'s lane.
    pub fn received(&self, owner: InstanceId) -> Vec<SyncChange> {
        let lanes = self.lanes.lock().unwrap_or_else(|e| e.into_inner());
        lanes.lanes.get(&owner).cloned().unwrap_or_default()
    }

    /// The server's refusal of a request declaring another protocol version,
    /// or `None` when the version matches.
    fn protocol_mismatch<T>(&self, declared: u32) -> Option<WireReply<T>> {
        (declared != SYNC_PROTOCOL_VERSION).then(|| {
            WireReply::rejected(
                self.peer_instance_id,
                WireErrorCode::ProtocolVersion,
                format!(
                    "server speaks protocol v{SYNC_PROTOCOL_VERSION}, request declared v{declared}"
                ),
            )
        })
    }

    /// The server's refusals of a pull, in the server's order, before any lane
    /// is read: protocol version, target, zero count, reply limit, cursor owner.
    fn refuse_pull(&self, request: &PullRequest) -> Option<WireReply<PullPage>> {
        let me = self.peer_instance_id;
        if let Some(reply) = self.protocol_mismatch(request.protocol_version) {
            return Some(reply);
        }
        if request.target_instance != me {
            return Some(WireReply::peer_changed(me, request.target_instance));
        }
        if request.batch_size == 0 {
            return Some(WireReply::rejected(
                me,
                WireErrorCode::InvalidRequest,
                "pull requested zero changes",
            ));
        }
        if request.reply_limit_bytes < MIN_CONTROL_FRAME_BYTES as u64 {
            return Some(WireReply::rejected(
                me,
                WireErrorCode::InvalidRequest,
                format!(
                    "pull declared a {}-byte reply limit, below the \
                     {MIN_CONTROL_FRAME_BYTES}-byte control minimum",
                    request.reply_limit_bytes
                ),
            ));
        }
        if request.cursor.instance_id != me {
            return Some(WireReply::rejected(
                me,
                WireErrorCode::InvalidRequest,
                format!(
                    "pull cursor names WAL owner {} but this instance is {me}",
                    request.cursor.instance_id
                ),
            ));
        }
        None
    }

    /// The lane standing in for the WAL: the next poll page of entries after
    /// `after`, in sequence order, whatever a filter will make of them.
    fn poll_page(&self, after: u64) -> Vec<SyncChange> {
        let lanes = self.lanes.lock().unwrap_or_else(|e| e.into_inner());
        let mut page: Vec<SyncChange> = lanes
            .lanes
            .get(&self.peer_instance_id)
            .map(|lane| {
                lane.iter()
                    .filter(|c| c.sequence > after)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        page.sort_by_key(|c| c.sequence);
        page.truncate(PULL_PAGE_EVENT_LIMIT);
        page
    }

    /// Answers a decoded pull exactly as `SyncServer::handle_pull` answers the
    /// same WAL: the same checks in the same order, the same poll page, and the
    /// same complete-frame packing through the same sizing helpers.
    fn serve_pull(&self, request: &PullRequest) -> Result<WireReply<PullPage>, SyncError> {
        let me = self.peer_instance_id;
        if let Some(reply) = self.refuse_pull(request) {
            return Ok(reply);
        }
        let batch_size = usize::try_from(request.batch_size).unwrap_or(usize::MAX);
        let reply_cap = usize::try_from(request.reply_limit_bytes)
            .unwrap_or(usize::MAX)
            .min(self.receive_limit_bytes);

        let page = self.poll_page(request.cursor.sequence);

        let envelope_at = |scan: u64| -> Result<usize, SyncError> {
            wire::encoded_len(&WireReply::ok(
                me,
                PullPage {
                    changes: Vec::new(),
                    has_more: true,
                    scan_position: SyncPosition::new(me, scan),
                },
            ))
        };
        let mut sizer = wire::FrameSizer::new(envelope_at(request.cursor.sequence)?);
        let mut changes: Vec<SyncChange> = Vec::new();
        let mut scanned = request.cursor.sequence;
        let mut truncated = false;
        let polled = page.len();

        for change in page {
            let excluded = request
                .collectives
                .as_ref()
                .is_some_and(|ids| !ids.contains(&change.collective_id));
            if excluded {
                if !advance_scan_within_cap(&mut sizer, &mut scanned, change.sequence, reply_cap)? {
                    truncated = true;
                    break;
                }
                continue;
            }
            if changes.len() >= batch_size {
                truncated = true;
                break;
            }
            let mut candidate = sizer;
            candidate.rebase(envelope_at(change.sequence)?);
            let item = wire::item_len(&change)?;
            if candidate.len_with(item) > reply_cap {
                if changes.is_empty() && scanned <= request.cursor.sequence {
                    return Ok(WireReply {
                        protocol_version: SYNC_PROTOCOL_VERSION,
                        responder: me,
                        result: WireResult::ChangeTooLarge {
                            sequence: change.sequence,
                            needed: candidate.len_with(item) as u64,
                            cap: reply_cap as u64,
                        },
                    });
                }
                truncated = true;
                break;
            }
            candidate.push(item);
            sizer = candidate;
            scanned = change.sequence;
            changes.push(change);
        }

        Ok(WireReply::ok(
            me,
            PullPage {
                changes,
                has_more: truncated || polled >= PULL_PAGE_EVENT_LIMIT,
                scan_position: SyncPosition::new(me, scanned),
            },
        ))
    }

    /// Round-trips a REQUEST through the real frame codec, honestly split by
    /// direction.
    ///
    /// The caller encodes against the budget it was given, exactly as an HTTP
    /// client does; this endpoint then reads under its own inbound limit. The
    /// two are different numbers and the loopback must not blur them — an
    /// over-budget encode here is the sender's
    /// [`SyncError::RequestTooLarge`], while a body that fits the sender's
    /// budget yet exceeds this endpoint's reader is the ordinary inbound
    /// [`SyncError::PayloadTooLarge`], as it would be over the wire.
    fn round_trip_request<T>(
        &self,
        operation: WireOperation,
        value: &T,
        send_budget_bytes: usize,
    ) -> Result<T, SyncError>
    where
        T: serde::Serialize + serde::de::DeserializeOwned,
    {
        let framed =
            wire::encode_bounded(operation, value, send_budget_bytes).map_err(|e| match e {
                SyncError::PayloadTooLarge { size, max } => SyncError::RequestTooLarge {
                    operation,
                    needed: size as u64,
                    cap: max as u64,
                },
                other => other,
            })?;
        wire::decode_bounded(operation, &framed, self.receive_limit_bytes)
    }

    /// Round-trips a REPLY through the real frame codec under this endpoint's
    /// own limit, so an in-process test still pays for serialization.
    ///
    /// The reply leg keeps `receive_limit_bytes` on both halves: this endpoint
    /// builds the answer under its own policy, and an oversized one is an
    /// inbound failure for the reader — never a `RequestTooLarge`, which names
    /// a request this side built.
    fn round_trip_reply<T>(&self, operation: WireOperation, value: &T) -> Result<T, SyncError>
    where
        T: serde::Serialize + serde::de::DeserializeOwned,
    {
        let framed = wire::encode_bounded(operation, value, self.receive_limit_bytes)?;
        wire::decode_bounded(operation, &framed, self.receive_limit_bytes)
    }
}

#[async_trait]
impl SyncTransport for InMemorySyncTransport {
    async fn handshake(
        &self,
        request: HandshakeRequest,
        send_budget_bytes: usize,
    ) -> Result<HandshakeResponse, SyncError> {
        let request: HandshakeRequest =
            self.round_trip_request(WireOperation::Handshake, &request, send_budget_bytes)?;
        request.check_bounds()?;
        let response = HandshakeResponse {
            instance_id: self.peer_instance_id,
            protocol_version: SYNC_PROTOCOL_VERSION,
            accepted: request.protocol_version == SYNC_PROTOCOL_VERSION,
            reason: None,
            receive_limit_bytes: self.receive_limit_bytes as u64,
        };
        self.round_trip_reply(WireOperation::Handshake, &response)
    }

    async fn push_changes(
        &self,
        request: PushRequest,
        send_budget_bytes: usize,
    ) -> Result<WireReply<PushAck>, SyncError> {
        let request: PushRequest =
            self.round_trip_request(WireOperation::Push, &request, send_budget_bytes)?;

        if let Some(reply) = self.protocol_mismatch(request.protocol_version) {
            return self.round_trip_reply(WireOperation::Push, &reply);
        }
        // Route next: a batch addressed to somebody else is not this peer's to
        // record, so nothing is written.
        if request.target_instance != self.peer_instance_id {
            let reply = WireReply::peer_changed(self.peer_instance_id, request.target_instance);
            return self.round_trip_reply(WireOperation::Push, &reply);
        }
        // Then the server's own batch-metadata and reply-capacity checks, in
        // its order, before the lane is touched.
        if let Err(detail) = validate_batch_metadata(&request) {
            let reply =
                WireReply::rejected(self.peer_instance_id, WireErrorCode::InvalidRequest, detail);
            return self.round_trip_reply(WireOperation::Push, &reply);
        }
        preflight_reply_budget(request.reply_limit_bytes, self.receive_limit_bytes)?;

        let total = request.changes.len() as u64;
        let safe_through = request.changes.iter().map(|c| c.sequence).max();
        {
            let mut lanes = self.lanes.lock().unwrap_or_else(|e| e.into_inner());
            lanes
                .lanes
                .entry(request.source_instance)
                .or_default()
                .extend(request.changes);
        }

        let reply = WireReply::ok(
            self.peer_instance_id,
            PushAck {
                // The SENDER's WAL is what `safe_through` indexes.
                wal_owner: request.source_instance,
                accepted: total,
                rejected: 0,
                total,
                safe_through,
            },
        );
        self.round_trip_reply(WireOperation::Push, &reply)
    }

    async fn pull_changes(
        &self,
        request: PullRequest,
        send_budget_bytes: usize,
    ) -> Result<WireReply<PullPage>, SyncError> {
        let request: PullRequest =
            self.round_trip_request(WireOperation::Pull, &request, send_budget_bytes)?;
        let reply = self.serve_pull(&request)?;
        self.round_trip_reply(WireOperation::Pull, &reply)
    }

    async fn health_check(&self) -> Result<(), SyncError> {
        // Liveness only — never identity evidence.
        Ok(())
    }

    fn receive_limit_bytes(&self) -> usize {
        self.receive_limit_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::types::{SyncEntityType, SyncPayload, SyncStatus};
    use crate::types::{CollectiveId, Timestamp};

    fn make_test_change(seq: u64, collective_id: CollectiveId, source: InstanceId) -> SyncChange {
        use crate::collective::Collective;
        SyncChange {
            sequence: seq,
            source_instance: source,
            collective_id,
            entity_type: SyncEntityType::Collective,
            payload: SyncPayload::CollectiveCreated(Collective {
                id: collective_id,
                name: format!("test-{}", seq),
                owner_id: None,
                embedding_dimension: 384,
                created_at: Timestamp::now(),
                updated_at: Timestamp::now(),
            }),
            timestamp: Timestamp::now(),
        }
    }

    fn pull_request(target: InstanceId, from: u64, batch_size: u64) -> PullRequest {
        PullRequest {
            protocol_version: SYNC_PROTOCOL_VERSION,
            source_instance: InstanceId::new(),
            target_instance: target,
            cursor: SyncPosition::new(target, from),
            batch_size,
            reply_limit_bytes: DEFAULT_MAX_REQUEST_BYTES as u64,
            collectives: None,
        }
    }

    fn push_request(
        source: InstanceId,
        target: InstanceId,
        changes: Vec<SyncChange>,
    ) -> PushRequest {
        PushRequest {
            protocol_version: SYNC_PROTOCOL_VERSION,
            source_instance: source,
            target_instance: target,
            reply_limit_bytes: DEFAULT_MAX_REQUEST_BYTES as u64,
            changes,
        }
    }

    #[tokio::test]
    async fn test_new_pair_creates_distinct_instances() {
        let (local, remote) = InMemorySyncTransport::new_pair();
        assert_ne!(local.instance_id(), remote.instance_id());
    }

    #[tokio::test]
    async fn test_handshake_accepts_a_matching_protocol_version() {
        let (transport, _) = InMemorySyncTransport::new_pair();
        let req = HandshakeRequest {
            instance_id: InstanceId::new(),
            protocol_version: SYNC_PROTOCOL_VERSION,
            capabilities: vec![],
        };
        let resp = transport
            .handshake(req, DEFAULT_MAX_REQUEST_BYTES)
            .await
            .unwrap();
        assert!(resp.accepted);
        assert_eq!(resp.protocol_version, SYNC_PROTOCOL_VERSION);
        assert_eq!(
            resp.receive_limit_bytes as usize,
            transport.receive_limit_bytes()
        );
    }

    #[tokio::test]
    async fn test_health_check_always_ok() {
        let (transport, _) = InMemorySyncTransport::new_pair();
        assert!(transport.health_check().await.is_ok());
    }

    /// A push lands in the SENDER's lane, and a pull of the peer's own lane
    /// does not see it. The pre-v5 shared buffer conflated the two.
    #[tokio::test]
    async fn recovery_v5_push_and_pull_use_separate_lanes() {
        let (local, _remote) = InMemorySyncTransport::new_pair();
        let source = InstanceId::new();
        let cid = CollectiveId::new();

        let changes = (1..=3)
            .map(|seq| make_test_change(seq, cid, source))
            .collect();
        let ack = local
            .push_changes(
                push_request(source, local.instance_id(), changes),
                DEFAULT_MAX_REQUEST_BYTES,
            )
            .await
            .unwrap()
            .into_result(local.instance_id())
            .unwrap();
        assert_eq!(ack.accepted, 3);
        assert_eq!(ack.rejected, 0);
        assert_eq!(ack.total, 3);
        assert_eq!(
            ack.wal_owner, source,
            "a push acknowledges the SENDER's WAL"
        );
        assert_eq!(ack.safe_through, Some(3));
        assert_eq!(local.received(source).len(), 3);

        // The peer's OWN lane is untouched by what was pushed into the sender's.
        let page = local
            .pull_changes(
                pull_request(local.instance_id(), 0, 100),
                DEFAULT_MAX_REQUEST_BYTES,
            )
            .await
            .unwrap()
            .into_result(local.instance_id())
            .unwrap();
        assert!(
            page.changes.is_empty(),
            "a pull reads the peer's own WAL, not the lane the sender pushed into"
        );
    }

    /// A request addressed to a different identity is refused with
    /// `PeerChanged`, and writes nothing.
    #[tokio::test]
    async fn recovery_v5_wrong_target_is_refused_with_no_side_effect() {
        let (local, _remote) = InMemorySyncTransport::new_pair();
        let source = InstanceId::new();
        let stranger = InstanceId::new();
        let cid = CollectiveId::new();

        let reply = local
            .push_changes(
                push_request(source, stranger, vec![make_test_change(1, cid, source)]),
                DEFAULT_MAX_REQUEST_BYTES,
            )
            .await
            .unwrap();
        let err = reply.into_result(stranger).unwrap_err();
        assert!(err.is_peer_changed(), "got {err}");
        assert!(
            local.received(source).is_empty(),
            "a misrouted push must record nothing"
        );

        let reply = local
            .pull_changes(pull_request(stranger, 0, 10), DEFAULT_MAX_REQUEST_BYTES)
            .await
            .unwrap();
        assert!(reply.into_result(stranger).unwrap_err().is_peer_changed());
    }

    /// A change whose `source_instance` disagrees with the request's is
    /// invalid payload, not a licence to file it under either identity.
    #[tokio::test]
    async fn recovery_v5_inconsistent_source_ownership_is_rejected() {
        let (local, _remote) = InMemorySyncTransport::new_pair();
        let source = InstanceId::new();
        let foreign = InstanceId::new();
        let cid = CollectiveId::new();

        let reply = local
            .push_changes(
                push_request(
                    source,
                    local.instance_id(),
                    vec![make_test_change(1, cid, foreign)],
                ),
                DEFAULT_MAX_REQUEST_BYTES,
            )
            .await
            .unwrap();
        let err = reply.into_result(local.instance_id()).unwrap_err();
        assert!(matches!(err, SyncError::RemoteRejected { .. }), "got {err}");
        assert!(local.received(source).is_empty());
        assert!(local.received(foreign).is_empty());
    }

    #[tokio::test]
    async fn test_pull_respects_cursor_and_batch_size() {
        let (local, _remote) = InMemorySyncTransport::new_pair();
        let cid = CollectiveId::new();
        local.seed(
            (1..=5)
                .map(|seq| make_test_change(seq, cid, local.instance_id()))
                .collect(),
        );

        let page = local
            .pull_changes(
                pull_request(local.instance_id(), 3, 100),
                DEFAULT_MAX_REQUEST_BYTES,
            )
            .await
            .unwrap()
            .into_result(local.instance_id())
            .unwrap();
        assert_eq!(page.changes.len(), 2);
        assert_eq!(page.changes[0].sequence, 4);
        assert_eq!(page.scan_position.sequence, 5);
        assert_eq!(page.scan_position.instance_id, local.instance_id());
        assert!(!page.has_more);

        let page = local
            .pull_changes(
                pull_request(local.instance_id(), 0, 3),
                DEFAULT_MAX_REQUEST_BYTES,
            )
            .await
            .unwrap()
            .into_result(local.instance_id())
            .unwrap();
        assert_eq!(page.changes.len(), 3);
        assert!(page.has_more);
        assert_eq!(page.scan_position.sequence, 3);
    }

    #[tokio::test]
    async fn test_pull_filters_by_collective() {
        let (local, _remote) = InMemorySyncTransport::new_pair();
        let cid_a = CollectiveId::new();
        let cid_b = CollectiveId::new();
        local.seed(vec![
            make_test_change(1, cid_a, local.instance_id()),
            make_test_change(2, cid_b, local.instance_id()),
            make_test_change(3, cid_a, local.instance_id()),
        ]);

        let mut request = pull_request(local.instance_id(), 0, 100);
        request.collectives = Some(vec![cid_a]);
        let page = local
            .pull_changes(request, DEFAULT_MAX_REQUEST_BYTES)
            .await
            .unwrap()
            .into_result(local.instance_id())
            .unwrap();
        assert_eq!(page.changes.len(), 2);
        assert!(page.changes.iter().all(|c| c.collective_id == cid_a));
    }

    #[tokio::test]
    async fn test_pull_empty_lane() {
        let (_, remote) = InMemorySyncTransport::new_pair();
        let page = remote
            .pull_changes(
                pull_request(remote.instance_id(), 0, 100),
                DEFAULT_MAX_REQUEST_BYTES,
            )
            .await
            .unwrap()
            .into_result(remote.instance_id())
            .unwrap();
        assert!(page.changes.is_empty());
        assert!(!page.has_more);
        assert_eq!(page.scan_position.sequence, 0);
    }

    /// A zero-count pull is refused independently of any byte budget.
    #[tokio::test]
    async fn recovery_v5_zero_count_pull_is_refused() {
        let (local, _remote) = InMemorySyncTransport::new_pair();
        let err = local
            .pull_changes(
                pull_request(local.instance_id(), 0, 0),
                DEFAULT_MAX_REQUEST_BYTES,
            )
            .await
            .unwrap()
            .into_result(local.instance_id())
            .unwrap_err();
        assert!(matches!(err, SyncError::RemoteRejected { .. }), "got {err}");
    }

    #[test]
    fn test_sync_status_not_used_here_but_compiles() {
        let status = SyncStatus::Idle;
        assert_eq!(status, SyncStatus::Idle);
    }
}
