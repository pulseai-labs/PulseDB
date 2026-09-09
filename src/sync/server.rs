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
//!     server.handle_handshake_bytes(&body).map_err(|e| {
//!         if e.is_payload_too_large() {
//!             StatusCode::PAYLOAD_TOO_LARGE
//!         } else {
//!             StatusCode::BAD_REQUEST
//!         }
//!     })
//! }
//! ```
//!
//! # Framing and the byte cap
//!
//! Every `handle_*_bytes` method runs the same ordered gate before anything is
//! applied or persisted: the raw body length against
//! [`SyncConfig::max_request_bytes`], then the frame header by raw byte-slicing
//! (magic, [`WIRE_FORMAT_VERSION`](super::WIRE_FORMAT_VERSION), operation),
//! then an exact postcard decode that refuses trailing bytes. PulseDB builds no
//! router, so this is the cap it owns at the network edge (ADR-009); the
//! consumer's framework body limit stacks on top of it — and must, because a
//! byte handler handed an already-buffered body cannot un-allocate it.
//!
//! The typed handlers enforce the **same semantic checks** as the byte
//! handlers, minus the framing: a consumer that wires up the typed entry points
//! gets identical route, budget and metadata validation. Neither assumes a
//! prior handshake — a stateless direct push is checked on its own merits.

use std::sync::{Arc, Mutex};

use tracing::{debug, info, instrument, warn};

use crate::db::PulseDB;
use crate::watch::ChangePoller;

use super::applier::RemoteChangeApplier;
use super::config::SyncConfig;
use super::error::SyncError;
use super::types::{
    HandshakeRequest, HandshakeResponse, InstanceId, PullPage, PullRequest, PushAck, PushRequest,
    SyncChange, SyncPosition, SyncStats, WireErrorCode, WireReply,
};
use super::wire::{self, WireOperation, MIN_CONTROL_FRAME_BYTES};
use super::SYNC_PROTOCOL_VERSION;

/// WAL events one pull reads from the store in a single page.
///
/// The server states the page size it wants instead of inheriting
/// [`ChangePoller`]'s private default, because `handle_pull` has to tell a FULL
/// page (the WAL may hold more) from a short one (the WAL is exhausted) and a
/// limit it did not set is a limit it cannot see. The value is the poller's own
/// default, so the page size is exactly what it has always been.
const PULL_PAGE_EVENT_LIMIT: usize = 1000;

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
    stats: Mutex<SyncStats>,
}

impl SyncServer {
    /// Creates a new SyncServer for the given database.
    ///
    /// # Errors
    ///
    /// [`SyncError::Config`] when the configuration is invalid, or when
    /// `max_request_bytes` is below
    /// [`MIN_CONTROL_FRAME_BYTES`] — a server that cannot carry its own bounded
    /// control traffic fails on every exchange, and failing here says so at the
    /// call that made it.
    ///
    /// **0.8.0 source break:** this used to return `Self`. Existing callers add
    /// `?` or an `expect`.
    ///
    /// # Identity precondition
    ///
    /// The instance id is read once, here.
    /// [`PulseDB::remint_instance_id`](crate::PulseDB::remint_instance_id) must
    /// run **before** this call; a remint afterwards is not observed by this
    /// server, and reminting a live server is outside the lifecycle contract.
    pub fn new(db: Arc<PulseDB>, config: SyncConfig) -> Result<Self, SyncError> {
        config
            .validate()
            .map_err(|e| SyncError::config(e.to_string()))?;
        if config.max_request_bytes < MIN_CONTROL_FRAME_BYTES {
            return Err(SyncError::config(format!(
                "max_request_bytes {} is below the {MIN_CONTROL_FRAME_BYTES}-byte control minimum",
                config.max_request_bytes
            )));
        }
        // Read once, here — same pre-construction rule as `SyncManager`.
        let instance_id = db.instance_id();
        Ok(Self {
            db,
            instance_id,
            config,
            stats: Mutex::new(SyncStats::default()),
        })
    }

    /// The body cap this server accepts and advertises, in bytes.
    pub fn receive_limit_bytes(&self) -> usize {
        self.config.max_request_bytes
    }

    /// Returns the server's instance ID.
    pub fn instance_id(&self) -> InstanceId {
        self.instance_id
    }

    /// Returns a snapshot of the local-only sync counters accumulated over
    /// every change pushed to this server.
    ///
    /// See [`SyncStats::skewed_timestamps`] for the #13 skew counter. These
    /// counters never travel on the wire — [`PushAck`] carries no statistics.
    pub fn stats(&self) -> SyncStats {
        *self.stats.lock().unwrap_or_else(|e| e.into_inner())
    }

    // ─── High-level handlers (typed) ─────────────────────────────────

    /// Handles a handshake request.
    ///
    /// The response carries this server's **own inbound body cap**, so the
    /// sender can pack a later push against `min(its policy, this)` instead of
    /// guessing.
    #[instrument(skip(self, request), fields(peer = %request.instance_id))]
    pub fn handle_handshake(
        &self,
        request: HandshakeRequest,
    ) -> Result<HandshakeResponse, SyncError> {
        request.check_bounds()?;
        if request.protocol_version != SYNC_PROTOCOL_VERSION {
            return Ok(HandshakeResponse {
                instance_id: self.instance_id,
                protocol_version: SYNC_PROTOCOL_VERSION,
                accepted: false,
                reason: Some(super::types::bound_wire_detail(format!(
                    "Protocol version mismatch: server v{}, client v{}",
                    SYNC_PROTOCOL_VERSION, request.protocol_version
                ))),
                receive_limit_bytes: self.config.max_request_bytes as u64,
            });
        }

        info!(peer = %request.instance_id, "Sync handshake accepted");
        Ok(HandshakeResponse {
            instance_id: self.instance_id,
            protocol_version: SYNC_PROTOCOL_VERSION,
            accepted: true,
            reason: None,
            receive_limit_bytes: self.config.max_request_bytes as u64,
        })
    }

    /// Handles a routed push request — applies remote changes locally.
    ///
    /// The acknowledged position is
    /// [`ApplyResult::safe_through`](super::applier::ApplyResult::safe_through)
    /// — the highest sequence at or below which every change in the batch was
    /// applied, resolved or idempotently skipped — **not** the highest sequence
    /// received and not the highest one that happened to succeed: the sender
    /// turns this into its `push_sequence`, and `compact_wal` deletes below it,
    /// so acknowledging past a change that failed to apply would let the sender
    /// discard a WAL event this peer never stored. The batch's order is the
    /// sender's choice, so the bound is by sequence, not by position.
    ///
    /// [`PushAck::wal_owner`] names whose WAL that position indexes — the
    /// sender's — separately from [`WireReply::responder`], which names who
    /// answered.
    #[instrument(skip(self, request), fields(count = request.changes.len()))]
    pub fn handle_push(&self, request: PushRequest) -> Result<WireReply<PushAck>, SyncError> {
        if request.protocol_version != SYNC_PROTOCOL_VERSION {
            return Ok(WireReply::rejected(
                self.instance_id,
                WireErrorCode::ProtocolVersion,
                format!(
                    "server speaks protocol v{SYNC_PROTOCOL_VERSION}, request declared v{}",
                    request.protocol_version
                ),
            ));
        }
        // ─── Route and metadata, BEFORE a single change is applied ───
        //
        // Everything below this point may touch storage, the WAL, a counter or
        // a cursor. Everything above it must not. A batch addressed to another
        // instance, or one whose own metadata is inconsistent, is refused with
        // zero side effects — which is what lets a sender treat `PeerChanged`
        // as "nothing happened" rather than "something happened, somewhere".
        if request.target_instance != self.instance_id {
            warn!(
                addressed = %request.target_instance,
                responder = %self.instance_id,
                "Refusing a push addressed to another instance; nothing applied"
            );
            return Ok(WireReply::peer_changed(
                self.instance_id,
                request.target_instance,
            ));
        }
        if let Err(detail) = validate_batch_metadata(&request) {
            return Ok(WireReply::rejected(
                self.instance_id,
                WireErrorCode::InvalidRequest,
                detail,
            ));
        }
        // Preflight the acknowledgement's capacity against the sender's stated
        // budget. Applying first and discovering afterwards that the answer
        // cannot be sent would leave the sender unable to learn what happened —
        // which is exactly the state a cursor must never be derived from.
        self.preflight_reply(request.reply_limit_bytes)?;

        let source = request.source_instance;
        let applier = RemoteChangeApplier::new(Arc::clone(&self.db), self.config.clone());
        let result = applier.apply_batch(request.changes)?;
        result.record_into(&self.stats);

        debug!(
            applied = result.applied,
            skipped = result.skipped,
            failed = result.failed,
            conflicts = result.conflicts,
            skewed_timestamps = result.skewed_timestamps,
            "Handled push"
        );

        let total = (result.applied + result.skipped) as u64;
        let rejected = result.failed as u64;
        Ok(WireReply::ok(
            self.instance_id,
            PushAck {
                wal_owner: source,
                // Accepted is applied plus GENUINE idempotent skips. The
                // applier counts a failure in `skipped` too, so the failures
                // are subtracted out rather than double-counted as acceptance.
                accepted: total.saturating_sub(rejected),
                rejected,
                total,
                safe_through: result.safe_through,
            },
        ))
    }

    /// Handles a routed pull request — serves local changes to the remote peer.
    ///
    /// `has_more` is a claim about the WAL, not about the batch: it is false
    /// only when this pull proved the WAL exhausted, which one poll can only do
    /// by coming back SHORT of [`PULL_PAGE_EVENT_LIMIT`].
    #[instrument(skip(self, request))]
    pub fn handle_pull(&self, request: PullRequest) -> Result<WireReply<PullPage>, SyncError> {
        if request.protocol_version != SYNC_PROTOCOL_VERSION {
            return Ok(WireReply::rejected(
                self.instance_id,
                WireErrorCode::ProtocolVersion,
                format!(
                    "server speaks protocol v{SYNC_PROTOCOL_VERSION}, request declared v{}",
                    request.protocol_version
                ),
            ));
        }
        if request.target_instance != self.instance_id {
            warn!(
                addressed = %request.target_instance,
                responder = %self.instance_id,
                "Refusing a pull addressed to another instance; nothing served"
            );
            return Ok(WireReply::peer_changed(
                self.instance_id,
                request.target_instance,
            ));
        }
        // A zero-count pull is refused on its own terms, INDEPENDENTLY of the
        // byte budget: it asks for nothing and could only ever stall a caller.
        if request.batch_size == 0 {
            return Ok(WireReply::rejected(
                self.instance_id,
                WireErrorCode::InvalidRequest,
                "pull requested zero changes",
            ));
        }
        // And an unusable reply budget is refused on ITS own terms: a requester
        // that cannot receive a bounded control frame cannot receive any answer,
        // so serving one would be pointless work followed by a failed read.
        if request.reply_limit_bytes < MIN_CONTROL_FRAME_BYTES as u64 {
            return Ok(WireReply::rejected(
                self.instance_id,
                WireErrorCode::InvalidRequest,
                format!(
                    "pull declared a {}-byte reply limit, below the \
                     {MIN_CONTROL_FRAME_BYTES}-byte control minimum",
                    request.reply_limit_bytes
                ),
            ));
        }
        if request.cursor.instance_id != self.instance_id {
            return Ok(WireReply::rejected(
                self.instance_id,
                WireErrorCode::InvalidRequest,
                format!(
                    "pull cursor names WAL owner {} but this instance is {}",
                    request.cursor.instance_id, self.instance_id
                ),
            ));
        }
        let batch_size = usize::try_from(request.batch_size).unwrap_or(usize::MAX);
        // The effective reply budget: the smaller of what the requester says it
        // can read and this server's own policy. Neither side's number alone is
        // the truth.
        let reply_cap = usize::try_from(request.reply_limit_bytes)
            .unwrap_or(usize::MAX)
            .min(self.config.max_request_bytes);

        let storage = self.db.storage_for_test();
        let mut poller = ChangePoller::from_sequence(request.cursor.sequence)
            .with_batch_limit(PULL_PAGE_EVENT_LIMIT);

        let events = poller
            .poll_sync_events(storage)
            .map_err(|e| SyncError::transport(format!("Failed to poll WAL for pull: {}", e)))?;

        // ─── The ordered scan, and its own position ──────────────────
        //
        // `scanned` is the last event this pull actually READ before the first
        // eligible change it could not include. Two kinds of event advance it:
        // one the `collectives` filter excludes, and one that no longer
        // resolves to an entity (deleted since its WAL event). Both are events
        // this peer will never be sent, so moving past them is progress, not a
        // skip — that is the #90 repair. A database error is NOT one of these:
        // `build_change_from_record` propagates it rather than classifying it
        // as an intentional skip.
        //
        // An eligible change that does not fit — by count or by bytes — does
        // NOT advance it. The position belongs to the prefix that is emitted,
        // never to the poller's eager end.
        //
        // Neither does a metadata-only advance whose complete reply would not
        // fit. The position travels INSIDE the frame as a postcard varint, so
        // walking a filtered run across 127 → 128 lengthens the very reply that
        // was already packed to its cap. `advance_scan_within_cap` commits such
        // a step only against the complete candidate frame; when it refuses,
        // the fitting prefix is returned with `has_more: true` and the withheld
        // tail is the next pull's first work.
        let envelope_at = |scan: u64| -> Result<usize, SyncError> {
            wire::encoded_len(&WireReply::ok(
                self.instance_id,
                PullPage {
                    changes: Vec::new(),
                    has_more: true,
                    scan_position: SyncPosition::new(self.instance_id, scan),
                },
            ))
        };
        let mut sizer = wire::FrameSizer::new(envelope_at(request.cursor.sequence)?);
        let mut changes: Vec<SyncChange> = Vec::new();
        let mut scanned = request.cursor.sequence;
        let mut truncated = false;
        let mut too_large: Option<(u64, usize)> = None;

        for (sequence, record) in &events {
            let change =
                match build_change_from_record(&self.db, *sequence, record, self.instance_id)? {
                    Some(change) => change,
                    None => {
                        if !advance_scan_within_cap(&mut sizer, &mut scanned, *sequence, reply_cap)?
                        {
                            truncated = true;
                            break;
                        }
                        continue;
                    }
                };
            if let Some(ref allowed) = request.collectives {
                if !allowed.contains(&change.collective_id) {
                    if !advance_scan_within_cap(&mut sizer, &mut scanned, *sequence, reply_cap)? {
                        truncated = true;
                        break;
                    }
                    continue;
                }
            }

            if changes.len() >= batch_size {
                truncated = true;
                break;
            }
            // Size the COMPLETE candidate frame — this change's own scan
            // position included, because that varint is part of the frame — and
            // commit nothing until it fits. A sizer rebased ahead of the
            // decision would describe a frame the handler does not emit, which
            // is the same class of mistake as running past the measured
            // position in the first place.
            let mut candidate = sizer;
            candidate.rebase(envelope_at(*sequence)?);
            let item = wire::item_len(&change)?;
            if candidate.len_with(item) > reply_cap {
                if changes.is_empty() && scanned <= request.cursor.sequence {
                    // Nothing fits, and no safe filtered progress precedes it:
                    // this one change cannot be served at all.
                    too_large = Some((change.sequence, candidate.len_with(item)));
                }
                truncated = true;
                break;
            }
            candidate.push(item);
            sizer = candidate;
            changes.push(change);
            scanned = *sequence;
        }

        if let Some((sequence, needed)) = too_large {
            warn!(
                sequence,
                needed,
                cap = reply_cap,
                "A single change cannot fit the pull reply budget"
            );
            return Ok(WireReply {
                protocol_version: SYNC_PROTOCOL_VERSION,
                responder: self.instance_id,
                result: super::types::WireResult::ChangeTooLarge {
                    sequence,
                    needed: needed as u64,
                    cap: reply_cap as u64,
                },
            });
        }

        // Exhaustion is a property of the SCAN, never of the emitted batch.
        // Truncation by count, by bytes, or by a withheld metadata-only advance
        // means more is available by construction — and the withheld case is
        // the one that MUST be reported. A page stopped there is typically
        // SHORT, so `events.len() >= PULL_PAGE_EVENT_LIMIT` is false and
        // nothing else would contradict a claim of exhaustion; the client would
        // persist the held-back position, take `initial_sync`'s `!has_more`
        // exit, and report a catch-up complete over an eligible change it never
        // received.
        //
        // Otherwise the only evidence the WAL holds nothing beyond this page is
        // a poll that came back SHORT of its limit; a FULL page may be followed
        // by more whatever the change count says.
        //
        // Over-reporting is the safe direction: the follow-up pull comes back
        // empty with `has_more: false`, the ordinary caught-up end that
        // `initial_sync` already handles. Under-reporting is a catch-up that
        // claims completion over a peer that still holds events.
        let has_more = truncated || events.len() >= PULL_PAGE_EVENT_LIMIT;

        Ok(WireReply::ok(
            self.instance_id,
            PullPage {
                changes,
                has_more,
                scan_position: SyncPosition::new(self.instance_id, scanned),
            },
        ))
    }

    /// Handles a health check.
    ///
    /// **Liveness only.** It proves something is listening and the store is
    /// readable; it is not identity evidence and no caller may treat it as
    /// confirmation that the peer behind the address is unchanged.
    pub fn handle_health(&self) -> Result<(), SyncError> {
        // Verify DB is accessible by reading metadata
        let _seq = self
            .db
            .get_current_sequence()
            .map_err(|e| SyncError::transport(format!("Health check failed: {}", e)))?;
        Ok(())
    }

    // ─── Byte-level handlers (framed postcard in/out) ────────────────

    /// Handles a handshake from raw wire bytes.
    pub fn handle_handshake_bytes(&self, body: &[u8]) -> Result<Vec<u8>, SyncError> {
        let request: HandshakeRequest = self.decode(WireOperation::Handshake, body)?;
        let response = self.handle_handshake(request)?;
        self.encode(WireOperation::Handshake, &response)
    }

    /// Handles a push from raw wire bytes.
    pub fn handle_push_bytes(&self, body: &[u8]) -> Result<Vec<u8>, SyncError> {
        let request: PushRequest = self.decode(WireOperation::Push, body)?;
        let response = self.handle_push(request)?;
        self.encode(WireOperation::Push, &response)
    }

    /// Handles a pull from raw wire bytes.
    pub fn handle_pull_bytes(&self, body: &[u8]) -> Result<Vec<u8>, SyncError> {
        let request: PullRequest = self.decode(WireOperation::Pull, body)?;
        let response = self.handle_pull(request)?;
        self.encode(WireOperation::Pull, &response)
    }

    /// Byte cap, frame header, then an exact decode — in that order, before
    /// anything else looks at the body.
    fn decode<T: serde::de::DeserializeOwned>(
        &self,
        operation: WireOperation,
        body: &[u8],
    ) -> Result<T, SyncError> {
        let max = self.config.max_request_bytes;
        if body.len() > max {
            warn!(
                size = body.len(),
                max_request_bytes = max,
                "Refusing oversized sync request before decode"
            );
        }
        wire::decode_bounded(operation, body, max)
    }

    fn encode<T: serde::Serialize>(
        &self,
        operation: WireOperation,
        value: &T,
    ) -> Result<Vec<u8>, SyncError> {
        wire::encode_bounded(operation, value, self.config.max_request_bytes)
    }

    /// Refuses a request whose stated reply budget cannot hold this server's
    /// answer, **before** the request is acted on.
    ///
    /// The effective budget is `min(what the requester says it can read, this
    /// server's own policy)`. Every reply this checks is a bounded control
    /// frame, so the worst case is computable rather than estimated.
    fn preflight_reply(&self, requester_limit: u64) -> Result<(), SyncError> {
        let requester = usize::try_from(requester_limit).unwrap_or(usize::MAX);
        let budget = requester.min(self.config.max_request_bytes);
        if budget < MIN_CONTROL_FRAME_BYTES {
            return Err(SyncError::PayloadTooLarge {
                size: MIN_CONTROL_FRAME_BYTES,
                max: budget,
            });
        }
        Ok(())
    }
}

/// Moves a pull's reported scan position to `sequence` — but only if the
/// COMPLETE reply carrying it still fits `cap`.
///
/// Advancing past an event this peer will never be sent is progress rather than
/// a skip (#90). The position that progress reports, however, travels INSIDE
/// the reply as a postcard varint, and its width grows at 127 → 128,
/// 16 383 → 16 384, … So a page already packed to its cap can be pushed over it
/// by an advance that adds no change at all: `encode_bounded` then refuses the
/// server's own reply, no page is served, the cursor does not move, the next
/// request is byte-identical, and `PayloadTooLarge` is not a terminal
/// `ChangeTooLarge` — so the sender retries a body already known not to fit,
/// indefinitely.
///
/// Returns whether the advance was committed. On `false` the caller holds the
/// last size-validated position, emits the fitting prefix with
/// `has_more: true`, and the withheld tail becomes the next pull's first work.
/// That pull starts with an empty change set, and an empty reply fits the 1 KiB
/// control minimum even at `u64::MAX`, so it always carries the position across
/// the boundary. This can therefore only fire behind a NON-EMPTY prefix: it
/// cannot produce a page that reports more while advancing nothing.
///
/// The scan is ordered, so the candidate position never moves backwards and the
/// envelope can only widen — by exactly `varint_len(b) − varint_len(a)`, the
/// identity [`wire::FrameSizer`] is already built on rather than a second
/// hand-maintained size formula. Nothing is committed until the candidate fits:
/// a sizer left describing a frame the handler does not emit is precisely the
/// defect this repairs.
fn advance_scan_within_cap(
    sizer: &mut wire::FrameSizer,
    scanned: &mut u64,
    sequence: u64,
    cap: usize,
) -> Result<bool, SyncError> {
    let widen = wire::varint_len(sequence)
        .checked_sub(wire::varint_len(*scanned))
        .ok_or_else(|| {
            SyncError::serialization(format!(
                "pull scan moved backwards, from {} to {sequence}",
                *scanned
            ))
        })?;
    let envelope = sizer.envelope().checked_add(widen).ok_or_else(|| {
        SyncError::serialization("pull reply envelope overflows the host's usize".to_string())
    })?;
    let mut candidate = *sizer;
    candidate.rebase(envelope);
    if candidate.len() > cap {
        return Ok(false);
    }
    *sizer = candidate;
    *scanned = sequence;
    Ok(true)
}

/// Refuses a push batch whose own metadata is inconsistent.
///
/// Three ways a batch can be untrustworthy before it is applied:
///
/// - a change claiming a `source_instance` other than the request's declared
///   sender — the sequences in it index a WAL nobody in this exchange owns;
/// - sequence `0`, which is below the first WAL sequence and would let an
///   acknowledgement name a position that cannot exist;
/// - a repeated sequence, which makes "the highest sequence at or below which
///   everything succeeded" ambiguous.
///
/// Order is deliberately NOT constrained: a peer chooses its batch's order, and
/// the applier's failure-floor rule is by sequence rather than by position
/// precisely so that it does not have to be.
fn validate_batch_metadata(request: &PushRequest) -> Result<(), String> {
    use std::collections::BTreeSet;

    let mut seen: BTreeSet<u64> = BTreeSet::new();
    for change in &request.changes {
        if change.source_instance != request.source_instance {
            return Err(format!(
                "change {} claims source {} but the request declares {}",
                change.sequence, change.source_instance, request.source_instance
            ));
        }
        if change.sequence == 0 {
            return Err("a change carries sequence 0, below the first WAL sequence".to_string());
        }
        if !seen.insert(change.sequence) {
            return Err(format!(
                "sequence {} appears more than once",
                change.sequence
            ));
        }
    }
    Ok(())
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
                // `.into()` MOVES the loaded record's vector into the wire
                // wrapper: the record still serializes without it, and the
                // vector travels beside it (#96).
                .map(|exp| SyncPayload::ExperienceCreated(exp.into()))
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
                        tags: Some(exp.tags.clone()),
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

    use tempfile::tempdir;

    use crate::Config;

    #[test]
    fn test_sync_server_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<SyncServer>();
    }

    fn open_db() -> (Arc<PulseDB>, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db = Arc::new(PulseDB::open(dir.path().join("server.db"), Config::default()).unwrap());
        (db, dir)
    }

    /// The constructor validates before it builds. A zero `batch_size` and a
    /// cap under the control minimum both fail HERE, not on the first request.
    #[test]
    fn recovery_v5_server_new_refuses_an_unusable_configuration() {
        let (db, _dir) = open_db();

        let err = SyncServer::new(
            Arc::clone(&db),
            SyncConfig {
                batch_size: 0,
                ..SyncConfig::default()
            },
        )
        .err()
        .expect("a zero batch_size is refused at construction");
        assert!(err.is_config(), "got {err}");
        assert!(err.to_string().contains("batch_size"), "{err}");

        let err = SyncServer::new(
            Arc::clone(&db),
            SyncConfig {
                max_request_bytes: MIN_CONTROL_FRAME_BYTES - 1,
                ..SyncConfig::default()
            },
        )
        .err()
        .expect("a cap under the control minimum is refused at construction");
        assert!(err.is_config(), "got {err}");

        let server = SyncServer::new(db, SyncConfig::default()).expect("defaults are usable");
        assert_eq!(
            server.receive_limit_bytes(),
            SyncConfig::default().max_request_bytes
        );
    }

    /// A protocol-v4 body carries no frame on the data endpoints, so each
    /// byte handler refuses it before the decoder — with no prior handshake.
    #[test]
    fn recovery_v5_data_endpoints_refuse_an_unframed_v4_body() {
        let (db, _dir) = open_db();
        let server = SyncServer::new(db, SyncConfig::default()).unwrap();

        let legacy_pull = postcard::to_allocvec(&PullRequest {
            protocol_version: 4,
            source_instance: InstanceId::new(),
            target_instance: server.instance_id(),
            cursor: SyncPosition::new(server.instance_id(), 0),
            batch_size: 10,
            reply_limit_bytes: 1024,
            collectives: None,
        })
        .unwrap();
        let err = server.handle_pull_bytes(&legacy_pull).unwrap_err();
        assert!(err.is_wire_format_mismatch(), "got {err}");
        assert!(err.is_protocol_incompatible(), "got {err}");

        let legacy_push = postcard::to_allocvec(&Vec::<SyncChange>::new()).unwrap();
        let err = server.handle_push_bytes(&legacy_push).unwrap_err();
        assert!(err.is_wire_format_mismatch(), "got {err}");
    }

    /// A well-formed frame for the wrong endpoint is refused before decode.
    #[test]
    fn recovery_v5_endpoints_refuse_a_frame_for_another_operation() {
        let (db, _dir) = open_db();
        let server = SyncServer::new(db, SyncConfig::default()).unwrap();

        let push_frame = wire::encode_bounded(
            WireOperation::Push,
            &PushRequest {
                protocol_version: SYNC_PROTOCOL_VERSION,
                source_instance: InstanceId::new(),
                target_instance: server.instance_id(),
                reply_limit_bytes: 4096,
                changes: Vec::new(),
            },
            usize::MAX,
        )
        .unwrap();
        let err = server.handle_pull_bytes(&push_frame).unwrap_err();
        assert!(err.is_wire_operation_mismatch(), "got {err}");
    }

    /// Trailing bytes after an exact body are refused rather than ignored.
    #[test]
    fn recovery_v5_endpoints_refuse_trailing_bytes() {
        let (db, _dir) = open_db();
        let server = SyncServer::new(db, SyncConfig::default()).unwrap();

        let mut frame = wire::encode_bounded(
            WireOperation::Handshake,
            &HandshakeRequest {
                instance_id: InstanceId::new(),
                protocol_version: SYNC_PROTOCOL_VERSION,
                capabilities: vec!["push".into()],
            },
            usize::MAX,
        )
        .unwrap();
        frame.extend_from_slice(&[0u8; 8]);
        let err = server.handle_handshake_bytes(&frame).unwrap_err();
        assert!(
            matches!(err, SyncError::Serialization(ref m) if m.contains("trailing")),
            "got {err}"
        );
    }

    /// The handshake advertises the server's ACTUAL inbound cap.
    #[test]
    fn recovery_v5_handshake_advertises_the_server_inbound_limit() {
        let (db, _dir) = open_db();
        let cap = 2 * 1024 * 1024;
        let server = SyncServer::new(
            db,
            SyncConfig {
                max_request_bytes: cap,
                ..SyncConfig::default()
            },
        )
        .unwrap();

        let response = server
            .handle_handshake(HandshakeRequest {
                instance_id: InstanceId::new(),
                protocol_version: SYNC_PROTOCOL_VERSION,
                capabilities: vec![],
            })
            .unwrap();
        assert!(response.accepted);
        assert_eq!(response.receive_limit_bytes, cap as u64);
    }

    /// A handshake whose capability list is outside the wire bounds is refused,
    /// because the control-frame budget is a guarantee only over bounded
    /// messages.
    #[test]
    fn recovery_v5_handshake_bounds_are_enforced() {
        let (db, _dir) = open_db();
        let server = SyncServer::new(db, SyncConfig::default()).unwrap();

        let err = server
            .handle_handshake(HandshakeRequest {
                instance_id: InstanceId::new(),
                protocol_version: SYNC_PROTOCOL_VERSION,
                capabilities: (0..super::super::types::MAX_HANDSHAKE_CAPABILITIES + 1)
                    .map(|i| format!("cap-{i}"))
                    .collect(),
            })
            .unwrap_err();
        assert!(matches!(err, SyncError::InvalidPayload(_)), "got {err}");
    }
}
