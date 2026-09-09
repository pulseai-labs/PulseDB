//! Sync manager — orchestrates sync lifecycle between PulseDB instances.
//!
//! [`SyncManager`] is the public API for sync. It manages:
//! - Handshake negotiation with the remote peer, including its inbound budget
//! - Background push/pull loops on configured intervals
//! - Manual one-shot sync via [`sync_once()`](SyncManager::sync_once)
//! - Initial catchup sync with progress callback
//! - Error recovery with exponential backoff, and a terminal stop on a
//!   deterministic failure no retry can fix
//! - Graceful shutdown

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, RwLock};

use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, instrument, warn};

use crate::db::PulseDB;

use super::applier::RemoteChangeApplier;
use super::config::{SyncConfig, SyncDirection};
use super::error::SyncError;
use super::progress::{next_progress, SyncProgressCallback};
use super::pusher::{LocalChangePusher, PushOutcome};
use super::transport::SyncTransport;
use super::types::{
    HandshakeRequest, InstanceId, PullPage, PullRequest, SyncPosition, SyncStats, SyncStatus,
};
use super::wire::MIN_CONTROL_FRAME_BYTES;
use super::{SYNC_CAPABILITY_GCOUNTER_APPLICATIONS, SYNC_PROTOCOL_VERSION};

/// What this session knows about the peer behind the endpoint.
///
/// Not just an id: the peer's advertised inbound body cap is bound at the same
/// moment and by the same exchange, because both are properties of *that*
/// instance. A replacement peer has its own cursors **and** its own budget, so
/// carrying the previous one's cap forward would pack bodies against a limit
/// the new endpoint never agreed to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PeerBinding {
    /// The peer's instance id — the key every cursor row is filed under.
    identity: InstanceId,
    /// The peer's own inbound body cap, from its handshake.
    receive_budget: usize,
}

/// Orchestrator for sync operations between two PulseDB instances.
///
/// The SyncManager is a **sidecar** — it holds `Arc<PulseDB>` but doesn't
/// wrap it. Local database operations are completely unaffected by sync state.
///
/// # Peer identity is bound to every exchange, not cached for the session
///
/// Every sync position is keyed on the peer's [`InstanceId`], so a manager that
/// trusted the handshake's answer for the life of the session would sync a
/// *different* peer against the previous one's cursors. That is not
/// hypothetical: [`PulseDB::remint_instance_id`] exists precisely to give a
/// restored file copy a fresh identity, so an endpoint restored from an older
/// snapshot comes back as a different peer holding **less** data.
///
/// Under protocol v5 the check does not depend on which message happens to
/// carry the peer's id. Every request names the `target_instance` it is for,
/// and every reply names the `responder` that produced it:
///
/// - a request that reaches a different instance is refused by that instance
///   **before anything applies** — no storage write, no statistic, no WAL
///   event, no cursor movement;
/// - a reply from an unexpected responder is refused here, whatever it says.
///
/// So a push is protected on its own terms rather than by the pull that ran
/// before it. Pull-before-push is retained because confirming the identity
/// before writing anything is still the cheaper order, but it is no longer what
/// makes the cycle safe: an endpoint replaced *between* the two is caught by
/// the push request's own target check.
///
/// # What a detected change does
///
/// Discard that exchange's result, handshake again, reload the **new**
/// identity's own cursors (absent meaning `0`, which re-pushes from the start),
/// rebuild the pusher, and retry. A re-push of changes the peer already holds
/// is absorbed by the applier's idempotent skip path; skipping changes it is
/// *missing* is silent data loss, so `0` is the conservative answer for a peer
/// whose contents cannot be known.
///
/// **One rebind is permitted per cycle**, and per `initial_sync` run — shared
/// across the pull and the push, not one each. An identity that changes twice
/// in one cycle is flapping (or two instances share one address), and
/// re-establishing on every answer would be an unbounded loop of handshakes
/// that never syncs.
///
/// **The previous identity's cursor row is retained, never deleted.** Deleting
/// it would be a data decision this manager is not entitled to make: the old
/// identity may legitimately return (a rolled-back snapshot, a second replica
/// restored from a copy taken before the remint), and its row is the only
/// record of what that peer was sent. Retaining it is also the safe direction
/// for compaction — [`PulseDB::compact_wal`] takes the *minimum*
/// `push_sequence` over all known peers, so an extra row can only hold
/// compaction back, never release it. Earlier valid work for the old peer is
/// not rolled back.
///
/// # PushOnly is covered
///
/// A [`SyncDirection::PushOnly`](super::config::SyncDirection) manager, and any
/// manager whose cursor already sits at the WAL head or whose whole page was
/// filtered, still sends a bounded **empty routed push** so the endpoint's
/// identity is checked. A health check is not identity evidence: it says
/// something is listening, not who.
///
/// # Identity precondition
///
/// The local instance id is read once, at construction.
/// [`PulseDB::remint_instance_id`] must run **before** the manager and any
/// local server are built. Reminting a live instance, and unreminted same-id
/// clones, remain outside the lifecycle contract. Recovery replays only WAL
/// history still retained: history compacted before an endpoint was restored
/// cannot be resurrected.
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
/// let mut manager = SyncManager::new(db, Box::new(local_transport), SyncConfig::default())?;
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
    /// The peer this session is bound to, or `None` before the first
    /// handshake. Re-established whenever a reply reveals a different
    /// responder.
    peer: Option<PeerBinding>,
    status: Arc<RwLock<SyncStatus>>,
    stats: Arc<Mutex<SyncStats>>,
    /// Replaced on every [`start`](Self::start): a `Notify` permit stored by a
    /// `stop()` whose task had already exited would otherwise stop the NEXT
    /// task the instant it starts.
    shutdown: Arc<Notify>,
    task_handle: Option<JoinHandle<()>>,
}

impl SyncManager {
    /// Creates a new SyncManager.
    ///
    /// Does NOT start sync — call [`start()`](Self::start) or
    /// [`sync_once()`](Self::sync_once) to begin.
    ///
    /// # Errors
    ///
    /// [`SyncError::Config`] when the configuration is invalid, or when the
    /// **effective control budget** — `min(config.max_request_bytes,
    /// transport.receive_limit_bytes())` — is below
    /// [`MIN_CONTROL_FRAME_BYTES`]. A manager whose transport cannot carry a
    /// maximum-sized bounded control frame fails on every exchange; failing at
    /// the call that made it says so once, instead of once a cycle forever.
    ///
    /// **0.8.0 source break:** this used to return `Self`. Existing callers add
    /// `?` or an `expect`.
    ///
    /// # Identity precondition
    ///
    /// The local instance id is read once, here: a
    /// [`PulseDB::remint_instance_id`] after this point is not observed by this
    /// manager.
    pub fn new(
        db: Arc<PulseDB>,
        transport: Box<dyn SyncTransport>,
        config: SyncConfig,
    ) -> Result<Self, SyncError> {
        config
            .validate()
            .map_err(|e| SyncError::config(e.to_string()))?;
        let transport: Arc<dyn SyncTransport> = Arc::from(transport);
        let effective = config
            .max_request_bytes
            .min(transport.receive_limit_bytes());
        if effective < MIN_CONTROL_FRAME_BYTES {
            return Err(SyncError::config(format!(
                "effective control budget {effective} bytes (min of max_request_bytes {} and the \
                 transport's receive limit {}) is below the {MIN_CONTROL_FRAME_BYTES}-byte minimum",
                config.max_request_bytes,
                transport.receive_limit_bytes()
            )));
        }
        let local_instance_id = db.instance_id();
        Ok(Self {
            db,
            transport,
            config,
            local_instance_id,
            peer: None,
            status: Arc::new(RwLock::new(SyncStatus::Idle)),
            stats: Arc::new(Mutex::new(SyncStats::default())),
            shutdown: Arc::new(Notify::new()),
            task_handle: None,
        })
    }

    /// Starts the background sync loop.
    ///
    /// Performs a handshake with the remote peer, then spawns a background
    /// tokio task that pushes and pulls on the configured intervals.
    ///
    /// A task that stopped on a terminal error is **reaped** here, so an
    /// explicit restart after the operator corrected the cause works. The
    /// shutdown signal is replaced at the same time: a permit left by a `stop()`
    /// whose task had already exited would otherwise stop the new task at once.
    #[instrument(skip(self), fields(instance_id = %self.local_instance_id))]
    pub async fn start(&mut self) -> Result<(), SyncError> {
        if let Some(handle) = self.task_handle.take() {
            if handle.is_finished() {
                let _ = handle.await;
            } else {
                self.task_handle = Some(handle);
                return Err(SyncError::transport("SyncManager already started"));
            }
        }

        // A fresh handshake on every start, not the cached binding: `start()`
        // is the explicit restart an operator reaches for after correcting a
        // terminal condition, and the peer's advertised inbound budget is part
        // of what has to be picked up again.
        let binding = Self::handshake_with(
            &self.transport,
            self.local_instance_id,
            self.config.max_request_bytes,
        )
        .await?;
        self.peer = Some(binding);

        self.set_status(SyncStatus::Syncing);

        // A fresh signal per run — see the field docs.
        self.shutdown = Arc::new(Notify::new());

        // Clone everything needed for the background task
        let db = Arc::clone(&self.db);
        let transport = Arc::clone(&self.transport);
        let config = self.config.clone();
        let local_id = self.local_instance_id;
        let status = Arc::clone(&self.status);
        let stats = Arc::clone(&self.stats);
        let shutdown = Arc::clone(&self.shutdown);

        let handle = tokio::spawn(async move {
            Self::background_loop(
                db, transport, config, local_id, binding, status, stats, shutdown,
            )
            .await;
        });

        self.task_handle = Some(handle);
        info!("SyncManager started");
        Ok(())
    }

    /// Stops the background sync loop.
    ///
    /// A task that already exited on a terminal error is reaped without
    /// disturbing the recorded [`SyncStatus::Error`], so the reason survives
    /// the stop.
    #[instrument(skip(self))]
    pub async fn stop(&mut self) -> Result<(), SyncError> {
        if let Some(handle) = self.task_handle.take() {
            let already_finished = handle.is_finished();
            if !already_finished {
                self.shutdown.notify_one();
            }
            handle
                .await
                .map_err(|e| SyncError::transport(format!("Background task panicked: {}", e)))?;
            if !already_finished {
                self.set_status(SyncStatus::Idle);
            }
            info!("SyncManager stopped");
        }
        Ok(())
    }

    /// Performs a single pull+push sync cycle (no background task needed).
    ///
    /// Useful for testing or manual sync triggers.
    ///
    /// # Ordering
    ///
    /// The pull runs first because confirming the peer before writing anything
    /// is the cheaper order: a cycle that discovers the endpoint was replaced
    /// persists nothing, re-establishes, and runs again against the new peer's
    /// own cursors. It is **not** what makes the cycle safe — the push request
    /// carries its own `target_instance`, so a replacement occurring *after*
    /// the pull is refused by the new endpoint before it can apply anything.
    #[instrument(skip(self))]
    pub async fn sync_once(&mut self) -> Result<SyncStatus, SyncError> {
        let mut binding = self.bound_peer().await?;

        self.set_status(SyncStatus::Syncing);

        let applier = RemoteChangeApplier::new(Arc::clone(&self.db), self.config.clone());
        let result = Self::run_sync_cycle(
            &applier,
            &self.transport,
            &self.db,
            &self.config,
            self.local_instance_id,
            &mut binding,
            &self.stats,
        )
        .await;
        // The binding is written back whatever happened: a rebind that occurred
        // before the failure is still the truth about the endpoint.
        self.peer = Some(binding);
        if let Err(e) = result {
            // A deterministic dead end is recorded, not just returned: a caller
            // that polls `status()` must see the same terminal condition the
            // background loop would record. Both size failures qualify — one
            // change that can never be sent, or a request this side built that
            // the peer will never read — because retrying either rebuilds a
            // body already known not to fit. Anything else is transient and the
            // manager is simply idle again.
            self.set_status(if e.is_change_too_large() || e.is_request_too_large() {
                SyncStatus::Error(e.to_string())
            } else {
                SyncStatus::Idle
            });
            return Err(e);
        }

        self.set_status(SyncStatus::Idle);
        debug!("sync_once complete");
        Ok(SyncStatus::Idle)
    }

    /// Performs initial sync — pulls all remote changes in batches.
    ///
    /// Call this before `start()` to catch up from a cold start.
    ///
    /// # Errors
    ///
    /// `Ok(())` means the catch-up COMPLETED: a page the peer reported as its
    /// last was pulled, and every change in the run applied or was idempotently
    /// skipped. Anything short of that is
    /// [`SyncError::CatchUpIncomplete`] — the peer stalled (it reported more
    /// changes while handing back an unadvanced scan position), or a change was
    /// left unapplied. Both leave the pull position persisted where the run
    /// stopped, so a later `initial_sync` or a background cycle resumes from
    /// there; neither is a reason to discard local state. Transport and
    /// handshake failures surface as their own variants, as before.
    ///
    /// "Left unapplied" is about the END of the run, not about attempts made
    /// along the way. This loop retries: an apply failure holds the position
    /// strictly below the failing sequence, so the next iteration re-requests
    /// that change, and a transient failure — a storage error, a contended lock
    /// — applies on the retry. Only a sequence still ABOVE the final pull
    /// position was never applied; the position is inclusive, so one at or
    /// below it was handled. A catch-up that stumbled and recovered reports
    /// `Ok(())`.
    ///
    /// This strictness is specific to `initial_sync`, a one-shot "catch me up"
    /// call whose `Ok` a caller is entitled to trust. A single background cycle
    /// ([`sync_once`](Self::sync_once)) still returns `Ok` with a change left
    /// unapplied — there the next cycle retries it, which is the design.
    #[instrument(skip(self, progress))]
    pub async fn initial_sync(
        &mut self,
        progress: Option<Box<dyn SyncProgressCallback>>,
    ) -> Result<(), SyncError> {
        let mut binding = self.bound_peer().await?;

        self.set_status(SyncStatus::Syncing);

        let applier = RemoteChangeApplier::new(Arc::clone(&self.db), self.config.clone());

        let mut total_pulled = 0usize;
        let mut position = SyncPosition::new(
            binding.identity,
            Self::load_pull_sequence(&self.db, binding.identity)?,
        );
        // The SEQUENCES of the changes that ERRORED (never the idempotent
        // skips, which are ordinary successful re-sync outcomes). A catch-up
        // that left one of these behind did not catch up, whatever the peer said
        // about further pages.
        //
        // Sequences, not a count, because a failure here is an ATTEMPT and this
        // loop retries: the position stops strictly below the batch's lowest
        // failure, so the next iteration re-requests from there and receives the
        // failed change again. Which sequences failed is resolved at termination
        // against the final position (below).
        let mut failed_sequences: BTreeSet<u64> = BTreeSet::new();
        // Set when the loop stops on an unadvanced position that the peer
        // claimed to have more changes beyond.
        let mut stalled_with_more = false;
        // ONE rebind for the whole run, shared with nothing else. A peer whose
        // identity changes again is flapping, and re-establishing on every
        // answer would be an unbounded loop of handshakes that never catches up.
        let mut rebinds_left = 1u32;

        loop {
            let requested = position.sequence;
            let page = match Self::pull_page(
                &self.transport,
                &self.config,
                self.local_instance_id,
                &binding,
                requested,
            )
            .await
            {
                Ok(page) => page,
                Err(SyncError::PeerChanged { responder, .. }) => {
                    // This page was requested from the OLD peer's position, so
                    // everything about it is derived from a stale identity.
                    // Discard it whole, re-establish, restart from the NEW
                    // peer's own cursor.
                    if rebinds_left == 0 {
                        self.set_status(SyncStatus::Idle);
                        return Err(SyncError::handshake(format!(
                            "peer identity changed twice during one initial_sync \
                             (bound {}, now {responder}); refusing to keep re-establishing",
                            binding.identity
                        )));
                    }
                    rebinds_left -= 1;
                    binding = Self::reestablish_peer(
                        &self.transport,
                        self.local_instance_id,
                        binding.identity,
                        responder,
                        self.config.max_request_bytes,
                    )
                    .await?;
                    self.peer = Some(binding);
                    position = SyncPosition::new(
                        binding.identity,
                        Self::load_pull_sequence(&self.db, binding.identity)?,
                    );
                    // The failures recorded so far name sequences in the OLD
                    // peer's WAL; carried into the new peer's sequence space
                    // they would be compared against a position that has
                    // nothing to do with them.
                    failed_sequences.clear();
                    stalled_with_more = false;
                    continue;
                }
                Err(e) => {
                    self.set_status(SyncStatus::Idle);
                    return Err(e);
                }
            };

            let batch_size = page.changes.len();
            let scanned = page.scan_position.sequence;
            let has_more = page.has_more;

            // Advance only as far as the applier actually got. `apply_batch`
            // records a per-change failure instead of returning an error, so
            // storing the responder's position would step over a change that was
            // never applied and never pull it again.
            let next = if batch_size > 0 {
                let result = applier.apply_batch(page.changes)?;
                result.record_into(&self.stats);
                failed_sequences.extend(result.failed_sequences.iter().copied());
                next_progress(requested, scanned, result.failed, result.safe_through)
            } else {
                // An empty page advances to what the responder actually
                // scanned. That is the #90 repair: a page whose every event was
                // filtered is genuine progress, not a stall — provided the
                // responder is authoritative for its own scan position, which
                // it is.
                next_progress(requested, scanned, 0, None)
            };

            total_pulled += batch_size;
            // Keyed on the identity this exchange was addressed to and the
            // reply confirmed, NOT on an id read out of the response body.
            position = SyncPosition::new(binding.identity, next);

            // Save the pull position after each batch (crash-safe). Pull side
            // only — the push position is never touched here.
            Self::save_pull_position(&self.db, &position)?;

            if let Some(ref cb) = progress {
                cb.on_progress(batch_size, total_pulled, has_more);
            }

            // The position did not move, so the next request would be
            // byte-identical to this one. With the filtered-page repair in
            // place the remaining ways to get here are:
            //
            // - the responder has nothing at or after this position — the
            //   ordinary "already caught up" end of an initial sync (empty
            //   batch, `has_more: false`), which is not a fault; and
            // - a batch whose FIRST change failed to apply, which leaves the
            //   position exactly where it was.
            //
            // The second spins forever without this guard. Stop and let the
            // next sync retry from here.
            if next <= requested {
                if batch_size > 0 || has_more {
                    warn!(
                        peer = %binding.identity,
                        position = requested,
                        batch_size,
                        has_more,
                        "Initial sync stopped: the pull position did not advance \
                         (the batch could not be applied past its first change, or \
                         the peer returned no changes at this position)"
                    );
                }
                stalled_with_more = has_more;
                break;
            }

            if !has_more {
                break;
            }
        }

        // The status transition is the same one the success path makes: the
        // manager is no longer syncing either way, and a stopped catch-up must
        // not leave it wedged in `Syncing`.
        self.set_status(SyncStatus::Idle);

        // Stopping short is not completion. Both shapes leave the pull position
        // persisted where the run stopped, so a retry resumes from there.
        //
        // A failure only counts if it is STILL UNRESOLVED here. The pull
        // position is INCLUSIVE — a `safe_through`, the highest sequence at or
        // below which every change was applied, resolved or idempotently
        // skipped — so a failed sequence at or below where the run ended was
        // handled by a later attempt in this same run, and is also a sequence
        // this cursor will never fetch again. Only `sequence > position` is
        // outstanding.
        let unresolved = failed_sequences
            .iter()
            .filter(|sequence| **sequence > position.sequence)
            .count();
        if unresolved > 0 {
            return Err(SyncError::catch_up_apply_failed(
                binding.identity,
                position.sequence,
                unresolved,
            ));
        }
        if stalled_with_more {
            return Err(SyncError::catch_up_stalled(
                binding.identity,
                position.sequence,
            ));
        }

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

    /// Returns a snapshot of the local-only sync counters accumulated over
    /// every change this manager has applied (`sync_once`, `initial_sync`
    /// and the background loop alike).
    pub fn stats(&self) -> SyncStats {
        *self.stats.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The peer identity this session is bound to, if any.
    pub fn peer_instance_id(&self) -> Option<InstanceId> {
        self.peer.map(|b| b.identity)
    }

    // ─── Internal helpers ────────────────────────────────────────────

    /// The peer this session is bound to, handshaking once if it has none yet.
    async fn bound_peer(&mut self) -> Result<PeerBinding, SyncError> {
        match self.peer {
            Some(binding) => Ok(binding),
            None => {
                let binding = Self::handshake_with(
                    &self.transport,
                    self.local_instance_id,
                    self.config.max_request_bytes,
                )
                .await?;
                self.peer = Some(binding);
                Ok(binding)
            }
        }
    }

    /// Re-establishes the peer after a reply came back from a different
    /// responder, and returns the binding to use from here on.
    ///
    /// The observed id is treated as *evidence that the binding is stale*, not
    /// as the new binding: the identity is re-negotiated through a full
    /// handshake, so the protocol-version check runs again, the new peer's own
    /// inbound budget is learned, and the answer comes from the same exchange
    /// that established the first one. Whatever the handshake returns wins.
    ///
    /// The caller must have persisted **nothing** from the reply that triggered
    /// this, and must reload its cursors for the returned identity — an absent
    /// cursor means position `0`.
    ///
    /// The previous identity's cursor row is left exactly as it was.
    async fn reestablish_peer(
        transport: &Arc<dyn SyncTransport>,
        local_instance_id: InstanceId,
        previous: InstanceId,
        observed: InstanceId,
        send_budget_bytes: usize,
    ) -> Result<PeerBinding, SyncError> {
        warn!(
            previous = %previous,
            observed = %observed,
            "Sync peer identity changed (a remint, or a different instance behind \
             the same endpoint); re-handshaking and switching to the new peer's own cursor"
        );

        let binding = Self::handshake_with(transport, local_instance_id, send_budget_bytes).await?;
        if binding.identity != observed {
            debug!(
                observed = %observed,
                negotiated = %binding.identity,
                "Re-handshake returned an identity other than the one the reply reported"
            );
        }
        Ok(binding)
    }

    /// The handshake proper, reachable without `&self`.
    ///
    /// The background task holds no `&self`, and an identity re-established
    /// there must go through exactly the same negotiation — protocol-version
    /// check included — as the one `start()` performed.
    ///
    /// # The one budget that cannot be peer-derived
    ///
    /// Every other request is encoded against a budget the peer told this
    /// instance about. The handshake precedes the binding, so no such number
    /// exists yet and `send_budget_bytes` is local policy alone. That it will
    /// be accepted rests on **peer conformance**, not on anything this side can
    /// check: a `HandshakeRequest` is a bounded control frame certified to fit
    /// [`MIN_CONTROL_FRAME_BYTES`], and every conforming v5 server refuses a
    /// `max_request_bytes` below that minimum at construction, so a conforming
    /// peer always accepts it. A NON-conforming peer refuses remotely, exactly
    /// as it does today; local detection before the first exchange is not
    /// possible, and this is an assumption about the peer rather than a
    /// property this instance enforces.
    async fn handshake_with(
        transport: &Arc<dyn SyncTransport>,
        local_instance_id: InstanceId,
        send_budget_bytes: usize,
    ) -> Result<PeerBinding, SyncError> {
        let request = HandshakeRequest {
            instance_id: local_instance_id,
            protocol_version: SYNC_PROTOCOL_VERSION,
            capabilities: vec![
                "push".into(),
                "pull".into(),
                SYNC_CAPABILITY_GCOUNTER_APPLICATIONS.into(),
            ],
        };

        let response = transport.handshake(request, send_budget_bytes).await?;

        // Protocol-version check FIRST. A server that speaks a different
        // version answers with the soft `accepted: false` path carrying its
        // own version, so mapping the rejection generically before this check
        // would shadow the typed variant behind a reason string (#12).
        if response.protocol_version != SYNC_PROTOCOL_VERSION {
            warn!(
                peer = %response.instance_id,
                local = SYNC_PROTOCOL_VERSION,
                remote = response.protocol_version,
                "Sync handshake refused: protocol version mismatch"
            );
            return Err(SyncError::ProtocolVersion {
                local: SYNC_PROTOCOL_VERSION,
                remote: response.protocol_version,
            });
        }

        if !response.accepted {
            return Err(SyncError::handshake(
                response.reason.unwrap_or_else(|| "rejected".into()),
            ));
        }

        let receive_budget = usize::try_from(response.receive_limit_bytes).unwrap_or(usize::MAX);
        if receive_budget < MIN_CONTROL_FRAME_BYTES {
            return Err(SyncError::handshake(format!(
                "peer {} advertises a {receive_budget}-byte inbound limit, below the \
                 {MIN_CONTROL_FRAME_BYTES}-byte control minimum",
                response.instance_id
            )));
        }

        debug!(peer = %response.instance_id, receive_budget, "Handshake accepted");
        Ok(PeerBinding {
            identity: response.instance_id,
            receive_budget,
        })
    }

    fn set_status(&self, status: SyncStatus) {
        if let Ok(mut s) = self.status.write() {
            *s = status;
        }
    }

    /// The persisted push position for `peer_id` (0 if no cursor yet).
    ///
    /// An absent cursor is `0` — start of the WAL — which is the conservative
    /// answer for an identity this store has never synced with, a peer restored
    /// from an older snapshot included: re-sending changes it already holds is
    /// absorbed by the applier's idempotent skip path, while skipping changes it
    /// is missing is silent data loss.
    fn load_push_sequence(db: &PulseDB, peer_id: InstanceId) -> Result<u64, SyncError> {
        db.storage_for_test()
            .load_sync_cursor(&peer_id)
            .map_err(|e| SyncError::transport(format!("Failed to load cursor: {}", e)))
            .map(|opt| opt.map_or(0, |c| c.push_sequence))
    }

    /// The persisted pull position for `peer_id` (0 if no cursor yet).
    fn load_pull_sequence(db: &PulseDB, peer_id: InstanceId) -> Result<u64, SyncError> {
        db.storage_for_test()
            .load_sync_cursor(&peer_id)
            .map_err(|e| SyncError::transport(format!("Failed to load cursor: {}", e)))
            .map(|opt| opt.map_or(0, |c| c.pull_sequence))
    }

    /// Persists a pull position **under the identity the position itself
    /// names** (pull side only).
    ///
    /// Taking the whole [`SyncPosition`] rather than an id and a sequence is
    /// the point: a position and the key it is filed under can no longer be
    /// supplied separately, so filing one peer's position under another peer's
    /// row is not an argument a caller can get wrong.
    fn save_pull_position(db: &PulseDB, position: &SyncPosition) -> Result<(), SyncError> {
        db.storage_for_test()
            .update_pull_cursor(&position.instance_id, position.sequence)
            .map_err(|e| SyncError::transport(format!("Failed to save pull cursor: {}", e)))
    }

    /// The reply budget this manager can actually receive.
    ///
    /// `min(policy, what the transport will actually read)` — never a guessed
    /// configuration value. A number larger than the reader accepts turns a
    /// fitting reply into an unreadable one.
    fn reply_limit_bytes(config: &SyncConfig, transport: &Arc<dyn SyncTransport>) -> u64 {
        config
            .max_request_bytes
            .min(transport.receive_limit_bytes()) as u64
    }

    /// Issues one routed pull and validates the page before returning it.
    ///
    /// Everything checked here runs **before** a change is applied or a cursor
    /// moves: the responder (via [`WireReply::into_result`]), the WAL owner of
    /// the returned scan position, the requested count, and every change's
    /// ownership and sequence range. Foreign metadata is invalid payload, not
    /// evidence granting an arbitrary rebind — only a genuine
    /// [`SyncError::PeerChanged`] is that.
    ///
    /// [`WireReply::into_result`]: super::types::WireReply::into_result
    async fn pull_page(
        transport: &Arc<dyn SyncTransport>,
        config: &SyncConfig,
        local_id: InstanceId,
        binding: &PeerBinding,
        from: u64,
    ) -> Result<PullPage, SyncError> {
        let request = PullRequest {
            protocol_version: SYNC_PROTOCOL_VERSION,
            source_instance: local_id,
            target_instance: binding.identity,
            cursor: SyncPosition::new(binding.identity, from),
            batch_size: config.batch_size as u64,
            reply_limit_bytes: Self::reply_limit_bytes(config, transport),
            collectives: config.collectives.clone(),
        };

        // The OUTBOUND budget: what the bound peer will actually read, floored
        // by this instance's own policy. It is a different quantity from
        // `reply_limit_bytes` inside the request, which is the INBOUND budget
        // for the answer. Sending against local policy alone would relocate the
        // very defect this repairs to any deployment whose peer reads less.
        let send_budget = config.max_request_bytes.min(binding.receive_budget);
        let page = transport
            .pull_changes(request, send_budget)
            .await?
            .into_result(binding.identity)?;

        if page.scan_position.instance_id != binding.identity {
            return Err(SyncError::invalid_payload(format!(
                "pull page reports a scan position owned by {} but {} was addressed",
                page.scan_position.instance_id, binding.identity
            )));
        }
        if page.scan_position.sequence < from {
            return Err(SyncError::invalid_payload(format!(
                "pull page scan position {} is below the requested {from}",
                page.scan_position.sequence
            )));
        }
        if page.changes.len() > config.batch_size {
            return Err(SyncError::invalid_payload(format!(
                "pull page returned {} changes for a batch of {}",
                page.changes.len(),
                config.batch_size
            )));
        }
        let mut previous = from;
        for change in &page.changes {
            if change.source_instance != binding.identity {
                return Err(SyncError::invalid_payload(format!(
                    "pulled change {} claims source {} but {} was addressed",
                    change.sequence, change.source_instance, binding.identity
                )));
            }
            if change.sequence <= previous || change.sequence > page.scan_position.sequence {
                return Err(SyncError::invalid_payload(format!(
                    "pulled change {} is out of the requested range ({previous}, {}]",
                    change.sequence, page.scan_position.sequence
                )));
            }
            previous = change.sequence;
        }
        Ok(page)
    }

    /// One pull: request from `binding`'s stored position, apply, persist.
    async fn pull_step(
        db: &Arc<PulseDB>,
        transport: &Arc<dyn SyncTransport>,
        applier: &RemoteChangeApplier,
        config: &SyncConfig,
        local_id: InstanceId,
        binding: &PeerBinding,
        stats: &Mutex<SyncStats>,
    ) -> Result<usize, SyncError> {
        let pull_seq = Self::load_pull_sequence(db, binding.identity)?;
        let page = Self::pull_page(transport, config, local_id, binding, pull_seq).await?;

        let count = page.changes.len();
        let scanned = page.scan_position.sequence;

        // Advance only as far as the applier got: a change that failed to apply
        // must stay ahead of the stored position so the next pull fetches it
        // again.
        let next = if count > 0 {
            let result = applier.apply_batch(page.changes)?;
            result.record_into(stats);
            next_progress(pull_seq, scanned, result.failed, result.safe_through)
        } else {
            next_progress(pull_seq, scanned, 0, None)
        };
        // Persist on EVERY successful pull, empty batches included. The record
        // is what represents this peer in the cursor store, and `compact_wal`
        // needs to see its `push_sequence == 0` to stay blocked; a PullOnly
        // peer that never returns changes would otherwise have no record at
        // all, and compaction would delete events it has never been sent.
        Self::save_pull_position(db, &SyncPosition::new(binding.identity, next))?;

        Ok(count)
    }

    /// Background loop that runs push+pull on configured intervals.
    #[allow(clippy::too_many_arguments)]
    async fn background_loop(
        db: Arc<PulseDB>,
        transport: Arc<dyn SyncTransport>,
        config: SyncConfig,
        local_id: InstanceId,
        binding: PeerBinding,
        status: Arc<RwLock<SyncStatus>>,
        stats: Arc<Mutex<SyncStats>>,
        shutdown: Arc<Notify>,
    ) {
        // Rebound in place by `run_sync_cycle` when a reply reveals that the
        // endpoint is answering under a different identity.
        let mut binding = binding;

        let interval_ms = std::cmp::max(config.push_interval_ms, config.pull_interval_ms);
        let interval = tokio::time::Duration::from_millis(interval_ms);

        let mut consecutive_failures = 0u32;
        let max_retries = config.retry.max_retries;
        let initial_backoff = config.retry.initial_backoff_ms;
        let max_backoff = config.retry.max_backoff_ms;
        let multiplier = config.retry.backoff_multiplier;
        // Set when the loop stops on a failure that retrying cannot fix. The
        // recorded `Error` status is left in place on the way out.
        let mut terminal = false;

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
                    let applier = RemoteChangeApplier::new(Arc::clone(&db), config.clone());

                    let result = Self::run_sync_cycle(
                        &applier, &transport, &db, &config, local_id, &mut binding, &stats,
                    )
                    .await;

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
                        Err(e) if e.is_change_too_large() || e.is_request_too_large() => {
                            // Deterministic and terminal: the same change, or
                            // the same request, rebuilt next cycle is the same
                            // size against the same cap. Retrying would send a
                            // body already known not to fit, forever. Record it
                            // and stop; an explicit `start()` after the operator
                            // raises the cap — or narrows the filter — runs
                            // again.
                            error!("Sync stopped: {}", e);
                            if let Ok(mut s) = status.write() {
                                *s = SyncStatus::Error(e.to_string());
                            }
                            terminal = true;
                            break;
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

        if !terminal {
            if let Ok(mut s) = status.write() {
                *s = SyncStatus::Idle;
            }
        }
    }

    /// Execute one pull+push cycle. Used by `sync_once` and the background loop.
    ///
    /// `binding` is rebound **in place** when a reply reveals a different
    /// responder, which is what keeps a long-lived task from running the rest
    /// of its life against the identity it was spawned with.
    ///
    /// **One rebind for the whole cycle**, shared between the pull and the push.
    /// On a rebind the cycle restarts from the top against the new identity's
    /// own cursors — the pusher included, since a pusher built from the old
    /// peer's position would resume in the wrong place.
    #[allow(clippy::too_many_arguments)]
    async fn run_sync_cycle(
        applier: &RemoteChangeApplier,
        transport: &Arc<dyn SyncTransport>,
        db: &Arc<PulseDB>,
        config: &SyncConfig,
        local_id: InstanceId,
        binding: &mut PeerBinding,
        stats: &Mutex<SyncStats>,
    ) -> Result<(), SyncError> {
        let mut rebinds_left = 1u32;

        loop {
            // Pull
            if matches!(
                config.direction,
                SyncDirection::PullOnly | SyncDirection::Bidirectional
            ) {
                match Self::pull_step(db, transport, applier, config, local_id, binding, stats)
                    .await
                {
                    Ok(_) => {}
                    Err(SyncError::PeerChanged { responder, .. }) => {
                        if rebinds_left == 0 {
                            return Err(Self::flapping(binding.identity, responder));
                        }
                        rebinds_left -= 1;
                        *binding = Self::reestablish_peer(
                            transport,
                            local_id,
                            binding.identity,
                            responder,
                            config.max_request_bytes,
                        )
                        .await?;
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }

            // Push, from the position the confirmed identity has acknowledged,
            // against the budget that identity advertised.
            if matches!(
                config.direction,
                SyncDirection::PushOnly | SyncDirection::Bidirectional
            ) {
                let push_seq = Self::load_push_sequence(db, binding.identity)?;
                let mut pusher = LocalChangePusher::new(
                    Arc::clone(db),
                    Arc::clone(transport),
                    config.clone(),
                    local_id,
                    binding.identity,
                    binding.receive_budget,
                    push_seq,
                );
                match pusher.push_pending().await? {
                    PushOutcome::Pushed(count) => {
                        debug!(count, peer = %binding.identity, "Push cycle complete");
                    }
                    PushOutcome::PeerChanged(responder) => {
                        if rebinds_left == 0 {
                            return Err(Self::flapping(binding.identity, responder));
                        }
                        rebinds_left -= 1;
                        *binding = Self::reestablish_peer(
                            transport,
                            local_id,
                            binding.identity,
                            responder,
                            config.max_request_bytes,
                        )
                        .await?;
                        continue;
                    }
                }
            }

            return Ok(());
        }
    }

    fn flapping(bound: InstanceId, observed: InstanceId) -> SyncError {
        SyncError::handshake(format!(
            "peer identity changed twice during one sync cycle \
             (bound {bound}, now {observed}); refusing to keep re-establishing"
        ))
    }
}
