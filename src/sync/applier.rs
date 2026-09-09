//! Remote change applier — applies changes received from a remote peer.
//!
//! The `RemoteChangeApplier` receives batches of `SyncChange` from pull
//! responses and applies them to the local database. It handles:
//! - Echo prevention via [`SyncApplyGuard`]
//! - Idempotent creates (skip if entity exists)
//! - Idempotent deletes (skip if entity missing)
//! - Conflict resolution for experience updates

use std::sync::{Arc, Mutex};

use tracing::{debug, instrument, trace, warn};

use crate::db::PulseDB;
use crate::experience::ExperienceUpdate;
use crate::types::Timestamp;

use super::config::{ConflictResolution, SyncConfig};
use super::error::SyncError;
use super::guard::SyncApplyGuard;
use super::types::{InstanceId, SyncChange, SyncPayload, SyncStats};

/// Upper bound on the number of per-instance buckets accepted in a single
/// experience's `applications` G-counter from a remote peer. Each bucket is one
/// distinct replica that reinforced the experience, so realistic counts are in
/// the tens-to-thousands even for large fleets; a payload exceeding this is
/// treated as malformed/hostile and rejected to prevent unbounded memory growth
/// and persistent state bloat (resource-exhaustion DoS) during sync apply.
const MAX_SYNC_APPLICATION_BUCKETS: usize = 65_536;

/// Result of applying a batch of remote changes.
#[derive(Clone, Debug, Default)]
pub struct ApplyResult {
    /// Number of changes successfully applied.
    pub applied: usize,
    /// Number of changes skipped (idempotent / filtered).
    pub skipped: usize,
    /// Number of changes that FAILED to apply — the applier's error arm alone.
    ///
    /// Distinct from [`skipped`](Self::skipped), which counts every change that
    /// left the store unchanged: the idempotent no-ops (a create whose entity
    /// already exists, a delete whose entity is already gone) **and** these
    /// failures, which it has always counted too. So `skipped > 0` says nothing
    /// about whether anything went wrong — `failed > 0` does, and
    /// `skipped - failed` is the idempotent part.
    ///
    /// An idempotent skip is a successful outcome: it is the ordinary shape of
    /// a re-sync — which is why a one-shot catch-up
    /// ([`SyncManager::initial_sync`]) reads the failures, not `skipped`, to
    /// decide whether it may report completion. It reads
    /// [`failed_sequences`](Self::failed_sequences) rather than this count,
    /// because a count cannot say whether a later retry fixed anything.
    ///
    /// [`SyncManager::initial_sync`]: super::manager::SyncManager::initial_sync
    pub failed: usize,
    /// The sequence of every change this batch FAILED to apply — the same
    /// changes [`failed`](Self::failed) counts, named rather than tallied.
    ///
    /// Arrival order, not sorted: a remote peer chooses the batch's order and
    /// this field does not reorder it.
    ///
    /// A caller that retries a batch needs the identities, not the count. A
    /// count only ever accumulates, so across several attempts it cannot
    /// distinguish "three changes are broken" from "one change failed three
    /// times" from "one change failed, then applied" — which is how
    /// [`SyncManager::initial_sync`] came to report a failure on a catch-up
    /// whose retry had succeeded. With the sequences named, a failure is
    /// resolved once the pull position has moved to or past it, since
    /// [`safe_through`](Self::safe_through) only names a sequence at or below
    /// which everything was handled.
    ///
    /// The lowest of these is also the batch's failure floor, the bound
    /// `safe_through` is computed against.
    ///
    /// [`SyncManager::initial_sync`]: super::manager::SyncManager::initial_sync
    pub failed_sequences: Vec<u64>,
    /// Number of changes where conflict resolution was used.
    pub conflicts: usize,
    /// Highest sequence at or below which EVERY change in this batch was
    /// applied, resolved, or idempotently skipped. `None` when no sequence
    /// satisfies that.
    ///
    /// A change that *errored* bounds this value: the peer must be able to
    /// retry it, and the sender's `push_sequence` (which is what `compact_wal`
    /// trusts) is derived from it, so acknowledging past a failure would let
    /// the sender delete a WAL event this peer never stored.
    ///
    /// **The batch's order is not trusted** — a remote peer chooses it. The
    /// bound is therefore by *sequence*, not by position: the value is the
    /// highest handled sequence lying strictly below the LOWEST sequence that
    /// failed anywhere in the batch. Position decides nothing, on either side
    /// of the rule — every success counts, whether it arrived before or after
    /// a failure. A batch arriving as `[9 ok, 3 err]` reports `None` (9 sits
    /// above the failure); `[1 ok, 5 ok, 3 err]` reports `1`; `[5 err, 1 ok]`
    /// also reports `1`, because 1 still lies below the floor; an ascending
    /// `[1 ok, 2 ok, 3 err]` reports `2`.
    pub safe_through: Option<u64>,
    /// Number of changes in this batch whose incoming `last_reinforced` lay
    /// beyond `now + SyncConfig::max_clock_skew_ms` (#13).
    ///
    /// Detection only: the batch is logged once at `warn` — carrying the peer,
    /// this count and the largest skew observed — and every value is merged
    /// unchanged (FR-031 max-merge, r1 veto fold C2). Never serialized on the
    /// wire.
    pub skewed_timestamps: u64,
}

impl ApplyResult {
    /// Folds this batch's counters into a cumulative, local-only [`SyncStats`].
    pub(crate) fn record_into(&self, stats: &Mutex<SyncStats>) {
        let mut stats = stats.lock().unwrap_or_else(|e| e.into_inner());
        stats.skewed_timestamps = stats
            .skewed_timestamps
            .saturating_add(self.skewed_timestamps);
    }
}

/// Applies remote sync changes to the local PulseDB instance.
pub(crate) struct RemoteChangeApplier {
    db: Arc<PulseDB>,
    config: SyncConfig,
}

impl RemoteChangeApplier {
    /// Creates a new applier.
    pub fn new(db: Arc<PulseDB>, config: SyncConfig) -> Self {
        Self { db, config }
    }

    /// Applies a batch of remote changes to the local database.
    ///
    /// Each change is applied under a [`SyncApplyGuard`] to prevent
    /// WAL re-emission (echo prevention). Changes are applied in order.
    #[instrument(skip(self, changes), fields(batch_size = changes.len()))]
    pub fn apply_batch(&self, changes: Vec<SyncChange>) -> Result<ApplyResult, SyncError> {
        let mut result = ApplyResult::default();

        // Once a change errors, nothing at or after it may be acknowledged:
        // the sender derives its push position from `safe_through`, and
        // compaction deletes below that position.
        //
        // "At or after it" is a statement about SEQUENCES, not about positions
        // in this vector — a remote peer chooses the order, so a lower sequence
        // can arrive after a higher one. Neither the last sequence handled nor
        // the running maximum of them is sound: `[9 ok, 3 err]` yields 9 under
        // both, acknowledging a change (3) that never applied. So the failures
        // set a FLOOR — the lowest sequence that failed anywhere in the batch —
        // EVERY success is recorded whatever its position, and `safe_through`
        // is the highest recorded success strictly below that floor. Any batch
        // change at or below that value that had failed would have to sit below
        // the floor, contradicting the floor's definition.
        //
        // Position must not gate the recording either: dropping successes that
        // arrive after a failure would report `None` for `[5 err, 1 ok]`, even
        // though 1 applied and lies below the floor — and a peer that always
        // sends that order could then never persist any progress at all.
        //
        // The floor is derived after the loop, as the minimum of the failed
        // sequences the batch collected — the same value a running fold would
        // reach, read off the field callers get to keep.
        //
        // Both vectors are bounded by the poller's batch limit
        // (`SyncConfig::batch_size` on the push path, `PullRequest::batch_size`
        // on the pull path).
        let mut succeeded: Vec<u64> = Vec::new();

        // #13 skew is reported ONCE per batch, not once per change: the peer's
        // clock does not self-correct (the value is deliberately merged
        // unchanged), so a per-change `warn!` would emit up to `batch_size`
        // lines per cycle, sustained, for as long as the peer is wrong. The
        // batch summary carries the peer and the largest skew it sent.
        let mut worst_skew: Option<(InstanceId, i64)> = None;

        for change in changes {
            let sequence = change.sequence;
            // #13: surface a skewed reinforcement timestamp BEFORE the apply,
            // so it is counted whatever the apply's outcome. Detection never
            // alters the change — it is applied exactly as received.
            if let Some(skew_ms) = self.detect_skew(&change) {
                result.skewed_timestamps += 1;
                if worst_skew.is_none_or(|(_, worst_ms)| skew_ms > worst_ms) {
                    worst_skew = Some((change.source_instance, skew_ms));
                }
            }
            match self.apply_single(change) {
                Ok(ApplyOutcome::Applied) => {
                    result.applied += 1;
                    succeeded.push(sequence);
                }
                Ok(ApplyOutcome::Skipped) => {
                    result.skipped += 1;
                    succeeded.push(sequence);
                }
                Ok(ApplyOutcome::ConflictResolved) => {
                    result.applied += 1;
                    result.conflicts += 1;
                    succeeded.push(sequence);
                }
                Err(e) => {
                    warn!(sequence, "Failed to apply sync change: {}", e);
                    // Continue applying the rest — a later change may be
                    // independent — but never acknowledge at or past this one.
                    // Recording the sequence is what lets the floor sink: a
                    // lower failing sequence arriving later must still pull the
                    // acknowledgement down, and a caller retrying the batch
                    // needs to know WHICH change to look for.
                    result.failed_sequences.push(sequence);
                    result.failed += 1;
                    result.skipped += 1;
                }
            }
        }

        // The LOWEST sequence that failed anywhere in the batch — a minimum
        // over the whole set, because arrival order decides nothing.
        let failure_floor = result.failed_sequences.iter().copied().min();

        // The highest success strictly below the failure floor (every success,
        // when nothing failed). `None` when no success qualifies — which is the
        // honest answer for `[9 ok, 3 err]`.
        result.safe_through = succeeded
            .into_iter()
            .filter(|sequence| failure_floor.is_none_or(|floor| *sequence < floor))
            .max();

        if let Some((peer, max_skew_ms)) = worst_skew {
            warn!(
                peer = %peer,
                skewed_timestamps = result.skewed_timestamps,
                max_skew_ms,
                max_clock_skew_ms = self.config.max_clock_skew_ms,
                "Incoming last_reinforced is beyond the clock-skew bound; merged unchanged (the bound is advisory: no protocol version carries a record-level time reference yet)"
            );
        }

        debug!(
            applied = result.applied,
            skipped = result.skipped,
            failed = result.failed,
            conflicts = result.conflicts,
            skewed_timestamps = result.skewed_timestamps,
            safe_through = result.safe_through,
            "Applied remote change batch"
        );
        Ok(result)
    }

    /// #13 (r1 veto fold C2) — skew **detection**, never correction.
    ///
    /// Returns `Some(skew_ms)` when the change carries a reinforcement
    /// timestamp beyond `now + max_clock_skew_ms` — at either apply site: the
    /// non-collision create (`ExperienceCreated`, timestamp written as-is) and
    /// the counter merge (`ExperienceCreated` collision or `ExperienceUpdated`,
    /// FR-031 max-merge) — and `None` otherwise. The change is left untouched:
    /// the merged value stays byte-for-byte what max-merge produces, so
    /// convergence is unaffected and the skew is visible in logs and
    /// [`SyncStats`] instead of silently freezing decay. A bound that also
    /// corrects needs a record-carried reference. Protocol v5 deliberately did
    /// not add one; that work is assigned to a later protocol version.
    ///
    /// **Detection does not log.** The condition never self-clears while the
    /// peer's clock is wrong, so [`apply_batch`](Self::apply_batch) emits one
    /// `warn!` summary per batch instead of one per change.
    fn detect_skew(&self, change: &SyncChange) -> Option<i64> {
        let incoming = match &change.payload {
            SyncPayload::ExperienceCreated(carried) => carried.experience.last_reinforced,
            SyncPayload::ExperienceUpdated { update, .. } => update.last_reinforced?,
            _ => return None,
        };

        let now_ms = Timestamp::now().as_millis();
        let allowance = i64::try_from(self.config.max_clock_skew_ms).unwrap_or(i64::MAX);
        let bound = now_ms.saturating_add(allowance);
        if incoming.as_millis() <= bound {
            return None;
        }

        Some(incoming.as_millis().saturating_sub(now_ms))
    }

    /// Applies a single remote change, returning the outcome.
    fn apply_single(&self, change: SyncChange) -> Result<ApplyOutcome, SyncError> {
        let _guard = SyncApplyGuard::enter();

        let map_err = |e: crate::error::PulseDBError| {
            SyncError::transport(format!("Failed to apply sync change: {}", e))
        };

        match change.payload {
            // ─── Experience ──────────────────────────────────────────
            SyncPayload::ExperienceCreated(carried) => {
                // Restore the wire-carried vector into the record BEFORE any
                // existing validation runs: `Experience::embedding` is
                // `#[serde(skip)]`, so what arrived is a record with an empty
                // vector plus the vector beside it. The dimension check below
                // and `apply_synced_experience`'s pre-write check both see the
                // real embedding, exactly as an in-process create would (#96).
                // Nothing is re-embedded — a payload that carried no vector
                // still has none.
                let experience: crate::experience::Experience = carried.into();
                let id = experience.id;
                if experience.applications.len() > MAX_SYNC_APPLICATION_BUCKETS {
                    return Err(SyncError::invalid_payload(format!(
                        "experience {id} sync create carries {} application buckets (max {MAX_SYNC_APPLICATION_BUCKETS})",
                        experience.applications.len()
                    )));
                }
                if self.db.get_experience(id).map_err(map_err)?.is_some() {
                    let merged = self
                        .db
                        .apply_synced_experience_counter_merge(
                            id,
                            &experience.applications,
                            Some(experience.last_reinforced),
                        )
                        .map_err(map_err)?;
                    if merged {
                        trace!(id = %id, "Merged ExperienceCreated counter collision");
                        return Ok(ApplyOutcome::Applied);
                    }
                    trace!(id = %id, "Skipping ExperienceCreated: already exists");
                    return Ok(ApplyOutcome::Skipped);
                }
                self.db
                    .apply_synced_experience(experience)
                    .map_err(map_err)?;
                Ok(ApplyOutcome::Applied)
            }

            SyncPayload::ExperienceUpdated {
                id,
                update,
                timestamp,
                ..
            } => {
                if let Some(incoming) = update.applications.as_ref() {
                    if incoming.len() > MAX_SYNC_APPLICATION_BUCKETS {
                        return Err(SyncError::invalid_payload(format!(
                            "experience {id} sync update carries {} application buckets (max {MAX_SYNC_APPLICATION_BUCKETS})",
                            incoming.len()
                        )));
                    }
                }
                // The dependency check comes BEFORE any mutation — before the
                // G-counter merge and before the scalar update. An update whose
                // target does not exist here is a FAILURE, not an idempotent
                // skip: acknowledging it would let the sender's `push_sequence`
                // move past it, and `compact_wal` would then be free to delete
                // the create this update depends on. Recovering the missing
                // dependency is out of scope; making the non-completion
                // explicit is the point.
                if self.db.get_experience(id).map_err(map_err)?.is_none() {
                    return Err(SyncError::missing_experience(id));
                }
                let applications = update.applications.as_ref().cloned().unwrap_or_default();
                let last_reinforced = update.last_reinforced;
                let has_counter_merge =
                    update.applications.is_some() || update.last_reinforced.is_some();
                let counter_merged = if has_counter_merge {
                    self.db
                        .apply_synced_experience_counter_merge(id, &applications, last_reinforced)
                        .map_err(map_err)?
                } else {
                    false
                };

                let mut apply_scalar_update = true;
                if self.config.conflict_resolution == ConflictResolution::LastWriteWins {
                    if let Some(local) = self.db.get_experience(id).map_err(map_err)? {
                        if local.timestamp > timestamp {
                            trace!(id = %id, "Skipping scalar ExperienceUpdated fields: local is newer (LastWriteWins)");
                            apply_scalar_update = false;
                        }
                    }
                }

                if !apply_scalar_update {
                    return if counter_merged {
                        Ok(ApplyOutcome::ConflictResolved)
                    } else {
                        Ok(ApplyOutcome::Skipped)
                    };
                }

                // ServerWins: always apply. LastWriteWins: remote is newer or equal.
                let experience_update: ExperienceUpdate = update.into();
                // A `false` here means the storage layer changed nothing — the
                // record went away between the check above and the write. It is
                // propagated rather than acknowledged, for the same reason.
                if !self
                    .db
                    .apply_synced_experience_update(id, experience_update)
                    .map_err(map_err)?
                {
                    return Err(SyncError::missing_experience(id));
                }
                if self.config.conflict_resolution == ConflictResolution::LastWriteWins {
                    Ok(ApplyOutcome::ConflictResolved)
                } else {
                    Ok(ApplyOutcome::Applied)
                }
            }

            SyncPayload::ExperienceArchived { id, .. } => {
                let update = ExperienceUpdate {
                    archived: Some(true),
                    ..Default::default()
                };
                // Skip if experience doesn't exist
                if self.db.get_experience(id).map_err(map_err)?.is_none() {
                    trace!(id = %id, "Skipping ExperienceArchived: not found");
                    return Ok(ApplyOutcome::Skipped);
                }
                // An archive of a record that IS present; the already-absent
                // case took the idempotent skip above and keeps it.
                if !self
                    .db
                    .apply_synced_experience_update(id, update)
                    .map_err(map_err)?
                {
                    return Err(SyncError::missing_experience(id));
                }
                Ok(ApplyOutcome::Applied)
            }

            SyncPayload::ExperienceDeleted { id, .. } => {
                // Idempotent: skip if already gone
                if self.db.get_experience(id).map_err(map_err)?.is_none() {
                    trace!(id = %id, "Skipping ExperienceDeleted: not found");
                    return Ok(ApplyOutcome::Skipped);
                }
                self.db
                    .apply_synced_experience_delete(id)
                    .map_err(map_err)?;
                Ok(ApplyOutcome::Applied)
            }

            // ─── Relation ────────────────────────────────────────────
            SyncPayload::RelationCreated(relation) => {
                let id = relation.id;
                // Idempotent: skip if already exists
                if self.db.get_relation(id).map_err(map_err)?.is_some() {
                    trace!(id = %id, "Skipping RelationCreated: already exists");
                    return Ok(ApplyOutcome::Skipped);
                }
                self.db.apply_synced_relation(relation).map_err(map_err)?;
                Ok(ApplyOutcome::Applied)
            }

            SyncPayload::RelationDeleted { id, .. } => {
                // Idempotent: skip if already gone
                if self.db.get_relation(id).map_err(map_err)?.is_none() {
                    trace!(id = %id, "Skipping RelationDeleted: not found");
                    return Ok(ApplyOutcome::Skipped);
                }
                self.db.apply_synced_relation_delete(id).map_err(map_err)?;
                Ok(ApplyOutcome::Applied)
            }

            // ─── Insight ─────────────────────────────────────────────
            SyncPayload::InsightCreated(insight) => {
                let id = insight.id;
                // Idempotent: skip if already exists
                if self.db.get_insight(id).map_err(map_err)?.is_some() {
                    trace!(id = %id, "Skipping InsightCreated: already exists");
                    return Ok(ApplyOutcome::Skipped);
                }
                self.db.apply_synced_insight(insight).map_err(map_err)?;
                Ok(ApplyOutcome::Applied)
            }

            SyncPayload::InsightDeleted { id, .. } => {
                // Idempotent: skip if already gone
                if self.db.get_insight(id).map_err(map_err)?.is_none() {
                    trace!(id = %id, "Skipping InsightDeleted: not found");
                    return Ok(ApplyOutcome::Skipped);
                }
                self.db.apply_synced_insight_delete(id).map_err(map_err)?;
                Ok(ApplyOutcome::Applied)
            }

            // ─── Collective ──────────────────────────────────────────
            SyncPayload::CollectiveCreated(collective) => {
                let id = collective.id;
                // Idempotent: skip if already exists
                if self.db.get_collective(id).map_err(map_err)?.is_some() {
                    trace!(id = %id, "Skipping CollectiveCreated: already exists");
                    return Ok(ApplyOutcome::Skipped);
                }
                self.db
                    .apply_synced_collective(collective)
                    .map_err(map_err)?;
                Ok(ApplyOutcome::Applied)
            }
        }
    }
}

/// Internal outcome of applying a single change.
#[derive(Debug)]
enum ApplyOutcome {
    Applied,
    Skipped,
    ConflictResolved,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use tempfile::tempdir;

    use super::*;
    use crate::sync::types::{SerializableExperienceUpdate, SyncEntityType};
    use crate::{
        CollectiveId, Config, ExperienceId, ExperienceType, InstanceId, NewExperience, PulseDB,
        Timestamp,
    };

    fn open_db() -> (Arc<PulseDB>, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db = Arc::new(PulseDB::open(dir.path().join("test.db"), Config::default()).unwrap());
        (db, dir)
    }

    fn minimal_exp(cid: CollectiveId) -> NewExperience {
        NewExperience {
            collective_id: cid,
            content: "applier merge test".to_string(),
            experience_type: ExperienceType::Generic { category: None },
            embedding: Some(vec![0.1f32; 384]),
            importance: 0.9,
            ..Default::default()
        }
    }

    fn change(payload: SyncPayload, cid: CollectiveId) -> SyncChange {
        SyncChange {
            sequence: 1,
            source_instance: InstanceId::new(),
            collective_id: cid,
            entity_type: SyncEntityType::Experience,
            payload,
            timestamp: Timestamp::now(),
        }
    }

    #[test]
    fn experience_created_collision_merges_gcounter_fields() {
        let (db, _dir) = open_db();
        let cid = db.create_collective("applier-create-collision").unwrap();
        let exp_id = db.record_experience(minimal_exp(cid)).unwrap();
        let remote_key = InstanceId::new();
        let incoming_last_reinforced = Timestamp::from_millis(i64::MAX);
        let mut remote = db.get_experience(exp_id).unwrap().unwrap();
        remote.applications = BTreeMap::from([(remote_key, 4)]);
        remote.last_reinforced = incoming_last_reinforced;

        let applier = RemoteChangeApplier::new(Arc::clone(&db), SyncConfig::default());
        let outcome = applier
            .apply_single(change(SyncPayload::ExperienceCreated(remote.into()), cid))
            .unwrap();

        assert!(matches!(outcome, ApplyOutcome::Applied));
        let merged = db.get_experience(exp_id).unwrap().unwrap();
        assert_eq!(merged.applications.get(&remote_key), Some(&4));
        assert_eq!(merged.last_reinforced, incoming_last_reinforced);
    }

    #[test]
    fn lww_skip_does_not_skip_gcounter_merge() {
        let (db, _dir) = open_db();
        let cid = db.create_collective("applier-lww-counter").unwrap();
        let exp_id = db.record_experience(minimal_exp(cid)).unwrap();
        let remote_key = InstanceId::new();
        let incoming_last_reinforced = Timestamp::from_millis(i64::MAX);
        let update = SerializableExperienceUpdate {
            importance: Some(0.1),
            applications: Some(BTreeMap::from([(remote_key, 6)])),
            last_reinforced: Some(incoming_last_reinforced),
            ..Default::default()
        };

        let applier = RemoteChangeApplier::new(
            Arc::clone(&db),
            SyncConfig {
                conflict_resolution: ConflictResolution::LastWriteWins,
                ..SyncConfig::default()
            },
        );
        let outcome = applier
            .apply_single(change(
                SyncPayload::ExperienceUpdated {
                    id: exp_id,
                    update,
                    timestamp: Timestamp::from_millis(0),
                },
                cid,
            ))
            .unwrap();

        assert!(matches!(outcome, ApplyOutcome::ConflictResolved));
        let merged = db.get_experience(exp_id).unwrap().unwrap();
        assert_eq!(merged.applications.get(&remote_key), Some(&6));
        assert_eq!(merged.applications(), 6);
        assert_eq!(merged.last_reinforced, incoming_last_reinforced);
        assert!((merged.importance - 0.9).abs() < f32::EPSILON);
    }

    /// A change that fails to apply must stop the acknowledged position dead:
    /// the sender turns `safe_through` into its `push_sequence`, and
    /// `compact_wal` deletes below that, so acknowledging past a rejected
    /// change would let the sender discard a WAL event this peer never stored.
    #[test]
    fn a_rejected_change_stops_the_acknowledged_position() {
        let (db, _dir) = open_db();
        let cid = db.create_collective("applier-ack-bound").unwrap();
        let good_a = db.record_experience(minimal_exp(cid)).unwrap();
        let good_b = db.record_experience(minimal_exp(cid)).unwrap();

        // A hostile payload the applier refuses (same bound as above).
        let mut buckets = BTreeMap::new();
        for i in 0..=(MAX_SYNC_APPLICATION_BUCKETS as u128) {
            buckets.insert(InstanceId::from_bytes(i.to_le_bytes()), 1u32);
        }
        let poison = SerializableExperienceUpdate {
            applications: Some(buckets),
            ..Default::default()
        };
        let benign = SerializableExperienceUpdate {
            importance: Some(0.5),
            ..Default::default()
        };

        let at = |seq: u64, id, update| {
            let mut c = change(
                SyncPayload::ExperienceUpdated {
                    id,
                    update,
                    timestamp: Timestamp::from_millis(0),
                },
                cid,
            );
            c.sequence = seq;
            c
        };
        let batch = vec![
            at(7, good_a, benign.clone()),
            at(8, good_b, poison),
            at(9, good_a, benign),
        ];

        let applier = RemoteChangeApplier::new(Arc::clone(&db), SyncConfig::default());
        let result = applier.apply_batch(batch).unwrap();

        assert_eq!(
            result.safe_through,
            Some(7),
            "acknowledgement must stop at the last change before the failure, \
             not run on to the last change that happened to succeed"
        );
        assert_eq!(result.skipped, 1, "the rejected change counts as skipped");
    }

    /// An `applications` map one bucket past [`MAX_SYNC_APPLICATION_BUCKETS`]:
    /// the payload the applier refuses, used to fail one change of a batch.
    fn poison_update() -> SerializableExperienceUpdate {
        let mut buckets = BTreeMap::new();
        for i in 0..=(MAX_SYNC_APPLICATION_BUCKETS as u128) {
            buckets.insert(InstanceId::from_bytes(i.to_le_bytes()), 1u32);
        }
        SerializableExperienceUpdate {
            applications: Some(buckets),
            ..Default::default()
        }
    }

    /// A remote peer chooses the batch's order, so a success may arrive AHEAD
    /// of a failure at a lower sequence. Acknowledging the higher sequence
    /// would let the sender's `compact_wal` delete the change that failed.
    #[test]
    fn a_success_above_a_later_failure_acknowledges_nothing() {
        let (db, _dir) = open_db();
        let cid = db.create_collective("applier-ack-unordered").unwrap();
        let good = db.record_experience(minimal_exp(cid)).unwrap();
        let benign = SerializableExperienceUpdate {
            importance: Some(0.5),
            ..Default::default()
        };

        let at = |seq: u64, update| {
            let mut c = change(
                SyncPayload::ExperienceUpdated {
                    id: good,
                    update,
                    timestamp: Timestamp::from_millis(0),
                },
                cid,
            );
            c.sequence = seq;
            c
        };

        let applier = RemoteChangeApplier::new(Arc::clone(&db), SyncConfig::default());
        let result = applier
            .apply_batch(vec![at(9, benign), at(3, poison_update())])
            .unwrap();

        assert_eq!(
            result.safe_through, None,
            "no sequence in this batch is safe: 9 applied but 3 failed, and \
             acknowledging 9 would discard the peer's WAL event 3"
        );
    }

    /// The bound is the LOWEST failing sequence, not the whole batch: a success
    /// genuinely below it stays acknowledged, so an unordered batch does not
    /// throw away progress it really made.
    #[test]
    fn a_success_below_the_lowest_failure_is_still_acknowledged() {
        let (db, _dir) = open_db();
        let cid = db.create_collective("applier-ack-floor").unwrap();
        let good = db.record_experience(minimal_exp(cid)).unwrap();
        let benign = SerializableExperienceUpdate {
            importance: Some(0.5),
            ..Default::default()
        };

        let at = |seq: u64, update| {
            let mut c = change(
                SyncPayload::ExperienceUpdated {
                    id: good,
                    update,
                    timestamp: Timestamp::from_millis(0),
                },
                cid,
            );
            c.sequence = seq;
            c
        };

        let applier = RemoteChangeApplier::new(Arc::clone(&db), SyncConfig::default());
        let result = applier
            .apply_batch(vec![
                at(1, benign.clone()),
                at(5, benign),
                at(3, poison_update()),
            ])
            .unwrap();

        assert_eq!(
            result.safe_through,
            Some(1),
            "1 applied and is below the failing 3, so it is safe; 5 is not"
        );
    }

    /// A success arriving AFTER a higher-sequence failure is still real
    /// progress: it sits below the failure floor, so it is safe to acknowledge.
    /// Recording successes only until the first failure would discard it and
    /// leave a peer that always sends this order stuck at `None` forever,
    /// replaying the same lower change on every cycle.
    #[test]
    fn a_success_after_a_higher_failure_is_still_acknowledged() {
        let (db, _dir) = open_db();
        let cid = db.create_collective("applier-ack-late-success").unwrap();
        let good = db.record_experience(minimal_exp(cid)).unwrap();
        let benign = SerializableExperienceUpdate {
            importance: Some(0.5),
            ..Default::default()
        };

        let at = |seq: u64, update| {
            let mut c = change(
                SyncPayload::ExperienceUpdated {
                    id: good,
                    update,
                    timestamp: Timestamp::from_millis(0),
                },
                cid,
            );
            c.sequence = seq;
            c
        };

        let applier = RemoteChangeApplier::new(Arc::clone(&db), SyncConfig::default());
        let result = applier
            .apply_batch(vec![at(5, poison_update()), at(1, benign)])
            .unwrap();

        assert_eq!(
            result.safe_through,
            Some(1),
            "1 applied and is strictly below the failing 5, so it is safe — \
             the batch's order must not decide what is recorded"
        );
    }

    /// `failed_sequences` NAMES the changes that errored, and the floor
    /// `safe_through` is bound by is the LOWEST of them wherever it arrived.
    ///
    /// The names are what a retrying caller needs: a count only accumulates, so
    /// across attempts it cannot tell "two changes are broken" from "one change
    /// failed twice" from "one change failed, then applied". `initial_sync`
    /// resolves each named sequence against the position it finally reached.
    #[test]
    fn failed_sequences_name_the_failures_and_the_lowest_sets_the_floor() {
        let (db, _dir) = open_db();
        let cid = db.create_collective("applier-failed-sequences").unwrap();
        let good = db.record_experience(minimal_exp(cid)).unwrap();
        let benign = SerializableExperienceUpdate {
            importance: Some(0.5),
            ..Default::default()
        };

        let at = |seq: u64, update| {
            let mut c = change(
                SyncPayload::ExperienceUpdated {
                    id: good,
                    update,
                    timestamp: Timestamp::from_millis(0),
                },
                cid,
            );
            c.sequence = seq;
            c
        };

        let applier = RemoteChangeApplier::new(Arc::clone(&db), SyncConfig::default());
        // The lower failure (4) arrives LAST, so a floor read off the first
        // failure alone would acknowledge 5 — a change that never applied.
        let result = applier
            .apply_batch(vec![
                at(2, benign.clone()),
                at(7, poison_update()),
                at(5, benign),
                at(4, poison_update()),
            ])
            .unwrap();

        assert_eq!(
            result.failed_sequences,
            vec![7, 4],
            "the failures are named in arrival order, not sorted or tallied"
        );
        assert_eq!(result.failed, 2, "the count still agrees with the names");
        assert_eq!(
            result.safe_through,
            Some(2),
            "the floor is the lowest failure (4), so only 2 is safe — 5 is not"
        );
    }

    #[test]
    fn oversized_application_bucket_map_is_rejected() {
        let (db, _dir) = open_db();
        let cid = db.create_collective("applier-bucket-bound").unwrap();
        let exp_id = db.record_experience(minimal_exp(cid)).unwrap();

        // A hostile peer payload with more buckets than the accepted bound.
        let mut buckets = BTreeMap::new();
        for i in 0..=(MAX_SYNC_APPLICATION_BUCKETS as u128) {
            buckets.insert(InstanceId::from_bytes(i.to_le_bytes()), 1u32);
        }
        assert_eq!(buckets.len(), MAX_SYNC_APPLICATION_BUCKETS + 1);

        let update = SerializableExperienceUpdate {
            applications: Some(buckets),
            ..Default::default()
        };

        let applier = RemoteChangeApplier::new(Arc::clone(&db), SyncConfig::default());
        let result = applier.apply_single(change(
            SyncPayload::ExperienceUpdated {
                id: exp_id,
                update,
                timestamp: Timestamp::now(),
            },
            cid,
        ));

        assert!(
            matches!(result, Err(SyncError::InvalidPayload(_))),
            "oversized application bucket map must be rejected, got {result:?}"
        );
        // Nothing from the oversized map may have been persisted.
        let stored = db.get_experience(exp_id).unwrap().unwrap();
        assert!(stored.applications.len() <= 1);
    }

    /// #13 (veto fold C2): an incoming `last_reinforced` beyond
    /// `now + max_clock_skew_ms` is COUNTED in `ApplyResult::skewed_timestamps`
    /// (and logged) at every apply site, and NEVER clamped, rejected or
    /// re-timestamped — the stored value is byte-for-byte what FR-031's
    /// max-merge produces.
    #[test]
    fn skewed_last_reinforced_is_counted_not_clamped() {
        let (db, _dir) = open_db();
        let cid = db.create_collective("applier-skew").unwrap();
        let exp_id = db.record_experience(minimal_exp(cid)).unwrap();
        let config = SyncConfig::default();
        assert_eq!(config.max_clock_skew_ms, 300_000, "grill Q4 default");
        let applier = RemoteChangeApplier::new(Arc::clone(&db), config.clone());
        let remote_key = InstanceId::new();

        let now_ms = Timestamp::now().as_millis();
        let allowance = i64::try_from(config.max_clock_skew_ms).unwrap();
        // One day past the skew bound.
        let skewed = Timestamp::from_millis(now_ms + allowance + 86_400_000);

        // 1. Counter-merge site (ExperienceUpdated): counted once, merged as-is.
        let update = SerializableExperienceUpdate {
            applications: Some(BTreeMap::from([(remote_key, 2)])),
            last_reinforced: Some(skewed),
            ..Default::default()
        };
        let result = applier
            .apply_batch(vec![change(
                SyncPayload::ExperienceUpdated {
                    id: exp_id,
                    update,
                    timestamp: Timestamp::now(),
                },
                cid,
            )])
            .unwrap();
        assert_eq!(result.skewed_timestamps, 1, "skewed update is counted once");
        assert_eq!(result.applied, 1);
        let stored = db.get_experience(exp_id).unwrap().unwrap();
        assert_eq!(
            stored.last_reinforced, skewed,
            "max-merge result is stored byte-for-byte — not clamped"
        );
        assert_eq!(stored.applications.get(&remote_key), Some(&2));

        // 2. An in-bound timestamp is not counted; the max-merge still holds.
        let in_bound = Timestamp::from_millis(now_ms + 1_000);
        let update = SerializableExperienceUpdate {
            last_reinforced: Some(in_bound),
            ..Default::default()
        };
        let result = applier
            .apply_batch(vec![change(
                SyncPayload::ExperienceUpdated {
                    id: exp_id,
                    update,
                    timestamp: Timestamp::now(),
                },
                cid,
            )])
            .unwrap();
        assert_eq!(
            result.skewed_timestamps, 0,
            "in-bound update is not counted"
        );
        assert_eq!(
            db.get_experience(exp_id).unwrap().unwrap().last_reinforced,
            skewed,
            "max-merge keeps the larger (skewed) value"
        );

        // 3. Non-collision create site (ExperienceCreated, unknown id): counted,
        //    the record is written with its timestamp untouched.
        let mut fresh = db.get_experience(exp_id).unwrap().unwrap();
        fresh.id = ExperienceId::new();
        fresh.last_reinforced = skewed;
        let result = applier
            .apply_batch(vec![change(
                SyncPayload::ExperienceCreated(fresh.clone().into()),
                cid,
            )])
            .unwrap();
        assert_eq!(result.skewed_timestamps, 1, "fresh create is counted");
        assert_eq!(result.applied, 1);
        assert_eq!(
            db.get_experience(fresh.id)
                .unwrap()
                .unwrap()
                .last_reinforced,
            skewed,
            "fresh create stores the skewed value unchanged"
        );

        // 4. Collision create site (ExperienceCreated, known id): counted and
        //    max-merged unchanged.
        let mut collision = db.get_experience(exp_id).unwrap().unwrap();
        collision.last_reinforced = Timestamp::from_millis(skewed.as_millis() + 1);
        let result = applier
            .apply_batch(vec![change(
                SyncPayload::ExperienceCreated(collision.clone().into()),
                cid,
            )])
            .unwrap();
        assert_eq!(result.skewed_timestamps, 1, "collision create is counted");
        assert_eq!(
            db.get_experience(exp_id).unwrap().unwrap().last_reinforced,
            collision.last_reinforced,
            "collision max-merge stores the skewed value unchanged"
        );

        // 5. A change with no reinforcement timestamp is never counted.
        let result = applier
            .apply_batch(vec![change(
                SyncPayload::ExperienceArchived {
                    id: exp_id,
                    timestamp: skewed,
                },
                cid,
            )])
            .unwrap();
        assert_eq!(result.skewed_timestamps, 0);
    }

    /// `safe_through` is documented as "the highest sequence at or below which
    /// every change was applied". A remote peer controls the batch's order, so
    /// a batch whose sequences are not ascending and where NOTHING failed must
    /// still report the highest one — reporting the last would rewind the
    /// sender's push position.
    #[test]
    fn safe_through_is_the_highest_sequence_not_the_last_one() {
        let (db, _dir) = open_db();
        let applier = RemoteChangeApplier::new(Arc::clone(&db), SyncConfig::default());

        let collective = |seq: u64| {
            let cid = CollectiveId::new();
            SyncChange {
                sequence: seq,
                source_instance: InstanceId::new(),
                collective_id: cid,
                entity_type: SyncEntityType::Collective,
                payload: SyncPayload::CollectiveCreated(crate::collective::Collective {
                    id: cid,
                    name: format!("out-of-order-{seq}"),
                    owner_id: None,
                    embedding_dimension: 384,
                    created_at: Timestamp::now(),
                    updated_at: Timestamp::now(),
                }),
                timestamp: Timestamp::now(),
            }
        };

        let result = applier
            .apply_batch(vec![collective(9), collective(3)])
            .unwrap();

        assert_eq!(result.applied, 2);
        assert_eq!(
            result.safe_through,
            Some(9),
            "with no failure to bound it, the highest sequence handled — not \
             the last one handled"
        );
    }
}
