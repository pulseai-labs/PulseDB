//! Remote change applier — applies changes received from a remote peer.
//!
//! The `RemoteChangeApplier` receives batches of `SyncChange` from pull
//! responses and applies them to the local database. It handles:
//! - Echo prevention via [`SyncApplyGuard`]
//! - Idempotent creates (skip if entity exists)
//! - Idempotent deletes (skip if entity missing)
//! - Conflict resolution for experience updates

use std::sync::Arc;

use tracing::{debug, instrument, trace, warn};

use crate::db::PulseDB;
use crate::experience::ExperienceUpdate;

use super::config::{ConflictResolution, SyncConfig};
use super::error::SyncError;
use super::guard::SyncApplyGuard;
use super::types::{SyncChange, SyncPayload};

/// Result of applying a batch of remote changes.
#[derive(Clone, Debug, Default)]
pub struct ApplyResult {
    /// Number of changes successfully applied.
    pub applied: usize,
    /// Number of changes skipped (idempotent / filtered).
    pub skipped: usize,
    /// Number of changes where conflict resolution was used.
    pub conflicts: usize,
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

        for change in changes {
            match self.apply_single(change) {
                Ok(ApplyOutcome::Applied) => result.applied += 1,
                Ok(ApplyOutcome::Skipped) => result.skipped += 1,
                Ok(ApplyOutcome::ConflictResolved) => {
                    result.applied += 1;
                    result.conflicts += 1;
                }
                Err(e) => {
                    warn!("Failed to apply sync change: {}", e);
                    // Continue applying remaining changes — don't fail the batch
                    result.skipped += 1;
                }
            }
        }

        debug!(
            applied = result.applied,
            skipped = result.skipped,
            conflicts = result.conflicts,
            "Applied remote change batch"
        );
        Ok(result)
    }

    /// Applies a single remote change, returning the outcome.
    fn apply_single(&self, change: SyncChange) -> Result<ApplyOutcome, SyncError> {
        let _guard = SyncApplyGuard::enter();

        let map_err = |e: crate::error::PulseDBError| {
            SyncError::transport(format!("Failed to apply sync change: {}", e))
        };

        match change.payload {
            // ─── Experience ──────────────────────────────────────────
            SyncPayload::ExperienceCreated(experience) => {
                let id = experience.id;
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
                self.db
                    .apply_synced_experience_update(id, experience_update)
                    .map_err(map_err)?;
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
                self.db
                    .apply_synced_experience_update(id, update)
                    .map_err(map_err)?;
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
        CollectiveId, Config, ExperienceType, InstanceId, NewExperience, PulseDB, Timestamp,
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
            .apply_single(change(SyncPayload::ExperienceCreated(remote), cid))
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
}
