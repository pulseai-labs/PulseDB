//! Instance identity: `PulseDB::instance_id()` + `PulseDB::remint_instance_id()`.
//!
//! The per-instance G-counter behind `applications` (FR-031 / DECAY_SPEC D7)
//! is exact only while every replica carries a distinct `InstanceId`. The id
//! is persisted inside the store file, so a file-level copy (backup restore,
//! snapshot clone) yields two replicas with the *same* id whose buckets
//! collide under the per-key max merge. `remint_instance_id()` is the explicit
//! operator escape hatch (triage D5: explicit API, no heuristic clone
//! detection): call it on the restored copy before its first reinforce.
//!
//! Requires the `sync` feature (the identity API is sync-scoped).

#![cfg(feature = "sync")]

mod common;

use std::path::Path;
use std::sync::Arc;

use common::sync_both_ways;
use pulsedb::{
    CollectiveId, Config, ExperienceId, InstanceId, NewExperience, PulseDB, PulseDBError,
};
use tempfile::tempdir;

// ============================================================================
// Helpers
// ============================================================================

fn open_at(path: &Path) -> PulseDB {
    PulseDB::open(path, Config::default()).unwrap()
}

fn minimal_exp(cid: CollectiveId) -> NewExperience {
    NewExperience {
        collective_id: cid,
        content: format!("experience-{}", uuid::Uuid::now_v7()),
        embedding: Some(vec![0.1f32; 384]),
        ..Default::default()
    }
}

fn reinforce_n(db: &PulseDB, id: ExperienceId, n: u32) {
    for _ in 0..n {
        db.reinforce_experience(id).unwrap();
    }
}

fn applications(db: &PulseDB, id: ExperienceId) -> u32 {
    db.get_experience(id).unwrap().unwrap().applications()
}

fn bucket_count(db: &PulseDB, id: ExperienceId) -> usize {
    db.get_experience(id).unwrap().unwrap().applications.len()
}

// ============================================================================
// instance_id()
// ============================================================================

#[test]
fn instance_id_persists_across_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("identity.db");

    let first = {
        let db = open_at(&path);
        let id = db.instance_id();
        assert_ne!(id, InstanceId::nil(), "a fresh store mints a real identity");
        db.close().unwrap();
        id
    };

    let second = {
        let db = open_at(&path);
        let id = db.instance_id();
        db.close().unwrap();
        id
    };

    assert_eq!(first, second, "instance_id must be stable across reopen");
}

// ============================================================================
// remint_instance_id()
// ============================================================================

#[test]
fn remint_changes_id_and_keeps_totals() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("remint.db");
    let db = open_at(&path);

    let cid = db.create_collective("remint").unwrap();
    let exp = db.record_experience(minimal_exp(cid)).unwrap();

    let old = db.instance_id();
    reinforce_n(&db, exp, 3);
    assert_eq!(applications(&db, exp), 3);
    assert_eq!(
        bucket_count(&db, exp),
        1,
        "all three increments land in the old bucket"
    );

    let new = db.remint_instance_id().unwrap();
    assert_ne!(new, old, "remint must mint a fresh identity");
    assert_ne!(new, InstanceId::nil());
    assert_eq!(
        db.instance_id(),
        new,
        "the cached identity flips with the persisted one"
    );

    reinforce_n(&db, exp, 2);
    let after = db.get_experience(exp).unwrap().unwrap();
    assert_eq!(
        after.applications(),
        5,
        "totals are preserved across the remint"
    );
    assert_eq!(
        after.applications.len(),
        2,
        "old bucket untouched, new bucket opened"
    );
    assert_eq!(after.applications.get(&old), Some(&3));
    assert_eq!(after.applications.get(&new), Some(&2));

    // The new identity is what the store persists: it survives a reopen.
    db.close().unwrap();
    let reopened = open_at(&path);
    assert_eq!(
        reopened.instance_id(),
        new,
        "the reminted identity is persisted"
    );
    assert_eq!(applications(&reopened, exp), 5);
}

#[test]
fn remint_on_read_only_store_is_refused() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("readonly.db");

    // Create the store writable first so a read-only open has something to open.
    let before = {
        let db = open_at(&path);
        let id = db.instance_id();
        db.close().unwrap();
        id
    };

    let ro = PulseDB::open(
        &path,
        Config {
            read_only: true,
            ..Config::default()
        },
    )
    .unwrap();

    let err = ro.remint_instance_id().unwrap_err();
    assert!(
        matches!(err, PulseDBError::ReadOnly),
        "expected PulseDBError::ReadOnly, got {err:?}"
    );
    assert_eq!(
        ro.instance_id(),
        before,
        "a refused remint leaves the identity alone"
    );
}

// ============================================================================
// Restored file copies: the hazard remint exists for
// ============================================================================

/// Builds the "restored copy" scenario shared by the two clone tests:
/// A records + reinforces once, is closed, and its file is copied to B's
/// path. Returns both stores reopened plus the experience id.
///
/// Only the store file is copied (as an operator's `cp` would); the HNSW
/// sidecar is rebuilt from the store on open.
fn restore_clone(dir_a: &Path, dir_b: &Path) -> (Arc<PulseDB>, Arc<PulseDB>, ExperienceId) {
    let path_a = dir_a.join("original.db");
    let path_b = dir_b.join("restored.db");

    let exp = {
        let db = open_at(&path_a);
        let cid = db.create_collective("restore").unwrap();
        let exp = db.record_experience(minimal_exp(cid)).unwrap();
        reinforce_n(&db, exp, 1);
        db.close().unwrap();
        exp
    };

    std::fs::copy(&path_a, &path_b).unwrap();

    let db_a = Arc::new(open_at(&path_a));
    let db_b = Arc::new(open_at(&path_b));
    assert_eq!(
        db_a.instance_id(),
        db_b.instance_id(),
        "a file copy carries the original's identity — that is the hazard"
    );
    (db_a, db_b, exp)
}

#[tokio::test]
async fn restored_clone_after_remint_syncs_exact_total() {
    let dir_a = tempdir().unwrap();
    let dir_b = tempdir().unwrap();
    let (db_a, db_b, exp) = restore_clone(dir_a.path(), dir_b.path());

    let id_a = db_a.instance_id();
    let id_b = db_b.remint_instance_id().unwrap();
    assert_ne!(id_b, id_a);

    // Original: 1 (pre-copy) + 2 = 3 under id_a. Copy: 1 inherited under id_a
    // + 3 fresh under id_b. True total across both replicas: 6.
    reinforce_n(&db_a, exp, 2);
    reinforce_n(&db_b, exp, 3);

    sync_both_ways(&db_a, &db_b).await;

    let exp_a = db_a.get_experience(exp).unwrap().unwrap();
    let exp_b = db_b.get_experience(exp).unwrap().unwrap();
    assert_eq!(exp_a.applications(), 6, "original reports the exact sum");
    assert_eq!(
        exp_b.applications(),
        6,
        "restored copy reports the exact sum"
    );
    assert_eq!(
        exp_a.applications, exp_b.applications,
        "both converge to one map"
    );
    assert_eq!(exp_a.applications.get(&id_a), Some(&3));
    assert_eq!(exp_a.applications.get(&id_b), Some(&3));
    assert_eq!(exp_a.applications.len(), 2);
}

/// Characterises the hazard `remint_instance_id` exists for: the same flow
/// with the remint left out. Both replicas reinforce under one shared id, so
/// the per-key max merge keeps only the larger of the two buckets — the
/// smaller replica's increments are silently lost and the sum under-counts.
#[tokio::test]
async fn restored_clone_without_remint_loses_increments() {
    let dir_a = tempdir().unwrap();
    let dir_b = tempdir().unwrap();
    let (db_a, db_b, exp) = restore_clone(dir_a.path(), dir_b.path());
    let shared_id = db_a.instance_id();

    // No remint. Original: 1 + 2 = 3; copy: 1 + 3 = 4 — both under `shared_id`.
    // The true total across both replicas is still 6.
    reinforce_n(&db_a, exp, 2);
    reinforce_n(&db_b, exp, 3);

    sync_both_ways(&db_a, &db_b).await;

    let exp_a = db_a.get_experience(exp).unwrap().unwrap();
    let exp_b = db_b.get_experience(exp).unwrap().unwrap();
    assert!(
        exp_a.applications() < 6 && exp_b.applications() < 6,
        "without a remint the merged total must under-count: a={} b={}",
        exp_a.applications(),
        exp_b.applications()
    );
    // Precisely: one bucket, per-key max(3, 4) = 4 on both sides.
    assert_eq!(exp_a.applications, exp_b.applications);
    assert_eq!(exp_a.applications.len(), 1, "a single shared bucket");
    assert_eq!(exp_a.applications.get(&shared_id), Some(&4));
    assert_eq!(exp_a.applications(), 4);
}
