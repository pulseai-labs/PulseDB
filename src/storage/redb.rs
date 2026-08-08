//! redb storage engine implementation.
//!
//! This module provides the primary storage backend for PulseDB using
//! [redb](https://docs.rs/redb), a pure Rust embedded key-value store.
//!
//! # Features
//!
//! - ACID transactions with MVCC
//! - Single-writer, multiple-reader concurrency
//! - Automatic crash recovery
//! - Zero external dependencies (pure Rust)
//!
//! # File Layout
//!
//! When you open a database at `./pulse.db`, redb creates:
//! - `./pulse.db` - Main database file
//! - `./pulse.db.lock` - Lock file for writer coordination (may not be visible)

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use ::redb::{Database, ReadableTable};
// redb 3.0 moved begin_read()/cache_stats() onto the ReadableDatabase trait;
// it must be in scope for the ~40 db.begin_read() call sites below.
use redb::ReadableDatabase;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, instrument, warn};

use crate::activity::Activity;
use crate::collective::Collective;
use crate::config::DecayConfig;
use crate::experience::{Experience, ExperienceUpdate};
use crate::insight::DerivedInsight;
use crate::relation::{ExperienceRelation, RelationType};
use crate::types::{CollectiveId, ExperienceId, InsightId, InstanceId, RelationId, Timestamp};

use super::legacy_bincode;
#[cfg(feature = "sync")]
use super::schema::SYNC_CURSORS_TABLE;
use super::schema::{
    decode_collective_from_activity_key, encode_activity_key, encode_type_index_key,
    DatabaseMetadata, EntityTypeTag, ExperienceTypeTag, ExperienceV2, WatchEventRecord,
    WatchEventTypeTag, ACTIVITIES_TABLE, COLLECTIVES_TABLE, CURRENT_SUBSTRATE_FORMAT,
    DECAY_CONFIGS_TABLE, EMBEDDINGS_TABLE, EXPERIENCES_BY_COLLECTIVE_TABLE,
    EXPERIENCES_BY_TYPE_TABLE, EXPERIENCES_TABLE, INSIGHTS_BY_COLLECTIVE_TABLE, INSIGHTS_TABLE,
    INSTANCE_ID_KEY, LEGACY_SUBSTRATE_FORMAT, METADATA_TABLE, PROVIDER_IDENTITY_KEY,
    PROVIDER_IDENTITY_STAMPED_AT_KEY, RELATIONS_BY_SOURCE_TABLE, RELATIONS_BY_TARGET_TABLE,
    RELATIONS_TABLE, SCHEMA_VERSION, SUBSTRATE_FORMAT_KEY, SUBSTRATE_MAGIC, SUBSTRATE_MARKER_LEN,
    WAL_SEQUENCE_KEY, WATCH_EVENTS_TABLE,
};
use super::StorageEngine;
use crate::config::{Config, EmbeddingDimension, RecallWeights};
use crate::embedding::ProviderIdentity;
use crate::error::{PulseDBError, Result, StorageError, ValidationError};

/// Metadata key in the metadata table.
const METADATA_KEY: &str = "db_metadata";

// ============================================================================
// Serde-blob table registry (#55) — single source of truth for the codec pass
// ============================================================================
//
// `SERDE_BLOB_TABLES` is the ONE authoritative list of the serde-blob tables
// (redb table name + human "what" label) that `reencode_serde_blobs_to_postcard`
// re-encodes from legacy bincode to postcard. It is intentionally REFACTOR-NEUTRAL:
// it lists exactly the tables the loop already visited, with exactly the "what"
// labels the loop already used — the loop's observable behavior and ordering are
// unchanged. The registry exists so that (a) the re-encode owner asserts it visits
// every registered table (see the `debug_assert_eq!` inside the loop) and (b) the
// #55 exhaustiveness guard has a single membership set to check against.
//
// Copy-through tables are DELIBERATELY EXCLUDED (never decoded): the raw-f32
// `embeddings` table, the 5 multimap indexes (`experiences_by_collective`,
// `experiences_by_type`, `relations_by_source`, `relations_by_target`,
// `insights_by_collective`), and the raw `metadata` marker keys (`instance_id`,
// `wal_sequence`, `substrate_format`). Only the `db_metadata` KEY inside
// `metadata` is a serde blob, so `metadata` appears here once for that key.
//
// The list order mirrors the re-encode loop's visit order for auditability.
const SERDE_BLOB_TABLES: &[(&str, &str)] = &[
    ("metadata", "db_metadata"),
    ("collectives", "collective"),
    ("decay_configs", "decay_config"),
    ("experiences", "experience"),
    ("relations", "relation"),
    ("insights", "insight"),
    ("activities", "activity"),
    ("watch_events", "watch_event"),
    ("sync_cursors", "sync_cursor"),
];

// ============================================================================
// Substrate-format marker — raw, serializer-independent (NOT serde)
// ============================================================================
//
// The marker tells the open path which serializer wrote the file's values
// (bincode-era vs postcard-era). It therefore CANNOT itself be a serde/bincode
// blob — it is hand-encoded as raw bytes and read *before* any value is decoded.
// See `schema.rs` (`SUBSTRATE_FORMAT_KEY`, `CURRENT_SUBSTRATE_FORMAT`).

/// Outcome of reading the on-disk substrate-format marker.
///
/// This is the redb-file-format + value-codec axis (distinct from the logical
/// schema version). The marker is **monotonic**: `0`/absent = `{redb-v2, bincode}`
/// (legacy v0.5.1), `1` = `{redb-v3, bincode}` (the VS-4.0.2 end-state, values
/// still bincode), `2` = `{redb-v3, postcard}` (VS-4.0.3). The migration gate keys
/// off this enum to decide whether a store opens directly, must migrate forward,
/// or is forward-incompatible. See [`CURRENT_SUBSTRATE_FORMAT`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubstrateFormat {
    /// No marker key present ⇒ a pre-4.0 (bincode-era) database; treated as
    /// substrate format `0` (`LEGACY_SUBSTRATE_FORMAT`).
    Absent,
    /// Marker present and equal to `CURRENT_SUBSTRATE_FORMAT` ⇒ open directly.
    Current,
    /// Marker present and **older** than current ⇒ migrate forward. Carries the
    /// found version.
    Older(u8),
    /// Marker present and **newer** than current ⇒ forward-incompatible; do not
    /// touch. Carries the found version.
    Newer(u8),
}

impl SubstrateFormat {
    /// The substrate-format version this value represents.
    ///
    /// `Absent` maps to `LEGACY_SUBSTRATE_FORMAT` (0); the others carry their
    /// stored version.
    pub fn version(self) -> u8 {
        match self {
            SubstrateFormat::Absent => LEGACY_SUBSTRATE_FORMAT,
            SubstrateFormat::Current => CURRENT_SUBSTRATE_FORMAT,
            SubstrateFormat::Older(v) | SubstrateFormat::Newer(v) => v,
        }
    }

    /// Classifies a found substrate-format version against the current build.
    fn classify(found: u8) -> Self {
        use std::cmp::Ordering;
        match found.cmp(&CURRENT_SUBSTRATE_FORMAT) {
            Ordering::Equal => SubstrateFormat::Current,
            Ordering::Less => SubstrateFormat::Older(found),
            Ordering::Greater => SubstrateFormat::Newer(found),
        }
    }
}

/// Encodes the substrate-format marker as raw fixed-layout bytes.
///
/// Layout: `[SUBSTRATE_MAGIC[0], SUBSTRATE_MAGIC[1], version]` — 3 bytes,
/// hand-written, never through serde. This is the byte sequence stored under
/// `SUBSTRATE_FORMAT_KEY`.
fn encode_substrate_marker(version: u8) -> [u8; SUBSTRATE_MARKER_LEN] {
    [SUBSTRATE_MAGIC[0], SUBSTRATE_MAGIC[1], version]
}

/// Decodes a raw substrate-format marker, validating the magic prefix.
///
/// Returns the substrate-format version on success. A wrong length or a magic
/// mismatch is a corruption signal (NOT an Absent/legacy database — absence is
/// detected by the key being missing, which the caller handles separately).
fn decode_substrate_marker(bytes: &[u8]) -> Result<u8> {
    if bytes.len() != SUBSTRATE_MARKER_LEN {
        return Err(StorageError::corrupted(format!(
            "substrate_format marker has wrong length: expected {} bytes, found {}",
            SUBSTRATE_MARKER_LEN,
            bytes.len()
        ))
        .into());
    }
    if bytes[0] != SUBSTRATE_MAGIC[0] || bytes[1] != SUBSTRATE_MAGIC[1] {
        return Err(StorageError::corrupted(format!(
            "substrate_format marker magic mismatch: expected {:?}, found {:?}",
            SUBSTRATE_MAGIC,
            &bytes[..2.min(bytes.len())]
        ))
        .into());
    }
    Ok(bytes[2])
}

/// Deterministic sibling path retained before schema-v3 migrations.
fn pre_v3_backup_path(path: &Path) -> PathBuf {
    let mut backup = path.to_path_buf();
    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_else(|| "pulsedb.redb".into());
    backup.set_file_name(format!("{file_name}.pre-v3.bak"));
    backup
}

/// Deterministic sibling path retained before the redb-format substrate migration
/// (the v2→v3 in-place `upgrade()`).
///
/// This is a **distinct sidecar** from [`pre_v3_backup_path`] (`.pre-v3.bak`, the
/// logical-schema migration backup). It captures the pristine `{redb-v2, bincode}`
/// file before the destructive in-place redb upgrade and is the rollback point for
/// the substrate migration. Never clobbers an existing `.pre-v3.bak`.
fn pre_substrate_backup_path(path: &Path) -> PathBuf {
    let mut backup = path.to_path_buf();
    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_else(|| "pulsedb.redb".into());
    backup.set_file_name(format!("{file_name}.pre-substrate.bak"));
    backup
}

/// Sibling temp path where [`backup_once`] stages the sidecar bytes before
/// atomically renaming them onto `backup_path`. A distinct `.tmp` suffix so it
/// never collides with the published `.pre-substrate.bak`; a crash before the
/// rename leaves only this temp (the final sidecar path stays absent).
fn backup_temp_path(backup_path: &Path) -> PathBuf {
    let mut temp = backup_path.as_os_str().to_owned();
    temp.push(".tmp");
    PathBuf::from(temp)
}

/// Whether [`backup_once`] wrote a fresh sidecar or preserved an existing one.
///
/// The caller (`create_or_migrate`) needs the distinction: only a `Created` sidecar
/// is this attempt's disposable copy (safe to remove on a lock-aborted upgrade). A
/// `Preserved` sidecar may be a valid rollback point left by an EARLIER attempt and
/// must never be deleted by a later attempt's failure handling.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum BackupOutcome {
    /// This call created the `.pre-substrate.bak` (a fresh copy of `src`).
    Created,
    /// A published sidecar already existed and was preserved untouched.
    Preserved,
}

/// Atomically publish a backup sidecar of `src` at `backup_path`.
///
/// The sidecar is the rollback point 4.05's kill-at-boundary crash tests trust,
/// so it must be **all-or-nothing**: a later open must see either a complete,
/// pristine sidecar or none at all — never a truncated/partial file it would
/// mistake for genuine. To guarantee that across an abrupt process death:
///
/// 1. If `backup_path` already holds a published sidecar, preserve it untouched
///    and return `Preserved` — an idempotent no-op (even if `src` has since
///    diverged).
/// 2. Otherwise stage the copy at a sibling temp path ([`backup_temp_path`]),
///    `sync_all` its bytes (#53c durability), then **atomically `rename`** it onto
///    `backup_path` and return `Created`. `rename` is atomic on POSIX and Windows,
///    so the final path transitions absent → fully-formed in one step.
///
/// #5 / T7 — a crash after the temp copy but before the rename leaves only the
/// temp; the final path stays ABSENT, so the retry re-copies a fresh pristine
/// backup. This replaces the earlier `create_new`-on-the-final-path design, where a
/// hard kill after the claim but before the fsync left a truncated file at the final
/// path that the `AlreadyExists` preserve-branch later treated as a genuine sidecar,
/// silently discarding the pristine pre-migration bytes.
///
/// Symlink-safety (PR#57-review P2): the temp is opened by `remove_file` (which
/// unlinks the entry itself, never following a symlink) followed by `create_new`
/// (O_EXCL) — so a hostile symlink pre-planted at the temp path by another local
/// user is unlinked, never written THROUGH to an attacker-chosen target, and a fresh
/// regular file is always created. This also overwrites a stale temp from a prior
/// crash, so it never wedges the retry. (Plain `create+truncate` would follow such a
/// symlink; plain `create_new` would wedge on a stale temp.)
///
/// After the rename, the parent directory is fsync'd (best-effort) so the new
/// directory entry is itself durable; an unsupported dir-fsync is not fatal — the
/// sidecar's own bytes were already made durable before the rename.
///
/// Lock classification is scoped to the ONE op that reads the redb file (`src`); all
/// sidecar-space ops map raw I/O errors to plain `Io`, so a Windows sharing violation
/// on the sidecar is never mislabeled as the retryable `DatabaseLocked`.
///
/// `backup_once` is only ever called under the exclusive [`MigrationLock`] (see
/// `create_or_migrate`), so the exists-check + stage + rename does not race a
/// second migrator.
fn backup_once(src: &Path, backup_path: &Path) -> Result<BackupOutcome> {
    // (1) Idempotent preserve: a genuine sidecar already published at the final path
    // is kept untouched (never re-copied, even if `src` has since diverged). Sidecar-
    // space op ⇒ plain `Io` (never redb lock contention).
    if backup_path.try_exists().map_err(PulseDBError::Io)? {
        debug!("backup sidecar already exists; preserving it");
        return Ok(BackupOutcome::Preserved);
    }

    // (2) Stage at a sibling temp, then atomically rename onto the final path.
    let temp_path = backup_temp_path(backup_path);
    // Symlink-safe open: unlink any pre-existing temp entry (a stale temp from a
    // prior crash OR a hostile symlink) — `remove_file` removes the entry itself,
    // never following a symlink — then `create_new` (O_EXCL) creates a fresh regular
    // file. We therefore never write THROUGH a symlink to an attacker-chosen target,
    // and never wedge on a stale temp. A concurrent re-plant makes `create_new` fail
    // closed rather than follow it; legitimately there is no race (MigrationLock).
    let _ = std::fs::remove_file(&temp_path);
    let mut temp_file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)
        .map_err(PulseDBError::Io)?;
    // Open the redb file `src`: reads of `src` are the lock-contention signal — a
    // concurrent migrator holding the redb file lock surfaces on Windows as a raw
    // lock/sharing violation, so classify it as the typed, retryable DatabaseLocked
    // (audit C2).
    let mut source = match std::fs::File::open(src) {
        Ok(s) => s,
        Err(error) => {
            drop(temp_file);
            let _ = std::fs::remove_file(&temp_path);
            return Err(migration_io_error(error));
        }
    };
    // Copy with a manual loop so failures are classified BY SIDE (`io::copy`
    // conflates them): a READ error on `src` — e.g. a concurrent migrator holding a
    // redb byte-range lock, which on Windows is `ERROR_LOCK_VIOLATION` (os error 33)
    // — is genuine lock contention ⇒ the typed retryable DatabaseLocked (audit C2);
    // a WRITE error to OUR temp (short write, disk full, or an AV/indexer sharing
    // violation on the temp) is sidecar-space ⇒ plain `Io`, never a false lock.
    {
        use std::io::{Read, Write};
        let mut buf = [0u8; 64 * 1024];
        loop {
            let n = match source.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => n,
                Err(error) => {
                    drop(temp_file);
                    let _ = std::fs::remove_file(&temp_path);
                    return Err(migration_io_error(error)); // `src` read ⇒ lock-classify
                }
            };
            if let Err(error) = temp_file.write_all(&buf[..n]) {
                drop(temp_file);
                let _ = std::fs::remove_file(&temp_path);
                return Err(PulseDBError::Io(error)); // temp write ⇒ plain `Io`
            }
        }
    }
    drop(source);
    // VS-4.0.4 (#46) crash boundary: pre-txn, the TEMP bytes are copied but NOT yet
    // fsync'd/renamed — the exact window #53c + #5 harden. A crash here leaves the
    // partial temp and NO final sidecar. Compiled out unless `fault-injection` is on.
    #[cfg(feature = "fault-injection")]
    crate::fault_injection::maybe_inject(crate::fault_injection::Boundary::MidBackupPreFsync);
    // #53c: fsync the temp's own bytes so the sidecar we are about to publish is
    // durable before the rename makes it visible at the final path. Sidecar-space
    // op ⇒ plain `Io`.
    if let Err(error) = temp_file.sync_all() {
        drop(temp_file);
        let _ = std::fs::remove_file(&temp_path);
        return Err(PulseDBError::Io(error));
    }
    drop(temp_file);
    // Atomically publish the completed sidecar: the final path goes absent →
    // fully-formed in one step. On failure, remove the temp so it cannot wedge.
    // Sidecar-space op ⇒ plain `Io` (the operands are our own temp and the absent
    // sidecar path — never the redb file).
    if let Err(error) = std::fs::rename(&temp_path, backup_path) {
        let _ = std::fs::remove_file(&temp_path);
        return Err(PulseDBError::Io(error));
    }
    // #53c: fsync the parent directory so the rename (the new directory entry) is
    // itself durable. Best-effort — unsupported dir-fsync is not fatal.
    if let Some(parent) = backup_path.parent() {
        if let Ok(dir) = std::fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    Ok(BackupOutcome::Created)
}

/// Deterministic sibling path for the PulseDB-owned migration lock.
///
/// Distinct from redb's own `.lock` and the watch coordinator's `.watch.lock`.
/// Serializes concurrent substrate migrators so two processes cannot race the
/// destructive in-place redb `upgrade()`.
fn migration_lock_path(db_path: &Path) -> PathBuf {
    let mut lock_path = db_path.as_os_str().to_owned();
    lock_path.push(".migrate.lock");
    PathBuf::from(lock_path)
}

/// An exclusive advisory lock guarding the substrate migration, released on drop.
///
/// Mirrors the `fs2` advisory-lock seam used by `src/watch/lock.rs` (the
/// `WatchLock`). Acquired **before** the backup + redb-v2 `upgrade()` and held
/// until the redb-4.1 reopen completes, so two upgraders cannot race the
/// destructive in-place upgrade (audit C2).
struct MigrationLock {
    // The locked file handle. Dropping it releases the OS advisory lock on all
    // platforms (POSIX + Windows); no explicit unlock (`unlock` needs Rust 1.89+,
    // above MSRV — see watch/lock.rs).
    _file: std::fs::File,
}

impl MigrationLock {
    /// Acquires the exclusive migration lock, blocking until it is available.
    fn acquire_exclusive(db_path: &Path) -> Result<Self> {
        use fs2::FileExt;
        let path = migration_lock_path(db_path);
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(PulseDBError::Io)?;
        file.lock_exclusive().map_err(PulseDBError::Io)?;
        Ok(Self { _file: file })
    }
}

/// Classifies a raw OS I/O error as database-file lock contention. On Windows a
/// concurrent opener contending for the file (e.g. during the one-time pre-v3
/// backup copy, while another process still holds the redb file lock) surfaces as
/// `ERROR_SHARING_VIOLATION` (32) or `ERROR_LOCK_VIOLATION` (33) rather than
/// redb's typed `DatabaseAlreadyOpen`. Mapping these to the typed, retryable
/// [`StorageError::DatabaseLocked`] keeps the concurrency contract (audit C2)
/// cross-platform. NOTE: Unix errno 32/33 are `EPIPE`/`EDOM` (unrelated), so this
/// classification is Windows-only; the non-Windows stub always returns `false`.
#[cfg(windows)]
fn is_lock_contention_io_error(error: &std::io::Error) -> bool {
    // ERROR_SHARING_VIOLATION = 32, ERROR_LOCK_VIOLATION = 33.
    matches!(error.raw_os_error(), Some(32) | Some(33))
}

#[cfg(not(windows))]
fn is_lock_contention_io_error(_error: &std::io::Error) -> bool {
    false
}

/// Maps a `std::io::Error` from a migration file operation to a `PulseDBError`,
/// promoting a Windows lock/sharing violation (see [`is_lock_contention_io_error`])
/// to the typed, retryable [`StorageError::DatabaseLocked`] so the concurrency
/// contract (audit C2) holds cross-platform, while passing every other I/O error
/// through unchanged as [`PulseDBError::Io`].
fn migration_io_error(error: std::io::Error) -> PulseDBError {
    if is_lock_contention_io_error(&error) {
        PulseDBError::Storage(StorageError::DatabaseLocked)
    } else {
        PulseDBError::Io(error)
    }
}

/// Runs the one-time redb file-format upgrade (v2 → v3) in place via the aliased
/// redb 2.6 dependency, then drops the 2.6 handle to release its lock.
///
/// `Database::upgrade()` lives **only** on the redb 2.6.x line (absent in 3.x/4.x).
/// It rewrites the on-disk file format v2 → v3 in place; values are untouched
/// (still bincode). The call is idempotent — `upgrade()` returns `Ok(false)` if
/// the file is already v3.
///
/// An old-version process still holding the redb file surfaces as a redb-2.6
/// `DatabaseError::DatabaseAlreadyOpen`, which we map to the typed
/// [`StorageError::DatabaseLocked`] rather than corrupting or spinning (audit C2).
fn upgrade_redb_v2_to_v3(path: &Path) -> Result<()> {
    // Test-only: a forced upgrade outcome lets an integration test drive
    // `create_or_migrate`'s post-backup sidecar-cleanup branch (#4 / T7)
    // deterministically. This fires BEFORE the redb-v2 open, so the store is
    // untouched (matching real DatabaseLocked semantics). Compiled out unless the
    // `fault-injection` feature is on.
    #[cfg(feature = "fault-injection")]
    if let Some(fault) = crate::fault_injection::take_upgrade_fault() {
        return Err(match fault {
            crate::fault_injection::UpgradeFault::Locked => {
                PulseDBError::Storage(StorageError::DatabaseLocked)
            }
            crate::fault_injection::UpgradeFault::Torn => PulseDBError::Storage(
                StorageError::Redb("fault-injection: torn in-place v2->v3 upgrade".into()),
            ),
        });
    }

    // Open under redb 2.6 (the v2→v3 bridge). A lock conflict means an old-version
    // process still holds the file — fail typed, do not corrupt.
    let mut db = redb_v2::Database::open(path).map_err(|e| match e {
        redb_v2::DatabaseError::DatabaseAlreadyOpen => {
            PulseDBError::Storage(StorageError::DatabaseLocked)
        }
        other => PulseDBError::Storage(StorageError::Redb(format!(
            "redb 2.6 open (for v2->v3 upgrade) failed: {other}"
        ))),
    })?;

    db.upgrade().map_err(|e| {
        PulseDBError::Storage(StorageError::Redb(format!(
            "redb 2.6 v2->v3 upgrade() failed: {e}"
        )))
    })?;

    // Drop the redb-2.6 handle explicitly to release its file lock BEFORE the
    // redb-4.1 reopen. (Returning would drop it anyway; explicit for clarity.)
    drop(db);
    Ok(())
}

/// Cleans up a now-possibly-stale `.pre-substrate.bak` after the destructive redb
/// v2→v3 upgrade aborted, given the upgrade `err`.
///
/// The caller invokes this ONLY when `backup_once` returned [`BackupOutcome::Created`]
/// (i.e. this attempt wrote the sidecar) — a preserved pre-existing sidecar may be a
/// valid rollback point from an earlier attempt and is never removed here.
///
/// #4 / T4. The sidecar is claimed by `backup_once` *before* [`upgrade_redb_v2_to_v3`]
/// runs. If the upgrade aborts with [`StorageError::DatabaseLocked`], the redb-v2
/// open (not the in-place `upgrade()`) failed, so the store is provably untouched —
/// and a legacy writer may have committed between the redb-4.1 `UpgradeRequired`
/// probe and the backup, making the sidecar a STALE snapshot that misses the
/// writer's latest data. Leaving it lets the next open's `AlreadyExists` branch
/// preserve it as genuine, so remove it: the retry re-backs-up a fresh pristine
/// copy. Every OTHER upgrade error (a torn in-place `upgrade()`) KEEPS the sidecar —
/// there it is the rollback point for a partially-rewritten primary. Best-effort
/// removal: a failure to unlink is non-fatal (the retry's `backup_once` would still
/// preserve it, the pre-fix behaviour).
fn cleanup_stale_backup_if_lock_aborted(path: &Path, err: &PulseDBError) {
    if matches!(err, PulseDBError::Storage(StorageError::DatabaseLocked)) {
        let _ = std::fs::remove_file(pre_substrate_backup_path(path));
    }
}

/// Reserved legacy bucket for scalar v2 application counts.
///
/// The bytes spell `PULSEDB_LEGACY__`; freshly minted UUIDv7 instance ids
/// cannot collide with this fixed non-v7 sentinel.
fn legacy_applications_instance_id() -> InstanceId {
    InstanceId::from_bytes(*b"PULSEDB_LEGACY__")
}

#[derive(Debug, Deserialize, Serialize)]
struct StoredDecayConfig {
    half_life_secs: u64,
    half_life_nanos: u32,
    freq_weight: f32,
    floor: f32,
    auto_archive_below_floor: bool,
    default_recall_weights: Option<RecallWeights>,
}

#[derive(Debug, Deserialize)]
struct StoredDecayConfigV1 {
    half_life_secs: u64,
    freq_weight: f32,
    floor: f32,
    auto_archive_below_floor: bool,
    default_recall_weights: Option<RecallWeights>,
}

impl From<&DecayConfig> for StoredDecayConfig {
    fn from(config: &DecayConfig) -> Self {
        Self {
            half_life_secs: config.half_life.as_secs(),
            half_life_nanos: config.half_life.subsec_nanos(),
            freq_weight: config.freq_weight,
            floor: config.floor,
            auto_archive_below_floor: config.auto_archive_below_floor,
            default_recall_weights: config.default_recall_weights,
        }
    }
}

impl From<StoredDecayConfigV1> for DecayConfig {
    fn from(config: StoredDecayConfigV1) -> Self {
        Self {
            half_life: std::time::Duration::from_secs(config.half_life_secs),
            freq_weight: config.freq_weight,
            floor: config.floor,
            auto_archive_below_floor: config.auto_archive_below_floor,
            default_recall_weights: config.default_recall_weights,
        }
    }
}

impl From<StoredDecayConfig> for DecayConfig {
    fn from(config: StoredDecayConfig) -> Self {
        Self {
            half_life: std::time::Duration::new(config.half_life_secs, config.half_life_nanos),
            freq_weight: config.freq_weight,
            floor: config.floor,
            auto_archive_below_floor: config.auto_archive_below_floor,
            default_recall_weights: config.default_recall_weights,
        }
    }
}

/// redb storage engine wrapper.
///
/// This struct holds the redb database handle and cached metadata.
/// It implements [`StorageEngine`] for use with PulseDB.
///
/// # Thread Safety
///
/// `RedbStorage` is `Send + Sync`. redb handles internal synchronization
/// using MVCC for readers and exclusive locking for writers.
#[derive(Debug)]
pub struct RedbStorage {
    /// The redb database handle.
    db: Database,

    /// Cached database metadata.
    metadata: DatabaseMetadata,

    /// Path to the database file.
    path: PathBuf,

    /// Persistent instance ID for local G-counter buckets and sync protocol.
    instance_id: InstanceId,
}

impl RedbStorage {
    /// Opens or creates a database at the given path.
    ///
    /// If the database doesn't exist, it will be created and initialized
    /// with the configuration settings. If it exists, the configuration
    /// will be validated against the stored metadata.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the database file
    /// * `config` - Database configuration
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The database file is corrupted
    /// - The database is locked by another process
    /// - Schema version doesn't match
    /// - Embedding dimension doesn't match (for existing databases)
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> pulsedb::Result<()> {
    /// # let dir = tempfile::tempdir().unwrap();
    /// use pulsedb::{Config, storage::RedbStorage};
    ///
    /// let storage = RedbStorage::open(dir.path().join("test.db"), &Config::default())?;
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(skip(config), fields(path = %path.as_ref().display()))]
    pub fn open(path: impl AsRef<Path>, config: &Config) -> Result<Self> {
        let path = path.as_ref();
        let db_exists = path.exists();

        debug!(db_exists = db_exists, "Opening storage engine");

        // Create or open the database under redb 4.1. A v2-format file (every
        // <=v0.5.1 database) hard-refuses here with `UpgradeRequired` *before* any
        // value is reachable — the redb-format axis is detected here, the
        // codec/schema axes inside `open_existing`. `create_or_migrate` runs the
        // one-time redb v2->v3 upgrade-on-open (FR-035 read-only gate + lock +
        // backup) and returns a v3 handle. (upgrade-on-open design §2 step A.)
        let db = Self::create_or_migrate(path, config)?;

        if db_exists {
            // Validate existing database
            Self::open_existing(db, path.to_path_buf(), config)
        } else {
            // Initialize new database
            Self::initialize_new(db, path.to_path_buf(), config)
        }
    }

    /// Opens the redb-4.1 database, running the one-time redb file-format
    /// (v2 → v3) upgrade-on-open if the file is a legacy v2 store.
    ///
    /// Flow (upgrade-on-open design §2 step A + §11 C2/C6):
    /// 1. Try `redb 4.1` `create(path)`. `Ok` ⇒ already v3, return the handle.
    /// 2. `Err(UpgradeRequired)` ⇒ a redb-v2 file needing migration:
    ///    - **FR-035 read-only gate** — a read-only open returns
    ///      [`PulseDBError::ReadOnly`] with **zero writes** (no lock, no backup, no
    ///      upgrade) *before* anything else.
    ///    - acquire the exclusive migration lock (audit C2);
    ///    - **re-try** `redb 4.1` create — another migrator may have finished while
    ///      we blocked on the lock; if so, release + proceed;
    ///    - back up the pristine `{redb-v2, bincode}` file to `.pre-substrate.bak`;
    ///    - run the redb-2.6 `upgrade()` (v2→v3, in place, values untouched), drop
    ///      the 2.6 handle to release its lock;
    ///    - release the migration lock, reopen under redb 4.1 (now v3).
    fn create_or_migrate(path: &Path, config: &Config) -> Result<Database> {
        match Self::create_database(path, config) {
            Ok(db) => Ok(db),
            Err(PulseDBError::Storage(StorageError::SubstrateUpgradeRequired {
                found, ..
            })) => {
                // FR-035 / audit C6: a read-only open of an un-migrated (redb-v2)
                // store returns ReadOnly BEFORE any write — no lock, no backup, no
                // upgrade. A read-only open performs ZERO writes.
                if config.read_only {
                    debug!(
                        redb_format = found,
                        "read-only open of redb-v2 store; refusing (migration needs write access)"
                    );
                    return Err(PulseDBError::ReadOnly);
                }

                info!(
                    redb_format = found,
                    "redb file-format v2 detected; migrating in place to v3 (one-time, \
                     exempt from the <100ms open budget — values stay bincode)"
                );

                // Audit C2: serialize concurrent migrators so two processes cannot
                // race the destructive in-place upgrade(). Hold the lock across the
                // backup + upgrade + reopen.
                let _migration_lock = MigrationLock::acquire_exclusive(path)?;

                // Re-check after acquiring the lock: another migrator may have
                // completed the upgrade while we blocked. If `create` now succeeds,
                // the file is already v3 — release the lock and proceed.
                match Self::create_database(path, config) {
                    Ok(db) => {
                        debug!("file already migrated by a concurrent upgrader; proceeding");
                        return Ok(db);
                    }
                    Err(PulseDBError::Storage(StorageError::SubstrateUpgradeRequired {
                        ..
                    })) => {
                        // Still v2 — we own the migration.
                    }
                    Err(other) => return Err(other),
                }

                // #53a — PREFLIGHT BEFORE DESTRUCTION. Run the headroom preflight
                // INSIDE the migration lock, AFTER the post-lock re-check, and
                // IMMEDIATELY BEFORE `backup_once` + the destructive
                // `upgrade_redb_v2_to_v3`. So a too-large redb-v2 store fails closed
                // with ZERO file mutation and NO `.pre-substrate.bak` written; the
                // lock still serializes concurrent migrators; and a peer that
                // already upgraded is let through by the re-check above (not
                // fail-closed here). The FR-035 read-only carve-out already ran
                // above the arm (a read-only too-large open returned `ReadOnly`, not
                // `SubstrateMigrationTooLarge`), so this arm is writable-only.
                //
                // The store is still redb-v2 here (not openable under redb 4.1), so
                // the copy-through split is unmeasurable — pass `copy_through_bytes =
                // 0`, the conservative over-estimate (whole store treated as
                // re-encodable → biased fail-closed = the safe side). The
                // copy_through_excluded refinement (#54) that lets embedding-heavy
                // stores open runs later in `open_existing`, once the file is v3.
                {
                    let store_size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
                    let available = fs2::available_space(path).unwrap_or(u64::MAX);
                    // Destructive redb-v2→v3 path: a .pre-substrate.bak IS written here.
                    Self::resolve_migration_headroom(store_size, 0, available, config, true)?;
                }

                // Back up the pristine {redb-v2, bincode} file (the rollback point)
                // before the destructive in-place upgrade. Distinct sidecar from
                // the schema-v3 `.pre-v3.bak`.
                let backup_outcome = backup_once(path, &pre_substrate_backup_path(path))?;

                // redb v2 -> v3 in place via the aliased redb 2.6; drop the 2.6
                // handle (release its lock) before the redb-4.1 reopen.
                if let Err(err) = upgrade_redb_v2_to_v3(path) {
                    // #4 / T4: a lock-aborted upgrade leaves the store untouched, so
                    // the `.pre-substrate.bak` may be a stale snapshot — remove it so
                    // the retry re-backs-up fresh. But ONLY if THIS attempt created it:
                    // a PRESERVED sidecar may be a valid rollback point left by an
                    // earlier attempt and must not be discarded (PR#57-review P2). A
                    // torn upgrade (any other error) always KEEPS the sidecar.
                    if backup_outcome == BackupOutcome::Created {
                        cleanup_stale_backup_if_lock_aborted(path, &err);
                    }
                    return Err(err);
                }

                // VS-4.0.4 (#46) crash boundary: pre-txn, AFTER the destructive
                // in-place redb v2→v3 upgrade but BEFORE the redb-4.1 reopen.
                // Recovery relies on the `.pre-substrate.bak` claimed above.
                // Compiled out unless the `fault-injection` feature is on.
                #[cfg(feature = "fault-injection")]
                crate::fault_injection::maybe_inject(
                    crate::fault_injection::Boundary::PostRedbUpgrade,
                );

                // Reopen under redb 4.1 — now a v3 file, so `create` succeeds.
                let db = Self::create_database(path, config)?;
                info!("redb file-format migration complete (v2 -> v3)");
                Ok(db)
            }
            Err(other) => Err(other),
        }
    }

    /// Creates the redb database with appropriate settings.
    fn create_database(path: &Path, _config: &Config) -> Result<Database> {
        let builder = Database::builder();

        // Note: redb's Builder exposes set_cache_size (since 2.x); PulseDB does not
        // wire it yet, so the cache_size_mb config remains reserved for a future
        // optimization pass. redb manages its read/write cache internally by default.

        // redb 4.x returns a typed DatabaseError. A lock conflict (another process
        // already holds the database) is DatabaseError::DatabaseAlreadyOpen — its
        // Display is "Database already open. Cannot acquire lock." (note: the word
        // "locked" no longer appears, so the prior string match on "locked" would
        // have silently fallen through to the generic Redb error). Match the typed
        // variant instead of string-matching.
        //
        // `UpgradeRequired(found)` means the on-disk file is an OLD redb format
        // (v2 — every <=v0.5.1 PulseDB database) that redb 4.x refuses to open.
        // It is surfaced as the typed `SubstrateUpgradeRequired` so the caller
        // (`create_or_migrate`) can run the upgrade-on-open path; `found` carries
        // the on-disk redb format version redb reported.
        let db = builder.create(path).map_err(|e| match e {
            ::redb::DatabaseError::DatabaseAlreadyOpen => {
                PulseDBError::Storage(StorageError::DatabaseLocked)
            }
            ::redb::DatabaseError::UpgradeRequired(found) => PulseDBError::Storage(
                StorageError::substrate_upgrade_required(found, CURRENT_SUBSTRATE_FORMAT),
            ),
            other => PulseDBError::Storage(StorageError::Redb(other.to_string())),
        })?;

        debug!("Database file opened successfully");
        Ok(db)
    }

    /// Reads the raw substrate-format marker, before decoding any serde value.
    ///
    /// This is the bootstrapping read: it inspects raw bytes under
    /// `SUBSTRATE_FORMAT_KEY` directly, so it is valid regardless of which
    /// serializer wrote the rest of the file. The result classifies the store as
    /// [`SubstrateFormat::Absent`] (no key ⇒ pre-4.0 / bincode era),
    /// [`SubstrateFormat::Current`], [`SubstrateFormat::Older`], or
    /// [`SubstrateFormat::Newer`].
    ///
    /// A present-but-malformed marker (wrong length or bad magic) is a corruption
    /// error, *not* an Absent database — absence is signalled only by the key
    /// being missing entirely.
    ///
    /// Note: this is a pure read primitive. It does NOT itself trigger any
    /// migration; the migration decision (acting on `Older`/`Absent`/`Newer`) lives
    /// in `open_existing`, which consumes this.
    pub(crate) fn read_substrate_marker(db: &Database) -> Result<SubstrateFormat> {
        let read_txn = db.begin_read().map_err(StorageError::from)?;
        let meta_table = read_txn
            .open_table(METADATA_TABLE)
            .map_err(|e| StorageError::corrupted(format!("Cannot open metadata table: {}", e)))?;

        match meta_table
            .get(SUBSTRATE_FORMAT_KEY)
            .map_err(StorageError::from)?
        {
            None => Ok(SubstrateFormat::Absent),
            Some(entry) => {
                let version = decode_substrate_marker(entry.value())?;
                Ok(SubstrateFormat::classify(version))
            }
        }
    }

    /// Reads `db_metadata`, decoding with the codec implied by the substrate
    /// marker (bootstrapping — design §2.2).
    ///
    /// This runs at `open_existing` start, **before** the codec re-encode pass, so
    /// it cannot assume the values are postcard yet. It branches on the raw marker
    /// (read serializer-independently via [`read_substrate_marker`]):
    /// - `marker == Current` (`{redb-v3, postcard}`) ⇒ `postcard::from_bytes`;
    /// - `Absent | Older` (bincode-era values not yet re-encoded) ⇒
    ///   `legacy_bincode::decode` (1.01 vendored reader).
    ///
    /// `Newer` is handled by the caller (`open_existing`) before any value read, so
    /// it never reaches here; we treat it as the postcard path defensively.
    fn read_metadata(db: &Database) -> Result<DatabaseMetadata> {
        let marker = Self::read_substrate_marker(db)?;
        let read_txn = db.begin_read().map_err(StorageError::from)?;
        let meta_table = read_txn
            .open_table(METADATA_TABLE)
            .map_err(|e| StorageError::corrupted(format!("Cannot open metadata table: {}", e)))?;

        let metadata_bytes = meta_table
            .get(METADATA_KEY)
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::corrupted("Missing database metadata"))?;

        match marker {
            // NOTE: `Absent` (a pre-marker store) and `Older(_)` (a marker present but below
            // CURRENT_SUBSTRATE_FORMAT) share this single decode path today. They are
            // intentionally unified because both indicate a bincode-era store that decodes the
            // same way. IF a future change splits these arms (e.g. to give `Older(_)` a
            // distinct decode or validation), it MUST add a covering test for the `Older(_)`
            // branch against a genuine marker-N store in that state — the #53b evidence
            // currently derives `Older(1)` from a crashed marker-Absent store, which does not
            // exercise the split path. (VS-4.1.4/4.03 deferred this fixture as speculative
            // until CURRENT_SUBSTRATE_FORMAT actually bumps; see the slice README disposition.)
            SubstrateFormat::Absent | SubstrateFormat::Older(_) => {
                legacy_bincode::decode::<DatabaseMetadata>(metadata_bytes.value()).map_err(|e| {
                    StorageError::corrupted(format!("Invalid legacy metadata format: {}", e)).into()
                })
            }
            // A store written by a NEWER PulseDB (marker > CURRENT) may encode its
            // metadata in a format this build cannot read; refuse with the actionable
            // forward-incompatibility error BEFORE attempting a postcard decode that
            // would otherwise surface as a misleading Corrupted/SchemaVersionMismatch
            // instead of the intended "upgrade PulseDB" signal.
            SubstrateFormat::Newer(found) => {
                Err(StorageError::substrate_format_too_new(found, CURRENT_SUBSTRATE_FORMAT).into())
            }
            SubstrateFormat::Current => {
                postcard::from_bytes::<DatabaseMetadata>(metadata_bytes.value()).map_err(|e| {
                    StorageError::corrupted(format!("Invalid metadata format: {}", e)).into()
                })
            }
        }
    }

    fn validate_existing_metadata(metadata: &DatabaseMetadata, config: &Config) -> Result<()> {
        if !matches!(metadata.schema_version, 1..=SCHEMA_VERSION) {
            warn!(
                expected = SCHEMA_VERSION,
                found = metadata.schema_version,
                "Schema version mismatch"
            );
            return Err(PulseDBError::Storage(StorageError::SchemaVersionMismatch {
                expected: SCHEMA_VERSION,
                found: metadata.schema_version,
            }));
        }

        if metadata.embedding_dimension != config.embedding_dimension {
            warn!(
                expected = config.embedding_dimension.size(),
                found = metadata.embedding_dimension.size(),
                "Embedding dimension mismatch"
            );
            return Err(PulseDBError::Validation(
                ValidationError::DimensionMismatch {
                    expected: config.embedding_dimension.size(),
                    got: metadata.embedding_dimension.size(),
                },
            ));
        }

        Ok(())
    }

    /// Initializes a new database with tables and metadata.
    #[instrument(skip(db, config), fields(path = %path.display()))]
    fn initialize_new(db: Database, path: PathBuf, config: &Config) -> Result<Self> {
        info!("Initializing new database");

        let metadata = DatabaseMetadata::new(config.embedding_dimension);

        // Create all tables and write metadata in a single transaction
        let write_txn = db.begin_write().map_err(StorageError::from)?;

        {
            // Create the metadata table and write metadata
            let mut meta_table = write_txn.open_table(METADATA_TABLE)?;
            let metadata_bytes = postcard::to_stdvec(&metadata)
                .map_err(|e| StorageError::serialization(e.to_string()))?;
            meta_table.insert(METADATA_KEY, metadata_bytes.as_slice())?;

            // Create other tables (they're created on first access)
            let _ = write_txn.open_table(COLLECTIVES_TABLE)?;
            let _ = write_txn.open_table(DECAY_CONFIGS_TABLE)?;
            let _ = write_txn.open_table(EXPERIENCES_TABLE)?;
            let _ = write_txn.open_table(EMBEDDINGS_TABLE)?;
            let _ = write_txn.open_multimap_table(EXPERIENCES_BY_COLLECTIVE_TABLE)?;
            let _ = write_txn.open_multimap_table(EXPERIENCES_BY_TYPE_TABLE)?;
            let _ = write_txn.open_table(RELATIONS_TABLE)?;
            let _ = write_txn.open_multimap_table(RELATIONS_BY_SOURCE_TABLE)?;
            let _ = write_txn.open_multimap_table(RELATIONS_BY_TARGET_TABLE)?;
            let _ = write_txn.open_table(INSIGHTS_TABLE)?;
            let _ = write_txn.open_multimap_table(INSIGHTS_BY_COLLECTIVE_TABLE)?;
            let _ = write_txn.open_table(ACTIVITIES_TABLE)?;
            let _ = write_txn.open_table(WATCH_EVENTS_TABLE)?;

            let instance_id = InstanceId::new();
            meta_table.insert(INSTANCE_ID_KEY, instance_id.as_bytes().as_slice())?;

            // A fresh database is created at the current substrate format. The
            // marker is raw, hand-encoded bytes (NEVER serde) so it is readable
            // before the serializer identity is known — see `read_substrate_marker`.
            let substrate_marker = encode_substrate_marker(CURRENT_SUBSTRATE_FORMAT);
            meta_table.insert(SUBSTRATE_FORMAT_KEY, substrate_marker.as_slice())?;

            #[cfg(feature = "sync")]
            {
                let _ = write_txn.open_table(SYNC_CURSORS_TABLE)?;
            }
        }

        write_txn.commit().map_err(StorageError::from)?;

        let instance_id = {
            let read_txn = db.begin_read().map_err(StorageError::from)?;
            let meta_table = read_txn.open_table(METADATA_TABLE)?;
            let entry = meta_table
                .get(INSTANCE_ID_KEY)?
                .ok_or_else(|| StorageError::corrupted("Missing instance_id after init"))?;
            let bytes: [u8; 16] = entry
                .value()
                .try_into()
                .map_err(|_| StorageError::corrupted("invalid instance_id bytes"))?;
            InstanceId::from_bytes(bytes)
        };

        info!(
            schema_version = SCHEMA_VERSION,
            dimension = config.embedding_dimension.size(),
            "Database initialized"
        );

        Ok(Self {
            db,
            metadata,
            path,
            instance_id,
        })
    }

    /// Opens and validates an existing database.
    #[instrument(skip(db, config), fields(path = %path.display()))]
    fn open_existing(db: Database, path: PathBuf, config: &Config) -> Result<Self> {
        info!("Opening existing database");

        let mut metadata = Self::read_metadata(&db)?;
        Self::validate_existing_metadata(&metadata, config)?;
        let mut needs_v2_migration = metadata.schema_version == 1;
        let mut needs_v3_migration = metadata.schema_version <= 2;

        if needs_v3_migration && config.read_only {
            return Err(PulseDBError::ReadOnly);
        }

        // The pre-v3 backup is a plain file copy of the on-disk database. On
        // Windows the live redb handle holds an OS file lock, so `fs::copy` fails
        // with a lock violation (error 33) while `db` is open. Drop the handle to
        // release the lock, copy, then re-open. Only runs on the one-time v3
        // migration path; `db` has had read-only access (metadata read) up to here,
        // so dropping and reopening loses no state.
        let db = if needs_v3_migration {
            drop(db);
            // Don't clobber an existing pre-v3 backup. The copy happens in the
            // lock-release window before the re-read below can confirm migration is
            // still needed; if a concurrent upgrader already migrated the file and
            // wrote the backup, copying now would overwrite the genuine pre-v3
            // sidecar with already-migrated v3 data. Atomically claim the backup
            // path (create_new / O_EXCL) so only the first writer creates it — a
            // concurrent upgrader that lost the race sees AlreadyExists and leaves
            // the genuine sidecar untouched (no TOCTOU between exists() and copy()).
            let backup_path = pre_v3_backup_path(&path);
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&backup_path)
            {
                Ok(mut backup_file) => {
                    // If the copy fails after we claimed the path (disk full, short
                    // write, interruption), remove the partial/empty backup so a
                    // later open does not treat it as a valid pre-v3 sidecar via the
                    // AlreadyExists branch — the next open then re-creates a complete one.
                    let copy_result = std::fs::File::open(&path).and_then(|mut source| {
                        std::io::copy(&mut source, &mut backup_file).map(|_| ())
                    });
                    if let Err(error) = copy_result {
                        drop(backup_file);
                        let _ = std::fs::remove_file(&backup_path);
                        // On Windows a concurrent opener holding the redb file lock
                        // surfaces here as a raw lock/sharing violation; promote it to
                        // the typed, retryable DatabaseLocked (audit C2) rather than a
                        // generic Io error the contention-retry loop cannot recognize.
                        return Err(migration_io_error(error));
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    debug!("pre-v3 backup already exists; preserving it");
                }
                Err(error) => return Err(migration_io_error(error)),
            }
            let reopened = Self::create_database(&path, config)?;
            let reopened_metadata = Self::read_metadata(&reopened)?;
            Self::validate_existing_metadata(&reopened_metadata, config)?;
            metadata = reopened_metadata;
            needs_v2_migration = metadata.schema_version == 1;
            needs_v3_migration = metadata.schema_version <= 2;
            reopened
        } else {
            db
        };

        // Substrate-format marker gate (codec/redb-format axis, distinct from the
        // logical schema_version handled above). At this point the file is
        // redb-v3 (the redb-format upgrade ran in `create_or_migrate` before we got
        // here, if it was needed). Read the raw marker BEFORE deciding on any write.
        //
        //  - `Newer(found)` ⇒ a file written by a future PulseDB (e.g. a VS-4.0.3
        //    postcard store). Refuse with the typed `SubstrateFormatTooNew`; do not
        //    touch it (forward-incompatibility).
        //  - `Absent`/`Older` ⇒ this store is at `{redb-v3, bincode}` but carries no
        //    (or an older) marker. We are about to bring it to CURRENT (=1) by
        //    writing the marker. A read-only open cannot do that → ReadOnly
        //    (FR-035); it has already been gated for the redb-v2 case in
        //    `create_or_migrate`, this is the bincode-era-marker leg.
        //  - `Current` ⇒ nothing to write for the marker axis.
        let substrate_marker = Self::read_substrate_marker(&db)?;
        let needs_marker_write = match substrate_marker {
            SubstrateFormat::Newer(found) => {
                return Err(PulseDBError::Storage(
                    StorageError::substrate_format_too_new(found, CURRENT_SUBSTRATE_FORMAT),
                ));
            }
            SubstrateFormat::Absent | SubstrateFormat::Older(_) => {
                // #53b — marker-1 `{redb-v3, bincode}` (and Absent bincode-era) codec
                // leg: NO `.pre-substrate.bak` is taken here, and that is CORRECT by
                // design, not an omission. A `SubstrateFormat::Older(1)` store is
                // already redb-v3, so it SKIPPED the redb-v2 `SubstrateUpgradeRequired`
                // arm of `create_or_migrate` entirely and took no substrate backup.
                // The bincode→postcard re-encode below is a SINGLE redb write txn
                // whose marker bump to CURRENT (= 2) is the atomic commit point (see
                // "MARKER = COMMIT POINT" further down): redb's MVCC ABORTS the whole
                // txn — leaving NO partial visible state — if the process dies
                // mid-re-encode, so the pristine pre-codec bytes remain the on-disk
                // state and are fully recoverable on the next open. A sidecar backup
                // here would add NO crash-recovery value redb's own single-atomic-txn
                // MVCC does not already provide — only a dead-weight fsync/cleanup
                // burden. Hence: DOCUMENT the no-backup posture, do NOT add a backup.
                if config.read_only {
                    return Err(PulseDBError::ReadOnly);
                }
                true
            }
            SubstrateFormat::Current => false,
        };

        // Codec migration (bincode→postcard) runs when the marker is Absent|Older
        // (`needs_marker_write`). Decide the transaction shape CONFIG-FIRST and
        // BEFORE opening any write txn, so a too-large store fails closed with the
        // typed `SubstrateMigrationTooLarge` and ZERO destructive writes (design
        // §6.3 / §6.4(3)). The common path (store below the conservative floor, or
        // a declared memory budget) is the realized single-txn primary path.
        //
        // #53a/#54: this preflight is idempotent + cheap and is NOT a double-fault
        // surface — a redb-v2 store already ran the conservative (copy_through = 0)
        // preflight in `create_or_migrate`'s redb-v2 arm before any mutation; by the
        // time it reaches here it is redb-v3 and re-evaluates with the MEASURED
        // copy-through set. A `{redb-v3, bincode}` marker-1/Absent store (which never
        // took the redb-v2 arm) is preflighted for the first time HERE, with the
        // copy_through_excluded footprint — so an embedding-heavy store whose
        // store_size is dominated by copy-through bytes now OPENS instead of wrongly
        // failing closed.
        if needs_marker_write && !config.read_only {
            let store_size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            // #54a: measure the FULL §2a copy-through set (embeddings + 5 multimaps +
            // raw metadata keys) of the now-redb-v3 store so the memory axis keys on
            // the copy_through_excluded re-encodable footprint, not raw store_size.
            let copy_through = Self::copy_through_bytes(&db).unwrap_or(0);
            // Unified headroom preflight (work-1.05 / audit C3): disk THEN memory,
            // BEFORE any destructive write. If free space can't be determined, don't
            // block on the disk axis (u64::MAX) — the memory axis + backup_once still
            // guard; the disk check is a best-effort early failure for the common case.
            let available = fs2::available_space(&path).unwrap_or(u64::MAX);
            // Codec-only leg (already redb-v3; Absent/marker-1): no .pre-substrate.bak
            // is written here, so don't charge the backup store-size (#57 review).
            Self::resolve_migration_headroom(store_size, copy_through, available, config, false)?;
            // Progress signal (work-1.05 / C3): the first-open migration is exempt
            // from NFR-001 (<100ms); make the multi-minute pass non-silent. Per-phase
            // `info!` lines (reshape / re-encode, below) report progress thereafter.
            info!(
                store_size,
                "migrating store substrate format (codec re-encode); this may take a while for large stores"
            );
        }

        // Audit C6 / FR-035: a read-only open performs ZERO writes — it must not
        // write `last_opened_at` (`touch()`) nor open a write txn, so it can run
        // against a locked/old store without faulting. (An un-migrated store has
        // already been refused above: redb-v2 in `create_or_migrate`, bincode-era
        // marker via the `needs_marker_write` read-only gate.) A writable open
        // always touches + writes, preserving prior behavior + the migration writes.
        if !config.read_only {
            // Update last_opened_at timestamp and bump schema version if migrating.
            metadata.touch();
            if needs_v2_migration || needs_v3_migration {
                metadata.schema_version = SCHEMA_VERSION;
            }

            let write_txn = db.begin_write().map_err(StorageError::from)?;
            {
                // Ensure watch_events table exists (migration for pre-E4-S02 databases)
                let _ = write_txn.open_table(WATCH_EVENTS_TABLE)?;
                let _ = write_txn.open_table(DECAY_CONFIGS_TABLE)?;

                // Migrate WAL records from v1 → v2 (add entity_type field). The
                // reshape READ side decodes legacy bincode; the rewrite is current
                // schema (the codec loop below re-encodes it idempotently).
                if needs_v2_migration {
                    Self::migrate_wal_v1_to_v2(&write_txn)?;
                    info!("Migrated WAL records from schema v1 to v2");
                }

                let mut meta_table = write_txn.open_table(METADATA_TABLE)?;

                if meta_table.get(INSTANCE_ID_KEY)?.is_none() {
                    let instance_id = InstanceId::new();
                    meta_table.insert(INSTANCE_ID_KEY, instance_id.as_bytes().as_slice())?;
                    debug!("Generated new instance_id for existing database");
                }

                #[cfg(feature = "sync")]
                {
                    let _ = write_txn.open_table(SYNC_CURSORS_TABLE)?;
                }

                drop(meta_table);

                // Schema reshape v2→v3 (READ side decodes legacy bincode; rewrite is
                // current schema). Orthogonal to the codec axis below.
                if needs_v3_migration {
                    Self::migrate_experiences_v2_to_v3(&write_txn)?;
                    info!("Migrated experiences from schema v2 to v3");
                }

                // DECOUPLED CODEC RE-ENCODE (design §2.3 / grill Q1): driven SOLELY
                // by the substrate marker (`needs_marker_write` ⇒ Absent|Older),
                // re-encode EVERY serde-blob table bincode→postcard, unconditionally
                // — no table is covered only by a reshape migrator. A `{redb-v3,
                // bincode}` store hits NO-OP reshape above yet has every row
                // re-encoded here. Copy-through tables (EMBEDDINGS, the 5 multimaps,
                // raw metadata keys) are NEVER touched → byte-identical.
                if needs_marker_write {
                    Self::reencode_serde_blobs_to_postcard(&write_txn)?;
                    info!("Re-encoded storage values from bincode to postcard");
                }

                // Final metadata (touched + schema-bumped) in postcard.
                let mut meta_table = write_txn.open_table(METADATA_TABLE)?;
                let metadata_bytes = postcard::to_stdvec(&metadata)
                    .map_err(|e| StorageError::serialization(e.to_string()))?;
                meta_table.insert(METADATA_KEY, metadata_bytes.as_slice())?;

                // MARKER = COMMIT POINT (design §2.3): write the substrate-format
                // marker = CURRENT (= 2 = {redb-v3, postcard}) as the **LAST write**
                // in this txn, AFTER the re-encode loop succeeded. A crash mid-pass
                // leaves the old marker (1/Absent) + old-codec-decodable data →
                // clean rollback. Raw, hand-encoded bytes (NEVER serde).
                if needs_marker_write {
                    let marker = encode_substrate_marker(CURRENT_SUBSTRATE_FORMAT);
                    // VS-4.0.4 (#46) crash boundary: write-txn, after the whole
                    // re-encode succeeded but BEFORE the commit-point marker is
                    // written. A crash here rolls the txn back → old marker + old-
                    // codec data (clean rollback). `maybe_inject` takes no args, so
                    // it never borrows `write_txn` (which `meta_table` holds mutably
                    // here). Compiled out unless the `fault-injection` feature is on.
                    #[cfg(feature = "fault-injection")]
                    crate::fault_injection::maybe_inject(
                        crate::fault_injection::Boundary::PreMarker,
                    );
                    meta_table.insert(SUBSTRATE_FORMAT_KEY, marker.as_slice())?;
                    debug!(
                        marker = CURRENT_SUBSTRATE_FORMAT,
                        "wrote substrate-format marker LAST (now {{redb-v3, postcard}})"
                    );
                }
            }
            write_txn.commit().map_err(StorageError::from)?;
        }

        let instance_id = {
            let read_txn = db.begin_read().map_err(StorageError::from)?;
            let meta_table = read_txn.open_table(METADATA_TABLE)?;
            let entry = meta_table
                .get(INSTANCE_ID_KEY)?
                .ok_or_else(|| StorageError::corrupted("Missing instance_id"))?;
            let bytes: [u8; 16] = entry
                .value()
                .try_into()
                .map_err(|_| StorageError::corrupted("invalid instance_id bytes"))?;
            InstanceId::from_bytes(bytes)
        };

        info!(
            schema_version = metadata.schema_version,
            dimension = metadata.embedding_dimension.size(),
            "Database opened successfully"
        );

        Ok(Self {
            db,
            metadata,
            path,
            instance_id,
        })
    }

    /// Returns a reference to the underlying redb database.
    ///
    /// This is for internal use by other PulseDB modules.
    #[inline]
    #[allow(dead_code)] // Used by Collective CRUD (E1-S02) and Experience CRUD (E1-S03)
    pub(crate) fn database(&self) -> &Database {
        &self.db
    }

    /// Increments the WAL sequence and records a watch event within an existing write transaction.
    ///
    /// This is the core of cross-process change detection. By executing within the caller's
    /// transaction, the sequence increment and event record are atomic with the data mutation:
    /// if the transaction commits, both are durable; if it rolls back, neither is visible.
    ///
    /// # Arguments
    ///
    /// * `write_txn` - The caller's open write transaction
    /// * `entity_id` - The entity that changed (16-byte UUID)
    /// * `collective_id` - The collective it belongs to
    /// * `entity_type` - What kind of entity changed (Experience, Relation, etc.)
    /// * `event_type` - What kind of change occurred (Created, Updated, etc.)
    /// * `timestamp` - When the change occurred
    fn increment_wal_and_record(
        &self,
        write_txn: &::redb::WriteTransaction,
        entity_id: &[u8; 16],
        collective_id: CollectiveId,
        entity_type: EntityTypeTag,
        event_type: WatchEventTypeTag,
        timestamp: Timestamp,
    ) -> Result<u64> {
        // Echo prevention: skip WAL recording when applying sync changes
        #[cfg(feature = "sync")]
        if crate::sync::guard::is_sync_applying() {
            return Ok(0);
        }

        // Read current sequence (0 if key doesn't exist yet)
        let mut meta_table = write_txn.open_table(METADATA_TABLE)?;
        let current_seq = match meta_table.get(WAL_SEQUENCE_KEY)? {
            Some(entry) => {
                let bytes: [u8; 8] = entry
                    .value()
                    .try_into()
                    .map_err(|_| StorageError::corrupted("invalid wal_sequence bytes"))?;
                u64::from_be_bytes(bytes)
            }
            None => 0,
        };
        let new_seq = current_seq + 1;

        // Write new sequence number
        let seq_bytes = new_seq.to_be_bytes();
        meta_table.insert(WAL_SEQUENCE_KEY, seq_bytes.as_slice())?;

        // Record the event (schema v2 with entity_type)
        let record = WatchEventRecord {
            entity_id: *entity_id,
            collective_id: *collective_id.as_bytes(),
            event_type,
            timestamp_ms: timestamp.as_millis(),
            entity_type,
        };
        let record_bytes =
            postcard::to_stdvec(&record).map_err(|e| StorageError::serialization(e.to_string()))?;

        let mut events_table = write_txn.open_table(WATCH_EVENTS_TABLE)?;
        events_table.insert(&seq_bytes, record_bytes.as_slice())?;

        Ok(new_seq)
    }

    /// Migrates WAL records from schema v1 to v2.
    ///
    /// V1 records have 4 fields: experience_id, collective_id, event_type, timestamp_ms.
    /// V2 adds entity_type (defaults to Experience for existing records).
    fn migrate_wal_v1_to_v2(write_txn: &::redb::WriteTransaction) -> Result<()> {
        use super::schema::WatchEventRecordV1;

        let events_table = write_txn.open_table(WATCH_EVENTS_TABLE)?;

        // Collect all (key, v1_record) pairs. The reshape READ side decodes the
        // legacy-bincode v1 bytes via the 1.01 vendored reader (design §2.3 — the
        // reshape migrators read legacy bincode; the codec loop owns the postcard
        // re-encode). The rewrite below is at current schema in postcard, which the
        // codec loop then treats idempotently.
        let mut entries: Vec<([u8; 8], WatchEventRecordV1)> = Vec::new();
        for entry in events_table.iter()? {
            let (key, value) = entry.map_err(StorageError::from)?;
            let seq_bytes: [u8; 8] = *key.value();
            let v1_record: WatchEventRecordV1 = legacy_bincode::decode(value.value())
                .map_err(|e| StorageError::serialization(format!("v1 WAL record: {}", e)))?;
            entries.push((seq_bytes, v1_record));
        }
        drop(events_table);

        // Rewrite as v2 records
        let mut events_table = write_txn.open_table(WATCH_EVENTS_TABLE)?;
        for (seq_bytes, v1) in &entries {
            let v2 = WatchEventRecord {
                entity_id: v1.experience_id,
                collective_id: v1.collective_id,
                event_type: v1.event_type,
                timestamp_ms: v1.timestamp_ms,
                entity_type: EntityTypeTag::Experience,
            };
            let v2_bytes =
                postcard::to_stdvec(&v2).map_err(|e| StorageError::serialization(e.to_string()))?;
            events_table.insert(seq_bytes, v2_bytes.as_slice())?;
        }

        debug!(count = entries.len(), "Migrated WAL records to v2");
        Ok(())
    }

    /// Migrates experience records from schema v2 to v3.
    ///
    /// V2 stores scalar application counts. V3 stores a per-instance G-counter
    /// and maps every legacy scalar into the reserved LEGACY bucket to avoid
    /// double-counting already-synced replicas in later merge logic.
    fn migrate_experiences_v2_to_v3(write_txn: &::redb::WriteTransaction) -> Result<()> {
        let experiences_table = write_txn.open_table(EXPERIENCES_TABLE)?;

        let mut experience_ids: Vec<[u8; 16]> = Vec::new();
        for entry in experiences_table.iter()? {
            let (key, _) = entry.map_err(StorageError::from)?;
            experience_ids.push(*key.value());
        }
        drop(experiences_table);

        let mut experiences_table = write_txn.open_table(EXPERIENCES_TABLE)?;
        for experience_id in experience_ids {
            let entry = experiences_table.get(&experience_id)?.ok_or_else(|| {
                StorageError::corrupted("experience disappeared during migration")
            })?;
            // Reshape READ side: decode the legacy-bincode v2 bytes via the 1.01
            // vendored reader (design §2.3). The rewrite below is current-schema
            // postcard, which the codec loop then treats idempotently.
            let v2: ExperienceV2 = legacy_bincode::decode(entry.value())
                .map_err(|e| StorageError::serialization(format!("v2 experience record: {}", e)))?;
            drop(entry);

            let mut applications = BTreeMap::new();
            applications.insert(legacy_applications_instance_id(), v2.applications);

            let v3 = Experience {
                id: v2.id,
                collective_id: v2.collective_id,
                content: v2.content,
                embedding: v2.embedding,
                experience_type: v2.experience_type,
                importance: v2.importance,
                confidence: v2.confidence,
                applications,
                domain: v2.domain,
                related_files: v2.related_files,
                source_agent: v2.source_agent,
                source_task: v2.source_task,
                timestamp: v2.timestamp,
                last_reinforced: v2.timestamp,
                archived: v2.archived,
            };
            let bytes =
                postcard::to_stdvec(&v3).map_err(|e| StorageError::serialization(e.to_string()))?;
            experiences_table.insert(&experience_id, bytes.as_slice())?;
        }

        debug!("Migrated experience records to v3");
        Ok(())
    }

    // ========================================================================
    // Codec re-encode pass (bincode → postcard) — VS-4.0.3 / work-1.04
    // ========================================================================
    //
    // Driven SOLELY by the substrate marker (Absent | Older), this pass is
    // ORTHOGONAL to the schema-reshape migrators (which key off `schema_version`).
    // It re-encodes EVERY serde-blob table unconditionally — no table is covered
    // only by a reshape migrator (design §2.3 / grill Q1). A `{redb-v3, bincode}`
    // store hits NO-OP reshape yet still gets every row re-encoded here.
    //
    // §2a copy-through matrix: this pass touches ONLY the 9 serde-blob tables. It
    // NEVER reads or rewrites `EMBEDDINGS_TABLE` (raw LE f32), the 5 multimap index
    // tables, or the raw `METADATA_TABLE` keys `instance_id` / `wal_sequence` /
    // `substrate_format` — those are left untouched in the same redb file, which IS
    // byte-identical copy-through.

    /// Single-txn re-encode budget (the conservative small-host store-size floor).
    ///
    /// 1 GiB: at the 1.02 peak-RSS coefficient (`0.10 × store_size`) a 1 GiB store
    /// projects to ~100 MB peak, which fits a ~512 MB–1 GB box. Below this floor we
    /// always take the single-txn path; above it we require a *declared* memory
    /// budget (config-first) or fail closed (design §6.3 / audit C1).
    const SINGLE_TXN_STORE_FLOOR_BYTES: u64 = 1024 * 1024 * 1024;

    /// The effective single-txn store-size floor. In production this is exactly
    /// [`Self::SINGLE_TXN_STORE_FLOOR_BYTES`] (1 GiB). Under `cfg(test)` ONLY, an
    /// optional thread-local override lets the integration tests drive the
    /// fail-closed-before-mutation boundary with a small physical fixture instead of
    /// a real >1 GiB file (there is no other injection seam at the open path). The
    /// override changes NOTHING in a non-test build — the function inlines to the
    /// const — so the hardened boundary's production behavior is unaffected.
    #[inline]
    fn single_txn_floor_bytes() -> u64 {
        #[cfg(test)]
        {
            if let Some(v) = tests::TEST_FLOOR_OVERRIDE.with(|c| c.get()) {
                return v;
            }
        }
        Self::SINGLE_TXN_STORE_FLOOR_BYTES
    }

    /// 1.02 conservative peak-RSS coefficient: `peak ≈ re_encodable_footprint × 0.10`.
    /// (Measured 0.06–0.10 flat across 10k→500k; 0.10 is the conservative envelope.)
    ///
    /// #54: the coefficient is now applied to the **re-encodable footprint**
    /// ([`Self::re_encodable_footprint`]), NOT the raw `store_size` — the §2a
    /// copy-through set (embeddings + the 5 multimap indexes + the raw metadata
    /// keys) is byte-identical passthrough and never buffered, so it does not
    /// contribute to the re-encode peak.
    const PEAK_RSS_NUMERATOR: u64 = 10; // peak ≈ footprint * 10 / 100
    const PEAK_RSS_DENOMINATOR: u64 = 100;

    /// #54b — DISTINCTLY-NAMED peak safety margin (NOT the bare `SAFETY_MARGIN`
    /// string, which already denotes the declared-budget margin at
    /// `DECLARED_MEM_SAFETY_*`). Applied to the coefficient result to INFLATE the
    /// projected peak, biasing it toward OVER-estimating (fail-closed = safe) and
    /// never under-estimating (OOM = unsafe). `3 / 2` = 1.5× accounts for:
    ///   - redb page/leaf overhead + table fragmentation,
    ///   - old + new value coexistence within the single write txn (both the
    ///     legacy-decoded and postcard-re-encoded copies of a blob are live
    ///     simultaneously before the old row is overwritten),
    ///   - allocation amplification (Vec growth / serializer scratch).
    ///
    /// The inflation is saturating and rounds UP: when the footprint or overhead
    /// is uncertain, the projected peak is biased high so a genuinely too-large
    /// re-encode still fails closed rather than being allowed to OOM.
    const PEAK_SAFETY_MARGIN_NUMERATOR: u64 = 3;
    const PEAK_SAFETY_MARGIN_DENOMINATOR: u64 = 2;

    /// Safety margin applied to a *declared* available-memory budget before it is
    /// allowed to authorize a single-txn migration of an above-floor store.
    const DECLARED_MEM_SAFETY_NUMERATOR: u64 = 1; // projected_peak < declared * (1/2)
    const DECLARED_MEM_SAFETY_DENOMINATOR: u64 = 2;

    /// #54a — the RE-ENCODABLE footprint: the bytes the codec pass actually
    /// decodes + re-encodes, which is `store_size` minus the FULL §2a copy-through
    /// set (`copy_through_bytes` = EMBEDDINGS raw LE f32 + the 5 multimap index
    /// tables + the raw `METADATA_TABLE` keys). The copy-through set is
    /// byte-identical passthrough (never decoded/re-encoded), so it is excluded
    /// from the re-encode peak. Excluding embeddings ALONE would still over-project
    /// a heavily-indexed store and could wrongly fail it closed — the exclusion
    /// set is the whole copy-through matrix, not embeddings alone. Saturating:
    /// a `copy_through_bytes` over-estimate (should never exceed `store_size`)
    /// floors the footprint at 0 rather than underflowing.
    fn re_encodable_footprint(store_size: u64, copy_through_bytes: u64) -> u64 {
        // copy_through_excluded: subtract the full copy-through matrix, saturating.
        store_size.saturating_sub(copy_through_bytes)
    }

    /// #54a — measures the on-disk byte size of the FULL §2a copy-through set of a
    /// live (redb-v3) store: the EMBEDDINGS table (raw LE f32) + the 5 multimap
    /// index tables + the raw `METADATA_TABLE` keys (`instance_id` / `wal_sequence`
    /// / `substrate_format`). These bytes are byte-identical passthrough — never
    /// decoded or re-encoded by the codec pass — so they are excluded from the
    /// re-encode peak (`copy_through_excluded`). The measure sums key+value lengths
    /// via a read txn; it is a logical-byte lower bound on the physical copy-through
    /// footprint, which biases the resulting `re_encodable_footprint` slightly HIGH
    /// (over-estimate → fail-closed = safe). Only callable once the file is redb-v3
    /// (post redb-format upgrade); the redb-v2 arm of `create_or_migrate` cannot
    /// open tables under redb 4.1 and conservatively passes `copy_through_bytes = 0`.
    fn copy_through_bytes(db: &Database) -> Result<u64> {
        // `iter()` on a multimap lives on ReadableMultimapTable (not the plain
        // ReadableTable imported at module scope) — bring it in locally.
        use ::redb::ReadableMultimapTable;
        let read_txn = db.begin_read().map_err(StorageError::from)?;
        let mut total: u64 = 0;

        // EMBEDDINGS: raw LE f32, the size-dominant copy-through table.
        if let Ok(table) = read_txn.open_table(EMBEDDINGS_TABLE) {
            for entry in table.iter().map_err(StorageError::from)? {
                let (k, v) = entry.map_err(StorageError::from)?;
                total = total
                    .saturating_add(k.value().len() as u64)
                    .saturating_add(v.value().len() as u64);
            }
        }

        // The 5 multimap index tables — raw id references, copy-through.
        macro_rules! sum_multimap {
            ($tbl:expr) => {
                if let Ok(mm) = read_txn.open_multimap_table($tbl) {
                    for group in mm.iter().map_err(StorageError::from)? {
                        let (k, values) = group.map_err(StorageError::from)?;
                        total = total.saturating_add(k.value().len() as u64);
                        for v in values {
                            let v = v.map_err(StorageError::from)?;
                            total = total.saturating_add(v.value().len() as u64);
                        }
                    }
                }
            };
        }
        sum_multimap!(EXPERIENCES_BY_COLLECTIVE_TABLE);
        sum_multimap!(EXPERIENCES_BY_TYPE_TABLE);
        sum_multimap!(RELATIONS_BY_SOURCE_TABLE);
        sum_multimap!(RELATIONS_BY_TARGET_TABLE);
        sum_multimap!(INSIGHTS_BY_COLLECTIVE_TABLE);

        // Raw METADATA_TABLE keys (instance_id / wal_sequence / substrate_format) —
        // NEVER decoded (the "db_metadata" serde blob is re-encodable and excluded).
        if let Ok(meta) = read_txn.open_table(METADATA_TABLE) {
            for raw_key in [INSTANCE_ID_KEY, WAL_SEQUENCE_KEY, SUBSTRATE_FORMAT_KEY] {
                if let Some(v) = meta.get(raw_key).map_err(StorageError::from)? {
                    total = total
                        .saturating_add(raw_key.len() as u64)
                        .saturating_add(v.value().len() as u64);
                }
            }
        }

        Ok(total)
    }

    /// The realized shape of the codec re-encode pass.
    ///
    /// #54: the projected peak is derived from the copy_through_excluded
    /// re-encodable footprint (not raw `store_size`), then INFLATED by the
    /// `PEAK_SAFETY_MARGIN_*` constant so it is biased to over-estimate. 1.05 (the
    /// headroom preflight) can call [`Self::resolve_codec_txn_shape`] to learn this
    /// + the `projected_peak` without re-deriving the rule.
    fn projected_peak_rss(re_encodable_footprint: u64) -> u64 {
        // copy_through_excluded footprint × coefficient, then × safety margin —
        // saturating and rounding UP (bias to fail-closed, never OOM).
        re_encodable_footprint.saturating_mul(Self::PEAK_RSS_NUMERATOR) / Self::PEAK_RSS_DENOMINATOR
    }

    /// #54b — the peak with the safety margin applied: `projected_peak_rss × 1.5`,
    /// saturating. This is the value the floor decision compares against a declared
    /// budget. Rounds UP (over-estimate → fail-closed is the safe error).
    fn projected_peak_with_margin(re_encodable_footprint: u64) -> u64 {
        Self::projected_peak_rss(re_encodable_footprint)
            .saturating_mul(Self::PEAK_SAFETY_MARGIN_NUMERATOR)
            .saturating_add(Self::PEAK_SAFETY_MARGIN_DENOMINATOR - 1) // round up
            / Self::PEAK_SAFETY_MARGIN_DENOMINATOR
    }

    /// Config-first transaction-shape decision for the codec re-encode pass
    /// (design §6.3 — the baked-in 1.02 deliverable).
    ///
    /// #54: the memory decision is keyed on the copy_through_excluded, safety-
    /// margined projected peak (derived from [`Self::re_encodable_footprint`]),
    /// NOT raw `store_size`. `copy_through_bytes` is the byte size of the FULL §2a
    /// copy-through set (EMBEDDINGS + the 5 multimap indexes + the raw metadata
    /// keys). An embedding-heavy store whose `store_size` is dominated by that
    /// copy-through set has a small re-encodable footprint → small projected peak
    /// → now OPENS instead of wrongly failing closed. Conversely a re-encodable-
    /// dominated store (near-zero copy-through) keeps a footprint ≈ `store_size`,
    /// so the margined peak stays conservative and a genuinely too-large re-encode
    /// still fails closed rather than being allowed to OOM.
    ///
    /// Returns `Ok(())` if a **single transaction** is safe (the realized primary
    /// path — `store_size <= FLOOR`, or the margined projected peak fits the
    /// floor-implied peak budget, or a declared memory budget covers it). Returns
    /// `Err(SubstrateMigrationTooLarge)` — the **fail-closed valve** (work-1.04
    /// §6.4(3)) — otherwise: a typed, actionable error with **zero destructive
    /// writes** (1.05's preflight surfaces the same). An above-floor store must
    /// be opened with a larger declared `migration_available_memory_bytes`; a
    /// dedicated offline large-store migration tool is possible future work, not
    /// yet built (no `pulsedb migrate` binary ships today).
    ///
    /// **Config-first, NOT host-memory auto-detect** (a cgroup-limited container
    /// over-reports host RAM → would wrongly pick single-txn → OOM). The embedder
    /// *declares* `migration_available_memory_bytes` to opt into a higher ceiling.
    fn resolve_codec_txn_shape(
        store_size: u64,
        copy_through_bytes: u64,
        config: &Config,
    ) -> Result<()> {
        // copy_through_excluded footprint → margined projected peak (bias to
        // over-estimate; fail-closed is the safe error, OOM is not).
        let footprint = Self::re_encodable_footprint(store_size, copy_through_bytes);
        let projected_peak = Self::projected_peak_with_margin(footprint);

        // The effective floor (const in production; a small test-only override lets
        // the integration tests drive the boundary without a real >1 GiB file).
        let floor = Self::single_txn_floor_bytes();

        // The floor-implied peak budget: the margined peak a floor-sized re-encode
        // would project. A store whose margined peak fits under this budget is
        // single-txn-safe regardless of raw store_size — this is what lets an
        // embedding-heavy (copy-through-dominated) store OPEN.
        let floor_peak_budget = Self::projected_peak_with_margin(floor);

        // Common path: below the conservative absolute store-size floor → single-txn.
        if store_size <= floor {
            debug!(
                store_size,
                footprint,
                projected_peak,
                floor,
                "codec migration: single-txn (store below the conservative floor)"
            );
            return Ok(());
        }

        // #54a: above the raw floor, but the copy_through_excluded margined peak
        // still fits the floor-implied peak budget → single-txn-safe. This is the
        // embedding-heavy carve-in: the re-encode buffers only the footprint, not
        // the (dominant) copy-through bytes.
        if projected_peak <= floor_peak_budget {
            debug!(
                store_size,
                footprint,
                projected_peak,
                floor_peak_budget,
                "codec migration: single-txn (copy-through-excluded peak fits floor budget)"
            );
            return Ok(());
        }

        // Above the floor with a re-encodable footprint too large for the floor
        // budget: only a *declared* budget (config-first) can authorize single-txn.
        // projected_peak < declared * SAFETY_MARGIN.
        if let Some(declared) = config.migration_available_memory_bytes {
            let allowed = declared.saturating_mul(Self::DECLARED_MEM_SAFETY_NUMERATOR)
                / Self::DECLARED_MEM_SAFETY_DENOMINATOR;
            if projected_peak < allowed {
                debug!(
                    store_size,
                    footprint,
                    projected_peak,
                    declared,
                    "codec migration: single-txn (declared memory budget covers projected peak)"
                );
                return Ok(());
            }
        }

        // Fail closed with a typed, actionable error and ZERO destructive writes
        // (work-1.04 §6.4(3) scope-relief valve). A phased durable-resume path for
        // above-floor stores was NOT built in Sprint 4.0 and remains a documented
        // residual; the safe behavior today is fail-closed (declare more memory).
        warn!(
            store_size,
            footprint,
            projected_peak,
            floor,
            declared = ?config.migration_available_memory_bytes,
            "codec migration: store above single-txn floor with no covering memory budget; \
             failing closed (no destructive write)"
        );
        Err(PulseDBError::Storage(
            StorageError::substrate_migration_too_large(store_size, projected_peak, floor),
        ))
    }

    /// Conservative free-disk estimate the codec migration needs (audit C3 / work-1.05).
    ///
    /// The pristine `.pre-substrate.bak` backup taken before any destructive write
    /// roughly **doubles** the on-disk footprint (~1× store_size), and the postcard
    /// re-encode does **not** shrink the size-dominant raw-`f32` embedding table
    /// (1.02 size-delta), so the migrated file is conservatively assumed to be
    /// ~1× store_size. Add a ~10% redb transaction-growth margin. Saturating
    /// throughout (never overflows).
    fn required_migration_disk_bytes(store_size: u64) -> u64 {
        // backup (~1×) + migrated (~1×) + ~10% txn-growth margin
        store_size.saturating_mul(2).saturating_add(store_size / 10)
    }

    /// Unified migration headroom preflight (audit C3 / work-1.05) — checks the
    /// **disk** axis THEN the **memory** axis, returning a typed error with ZERO
    /// destructive writes when either is short, BEFORE the `.pre-substrate.bak`
    /// backup + codec re-encode pass run.
    ///
    /// The disk axis is evaluated first: it is the cheaper, earlier guard against a
    /// half-migration that exhausts disk mid-pass. `available` is the observed free
    /// space on the store's filesystem, injected for testability; the open path
    /// passes `fs2::available_space(path)`. The memory axis reuses
    /// [`Self::resolve_codec_txn_shape`] (1.02 coefficient + config-first floor /
    /// declared-memory opt-in — never raw host auto-detect).
    ///
    /// #54c: the DISK axis stays keyed on full `store_size` — the backup + migrated
    /// file both hold the full store (embeddings don't shrink), so disk footprint is
    /// ~2× store_size regardless of what is re-encoded. Only the MEMORY axis uses the
    /// copy_through_excluded footprint (via `copy_through_bytes`).
    fn resolve_migration_headroom(
        store_size: u64,
        copy_through_bytes: u64,
        available: u64,
        config: &Config,
        makes_backup: bool,
    ) -> Result<()> {
        // Disk axis first (cheaper guard against a half-migration that exhausts disk).
        // Keyed on FULL store_size — the copy-through set stays on disk in both the
        // backup and the migrated file (embeddings don't shrink). The pristine
        // `.pre-substrate.bak` (~1×store) is only claimed on the destructive
        // redb-v2→v3 path; the codec-only leg (already redb-v3, no backup) must NOT be
        // charged for a backup it never writes (#57 review — double-counted backup).
        let required = if makes_backup {
            Self::required_migration_disk_bytes(store_size)
        } else {
            // migrated file (~1×) + ~10% txn-growth margin — no backup term.
            store_size.saturating_add(store_size / 10)
        };
        if available < required {
            warn!(
                store_size,
                required,
                available,
                "codec migration: insufficient free disk; failing closed (no destructive write)"
            );
            return Err(PulseDBError::Storage(
                StorageError::substrate_migration_insufficient_disk(
                    store_size, required, available,
                ),
            ));
        }
        // Memory axis: reuse 1.04's config-first single-txn-vs-fail-closed decision,
        // keyed on the copy_through_excluded re-encodable footprint.
        Self::resolve_codec_txn_shape(store_size, copy_through_bytes, config)
    }

    /// Re-encodes every serde-blob table from legacy bincode to postcard, in the
    /// caller's single write transaction (design §2.3 + §2a matrix).
    ///
    /// Per-row decode is **try-legacy-bincode-then-postcard**: a row still in
    /// bincode decodes via the 1.01 vendored reader and is rewritten as postcard;
    /// a row already in postcard (e.g. one a reshape migrator just rewrote at
    /// current schema) is read back via postcard and rewritten idempotently. This
    /// keeps the pass unconditional and safe regardless of whether a reshape ran,
    /// and never feeds a postcard row to `legacy_bincode::decode` blindly.
    ///
    /// EXPERIENCES and WATCH_EVENTS additionally fall back to their legacy *schema*
    /// shape (ExperienceV2 / WatchEventRecordV1) so a store whose reshape did not
    /// run (pure codec migration of an already-current-schema store) is still
    /// reshaped+re-encoded by this single owner.
    fn reencode_serde_blobs_to_postcard(write_txn: &::redb::WriteTransaction) -> Result<()> {
        // #55: the SERDE_BLOB_TABLES registry is the single source of truth for
        // which serde-blob tables this loop re-encodes. We record each "what" label
        // the loop actually processes and assert (in debug builds) that the set of
        // processed labels is EXACTLY the registry's — so adding a serde-blob table
        // to `schema.rs` + this loop without registering it (or vice versa) trips
        // here. This is refactor-neutral: it observes, it does not alter, the pass.
        let mut visited_labels: Vec<&'static str> = Vec::with_capacity(SERDE_BLOB_TABLES.len());

        // VS-4.0.4 (#46) crash boundary: write-txn, BEFORE any table is re-encoded.
        // A crash here rolls the whole txn back (nothing re-encoded, marker
        // unchanged). Compiled out unless the `fault-injection` feature is on.
        #[cfg(feature = "fault-injection")]
        crate::fault_injection::maybe_inject(crate::fault_injection::Boundary::PreReencode);

        // --- METADATA_TABLE["db_metadata"] only (per-key; mixed table) ---
        // instance_id / wal_sequence / substrate_format are RAW bytes — NEVER decode.
        visited_labels.push("db_metadata");
        {
            let meta_table = write_txn.open_table(METADATA_TABLE)?;
            let existing = meta_table.get(METADATA_KEY)?.map(|v| v.value().to_vec());
            drop(meta_table);
            if let Some(bytes) = existing {
                let meta: DatabaseMetadata =
                    Self::decode_blob_legacy_or_postcard(&bytes, "db_metadata")?;
                let re = postcard::to_stdvec(&meta)
                    .map_err(|e| StorageError::serialization(e.to_string()))?;
                let mut meta_table = write_txn.open_table(METADATA_TABLE)?;
                meta_table.insert(METADATA_KEY, re.as_slice())?;
            }
        }

        // --- COLLECTIVES_TABLE: Collective ---
        visited_labels.push("collective");
        Self::reencode_keyed_table::<Collective>(write_txn, COLLECTIVES_TABLE, "collective")?;

        // --- DECAY_CONFIGS_TABLE: StoredDecayConfig (try) then StoredDecayConfigV1 ---
        visited_labels.push("decay_config");
        {
            let table = write_txn.open_table(DECAY_CONFIGS_TABLE)?;
            let mut rows: Vec<([u8; 16], Vec<u8>)> = Vec::new();
            for entry in table.iter()? {
                let (k, v) = entry.map_err(StorageError::from)?;
                rows.push((*k.value(), v.value().to_vec()));
            }
            drop(table);
            let mut table = write_txn.open_table(DECAY_CONFIGS_TABLE)?;
            for (key, bytes) in rows {
                let stored: StoredDecayConfig = match Self::decode_blob_legacy_or_postcard::<
                    StoredDecayConfig,
                >(&bytes, "decay_config")
                {
                    Ok(v) => v,
                    Err(_) => {
                        let v1: StoredDecayConfigV1 =
                            Self::decode_blob_legacy_or_postcard(&bytes, "decay_config_v1")?;
                        // Re-shape V1 → current via DecayConfig, then back to stored.
                        let cfg: DecayConfig = v1.into();
                        StoredDecayConfig::from(&cfg)
                    }
                };
                let re = postcard::to_stdvec(&stored)
                    .map_err(|e| StorageError::serialization(e.to_string()))?;
                table.insert(&key, re.as_slice())?;
            }
        }

        // --- EXPERIENCES_TABLE: Experience (try) then ExperienceV2 + reshape ---
        visited_labels.push("experience");
        {
            let table = write_txn.open_table(EXPERIENCES_TABLE)?;
            let mut rows: Vec<([u8; 16], Vec<u8>)> = Vec::new();
            for entry in table.iter()? {
                let (k, v) = entry.map_err(StorageError::from)?;
                rows.push((*k.value(), v.value().to_vec()));
            }
            drop(table);
            let mut table = write_txn.open_table(EXPERIENCES_TABLE)?;
            for (key, bytes) in rows {
                let exp: Experience = match Self::decode_blob_legacy_or_postcard::<Experience>(
                    &bytes,
                    "experience",
                ) {
                    Ok(v) => v,
                    Err(_) => {
                        let v2: ExperienceV2 =
                            Self::decode_blob_legacy_or_postcard(&bytes, "experience_v2")?;
                        let mut applications = BTreeMap::new();
                        applications.insert(legacy_applications_instance_id(), v2.applications);
                        Experience {
                            id: v2.id,
                            collective_id: v2.collective_id,
                            content: v2.content,
                            embedding: v2.embedding,
                            experience_type: v2.experience_type,
                            importance: v2.importance,
                            confidence: v2.confidence,
                            applications,
                            domain: v2.domain,
                            related_files: v2.related_files,
                            source_agent: v2.source_agent,
                            source_task: v2.source_task,
                            timestamp: v2.timestamp,
                            last_reinforced: v2.timestamp,
                            archived: v2.archived,
                        }
                    }
                };
                let re = postcard::to_stdvec(&exp)
                    .map_err(|e| StorageError::serialization(e.to_string()))?;
                table.insert(&key, re.as_slice())?;
            }
        }

        // VS-4.0.4 (#46) crash boundary: write-txn, PARTWAY through the registry
        // re-encode loop — the EXPERIENCES pass just committed ≥1 row to the txn
        // (uncommitted), the RELATIONS pass has not started. A crash here rolls the
        // WHOLE txn back (no partial re-encode survives — atomicity, not resume).
        // At this top-level point no table handle borrows `write_txn`. Compiled out
        // unless the `fault-injection` feature is on.
        #[cfg(feature = "fault-injection")]
        crate::fault_injection::maybe_inject(crate::fault_injection::Boundary::MidReencode);

        // --- RELATIONS_TABLE: ExperienceRelation ---
        visited_labels.push("relation");
        Self::reencode_keyed_table::<ExperienceRelation>(write_txn, RELATIONS_TABLE, "relation")?;

        // --- INSIGHTS_TABLE: DerivedInsight ---
        visited_labels.push("insight");
        Self::reencode_keyed_table::<DerivedInsight>(write_txn, INSIGHTS_TABLE, "insight")?;

        // --- ACTIVITIES_TABLE: Activity (variable-length key) ---
        visited_labels.push("activity");
        {
            let table = write_txn.open_table(ACTIVITIES_TABLE)?;
            let mut rows: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
            for entry in table.iter()? {
                let (k, v) = entry.map_err(StorageError::from)?;
                rows.push((k.value().to_vec(), v.value().to_vec()));
            }
            drop(table);
            let mut table = write_txn.open_table(ACTIVITIES_TABLE)?;
            for (key, bytes) in rows {
                let activity: Activity = Self::decode_blob_legacy_or_postcard(&bytes, "activity")?;
                let re = postcard::to_stdvec(&activity)
                    .map_err(|e| StorageError::serialization(e.to_string()))?;
                table.insert(key.as_slice(), re.as_slice())?;
            }
        }

        // --- WATCH_EVENTS_TABLE: WatchEventRecord (try) then WatchEventRecordV1 ---
        visited_labels.push("watch_event");
        {
            use super::schema::WatchEventRecordV1;
            let table = write_txn.open_table(WATCH_EVENTS_TABLE)?;
            let mut rows: Vec<([u8; 8], Vec<u8>)> = Vec::new();
            for entry in table.iter()? {
                let (k, v) = entry.map_err(StorageError::from)?;
                rows.push((*k.value(), v.value().to_vec()));
            }
            drop(table);
            let mut table = write_txn.open_table(WATCH_EVENTS_TABLE)?;
            for (key, bytes) in rows {
                let record: WatchEventRecord = match Self::decode_blob_legacy_or_postcard::<
                    WatchEventRecord,
                >(&bytes, "watch_event")
                {
                    Ok(v) => v,
                    Err(_) => {
                        let v1: WatchEventRecordV1 =
                            Self::decode_blob_legacy_or_postcard(&bytes, "watch_event_v1")?;
                        WatchEventRecord {
                            entity_id: v1.experience_id,
                            collective_id: v1.collective_id,
                            event_type: v1.event_type,
                            timestamp_ms: v1.timestamp_ms,
                            entity_type: EntityTypeTag::Experience,
                        }
                    }
                };
                let re = postcard::to_stdvec(&record)
                    .map_err(|e| StorageError::serialization(e.to_string()))?;
                table.insert(&key, re.as_slice())?;
            }
        }

        // --- SYNC_CURSORS_TABLE: SyncCursor (feature: sync) ---
        #[cfg(feature = "sync")]
        {
            visited_labels.push("sync_cursor");
            Self::reencode_keyed_table::<crate::sync::SyncCursor>(
                write_txn,
                SYNC_CURSORS_TABLE,
                "sync_cursor",
            )?;
        }

        // #57 review: a build WITHOUT the `sync` feature cannot decode/re-encode the
        // SyncCursor rows, yet the migration is about to bump the substrate marker to
        // CURRENT (postcard). If this store carries sync_cursors written by a prior
        // sync-enabled build, completing the migration would strand those rows in
        // bincode under a postcard marker — silent corruption a later sync build hits
        // reading them as postcard. Fail closed BEFORE the marker write (this write
        // txn rolls back, nothing committed, store stays re-migratable) so a
        // sync-enabled build can finish the codec migration.
        #[cfg(not(feature = "sync"))]
        {
            // `SYNC_CURSORS_TABLE` (schema.rs) is itself `#[cfg(feature = "sync")]`, so
            // declare a local probe with the SAME name + types to detect rows a prior
            // sync-enabled build persisted. Only probe if the table already exists, so
            // a store that never used sync is not mutated by an open-creates-it call.
            const SYNC_CURSORS_PROBE: ::redb::TableDefinition<&[u8; 16], &[u8]> =
                ::redb::TableDefinition::new("sync_cursors");
            let has_sync_table = {
                use ::redb::TableHandle;
                write_txn
                    .list_tables()
                    .map_err(StorageError::from)?
                    .any(|h| h.name() == "sync_cursors")
            };
            if has_sync_table {
                let table = write_txn.open_table(SYNC_CURSORS_PROBE)?;
                let has_sync_rows = table.iter()?.next().is_some();
                drop(table);
                if has_sync_rows {
                    return Err(PulseDBError::Storage(
                        StorageError::SubstrateMigrationRequiresSync,
                    ));
                }
            }
        }

        // #55 refactor-neutral coverage assertion: the loop must have visited
        // EXACTLY the registry's serde-blob labels. `sync_cursor` is only expected
        // when the `sync` feature is on, so filter the registry the same way the
        // loop is gated. Any drift (a registered table not visited, or a visited
        // table not registered) is a debug-build panic — the single-source invariant.
        debug_assert_eq!(
            {
                let mut v = visited_labels.clone();
                v.sort_unstable();
                v
            },
            {
                let mut expected: Vec<&'static str> = SERDE_BLOB_TABLES
                    .iter()
                    .map(|(_, what)| *what)
                    .filter(|what| cfg!(feature = "sync") || *what != "sync_cursor")
                    .collect();
                expected.sort_unstable();
                expected
            },
            "reencode loop and SERDE_BLOB_TABLES registry disagree on the serde-blob set",
        );

        debug!("re-encoded all serde-blob tables from bincode to postcard");
        Ok(())
    }

    /// Re-encodes a 16-byte-keyed serde-blob table in place (legacy bincode →
    /// postcard, idempotent on already-postcard rows).
    fn reencode_keyed_table<V>(
        write_txn: &::redb::WriteTransaction,
        table_def: ::redb::TableDefinition<&[u8; 16], &[u8]>,
        what: &str,
    ) -> Result<()>
    where
        V: serde::Serialize + serde::de::DeserializeOwned,
    {
        let table = write_txn.open_table(table_def)?;
        let mut rows: Vec<([u8; 16], Vec<u8>)> = Vec::new();
        for entry in table.iter()? {
            let (k, v) = entry.map_err(StorageError::from)?;
            rows.push((*k.value(), v.value().to_vec()));
        }
        drop(table);
        let mut table = write_txn.open_table(table_def)?;
        for (key, bytes) in rows {
            let value: V = Self::decode_blob_legacy_or_postcard(&bytes, what)?;
            let re = postcard::to_stdvec(&value)
                .map_err(|e| StorageError::serialization(e.to_string()))?;
            table.insert(&key, re.as_slice())?;
        }
        Ok(())
    }

    /// Decodes a serde-blob value trying legacy bincode first, then postcard.
    ///
    /// The codec pass runs only when the marker is `Absent | Older`, so the rows
    /// are *expected* to be bincode; the postcard fallback covers a row a reshape
    /// migrator just rewrote at current schema in the same txn (idempotent).
    fn decode_blob_legacy_or_postcard<V>(bytes: &[u8], what: &str) -> Result<V>
    where
        V: serde::de::DeserializeOwned,
    {
        match legacy_bincode::decode::<V>(bytes) {
            Ok(v) => Ok(v),
            Err(bincode_err) => postcard::from_bytes::<V>(bytes).map_err(|postcard_err| {
                StorageError::serialization(format!(
                    "{what}: legacy bincode decode failed ({bincode_err}); \
                     postcard decode also failed ({postcard_err})"
                ))
                .into()
            }),
        }
    }

    /// Returns the embedding dimension configured for this database.
    #[inline]
    pub fn embedding_dimension(&self) -> EmbeddingDimension {
        self.metadata.embedding_dimension
    }
}

impl StorageEngine for RedbStorage {
    // =========================================================================
    // Lifecycle
    // =========================================================================

    fn metadata(&self) -> &DatabaseMetadata {
        &self.metadata
    }

    #[instrument(skip(self))]
    fn close(self: Box<Self>) -> Result<()> {
        info!("Closing storage engine");

        // redb flushes all data durably on drop. Since `Database::drop` is
        // infallible, this method currently always returns Ok(()). The Result
        // return type is retained for API forward-compatibility if a future
        // storage backend can report flush errors.
        drop(self.db);

        info!("Storage engine closed");
        Ok(())
    }

    fn path(&self) -> Option<&Path> {
        Some(&self.path)
    }

    // =========================================================================
    // Provider Identity Stamp (VS-4.3.1 work 1.03 — embedding injection seam)
    // =========================================================================
    //
    // Mirrors the `INSTANCE_ID_KEY` raw-bytes-in-METADATA_TABLE pattern: write
    // under a dedicated `PROVIDER_IDENTITY_KEY` key, postcard-encoded (NOT raw
    // bytes — the value is serde-shaped, and unlike `instance_id` / the
    // substrate marker it is only read after the serializer identity is
    // established, so postcard is safe here). The value shape 1.01's
    // `test_provider_identity_postcard_roundtrip` proved round-trippable.

    fn stamp_provider_identity(&self, identity: &ProviderIdentity) -> Result<()> {
        let bytes = postcard::to_stdvec(identity)
            .map_err(|e| StorageError::serialization(e.to_string()))?;

        // Era marker (VS-4.3.3/1.01): co-stamped atomically in the SAME write
        // transaction as the identity, so a torn commit cannot leave one key
        // without the other. The value is the stamp time as postcard-encoded
        // `u64` epoch-millis (matches the schema doc).
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let era_bytes =
            postcard::to_stdvec(&now_ms).map_err(|e| StorageError::serialization(e.to_string()))?;

        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        {
            let mut meta_table = write_txn.open_table(METADATA_TABLE)?;
            meta_table.insert(PROVIDER_IDENTITY_KEY, bytes.as_slice())?;
            meta_table.insert(PROVIDER_IDENTITY_STAMPED_AT_KEY, era_bytes.as_slice())?;
        }
        write_txn.commit().map_err(StorageError::from)?;

        debug!(
            provider = %identity.provider,
            model_id = %identity.model_id,
            "Stamped provider identity + era marker into redb metadata"
        );
        Ok(())
    }

    fn provider_identity(&self) -> Result<Option<ProviderIdentity>> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let meta_table = read_txn.open_table(METADATA_TABLE)?;
        match meta_table.get(PROVIDER_IDENTITY_KEY)? {
            None => Ok(None),
            Some(entry) => {
                let identity: ProviderIdentity = postcard::from_bytes(entry.value())
                    .map_err(|e| StorageError::serialization(e.to_string()))?;
                Ok(Some(identity))
            }
        }
    }

    fn provider_identity_era_marker(&self) -> Result<bool> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let meta_table = read_txn.open_table(METADATA_TABLE)?;
        Ok(meta_table.get(PROVIDER_IDENTITY_STAMPED_AT_KEY)?.is_some())
    }

    // =========================================================================
    // Collective Storage Operations
    // =========================================================================

    fn save_collective(&self, collective: &Collective) -> Result<()> {
        let bytes = postcard::to_stdvec(collective)
            .map_err(|e| StorageError::serialization(e.to_string()))?;

        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        {
            let mut table = write_txn.open_table(COLLECTIVES_TABLE)?;
            table.insert(collective.id.as_bytes(), bytes.as_slice())?;
        }
        self.increment_wal_and_record(
            &write_txn,
            collective.id.as_bytes(),
            collective.id,
            EntityTypeTag::Collective,
            WatchEventTypeTag::Created,
            collective.created_at,
        )?;
        write_txn.commit().map_err(StorageError::from)?;

        debug!(id = %collective.id, name = %collective.name, "Collective saved");
        Ok(())
    }

    fn get_collective(&self, id: CollectiveId) -> Result<Option<Collective>> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_table(COLLECTIVES_TABLE)?;

        match table.get(id.as_bytes())? {
            Some(value) => {
                let collective: Collective = postcard::from_bytes(value.value())
                    .map_err(|e| StorageError::serialization(e.to_string()))?;
                Ok(Some(collective))
            }
            None => Ok(None),
        }
    }

    fn list_collectives(&self) -> Result<Vec<Collective>> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_table(COLLECTIVES_TABLE)?;

        let mut collectives = Vec::new();
        for result in table.iter()? {
            let (_, value) = result.map_err(StorageError::from)?;
            let collective: Collective = postcard::from_bytes(value.value())
                .map_err(|e| StorageError::serialization(e.to_string()))?;
            collectives.push(collective);
        }

        Ok(collectives)
    }

    fn delete_collective(&self, id: CollectiveId) -> Result<bool> {
        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        let existed;
        {
            let mut table = write_txn.open_table(COLLECTIVES_TABLE)?;
            existed = table.remove(id.as_bytes())?.is_some();
        }
        write_txn.commit().map_err(StorageError::from)?;

        if existed {
            debug!(id = %id, "Collective deleted");
        }
        Ok(existed)
    }

    fn get_decay_config(&self, collective_id: CollectiveId) -> Result<Option<DecayConfig>> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_table(DECAY_CONFIGS_TABLE)?;

        match table.get(collective_id.as_bytes())? {
            Some(value) => match postcard::from_bytes::<StoredDecayConfig>(value.value()) {
                Ok(stored) => Ok(Some(stored.into())),
                Err(current_error) => {
                    let legacy: StoredDecayConfigV1 =
                        postcard::from_bytes(value.value()).map_err(|legacy_error| {
                            StorageError::serialization(format!(
                                "decay config current format: {}; legacy format: {}",
                                current_error, legacy_error
                            ))
                        })?;
                    Ok(Some(legacy.into()))
                }
            },
            None => Ok(None),
        }
    }

    fn set_decay_config(&self, collective_id: CollectiveId, config: DecayConfig) -> Result<()> {
        let stored = StoredDecayConfig::from(&config);
        let bytes =
            postcard::to_stdvec(&stored).map_err(|e| StorageError::serialization(e.to_string()))?;

        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        {
            let mut table = write_txn.open_table(DECAY_CONFIGS_TABLE)?;
            table.insert(collective_id.as_bytes(), bytes.as_slice())?;
        }
        write_txn.commit().map_err(StorageError::from)?;

        debug!(collective_id = %collective_id, "Decay config saved");
        Ok(())
    }

    // =========================================================================
    // Experience Index Operations
    // =========================================================================

    fn count_experiences_in_collective(&self, id: CollectiveId) -> Result<u64> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_multimap_table(EXPERIENCES_BY_COLLECTIVE_TABLE)?;

        let count = table.get(id.as_bytes())?.count() as u64;

        Ok(count)
    }

    fn delete_experiences_by_collective(&self, id: CollectiveId) -> Result<u64> {
        // Phase 1: Read — collect experience IDs and relation IDs to delete
        let (exp_ids, relation_ids): (Vec<[u8; 16]>, Vec<[u8; 16]>) = {
            let read_txn = self.db.begin_read().map_err(StorageError::from)?;
            let table = read_txn.open_multimap_table(EXPERIENCES_BY_COLLECTIVE_TABLE)?;

            let mut ids = Vec::new();
            for result in table.get(id.as_bytes())? {
                let value = result.map_err(StorageError::from)?;
                let entry = value.value();
                // Entry is [timestamp: 8 bytes][experience_id: 16 bytes]
                let mut exp_id = [0u8; 16];
                exp_id.copy_from_slice(&entry[8..24]);
                ids.push(exp_id);
            }

            // Collect all relation IDs for these experiences (deduplicated)
            let mut rel_ids = std::collections::HashSet::new();
            let source_table = read_txn.open_multimap_table(RELATIONS_BY_SOURCE_TABLE)?;
            let target_table = read_txn.open_multimap_table(RELATIONS_BY_TARGET_TABLE)?;
            for exp_id in &ids {
                for result in source_table.get(exp_id)? {
                    let value = result.map_err(StorageError::from)?;
                    rel_ids.insert(*value.value());
                }
                for result in target_table.get(exp_id)? {
                    let value = result.map_err(StorageError::from)?;
                    rel_ids.insert(*value.value());
                }
            }

            (ids, rel_ids.into_iter().collect())
        };

        let count = exp_ids.len() as u64;
        if count == 0 {
            return Ok(0);
        }

        // Phase 2: Write — delete from all tables in a single transaction
        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        {
            // Delete experience records
            let mut exp_table = write_txn.open_table(EXPERIENCES_TABLE)?;
            for exp_id in &exp_ids {
                exp_table.remove(exp_id)?;
            }
        }
        {
            // Delete embedding vectors
            let mut emb_table = write_txn.open_table(EMBEDDINGS_TABLE)?;
            for exp_id in &exp_ids {
                emb_table.remove(exp_id)?;
            }
        }
        {
            // Clear the by-collective index for this collective
            let mut idx_table = write_txn.open_multimap_table(EXPERIENCES_BY_COLLECTIVE_TABLE)?;
            idx_table.remove_all(id.as_bytes())?;
        }
        {
            // Clear the by-type index for all type variants of this collective
            let mut type_table = write_txn.open_multimap_table(EXPERIENCES_BY_TYPE_TABLE)?;
            for tag in ExperienceTypeTag::all() {
                let key = encode_type_index_key(id.as_bytes(), *tag);
                type_table.remove_all(&key)?;
            }
        }
        {
            // Delete relations and their index entries
            if !relation_ids.is_empty() {
                let mut rel_table = write_txn.open_table(RELATIONS_TABLE)?;
                let mut source_idx = write_txn.open_multimap_table(RELATIONS_BY_SOURCE_TABLE)?;
                let mut target_idx = write_txn.open_multimap_table(RELATIONS_BY_TARGET_TABLE)?;

                // Clear relation indexes for all affected experiences
                for exp_id in &exp_ids {
                    source_idx.remove_all(exp_id)?;
                    target_idx.remove_all(exp_id)?;
                }
                // Delete the relation records themselves
                for rel_id in &relation_ids {
                    rel_table.remove(rel_id)?;
                }

                debug!(
                    count = relation_ids.len(),
                    "Cascade-deleted relations for collective"
                );
            }
        }
        write_txn.commit().map_err(StorageError::from)?;

        debug!(id = %id, count = count, "Cascade-deleted experiences for collective");
        Ok(count)
    }

    fn list_experience_ids_in_collective(&self, id: CollectiveId) -> Result<Vec<ExperienceId>> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_multimap_table(EXPERIENCES_BY_COLLECTIVE_TABLE)?;

        let mut ids = Vec::new();
        for result in table.get(id.as_bytes())? {
            let value = result.map_err(StorageError::from)?;
            let entry = value.value();
            // Entry is [timestamp: 8 bytes][experience_id: 16 bytes]
            let mut exp_bytes = [0u8; 16];
            exp_bytes.copy_from_slice(&entry[8..24]);
            ids.push(ExperienceId::from_bytes(exp_bytes));
        }

        Ok(ids)
    }

    fn get_recent_experience_ids(
        &self,
        collective_id: CollectiveId,
        limit: usize,
    ) -> Result<Vec<(ExperienceId, Timestamp)>> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_multimap_table(EXPERIENCES_BY_COLLECTIVE_TABLE)?;

        // Collect all (ExperienceId, Timestamp) pairs for this collective.
        // Multimap values are sorted ascending by [timestamp_be][exp_id],
        // so we collect all and then take from the end for newest-first.
        let mut entries = Vec::new();
        for result in table.get(collective_id.as_bytes())? {
            let value = result.map_err(StorageError::from)?;
            let entry = value.value();
            // Entry layout: [timestamp_be: 8 bytes][experience_id: 16 bytes]
            let mut ts_bytes = [0u8; 8];
            ts_bytes.copy_from_slice(&entry[..8]);
            let timestamp = Timestamp::from_millis(i64::from_be_bytes(ts_bytes));

            let mut exp_bytes = [0u8; 16];
            exp_bytes.copy_from_slice(&entry[8..24]);
            entries.push((ExperienceId::from_bytes(exp_bytes), timestamp));
        }

        // Take the last `limit` entries (newest) and reverse to get descending order
        let start = entries.len().saturating_sub(limit);
        let mut recent = entries.split_off(start);
        recent.reverse();

        Ok(recent)
    }

    // =========================================================================
    // Experience Storage Operations
    // =========================================================================

    fn save_experience(&self, experience: &Experience) -> Result<()> {
        // Serialize experience (embedding is #[serde(skip)], excluded automatically)
        let exp_bytes = postcard::to_stdvec(experience)
            .map_err(|e| StorageError::serialization(e.to_string()))?;

        // Convert embedding to raw little-endian bytes
        let emb_bytes = f32_slice_to_bytes(&experience.embedding);

        // Build index keys
        let type_key = encode_type_index_key(
            experience.collective_id.as_bytes(),
            experience.experience_type.type_tag(),
        );

        // Write to all 4 tables in a single atomic transaction
        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        {
            // Main experience record
            let mut exp_table = write_txn.open_table(EXPERIENCES_TABLE)?;
            exp_table.insert(experience.id.as_bytes(), exp_bytes.as_slice())?;
        }
        {
            // Embedding vector (stored separately for compactness)
            let mut emb_table = write_txn.open_table(EMBEDDINGS_TABLE)?;
            emb_table.insert(experience.id.as_bytes(), emb_bytes.as_slice())?;
        }
        {
            // By-collective index: key=collective_id, value=timestamp+experience_id
            let mut idx_table = write_txn.open_multimap_table(EXPERIENCES_BY_COLLECTIVE_TABLE)?;
            // Value is [timestamp_be: 8 bytes][experience_id: 16 bytes] = 24 bytes
            let mut value = [0u8; 24];
            value[..8].copy_from_slice(&experience.timestamp.to_be_bytes());
            value[8..24].copy_from_slice(experience.id.as_bytes());
            idx_table.insert(experience.collective_id.as_bytes(), &value)?;
        }
        {
            // By-type index: key=collective_id+type_tag, value=experience_id
            let mut type_table = write_txn.open_multimap_table(EXPERIENCES_BY_TYPE_TABLE)?;
            type_table.insert(&type_key, experience.id.as_bytes())?;
        }
        // Record WAL event for cross-process change detection
        self.increment_wal_and_record(
            &write_txn,
            experience.id.as_bytes(),
            experience.collective_id,
            EntityTypeTag::Experience,
            WatchEventTypeTag::Created,
            experience.timestamp,
        )?;
        write_txn.commit().map_err(StorageError::from)?;

        debug!(
            id = %experience.id,
            collective_id = %experience.collective_id,
            "Experience saved"
        );
        Ok(())
    }

    fn get_experience(&self, id: ExperienceId) -> Result<Option<Experience>> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;

        // Read main experience record
        let exp_table = read_txn.open_table(EXPERIENCES_TABLE)?;
        let exp_entry = match exp_table.get(id.as_bytes())? {
            Some(v) => v,
            None => return Ok(None),
        };

        let mut experience: Experience = postcard::from_bytes(exp_entry.value())
            .map_err(|e| StorageError::serialization(e.to_string()))?;

        // Read embedding from separate table and reconstitute
        let emb_table = read_txn.open_table(EMBEDDINGS_TABLE)?;
        if let Some(emb_entry) = emb_table.get(id.as_bytes())? {
            experience.embedding = bytes_to_f32_vec(emb_entry.value());
        }

        Ok(Some(experience))
    }

    fn update_experience(&self, id: ExperienceId, update: &ExperienceUpdate) -> Result<bool> {
        // Read-modify-write: read the current record, apply updates, write back
        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        let collective_id;
        let timestamp;
        let is_archive;
        {
            let mut exp_table = write_txn.open_table(EXPERIENCES_TABLE)?;

            let entry = match exp_table.get(id.as_bytes())? {
                Some(v) => v,
                None => return Ok(false),
            };

            let mut experience: Experience = postcard::from_bytes(entry.value())
                .map_err(|e| StorageError::serialization(e.to_string()))?;

            // Drop the borrow on entry before mutating the table
            drop(entry);

            // Capture metadata for WAL event before applying updates
            collective_id = experience.collective_id;
            timestamp = experience.timestamp;
            is_archive = update.archived == Some(true);

            // Apply updates (only Some fields)
            if let Some(importance) = update.importance {
                experience.importance = importance;
            }
            if let Some(confidence) = update.confidence {
                experience.confidence = confidence;
            }
            if let Some(ref domain) = update.domain {
                experience.domain = domain.clone();
            }
            if let Some(ref related_files) = update.related_files {
                experience.related_files = related_files.clone();
            }
            if let Some(archived) = update.archived {
                experience.archived = archived;
            }

            // Re-serialize and write back
            let bytes = postcard::to_stdvec(&experience)
                .map_err(|e| StorageError::serialization(e.to_string()))?;
            exp_table.insert(id.as_bytes(), bytes.as_slice())?;
        }
        // Record WAL event for cross-process change detection
        let event_type = if is_archive {
            WatchEventTypeTag::Archived
        } else {
            WatchEventTypeTag::Updated
        };
        self.increment_wal_and_record(
            &write_txn,
            id.as_bytes(),
            collective_id,
            EntityTypeTag::Experience,
            event_type,
            timestamp,
        )?;
        write_txn.commit().map_err(StorageError::from)?;

        debug!(id = %id, "Experience updated");
        Ok(true)
    }

    #[cfg(feature = "sync")]
    fn merge_experience_applications(
        &self,
        id: ExperienceId,
        applications: &BTreeMap<InstanceId, u32>,
        last_reinforced: Option<Timestamp>,
    ) -> Result<bool> {
        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        let found = {
            let mut exp_table = write_txn.open_table(EXPERIENCES_TABLE)?;
            let entry = match exp_table.get(id.as_bytes())? {
                Some(v) => v,
                None => return Ok(false),
            };

            let mut experience: Experience = postcard::from_bytes(entry.value())
                .map_err(|e| StorageError::serialization(e.to_string()))?;
            drop(entry);

            for (instance_id, count) in applications {
                let bucket = experience.applications.entry(*instance_id).or_insert(0);
                *bucket = (*bucket).max(*count);
            }

            if let Some(incoming) = last_reinforced {
                experience.last_reinforced = experience.last_reinforced.max(incoming);
            }

            let bytes = postcard::to_stdvec(&experience)
                .map_err(|e| StorageError::serialization(e.to_string()))?;
            exp_table.insert(id.as_bytes(), bytes.as_slice())?;
            true
        };

        write_txn.commit().map_err(StorageError::from)?;
        debug!(id = %id, "Experience applications merged");
        Ok(found)
    }

    fn delete_experience(&self, id: ExperienceId) -> Result<bool> {
        // First read the experience to get collective_id, timestamp, and type_tag
        // (needed for cleaning up secondary indices and WAL event)
        let (collective_id, timestamp, type_tag) = {
            let read_txn = self.db.begin_read().map_err(StorageError::from)?;
            let exp_table = read_txn.open_table(EXPERIENCES_TABLE)?;

            match exp_table.get(id.as_bytes())? {
                Some(entry) => {
                    let exp: Experience = postcard::from_bytes(entry.value())
                        .map_err(|e| StorageError::serialization(e.to_string()))?;
                    (
                        exp.collective_id,
                        exp.timestamp,
                        exp.experience_type.type_tag(),
                    )
                }
                None => return Ok(false),
            }
        };

        // Delete from all 4 tables in a single transaction
        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        {
            let mut exp_table = write_txn.open_table(EXPERIENCES_TABLE)?;
            exp_table.remove(id.as_bytes())?;
        }
        {
            let mut emb_table = write_txn.open_table(EMBEDDINGS_TABLE)?;
            emb_table.remove(id.as_bytes())?;
        }
        {
            // Remove specific entry from by-collective multimap
            let mut idx_table = write_txn.open_multimap_table(EXPERIENCES_BY_COLLECTIVE_TABLE)?;
            let mut value = [0u8; 24];
            value[..8].copy_from_slice(&timestamp.to_be_bytes());
            value[8..24].copy_from_slice(id.as_bytes());
            idx_table.remove(collective_id.as_bytes(), &value)?;
        }
        {
            // Remove specific entry from by-type multimap
            let mut type_table = write_txn.open_multimap_table(EXPERIENCES_BY_TYPE_TABLE)?;
            let type_key = encode_type_index_key(collective_id.as_bytes(), type_tag);
            type_table.remove(&type_key, id.as_bytes())?;
        }
        // Record WAL event for cross-process change detection
        self.increment_wal_and_record(
            &write_txn,
            id.as_bytes(),
            collective_id,
            EntityTypeTag::Experience,
            WatchEventTypeTag::Deleted,
            timestamp,
        )?;
        write_txn.commit().map_err(StorageError::from)?;

        debug!(id = %id, "Experience deleted");
        Ok(true)
    }

    fn reinforce_experience(&self, id: ExperienceId) -> Result<Option<u32>> {
        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        let (new_count, collective_id, timestamp) = {
            let mut exp_table = write_txn.open_table(EXPERIENCES_TABLE)?;

            let entry = match exp_table.get(id.as_bytes())? {
                Some(v) => v,
                None => return Ok(None),
            };

            let mut experience: Experience = postcard::from_bytes(entry.value())
                .map_err(|e| StorageError::serialization(e.to_string()))?;
            drop(entry);

            let bucket = experience.applications.entry(self.instance_id).or_insert(0);
            *bucket = bucket.saturating_add(1);
            experience.last_reinforced = Timestamp::now();
            let new_count = experience.applications();
            let collective_id = experience.collective_id;
            let timestamp = experience.timestamp;

            let bytes = postcard::to_stdvec(&experience)
                .map_err(|e| StorageError::serialization(e.to_string()))?;
            exp_table.insert(id.as_bytes(), bytes.as_slice())?;
            (new_count, collective_id, timestamp)
        };
        // Record WAL event for cross-process change detection
        self.increment_wal_and_record(
            &write_txn,
            id.as_bytes(),
            collective_id,
            EntityTypeTag::Experience,
            WatchEventTypeTag::Updated,
            timestamp,
        )?;
        write_txn.commit().map_err(StorageError::from)?;

        debug!(id = %id, applications = new_count, "Experience reinforced");
        Ok(Some(new_count))
    }

    fn save_embedding(&self, id: ExperienceId, embedding: &[f32]) -> Result<()> {
        let bytes = f32_slice_to_bytes(embedding);

        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        {
            let mut table = write_txn.open_table(EMBEDDINGS_TABLE)?;
            table.insert(id.as_bytes(), bytes.as_slice())?;
        }
        write_txn.commit().map_err(StorageError::from)?;

        debug!(id = %id, dim = embedding.len(), "Embedding saved");
        Ok(())
    }

    fn get_embedding(&self, id: ExperienceId) -> Result<Option<Vec<f32>>> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_table(EMBEDDINGS_TABLE)?;

        match table.get(id.as_bytes())? {
            Some(entry) => Ok(Some(bytes_to_f32_vec(entry.value()))),
            None => Ok(None),
        }
    }

    // =========================================================================
    // Relation Storage Operations (E3-S01)
    // =========================================================================

    fn save_relation(&self, relation: &ExperienceRelation) -> Result<()> {
        let bytes = postcard::to_stdvec(relation)
            .map_err(|e| StorageError::serialization(e.to_string()))?;

        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        {
            let mut table = write_txn.open_table(RELATIONS_TABLE)?;
            table.insert(relation.id.as_bytes(), bytes.as_slice())?;
        }
        {
            let mut table = write_txn.open_multimap_table(RELATIONS_BY_SOURCE_TABLE)?;
            table.insert(relation.source_id.as_bytes(), relation.id.as_bytes())?;
        }
        {
            let mut table = write_txn.open_multimap_table(RELATIONS_BY_TARGET_TABLE)?;
            table.insert(relation.target_id.as_bytes(), relation.id.as_bytes())?;
        }
        // Look up collective_id from source experience for WAL record
        let collective_id = {
            let exp_table = write_txn.open_table(EXPERIENCES_TABLE)?;
            let entry = exp_table
                .get(relation.source_id.as_bytes())?
                .ok_or_else(|| {
                    StorageError::corrupted("relation source experience not found for WAL record")
                })?;
            let exp: Experience = postcard::from_bytes(entry.value())
                .map_err(|e| StorageError::serialization(e.to_string()))?;
            exp.collective_id
        };
        self.increment_wal_and_record(
            &write_txn,
            relation.id.as_bytes(),
            collective_id,
            EntityTypeTag::Relation,
            WatchEventTypeTag::Created,
            relation.created_at,
        )?;
        write_txn.commit().map_err(StorageError::from)?;

        debug!(id = %relation.id, "Relation saved");
        Ok(())
    }

    fn get_relation(&self, id: RelationId) -> Result<Option<ExperienceRelation>> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_table(RELATIONS_TABLE)?;

        match table.get(id.as_bytes())? {
            Some(value) => {
                let relation: ExperienceRelation = postcard::from_bytes(value.value())
                    .map_err(|e| StorageError::serialization(e.to_string()))?;
                Ok(Some(relation))
            }
            None => Ok(None),
        }
    }

    fn delete_relation(&self, id: RelationId) -> Result<bool> {
        // Read the relation first to get source/target IDs for index cleanup
        // and source experience's collective_id for WAL record
        let (source_id, target_id, collective_id) = {
            let read_txn = self.db.begin_read().map_err(StorageError::from)?;
            let rel_table = read_txn.open_table(RELATIONS_TABLE)?;

            match rel_table.get(id.as_bytes())? {
                Some(entry) => {
                    let rel: ExperienceRelation = postcard::from_bytes(entry.value())
                        .map_err(|e| StorageError::serialization(e.to_string()))?;
                    // Look up collective_id from source experience
                    let exp_table = read_txn.open_table(EXPERIENCES_TABLE)?;
                    let cid = match exp_table.get(rel.source_id.as_bytes())? {
                        Some(exp_entry) => {
                            let exp: Experience = postcard::from_bytes(exp_entry.value())
                                .map_err(|e| StorageError::serialization(e.to_string()))?;
                            exp.collective_id
                        }
                        // Source experience may have been deleted; use nil collective
                        None => CollectiveId::nil(),
                    };
                    (rel.source_id, rel.target_id, cid)
                }
                None => return Ok(false),
            }
        };

        // Delete from all 3 tables atomically
        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        {
            let mut table = write_txn.open_table(RELATIONS_TABLE)?;
            table.remove(id.as_bytes())?;
        }
        {
            let mut table = write_txn.open_multimap_table(RELATIONS_BY_SOURCE_TABLE)?;
            table.remove(source_id.as_bytes(), id.as_bytes())?;
        }
        {
            let mut table = write_txn.open_multimap_table(RELATIONS_BY_TARGET_TABLE)?;
            table.remove(target_id.as_bytes(), id.as_bytes())?;
        }
        self.increment_wal_and_record(
            &write_txn,
            id.as_bytes(),
            collective_id,
            EntityTypeTag::Relation,
            WatchEventTypeTag::Deleted,
            Timestamp::now(),
        )?;
        write_txn.commit().map_err(StorageError::from)?;

        debug!(id = %id, "Relation deleted");
        Ok(true)
    }

    fn get_relation_ids_by_source(&self, experience_id: ExperienceId) -> Result<Vec<RelationId>> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_multimap_table(RELATIONS_BY_SOURCE_TABLE)?;

        let mut ids = Vec::new();
        for result in table.get(experience_id.as_bytes())? {
            let value = result.map_err(StorageError::from)?;
            let bytes = value.value();
            ids.push(RelationId::from_bytes(*bytes));
        }

        Ok(ids)
    }

    fn get_relation_ids_by_target(&self, experience_id: ExperienceId) -> Result<Vec<RelationId>> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_multimap_table(RELATIONS_BY_TARGET_TABLE)?;

        let mut ids = Vec::new();
        for result in table.get(experience_id.as_bytes())? {
            let value = result.map_err(StorageError::from)?;
            let bytes = value.value();
            ids.push(RelationId::from_bytes(*bytes));
        }

        Ok(ids)
    }

    fn delete_relations_for_experience(&self, experience_id: ExperienceId) -> Result<u64> {
        // Phase 1: Read — collect all relation IDs from both indexes
        let relation_ids: Vec<RelationId> = {
            let read_txn = self.db.begin_read().map_err(StorageError::from)?;
            let source_table = read_txn.open_multimap_table(RELATIONS_BY_SOURCE_TABLE)?;
            let target_table = read_txn.open_multimap_table(RELATIONS_BY_TARGET_TABLE)?;

            let mut ids = std::collections::HashSet::new();

            // Outgoing relations (this experience is source)
            for result in source_table.get(experience_id.as_bytes())? {
                let value = result.map_err(StorageError::from)?;
                ids.insert(RelationId::from_bytes(*value.value()));
            }

            // Incoming relations (this experience is target)
            for result in target_table.get(experience_id.as_bytes())? {
                let value = result.map_err(StorageError::from)?;
                ids.insert(RelationId::from_bytes(*value.value()));
            }

            ids.into_iter().collect()
        };

        let count = relation_ids.len() as u64;
        if count == 0 {
            return Ok(0);
        }

        // Phase 2: Read each relation to get source/target IDs for index cleanup
        let relations: Vec<ExperienceRelation> = {
            let read_txn = self.db.begin_read().map_err(StorageError::from)?;
            let table = read_txn.open_table(RELATIONS_TABLE)?;

            let mut rels = Vec::with_capacity(relation_ids.len());
            for rel_id in &relation_ids {
                if let Some(entry) = table.get(rel_id.as_bytes())? {
                    let rel: ExperienceRelation = postcard::from_bytes(entry.value())
                        .map_err(|e| StorageError::serialization(e.to_string()))?;
                    rels.push(rel);
                }
            }
            rels
        };

        // Phase 3: Write — delete from all 3 tables atomically
        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        {
            let mut rel_table = write_txn.open_table(RELATIONS_TABLE)?;
            for rel in &relations {
                rel_table.remove(rel.id.as_bytes())?;
            }
        }
        {
            let mut source_table = write_txn.open_multimap_table(RELATIONS_BY_SOURCE_TABLE)?;
            for rel in &relations {
                source_table.remove(rel.source_id.as_bytes(), rel.id.as_bytes())?;
            }
        }
        {
            let mut target_table = write_txn.open_multimap_table(RELATIONS_BY_TARGET_TABLE)?;
            for rel in &relations {
                target_table.remove(rel.target_id.as_bytes(), rel.id.as_bytes())?;
            }
        }
        write_txn.commit().map_err(StorageError::from)?;

        debug!(
            experience_id = %experience_id,
            count = count,
            "Cascade-deleted relations for experience"
        );
        Ok(count)
    }

    fn relation_exists(
        &self,
        source_id: ExperienceId,
        target_id: ExperienceId,
        relation_type: RelationType,
    ) -> Result<bool> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let index_table = read_txn.open_multimap_table(RELATIONS_BY_SOURCE_TABLE)?;
        let rel_table = read_txn.open_table(RELATIONS_TABLE)?;

        // Scan all relations for this source and check each
        for result in index_table.get(source_id.as_bytes())? {
            let value = result.map_err(StorageError::from)?;
            let rel_id = RelationId::from_bytes(*value.value());

            if let Some(entry) = rel_table.get(rel_id.as_bytes())? {
                let rel: ExperienceRelation = postcard::from_bytes(entry.value())
                    .map_err(|e| StorageError::serialization(e.to_string()))?;
                if rel.target_id == target_id && rel.relation_type == relation_type {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    // =========================================================================
    // Insight Storage Operations (E3-S02)
    // =========================================================================

    fn save_insight(&self, insight: &DerivedInsight) -> Result<()> {
        let bytes =
            postcard::to_stdvec(insight).map_err(|e| StorageError::serialization(e.to_string()))?;

        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        {
            let mut table = write_txn.open_table(INSIGHTS_TABLE)?;
            table.insert(insight.id.as_bytes(), bytes.as_slice())?;
        }
        {
            let mut table = write_txn.open_multimap_table(INSIGHTS_BY_COLLECTIVE_TABLE)?;
            table.insert(insight.collective_id.as_bytes(), insight.id.as_bytes())?;
        }
        self.increment_wal_and_record(
            &write_txn,
            insight.id.as_bytes(),
            insight.collective_id,
            EntityTypeTag::Insight,
            WatchEventTypeTag::Created,
            insight.created_at,
        )?;
        write_txn.commit().map_err(StorageError::from)?;

        debug!(id = %insight.id, collective_id = %insight.collective_id, "Insight saved");
        Ok(())
    }

    fn get_insight(&self, id: InsightId) -> Result<Option<DerivedInsight>> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_table(INSIGHTS_TABLE)?;

        match table.get(id.as_bytes())? {
            Some(value) => {
                let insight: DerivedInsight = postcard::from_bytes(value.value())
                    .map_err(|e| StorageError::serialization(e.to_string()))?;
                Ok(Some(insight))
            }
            None => Ok(None),
        }
    }

    fn delete_insight(&self, id: InsightId) -> Result<bool> {
        // Read the insight first to get collective_id for index cleanup
        let collective_id = {
            let read_txn = self.db.begin_read().map_err(StorageError::from)?;
            let table = read_txn.open_table(INSIGHTS_TABLE)?;

            match table.get(id.as_bytes())? {
                Some(entry) => {
                    let insight: DerivedInsight = postcard::from_bytes(entry.value())
                        .map_err(|e| StorageError::serialization(e.to_string()))?;
                    insight.collective_id
                }
                None => return Ok(false),
            }
        };

        // Delete from both tables atomically
        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        {
            let mut table = write_txn.open_table(INSIGHTS_TABLE)?;
            table.remove(id.as_bytes())?;
        }
        {
            let mut table = write_txn.open_multimap_table(INSIGHTS_BY_COLLECTIVE_TABLE)?;
            table.remove(collective_id.as_bytes(), id.as_bytes())?;
        }
        self.increment_wal_and_record(
            &write_txn,
            id.as_bytes(),
            collective_id,
            EntityTypeTag::Insight,
            WatchEventTypeTag::Deleted,
            Timestamp::now(),
        )?;
        write_txn.commit().map_err(StorageError::from)?;

        debug!(id = %id, "Insight deleted");
        Ok(true)
    }

    fn list_insight_ids_in_collective(&self, id: CollectiveId) -> Result<Vec<InsightId>> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_multimap_table(INSIGHTS_BY_COLLECTIVE_TABLE)?;

        let mut ids = Vec::new();
        for result in table.get(id.as_bytes())? {
            let value = result.map_err(StorageError::from)?;
            ids.push(InsightId::from_bytes(*value.value()));
        }

        Ok(ids)
    }

    fn delete_insights_by_collective(&self, id: CollectiveId) -> Result<u64> {
        // Phase 1: Read — collect insight IDs
        let insight_ids: Vec<[u8; 16]> = {
            let read_txn = self.db.begin_read().map_err(StorageError::from)?;
            let table = read_txn.open_multimap_table(INSIGHTS_BY_COLLECTIVE_TABLE)?;

            let mut ids = Vec::new();
            for result in table.get(id.as_bytes())? {
                let value = result.map_err(StorageError::from)?;
                ids.push(*value.value());
            }
            ids
        };

        let count = insight_ids.len() as u64;
        if count == 0 {
            return Ok(0);
        }

        // Phase 2: Write — delete from both tables atomically
        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        {
            let mut table = write_txn.open_table(INSIGHTS_TABLE)?;
            for insight_id in &insight_ids {
                table.remove(insight_id)?;
            }
        }
        {
            let mut table = write_txn.open_multimap_table(INSIGHTS_BY_COLLECTIVE_TABLE)?;
            table.remove_all(id.as_bytes())?;
        }
        write_txn.commit().map_err(StorageError::from)?;

        debug!(id = %id, count = count, "Cascade-deleted insights for collective");
        Ok(count)
    }

    // =========================================================================
    // Activity Storage Operations (E3-S03)
    // =========================================================================

    fn save_activity(&self, activity: &Activity) -> Result<()> {
        let key = encode_activity_key(activity.collective_id.as_bytes(), &activity.agent_id);
        let bytes = postcard::to_stdvec(activity)
            .map_err(|e| StorageError::serialization(e.to_string()))?;

        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        {
            let mut table = write_txn.open_table(ACTIVITIES_TABLE)?;
            table.insert(key.as_slice(), bytes.as_slice())?;
        }
        write_txn.commit().map_err(StorageError::from)?;

        debug!(
            agent_id = %activity.agent_id,
            collective_id = %activity.collective_id,
            "Activity saved"
        );
        Ok(())
    }

    fn get_activity(
        &self,
        agent_id: &str,
        collective_id: CollectiveId,
    ) -> Result<Option<Activity>> {
        let key = encode_activity_key(collective_id.as_bytes(), agent_id);

        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_table(ACTIVITIES_TABLE)?;

        match table.get(key.as_slice())? {
            Some(value) => {
                let activity: Activity = postcard::from_bytes(value.value())
                    .map_err(|e| StorageError::serialization(e.to_string()))?;
                Ok(Some(activity))
            }
            None => Ok(None),
        }
    }

    fn delete_activity(&self, agent_id: &str, collective_id: CollectiveId) -> Result<bool> {
        let key = encode_activity_key(collective_id.as_bytes(), agent_id);

        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        let existed = {
            let mut table = write_txn.open_table(ACTIVITIES_TABLE)?;
            let removed = table.remove(key.as_slice())?;
            removed.is_some()
        };
        write_txn.commit().map_err(StorageError::from)?;

        if existed {
            debug!(agent_id = %agent_id, collective_id = %collective_id, "Activity deleted");
        }
        Ok(existed)
    }

    fn list_activities_in_collective(&self, collective_id: CollectiveId) -> Result<Vec<Activity>> {
        let prefix = collective_id.as_bytes();

        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_table(ACTIVITIES_TABLE)?;

        let mut activities = Vec::new();
        for result in table.iter()? {
            let (key, value) = result.map_err(StorageError::from)?;
            let key_bytes = key.value();

            // Check if this key belongs to the requested collective (16-byte prefix)
            if key_bytes.len() >= 16 && decode_collective_from_activity_key(key_bytes) == *prefix {
                let activity: Activity = postcard::from_bytes(value.value())
                    .map_err(|e| StorageError::serialization(e.to_string()))?;
                activities.push(activity);
            }
        }

        Ok(activities)
    }

    fn delete_activities_by_collective(&self, collective_id: CollectiveId) -> Result<u64> {
        let prefix = collective_id.as_bytes();

        // Phase 1: Read — collect matching keys
        let keys_to_delete: Vec<Vec<u8>> = {
            let read_txn = self.db.begin_read().map_err(StorageError::from)?;
            let table = read_txn.open_table(ACTIVITIES_TABLE)?;

            let mut keys = Vec::new();
            for result in table.iter()? {
                let (key, _) = result.map_err(StorageError::from)?;
                let key_bytes = key.value();

                if key_bytes.len() >= 16
                    && decode_collective_from_activity_key(key_bytes) == *prefix
                {
                    keys.push(key_bytes.to_vec());
                }
            }
            keys
        };

        let count = keys_to_delete.len() as u64;
        if count == 0 {
            return Ok(0);
        }

        // Phase 2: Write — delete all collected keys
        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        {
            let mut table = write_txn.open_table(ACTIVITIES_TABLE)?;
            for key in &keys_to_delete {
                table.remove(key.as_slice())?;
            }
        }
        write_txn.commit().map_err(StorageError::from)?;

        debug!(
            collective_id = %collective_id,
            count = count,
            "Cascade-deleted activities for collective"
        );
        Ok(count)
    }

    // =========================================================================
    // Paginated List Operations (PulseVision)
    // =========================================================================

    fn list_experience_ids_paginated(
        &self,
        collective_id: CollectiveId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ExperienceId>> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_multimap_table(EXPERIENCES_BY_COLLECTIVE_TABLE)?;

        let mut ids = Vec::new();
        let mut skipped = 0usize;

        // Multimap: key=collective_id (16 bytes), values=[timestamp_be:8][exp_id:16] (24 bytes)
        for result in table.get(collective_id.as_bytes())? {
            let value = result.map_err(StorageError::from)?;
            if skipped < offset {
                skipped += 1;
                continue;
            }
            let entry = value.value();
            let mut id_bytes = [0u8; 16];
            id_bytes.copy_from_slice(&entry[8..24]);
            ids.push(ExperienceId::from_bytes(id_bytes));
            if ids.len() >= limit {
                return Ok(ids);
            }
        }

        Ok(ids)
    }

    fn list_relations_in_collective(
        &self,
        collective_id: CollectiveId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<crate::relation::ExperienceRelation>> {
        // Get all experience IDs in this collective first
        let exp_ids = self.list_experience_ids_in_collective(collective_id)?;

        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let source_table = read_txn.open_multimap_table(RELATIONS_BY_SOURCE_TABLE)?;
        let rel_table = read_txn.open_table(RELATIONS_TABLE)?;

        let mut relations = Vec::new();
        let mut skipped = 0usize;

        for exp_id in &exp_ids {
            for result in source_table.get(exp_id.as_bytes())? {
                let rel_id_value = result.map_err(StorageError::from)?;
                let rel_id = RelationId::from_bytes(*rel_id_value.value());

                if skipped < offset {
                    skipped += 1;
                    continue;
                }

                if let Some(entry) = rel_table.get(rel_id.as_bytes())? {
                    let relation: crate::relation::ExperienceRelation =
                        postcard::from_bytes(entry.value())
                            .map_err(|e| StorageError::serialization(e.to_string()))?;
                    relations.push(relation);
                    if relations.len() >= limit {
                        return Ok(relations);
                    }
                }
            }
        }

        Ok(relations)
    }

    fn list_insight_ids_paginated(
        &self,
        collective_id: CollectiveId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<InsightId>> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_multimap_table(INSIGHTS_BY_COLLECTIVE_TABLE)?;

        let mut ids = Vec::new();
        let mut skipped = 0usize;

        for result in table.get(collective_id.as_bytes())? {
            let value = result.map_err(StorageError::from)?;
            if skipped < offset {
                skipped += 1;
                continue;
            }
            ids.push(InsightId::from_bytes(*value.value()));
            if ids.len() >= limit {
                return Ok(ids);
            }
        }

        Ok(ids)
    }

    // =========================================================================
    // Watch Event Operations (E4-S02)
    // =========================================================================

    fn get_wal_sequence(&self) -> Result<u64> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let meta_table = read_txn.open_table(METADATA_TABLE)?;
        match meta_table.get(WAL_SEQUENCE_KEY)? {
            Some(entry) => {
                let bytes: [u8; 8] = entry
                    .value()
                    .try_into()
                    .map_err(|_| StorageError::corrupted("invalid wal_sequence bytes"))?;
                Ok(u64::from_be_bytes(bytes))
            }
            None => Ok(0),
        }
    }

    fn poll_watch_events(
        &self,
        since_seq: u64,
        limit: usize,
    ) -> Result<(Vec<WatchEventRecord>, u64)> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let events_table = read_txn.open_table(WATCH_EVENTS_TABLE)?;

        let start_key = (since_seq + 1).to_be_bytes();
        let end_key = u64::MAX.to_be_bytes();
        let mut events = Vec::new();
        let mut max_seq = since_seq;

        for entry in events_table.range::<&[u8; 8]>(&start_key..=&end_key)? {
            let (key, value) = entry.map_err(StorageError::from)?;
            let seq = u64::from_be_bytes(*key.value());
            let record: WatchEventRecord = postcard::from_bytes(value.value())
                .map_err(|e| StorageError::serialization(e.to_string()))?;
            events.push(record);
            max_seq = seq;
            if events.len() >= limit {
                break;
            }
        }

        Ok((events, max_seq))
    }

    // =========================================================================
    // Sync Operations (feature: sync)
    // =========================================================================

    #[cfg(feature = "sync")]
    fn poll_sync_events(
        &self,
        since_seq: u64,
        limit: usize,
    ) -> Result<Vec<(u64, WatchEventRecord)>> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let events_table = read_txn.open_table(WATCH_EVENTS_TABLE)?;

        let start_key = (since_seq + 1).to_be_bytes();
        let end_key = u64::MAX.to_be_bytes();
        let mut events = Vec::new();

        for entry in events_table.range::<&[u8; 8]>(&start_key..=&end_key)? {
            let (key, value) = entry.map_err(StorageError::from)?;
            let seq = u64::from_be_bytes(*key.value());
            let record: WatchEventRecord = postcard::from_bytes(value.value())
                .map_err(|e| StorageError::serialization(e.to_string()))?;
            events.push((seq, record));
            if events.len() >= limit {
                break;
            }
        }

        Ok(events)
    }

    #[cfg(feature = "sync")]
    fn instance_id(&self) -> crate::sync::InstanceId {
        self.instance_id
    }

    #[cfg(feature = "sync")]
    fn save_sync_cursor(&self, cursor: &crate::sync::SyncCursor) -> Result<()> {
        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        {
            let mut table = write_txn.open_table(SYNC_CURSORS_TABLE)?;
            let bytes = postcard::to_stdvec(cursor)
                .map_err(|e| StorageError::serialization(e.to_string()))?;
            table.insert(cursor.instance_id.as_bytes(), bytes.as_slice())?;
        }
        write_txn.commit().map_err(StorageError::from)?;
        debug!(
            peer = %cursor.instance_id,
            last_sequence = cursor.last_sequence,
            "Saved sync cursor"
        );
        Ok(())
    }

    #[cfg(feature = "sync")]
    fn load_sync_cursor(
        &self,
        instance_id: &crate::sync::InstanceId,
    ) -> Result<Option<crate::sync::SyncCursor>> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_table(SYNC_CURSORS_TABLE)?;
        match table.get(instance_id.as_bytes())? {
            Some(entry) => {
                let cursor: crate::sync::SyncCursor = postcard::from_bytes(entry.value())
                    .map_err(|e| StorageError::serialization(e.to_string()))?;
                Ok(Some(cursor))
            }
            None => Ok(None),
        }
    }

    #[cfg(feature = "sync")]
    fn list_sync_cursors(&self) -> Result<Vec<crate::sync::SyncCursor>> {
        let read_txn = self.db.begin_read().map_err(StorageError::from)?;
        let table = read_txn.open_table(SYNC_CURSORS_TABLE)?;
        let mut cursors = Vec::new();
        for entry in table.iter()? {
            let (_, value) = entry.map_err(StorageError::from)?;
            let cursor: crate::sync::SyncCursor = postcard::from_bytes(value.value())
                .map_err(|e| StorageError::serialization(e.to_string()))?;
            cursors.push(cursor);
        }
        Ok(cursors)
    }

    #[cfg(feature = "sync")]
    fn compact_wal_events(&self, up_to_seq: u64) -> Result<u64> {
        if up_to_seq == 0 {
            return Ok(0);
        }

        // Collect keys to delete in a read pass
        let keys_to_delete: Vec<[u8; 8]> = {
            let read_txn = self.db.begin_read().map_err(StorageError::from)?;
            let events_table = read_txn.open_table(WATCH_EVENTS_TABLE)?;

            let start_key = 1u64.to_be_bytes();
            let end_key = up_to_seq.to_be_bytes();
            let mut keys = Vec::new();

            for entry in events_table.range::<&[u8; 8]>(&start_key..=&end_key)? {
                let (key, _) = entry.map_err(StorageError::from)?;
                keys.push(*key.value());
            }
            keys
        };

        if keys_to_delete.is_empty() {
            return Ok(0);
        }

        let count = keys_to_delete.len() as u64;

        // Delete in a write transaction
        let write_txn = self.db.begin_write().map_err(StorageError::from)?;
        {
            let mut events_table = write_txn.open_table(WATCH_EVENTS_TABLE)?;
            for key in &keys_to_delete {
                events_table.remove(key)?;
            }
        }
        write_txn.commit().map_err(StorageError::from)?;

        debug!(count, up_to_seq, "Compacted WAL events");
        Ok(count)
    }
}

// ============================================================================
// Embedding byte conversion helpers
// ============================================================================

/// Converts a slice of f32 values to raw little-endian bytes.
#[inline]
fn f32_slice_to_bytes(data: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(data.len() * 4);
    for &val in data {
        bytes.extend_from_slice(&val.to_le_bytes());
    }
    bytes
}

/// Converts raw little-endian bytes back to a Vec<f32>.
#[inline]
fn bytes_to_f32_vec(data: &[u8]) -> Vec<f32> {
    data.chunks_exact(4)
        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect()
}

// RedbStorage is auto Send + Sync: Database, DatabaseMetadata, and PathBuf
// are all Send + Sync.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PulseDB;
    use tempfile::tempdir;

    // 1.01 frozen oracle byte constants (audit C2): the {v3, bincode} migration
    // fixtures + decay goldens reuse these FROZEN legacy bytes rather than calling
    // the live serializer, so `src/storage/redb.rs` is free of bare-crate serde
    // calls (AC-3) while still seeding genuine legacy on-disk values.
    use crate::storage::legacy_bincode::tests::{
        COLLECTIVE_GOLDEN, EXPERIENCE_GOLDEN, EXPERIENCE_V2_GOLDEN, INSIGHT_GOLDEN,
        RELATION_GOLDEN, WATCH_EVENT_GOLDEN,
    };
    use std::cell::Cell;

    thread_local! {
        /// #53a/#54 test seam: an optional override for the single-txn store-size
        /// floor, consulted ONLY by [`super::RedbStorage::single_txn_floor_bytes`]
        /// under `cfg(test)`. Lets an integration test drive the
        /// fail-closed-before-mutation boundary with a small physical fixture (no
        /// real >1 GiB file). `None` ⇒ the production 1 GiB const. A guard restores
        /// `None` on drop so tests don't leak the override across the shared runner.
        pub(super) static TEST_FLOOR_OVERRIDE: Cell<Option<u64>> = const { Cell::new(None) };
    }

    /// RAII guard: sets the test floor override for its lifetime, restoring the
    /// prior value on drop (so a panicking test can't poison sibling tests).
    struct FloorOverrideGuard(Option<u64>);
    impl FloorOverrideGuard {
        fn set(floor: u64) -> Self {
            let prev = TEST_FLOOR_OVERRIDE.with(|c| c.replace(Some(floor)));
            FloorOverrideGuard(prev)
        }
    }
    impl Drop for FloorOverrideGuard {
        fn drop(&mut self) {
            TEST_FLOOR_OVERRIDE.with(|c| c.set(self.0));
        }
    }

    // ====================================================================
    // Frozen oracle byte constants for 1.04 (audit C2)
    // ====================================================================
    //
    // Minted ONCE via a throwaway `examples/oracle_gen_104.rs` generator (deleted
    // before commit, per the legacy_bincode/tests.rs regen procedure) so no live
    // serializer call survives in `src/`. These are bincode-1.3 `DefaultOptions`
    // (fixint LE) bytes — the same wire format the vendored reader reproduces.

    // DatabaseMetadata: schema_version 3, D384, created/last_opened = 1_700_000_000_000ms.
    pub(crate) const META_V3_D384_GOLDEN: &[u8] = &[
        0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x68, 0xe5, 0xcf, 0x8b, 0x01, 0x00,
        0x00, 0x00, 0x68, 0xe5, 0xcf, 0x8b, 0x01, 0x00, 0x00,
    ];

    // DatabaseMetadata: schema_version 2 (triggers the v2→v3 reshape), D384.
    pub(crate) const META_V2_D384_GOLDEN: &[u8] = &[
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x68, 0xe5, 0xcf, 0x8b, 0x01, 0x00,
        0x00, 0x00, 0x68, 0xe5, 0xcf, 0x8b, 0x01, 0x00, 0x00,
    ];

    // Collective with id [0;16] (matches EXPERIENCE_V2_GOLDEN's collective_id so the
    // HNSW index, keyed by the decoded collective id, indexes the migrated row),
    // name "v2coll", no owner, dim 384.
    pub(crate) const COLLECTIVE_ZERO_ID_GOLDEN: &[u8] = &[
        0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x76, 0x32, 0x63, 0x6f, 0x6c, 0x6c, 0x00, 0x80, 0x01, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];

    // StoredDecayConfig (current schema: half_life_secs=86400 + half_life_nanos=500,
    // freq_weight 0.25, floor 0.1, auto_archive true, weights Some(0.6,0.4)).
    pub(crate) const STORED_DECAY_CONFIG_GOLDEN: &[u8] = &[
        0x80, 0x51, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf4, 0x01, 0x00, 0x00, 0x00, 0x00, 0x80,
        0x3e, 0xcd, 0xcc, 0xcc, 0x3d, 0x01, 0x01, 0x9a, 0x99, 0x19, 0x3f, 0xcd, 0xcc, 0xcc, 0x3e,
    ];

    // StoredDecayConfigV1 (legacy second-precision: half_life_secs=3600, freq 0.5,
    // floor 0.2, auto_archive false, weights None — NO half_life_nanos field).
    pub(crate) const STORED_DECAY_CONFIG_V1_GOLDEN: &[u8] = &[
        0x10, 0x0e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3f, 0xcd, 0xcc, 0x4c,
        0x3e, 0x00, 0x00,
    ];

    /// The ground-truth `Collective` that `COLLECTIVE_GOLDEN` decodes to.
    fn oracle_collective() -> Collective {
        Collective {
            id: CollectiveId::from_bytes([0x11; 16]),
            name: "demo".into(),
            owner_id: Some("owner".into()),
            embedding_dimension: 384,
            created_at: Timestamp::from_millis(1000),
            updated_at: Timestamp::from_millis(2000),
        }
    }

    /// The ground-truth `Experience` that `EXPERIENCE_GOLDEN` decodes to (Fact,
    /// id [0x21;16], collective [0x11;16], content "hello").
    fn oracle_experience() -> Experience {
        let mut applications: BTreeMap<InstanceId, u32> = BTreeMap::new();
        applications.insert(InstanceId::from_bytes([0x01; 16]), 3);
        applications.insert(InstanceId::from_bytes([0x02; 16]), 7);
        Experience {
            id: ExperienceId::from_bytes([0x21; 16]),
            collective_id: CollectiveId::from_bytes([0x11; 16]),
            content: "hello".into(),
            embedding: Vec::new(),
            experience_type: crate::experience::ExperienceType::Fact {
                statement: "rust".into(),
                source: "docs".into(),
            },
            importance: 0.5,
            confidence: 0.25,
            applications,
            domain: vec!["a".into(), "bb".into()],
            related_files: Vec::new(),
            source_agent: AgentId::new("agent"),
            source_task: Some(crate::types::TaskId::new("task")),
            timestamp: Timestamp::from_millis(111),
            last_reinforced: Timestamp::from_millis(222),
            archived: false,
        }
    }

    // ====================================================================
    // Substrate-format marker tests (work 1.02)
    // ====================================================================

    #[test]
    fn test_substrate_marker_fixed_layout_encoding() {
        // The marker is 3 raw bytes: [magic, magic, version]. It must be
        // parseable WITHOUT serde — assert the exact byte layout.
        let bytes = encode_substrate_marker(CURRENT_SUBSTRATE_FORMAT);
        assert_eq!(bytes.len(), SUBSTRATE_MARKER_LEN);
        assert_eq!(bytes[0], SUBSTRATE_MAGIC[0]);
        assert_eq!(bytes[1], SUBSTRATE_MAGIC[1]);
        assert_eq!(bytes[2], CURRENT_SUBSTRATE_FORMAT);
        // Magic spells "PS".
        assert_eq!(&bytes[..2], b"PS");
    }

    #[test]
    fn test_substrate_marker_encode_decode_roundtrip() {
        for version in [0u8, 1, 7, 42, 255] {
            let bytes = encode_substrate_marker(version);
            let decoded = decode_substrate_marker(&bytes).unwrap();
            assert_eq!(decoded, version);
        }
    }

    #[test]
    fn test_decode_substrate_marker_rejects_wrong_length() {
        // Too short and too long are both corruption, not Absent.
        assert!(decode_substrate_marker(&[]).is_err());
        assert!(decode_substrate_marker(b"PS").is_err());
        assert!(decode_substrate_marker(&[b'P', b'S', 1, 0]).is_err());
    }

    #[test]
    fn test_decode_substrate_marker_rejects_bad_magic() {
        // Right length, wrong magic ⇒ corruption error (not a legacy DB).
        let err = decode_substrate_marker(&[b'X', b'Y', 1]).unwrap_err();
        assert!(err.is_storage());
        assert!(err.to_string().contains("magic"), "err: {err}");
    }

    #[test]
    fn test_substrate_format_classify() {
        assert_eq!(
            SubstrateFormat::classify(CURRENT_SUBSTRATE_FORMAT),
            SubstrateFormat::Current
        );
        assert_eq!(
            SubstrateFormat::classify(CURRENT_SUBSTRATE_FORMAT - 1),
            SubstrateFormat::Older(CURRENT_SUBSTRATE_FORMAT - 1)
        );
        assert_eq!(
            SubstrateFormat::classify(CURRENT_SUBSTRATE_FORMAT + 1),
            SubstrateFormat::Newer(CURRENT_SUBSTRATE_FORMAT + 1)
        );
        assert_eq!(SubstrateFormat::Absent.version(), LEGACY_SUBSTRATE_FORMAT);
        assert_eq!(SubstrateFormat::Older(0).version(), 0);
        assert_eq!(SubstrateFormat::Newer(9).version(), 9);
    }

    #[test]
    fn test_fresh_db_writes_current_substrate_marker() {
        // A freshly initialized database carries the marker and reads as Current.
        let dir = tempdir().unwrap();
        let path = dir.path().join("fresh.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let marker = RedbStorage::read_substrate_marker(storage.database()).unwrap();
        assert_eq!(marker, SubstrateFormat::Current);
        assert_eq!(marker.version(), CURRENT_SUBSTRATE_FORMAT);

        // And the on-disk bytes are exactly the raw fixed layout (not serde).
        let read_txn = storage.database().begin_read().unwrap();
        let meta = read_txn.open_table(METADATA_TABLE).unwrap();
        let raw = meta.get(SUBSTRATE_FORMAT_KEY).unwrap().unwrap();
        assert_eq!(raw.value(), &[b'P', b'S', CURRENT_SUBSTRATE_FORMAT][..]);
    }

    #[test]
    fn test_db_without_marker_reads_as_absent() {
        // A database written WITHOUT the substrate_format key (a pre-4.0,
        // bincode-era store) reads as Absent — the additive marker read does not
        // require the key to exist.
        let dir = tempdir().unwrap();
        let path = dir.path().join("legacy.db");
        {
            let db = Database::builder().create(&path).unwrap();
            let write_txn = db.begin_write().unwrap();
            {
                let mut meta = write_txn.open_table(METADATA_TABLE).unwrap();
                // Write some unrelated metadata, but NOT the substrate marker.
                let metadata = DatabaseMetadata::new(EmbeddingDimension::D384);
                let bytes = postcard::to_stdvec(&metadata).unwrap();
                meta.insert(METADATA_KEY, bytes.as_slice()).unwrap();
            }
            write_txn.commit().unwrap();

            let marker = RedbStorage::read_substrate_marker(&db).unwrap();
            assert_eq!(marker, SubstrateFormat::Absent);
            assert_eq!(marker.version(), LEGACY_SUBSTRATE_FORMAT);
        }
    }

    #[test]
    fn test_malformed_marker_is_corruption_not_absent() {
        // A present-but-malformed marker is a corruption error, NOT Absent.
        let dir = tempdir().unwrap();
        let path = dir.path().join("corrupt.db");
        let db = Database::builder().create(&path).unwrap();
        let write_txn = db.begin_write().unwrap();
        {
            let mut meta = write_txn.open_table(METADATA_TABLE).unwrap();
            // Wrong magic bytes under the right key.
            meta.insert(SUBSTRATE_FORMAT_KEY, &[0xDE_u8, 0xAD, 0x01][..])
                .unwrap();
        }
        write_txn.commit().unwrap();

        let err = RedbStorage::read_substrate_marker(&db).unwrap_err();
        assert!(err.is_storage(), "expected storage/corruption error: {err}");
    }

    #[derive(Serialize)]
    struct LegacyStoredDecayConfig {
        half_life_secs: u64,
        freq_weight: f32,
        floor: f32,
        auto_archive_below_floor: bool,
        default_recall_weights: Option<RecallWeights>,
    }

    fn default_config() -> Config {
        Config::default()
    }

    /// Seeds a redb-v3 file at schema-v2 + Absent marker, using the FROZEN
    /// 1.01 frozen oracle bytes (audit C2 — no live serializer call).
    ///
    /// `EXPERIENCE_V2_GOLDEN` decodes to: id [0;16], collective [0;16], content
    /// "v2", Generic, importance 0.5, **scalar applications = 42**, timestamp 0.
    /// On open the v2→v3 reshape maps that scalar into the LEGACY bucket and the
    /// codec loop re-encodes to postcard. Returns the FIXED golden ids/timestamp.
    fn seed_schema_v2_store(path: &Path) -> (ExperienceId, CollectiveId, Timestamp) {
        let db = Database::builder().create(path).unwrap();
        // Golden experience facts (frozen): the experience's collective is [0;16].
        let experience_id = ExperienceId::from_bytes([0; 16]);
        let collective_id = CollectiveId::from_bytes([0; 16]);
        let timestamp = Timestamp::from_millis(0);

        let metadata_bytes = META_V2_D384_GOLDEN.to_vec();
        // COLLECTIVES is keyed by the experience's collective_id [0;16] and the
        // frozen value decodes to a Collective whose id is also [0;16], so the HNSW
        // index (keyed by the decoded collective id) indexes the migrated row and
        // `search_similar`/`get_collective` resolve. The codec loop re-encodes it.
        let collective_bytes = COLLECTIVE_ZERO_ID_GOLDEN.to_vec();
        let experience_bytes = EXPERIENCE_V2_GOLDEN.to_vec();
        let embedding = vec![0.25_f32; 384];
        let embedding_bytes = f32_slice_to_bytes(&embedding);

        let write_txn = db.begin_write().unwrap();
        {
            let mut meta_table = write_txn.open_table(METADATA_TABLE).unwrap();
            meta_table
                .insert(METADATA_KEY, metadata_bytes.as_slice())
                .unwrap();
            let mut collectives = write_txn.open_table(COLLECTIVES_TABLE).unwrap();
            collectives
                .insert(collective_id.as_bytes(), collective_bytes.as_slice())
                .unwrap();
            let mut experiences = write_txn.open_table(EXPERIENCES_TABLE).unwrap();
            experiences
                .insert(experience_id.as_bytes(), experience_bytes.as_slice())
                .unwrap();
            let mut embeddings = write_txn.open_table(EMBEDDINGS_TABLE).unwrap();
            embeddings
                .insert(experience_id.as_bytes(), embedding_bytes.as_slice())
                .unwrap();

            let mut by_collective = write_txn
                .open_multimap_table(EXPERIENCES_BY_COLLECTIVE_TABLE)
                .unwrap();
            let mut value = [0u8; 24];
            value[..8].copy_from_slice(&timestamp.to_be_bytes());
            value[8..24].copy_from_slice(experience_id.as_bytes());
            by_collective
                .insert(collective_id.as_bytes(), &value)
                .unwrap();

            let mut by_type = write_txn
                .open_multimap_table(EXPERIENCES_BY_TYPE_TABLE)
                .unwrap();
            let type_key =
                encode_type_index_key(collective_id.as_bytes(), ExperienceTypeTag::Generic);
            by_type.insert(&type_key, experience_id.as_bytes()).unwrap();

            let _ = write_txn.open_table(DECAY_CONFIGS_TABLE).unwrap();
            let _ = write_txn.open_table(WATCH_EVENTS_TABLE).unwrap();
            let _ = write_txn.open_table(RELATIONS_TABLE).unwrap();
            let _ = write_txn
                .open_multimap_table(RELATIONS_BY_SOURCE_TABLE)
                .unwrap();
            let _ = write_txn
                .open_multimap_table(RELATIONS_BY_TARGET_TABLE)
                .unwrap();
            let _ = write_txn.open_table(INSIGHTS_TABLE).unwrap();
            let _ = write_txn
                .open_multimap_table(INSIGHTS_BY_COLLECTIVE_TABLE)
                .unwrap();
            let _ = write_txn.open_table(ACTIVITIES_TABLE).unwrap();
        }
        write_txn.commit().unwrap();

        drop(db);

        (experience_id, collective_id, timestamp)
    }

    #[test]
    fn test_schema_v2_experience_migration_writes_legacy_bucket_backup_and_preserves_queries() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let (experience_id, collective_id, timestamp) = seed_schema_v2_store(&path);

        let backup_path = pre_v3_backup_path(&path);
        assert!(!backup_path.exists());

        let db = PulseDB::open(&path, default_config()).unwrap();

        assert!(backup_path.exists(), "v2 migration must retain a backup");
        let experience = db.get_experience(experience_id).unwrap().unwrap();
        assert_eq!(experience.last_reinforced, timestamp);
        // EXPERIENCE_V2_GOLDEN carries scalar applications = 42 → LEGACY bucket.
        assert_eq!(experience.applications(), 42);
        assert_eq!(
            experience
                .applications
                .get(&legacy_applications_instance_id()),
            Some(&42)
        );

        let query = vec![0.25_f32; 384];
        let search_results = db.search_similar(collective_id, &query, 10).unwrap();
        assert!(
            search_results
                .iter()
                .any(|result| result.experience.id == experience_id),
            "migrated experience must remain searchable"
        );

        let recent = db.get_recent_experiences(collective_id, 10).unwrap();
        assert!(
            recent
                .iter()
                .any(|experience| experience.id == experience_id),
            "migrated experience must remain in the by-collective index"
        );

        db.close().unwrap();
    }

    #[test]
    fn test_read_only_open_refuses_unmigrated_schema_v2_store() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        seed_schema_v2_store(&path);

        let err = RedbStorage::open(&path, &Config::read_only()).unwrap_err();
        assert!(matches!(err, PulseDBError::ReadOnly));
        assert!(
            !pre_v3_backup_path(&path).exists(),
            "read-only refusal must happen before migration backup/write work"
        );
    }

    #[test]
    fn test_open_creates_new_database() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        assert!(!path.exists());

        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        assert!(path.exists());
        assert_eq!(storage.metadata().schema_version, SCHEMA_VERSION);
        assert_eq!(
            storage.metadata().embedding_dimension,
            EmbeddingDimension::D384
        );

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_open_existing_database() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        // Create database
        let storage = RedbStorage::open(&path, &default_config()).unwrap();
        let created_at = storage.metadata().created_at;
        Box::new(storage).close().unwrap();

        // Reopen
        std::thread::sleep(std::time::Duration::from_millis(10));
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        // created_at should be preserved
        assert_eq!(storage.metadata().created_at, created_at);
        // last_opened_at should be updated
        assert!(storage.metadata().last_opened_at > created_at);

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_dimension_mismatch_returns_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        // Create with D384
        let config_384 = Config {
            embedding_dimension: EmbeddingDimension::D384,
            ..Default::default()
        };
        let storage = RedbStorage::open(&path, &config_384).unwrap();
        Box::new(storage).close().unwrap();

        // Try to reopen with D768
        let config_768 = Config {
            embedding_dimension: EmbeddingDimension::D768,
            ..Default::default()
        };
        let result = RedbStorage::open(&path, &config_768);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(
            err,
            PulseDBError::Validation(ValidationError::DimensionMismatch { .. })
        ));
    }

    #[test]
    fn test_database_files_created() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("pulse.db");

        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        // Main database file should exist
        assert!(path.exists());
        assert!(storage.path().is_some());
        assert_eq!(storage.path().unwrap(), path);

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_metadata_preserved_across_opens() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let config = Config {
            embedding_dimension: EmbeddingDimension::Custom(512),
            ..Default::default()
        };

        // Create
        let storage = RedbStorage::open(&path, &config).unwrap();
        assert_eq!(
            storage.metadata().embedding_dimension,
            EmbeddingDimension::Custom(512)
        );
        Box::new(storage).close().unwrap();

        // Reopen
        let storage = RedbStorage::open(&path, &config).unwrap();
        assert_eq!(
            storage.metadata().embedding_dimension,
            EmbeddingDimension::Custom(512)
        );
        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_embedding_dimension_accessor() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let config = Config {
            embedding_dimension: EmbeddingDimension::D768,
            ..Default::default()
        };

        let storage = RedbStorage::open(&path, &config).unwrap();
        assert_eq!(storage.embedding_dimension(), EmbeddingDimension::D768);

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_all_six_tables_created() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        // Verify all 6 tables exist by opening each in a read transaction.
        // If any table wasn't created during initialize_new(), this would
        // return a TableDoesNotExist error.
        let read_txn = storage.database().begin_read().unwrap();

        read_txn.open_table(METADATA_TABLE).unwrap();
        read_txn.open_table(COLLECTIVES_TABLE).unwrap();
        read_txn.open_table(EXPERIENCES_TABLE).unwrap();
        read_txn.open_table(EMBEDDINGS_TABLE).unwrap();
        read_txn
            .open_multimap_table(EXPERIENCES_BY_COLLECTIVE_TABLE)
            .unwrap();
        read_txn
            .open_multimap_table(EXPERIENCES_BY_TYPE_TABLE)
            .unwrap();
        read_txn.open_table(DECAY_CONFIGS_TABLE).unwrap();

        Box::new(storage).close().unwrap();
    }

    // ====================================================================
    // Collective CRUD tests
    // ====================================================================

    #[test]
    fn test_save_and_get_collective() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test-project", 384);
        let id = collective.id;

        storage.save_collective(&collective).unwrap();

        let retrieved = storage.get_collective(id).unwrap().unwrap();
        assert_eq!(retrieved.id, id);
        assert_eq!(retrieved.name, "test-project");
        assert_eq!(retrieved.embedding_dimension, 384);
        assert!(retrieved.owner_id.is_none());

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_get_nonexistent_collective_returns_none() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let result = storage.get_collective(CollectiveId::new()).unwrap();
        assert!(result.is_none());

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_decay_config_absent_then_round_trips() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();
        let collective_id = CollectiveId::new();

        assert!(storage.get_decay_config(collective_id).unwrap().is_none());

        let config = crate::config::DecayConfig {
            half_life: std::time::Duration::from_secs(7 * 24 * 60 * 60),
            freq_weight: 0.5,
            floor: 0.2,
            auto_archive_below_floor: true,
            default_recall_weights: None,
        };

        storage
            .set_decay_config(collective_id, config.clone())
            .unwrap();

        let restored = storage.get_decay_config(collective_id).unwrap();
        assert_eq!(restored, Some(config));

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_decay_config_preserves_subsecond_half_life() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();
        let collective_id = CollectiveId::new();

        let config = crate::config::DecayConfig {
            half_life: std::time::Duration::from_millis(750),
            ..crate::config::DecayConfig::default()
        };

        storage
            .set_decay_config(collective_id, config.clone())
            .unwrap();

        let restored = storage.get_decay_config(collective_id).unwrap();
        assert_eq!(restored, Some(config));

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_decay_config_reads_legacy_second_precision_rows() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();
        let collective_id = CollectiveId::new();

        // Post-cutover, `get_decay_config` reads postcard and tries the current
        // `StoredDecayConfig` shape then falls back to the V1 shape (no
        // `half_life_nanos`). Seed a postcard-encoded V1-shaped row to exercise the
        // steady-state dual-version fallback. (A live serializer call would
        // fail AC-3; the legacy V1 byte layout is covered by the decay GOLDEN test.)
        let legacy = LegacyStoredDecayConfig {
            half_life_secs: 42,
            freq_weight: 0.5,
            floor: 0.2,
            auto_archive_below_floor: true,
            default_recall_weights: Some(RecallWeights::new(0.7, 0.3)),
        };
        let bytes = postcard::to_stdvec(&legacy).unwrap();
        let write_txn = storage.db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(DECAY_CONFIGS_TABLE).unwrap();
            table
                .insert(collective_id.as_bytes(), bytes.as_slice())
                .unwrap();
        }
        write_txn.commit().unwrap();

        let restored = storage.get_decay_config(collective_id).unwrap().unwrap();
        assert_eq!(restored.half_life, std::time::Duration::from_secs(42));
        assert_eq!(restored.freq_weight, 0.5);
        assert_eq!(restored.floor, 0.2);
        assert!(restored.auto_archive_below_floor);
        assert_eq!(
            restored.default_recall_weights,
            Some(RecallWeights::new(0.7, 0.3))
        );

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_schema_v3_reopen_skips_pre_v3_migration_path() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let (experience_id, _, _) = seed_schema_v2_store(&path);

        let backup_path = pre_v3_backup_path(&path);
        let db = RedbStorage::open(&path, &default_config()).unwrap();
        assert_eq!(db.metadata().schema_version, SCHEMA_VERSION);
        assert!(backup_path.exists());
        Box::new(db).close().unwrap();

        let reopened = RedbStorage::open(&path, &default_config()).unwrap();
        assert_eq!(reopened.metadata().schema_version, SCHEMA_VERSION);
        let experience = reopened.get_experience(experience_id).unwrap().unwrap();
        assert_eq!(
            experience
                .applications
                .get(&legacy_applications_instance_id()),
            Some(&42)
        );

        Box::new(reopened).close().unwrap();
    }

    #[test]
    fn test_save_collective_overwrites_existing() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let mut collective = Collective::new("original-name", 384);
        let id = collective.id;
        storage.save_collective(&collective).unwrap();

        // Overwrite with updated name
        collective.name = "updated-name".to_string();
        storage.save_collective(&collective).unwrap();

        let retrieved = storage.get_collective(id).unwrap().unwrap();
        assert_eq!(retrieved.name, "updated-name");

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_list_collectives_empty() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collectives = storage.list_collectives().unwrap();
        assert!(collectives.is_empty());

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_list_collectives_returns_all() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let c1 = Collective::new("project-alpha", 384);
        let c2 = Collective::new("project-beta", 384);
        let c3 = Collective::new("project-gamma", 384);

        storage.save_collective(&c1).unwrap();
        storage.save_collective(&c2).unwrap();
        storage.save_collective(&c3).unwrap();

        let collectives = storage.list_collectives().unwrap();
        assert_eq!(collectives.len(), 3);

        // Verify all IDs are present
        let ids: Vec<CollectiveId> = collectives.iter().map(|c| c.id).collect();
        assert!(ids.contains(&c1.id));
        assert!(ids.contains(&c2.id));
        assert!(ids.contains(&c3.id));

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_delete_collective_existing() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("to-delete", 384);
        let id = collective.id;
        storage.save_collective(&collective).unwrap();

        // Delete it
        let deleted = storage.delete_collective(id).unwrap();
        assert!(deleted);

        // Verify it's gone
        assert!(storage.get_collective(id).unwrap().is_none());

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_delete_collective_nonexistent() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let deleted = storage.delete_collective(CollectiveId::new()).unwrap();
        assert!(!deleted);

        Box::new(storage).close().unwrap();
    }

    // ====================================================================
    // ACID Guarantee Tests
    // ====================================================================

    #[test]
    fn test_uncommitted_transaction_is_invisible() {
        // ATOMICITY: If we don't commit a write transaction, the data
        // must not be visible to subsequent reads.
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("phantom", 384);
        let id = collective.id;
        let bytes = postcard::to_stdvec(&collective).unwrap();

        // Open a write transaction, insert data, but DON'T commit -- just drop
        {
            let write_txn = storage.database().begin_write().unwrap();
            {
                let mut table = write_txn.open_table(COLLECTIVES_TABLE).unwrap();
                table.insert(id.as_bytes(), bytes.as_slice()).unwrap();
            }
            // write_txn is dropped here without commit() -- rolled back
        }

        // The collective should NOT be visible
        let result = storage.get_collective(id).unwrap();
        assert!(result.is_none(), "Uncommitted data must not be visible");

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_committed_transaction_is_visible() {
        // DURABILITY (within session): committed data must be immediately
        // visible to subsequent reads.
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("committed", 384);
        let id = collective.id;

        storage.save_collective(&collective).unwrap();

        let result = storage.get_collective(id).unwrap();
        assert!(result.is_some(), "Committed data must be visible");

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_multi_table_atomicity() {
        // ATOMICITY: A single transaction writing to multiple tables
        // is all-or-nothing. Here we write to both COLLECTIVES and METADATA
        // in one transaction and verify both are visible after commit.
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("multi-table", 384);
        let id = collective.id;
        let collective_bytes = postcard::to_stdvec(&collective).unwrap();

        // Write to TWO tables in a single transaction
        let write_txn = storage.database().begin_write().unwrap();
        {
            let mut coll_table = write_txn.open_table(COLLECTIVES_TABLE).unwrap();
            coll_table
                .insert(id.as_bytes(), collective_bytes.as_slice())
                .unwrap();
        }
        {
            let mut meta_table = write_txn.open_table(METADATA_TABLE).unwrap();
            meta_table
                .insert("test_marker", b"multi_table_test".as_slice())
                .unwrap();
        }
        write_txn.commit().unwrap();

        // Verify BOTH writes are visible
        let coll = storage.get_collective(id).unwrap();
        assert!(coll.is_some(), "Collective from multi-table txn must exist");

        let read_txn = storage.database().begin_read().unwrap();
        let meta_table = read_txn.open_table(METADATA_TABLE).unwrap();
        let marker = meta_table.get("test_marker").unwrap();
        assert!(marker.is_some(), "Metadata from multi-table txn must exist");

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_mvcc_read_consistency() {
        // ISOLATION (MVCC): A single read transaction sees a consistent
        // snapshot reflecting all committed writes up to the moment the
        // read was opened, and none of the uncommitted or subsequent ones.
        //
        // We write across multiple separate transactions, then verify a
        // read sees the expected consistent state. Combined with
        // test_uncommitted_transaction_is_invisible (atomicity), this
        // covers the key ACID isolation properties.
        //
        // Note: redb 2.6.3 has a page allocation constraint that prevents
        // holding a read transaction open while a write commits on the
        // same Database handle. redb guarantees MVCC isolation internally
        // via shadow paging; this test verifies our usage is correct.
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        // Write 3 collectives across separate transactions
        let c1 = Collective::new("alpha", 384);
        let c2 = Collective::new("beta", 384);
        let c3 = Collective::new("gamma", 384);

        storage.save_collective(&c1).unwrap();
        storage.save_collective(&c2).unwrap();
        storage.save_collective(&c3).unwrap();

        // Delete c2 (another transaction)
        storage.delete_collective(c2.id).unwrap();

        // A read transaction must see the consistent state:
        // c1 and c3 present, c2 absent
        let read_txn = storage.database().begin_read().unwrap();
        let table = read_txn.open_table(COLLECTIVES_TABLE).unwrap();

        assert!(
            table.get(c1.id.as_bytes()).unwrap().is_some(),
            "c1 must be visible (committed)"
        );
        assert!(
            table.get(c2.id.as_bytes()).unwrap().is_none(),
            "c2 must be absent (deleted)"
        );
        assert!(
            table.get(c3.id.as_bytes()).unwrap().is_some(),
            "c3 must be visible (committed)"
        );

        // Count should be exactly 2
        let count = table.iter().unwrap().count();
        // +1 for the metadata entry? No -- COLLECTIVES_TABLE is separate.
        assert_eq!(count, 2, "Exactly 2 collectives should exist");

        drop(table);
        drop(read_txn);

        Box::new(storage).close().unwrap();
    }

    // ====================================================================
    // Corruption Detection Tests
    // ====================================================================

    #[test]
    fn test_corruption_detection_invalid_metadata_bytes() {
        // Opening a database whose metadata contains garbage bytes
        // must return a Corrupted error, not a panic or deserialization UB.
        let dir = tempdir().unwrap();
        let path = dir.path().join("corrupt.db");

        // Create a valid database, then corrupt the metadata
        let storage = RedbStorage::open(&path, &default_config()).unwrap();
        let write_txn = storage.database().begin_write().unwrap();
        {
            let mut meta = write_txn.open_table(METADATA_TABLE).unwrap();
            meta.insert(METADATA_KEY, b"not-valid-bincode-data".as_slice())
                .unwrap();
        }
        write_txn.commit().unwrap();
        Box::new(storage).close().unwrap();

        // Reopen must detect the corruption
        let result = RedbStorage::open(&path, &default_config());
        assert!(result.is_err(), "Corrupted metadata must be rejected");
        let err = result.unwrap_err();
        match err {
            PulseDBError::Storage(StorageError::Corrupted(msg)) => {
                assert!(
                    msg.contains("Invalid metadata format"),
                    "Error should mention invalid format, got: {}",
                    msg
                );
            }
            other => panic!("Expected StorageError::Corrupted, got: {:?}", other),
        }
    }

    #[test]
    fn test_corruption_detection_missing_metadata_key() {
        // If the metadata table exists but the "db_metadata" key is absent,
        // open_existing must return a Corrupted error.
        let dir = tempdir().unwrap();
        let path = dir.path().join("no_key.db");

        // Create a valid database, then delete the metadata key
        let storage = RedbStorage::open(&path, &default_config()).unwrap();
        let write_txn = storage.database().begin_write().unwrap();
        {
            let mut meta = write_txn.open_table(METADATA_TABLE).unwrap();
            meta.remove(METADATA_KEY).unwrap();
        }
        write_txn.commit().unwrap();
        Box::new(storage).close().unwrap();

        // Reopen must detect the missing key
        let result = RedbStorage::open(&path, &default_config());
        assert!(result.is_err(), "Missing metadata key must be rejected");
        let err = result.unwrap_err();
        match err {
            PulseDBError::Storage(StorageError::Corrupted(msg)) => {
                assert!(
                    msg.contains("Missing database metadata"),
                    "Error should mention missing metadata, got: {}",
                    msg
                );
            }
            other => panic!("Expected StorageError::Corrupted, got: {:?}", other),
        }
    }

    #[test]
    fn test_corruption_detection_missing_metadata_table() {
        // If the metadata table doesn't exist at all, open_existing must
        // return a Corrupted error. We simulate this by creating a raw
        // redb database without our schema tables.
        let dir = tempdir().unwrap();
        let path = dir.path().join("no_table.db");

        // Create a raw redb database with a dummy table (not our schema)
        {
            let db = ::redb::Database::create(&path).unwrap();
            let write_txn = db.begin_write().unwrap();
            {
                let dummy: ::redb::TableDefinition<&str, &str> =
                    ::redb::TableDefinition::new("dummy");
                let mut table = write_txn.open_table(dummy).unwrap();
                table.insert("key", "value").unwrap();
            }
            write_txn.commit().unwrap();
        }

        // Opening this as a PulseDB must detect the missing metadata table
        let result = RedbStorage::open(&path, &default_config());
        assert!(result.is_err(), "Missing metadata table must be rejected");
        let err = result.unwrap_err();
        match err {
            PulseDBError::Storage(StorageError::Corrupted(msg)) => {
                assert!(
                    msg.contains("Cannot open metadata table"),
                    "Error should mention metadata table, got: {}",
                    msg
                );
            }
            other => panic!("Expected StorageError::Corrupted, got: {:?}", other),
        }
    }

    // ====================================================================
    // Experience CRUD tests
    // ====================================================================

    use crate::experience::{Experience, ExperienceType, ExperienceUpdate, Severity};
    use crate::types::{AgentId, ExperienceId, Timestamp};

    /// Creates a test experience with a given collective_id and embedding dimension.
    fn test_experience(collective_id: CollectiveId, dim: usize) -> Experience {
        let timestamp = Timestamp::now();
        Experience {
            id: ExperienceId::new(),
            collective_id,
            content: "Test experience content".into(),
            embedding: vec![0.42; dim],
            experience_type: ExperienceType::Fact {
                statement: "redb uses shadow paging".into(),
                source: "docs".into(),
            },
            importance: 0.8,
            confidence: 0.7,
            applications: BTreeMap::new(),
            domain: vec!["rust".into(), "databases".into()],
            related_files: vec!["src/storage/redb.rs".into()],
            source_agent: AgentId::new("test-agent"),
            source_task: None,
            timestamp,
            last_reinforced: timestamp,
            archived: false,
        }
    }

    #[test]
    fn test_save_and_get_experience() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test", 384);
        storage.save_collective(&collective).unwrap();

        let exp = test_experience(collective.id, 384);
        let exp_id = exp.id;

        storage.save_experience(&exp).unwrap();

        let retrieved = storage.get_experience(exp_id).unwrap().unwrap();
        assert_eq!(retrieved.id, exp_id);
        assert_eq!(retrieved.collective_id, collective.id);
        assert_eq!(retrieved.content, "Test experience content");
        assert_eq!(retrieved.importance, 0.8);
        assert_eq!(retrieved.confidence, 0.7);
        assert_eq!(retrieved.applications(), 0);
        assert_eq!(retrieved.domain, vec!["rust", "databases"]);
        assert!(!retrieved.archived);
        // Embedding should be reconstituted from EMBEDDINGS_TABLE
        assert_eq!(retrieved.embedding.len(), 384);
        assert_eq!(retrieved.embedding[0], 0.42);

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_get_nonexistent_experience_returns_none() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let result = storage.get_experience(ExperienceId::new()).unwrap();
        assert!(result.is_none());

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_update_experience_fields() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test", 384);
        storage.save_collective(&collective).unwrap();

        let exp = test_experience(collective.id, 384);
        let exp_id = exp.id;
        storage.save_experience(&exp).unwrap();

        // Update importance and domain
        let update = ExperienceUpdate {
            importance: Some(0.95),
            domain: Some(vec!["updated-tag".into()]),
            ..Default::default()
        };
        let updated = storage.update_experience(exp_id, &update).unwrap();
        assert!(updated);

        let retrieved = storage.get_experience(exp_id).unwrap().unwrap();
        assert_eq!(retrieved.importance, 0.95);
        assert_eq!(retrieved.domain, vec!["updated-tag"]);
        // Unchanged fields
        assert_eq!(retrieved.confidence, 0.7);

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_update_nonexistent_experience_returns_false() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let update = ExperienceUpdate {
            importance: Some(0.5),
            ..Default::default()
        };
        let result = storage
            .update_experience(ExperienceId::new(), &update)
            .unwrap();
        assert!(!result);

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_delete_experience() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test", 384);
        storage.save_collective(&collective).unwrap();

        let exp = test_experience(collective.id, 384);
        let exp_id = exp.id;
        storage.save_experience(&exp).unwrap();

        // Verify exists
        assert!(storage.get_experience(exp_id).unwrap().is_some());

        // Delete
        let deleted = storage.delete_experience(exp_id).unwrap();
        assert!(deleted);

        // Verify gone
        assert!(storage.get_experience(exp_id).unwrap().is_none());
        assert!(storage.get_embedding(exp_id).unwrap().is_none());

        // Verify index cleaned up
        assert_eq!(
            storage
                .count_experiences_in_collective(collective.id)
                .unwrap(),
            0
        );

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_delete_nonexistent_experience_returns_false() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let result = storage.delete_experience(ExperienceId::new()).unwrap();
        assert!(!result);

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_save_and_get_embedding() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let id = ExperienceId::new();
        let embedding = vec![0.1, 0.2, 0.3, -0.5, 1.0, f32::MIN_POSITIVE];

        storage.save_embedding(id, &embedding).unwrap();

        let retrieved = storage.get_embedding(id).unwrap().unwrap();
        assert_eq!(retrieved, embedding);

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_experience_by_collective_index() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test", 384);
        storage.save_collective(&collective).unwrap();

        // Add 3 experiences
        for _ in 0..3 {
            let exp = test_experience(collective.id, 384);
            storage.save_experience(&exp).unwrap();
        }

        // Count should be 3
        assert_eq!(
            storage
                .count_experiences_in_collective(collective.id)
                .unwrap(),
            3
        );

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_cascade_delete_includes_experiences() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test", 384);
        storage.save_collective(&collective).unwrap();

        let exp1 = test_experience(collective.id, 384);
        let exp2 = test_experience(collective.id, 384);
        let id1 = exp1.id;
        let id2 = exp2.id;
        storage.save_experience(&exp1).unwrap();
        storage.save_experience(&exp2).unwrap();

        // Cascade delete
        let count = storage
            .delete_experiences_by_collective(collective.id)
            .unwrap();
        assert_eq!(count, 2);

        // Verify experiences are gone
        assert!(storage.get_experience(id1).unwrap().is_none());
        assert!(storage.get_experience(id2).unwrap().is_none());
        assert!(storage.get_embedding(id1).unwrap().is_none());
        assert!(storage.get_embedding(id2).unwrap().is_none());

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_update_experience_archived_flag() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test", 384);
        storage.save_collective(&collective).unwrap();

        let exp = test_experience(collective.id, 384);
        let exp_id = exp.id;
        storage.save_experience(&exp).unwrap();

        // Archive
        let update = ExperienceUpdate {
            archived: Some(true),
            ..Default::default()
        };
        storage.update_experience(exp_id, &update).unwrap();

        let retrieved = storage.get_experience(exp_id).unwrap().unwrap();
        assert!(retrieved.archived);

        // Unarchive
        let update = ExperienceUpdate {
            archived: Some(false),
            ..Default::default()
        };
        storage.update_experience(exp_id, &update).unwrap();

        let retrieved = storage.get_experience(exp_id).unwrap().unwrap();
        assert!(!retrieved.archived);

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_f32_byte_conversion_roundtrip() {
        let original = vec![0.0, 1.0, -1.0, f32::MAX, f32::MIN, std::f32::consts::PI];
        let bytes = f32_slice_to_bytes(&original);
        assert_eq!(bytes.len(), original.len() * 4);

        let restored = bytes_to_f32_vec(&bytes);
        assert_eq!(original, restored);
    }

    #[test]
    fn test_experience_with_all_type_variants() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test", 384);
        storage.save_collective(&collective).unwrap();

        // Save one experience per type variant
        let types = vec![
            ExperienceType::Difficulty {
                description: "test".into(),
                severity: Severity::High,
            },
            ExperienceType::Solution {
                problem_ref: None,
                approach: "test".into(),
                worked: true,
            },
            ExperienceType::ErrorPattern {
                signature: "E0308".into(),
                fix: "check types".into(),
                prevention: "use clippy".into(),
            },
            ExperienceType::SuccessPattern {
                task_type: "refactor".into(),
                approach: "extract method".into(),
                quality: 0.9,
            },
            ExperienceType::UserPreference {
                category: "style".into(),
                preference: "snake_case".into(),
                strength: 1.0,
            },
            ExperienceType::ArchitecturalDecision {
                decision: "use redb".into(),
                rationale: "pure Rust".into(),
            },
            ExperienceType::TechInsight {
                technology: "tokio".into(),
                insight: "spawn_blocking".into(),
            },
            ExperienceType::Fact {
                statement: "Rust is safe".into(),
                source: "docs".into(),
            },
            ExperienceType::Generic { category: None },
        ];

        for experience_type in types {
            let mut exp = test_experience(collective.id, 384);
            exp.experience_type = experience_type;
            storage.save_experience(&exp).unwrap();

            // Verify roundtrip
            let retrieved = storage.get_experience(exp.id).unwrap().unwrap();
            assert_eq!(
                retrieved.experience_type.type_tag(),
                exp.experience_type.type_tag()
            );
        }

        assert_eq!(
            storage
                .count_experiences_in_collective(collective.id)
                .unwrap(),
            9
        );

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_reinforce_experience_atomic() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test", 384);
        storage.save_collective(&collective).unwrap();

        let exp = test_experience(collective.id, 384);
        let exp_id = exp.id;
        storage.save_experience(&exp).unwrap();

        // Reinforce 3 times
        assert_eq!(storage.reinforce_experience(exp_id).unwrap(), Some(1));
        assert_eq!(storage.reinforce_experience(exp_id).unwrap(), Some(2));
        assert_eq!(storage.reinforce_experience(exp_id).unwrap(), Some(3));

        // Verify the stored value
        let retrieved = storage.get_experience(exp_id).unwrap().unwrap();
        assert_eq!(retrieved.applications(), 3);

        // Verify embedding was NOT re-written (still intact)
        let emb = storage.get_embedding(exp_id).unwrap().unwrap();
        assert_eq!(emb.len(), 384);

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_reinforce_experience_nonexistent() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let result = storage.reinforce_experience(ExperienceId::new()).unwrap();
        assert!(result.is_none());

        Box::new(storage).close().unwrap();
    }

    // ====================================================================
    // WAL Sequence Tracking Tests (E4-S02)
    // ====================================================================

    #[test]
    fn test_wal_sequence_starts_at_zero() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        assert_eq!(storage.get_wal_sequence().unwrap(), 0);

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_save_experience_increments_wal_sequence() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test", 384);
        storage.save_collective(&collective).unwrap();
        // save_collective now records WAL event #1
        assert_eq!(storage.get_wal_sequence().unwrap(), 1);

        let exp1 = test_experience(collective.id, 384);
        storage.save_experience(&exp1).unwrap();
        assert_eq!(storage.get_wal_sequence().unwrap(), 2);

        let exp2 = test_experience(collective.id, 384);
        storage.save_experience(&exp2).unwrap();
        assert_eq!(storage.get_wal_sequence().unwrap(), 3);

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_poll_watch_events_returns_correct_events() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test", 384);
        storage.save_collective(&collective).unwrap();
        // Collective creates WAL event #1

        let exp1 = test_experience(collective.id, 384);
        let exp2 = test_experience(collective.id, 384);
        let exp3 = test_experience(collective.id, 384);
        storage.save_experience(&exp1).unwrap();
        storage.save_experience(&exp2).unwrap();
        storage.save_experience(&exp3).unwrap();

        // Poll all events (collective + 3 experiences = 4 total)
        let (events, max_seq) = storage.poll_watch_events(0, 100).unwrap();
        assert_eq!(events.len(), 4);
        assert_eq!(max_seq, 4);
        // First event is collective creation, rest are experience creations
        assert!(events
            .iter()
            .all(|e| e.event_type == WatchEventTypeTag::Created));
        // Skip collective event (index 0), experience IDs should match
        assert_eq!(events[1].entity_id, *exp1.id.as_bytes());
        assert_eq!(events[2].entity_id, *exp2.id.as_bytes());
        assert_eq!(events[3].entity_id, *exp3.id.as_bytes());

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_poll_watch_events_since_midpoint() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test", 384);
        storage.save_collective(&collective).unwrap();

        // Create 5 experiences
        for _ in 0..5 {
            let exp = test_experience(collective.id, 384);
            storage.save_experience(&exp).unwrap();
        }

        // Collective = seq 1, 5 experiences = seq 2-6. Total = 6.
        // Poll from seq 4 — should get 2 events (seq 5 and 6)
        let (events, max_seq) = storage.poll_watch_events(4, 100).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(max_seq, 6);

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_poll_watch_events_empty_when_caught_up() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test", 384);
        storage.save_collective(&collective).unwrap();

        let exp = test_experience(collective.id, 384);
        storage.save_experience(&exp).unwrap();

        // Poll everything (collective + experience = 2 events)
        let (events, max_seq) = storage.poll_watch_events(0, 100).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(max_seq, 2);

        // Poll again from same position — empty
        let (events, max_seq) = storage.poll_watch_events(2, 100).unwrap();
        assert_eq!(events.len(), 0);
        assert_eq!(max_seq, 2); // stays the same

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_delete_records_watch_event() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test", 384);
        storage.save_collective(&collective).unwrap();

        let exp = test_experience(collective.id, 384);
        storage.save_experience(&exp).unwrap();
        storage.delete_experience(exp.id).unwrap();

        // Collective(1) + Created(2) + Deleted(3) = 3 events
        let (events, max_seq) = storage.poll_watch_events(0, 100).unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(max_seq, 3);
        assert_eq!(events[0].event_type, WatchEventTypeTag::Created); // collective
        assert_eq!(events[1].event_type, WatchEventTypeTag::Created); // experience
        assert_eq!(events[2].event_type, WatchEventTypeTag::Deleted); // experience deleted
        assert_eq!(events[2].entity_id, *exp.id.as_bytes());

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_update_records_watch_event() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test", 384);
        storage.save_collective(&collective).unwrap();

        let exp = test_experience(collective.id, 384);
        storage.save_experience(&exp).unwrap();

        let update = ExperienceUpdate {
            importance: Some(0.99),
            ..Default::default()
        };
        storage.update_experience(exp.id, &update).unwrap();

        // Collective(1) + Created(2) + Updated(3) = 3 events
        let (events, _) = storage.poll_watch_events(0, 100).unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[2].event_type, WatchEventTypeTag::Updated);

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_reinforce_records_watch_event() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test", 384);
        storage.save_collective(&collective).unwrap();

        let exp = test_experience(collective.id, 384);
        storage.save_experience(&exp).unwrap();
        storage.reinforce_experience(exp.id).unwrap();

        // Collective(1) + Created(2) + Updated(3) = 3 events
        let (events, _) = storage.poll_watch_events(0, 100).unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[1].event_type, WatchEventTypeTag::Created);
        assert_eq!(events[2].event_type, WatchEventTypeTag::Updated);

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_archive_records_archived_event() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test", 384);
        storage.save_collective(&collective).unwrap();

        let exp = test_experience(collective.id, 384);
        storage.save_experience(&exp).unwrap();

        let update = ExperienceUpdate {
            archived: Some(true),
            ..Default::default()
        };
        storage.update_experience(exp.id, &update).unwrap();

        // Collective(1) + Created(2) + Archived(3) = 3 events
        let (events, _) = storage.poll_watch_events(0, 100).unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[2].event_type, WatchEventTypeTag::Archived);

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_poll_watch_events_batch_limit() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        let collective = Collective::new("test", 384);
        storage.save_collective(&collective).unwrap();

        // Create 10 experiences
        for _ in 0..10 {
            let exp = test_experience(collective.id, 384);
            storage.save_experience(&exp).unwrap();
        }

        // Poll with limit of 3
        let (events, max_seq) = storage.poll_watch_events(0, 3).unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(max_seq, 3);

        // Continue from where we left off
        let (events, max_seq) = storage.poll_watch_events(3, 3).unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(max_seq, 6);

        Box::new(storage).close().unwrap();
    }

    // ====================================================================
    // redb file-format upgrade-on-open (dual-redb migrator, work 1.03)
    // ====================================================================

    /// redb-2.6 `TableDefinition` mirror of `schema::METADATA_TABLE` ("metadata",
    /// `&str -> &[u8]`). The 4.x `METADATA_TABLE` const is a `redb::TableDefinition`
    /// and cannot be used with a `redb_v2::Database`, so the v2 seed re-declares it.
    const V2_METADATA_TABLE: redb_v2::TableDefinition<&str, &[u8]> =
        redb_v2::TableDefinition::new("metadata");

    /// Builds a genuine **redb file-format v2** PulseDB store via the aliased
    /// `redb_v2` (2.6) dependency, populated so that after the v2→v3 upgrade it is a
    /// valid schema-v3 store. The store carries `DatabaseMetadata` (bincode),
    /// `instance_id`, and — optionally — a substrate-format marker.
    ///
    /// `marker`: `None` ⇒ no `substrate_format` key (a legacy `{redb-v2, bincode}`
    /// store, Absent); `Some(v)` ⇒ a raw marker with version `v`.
    ///
    /// redb 2.6 creates v2 files by default (no `create_with_file_format_v3`), so
    /// opening this file under redb 4.1 yields `UpgradeRequired`.
    fn seed_redb_v2_store(path: &Path, marker: Option<u8>) {
        let db = redb_v2::Database::builder().create(path).unwrap();
        let write_txn = db.begin_write().unwrap();
        {
            let mut meta = write_txn.open_table(V2_METADATA_TABLE).unwrap();

            // Frozen oracle schema-v3 metadata bytes (audit C2; no live serializer).
            let metadata_bytes = META_V3_D384_GOLDEN.to_vec();
            meta.insert(METADATA_KEY, metadata_bytes.as_slice())
                .unwrap();

            let instance_id = InstanceId::new();
            meta.insert(INSTANCE_ID_KEY, instance_id.as_bytes().as_slice())
                .unwrap();

            if let Some(version) = marker {
                let raw = encode_substrate_marker(version);
                meta.insert(SUBSTRATE_FORMAT_KEY, raw.as_slice()).unwrap();
            }
        }
        write_txn.commit().unwrap();
        drop(db); // release the redb-2.6 lock before any redb-4.1 open
    }

    #[test]
    fn test_redb_v2_file_triggers_upgrade_required_under_redb4() {
        // The premise of the whole migrator: a redb-2.6-created file is format v2,
        // and redb 4.1's `create()` hard-refuses it with `UpgradeRequired` — which
        // `create_database` surfaces as the typed `SubstrateUpgradeRequired`. (This
        // is audit A4 / VS-4.0.1 §10 q3 — the residual un-code-verified claim.)
        let dir = tempdir().unwrap();
        let path = dir.path().join("legacy_v2.db");
        seed_redb_v2_store(&path, None);

        let err = RedbStorage::create_database(&path, &default_config()).unwrap_err();
        assert!(
            matches!(
                err,
                PulseDBError::Storage(StorageError::SubstrateUpgradeRequired { .. })
            ),
            "redb-v2 file must surface as SubstrateUpgradeRequired, got: {err}"
        );
    }

    #[test]
    fn test_writable_open_of_redb_v2_store_migrates_and_writes_marker() {
        // A WRITABLE open of a redb-v2 store migrates it in place (v2->v3), creates
        // the .pre-substrate.bak backup, reopens under redb 4.1, and writes the
        // substrate marker = CURRENT (= {redb-v3, bincode}).
        let dir = tempdir().unwrap();
        let path = dir.path().join("legacy_v2.db");
        seed_redb_v2_store(&path, None);

        let backup_path = pre_substrate_backup_path(&path);
        assert!(!backup_path.exists(), "no backup before migration");

        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        // Backup of the pristine {redb-v2, bincode} file exists (rollback point).
        assert!(
            backup_path.exists(),
            "redb-format migration must retain a .pre-substrate backup"
        );

        // The reopened store reads under redb 4.1 (file is now v3) and carries the
        // CURRENT marker.
        let marker = RedbStorage::read_substrate_marker(storage.database()).unwrap();
        assert_eq!(marker, SubstrateFormat::Current);
        assert_eq!(marker.version(), CURRENT_SUBSTRATE_FORMAT);

        // The schema-v3 .pre-v3.bak sidecar is DISTINCT and not clobbered.
        assert_ne!(backup_path, pre_v3_backup_path(&path));

        Box::new(storage).close().unwrap();

        // Reopening the now-v3 store is a no-op redb-format-wise (idempotent) and
        // still reads as Current.
        let storage = RedbStorage::open(&path, &default_config()).unwrap();
        assert_eq!(
            RedbStorage::read_substrate_marker(storage.database()).unwrap(),
            SubstrateFormat::Current
        );
        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_read_only_open_of_redb_v2_store_refuses_with_zero_writes() {
        // FR-035 / audit C6: a read-only open of an un-migrated redb-v2 store returns
        // ReadOnly BEFORE any write — no backup, no migration-lock content, no
        // upgrade. The file stays redb-v2 (untouched).
        let dir = tempdir().unwrap();
        let path = dir.path().join("legacy_v2.db");
        seed_redb_v2_store(&path, None);

        let err = RedbStorage::open(&path, &Config::read_only()).unwrap_err();
        assert!(
            matches!(err, PulseDBError::ReadOnly),
            "read-only open of redb-v2 store must return ReadOnly, got: {err}"
        );
        assert!(
            !pre_substrate_backup_path(&path).exists(),
            "read-only refusal must happen BEFORE any backup write (zero writes)"
        );

        // The file is untouched — still redb-v2 (a fresh redb-4.1 create still
        // refuses it).
        let still_v2 = RedbStorage::create_database(&path, &Config::read_only());
        assert!(
            matches!(
                still_v2,
                Err(PulseDBError::Storage(
                    StorageError::SubstrateUpgradeRequired { .. }
                ))
            ),
            "read-only open must NOT have upgraded the file"
        );
    }

    #[test]
    fn test_migration_lock_serializes_and_releases() {
        // The PulseDB-owned migration lock (audit C2) is exclusive: a second
        // acquisition fails while the first is held, and succeeds once released.
        // (fs2 advisory lock; same seam as src/watch/lock.rs.)
        use fs2::FileExt;
        let dir = tempdir().unwrap();
        let path = dir.path().join("locked.db");

        let lock_path = migration_lock_path(&path);
        {
            let _held = MigrationLock::acquire_exclusive(&path).unwrap();
            assert!(lock_path.exists(), "lock sidecar must be created");

            // A non-blocking try on the same advisory lock must fail while held.
            let probe = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&lock_path)
                .unwrap();
            assert!(
                probe.try_lock_exclusive().is_err(),
                "migration lock must be exclusive while held"
            );
        }
        // After drop, the lock is released — a fresh acquisition succeeds.
        let _reacquired = MigrationLock::acquire_exclusive(&path).unwrap();
    }

    #[test]
    fn test_backup_once_is_atomic_and_idempotent() {
        // The .pre-substrate backup claim is atomic (create_new) and idempotent: a
        // second call against an already-claimed path is a no-op and does NOT
        // clobber the genuine pristine sidecar.
        let dir = tempdir().unwrap();
        let path = dir.path().join("data.db");
        std::fs::write(&path, b"pristine-v2-bytes").unwrap();
        let backup_path = pre_substrate_backup_path(&path);

        assert_eq!(
            backup_once(&path, &backup_path).unwrap(),
            BackupOutcome::Created,
            "the first call creates the sidecar"
        );
        assert_eq!(std::fs::read(&backup_path).unwrap(), b"pristine-v2-bytes");

        // Mutate the source, then call backup_once again — the existing backup must
        // be preserved (idempotent no-op), NOT overwritten with the new bytes.
        std::fs::write(&path, b"already-migrated-v3-bytes").unwrap();
        assert_eq!(
            backup_once(&path, &backup_path).unwrap(),
            BackupOutcome::Preserved,
            "the second call preserves the existing sidecar"
        );
        assert_eq!(
            std::fs::read(&backup_path).unwrap(),
            b"pristine-v2-bytes",
            "backup_once must not clobber an existing pristine sidecar"
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_backup_once_does_not_follow_temp_symlink() {
        // PR#57-review P2 (symlink-safety): a hostile symlink pre-planted at the temp
        // staging path (by another local user with write access to a shared parent
        // dir) must NOT be followed — `backup_once` must never write THROUGH it to an
        // attacker-chosen target, and must still publish a correct regular-file sidecar.
        use std::os::unix::fs::symlink;
        let dir = tempdir().unwrap();
        let src = dir.path().join("data.db");
        std::fs::write(&src, b"pristine-v2-bytes").unwrap();
        let victim = dir.path().join("victim.txt");
        std::fs::write(&victim, b"DO-NOT-TOUCH").unwrap();
        let backup_path = pre_substrate_backup_path(&src);
        let temp_path = backup_temp_path(&backup_path);
        symlink(&victim, &temp_path).unwrap(); // hostile pre-planted temp symlink

        assert_eq!(
            backup_once(&src, &backup_path).unwrap(),
            BackupOutcome::Created
        );

        // The target the symlink pointed at must be untouched (not truncated/rewritten).
        assert_eq!(
            std::fs::read(&victim).unwrap(),
            b"DO-NOT-TOUCH",
            "backup_once must not write through a pre-planted temp symlink to its target"
        );
        // The published sidecar must be a correct, byte-identical REGULAR file.
        assert_eq!(std::fs::read(&backup_path).unwrap(), b"pristine-v2-bytes");
        assert!(
            std::fs::symlink_metadata(&backup_path)
                .unwrap()
                .file_type()
                .is_file(),
            "the published sidecar must be a regular file, not a symlink"
        );
    }

    #[test]
    fn test_substrate_format_too_new_is_refused() {
        // A store whose substrate marker is NEWER than this build (a future
        // VS-4.0.3 postcard file, marker = 2) is forward-incompatible: opening it
        // returns the typed SubstrateFormatTooNew and does NOT touch the file.
        //
        // Built as a redb-v2 store with marker=2 so it exercises the full path
        // (redb upgrade -> reopen -> marker gate). After the redb-format upgrade the
        // marker (a copied-through raw entry) still reads as Newer(2).
        let dir = tempdir().unwrap();
        let path = dir.path().join("future.db");
        seed_redb_v2_store(&path, Some(CURRENT_SUBSTRATE_FORMAT + 1));

        let err = RedbStorage::open(&path, &default_config()).unwrap_err();
        match err {
            PulseDBError::Storage(StorageError::SubstrateFormatTooNew { found, current }) => {
                assert_eq!(found, CURRENT_SUBSTRATE_FORMAT + 1);
                assert_eq!(current, CURRENT_SUBSTRATE_FORMAT);
            }
            other => panic!("expected SubstrateFormatTooNew, got: {other}"),
        }
    }

    #[test]
    fn test_read_only_open_of_current_store_writes_nothing() {
        // Audit C6: a read-only open of an already-current store performs ZERO
        // writes — in particular it must NOT update last_opened_at (`touch()`).
        let dir = tempdir().unwrap();
        let path = dir.path().join("current.db");

        // Create + close a fresh (current) store, capturing last_opened_at.
        let storage = RedbStorage::open(&path, &default_config()).unwrap();
        let opened_at_before = storage.metadata().last_opened_at;
        Box::new(storage).close().unwrap();

        std::thread::sleep(std::time::Duration::from_millis(10));

        // Read-only reopen must not bump last_opened_at (no write txn at all).
        let storage = RedbStorage::open(&path, &Config::read_only()).unwrap();
        assert_eq!(
            storage.metadata().last_opened_at,
            opened_at_before,
            "read-only open must not write last_opened_at"
        );
        Box::new(storage).close().unwrap();
    }

    // ====================================================================
    // redb-2.x representative fixture + migration/concurrency (work 1.04)
    // ====================================================================
    //
    // This section proves the VS-4.0.2 user demo at the INTEGRATION /
    // value-identity level: a pre-existing v0.5.1 (redb-2.x) PulseDB database
    // opens under redb 4.x, migrates behind a `.pre-substrate` backup, and every
    // entity reads back identically with no data loss — plus the audit C2
    // concurrency cases and the FR-035/C6 read-only carve-out, exercised against
    // a *representative* programmatically-built v2 fixture (collective +
    // experience + raw-f32 embedding + secondary-index entries).
    //
    // The full real-v0.5.1 checked-in file (every entity type) is VS-4.0.4; the
    // programmatically-built fixture below is sufficient for the redb-format proof.
    //
    // 1.03's `seed_redb_v2_store` (metadata-only) and its unit-level migration
    // tests are LEFT UNTOUCHED; this is a new, richer builder + five new tests.

    /// redb-2.6 `TableDefinition`/`MultimapTableDefinition` mirrors of the schema
    /// tables, with the **same string names** as the 4.x consts. The 4.x consts are
    /// `redb::TableDefinition`s and cannot be used with a `redb_v2::Database`, so the
    /// v2 seed re-declares each table it needs to write.
    const V2_COLLECTIVES_TABLE: redb_v2::TableDefinition<&[u8; 16], &[u8]> =
        redb_v2::TableDefinition::new("collectives");
    const V2_EXPERIENCES_TABLE: redb_v2::TableDefinition<&[u8; 16], &[u8]> =
        redb_v2::TableDefinition::new("experiences");
    const V2_EMBEDDINGS_TABLE: redb_v2::TableDefinition<&[u8; 16], &[u8]> =
        redb_v2::TableDefinition::new("embeddings");
    const V2_EXPERIENCES_BY_COLLECTIVE_TABLE: redb_v2::MultimapTableDefinition<
        &[u8; 16],
        &[u8; 24],
    > = redb_v2::MultimapTableDefinition::new("experiences_by_collective");
    const V2_EXPERIENCES_BY_TYPE_TABLE: redb_v2::MultimapTableDefinition<&[u8; 17], &[u8; 16]> =
        redb_v2::MultimapTableDefinition::new("experiences_by_type");

    /// Builds a **representative** genuine redb-file-format-v2 PulseDB store via the
    /// aliased `redb_v2` (2.6 creates v2 files by default), populated the way
    /// production `save_*` does — bincode `DatabaseMetadata` (schema-v3) + `instance_id`,
    /// a real `Collective`, a real schema-v3 `Experience`, the raw-f32 embedding blob,
    /// and both secondary-index entries. **No** `substrate_format` key is written
    /// (Absent ⇒ a legacy `{redb-v2, bincode}` store).
    ///
    /// On-disk encodings mirror `save_collective` / `save_experience` exactly so the
    /// post-migration readback is a true byte-identity check (audit A3): redb's v2→v3
    /// `upgrade()` rewrites the file format but preserves stored key/value bytes, so
    /// the raw tables (embeddings, multimaps, instance_id) carry through byte-identically.
    ///
    /// Returns the seeded `(Collective, Experience, Vec<f32>)` so tests assert against
    /// known ground truth. All values are deterministic. Drops the redb-2.6 handle
    /// before returning to release its lock ahead of any redb-4.1 open.
    fn seed_representative_redb_v2_store(path: &Path) -> (Collective, Experience, Vec<f32>) {
        // Ground truth is the 1.01 oracle entities (audit C2): the on-disk
        // collective / experience / metadata blobs are the FROZEN oracle bincode
        // bytes (`*_GOLDEN`), NOT a live serializer call. So this fixture seeds
        // genuine legacy on-disk values without any bare-crate serde call in `src/`.
        let collective = oracle_collective();
        let experience = oracle_experience();

        // Deterministic 384-length embedding with a NON-uniform pattern, so a
        // silently-wrong copy is caught and the length matches D384.
        let embedding: Vec<f32> = (0..384).map(|i| (i as f32) * 0.0013 - 0.1).collect();

        // Frozen oracle bincode bytes (the embedding is serde-skipped, so the
        // experience blob does NOT contain it).
        let metadata_bytes = META_V3_D384_GOLDEN.to_vec();
        let collective_bytes = COLLECTIVE_GOLDEN.to_vec();
        let experience_bytes = EXPERIENCE_GOLDEN.to_vec();
        let embedding_bytes = f32_slice_to_bytes(&embedding);

        let instance_id = InstanceId::from_bytes(*b"FIXTUREINSTANCE_");

        // 24-byte by-collective index value: [timestamp_be: 8][exp_id: 16].
        let mut by_collective_value = [0u8; 24];
        by_collective_value[..8].copy_from_slice(&experience.timestamp.to_be_bytes());
        by_collective_value[8..24].copy_from_slice(experience.id.as_bytes());

        // 17-byte by-type index key: [collective_id: 16][type_tag: 1].
        let type_key = encode_type_index_key(
            collective.id.as_bytes(),
            experience.experience_type.type_tag(),
        );

        let db = redb_v2::Database::builder().create(path).unwrap();
        let write_txn = db.begin_write().unwrap();
        {
            // metadata table: db_metadata (bincode) + instance_id (raw 16 bytes),
            // NO substrate_format key (Absent ⇒ legacy v2).
            let mut meta = write_txn.open_table(V2_METADATA_TABLE).unwrap();
            meta.insert(METADATA_KEY, metadata_bytes.as_slice())
                .unwrap();
            meta.insert(INSTANCE_ID_KEY, instance_id.as_bytes().as_slice())
                .unwrap();

            let mut collectives = write_txn.open_table(V2_COLLECTIVES_TABLE).unwrap();
            collectives
                .insert(collective.id.as_bytes(), collective_bytes.as_slice())
                .unwrap();

            let mut experiences = write_txn.open_table(V2_EXPERIENCES_TABLE).unwrap();
            experiences
                .insert(experience.id.as_bytes(), experience_bytes.as_slice())
                .unwrap();

            let mut embeddings = write_txn.open_table(V2_EMBEDDINGS_TABLE).unwrap();
            embeddings
                .insert(experience.id.as_bytes(), embedding_bytes.as_slice())
                .unwrap();

            let mut by_collective = write_txn
                .open_multimap_table(V2_EXPERIENCES_BY_COLLECTIVE_TABLE)
                .unwrap();
            by_collective
                .insert(collective.id.as_bytes(), &by_collective_value)
                .unwrap();

            let mut by_type = write_txn
                .open_multimap_table(V2_EXPERIENCES_BY_TYPE_TABLE)
                .unwrap();
            by_type.insert(&type_key, experience.id.as_bytes()).unwrap();
        }
        write_txn.commit().unwrap();
        drop(db); // release the redb-2.6 lock before any redb-4.1 open

        // Return the experience with the embedding set in-memory (ground truth).
        let mut experience = experience;
        experience.embedding = embedding.clone();
        (collective, experience, embedding)
    }

    /// Asserts two experiences are field-by-field identical, EXCLUDING the serde-skip
    /// embedding (compared separately by the byte-identity assertions). `Experience`
    /// does not derive `PartialEq`, so this compares every persisted field.
    fn assert_experience_eq(got: &Experience, want: &Experience) {
        assert_eq!(got.id, want.id, "experience id");
        assert_eq!(got.collective_id, want.collective_id, "collective_id");
        assert_eq!(got.content, want.content, "content");
        // ExperienceType does not derive PartialEq (and production types are out of
        // scope for this work item), so compare its tag + its bincode encoding — a
        // true structural value-identity check for the enum + its payload.
        assert_eq!(
            got.experience_type.type_tag(),
            want.experience_type.type_tag(),
            "experience_type tag"
        );
        assert_eq!(
            postcard::to_stdvec(&got.experience_type).unwrap(),
            postcard::to_stdvec(&want.experience_type).unwrap(),
            "experience_type payload"
        );
        assert_eq!(got.importance, want.importance, "importance");
        assert_eq!(got.confidence, want.confidence, "confidence");
        assert_eq!(got.applications, want.applications, "applications BTreeMap");
        assert_eq!(
            got.applications(),
            want.applications(),
            "applications() total"
        );
        assert_eq!(got.domain, want.domain, "domain");
        assert_eq!(got.related_files, want.related_files, "related_files");
        assert_eq!(got.source_agent, want.source_agent, "source_agent");
        assert_eq!(got.source_task, want.source_task, "source_task");
        assert_eq!(got.timestamp, want.timestamp, "timestamp");
        assert_eq!(got.last_reinforced, want.last_reinforced, "last_reinforced");
        assert_eq!(got.archived, want.archived, "archived");
    }

    fn assert_collective_eq(got: &Collective, want: &Collective) {
        assert_eq!(got.id, want.id, "collective id");
        assert_eq!(got.name, want.name, "name");
        assert_eq!(got.owner_id, want.owner_id, "owner_id");
        assert_eq!(
            got.embedding_dimension, want.embedding_dimension,
            "embedding_dimension"
        );
        assert_eq!(got.created_at, want.created_at, "created_at");
        assert_eq!(got.updated_at, want.updated_at, "updated_at");
    }

    #[test]
    fn test_representative_redb_v2_store_is_genuinely_v2() {
        // Guard against a false-green v3 fixture: the representative fixture is a
        // genuine redb-file-format-v2 file, so a redb-4.1 `create` hard-refuses it
        // with `SubstrateUpgradeRequired` BEFORE any value is reachable.
        let dir = tempdir().unwrap();
        let path = dir.path().join("representative_v2.db");
        let (_c, _e, _emb) = seed_representative_redb_v2_store(&path);

        let err = RedbStorage::create_database(&path, &default_config()).unwrap_err();
        assert!(
            matches!(
                err,
                PulseDBError::Storage(StorageError::SubstrateUpgradeRequired { .. })
            ),
            "representative fixture must be genuinely redb-v2 (SubstrateUpgradeRequired), got: {err}"
        );
    }

    #[test]
    fn test_writable_migrate_of_representative_v2_store_preserves_every_entity() {
        // THE USER DEMO: open the representative v0.5.1 (redb-v2) fixture WRITABLE
        // under redb 4.1 -> migrates behind a `.pre-substrate` backup, marker becomes
        // Current, and EVERY entity reads back identically (value-identity for the
        // collective + experience; byte-identity for the embedding + index entries).
        let dir = tempdir().unwrap();
        let path = dir.path().join("representative_v2.db");
        let (collective, experience, embedding) = seed_representative_redb_v2_store(&path);

        let backup_path = pre_substrate_backup_path(&path);
        assert!(!backup_path.exists(), "no backup before migration");

        // Opens Ok — no UpgradeRequired / error leaks to the caller.
        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        // The pristine {redb-v2, bincode} backup sidecar exists (rollback point).
        assert!(
            backup_path.exists(),
            "writable migrate must retain a .pre-substrate backup"
        );

        // Marker reads as Current (= {redb-v3, bincode}).
        assert_eq!(
            RedbStorage::read_substrate_marker(storage.database()).unwrap(),
            SubstrateFormat::Current
        );

        // --- value identity: collective + experience read back field-for-field ---
        let got_collective = storage.get_collective(collective.id).unwrap().unwrap();
        assert_collective_eq(&got_collective, &collective);

        let got_experience = storage.get_experience(experience.id).unwrap().unwrap();
        assert_experience_eq(&got_experience, &experience);
        // The reconstituted embedding (joined from EMBEDDINGS_TABLE) equals ground truth.
        assert_eq!(
            got_experience.embedding, embedding,
            "reconstituted embedding must equal the seeded vector"
        );
        assert_eq!(got_experience.embedding.len(), 384, "D384 length preserved");

        // --- byte identity (audit A3): embedding raw bytes carried through unchanged ---
        {
            let read_txn = storage.database().begin_read().unwrap();
            let emb_table = read_txn.open_table(EMBEDDINGS_TABLE).unwrap();
            let raw = emb_table.get(experience.id.as_bytes()).unwrap().unwrap();
            assert_eq!(
                raw.value(),
                f32_slice_to_bytes(&embedding).as_slice(),
                "embedding bytes must survive the v2->v3 upgrade byte-for-byte"
            );
            assert_eq!(
                bytes_to_f32_vec(raw.value()),
                embedding,
                "embedding decodes back to the seeded vector"
            );
        }

        // --- byte identity (audit A3): secondary-index entries carried through ---
        {
            let read_txn = storage.database().begin_read().unwrap();

            // by-collective multimap: exact 24-byte [ts_be][exp_id] value present.
            let by_collective = read_txn
                .open_multimap_table(EXPERIENCES_BY_COLLECTIVE_TABLE)
                .unwrap();
            let mut expected_idx = [0u8; 24];
            expected_idx[..8].copy_from_slice(&experience.timestamp.to_be_bytes());
            expected_idx[8..24].copy_from_slice(experience.id.as_bytes());
            let mut found_idx = false;
            for entry in by_collective.get(collective.id.as_bytes()).unwrap() {
                let entry = entry.unwrap();
                if entry.value() == &expected_idx {
                    found_idx = true;
                }
            }
            assert!(
                found_idx,
                "by-collective index must contain the exact [ts_be][exp_id] 24-byte value"
            );

            // by-type multimap: e.id present under the (collective_id, type_tag) key.
            let by_type = read_txn
                .open_multimap_table(EXPERIENCES_BY_TYPE_TABLE)
                .unwrap();
            let type_key = encode_type_index_key(
                collective.id.as_bytes(),
                experience.experience_type.type_tag(),
            );
            let mut found_type = false;
            for entry in by_type.get(&type_key).unwrap() {
                let entry = entry.unwrap();
                if entry.value() == experience.id.as_bytes() {
                    found_type = true;
                }
            }
            assert!(
                found_type,
                "by-type index must contain e.id under the (collective_id, type_tag) key"
            );
        }

        // --- functional queries over the migrated indices ---
        assert_eq!(
            storage
                .list_experience_ids_in_collective(collective.id)
                .unwrap(),
            vec![experience.id],
            "by-collective functional query must yield exactly the seeded experience"
        );
        let recent = storage
            .get_recent_experience_ids(collective.id, 10)
            .unwrap();
        assert_eq!(
            recent,
            vec![(experience.id, experience.timestamp)],
            "get_recent_experience_ids must yield (e.id, e.timestamp)"
        );

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_read_only_open_of_representative_v2_store_refuses_with_zero_writes() {
        // FR-035 / audit C6: a read-only open of the un-migrated representative v2
        // fixture returns ReadOnly with ZERO writes — proving both that the fixture is
        // genuinely v2 AND that a read-only open touches nothing.
        let dir = tempdir().unwrap();
        let path = dir.path().join("representative_v2.db");
        let (_c, _e, _emb) = seed_representative_redb_v2_store(&path);

        let backup_path = pre_substrate_backup_path(&path);
        assert!(
            !backup_path.exists(),
            "no backup before the read-only attempt"
        );
        let mtime_before = std::fs::metadata(&path).unwrap().modified().unwrap();

        let err = RedbStorage::open(&path, &Config::read_only()).unwrap_err();
        assert!(
            matches!(err, PulseDBError::ReadOnly),
            "read-only open of representative v2 store must return ReadOnly, got: {err}"
        );

        // ZERO writes: mtime unchanged, no backup created, file still genuinely v2.
        let mtime_after = std::fs::metadata(&path).unwrap().modified().unwrap();
        assert_eq!(
            mtime_before, mtime_after,
            "read-only refusal must not modify the fixture file (mtime unchanged)"
        );
        assert!(
            !backup_path.exists(),
            "read-only refusal must happen BEFORE any backup write (zero writes)"
        );
        let still_v2 = RedbStorage::create_database(&path, &Config::read_only());
        assert!(
            matches!(
                still_v2,
                Err(PulseDBError::Storage(
                    StorageError::SubstrateUpgradeRequired { .. }
                ))
            ),
            "read-only open must NOT have upgraded the file (still genuinely v2)"
        );
    }

    #[test]
    fn test_concurrent_writable_openers_migrate_exactly_once_without_corruption() {
        // Audit C2(a): two concurrent writable openers of the same v2 fixture. The
        // fs2 migration lock serializes the destructive upgrade => exactly one
        // `.pre-substrate.bak`; `DatabaseLocked` is the typed, recoverable contention
        // outcome (redb 4.1 holds an exclusive file lock, so a concurrent opener may
        // transiently collide), NEVER corruption. Any OTHER error fails the test.
        use std::sync::{Arc, Barrier};

        let dir = tempdir().unwrap();
        let path = dir.path().join("representative_v2.db");
        let (collective, experience, _emb) = seed_representative_redb_v2_store(&path);

        let barrier = Arc::new(Barrier::new(2));
        let mut handles = Vec::new();
        for _ in 0..2 {
            let barrier = Arc::clone(&barrier);
            let path = path.clone();
            let collective_id = collective.id;
            let experience_id = experience.id;
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                // Bounded retry: the ONLY acceptable transient error is the typed
                // DatabaseLocked (the redb-4.1 exclusive file lock). Anything else
                // is a hard failure (returned as Err for the joiner to surface).
                let mut last_locked: Option<String> = None;
                for _ in 0..200 {
                    match RedbStorage::open(&path, &default_config()) {
                        Ok(storage) => {
                            // Validate the migrated handle reads the seeded data, then
                            // drop promptly to release the redb-4.1 handle.
                            let c = storage
                                .get_collective(collective_id)
                                .expect("get_collective")
                                .expect("collective present");
                            assert_eq!(c.id, collective_id);
                            let e = storage
                                .get_experience(experience_id)
                                .expect("get_experience")
                                .expect("experience present");
                            assert_eq!(e.id, experience_id);
                            Box::new(storage).close().expect("close");
                            return Ok(());
                        }
                        Err(PulseDBError::Storage(StorageError::DatabaseLocked)) => {
                            last_locked = Some("DatabaseLocked".into());
                            std::thread::sleep(std::time::Duration::from_millis(10));
                        }
                        Err(other) => {
                            return Err(format!("unexpected non-locked error: {other}"));
                        }
                    }
                }
                Err(format!(
                    "exhausted retries still contending (last transient: {last_locked:?})"
                ))
            }));
        }

        for handle in handles {
            handle
                .join()
                .expect("thread panicked")
                .expect("opener failed");
        }

        // Exactly one destructive upgrade => exactly one .pre-substrate backup.
        assert!(
            pre_substrate_backup_path(&path).exists(),
            "the migration must have produced a .pre-substrate backup"
        );

        // Final reopen: marker Current and ALL seeded data intact (no corruption).
        let storage = RedbStorage::open(&path, &default_config()).unwrap();
        assert_eq!(
            RedbStorage::read_substrate_marker(storage.database()).unwrap(),
            SubstrateFormat::Current
        );
        let got_collective = storage.get_collective(collective.id).unwrap().unwrap();
        assert_collective_eq(&got_collective, &collective);
        let got_experience = storage.get_experience(experience.id).unwrap().unwrap();
        assert_experience_eq(&got_experience, &experience);
        assert_eq!(
            storage
                .list_experience_ids_in_collective(collective.id)
                .unwrap(),
            vec![experience.id]
        );
        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_old_version_lock_holder_fails_typed_not_corrupt() {
        // Audit C2(b): an old-version process still holding the v2 file (simulated by
        // an open redb_v2::Database handle) must make a concurrent writable migrate
        // surface the TYPED DatabaseLocked (the DatabaseAlreadyOpen -> DatabaseLocked
        // mapping fires — empirically either at redb-4.1 `create` or inside
        // upgrade_redb_v2_to_v3's redb_v2::Database::open; either path yields the typed
        // error), NEVER corruption. Do NOT assert on backup presence here.
        let dir = tempdir().unwrap();
        let path = dir.path().join("representative_v2.db");
        let (collective, experience, embedding) = seed_representative_redb_v2_store(&path);

        // Hold an old-version (redb 2.6) handle open across the contended attempt.
        let held = redb_v2::Database::builder().open(&path).unwrap();

        let err = RedbStorage::open(&path, &default_config()).unwrap_err();
        assert!(
            matches!(err, PulseDBError::Storage(StorageError::DatabaseLocked)),
            "an old-version lock holder must yield the typed DatabaseLocked, got: {err}"
        );

        // Release the held handle; the file must be uncorrupted and still migratable:
        // a fresh writable open succeeds, migrates, and reads every entity identically.
        drop(held);
        let storage = RedbStorage::open(&path, &default_config()).unwrap();
        assert_eq!(
            RedbStorage::read_substrate_marker(storage.database()).unwrap(),
            SubstrateFormat::Current
        );
        let got_collective = storage.get_collective(collective.id).unwrap().unwrap();
        assert_collective_eq(&got_collective, &collective);
        let got_experience = storage.get_experience(experience.id).unwrap().unwrap();
        assert_experience_eq(&got_experience, &experience);
        assert_eq!(
            got_experience.embedding, embedding,
            "embedding intact after the contended-then-successful migrate"
        );
        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_lock_aborted_upgrade_leaves_no_stale_pre_substrate_sidecar() {
        // #4 / T4: if the destructive redb-v2→v3 upgrade aborts because a legacy
        // (redb-2.x) writer still holds the file, the `.pre-substrate.bak` claimed
        // just before it may be a STALE snapshot (it can miss the legacy writer's
        // latest commit). Leaving it behind lets the next open's `AlreadyExists`
        // branch preserve it as a genuine rollback point; instead the lock-aborted
        // migrate must remove it so the retry re-backs-up a fresh pristine copy.
        //
        // The DatabaseAlreadyOpen→DatabaseLocked mapping fires either at the redb-4.1
        // `create` (BEFORE the backup — no sidecar is ever written) or inside
        // `upgrade_redb_v2_to_v3`'s redb_v2 open (AFTER the backup — where the fix's
        // cleanup runs). The asserted invariant "no stale sidecar afterward" holds
        // for both; only the post-backup path exercises the new cleanup branch.
        let dir = tempdir().unwrap();
        let path = dir.path().join("representative_v2.db");
        let (collective, experience, _emb) = seed_representative_redb_v2_store(&path);
        let backup_path = pre_substrate_backup_path(&path);

        // Hold an old-version (redb 2.6) handle open across the contended migrate.
        let held = redb_v2::Database::builder().open(&path).unwrap();

        let err = RedbStorage::open(&path, &default_config()).unwrap_err();
        assert!(
            matches!(err, PulseDBError::Storage(StorageError::DatabaseLocked)),
            "a legacy lock holder must yield the typed DatabaseLocked, got: {err}"
        );
        assert!(
            !backup_path.exists(),
            "a lock-aborted upgrade must not leave a stale `.pre-substrate.bak` behind \
             (it must be removed so the retry re-backs-up a fresh pristine copy)"
        );

        // Release the holder: the file is uncorrupted and still migratable — a fresh
        // open migrates and reads every entity back identically, proving the removed
        // sidecar did not compromise recovery (the store's own bytes were untouched).
        drop(held);
        let storage = RedbStorage::open(&path, &default_config()).unwrap();
        assert_eq!(
            RedbStorage::read_substrate_marker(storage.database()).unwrap(),
            SubstrateFormat::Current
        );
        let got_collective = storage.get_collective(collective.id).unwrap().unwrap();
        assert_collective_eq(&got_collective, &collective);
        let got_experience = storage.get_experience(experience.id).unwrap().unwrap();
        assert_experience_eq(&got_experience, &experience);
        // The successful retry re-created a pristine `.pre-substrate.bak`.
        assert!(
            backup_path.exists(),
            "the successful retry must produce a fresh `.pre-substrate.bak`"
        );
        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_cleanup_stale_backup_removes_sidecar_only_on_lock_abort() {
        // #4 / T4 — the deterministic unit of the cleanup DECISION (the lock-holder
        // integration test above can only reach the pre-backup lock stage on this
        // platform, so it cannot drive this branch directly). A lock-aborted upgrade
        // (DatabaseLocked ⟹ redb-v2 open failed ⟹ store untouched ⟹ stale sidecar)
        // must REMOVE the sidecar; any OTHER upgrade error (a torn in-place upgrade)
        // must KEEP it as the rollback point.
        let dir = tempdir().unwrap();
        let path = dir.path().join("data.db");
        let backup_path = pre_substrate_backup_path(&path);

        // Lock abort ⇒ remove the (possibly stale) sidecar.
        std::fs::write(&backup_path, b"stale-snapshot").unwrap();
        cleanup_stale_backup_if_lock_aborted(
            &path,
            &PulseDBError::Storage(StorageError::DatabaseLocked),
        );
        assert!(
            !backup_path.exists(),
            "a DatabaseLocked upgrade abort must remove the stale sidecar"
        );

        // Any non-lock upgrade error (torn in-place upgrade) ⇒ KEEP the rollback point.
        std::fs::write(&backup_path, b"pristine-rollback-point").unwrap();
        cleanup_stale_backup_if_lock_aborted(
            &path,
            &PulseDBError::Storage(StorageError::Redb("torn in-place upgrade".into())),
        );
        assert!(
            backup_path.exists(),
            "a non-lock upgrade error must KEEP the sidecar as the rollback point"
        );
        assert_eq!(
            std::fs::read(&backup_path).unwrap(),
            b"pristine-rollback-point",
            "the kept sidecar must be untouched"
        );
    }

    // ====================================================================
    // NEW {redb-v3, bincode} "Older" fixture — the gate-2 grill Q1 pin
    // (decoupled codec axis; work-1.04 §7)
    // ====================================================================

    /// Seeds a genuine **redb-v3** (redb 4.1) store at marker `1`
    /// (`{redb-v3, bincode}`, schema v3), every serde-blob value being the FROZEN
    /// 1.01 oracle bincode bytes (audit C2). This is the NO-OP-reshape path: schema
    /// is already v3, so the reshape migrators do nothing — yet the codec loop must
    /// still re-encode EVERY serde-blob row to postcard. A fused codec-in-reshape
    /// would silently skip EXPERIENCES + WAL here (grill Q1 = silent corruption).
    ///
    /// The redb-4.1 file scaffold + the raw `substrate_format = 1` marker carry NO
    /// serde — only the value blobs are bincode (frozen oracle bytes).
    fn seed_redb_v3_bincode_older_store(path: &Path) -> (Collective, Experience) {
        let collective = oracle_collective(); // id [0x11;16]
        let experience = oracle_experience(); // id [0x21;16], collective [0x11;16]
        let embedding: Vec<f32> = (0..384).map(|i| (i as f32) * 0.0011 + 0.05).collect();
        let embedding_bytes = f32_slice_to_bytes(&embedding);

        // WATCH_EVENTS: WATCH_EVENT_GOLDEN decodes to entity_id [0x71;16],
        // collective [0x72;16], Updated, ts 12345, Experience. Seq key = 1 (BE).
        let wal_seq: u64 = 1;
        let wal_key = wal_seq.to_be_bytes();

        let db = Database::builder().create(path).unwrap(); // redb 4.1 ⇒ v3 file
        let write_txn = db.begin_write().unwrap();
        {
            let mut meta = write_txn.open_table(METADATA_TABLE).unwrap();
            meta.insert(METADATA_KEY, META_V3_D384_GOLDEN).unwrap();
            meta.insert(
                INSTANCE_ID_KEY,
                InstanceId::from_bytes(*b"OLDERFIXTUREINST")
                    .as_bytes()
                    .as_slice(),
            )
            .unwrap();
            // wal_sequence: raw 8-byte BE (copy-through; NEVER decoded).
            meta.insert(WAL_SEQUENCE_KEY, wal_seq.to_be_bytes().as_slice())
                .unwrap();
            // Raw marker = 1 (Older): 3 hand-encoded bytes, no serde.
            meta.insert(SUBSTRATE_FORMAT_KEY, encode_substrate_marker(1).as_slice())
                .unwrap();

            write_txn
                .open_table(COLLECTIVES_TABLE)
                .unwrap()
                .insert(collective.id.as_bytes(), COLLECTIVE_GOLDEN)
                .unwrap();
            write_txn
                .open_table(EXPERIENCES_TABLE)
                .unwrap()
                .insert(experience.id.as_bytes(), EXPERIENCE_GOLDEN)
                .unwrap();
            write_txn
                .open_table(EMBEDDINGS_TABLE)
                .unwrap()
                .insert(experience.id.as_bytes(), embedding_bytes.as_slice())
                .unwrap();
            write_txn
                .open_table(RELATIONS_TABLE)
                .unwrap()
                .insert(&[0x41u8; 16], RELATION_GOLDEN)
                .unwrap();
            write_txn
                .open_table(INSIGHTS_TABLE)
                .unwrap()
                .insert(&[0x51u8; 16], INSIGHT_GOLDEN)
                .unwrap();
            write_txn
                .open_table(WATCH_EVENTS_TABLE)
                .unwrap()
                .insert(&wal_key, WATCH_EVENT_GOLDEN)
                .unwrap();

            // Secondary indexes (copy-through, NEVER decoded).
            let mut idx_value = [0u8; 24];
            idx_value[..8].copy_from_slice(&experience.timestamp.to_be_bytes());
            idx_value[8..24].copy_from_slice(experience.id.as_bytes());
            write_txn
                .open_multimap_table(EXPERIENCES_BY_COLLECTIVE_TABLE)
                .unwrap()
                .insert(collective.id.as_bytes(), &idx_value)
                .unwrap();
            let type_key = encode_type_index_key(
                collective.id.as_bytes(),
                experience.experience_type.type_tag(),
            );
            write_txn
                .open_multimap_table(EXPERIENCES_BY_TYPE_TABLE)
                .unwrap()
                .insert(&type_key, experience.id.as_bytes())
                .unwrap();
            let _ = write_txn.open_table(DECAY_CONFIGS_TABLE).unwrap();
            let _ = write_txn.open_table(ACTIVITIES_TABLE).unwrap();
            let _ = write_txn
                .open_multimap_table(RELATIONS_BY_SOURCE_TABLE)
                .unwrap();
            let _ = write_txn
                .open_multimap_table(RELATIONS_BY_TARGET_TABLE)
                .unwrap();
            let _ = write_txn
                .open_multimap_table(INSIGHTS_BY_COLLECTIVE_TABLE)
                .unwrap();
        }
        write_txn.commit().unwrap();
        drop(db);

        let mut experience = experience;
        experience.embedding = embedding;
        (collective, experience)
    }

    #[test]
    fn test_older_marker1_store_reads_as_older_before_migration() {
        // Premise: the seeder builds a genuine marker-1 {redb-v3, bincode} store
        // (Older), NOT Absent and NOT a redb-v2 file.
        let dir = tempdir().unwrap();
        let path = dir.path().join("older.db");
        let _ = seed_redb_v3_bincode_older_store(&path);

        // redb-4.1 opens it directly (already v3 — no UpgradeRequired).
        let db = RedbStorage::create_database(&path, &default_config()).unwrap();
        assert_eq!(
            RedbStorage::read_substrate_marker(&db).unwrap(),
            SubstrateFormat::Older(1),
            "marker-1 v3 store must classify as Older(1) once CURRENT bumped to 2"
        );
    }

    #[test]
    fn test_writable_migrate_of_older_marker1_store_reencodes_experiences_and_wal_to_postcard() {
        // THE gate-2 grill Q1 PIN: the {redb-v3, bincode} no-op-reshape path. A
        // writable open must re-encode EXPERIENCES + WAL (and every serde-blob) to
        // postcard via the DECOUPLED codec loop — NOT merely flip the marker. A
        // fused codec-in-reshape would silently skip these rows (silent corruption).
        let dir = tempdir().unwrap();
        let path = dir.path().join("older.db");
        let (collective, experience) = seed_redb_v3_bincode_older_store(&path);

        let storage = RedbStorage::open(&path, &default_config()).unwrap();

        // Marker bumped to CURRENT (2 = {redb-v3, postcard}).
        assert_eq!(
            RedbStorage::read_substrate_marker(storage.database()).unwrap(),
            SubstrateFormat::Current
        );
        assert_eq!(CURRENT_SUBSTRATE_FORMAT, 2);

        // --- EXPERIENCES re-encoded to postcard (decodes as postcard, NOT bincode) ---
        {
            let read_txn = storage.database().begin_read().unwrap();
            let exp_table = read_txn.open_table(EXPERIENCES_TABLE).unwrap();
            let raw = exp_table.get(experience.id.as_bytes()).unwrap().unwrap();
            let raw_bytes = raw.value().to_vec();
            // Postcard-decodable (the codec loop re-encoded it)...
            let as_postcard: Experience = postcard::from_bytes(&raw_bytes)
                .expect("migrated EXPERIENCES row must decode as postcard");
            assert_eq!(as_postcard.id, experience.id);
            assert_eq!(as_postcard.content, "hello");
            // ...and NO LONGER the original frozen bincode bytes (proves a rewrite).
            assert_ne!(
                raw_bytes.as_slice(),
                EXPERIENCE_GOLDEN,
                "EXPERIENCES must have been re-encoded away from the bincode bytes"
            );
        }

        // --- WAL (WATCH_EVENTS) re-encoded to postcard ---
        {
            let read_txn = storage.database().begin_read().unwrap();
            let wal_table = read_txn.open_table(WATCH_EVENTS_TABLE).unwrap();
            let raw = wal_table.get(&1u64.to_be_bytes()).unwrap().unwrap();
            let raw_bytes = raw.value().to_vec();
            let as_postcard: WatchEventRecord = postcard::from_bytes(&raw_bytes)
                .expect("migrated WATCH_EVENTS row must decode as postcard");
            assert_eq!(as_postcard.entity_id, [0x71; 16]);
            assert_eq!(as_postcard.timestamp_ms, 12345);
            assert_ne!(
                raw_bytes.as_slice(),
                WATCH_EVENT_GOLDEN,
                "WAL must have been re-encoded away from the bincode bytes"
            );
        }

        // --- value identity through the steady-state postcard read path ---
        let got_collective = storage.get_collective(collective.id).unwrap().unwrap();
        assert_collective_eq(&got_collective, &collective);
        let got_experience = storage.get_experience(experience.id).unwrap().unwrap();
        assert_experience_eq(&got_experience, &experience);

        // --- §2a copy-through byte-identity: embeddings + indexes + raw meta keys ---
        {
            let read_txn = storage.database().begin_read().unwrap();
            let emb = read_txn.open_table(EMBEDDINGS_TABLE).unwrap();
            let raw = emb.get(experience.id.as_bytes()).unwrap().unwrap();
            assert_eq!(
                raw.value(),
                f32_slice_to_bytes(&experience.embedding).as_slice(),
                "embedding bytes copied through byte-identically (never decoded)"
            );
            let meta = read_txn.open_table(METADATA_TABLE).unwrap();
            // wal_sequence raw 8-byte BE copied through untouched.
            assert_eq!(
                meta.get(WAL_SEQUENCE_KEY).unwrap().unwrap().value(),
                1u64.to_be_bytes().as_slice(),
                "wal_sequence raw bytes copied through (never decoded)"
            );
            // instance_id raw 16 bytes copied through untouched.
            assert_eq!(
                meta.get(INSTANCE_ID_KEY).unwrap().unwrap().value(),
                InstanceId::from_bytes(*b"OLDERFIXTUREINST")
                    .as_bytes()
                    .as_slice(),
                "instance_id raw bytes copied through (never decoded)"
            );
        }

        Box::new(storage).close().unwrap();
    }

    #[test]
    fn test_read_only_open_of_older_marker1_store_refuses_with_zero_codec_writes() {
        // FR-035 / audit C6: a read-only open of the marker-1 (Older) {redb-v3,
        // bincode} store refuses with ReadOnly and performs ZERO PulseDB writes —
        // no codec migration, no marker bump, the values stay bincode.
        //
        // Note on mtime: unlike the redb-v2 read-only test (refused in
        // `create_or_migrate` BEFORE redb ever opens the file), a marker-1 store is
        // already redb-v3, so redb's own `Database::create` opens the file (and may
        // touch its mtime / lock page) BEFORE the codec read-only gate fires. That
        // open-time touch is redb's bookkeeping, NOT a PulseDB write; the meaningful
        // FR-035 guarantee here is "no codec migration / marker unchanged", asserted
        // via the still-`Older(1)` marker + still-bincode values below.
        let dir = tempdir().unwrap();
        let path = dir.path().join("older.db");
        let (_collective, experience) = seed_redb_v3_bincode_older_store(&path);

        let err = RedbStorage::open(&path, &Config::read_only()).unwrap_err();
        assert!(
            matches!(err, PulseDBError::ReadOnly),
            "read-only open of an un-migrated (marker-1) store must return ReadOnly, got: {err}"
        );

        // Still marker 1 (no codec migration ran).
        let db = RedbStorage::create_database(&path, &Config::read_only()).unwrap();
        assert_eq!(
            RedbStorage::read_substrate_marker(&db).unwrap(),
            SubstrateFormat::Older(1),
            "read-only refusal must leave the store at marker 1 (no codec migration)"
        );
        // And the EXPERIENCES value is STILL the original frozen bincode bytes
        // (proving the codec loop did not run) — decodes via legacy_bincode, and the
        // on-disk bytes equal the seeded golden.
        let read_txn = db.begin_read().unwrap();
        let exp_table = read_txn.open_table(EXPERIENCES_TABLE).unwrap();
        let raw = exp_table.get(experience.id.as_bytes()).unwrap().unwrap();
        assert_eq!(
            raw.value(),
            EXPERIENCE_GOLDEN,
            "read-only refusal must leave EXPERIENCES as the original bincode bytes"
        );
    }

    // ====================================================================
    // Transaction-shape decision (§6.3 config-first) — single-txn primary +
    // fail-closed-above-floor valve (§6.4(3)).
    // ====================================================================

    #[test]
    fn test_codec_txn_shape_below_floor_is_single_txn() {
        // The common path: a store below the 1 GiB conservative floor always takes
        // the single-txn path regardless of declared memory.
        let cfg = default_config();
        // copy_through = 0 (re-encodable-dominated) — the strictest case for the floor.
        assert!(RedbStorage::resolve_codec_txn_shape(10 * 1024 * 1024, 0, &cfg).is_ok());
        assert!(RedbStorage::resolve_codec_txn_shape(
            RedbStorage::SINGLE_TXN_STORE_FLOOR_BYTES,
            0,
            &cfg
        )
        .is_ok());
    }

    #[test]
    fn test_codec_txn_shape_above_floor_undeclared_fails_closed() {
        // Above the floor with NO declared memory budget: fail closed with the
        // typed SubstrateMigrationTooLarge — zero destructive writes (§6.4(3)).
        let cfg = default_config();
        let store_size = 4 * RedbStorage::SINGLE_TXN_STORE_FLOOR_BYTES; // 4 GiB
                                                                        // copy_through = 0 → footprint == store_size (re-encodable-dominated, the
                                                                        // strictest / most-conservative peak for this store size).
        let err = RedbStorage::resolve_codec_txn_shape(store_size, 0, &cfg).unwrap_err();
        match err {
            PulseDBError::Storage(StorageError::SubstrateMigrationTooLarge {
                store_size: s,
                projected_peak,
                budget,
            }) => {
                assert_eq!(s, store_size);
                // #54b: projected_peak is now the copy_through_excluded footprint ×
                // coefficient × PEAK_SAFETY_MARGIN (1.5). With copy_through = 0 the
                // footprint is the whole store, so peak == margined(store_size).
                assert_eq!(
                    projected_peak,
                    RedbStorage::projected_peak_with_margin(store_size)
                );
                // Margined peak strictly exceeds the bare coefficient peak (bias to
                // over-estimate → fail-closed).
                assert!(projected_peak > store_size / 10);
                assert_eq!(budget, RedbStorage::SINGLE_TXN_STORE_FLOOR_BYTES);
            }
            other => panic!("expected SubstrateMigrationTooLarge, got: {other}"),
        }
    }

    #[test]
    fn test_codec_txn_shape_above_floor_declared_memory_opts_into_single_txn() {
        // Config-first opt-in: a declared budget covering the projected peak (with
        // the 0.5 safety margin) authorizes single-txn for an above-floor store.
        let store_size = 4 * RedbStorage::SINGLE_TXN_STORE_FLOOR_BYTES; // 4 GiB
                                                                        // #54b: the peak the memory axis compares is now the margined footprint peak
                                                                        // (copy_through = 0 → footprint == store_size).
        let projected_peak = RedbStorage::projected_peak_with_margin(store_size);

        // Budget over 2× projected_peak clears the declared-mem margin (peak < budget/2).
        let mut cfg = default_config();
        cfg.migration_available_memory_bytes = Some(projected_peak * 2 + 2);
        assert!(
            RedbStorage::resolve_codec_txn_shape(store_size, 0, &cfg).is_ok(),
            "a declared budget covering 2× projected peak must allow single-txn"
        );

        // A too-small declared budget still fails closed.
        let mut cfg_small = default_config();
        cfg_small.migration_available_memory_bytes = Some(projected_peak); // peak !< peak/2
        assert!(matches!(
            RedbStorage::resolve_codec_txn_shape(store_size, 0, &cfg_small),
            Err(PulseDBError::Storage(
                StorageError::SubstrateMigrationTooLarge { .. }
            ))
        ));
    }

    // ====================================================================
    // Unified headroom preflight — disk axis (VS-4.0.3 work-1.05 / audit C3).
    // The memory axis is `resolve_codec_txn_shape` (tested above). These cover
    // the NEW disk free-space leg and the unification that runs disk THEN memory,
    // failing with a typed error BEFORE any destructive write.
    // ====================================================================

    #[test]
    fn test_required_migration_disk_bytes_accounts_for_backup_and_margin() {
        // The pristine `.pre-substrate.bak` backup ≈ doubles the on-disk footprint,
        // the postcard re-encode does NOT shrink the size-dominant embedding table
        // (so migrated ≈ 1× store_size), and redb needs a txn-growth margin. The
        // required free-space estimate must therefore exceed the raw store size by
        // a clear margin — at least ~2× (backup + migrated) is the conservative
        // floor the disk preflight enforces.
        let store_size = 100 * 1024 * 1024; // 100 MiB
        let required = RedbStorage::required_migration_disk_bytes(store_size);
        assert!(
            required >= 2 * store_size,
            "required free ({required}) must cover backup (~1×) + migrated (~1×) for store {store_size}"
        );
    }

    #[test]
    fn test_headroom_preflight_insufficient_disk_fails_closed_before_write() {
        // Disk axis: when available free space is below the required estimate, the
        // unified preflight returns the typed SubstrateMigrationInsufficientDisk
        // with zero destructive writes — and surfaces the observed/required figures.
        let cfg = default_config();
        let store_size = 100 * 1024 * 1024; // 100 MiB, well below the memory floor
        let required = RedbStorage::required_migration_disk_bytes(store_size);
        let available = required - 1; // one byte short
        let err = RedbStorage::resolve_migration_headroom(store_size, 0, available, &cfg, true)
            .unwrap_err();
        match err {
            PulseDBError::Storage(StorageError::SubstrateMigrationInsufficientDisk {
                store_size: s,
                required: r,
                available: a,
            }) => {
                assert_eq!(s, store_size);
                assert_eq!(r, required);
                assert_eq!(a, available);
            }
            other => panic!("expected SubstrateMigrationInsufficientDisk, got: {other}"),
        }
    }

    #[test]
    fn test_headroom_preflight_sufficient_disk_below_floor_proceeds() {
        // Sufficient disk + a below-floor store (memory axis trivially OK) →
        // the unified preflight returns Ok and the migration may proceed.
        let cfg = default_config();
        let store_size = 100 * 1024 * 1024; // 100 MiB
        let available = RedbStorage::required_migration_disk_bytes(store_size); // exactly enough
        assert!(
            RedbStorage::resolve_migration_headroom(store_size, 0, available, &cfg, true).is_ok(),
            "sufficient disk + below-floor store must pass the unified preflight"
        );
    }

    #[test]
    fn test_headroom_preflight_disk_checked_before_memory() {
        // Unification ordering / fail-closed: an above-floor store with NO declared
        // memory budget AND insufficient disk must still fail closed. The disk axis
        // is checked first (it is the cheaper, earlier guard against a half-migration
        // that exhausts disk), so the surfaced error is the disk one.
        let cfg = default_config(); // no declared memory budget
        let store_size = 4 * RedbStorage::SINGLE_TXN_STORE_FLOOR_BYTES; // 4 GiB, above floor
        let required = RedbStorage::required_migration_disk_bytes(store_size);
        let available = required / 2; // insufficient disk
        let err = RedbStorage::resolve_migration_headroom(store_size, 0, available, &cfg, true)
            .unwrap_err();
        assert!(
            matches!(
                err,
                PulseDBError::Storage(StorageError::SubstrateMigrationInsufficientDisk { .. })
            ),
            "disk axis must be evaluated before memory; got: {err}"
        );
    }

    #[test]
    fn test_headroom_preflight_sufficient_disk_above_floor_undeclared_fails_on_memory() {
        // Ample disk but an above-floor store with NO declared memory budget must
        // STILL fail — on the memory axis (SubstrateMigrationTooLarge). This proves
        // the unified preflight covers BOTH axes (it does not pass just because disk
        // is fine) and reuses 1.04's resolve_codec_txn_shape rather than re-deriving.
        let cfg = default_config(); // no declared memory budget
        let store_size = 4 * RedbStorage::SINGLE_TXN_STORE_FLOOR_BYTES; // 4 GiB, above floor
                                                                        // Plenty of disk: far more than required.
        let available = RedbStorage::required_migration_disk_bytes(store_size) * 2;
        // copy_through = 0 → footprint == store_size → margined peak exceeds the floor
        // budget → still fails on the memory axis (re-encodable-dominated worst case).
        let err = RedbStorage::resolve_migration_headroom(store_size, 0, available, &cfg, true)
            .unwrap_err();
        assert!(
            matches!(
                err,
                PulseDBError::Storage(StorageError::SubstrateMigrationTooLarge { .. })
            ),
            "ample disk but no memory budget above floor must fail on the memory axis; got: {err}"
        );
    }

    #[test]
    fn test_headroom_preflight_above_floor_declared_memory_and_disk_proceeds() {
        // Both axes satisfied for an above-floor store: a declared memory budget
        // covering the projected peak AND sufficient disk → Ok.
        let store_size = 4 * RedbStorage::SINGLE_TXN_STORE_FLOOR_BYTES; // 4 GiB
                                                                        // #54b: budget must cover the MARGINED footprint peak (copy_through = 0).
        let projected_peak = RedbStorage::projected_peak_with_margin(store_size);
        let mut cfg = default_config();
        cfg.migration_available_memory_bytes = Some(projected_peak * 2 + 2); // clears the margin
        let available = RedbStorage::required_migration_disk_bytes(store_size);
        assert!(
            RedbStorage::resolve_migration_headroom(store_size, 0, available, &cfg, true).is_ok(),
            "declared memory + sufficient disk must pass the unified preflight above the floor"
        );
    }

    // ====================================================================
    // #54 — copy-through-excluded, safety-margined projected-peak floor.
    // TWO-SIDED: an embedding-heavy (copy-through-dominated) store must now OPEN,
    // AND a serde-blob-heavy (re-encodable-dominated) store must stay conservatively
    // sized (NOT under-projected into OOM). The margin biases every uncertain call
    // toward over-estimating the peak — fail-closed is the safe error, OOM is not.
    // ====================================================================

    #[test]
    fn test_re_encodable_footprint_excludes_full_copy_through_set() {
        // #54a: the footprint is store_size MINUS the full §2a copy-through set
        // (embeddings + 5 multimaps + raw metadata keys), saturating.
        let store_size = 10 * 1024 * 1024;
        let copy_through = 9 * 1024 * 1024; // copy-through dominates
        assert_eq!(
            RedbStorage::re_encodable_footprint(store_size, copy_through),
            1024 * 1024,
            "footprint must exclude the whole copy-through set"
        );
        // Saturating: copy_through > store_size floors at 0 (never underflows).
        assert_eq!(
            RedbStorage::re_encodable_footprint(store_size, store_size + 1),
            0
        );
    }

    #[test]
    fn test_peak_safety_margin_biases_projected_peak_up() {
        // #54b: the margined peak strictly exceeds the bare coefficient peak, so the
        // formula over-estimates (fail-closed side) rather than under-estimating.
        let footprint = 4 * RedbStorage::SINGLE_TXN_STORE_FLOOR_BYTES;
        let bare = RedbStorage::projected_peak_rss(footprint);
        let margined = RedbStorage::projected_peak_with_margin(footprint);
        assert!(
            margined > bare,
            "PEAK_SAFETY_MARGIN must inflate the peak (margined {margined} > bare {bare})"
        );
        // 1.5×, rounding UP (bias to over-estimate → fail-closed is the safe side).
        assert_eq!(margined, bare.saturating_mul(3).div_ceil(2));
    }

    #[test]
    fn test_embedding_heavy_store_now_opens() {
        // #54c lower-bound falsifier: a copy-through-DOMINATED store (its store_size
        // is above the floor, but almost all of it is embeddings + indexes + raw
        // metadata keys, i.e. copy-through) has a tiny re-encodable footprint → its
        // margined peak fits the floor-implied budget → it now OPENS (single-txn OK),
        // where keying the floor on raw store_size would have WRONGLY failed it closed.
        let cfg = default_config(); // NO declared memory budget — must pass on its own
        let store_size = 8 * RedbStorage::SINGLE_TXN_STORE_FLOOR_BYTES; // 8 GiB total
                                                                        // 99.99% copy-through (embedding-heavy): footprint is a sliver.
        let copy_through = store_size - (store_size / 10_000);

        // Sanity: keying on raw store_size (copy_through = 0) WOULD fail closed —
        // this is the bug the change fixes.
        assert!(
            RedbStorage::resolve_codec_txn_shape(store_size, 0, &cfg).is_err(),
            "raw-store_size keying would wrongly fail this store closed (the bug)"
        );
        // With the copy-through-excluded footprint it OPENS.
        assert!(
            RedbStorage::resolve_codec_txn_shape(store_size, copy_through, &cfg).is_ok(),
            "copy-through-dominated store must now open (copy_through_excluded peak fits the floor budget)"
        );
    }

    #[test]
    fn test_projected_peak_serde_blob_heavy_not_under_projected() {
        // #54c UPPER-guard falsifier (the C5 no-under-projection guard): a
        // re-encodable-DOMINATED store (near-zero copy-through — many small serde
        // blobs, almost no embeddings/indexes) must be sized CONSERVATIVELY and NOT
        // under-projected. Its projected peak must remain ≥ coefficient × footprint
        // WITH the safety margin applied, so an above-budget re-encode still fails
        // closed rather than being allowed to OOM.
        let store_size = 4 * RedbStorage::SINGLE_TXN_STORE_FLOOR_BYTES; // 4 GiB
        let copy_through = store_size / 1000; // near-zero copy-through
        let footprint = RedbStorage::re_encodable_footprint(store_size, copy_through);

        // The projected peak the decision uses must be the MARGINED peak (over the
        // bare coefficient peak) — the formula does not under-count re-encode cost.
        let margined = RedbStorage::projected_peak_with_margin(footprint);
        assert!(
            margined >= RedbStorage::projected_peak_rss(footprint),
            "serde-blob-heavy peak must keep the safety margin (never under-projected)"
        );
        assert!(
            margined > footprint / 10,
            "margined peak must exceed the bare 0.10× footprint (conservative sizing)"
        );

        // And with no declared budget it STILL fails closed above the floor — it is
        // NOT allowed to slip through into an OOM.
        let cfg = default_config();
        let err = RedbStorage::resolve_codec_txn_shape(store_size, copy_through, &cfg).unwrap_err();
        match err {
            PulseDBError::Storage(StorageError::SubstrateMigrationTooLarge {
                projected_peak,
                ..
            }) => {
                assert_eq!(
                    projected_peak, margined,
                    "the surfaced peak must be the margined (conservative) peak"
                );
            }
            other => panic!("re-encodable-dominated store must fail closed, got: {other}"),
        }

        // A too-small declared budget must NOT authorize it (no under-projection
        // lets a genuinely memory-heavy re-encode proceed and OOM).
        let mut cfg_small = default_config();
        cfg_small.migration_available_memory_bytes = Some(margined); // peak !< peak/2
        assert!(
            RedbStorage::resolve_codec_txn_shape(store_size, copy_through, &cfg_small).is_err(),
            "a budget below 2× the margined peak must still fail closed"
        );
    }

    // ====================================================================
    // #53a — preflight BEFORE destruction (fail-closed with ZERO mutation).
    // The falsifier proves the rejected open leaves the store's whole-file bytes
    // AND mtime pristine — not merely that an error returned (C10). Dep-free:
    // std::fs whole-file byte read + mtime, no sha2/blake3.
    // ====================================================================

    #[test]
    fn test_too_large_fails_closed_before_mutation_store_bytes_unchanged() {
        // #53a + C10: a too-large redb-v2 store fails closed INSIDE the migration
        // lock, AFTER the post-lock re-check, BEFORE backup_once — so NO
        // `.pre-substrate.bak` is written AND the store file's whole-file bytes +
        // mtime are byte-identical before vs after the rejected open.
        let dir = tempdir().unwrap();
        let path = dir.path().join("too_large_v2.db");
        seed_redb_v2_store(&path, None);

        // Drive the boundary with a tiny physical fixture: force the single-txn floor
        // to 0 so ANY store is "above the floor" and (with no declared budget and
        // copy_through = 0 in the redb-v2 arm) fails closed.
        let _floor = FloorOverrideGuard::set(0);

        // Snapshot whole-file bytes + mtime BEFORE the rejected open.
        let bytes_before = std::fs::read(&path).unwrap();
        let mtime_before = std::fs::metadata(&path).unwrap().modified().unwrap();
        let backup_path = pre_substrate_backup_path(&path);
        assert!(!backup_path.exists(), "no backup before the open attempt");

        // A writable open must fail closed with the typed too-large error.
        let err = RedbStorage::open(&path, &default_config()).unwrap_err();
        assert!(
            matches!(
                err,
                PulseDBError::Storage(StorageError::SubstrateMigrationTooLarge { .. })
            ),
            "too-large redb-v2 store must fail closed with SubstrateMigrationTooLarge, got: {err}"
        );

        // ZERO mutation: no backup sidecar was written...
        assert!(
            !backup_path.exists(),
            "fail-closed must happen BEFORE backup_once — NO .pre-substrate.bak"
        );
        // ...and the store file's whole-file bytes + mtime are UNCHANGED (a stronger
        // falsifier than a hash digest, and dep-free).
        let bytes_after = std::fs::read(&path).unwrap();
        let mtime_after = std::fs::metadata(&path).unwrap().modified().unwrap();
        assert_eq!(
            bytes_before, bytes_after,
            "rejected open must leave the store's whole-file bytes UNCHANGED (content_unchanged)"
        );
        assert_eq!(
            mtime_before, mtime_after,
            "rejected open must leave the store file's mtime UNCHANGED"
        );

        // The file is still genuinely redb-v2 (the destructive upgrade never ran).
        let still_v2 = RedbStorage::create_database(&path, &default_config());
        assert!(
            matches!(
                still_v2,
                Err(PulseDBError::Storage(
                    StorageError::SubstrateUpgradeRequired { .. }
                ))
            ),
            "fail-closed open must NOT have upgraded the file"
        );
    }

    #[test]
    fn test_read_only_open_too_large_returns_readonly() {
        // #53a carve-out (FR-035/C6): a READ-ONLY open of a too-large redb-v2 store
        // returns ReadOnly, NOT SubstrateMigrationTooLarge — the read-only carve-out
        // precedes the too-large preflight. (Name must not collide with the existing
        // test_read_only_open_of_redb_v2_store_* tests.)
        let dir = tempdir().unwrap();
        let path = dir.path().join("too_large_ro_v2.db");
        seed_redb_v2_store(&path, None);

        // Even with the floor forced to 0 (every store "too large"), read-only wins.
        let _floor = FloorOverrideGuard::set(0);

        let err = RedbStorage::open(&path, &Config::read_only()).unwrap_err();
        assert!(
            matches!(err, PulseDBError::ReadOnly),
            "read-only open of a too-large redb-v2 store must return ReadOnly (carve-out precedes preflight), got: {err}"
        );
        assert!(
            !pre_substrate_backup_path(&path).exists(),
            "read-only refusal writes nothing (no backup)"
        );
    }

    #[test]
    fn test_backup_once_fsyncs_sidecar_and_content_unchanged() {
        // #53c: backup_once durably copies the source to the sidecar (fsync'd) so the
        // rollback point survives a crash; the sidecar is a byte-identical copy.
        let dir = tempdir().unwrap();
        let src = dir.path().join("source.db");
        let sidecar = dir.path().join("source.db.pre-substrate.bak");
        let payload = vec![0xABu8; 64 * 1024];
        std::fs::write(&src, &payload).unwrap();

        assert_eq!(
            backup_once(&src, &sidecar).expect("backup_once succeeds and fsyncs"),
            BackupOutcome::Created
        );
        assert!(sidecar.exists(), "sidecar must exist after backup_once");
        assert_eq!(
            std::fs::read(&sidecar).unwrap(),
            payload,
            "the fsync'd sidecar must be a byte-identical copy of the source"
        );

        // Idempotent: a second call preserves the genuine sidecar (no-op).
        assert_eq!(
            backup_once(&src, &sidecar).expect("second backup_once is a no-op Ok"),
            BackupOutcome::Preserved
        );
        assert_eq!(std::fs::read(&sidecar).unwrap(), payload);
    }

    // ====================================================================
    // Decay-config goldens (§4.5 / audit C2) — direct legacy_bincode::decode of
    // BOTH StoredDecayConfig and StoredDecayConfigV1 from frozen oracle bytes.
    // ====================================================================

    #[test]
    fn test_decay_config_golden_decodes_current_shape() {
        // STORED_DECAY_CONFIG_GOLDEN: half_life 86400s + 500ns, freq 0.25,
        // floor 0.1, auto_archive true, weights Some(0.6, 0.4).
        let stored: StoredDecayConfig =
            legacy_bincode::decode(STORED_DECAY_CONFIG_GOLDEN).expect("StoredDecayConfig decodes");
        assert_eq!(stored.half_life_secs, 86_400);
        assert_eq!(stored.half_life_nanos, 500);
        assert_eq!(stored.freq_weight, 0.25);
        assert_eq!(stored.floor, 0.1);
        assert!(stored.auto_archive_below_floor);
        assert_eq!(
            stored.default_recall_weights,
            Some(RecallWeights::new(0.6, 0.4))
        );

        // Round-trips through the public DecayConfig conversion.
        let cfg: DecayConfig = stored.into();
        assert_eq!(cfg.half_life, std::time::Duration::new(86_400, 500));
    }

    #[test]
    fn test_decay_config_golden_decodes_v1_legacy_shape() {
        // STORED_DECAY_CONFIG_V1_GOLDEN: NO half_life_nanos field — half_life 3600s,
        // freq 0.5, floor 0.2, auto_archive false, weights None.
        let v1: StoredDecayConfigV1 = legacy_bincode::decode(STORED_DECAY_CONFIG_V1_GOLDEN)
            .expect("StoredDecayConfigV1 decodes");
        assert_eq!(v1.half_life_secs, 3_600);
        assert_eq!(v1.freq_weight, 0.5);
        assert_eq!(v1.floor, 0.2);
        assert!(!v1.auto_archive_below_floor);
        assert!(v1.default_recall_weights.is_none());

        // V1 lacks nanos ⇒ second-precision Duration after conversion.
        let cfg: DecayConfig = v1.into();
        assert_eq!(cfg.half_life, std::time::Duration::from_secs(3_600));

        // The two goldens have DISTINCT byte layouts (the nanos field): decoding the
        // V1 bytes as the current shape would mis-align ⇒ must NOT succeed cleanly.
        assert!(
            legacy_bincode::decode::<StoredDecayConfig>(STORED_DECAY_CONFIG_V1_GOLDEN).is_err(),
            "V1 bytes (no nanos) must not silently decode as the current StoredDecayConfig"
        );
    }

    // ========================================================================
    // #52 — postcard ⊥ legacy-bincode DISJOINTNESS guards
    // ========================================================================
    //
    // The on-open codec pass decodes every serde-blob row via
    // `decode_blob_legacy_or_postcard`, which tries `legacy_bincode::decode`
    // FIRST and only falls back to postcard on an `Err`. That ordering is safe
    // ONLY IF a postcard encoding is NEVER accepted by `legacy_bincode::decode`
    // — otherwise a postcard row (e.g. one a reshape migrator just rewrote in
    // the same txn) could be silently mis-dispatched through the bincode branch
    // and corrupted. These tests prove the two wire formats are DISJOINT in the
    // postcard→bincode direction for all 9 serde-blob shapes:
    //
    //   * a fast, deterministic regression FLOOR: representative / `*_GOLDEN`-
    //     anchored values, postcard-encoded, asserted `Err` under legacy bincode;
    //   * a `proptest!` BREADTH layer over random + boundary values (empty /
    //     max-length collections, `NaN` f32, unknown/added fields, truncated
    //     inputs) for the high-risk shapes.
    //
    // #57 review T3: the codec-only leg (no `.pre-substrate.bak`) must not be charged
    // the backup store-size. A store that clears the no-backup requirement but not the
    // backup-inclusive one PASSES makes_backup=false and FAILS makes_backup=true.
    #[test]
    fn test_codec_leg_headroom_excludes_backup_cost() {
        let cfg = default_config();
        let store_size: u64 = 100_000; // well below the single-txn floor (memory axis Ok)
        let backup_required = RedbStorage::required_migration_disk_bytes(store_size); // ~2.1×
        let no_backup_required = store_size + store_size / 10; // ~1.1×, matches the codec-leg branch
        assert!(
            no_backup_required < backup_required,
            "the no-backup requirement must be strictly smaller"
        );
        let available = (no_backup_required + backup_required) / 2; // between the two thresholds
                                                                    // Destructive path (writes a backup) fails closed at the disk axis:
        assert!(
            RedbStorage::resolve_migration_headroom(store_size, 0, available, &cfg, true).is_err(),
            "backup path must fail with only backup-excluding disk free"
        );
        // Codec-only leg (no backup) succeeds with the SAME available disk:
        assert!(
            RedbStorage::resolve_migration_headroom(store_size, 0, available, &cfg, false).is_ok(),
            "codec-only leg must not be charged for a backup it never writes"
        );
    }

    // #57 review T5: a build WITHOUT the `sync` feature must NOT silently migrate a
    // store that carries `sync_cursors` rows (a prior sync build's data) — doing so
    // would strand those bincode rows under a postcard marker (silent corruption a
    // later sync build hits). It must fail closed with SubstrateMigrationRequiresSync
    // and leave the marker unchanged (the store stays re-migratable by a sync build).
    #[cfg(not(feature = "sync"))]
    #[test]
    fn test_non_sync_build_refuses_migration_when_sync_cursors_present() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("older_with_sync_cursors.redb");
        let _ = seed_redb_v3_bincode_older_store(&path); // {redb-v3, bincode, marker Older(1)}

        // Inject a raw sync_cursors row the way a prior sync build would, without
        // needing the `sync` feature here (probe table = same name + types).
        {
            const SYNC_CURSORS_PROBE: ::redb::TableDefinition<&[u8; 16], &[u8]> =
                ::redb::TableDefinition::new("sync_cursors");
            let db = Database::builder().create(&path).unwrap();
            let wtx = db.begin_write().unwrap();
            {
                let mut t = wtx.open_table(SYNC_CURSORS_PROBE).unwrap();
                t.insert(&[0x99u8; 16], [1u8, 2, 3].as_slice()).unwrap();
            }
            wtx.commit().unwrap();
        }

        let err = RedbStorage::open(&path, &default_config()).unwrap_err();
        assert!(
            matches!(
                err,
                PulseDBError::Storage(StorageError::SubstrateMigrationRequiresSync)
            ),
            "a non-sync build must fail closed on sync_cursors rows; got: {err}"
        );

        // The refused migration's write-txn rolled back → marker still Older(1).
        let db = Database::builder().create(&path).unwrap();
        assert_eq!(
            RedbStorage::read_substrate_marker(&db).unwrap(),
            SubstrateFormat::Older(1),
            "a refused migration must leave the substrate marker untouched"
        );
    }

    // Naming note: the module and its assertions use the token `disjoint`/
    // `postcard.*reject` so the codec-correctness regression grep guards bind.
    mod postcard_bincode_disjointness {
        use super::*;
        use crate::config::DecayConfig;
        use crate::insight::InsightType;
        use proptest::prelude::*;

        // ---- ground-truth value builders for the 4 shapes WITHOUT a frozen
        // golden (db_metadata / decay_config / activity / sync_cursor). The other
        // five (collective / experience / relation / insight / watch_event) reuse
        // the parent module's `*_GOLDEN` bytes decoded back to real values. ----

        fn sample_db_metadata() -> DatabaseMetadata {
            DatabaseMetadata::new(EmbeddingDimension::D384)
        }

        fn sample_decay_config() -> StoredDecayConfig {
            StoredDecayConfig::from(&DecayConfig::default())
        }

        fn sample_activity() -> Activity {
            Activity {
                agent_id: "agent-disjoint".into(),
                collective_id: CollectiveId::from_bytes([0x33; 16]),
                current_task: Some("proving disjointness".into()),
                context_summary: None,
                started_at: Timestamp::from_millis(1_700_000_000_000),
                last_heartbeat: Timestamp::from_millis(1_700_000_000_500),
            }
        }

        #[cfg(feature = "sync")]
        fn sample_sync_cursor() -> crate::sync::SyncCursor {
            crate::sync::SyncCursor {
                instance_id: InstanceId::from_bytes([0x44; 16]),
                last_sequence: 42,
            }
        }

        // (4.04 sanctioned cleanup: the previously-present `sample_relation()` and
        // `sample_insight()` test helpers were UNUSED under --all-features — both
        // emitted `never used` warnings and neither was referenced — so they are
        // removed here. The 5 golden-bearing shapes anchor via the `*_GOLDEN` bytes;
        // relation/insight disjointness is covered by the golden-anchored path.)

        /// The core disjointness predicate: `legacy_bincode::decode::<T>` MUST
        /// REJECT (return `Err`) the postcard encoding of `value`. A `true`
        /// result means the two codecs are disjoint for this value.
        fn postcard_is_rejected_by_legacy_bincode<T>(value: &T) -> bool
        where
            T: Serialize + serde::de::DeserializeOwned,
        {
            let postcard_bytes = postcard::to_stdvec(value).expect("postcard encodes");
            legacy_bincode::decode::<T>(&postcard_bytes).is_err()
        }

        // --------------------------------------------------------------------
        // Regression FLOOR: representative-value / `*_GOLDEN`-anchored assertions
        // for ALL 9 serde-blob shapes. For the 5 golden-bearing shapes we decode
        // the frozen legacy bytes back to the real value, then re-encode it with
        // postcard and assert legacy bincode rejects it — anchoring the property
        // to the exact shapes captured on disk.
        // --------------------------------------------------------------------

        #[test]
        fn golden_anchored_postcard_rejected_all_nine_shapes() {
            // db_metadata
            assert!(
                postcard_is_rejected_by_legacy_bincode(&sample_db_metadata()),
                "db_metadata: postcard encoding must be rejected (disjoint) by legacy bincode"
            );
            // collective (golden-anchored)
            let collective: Collective =
                legacy_bincode::decode(COLLECTIVE_GOLDEN).expect("collective golden decodes");
            assert!(
                postcard_is_rejected_by_legacy_bincode(&collective),
                "collective: postcard must be rejected (disjoint) by legacy bincode"
            );
            // decay_config
            assert!(
                postcard_is_rejected_by_legacy_bincode(&sample_decay_config()),
                "decay_config: postcard must be rejected (disjoint) by legacy bincode"
            );
            // experience (golden-anchored)
            let experience: Experience =
                legacy_bincode::decode(EXPERIENCE_GOLDEN).expect("experience golden decodes");
            assert!(
                postcard_is_rejected_by_legacy_bincode(&experience),
                "experience: postcard must be rejected (disjoint) by legacy bincode"
            );
            // relation (golden-anchored)
            let relation: ExperienceRelation =
                legacy_bincode::decode(RELATION_GOLDEN).expect("relation golden decodes");
            assert!(
                postcard_is_rejected_by_legacy_bincode(&relation),
                "relation: postcard must be rejected (disjoint) by legacy bincode"
            );
            // insight (golden-anchored)
            let insight: DerivedInsight =
                legacy_bincode::decode(INSIGHT_GOLDEN).expect("insight golden decodes");
            assert!(
                postcard_is_rejected_by_legacy_bincode(&insight),
                "insight: postcard must be rejected (disjoint) by legacy bincode"
            );
            // activity
            assert!(
                postcard_is_rejected_by_legacy_bincode(&sample_activity()),
                "activity: postcard must be rejected (disjoint) by legacy bincode"
            );
            // watch_event (golden-anchored)
            let watch: WatchEventRecord =
                legacy_bincode::decode(WATCH_EVENT_GOLDEN).expect("watch_event golden decodes");
            assert!(
                postcard_is_rejected_by_legacy_bincode(&watch),
                "watch_event: postcard must be rejected (disjoint) by legacy bincode"
            );
            // sync_cursor (feature-gated)
            #[cfg(feature = "sync")]
            assert!(
                postcard_is_rejected_by_legacy_bincode(&sample_sync_cursor()),
                "sync_cursor: postcard must be rejected (disjoint) by legacy bincode"
            );
        }

        // --------------------------------------------------------------------
        // BREADTH layer: proptest over random + boundary values for the
        // high-risk shapes. Bounded case count (mirrors rerank.rs:206) keeps
        // `cargo test` fast. Each generated value's postcard encoding is asserted
        // `Err` under legacy bincode — proving disjointness over a value space,
        // not one lucky byte pattern. Boundary cases exercised: empty / max-length
        // collections and strings, NaN f32 payloads, and (via truncation +
        // trailing-byte tests below) unknown/added-field and truncated inputs.
        // --------------------------------------------------------------------

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(24))]

            // Experience: content string 0..=256 (empty & max-length), NaN-capable
            // embedding, applications map 0..8, domain 0..8. The postcard encoding
            // of every generated Experience must be rejected by legacy bincode.
            #[test]
            fn proptest_experience_postcard_disjoint(
                content in ".{0,256}",
                emb in prop::collection::vec(prop::num::f32::ANY, 0..64),
                app_count in 0usize..8,
                domain in prop::collection::vec("[a-z]{0,16}", 0..8),
                importance in prop::num::f32::ANY,
            ) {
                let mut applications: BTreeMap<InstanceId, u32> = BTreeMap::new();
                for i in 0..app_count {
                    applications.insert(InstanceId::from_bytes([i as u8; 16]), i as u32);
                }
                let exp = Experience {
                    id: ExperienceId::from_bytes([0x21; 16]),
                    collective_id: CollectiveId::from_bytes([0x11; 16]),
                    content,
                    embedding: emb,
                    experience_type: crate::experience::ExperienceType::Fact {
                        statement: "s".into(),
                        source: "d".into(),
                    },
                    importance,
                    confidence: 0.25,
                    applications,
                    domain,
                    related_files: Vec::new(),
                    source_agent: AgentId::new("agent"),
                    source_task: None,
                    timestamp: Timestamp::from_millis(111),
                    last_reinforced: Timestamp::from_millis(222),
                    archived: false,
                };
                prop_assert!(
                    postcard_is_rejected_by_legacy_bincode(&exp),
                    "experience proptest value round-tripped through legacy bincode — NOT disjoint"
                );
            }

            // Insight: NaN-capable embedding + confidence, variable source-id and
            // domain vectors (empty & long), content 0..=256.
            #[test]
            fn proptest_insight_postcard_disjoint(
                content in ".{0,256}",
                emb in prop::collection::vec(prop::num::f32::ANY, 0..64),
                src_count in 0usize..8,
                confidence in prop::num::f32::ANY,
            ) {
                let source_experience_ids = (0..src_count)
                    .map(|i| ExperienceId::from_bytes([i as u8; 16]))
                    .collect();
                let insight = DerivedInsight {
                    id: InsightId::from_bytes([0x66; 16]),
                    collective_id: CollectiveId::from_bytes([0x11; 16]),
                    content,
                    embedding: emb,
                    source_experience_ids,
                    insight_type: InsightType::Synthesis,
                    confidence,
                    domain: Vec::new(),
                    created_at: Timestamp::from_millis(1),
                    updated_at: Timestamp::from_millis(2),
                };
                prop_assert!(
                    postcard_is_rejected_by_legacy_bincode(&insight),
                    "insight proptest value round-tripped through legacy bincode — NOT disjoint"
                );
            }

            // Relation: metadata None/Some (empty & long JSON-ish string), each
            // of the 6 relation-type discriminants, NaN-capable strength.
            #[test]
            fn proptest_relation_postcard_disjoint(
                has_meta in any::<bool>(),
                meta in ".{0,256}",
                type_idx in 0usize..6,
                strength in prop::num::f32::ANY,
            ) {
                let relation_type = [
                    RelationType::Supports,
                    RelationType::Contradicts,
                    RelationType::Elaborates,
                    RelationType::Supersedes,
                    RelationType::Implies,
                    RelationType::RelatedTo,
                ][type_idx];
                let rel = ExperienceRelation {
                    id: RelationId::from_bytes([0x55; 16]),
                    source_id: ExperienceId::from_bytes([0x21; 16]),
                    target_id: ExperienceId::from_bytes([0x22; 16]),
                    relation_type,
                    strength,
                    metadata: if has_meta { Some(meta) } else { None },
                    created_at: Timestamp::from_millis(1),
                };
                prop_assert!(
                    postcard_is_rejected_by_legacy_bincode(&rel),
                    "relation proptest value round-tripped through legacy bincode — NOT disjoint"
                );
            }
        }

        // --------------------------------------------------------------------
        // Boundary-case anchors that are awkward to express as strategies:
        // NaN f32, empty & max-length collections, and unknown/added-field /
        // truncated inputs. These assert the disjointness property holds at the
        // exact edges the migration cares about.
        // --------------------------------------------------------------------

        #[test]
        fn disjoint_at_nan_and_empty_and_maxlen_boundaries() {
            // NaN f32 embedding + NaN importance.
            let mut exp = oracle_experience();
            exp.embedding = vec![f32::NAN, f32::INFINITY, f32::NEG_INFINITY];
            exp.importance = f32::NAN;
            assert!(
                postcard_is_rejected_by_legacy_bincode(&exp),
                "NaN-bearing experience: postcard must remain disjoint from legacy bincode"
            );

            // Empty collections everywhere.
            let empty = Experience {
                id: ExperienceId::from_bytes([0; 16]),
                collective_id: CollectiveId::from_bytes([0; 16]),
                content: String::new(),
                embedding: Vec::new(),
                experience_type: crate::experience::ExperienceType::Fact {
                    statement: String::new(),
                    source: String::new(),
                },
                importance: 0.0,
                confidence: 0.0,
                applications: BTreeMap::new(),
                domain: Vec::new(),
                related_files: Vec::new(),
                source_agent: AgentId::new(""),
                source_task: None,
                timestamp: Timestamp::from_millis(0),
                last_reinforced: Timestamp::from_millis(0),
                archived: false,
            };
            assert!(
                postcard_is_rejected_by_legacy_bincode(&empty),
                "empty-collection experience: postcard must remain disjoint from legacy bincode"
            );

            // Max-length-ish: a long content string + large embedding.
            let mut big = oracle_experience();
            big.content = "x".repeat(4096);
            big.embedding = vec![0.5f32; 384];
            big.domain = (0..64).map(|i| format!("tag{i}")).collect();
            assert!(
                postcard_is_rejected_by_legacy_bincode(&big),
                "max-length experience: postcard must remain disjoint from legacy bincode"
            );
        }

        #[test]
        fn disjoint_under_truncated_and_trailing_byte_perturbations() {
            // A postcard encoding with trailing/unknown extra bytes appended
            // (an "added field" from a future schema) must still be rejected.
            let collective: Collective =
                legacy_bincode::decode(COLLECTIVE_GOLDEN).expect("collective golden decodes");
            let mut extended = postcard::to_stdvec(&collective).expect("postcard encodes");
            extended.extend_from_slice(&[0xAA, 0xBB, 0xCC, 0xDD]);
            assert!(
                legacy_bincode::decode::<Collective>(&extended).is_err(),
                "postcard + trailing unknown bytes must be rejected by legacy bincode"
            );

            // A truncated postcard encoding must be rejected (never a clean decode).
            let full = postcard::to_stdvec(&collective).expect("postcard encodes");
            if full.len() > 2 {
                let truncated = &full[..full.len() - 2];
                assert!(
                    legacy_bincode::decode::<Collective>(truncated).is_err(),
                    "truncated postcard bytes must be rejected by legacy bincode"
                );
            }
        }
    }

    // ========================================================================
    // #55 — re-encode exhaustiveness / independent schema audit
    // ========================================================================
    //
    // The `SERDE_BLOB_TABLES` registry is the single source that
    // `reencode_serde_blobs_to_postcard` iterates/asserts against (see the
    // `visited_labels` coverage assertion in that fn). The guard below proves
    // that registry is COMPLETE — that every serde-blob table in the real schema
    // is registered — WITHOUT using the registry as its own oracle. It derives
    // the serde-blob table set INDEPENDENTLY by enumerating every `*_TABLE`
    // constant in `schema.rs` and partitioning it into copy-through vs serde-blob.
    //
    // Token note: this module is named with `schema_audit` and its assertions use
    // `exhaustiv`/`reencode.*registry` so the regression grep guards bind. The
    // negative (falsification) test uses `#[should_panic]` + a `fake_serde_blob`
    // descriptor so a genuinely-missing table is proven to FAIL the audit.
    mod serde_blob_schema_audit {
        use super::*;
        use redb::{MultimapTableHandle, TableHandle};

        /// The audit's INDEPENDENT enumeration of every `*_TABLE` constant defined
        /// in `schema.rs`, tagged copy-through (never decoded) vs serde-blob. This
        /// list is authored against `schema.rs` — NOT derived from
        /// `SERDE_BLOB_TABLES` — so a table added to `schema.rs` but omitted from
        /// the registry is caught, and drift between this list and the real schema
        /// is caught by the count assertion below.
        ///
        /// Each entry is `(redb table name, is_serde_blob)`.
        fn schema_audit_tables() -> Vec<(&'static str, bool)> {
            // `mut` is used only under `sync` (the `sync_cursors` push below); the
            // default-feature build leaves it unmutated, so silence unused_mut there.
            #[cfg_attr(not(feature = "sync"), allow(unused_mut))]
            let mut tables: Vec<(&'static str, bool)> = vec![
                // --- serde-blob tables (decoded ⇒ must be in SERDE_BLOB_TABLES) ---
                (METADATA_TABLE.name(), true),
                (COLLECTIVES_TABLE.name(), true),
                (DECAY_CONFIGS_TABLE.name(), true),
                (EXPERIENCES_TABLE.name(), true),
                (RELATIONS_TABLE.name(), true),
                (INSIGHTS_TABLE.name(), true),
                (ACTIVITIES_TABLE.name(), true),
                (WATCH_EVENTS_TABLE.name(), true),
                // --- copy-through tables (raw bytes ⇒ never decoded, excluded) ---
                (EMBEDDINGS_TABLE.name(), false),
                (EXPERIENCES_BY_COLLECTIVE_TABLE.name(), false),
                (EXPERIENCES_BY_TYPE_TABLE.name(), false),
                (RELATIONS_BY_SOURCE_TABLE.name(), false),
                (RELATIONS_BY_TARGET_TABLE.name(), false),
                (INSIGHTS_BY_COLLECTIVE_TABLE.name(), false),
            ];
            // sync_cursors is a serde-blob table but only compiled under `sync`;
            // gate its enumeration the same way schema.rs gates the const.
            #[cfg(feature = "sync")]
            tables.push((SYNC_CURSORS_TABLE.name(), true));
            tables
        }

        /// The number of `*_TABLE` constants the audit expects to enumerate. This
        /// is the anti-drift anchor: if `schema.rs` gains or loses a `*_TABLE`
        /// const and this audit's list is not updated, the count assertion below
        /// fails. 14 tables without `sync`, 15 with (the +`sync_cursors`).
        const fn expected_table_count() -> usize {
            if cfg!(feature = "sync") {
                15
            } else {
                14
            }
        }

        /// Runs the audit against an arbitrary table list: every serde-blob table
        /// must appear in `SERDE_BLOB_TABLES`. Returns `Err(name)` for the first
        /// serde-blob table missing from the registry (fails closed). Kept generic
        /// over the table list so the falsification test can feed it a fake table.
        fn audit_serde_blob_coverage(tables: &[(&str, bool)]) -> std::result::Result<(), String> {
            for (name, is_serde_blob) in tables {
                if *is_serde_blob {
                    let registered = SERDE_BLOB_TABLES
                        .iter()
                        .any(|(reg_name, _)| reg_name == name);
                    if !registered {
                        return Err((*name).to_string());
                    }
                }
            }
            Ok(())
        }

        /// #55 exhaustiveness: every serde-blob table the independent schema audit
        /// finds must be covered by the `SERDE_BLOB_TABLES` registry that drives
        /// `reencode_serde_blobs_to_postcard`. Because the audit's table set is
        /// derived from `schema.rs` (not from the registry), a serde-blob table
        /// added to `schema.rs` but not to the registry FAILS here.
        #[test]
        fn every_schema_serde_blob_table_is_in_reencode_registry() {
            let tables = schema_audit_tables();

            // Anti-drift: the audit's own enumeration must match the real number of
            // `*_TABLE` consts in schema.rs, so the audit cannot silently fall
            // behind the schema (e.g. a new table added but not enumerated here).
            assert_eq!(
                tables.len(),
                expected_table_count(),
                "schema audit enumeration ({}) drifted from the real *_TABLE count ({}) — \
                 add the new schema.rs table to schema_audit_tables()",
                tables.len(),
                expected_table_count(),
            );

            // Exhaustiveness: independent serde-blob set ⊆ registry.
            audit_serde_blob_coverage(&tables)
                .expect("every serde-blob table from schema.rs must be in SERDE_BLOB_TABLES");

            // And the registry has no MORE serde-blob tables than the schema audit
            // knows about (registry ⊆ audit's serde-blob set) — a two-way check so
            // the registry cannot list a phantom table either.
            let audit_serde_blob: Vec<&str> = tables
                .iter()
                .filter(|(_, blob)| *blob)
                .map(|(name, _)| *name)
                .collect();
            for (reg_name, _what) in SERDE_BLOB_TABLES {
                // sync_cursors is only in the audit set under `sync`; skip it when
                // the feature is off (the registry always lists it as a const).
                #[cfg(not(feature = "sync"))]
                if *reg_name == "sync_cursors" {
                    continue;
                }
                assert!(
                    audit_serde_blob.contains(reg_name),
                    "registry lists '{reg_name}' but the independent schema audit does not \
                     classify it as a serde-blob table",
                );
            }
        }

        /// C10 falsification: the audit must FAIL CLOSED. A deliberately-fake
        /// serde-blob table descriptor whose name is absent from
        /// `SERDE_BLOB_TABLES` must be REJECTED — proving the guard actually bites
        /// rather than passing vacuously.
        #[test]
        fn audit_rejects_fake_unregistered_serde_blob_table() {
            let fake_serde_blob = ("definitely_not_a_real_table", true);
            let result = audit_serde_blob_coverage(&[fake_serde_blob]);
            assert!(
                result.is_err(),
                "a fake serde-blob table absent from SERDE_BLOB_TABLES must FAIL the audit"
            );
            assert_eq!(result.unwrap_err(), "definitely_not_a_real_table");
        }

        /// C10 falsification, panic form: the same fake-table rejection expressed
        /// as a `#[should_panic]` test so the guard's fail-closed behavior is
        /// asserted through the panic path the exhaustiveness test uses in prod.
        #[test]
        #[should_panic(expected = "fake serde-blob table")]
        fn audit_of_fake_table_panics_when_unwrapped() {
            let fake_serde_blob = ("phantom_table_audit_fake", true);
            audit_serde_blob_coverage(&[fake_serde_blob])
                .expect("fake serde-blob table must be rejected");
        }
    }
}
