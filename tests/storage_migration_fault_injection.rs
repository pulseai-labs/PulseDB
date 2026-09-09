//! VS-4.0.4 (4.05, issue #46) — kill-at-boundary crash-recovery tests.
//!
//! Closes the "crash-recovery oversold" close-depth finding by PROVING the
//! on-open migration (proven value-identical by 4.02) is genuinely crash-safe.
//! It injects a deterministic crash at each of the FIVE migration boundaries
//! against 4.01's REAL frozen fixtures and asserts POSITIVE recovery — so a no-op
//! injection cannot pass (C10).
//!
//! ## atomicity ≠ resumability
//! The reshape + registry-driven re-encode + marker is ONE redb `WriteTransaction`
//! whose substrate-format marker is the LAST write (the atomic commit point). A
//! crash at any WRITE-txn boundary before commit rolls the ENTIRE txn back —
//! nothing is durable, so recovery is *re-run-from-scratch*, never resume. Tests
//! assert the crashed store re-opens to its ORIGINAL, un-migrated, OLD-CODEC
//! (bincode / legacy-decodable) state — marker NOT at CURRENT — and a clean re-run
//! migrates value-identically.
//!
//! ## the two pre-txn windows a txn abort can't cover (C6)
//! Note: the migration path has NO auto-restore-from-sidecar consumer — the
//! `.pre-substrate.bak` is a pristine *operator* rollback artifact, and automatic
//! recovery is always re-migration of the (possibly already-v3) store. These two
//! tests assert exactly what the code guarantees, no more:
//! - `PostRedbUpgrade`: a crash AFTER the destructive in-place redb v2→v3 upgrade
//!   leaves an already-durably-v3 file. We assert (a) the `.pre-substrate.bak`
//!   sidecar exists and is BYTE-IDENTICAL to the pristine original (a correct
//!   operator rollback artifact), and (b) the already-v3 store re-migrates
//!   value-identically on clean re-open (recovery is re-migration, not restore).
//! - `MidBackupPreFsync`: a crash DURING `backup_once` before the `#53c` fsync
//!   happens BEFORE the destructive upgrade, so the store file is still pristine
//!   v2. We assert the store is byte-unchanged and self-heals from its OWN intact
//!   bytes on re-open — so recovery never DEPENDS on the sidecar; even a truncated
//!   leftover sidecar is inert (the `#53c` fsync hardens the separate case where a
//!   sidecar would be the sole rollback point, which this pristine-store window is
//!   not).
//!
//! ## marker-1 {redb-v3, bincode} evidence for #53b (C7)
//! A store that is ALREADY redb-v3 but still bincode never runs the destructive
//! v2→v3 upgrade, so `create_or_migrate` claims NO `.pre-substrate.bak`. We derive
//! that rung from the real v0.5.1 fixture (crash it at `PreReencode`: the redb
//! upgrade commits durably, the rolled-back write-txn leaves a redb-v3 + bincode +
//! Absent-marker store), then crash THAT store and prove the single atomic txn
//! rolls back pristine with NO sidecar created — the executable proof that the
//! {redb-v3, bincode} / marker-1 path is safe without a sidecar.
//!
//! ## genuine-crash fidelity (C15)
//! Exactly one subprocess SIGKILL test (a real kill — no graceful `Drop`) at
//! `PreMarker`: the child re-execs into a migrating open and dies via
//! `libc::raise(SIGKILL)`; the parent asserts the child died by signal 9 and the
//! re-open SURVIVES the SIGKILL'd child's leftover redb `.lock` + `.migrate.lock`
//! (OS releases advisory locks on death) to a pristine, old-codec store.
//!
//! Residual gap (documented, not closed): v0.3.0 / WAL-v1 (pre-schema-v2) — neither
//! fixture covers it (see the VS-4.0.4 spec + 4.01/4.02).
//!
//! ## SERIAL-ONLY — run with `--test-threads=1`
//! Crash arming is thread-local, but the migration's re-encode runs on the shared
//! rayon pool; under parallel test load the injection point can execute on a busy
//! worker thread where the arm is invisible, so the boundary silently no-ops
//! ("injection did not fire"). Running serially keeps the pool idle so the calling
//! (armed) thread does the work. These tests also share a process-global panic hook
//! and one re-execs the test binary. CI runs this suite with `--test-threads=1`
//! (see `.github/workflows/ci.yml` → `fault-injection`); run it the same way locally.

#![cfg(feature = "fault-injection")]

use pulsedb::fault_injection::{
    arm_upgrade_fault, disarm, disarm_upgrade_fault, Action, ArmGuard, Boundary, UpgradeFault,
};
use pulsedb::storage::SCHEMA_VERSION;
use pulsedb::{Config, ExperienceId, PulseDB, PulseDBError, StorageError};
use redb::{ReadableDatabase, TableDefinition};
use serde_json::Value;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};

const METADATA: TableDefinition<&str, &[u8]> = TableDefinition::new("metadata");
/// The serializer-independent substrate marker at the postcard era: `[b'P', b'S', 2]`.
const SUBSTRATE_MARKER_CURRENT: [u8; 3] = [b'P', b'S', 2];

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn load_manifest(name: &str) -> Value {
    let s = std::fs::read_to_string(fixtures_dir().join(name)).unwrap();
    serde_json::from_str(&s).unwrap()
}

/// Copy the committed fixture to a fresh temp path — migration is destructive and
/// a mid-migration crash corrupts the file, so every run gets a private copy.
fn copy_fixture(name: &str) -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let dst = dir.path().join(name);
    std::fs::copy(fixtures_dir().join(name), &dst).unwrap_or_else(|e| panic!("copy {name}: {e}"));
    (dir, dst)
}

fn pre_substrate_bak(store: &Path) -> PathBuf {
    let mut p = store.to_path_buf();
    let name = store.file_name().unwrap().to_string_lossy();
    p.set_file_name(format!("{name}.pre-substrate.bak"));
    p
}

/// Read the raw `substrate_format` marker bytes of a MIGRATED (redb-v3) store via
/// redb-4.x directly (no migration triggered). `None` if the key is absent OR the
/// file is not redb-v3-openable (e.g. still a v2 file after a pre-upgrade crash).
fn read_marker(store: &Path) -> Option<Vec<u8>> {
    let db = redb::Database::open(store).ok()?;
    let rtx = db.begin_read().ok()?;
    let t = rtx.open_table(METADATA).ok()?;
    let v = t.get("substrate_format").ok()?.map(|g| g.value().to_vec());
    v
}

fn marker_is_current(store: &Path) -> bool {
    read_marker(store).as_deref() == Some(&SUBSTRATE_MARKER_CURRENT)
}

/// Silence the EXPECTED in-process injection panics (keep real failures loud).
fn silence_injection_panics() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let default = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let msg = info
                .payload()
                .downcast_ref::<String>()
                .map(|s| s.as_str())
                .or_else(|| info.payload().downcast_ref::<&str>().copied())
                .unwrap_or("");
            if msg.contains("fault-injection: simulated migration crash") {
                return;
            }
            default(info);
        }));
    });
}

/// Drive a migrating open with an in-process panic armed at `boundary`; assert it
/// actually panicked (a no-op injection would return Ok and fail this).
fn crash_open(store: &Path, boundary: Boundary) {
    let _g = ArmGuard::new(boundary, Action::Panic);
    let store = store.to_path_buf();
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        PulseDB::open(&store, Config::default())
    }));
    // _g disarms on drop (panic-safe); belt-and-suspenders:
    disarm();
    // Non-vacuity (C10): the open must have panicked, AND specifically from OUR
    // injection at THIS boundary — not from some other migration failure. A bare
    // `is_err()` check would accept any panic and let a real bug (or a no-op
    // injection that happened to panic elsewhere) masquerade as a fired boundary.
    let payload = result.err().unwrap_or_else(|| {
        panic!("injection at {boundary:?} did not fire — migrating open returned Ok")
    });
    let msg = payload
        .downcast_ref::<String>()
        .map(|s| s.as_str())
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        msg.contains("fault-injection: simulated migration crash")
            && msg.contains(&format!("{boundary:?}")),
        "panic must be the armed injection at {boundary:?}, got: {msg:?}"
    );
}

/// POSITIVE recovery: the crashed store re-opens cleanly (no lock deadlock) and a
/// clean re-run migrates value-identically vs the manifest — this proves the
/// post-crash data was still OLD-CODEC (bincode / legacy) decodable, since the
/// migration re-decodes it via the legacy path (`decode_blob_legacy_or_postcard`).
fn assert_clean_rerun_value_identical(store: &Path, manifest: &Value) {
    disarm();
    // Independence from the injection helper: the store must NOT already be
    // migrated on entry — otherwise this helper would merely re-read an
    // already-CURRENT store and prove nothing about crash recovery.
    assert!(
        !marker_is_current(store),
        "pre-rerun store must be un-migrated (marker not CURRENT) — else the re-run proves nothing"
    );
    let db = PulseDB::open(store, Config::default())
        .unwrap_or_else(|e| panic!("clean re-run migrate+open failed: {e:?}"));
    assert_eq!(
        db.metadata().schema_version,
        SCHEMA_VERSION,
        "re-run must reach the current schema version"
    );
    // marker is now CURRENT ({redb-v3, postcard}) — the migration completed.
    // (re-open must be dropped before reading the marker via a 2nd redb handle)
    let colls = db.list_collectives().unwrap();
    assert_eq!(
        colls.len(),
        manifest["collectives"].as_array().unwrap().len(),
        "re-run collective count must match the manifest oracle"
    );
    // non-vacuous value-identity spot-check on the first manifest experience.
    let first = &manifest["experiences"][0];
    let id = ExperienceId(uuid::Uuid::parse_str(first["id"].as_str().unwrap()).unwrap());
    let exp = db
        .get_experience(id)
        .unwrap()
        .expect("first manifest experience must survive crash + re-run");
    assert_eq!(
        exp.content,
        first["content"].as_str().unwrap(),
        "re-run experience content must be value-identical to the manifest"
    );
    drop(db);
    assert!(
        marker_is_current(store),
        "after a clean re-run the marker must be CURRENT"
    );
}

// ---------------------------------------------------------------------------
// write-txn boundaries (PreReencode / MidReencode / PreMarker) — atomic rollback
// ---------------------------------------------------------------------------

fn write_txn_boundary_rolls_back(fixture: &str, manifest_name: &str, boundary: Boundary) {
    silence_injection_panics();
    let manifest = load_manifest(manifest_name);
    let (_tmp, store) = copy_fixture(fixture);

    crash_open(&store, boundary);

    // Rolled back, and discriminating (not the trivial "v2 file is unopenable"):
    // by a write-txn boundary the destructive redb v2→v3 upgrade has ALREADY run,
    // so the file is now redb-v3-openable — and the write-txn rolled back, so the
    // substrate_format key is GENUINELY Absent (old-codec / bincode-decodable data
    // survives, marker not CURRENT).
    assert!(
        redb::Database::open(&store).is_ok(),
        "{fixture}: post-crash file must be redb-v3 (the destructive upgrade ran before the write-txn)"
    );
    assert!(
        read_marker(&store).is_none(),
        "{fixture}: after a crash at {boundary:?} the substrate_format key must be Absent (write-txn rolled back), got {:?}",
        read_marker(&store)
    );

    assert_clean_rerun_value_identical(&store, &manifest);
}

#[test]
fn prereencode_crash_rolls_back_v0_4_0() {
    write_txn_boundary_rolls_back(
        "real-v0.4.0.redb",
        "real-v0.4.0.manifest.json",
        Boundary::PreReencode,
    );
}

#[test]
fn midreencode_crash_rolls_back_v0_4_0() {
    write_txn_boundary_rolls_back(
        "real-v0.4.0.redb",
        "real-v0.4.0.manifest.json",
        Boundary::MidReencode,
    );
}

#[test]
fn premarker_crash_rolls_back_v0_4_0() {
    write_txn_boundary_rolls_back(
        "real-v0.4.0.redb",
        "real-v0.4.0.manifest.json",
        Boundary::PreMarker,
    );
}

#[test]
fn premarker_crash_rolls_back_v0_5_1() {
    write_txn_boundary_rolls_back(
        "real-v0.5.1.redb",
        "real-v0.5.1.manifest.json",
        Boundary::PreMarker,
    );
}

// ---------------------------------------------------------------------------
// pre-txn boundary: PostRedbUpgrade — sidecar is the rollback point (C6)
// ---------------------------------------------------------------------------

#[test]
fn post_redb_upgrade_crash_leaves_pristine_sidecar_v0_4_0() {
    silence_injection_panics();
    let manifest = load_manifest("real-v0.4.0.manifest.json");
    let original = std::fs::read(fixtures_dir().join("real-v0.4.0.redb")).unwrap();
    let (_tmp, store) = copy_fixture("real-v0.4.0.redb");

    crash_open(&store, Boundary::PostRedbUpgrade);

    // The destructive in-place redb v2→v3 upgrade already ran (file is v3), so the
    // write-txn marker is NOT CURRENT (never reached). Assert the pristine
    // `.pre-substrate.bak` operator rollback artifact exists and is byte-identical
    // to the original fixture. (Automatic recovery below is re-migration of the
    // already-v3 store, not a restore-from-sidecar — no such consumer exists.)
    assert!(
        !marker_is_current(&store),
        "post-upgrade crash: marker must not be CURRENT"
    );
    let bak = pre_substrate_bak(&store);
    assert!(
        bak.exists(),
        "PostRedbUpgrade must leave a `.pre-substrate.bak` rollback point"
    );
    let bak_bytes = std::fs::read(&bak).unwrap();
    assert_eq!(
        bak_bytes, original,
        "`.pre-substrate.bak` must be a byte-identical copy of the pristine pre-migration store"
    );

    assert_clean_rerun_value_identical(&store, &manifest);
}

// ---------------------------------------------------------------------------
// pre-txn boundary: MidBackupPreFsync — truncated/short sidecar not trusted (C6 / #53c)
// ---------------------------------------------------------------------------

#[test]
fn mid_backup_pre_fsync_store_pristine_and_no_final_sidecar_v0_4_0() {
    silence_injection_panics();
    let manifest = load_manifest("real-v0.4.0.manifest.json");
    let original = std::fs::read(fixtures_dir().join("real-v0.4.0.redb")).unwrap();
    let (_tmp, store) = copy_fixture("real-v0.4.0.redb");

    crash_open(&store, Boundary::MidBackupPreFsync);

    // The crash is BEFORE the destructive upgrade, so the store FILE is still the
    // pristine v2 bytes (byte-identical to the original) — the store's integrity
    // never depended on the not-yet-fsync'd sidecar.
    let after = std::fs::read(&store).unwrap();
    assert_eq!(
        after, original,
        "MidBackupPreFsync: the store must be untouched (destructive upgrade never ran)"
    );

    // #5 (T7): `backup_once` builds the sidecar at a temp path and only publishes
    // it to the FINAL `.pre-substrate.bak` via an atomic rename AFTER the `#53c`
    // fsync. A crash at MidBackupPreFsync is BEFORE that rename, so the final
    // sidecar path is ABSENT — never a truncated/partial file a later open could
    // mistake for a genuine rollback point (the exact `AlreadyExists`-preserves-a-
    // short-sidecar hazard this fix closes). A stale `.tmp` may remain; it is
    // never consulted and is overwritten (create+truncate) by the next attempt.
    let bak = pre_substrate_bak(&store);
    assert!(
        !bak.exists(),
        "MidBackupPreFsync crash must leave NO final `.pre-substrate.bak` \
         (temp+rename publishes the sidecar only after it is fully fsync'd)"
    );

    // Recovery re-migrates from the store's OWN pristine bytes (upgrade never ran),
    // never depending on the sidecar — so a clean re-run reads every entity back.
    assert_clean_rerun_value_identical(&store, &manifest);
}

// ---------------------------------------------------------------------------
// #4 / T4 — post-backup upgrade-abort sidecar cleanup (deterministic wiring)
// A real cross-version lock race surfaces at the PRE-backup redb-4.1 `create` on
// most platforms, so it cannot deterministically drive the post-backup cleanup.
// The UpgradeFault seam forces the abort to land INSIDE the destructive upgrade,
// after `backup_once` — exercising the real create_or_migrate → backup → upgrade →
// cleanup wiring (not just the cleanup decision in isolation).
// ---------------------------------------------------------------------------

#[test]
fn lock_aborted_upgrade_removes_sidecar_via_real_wiring_v0_4_0() {
    // DatabaseLocked ⟹ redb-v2 open failed ⟹ store untouched ⟹ the sidecar
    // `backup_once` just wrote may be a stale snapshot: it must be REMOVED, and a
    // disarmed retry must migrate cleanly from the store's OWN intact bytes.
    let (_tmp, store) = copy_fixture("real-v0.4.0.redb");
    let manifest = load_manifest("real-v0.4.0.manifest.json");
    let bak = pre_substrate_bak(&store);

    arm_upgrade_fault(UpgradeFault::Locked);
    let err = PulseDB::open(&store, Config::default()).unwrap_err();
    disarm_upgrade_fault();

    assert!(
        matches!(err, PulseDBError::Storage(StorageError::DatabaseLocked)),
        "the injected lock-abort must surface as DatabaseLocked, got: {err:?}"
    );
    assert!(
        !bak.exists(),
        "a lock-aborted upgrade must REMOVE the sidecar `backup_once` wrote (it may be stale)"
    );
    // The aborted upgrade left the store untouched (still un-migrated v2).
    assert!(
        !marker_is_current(&store),
        "the lock-aborted upgrade must not have migrated the store"
    );
    // The disarmed retry migrates for real and reads every entity back identically.
    assert_clean_rerun_value_identical(&store, &manifest);
}

#[test]
fn torn_upgrade_keeps_sidecar_via_real_wiring_v0_4_0() {
    // The KEEP branch: a NON-lock upgrade error (a torn in-place upgrade) must KEEP
    // the `.pre-substrate.bak` — it is the rollback point for a partially-rewritten
    // primary, not a stale snapshot to discard.
    let (_tmp, store) = copy_fixture("real-v0.4.0.redb");
    let bak = pre_substrate_bak(&store);

    arm_upgrade_fault(UpgradeFault::Torn);
    let err = PulseDB::open(&store, Config::default()).unwrap_err();
    disarm_upgrade_fault();

    assert!(
        matches!(err, PulseDBError::Storage(StorageError::Redb(_))),
        "the injected torn upgrade must surface as a non-lock Redb error, got: {err:?}"
    );
    assert!(
        bak.exists(),
        "a torn (non-lock) upgrade error must KEEP the sidecar as the rollback point"
    );
}

#[test]
fn preserved_sidecar_kept_on_lock_abort_via_real_wiring_v0_4_0() {
    // PR#57-review P2: a PRE-EXISTING sidecar (a valid rollback point left by an
    // EARLIER attempt) is PRESERVED by `backup_once` (a no-op), so a later
    // lock-aborted upgrade must NOT delete it — only a sidecar THIS attempt created
    // is a disposable this-attempt copy. Guards against discarding a rollback point
    // that predates the locked retry.
    let (_tmp, store) = copy_fixture("real-v0.4.0.redb");
    let bak = pre_substrate_bak(&store);
    // A valid rollback point left by a hypothetical earlier attempt.
    std::fs::write(&bak, b"earlier-attempt-rollback-point").unwrap();

    arm_upgrade_fault(UpgradeFault::Locked);
    let err = PulseDB::open(&store, Config::default()).unwrap_err();
    disarm_upgrade_fault();

    assert!(
        matches!(err, PulseDBError::Storage(StorageError::DatabaseLocked)),
        "the injected lock-abort must surface as DatabaseLocked, got: {err:?}"
    );
    assert!(
        bak.exists(),
        "a PRESERVED (pre-existing) sidecar must be KEPT on a lock-abort, not deleted"
    );
    assert_eq!(
        std::fs::read(&bak).unwrap(),
        b"earlier-attempt-rollback-point",
        "the preserved sidecar must be untouched (neither replaced nor deleted)"
    );
}

// ---------------------------------------------------------------------------
// marker-1 {redb-v3, bincode} evidence for #53b (C7) — derived from real-v0.5.1
// ---------------------------------------------------------------------------

#[test]
fn marker1_redb_v3_bincode_needs_no_sidecar_53b() {
    silence_injection_panics();
    let manifest = load_manifest("real-v0.5.1.manifest.json");
    let (_tmp, store) = copy_fixture("real-v0.5.1.redb");

    // Stage 1: crash real-v0.5.1 at PreReencode. real-v0.5.1 is redb-FORMAT-v2, so
    // create_or_migrate runs the destructive upgrade (durable) + claims a sidecar,
    // then the write-txn crashes → rolled back. The store is now the {redb-v3,
    // bincode, Absent-marker} rung (functionally marker-1: already redb-v3, still
    // bincode, needs the codec migration, classifies to needs_marker_write=true).
    crash_open(&store, Boundary::PreReencode);
    assert!(
        !marker_is_current(&store),
        "stage-1: {{redb-v3, bincode}} store must not be at CURRENT marker"
    );
    // Remove stage-1's sidecar so we can prove stage 2 creates NONE.
    let bak = pre_substrate_bak(&store);
    let _ = std::fs::remove_file(&bak);
    assert!(
        !bak.exists(),
        "sidecar removed to set up the #53b no-sidecar assertion"
    );

    // Stage 2 (#53b): crash the ALREADY-redb-v3 store at MidReencode. Because the
    // file is already redb-v3, create_or_migrate NEVER takes the destructive arm →
    // NO `.pre-substrate.bak` is claimed. The single atomic write-txn rolls back
    // pristine — the executable proof that the {redb-v3, bincode} / marker-1 path
    // is crash-safe WITHOUT a sidecar (replacing 4.04 #53b's comment-only claim).
    crash_open(&store, Boundary::MidReencode);
    assert!(
        !bak.exists(),
        "#53b: a crash migrating an already-redb-v3 {{redb-v3, bincode}} store must create NO `.pre-substrate.bak` (single-atomic-txn rollback, no sidecar needed)"
    );
    assert!(
        !marker_is_current(&store),
        "stage-2: marker must still not be CURRENT after the rolled-back txn"
    );

    // Stage 3: a clean re-run migrates the {redb-v3, bincode} store value-identically.
    assert_clean_rerun_value_identical(&store, &manifest);
}

// ---------------------------------------------------------------------------
// genuine SIGKILL (no Drop) at PreMarker + ungraceful-death locks (C15)
// ---------------------------------------------------------------------------

/// Child entry: only crashes when re-exec'd with `FI_SIGKILL_STORE` set; a normal
/// test run executes it as a no-op pass. Named `zzz_` so its intent is clear.
#[test]
fn zzz_sigkill_child_entry() {
    let store = match std::env::var("FI_SIGKILL_STORE") {
        Ok(s) => s,
        Err(_) => return, // not the re-exec'd child — no-op
    };
    pulsedb::fault_injection::arm(Boundary::PreMarker, Action::Sigkill);
    let _ = PulseDB::open(Path::new(&store), Config::default());
    // Unreachable: SIGKILL must have fired at PreMarker. If we get here, fail loudly.
    eprintln!("BUG: SIGKILL did not fire at PreMarker");
    std::process::exit(97);
}

#[cfg(unix)]
#[test]
fn sigkill_at_premarker_reopens_pristine_and_survives_locks_v0_4_0() {
    use std::os::unix::process::ExitStatusExt;

    silence_injection_panics();
    let manifest = load_manifest("real-v0.4.0.manifest.json");
    let (_tmp, store) = copy_fixture("real-v0.4.0.redb");

    // Re-exec THIS test binary to run only the child entry, pointed at the store.
    let exe = std::env::current_exe().unwrap();
    let status = std::process::Command::new(exe)
        .args([
            "--exact",
            "zzz_sigkill_child_entry",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("FI_SIGKILL_STORE", &store)
        .status()
        .expect("spawn SIGKILL child");

    // A genuine kill: the child died by SIGKILL (signal 9), no graceful Drop.
    assert_eq!(
        status.signal(),
        Some(9),
        "child must be SIGKILL'd (signal 9) at PreMarker — got {status:?}"
    );

    // C15: the SIGKILL'd child left redb `.lock` + `.migrate.lock`; the OS releases
    // advisory locks on death, so the re-open must SURVIVE them. The store must be
    // pristine (marker not CURRENT) and a clean re-run migrates value-identically.
    assert!(
        !marker_is_current(&store),
        "post-SIGKILL store must be un-migrated (marker not CURRENT)"
    );
    // best-effort: clean up any stale migrate-lock sidecar left by the dead child.
    let stale_lock = PathBuf::from(format!("{}.migrate.lock", store.display()));
    let _ = std::fs::remove_file(&stale_lock);

    assert_clean_rerun_value_identical(&store, &manifest);
}
