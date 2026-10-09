//! Test-only migration fault-injection seam (VS-4.0.4 work 4.05, issue #46).
//!
//! Compiled **only** under `--features fault-injection` — never in default or
//! release builds, so the production migration path is byte-identical when the
//! feature is off (each call site is a statement-level `#[cfg(...)]` that
//! disappears entirely). This lets a test ARM a simulated crash — or a pause —
//! at a specific migration boundary; the migration path calls `maybe_inject` at
//! each of the six boundaries, which either `panic!`s (in-process — Drop runs,
//! so the redb write-txn aborts gracefully / MVCC rolls back), `raise(SIGKILL)`s
//! (the subprocess crash-fidelity tests — no Drop, forcing redb's file-level
//! recovery), or blocks until a test releases it (`Action::Pause`, the
//! concurrent-upgrade test — r1.s6.w1).
//!
//! The armed state is **thread-local**: the migration runs synchronously on the
//! same thread that calls `PulseDB::open`, so arming on that thread is sufficient
//! and there is zero cross-test contamination (each `#[test]` gets its own
//! thread, hence its own register).
//!
//! This module changes NO migration behavior — it only observes boundaries. The
//! boundary ordering/semantics are owned by 4.03 (registry re-encode loop) and
//! 4.04 (reordered pre-txn path + backup/fsync + marker rules).

use std::cell::Cell;

/// The six migration boundaries a test can crash at.
///
/// Three are **pre-txn** (a crash there is NOT covered by a redb txn abort):
/// - [`Boundary::MidBackupPreFsync`] — inside `backup_once`, after the sidecar
///   bytes are copied but before the `#53c` fsync makes them durable.
/// - [`Boundary::MidSchemaBackup`] — inside the durable schema-sidecar publish
///   (r1.s6.w1, #89/#25), after the staged copy is copied and fsync'd but
///   before it is validated and published. A crash here leaves no sidecar at
///   the final path and the store untouched.
/// - [`Boundary::PostRedbUpgrade`] — after the destructive in-place redb v2→v3
///   upgrade returns, before the redb-4.1 reopen.
///
/// Three are **write-txn** (a crash before `commit()` rolls the whole txn back):
/// - [`Boundary::PreReencode`] — before the registry-driven codec re-encode pass.
/// - [`Boundary::MidReencode`] — inside the re-encode loop, after ≥1 table pass.
/// - [`Boundary::PreMarker`] — after re-encode, just before the marker insert
///   (the atomic commit point).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Boundary {
    /// Pre-txn: inside `backup_once`, after the sidecar bytes are copied but
    /// before the `#53c` fsync makes them durable.
    MidBackupPreFsync,
    /// Pre-txn: inside the durable schema-sidecar publish, after the staged
    /// copy is copied and fsync'd but before it is validated and published.
    /// A crash here leaves the final sidecar path ABSENT (the copy is still a
    /// temp) and the store untouched.
    MidSchemaBackup,
    /// Pre-txn: after the destructive in-place redb v2→v3 upgrade returns, before
    /// the redb-4.1 reopen.
    PostRedbUpgrade,
    /// Write-txn: before the registry-driven codec re-encode pass begins.
    PreReencode,
    /// Write-txn: partway through the re-encode loop, after ≥1 table pass.
    MidReencode,
    /// Write-txn: after the re-encode succeeded, just before the marker insert
    /// (the atomic commit point).
    PreMarker,
}

/// How to crash — or hold — when the armed boundary is reached.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Action {
    /// Unwind via `panic!` — Drop runs (graceful redb txn abort). Deterministic,
    /// CI-stable, catchable with `catch_unwind`; covers every boundary.
    Panic,
    /// `libc::raise(SIGKILL)` — the process dies immediately, no Drop. Used by
    /// the subprocess crash-fidelity tests for genuine crash semantics.
    Sigkill,
    /// Hold the migration at the boundary until the test releases it. Writes
    /// the file named by `FI_PAUSE_MARKER`, then waits for the file named by
    /// `FI_PAUSE_RELEASE` (a 50 ms poll, at most 60 s); a timeout exits the
    /// process non-zero so a crashed parent cannot leave the child hung. For
    /// subprocess use only — in-process it would block the test thread.
    Pause,
}

/// How long `Action::Pause` waits for the release file before exiting non-zero.
const PAUSE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// The poll interval of `Action::Pause`'s release-file wait.
const PAUSE_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);

thread_local! {
    static ARMED: Cell<Option<(Boundary, Action)>> = const { Cell::new(None) };
}

/// Arm a crash at `boundary` with `action`, on the current thread.
pub fn arm(boundary: Boundary, action: Action) {
    ARMED.with(|c| c.set(Some((boundary, action))));
}

/// Clear any armed injection on the current thread.
pub fn disarm() {
    ARMED.with(|c| c.set(None));
}

/// RAII: arm on construction, disarm on drop — so a panicking (in-process)
/// injection can't leave the register armed for a later assertion on the same
/// thread. Cheap; tests may also call [`disarm`] explicitly.
pub struct ArmGuard;

impl ArmGuard {
    /// Arm the given `boundary`/`action` on the current thread; the returned
    /// guard disarms on drop.
    pub fn new(boundary: Boundary, action: Action) -> Self {
        arm(boundary, action);
        ArmGuard
    }
}

impl Drop for ArmGuard {
    fn drop(&mut self) {
        disarm();
    }
}

/// A forced outcome for the destructive redb v2→v3 upgrade, armable by a test so
/// it can drive `create_or_migrate`'s post-backup sidecar-cleanup branch (#4 / T7)
/// deterministically. A real cross-version file-lock race is not enough: on most
/// platforms it surfaces at the pre-backup redb-4.1 `create`, so the sidecar is
/// never written and the post-backup cleanup path is never exercised. This forces
/// the abort to land INSIDE the upgrade (after `backup_once`). Compiled out unless
/// `fault-injection` is on; it changes no production behavior.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum UpgradeFault {
    /// Abort as if a legacy writer still held the file: the redb-v2 open failed, so
    /// the store is untouched. `upgrade_redb_v2_to_v3` maps this to `DatabaseLocked`
    /// (the sidecar must be REMOVED — it may be a stale snapshot).
    Locked,
    /// Abort with a non-lock error (a torn in-place `upgrade()`); the sidecar must
    /// be KEPT as the rollback point.
    Torn,
}

thread_local! {
    static UPGRADE_FAULT: Cell<Option<UpgradeFault>> = const { Cell::new(None) };
}

/// Arm a forced upgrade outcome on the current thread (consumed on first read, so a
/// disarmed retry migrates for real).
pub fn arm_upgrade_fault(fault: UpgradeFault) {
    UPGRADE_FAULT.with(|c| c.set(Some(fault)));
}

/// Clear any armed upgrade fault on the current thread.
pub fn disarm_upgrade_fault() {
    UPGRADE_FAULT.with(|c| c.set(None));
}

/// Consume any armed upgrade fault (the migration path calls this at the top of the
/// destructive upgrade). `None` in the common case.
pub(crate) fn take_upgrade_fault() -> Option<UpgradeFault> {
    UPGRADE_FAULT.with(|c| c.take())
}

/// Called by the migration path at each boundary. When the armed boundary
/// matches, crash per the armed action; otherwise a no-op.
pub(crate) fn maybe_inject(boundary: Boundary) {
    let armed = ARMED.with(|c| c.get());
    if let Some((b, action)) = armed {
        if b == boundary {
            // Consume the arm so a retry/loop can't re-trigger on the same thread.
            disarm();
            match action {
                Action::Panic => {
                    panic!("fault-injection: simulated migration crash at {boundary:?}")
                }
                Action::Sigkill => {
                    // SAFETY: `raise` with a valid signal number is always sound;
                    // SIGKILL terminates this process immediately (no unwinding,
                    // no Drop) — the point of the crash-fidelity test.
                    unsafe {
                        libc::raise(libc::SIGKILL);
                    }
                    // Unreachable in practice; guard against a spurious return.
                    unreachable!("SIGKILL did not terminate the process");
                }
                Action::Pause => pause_until_released(boundary),
            }
        }
    }
}

/// `Action::Pause`: announce the pause, then wait for the release file.
///
/// The marker is written (and fsync'd) FIRST, so a parent that waits for it
/// observes a child that is already past the boundary. Paths come from
/// `FI_PAUSE_MARKER` / `FI_PAUSE_RELEASE`; either missing is a test bug and
/// panics loudly. On timeout the process exits non-zero — the parent will not
/// release a dead child, and nothing should hang waiting for it.
fn pause_until_released(boundary: Boundary) {
    use std::io::Write as _;

    let marker = std::env::var("FI_PAUSE_MARKER").unwrap_or_else(|_| {
        panic!("fault-injection: paused at {boundary:?} but FI_PAUSE_MARKER is not set")
    });
    let release = std::env::var("FI_PAUSE_RELEASE").unwrap_or_else(|_| {
        panic!("fault-injection: paused at {boundary:?} but FI_PAUSE_RELEASE is not set")
    });

    match std::fs::File::create(&marker) {
        Ok(mut file) => {
            let _ = writeln!(file, "paused at {boundary:?}");
            let _ = file.sync_all();
        }
        Err(error) => {
            eprintln!("fault-injection: cannot write pause marker {marker}: {error}");
            std::process::exit(93);
        }
    }

    let deadline = std::time::Instant::now() + PAUSE_TIMEOUT;
    while std::time::Instant::now() < deadline {
        if std::path::Path::new(&release).exists() {
            return;
        }
        std::thread::sleep(PAUSE_POLL_INTERVAL);
    }
    eprintln!(
        "fault-injection: pause at {boundary:?} timed out after {:?} waiting for {release}; \
         exiting non-zero",
        PAUSE_TIMEOUT
    );
    std::process::exit(94);
}
