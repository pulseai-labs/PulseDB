//! Collective-memory measurement harness — the evidence half of release r1's
//! exit criterion 8 (F-CI2b).
//!
//! One measurement **point** is `(max_elements, collectives, phase)`, and each
//! point runs in a **fresh child process**, so both baselines — this binary's
//! allocator counters and the OS metric — start clean. The child emits exactly
//! one `COLLECTIVE_MEMORY {json}` line. The parent (`collective_memory_grid`,
//! `#[ignore]`) drives the grid, re-prints every line, appends each one to the
//! file named by `PULSEDB_MEM_OUT` as it arrives, and then prints one
//! `VERDICT-INPUT {json}` line per phase.
//!
//! The harness measures; it does not judge. Whether these numbers falsify the
//! HNSW pre-reservation hypothesis is read at the round-1 barrier from the
//! Windows CI artifacts, not decided here.
//!
//! Attribution discipline: `alloc_requested_delta` comes from the global
//! allocator, `os_metric_delta` from the OS. A gap between them is *outside the
//! global allocator (direct OS allocation, thread stacks, file mappings) or
//! allocator amplification* — these numbers cannot separate the two, so no
//! single cause is named.
//!
//! `os_metric` names carry their unit (`…_bytes`): `PrivateUsage_bytes` on
//! Windows — the commit charge PR #88's leg exhausted — and `VmData_bytes` on
//! Linux, which is informational, because Linux overcommits and the reservation
//! that is invisible here is the one that is fatal there.

use std::alloc::{GlobalAlloc, Layout, System};
use std::io::Write;
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

use pulsedb::{Config, HnswConfig, PulseDB};
use serde_json::{json, Value};

// ============================================================================
// Counting allocator (private to this test binary)
// ============================================================================

static ALLOC_REQUESTED: AtomicU64 = AtomicU64::new(0);
static ALLOC_LIVE: AtomicU64 = AtomicU64::new(0);

/// Wraps [`System`], counting cumulative bytes requested and bytes still live.
///
/// Relaxed atomics: the counters are a monotonic ledger, not a synchronisation
/// device, and both snapshots are taken on the measuring thread between phases.
struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: `layout` is forwarded unchanged to the allocator the default
        // implementation would have used.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            record_requested(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: as in `alloc`.
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            record_requested(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` and `layout` come from a matching `alloc*` call on this
        // same allocator and are forwarded unchanged.
        unsafe { System.dealloc(ptr, layout) };
        ALLOC_LIVE.fetch_sub(layout.size() as u64, Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: `ptr` and `layout` come from a matching `alloc*` call on this
        // same allocator; `new_size` is the caller's requested size, forwarded
        // unchanged.
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            // The request is the NEW size — the old block was counted when it
            // was allocated — and live moves by the difference.
            record_requested(new_size);
            if new_size >= layout.size() {
                ALLOC_LIVE.fetch_add((new_size - layout.size()) as u64, Ordering::Relaxed);
            } else {
                ALLOC_LIVE.fetch_sub((layout.size() - new_size) as u64, Ordering::Relaxed);
            }
        }
        new_ptr
    }
}

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

fn record_requested(bytes: usize) {
    ALLOC_REQUESTED.fetch_add(bytes as u64, Ordering::Relaxed);
    ALLOC_LIVE.fetch_add(bytes as u64, Ordering::Relaxed);
}

/// The two counters, requested first. The order is load-bearing: [`os_read`]
/// allocates on Linux, so a probe of the OS metric must never sit between them.
fn counters() -> (u64, u64) {
    (
        ALLOC_REQUESTED.load(Ordering::Relaxed),
        ALLOC_LIVE.load(Ordering::Relaxed),
    )
}

/// Both counters plus the OS metric.
fn snapshot() -> Snapshot {
    let (requested, live) = counters();
    Snapshot {
        requested,
        live,
        os: os_read(),
    }
}

#[derive(Clone, Copy)]
struct Snapshot {
    requested: u64,
    live: u64,
    os: u64,
}

// ============================================================================
// OS metric
// ============================================================================

/// Windows: `PROCESS_MEMORY_COUNTERS_EX.PrivateUsage`, in bytes. Zero when the
/// call fails, which the parent reads as a missing metric, not a measurement.
#[cfg(windows)]
fn os_read() -> u64 {
    use windows_sys::Win32::System::ProcessStatus::{
        GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS, PROCESS_MEMORY_COUNTERS_EX,
    };
    use windows_sys::Win32::System::Threading::GetCurrentProcess;

    // SAFETY: the handle is this process's own, valid for the call's duration and
    // never closed here. `counters` is a live local initialised to all-zero of
    // exactly the size passed in `cb`, and its address is passed as the
    // `*mut PROCESS_MEMORY_COUNTERS` the API declares — the documented use of the
    // EX struct, whose leading fields are the plain struct's. The call writes
    // only into `counters` and `cb` is that struct's own size, so the buffer it
    // may write cannot exceed it.
    unsafe {
        let mut counters = PROCESS_MEMORY_COUNTERS_EX {
            cb: std::mem::size_of::<PROCESS_MEMORY_COUNTERS_EX>() as u32,
            ..Default::default()
        };
        let ok = GetProcessMemoryInfo(
            GetCurrentProcess(),
            (&mut counters as *mut PROCESS_MEMORY_COUNTERS_EX).cast::<PROCESS_MEMORY_COUNTERS>(),
            counters.cb,
        );
        if ok != 0 {
            counters.PrivateUsage as u64
        } else {
            0
        }
    }
}

/// Linux: `VmData` from `/proc/self/status`, in bytes. Informational — Linux
/// overcommits, so this is not the fatal figure it is on Windows.
///
/// Deliberately allocation-light — a stack buffer and a borrow-only `Cow`
/// instead of `read_to_string`'s heap `String`. This probe runs *inside* the
/// window the allocator counters bracket, so its own heap traffic would land in
/// `alloc_requested_delta` (a `read_to_string` adds a flat ~4 KiB per point).
#[cfg(target_os = "linux")]
fn os_read() -> u64 {
    use std::io::Read;

    let Ok(mut file) = std::fs::File::open("/proc/self/status") else {
        return 0;
    };
    let mut buf = [0u8; 4096];
    let Ok(read) = file.read(&mut buf) else {
        return 0;
    };
    // Borrowed (no allocation) for the valid-UTF-8 case this file always is.
    let status = String::from_utf8_lossy(&buf[..read]);
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmData:") {
            // "VmData:\t  12345 kB"
            let kb: u64 = rest
                .trim()
                .trim_end_matches("kB")
                .trim()
                .parse()
                .unwrap_or(0);
            return kb * 1024;
        }
    }
    0
}

#[cfg(not(any(windows, target_os = "linux")))]
fn os_read() -> u64 {
    0
}

#[cfg(windows)]
fn os_metric_name() -> &'static str {
    "PrivateUsage_bytes"
}

#[cfg(target_os = "linux")]
fn os_metric_name() -> &'static str {
    "VmData_bytes"
}

#[cfg(not(any(windows, target_os = "linux")))]
fn os_metric_name() -> &'static str {
    "none"
}

/// Audit C4's ceiling: on Windows the `10 000 × 1000` point re-creates PR #88's
/// ~10.9 GiB commit, so the child stops itself at 2 GiB and reports what it has
/// (`"aborted": true`) rather than taking the runner down.
const OS_CEILING_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// The ceiling comparison itself, platform-independent so its boundary is
/// drivable wherever this binary runs. Whether a reading is *enforced* against it
/// is a separate question — see [`os_ceiling_hit_at_current`].
fn ceiling_hit_at(bytes: u64) -> bool {
    bytes >= OS_CEILING_BYTES
}

/// Reads the OS metric after every `create_collective`, on Windows only: there
/// the read is a syscall filling a stack struct, while on Linux it is a `/proc`
/// parse that allocates and would land inside the very counters the loop
/// measures. Linux samples the metric at the snapshot points alone — and its
/// `VmData` is informational, with a large point legitimately above 2 GiB — so
/// the ceiling is not enforced off Windows.
#[cfg(windows)]
fn os_ceiling_hit_at_current() -> bool {
    ceiling_hit_at(os_read())
}

#[cfg(not(windows))]
fn os_ceiling_hit_at_current() -> bool {
    false
}

/// The single `PulseDB::open` a reopen point makes cannot be interrupted
/// mid-call, so it is checked once, against the snapshot taken right after it.
#[cfg(windows)]
fn os_ceiling_hit_at(bytes: u64) -> bool {
    ceiling_hit_at(bytes)
}

#[cfg(not(windows))]
fn os_ceiling_hit_at(_bytes: u64) -> bool {
    false
}

// ============================================================================
// The measurement points
// ============================================================================

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Create,
    Reopen,
}

impl Phase {
    const ALL: [Phase; 2] = [Phase::Create, Phase::Reopen];

    fn parse(field: &str) -> Option<Self> {
        match field {
            "create" => Some(Phase::Create),
            "reopen" => Some(Phase::Reopen),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Phase::Create => "create",
            Phase::Reopen => "reopen",
        }
    }
}

/// The grid (spec Approach §1, grill Q1). Every platform yields six
/// combinations, which with both phases is the 12 points AC-3 counts.
///
/// Windows keeps the 10 000-wide arm short because `10 000 × 1000` is the point
/// that re-created PR #88's ~10.9 GiB commit. Both arms still include the common
/// count of 100 that `VERDICT-INPUT` compares the two `max_elements` on.
fn grid() -> &'static [(usize, usize)] {
    #[cfg(windows)]
    const POINTS: &[(usize, usize)] = &[
        (10_000, 0),
        (10_000, 50),
        (10_000, 100),
        (100, 0),
        (100, 100),
        (100, 1000),
    ];
    #[cfg(not(windows))]
    const POINTS: &[(usize, usize)] = &[
        (10_000, 0),
        (10_000, 100),
        (10_000, 1000),
        (100, 0),
        (100, 100),
        (100, 1000),
    ];
    POINTS
}

/// The two `max_elements` arms `VERDICT-INPUT` compares: the
/// `HnswConfig::max_elements` default, and a hundredth of it.
const WIDE_MAX_ELEMENTS: usize = 10_000;
const NARROW_MAX_ELEMENTS: usize = 100;

/// The count both arms share, so their per-collective figures compare directly.
const COMMON_COUNT: usize = 100;

/// The point's config: only `hnsw.max_elements` moves, everything else default.
fn point_config(max_elements: usize) -> Config {
    Config {
        hnsw: HnswConfig {
            max_elements,
            ..HnswConfig::default()
        },
        ..Config::default()
    }
}

fn features_label() -> &'static str {
    // `sync-http` implies `sync`, so the wider feature is tested first.
    if cfg!(feature = "sync-http") {
        "sync-http"
    } else if cfg!(feature = "sync") {
        "sync"
    } else {
        "default"
    }
}

/// Creates `collectives` empty collectives in `store`, checking the audit C4
/// ceiling after **every** `create_collective`. Both phases drive their creates
/// through here, so the reopen phase's build is guarded exactly as the create
/// phase's measured loop is — a `10 000 × 50/100` reopen point would otherwise
/// commit gigabytes before any stop.
///
/// Returns how many creates completed and whether the ceiling stopped the loop.
fn create_collectives(store: &PulseDB, collectives: usize) -> (usize, bool) {
    let mut done = 0usize;
    for index in 0..collectives {
        store
            .create_collective(&format!("collective-{index}"))
            .expect("create_collective");
        done += 1;
        if os_ceiling_hit_at_current() {
            return (done, true);
        }
    }
    (done, false)
}

/// A fresh store holding `collectives` empty collectives at this `max_elements`.
fn build_store(path: &Path, max_elements: usize, collectives: usize) -> (PulseDB, usize, bool) {
    let store = PulseDB::open(path, point_config(max_elements)).expect("open store");
    let (done, aborted) = create_collectives(&store, collectives);
    (store, done, aborted)
}

/// Runs one point and returns its `COLLECTIVE_MEMORY` payload.
fn measure(max_elements: usize, collectives: usize, phase: Phase) -> Value {
    let dir = tempfile::tempdir().expect("create measurement tempdir");
    let path = dir.path().join("collective-memory.db");

    match phase {
        // `create` measures the create loop itself, so the fresh store is opened
        // first and the baseline carries the store open and nothing else.
        Phase::Create => {
            let store = PulseDB::open(&path, point_config(max_elements)).expect("open fresh store");
            let before = snapshot();

            let (done, aborted) = create_collectives(&store, collectives);

            let after = snapshot();
            store.close().expect("close store");
            point_line(
                max_elements,
                collectives,
                done,
                phase,
                before,
                after,
                aborted,
            )
        }
        // `reopen` measures the open alone: the same store is built and closed
        // first, and the baseline is taken after the close.
        Phase::Reopen => {
            let (store, built, build_aborted) = build_store(&path, max_elements, collectives);
            store.close().expect("close store");

            let before = snapshot();
            let (after, aborted) = if build_aborted {
                // The build stopped at the ceiling, so the reopen it exists to
                // measure never ran: report the zero delta — nothing happened
                // between the snapshots — rather than one spanning the build.
                (before, true)
            } else {
                let reopened =
                    PulseDB::open(&path, point_config(max_elements)).expect("reopen store");
                let after = snapshot();
                let aborted = os_ceiling_hit_at(after.os);
                reopened.close().expect("close reopened store");
                (after, aborted)
            };

            point_line(
                max_elements,
                collectives,
                built,
                phase,
                before,
                after,
                aborted,
            )
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn point_line(
    max_elements: usize,
    collectives: usize,
    collectives_done: usize,
    phase: Phase,
    before: Snapshot,
    after: Snapshot,
    aborted: bool,
) -> Value {
    json!({
        "max_elements": max_elements,
        "collectives": collectives,
        // How many `create_collective` calls completed inside the measured
        // window: equal to `collectives` on a complete point, the count reached
        // on an aborted `create`, and the count built on a reopen whose build
        // aborted (where every delta is 0 because nothing was measured).
        "collectives_done": collectives_done,
        "phase": phase.as_str(),
        "os": std::env::consts::OS,
        "features": features_label(),
        // Saturating: the counter is monotonic, so this only guards the cast.
        "alloc_requested_delta": after.requested.saturating_sub(before.requested),
        "alloc_live_delta": after.live as i64 - before.live as i64,
        "os_metric": os_metric_name(),
        "os_metric_delta": after.os as i64 - before.os as i64,
        "aborted": aborted,
    })
}

// ============================================================================
// The child
// ============================================================================

const POINT_TEST_NAME: &str = "collective_memory_point";
const COLLECTIVE_MEMORY_TAG: &str = "COLLECTIVE_MEMORY";
const VERDICT_INPUT_TAG: &str = "VERDICT-INPUT";

/// The payload of a `TAG {json}` line, if `line` carries one.
///
/// The tag is located anywhere in the line, not only at its start: libtest's
/// `--nocapture` writes the per-test progress prefix (`test NAME ... `) with no
/// trailing newline, so the child's own output is glued onto the end of that
/// line. The child also leads with a newline so the tag is a real line in its
/// stream; this scan is what makes the parent independent of both behaviours.
fn tagged_payload<'a>(line: &'a str, tag: &str) -> Option<&'a str> {
    let rest = line.get(line.find(tag)? + tag.len()..)?.strip_prefix(' ')?;
    rest.starts_with('{').then_some(rest)
}

/// The measurement child. The parent re-executes this binary with this test's
/// exact name once per point; a run that does not set `PULSEDB_MEM_POINT` — any
/// ordinary `cargo test` — returns without measuring.
///
/// This name must NOT contain the substring `collective_memory_grid`: the parent
/// is invoked with that as a *substring* filter, so a child matching it would
/// turn the parent's `1 passed` into `2 passed`.
#[test]
#[ignore]
fn collective_memory_point() {
    let Ok(point) = std::env::var("PULSEDB_MEM_POINT") else {
        return;
    };

    let fields: Vec<&str> = point.split(',').collect();
    assert_eq!(
        fields.len(),
        3,
        "PULSEDB_MEM_POINT must be <max_elements>,<collectives>,<phase>, got {point:?}"
    );
    let max_elements: usize = fields[0].parse().expect("max_elements");
    let collectives: usize = fields[1].parse().expect("collectives");
    let phase = Phase::parse(fields[2]).unwrap_or_else(|| panic!("unknown phase in {point:?}"));

    let payload = measure(max_elements, collectives, phase);
    // The leading newline keeps the tag at the start of a real line: without it
    // libtest's `--nocapture` progress prefix would share the line.
    println!("\n{COLLECTIVE_MEMORY_TAG} {payload}");
}

// ============================================================================
// The parent
// ============================================================================

/// Every field a `COLLECTIVE_MEMORY` line must carry.
const REQUIRED_POINT_FIELDS: [&str; 11] = [
    "max_elements",
    "collectives",
    "collectives_done",
    "phase",
    "os",
    "features",
    "alloc_requested_delta",
    "alloc_live_delta",
    "os_metric",
    "os_metric_delta",
    "aborted",
];

/// Runs one point in a fresh child and returns its validated payload.
///
/// Shared by the grid driver and the correction regressions, so a regression
/// exercises the same spawn-and-validate path the grid uses rather than a copy
/// of it.
fn run_child(exe: &Path, point: &str) -> Value {
    let output = Command::new(exe)
        .arg(POINT_TEST_NAME)
        .args(["--ignored", "--exact", "--nocapture", "--test-threads=1"])
        .env("PULSEDB_MEM_POINT", point)
        .output()
        .expect("spawn measurement child");

    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    assert!(
        output.status.success(),
        "child for point {point} exited with {}\n--- stderr ---\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr),
    );

    let emitted: Vec<&str> = stdout
        .lines()
        .filter_map(|line| tagged_payload(line, COLLECTIVE_MEMORY_TAG))
        .collect();
    assert_eq!(
        emitted.len(),
        1,
        "child for point {point} emitted {} {COLLECTIVE_MEMORY_TAG} lines, expected exactly 1\
         \n--- stdout ---\n{stdout}",
        emitted.len(),
    );

    let payload: Value = serde_json::from_str(emitted[0]).unwrap_or_else(|error| {
        panic!("child for point {point} emitted malformed JSON: {error}\n---\n{stdout}")
    });
    for field in REQUIRED_POINT_FIELDS {
        assert!(
            payload.get(field).is_some(),
            "child for point {point} emitted no `{field}` field: {payload}"
        );
    }
    payload
}

/// The grid driver. Ignored by default: it spawns a child process per point.
#[test]
#[ignore]
fn collective_memory_grid() {
    // Close libtest's progress line first: with `--nocapture` the harness writes
    // `test NAME ... ` with no trailing newline, and every measurement line below
    // has to start at column 0 for `grep '^COLLECTIVE_MEMORY {'` to count it.
    println!();

    let exe = std::env::current_exe().expect("current test executable");
    let mut sink = artifact_sink();
    let mut measured: Vec<Value> = Vec::with_capacity(2 * grid().len());

    for &(max_elements, collectives) in grid() {
        for phase in Phase::ALL {
            let point = format!("{max_elements},{collectives},{}", phase.as_str());
            let payload = run_child(&exe, &point);

            // Re-print and persist: the artifact keeps every point that finished,
            // even if a later one takes the run down (audit C4).
            println!("{COLLECTIVE_MEMORY_TAG} {payload}");
            append(&mut sink, &format!("{COLLECTIVE_MEMORY_TAG} {payload}"));

            measured.push(payload);
        }
    }

    for phase in Phase::ALL {
        let line = format!("{VERDICT_INPUT_TAG} {}", verdict_input(phase, &measured));
        println!("{line}");
        append(&mut sink, &line);
    }
}

/// The append-on-arrival sink named by `PULSEDB_MEM_OUT` (audit C4), so a killed
/// run keeps what it measured. Unset — as in the local AC runs — prints only.
fn artifact_sink() -> Option<std::fs::File> {
    let path = std::env::var("PULSEDB_MEM_OUT")
        .ok()
        .filter(|p| !p.is_empty())?;
    Some(
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .unwrap_or_else(|error| panic!("open PULSEDB_MEM_OUT {path:?}: {error}")),
    )
}

fn append(sink: &mut Option<std::fs::File>, line: &str) {
    if let Some(file) = sink.as_mut() {
        writeln!(file, "{line}").expect("append artifact line");
        file.flush().expect("flush artifact line");
    }
}

/// Per-collective delta for one metric, at the count both arms share.
///
/// The denominator is `collectives_done`, not the planned `collectives`: an
/// aborted point's delta is a partial measurement, and dividing it by what was
/// *planned* would understate the cost by exactly the ratio of the two. `None`
/// when the point is missing or nothing was measured — an undefined figure must
/// not read as 0.
fn per_collective(
    measured: &[Value],
    phase: Phase,
    max_elements: usize,
    metric: &str,
) -> Option<f64> {
    let point = measured.iter().find(|point| {
        point.get("phase").and_then(Value::as_str) == Some(phase.as_str())
            && point.get("max_elements").and_then(Value::as_u64) == Some(max_elements as u64)
            && point.get("collectives").and_then(Value::as_u64) == Some(COMMON_COUNT as u64)
    })?;

    let done = point.get("collectives_done")?.as_u64()?;
    if done == 0 {
        return None;
    }
    let delta = point.get(metric)?.as_i64()? as f64;
    Some(round3(delta / done as f64))
}

/// One `VERDICT-INPUT` line per phase: the inputs to the verdict, not the
/// verdict. The round-1 barrier reads them beside the Windows artifacts.
fn verdict_input(phase: Phase, measured: &[Value]) -> Value {
    // `null` when either arm is missing or the narrow arm is zero: an undefined
    // ratio must not read as 0.
    let ratio = |metric: &str| -> Value {
        match (
            per_collective(measured, phase, WIDE_MAX_ELEMENTS, metric),
            per_collective(measured, phase, NARROW_MAX_ELEMENTS, metric),
        ) {
            (Some(wide), Some(narrow)) if narrow != 0.0 => json!(round3(wide / narrow)),
            _ => Value::Null,
        }
    };

    json!({
        "phase": phase.as_str(),
        // Deliberately NOT `features`: that exact key/value pair is the child
        // line's marker, and AC-5 counts exactly 12 lines carrying it. This line
        // states the same fact under a name that cannot inflate that count.
        "feature_set": features_label(),
        "os_metric": os_metric_name(),
        "count": COMMON_COUNT,
        "per_collective": {
            "max_elements_10000": {
                "alloc_requested_delta": per_collective(measured, phase, WIDE_MAX_ELEMENTS, "alloc_requested_delta"),
                "os_metric_delta": per_collective(measured, phase, WIDE_MAX_ELEMENTS, "os_metric_delta"),
            },
            "max_elements_100": {
                "alloc_requested_delta": per_collective(measured, phase, NARROW_MAX_ELEMENTS, "alloc_requested_delta"),
                "os_metric_delta": per_collective(measured, phase, NARROW_MAX_ELEMENTS, "os_metric_delta"),
            },
        },
        "ratio": {
            "alloc_requested_delta": ratio("alloc_requested_delta"),
            "os_metric_delta": ratio("os_metric_delta"),
        },
        // Run-wide, not per-phase: a reader must never have to infer completeness
        // from a line that only reports its own phase.
        "any_aborted": measured
            .iter()
            .any(|point| point.get("aborted").and_then(Value::as_bool).unwrap_or(false)),
    })
}

fn round3(value: f64) -> f64 {
    (value * 1000.0).round() / 1000.0
}

// ============================================================================
// Correction regressions (Dispatch 2)
// ============================================================================

/// The child's line must carry the measured count, not only the planned one: an
/// aborted point's delta is a *partial* measurement, so its per-collective
/// figure is only meaningful against how many `create_collective` calls
/// actually completed inside the measured window.
#[test]
fn collective_memory_child_line_carries_collectives_done() {
    let exe = std::env::current_exe().expect("current test executable");
    let payload = run_child(&exe, "100,0,create");

    assert_eq!(
        payload.get("collectives_done").and_then(Value::as_u64),
        Some(0),
        "a point that creates nothing must still report `collectives_done`: {payload}"
    );
}

/// The ceiling predicate is exact and drivable on every platform. Only the
/// *reading* is Windows-gated (Linux `VmData` is informational, and a large
/// point legitimately exceeds 2 GiB there); the comparison it feeds is not, so
/// the boundary is testable wherever this binary runs.
#[test]
fn collective_memory_ceiling_predicate_is_exact() {
    assert!(
        ceiling_hit_at(OS_CEILING_BYTES),
        "the ceiling itself is a hit"
    );
    assert!(
        !ceiling_hit_at(OS_CEILING_BYTES - 1),
        "one byte below the ceiling is not a hit"
    );
}

/// A reopen point whose *build* hit the ceiling skips the reopen and reports the
/// zero delta rather than one spanning the build, while still naming how many
/// collectives it built and what the grid planned.
#[test]
fn collective_memory_point_line_zeroes_an_aborted_build() {
    let snapshot = Snapshot {
        requested: 1_000,
        live: 500,
        os: 2_048,
    };
    let payload = point_line(10_000, 50, 35, Phase::Reopen, snapshot, snapshot, true);

    assert_eq!(
        payload.get("collectives").and_then(Value::as_u64),
        Some(50),
        "the planned count is unchanged: {payload}"
    );
    assert_eq!(
        payload.get("collectives_done").and_then(Value::as_u64),
        Some(35),
        "the measured count is what was built: {payload}"
    );
    assert_eq!(
        payload.get("aborted").and_then(Value::as_bool),
        Some(true),
        "{payload}"
    );
    for metric in [
        "alloc_requested_delta",
        "alloc_live_delta",
        "os_metric_delta",
    ] {
        assert_eq!(
            payload.get(metric).and_then(Value::as_i64),
            Some(0),
            "{metric} must be 0 when nothing was measured: {payload}"
        );
    }
}

/// The parent divides a partial delta by what was *measured*, not by what was
/// planned, and refuses to divide by zero.
#[test]
fn collective_memory_partial_denominator_uses_collectives_done() {
    let point = |phase: &str, max_elements: u64, collectives: u64, done: u64, delta: i64| {
        json!({
            "phase": phase,
            "max_elements": max_elements,
            "collectives": collectives,
            "collectives_done": done,
            "alloc_requested_delta": delta,
            "os_metric_delta": delta,
        })
    };

    // Aborted at 7 of the planned 100, having measured 700 bytes: the
    // per-collective figure is 700/7 = 100, not 700/100 = 7.
    let aborted = vec![point("create", 10_000, 100, 7, 700)];
    assert_eq!(
        per_collective(&aborted, Phase::Create, 10_000, "alloc_requested_delta"),
        Some(100.0)
    );

    // Nothing measured: undefined, and reported as such rather than as 0.
    let nothing = vec![point("reopen", 10_000, 100, 0, 0)];
    assert_eq!(
        per_collective(&nothing, Phase::Reopen, 10_000, "alloc_requested_delta"),
        None
    );

    // A point at another count is not the common-count point the ratio uses.
    let other_count = vec![point("create", 10_000, 1000, 1000, 1_000_000)];
    assert_eq!(
        per_collective(&other_count, Phase::Create, 10_000, "alloc_requested_delta"),
        None
    );
}

// ============================================================================
// Counter sanity
// ============================================================================

/// Proves the counter is not vacuous: a 1 MiB reservation must raise the
/// **requested** counter by at least 1 MiB. Runs in the ordinary suite, on every
/// feature set.
///
/// It asserts on the requested counter alone, and that is deliberate. The
/// requested counter is monotonic — every `alloc`/`alloc_zeroed`/`realloc` adds
/// to it and nothing subtracts — so no other test can move it backwards. The
/// live counter is process-global: this binary's other tests free memory
/// concurrently, and a free landing between the two reads below lowers the live
/// delta below the reservation. Asserting on it made this test fail at random
/// (Dispatch 3).
#[test]
fn collective_memory_counter_sees_a_known_reservation() {
    let before = counters();

    let reservation: Vec<u8> = Vec::with_capacity(1 << 20);
    let after = counters();

    assert!(
        reservation.capacity() >= 1 << 20,
        "the reservation under test is smaller than 1 MiB"
    );
    assert!(
        after.0 - before.0 >= 1 << 20,
        "reserving 1 MiB moved the requested counter by {} bytes",
        after.0 - before.0
    );

    drop(reservation);
}
