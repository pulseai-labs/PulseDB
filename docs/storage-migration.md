# Storage migration & crash-recovery posture

> Applies to the one-time **upgrade-on-open** migration that runs when an older PulseDB store is opened
> by a newer binary. Covers the redb file-format upgrade (v2→v3), the value-codec cutover
> (bincode→postcard, `SUBSTRATE_FORMAT` marker 1→2) and the logical-schema reshapes (v3→v4 tags,
> v4→v5 sync-cursor split — see *Schema v5* below). A steady-state open performs **no migration
> work**, but since 0.8.0 every **writable** open of an existing store first takes a bounded
> read-only peek at the file — one `open_read_only` plus one metadata read (the substrate marker and
> the `db_metadata` row, read in a single read transaction since #95) — to decide whether the
> pre-migration sidecar must be published before the writable open rewrites the allocator pages.
> A current-schema (v5) store therefore pays that peek on every writable open, then opens the file
> again to serve traffic (about 2.8 ms median for the storage open on the reference machine, 1k and
> 10k × 384-d stores alike; the peek is 27–32 µs median, p95 ≈ 40 µs, of that). **That peek is the
> supported floor** for the steady-state open: it is O(1) in store size, ≤ 0.04 % of the NFR-001 open
> budget (< 100 ms) — which the release-mode test `current_schema_store_opens_within_nfr001_budget`
> asserts — and it cannot be removed without weakening ADR-011's byte-identical rollback image.
> Proving a store is current needs a read of its `metadata` row; each cheaper alternative was
> rejected: skipping the peek when a `.pre-vN.bak` exists never fires for a fresh v5 store and an
> older sidecar is not proof that the store is current (nothing deletes one), a durable `<db>.schema`
> marker file adds a persistent artifact with a stale-marker invalidation mode, and forwarding the
> peek's version to the writable handle saves nothing because the under-lock re-read must stay. A
> **read-only** open never peeks and makes no PulseDB-level write — no migration lock, no sidecar, no
> metadata write; redb itself still opens the file writable and rewrites header bytes on
> open+close (#117).

## What happens on first open of an older store

When `PulseDB::open()` (writable) encounters an older on-disk store, it performs a one-time migration
**inside the open call**:

1. **Headroom preflight (before any destructive write).** A unified check covering two axes:
   - **Disk** — free space on the store's filesystem must cover the pristine backup (~1× the store size)
     plus the migrated file (~1× — the postcard re-encode does **not** shrink the size-dominant raw-`f32`
     embedding table) plus a transaction-growth margin. If short, the open fails with a typed
     `SubstrateMigrationInsufficientDisk` error and **zero writes**.
   - **Memory** — the projected single-transaction peak (≈ `0.10 × store_size`, measured) is checked
     **config-first** against a conservative store-size floor (1 GiB by default) or, when the embedder
     declares `Config::migration_available_memory_bytes`, against that declared budget. Host memory is
     **not** auto-detected (a cgroup-limited container over-reports RAM and would OOM). If the store is
     above the floor with no covering declared budget, the open fails with a typed
     `SubstrateMigrationTooLarge` error and **zero writes**.

   Both axes fail **closed** before any destructive write — never a half-migration that runs the machine
   out of disk or memory mid-pass.

2. **Pristine backup.** Before any migration write, the original file is copied to
   `<db>.pre-substrate.bak` (atomic, create-once). This single pristine backup precedes **both** the
   redb-format and the codec migrations, so it is the rollback point for the whole window.

3. **Migration (single write transaction).** The redb format upgrade (v2→v3) and the codec re-encode
   (every serde-blob value bincode→postcard; raw-byte tables — embeddings, secondary indexes, raw
   metadata keys — copied through byte-identically) run, and the `SUBSTRATE_FORMAT` marker is bumped to
   `2` as the **last write in the same transaction** (the atomic commit point).

4. **Progress signal.** The migration emits a one-time "this may take a while" log line plus per-phase
   `info!` progress, so a multi-minute migration of a large store is not a silent hang. The first-open
   migration is **explicitly exempt from NFR-001** (`<100ms` open); steady-state opens still meet it.

## Crash-recovery posture

The migration is realized as a **single write transaction** (the common path — measurements show
essentially all real stores fit the single-txn memory budget). Its crash-recovery contract:

- **Crash during the single-txn migration → re-run from scratch.** Because the marker bump is the last
  write in the transaction, a crash before commit leaves the **old marker and old-codec-decodable data
  intact** (redb rolls the uncommitted transaction back). On the next open the migration simply
  **re-runs from scratch** against the still-pristine `.pre-substrate.bak` rollback point. This is
  **safe, not fast**: a crashed migration is re-done in full, not resumed mid-way.

- **Store above the single-txn floor with no declared memory budget → fail closed.** Such a store
  currently refuses to open (typed `SubstrateMigrationTooLarge`) rather than risk an OOM that never
  finishes. To migrate it, either declare available memory via `Config::migration_available_memory_bytes`
  (opting into a single-txn migration the host can hold) or use the **offline migration tool** (below).
  A **resumable phased migration** for this case is **not yet implemented** (planned — see *Deferred*).

- **Restore from backup.** If a migrated store is ever suspect, the pristine `<db>.pre-substrate.bak`
  is a byte-for-byte snapshot of the pre-migration file and can be restored manually.

## Offline migration tool (`pulsedb migrate`)

For very large stores — especially the above-floor / undeclared-memory case — the recommended path is an
**offline `pulsedb migrate` tool** that performs the upgrade out of the hot open path with explicit
resource control. **Status: deferred** (planned for a later release; tracked with the large-store
safeguards). Until it ships, large stores migrate by declaring a memory budget, or by running on a host
that clears the single-txn budget.

## Deferred

- **Resumable phased migration** (per-table durable progress markers) for above-floor / undeclared-memory
  stores — deferred (tracked as crash-recovery / resumability follow-up #46).
- **Kill-at-every-boundary fault-injection tests** — deferred to the real-fixture hardening slice (#46).
- **Offline `pulsedb migrate` tool** implementation — deferred (#45).

## Schema v5 (0.8.0): sync cursors reset

**What changed.** The per-peer sync cursor used to be a single slot (`SyncCursor { instance_id,
last_sequence }`) that the push path and the pull path both overwrote. A remote *pull* position could
therefore land in the slot `compact_wal` trusted, and compaction deleted local events that had never
been pushed (issue #9). Schema v5 splits the record into
`SyncCursor { instance_id, push_sequence, pull_sequence }`: `push_sequence` is the local WAL sequence
the peer has acknowledged, `pull_sequence` is the remote WAL sequence applied locally, and
`compact_wal` uses `min(push_sequence)` only.

**Disk schema and sync protocol are independent version axes.** This migration is about the *stored*
record and is unaffected by the protocol. `SyncPosition { instance_id, sequence }` — whose bytes
still equal the old wire cursor — is the single-direction position carried in `PullRequest::cursor`
and `PullPage::scan_position`. The 0.8.0 sync protocol is **v5** and does not interoperate with v4
(see the CHANGELOG), but nothing about that changes the schema-v5 cursor migration described here.

**What happens on the first writable open of a schema-4 store** (every 0.7.x store):

1. **Durable sidecar, under ownership.** `<db>.pre-v5.bak` is a byte-for-byte copy of the file,
   published **before any migration write** and only while this process holds the migration lock —
   `.migrate.lock` is acquired before any writable redb handle and held through the schema-migration
   commit, so a second process opening the same store waits, then finds it already migrated. The
   copy is staged at a process-unique sibling temp, `fsync`ed, **validated as a whole image**
   (re-opened read-only: `schema_version` read back, then every table and multimap the image lists
   traversed and every entry read — a metadata-only check cannot see a copy torn by a concurrent
   writer's commit), and published by an atomic create-if-absent hard link, which never replaces an
   existing sidecar. On link-less volumes, an exclusive create-and-copy of the validated stage
   is synced before success; an interrupted copy is validated and quarantined on the next
   migrating open. Any failure — a failed fsync, a torn image, a failed publish — **refuses the
   migration** with a typed error; the store stays at its pre-migration schema and the temp is
   removed. On Unix, parent-directory open and fsync must also succeed before migration,
   including when an existing validated sidecar is kept on retry. On every platform,
   a kept sidecar's file contents must sync before the directory barrier; inability to open
   it with write access for sync (for example a read-only file on Windows), or a failed sync,
   refuses migration with a typed I/O error. Bare relative filenames use `.` as the parent. Windows cannot sync directories through this API; directory-entry
   durability remains best-effort there, while the file contents are synced. If the peek cannot
   run (crashed session, locked file) and the open discovers a pending
   migration, the copy is taken from the post-open bytes instead — a valid store, but not
   byte-identical; the open says so at `info!`. An **existing** sidecar is proof only after the same
   whole-image validation: a valid one is kept, and one that fails (does not open, wrong
   `schema_version`, or fails the traversal) is renamed to
   `<db>.pre-v5.bak.invalid-<unix-seconds>` (`-<n>` if taken) — logged at `warn` with both paths,
   never deleted — and a fresh image is published. (The same rules cover `.pre-v4.bak` for redb-v3
   schema-3 stores; redb-v2 stores keep `.pre-substrate.bak` as their pristine copy.)
   A crash between the staged copy and the publish leaves a store-sized
   `<db>.pre-vN.bak.<pid>.<nonce>.sidecar.tmp`. Nothing removes it; it is safe to delete when no
   migration is running.
2. **Cursor reset (single write transaction).** Every `sync_cursors` row is rewritten as
   `{ instance_id, push_sequence: 0, pull_sequence: 0 }`. The legacy `last_sequence` is **not** used
   to seed either side — it may hold a local *or* a remote sequence, and seeding from it could skip
   events. Each discarded value is logged at `warn` (one line per peer) as the audit trail. The store's
   `schema_version` becomes `5`; the substrate marker (`redb-v3, postcard`) is unchanged. The reset runs
   on builds **with and without** the `sync` feature (it goes through a feature-independent raw-table
   helper).
3. **Read-only opens refuse.** A `read_only` open of a not-yet-migrated schema-4 store returns the typed
   `ReadOnly` error and makes no PulseDB-level write (no migration lock, no sidecar, no metadata
   write), as for v3→v4.

**Consequences for operators.**

- **One full, idempotent resync per peer.** With both positions at 0, the next sync pushes the whole
  local WAL again and pulls the peer's WAL from the start; all sync applies are idempotent (G-counter
  merges, create-collision merges), so totals stay exact.
- **Compaction stays blocked until the next push to each peer** (`push_sequence == 0` blocks it). A
  peer this instance only ever pulls from (`SyncDirection::PullOnly`) keeps the WAL growing until a push
  happens — the conservative rule is deliberate (see `PulseDB::compact_wal`).
- **Events compacted before the upgrade are not recoverable.** If a pre-0.8.0 compaction already
  deleted unpushed events (the #9 failure), the migration cannot restore them; the resync covers only
  what is still in the WAL.
- **A second opener waits, then gets `DatabaseLocked`.** Other writable openers block on the
  migration lock with no timeout — a multi-minute migration must not fail its waiters — and a `warn!`
  carrying the lock path is logged every 30 s while blocked. The first process keeps its writable redb handle after
  `open` returns, so the waiter then gets the typed, retryable `DatabaseLocked` (ADR-003: one writable
  process) while that process holds the store. The waiter runs no migration and leaves the sidecar untouched. A
  read-only open takes no lock and creates no `.migrate.lock` file.
- **Rollback** (ADR-011): reinstall 0.7.x and restore `<db>.pre-v5.bak` over the store. A 0.7.x
  binary refuses a schema-5 store with `SchemaVersionMismatch`, so a downgrade without the restore
  fails loud rather than misreading cursors.
- **Check the rollback image before restoring it.** Open it read-only with the binary you are
  reinstalling (a 0.7.x `Config::read_only()` open of the schema-4 image) and confirm it reads. An
  image published by a 0.8.0 binary was validated `schema_version`-only when written and is **not
  re-validated later** — a schema-5 store needs no migration, so no open triggers a check
  (re-validating on every open would add a second read to the steady-state open, whose floor is the
  single metadata peek — see the top of this document). The whole-image
  validation above applies at publish and quarantine time, on the upgrading open.
