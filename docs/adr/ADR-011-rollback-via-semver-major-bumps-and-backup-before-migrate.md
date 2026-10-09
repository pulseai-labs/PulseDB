# ADR-011: Rollback via SemVer Major Bumps and Backup-Before-Migrate

## Status
Accepted

## Date
2026-08-23

## Context
PulseDB needs a rollback strategy for bad releases and schema migration paths.

## Decision
Versioning follows ADR-008: while pre-1.0 (now), **breaking changes bump MINOR**; after 1.0, breaking changes bump MAJOR. (One policy — this ADR defers to ADR-008 on pre-1.0 levels.) For storage format changes (redb major versions, serializer changes), a backup-before-migrate strategy is used: `.pre-substrate.bak` is written before any destructive migration. **Known exception:** the codec-only leg (an already-redb-v3 store still carrying the bincode-era substrate marker) re-encodes to postcard *without* writing a backup — by then the previous binary cannot read the rewritten values either, so `.pre-substrate.bak` restore is not the rollback for that leg; the operator's rollback there is restoring from their own external backup or re-creating from a pre-upgrade copy. Rollback means reinstalling the previous version and restoring from backup. The migration is idempotent — reopening a migrated store re-runs migration safely.

## Touch surface
`src/storage/redb.rs`, `src/lib.rs`, `Cargo.toml`

## Revisit trigger
Revisit when rollback strategy needs updating (e.g., point-in-time recovery).

## Amendment — Durable sidecar protocol

**2026-10-09 — r1.s6.w1 (#89, #25).** The backup-before-migrate rule above
assumed the sidecar was in place before the first destructive write. For the
logical-schema migrations on an already-redb-v3 store (the `.pre-v3.bak` /
`.pre-v4.bak` / `.pre-v5.bak` family) it now is, under these rules. The
redb-format substrate sidecar (`.pre-substrate.bak`) and its `backup_once`
mechanics are unchanged.

- **Validated before proof.** An existing sidecar is a rollback point only
  after whole-image validation: the image is opened read-only, its
  `schema_version` is read back and required to match the pre-migration
  version, and then **every** table and multimap the image lists is traversed
  and every entry read. A metadata-only `schema_version` check is not proof — a
  copy torn by a concurrent writer can keep a readable metadata row while other
  pages are damaged. Limit (redb 4.2): `ReadOnlyUntypedTable` exposes no
  untyped entry iteration, so each listed table gets the untyped page walk
  (`len()` / `stats()`) plus, for every table name this build knows, a typed
  full entry read; and redb verifies page checksums only on its
  repair/integrity paths, which ordinary reads do not take.
- **Durable before visible.** The staged copy is `fsync`ed (`File::sync_all`)
  before publication; a failed fsync refuses the migration with a typed error.
  After the publish the parent directory is fsync'd best-effort, where the
  platform supports it; an unsupported directory fsync is not fatal — the
  file's own bytes are already durable.
- **Never replace.** Publication is a create-if-absent hard link, not a rename.
  `rename` replaces an existing destination on both Unix and Windows, which
  would make "preserve an existing sidecar" a check-then-act race that could
  overwrite another process's genuine rollback point.
- **Ownership held through commit.** The migration lock (`.migrate.lock`,
  per ADR-003) is acquired before any writable redb handle on the migration
  path and held through the schema-migration commit (or released on error). A
  second opener waits — no timeout, since a multi-minute migration must not
  fail its waiters, with a `warn!` carrying the lock path every 30 s — and
  after the first commit finds the store already migrated. Read-only opens take
  no lock and create no `.migrate.lock`: they perform zero writes (FR-035).
- **Invalid sidecars are quarantined, never deleted.** An existing sidecar that
  fails whole-image validation is renamed to
  `.pre-vN.bak.invalid-<unix-seconds>` (`-<n>` if that name is taken), logged
  at `warn!` with both paths, and a fresh image is published from the
  still-unmigrated source. A failed quarantine rename refuses the migration
  with a typed error, and nothing destructive runs.
- **Mixed-version concurrent upgrades are unsupported.** These rules serialize
  two openers of the *same* PulseDB version; two different versions migrating
  one store concurrently remain outside the contract (B5).

**2026-10-09 — r1.s6.w2 (#95).** A steady-state writable open of a
current-schema store pays a stated, supported floor: one read-only open plus
one read of the `db_metadata` row, taken in a single read transaction as of
this item, measured on the reference machine at a few tens of microseconds and
inside NFR-001's 100 ms open budget with a wide margin. The floor replaces the
earlier "tracked follow-up" wording; the three cheaper alternatives were each
rejected, and none is to be reintroduced without revisiting this note:

- **Skip the peek when a `.pre-vN.bak` exists.** It never fires for a fresh v5
  store (which has no sidecar), and sidecar existence is not proof that the
  store is current: nothing deletes an older `.pre-vN.bak`, so a v4 store
  carrying a `.pre-v3.bak` (or a store whose needed image was quarantined)
  would skip the peek and publish its rollback image from post-open bytes —
  breaking the byte-identity this amendment requires.
- **A durable `<db>.schema` marker file.** A new persistent artifact plus a new
  invalidation mode: an operator restore can leave a stale marker, and the
  design then needs a typed refusal in `open_existing` and a delete step in the
  ADR-011 rollback path — a new failure mode for a few microseconds.
- **Feed the peek's `schema_version` forward to the writable handle.** Saves
  nothing: the re-read on the writable handle is the only check made under the
  exclusive lock, and it is what catches a newer binary that migrated the store
  in the gap (`validate_existing_metadata` → `SchemaVersionMismatch`). Trusting
  the pre-open snapshot there would let this build rewrite v5 metadata into a
  future-version store.
