# fixture-gen-v0_7_0 — real `pulsehive-db =0.7.0` (schema-v4 + sync cursor) golden-fixture generator

Generates the **real prior-release** on-disk store `tests/fixtures/real-v0.7.0.redb`
+ its provenance manifest `tests/fixtures/real-v0.7.0.manifest.json`, built by the
**published** `pulsehive-db =0.7.0` crate resolved from crates.io — NOT synthesized
in-tree. It is the golden fixture for the **schema v4 → v5** migration (r1.s1.w1,
issue #9): the per-peer sync cursor is split into separate push and pull positions,
and every existing cursor is reset to `{push_sequence: 0, pull_sequence: 0}` under
backup-before-migrate (`.pre-v5.bak`, ADR-011).

## Regenerate

```bash
# from anywhere in the repo — regenerates ALL real fixtures + manifests:
tools/regen-fixtures.sh
```

Regeneration is **reproducible-with-drift**: each run mints new UUIDv7 ids +
timestamps, so it produces a **new `blob_sha256`**. The committed `.redb` blob and
its manifest are a matched, **frozen** pair — regen is for provenance/audit, not a
byte-for-byte reproduction. The generator is committed for provenance and is **NOT**
run in CI; `tests/storage_format_upgrade.rs` runs the upgrade tests against the
frozen blob.

## Build isolation (audit C9) — OUTSIDE the production workspace

- This crate's `Cargo.toml` carries an **empty `[workspace]` table**, making it its
  own workspace root. Cargo never walks up into the production root manifest.
- It is built **only** via an explicit `--manifest-path tools/fixture-gen-v0_7_0/Cargo.toml`
  (see `regen-fixtures.sh`), and its build `target/` defaults **outside** the repo.
- The production root `Cargo.toml` is a single `[package]` manifest with **NO
  `[workspace]` section and NO `exclude` entry**, and it stays that way.
- `pulsehive-db` is pinned **exactly** (`=0.7.0`) so the generator resolves the
  published crates.io artifact — never a local path override. The `sync` feature is
  on (no `builtin-embeddings` → no ONNX/`ort`); embeddings are injected as raw f32
  vectors via the **External** embedding provider.

## Shape coverage (v0.7.0, schema-v4)

The same entity set as the v0.5.1 fixture, plus what 0.7.0 added:
- **collectives** (incl. an owner via `create_collective_with_owner`)
- **experiences** across every `ExperienceType` variant, each with a **384-d
  embedding** and (0.7.0) **key-value `tags`** on some of them — populating the raw-f32
  `embeddings` table and the `experiences_by_collective` / `experiences_by_type` /
  `experiences_by_tag` secondary multimap indexes
- **experience-relations**, **derived insights**, **watch events**, **decay config**,
  **db_metadata** (`schema_version` = 4)
- **sync cursor** — the point of this fixture: one `SyncManager::sync_once` over the
  in-memory transport persists a genuine 0.7.0 `SyncCursor { instance_id, last_sequence }`
  row with `last_sequence > 0`. The manifest records the peer id, the value, and the
  **raw on-disk bytes** of that row so the v5 test can prove the reset.

`instance_id` is present and must be **preserved** across migration.

### Migration axes exercised
v0.7.0 is already `{redb-v3, postcard, schema-v4}`, so this fixture exercises **only**
the logical-schema axis (v4 → v5: the sync-cursor reshape + reset) and the
`.pre-v5.bak` sidecar. The substrate marker is unchanged by that migration.

## Manifest = mechanical provenance + ground truth

Same layout as the v0.5.1 manifest (`blob_sha256`, `generator_git_commit`,
`generator_cargo_lock_sha256`, `resolved_dependency_checksums`, `feature_set`,
`build_env`, typed read-back values, raw copy-through bytes, search ground truth),
plus a `sync_cursor` object: `{ peer_instance_id, last_sequence, raw_key_hex,
raw_value_bytes_hex }`.
