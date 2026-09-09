//! VS-4.0.4 (4.02) — real prior-release golden-fixture upgrade test. THE hard
//! sprint→main-PR gate (MIGRATE-020 / NFR-020): it opens the two REAL frozen
//! prior-release stores produced by 4.01 (`real-v0.5.1.redb`, `real-v0.4.0.redb`)
//! under the CURRENT PulseDB (redb-4.x + postcard), driving the full on-open
//! migration end-to-end, and asserts every entity reads back identical to each
//! fixture's committed manifest oracle. Passing this promotes VS-4.0.3's
//! *provisional* bincode-crate drop into a proven migration guarantee.
//!
//! ## Axis coverage (asymmetric by design — why BOTH fixtures ship)
//! - `real-v0.5.1.redb` is ALREADY logical-schema-v3 → exercises axis-1 (redb
//!   file-format v2→v3) + axis-2 (bincode→postcard codec) on a real artifact.
//! - `real-v0.4.0.redb` is schema-v2 → exercises ALL THREE axes, including
//!   axis-3 (`migrate_experiences_v2_to_v3` + `migrate_wal_v1_to_v2` reshape).
//!
//! ## Fidelity levels (do NOT overclaim uniform byte-identity — close-depth C4/C14)
//! - RAW byte-identity: the EMBEDDINGS raw-f32 rows + raw metadata keys are
//!   copy-through (§2a matrix) and are asserted BYTE-identical against the
//!   manifest's captured raw bytes, read from the migrated store via redb-4.x
//!   directly (the `raw_table_bytes` / `raw_embeddings` inspectors below). A
//!   defense-in-depth cross-check re-encodes the decoded embedding LE
//!   (`to_le_bytes`) and compares it to the same raw bytes.
//! - Field-level VALUE identity: collectives / experiences / relations / insights
//!   read back field-identical for the migration-stable fields. Reshape-derived
//!   fields (`applications` scalar→G-counter, synthesized `last_reinforced`) are
//!   NOT compared against the pre-migration manifest (they legitimately change on
//!   the v0.4.0 v2→v3 reshape); schema_version=3 post-migration is asserted instead.
//! - BEHAVIORAL / lookup-equivalence for the secondary multimap indexes: redb
//!   multimap value-order / page layout is not a stable migration contract, so
//!   raw multimap bytes are NOT asserted; instead every expected index membership
//!   resolves through the public read-back surface (`get_relation_ids_by_source` /
//!   `_by_target`; experiences present in each collective listing).
//! - SEARCH-RESULT equivalence for HNSW: the index rebuilds from redb on open
//!   (`src/db.rs:200`, #18), so HNSW internals are NOT asserted — a fixed query
//!   (captured in the manifest) run through `search_similar` must return the
//!   manifest's captured neighbor experience ids.
//!
//! ## Provenance guard (close-depth C1)
//! Per fixture, the committed blob's SHA-256 is verified against the manifest's
//! `blob_sha256` BEFORE copying/opening — a substituted/corrupted blob fails
//! loudly before migration runs, so the oracle can never silently drift.
//!
//! ## Falsification (close-depth C10)
//! `manifest_corruption_is_detected` proves the byte-identity oracle actually
//! bites; `truncated_fixture_fails_explicitly` proves a truncated store fails
//! loudly rather than opening silently.
//!
//! ## Residual gap (documented, NOT closed)
//! Neither fixture covers **v0.3.0 / WAL-v1** (the logical schema before v2).
//! v0.4.0 is already schema-v2 (WAL-v2); a genuine WAL-v1 / v0.3.0 artifact is a
//! known residual, not synthesized here (see the VS-4.0.4 spec + issue backlog).
//!
//! NOTE (round-ordering): the 4.02 spec assumed a `src/`-side `raw_table_bytes`
//! inspector added by 4.04; 4.04 did not add it. Rather than edit `src/` here
//! (out of this item's charter, and it would entangle with 4.05's redb.rs work),
//! the RAW byte-identity level re-opens the MIGRATED store read-only via a
//! `redb = "4.1"` dev-dependency — the same technique 4.01's generator used. The
//! guarantee is identical; the inspector is named `raw_table_bytes` per the AC.

mod common;

use common::{copy_fixture, fixtures_dir};
use pulsedb::{CollectiveId, Config, ExperienceId, InsightId, PulseDB, RelationId};
use redb::{ReadableDatabase, ReadableTable, TableDefinition};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

// Copy-through tables, mirrored from the current `src/storage/schema.rs`.
const EMBEDDINGS: TableDefinition<&[u8; 16], &[u8]> = TableDefinition::new("embeddings");
const METADATA: TableDefinition<&str, &[u8]> = TableDefinition::new("metadata");

// The serializer-independent substrate marker (VS-4.0.2): a 3-byte `[b'P', b'S',
// <format>]` value under the `substrate_format` metadata key, read before any
// serde decode. `<format> = 2` is the postcard era; migration writes it as the
// atomic commit point.
const SUBSTRATE_MARKER: [u8; 3] = [b'P', b'S', 2];

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn uuid_str(b: &[u8]) -> String {
    let h = hex(b);
    if h.len() == 32 {
        format!(
            "{}-{}-{}-{}-{}",
            &h[0..8],
            &h[8..12],
            &h[12..16],
            &h[16..20],
            &h[20..32]
        )
    } else {
        h
    }
}

fn load_manifest(name: &str) -> Value {
    let s = std::fs::read_to_string(fixtures_dir().join(name))
        .unwrap_or_else(|e| panic!("read manifest {name}: {e}"));
    serde_json::from_str(&s).unwrap_or_else(|e| panic!("parse manifest {name}: {e}"))
}

fn to_uuid(s: &str) -> uuid::Uuid {
    uuid::Uuid::parse_str(s).unwrap_or_else(|e| panic!("bad uuid {s}: {e}"))
}

/// Serialize a typed read-back entity and assert the migration-STABLE field
/// subset equals the manifest's captured values.
fn assert_fields_eq(label: &str, manifest: &Value, readback: &Value, keys: &[&str]) {
    for k in keys {
        let m = manifest.get(*k).unwrap_or(&Value::Null);
        let r = readback.get(*k).unwrap_or(&Value::Null);
        assert_eq!(
            m, r,
            "{label}: field `{k}` differs (manifest={m}, migrated={r})"
        );
    }
}

/// RAW on-disk value bytes for a key in the `metadata` table of the MIGRATED
/// store, read via redb-4.x directly (the store must be closed first).
fn raw_table_bytes(store: &Path, key: &str) -> Option<Vec<u8>> {
    let db = redb::Database::open(store).expect("reopen migrated store (redb 4.x)");
    let rtx = db.begin_read().unwrap();
    let t = rtx.open_table(METADATA).unwrap();
    let v = t.get(key).unwrap().map(|g| g.value().to_vec());
    v
}

/// RAW on-disk EMBEDDINGS rows (experience-id → value bytes) of the MIGRATED
/// store, read via redb-4.x directly (the store must be closed first).
fn raw_embeddings(store: &Path) -> BTreeMap<String, Vec<u8>> {
    let db = redb::Database::open(store).expect("reopen migrated store (redb 4.x)");
    let rtx = db.begin_read().unwrap();
    let t = rtx.open_table(EMBEDDINGS).unwrap();
    let mut out = BTreeMap::new();
    for row in t.iter().unwrap() {
        let (k, v) = row.unwrap();
        out.insert(uuid_str(k.value()), v.value().to_vec());
    }
    out
}

enum InstanceIdMode {
    /// v0.5.1 persisted an instance_id → migration must PRESERVE the exact value.
    Preserve,
    /// v0.4.0 schema-v2 (default features) had none (sync-gated) → migration MINTS one.
    Minted,
}

struct Fixture {
    redb: &'static str,
    manifest: &'static str,
    instance_id: InstanceIdMode,
}

// Fields identical across the migration for every fixture (excludes the
// reshape-derived `applications` / `last_reinforced` + serde-skipped `embedding`).
const EXP_STABLE: &[&str] = &[
    "id",
    "collective_id",
    "content",
    "experience_type",
    "importance",
    "confidence",
    "domain",
    "related_files",
    "source_agent",
    "source_task",
    "timestamp",
    "archived",
];
const COLL_STABLE: &[&str] = &[
    "id",
    "name",
    "owner_id",
    "embedding_dimension",
    "created_at",
    "updated_at",
];
const REL_STABLE: &[&str] = &[
    "id",
    "source_id",
    "target_id",
    "relation_type",
    "strength",
    "metadata",
    "created_at",
];
const INS_STABLE: &[&str] = &[
    "id",
    "collective_id",
    "content",
    "insight_type",
    "confidence",
    "domain",
    "source_experience_ids",
    "created_at",
    "updated_at",
];

/// Returns the migrated temp store (and its guard) so fixture-specific checks can
/// run raw reads against it after the shared verification.
fn verify_fixture(fx: &Fixture) -> (tempfile::TempDir, PathBuf) {
    let manifest = load_manifest(fx.manifest);

    // ---- C1 provenance guard: committed blob SHA-256 == manifest, BEFORE opening.
    let committed = std::fs::read(fixtures_dir().join(fx.redb)).unwrap();
    assert_eq!(
        sha256_hex(&committed),
        manifest["blob_sha256"].as_str().unwrap(),
        "{}: committed fixture SHA-256 != manifest (provenance drift)",
        fx.redb
    );

    // ---- migrate via the public open path (fires all three axes).
    let (tmp, store) = copy_fixture(fx.redb);
    let db = PulseDB::open(&store, Config::default())
        .unwrap_or_else(|e| panic!("{}: migrate+open failed: {e:?}", fx.redb));
    assert_eq!(
        db.metadata().schema_version,
        5,
        "{}: expected schema v5 post-migration",
        fx.redb
    );

    let storage = db.storage_for_test();

    // ---- collectives: field-level value identity.
    for cm in manifest["collectives"].as_array().unwrap() {
        let id = CollectiveId(to_uuid(cm["id"].as_str().unwrap()));
        let got = storage
            .get_collective(id)
            .unwrap()
            .unwrap_or_else(|| panic!("{}: collective {id:?} missing after migration", fx.redb));
        assert_fields_eq(
            &format!("{} collective {}", fx.redb, cm["name"]),
            cm,
            &serde_json::to_value(&got).unwrap(),
            COLL_STABLE,
        );
    }

    // ---- experiences: field-level value identity + RAW/decoded embedding byte-identity.
    let raw_emb_manifest: BTreeMap<String, String> = manifest["raw_stored_bytes"]["embeddings"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| {
            (
                e["experience_id"].as_str().unwrap().to_string(),
                e["value_bytes_hex"].as_str().unwrap().to_string(),
            )
        })
        .collect();

    // group experiences by collective for the BEHAVIORAL by-collective index check.
    let mut expected_by_collective: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

    for em in manifest["experiences"].as_array().unwrap() {
        let id_str = em["id"].as_str().unwrap().to_string();
        let id = ExperienceId(to_uuid(&id_str));
        let got = db
            .get_experience(id)
            .unwrap()
            .unwrap_or_else(|| panic!("{}: experience {id_str} missing after migration", fx.redb));
        assert_fields_eq(
            &format!("{} experience {id_str}", fx.redb),
            em,
            &serde_json::to_value(&got).unwrap(),
            EXP_STABLE,
        );
        expected_by_collective
            .entry(em["collective_id"].as_str().unwrap().to_string())
            .or_default()
            .insert(id_str.clone());

        // decoded embedding LE re-encode == manifest raw bytes (byte-identity cross-check).
        let decoded = storage
            .get_embedding(id)
            .unwrap()
            .unwrap_or_else(|| panic!("{}: embedding {id_str} missing", fx.redb));
        let le: Vec<u8> = decoded.iter().flat_map(|f| f.to_le_bytes()).collect();
        let want = raw_emb_manifest
            .get(&id_str)
            .unwrap_or_else(|| panic!("{}: manifest has no raw embedding for {id_str}", fx.redb));
        assert_eq!(
            hex(&le),
            *want,
            "{}: decoded embedding LE bytes != manifest raw bytes for {id_str}",
            fx.redb
        );
    }

    // ---- relations: value identity + BEHAVIORAL by-source index lookup-equivalence.
    for rm in manifest["relations"].as_array().unwrap() {
        let rid_str = rm["id"].as_str().unwrap().to_string();
        let rid = RelationId(to_uuid(&rid_str));
        let got = db
            .get_relation(rid)
            .unwrap()
            .unwrap_or_else(|| panic!("{}: relation {rid_str} missing after migration", fx.redb));
        assert_fields_eq(
            &format!("{} relation {rid_str}", fx.redb),
            rm,
            &serde_json::to_value(&got).unwrap(),
            REL_STABLE,
        );
        let src = ExperienceId(to_uuid(rm["source_id"].as_str().unwrap()));
        let by_source = storage.get_relation_ids_by_source(src).unwrap();
        assert!(
            by_source.contains(&rid),
            "{}: relations_by_source index missing {rid_str} for source {:?}",
            fx.redb,
            src
        );
        let tgt = ExperienceId(to_uuid(rm["target_id"].as_str().unwrap()));
        let by_target = storage.get_relation_ids_by_target(tgt).unwrap();
        assert!(
            by_target.contains(&rid),
            "{}: relations_by_target index missing {rid_str} for target {:?}",
            fx.redb,
            tgt
        );
    }

    // ---- insights: value identity (embedding excluded — f32 precision; copy-through
    //      byte-identity is proven on the experience EMBEDDINGS table).
    for im in manifest["insights"].as_array().unwrap() {
        let iid_str = im["id"].as_str().unwrap().to_string();
        let iid = InsightId(to_uuid(&iid_str));
        let got = db
            .get_insight(iid)
            .unwrap()
            .unwrap_or_else(|| panic!("{}: insight {iid_str} missing after migration", fx.redb));
        assert_fields_eq(
            &format!("{} insight {iid_str}", fx.redb),
            im,
            &serde_json::to_value(&got).unwrap(),
            INS_STABLE,
        );
    }

    // ---- BEHAVIORAL experiences-by-collective index lookup-equivalence.
    for (cid_str, expected_ids) in &expected_by_collective {
        let cid = CollectiveId(to_uuid(cid_str));
        let listed: BTreeSet<String> = db
            .list_experiences(cid, 10_000, 0)
            .unwrap()
            .iter()
            .map(|e| {
                serde_json::to_value(e.id)
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect();
        for want in expected_ids {
            assert!(
                listed.contains(want),
                "{}: experiences_by_collective index missing {want} in collective {cid_str}",
                fx.redb
            );
        }
    }

    // ---- SEARCH-RESULT equivalence (C14): fixed manifest query → captured neighbor ids.
    let es = &manifest["expected_search"];
    let cid = CollectiveId(to_uuid(es["collective_id"].as_str().unwrap()));
    let query: Vec<f32> = es["query_embedding_f32"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_f64().unwrap() as f32)
        .collect();
    let k = es["k"].as_u64().unwrap() as usize;
    let got_ids: Vec<String> = db
        .search_similar(cid, &query, k)
        .unwrap()
        .iter()
        .map(|r| {
            serde_json::to_value(r.experience.id)
                .unwrap()
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    let want_ids: Vec<String> = es["top_k"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["experience_id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        got_ids, want_ids,
        "{}: post-migration search_similar neighbor ids differ from manifest (C14)",
        fx.redb
    );

    // ---- close the store, then RAW copy-through byte-identity via redb-4.x direct read.
    drop(db);

    let raw = raw_embeddings(&store);
    for (id_str, want_hex) in &raw_emb_manifest {
        let got = raw
            .get(id_str)
            .unwrap_or_else(|| panic!("{}: migrated EMBEDDINGS missing {id_str}", fx.redb));
        assert_eq!(
            &hex(got),
            want_hex,
            "{}: EMBEDDINGS raw on-disk bytes not byte-identical after migration for {id_str}",
            fx.redb
        );
    }

    // ---- RAW metadata keys: substrate_format marker + instance_id semantics.
    let marker = raw_table_bytes(&store, "substrate_format").unwrap_or_else(|| {
        panic!(
            "{}: substrate_format marker missing post-migration",
            fx.redb
        )
    });
    assert_eq!(
        marker,
        SUBSTRATE_MARKER.to_vec(),
        "{}: substrate_format marker not at the current [P,S,2] postcard value post-migration",
        fx.redb
    );

    let instance_id = raw_table_bytes(&store, "instance_id");
    match fx.instance_id {
        InstanceIdMode::Preserve => {
            let got = instance_id
                .unwrap_or_else(|| panic!("{}: instance_id lost across migration", fx.redb));
            assert_eq!(
                uuid_str(&got),
                manifest["instance_id"].as_str().unwrap(),
                "{}: instance_id not PRESERVED across migration",
                fx.redb
            );
        }
        InstanceIdMode::Minted => {
            assert!(
                manifest["instance_id"].is_null(),
                "{}: v0.4.0 manifest should record instance_id as absent (null)",
                fx.redb
            );
            let got = instance_id.unwrap_or_else(|| {
                panic!(
                    "{}: instance_id should be MINTED during v2→v3 migration",
                    fx.redb
                )
            });
            assert_eq!(
                got.len(),
                16,
                "{}: minted instance_id must be a 16-byte uuid",
                fx.redb
            );
        }
    }

    (tmp, store)
}

// ---------------------------------------------------------------------------
// positive tests — the headline gate (both real fixtures migrate identically)
// ---------------------------------------------------------------------------

#[test]
fn real_v0_5_1_upgrades_value_and_byte_identical() {
    verify_fixture(&Fixture {
        redb: "real-v0.5.1.redb",
        manifest: "real-v0.5.1.manifest.json",
        instance_id: InstanceIdMode::Preserve,
    });
}

#[test]
fn real_v0_4_0_upgrades_value_and_byte_identical() {
    verify_fixture(&Fixture {
        redb: "real-v0.4.0.redb",
        manifest: "real-v0.4.0.manifest.json",
        instance_id: InstanceIdMode::Minted,
    });
}

// ---------------------------------------------------------------------------
// negative / falsification tests (close-depth C10) — the harness must bite
// ---------------------------------------------------------------------------

#[test]
fn manifest_corruption_is_detected() {
    // Prove the byte-identity oracle actually bites: migrate for real, read the
    // genuine on-disk embedding bytes, then show a one-nibble-corrupted expected
    // value does NOT equal them (an equality oracle would FAIL on it).
    let (_tmp, store) = copy_fixture("real-v0.5.1.redb");
    {
        let db = PulseDB::open(&store, Config::default()).unwrap();
        drop(db);
    }
    let raw = raw_embeddings(&store);
    let manifest = load_manifest("real-v0.5.1.manifest.json");
    let first = &manifest["raw_stored_bytes"]["embeddings"][0];
    let id = first["experience_id"].as_str().unwrap();
    let genuine = hex(raw.get(id).expect("genuine embedding bytes"));

    let mut corrupted: Vec<u8> = genuine.clone().into_bytes();
    corrupted[0] = if corrupted[0] == b'a' { b'b' } else { b'a' };
    let corrupted = String::from_utf8(corrupted).unwrap();

    assert_ne!(
        corrupted, genuine,
        "a corrupted manifest expectation MUST differ from the genuine on-disk bytes \
         (proves the byte-identity oracle can fail — it is not vacuous)"
    );
}

#[test]
fn truncated_fixture_fails_explicitly() {
    // A truncated store must fail LOUDLY (Err or panic), never open silently.
    let (_tmp, store) = copy_fixture("real-v0.4.0.redb");
    {
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(&store)
            .unwrap();
        f.set_len(8192).unwrap(); // brutal truncation → corrupt redb file
    }
    let outcome = std::panic::catch_unwind(|| PulseDB::open(&store, Config::default()));
    let opened_silently = matches!(outcome, Ok(Ok(_)));
    assert!(
        !opened_silently,
        "a truncated fixture must fail explicitly (Err/panic), not open silently"
    );
}

// ---------------------------------------------------------------------------
// r1.s1.w1 (#9) — real v0.7.0 (schema-v4, sync cursor present) → schema v5
// ---------------------------------------------------------------------------

/// Copy-through mirror of the `sync_cursors` table (`src/storage/schema.rs`),
/// declared locally so the raw-bytes check is feature-independent.
const SYNC_CURSORS: TableDefinition<&[u8; 16], &[u8]> = TableDefinition::new("sync_cursors");

/// The postcard encoding of a schema-v5 `SyncCursor { instance_id, push_sequence: 0,
/// pull_sequence: 0 }`: `varint(16) ‖ uuid bytes ‖ varint(0) ‖ varint(0)`.
fn v5_reset_cursor_bytes(peer: &uuid::Uuid) -> Vec<u8> {
    let mut v = vec![0x10];
    v.extend_from_slice(peer.as_bytes());
    v.extend_from_slice(&[0x00, 0x00]);
    v
}

/// RAW on-disk `sync_cursors` rows (peer-id → value bytes) of the MIGRATED store.
fn raw_sync_cursors(store: &Path) -> BTreeMap<String, Vec<u8>> {
    let db = redb::Database::open(store).expect("reopen migrated store (redb 4.x)");
    let rtx = db.begin_read().unwrap();
    let t = rtx.open_table(SYNC_CURSORS).unwrap();
    let mut out = BTreeMap::new();
    for row in t.iter().unwrap() {
        let (k, v) = row.unwrap();
        out.insert(uuid_str(k.value()), v.value().to_vec());
    }
    out
}

/// Shared v5 assertions for the real v0.7.0 fixture, feature-independent:
/// `.pre-v5.bak` byte-identical to the committed fixture, `schema_version == 5`,
/// and the legacy `{instance_id, last_sequence}` cursor row rewritten to
/// `{instance_id, 0, 0}` (grill Q1: both positions reset, never seeded).
fn assert_v0_7_0_migrated_to_v5(store: &Path, manifest: &Value) {
    // ---- backup-before-migrate (ADR-011): the pristine pre-v5 sidecar is the fixture, byte for byte.
    let sidecar = store.with_file_name("real-v0.7.0.redb.pre-v5.bak");
    let sidecar_bytes = std::fs::read(&sidecar)
        .unwrap_or_else(|e| panic!("`.pre-v5.bak` sidecar missing after migration: {e}"));
    let committed = std::fs::read(fixtures_dir().join("real-v0.7.0.redb")).unwrap();
    if sidecar_bytes != committed {
        let first_diff = sidecar_bytes
            .iter()
            .zip(committed.iter())
            .position(|(a, b)| a != b)
            .unwrap_or(sidecar_bytes.len().min(committed.len()));
        let diff_count = sidecar_bytes
            .iter()
            .zip(committed.iter())
            .filter(|(a, b)| a != b)
            .count()
            + sidecar_bytes.len().abs_diff(committed.len());
        let lo = first_diff.saturating_sub(8);
        let hi = (first_diff + 24).min(sidecar_bytes.len().min(committed.len()));
        panic!(
            ".pre-v5.bak is not byte-identical to the committed v0.7.0 fixture: \
             sidecar {} bytes, fixture {} bytes, {} differing bytes, first at offset {first_diff}; \
             sidecar[{lo}..{hi}]={} fixture[{lo}..{hi}]={}",
            sidecar_bytes.len(),
            committed.len(),
            diff_count,
            hex(&sidecar_bytes[lo..hi]),
            hex(&committed[lo..hi]),
        );
    }

    // ---- logical schema: v5, and idempotent on reopen.
    let db = PulseDB::open(store, Config::default()).expect("reopen migrated store");
    assert_eq!(
        db.metadata().schema_version,
        5,
        "expected schema v5 post-migration"
    );
    drop(db);

    // ---- the legacy cursor row is reset, not seeded (raw bytes, no `sync` feature needed).
    let legacy = &manifest["sync_cursor"];
    let peer = legacy["peer_instance_id"].as_str().unwrap();
    assert!(
        legacy["last_sequence"].as_u64().unwrap() > 0,
        "fixture precondition: the v0.7.0 store must carry a NON-ZERO legacy cursor"
    );
    let raw = raw_sync_cursors(store);
    let got = raw
        .get(peer)
        .unwrap_or_else(|| panic!("sync cursor for peer {peer} lost across the v5 migration"));
    assert_eq!(
        hex(got),
        hex(&v5_reset_cursor_bytes(&to_uuid(peer))),
        "migrated sync cursor must be `{{instance_id, push_sequence: 0, pull_sequence: 0}}`"
    );
    assert_ne!(
        hex(got),
        legacy["raw_value_bytes_hex"].as_str().unwrap(),
        "migrated cursor bytes still equal the legacy v4 encoding"
    );
}

#[test]
fn real_v0_7_0_sync_cursor_store_upgrades_to_v5() {
    let manifest = load_manifest("real-v0.7.0.manifest.json");
    let (_tmp, store) = verify_fixture(&Fixture {
        redb: "real-v0.7.0.redb",
        manifest: "real-v0.7.0.manifest.json",
        instance_id: InstanceIdMode::Preserve,
    });
    assert_v0_7_0_migrated_to_v5(&store, &manifest);

    // Under `sync`, the typed port sees the same reset record.
    #[cfg(feature = "sync")]
    {
        let db = PulseDB::open(&store, Config::default()).unwrap();
        let cursors = db.storage_for_test().list_sync_cursors().unwrap();
        assert_eq!(cursors.len(), 1, "exactly one migrated peer cursor");
        let peer = to_uuid(
            manifest["sync_cursor"]["peer_instance_id"]
                .as_str()
                .unwrap(),
        );
        assert_eq!(cursors[0].instance_id.0, peer);
        assert_eq!(
            cursors[0].push_sequence, 0,
            "push position reset to 0 (grill Q1)"
        );
        assert_eq!(
            cursors[0].pull_sequence, 0,
            "pull position reset to 0 (grill Q1)"
        );
        // A reset push position keeps compaction blocked until a real push.
        assert_eq!(db.compact_wal().unwrap(), 0);
    }
}

/// The v4→v5 cursor reset runs through a feature-independent raw helper: a build
/// WITHOUT `sync` (where the cursor table type is cfg'd out) must still migrate
/// the store to v5 and reset the row (AC-3 runs this under default features).
///
/// Gated to a non-`sync` build so it covers what its name promises. Under
/// `--features sync` it would silently re-run
/// `real_v0_7_0_sync_cursor_store_upgrades_to_v5`, and a regression in the
/// non-sync leg would still show green.
#[test]
#[cfg(not(feature = "sync"))]
fn real_v0_7_0_opens_without_sync_feature() {
    let manifest = load_manifest("real-v0.7.0.manifest.json");
    let (_tmp, store) = copy_fixture("real-v0.7.0.redb");
    {
        let db = PulseDB::open(&store, Config::default())
            .unwrap_or_else(|e| panic!("real-v0.7.0.redb: migrate+open failed: {e:?}"));
        assert_eq!(db.metadata().schema_version, 5);
    }
    assert_v0_7_0_migrated_to_v5(&store, &manifest);
}
