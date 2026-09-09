//! Fixture generator for the REAL published `pulsehive-db =0.7.0` (schema-v4) store
//! carrying a persisted sync cursor.
//!
//! Produces a comprehensive real prior-release `.redb` on-disk store using ONLY
//! the published 0.7.0 public API (`sync` feature on; no ONNX — embeddings are
//! injected as raw f32 via the External provider), runs ONE in-memory sync so a
//! genuine schema-v4 `SyncCursor { instance_id, last_sequence }` row with
//! `last_sequence > 0` is persisted, then emits a CONTENT manifest of ground-truth
//! values + genuine on-disk copy-through raw bytes (+ the raw cursor row).
//! Environment provenance (blob SHA-256, git commit, Cargo.lock hash, dep
//! checksums, rustc) is layered on by `finalize_manifest.py` (see ../README +
//! regen-fixtures.sh).
//!
//! Usage: `fixture-gen-v0_7_0 <out.redb> <out.content.json>`
//!
//! v0.7.0 is ALREADY `{redb-v3, postcard, schema-v4}`, so this fixture exercises
//! only the logical-schema axis (v4→v5: the push/pull sync-cursor split, r1.s1.w1
//! / issue #9) and the `.pre-v5.bak` backup-before-migrate sidecar.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use pulsedb::sync::{InMemorySyncTransport, SyncConfig, SyncManager};
use pulsedb::{
    AgentId, Config, DecayConfig, EmbeddingDimension, ExperienceType, InsightType,
    NewDerivedInsight, NewExperience, NewExperienceRelation, PulseDB, RelationType, Severity,
};
use redb::{
    MultimapTableDefinition, ReadableDatabase, ReadableMultimapTable, ReadableTable,
    TableDefinition,
};
use serde_json::{json, Value};

// Copy-through table definitions, mirrored VERBATIM from pulsehive-db 0.7.0
// `src/storage/schema.rs`, so redb re-opens them by name+type for genuine
// on-disk raw-byte extraction.
const METADATA: TableDefinition<&str, &[u8]> = TableDefinition::new("metadata");
const EMBEDDINGS: TableDefinition<&[u8; 16], &[u8]> = TableDefinition::new("embeddings");
const EXP_BY_COLLECTIVE: MultimapTableDefinition<&[u8; 16], &[u8; 24]> =
    MultimapTableDefinition::new("experiences_by_collective");
const EXP_BY_TYPE: MultimapTableDefinition<&[u8; 17], &[u8; 16]> =
    MultimapTableDefinition::new("experiences_by_type");
const EXP_BY_TAG: MultimapTableDefinition<&[u8], &[u8; 16]> =
    MultimapTableDefinition::new("experiences_by_tag");
// The schema-v4 sync cursor table: key = peer InstanceId bytes, value =
// postcard `SyncCursor { instance_id, last_sequence }` (0.7.0 shape).
const SYNC_CURSORS: TableDefinition<&[u8; 16], &[u8]> = TableDefinition::new("sync_cursors");

const INSTANCE_ID_KEY: &str = "instance_id";
const QUERY_SEED: u32 = 4242;

/// Deterministic-but-nontrivial 384-d embedding for a given seed. Deterministic
/// so the store's vector content is reproducible-with-drift (only UUIDv7 ids +
/// timestamps vary per regen — which is why the blob is FROZEN + manifested).
fn emb(seed: u32) -> Vec<f32> {
    (0..384u32)
        .map(|i| (i.wrapping_mul(2_654_435_761).wrapping_add(seed) % 1000) as f32 / 1000.0)
        .collect()
}

fn hexs(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        s.push_str(&format!("{:02x}", x));
    }
    s
}

fn uuid_str(b: &[u8]) -> String {
    let h = hexs(b);
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

fn tags(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let redb_path = args
        .next()
        .ok_or("usage: fixture-gen-v0_7_0 <out.redb> <out.content.json>")?;
    let content_path = args
        .next()
        .ok_or("usage: fixture-gen-v0_7_0 <out.redb> <out.content.json>")?;

    // Fresh generation: remove any stale blob + derived HNSW sidecar dir.
    let _ = std::fs::remove_file(&redb_path);
    let _ = std::fs::remove_dir_all(format!("{}.hnsw", redb_path));
    if let Some(p) = std::path::Path::new(&redb_path).parent() {
        std::fs::create_dir_all(p)?;
    }

    // ---- WRITE PHASE (published 0.7.0 public API) ----
    let mut cfg = Config::with_external_embeddings(EmbeddingDimension::D384);
    // Distinctive (non-default) decay config so the recorded decay ground truth
    // is unambiguous.
    let decay = DecayConfig {
        half_life: Duration::from_secs(7 * 24 * 3600),
        freq_weight: 0.3,
        floor: 0.05,
        auto_archive_below_floor: true,
        default_recall_weights: None,
    };
    cfg.decay = decay.clone();

    let db = PulseDB::open(&redb_path, cfg)?;

    let c_eng = db.create_collective("engineering")?;
    let c_res = db.create_collective_with_owner("research", "owner-alpha")?;

    // Comprehensive experience coverage: every ExperienceType variant, spread
    // across both collectives, each carrying a distinct 384-d embedding, and
    // (0.7.0) key-value tags on some of them so the by-tag index populates.
    let mut recorded: Vec<pulsedb::ExperienceId> = Vec::new();
    let push = |db: &PulseDB,
                cid: pulsedb::CollectiveId,
                seed: u32,
                etype: ExperienceType,
                content: &str,
                tags: BTreeMap<String, String>|
     -> Result<pulsedb::ExperienceId, Box<dyn std::error::Error>> {
        let id = db.record_experience(NewExperience {
            collective_id: cid,
            content: content.to_string(),
            experience_type: etype,
            embedding: Some(emb(seed)),
            importance: 0.6,
            confidence: 0.7,
            domain: vec!["golden".to_string(), "fixture".to_string()],
            tags,
            related_files: vec!["src/lib.rs".to_string()],
            source_agent: AgentId::new("fixture-gen"),
            source_task: None,
        })?;
        Ok(id)
    };

    let e_diff = push(
        &db,
        c_eng,
        11,
        ExperienceType::Difficulty {
            description: "pull cursor overwrote the push cursor".to_string(),
            severity: Severity::High,
        },
        "A difficulty experience",
        tags(&[("issue", "9"), ("area", "sync")]),
    )?;
    recorded.push(e_diff);
    recorded.push(push(
        &db,
        c_eng,
        12,
        ExperienceType::Solution {
            problem_ref: Some(e_diff),
            approach: "split push and pull positions".to_string(),
            worked: true,
        },
        "A solution experience",
        tags(&[("area", "sync")]),
    )?);
    recorded.push(push(
        &db,
        c_eng,
        13,
        ExperienceType::ErrorPattern {
            signature: "E0499".to_string(),
            fix: "split the borrow".to_string(),
            prevention: "scope the mutable ref".to_string(),
        },
        "An error-pattern experience",
        BTreeMap::new(),
    )?);
    recorded.push(push(
        &db,
        c_eng,
        14,
        ExperienceType::SuccessPattern {
            task_type: "schema-migration".to_string(),
            approach: "backup before migrate".to_string(),
            quality: 0.95,
        },
        "A success-pattern experience",
        tags(&[("adr", "011")]),
    )?);
    recorded.push(push(
        &db,
        c_res,
        15,
        ExperienceType::UserPreference {
            category: "serialization".to_string(),
            preference: "postcard".to_string(),
            strength: 0.9,
        },
        "A user-preference experience",
        BTreeMap::new(),
    )?);
    recorded.push(push(
        &db,
        c_res,
        16,
        ExperienceType::ArchitecturalDecision {
            decision: "compaction trusts only push positions".to_string(),
            rationale: "a pull position is a remote sequence".to_string(),
        },
        "An architectural-decision experience",
        tags(&[("area", "sync"), ("release", "r1")]),
    )?);
    recorded.push(push(
        &db,
        c_res,
        17,
        ExperienceType::TechInsight {
            technology: "redb".to_string(),
            insight: "schema bumps take a sidecar backup first".to_string(),
        },
        "A tech-insight experience",
        BTreeMap::new(),
    )?);
    recorded.push(push(
        &db,
        c_res,
        18,
        ExperienceType::Fact {
            statement: "schema v4 arrived in v0.7.0".to_string(),
            source: "CHANGELOG".to_string(),
        },
        "A fact experience",
        BTreeMap::new(),
    )?);
    recorded.push(push(
        &db,
        c_res,
        19,
        ExperienceType::Generic {
            category: Some("misc".to_string()),
        },
        "A generic experience",
        tags(&[("misc", "yes")]),
    )?);

    // Relations (source + target must share a collective).
    let rel_ids = vec![
        db.store_relation(NewExperienceRelation {
            source_id: recorded[1],
            target_id: recorded[0],
            relation_type: RelationType::Supports,
            strength: 0.85,
            metadata: Some("{\"note\":\"solution-supports-difficulty\"}".to_string()),
        })?,
        db.store_relation(NewExperienceRelation {
            source_id: recorded[2],
            target_id: recorded[3],
            relation_type: RelationType::RelatedTo,
            strength: 0.4,
            metadata: None,
        })?,
    ];

    // Insights (External provider requires a precomputed embedding).
    let insight_ids = vec![
        db.store_insight(NewDerivedInsight {
            collective_id: c_eng,
            content: "recurring cursor-overwrite pattern".to_string(),
            embedding: Some(emb(901)),
            source_experience_ids: vec![recorded[1], recorded[3]],
            insight_type: InsightType::Pattern,
            confidence: 0.9,
            domain: vec!["patterns".to_string()],
        })?,
        db.store_insight(NewDerivedInsight {
            collective_id: c_res,
            content: "synthesis of sync-position decisions".to_string(),
            embedding: Some(emb(902)),
            source_experience_ids: vec![recorded[4], recorded[5]],
            insight_type: InsightType::Synthesis,
            confidence: 0.8,
            domain: vec!["sync".to_string()],
        })?,
    ];

    // Expected search ground truth for a FIXED query vector.
    let query = emb(QUERY_SEED);
    let search = db.search_similar(c_eng, &query, 5)?;
    let expected_top_k: Vec<Value> = search
        .iter()
        .map(|r| {
            json!({
                "experience_id": serde_json::to_value(r.experience.id).unwrap_or(Value::Null),
                "similarity": r.similarity,
            })
        })
        .collect();

    // Typed read-back ground truth (via Serialize on the published types).
    let mut experiences_json = Vec::new();
    for id in &recorded {
        let e = db
            .get_experience(*id)?
            .ok_or("recorded experience vanished before read-back")?;
        experiences_json.push(serde_json::to_value(&e)?);
    }
    let mut collectives_json = Vec::new();
    for cid in [c_eng, c_res] {
        if let Some(c) = db.get_collective(cid)? {
            collectives_json.push(serde_json::to_value(&c)?);
        }
    }
    let mut relations_json = Vec::new();
    for rid in &rel_ids {
        if let Some(r) = db.get_relation(*rid)? {
            relations_json.push(serde_json::to_value(&r)?);
        }
    }
    let mut insights_json = Vec::new();
    for iid in &insight_ids {
        if let Some(i) = db.get_insight(*iid)? {
            insights_json.push(serde_json::to_value(&i)?);
        }
    }

    // Watch events (WatchEvent is not Serialize → capture selected fields).
    let (events, seq) = db.poll_changes(0)?;
    let watch_events: Vec<Value> = events
        .iter()
        .map(|ev| {
            json!({
                "event_type": format!("{:?}", ev.event_type),
                "experience_id": serde_json::to_value(ev.experience_id).unwrap_or(Value::Null),
                "collective_id": serde_json::to_value(ev.collective_id).unwrap_or(Value::Null),
                "timestamp": serde_json::to_value(ev.timestamp).unwrap_or(Value::Null),
            })
        })
        .collect();

    let schema_version = db.metadata().schema_version;
    let embedding_dimension = db.metadata().embedding_dimension.size();

    // ---- SYNC PHASE: persist a GENUINE schema-v4 sync cursor row ----
    // One push+pull cycle over the in-memory transport. 0.7.0 writes the
    // single-slot `SyncCursor { instance_id, last_sequence }` for the peer the
    // transport reports; after the cycle `last_sequence` is the WAL head (> 0).
    let db = Arc::new(db);
    let (transport, _remote) = InMemorySyncTransport::new_pair();
    let peer_id = transport.instance_id();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    {
        let mut manager =
            SyncManager::new(Arc::clone(&db), Box::new(transport), SyncConfig::default());
        runtime.block_on(manager.sync_once())?;
    }
    let cursor = db
        .storage_for_test()
        .load_sync_cursor(&peer_id)?
        .ok_or("sync_once persisted no cursor for the in-memory peer")?;
    if cursor.last_sequence == 0 {
        return Err("the legacy cursor must carry last_sequence > 0 for the v5 reset to be provable".into());
    }
    let cursor_last_sequence = cursor.last_sequence;
    let wal_head_after_sync = db.get_current_sequence()?;
    let db = Arc::try_unwrap(db).map_err(|_| "PulseDB still shared after the sync cycle")?;

    // Clean shutdown so redb persists a consistent single-file store.
    db.close()?;

    // ---- RAW-BYTES PHASE (genuine on-disk copy-through bytes, audit C4) ----
    let rdb = redb::Database::open(&redb_path)?;
    let rtx = rdb.begin_read()?;

    let instance_id = {
        let meta = rtx.open_table(METADATA)?;
        meta.get(INSTANCE_ID_KEY)?
            .map(|g| uuid_str(g.value()))
            .unwrap_or_default()
    };

    let mut emb_raw = Vec::new();
    {
        let t = rtx.open_table(EMBEDDINGS)?;
        for row in t.iter()? {
            let (k, v) = row?;
            emb_raw.push(json!({
                "experience_id": uuid_str(k.value()),
                "value_bytes_hex": hexs(v.value()),
            }));
        }
    }
    let mut by_coll = Vec::new();
    {
        let t = rtx.open_multimap_table(EXP_BY_COLLECTIVE)?;
        for row in t.iter()? {
            let (k, vals) = row?;
            let mut entries = Vec::new();
            for v in vals {
                let v = v?;
                entries.push(hexs(v.value()));
            }
            by_coll.push(json!({
                "collective_id": uuid_str(k.value()),
                "value_bytes_hex": entries,
            }));
        }
    }
    let mut by_type = Vec::new();
    {
        let t = rtx.open_multimap_table(EXP_BY_TYPE)?;
        for row in t.iter()? {
            let (k, vals) = row?;
            let mut entries = Vec::new();
            for v in vals {
                let v = v?;
                entries.push(hexs(v.value()));
            }
            by_type.push(json!({
                "key_hex": hexs(k.value()),
                "value_bytes_hex": entries,
            }));
        }
    }
    let mut by_tag = Vec::new();
    {
        let t = rtx.open_multimap_table(EXP_BY_TAG)?;
        for row in t.iter()? {
            let (k, vals) = row?;
            let mut entries = Vec::new();
            for v in vals {
                let v = v?;
                entries.push(hexs(v.value()));
            }
            by_tag.push(json!({
                "key_hex": hexs(k.value()),
                "value_bytes_hex": entries,
            }));
        }
    }
    // The legacy cursor row, raw: the exact bytes the v4→v5 migration reshapes.
    let mut cursor_rows = Vec::new();
    {
        let t = rtx.open_table(SYNC_CURSORS)?;
        for row in t.iter()? {
            let (k, v) = row?;
            cursor_rows.push(json!({
                "peer_instance_id": uuid_str(k.value()),
                "raw_key_hex": hexs(k.value()),
                "raw_value_bytes_hex": hexs(v.value()),
            }));
        }
    }
    drop(rtx);
    drop(rdb);

    if cursor_rows.len() != 1 {
        return Err(format!("expected exactly one sync cursor row, found {}", cursor_rows.len()).into());
    }
    let mut sync_cursor = cursor_rows.remove(0);
    if sync_cursor["peer_instance_id"].as_str() != Some(&peer_id.to_string()) {
        return Err("sync cursor row is keyed by an unexpected peer id".into());
    }
    sync_cursor["last_sequence"] = json!(cursor_last_sequence);
    sync_cursor["legacy_shape"] = json!("SyncCursor { instance_id: InstanceId, last_sequence: u64 } (postcard)");
    sync_cursor["note"] = json!(
        "GENUINE 0.7.0 cursor persisted by one SyncManager::sync_once over the in-memory transport. \
         The schema-v5 migration (0.8.0) rewrites this row as {instance_id, push_sequence: 0, pull_sequence: 0} \
         under a .pre-v5.bak sidecar; the upgrade test asserts the reset against these raw bytes."
    );

    // ---- CONTENT MANIFEST (environment provenance added by finalize step) ----
    let content = json!({
        "fixture_blob": std::path::Path::new(&redb_path)
            .file_name().and_then(|s| s.to_str()).unwrap_or("real-v0.7.0.redb"),
        "source_release": "0.7.0",
        "generated_by": "fixture-gen-v0_7_0",
        "real_published_crate": "pulsehive-db (=0.7.0, crates.io)",
        "schema_version": schema_version,
        "embedding_dimension": embedding_dimension,
        "instance_id": instance_id,
        "migration_axes": [
            "axis3_logical_schema_v4_to_v5_sync_cursor_split"
        ],
        "migration_axes_note": "v0.7.0 is already {redb-v3, postcard, schema-v4}: the redb file-format and codec axes are NOT exercised by this fixture (see real-v0.5.1 / real-v0.4.0). It exercises the schema v4→v5 sync-cursor split + reset (r1.s1.w1, issue #9) and the .pre-v5.bak sidecar.",
        "feature_set": {
            "crate_default_features": true,
            "enabled_features": ["sync"],
            "note": "`sync` enabled (in-memory transport + SyncManager for the cursor row); builtin-embeddings/ort NOT enabled; embeddings injected via the External provider."
        },
        "build_env": {
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "family": std::env::consts::FAMILY
        },
        "collectives": collectives_json,
        "experiences_note": "Experience.embedding is #[serde(skip)] in the published crate (stored separately in the EMBEDDINGS table, copy-through / not re-encoded), so the typed experience records below carry NO embedding field — the per-experience embedding ground truth is the genuine on-disk bytes under raw_stored_bytes.embeddings, keyed by experience_id.",
        "experiences": experiences_json,
        "relations": relations_json,
        "insights": insights_json,
        "watch_events": { "count": watch_events.len(), "max_sequence": seq, "wal_head_after_sync": wal_head_after_sync, "events": watch_events },
        "sync_cursor": sync_cursor,
        "decay_config": {
            "half_life_secs": decay.half_life.as_secs(),
            "freq_weight": decay.freq_weight,
            "floor": decay.floor,
            "auto_archive_below_floor": decay.auto_archive_below_floor,
            "default_recall_weights": Value::Null,
            "note": "recorded as the global Config.decay."
        },
        "expected_search": {
            "collective": "engineering",
            "collective_id": serde_json::to_value(c_eng)?,
            "k": 5,
            "query_seed": QUERY_SEED,
            "query_embedding_f32": query,
            "top_k": expected_top_k,
            "note": "HNSW index internals are NOT captured — the index is rebuilt from redb on every open (issue #18); expected top-k experience ids + similarities are the search ground truth."
        },
        "raw_stored_bytes": {
            "note": "GENUINE on-disk bytes of the copy-through tables, read back via redb 4.x directly (audit C4). The upgrade test asserts these survive migration byte-identically.",
            "embeddings": emb_raw,
            "experiences_by_collective": by_coll,
            "experiences_by_type": by_type,
            "experiences_by_tag": by_tag
        },
        "coverage": {
            "included": [
                "collectives", "experiences+embeddings+tags", "relations", "insights",
                "watch_events", "decay_config", "db_metadata", "sync_cursor"
            ],
            "residual_gap_v0_3_0_wal_v1": "v0.3.0 / WAL-v1 (logical schema before v2) is NOT synthesized here; documented residual gap, not closed.",
            "decay_configs_table": "per-collective decay override is not populated; the global Config.decay is recorded instead."
        },
        "falsification_contract": "A corrupted or truncated real-v0.7.0.redb MUST fail loudly downstream: this fixture's blob_sha256 (provenance AC) + the corrupt/truncated-fixture negative tests together guarantee a mutated/truncated blob fails rather than silently passing."
    });

    std::fs::write(&content_path, serde_json::to_vec_pretty(&content)?)?;
    eprintln!(
        "fixture-gen-v0_7_0: wrote {} (schema={}, {} experiences, {} embeddings raw, cursor last_sequence={}) + {}",
        redb_path,
        schema_version,
        recorded.len(),
        emb_raw.len(),
        cursor_last_sequence,
        content_path
    );
    Ok(())
}
