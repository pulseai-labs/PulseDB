//! Benchmarks for PulseDB sync operations.
//!
//! Run with: `cargo bench --features sync -- sync`
//!
//! Performance targets (sync protocol spec):
//! - SyncChange serialization < 5µs per change
//! - Echo prevention check < 10ns
//! - WAL poll (10K events) < 10ms
//! - WAL compaction (10K events) — baseline measurement

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use pulsedb::storage::StorageEngine;
use pulsedb::sync::guard::{is_sync_applying, SyncApplyGuard};
use pulsedb::sync::types::{InstanceId, SyncChange, SyncCursor, SyncEntityType, SyncPayload};
use pulsedb::{
    Collective, CollectiveId, Config, ExperienceType, NewExperience, PulseDB, Timestamp,
};
use tempfile::tempdir;

const DIM: usize = 384;

fn make_embedding(seed: u64) -> Vec<f32> {
    (0..DIM)
        .map(|i| {
            let h = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(i as u64)
                .wrapping_mul(1442695040888963407);
            (h >> 33) as f32 / (u32::MAX as f32) - 0.5
        })
        .collect()
}

fn make_test_experience(cid: CollectiveId, seed: u64) -> pulsedb::Experience {
    let timestamp = Timestamp::now();
    pulsedb::Experience {
        id: pulsedb::ExperienceId::new(),
        collective_id: cid,
        content: format!("bench experience {}", seed),
        embedding: make_embedding(seed),
        experience_type: ExperienceType::Generic { category: None },
        importance: 0.5,
        confidence: 0.8,
        applications: std::collections::BTreeMap::new(),
        domain: vec!["benchmark".to_string()],
        related_files: vec![],
        source_agent: pulsedb::AgentId::new("bench"),
        source_task: None,
        timestamp,
        last_reinforced: timestamp,
        archived: false,
    }
}

fn bench_sync_change_serialization(c: &mut Criterion) {
    let cid = CollectiveId::new();
    let exp = make_test_experience(cid, 42);
    let change = SyncChange {
        sequence: 1,
        source_instance: InstanceId::new(),
        collective_id: cid,
        entity_type: SyncEntityType::Experience,
        payload: SyncPayload::ExperienceCreated(exp),
        timestamp: Timestamp::now(),
    };

    c.bench_function("sync/change_serialization", |b| {
        b.iter(|| {
            let bytes = bincode::serialize(black_box(&change)).unwrap();
            black_box(bytes);
        })
    });
}

fn bench_sync_change_deserialization(c: &mut Criterion) {
    let cid = CollectiveId::new();
    let exp = make_test_experience(cid, 42);
    let change = SyncChange {
        sequence: 1,
        source_instance: InstanceId::new(),
        collective_id: cid,
        entity_type: SyncEntityType::Experience,
        payload: SyncPayload::ExperienceCreated(exp),
        timestamp: Timestamp::now(),
    };
    let bytes = bincode::serialize(&change).unwrap();

    c.bench_function("sync/change_deserialization", |b| {
        b.iter(|| {
            let decoded: SyncChange = bincode::deserialize(black_box(&bytes)).unwrap();
            black_box(decoded);
        })
    });
}

fn bench_echo_prevention_check(c: &mut Criterion) {
    c.bench_function("sync/echo_prevention_check", |b| {
        b.iter(|| {
            black_box(is_sync_applying());
        })
    });
}

fn bench_echo_prevention_guard(c: &mut Criterion) {
    c.bench_function("sync/echo_prevention_guard_lifecycle", |b| {
        b.iter(|| {
            let guard = SyncApplyGuard::enter();
            black_box(is_sync_applying());
            drop(guard);
        })
    });
}

fn bench_wal_poll(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let db = PulseDB::open(dir.path().join("bench.db"), Config::default()).unwrap();
    let cid = db.create_collective("bench").unwrap();

    // Populate WAL with 1000 experience events
    for i in 0..1000 {
        db.record_experience(NewExperience {
            collective_id: cid,
            content: format!("exp {}", i),
            embedding: Some(make_embedding(i)),
            ..Default::default()
        })
        .unwrap();
    }

    let storage = db.storage_for_test();

    c.bench_function("sync/wal_poll_1000_events", |b| {
        b.iter(|| {
            let events = storage.poll_sync_events(black_box(0), 1000).unwrap();
            black_box(events);
        })
    });
}

fn bench_wal_compaction(c: &mut Criterion) {
    // We need to create a fresh DB for each iteration because compaction is destructive
    c.bench_function("sync/wal_compaction_setup_and_compact", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let dir = tempdir().unwrap();
                let db = PulseDB::open(dir.path().join("bench.db"), Config::default()).unwrap();
                let cid = db.create_collective("bench").unwrap();

                // Create 100 experiences (creates WAL events)
                for i in 0..100 {
                    db.record_experience(NewExperience {
                        collective_id: cid,
                        content: format!("exp {}", i),
                        embedding: Some(make_embedding(i)),
                        ..Default::default()
                    })
                    .unwrap();
                }

                // Save a cursor so compaction has a target
                let cursor = SyncCursor {
                    instance_id: InstanceId::new(),
                    last_sequence: 50, // compact up to seq 50
                };
                db.storage_for_test().save_sync_cursor(&cursor).unwrap();

                // Measure compaction
                let start = std::time::Instant::now();
                let deleted = db.compact_wal().unwrap();
                total += start.elapsed();
                black_box(deleted);
            }
            total
        })
    });
}

criterion_group!(
    sync_benches,
    bench_sync_change_serialization,
    bench_sync_change_deserialization,
    bench_echo_prevention_check,
    bench_echo_prevention_guard,
    bench_wal_poll,
    bench_wal_compaction,
);
criterion_main!(sync_benches);
