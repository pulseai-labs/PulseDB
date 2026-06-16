//! Benchmarks for NFR-018 energy-weighted recall characterization.
//!
//! Run with: `cargo bench --bench recall_rerank`
//!
//! NFR-018 EXTRAPOLATION (characterization, NOT a 1M P99 gate; that is
//! VS-3.5.3's `cargo bench search` guard):
//! - Measured N uses the fallback 1,000 fixture, which is above
//!   BRUTE_FORCE_THRESHOLD = 128.
//! - Per-query re-rank delta is characterized as beta>0 weighted search minus
//!   beta=0 search; beta=0 is expected to match legacy search_similar overhead.
//! - The blend stage is linear in k-prime, so the 1M extrapolation should scale
//!   from the measured per-query delta while keeping the literal 1M P99 claim
//!   out of this bench.
//! - Over-fetch is expected to dominate added cost; energy exp() is isolated in
//!   rerank_microbench to confirm it is negligible beside vector retrieval.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use pulsedb::{
    CollectiveId, Config, NewExperience, PulseDB, RecallWeights, SearchFilter, SearchOptions,
    Timestamp,
};
use std::cmp::Ordering;
use std::time::Duration;
use tempfile::tempdir;

/// Default embedding dimension (D384, matches Config::default()).
const DIM: usize = 384;

/// Scale fixture size for weighted_search_at_scale.
///
/// This intentionally stays above BRUTE_FORCE_THRESHOLD = 128 so the benchmark
/// measures the HNSW path rather than the brute-force path.
const SCALE_N: usize = 1_000;

/// Generates a deterministic embedding from a seed.
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

/// Sets up a database pre-populated with `n` experiences.
fn setup_db_with_n(n: usize) -> (PulseDB, CollectiveId, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let db = PulseDB::open(dir.path().join("bench.db"), Config::default()).unwrap();
    let cid = db.create_collective("bench").unwrap();

    for i in 0..n as u64 {
        db.record_experience(NewExperience {
            collective_id: cid,
            content: format!("Experience {i}"),
            importance: 0.35 + ((i % 100) as f32 / 200.0),
            embedding: Some(make_embedding(i)),
            ..Default::default()
        })
        .unwrap();
    }

    (db, cid, dir)
}

fn synthetic_similarity(i: usize, k_prime: usize) -> f32 {
    let denominator = k_prime.saturating_sub(1).max(1) as f32;
    ((i as f32 / denominator) * 1.2) - 0.1
}

fn rerank_microbench(c: &mut Criterion) {
    let mut group = c.benchmark_group("rerank_microbench");
    let cfg = Config::default().decay;
    let now = Timestamp::from_millis(1_700_000_000_000);
    let weights = RecallWeights::new(0.5, 0.5);
    let k = 10usize;

    for &k_prime in &[64usize, 4_000] {
        group.bench_with_input(BenchmarkId::from_parameter(k_prime), &k_prime, |b, &kp| {
            b.iter(|| {
                let mut scored: Vec<(usize, f32)> = (0..kp)
                    .map(|i| {
                        let last_reinforced =
                            Timestamp::from_millis(now.as_millis() - ((i % 10_000) as i64 * 1_000));
                        let energy = pulsedb::energy(
                            0.35 + ((i % 100) as f32 / 200.0),
                            (i % 32) as u32,
                            last_reinforced,
                            now,
                            &cfg,
                        );
                        let similarity = synthetic_similarity(i, kp);
                        let score = weights.similarity * similarity.clamp(0.0, 1.0)
                            + weights.energy * energy;
                        (i, score)
                    })
                    .collect();

                scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
                scored.truncate(k);
                black_box(scored);
            });
        });
    }

    group.finish();
}

fn weighted_search_at_scale(c: &mut Criterion) {
    let mut group = c.benchmark_group("weighted_search_at_scale");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));

    let (db, cid, _dir) = setup_db_with_n(SCALE_N);
    let query = make_embedding(SCALE_N as u64 + 1);
    let weighted = SearchOptions {
        k: 10,
        filter: SearchFilter::default(),
        weights: Some(RecallWeights::new(0.5, 0.5)),
    };
    let beta_zero = SearchOptions {
        k: 10,
        filter: SearchFilter::default(),
        weights: None,
    };

    group.bench_function("weighted_search_beta_gt0", |b| {
        b.iter(|| black_box(db.search(cid, &query, weighted.clone()).unwrap()));
    });
    group.bench_function("weighted_search_beta0", |b| {
        b.iter(|| black_box(db.search(cid, &query, beta_zero.clone()).unwrap()));
    });
    group.bench_function("legacy_search_similar", |b| {
        b.iter(|| black_box(db.search_similar(cid, &query, 10).unwrap()));
    });

    group.finish();
}

criterion_group!(benches, rerank_microbench, weighted_search_at_scale);
criterion_main!(benches);
