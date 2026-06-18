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
use std::path::PathBuf;
use std::time::{Duration, Instant};
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

// ---------------------------------------------------------------------------
// NFR-018 1M at-scale search bench + manual P99 harness (VS-3.5.3 work-1.03)
// ---------------------------------------------------------------------------
//
// Design (spec §3 — D2/D5/C1/C6):
// - D2: TWO measurements, ONE command (`cargo bench search`), NON-panicking.
//   (1) `search_1m_p99_manual` = the SOURCE OF TRUTH. A manual sampling loop
//       issues real `db.search()` queries, records each latency, and prints
//       `NFR-018 1M P99 = <ms>ms (budget 50ms)`. It does NOT assert!/panic! on
//       the value — the 50ms GATE verdict is 1.04's job (D1).
//   (2) `weighted_search_1m` (criterion `b.iter()`) = regression baseline ONLY,
//       for criterion's `% change` tracking. Best-effort, not the gate.
// - D5: the 1M fixture is built ONCE into a persisted, seed-stamped redb file
//   under `target/bench-fixtures/nfr018-1m-seed<N>.db`, rebuilt only if absent.
//   The build happens in UNMEASURED setup so the (minutes + GBs) build cost
//   never lands inside a measured sample. An open-cost probe records whether
//   `PulseDB::open()` on the persisted file rebuilds the HNSW graph or loads it.
// - C1: feasibility floor. The target N is read from `NFR018_N` (default 1M);
//   if 1M is infeasible in the local build budget, re-run with a smaller
//   `NFR018_N` (>= BRUTE_FORCE_THRESHOLD = 128) and record the fallback + a
//   linear-extrapolation-to-1M caveat. The chosen N is stamped into the
//   fixture filename and printed, so a sub-1M run is acceptable-with-caveat.
// - C6: tail hygiene. The first NFR018_WARMUP (~100) queries are discarded as
//   warm-up; >= NFR018_SAMPLES (default 1000) post-warm-up queries are measured.
//   P95 / P99 / max are recorded together. Query vectors are RANDOM fixed-seed
//   (`make_embedding(N + q)`), NOT one repeated vector (tail realism).

/// Target fixture size for the NFR-018 1M bench. Default 1_000_000.
///
/// Overridable via the `NFR018_N` env var for the C1 fallback path: if a 1M
/// fixture cannot build/measure within budget locally, set e.g. `NFR018_N=50000`
/// to record the largest feasible N (must stay >= BRUTE_FORCE_THRESHOLD = 128).
fn target_n() -> usize {
    std::env::var("NFR018_N")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1_000_000)
        .max(128)
}

/// Number of warm-up queries to DISCARD before sampling (C6). Default 100.
fn warmup_queries() -> usize {
    std::env::var("NFR018_WARMUP")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(100)
}

/// Number of post-warm-up queries to MEASURE for the percentile (C6). Default 1000.
fn sample_queries() -> usize {
    std::env::var("NFR018_SAMPLES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1_000)
        .max(1_000)
}

/// Stable on-disk location for the persisted, seed-stamped 1M fixture (D5).
///
/// N is stamped into the filename so a size/seed change invalidates the cache.
fn fixture_path(n: usize) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("bench-fixtures")
        .join(format!("nfr018-1m-seed{n}.db"))
}

/// Builds the persisted N-experience fixture if absent, else reuses it (D5).
///
/// The build runs in UNMEASURED setup. Returns the fixture path plus the build
/// wall-clock (`None` if the fixture already existed and was reused) so the
/// caller can record the C1 budget figure.
fn build_or_load_fixture(n: usize) -> (PathBuf, Option<Duration>) {
    let path = fixture_path(n);
    if path.exists() {
        eprintln!(
            "NFR-018 fixture: reusing persisted {} (N={n}, no rebuild of redb rows)",
            path.display()
        );
        return (path, None);
    }

    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    eprintln!("NFR-018 fixture: building N={n} at {} ...", path.display());
    let start = Instant::now();

    let db = PulseDB::open(&path, Config::default()).unwrap();
    let cid = db.create_collective("nfr018").unwrap();
    // Populate in chunks; progress every 100k so a slow 1M build is observable
    // (and a stall is distinguishable from progress per C1).
    for i in 0..n as u64 {
        db.record_experience(NewExperience {
            collective_id: cid,
            content: format!("Experience {i}"),
            importance: 0.35 + ((i % 100) as f32 / 200.0),
            embedding: Some(make_embedding(i)),
            ..Default::default()
        })
        .unwrap();
        if (i + 1) % 100_000 == 0 {
            eprintln!(
                "NFR-018 fixture: recorded {}/{n} ({:?} elapsed)",
                i + 1,
                start.elapsed()
            );
        }
    }
    db.close().unwrap();
    let build = start.elapsed();
    eprintln!("NFR-018 fixture: build complete in {build:?}");
    (path, Some(build))
}

/// Opens the persisted fixture and records the open-cost probe verdict (D5).
///
/// `load_all_indexes` (src/db.rs) ALWAYS calls `rebuild_from_embeddings` from
/// the redb embeddings on open — it loads only the `.hnsw.meta` deleted-set,
/// never a serialized graph. The probe times `open()` empirically and prints a
/// verdict; a multi-second open at scale confirms the graph is REBUILT (not
/// loaded), which is why the criterion regression-% is best-effort, not the gate.
fn open_with_probe(path: &PathBuf) -> (PulseDB, CollectiveId, Duration) {
    let start = Instant::now();
    let db = PulseDB::open(path, Config::default()).unwrap();
    let open_cost = start.elapsed();
    let cid = db
        .list_collectives()
        .unwrap()
        .into_iter()
        .next()
        .expect("fixture has a collective")
        .id;
    eprintln!(
        "NFR-018 OPEN-COST PROBE: PulseDB::open() took {open_cost:?} on the persisted fixture. \
         load_all_indexes rebuilds the HNSW graph from redb embeddings on every open \
         (it loads only the .hnsw.meta deleted-set, not a serialized graph), so a \
         multi-second open at scale = REBUILT, not loaded."
    );
    (db, cid, open_cost)
}

/// Computes the value at the `percentile` (e.g. 0.99) of an unsorted slice.
fn percentile(sorted_ms: &[f64], p: f64) -> f64 {
    if sorted_ms.is_empty() {
        return f64::NAN;
    }
    let idx = ((p * sorted_ms.len() as f64) as usize).min(sorted_ms.len() - 1);
    sorted_ms[idx]
}

/// Manual P99 sampling loop — the SOURCE OF TRUTH for the NFR-018 gate (D2/C6).
///
/// Runs in criterion's UNMEASURED setup-and-print form: it does its own timing
/// (not `b.iter`), discards warm-up queries, samples >= 1000 post-warm-up, and
/// prints `NFR-018 1M P99 = <ms>ms (budget 50ms)`. It NEVER asserts/panics on
/// the value (the gate verdict is 1.04's, D1).
fn search_1m_p99_manual(c: &mut Criterion) {
    let n = target_n();
    let (path, build_wall) = build_or_load_fixture(n);
    let (db, cid, open_cost) = open_with_probe(&path);

    let opts = SearchOptions {
        k: 10,
        filter: SearchFilter::default(),
        weights: Some(RecallWeights::new(0.5, 0.5)),
    };

    let warmup = warmup_queries();
    let samples = sample_queries();

    // Warm-up: discard the first `warmup` queries (cold cache / lazy init / the
    // first-query open-cost tail) so the recorded P99 is a property of the WARM
    // system (C6). RANDOM fixed-seed query vectors (make_embedding(n + q)).
    for q in 0..warmup {
        let query = make_embedding(n as u64 + q as u64);
        let _ = black_box(db.search(cid, &query, opts.clone()).unwrap());
    }

    // Measured window: >= 1000 post-warm-up queries, each timed individually.
    let mut latencies_ms: Vec<f64> = Vec::with_capacity(samples);
    for q in 0..samples {
        let query = make_embedding(n as u64 + (warmup + q) as u64);
        let t0 = Instant::now();
        let res = db.search(cid, &query, opts.clone()).unwrap();
        latencies_ms.push(t0.elapsed().as_secs_f64() * 1_000.0);
        black_box(res);
    }

    latencies_ms.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    let p50 = percentile(&latencies_ms, 0.50);
    let p95 = percentile(&latencies_ms, 0.95);
    let p99 = percentile(&latencies_ms, 0.99);
    let max = *latencies_ms.last().unwrap();

    // The canonical marker (D2). NON-panicking: print + record only.
    eprintln!("NFR-018 1M P99 = {p99:.3}ms (budget 50ms)");
    eprintln!(
        "NFR-018 1M latency summary: N={n} (measured{}) warmup={warmup} samples={samples} \
         P50={p50:.3}ms P95={p95:.3}ms P99={p99:.3}ms max={max:.3}ms \
         | build_wall={:?} open_cost={open_cost:?} query_dist=random-fixed-seed",
        if n == 1_000_000 { "" } else { ", FALLBACK<1M — linear extrapolation to 1M is a caveat, not measured" },
        build_wall.unwrap_or(Duration::ZERO),
    );

    // Register a no-op criterion bench so this id participates in `cargo bench
    // search` selection even though the real measurement is the manual loop.
    let mut group = c.benchmark_group("search_1m_p99");
    group.sample_size(10);
    group.bench_function(BenchmarkId::from_parameter(n), |b| {
        b.iter(|| black_box(p99));
    });
    group.finish();
}

/// Criterion regression baseline for at-scale weighted search (D2 #2).
///
/// `b.iter()` over real `db.search()` on the persisted 1M fixture — for
/// criterion's `% change` regression tracking ONLY (best-effort, not the gate;
/// the absolute P99 from `search_1m_p99_manual` is the trustworthy number).
fn weighted_search_1m(c: &mut Criterion) {
    let n = target_n();
    let (path, _build) = build_or_load_fixture(n);
    let (db, cid, _open_cost) = open_with_probe(&path);

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

    let mut group = c.benchmark_group("weighted_search_1m");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));

    // Distinct query per iter is not possible inside b.iter (must be cheap +
    // repeatable), so use a single fixed-seed query for the regression baseline;
    // the manual loop above carries the random-distribution tail measurement.
    let query = make_embedding(n as u64 + 7);
    group.bench_function("weighted_search_1m_beta_gt0", |b| {
        b.iter(|| black_box(db.search(cid, &query, weighted.clone()).unwrap()));
    });
    group.bench_function("weighted_search_1m_beta0", |b| {
        b.iter(|| black_box(db.search(cid, &query, beta_zero.clone()).unwrap()));
    });
    group.finish();
}

criterion_group!(benches, rerank_microbench, weighted_search_at_scale);
criterion_group!(nfr018_1m, search_1m_p99_manual, weighted_search_1m);
criterion_main!(benches, nfr018_1m);
