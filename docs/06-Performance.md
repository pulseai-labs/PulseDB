# PulseDB: Performance Specification

> **Version:** 1.0.0  
> **Status:** Approved  
> **Last Updated:** February 2026  
> **Owner:** PulseDB Team

---

## 1. Overview

This document defines performance requirements, benchmark methodology, and optimization strategies for PulseDB.

---

## 2. Performance Targets

### 2.1 Latency Targets

| Operation | Target (P50) | Target (P99) | Conditions |
|-----------|--------------|--------------|------------|
| `open()` | < 50ms | < 100ms | Existing DB, 100K experiences |
| `record_experience()` | < 5ms | < 10ms | Collective with 100K experiences |
| `get_context_candidates()` | < 50ms | < 100ms | 100K experiences, k=20 |
| `search_similar()` | < 20ms | < 50ms | 1M experiences, k=20 |
| `get_experience()` | < 1ms | < 2ms | Direct ID lookup |
| `store_relation()` | < 2ms | < 5ms | Any scale |
| `watch` notification | < 1ms | < 10ms | In-process (crossbeam) |

### 2.2 Throughput Targets

| Operation | Target | Conditions |
|-----------|--------|------------|
| Sequential writes | > 1,000 exp/sec | Single writer |
| Sequential reads | > 10,000 exp/sec | Single reader |
| Concurrent reads | > 50,000 exp/sec | 10 readers |
| Search QPS | > 100 queries/sec | k=20, 100K experiences |

### 2.3 Resource Targets

| Resource | Target | Conditions |
|----------|--------|------------|
| Binary size | < 20 MB | With ONNX model |
| Binary size | < 5 MB | Without ONNX |
| Base memory | < 50 MB | Empty database |
| Memory per 100K exp | ~150 MB | Including HNSW index |
| Startup time | < 100ms | 100K experiences |
| Disk per experience | < 2 KB | Excluding embedding |
| Disk per embedding (384d) | 1.5 KB | Raw f32 |

---

## 3. Benchmark Methodology

### 3.1 Hardware Baseline

**Reference Machine:**
```
CPU: Apple M2 Pro (12 cores) / AMD Ryzen 9 5900X (12 cores)
RAM: 32 GB
Storage: NVMe SSD (>3000 MB/s read)
OS: macOS Sonoma / Ubuntu 22.04
```

**Minimum Viable Machine:**
```
CPU: 4 cores, 2.5 GHz+
RAM: 8 GB
Storage: SSD (500 MB/s read)
```

### 3.2 Dataset Specifications

| Dataset | Experiences | Avg Content | Embedding Dim | Total Size |
|---------|-------------|-------------|---------------|------------|
| Tiny | 1,000 | 500 bytes | 384 | ~3 MB |
| Small | 10,000 | 500 bytes | 384 | ~25 MB |
| Medium | 100,000 | 500 bytes | 384 | ~250 MB |
| Large | 1,000,000 | 500 bytes | 384 | ~2.5 GB |
| XLarge | 10,000,000 | 500 bytes | 384 | ~25 GB |

### 3.3 Workload Patterns

#### Pattern A: Write-Heavy (Ingestion)
```
90% record_experience
10% get_experience
```

#### Pattern B: Read-Heavy (Query)
```
10% record_experience
40% search_similar
30% get_context_candidates
20% get_experience
```

#### Pattern C: Mixed (Typical Agent)
```
30% record_experience
30% search_similar
20% get_context_candidates
10% store_relation
10% get_experience
```

#### Pattern D: Real-Time (Watch)
```
50% watch stream consumption
30% record_experience
20% search_similar
```

---

## 4. Benchmark Suite

### 4.1 Micro-Benchmarks

```rust
// benches/micro.rs

use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use pulsedb::{PulseDB, Config, NewExperience};

fn bench_record_experience(c: &mut Criterion) {
    let db = setup_db(100_000);
    let collective_id = db.create_collective("bench").unwrap();
    
    c.bench_function("record_experience", |b| {
        b.iter(|| {
            db.record_experience(new_experience(collective_id))
        })
    });
}

fn bench_search_similar(c: &mut Criterion) {
    let mut group = c.benchmark_group("search_similar");
    
    for size in [1_000, 10_000, 100_000, 1_000_000].iter() {
        let db = setup_db_with_experiences(*size);
        let collective_id = get_collective(&db);
        let query = random_embedding(384);
        
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, _| {
                b.iter(|| {
                    db.search_similar(collective_id, &query, 20)
                })
            },
        );
    }
    group.finish();
}

fn bench_get_context_candidates(c: &mut Criterion) {
    let db = setup_db_with_experiences(100_000);
    let collective_id = get_collective(&db);
    let query = random_embedding(384);
    
    c.bench_function("get_context_candidates", |b| {
        b.iter(|| {
            db.get_context_candidates(ContextCandidatesRequest {
                collective_id,
                query_embedding: query.clone(),
                max_recent: 10,
                max_similar: 20,
                include_activities: true,
                include_insights: true,
                include_relations: true,
                ..Default::default()
            })
        })
    });
}

criterion_group!(
    benches,
    bench_record_experience,
    bench_search_similar,
    bench_get_context_candidates,
);
criterion_main!(benches);
```

### 4.2 Macro-Benchmarks

```rust
// benches/workloads.rs

fn bench_workload_a_write_heavy(c: &mut Criterion) {
    let db = setup_db(100_000);
    let collective_id = db.create_collective("bench").unwrap();
    
    c.bench_function("workload_a_write_heavy", |b| {
        b.iter(|| {
            for _ in 0..100 {
                // 90% writes
                for _ in 0..90 {
                    db.record_experience(new_experience(collective_id)).unwrap();
                }
                // 10% reads
                for _ in 0..10 {
                    let id = random_experience_id();
                    let _ = db.get_experience(id);
                }
            }
        })
    });
}

fn bench_workload_b_read_heavy(c: &mut Criterion) {
    let db = setup_db_with_experiences(100_000);
    let collective_id = get_collective(&db);
    
    c.bench_function("workload_b_read_heavy", |b| {
        b.iter(|| {
            for _ in 0..100 {
                // 10% writes
                for _ in 0..10 {
                    db.record_experience(new_experience(collective_id)).unwrap();
                }
                // 40% search
                for _ in 0..40 {
                    db.search_similar(collective_id, &random_embedding(384), 20).unwrap();
                }
                // 30% context
                for _ in 0..30 {
                    db.get_context_candidates(context_request(collective_id)).unwrap();
                }
                // 20% point reads
                for _ in 0..20 {
                    db.get_experience(random_experience_id()).ok();
                }
            }
        })
    });
}
```

### 4.3 Concurrency Benchmarks

```rust
// benches/concurrency.rs

fn bench_concurrent_reads(c: &mut Criterion) {
    let db = Arc::new(setup_db_with_experiences(100_000));
    let collective_id = get_collective(&db);
    
    for num_readers in [1, 2, 4, 8, 16, 32] {
        c.bench_with_input(
            BenchmarkId::new("concurrent_reads", num_readers),
            &num_readers,
            |b, &n| {
                b.iter(|| {
                    let handles: Vec<_> = (0..n)
                        .map(|_| {
                            let db = Arc::clone(&db);
                            std::thread::spawn(move || {
                                for _ in 0..1000 {
                                    db.search_similar(collective_id, &random_embedding(384), 20).unwrap();
                                }
                            })
                        })
                        .collect();
                    
                    for h in handles {
                        h.join().unwrap();
                    }
                })
            },
        );
    }
}

fn bench_writer_with_readers(c: &mut Criterion) {
    // Test write latency under read load
    let db = Arc::new(setup_db_with_experiences(100_000));
    let collective_id = get_collective(&db);
    
    // Start background readers
    let stop = Arc::new(AtomicBool::new(false));
    let readers: Vec<_> = (0..8)
        .map(|_| {
            let db = Arc::clone(&db);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    db.search_similar(collective_id, &random_embedding(384), 20).ok();
                }
            })
        })
        .collect();
    
    c.bench_function("write_under_read_load", |b| {
        b.iter(|| {
            db.record_experience(new_experience(collective_id)).unwrap()
        })
    });
    
    stop.store(true, Ordering::Relaxed);
    for r in readers {
        r.join().unwrap();
    }
}
```

---

## 5. Performance Profiling

### 5.1 Profiling Tools

| Tool | Purpose | Platform |
|------|---------|----------|
| `perf` | CPU profiling | Linux |
| `Instruments` | CPU/Memory profiling | macOS |
| `flamegraph` | Visualization | All |
| `heaptrack` | Memory allocation | Linux |
| `dhat` | Heap profiling | All (Valgrind) |
| `criterion` | Micro-benchmarks | All |

### 5.2 Profiling Commands

```bash
# CPU profiling with flamegraph
cargo build --release
perf record -F 99 -g target/release/benchmark
perf script | stackcollapse-perf.pl | flamegraph.pl > flamegraph.svg

# Memory profiling with heaptrack
heaptrack target/release/benchmark
heaptrack_print heaptrack.benchmark.*.zst

# macOS Instruments
xcrun xctrace record --template 'Time Profiler' --launch target/release/benchmark
```

### 5.3 Profiling Focus Areas

| Component | Key Metrics |
|-----------|-------------|
| HNSW search | Distance calculations, cache locality |
| redb reads | Page faults, read amplification |
| redb writes | WAL overhead, compaction |
| Serialization | Allocation, copy overhead |
| Embedding | ONNX inference time, batch efficiency |

---

## 6. Optimization Strategies

### 6.1 HNSW Optimizations

```rust
// Optimal HNSW parameters by scale
fn optimal_hnsw_config(experience_count: u64) -> HnswConfig {
    match experience_count {
        0..=10_000 => HnswConfig {
            m: 16,
            ef_construction: 100,
            ef_search: 50,
        },
        10_001..=100_000 => HnswConfig {
            m: 16,
            ef_construction: 200,
            ef_search: 100,
        },
        100_001..=1_000_000 => HnswConfig {
            m: 24,
            ef_construction: 200,
            ef_search: 150,
        },
        _ => HnswConfig {
            m: 32,
            ef_construction: 400,
            ef_search: 200,
        },
    }
}
```

**Trade-offs:**
| Parameter | Higher Value | Lower Value |
|-----------|--------------|-------------|
| `m` | Better recall, more memory | Faster build, less memory |
| `ef_construction` | Better index quality, slower build | Faster build |
| `ef_search` | Better recall, slower search | Faster search |

### 6.2 redb Optimizations

```rust
// Optimal redb configuration
fn optimal_redb_config() -> redb::Builder {
    redb::Builder::new()
        .set_cache_size(64 * 1024 * 1024)  // 64MB cache
        .set_page_size(4096)                // Match OS page size
}
```

**Key Optimizations:**
1. **Batch writes**: Group multiple writes in single transaction
2. **Read caching**: Tune cache size based on working set
3. **Key design**: Prefix keys for efficient range scans

### 6.3 Embedding Optimizations

```rust
// Batch embedding for efficiency
impl EmbeddingService {
    pub fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        // ONNX batch inference is ~3x faster than sequential
        let batch_size = 32;
        let mut results = Vec::with_capacity(texts.len());
        
        for chunk in texts.chunks(batch_size) {
            let batch_result = self.session.run_batch(chunk)?;
            results.extend(batch_result);
        }
        
        Ok(results)
    }
}
```

### 6.4 Memory Optimizations

```rust
// Avoid unnecessary allocations
impl PulseDB {
    // Return iterator instead of Vec when possible
    pub fn iter_experiences(&self, collective_id: CollectiveId) 
        -> impl Iterator<Item = Experience> + '_ 
    {
        // Stream from storage, don't collect all into memory
        self.storage.scan_experiences(collective_id)
    }
    
    // Reuse buffers for embeddings
    pub fn search_similar_into(
        &self,
        collective_id: CollectiveId,
        query: &[f32],
        k: usize,
        results: &mut Vec<(ExperienceId, f32)>,  // Reusable buffer
    ) -> Result<()> {
        results.clear();
        // Fill results buffer
        Ok(())
    }
}
```

---

## 7. Known Bottlenecks

### 7.1 Current Bottlenecks

| Bottleneck | Impact | Mitigation |
|------------|--------|------------|
| HNSW index loading | Cold start latency | Lazy loading, memory mapping |
| Single writer lock | Write throughput | Batch writes, async queue |
| Embedding generation | Write latency (Builtin mode) | Batch embedding, async generation |
| Cross-process watch | Notification latency | Tune poll interval |

### 7.2 Bottleneck Detection

```rust
// Instrumentation for bottleneck detection
#[cfg(feature = "metrics")]
pub struct Metrics {
    pub hnsw_search_us: Histogram,
    pub redb_read_us: Histogram,
    pub redb_write_us: Histogram,
    pub embedding_us: Histogram,
    pub serialization_us: Histogram,
}

impl PulseDB {
    fn record_experience_instrumented(&self, exp: NewExperience) -> Result<ExperienceId> {
        let start = Instant::now();
        
        // Embedding
        let emb_start = Instant::now();
        let embedding = self.embedding_service.embed(&exp.content)?;
        self.metrics.embedding_us.record(emb_start.elapsed().as_micros());
        
        // Serialization
        let ser_start = Instant::now();
        let bytes = bincode::serialize(&exp)?;
        self.metrics.serialization_us.record(ser_start.elapsed().as_micros());
        
        // Storage write
        let write_start = Instant::now();
        let id = self.storage.insert(bytes)?;
        self.metrics.redb_write_us.record(write_start.elapsed().as_micros());
        
        // HNSW insert
        let hnsw_start = Instant::now();
        self.hnsw.add(id, &embedding)?;
        self.metrics.hnsw_insert_us.record(hnsw_start.elapsed().as_micros());
        
        Ok(id)
    }
}
```

---

## 8. Scaling Characteristics

### 8.1 Scaling Curves

```
Write Latency vs Experience Count
─────────────────────────────────
Latency (ms)
    │
 10 ┤                          ●
    │                      ●
  5 ┤              ●   ●
    │      ●   ●
  1 ┼──●───────────────────────────
    │
    └──┬───┬───┬───┬───┬───┬───
      1K  10K 100K 500K 1M  5M
                Experiences

Search Latency vs Experience Count (k=20)
─────────────────────────────────────────
Latency (ms)
    │
100 ┤                              ●
    │                          ●
 50 ┤                      ●
    │              ●   ●
 20 ┤      ●   ●
 10 ┼──●───────────────────────────
    │
    └──┬───┬───┬───┬───┬───┬───
      1K  10K 100K 500K 1M  5M
                Experiences
```

### 8.2 Memory Scaling

| Experiences | HNSW Memory | redb Cache | Total |
|-------------|-------------|------------|-------|
| 1K | ~5 MB | 10 MB | ~15 MB |
| 10K | ~30 MB | 20 MB | ~50 MB |
| 100K | ~200 MB | 50 MB | ~250 MB |
| 1M | ~1.5 GB | 100 MB | ~1.6 GB |
| 10M | ~15 GB | 200 MB | ~15 GB |

### 8.3 Disk Scaling

| Experiences | redb Size | HNSW Index | Embeddings | Total |
|-------------|-----------|------------|------------|-------|
| 1K | ~1 MB | ~2 MB | ~1.5 MB | ~5 MB |
| 10K | ~8 MB | ~15 MB | ~15 MB | ~40 MB |
| 100K | ~70 MB | ~150 MB | ~150 MB | ~370 MB |
| 1M | ~650 MB | ~1.5 GB | ~1.5 GB | ~3.6 GB |

---

## 9. Performance Testing CI

### 9.1 CI Pipeline

```yaml
# .github/workflows/perf.yml
name: Performance Tests

on:
  push:
    branches: [main]
  pull_request:

jobs:
  benchmarks:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Run benchmarks
        run: cargo bench --bench micro -- --save-baseline pr
      
      - name: Compare with main
        run: |
          git fetch origin main
          git checkout origin/main -- target/criterion
          cargo bench --bench micro -- --baseline main
      
      - name: Check regression
        run: |
          # Fail if any benchmark regressed >10%
          python scripts/check_regression.py --threshold 0.10
```

### 9.2 Regression Detection

```python
# scripts/check_regression.py
import json
import sys

def check_regression(threshold=0.10):
    with open('target/criterion/comparison.json') as f:
        results = json.load(f)
    
    regressions = []
    for bench, data in results.items():
        change = data['mean']['change']
        if change > threshold:
            regressions.append(f"{bench}: {change*100:.1f}% slower")
    
    if regressions:
        print("Performance regressions detected:")
        for r in regressions:
            print(f"  - {r}")
        sys.exit(1)
    
    print("No significant regressions detected")
```

---

## 10. Comparison with Alternatives

### 10.1 Vector Search Comparison

| Database | 1M Vectors, k=10 | Index Build | Memory |
|----------|------------------|-------------|--------|
| PulseDB (HNSW) | ~20ms | ~30 min | ~1.5 GB |
| Qdrant | ~15ms | ~25 min | ~1.8 GB |
| LanceDB | ~25ms | ~20 min | ~1.2 GB |
| pgvector (IVFFlat) | ~50ms | ~45 min | ~2 GB |

### 10.2 Write Performance Comparison

| Database | 10K Writes/sec | Embedded | Notes |
|----------|----------------|----------|-------|
| PulseDB | 1,500 | ✓ | Single writer |
| SQLite | 5,000 | ✓ | WAL mode |
| LanceDB | 2,000 | ✓ | Lance format |
| redb (raw) | 8,000 | ✓ | No vector index |

---

## 11. References

- [02-SRS.md](./02-SRS.md) — Performance requirements (NFR section)
- [03-Architecture.md](./03-Architecture.md) — System architecture
- [Criterion.rs](https://bheisler.github.io/criterion.rs/book/) — Benchmarking framework

---

## Changelog

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0.0 | February 2026 | PulseDB Team | Initial performance specification |
