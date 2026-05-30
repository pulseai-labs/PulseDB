# PulseDB: Testing Strategy

> **Version:** 1.0.0  
> **Status:** Approved  
> **Last Updated:** February 2026  
> **Owner:** PulseDB Team

---

## 1. Overview

This document defines the testing strategy for PulseDB, including test types, coverage goals, and CI/CD integration.

### 1.1 Testing Goals

| Goal | Target |
|------|--------|
| Code coverage | > 80% |
| Critical path coverage | 100% |
| Regression prevention | Zero regressions in releases |
| Performance verification | Benchmarks in CI |
| Security validation | Fuzz testing, property tests |

### 1.2 Test Pyramid

```
                    ╱╲
                   ╱  ╲
                  ╱ E2E╲           Few, slow, high confidence
                 ╱──────╲
                ╱        ╲
               ╱Integration╲       Medium count, medium speed
              ╱────────────╲
             ╱              ╲
            ╱   Unit Tests   ╲     Many, fast, low-level
           ╱──────────────────╲
```

| Layer | Count | Speed | Focus |
|-------|-------|-------|-------|
| Unit | ~200 | < 1s each | Individual functions |
| Integration | ~50 | < 5s each | Component interactions |
| E2E | ~20 | < 30s each | Full workflows |
| Performance | ~10 | < 60s each | Benchmarks |
| Fuzz | Continuous | Hours | Edge cases |

---

## 2. Test Categories

### 2.1 Unit Tests

**Location:** `src/**/tests.rs` or `#[cfg(test)]` modules

**Scope:** Individual functions, structs, and modules in isolation.

```rust
// src/experience.rs
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_new_experience_validation_valid() {
        let exp = NewExperience {
            collective_id: CollectiveId::new(),
            content: "Test content".into(),
            importance: 0.5,
            confidence: 0.8,
            ..Default::default()
        };
        
        assert!(exp.validate().is_ok());
    }
    
    #[test]
    fn test_new_experience_validation_empty_content() {
        let exp = NewExperience {
            collective_id: CollectiveId::new(),
            content: "".into(),
            ..Default::default()
        };
        
        let err = exp.validate().unwrap_err();
        assert!(matches!(err, ValidationError::InvalidField { field, .. } if field == "content"));
    }
    
    #[test]
    fn test_new_experience_validation_importance_too_high() {
        let exp = NewExperience {
            collective_id: CollectiveId::new(),
            content: "Test".into(),
            importance: 1.5,  // Invalid
            ..Default::default()
        };
        
        let err = exp.validate().unwrap_err();
        assert!(matches!(err, ValidationError::InvalidField { field, .. } if field == "importance"));
    }
    
    #[test]
    fn test_experience_type_serialization_roundtrip() {
        let types = vec![
            ExperienceType::Difficulty {
                description: "Test".into(),
                severity: Severity::High,
            },
            ExperienceType::Solution {
                problem_ref: Some(ExperienceId::new()),
                approach: "Approach".into(),
                worked: true,
            },
            ExperienceType::UserPreference {
                category: "style".into(),
                preference: "functional".into(),
                strength: 0.9,
            },
        ];
        
        for exp_type in types {
            let bytes = bincode::serialize(&exp_type).unwrap();
            let decoded: ExperienceType = bincode::deserialize(&bytes).unwrap();
            assert_eq!(format!("{:?}", exp_type), format!("{:?}", decoded));
        }
    }
}
```

### 2.2 Integration Tests

**Location:** `tests/`

**Scope:** Multiple components working together.

```rust
// tests/experience_lifecycle.rs
use pulsedb::{PulseDB, Config, NewExperience, ExperienceType};
use tempfile::TempDir;

fn setup_db() -> (PulseDB, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = PulseDB::open(dir.path().join("test.db"), Config::default()).unwrap();
    (db, dir)
}

#[test]
fn test_experience_crud_lifecycle() {
    let (db, _dir) = setup_db();
    let collective_id = db.create_collective("test").unwrap();
    
    // Create
    let exp_id = db.record_experience(NewExperience {
        collective_id,
        content: "Test experience".into(),
        importance: 0.8,
        experience_type: ExperienceType::TechInsight {
            technology: "Rust".into(),
            insight: "Great for systems".into(),
        },
        ..Default::default()
    }).unwrap();
    
    // Read
    let exp = db.get_experience(exp_id).unwrap().unwrap();
    assert_eq!(exp.content, "Test experience");
    assert_eq!(exp.importance, 0.8);
    
    // Update
    db.update_experience(exp_id, ExperienceUpdate {
        importance: Some(0.9),
        ..Default::default()
    }).unwrap();
    
    let exp = db.get_experience(exp_id).unwrap().unwrap();
    assert_eq!(exp.importance, 0.9);
    
    // Archive
    db.archive_experience(exp_id).unwrap();
    let exp = db.get_experience(exp_id).unwrap().unwrap();
    assert!(exp.archived);
    
    // Unarchive
    db.unarchive_experience(exp_id).unwrap();
    let exp = db.get_experience(exp_id).unwrap().unwrap();
    assert!(!exp.archived);
    
    // Delete
    db.delete_experience(exp_id).unwrap();
    assert!(db.get_experience(exp_id).unwrap().is_none());
}

#[test]
fn test_search_similar_returns_relevant_results() {
    let (db, _dir) = setup_db();
    let collective_id = db.create_collective("test").unwrap();
    
    // Create experiences with varying content
    let contents = vec![
        "Authentication with JWT tokens in React",
        "Database connection pooling in PostgreSQL",
        "Auth middleware for Express.js",
        "CSS Grid layout techniques",
    ];
    
    for content in contents {
        db.record_experience(NewExperience {
            collective_id,
            content: content.into(),
            ..Default::default()
        }).unwrap();
    }
    
    // Search for auth-related
    let query = db.embed("authentication").unwrap();
    let results = db.search_similar(collective_id, &query, 2).unwrap();
    
    assert_eq!(results.len(), 2);
    // Auth-related should be top results
    assert!(results[0].0.content.to_lowercase().contains("auth"));
    assert!(results[1].0.content.to_lowercase().contains("auth"));
}

#[test]
fn test_collective_isolation() {
    let (db, _dir) = setup_db();
    
    let collective_a = db.create_collective("project-a").unwrap();
    let collective_b = db.create_collective("project-b").unwrap();
    
    // Add experience to collective A
    db.record_experience(NewExperience {
        collective_id: collective_a,
        content: "Secret from project A".into(),
        ..Default::default()
    }).unwrap();
    
    // Add experience to collective B
    db.record_experience(NewExperience {
        collective_id: collective_b,
        content: "Secret from project B".into(),
        ..Default::default()
    }).unwrap();
    
    // Search in collective A should NOT return B's data
    let query = db.embed("secret").unwrap();
    let results = db.search_similar(collective_a, &query, 10).unwrap();
    
    for (exp, _) in results {
        assert_eq!(exp.collective_id, collective_a);
        assert!(!exp.content.contains("project B"));
    }
}
```

### 2.3 End-to-End Tests

**Location:** `tests/e2e/`

**Scope:** Full workflows simulating real usage.

```rust
// tests/e2e/hive_mind_simulation.rs

#[test]
fn test_multi_agent_hive_mind_workflow() {
    let (db, _dir) = setup_db();
    let collective_id = db.create_collective("project").unwrap();
    
    // Agent 1: Discovers a problem
    let problem_id = db.record_experience(NewExperience {
        collective_id,
        content: "Prisma client not available in edge runtime".into(),
        experience_type: ExperienceType::Difficulty {
            description: "Next.js middleware runs in edge".into(),
            severity: Severity::High,
        },
        source_agent: AgentId("agent-1".into()),
        domain: vec!["prisma".into(), "nextjs".into()],
        importance: 0.9,
        ..Default::default()
    }).unwrap();
    
    // Agent 2: Working on related task, gets context
    let query = db.embed("database queries in Next.js API routes").unwrap();
    let candidates = db.get_context_candidates(ContextCandidatesRequest {
        collective_id,
        query_embedding: query,
        max_similar: 10,
        ..Default::default()
    }).unwrap();
    
    // Agent 2 should see Agent 1's problem
    assert!(candidates.similar_experiences.iter()
        .any(|(e, _)| e.id == problem_id));
    
    // Agent 2: Finds solution, records it
    let solution_id = db.record_experience(NewExperience {
        collective_id,
        content: "Use Prisma adapter pattern for edge compatibility".into(),
        experience_type: ExperienceType::Solution {
            problem_ref: Some(problem_id),
            approach: "adapter pattern".into(),
            worked: true,
        },
        source_agent: AgentId("agent-2".into()),
        domain: vec!["prisma".into(), "nextjs".into()],
        importance: 0.95,
        ..Default::default()
    }).unwrap();
    
    // Store relation
    db.store_relation(NewExperienceRelation {
        source_id: solution_id,
        target_id: problem_id,
        relation_type: RelationType::Elaborates,
        strength: 1.0,
        metadata: None,
    }).unwrap();
    
    // Agent 3: Gets context for related work
    let query = db.embed("edge runtime database access").unwrap();
    let candidates = db.get_context_candidates(ContextCandidatesRequest {
        collective_id,
        query_embedding: query,
        max_similar: 10,
        include_relations: true,
        ..Default::default()
    }).unwrap();
    
    // Agent 3 should see both problem and solution
    let exp_ids: Vec<_> = candidates.similar_experiences.iter()
        .map(|(e, _)| e.id)
        .collect();
    assert!(exp_ids.contains(&problem_id) || exp_ids.contains(&solution_id));
    
    // Relations should link them
    if let Some((solution_exp, _)) = candidates.similar_experiences.iter()
        .find(|(e, _)| e.id == solution_id)
    {
        let relations: Vec<_> = candidates.relations.iter()
            .filter(|r| r.source_id == solution_id)
            .collect();
        assert!(relations.iter().any(|r| r.target_id == problem_id));
    }
}

#[test]
fn test_watch_real_time_updates() {
    let (db, _dir) = setup_db();
    let collective_id = db.create_collective("test").unwrap();
    let db = Arc::new(db);
    
    let received = Arc::new(Mutex::new(Vec::new()));
    let received_clone = Arc::clone(&received);
    
    // Start watcher in background
    let db_clone = Arc::clone(&db);
    let handle = std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut stream = db_clone.watch_experiences(collective_id).await.unwrap();
            let mut count = 0;
            while let Some(exp) = stream.next().await {
                received_clone.lock().unwrap().push(exp);
                count += 1;
                if count >= 3 {
                    break;
                }
            }
        });
    });
    
    // Give watcher time to start
    std::thread::sleep(Duration::from_millis(100));
    
    // Record experiences
    for i in 0..3 {
        db.record_experience(NewExperience {
            collective_id,
            content: format!("Experience {}", i),
            ..Default::default()
        }).unwrap();
        std::thread::sleep(Duration::from_millis(50));
    }
    
    // Wait for watcher
    handle.join().unwrap();
    
    // Verify all received
    let received = received.lock().unwrap();
    assert_eq!(received.len(), 3);
}
```

### 2.4 Performance Tests

**Location:** `benches/`

**Framework:** Criterion.rs

```rust
// benches/core.rs
use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};

fn bench_record_experience(c: &mut Criterion) {
    let (db, _dir) = setup_db_with_experiences(10_000);
    let collective_id = get_collective(&db);
    
    c.bench_function("record_experience", |b| {
        b.iter(|| {
            db.record_experience(random_experience(collective_id)).unwrap()
        })
    });
}

fn bench_search_similar_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("search_similar");
    
    for size in [1_000, 10_000, 100_000] {
        group.bench_with_input(BenchmarkId::new("experiences", size), &size, |b, &size| {
            let (db, _dir) = setup_db_with_experiences(size);
            let collective_id = get_collective(&db);
            let query = random_embedding(384);
            
            b.iter(|| {
                db.search_similar(collective_id, &query, 20).unwrap()
            })
        });
    }
    
    group.finish();
}

criterion_group!(benches, bench_record_experience, bench_search_similar_scaling);
criterion_main!(benches);
```

### 2.5 Property-Based Tests

**Framework:** proptest

```rust
// tests/properties.rs
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]
    
    #[test]
    fn prop_experience_importance_always_valid(importance in 0.0f32..=1.0f32) {
        let (db, _dir) = setup_db();
        let collective_id = db.create_collective("test").unwrap();
        
        let result = db.record_experience(NewExperience {
            collective_id,
            content: "Test".into(),
            importance,
            ..Default::default()
        });
        
        prop_assert!(result.is_ok());
    }
    
    #[test]
    fn prop_experience_importance_invalid_rejected(importance in prop::num::f32::ANY) {
        prop_assume!(importance < 0.0 || importance > 1.0 || importance.is_nan());
        
        let (db, _dir) = setup_db();
        let collective_id = db.create_collective("test").unwrap();
        
        let result = db.record_experience(NewExperience {
            collective_id,
            content: "Test".into(),
            importance,
            ..Default::default()
        });
        
        prop_assert!(result.is_err());
    }
    
    #[test]
    fn prop_search_returns_at_most_k(k in 1usize..100) {
        let (db, _dir) = setup_db_with_experiences(100);
        let collective_id = get_collective(&db);
        let query = random_embedding(384);
        
        let results = db.search_similar(collective_id, &query, k).unwrap();
        
        prop_assert!(results.len() <= k);
    }
    
    #[test]
    fn prop_search_results_sorted_by_similarity(k in 1usize..50) {
        let (db, _dir) = setup_db_with_experiences(100);
        let collective_id = get_collective(&db);
        let query = random_embedding(384);
        
        let results = db.search_similar(collective_id, &query, k).unwrap();
        
        for i in 1..results.len() {
            prop_assert!(results[i-1].1 >= results[i].1, "Results not sorted");
        }
    }
}
```

### 2.6 Fuzz Tests

**Framework:** cargo-fuzz / libfuzzer

**Setup:**
```bash
# Install (one-time)
rustup toolchain install nightly
cargo install cargo-fuzz
```

**Targets:** Three fuzz targets in `fuzz/fuzz_targets/`:

| Target | What it fuzzes | Input derivation |
|--------|---------------|------------------|
| `fuzz_record_experience` | Experience storage with arbitrary content, embeddings, importance, confidence | Raw bytes → lossy UTF-8 content, cycled bytes → 384-dim embedding, first bytes → float fields |
| `fuzz_search_similar` | HNSW search with arbitrary query vectors and k values | Pre-populates 5 fixed experiences, fuzz bytes → query vector + k |
| `fuzz_create_collective` | Collective creation with arbitrary names | Raw bytes → lossy UTF-8 name |

**Running fuzz tests:**
```bash
# Run individual targets (recommended: 1-5 minutes each)
cargo +nightly fuzz run fuzz_record_experience -- -max_total_time=60
cargo +nightly fuzz run fuzz_search_similar -- -max_total_time=60
cargo +nightly fuzz run fuzz_create_collective -- -max_total_time=60

# Run with more iterations (longer, deeper exploration)
cargo +nightly fuzz run fuzz_record_experience -- -max_total_time=300

# List all available targets
cargo +nightly fuzz list
```

**Interpreting results:**
- `DONE N runs in Xs` — success, no crashes found
- `SUMMARY: ... crash-...` — a crash was found; the crashing input is saved in `fuzz/artifacts/`
- `cov: N` — number of code coverage edges reached (higher = more thorough)
- Corpus files are saved in `fuzz/corpus/<target>/` for future runs

**Example target (fuzz_record_experience):**
```rust
#![no_main]
use libfuzzer_sys::fuzz_target;
use pulsedb::{Config, NewExperience, PulseDB};
use tempfile::tempdir;

fuzz_target!(|data: &[u8]| {
    if data.is_empty() { return; }
    let dir = tempdir().unwrap();
    let db = PulseDB::open(dir.path().join("fuzz.db"), Config::default()).unwrap();
    let cid = db.create_collective("fuzz").unwrap();
    let content = String::from_utf8_lossy(data).to_string();
    // ... derive embedding, importance, confidence from data bytes ...
    let _ = db.record_experience(NewExperience { /* ... */ });
});
```

---

## 3. Test Data Strategy

### 3.1 Test Fixtures

```rust
// tests/fixtures/mod.rs

pub fn sample_experiences() -> Vec<NewExperience> {
    vec![
        NewExperience {
            content: "User prefers functional React components".into(),
            experience_type: ExperienceType::UserPreference {
                category: "code_style".into(),
                preference: "functional".into(),
                strength: 0.9,
            },
            importance: 0.8,
            domain: vec!["react".into()],
            ..Default::default()
        },
        NewExperience {
            content: "Prisma edge runtime issue".into(),
            experience_type: ExperienceType::Difficulty {
                description: "Prisma not available in edge".into(),
                severity: Severity::High,
            },
            importance: 0.9,
            domain: vec!["prisma".into(), "nextjs".into()],
            ..Default::default()
        },
        // ... more fixtures
    ]
}

pub fn random_experience(collective_id: CollectiveId) -> NewExperience {
    NewExperience {
        collective_id,
        content: format!("Random experience {}", rand::random::<u64>()),
        importance: rand::random::<f32>().clamp(0.0, 1.0),
        confidence: rand::random::<f32>().clamp(0.0, 1.0),
        ..Default::default()
    }
}

pub fn random_embedding(dim: usize) -> Vec<f32> {
    (0..dim).map(|_| rand::random::<f32>() * 2.0 - 1.0).collect()
}
```

### 3.2 Test Database Setup

```rust
// tests/common/mod.rs

use tempfile::TempDir;
use pulsedb::{PulseDB, Config};

pub struct TestDb {
    pub db: PulseDB,
    _dir: TempDir,  // Dropped when TestDb drops
}

impl TestDb {
    pub fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let db = PulseDB::open(
            dir.path().join("test.db"),
            Config::default(),
        ).unwrap();
        Self { db, _dir: dir }
    }
    
    pub fn with_experiences(count: usize) -> Self {
        let test_db = Self::new();
        let collective_id = test_db.db.create_collective("test").unwrap();
        
        for _ in 0..count {
            test_db.db.record_experience(random_experience(collective_id)).unwrap();
        }
        
        test_db
    }
}
```

---

## 4. Coverage Goals

### 4.1 Coverage Requirements

| Category | Target |
|----------|--------|
| Overall line coverage | > 80% |
| Critical paths | 100% |
| Public API | 100% |
| Error handling | > 90% |

### 4.2 Critical Paths (100% Required)

```
Critical Path Coverage Requirements:
├── Database lifecycle
│   ├── open() - all code paths
│   ├── close() - flush and cleanup
│   └── recovery after crash
├── Experience operations
│   ├── record_experience() - all validation
│   ├── get_experience() - found and not found
│   ├── search_similar() - with and without filters
│   └── delete_experience() - cascade cleanup
├── Collective isolation
│   ├── Cross-collective access prevented
│   └── Collective deletion cascades
├── Concurrency
│   ├── Single-writer enforcement
│   └── MVCC read isolation
└── Error handling
    ├── All ValidationError variants
    ├── All StorageError variants
    └── Error recovery
```

### 4.3 Coverage Measurement

```bash
# Generate coverage report
cargo tarpaulin --out Html --output-dir coverage/

# Check coverage threshold
cargo tarpaulin --fail-under 80

# Ignore test code in coverage
cargo tarpaulin --ignore-tests
```

### 4.4 Current Coverage (Sprint 6, 2026-03-14)

**Overall: 89.56%** (2033/2270 lines covered)

| Module | Lines | Coverage | Notes |
|--------|-------|----------|-------|
| `activity/mod.rs` | 20/20 | 100% | |
| `collective/mod.rs` | 14/14 | 100% | |
| `collective/types.rs` | 8/8 | 100% | |
| `config.rs` | 61/63 | 96.8% | |
| `db.rs` | 595/644 | 92.4% | |
| `embedding/mod.rs` | 22/22 | 100% | |
| `error.rs` | 45/83 | 54.2% | Uncovered: `From<redb::*>` impls (fault injection needed) |
| `experience/types.rs` | 19/19 | 100% | |
| `experience/validation.rs` | 114/127 | 89.8% | |
| `insight/mod.rs` | 22/22 | 100% | |
| `relation/mod.rs` | 18/18 | 100% | |
| `search/context.rs` | 4/4 | 100% | |
| `search/filter.rs` | 25/25 | 100% | |
| `storage/redb.rs` | 639/666 | 96.0% | |
| `storage/schema.rs` | 68/75 | 90.7% | |
| `substrate/impl.rs` | 25/26 | 96.2% | |
| `types.rs` | 58/78 | 74.4% | Uncovered: Display/Debug impls |
| `vector/hnsw.rs` | 131/188 | 69.7% | Uncovered: HNSW rebuild/error paths |
| `watch/lock.rs` | 31/37 | 83.8% | |
| `watch/mod.rs` | 81/84 | 96.4% | |
| `watch/poll.rs` | 9/13 | 69.2% | Uncovered: poll timeout paths |
| `watch/types.rs` | 19/29 | 65.5% | Uncovered: Display/filter impls |

**Analysis:** The only module below 60% is `error.rs` (54.2%), where all uncovered lines are
`From` trait conversions from redb error types. These only trigger on actual database failures
(disk corruption, transaction errors) and would require fault injection to test. All business
logic modules are above 80%.

---

## 5. CI/CD Integration

### 5.1 GitHub Actions Workflow

```yaml
# .github/workflows/test.yml
name: Tests

on:
  push:
    branches: [main, develop]
  pull_request:

env:
  CARGO_TERM_COLOR: always

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Setup Rust
        uses: dtolnay/rust-action@stable
      
      - name: Cache
        uses: Swatinem/rust-cache@v2
      
      - name: Run tests
        run: cargo test --all-features
      
      - name: Run tests (no default features)
        run: cargo test --no-default-features

  coverage:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Setup Rust
        uses: dtolnay/rust-action@stable
      
      - name: Install tarpaulin
        run: cargo install cargo-tarpaulin
      
      - name: Generate coverage
        run: cargo tarpaulin --out Xml --fail-under 80
      
      - name: Upload to Codecov
        uses: codecov/codecov-action@v3

  benchmarks:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Setup Rust
        uses: dtolnay/rust-action@stable
      
      - name: Run benchmarks
        run: cargo bench --bench micro -- --noplot
      
      - name: Check for regressions
        run: |
          # Compare against baseline
          cargo bench --bench micro -- --save-baseline pr
          # Fail if >10% regression
          python scripts/check_regression.py

  fuzz:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Setup Rust nightly
        uses: dtolnay/rust-action@nightly
      
      - name: Install cargo-fuzz
        run: cargo install cargo-fuzz
      
      - name: Run fuzz tests (5 minutes)
        run: |
          cargo +nightly fuzz run record_experience -- -max_total_time=300

  miri:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Setup Rust nightly
        uses: dtolnay/rust-action@nightly
        with:
          components: miri
      
      - name: Run Miri
        run: cargo +nightly miri test -- --skip hnsw  # Skip FFI tests
```

### 5.2 Pre-commit Hooks

```bash
#!/bin/bash
# .git/hooks/pre-commit

set -e

echo "Running pre-commit checks..."

# Format check
cargo fmt --check

# Lint check
cargo clippy -- -D warnings

# Quick tests
cargo test --lib

echo "Pre-commit checks passed!"
```

---

## 6. Test Execution Commands

### 6.1 Common Commands

```bash
# Run all tests
cargo test

# Run specific test
cargo test test_experience_crud_lifecycle

# Run tests matching pattern
cargo test search

# Run tests with output
cargo test -- --nocapture

# Run ignored (slow) tests
cargo test -- --ignored

# Run integration tests only
cargo test --test '*'

# Run benchmarks
cargo bench

# Run specific benchmark
cargo bench search_similar

# Run with coverage
cargo tarpaulin

# Run fuzz tests (requires nightly)
cargo +nightly fuzz run fuzz_record_experience -- -max_total_time=60
cargo +nightly fuzz run fuzz_search_similar -- -max_total_time=60
cargo +nightly fuzz run fuzz_create_collective -- -max_total_time=60

# Run property tests with more cases
PROPTEST_CASES=10000 cargo test --test property_tests
```

### 6.2 Test Tags

```rust
#[test]
fn test_fast() {
    // Runs by default
}

#[test]
#[ignore]  // cargo test -- --ignored
fn test_slow_integration() {
    // Slow test, run explicitly
}

#[test]
#[cfg(feature = "expensive-tests")]
fn test_expensive() {
    // Only with feature flag
}
```

---

## 7. Mocking Strategy

### 7.1 Trait-Based Mocking

```rust
// Use traits for mockable dependencies
pub trait EmbeddingProvider {
    fn embed(&self, text: &str) -> Result<Vec<f32>>;
}

// Real implementation
pub struct OnnxEmbedder { /* ... */ }

impl EmbeddingProvider for OnnxEmbedder {
    fn embed(&self, text: &str) -> Result<Vec<f32>> {
        // ONNX inference
    }
}

// Mock for tests
#[cfg(test)]
pub struct MockEmbedder {
    pub dimension: usize,
}

#[cfg(test)]
impl EmbeddingProvider for MockEmbedder {
    fn embed(&self, _text: &str) -> Result<Vec<f32>> {
        Ok(vec![0.0; self.dimension])
    }
}
```

### 7.2 Test Doubles

```rust
#[cfg(test)]
mod test_helpers {
    // Deterministic "random" for reproducible tests
    pub struct DeterministicRng(u64);
    
    impl DeterministicRng {
        pub fn new(seed: u64) -> Self {
            Self(seed)
        }
        
        pub fn next_f32(&mut self) -> f32 {
            self.0 = self.0.wrapping_mul(1103515245).wrapping_add(12345);
            (self.0 as f32) / (u64::MAX as f32)
        }
        
        pub fn embedding(&mut self, dim: usize) -> Vec<f32> {
            (0..dim).map(|_| self.next_f32() * 2.0 - 1.0).collect()
        }
    }
}
```

---

## 8. References

- [02-SRS.md](./02-SRS.md) — Requirements traceability
- [06-Performance.md](./06-Performance.md) — Performance benchmarks
- [07-Security.md](./07-Security.md) — Security test cases
- [Criterion.rs](https://bheisler.github.io/criterion.rs/book/)
- [proptest](https://proptest-rs.github.io/proptest/)
- [cargo-fuzz](https://rust-fuzz.github.io/book/)

---

## Changelog

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0.0 | February 2026 | PulseDB Team | Initial testing strategy |
