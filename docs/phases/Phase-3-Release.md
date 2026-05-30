# Phase 3: Polish & Release

> **Weeks:** 9-10  
> **Sprints:** 5-6  
> **Story Points:** 39  
> **Milestones:** M5 (Quality), M6 (Release)

---

## 1. Overview

Phase 3 completes the MVP: real-time watch system, SubstrateProvider trait, comprehensive testing, benchmarks, documentation polish, and crates.io release.

```
Week 9                          Week 10
┌─────────────────────────────┐ ┌─────────────────────────────┐
│ Watch System                │ │ Documentation               │
│ SubstrateProvider           │ │ Benchmarks                  │
│ Test Suite                  │ │ Release to crates.io        │
└──────────────┬──────────────┘ └──────────────┬──────────────┘
               │                               │
               ▼                               ▼
           M5: Quality                     M6: Ship
           (Tests Pass)                   (Published)
```

**Prerequisites:** Phase 1 + Phase 2 complete

---

## 2. Stories & Acceptance Criteria

### Sprint 5 (Week 9): 21 points

#### E4-S01: In-Process Watch (5 pts)

**User Story:** As an agent developer, I want to subscribe to new experiences so that agents react in real-time.

**Acceptance Criteria:**
- [ ] `watch_experiences(collective_id)` returns async Stream
- [ ] New experiences emitted to stream
- [ ] crossbeam-channel for low latency (<100ns overhead)
- [ ] Multiple subscribers supported
- [ ] Filter by domain/type supported
- [ ] Stream ends when dropped
- [ ] No memory leak on drop

---

#### E4-S02: Cross-Process Watch (5 pts)

**User Story:** As a developer, I want multiple processes to detect changes so that distributed agents can coordinate.

**Acceptance Criteria:**
- [ ] WAL sequence number tracked
- [ ] `poll_changes(since_seq)` returns new experiences
- [ ] Configurable poll interval (default: 100ms)
- [ ] File lock coordination for writer detection
- [ ] Detect new experiences since last check

---

#### E4-S03: Watch Configuration (3 pts)

**User Story:** As a developer, I want to configure watch behavior so that I can tune for my use case.

**Acceptance Criteria:**
- [ ] `WatchConfig` struct in Config
- [ ] `in_process` flag (default: true)
- [ ] `poll_interval_ms` for cross-process (default: 100)
- [ ] `buffer_size` for channel (default: 1000)
- [ ] Graceful degradation if buffer full

---

#### E3-S04: SubstrateProvider Trait (8 pts)

**User Story:** As PulseHive, I want PulseDB to implement SubstrateProvider so that I can use it as my storage layer.

**Acceptance Criteria:**
- [ ] `SubstrateProvider` trait defined
- [ ] `PulseDBSubstrate` implements trait
- [ ] Async wrappers over sync core (tokio::spawn_blocking)
- [ ] All required methods implemented
- [ ] Works with PulseHive HiveMind
- [ ] Re-exported from crate root

---

### Sprint 6 (Week 10): 18 points

#### E5-S01: Error Handling (3 pts)

**User Story:** As a developer, I want clear error types so that I can handle errors appropriately.

**Acceptance Criteria:**
- [ ] `PulseDBError` enum comprehensive
- [ ] All error variants documented
- [ ] `thiserror` for derivation
- [ ] Actionable error messages
- [ ] No panics in library code
- [ ] Error conversion traits implemented

---

#### E5-S02: Documentation (5 pts)

**User Story:** As a developer, I want comprehensive documentation so that I can use PulseDB effectively.

**Acceptance Criteria:**
- [ ] All public types documented
- [ ] All public functions documented with examples
- [ ] Module-level documentation
- [ ] README with quick start guide
- [ ] rustdoc builds without warnings
- [ ] Examples compile and run

---

#### E5-S03: Test Suite (5 pts)

**User Story:** As a developer, I want high test coverage so that I can trust PulseDB works correctly.

**Acceptance Criteria:**
- [ ] Unit tests for all modules
- [ ] Integration tests for workflows
- [ ] Property-based tests for invariants
- [ ] Fuzz tests for crash resistance
- [ ] Coverage > 80%
- [ ] CI pipeline running all tests
- [ ] All tests pass on Linux, macOS, Windows

---

#### E5-S04: Benchmarks (3 pts)

**User Story:** As a developer, I want performance benchmarks so that I can verify PulseDB meets requirements.

**Acceptance Criteria:**
- [ ] Criterion benchmarks for core operations
- [ ] `record_experience` < 10ms
- [ ] `search_similar` < 50ms @ 100K
- [ ] `get_context_candidates` < 100ms
- [ ] Scaling benchmarks (1K to 1M)
- [ ] CI regression detection (10% threshold)

---

#### E5-S05: Release (2 pts)

**User Story:** As a developer, I want PulseDB on crates.io so that I can add it as a dependency.

**Acceptance Criteria:**
- [ ] Cargo.toml metadata complete
- [ ] CHANGELOG.md updated
- [ ] Version 0.1.0 tagged
- [ ] `cargo publish` succeeds
- [ ] GitHub release created
- [ ] Demo example published

---

## 3. Dependency Graph

```
Phase 2 Complete
       │
       ├── E4-S01 (In-Process Watch)
       │       │
       │       └── E4-S02 (Cross-Process Watch)
       │               │
       │               └── E4-S03 (Watch Config)
       │
       └── E3-S04 (SubstrateProvider)
               │
               └──────────────────┐
                                  │
       ┌──────────────────────────┼──────────────────────────┐
       │                          │                          │
       ▼                          ▼                          ▼
E5-S01 (Errors)            E5-S03 (Tests)            E5-S04 (Benchmarks)
       │                          │                          │
       └──────────────────────────┼──────────────────────────┘
                                  │
                                  ▼
                         E5-S02 (Documentation)
                                  │
                                  ▼
                         E5-S05 (Release)
```

---

## 4. Architecture Context

### 4.1 Components to Implement

```
┌─────────────────────────────────────────────────────────────┐
│                      PulseDB                                │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              SubstrateProvider Trait                 │   │
│  │  (async interface for PulseHive integration)        │   │
│  └───────────────────────────┬─────────────────────────┘   │
│                              │                              │
│  ┌───────────────────────────▼─────────────────────────┐   │
│  │                   Watch System                       │   │
│  │  ┌──────────────┐  ┌──────────────┐                 │   │
│  │  │ In-Process   │  │ Cross-Process│                 │   │
│  │  │ (channels)   │  │ (WAL poll)   │                 │   │
│  │  └──────────────┘  └──────────────┘                 │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │                   Quality Layer                      │   │
│  │  Tests │ Benchmarks │ Documentation │ CI/CD         │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### 4.2 File Structure Additions

```
src/
├── watch/
│   ├── mod.rs          # WatchService
│   ├── channel.rs      # In-process (crossbeam)
│   ├── poll.rs         # Cross-process (WAL)
│   └── tests.rs
│
├── substrate/
│   ├── mod.rs          # SubstrateProvider trait
│   └── impl.rs         # PulseDBSubstrate
│
└── lib.rs              # Re-exports, crate docs

tests/
├── watch_integration.rs
├── substrate_integration.rs
└── e2e/
    └── hive_mind_simulation.rs

benches/
├── micro.rs            # Micro-benchmarks
├── workloads.rs        # Realistic workloads
└── scaling.rs          # Scale testing
```

---

## 5. Data Model

### 5.1 Watch Types

```rust
pub struct WatchConfig {
    pub in_process: bool,
    pub poll_interval_ms: u64,
    pub buffer_size: usize,
}

impl Default for WatchConfig {
    fn default() -> Self {
        Self {
            in_process: true,
            poll_interval_ms: 100,
            buffer_size: 1000,
        }
    }
}

pub struct WatchEvent {
    pub experience_id: ExperienceId,
    pub collective_id: CollectiveId,
    pub event_type: WatchEventType,
    pub timestamp: Timestamp,
}

pub enum WatchEventType {
    Created,
    Updated,
    Archived,
    Deleted,
}

pub struct WatchFilter {
    pub domains: Option<Vec<String>>,
    pub experience_types: Option<Vec<ExperienceType>>,
}
```

### 5.2 SubstrateProvider Trait

```rust
#[async_trait]
pub trait SubstrateProvider: Send + Sync {
    // Experience operations
    async fn store_experience(&self, exp: NewExperience) -> Result<ExperienceId>;
    async fn get_experience(&self, id: ExperienceId) -> Result<Option<Experience>>;
    async fn update_experience(&self, id: ExperienceId, update: ExperienceUpdate) -> Result<()>;
    
    // Search operations
    async fn search_similar(&self, collective: CollectiveId, query: &[f32], k: usize) -> Result<Vec<SearchResult>>;
    async fn get_recent(&self, collective: CollectiveId, limit: usize) -> Result<Vec<Experience>>;
    
    // Relation operations
    async fn store_relation(&self, rel: NewExperienceRelation) -> Result<RelationId>;
    async fn get_related(&self, exp_id: ExperienceId, dir: RelationDirection) -> Result<Vec<(Experience, ExperienceRelation)>>;
    
    // Insight operations
    async fn store_insight(&self, insight: NewDerivedInsight) -> Result<InsightId>;
    async fn get_insights(&self, collective: CollectiveId, query: &[f32], k: usize) -> Result<Vec<DerivedInsight>>;
    
    // Activity operations
    async fn register_activity(&self, activity: NewActivity) -> Result<()>;
    async fn get_active_agents(&self, collective: CollectiveId) -> Result<Vec<Activity>>;
    
    // Context assembly
    async fn get_context_candidates(&self, request: ContextRequest) -> Result<ContextCandidates>;
    
    // Watch
    fn watch(&self, collective: CollectiveId) -> Pin<Box<dyn Stream<Item = WatchEvent> + Send>>;
}
```

---

## 6. API Signatures

### 6.1 Watch System

```rust
impl PulseDB {
    pub fn watch_experiences(
        &self,
        collective_id: CollectiveId,
    ) -> impl Stream<Item = WatchEvent>;

    pub fn watch_experiences_filtered(
        &self,
        collective_id: CollectiveId,
        filter: WatchFilter,
    ) -> impl Stream<Item = WatchEvent>;

    pub fn poll_changes(
        &self,
        collective_id: CollectiveId,
        since_sequence: u64,
    ) -> Result<(Vec<WatchEvent>, u64), PulseDBError>;

    pub fn get_current_sequence(&self) -> u64;
}
```

### 6.2 SubstrateProvider

```rust
pub struct PulseDBSubstrate {
    db: Arc<PulseDB>,
    runtime: Handle,
}

impl PulseDBSubstrate {
    pub fn new(db: PulseDB) -> Self;
    pub fn new_with_runtime(db: PulseDB, runtime: Handle) -> Self;
}

impl SubstrateProvider for PulseDBSubstrate {
    // All trait methods implemented
}
```

---

## 7. Performance Targets

| Operation | Target | Notes |
|-----------|--------|-------|
| Watch event delivery | < 1ms | In-process |
| Watch channel overhead | < 100ns | Per event |
| Cross-process poll | < 10ms | Check for changes |
| SubstrateProvider overhead | < 1ms | Async wrapper |

### 7.1 Benchmark Suite

```rust
// benches/micro.rs
fn bench_record_experience(c: &mut Criterion) {
    c.bench_function("record_experience", |b| {
        b.iter(|| db.record_experience(new_exp.clone()))
    });
}

fn bench_search_similar(c: &mut Criterion) {
    let mut group = c.benchmark_group("search_similar");
    for size in [1_000, 10_000, 100_000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, &size| {
                let db = setup_db_with_n_experiences(size);
                b.iter(|| db.search_similar(collective, &query, 20))
            },
        );
    }
}

// benches/workloads.rs
fn bench_mixed_workload(c: &mut Criterion) {
    c.bench_function("mixed_read_write", |b| {
        b.iter(|| {
            // 80% reads, 20% writes
            for _ in 0..80 { db.search_similar(...); }
            for _ in 0..20 { db.record_experience(...); }
        })
    });
}
```

---

## 8. Testing Requirements

### 8.1 Unit Tests

```rust
#[cfg(test)]
mod tests {
    // Watch
    #[test] fn test_watch_receives_new_experiences() { }
    #[test] fn test_watch_filter_by_domain() { }
    #[test] fn test_watch_multiple_subscribers() { }
    #[test] fn test_watch_no_leak_on_drop() { }
    #[test] fn test_poll_changes_returns_since() { }
    
    // SubstrateProvider
    #[tokio::test] async fn test_substrate_store_experience() { }
    #[tokio::test] async fn test_substrate_search() { }
    #[tokio::test] async fn test_substrate_context_candidates() { }
    
    // Error handling
    #[test] fn test_no_panics_on_invalid_input() { }
    #[test] fn test_error_messages_actionable() { }
}
```

### 8.2 Integration Tests

```rust
// tests/watch_integration.rs
#[tokio::test]
async fn test_watch_end_to_end() {
    let db = PulseDB::open(temp_dir(), Config::default())?;
    let collective = db.create_collective("test")?;
    
    let mut watch = db.watch_experiences(collective);
    
    // Record in separate task
    let db2 = db.clone();
    tokio::spawn(async move {
        db2.record_experience(new_exp)?;
    });
    
    // Should receive event
    let event = tokio::time::timeout(
        Duration::from_secs(1),
        watch.next()
    ).await??;
    
    assert_eq!(event.event_type, WatchEventType::Created);
}

// tests/substrate_integration.rs
#[tokio::test]
async fn test_substrate_with_hive_mind() {
    let db = PulseDB::open(temp_dir(), Config::default())?;
    let substrate = PulseDBSubstrate::new(db);
    
    // Simulate HiveMind usage
    let exp_id = substrate.store_experience(new_exp).await?;
    let results = substrate.search_similar(collective, &query, 10).await?;
    let candidates = substrate.get_context_candidates(request).await?;
    
    assert!(!candidates.similar_experiences.is_empty());
}
```

### 8.3 Property-Based Tests

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_search_never_returns_more_than_k(k in 1usize..100) {
        let db = setup_db();
        let results = db.search_similar(collective, &query, k)?;
        prop_assert!(results.len() <= k);
    }
    
    #[test]
    fn test_importance_always_in_range(imp in 0.0f32..=1.0) {
        let exp = NewExperience { importance: imp, ..default() };
        let result = db.record_experience(exp);
        prop_assert!(result.is_ok());
    }
}
```

### 8.4 Fuzz Tests

```rust
// fuzz/fuzz_targets/record_experience.rs
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(content) = std::str::from_utf8(data) {
        let exp = NewExperience {
            content: content.to_string(),
            ..Default::default()
        };
        // Should not panic
        let _ = db.record_experience(exp);
    }
});
```

---

## 9. CI/CD Pipeline

### 9.1 GitHub Actions Workflow

```yaml
name: CI

on: [push, pull_request]

jobs:
  test:
    runs-on: ${{ matrix.os }}
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest, windows-latest]
        rust: [stable, beta]
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@master
        with:
          toolchain: ${{ matrix.rust }}
      - run: cargo test --all-features
      
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: clippy, rustfmt
      - run: cargo fmt --check
      - run: cargo clippy -- -D warnings
      
  coverage:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: cargo install cargo-tarpaulin
      - run: cargo tarpaulin --out Xml
      - uses: codecov/codecov-action@v3
      
  bench:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: cargo bench -- --save-baseline pr
      # Compare with main baseline
      
  publish:
    if: startsWith(github.ref, 'refs/tags/v')
    needs: [test, lint, coverage, bench]
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: cargo publish
        env:
          CARGO_REGISTRY_TOKEN: ${{ secrets.CARGO_TOKEN }}
```

---

## 10. Release Checklist

### 10.1 Pre-Release

```markdown
## Code Quality
- [ ] All tests passing on CI
- [ ] Coverage > 80%
- [ ] No clippy warnings
- [ ] Code formatted

## Performance
- [ ] All benchmark targets met
- [ ] No regression from baseline
- [ ] Memory usage acceptable

## Documentation
- [ ] All public APIs documented
- [ ] README complete with examples
- [ ] CHANGELOG updated
- [ ] rustdoc builds clean
```

### 10.2 Release Process

```markdown
## Version Bump
- [ ] Update version in Cargo.toml
- [ ] Update version in README badges
- [ ] Update CHANGELOG with release date

## Tag & Publish
- [ ] Create git tag: `git tag v0.1.0`
- [ ] Push tag: `git push origin v0.1.0`
- [ ] CI publishes to crates.io
- [ ] Create GitHub release with notes

## Post-Release
- [ ] Verify crates.io page
- [ ] Test `cargo add pulsedb`
- [ ] Announce release
```

### 10.3 Cargo.toml Metadata

```toml
[package]
name = "pulsedb"
version = "0.1.0"
edition = "2021"
rust-version = "1.75"
license = "MIT"
description = "Embedded database for agentic AI systems"
repository = "https://github.com/pulsedb/pulsedb"
documentation = "https://docs.rs/pulsedb"
readme = "README.md"
keywords = ["database", "embedded", "ai", "agents", "vector"]
categories = ["database", "data-structures"]

[package.metadata.docs.rs]
all-features = true
```

---

## 11. Milestones Checklist

### M5: Quality (End Week 9)

| Criteria | Status |
|----------|--------|
| Watch system working | ☐ |
| SubstrateProvider implemented | ☐ |
| Test coverage > 80% | ☐ |
| All benchmarks passing | ☐ |
| No P0/P1 bugs open | ☐ |
| CI pipeline green | ☐ |

### M6: Release (End Week 10)

| Criteria | Status |
|----------|--------|
| Documentation complete | ☐ |
| README polished | ☐ |
| Examples working | ☐ |
| CHANGELOG updated | ☐ |
| Version 0.1.0 tagged | ☐ |
| Published to crates.io | ☐ |
| GitHub release created | ☐ |

---

## 12. Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| `crossbeam-channel` | 0.5+ | In-process watch |
| `tokio` | 1.0+ | Async runtime |
| `async-trait` | 0.1+ | Async trait support |
| `futures` | 0.3+ | Stream utilities |
| `criterion` | 0.5+ | Benchmarks |
| `proptest` | 1.0+ | Property testing |

---

## 13. References

- [Phase-1-Foundation.md](./Phase-1-Foundation.md) — Foundation phase
- [Phase-2-Substrate.md](./Phase-2-Substrate.md) — Substrate phase
- [08-Testing.md](../08-Testing.md) — Full testing strategy
- [06-Performance.md](../06-Performance.md) — Performance targets
- [12-Operations.md](../12-Operations.md) — Operations guide
