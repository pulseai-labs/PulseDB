# PulseDB: Developer Guide

> **Version:** 1.0.0  
> **Status:** Approved  
> **Last Updated:** February 2026  
> **Owner:** PulseDB Team

---

## 1. Getting Started

### 1.1 Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| Rust | 1.75+ | MSRV |
| Cargo | Latest | Comes with Rust |
| (No C++ needed) | — | hnsw_rs is pure Rust (ADR-005) |

**Platform Support:**
- Linux (x86_64, aarch64)
- macOS (x86_64, aarch64/Apple Silicon)
- Windows (x86_64)

### 1.2 Clone and Build

```bash
# Clone repository
git clone https://github.com/pulsedb/pulsedb.git
cd pulsedb

# Build (debug)
cargo build

# Build (release)
cargo build --release

# Build without ONNX (smaller binary)
cargo build --release --no-default-features
```

### 1.3 Run Tests

```bash
# All tests
cargo test

# With output
cargo test -- --nocapture

# Specific test
cargo test test_experience_crud

# Integration tests only
cargo test --test '*'
```

### 1.4 Run Benchmarks

```bash
# All benchmarks
cargo bench

# Specific benchmark
cargo bench search_similar
```

---

## 2. Project Structure

```
pulsedb/
├── Cargo.toml              # Package manifest
├── README.md               # User-facing readme
├── LICENSE                 # MIT license
├── CHANGELOG.md            # Version history
│
├── src/
│   ├── lib.rs              # Library entry, re-exports
│   ├── db.rs               # PulseDB struct, lifecycle
│   ├── config.rs           # Configuration types
│   ├── types.rs            # Core types (IDs, timestamps)
│   ├── error.rs            # Error types
│   │
│   ├── collective/
│   │   ├── mod.rs          # Collective management
│   │   └── tests.rs        # Unit tests
│   │
│   ├── experience/
│   │   ├── mod.rs          # Experience CRUD
│   │   ├── types.rs        # Experience, ExperienceType
│   │   ├── validation.rs   # Input validation
│   │   └── tests.rs        # Unit tests
│   │
│   ├── search/
│   │   ├── mod.rs          # Search operations
│   │   ├── query.rs        # Query engine
│   │   ├── filter.rs       # Search filters
│   │   └── tests.rs        # Unit tests
│   │
│   ├── relation/
│   │   ├── mod.rs          # Relation storage
│   │   └── tests.rs        # Unit tests
│   │
│   ├── insight/
│   │   ├── mod.rs          # Insight storage
│   │   └── tests.rs        # Unit tests
│   │
│   ├── activity/
│   │   ├── mod.rs          # Activity tracking
│   │   └── tests.rs        # Unit tests
│   │
│   ├── watch/
│   │   ├── mod.rs          # Watch system
│   │   ├── channel.rs      # In-process channels
│   │   └── tests.rs        # Unit tests
│   │
│   ├── storage/
│   │   ├── mod.rs          # Storage abstraction
│   │   ├── redb.rs         # redb implementation
│   │   ├── tables.rs       # Table definitions
│   │   └── tests.rs        # Unit tests
│   │
│   ├── vector/
│   │   ├── mod.rs          # Vector index abstraction
│   │   ├── hnsw.rs         # HNSW wrapper
│   │   └── tests.rs        # Unit tests
│   │
│   ├── embedding/
│   │   ├── mod.rs          # Embedding service
│   │   ├── onnx.rs         # ONNX provider
│   │   └── tests.rs        # Unit tests
│   │
│   └── substrate/
│       ├── mod.rs          # SubstrateProvider trait
│       └── impl.rs         # PulseDBSubstrate implementation
│
├── tests/
│   ├── common/             # Test utilities
│   │   └── mod.rs
│   ├── experience_lifecycle.rs
│   ├── search_integration.rs
│   ├── collective_isolation.rs
│   ├── watch_integration.rs
│   └── e2e/
│       └── hive_mind_simulation.rs
│
├── benches/
│   ├── micro.rs            # Micro-benchmarks
│   ├── workloads.rs        # Workload benchmarks
│   └── concurrency.rs      # Concurrency benchmarks
│
├── fuzz/
│   └── fuzz_targets/
│       ├── record_experience.rs
│       └── search.rs
│
├── examples/
│   ├── basic_usage.rs      # Simple example
│   ├── multi_agent.rs      # Multi-agent demo
│   └── substrate_provider.rs # PulseHive integration
│
└── docs/
    ├── 01-PRD.md
    ├── 02-SRS.md
    ├── ...
    └── 12-Operations.md
```

---

## 3. Code Style & Conventions

### 3.1 Rust Style

Follow the [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/).

```rust
// ✓ Good: Descriptive names, proper visibility
pub struct Experience {
    pub id: ExperienceId,
    pub content: String,
    // ...
}

// ✗ Bad: Abbreviated, unclear
pub struct Exp {
    pub i: ExpId,
    pub c: String,
}
```

### 3.2 Error Handling

```rust
// ✓ Good: Use Result, specific error types
pub fn record_experience(&self, exp: NewExperience) -> Result<ExperienceId, PulseDBError> {
    exp.validate()?;
    // ...
}

// ✗ Bad: Panic, generic errors
pub fn record_experience(&self, exp: NewExperience) -> ExperienceId {
    if exp.content.is_empty() {
        panic!("Content cannot be empty");  // Don't panic!
    }
    // ...
}
```

### 3.3 Documentation

```rust
/// Records a new experience to the collective consciousness.
///
/// This is the primary write operation. When an agent learns something,
/// it records an experience that other agents can perceive.
///
/// # Arguments
///
/// * `experience` - The new experience to record
///
/// # Returns
///
/// The unique identifier for the recorded experience.
///
/// # Errors
///
/// Returns `ValidationError` if:
/// - Content is empty or exceeds 100KB
/// - Importance/confidence not in 0.0-1.0 range
/// - Embedding dimension doesn't match collective
///
/// # Example
///
/// ```rust
/// let id = db.record_experience(NewExperience {
///     collective_id,
///     content: "User prefers concise responses".into(),
///     importance: 0.8,
///     ..Default::default()
/// })?;
/// ```
pub fn record_experience(&self, experience: NewExperience) -> Result<ExperienceId, PulseDBError> {
    // ...
}
```

### 3.4 Naming Conventions

| Type | Convention | Example |
|------|------------|---------|
| Types | PascalCase | `Experience`, `CollectiveId` |
| Functions | snake_case | `record_experience`, `search_similar` |
| Constants | SCREAMING_SNAKE_CASE | `MAX_CONTENT_SIZE` |
| Modules | snake_case | `experience`, `search` |
| Feature flags | kebab-case | `builtin-embeddings` |

### 3.5 Module Organization

```rust
// src/experience/mod.rs

// Re-exports at top
pub use self::types::{Experience, ExperienceId, ExperienceType, NewExperience};
pub use self::validation::ExperienceUpdate;

// Module declarations
mod types;
mod validation;

#[cfg(test)]
mod tests;

// Implementation
impl Experience {
    // Methods here
}
```

---

## 4. Architecture Decisions

### 4.1 ADR Template

```markdown
# ADR-XXX: Title

## Status
Proposed | Accepted | Deprecated | Superseded by ADR-YYY

## Context
What is the issue that we're seeing that is motivating this decision?

## Decision
What is the change that we're proposing/doing?

## Consequences
What becomes easier or harder because of this change?
```

### 4.2 Key ADRs

#### ADR-001: Use redb for Storage

**Status:** Accepted

**Context:** Need embedded key-value storage with ACID transactions.

**Decision:** Use redb instead of SQLite or RocksDB.

**Consequences:**
- ✓ Pure Rust, no C/C++ dependencies
- ✓ Simple API, good performance
- ✓ MVCC for concurrent reads
- ✗ Less battle-tested than SQLite/RocksDB
- ✗ Fewer features (no SQL)

#### ADR-002: Use hnswlib for Vector Index

**Status:** Superseded by ADR-005

#### ADR-005: Pure Rust HNSW via hnsw_rs

**Status:** Accepted

**Context:** Need fast ANN search without C++ FFI risks.

**Decision:** Use hnsw_rs (pure Rust) wrapped behind VectorIndex trait.

**Consequences:**
- ✓ No FFI risks (memory opacity, panic UB, concurrency conflicts)
- ✓ Native filtered search via FilterT trait
- ✓ Cross-compiles trivially (no C++ toolchain)
- ✓ Swappable in 3-5 days via VectorIndex trait

#### ADR-003: Single-Writer Concurrency Model

**Status:** Accepted

**Context:** Need simple, correct concurrency without complexity.

**Decision:** Single writer, multiple readers (SWMR).

**Consequences:**
- ✓ Simple to reason about
- ✓ No write conflicts
- ✓ Matches redb's model
- ✗ Write throughput limited to one thread
- ✗ Batch writes needed for high throughput

---

## 5. Contributing Guidelines

### 5.1 Contribution Workflow

```
1. Fork repository
2. Create feature branch (feat/my-feature)
3. Make changes
4. Run tests (cargo test)
5. Run lints (cargo clippy)
6. Format code (cargo fmt)
7. Submit pull request
8. Address review feedback
9. Merge after approval
```

### 5.2 Branch Naming

| Type | Pattern | Example |
|------|---------|---------|
| Feature | `feat/description` | `feat/batch-insert` |
| Bug fix | `fix/description` | `fix/search-filter` |
| Refactor | `refactor/description` | `refactor/storage-layer` |
| Docs | `docs/description` | `docs/api-examples` |
| Chore | `chore/description` | `chore/update-deps` |

### 5.3 Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation
- `refactor`: Code refactoring
- `test`: Adding tests
- `chore`: Maintenance

**Examples:**
```
feat(experience): add batch insert support

Adds `record_experiences_batch()` for inserting multiple
experiences in a single transaction.

Closes #42
```

```
fix(search): handle empty embedding gracefully

Previously, passing an empty embedding caused a panic.
Now returns ValidationError::InvalidField.
```

### 5.4 Pull Request Template

```markdown
## Description
Brief description of changes.

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] Manual testing performed

## Checklist
- [ ] Code follows style guidelines
- [ ] Self-review completed
- [ ] Documentation updated
- [ ] No new warnings
```

---

## 6. Review Checklist

### 6.1 Code Review Checklist

```markdown
## Correctness
- [ ] Logic is correct
- [ ] Edge cases handled
- [ ] Error handling appropriate
- [ ] No panics in library code

## Performance
- [ ] No unnecessary allocations
- [ ] No unnecessary clones
- [ ] Appropriate data structures
- [ ] No blocking in async code

## Security
- [ ] Input validated
- [ ] No sensitive data in logs
- [ ] Resource limits respected

## Style
- [ ] Follows conventions
- [ ] Well-documented
- [ ] Tests included
- [ ] No commented-out code

## API
- [ ] API is intuitive
- [ ] Breaking changes documented
- [ ] Deprecations handled properly
```

### 6.2 Pre-Merge Checklist

```markdown
- [ ] CI passes
- [ ] Coverage maintained (>80%)
- [ ] No benchmark regressions (>10%)
- [ ] Documentation updated
- [ ] CHANGELOG updated
- [ ] Version bumped (if needed)
```

---

## 7. Release Process

### 7.1 Version Numbering

Follow [Semantic Versioning](https://semver.org/):

```
MAJOR.MINOR.PATCH

MAJOR: Breaking API changes
MINOR: New features, backward compatible
PATCH: Bug fixes, backward compatible
```

**Pre-1.0:** Breaking changes increment MINOR.

### 7.2 Release Checklist

```markdown
## Pre-Release
- [ ] All tests passing
- [ ] Benchmarks run
- [ ] CHANGELOG updated
- [ ] Version bumped in Cargo.toml
- [ ] Documentation generated
- [ ] Examples verified

## Release
- [ ] Create git tag (v0.1.0)
- [ ] Push tag
- [ ] Publish to crates.io (cargo publish)
- [ ] Create GitHub release
- [ ] Update documentation site

## Post-Release
- [ ] Announce release
- [ ] Monitor for issues
- [ ] Update dependent projects
```

### 7.3 CHANGELOG Format

```markdown
# Changelog

## [Unreleased]

### Added
- New feature X

### Changed
- Modified behavior of Y

### Fixed
- Bug in Z

### Removed
- Deprecated API W

## [0.1.0] - 2026-03-01

### Added
- Initial release
- Core storage functionality
- Vector search
- Experience CRUD
```

---

## 8. Debugging Tips

### 8.1 Logging

```rust
use tracing::{debug, info, warn, error, instrument};

#[instrument(skip(self))]
pub fn record_experience(&self, exp: NewExperience) -> Result<ExperienceId> {
    debug!(?exp.collective_id, content_len = exp.content.len(), "Recording experience");
    
    let id = self.storage.insert(&exp)?;
    info!(?id, "Experience recorded");
    
    Ok(id)
}
```

**Enable logging:**
```bash
RUST_LOG=pulsedb=debug cargo run
RUST_LOG=pulsedb::search=trace cargo run
```

### 8.2 Debugging Tests

```bash
# Run single test with output
cargo test test_name -- --nocapture

# Run with backtrace
RUST_BACKTRACE=1 cargo test test_name

# Run with debug logging
RUST_LOG=debug cargo test test_name -- --nocapture
```

### 8.3 Profiling

```bash
# CPU profiling (Linux)
perf record -F 99 -g target/release/benchmark
perf script | stackcollapse-perf.pl | flamegraph.pl > flame.svg

# CPU profiling (macOS)
cargo instruments -t "Time Profiler" --release --bench micro

# Memory profiling
cargo build --release
heaptrack ./target/release/benchmark
heaptrack_print heaptrack.benchmark.*.zst
```

### 8.4 Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| "Database locked" | Another process has lock | Close other process or use different file |
| Dimension mismatch | Wrong embedding size | Check collective's embedding_dimension |
| Slow search | Large dataset, low ef | Increase ef_search or reduce k |
| High memory | HNSW index in memory | Expected; use smaller M or dimension |
| Crash on exit | Drop order issue | Ensure db.close() called |

---

## 9. IDE Setup

### 9.1 VS Code

**Recommended Extensions:**
- rust-analyzer
- Even Better TOML
- CodeLLDB (debugging)
- Error Lens

**settings.json:**
```json
{
    "rust-analyzer.cargo.features": "all",
    "rust-analyzer.checkOnSave.command": "clippy",
    "editor.formatOnSave": true,
    "[rust]": {
        "editor.defaultFormatter": "rust-lang.rust-analyzer"
    }
}
```

### 9.2 IntelliJ IDEA / CLion

- Install Rust plugin
- Enable Clippy: Settings → Languages & Frameworks → Rust → External Linters → Clippy
- Enable format on save: Settings → Tools → Actions on Save → Reformat code

---

## 10. Useful Commands

```bash
# Development
cargo build                    # Build debug
cargo build --release          # Build release
cargo test                     # Run tests
cargo clippy                   # Lint
cargo fmt                      # Format
cargo doc --open              # Generate and open docs

# Analysis
cargo bloat --release         # Binary size analysis
cargo tree                    # Dependency tree
cargo outdated               # Check for updates
cargo audit                  # Security audit

# Benchmarking
cargo bench                  # Run benchmarks
cargo bench -- --save-baseline main  # Save baseline

# Fuzzing (nightly required)
cargo +nightly fuzz run record_experience

# Coverage
cargo tarpaulin --out Html
```

---

## 11. References

- [Rust Book](https://doc.rust-lang.org/book/)
- [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)
- [Rust Performance Book](https://nnethercote.github.io/perf-book/)
- [03-Architecture.md](./03-Architecture.md)
- [08-Testing.md](./08-Testing.md)

---

## Changelog

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0.0 | February 2026 | PulseDB Team | Initial developer guide |
