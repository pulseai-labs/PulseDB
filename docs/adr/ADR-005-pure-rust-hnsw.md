# ADR-005: Pure Rust HNSW via hnsw_rs (Supersedes ADR-002)

## Status

Accepted (supersedes ADR-002)

## Date

2026-02-14

## Context

ADR-002 selected hnswlib (C++ via FFI) for vector indexing based on a benchmark comparison showing ~2x query latency advantage over `instant-distance` (a minimal pure-Rust HNSW crate). However:

1. **The benchmark was flawed** — it compared hnswlib against `instant-distance`, not against `hnsw_rs` (Jean-Pierre Both), which is the mature, battle-tested pure-Rust HNSW implementation with SIMD support, multithreaded insert/search, and filtered traversal.

2. **No HNSW code has been implemented yet** — Phase 2 (Substrate) hasn't started. `Cargo.toml` has no hnswlib dependency. This is the ideal time to correct the decision before any code couples to C++ FFI.

3. **The `.claude/Rust HNSW Vector Search Deep Dive.md` research** (already in-repo) independently recommends "Eliminate FFI" as step #1 of the strategic roadmap, citing three critical FFI risks:
   - **Memory opacity**: C++ heap invisible to Rust allocator — cannot track/limit RSS
   - **Panic unsafety**: Panic across FFI boundary is undefined behavior
   - **Concurrency conflict**: OpenMP fights with tokio/rayon for thread scheduling

4. **At PulseDB's realistic scale (10K–500K vectors, 384d), the performance gap is negligible** — both implementations return well under 1ms, far exceeding the <50ms target.

## Decision

Use **[hnsw_rs](https://crates.io/crates/hnsw_rs)** (pure Rust) instead of hnswlib (C++ FFI) for HNSW vector indexing. Wrap it behind a `VectorIndex` trait to enable future swapping with minimal effort.

### hnsw_rs Capabilities

| Feature | Details |
|---------|---------|
| **SIFT1M benchmark** | 15,000 req/s at 0.9907 recall; 8,300 req/s at 0.9959 recall (1M vectors, 128d) |
| **Fashion-MNIST** | 62,000 req/s at 0.977 recall (with SIMD, i9-13900HX) |
| **Distance metrics** | L1, L2, Cosine, Jaccard, Hamming, custom via trait |
| **Filtered search** | During traversal (not post-filter) via `FilterT` trait |
| **Concurrency** | Multithreaded insert (`parallel_insert`) and search (`parallel_search`) via `parking_lot` |
| **Persistence** | Dump/reload via `hnswio` module; mmap support for large datasets |
| **License** | Apache-2.0 / MIT dual license |

### Abstraction Layer: VectorIndex Trait

To keep coupling low and enable future switching (e.g., to `hnswlib-rs` or a custom implementation):

```rust
pub trait VectorIndex: Send + Sync {
    fn insert(&self, id: u64, embedding: &[f32]) -> Result<()>;
    fn insert_batch(&self, items: &[(u64, &[f32])]) -> Result<()>;
    fn search(&self, query: &[f32], k: usize) -> Result<Vec<(u64, f32)>>;
    fn search_filtered(&self, query: &[f32], k: usize, filter: &dyn Fn(u64) -> bool) -> Result<Vec<(u64, f32)>>;
    fn delete(&self, id: u64) -> Result<()>;
    fn save(&self, path: &Path) -> Result<()>;
    fn load(path: &Path, config: &HnswConfig) -> Result<Self> where Self: Sized;
    fn len(&self) -> usize;
}
```

Embeddings are always stored in redb (source of truth). The HNSW index is a derived, rebuildable structure — worst case, rebuild from stored embeddings.

### Switchability

| Concern | Effort | Notes |
|---------|--------|-------|
| Core insert/search API | 1-2 days | Behind `VectorIndex` trait |
| Persistence format | 1 day | Rebuild from redb embeddings |
| Filter integration | 1 day | `FilterT` maps to trait callback |
| Distance metric | Trivial | Both crates support cosine/L2 |
| Concurrent access | 1 day | Both use internal RwLock |
| **Total** | **3-5 days** | |

### SIMD Strategy

For MVP: scalar-only (no SIMD feature flags). At 100K vectors with M=16, search performs ~300-500 distance calculations — <5ms even without SIMD, well under the 50ms target. Post-MVP: add portable SIMD when `std::simd` stabilizes (expected 2026-2027) as an optional feature flag with zero API changes.

## Consequences

### Positive

- **No FFI risks** — memory safety, no panic UB, no concurrency conflicts
- **Trivial cross-compilation** — `cargo build` works on Windows, Linux, macOS (x86_64 + ARM) with no C++ toolchain
- **Native filtered search** — `FilterT` trait enables filter-during-traversal (critical for domain/collective filtering)
- **Smaller binary** — no C++ runtime linked
- **Zero unsafe** — for the vector index layer (redb handles its own unsafe internally)
- **Future-proof** — `VectorIndex` trait allows swapping to any implementation in 3-5 days

### Negative

- **~25% fewer req/s at 1M+ scale** — only relevant if a single collective exceeds 1M experiences (unlikely for MVP)
- **Less SIMD coverage on stable Rust** — `simdeez_f` only works on x86_64; portable SIMD requires nightly. Mitigated by scalar being sufficient for PulseDB's scale.

## References

- `docs/adr/ADR-002-hnswlib-for-vector-index.md` — Superseded decision
- `.claude/Rust HNSW Vector Search Deep Dive.md` — In-repo research recommending FFI elimination
- [hnsw_rs on crates.io](https://crates.io/crates/hnsw_rs) — Crate page
- [hnsw_rs on GitHub](https://github.com/jean-pierreBoth/hnswlib-rs) — Source with benchmarks
- [ann-benchmarks](https://github.com/erikbern/ann-benchmarks) — Standard ANN evaluation methodology

### Verified claims

- hnsw_rs 0.3 in the shipped dependency set.
- ANN latency at PulseDB's own scale: measured by the NFR-018 bench at literal 1M with `k=10` (P99 9.35 ms ≤ 50 ms; CHANGELOG records the same figure). The NFR-018 `k=20` workload was not re-measured — see unverified below.

### Unverified claims

- The SIFT1M / Fashion-MNIST throughput and recall tables above are vendor-reported figures, not reproduced locally. The project's own 1M measurement supersedes them for the load-bearing claim.
- The NFR-018 `<50 ms P99 @1M` budget is measured (at `k=10`) but has **no CI enforcement** (printed marker, not an assert — tracked as tech-debt #19); the specified `k=20` workload is unmeasured.
- The filtered-traversal claim (`hnsw_rs::search_filter`) is **not exercised** by the shipped tag-filter tests: they use ≤14 experiences, below the `BRUTE_FORCE_THRESHOLD` (128) linear-scan cutoff, so the HNSW filter branch never runs in CI. Add an above-threshold filtered test before trusting filter-during-traversal at scale.

<!-- Appended at ossify adoption smoke pass, 2026-08-23 -->

## Amendment 2026-10-09 — lazy derived-index lifecycle

### Context

Eager experience and insight indexes imposed a fixed allocation on empty collectives. The r1.s5 lifecycle keeps redb authoritative and allocates derived HNSW state as data is indexed.

### Decision

- Determine collective existence from redb. For a valid search, an existing collective without an index returns empty; reads never create indexes (`src/db.rs:1015–1053`, `src/db.rs:2110–2117`, `src/db.rs:2151–2168`).
- The first indexed write creates the relevant index under its map write lock, re-checking the entry after the read-lock fast path. This applies to both experience and insight maps (`src/db.rs:1056–1125`).
- On reopen, rebuild each index only when its collective has stored embeddings. On writable open, attempt removal of stale sidecars for an emptied collective; cleanup errors are logged and open continues. Read-only open skips removal (`src/db.rs:878–931`, `src/db.rs:955–1009`).
- Compensate the `hnsw_rs` 0.3.4 allocation hint for its missing second `exp()`, targeting about `max_elements` total reserved slots; layer zero still grows independently. The upstream defect is [hnswlib-rs#41](https://github.com/jean-pierreBoth/hnswlib-rs/issues/41), with dependency-sensitive compensation tracked by [#107](https://github.com/pulseai-labs/PulseDB/issues/107) (`src/vector/hnsw.rs:36–79`, `src/vector/hnsw.rs:231–237`).

### Consequences

Empty collectives need no HNSW graph; first indexed writes pay creation cost while holding the map write lock. redb remains the rebuild source. Open-time stale-sidecar cleanup is best effort, **not fail-closed** at this baseline; that distinction limits the cleanup guarantee. Whether a leftover deleted mark after failed cleanup can hide a re-recorded id remains an open question in [#115](https://github.com/pulseai-labs/PulseDB/issues/115). The compensation must be revisited when upstream allocation arithmetic changes. See [ADR-001](ADR-001-redb-for-storage.md).
