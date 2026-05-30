# ADR-002: Use hnswlib for Vector Index

## Status

**Superseded** by [ADR-005: Pure Rust HNSW via hnsw_rs](ADR-005-pure-rust-hnsw.md)

## Date

2026-02-01

## Context

PulseDB requires fast approximate nearest neighbor (ANN) search to find semantically similar experiences. The vector index must support:

- **Sub-50ms search latency** at 100K experiences (k=20)
- **Cosine similarity** as the default distance metric
- **Incremental updates** - add/remove vectors without full rebuild
- **Per-collective isolation** - separate index per collective

### Alternatives Considered

| Library | Language | Latency (100K, k=20) | Incremental | Maturity |
|---------|----------|---------------------|-------------|----------|
| **hnswlib** | C++ (FFI) | ~5ms | Yes | Production-proven |
| hora | Rust | ~10ms | Limited | Experimental |
| annoy | C++ (FFI) | ~8ms | No (immutable) | Mature |
| faiss | C++ (FFI) | ~3ms | Yes | Complex build |

### Performance Requirements

From `docs/06-Performance.md`:
- `search_similar(k=20)`: < 50ms at 100K experiences
- `get_context_candidates`: < 100ms at 100K experiences
- Competitive with Qdrant (~15ms), LanceDB (~25ms), pgvector (~50ms) at 1M vectors

## Decision

Use **hnswlib** via C++ FFI for the HNSW approximate nearest neighbor index.

### Configuration Strategy

HNSW parameters tuned by scale (`docs/06-Performance.md`):

| Scale | m | ef_construction | ef_search | Recall |
|-------|---|----------------|-----------|--------|
| 0-10K | 16 | 100 | 50 | >95% |
| 10K-100K | 16 | 200 | 100 | >95% |
| 100K-1M | 24 | 200 | 150 | >93% |
| 1M+ | 32 | 400 | 200 | >90% |

### Memory Scaling

| Experiences | Index Memory | Total (with data) |
|------------|-------------|-------------------|
| 1K | ~5MB | ~10MB |
| 10K | ~50MB | ~80MB |
| 100K | ~150MB | ~300MB |
| 1M | ~1.5GB | ~3GB |

### Integration Architecture

```
PulseDB
  └── Vector Module (src/vector/)
        └── HnswIndex (per collective)
              ├── hnswlib C++ core (via FFI)
              ├── Cosine similarity (default)
              └── Persisted to disk alongside redb
```

## Consequences

### Positive

- 2x faster than pure Rust alternatives (hora, instant-distance)
- Well-tested in production systems (Vespa, Milvus use HNSW variants)
- Supports incremental add/remove without full index rebuild
- Configurable recall vs. latency tradeoff via ef_search parameter
- Per-collective index isolation maps naturally to HNSW instances

### Negative

- C++ dependency introduces build complexity (CMake, platform-specific compilation)
- FFI boundary requires careful memory management and error handling
- C++ exceptions must be caught at FFI boundary to prevent UB
- Increases binary size and compile time

### Mitigations

- FFI wrapper with safe Rust API hides C++ complexity
- Lock hierarchy prevents deadlocks: database lock -> write txn lock -> HNSW lock
- Per-collective isolation limits blast radius of index corruption

## References

- `src/vector/` - HNSW vector index module (Phase 2)
- `docs/03-Architecture.md` - Vector index architecture (Section 5.3)
- `docs/06-Performance.md` - HNSW benchmarks and parameter tuning
- `Cargo.toml` - hnswlib dependency configuration
