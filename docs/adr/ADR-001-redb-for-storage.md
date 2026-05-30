# ADR-001: Use redb for Storage

## Status

Accepted

## Date

2026-02-01

## Context

PulseDB needs an embedded key-value storage engine with ACID transactions to persist agent experiences reliably. The storage layer must be:

- **Embedded** - Single binary, no external server dependencies
- **ACID-compliant** - Atomic transactions, crash recovery, data integrity
- **Concurrent** - Multiple readers during writes (MVCC)
- **Pure Rust preferred** - Minimize C/C++ dependencies, leverage Rust safety guarantees

### Alternatives Considered

| Engine | Language | ACID | MVCC | Dependencies | Maturity |
|--------|----------|------|------|-------------|----------|
| **redb** | Pure Rust | Yes | Yes | None | Growing |
| SQLite | C | Yes | WAL mode | C FFI | Very high |
| RocksDB | C++ | Yes | Snapshots | C++ FFI | High |
| sled | Rust | Partial | No | None | Stalled |

### Performance Baseline

From benchmarking (`docs/06-Performance.md`):
- redb raw write throughput: ~8,000 exp/sec (no vector index overhead)
- Sufficient for PulseDB's target of >1,000 sequential writes/sec

## Decision

Use **redb** (`redb = "2.1"`) as the embedded key-value storage engine instead of SQLite or RocksDB.

### Key Capabilities Used

1. **ACID Transactions** - Atomic writes across multiple tables (experiences, embeddings, indices)
2. **MVCC** - Read transactions see consistent snapshots; readers never block writers
3. **Table Definitions** - Compile-time typed tables (`TableDefinition`, `MultimapTableDefinition`)
4. **Crash Recovery** - Automatic recovery on restart, no data loss
5. **File-level Locking** - Serializes writers across processes via `pulse.db.lock`

### Storage Schema

```
METADATA_TABLE          - Database metadata (schema version, config)
COLLECTIVES_TABLE       - Collective records (bincode-serialized)
EXPERIENCES_TABLE       - Experience records (without embeddings)
EMBEDDINGS_TABLE        - Embedding vectors (raw f32 bytes, separate for compactness)
EXPERIENCES_BY_COLLECTIVE_TABLE - Secondary index (collective + timestamp -> experience)
EXPERIENCES_BY_TYPE_TABLE       - Secondary index (collective + type tag -> experience)
```

## Consequences

### Positive

- Pure Rust implementation - no C/C++ build dependencies (entire stack is pure Rust since ADR-005)
- Simple, well-documented API with compile-time table type checking
- MVCC enables non-blocking concurrent reads during writes
- Automatic crash recovery without manual WAL management
- Small binary footprint
- Serialization flexibility (bincode for structs, raw bytes for embeddings)

### Negative

- Less battle-tested than SQLite (decades) or RocksDB (Meta-scale)
- No SQL query language - all queries via key/range scans and secondary indices
- Fewer optimization knobs compared to RocksDB (compaction, bloom filters, etc.)
- Single-file database limits maximum practical size

### Mitigations

- Comprehensive ACID and corruption detection tests (`tests/storage_acid.rs`)
- Secondary indices for efficient queries without SQL
- Batch write optimization for throughput (group writes in single transaction)

## References

- `src/storage/redb.rs` - RedbStorage implementation
- `src/storage/schema.rs` - Table definitions and schema versioning
- `docs/03-Architecture.md` - Storage layer architecture (Section 5.2)
- `docs/06-Performance.md` - Write throughput benchmarks
