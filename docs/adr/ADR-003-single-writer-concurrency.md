# ADR-003: Single-Writer Concurrency Model

## Status

Accepted

## Date

2026-02-01

## Context

PulseDB needs a concurrency model that is correct, simple to reason about, and compatible with both the redb storage engine and the HNSW vector index. The model must handle:

- **Multiple AI agents** reading experiences concurrently
- **Single recording agent** writing at a time (typical agentic workflow)
- **Cross-process access** - multiple processes may open the same database
- **Real-time watch** - readers need to observe new writes

### Alternatives Considered

| Model | Complexity | Write Throughput | Read Throughput | Correctness Risk |
|-------|-----------|-----------------|----------------|-----------------|
| **SWMR** | Low | Limited (1 writer) | High (unlimited) | Low |
| MWMR (locking) | High | Higher | Lower (contention) | Medium |
| Actor model | Medium | Variable | Variable | Low |
| Sharded writes | High | Highest | High | High |

### Throughput Targets

From `docs/06-Performance.md`:
- Sequential writes: > 1,000 exp/sec
- Sequential reads: > 10,000 exp/sec
- Concurrent reads: > 50,000 exp/sec (10 readers)

## Decision

Use **Single-Writer, Multiple-Reader (SWMR)** concurrency model, matching redb's native semantics.

### How It Works

#### Within a Single Process

```
Writer Thread ──────► Write Txn (exclusive)
                      │
Reader Thread 1 ────► Read Txn (MVCC snapshot)  ─┐
Reader Thread 2 ────► Read Txn (MVCC snapshot)   ├─ Non-blocking
Reader Thread N ────► Read Txn (MVCC snapshot)  ─┘
```

- Writer holds exclusive write transaction lock
- Readers get MVCC snapshots - never blocked by writes
- Reads see a consistent point-in-time view

#### Across Multiple Processes

```
Process A (Writer) ──► Holds file lock during writes
Process B (Reader) ──► MVCC read view (may be slightly stale)
Process C (Reader) ──► MVCC read view (may be slightly stale)
```

- File lock (`pulse.db.lock`) serializes writers across processes
- Readers use MVCC views, may see slightly stale data

### Lock Hierarchy

```
1. Database lock (file-level)     ← acquired first
   2. Write transaction (redb)    ← acquired second
      3. HNSW index lock          ← acquired last (per-collective)
```

**Deadlock prevention:** Always acquire locks in this order. Never acquire database lock while holding HNSW lock.

### Transaction Isolation

| Operation | Isolation | Blocking |
|-----------|-----------|----------|
| Read single experience | Snapshot | Non-blocking |
| Read multiple (scan) | Snapshot | Non-blocking |
| Write single experience | Serializable | Blocks other writes |
| Write batch | Serializable | Blocks other writes |

## Consequences

### Positive

- Simple to reason about - no write conflicts, no deadlocks (with lock ordering)
- Matches redb's native MVCC model - no impedance mismatch
- Unlimited concurrent readers with consistent snapshots
- No write-write conflicts to resolve
- Predictable performance characteristics

### Negative

- Write throughput limited to single thread (~1,500 exp/sec with redb + HNSW)
- Batch writes required for high-throughput ingestion scenarios
- Cross-process readers may see slightly stale data (configurable poll interval, default 100ms)
- Single writer can become a bottleneck for multi-agent write-heavy workloads

### Mitigations

- **Batch writes**: Group multiple experiences in single transaction for throughput
- **Async write queue**: Buffer writes and flush in batches
- **Watch system**: Cross-process polling with configurable interval (default 100ms) to observe new writes
- **Per-collective HNSW locks**: Vector index operations don't block across collectives

## References

- `src/storage/redb.rs` - SWMR implementation with redb transactions
- `docs/03-Architecture.md` - Concurrency architecture (Section 6)
- `docs/06-Performance.md` - Throughput targets and bottleneck analysis
- ADR-001 - redb choice enables this concurrency model
