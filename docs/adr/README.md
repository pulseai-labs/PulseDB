# PulseDB Architecture Decision Records

This directory contains Architecture Decision Records (ADRs) documenting key technical decisions made during PulseDB development.

## ADR Index

| ADR | Title | Status | Date | Summary |
|-----|-------|--------|------|---------|
| [ADR-001](ADR-001-redb-for-storage.md) | Use redb for Storage | Accepted | 2026-02-01 | Pure Rust embedded KV store with ACID and MVCC |
| [ADR-002](ADR-002-hnswlib-for-vector-index.md) | Use hnswlib for Vector Index | **Superseded** | 2026-02-01 | Superseded by ADR-005 |
| [ADR-003](ADR-003-single-writer-concurrency.md) | Single-Writer Concurrency | Accepted | 2026-02-01 | SWMR model matching redb semantics |
| [ADR-004](ADR-004-rich-experience-types.md) | Rich ExperienceType (9 variants) | Accepted | 2026-02-13 | 9 structured variants from Data Model over simplified 6 |
| [ADR-005](ADR-005-pure-rust-hnsw.md) | Pure Rust HNSW via hnsw_rs | Accepted | 2026-02-14 | Replace C++ hnswlib FFI with pure Rust hnsw_rs + VectorIndex trait |

## ADR Template

```markdown
# ADR-XXX: Title

## Status
Proposed | Accepted | Deprecated | Superseded by ADR-YYY

## Date
YYYY-MM-DD

## Context
What is the issue that we're seeing that is motivating this decision?

## Decision
What is the change that we're proposing/doing?

## Consequences
What becomes easier or harder because of this change?

## References
Links to related code, docs, and tickets.
```

## Conventions

- ADR files are named `ADR-NNN-short-title.md`
- Numbers are sequential and never reused
- Superseded ADRs are kept for historical context (status updated)
- Each ADR should reference the relevant code paths and documentation
