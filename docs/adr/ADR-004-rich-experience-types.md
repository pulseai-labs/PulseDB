# ADR-004: Rich ExperienceType with 9 Structured Variants

## Status

Accepted

## Date

2026-02-13

## Context

The Phase-1-Foundation.md simplified `ExperienceType` to 6 flat variants (`Observation`, `Decision`, `Outcome`, `Lesson`, `Pattern`, `Preference`) without associated data. The storage schema (`src/storage/schema.rs`) implements this as `ExperienceTypeTag` with `repr(u8)` discriminants 0-5, along with helper methods (`from_u8()`, `all()`) and secondary index key encoding.

However, the approved Data Model (`docs/04-DataModel.md`) defines 9 richer variants with structured associated data:

| Variant | Associated Data | Purpose |
|---------|----------------|---------|
| `Difficulty` | `description: String`, `severity: Severity` | Problem encountered by agent |
| `Solution` | `problem_ref: Option<ExperienceId>`, `approach: String`, `worked: bool` | Fix for a problem (cross-linked) |
| `ErrorPattern` | `signature: String`, `fix: String`, `prevention: String` | Reusable error knowledge |
| `SuccessPattern` | `task_type: String`, `approach: String`, `quality: f32` | Proven successful approaches |
| `UserPreference` | `category: String`, `preference: String`, `strength: f32` | User preferences with strength |
| `ArchitecturalDecision` | `decision: String`, `rationale: String` | Design decisions with rationale |
| `TechInsight` | `technology: String`, `insight: String` | Technical knowledge |
| `Fact` | `statement: String`, `source: String` | Verified factual statements |
| `Generic` | `category: Option<String>` | Catch-all for unstructured experiences |

Since PulseDB is a production system (not a prototype), starting with the simplified 6-variant design would require a painful migration later:

1. **Schema migration** - `ExperienceTypeTag` byte values baked into `EXPERIENCES_BY_TYPE_TABLE` secondary index keys would need rebuilding
2. **Serialization breakage** - `ExperienceType` enum serialized via bincode into `EXPERIENCES_TABLE`; changing variants breaks deserialization
3. **API breakage** - Consumer code using the old variant names would need updating

## Decision

Use the full 9-variant `ExperienceType` from the Data Model doc with structured associated data. Refactor `ExperienceTypeTag` in `src/storage/schema.rs` from 6 to 9 variants before any experience data is stored (Sprint 2, Ticket #4).

### Variant Mapping (old to new)

| Old Tag (6 variants) | New Tag (9 variants) | Discriminant |
|---------------------|---------------------|-------------|
| `Observation = 0` | `Difficulty = 0` | 0 |
| `Decision = 1` | `Solution = 1` | 1 |
| `Outcome = 2` | `ErrorPattern = 2` | 2 |
| `Lesson = 3` | `SuccessPattern = 3` | 3 |
| `Pattern = 4` | `UserPreference = 4` | 4 |
| `Preference = 5` | `ArchitecturalDecision = 5` | 5 |
| -- | `TechInsight = 6` | 6 (new) |
| -- | `Fact = 7` | 7 (new) |
| -- | `Generic = 8` | 8 (new) |

### Refactoring Required

**File: `src/storage/schema.rs`**
- Rename all 6 existing `ExperienceTypeTag` variants to match Data Model names
- Add 3 new variants (`TechInsight = 6`, `Fact = 7`, `Generic = 8`)
- Update `from_u8()` match arms (add cases for 6, 7, 8)
- Update `all()` to return 9 variants
- Update doc comments to reference Data Model specification
- Update all tests (roundtrip, `all_variants`, bincode, key encoding)

**New types needed:**
- `Severity { Low, Medium, High, Critical }` enum (for `Difficulty` variant)
- `ExperienceType` rich enum in `src/experience/types.rs` with `type_tag() -> ExperienceTypeTag` method

### Key Design Detail: Two-Level Type System

```
ExperienceType (rich, with data)     ExperienceTypeTag (compact, for indexing)
    Difficulty { ... }          -->       Difficulty = 0
    Solution { ... }            -->       Solution = 1
    ...                                   ...
    Generic { ... }             -->       Generic = 8
```

- `ExperienceType` lives in the `Experience` struct, serialized via bincode into `EXPERIENCES_TABLE`
- `ExperienceTypeTag` is a 1-byte discriminant used in `EXPERIENCES_BY_TYPE_TABLE` secondary index keys
- `ExperienceType::type_tag()` maps between them

## Consequences

### Positive

- No migration cost (no experience data stored yet; this is implemented in Sprint 2)
- Richer structured data for agent learning (e.g., `Solution.problem_ref` creates native links between problems and solutions)
- Quantitative signals (`SuccessPattern.quality`, `UserPreference.strength`) enable ranked retrieval
- `Generic { category }` catch-all makes the API future-proof
- Matches the approved Data Model specification exactly
- `ExperienceTypeTag` index key remains 1 byte (sufficient for up to 256 variants)

### Negative

- More complex serialization (bincode handles enums with associated data correctly)
- Larger per-experience storage due to associated data fields
- More validation needed (e.g., `SuccessPattern.quality` must be 0.0-1.0)

## References

- `docs/04-DataModel.md` - Approved Data Model with 9-variant design
- `docs/phases/Phase-1-Foundation.md` - Simplified 6-variant design (superseded by this ADR)
- `src/storage/schema.rs` - Current ExperienceTypeTag implementation (to be refactored)
- Sprint 2, Ticket #4 - Experience CRUD operations
