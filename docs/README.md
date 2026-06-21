# PulseDB Documentation — Index & Map

> **Entry point for this repo's docs.** Maps every document, where each ID type
> is defined, and how to trace a requirement → slice → test. Read this first when
> you need to find something; grep the specific doc second.

---

## 1. Where to look for X

| You need… | Go to | Find it by |
|-----------|-------|-----------|
| A **functional requirement** (`FR-NNN`) | [`docs/02-SRS.md`](./02-SRS.md) §3 | `grep -n "#### FR-001" docs/02-SRS.md` |
| A **non-functional requirement** (`NFR-NNN`) | [`docs/02-SRS.md`](./02-SRS.md) §4 | `grep -n "#### NFR-020" docs/02-SRS.md` |
| **Requirement → test traceability** | [`docs/02-SRS.md`](./02-SRS.md) §7 (matrix) | search `## 7. Traceability Matrix` |
| The **roadmap** (phases, sprints, `VS-N.M.K` slices, demo criteria) | [`../ROADMAP.md`](../ROADMAP.md) | `grep -n "VS-4.0" ROADMAP.md` (from repo root) |
| An **architecture decision** (`ADR-NNN`) | [`docs/adr/`](./adr/) | [`docs/adr/README.md`](./adr/README.md) index |
| **Product vision / goals / users** | [`docs/01-PRD.md`](./01-PRD.md) | — |
| **Data model / entities / on-disk schema** | [`docs/04-DataModel.md`](./04-DataModel.md) | — |
| **Public API surface** | [`docs/05-API-Reference.md`](./05-API-Reference.md) | — |
| **Performance targets & benchmarks** | [`docs/06-Performance.md`](./06-Performance.md) | — |
| **Security model / threat surface** | [`docs/07-Security.md`](./07-Security.md) | — |
| **Testing strategy** | [`docs/08-Testing.md`](./08-Testing.md) | — |
| **Backlog** (epic-story `E#-S##`) | [`docs/10-Backlog.md`](./10-Backlog.md) | — |
| **Operations / observability / support** | [`docs/12-Operations.md`](./12-Operations.md) | — |

---

## 2. The governance docs (`docs/`)

The 12 numbered docs hold the formal, ID-tagged product contract.

| Doc | Purpose | Defines IDs |
|-----|---------|-------------|
| [`01-PRD.md`](./01-PRD.md) | Product Requirements — vision, users, goals | — |
| [`02-SRS.md`](./02-SRS.md) | **Software Requirements Spec — the FR/NFR registry** | `FR-NNN` (§3), `NFR-NNN` (§4); the §7 matrix maps requirements → test IDs (`TC-`, `PERF-`, `BENCH-`, `MIGRATE-`) |
| [`03-Architecture.md`](./03-Architecture.md) | System architecture & component shape | — |
| [`04-DataModel.md`](./04-DataModel.md) | Entities, schema, on-disk layout | — |
| [`05-API-Reference.md`](./05-API-Reference.md) | Public API surface (`PulseDB`, `SubstrateProvider`) | — |
| [`06-Performance.md`](./06-Performance.md) | Perf targets + benchmark methodology | — |
| [`07-Security.md`](./07-Security.md) | Security model, sensitivity, threat surface | — |
| [`08-Testing.md`](./08-Testing.md) | Test pyramid & strategy | — |
| [`09-Developer-Guide.md`](./09-Developer-Guide.md) | Contributor / build guide | — |
| [`10-Backlog.md`](./10-Backlog.md) | Backlog & epics | `E#-S##` |
| [`11-ProjectPlan.md`](./11-ProjectPlan.md) | v0.1.0 timeline doc (**not** the roadmap) | — |
| [`12-Operations.md`](./12-Operations.md) | Rollout, observability, support, deprecation/migration | — |

**ADRs** — [`docs/adr/`](./adr/) (ADR-001 redb storage · ADR-002 hnswlib · ADR-003
single-writer · ADR-004 rich experience types · ADR-005 pure-Rust HNSW). Index:
[`docs/adr/README.md`](./adr/README.md).

**Historical build-phase notes** — [`docs/phases/`](./phases/) (Phase 1 Foundation,
Phase 2 Substrate, Phase 3 Release; the original Weeks 1–10 MVP timeline). See the
disambiguation in [§3](#3-phase-means-two-things-here).

---

## 3. "Phase" means two things here ⚠️

The word **Phase** appears on two different axes — check which one you're reading:

| "Phase" usage | Where | Meaning | Example |
|---------------|-------|---------|---------|
| **Product / delivery phases** | [`../ROADMAP.md`](../ROADMAP.md) | The forward-looking Phase → Sprint → Slice plan | Phase 4 = "Production Reach"; Phase 5 = "Collective Intelligence" |
| **Historical build phases (1–3)** | [`docs/phases/`](./phases/) | The original MVP delivery timeline (Weeks 1–10) | Phase 2 = "Substrate" (Weeks 5–8) |

**Default:** unqualified "Phase 4/5/6" means the **ROADMAP product phases** (the live
planning artifact). The `docs/phases/` notes are historical.

---

## 4. ID conventions

| ID | Lives in | Meaning |
|----|----------|---------|
| `FR-NNN` | `02-SRS.md` §3 | Functional requirement |
| `NFR-NNN` | `02-SRS.md` §4 | Non-functional requirement |
| `VS-N.M.K` | `ROADMAP.md` | Vertical slice (phase.sprint.slice) |
| `ADR-NNN` | `docs/adr/` | Architecture decision |
| `E#-S##` | `10-Backlog.md` | Epic-story backlog item |
| `TC-` / `PERF-` / `BENCH-` / `MIGRATE-` | `02-SRS.md` §7 matrix | Test/verification IDs that a requirement traces to |

---

## 5. Tracing a requirement end-to-end (worked example)

> *"What does slice `VS-4.0.3` guarantee, and where's it verified?"*

1. **Slice → requirement:** read `VS-4.0.3`'s **Traceability** block in
   [`../ROADMAP.md`](../ROADMAP.md) → it cites `NFR-020`.
2. **Requirement definition:** `grep -n "#### NFR-020" docs/02-SRS.md` →
   the Storage-Format Upgrade Safety requirement + its `Measurement`.
3. **Requirement → test:** the SRS §7 traceability matrix maps `NFR-020 → MIGRATE-020`.
4. **Demo bar:** the slice's **Demo criteria** block in `ROADMAP.md` gives the
   `auto:` / `user:` acceptance checks.

That chain — **slice (ROADMAP) → requirement (SRS) → test (SRS §7)** — is the
canonical query path. Keep it intact when adding work.
