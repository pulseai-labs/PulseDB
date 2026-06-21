# PulseDB Documentation — Index & Map

> **Agent-query entry point.** This file maps every doc, where each ID type is
> defined, and how to trace a requirement → slice → test. Read this first when
> you need to find something; grep the specific doc second.

PulseDB's docs live in **two repos**:

- **This repo (canonical / product docs)** — what consumers and contributors
  read. The authoritative product contract: PRD, SRS, Architecture, ADRs, and
  the `ROADMAP.md` Phase→Sprint→Slice plan.
- **The paired AI workspace (`PulseDB-ai`, separate repo — process / SSoT)** —
  `MASTER-SPEC.md` (the prose source-of-truth the docs below are derived from),
  the tiered `memory-bank/`, and per-sprint `docs/specs/`. Not in this repo; see
  [§4](#4-the-ssot-lives-in-the-ai-workspace).

---

## 1. Where to look for X (quick agent lookup)

| You need… | Go to | Find it by |
|-----------|-------|-----------|
| A **functional requirement** (`FR-NNN`) | [`docs/02-SRS.md`](./02-SRS.md) §3 | `grep -n "#### FR-001" docs/02-SRS.md` |
| A **non-functional requirement** (`NFR-NNN`) | [`docs/02-SRS.md`](./02-SRS.md) §4 | `grep -n "#### NFR-020" docs/02-SRS.md` |
| **Requirement → test traceability** | [`docs/02-SRS.md`](./02-SRS.md) §7 (matrix) | search `## 7. Traceability Matrix` |
| The **roadmap** (phases, sprints, `VS-N.M.K` slices, demo criteria) | [`../ROADMAP.md`](../ROADMAP.md) | `grep -n "VS-4.0" ../ROADMAP.md` |
| An **architecture decision** (`ADR-NNN`) | [`docs/adr/`](./adr/) | [`docs/adr/README.md`](./adr/README.md) index |
| **Product vision / goals / users** | [`docs/01-PRD.md`](./01-PRD.md) | — |
| **Data model / entities / on-disk schema** | [`docs/04-DataModel.md`](./04-DataModel.md) | — |
| **Public API surface** | [`docs/05-API-Reference.md`](./05-API-Reference.md) | — |
| **Performance targets & benchmarks** | [`docs/06-Performance.md`](./06-Performance.md) | — |
| **Security model / threat surface** | [`docs/07-Security.md`](./07-Security.md) | — |
| **Testing strategy** | [`docs/08-Testing.md`](./08-Testing.md) | — |
| **Backlog items** (`E#-S#` / epics) | [`docs/10-Backlog.md`](./10-Backlog.md) | — |
| **Operations / observability / support** | [`docs/12-Operations.md`](./12-Operations.md) | — |
| The **prose source-of-truth** (FR/NFR are *derived* from it) | `PulseDB-ai/MASTER-SPEC.md` | (AI workspace repo) |

---

## 2. The governance docs (this repo, `docs/`)

The 12 numbered docs are **derived from `MASTER-SPEC.md`** (in the AI workspace) and
hold the formal, ID-tagged product contract.

| Doc | Purpose | Owns IDs |
|-----|---------|----------|
| [`01-PRD.md`](./01-PRD.md) | Product Requirements — vision, users, goals | — |
| [`02-SRS.md`](./02-SRS.md) | **Software Requirements Spec — the FR/NFR registry** | `FR-NNN` (§3), `NFR-NNN` (§4), traceability matrix (§7) |
| [`03-Architecture.md`](./03-Architecture.md) | System architecture & component shape | — |
| [`04-DataModel.md`](./04-DataModel.md) | Entities, schema, on-disk layout | — |
| [`05-API-Reference.md`](./05-API-Reference.md) | Public API surface (`PulseDB`, `SubstrateProvider`) | — |
| [`06-Performance.md`](./06-Performance.md) | Perf targets + benchmark methodology | `PERF-`, `BENCH-` |
| [`07-Security.md`](./07-Security.md) | Security model, sensitivity, threat surface | — |
| [`08-Testing.md`](./08-Testing.md) | Test pyramid & strategy | `TC-`, `MIGRATE-` |
| [`09-Developer-Guide.md`](./09-Developer-Guide.md) | Contributor / build guide | — |
| [`10-Backlog.md`](./10-Backlog.md) | Backlog & epics | `E#-S#` |
| [`11-ProjectPlan.md`](./11-ProjectPlan.md) | v0.1.0 timeline doc (Phase-2-Strategy-derived; **not** the roadmap) | — |
| [`12-Operations.md`](./12-Operations.md) | Rollout, observability, support, deprecation/migration | — |

**ADRs** — [`docs/adr/`](./adr/) (ADR-001 redb storage · ADR-002 hnswlib · ADR-003
single-writer · ADR-004 rich experience types · ADR-005 pure-Rust HNSW). Index:
[`docs/adr/README.md`](./adr/README.md).

**Historical build-phase notes** — [`docs/phases/`](./phases/) (Phase 1 Foundation,
Phase 2 Substrate, Phase 3 Release; the original Weeks 1–10 MVP timeline). See the
disambiguation in [§3](#3-phase-means-three-different-things).

---

## 3. "Phase" means three different things ⚠️

The word **Phase** is overloaded across the project. When you read "Phase N",
check which axis it's on:

| "Phase" usage | Where | Meaning | Example |
|---------------|-------|---------|---------|
| **Product / delivery phases** | [`../ROADMAP.md`](../ROADMAP.md) | Forward-looking Phase→Sprint→Slice plan | Phase 4 = "Production Reach"; Phase 5 = "Collective Intelligence" |
| **Onboarding phases (1–10)** | `PulseDB-ai/MASTER-SPEC.md` | The `/onboard` spec structure | Phase 4 = "Security & Compliance"; Phase 5 = "Architecture" |
| **Historical build phases (1–3)** | [`docs/phases/`](./phases/) | The original MVP delivery timeline (Weeks 1–10) | Phase 2 = "Substrate" (Weeks 5–8) |

**Default:** unqualified "Phase 4/5/6" almost always means the **ROADMAP product
phases**. The roadmap is the live planning artifact; the other two are spec
structure and history.

---

## 4. The SSoT lives in the AI workspace

These product docs are **derived**, not authored from scratch. The source-of-truth
is `MASTER-SPEC.md` in the paired AI workspace repo (`PulseDB-ai`), and the docs
here are regenerated/amended from it (see each doc's revision history). That repo
also holds:

- `MASTER-SPEC.md` — prose SSoT (onboarding phases 1–10).
- `.claude/memory-bank/` — tiered context router (`index.md` + `00`–`10` + live
  `05-active-context.md` / `06-progress.md`); the agent-query index for *process*
  state.
- `docs/specs/` — per-sprint vertical-slice specs, work-items, retrospectives.

**Editing rule:** to change a requirement, amend `MASTER-SPEC.md` first, then
amend the affected governance doc here (targeted amendment + a revision-history
entry — see `02-SRS.md`'s revision history for the pattern). Do not edit a
derived doc as if it were the source.

---

## 5. Tracing a requirement end-to-end (worked example)

> *"What does slice `VS-4.0.3` guarantee, and where's it tested?"*

1. **Slice → requirement:** read `VS-4.0.3`'s **Traceability** block in
   [`../ROADMAP.md`](../ROADMAP.md) → it cites `NFR-020`.
2. **Requirement definition:** `grep -n "#### NFR-020" docs/02-SRS.md` →
   the Storage-Format Upgrade Safety requirement + its `Measurement`.
3. **Requirement → test:** the SRS §7 traceability matrix maps `NFR-020 → MIGRATE-020`.
4. **Demo bar:** the slice's **Demo criteria** block in `ROADMAP.md` gives the
   `auto:` / `user:` acceptance checks.

That chain — **slice (ROADMAP) → requirement (SRS) → test (SRS §7 / Testing)** —
is the canonical query path. Keep it intact when adding work.
