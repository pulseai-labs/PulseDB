# PulseDB: Project Plan

> **Version:** 1.0.0  
> **Status:** Approved  
> **Last Updated:** February 2026  
> **Owner:** PulseDB Team

---

## 1. Overview

This document outlines the project plan for PulseDB MVP development, including timeline, milestones, resources, and risk management.

### 1.1 Project Summary

| Attribute | Value |
|-----------|-------|
| **Project Name** | PulseDB |
| **Type** | Open Source Library |
| **Duration** | 10 weeks |
| **Team Size** | 1 developer (agentic workflow) |
| **Target Release** | v0.1.0 |
| **Primary Consumer** | PulseHive |

### 1.2 Objectives

1. Deliver embedded database for agentic AI systems
2. Implement core hive mind primitives
3. Achieve SubstrateProvider compatibility with PulseHive
4. Publish to crates.io with comprehensive documentation

---

## 2. Timeline

### 2.1 Phase Overview

```
Week 1-2    Week 3-4    Week 5-6    Week 7-8    Week 9      Week 10
┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐
│ Phase 1 │ │ Phase 1 │ │ Phase 2 │ │ Phase 2 │ │ Phase 3 │ │ Phase 3 │
│Foundation│ │Foundation│ │Substrate│ │Substrate│ │ Polish  │ │ Release │
└─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘
     │           │           │           │           │           │
     ▼           ▼           ▼           ▼           ▼           ▼
   M1: DB      M2: Core    M3: HNSW   M4: Hive    M5: Tests   M6: Ship
   Opens      Storage    Working   Primitives   Passing    to Crates
```

### 2.2 Detailed Timeline

#### Phase 1: Foundation (Weeks 1-4)

| Week | Focus | Deliverables |
|------|-------|--------------|
| Week 1 | Project setup, redb integration | Cargo project, redb wrapper, basic tables |
| Week 2 | Collective management | create/list/delete collective, stats |
| Week 3 | Experience CRUD | record/get/update/delete experience |
| Week 4 | Embedding service | ONNX integration, builtin/external modes |

**Milestone M1 (End Week 1):** Database opens and closes correctly  
**Milestone M2 (End Week 4):** Core storage operations working

#### Phase 2: Substrate (Weeks 5-8)

| Week | Focus | Deliverables |
|------|-------|--------------|
| Week 5 | HNSW integration | hnsw_rs via VectorIndex trait, index per collective |
| Week 6 | Search operations | search_similar, get_recent, filters |
| Week 7 | Hive primitives | Relations, insights, activities |
| Week 8 | Context candidates | get_context_candidates, SubstrateProvider |

**Milestone M3 (End Week 6):** Vector search working  
**Milestone M4 (End Week 8):** All hive mind primitives complete

#### Phase 3: Polish & Release (Weeks 9-10)

| Week | Focus | Deliverables |
|------|-------|--------------|
| Week 9 | Real-time, testing | Watch system, test suite, benchmarks |
| Week 10 | Documentation, release | Docs, README, examples, crates.io |

**Milestone M5 (End Week 9):** All tests passing, coverage > 80%  
**Milestone M6 (End Week 10):** Published to crates.io

---

## 3. Milestones

### M1: Database Opens (Week 1)

| Criteria | Status |
|----------|--------|
| PulseDB::open() creates new database | ☐ |
| PulseDB::open() opens existing database | ☐ |
| PulseDB::close() flushes data | ☐ |
| Config validation working | ☐ |
| Basic smoke test passing | ☐ |

### M2: Core Storage (Week 4)

| Criteria | Status |
|----------|--------|
| Collective CRUD operations | ☐ |
| Experience CRUD operations | ☐ |
| Embedding generation (builtin) | ☐ |
| Embedding validation (external) | ☐ |
| Input validation complete | ☐ |
| Unit tests for all operations | ☐ |

### M3: Vector Search (Week 6)

| Criteria | Status |
|----------|--------|
| HNSW index integration | ☐ |
| search_similar() working | ☐ |
| get_recent_experiences() working | ☐ |
| Filters implemented | ☐ |
| Search latency < 50ms @ 100K | ☐ |

### M4: Hive Mind Primitives (Week 8)

| Criteria | Status |
|----------|--------|
| Relation storage | ☐ |
| Insight storage | ☐ |
| Activity tracking | ☐ |
| get_context_candidates() | ☐ |
| SubstrateProvider trait | ☐ |
| PulseDBSubstrate implementation | ☐ |

### M5: Quality (Week 9)

| Criteria | Status |
|----------|--------|
| Watch system working | ☐ |
| Test coverage > 80% | ☐ |
| All benchmarks passing | ☐ |
| No P0/P1 bugs open | ☐ |
| CI pipeline green | ☐ |

### M6: Release (Week 10)

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

## 4. Resource Allocation

### 4.1 Development Effort

| Phase | Weeks | Story Points | Hours (Est) |
|-------|-------|--------------|-------------|
| Foundation | 4 | 37 | 60-80 |
| Substrate | 4 | 57 | 80-100 |
| Polish | 2 | 18 | 30-40 |
| **Total** | **10** | **112** | **170-220** |

### 4.2 Weekly Capacity

| Mode | Hours/Week | Notes |
|------|------------|-------|
| Full-time | 40 | Dedicated development |
| Part-time | 20 | Parallel with other work |
| Agentic | Variable | With coding agents |

### 4.3 Dependencies

| Dependency | Version | Risk Level |
|------------|---------|------------|
| redb | 2.0+ | Low (stable) |
| hnsw_rs | 0.3+ | Low (pure Rust) |
| ort (ONNX) | 2.0+ | Medium (optional) |
| bincode | 1.3+ | Low (stable) |
| crossbeam-channel | 0.5+ | Low (stable) |

---

## 5. Risk Register

### 5.1 Technical Risks

| ID | Risk | Likelihood | Impact | Mitigation |
|----|------|------------|--------|------------|
| T1 | hnsw_rs integration | Low | Low | Pure Rust, wrapped behind VectorIndex trait (ADR-005) |
| T2 | ONNX model size bloats binary | Medium | Medium | Make optional, lazy loading |
| T3 | Performance targets not met | Low | High | Early benchmarking, profiling |
| T4 | redb limitations discovered | Low | High | Evaluate early, have backup plan |
| T5 | Cross-platform issues | Medium | Medium | CI on all platforms from start |

### 5.2 Resource Risks

| ID | Risk | Likelihood | Impact | Mitigation |
|----|------|------------|--------|------------|
| R1 | Solo developer bottleneck | High | Medium | Prioritize ruthlessly, MVP only |
| R2 | Scope creep | Medium | High | Strict out-of-scope list |
| R3 | Timeline slip | Medium | Medium | Buffer in schedule, cut scope |
| R4 | Burnout | Medium | High | Sustainable pace, take breaks |

### 5.3 External Risks

| ID | Risk | Likelihood | Impact | Mitigation |
|----|------|------------|--------|------------|
| E1 | Competitor releases similar | Low | Medium | First-mover advantage |
| E2 | Dependency breaks | Low | Medium | Pin versions, test on CI |
| E3 | PulseHive requirements change | Medium | Medium | Flexible SubstrateProvider |

### 5.4 Risk Response Plan

| Risk Level | Response |
|------------|----------|
| High (>= 2 risks triggered) | Scope reduction, timeline extension |
| Medium (1 risk triggered) | Overtime, reprioritize backlog |
| Low (0 risks triggered) | Proceed as planned |

---

## 6. Quality Gates

### 6.1 Definition of Done

A story is done when:

```markdown
Code:
- [ ] Implementation complete
- [ ] Unit tests written
- [ ] Integration tests (if applicable)
- [ ] No new warnings
- [ ] Code reviewed (self-review for solo)

Quality:
- [ ] Coverage maintained (>80%)
- [ ] No performance regression
- [ ] Error handling complete

Documentation:
- [ ] Public API documented
- [ ] Examples updated (if API changed)
```

### 6.2 Release Criteria

MVP release requires:

```markdown
Functionality:
- [ ] All Must-have stories complete
- [ ] PulseHive integration verified
- [ ] Demo working

Quality:
- [ ] Test coverage > 80%
- [ ] All performance targets met
- [ ] Zero P0/P1 bugs
- [ ] Security review complete

Documentation:
- [ ] All public API documented
- [ ] README complete
- [ ] Examples working
- [ ] CHANGELOG updated

Process:
- [ ] Version tagged
- [ ] CI green on all platforms
- [ ] crates.io publish successful
```

---

## 7. Communication Plan

### 7.1 Status Updates

| Event | Frequency | Format |
|-------|-----------|--------|
| Development log | Daily | Commit messages |
| Week summary | Weekly | Progress notes |
| Milestone review | Per milestone | Status report |

### 7.2 Decision Log

| Decision | Date | Rationale |
|----------|------|-----------|
| Use redb over SQLite | TBD | Pure Rust, simpler |
| Use hnsw_rs (pure Rust) | TBD | No FFI risks, VectorIndex trait for swappability (ADR-005) |
| Single-writer model | TBD | Simplicity, matches redb |

---

## 8. Development Environment

### 8.1 Required Setup

```bash
# Rust toolchain
rustup install stable
rustup default stable

# Required components
rustup component add rustfmt clippy

# No C++ build dependencies needed - hnsw_rs is pure Rust (ADR-005)

# macOS:
xcode-select --install
brew install cmake

# Windows:
# Install Visual Studio Build Tools + CMake
```

### 8.2 CI/CD Pipeline

```yaml
Pipeline stages:
1. Build (all platforms)
2. Test (unit + integration)
3. Lint (clippy + fmt)
4. Coverage (tarpaulin)
5. Benchmark (criterion, regression check)
6. Fuzz (on merge to main)
7. Publish (on tag)
```

---

## 9. Post-MVP Roadmap

### 9.1 v0.2.0 (Month 2)

- Entity-Relationship Graph
- Performance optimizations
- Community feedback integration

### 9.2 v0.3.0 (Month 3)

- Cross-Collective Wisdom
- KV Cache storage (REFRAG)
- Advanced watch features

### 9.3 v0.4.0 (Month 4-6)

- Python bindings
- Training data collection
- Production hardening

---

## 10. Appendix

### 10.1 Sprint Velocity Tracking

| Sprint | Planned | Completed | Velocity |
|--------|---------|-----------|----------|
| Sprint 1 | 18 pts | - | - |
| Sprint 2 | 19 pts | - | - |
| Sprint 3 | 16 pts | - | - |
| Sprint 4 | 20 pts | - | - |
| Sprint 5 | 21 pts | - | - |
| Sprint 6 | 18 pts | - | - |

### 10.2 Burndown Template

```
Story Points Remaining
    │
120 ┤ ●
    │   ●
100 ┤     ●
    │       ●
 80 ┤         ●
    │           ●
 60 ┤             ●
    │               ●
 40 ┤                 ●
    │                   ●
 20 ┤                     ●
    │                       ●
  0 ┼─────────────────────────●
    W1  W2  W3  W4  W5  W6  W7  W8  W9  W10
```

### 10.3 Key Contacts

| Role | Responsibility |
|------|----------------|
| Lead Developer | Implementation, decisions |
| PulseHive Team | Integration requirements |
| Community | Feedback, contributions |

---

## Changelog

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0.0 | February 2026 | PulseDB Team | Initial project plan |
