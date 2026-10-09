# ADR-006: Serializer selection — replace bincode with postcard

## Status

**Accepted** (2026-06-30, VS-4.0.3 work-1.06).

> **Empirical validation (flip from Proposed → Accepted).** postcard 1.1.3 landed at **every** storage and
> sync call site, the `bincode` crate dependency + its `RUSTSEC-2025-0141` `deny.toml` ignore were dropped,
> and `cargo deny check --all-features` is green. The on-disk bincode→postcard migration (vendored
> `legacy_bincode` decode → postcard re-encode, marker 1→2) is **value-identity verified** by the
> representative-fixture and `{redb-v3, bincode}` "Older"-fixture migration tests (collective/experience
> field-by-field; embeddings + secondary indexes byte-identical), and the 1.02 scale probe confirmed the
> encoding deltas (Experience −22.8%, Collective −26.3%; `Vec<f32>` parity). **Provisional gate:** this
> decision remains provisional until **VS-4.0.4's real-v0.5.1 golden-fixture** test passes against a real
> prior-release on-disk store; a vendored-decoder defect surfaced there blocks the sprint→main PR.
>
> **Update (2026-08-24): the provisional gate has PASSED** — `tests/storage_format_upgrade.rs` runs against the checked-in real v0.5.1 and v0.4.0 stores on `main`. The gate is discharged; this ADR is unconditionally Accepted.
>
> **Provenance:** decision produced by a per-serializer research fan-out (postcard / bitcode / rkyv + a
> cross-cutting RUSTSEC pass) and **adversarially verified** — a skeptic tried to refute postcard on every
> gating axis and each refutation failed (`refuted=false`, confidence **HIGH**). Scores: postcard **9**,
> wincode **6**, rkyv **4**, bitcode **2**. Authored as VS-4.0.1 / work 1.02; promoted to canonical here.

## Date

2026-06-21 (decided) · 2026-06-30 (accepted) · Deciders: PulseDB maintainers · Policy: NFR-020 (storage-format upgrade safety) + the Dependabot storage-format-major rule (spec + golden-fixture test required)

## Context

PulseDB persists every value in a single redb file, encoded with **bincode 1.3** (lockfile 1.3.3), and uses the same encoding on the **sync-http wire** (`src/sync/transport_http.rs`, `src/sync/server.rs`). The same bytes therefore must (a) survive on disk across PulseDB releases and (b) travel between peers — both are durable, cross-version contracts.

**bincode is unmaintained at all versions.** `RUSTSEC-2025-0141` covers the whole crate: *"Due to a doxxing and harassment incident, the bincode team has taken the decision to cease development permanently."* The 1.x line is frozen at the last working **1.3.3**; **3.0.0 is a non-functional tombstone**; **2.0.1 is the last working 2.x release** but is itself swept under the same advisory. Staying on bincode means depending on an unmaintained serializer for the storage substrate of a multi-agent system — unacceptable for a durable store. The advisory explicitly recommends four replacements: **wincode, postcard, bitcode, rkyv**. This ADR selects one to take over both the on-disk redb encoding and the sync-http wire.

**Constraints that gate the choice:**

- All 9 persisted shapes (`Collective`, `Experience`, `ExperienceRelation`, `DerivedInsight`, `Activity`, `WatchEventRecord`, `DatabaseMetadata`, per-collective `DecayConfig`, `SyncCursor`) already `#[derive(Serialize, Deserialize)]`. A serde-native replacement changes only the ~50 `bincode::serialize/deserialize` call sites; a non-serde replacement forces a per-type derive/schema migration.
- Field kinds in scope: `#[repr(u8)]` enums, `Option<T>`, `Vec<T>`, `String`, nested structs, `[u8;16]` UUID keys, `BTreeMap<InstanceId,u32>` G-counters, `Timestamp`.
- redb hands back values as **unaligned `&[u8]`** borrowed from its data pages.
- PulseDB MSRV (`rust-version`) = **1.89**; license must be permissive-compatible (project is `AGPL-3.0-only`).
- Wire-format stability is the **gating** axis: the encoded bytes are a durable cross-version contract on disk and on the sync wire.

## Alternatives Considered

| Crate (version) | Wire-format stability | serde compat | Size / speed (vs bincode) | Maintenance | MSRV / License |
|---|---|---|---|---|---|
| **postcard 1.1.3** (CHOSEN) | **Documented, versioned, stable since v1.0.0** — a break requires v2.0.0 + new spec version; cross-version *read* compat within 1.x. NOT self-describing (positional fields, ordinal enum variants) — same evolution discipline as bincode. | **Native serde** — true drop-in; only ~50 call sites change (`postcard::to_stdvec`/`to_allocvec` + `from_bytes`), no type changes. | Smaller (varint): ~31–36% smaller on structured records; serialize faster; deserialize comparable. `Vec<f32>` embeddings: parity (floats don't varint-compress). | Active/mature; 1.1.3 (2025-07-24), steady cadence; ~39.4M dl, 944 rev-deps; **0 RUSTSEC**; no_std-first; advisory itself recommends it. | No declared `rust-version` (builds well below 1.89); edition 2021; **MIT OR Apache-2.0**. |
| **wincode 0.5.5** | No documented/versioned cross-version stability guarantee (0.x); offers *optional* byte-compatibility with bincode 1.3 default config (potential no-rewrite migration). | **NOT serde** — own `SchemaRead`/`SchemaWrite` traits, per-type schema impls; self-described "not a complete drop-in replacement". HIGH churn. | bincode-equivalent bytes; placement-init read path claims faster deserialize. | anza-xyz (Solana/Anza), active; ~2.9M dl; 0 RUSTSEC; pre-1.0. | **MSRV 1.89.0** (exact match); Apache-2.0. |
| **bitcode 0.6.9** | **DISQUALIFIED** — "Stable format across major versions" and "Self describing format" are explicit **NON-GOALS**; docs: format "subject to change between major versions"; 0.5→0.6 was a breaking format change. | serde only behind optional `serde` feature; fastest/smallest path is its own `Encode`/`Decode` derives. | Smallest output, ~2–2.5× faster serialize, highly compressible — but native-derive numbers, not serde-path. | Softbear Studios, active; ~7.2M dl; 0 RUSTSEC; pre-1.0, smaller bus-factor. | MSRV 1.70; MIT OR Apache-2.0. |
| **rkyv 0.8.16** | Stable within 0.8 line, but pre-1.0 (0.7→0.8 broke format; 0.8→1.0 expected break); format-control features (endianness/alignment/`unaligned`) are breaking; `repr(u8)` discriminants NOT honored (rkyv-generated tags). | **NOT serde** — own `Archive`/`Serialize`/`Deserialize` derives; largest change surface (~48 derive lines + call-site rewrite). | Serialize/deserialize faster, but **~36% LARGER on disk**; zero-copy access needs aligned buffers which **redb's unaligned `&[u8]` does not provide** (align-copy erases the win, or `unaligned` feature = permanent format break). | Very active; ~120M dl; **3 RUSTSEC** (2021-0054, 2026-0001, 2026-0122) all patched ≤0.8.16. | MSRV 1.81; MIT. |

## Decision

**Adopt `postcard` version `1.1.3`** as the serializer for both the on-disk redb values and the sync-http wire, replacing bincode 1.3.

Rationale — postcard is the **only** advisory-recommended crate that satisfies both hard requirements:

1. **Serde-native drop-in** — the 9 persisted types are unchanged; only the ~50 `bincode::serialize/deserialize` call sites swap to `postcard::to_stdvec`/`to_allocvec` + `from_bytes`. Lowest change surface of any candidate.
2. **Documented, versioned, stable wire format** — *"The Postcard wire format is considered stable as of v1.0.0 and above"*; a breaking change forces v2.0.0 plus a new spec version. Strictly *better* than bincode 1.3, which never published a spec.

Supporting: advisory-clean (OSV empty, rustsec packages page 404), MIT OR Apache-2.0, ~39.4M downloads / 944 reverse-deps, no_std-first, and the bincode advisory itself names postcard as a recommended replacement. All PulseDB field kinds encode cleanly; varint encoding makes the integer/enum/string-heavy records smaller.

The losing candidates each fail a hard requirement: **bitcode** disqualifies itself on format stability (explicit non-goal); **wincode** is not serde and publishes no versioned stability guarantee; **rkyv** is not serde, regresses ~36% on disk size, carries the only advisory history, and structurally clashes with redb's unaligned `&[u8]` (its zero-copy advantage is unrealizable here on small per-key records).

> **`postcard2` is NOT in scope.** A separate `postcard2` v0.2.1 (Dec 2025, edition 2024) exists as the in-progress next-gen effort; it is pre-1.0 with a still-moving spec. Use **stable postcard 1.1.3** for durable storage.

**Pinning:** pin `postcard = "=1.1.3"` (or assert MSRV in CI), because postcard declares no `rust-version` and a future patch could silently raise the effective MSRV.

**Legacy decode (how the bincode crate was dropped without a flag day):** rather than retaining bincode as a "read-only" dependency, VS-4.0.3 vendored a **decode-only bincode-1.3 `DefaultOptions` reader** (`src/storage/legacy_bincode.rs`, no `bincode` crate dependency). On first writable open of an older store, legacy bytes are decoded via this vendored reader and re-encoded as postcard (substrate marker 1→2). This is what allowed the `bincode` crate — and its `RUSTSEC-2025-0141` `deny.toml` ignore — to be removed entirely (work-1.06).

## Consequences

### Positive

- Escapes the unmaintained-serializer advisory (RUSTSEC-2025-0141) that motivated the sprint — the `deny.toml` ignore dropped once the bincode **crate** was gone (VS-4.0.3 work-1.06).
- Upgrades the *encoding-rule* stability story from "no published spec" (bincode) to "documented, versioned, stable-since-v1.0.0 with v2.0.0-break discipline" (postcard).
- Smaller on-disk records for integer/enum/string/`BTreeMap`-heavy shapes via varint; faster serialize.
- Minimal code churn: type definitions untouched, ~50 call sites changed.
- no_std-first and permissively licensed; clean advisory posture.

### Negative

- **Not self-describing — same fragility class as bincode 1.x.** Structs encode fields in declaration order with no names/length; enum variants encode by ordinal discriminant index (the `#[repr(u8)]` value is irrelevant on the wire). Adding/removing/reordering a struct field, or inserting an enum variant mid-list, **silently breaks decode of existing on-disk data**. (No regression vs bincode, but no improvement to schema evolution either.)
- **bincode bytes and postcard bytes are NOT interchangeable.** The switch is a hard format break: a **one-time on-disk re-encode is required** — coupled to migration-on-open (VS-4.0.3 work-1.04). There is no in-place reinterpret.
- **Sync-http wire implication:** the wire format changes with the on-disk format. A bincode-era peer and a postcard-era peer cannot exchange records directly — the sync wire negotiates a format-version (a fixed-layout preamble + `SYNC_PROTOCOL_VERSION` 2→3, work-1.03) so mixed-version peers fail loud rather than mis-decode.
- postcard declares no MSRV — a future patch could raise the effective floor silently (mitigated by the `=1.1.3` pin).
- Embedding `Vec<f32>` payloads won't shrink (floats don't varint-compress) — size wins concentrate in the structured records, not the 384-d MiniLM vectors. (In production, embeddings are stored as raw LE `f32` bytes, not serde, so the codec migration does not touch the size-dominant table at all.)
- `#[serde(flatten)]` and `#[serde(skip_serializing_if)]` are unsupported/problematic under postcard.

### Mitigations (implemented across VS-4.0.3; real-fixture gate in VS-4.0.4)

1. **Format-version envelope:** a per-file substrate-format marker + a sync-wire preamble so a postcard-era reader identifies bincode-era data and triggers migration, and the sync wire negotiates/fails-loud (work-1.03 / work-1.04).
2. **Golden-fixture cross-version round-trip tests** (**VS-4.0.4**, required gate): pin the postcard byte layout for each of the 9 shapes against a **full prior-release redb-2.x DB file**; fail CI on any unintended layout change.
3. **Migration-on-open path** (work-1.04): one-time bincode→postcard re-encode on open via a **vendored bincode-1.3 decoder carrying no maintained-crate dependency**.
4. **Append-only schema discipline:** lock current field/variant order; only append at the end.
5. **Grep audit:** confirmed none of the 9 persisted types use `#[serde(flatten)]`/`#[serde(skip_serializing_if)]`.
6. **Pin `=1.1.3`** given the undeclared `rust-version`.
7. **Sequenced with the redb 2.x→4.x bump as a single format-version migration** — one envelope bump, one migration-on-open window (VS-4.0.2 + VS-4.0.3).
8. **Empirical size/speed validation** on real PulseDB fixtures (work-1.02 scale probe) — confirmed the directional deltas and the single-transaction memory budget.

## References

- RUSTSEC advisory (bincode unmaintained) — https://rustsec.org/advisories/RUSTSEC-2025-0141
- postcard wire-format spec — https://postcard.jamesmunns.com/wire-format
- postcard crate — https://crates.io/crates/postcard (1.1.3)
- Code: `src/storage/redb.rs` (storage codec) · `src/storage/legacy_bincode.rs` (vendored decode) · `src/sync/transport_http.rs` + `src/sync/server.rs` (sync wire)
- Prior storage ADR: [ADR-001-redb-for-storage.md](ADR-001-redb-for-storage.md)

### Verified claims

- On-disk format claims: verified by the golden-fixture upgrade tests (`tests/storage_format_upgrade.rs`) against real v0.5.1 / v0.4.0 stores, for the axes those fixtures cover (v0.5.1: collective + experience + embeddings + indices, schema-v3; v0.4.0: all three axes). Entity kinds outside the fixtures (activities, decay configs, sync cursors) are covered by representative synthetic-store migration tests, not by the real fixtures.
- postcard `=1.1.3` resolves in the shipped dependency set; `cargo deny` green with bincode dropped.

### Unverified claims

- Maintenance/census figures in the alternatives table (download counts, 0-RUSTSEC status, release cadence) are research facts as of 2026-06, not re-verified at adoption 2026-08-23.
- **Sync wire-format stability is unverified**: the sync tests serialize/deserialize with the *current* postcard on both sides (same-version round-trip), and `storage_format_upgrade.rs` exercises only on-disk redb fixtures — no frozen prior-release wire-byte fixture exists. A field reorder could change the wire layout while every test stays green. Mixed-release sync compatibility depends on catching that (protocol bump); frozen wire fixtures would close this.

<!-- Appended at ossify adoption smoke pass, 2026-08-23 -->

## Amendment 2026-10-09 — sync protocol v5

**Context:** The r1.s1 recovery supersedes the earlier sync-version account above. **Decision:** Retain postcard, but use protocol v5/framing v4 and wire-only `SyncExperience` embeddings; the on-disk Experience encoding remains unchanged (`src/sync/mod.rs:167–180`, `src/sync/mod.rs:231`, `src/sync/types.rs:186–224`). **Consequences:** No v4 interoperability; both replicas upgrade. [ADR-014's amendment](ADR-014-layered-sync-core-with-port-traits.md#amendment-2026-10-09--sync-protocol-v5) records routing, exact byte packing, typed noncompletion and the approved capability-negotiation deviation.
