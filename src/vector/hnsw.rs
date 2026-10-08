//! HNSW vector index implementation using hnsw_rs.
//!
//! Wraps `hnsw_rs::Hnsw<f32, DistCosine>` with:
//! - Bidirectional `ExperienceId` ↔ `usize` ID mapping
//! - Soft-delete via `HashSet` + filtered search
//! - JSON metadata persistence (`.hnsw.meta`)
//!
//! # Thread Safety
//!
//! The `hnsw_rs::Hnsw` graph uses `parking_lot::RwLock` internally,
//! so `insert()` takes `&self`. Our metadata (`IndexState`) is
//! protected by `std::sync::RwLock`.

#[cfg(test)]
use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::sync::RwLock;

use hnsw_rs::prelude::*;

use crate::config::HnswConfig;
use crate::error::{PulseDBError, Result};
use crate::types::ExperienceId;

use super::VectorIndex;

/// Below this threshold, search uses brute-force linear scan instead of HNSW
/// graph traversal. hnsw_rs stores each point only in its assigned layer, so
/// points placed in upper layers are unreachable during layer-0 search. For
/// small collections this causes missed results. Linear scan is both more
/// reliable (100% recall) and faster (no graph overhead) at this scale.
const BRUTE_FORCE_THRESHOLD: usize = 128;

/// Compensates hnsw_rs 0.3.4's missing second exp() at hnsw.rs line 453
/// (https://github.com/jean-pierreBoth/hnswlib-rs/issues/41). Remove when fixed
/// upstream. This gives a total reservation of about `max_elements` slots;
/// the skew between layers remains, and layer zero grows by ordinary Vec growth.
fn allocation_hint(config: &HnswConfig) -> usize {
    let s = 1. / (config.max_nb_connection as f64).ln();
    let fractions: Vec<f64> = (0..config.max_layer.min(16))
        .map(|i| (-(i as f64) / s).exp() - (-((i + 1) as f64) / s))
        .collect();
    let factor: f64 = fractions.iter().sum();
    if factor.is_nan() || factor < 1. {
        return config.max_elements;
    }
    let reservation = |hint: usize| {
        fractions.iter().fold(0usize, |total, frac| {
            total.saturating_add((frac * hint as f64).round() as usize)
        })
    };
    // Search the rounded sum, rather than rounding N/factor: the latter can
    // under-reserve or skip the smallest hint at rounding boundaries.
    let mut low = 1;
    let mut high = config.max_elements.max(1);
    while low < high {
        let mid = low + (high - low) / 2;
        if reservation(mid) >= config.max_elements {
            high = mid;
        } else {
            low = mid + 1;
        }
    }
    low
}

/// Newtype wrapper that bridges `&dyn Fn(&usize) -> bool` to `FilterT`.
///
/// Rust's blanket impl `impl<F: Fn(&DataId) -> bool> FilterT for F` only
/// works for concrete types. When we have a `&dyn Fn` trait object (from the
/// `VectorIndex` trait's `search_filtered` method), we can't coerce it to
/// `&dyn FilterT` directly. This wrapper implements `FilterT` by delegating
/// to the wrapped closure trait object.
struct FilterBridge<'a>(&'a (dyn Fn(&usize) -> bool + Sync));

impl FilterT for FilterBridge<'_> {
    fn hnsw_filter(&self, id: &DataId) -> bool {
        (self.0)(id)
    }
}

/// HNSW vector index backed by `hnsw_rs`.
///
/// Each collective gets its own `HnswIndex` instance, providing
/// complete data isolation between collectives.
///
/// # Persistence Strategy
///
/// Metadata (ID mappings, deleted set) is persisted to a JSON `.hnsw.meta`
/// file. The graph itself is rebuilt from redb embeddings on open, because
/// `hnsw_rs::HnswIo::load_hnsw` has lifetime constraints that create
/// self-referential struct issues. The graph dump files (via `file_dump`)
/// are saved for future optimization but not currently loaded.
pub struct HnswIndex {
    /// The underlying HNSW graph. Uses `'static` lifetime because
    /// all data is heap-owned (not memory-mapped).
    hnsw: Hnsw<'static, f32, DistCosine>,

    /// Mutable metadata protected by RwLock.
    state: RwLock<IndexState>,

    /// Immutable configuration (used during save/rebuild lifecycle).
    #[allow(dead_code)]
    config: HnswConfig,

    /// Embedding dimension (must match all inserted vectors).
    dimension: usize,
}

/// Internal mutable state for ID mapping and soft-deletion.
#[derive(Debug)]
struct IndexState {
    /// Forward map: ExperienceId → internal usize ID.
    ///
    /// An entry here means the graph insert for that id **returned**: the
    /// mapping is published after the insert, never before, so a reader can
    /// treat it as "search can reach this point".
    id_to_internal: HashMap<ExperienceId, usize>,

    /// Reverse map: internal usize ID → ExperienceId.
    /// Uses Vec for O(1) lookup by index.
    ///
    /// `None` marks an internal id that was claimed but never published — a
    /// graph insert that failed or unwound. Every search path skips those
    /// slots rather than treating them as a mapping.
    internal_to_id: Vec<Option<ExperienceId>>,

    /// Set of soft-deleted internal IDs (excluded from search).
    ///
    /// Holds live mappings, and — transiently — the internal id of an insert
    /// that is still in flight when its id is deleted. Live counts subtract
    /// only deleted IDs whose mappings have actually been published.
    deleted: HashSet<usize>,

    /// Next internal ID to assign (monotonically increasing).
    next_id: usize,

    /// Inserts in flight: ExperienceId → the internal id claimed for it.
    ///
    /// A claim is taken before the graph insert and released after the mapping
    /// is published, so a second insert of the same id can tell "another
    /// thread is putting this point in right now" from "this id is absent" and
    /// refrain from adding a second graph point for it.
    pending: HashMap<ExperienceId, usize>,
}

impl IndexState {
    fn active_count(&self) -> usize {
        let deleted_published = self
            .deleted
            .iter()
            .filter(|&&id| self.internal_to_id.get(id).is_some_and(Option::is_some))
            .count();
        self.id_to_internal.len() - deleted_published
    }

    /// Grows `internal_to_id` until `internal_id` is addressable.
    ///
    /// Ids are handed out in increasing order, so growth is the common case;
    /// the resize is what makes a *non*-adjacent id (one published while an
    /// earlier insert was still in flight, or one whose predecessor failed)
    /// addressable at its own index instead of shifting every later mapping.
    fn reserve_slot(&mut self, internal_id: usize) {
        if self.internal_to_id.len() <= internal_id {
            self.internal_to_id.resize(internal_id + 1, None);
        }
    }
}

/// Serializable metadata for persistence.
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct IndexMetadata {
    pub(crate) dimension: usize,
    pub(crate) next_id: usize,
    /// Vec of (ExperienceId UUID string, internal usize ID) pairs.
    pub(crate) id_map: Vec<(String, usize)>,
    /// Deleted ExperienceId UUID strings (not internal IDs).
    ///
    /// We store UUIDs instead of internal usize IDs because internal IDs
    /// are reassigned sequentially on rebuild. Using UUIDs ensures the
    /// correct experiences are marked as deleted after rebuild.
    pub(crate) deleted: Vec<String>,
}

/// Releases an in-flight claim on an id if the graph insert never returned.
///
/// Without this, a graph insert that unwinds would leave the id claimed
/// forever: every later `insert_experience` of it would read the claim as
/// "another thread is on it" and return without a point, so the id could never
/// become searchable. The abandoned internal id is not reused — its slot in
/// `internal_to_id` stays `None` and every search path skips it — so the id
/// counter keeps moving forward and no live mapping is ever displaced.
struct PendingClaim<'a> {
    index: &'a HnswIndex,
    exp_id: ExperienceId,
    internal_id: usize,
    /// Set once the mapping has been published, at which point there is nothing
    /// left to release.
    published: bool,
}

impl Drop for PendingClaim<'_> {
    fn drop(&mut self) {
        if self.published {
            return;
        }
        // Best effort: a poisoned lock means the whole index is unusable
        // anyway, and a panic here would replace the caller's.
        if let Ok(mut state) = self.index.state.write() {
            if state.pending.get(&self.exp_id) == Some(&self.internal_id) {
                state.pending.remove(&self.exp_id);
                // The id never landed, so a soft-delete mark taken against the
                // claim has nothing left to apply to. Dropping it keeps
                // `deleted` a subset of "mapped or pending".
                state.deleted.remove(&self.internal_id);
            }
        }
    }
}

impl HnswIndex {
    /// Creates a new empty HNSW index.
    ///
    /// # Arguments
    ///
    /// * `dimension` - Expected embedding dimension (validated on insert)
    /// * `config` - HNSW tuning parameters
    ///
    /// Compensates hnsw_rs 0.3.4's allocation hint for a total reservation of
    /// about `max_elements` slots. Layer zero grows independently as points
    /// arrive; this does not reserve `max_elements` slots in every layer.
    pub fn new(dimension: usize, config: &HnswConfig) -> Self {
        let hnsw = Hnsw::new(
            config.max_nb_connection,
            allocation_hint(config),
            config.max_layer,
            config.ef_construction,
            DistCosine,
        );

        Self {
            hnsw,
            state: RwLock::new(IndexState {
                id_to_internal: HashMap::new(),
                internal_to_id: Vec::new(),
                deleted: HashSet::new(),
                next_id: 0,
                pending: HashMap::new(),
            }),
            config: config.clone(),
            dimension,
        }
    }

    /// Checks an embedding against this index's dimension WITHOUT inserting it.
    ///
    /// [`insert_experience`](Self::insert_experience) applies exactly this
    /// check, so a caller that needs to know an insert would be rejected —
    /// before it commits anything else that would have to be undone — can ask
    /// here and get the same answer for the same reason.
    ///
    /// A pass is not a promise: the insert can still fail afterwards (a
    /// poisoned state lock), and nothing here reserves the id.
    pub fn validate_embedding(&self, embedding: &[f32]) -> Result<()> {
        if embedding.len() != self.dimension {
            return Err(PulseDBError::vector(format!(
                "Embedding dimension mismatch: expected {}, got {}",
                self.dimension,
                embedding.len()
            )));
        }
        Ok(())
    }

    /// Inserts an experience embedding into the index.
    ///
    /// If the ExperienceId is already present, this is a no-op — including an
    /// id that is present but soft-deleted: reviving one of those is
    /// `clear_deleted_mark`'s job, and a second graph point for the same id is
    /// never the answer.
    ///
    /// # Publishing order, and why it is load-bearing
    ///
    /// The id is claimed, the graph insert runs, and only then is the mapping
    /// published. So [`contains`](Self::contains) answers "search can reach this
    /// point", not "someone tried to insert this id" — an insert that fails or
    /// unwinds leaves nothing behind, and a later insert of the same id
    /// succeeds and becomes searchable. A claim that is abandoned (the graph
    /// insert unwound) is released by
    /// [`PendingClaim`]'s `Drop`, so it can never wedge the id.
    ///
    /// The state lock is released around the graph insert, which keeps the
    /// lock order the search paths already use (`state` then the graph's own
    /// lock — never the other way round) and keeps a search from waiting on an
    /// insert. Two concurrent inserts of one id add at most one graph point:
    /// the second sees the claim and returns.
    pub fn insert_experience(&self, exp_id: ExperienceId, embedding: &[f32]) -> Result<()> {
        self.validate_embedding(embedding)?;

        // Claim an internal id, then let go of the lock: the graph insert must
        // not run under `state` (search walks the graph while holding it).
        let internal_id = {
            let mut state = self
                .state
                .write()
                .map_err(|_| PulseDBError::vector("Index state lock poisoned"))?;

            // Already published: idempotent, mapping and point both in place.
            if state.id_to_internal.contains_key(&exp_id) {
                return Ok(());
            }
            // In flight on another thread: one point per id is the contract,
            // so this call joins that insert instead of racing a second point
            // into the graph.
            if state.pending.contains_key(&exp_id) {
                return Ok(());
            }

            // Assign next sequential internal ID
            let internal_id = state.next_id;
            state.next_id += 1;
            state.pending.insert(exp_id, internal_id);
            internal_id
        };

        let mut claim = PendingClaim {
            index: self,
            exp_id,
            internal_id,
            published: false,
        };

        #[cfg(test)]
        maybe_fail_graph_insert();

        // Insert into HNSW graph (uses interior mutability via parking_lot::RwLock)
        self.hnsw.insert((embedding, internal_id));

        // The point is in the graph — now, and not before, is the mapping true.
        {
            let mut state = self
                .state
                .write()
                .map_err(|_| PulseDBError::vector("Index state lock poisoned"))?;
            state.pending.remove(&exp_id);
            state.id_to_internal.insert(exp_id, internal_id);
            state.reserve_slot(internal_id);
            state.internal_to_id[internal_id] = Some(exp_id);
        }
        claim.published = true;

        Ok(())
    }

    /// Test-only: takes an in-flight claim on `exp_id`, leaving it held, and
    /// returns the internal id it claimed.
    ///
    /// A real insert holds exactly this state for the duration of its graph
    /// insert. Nothing else can construct it without a second thread and a
    /// timing window, and the two behaviours that live in that window — a
    /// delete landing inside it, and a second insert that joins it publishing
    /// nothing — are what the callers of this probe assert. `#[cfg(test)]`-only
    /// and crate-visible: absent from non-test builds, and not public API.
    #[cfg(test)]
    pub(crate) fn claim_for_test(&self, exp_id: ExperienceId) -> usize {
        let mut state = self
            .state
            .write()
            .expect("test claim: index state lock poisoned");
        assert!(
            !state.id_to_internal.contains_key(&exp_id),
            "test claim: the id is already published"
        );
        assert!(
            !state.pending.contains_key(&exp_id),
            "test claim: the id is already claimed"
        );
        let internal_id = state.next_id;
        state.next_id += 1;
        state.pending.insert(exp_id, internal_id);
        internal_id
    }

    /// Marks an experience as deleted in the index.
    ///
    /// The vector remains in the graph but is excluded from search
    /// results via filtered search. Returns Ok even if the experience
    /// is not in the index (idempotent).
    ///
    /// An id whose insert is still in flight is deleted too: the delete marks
    /// the internal id that insert has claimed, so the publish that follows
    /// lands already soft-deleted instead of resurrecting an id the store no
    /// longer has.
    pub fn delete_experience(&self, exp_id: ExperienceId) -> Result<()> {
        let mut state = self
            .state
            .write()
            .map_err(|_| PulseDBError::vector("Index state lock poisoned"))?;

        if let Some(&internal_id) = state.id_to_internal.get(&exp_id) {
            state.deleted.insert(internal_id);
        } else if let Some(&pending_id) = state.pending.get(&exp_id) {
            // In flight, not yet published. Marking the pending internal id is
            // what makes the delete survive the insert that finishes after it.
            state.deleted.insert(pending_id);
        }

        Ok(())
    }

    /// Searches for the k nearest experiences, excluding deleted ones.
    ///
    /// Returns `(ExperienceId, distance)` pairs sorted by distance
    /// ascending (closest first). Distance is cosine distance:
    /// 0.0 = identical, 2.0 = opposite.
    pub fn search_experiences(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
    ) -> Result<Vec<(ExperienceId, f32)>> {
        self.search_experiences_with_allowed(query, k, ef_search, None)
    }

    /// Searches with an optional **allowed-set** constraint applied during
    /// traversal (filtered ANN, not post-filter).
    ///
    /// When `allowed` is `Some(set)`, only experiences whose ExperienceId is in
    /// `set` are considered — the HNSW graph traversal skips non-allowed nodes
    /// entirely. This bounds the work done by the vector index, which is the
    /// load-bearing requirement of VS-4.3.2: a search for `k` results among a
    /// tagged subset returns exactly `k` tagged results, not `k′ < k` after a
    /// post-recall truncate. If ANN cannot fill that budget because of graph
    /// fragmentation, an exact scan of eligible points completes the search.
    ///
    /// When `allowed` is `None`, behavior is identical to
    /// [`search_experiences`](Self::search_experiences).
    pub fn search_experiences_with_allowed(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
        allowed: Option<&HashSet<ExperienceId>>,
    ) -> Result<Vec<(ExperienceId, f32)>> {
        if query.len() != self.dimension {
            return Err(PulseDBError::vector(format!(
                "Query dimension mismatch: expected {}, got {}",
                self.dimension,
                query.len()
            )));
        }

        let state = self
            .state
            .read()
            .map_err(|_| PulseDBError::vector("Index state lock poisoned"))?;

        // Pending/raw graph IDs are not part of the published search budget.
        let active_count = state.active_count();
        if active_count == 0 {
            return Ok(vec![]);
        }

        // Convert the allowed ExperienceId set to internal IDs for the HNSW
        // filter predicate. Unknown IDs (deleted or not in index) are silently
        // dropped — the caller's allowed set is an over-approximation.
        let allowed_internal: Option<HashSet<usize>> = allowed.map(|ids| {
            ids.iter()
                .filter_map(|exp_id| state.id_to_internal.get(exp_id).copied())
                .filter(|internal| !state.deleted.contains(internal))
                .collect()
        });

        // Cap effective_k to the searchable point count so the HNSW engine
        // doesn't over-search when the allowed set is small.
        let searchable = match &allowed_internal {
            Some(internal) => active_count.min(internal.len()),
            None => active_count,
        };
        if searchable == 0 {
            return Ok(vec![]);
        }
        let effective_k = k.min(searchable);

        if active_count > BRUTE_FORCE_THRESHOLD {
            // HNSW graph search for larger collections.
            let effective_ef = ef_search.max(effective_k);
            let deleted_ref = &state.deleted;

            // Mapping-facing search always excludes unpublished/raw points before
            // they can consume k. The raw-ID VectorIndex search APIs stay unchanged.
            let published = &state.internal_to_id;
            let allowed_ref = allowed_internal.as_ref();
            let filter_fn = move |id: &usize| -> bool {
                published.get(*id).is_some_and(Option::is_some)
                    && !deleted_ref.contains(id)
                    && allowed_ref.is_none_or(|internal| internal.contains(id))
            };
            let results =
                self.hnsw
                    .search_filter(query, effective_k, effective_ef, Some(&filter_fn));

            // Map internal IDs back to ExperienceIds
            let mapped: Vec<(ExperienceId, f32)> = results
                .into_iter()
                .filter_map(|n| {
                    state
                        .internal_to_id
                        .get(n.d_id)
                        .copied()
                        .flatten()
                        .map(|exp_id| (exp_id, n.distance))
                })
                .collect();

            if mapped.len() == effective_k {
                return Ok(mapped);
            }
        }

        // Small collections use exact search. Larger collections reach this
        // fallback only when filtered ANN underfills the published budget:
        // random graph fragmentation can make live points unreachable from
        // its pivot even with ef greater than the whole collection. Preserve
        // full k without admitting unpublished, deleted, or disallowed points.
        let dist_fn = DistCosine;
        let mut all_distances: Vec<(ExperienceId, f32)> = Vec::with_capacity(searchable);

        for point in self.hnsw.get_point_indexation().into_iter() {
            let origin_id = point.get_origin_id();
            if state.deleted.contains(&origin_id) {
                continue;
            }
            // Skip non-allowed points when an allowed set is provided.
            if let Some(ref internal) = allowed_internal {
                if !internal.contains(&origin_id) {
                    continue;
                }
            }
            let distance = dist_fn.eval(query, point.get_v());
            // A point whose mapping is not published (an insert still in
            // flight, or one that never landed) is skipped, never panicked
            // on: the graph can hold a point this index cannot name yet.
            if let Some(exp_id) = state.internal_to_id.get(origin_id).copied().flatten() {
                all_distances.push((exp_id, distance));
            }
        }

        all_distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        all_distances.truncate(effective_k);
        Ok(all_distances)
    }

    /// Returns true if the given experience is in the index (and not deleted).
    ///
    /// The same predicate the crate's sync paths ask for by name
    /// (`is_searchable`), under the name this type has always had. Callers that
    /// mean "can search find this" should ask for searchability explicitly: a
    /// soft-deleted id stays *mapped*, and the two states are easy to conflate.
    pub fn contains(&self, exp_id: ExperienceId) -> bool {
        self.is_searchable(exp_id)
    }

    /// Returns true only when a search can find `exp_id` right now.
    ///
    /// Crate-visible: the sync repair path is the caller that needs the name,
    /// and it is not public API.
    ///
    /// Both halves are required: the id must be mapped (its graph insert
    /// returned — see [`insert_experience`](Self::insert_experience)) **and**
    /// free of a soft-delete mark. A soft-deleted id keeps its mapping, so a
    /// check that only looked at the mapping would call it searchable.
    pub(crate) fn is_searchable(&self, exp_id: ExperienceId) -> bool {
        let state = self.state.read().ok();
        state.is_some_and(|s| {
            s.id_to_internal
                .get(&exp_id)
                .is_some_and(|id| !s.deleted.contains(id))
        })
    }

    /// Clears a soft-delete mark, making a mapped id searchable again.
    ///
    /// Crate-visible: the sync repair path is the only caller, and it is not
    /// public API. Without the `sync` feature nothing in the crate calls it,
    /// which is what the conditional `allow` says.
    ///
    /// Returns whether a mark was actually cleared: `false` for an id that is
    /// not mapped, and for one that was not soft-deleted.
    ///
    /// Nothing is re-inserted. The graph point is still there and already
    /// carries this id's embedding, and **experience embeddings are immutable
    /// per id**, so the point a search would now reach holds the same vector
    /// the id has in redb — which is also why this cannot resurrect a point
    /// whose embedding later changed: nothing can.
    #[cfg_attr(not(feature = "sync"), allow(dead_code))]
    pub(crate) fn clear_deleted_mark(&self, exp_id: ExperienceId) -> Result<bool> {
        let mut state = self
            .state
            .write()
            .map_err(|_| PulseDBError::vector("Index state lock poisoned"))?;
        let Some(&internal_id) = state.id_to_internal.get(&exp_id) else {
            return Ok(false);
        };
        Ok(state.deleted.remove(&internal_id))
    }

    /// Returns the number of active (non-deleted) vectors.
    ///
    /// Only published mappings count; a deleted pending or raw graph ID does
    /// not reduce the count of unrelated searchable experiences.
    pub fn active_count(&self) -> usize {
        let state = self.state.read().ok();
        state.map_or(0, |s| s.active_count())
    }

    /// Returns the total number of vectors (including deleted).
    pub fn total_count(&self) -> usize {
        self.hnsw.get_nb_point()
    }

    /// Restores the deleted set from persisted metadata.
    ///
    /// Called during `PulseDB::open()` after rebuilding the graph from redb.
    /// Accepts ExperienceId UUID strings and maps them to the current
    /// internal IDs (which may differ from the previous session's IDs
    /// after a rebuild).
    pub fn restore_deleted_set(&self, deleted_exp_ids: &[String]) -> Result<()> {
        let mut state = self
            .state
            .write()
            .map_err(|_| PulseDBError::vector("Index state lock poisoned"))?;
        for exp_id_str in deleted_exp_ids {
            // Parse UUID string back to ExperienceId
            let uuid = uuid::Uuid::parse_str(exp_id_str)
                .map_err(|e| PulseDBError::vector(format!("Invalid UUID in deleted set: {}", e)))?;
            let exp_id = ExperienceId::from_bytes(*uuid.as_bytes());
            // Map to current internal ID (skip if not found — experience
            // may have been hard-deleted from redb since last save)
            if let Some(&internal_id) = state.id_to_internal.get(&exp_id) {
                state.deleted.insert(internal_id);
            }
        }
        Ok(())
    }

    /// Saves index metadata to a JSON file.
    ///
    /// Creates `{dir}/{name}.hnsw.meta` with ID mappings and deleted set.
    /// Also attempts to save the HNSW graph via `file_dump` for future
    /// optimization (graph loading is not yet implemented due to lifetime
    /// constraints in hnsw_rs).
    pub fn save_to_dir(&self, dir: &Path, name: &str) -> Result<()> {
        fs::create_dir_all(dir)
            .map_err(|e| PulseDBError::vector(format!("Failed to create HNSW directory: {}", e)))?;

        let state = self
            .state
            .read()
            .map_err(|_| PulseDBError::vector("Index state lock poisoned"))?;

        // Build metadata
        let metadata = IndexMetadata {
            dimension: self.dimension,
            next_id: state.next_id,
            id_map: state
                .id_to_internal
                .iter()
                .map(|(exp_id, &internal_id)| (exp_id.to_string(), internal_id))
                .collect(),
            deleted: state
                .deleted
                .iter()
                .filter_map(|&internal_id| {
                    state
                        .internal_to_id
                        .get(internal_id)
                        .copied()
                        .flatten()
                        .map(|exp_id| exp_id.to_string())
                })
                .collect(),
        };

        // Write metadata as JSON
        let meta_path = dir.join(format!("{}.hnsw.meta", name));
        let json = serde_json::to_string_pretty(&metadata).map_err(|e| {
            PulseDBError::vector(format!("Failed to serialize HNSW metadata: {}", e))
        })?;
        fs::write(&meta_path, json)
            .map_err(|e| PulseDBError::vector(format!("Failed to write HNSW metadata: {}", e)))?;

        // Also dump the HNSW graph (for future direct-load optimization)
        if state.id_to_internal.is_empty() {
            return Ok(());
        }
        drop(state);

        if let Err(e) = self.hnsw.file_dump(dir, name) {
            tracing::warn!(error = %e, "Failed to dump HNSW graph (non-fatal, will rebuild on next open)");
        }

        Ok(())
    }

    /// Loads index metadata from a JSON file.
    ///
    /// Returns the metadata needed to rebuild the graph. The caller must
    /// create a new `HnswIndex` and re-insert embeddings using the
    /// stored ID mappings.
    #[allow(dead_code)] // Used in Step 4 (db.rs open/close lifecycle)
    pub(crate) fn load_metadata(dir: &Path, name: &str) -> Result<Option<IndexMetadata>> {
        let meta_path = dir.join(format!("{}.hnsw.meta", name));
        if !meta_path.exists() {
            return Ok(None);
        }

        let json = fs::read_to_string(&meta_path)
            .map_err(|e| PulseDBError::vector(format!("Failed to read HNSW metadata: {}", e)))?;
        let metadata: IndexMetadata = serde_json::from_str(&json)
            .map_err(|e| PulseDBError::vector(format!("Failed to parse HNSW metadata: {}", e)))?;

        Ok(Some(metadata))
    }

    /// Invalidates one absent row's stale mark before its durable re-create.
    /// The caller serializes edits to this sidecar. Other pending marks survive.
    #[cfg(feature = "sync")]
    pub(crate) fn clear_persisted_deleted_mark(
        dir: &Path,
        name: &str,
        id: ExperienceId,
    ) -> Result<()> {
        let Some(mut metadata) = Self::load_metadata(dir, name)? else {
            return Ok(());
        };
        let mut kept = Vec::with_capacity(metadata.deleted.len());
        for mark in &metadata.deleted {
            let uuid = uuid::Uuid::parse_str(mark)
                .map_err(|e| PulseDBError::vector(format!("Invalid UUID in deleted set: {e}")))?;
            if ExperienceId::from_bytes(*uuid.as_bytes()) != id {
                kept.push(mark.clone());
            }
        }
        if kept.len() == metadata.deleted.len() {
            return Ok(());
        }
        metadata.deleted = kept;
        let json = serde_json::to_vec_pretty(&metadata)
            .map_err(|e| PulseDBError::vector(format!("Failed to serialize HNSW metadata: {e}")))?;
        let target = dir.join(format!("{name}.hnsw.meta"));
        let temp = dir.join(format!(".{name}.{}.meta.tmp", uuid::Uuid::now_v7()));
        let result = (|| -> std::io::Result<()> {
            use std::io::Write;
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp)?;
            file.write_all(&json)?;
            file.sync_all()?;
            drop(file);
            fs::rename(&temp, &target)?;
            #[cfg(unix)]
            fs::File::open(dir)?.sync_all()?;
            Ok(())
        })();
        if let Err(error) = result {
            let _ = fs::remove_file(&temp);
            return Err(PulseDBError::vector(format!(
                "Failed to clear persisted deleted mark: {error}"
            )));
        }
        Ok(())
    }

    /// Rebuilds an index from a set of embeddings.
    ///
    /// Used during `PulseDB::open()` to reconstruct the HNSW graph
    /// from embeddings stored in redb (the source of truth).
    ///
    /// This bulk path publishes its mappings before the batch insert, unlike
    /// [`insert_experience`](Self::insert_experience): the index is being
    /// constructed and is not reachable by any reader until this returns, so
    /// there is no window for anyone to observe a mapping without its point.
    pub fn rebuild_from_embeddings(
        dimension: usize,
        config: &HnswConfig,
        embeddings: Vec<(ExperienceId, Vec<f32>)>,
    ) -> Result<Self> {
        let index = Self::new(dimension, config);

        if embeddings.is_empty() {
            return Ok(index);
        }

        // Prepare batch data for parallel insertion
        let mut state = index
            .state
            .write()
            .map_err(|_| PulseDBError::vector("Index state lock poisoned"))?;

        let mut batch: Vec<(&Vec<f32>, usize)> = Vec::with_capacity(embeddings.len());

        for (exp_id, embedding) in &embeddings {
            let internal_id = state.next_id;
            state.next_id += 1;
            state.id_to_internal.insert(*exp_id, internal_id);
            state.reserve_slot(internal_id);
            state.internal_to_id[internal_id] = Some(*exp_id);
            batch.push((embedding, internal_id));
        }

        drop(state);

        // Parallel bulk insert (uses rayon internally)
        index.hnsw.parallel_insert(&batch);

        Ok(index)
    }

    /// Removes HNSW files for a collective from disk.
    pub fn remove_files(dir: &Path, name: &str) -> Result<()> {
        // Remove metadata file
        let meta_path = dir.join(format!("{}.hnsw.meta", name));
        if meta_path.exists() {
            fs::remove_file(&meta_path).map_err(|e| {
                PulseDBError::vector(format!("Failed to remove HNSW metadata: {}", e))
            })?;
        }

        // Remove graph dump files (hnsw_rs creates files with the name as prefix)
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let file_name = entry.file_name();
                let file_str = file_name.to_string_lossy();
                if file_str.starts_with(name) && file_str.contains("hnswdump") {
                    let _ = fs::remove_file(entry.path());
                }
            }
        }

        Ok(())
    }
}

// ==========================================================================
// VectorIndex trait implementation
// ==========================================================================

impl VectorIndex for HnswIndex {
    fn insert(&self, id: usize, embedding: &[f32]) -> Result<()> {
        if embedding.len() != self.dimension {
            return Err(PulseDBError::vector(format!(
                "Embedding dimension mismatch: expected {}, got {}",
                self.dimension,
                embedding.len()
            )));
        }
        self.hnsw.insert((embedding, id));
        Ok(())
    }

    fn insert_batch(&self, items: &[(&Vec<f32>, usize)]) -> Result<()> {
        self.hnsw.parallel_insert(items);
        Ok(())
    }

    fn search(&self, query: &[f32], k: usize, ef_search: usize) -> Result<Vec<(usize, f32)>> {
        let results = self.hnsw.search(query, k, ef_search);
        Ok(results.into_iter().map(|n| (n.d_id, n.distance)).collect())
    }

    fn search_filtered(
        &self,
        query: &[f32],
        k: usize,
        ef_search: usize,
        filter: &(dyn Fn(&usize) -> bool + Sync),
    ) -> Result<Vec<(usize, f32)>> {
        // Wrap the dyn Fn trait object in FilterBridge to satisfy hnsw_rs's
        // FilterT requirement (trait objects can't auto-coerce between traits)
        let bridge = FilterBridge(filter);
        let results = self.hnsw.search_filter(query, k, ef_search, Some(&bridge));
        Ok(results.into_iter().map(|n| (n.d_id, n.distance)).collect())
    }

    fn delete(&self, id: usize) -> Result<()> {
        let mut state = self
            .state
            .write()
            .map_err(|_| PulseDBError::vector("Index state lock poisoned"))?;
        state.deleted.insert(id);
        Ok(())
    }

    fn is_deleted(&self, id: usize) -> bool {
        self.state
            .read()
            .ok()
            .is_some_and(|s| s.deleted.contains(&id))
    }

    fn len(&self) -> usize {
        self.active_count()
    }

    fn save(&self, dir: &Path, name: &str) -> Result<()> {
        self.save_to_dir(dir, name)
    }
}

// ==========================================================================
// Tests
// ==========================================================================

// Test-only: this thread's next graph insert must fail.
//
// The graph insert is where an insert can fail after the index has already
// committed to an id, so it is the only place a test can reach the state the
// publish-after-insert ordering exists for. Thread-local on purpose: an armed
// failure can never leak into a test running on another thread.
#[cfg(test)]
thread_local! {
    static FAIL_NEXT_GRAPH_INSERT: Cell<bool> = const { Cell::new(false) };
}

/// Arms [`maybe_fail_graph_insert`] for this thread — one shot.
#[cfg(test)]
fn arm_graph_insert_failure() {
    FAIL_NEXT_GRAPH_INSERT.with(|armed| armed.set(true));
}

/// Fails the graph insert if this thread armed one, then disarms.
///
/// Checked immediately before `self.hnsw.insert(..)`, which is the point the
/// spec calls out: everything the index publishes about an id must come after
/// the graph insert returns, so failing here must leave no trace.
#[cfg(test)]
fn maybe_fail_graph_insert() {
    FAIL_NEXT_GRAPH_INSERT.with(|armed| {
        if armed.replace(false) {
            panic!("test hook: graph insert failure armed for this thread");
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HnswConfig;

    #[test]
    fn allocation_hint_reserves_about_max_elements() {
        // Independent copy of upstream 0.3.4's expression and per-layer rounding.
        let reservation = |config: &HnswConfig, hint: usize| -> usize {
            let s = 1. / (config.max_nb_connection as f64).ln();
            (0..config.max_layer.min(16))
                .map(|i| {
                    let frac = (-(i as f64) / s).exp() - (-((i + 1) as f64) / s);
                    (frac * hint as f64).round() as usize
                })
                .sum()
        };
        assert!(allocation_hint(&HnswConfig::default()) <= 100);
        for max_nb_connection in [2, 16, 32] {
            for max_layer in [1, 8, 16, 32] {
                for max_elements in [1, 100, 10_000, 1_000_000] {
                    let config = HnswConfig {
                        max_nb_connection,
                        max_layer,
                        max_elements,
                        ..HnswConfig::default()
                    };
                    let hint = allocation_hint(&config);
                    let total = reservation(&config, hint);
                    let step = total - reservation(&config, hint.saturating_sub(1));
                    assert!(hint >= 1);
                    assert!(total >= max_elements, "{config:?}: {hint} reserves {total}");
                    assert!(
                        total <= max_elements + step,
                        "{config:?}: excess reservation {total}"
                    );
                    if hint > 1 {
                        assert!(reservation(&config, hint - 1) < max_elements);
                    }
                }
            }
        }
        let no_layers = HnswConfig {
            max_layer: 0,
            ..HnswConfig::default()
        };
        assert_eq!(allocation_hint(&no_layers), no_layers.max_elements);
        let invalid_factor = HnswConfig {
            max_nb_connection: 0,
            ..HnswConfig::default()
        };
        assert_eq!(
            allocation_hint(&invalid_factor),
            invalid_factor.max_elements
        );
    }

    fn test_config() -> HnswConfig {
        HnswConfig {
            max_nb_connection: 16,
            ef_construction: 100,
            ef_search: 50,
            max_layer: 8,
            max_elements: 1000,
        }
    }

    /// Generates a deterministic embedding from a seed.
    /// Vectors with close seeds produce similar embeddings.
    fn make_embedding(seed: u64, dim: usize) -> Vec<f32> {
        (0..dim)
            .map(|i| (seed as f32 * 0.1 + i as f32 * 0.01).sin())
            .collect()
    }

    #[test]
    fn test_new_index_is_empty() {
        let index = HnswIndex::new(384, &test_config());
        assert_eq!(index.active_count(), 0);
        assert_eq!(index.total_count(), 0);
        assert!(index.is_empty());
    }

    #[test]
    fn test_insert_and_search() {
        let dim = 8;
        let config = test_config();
        let index = HnswIndex::new(dim, &config);

        // Insert 10 embeddings
        for i in 0..10u64 {
            let exp_id = ExperienceId::new();
            let embedding = make_embedding(i, dim);
            index.insert_experience(exp_id, &embedding).unwrap();
        }

        assert_eq!(index.active_count(), 10);

        // Search for something similar to embedding 5
        let query = make_embedding(5, dim);
        let results = index.search_experiences(&query, 3, 50).unwrap();

        assert!(!results.is_empty());
        assert!(results.len() <= 3);
        // Results should be sorted by distance ascending
        for w in results.windows(2) {
            assert!(w[0].1 <= w[1].1, "Results not sorted by distance");
        }
    }

    #[test]
    fn test_insert_idempotent() {
        let dim = 4;
        let index = HnswIndex::new(dim, &test_config());

        let exp_id = ExperienceId::new();
        let embedding = make_embedding(1, dim);

        index.insert_experience(exp_id, &embedding).unwrap();
        index.insert_experience(exp_id, &embedding).unwrap(); // duplicate

        assert_eq!(index.active_count(), 1);
    }

    #[test]
    fn test_dimension_mismatch_rejected() {
        let index = HnswIndex::new(384, &test_config());

        let exp_id = ExperienceId::new();
        let wrong_dim = vec![1.0f32; 128]; // wrong dimension

        let result = index.insert_experience(exp_id, &wrong_dim);
        assert!(result.is_err());
        assert!(result.unwrap_err().is_vector());
    }

    #[test]
    fn test_delete_excludes_from_search() {
        let dim = 8;
        let index = HnswIndex::new(dim, &test_config());

        // Insert 5 embeddings, remembering IDs
        let mut ids = Vec::new();
        for i in 0..5u64 {
            let exp_id = ExperienceId::new();
            index
                .insert_experience(exp_id, &make_embedding(i, dim))
                .unwrap();
            ids.push(exp_id);
        }

        assert_eq!(index.active_count(), 5);

        // Delete the first one
        index.delete_experience(ids[0]).unwrap();
        assert_eq!(index.active_count(), 4);
        assert!(!index.contains(ids[0]));
        assert!(index.contains(ids[1]));

        // Search should not return the deleted ID
        let query = make_embedding(0, dim); // similar to deleted entry
        let results = index.search_experiences(&query, 10, 50).unwrap();
        let result_ids: Vec<ExperienceId> = results.iter().map(|r| r.0).collect();
        assert!(!result_ids.contains(&ids[0]));
    }

    #[test]
    fn test_search_k_larger_than_index() {
        let dim = 4;
        let index = HnswIndex::new(dim, &test_config());

        let exp_id = ExperienceId::new();
        index
            .insert_experience(exp_id, &make_embedding(1, dim))
            .unwrap();

        // Ask for more results than exist
        let results = index
            .search_experiences(&make_embedding(1, dim), 100, 50)
            .unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_search_empty_index() {
        let dim = 4;
        let index = HnswIndex::new(dim, &test_config());

        let results = index
            .search_experiences(&make_embedding(1, dim), 10, 50)
            .unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_rebuild_from_embeddings() {
        let dim = 8;
        let config = test_config();

        // Prepare embeddings
        let embeddings: Vec<(ExperienceId, Vec<f32>)> = (0..20u64)
            .map(|i| (ExperienceId::new(), make_embedding(i, dim)))
            .collect();

        let index = HnswIndex::rebuild_from_embeddings(dim, &config, embeddings.clone()).unwrap();

        assert_eq!(index.active_count(), 20);

        // Verify all IDs are searchable
        let query = make_embedding(10, dim);
        let results = index.search_experiences(&query, 5, 50).unwrap();
        assert!(!results.is_empty());
    }

    #[test]
    fn test_rebuild_empty() {
        let dim = 384;
        let config = test_config();
        let index = HnswIndex::rebuild_from_embeddings(dim, &config, vec![]).unwrap();
        assert!(index.is_empty());
    }

    #[test]
    fn test_save_and_load_metadata_roundtrip() {
        let dim = 4;
        let index = HnswIndex::new(dim, &test_config());

        let mut exp_ids = Vec::new();
        for i in 0..5u64 {
            let exp_id = ExperienceId::new();
            index
                .insert_experience(exp_id, &make_embedding(i, dim))
                .unwrap();
            exp_ids.push(exp_id);
        }
        index.delete_experience(exp_ids[2]).unwrap();

        // Save to temp directory
        let dir = tempfile::tempdir().unwrap();
        index.save_to_dir(dir.path(), "test_collective").unwrap();

        // Load metadata
        let metadata = HnswIndex::load_metadata(dir.path(), "test_collective")
            .unwrap()
            .expect("Metadata should exist");

        assert_eq!(metadata.dimension, dim);
        assert_eq!(metadata.next_id, 5);
        assert_eq!(metadata.id_map.len(), 5);
        assert_eq!(metadata.deleted.len(), 1);
        // Deleted set stores ExperienceId UUIDs, not internal IDs
        assert_eq!(metadata.deleted[0], exp_ids[2].to_string());
    }

    #[test]
    fn test_remove_files() {
        let dim = 4;
        let index = HnswIndex::new(dim, &test_config());
        index
            .insert_experience(ExperienceId::new(), &make_embedding(1, dim))
            .unwrap();

        let dir = tempfile::tempdir().unwrap();
        index.save_to_dir(dir.path(), "test_coll").unwrap();

        // Verify files exist
        let meta_path = dir.path().join("test_coll.hnsw.meta");
        assert!(meta_path.exists());

        // Remove files
        HnswIndex::remove_files(dir.path(), "test_coll").unwrap();
        assert!(!meta_path.exists());
    }

    #[test]
    fn test_brute_force_search_returns_all_items() {
        let dim = 8;
        let config = test_config();
        let index = HnswIndex::new(dim, &config);

        // Insert 20 items (well below BRUTE_FORCE_THRESHOLD of 128)
        let mut ids = Vec::new();
        for i in 0..20u64 {
            let exp_id = ExperienceId::new();
            index
                .insert_experience(exp_id, &make_embedding(i, dim))
                .unwrap();
            ids.push(exp_id);
        }

        // Search for all 20 — brute-force path must return every one
        let query = make_embedding(10, dim);
        let results = index.search_experiences(&query, 20, 50).unwrap();
        assert_eq!(results.len(), 20, "Brute-force must return all 20 items");

        // Results sorted by distance ascending
        for w in results.windows(2) {
            assert!(
                w[0].1 <= w[1].1,
                "Brute-force results not sorted: {} > {}",
                w[0].1,
                w[1].1
            );
        }

        // The exact query match (seed=10) should be first with distance ≈ 0
        assert_eq!(results[0].0, ids[10]);
        assert!(
            results[0].1 < 0.001,
            "Expected near-zero distance for exact match, got {}",
            results[0].1
        );
    }

    #[test]
    fn test_brute_force_excludes_deleted() {
        let dim = 8;
        let index = HnswIndex::new(dim, &test_config());

        let mut ids = Vec::new();
        for i in 0..5u64 {
            let exp_id = ExperienceId::new();
            index
                .insert_experience(exp_id, &make_embedding(i, dim))
                .unwrap();
            ids.push(exp_id);
        }

        // Delete one
        index.delete_experience(ids[2]).unwrap();

        let query = make_embedding(2, dim);
        let results = index.search_experiences(&query, 10, 50).unwrap();
        assert_eq!(results.len(), 4, "Should return 4 after deleting 1 of 5");
        let result_ids: Vec<ExperienceId> = results.iter().map(|r| r.0).collect();
        assert!(
            !result_ids.contains(&ids[2]),
            "Deleted item must be excluded"
        );
    }

    #[test]
    fn test_cosine_distance_identical_vectors() {
        let dim = 8;
        let index = HnswIndex::new(dim, &test_config());

        let embedding = make_embedding(42, dim);
        let exp_id = ExperienceId::new();
        index.insert_experience(exp_id, &embedding).unwrap();

        // Search with the same vector
        let results = index.search_experiences(&embedding, 1, 50).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, exp_id);
        // Distance should be ~0 for identical vectors
        assert!(
            results[0].1 < 0.001,
            "Expected near-zero distance for identical vectors, got {}",
            results[0].1
        );
    }

    /// A graph insert that fails must leave the id absent, and the same id must
    /// still be insertable afterwards.
    ///
    /// Both halves matter to the sync repair (#96): the repair decides whether
    /// to insert by asking whether the id is already there, so an id published
    /// by an insert that never reached the graph makes the repair skip a row
    /// search can never find. And a failed attempt that keeps its claim would
    /// make every later attempt a silent no-op, so the row could never be
    /// repaired at all.
    #[test]
    fn graph_insert_failure_leaves_id_absent_and_retryable() {
        let dim = 8;
        let index = HnswIndex::new(dim, &test_config());
        let exp_id = ExperienceId::new();
        let embedding = make_embedding(3, dim);

        arm_graph_insert_failure();
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            index.insert_experience(exp_id, &embedding)
        }));
        assert!(
            caught.is_err(),
            "the armed hook must fail the graph insert, or this test proves nothing"
        );

        assert!(
            !index.contains(exp_id),
            "an id whose graph insert never returned must not be reported as present"
        );
        assert_eq!(
            index.total_count(),
            0,
            "the failed insert must not have put a point in the graph"
        );

        // The retry: the failed attempt's claim on the id must not survive it.
        index
            .insert_experience(exp_id, &embedding)
            .expect("the retry must not be refused by the failed attempt");
        assert!(index.contains(exp_id), "the retry must publish the id");
        assert_eq!(index.total_count(), 1, "exactly one point for the id");
        let hits = index.search_experiences(&embedding, 5, 50).unwrap();
        assert!(
            hits.iter().any(|(hit, _)| *hit == exp_id),
            "the retry must make the id findable, not merely present"
        );
    }

    #[test]
    fn pending_delete_does_not_reduce_published_search_budget() {
        let index = HnswIndex::new(4, &test_config());
        let live = ExperienceId::new();
        index
            .insert_experience(live, &[1.0, 0.0, 0.0, 0.0])
            .unwrap();
        let pending = ExperienceId::new();
        index.claim_for_test(pending);
        index.delete_experience(pending).unwrap();
        assert_eq!(
            index.active_count(),
            1,
            "a deleted pending claim is not a deleted published point"
        );
        let hits = index
            .search_experiences(&[1.0, 0.0, 0.0, 0.0], 1, 50)
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].0, live);
        let allowed = HashSet::from([live, pending]);
        assert_eq!(
            index
                .search_experiences_with_allowed(&[1.0, 0.0, 0.0, 0.0], 1, 50, Some(&allowed))
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn unmapped_graph_points_cannot_consume_mapped_top_k() {
        // All 145 points fit in each one-layer neighborhood, avoiding random
        // layer/neighbor pruning. The unpublished point is separated from all
        // mapped points by cosine distance 1, so raw k=1 has a unique answer.
        let config = HnswConfig {
            max_nb_connection: 200,
            max_layer: 1,
            ef_construction: 200,
            ..test_config()
        };
        let index = HnswIndex::new(4, &config);
        let mut ids = Vec::new();
        for i in 0..(BRUTE_FORCE_THRESHOLD + 16) {
            let id = ExperienceId::new();
            index
                .insert_experience(id, &[0.0, 1.0, i as f32 / 144.0, 0.0])
                .unwrap();
            ids.push(id);
        }
        let pending = ExperienceId::new();
        let internal = index.claim_for_test(pending);
        let query = [1.0, 0.0, 0.0, 0.0];
        // Exactly the graph-insert-before-mapping-publication window.
        index.hnsw.insert((&query, internal));
        let raw = VectorIndex::search(&index, &query, 1, 200).unwrap();
        assert_eq!(
            raw[0].0, internal,
            "raw-ID searches must still see raw graph points"
        );
        let mapped = index.search_experiences(&query, 1, 200).unwrap();
        assert_eq!(
            mapped.len(),
            1,
            "an unmapped closest point must not consume mapped k=1"
        );
        assert!(ids.contains(&mapped[0].0));
        assert_eq!(index.search_experiences(&query, 4, 200).unwrap().len(), 4);
        let full = index.search_experiences(&query, ids.len(), 200).unwrap();
        assert_eq!(full.len(), ids.len());
        assert_eq!(
            full.iter().map(|(id, _)| *id).collect::<HashSet<_>>(),
            ids.iter().copied().collect()
        );
        index.delete_experience(ids[0]).unwrap();
        assert_eq!(
            index
                .search_experiences(&query, ids.len(), 200)
                .unwrap()
                .len(),
            ids.len() - 1
        );
        let allowed = HashSet::from([ids[0], ids[1], ids[2], pending]);
        let hits = index
            .search_experiences_with_allowed(&query, 4, 200, Some(&allowed))
            .unwrap();
        assert_eq!(
            hits.len(),
            2,
            "budget counts only allowed, published, undeleted IDs"
        );
        assert!(hits.iter().all(|(id, _)| *id == ids[1] || *id == ids[2]));
        assert_eq!(index.active_count(), ids.len() - 1);
    }

    #[test]
    fn fragmented_graph_does_not_underfill_published_search_budget() {
        // One layer fixes the entry point; four bottom-layer connections keep
        // the five identical early points isolated from the later far cluster.
        let config = HnswConfig {
            max_nb_connection: 2,
            max_layer: 1,
            ..test_config()
        };
        let index = HnswIndex::new(4, &config);
        let query = [1.0, 0.0, 0.0, 0.0];
        let mut ids = Vec::new();
        for i in 0..(BRUTE_FORCE_THRESHOLD + 16) {
            let id = ExperienceId::new();
            let vector = if i < 5 {
                query
            } else {
                [0.0, 1.0, i as f32 * 0.001, 0.0]
            };
            index.insert_experience(id, &vector).unwrap();
            ids.push(id);
        }
        let raw = VectorIndex::search(&index, &query, 8, 200).unwrap();
        assert_eq!(raw.len(), 5, "fixture must exercise a real ANN shortfall");
        assert_eq!(index.search_experiences(&query, 8, 200).unwrap().len(), 8);
        index.delete_experience(ids[0]).unwrap();
        let allowed = HashSet::from([ids[0], ids[1], ids[2], ids[130], ids[131]]);
        let hits = index
            .search_experiences_with_allowed(&query, 8, 200, Some(&allowed))
            .unwrap();
        assert_eq!(hits.len(), 4);
        assert!(hits
            .iter()
            .all(|(id, _)| allowed.contains(id) && *id != ids[0]));
    }

    /// A delete that lands while an insert for the same id is in flight must
    /// survive that insert.
    ///
    /// The delete finds no mapping (the insert has not published one yet), so
    /// without the claim being consulted it marks nothing — and the insert that
    /// finishes afterwards publishes the id as searchable, for a record the
    /// store no longer has. The delete has to reach the pending internal id so
    /// the publish lands already soft-deleted.
    #[test]
    fn a_delete_during_an_in_flight_insert_lands_soft_deleted() {
        let index = HnswIndex::new(8, &test_config());
        let exp_id = ExperienceId::new();

        // The insert is in flight: claimed, not yet published.
        let internal_id = index.claim_for_test(exp_id);
        assert!(
            !index.is_searchable(exp_id),
            "a claim alone must not make an id searchable"
        );

        index.delete_experience(exp_id).unwrap();

        assert!(
            index.is_deleted(internal_id),
            "the delete must mark the pending internal id, so the insert that \
             finishes afterwards publishes an id that is already soft-deleted"
        );
    }
}
