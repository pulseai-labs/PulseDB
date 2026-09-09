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
    id_to_internal: HashMap<ExperienceId, usize>,

    /// Reverse map: internal usize ID → ExperienceId.
    /// Uses Vec for O(1) lookup by index.
    internal_to_id: Vec<ExperienceId>,

    /// Set of soft-deleted internal IDs (excluded from search).
    deleted: HashSet<usize>,

    /// Next internal ID to assign (monotonically increasing).
    next_id: usize,
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

impl HnswIndex {
    /// Creates a new empty HNSW index.
    ///
    /// # Arguments
    ///
    /// * `dimension` - Expected embedding dimension (validated on insert)
    /// * `config` - HNSW tuning parameters
    pub fn new(dimension: usize, config: &HnswConfig) -> Self {
        let hnsw = Hnsw::new(
            config.max_nb_connection,
            config.max_elements,
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
    /// Assigns a new internal usize ID and records the mapping.
    /// If the ExperienceId is already present, this is a no-op.
    pub fn insert_experience(&self, exp_id: ExperienceId, embedding: &[f32]) -> Result<()> {
        self.validate_embedding(embedding)?;

        let mut state = self
            .state
            .write()
            .map_err(|_| PulseDBError::vector("Index state lock poisoned"))?;

        // Skip if already inserted (idempotent)
        if state.id_to_internal.contains_key(&exp_id) {
            return Ok(());
        }

        // Assign next sequential internal ID
        let internal_id = state.next_id;
        state.next_id += 1;

        // Record bidirectional mapping
        state.id_to_internal.insert(exp_id, internal_id);
        state.internal_to_id.push(exp_id);

        // Drop the lock before calling hnsw insert (which acquires its own lock)
        drop(state);

        // Insert into HNSW graph (uses interior mutability via parking_lot::RwLock)
        self.hnsw.insert((embedding, internal_id));

        Ok(())
    }

    /// Marks an experience as deleted in the index.
    ///
    /// The vector remains in the graph but is excluded from search
    /// results via filtered search. Returns Ok even if the experience
    /// is not in the index (idempotent).
    pub fn delete_experience(&self, exp_id: ExperienceId) -> Result<()> {
        let mut state = self
            .state
            .write()
            .map_err(|_| PulseDBError::vector("Index state lock poisoned"))?;

        if let Some(&internal_id) = state.id_to_internal.get(&exp_id) {
            state.deleted.insert(internal_id);
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
    /// post-recall truncate.
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

        let active_count = state.next_id - state.deleted.len();
        if active_count == 0 {
            return Ok(vec![]);
        }

        // Convert the allowed ExperienceId set to internal IDs for the HNSW
        // filter predicate. Unknown IDs (deleted or not in index) are silently
        // dropped — the caller's allowed set is an over-approximation.
        let allowed_internal: Option<HashSet<usize>> = allowed.map(|ids| {
            ids.iter()
                .filter_map(|exp_id| state.id_to_internal.get(exp_id).copied())
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

        if active_count <= BRUTE_FORCE_THRESHOLD {
            // Linear scan: iterate all stored vectors and compute exact distances.
            // Guarantees 100% recall for small collections where HNSW's layer
            // fragmentation causes missed results.
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
                if let Some(&exp_id) = state.internal_to_id.get(origin_id) {
                    all_distances.push((exp_id, distance));
                }
            }

            all_distances
                .sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
            all_distances.truncate(effective_k);
            return Ok(all_distances);
        }

        // HNSW graph search for larger collections.
        let effective_ef = ef_search.max(effective_k);
        let deleted_ref = &state.deleted;

        // Combined predicate: not-deleted AND (allowed is None OR in allowed set).
        let needs_filter = !state.deleted.is_empty() || allowed_internal.is_some();
        let results = if needs_filter {
            let allowed_ref = allowed_internal.as_ref();
            let filter_fn = move |id: &usize| -> bool {
                !deleted_ref.contains(id)
                    && allowed_ref.is_none_or(|internal| internal.contains(id))
            };
            self.hnsw
                .search_filter(query, effective_k, effective_ef, Some(&filter_fn))
        } else {
            self.hnsw.search(query, effective_k, effective_ef)
        };

        // Map internal IDs back to ExperienceIds
        let mapped: Vec<(ExperienceId, f32)> = results
            .into_iter()
            .filter_map(|n| {
                state
                    .internal_to_id
                    .get(n.d_id)
                    .map(|&exp_id| (exp_id, n.distance))
            })
            .collect();

        Ok(mapped)
    }

    /// Returns true if the given experience is in the index (and not deleted).
    pub fn contains(&self, exp_id: ExperienceId) -> bool {
        let state = self.state.read().ok();
        state.is_some_and(|s| {
            s.id_to_internal
                .get(&exp_id)
                .is_some_and(|id| !s.deleted.contains(id))
        })
    }

    /// Returns the number of active (non-deleted) vectors.
    pub fn active_count(&self) -> usize {
        let state = self.state.read().ok();
        state.map_or(0, |s| s.id_to_internal.len() - s.deleted.len())
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

    /// Rebuilds an index from a set of embeddings.
    ///
    /// Used during `PulseDB::open()` to reconstruct the HNSW graph
    /// from embeddings stored in redb (the source of truth).
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
            state.internal_to_id.push(*exp_id);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HnswConfig;

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
}
