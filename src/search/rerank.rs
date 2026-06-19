//! Weight resolution helpers for recall ranking.

use std::cmp::Ordering;

use crate::config::RecallWeights;
use crate::error::Result;
use crate::search::SearchResult;

/// Resolves effective recall weights from request and collective defaults.
///
/// Explicit request weights take precedence over stored collective defaults.
/// Request weights are validated so bad caller input fails loudly instead of
/// silently falling back to legacy ranking.
pub(crate) fn resolve_recall_weights(
    request: Option<RecallWeights>,
    collective_default: Option<RecallWeights>,
) -> Result<Option<RecallWeights>> {
    if let Some(weights) = request {
        weights.validate("weights")?;
        Ok(Some(weights))
    } else {
        Ok(collective_default)
    }
}

/// Returns true when effective weights should use the legacy search path.
pub(crate) fn is_legacy_recall(weights: Option<RecallWeights>) -> bool {
    match weights {
        None => true,
        Some(weights) => weights.energy == 0.0,
    }
}

/// Clamps a value into the `[0, 1]` interval.
pub(crate) fn clamp01(value: f32) -> f32 {
    value.clamp(0.0, 1.0)
}

/// Computes the linear recall blend from raw similarity and pre-computed energy.
pub(crate) fn blend_score(similarity: f32, energy: f32, weights: RecallWeights) -> f32 {
    weights.similarity * clamp01(similarity) + weights.energy * energy
}

/// Sorts scored results by score descending and truncates to `k`.
pub(crate) fn rerank(scored: Vec<(SearchResult, f32)>, k: usize) -> Vec<SearchResult> {
    let mut indexed: Vec<_> = scored.into_iter().enumerate().collect();
    indexed.sort_by(
        |(left_index, (_, left_score)), (right_index, (_, right_score))| {
            right_score
                .partial_cmp(left_score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| left_index.cmp(right_index))
        },
    );
    indexed.truncate(k);
    indexed.into_iter().map(|(_, (result, _))| result).collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use proptest::prelude::*;

    use super::*;
    use crate::config::{Config, DecayConfig};
    use crate::experience::{Experience, ExperienceType};
    use crate::search::{SearchFilter, SearchOptions};
    use crate::types::{AgentId, CollectiveId, ExperienceId, InstanceId, Timestamp};
    use crate::PulseDB;

    fn make_result(content: &str, similarity: f32) -> SearchResult {
        let timestamp = Timestamp::now();
        SearchResult {
            experience: Experience {
                id: ExperienceId::new(),
                collective_id: CollectiveId::new(),
                content: content.to_string(),
                embedding: vec![0.1; 384],
                experience_type: ExperienceType::default(),
                importance: 0.5,
                confidence: 0.8,
                applications: std::collections::BTreeMap::new(),
                domain: vec!["test".to_string()],
                related_files: vec![],
                source_agent: AgentId::new("agent-1"),
                source_task: None,
                timestamp,
                last_reinforced: timestamp,
                archived: false,
            },
            similarity,
        }
    }

    fn embedding_with_query_cosine(cosine: f32) -> Vec<f32> {
        let mut embedding = vec![0.0; 384];
        embedding[0] = cosine;
        embedding[1] = (1.0 - cosine.powi(2)).sqrt();
        embedding
    }

    fn recall_default(weights: RecallWeights) -> DecayConfig {
        DecayConfig {
            default_recall_weights: Some(weights),
            ..Config::default().decay
        }
    }

    fn min_importance_filter() -> SearchFilter {
        SearchFilter {
            min_importance: Some(0.2),
            ..SearchFilter::default()
        }
    }

    fn compare_legacy_results(left: &[SearchResult], right: &[SearchResult]) {
        assert_eq!(left.len(), right.len());
        for (left, right) in left.iter().zip(right) {
            assert_eq!(left.experience.id, right.experience.id);
            assert_eq!(left.similarity, right.similarity);
        }
    }

    fn open_search_fixture(name: &str) -> (tempfile::TempDir, PulseDB, CollectiveId) {
        let dir = tempfile::tempdir().unwrap();
        let db = PulseDB::open(dir.path().join(format!("{name}.db")), Config::default()).unwrap();
        let collective_id = db.create_collective(name).unwrap();
        (dir, db, collective_id)
    }

    fn insert_similarity_fixture(db: &PulseDB, collective_id: CollectiveId, similarities: &[f32]) {
        let now = Timestamp::now();
        for (index, similarity) in similarities.iter().copied().enumerate() {
            db.insert_experience_backdated(
                collective_id,
                &format!("fixture-{index}"),
                embedding_with_query_cosine(similarity),
                0.5,
                BTreeMap::new(),
                now,
            )
            .unwrap();
        }
    }

    fn search_with(
        db: &PulseDB,
        collective_id: CollectiveId,
        weights: Option<RecallWeights>,
        filter: SearchFilter,
        k: usize,
    ) -> Vec<SearchResult> {
        db.search(
            collective_id,
            &embedding_with_query_cosine(1.0),
            SearchOptions { k, filter, weights },
        )
        .unwrap()
    }

    fn legacy_search(
        db: &PulseDB,
        collective_id: CollectiveId,
        filter: SearchFilter,
        k: usize,
    ) -> Vec<SearchResult> {
        db.search_similar_filtered(collective_id, &embedding_with_query_cosine(1.0), k, filter)
            .unwrap()
    }

    fn insert_pinned_stale_fresh_pair(
        db: &PulseDB,
        collective_id: CollectiveId,
    ) -> (ExperienceId, ExperienceId) {
        let now = Timestamp::now();
        let stale_last_reinforced =
            Timestamp::from_millis(now.as_millis() - 365 * 24 * 60 * 60 * 1000);
        let applications = BTreeMap::from([(InstanceId::new(), 1)]);

        let stale_id = db
            .insert_experience_backdated(
                collective_id,
                "A stale-but-similar",
                embedding_with_query_cosine(0.90),
                0.9,
                applications.clone(),
                stale_last_reinforced,
            )
            .unwrap();
        let fresh_id = db
            .insert_experience_backdated(
                collective_id,
                "B fresh-reinforced",
                embedding_with_query_cosine(0.70),
                0.7,
                applications,
                now,
            )
            .unwrap();

        (stale_id, fresh_id)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(12))]

        #[test]
        fn beta_zero_request_matches_legacy_despite_weighted_collective_default(
            similarities in prop::collection::vec(0.25f32..0.98, 3..10),
        ) {
            let (_dir, db, collective_id) = open_search_fixture("beta-zero-request");
            insert_similarity_fixture(&db, collective_id, &similarities);
            db.set_decay_config_for_test(
                collective_id,
                recall_default(RecallWeights::new(0.6, 0.4)),
            )
            .unwrap();

            for filter in [SearchFilter::default(), min_importance_filter()] {
                let legacy = legacy_search(&db, collective_id, filter.clone(), similarities.len());
                let weighted = search_with(
                    &db,
                    collective_id,
                    Some(RecallWeights::new(1.0, 0.0)),
                    filter,
                    similarities.len(),
                );
                compare_legacy_results(&weighted, &legacy);
            }
        }

        #[test]
        fn absent_weights_without_collective_default_match_legacy(
            similarities in prop::collection::vec(0.25f32..0.98, 3..10),
        ) {
            let (_dir, db, collective_id) = open_search_fixture("absent-weights");
            insert_similarity_fixture(&db, collective_id, &similarities);

            for filter in [SearchFilter::default(), min_importance_filter()] {
                let legacy = legacy_search(&db, collective_id, filter.clone(), similarities.len());
                let weighted = search_with(&db, collective_id, None, filter, similarities.len());
                compare_legacy_results(&weighted, &legacy);
            }
        }
    }

    #[test]
    fn absent_request_uses_collective_default_and_diverges_from_legacy() {
        let (_dir, db, collective_id) = open_search_fixture("resolved-default-diverges");
        let (stale_id, fresh_id) = insert_pinned_stale_fresh_pair(&db, collective_id);
        db.set_decay_config_for_test(collective_id, recall_default(RecallWeights::new(0.6, 0.4)))
            .unwrap();

        let legacy = legacy_search(&db, collective_id, SearchFilter::default(), 2);
        let weighted = search_with(&db, collective_id, None, SearchFilter::default(), 2);

        assert_eq!(legacy[0].experience.id, stale_id);
        assert_eq!(weighted[0].experience.id, fresh_id);
        assert_ne!(
            legacy
                .iter()
                .map(|result| result.experience.id)
                .collect::<Vec<_>>(),
            weighted
                .iter()
                .map(|result| result.experience.id)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn pinned_stale_fresh_fixture_flips_only_when_energy_weighted() {
        let (_dir, db, collective_id) = open_search_fixture("pinned-stale-fresh");
        let (stale_id, fresh_id) = insert_pinned_stale_fresh_pair(&db, collective_id);

        let legacy_none = search_with(&db, collective_id, None, SearchFilter::default(), 2);
        let legacy_explicit = search_with(
            &db,
            collective_id,
            Some(RecallWeights::new(1.0, 0.0)),
            SearchFilter::default(),
            2,
        );

        db.set_decay_config_for_test(collective_id, recall_default(RecallWeights::new(0.7, 0.3)))
            .unwrap();
        let default_weighted = search_with(&db, collective_id, None, SearchFilter::default(), 2);
        let headline_weighted = search_with(
            &db,
            collective_id,
            Some(RecallWeights::new(0.5, 0.5)),
            SearchFilter::default(),
            2,
        );

        assert_eq!(legacy_none[0].experience.id, stale_id);
        assert_eq!(legacy_none[1].experience.id, fresh_id);
        assert_eq!(legacy_explicit[0].experience.id, stale_id);
        assert_eq!(legacy_explicit[1].experience.id, fresh_id);
        assert_eq!(default_weighted[0].experience.id, fresh_id);
        assert_eq!(default_weighted[1].experience.id, stale_id);
        assert_eq!(headline_weighted[0].experience.id, fresh_id);
        assert_eq!(headline_weighted[1].experience.id, stale_id);
        assert!((headline_weighted[0].similarity - 0.70).abs() < 0.001);
        assert!((headline_weighted[1].similarity - 0.90).abs() < 0.001);
    }

    #[test]
    fn global_config_default_recall_weights_apply_without_stored_override() {
        // PR #23 review (relates to #16): a globally-configured
        // `Config.decay.default_recall_weights` must drive ranking for collectives
        // that have NO per-collective stored DecayConfig override. Otherwise the
        // documented global default silently no-ops to the legacy similarity path.
        let dir = tempfile::tempdir().unwrap();
        let config = Config {
            decay: DecayConfig {
                default_recall_weights: Some(RecallWeights::new(0.7, 0.3)),
                ..Config::default().decay
            },
            ..Config::default()
        };
        let db = PulseDB::open(dir.path().join("global-default.db"), config).unwrap();
        let collective_id = db.create_collective("global-default").unwrap();
        let (stale_id, fresh_id) = insert_pinned_stale_fresh_pair(&db, collective_id);

        // No set_decay_config_for_test: the global config is the only weight source.
        let weighted = search_with(&db, collective_id, None, SearchFilter::default(), 2);

        assert_eq!(
            weighted[0].experience.id, fresh_id,
            "global default recall weights must rank the fresh/high-energy experience first"
        );
        assert_eq!(weighted[1].experience.id, stale_id);
    }

    #[test]
    fn energy_scenario_captures_decay_and_reinforcement_boost() {
        let (_dir, db, collective_id) = open_search_fixture("energy-scenario");
        let now = Timestamp::now();
        let stale_last_reinforced =
            Timestamp::from_millis(now.as_millis() - 365 * 24 * 60 * 60 * 1000);

        let stale_id = db
            .insert_experience_backdated(
                collective_id,
                "fully decayed memory",
                embedding_with_query_cosine(0.90),
                0.9,
                BTreeMap::from([(InstanceId::new(), 1)]),
                stale_last_reinforced,
            )
            .unwrap();
        let fresh_id = db
            .insert_experience_backdated(
                collective_id,
                "fresh reinforced memory",
                embedding_with_query_cosine(0.70),
                0.7,
                BTreeMap::from([(InstanceId::new(), 1)]),
                now,
            )
            .unwrap();

        let stale_energy = db.energy(stale_id).unwrap();
        let fresh_reinforced_energy = db.energy(fresh_id).unwrap();

        assert!(stale_energy < 0.001);
        assert!(fresh_reinforced_energy > 0.82);
        assert!(fresh_reinforced_energy > 0.7);
    }

    #[test]
    fn archived_experiences_stay_excluded_under_energy_weighting() {
        let (_dir, db, collective_id) = open_search_fixture("archived-weighted");
        let now = Timestamp::now();
        let archived_id = db
            .insert_experience_backdated(
                collective_id,
                "archived high-signal memory",
                embedding_with_query_cosine(0.99),
                1.0,
                BTreeMap::from([(InstanceId::new(), 100)]),
                now,
            )
            .unwrap();
        let active_id = db
            .insert_experience_backdated(
                collective_id,
                "active lower-signal memory",
                embedding_with_query_cosine(0.75),
                0.5,
                BTreeMap::new(),
                now,
            )
            .unwrap();
        db.archive_experience(archived_id).unwrap();

        let weighted = search_with(
            &db,
            collective_id,
            Some(RecallWeights::new(0.5, 0.5)),
            SearchFilter::default(),
            2,
        );

        assert_eq!(weighted.len(), 1);
        assert_eq!(weighted[0].experience.id, active_id);
        assert!(!weighted
            .iter()
            .any(|result| result.experience.id == archived_id));
    }

    #[test]
    fn resolve_absent_request_uses_collective_default() {
        let collective_default = RecallWeights::new(0.7, 0.3);

        let resolved = resolve_recall_weights(None, Some(collective_default)).unwrap();

        assert_eq!(resolved, Some(collective_default));
    }

    #[test]
    fn resolve_request_overrides_collective_default() {
        let request = RecallWeights::new(1.0, 0.0);
        let collective_default = RecallWeights::new(0.7, 0.3);

        let resolved = resolve_recall_weights(Some(request), Some(collective_default)).unwrap();

        assert_eq!(resolved, Some(request));
        assert!(is_legacy_recall(resolved));
    }

    #[test]
    fn beta_zero_predicate_covers_none_and_explicit_legacy() {
        assert!(is_legacy_recall(None));
        assert!(is_legacy_recall(Some(RecallWeights::new(1.0, 0.0))));
        assert!(!is_legacy_recall(Some(RecallWeights::new(0.7, 0.3))));
    }

    #[test]
    fn invalid_request_weights_err() {
        let err = resolve_recall_weights(Some(RecallWeights::new(0.5, 0.9)), None);

        assert!(err.is_err());
    }

    #[test]
    fn blend_identity_similarity() {
        let weights = RecallWeights::new(1.0, 0.0);

        assert_eq!(blend_score(0.42, 0.9, weights), 0.42);
    }

    #[test]
    fn blend_identity_energy() {
        let weights = RecallWeights::new(0.0, 1.0);

        assert_eq!(blend_score(0.42, 0.9, weights), 0.9);
    }

    #[test]
    fn blend_clamps_negative_similarity() {
        let weights = RecallWeights::new(1.0, 0.0);

        assert_eq!(blend_score(-0.2, 0.9, weights), 0.0);
    }

    #[test]
    fn rerank_sorts_desc_truncates_and_preserves_ties() {
        let scored = vec![
            (make_result("low", 0.2), 0.2),
            (make_result("tie-a", 0.7), 0.7),
            (make_result("high", 0.9), 0.9),
            (make_result("tie-b", 0.7), 0.7),
        ];

        let reranked = rerank(scored, 3);

        assert_eq!(reranked.len(), 3);
        assert_eq!(reranked[0].experience.content, "high");
        assert_eq!(reranked[1].experience.content, "tie-a");
        assert_eq!(reranked[2].experience.content, "tie-b");
    }

    // -----------------------------------------------------------------------
    // NFR-018 HNSW-path recall guard (VS-3.5.3 work-1.04, C2/C3)
    // -----------------------------------------------------------------------

    /// Embedding dimension for the recall-guard fixture (matches D384).
    const GUARD_DIM: usize = 384;

    /// Deterministic embedding from a seed (independent of the bench helper so
    /// the guard is self-contained). Distinct seeds yield well-separated
    /// vectors, so cosine ranks are unambiguous (no ties to break).
    fn guard_embedding(seed: u64) -> Vec<f32> {
        (0..GUARD_DIM)
            .map(|i| {
                let h = seed
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(i as u64)
                    .wrapping_mul(1_442_695_040_888_963_407);
                (h >> 33) as f32 / (u32::MAX as f32) - 0.5
            })
            .collect()
    }

    /// Cosine similarity of two equal-length vectors (the in-test ground-truth
    /// metric). Mirrors the engine's `similarity = 1.0 - cosine_distance`.
    fn cosine(a: &[f32], b: &[f32]) -> f32 {
        let mut dot = 0.0f32;
        let mut na = 0.0f32;
        let mut nb = 0.0f32;
        for (x, y) in a.iter().zip(b) {
            dot += x * y;
            na += x * x;
            nb += y * y;
        }
        let denom = na.sqrt() * nb.sqrt();
        if denom == 0.0 {
            0.0
        } else {
            dot / denom
        }
    }

    /// HNSW-path recall guard for the energy-weighted `search()` (C2/C3).
    ///
    /// VS-3.5.2's `search::` correctness suite runs only `<128`-experience
    /// brute-force collectives, so below `BRUTE_FORCE_THRESHOLD = 128` the
    /// over-fetch / `ef_search` / `k'` knobs are inert — that suite is
    /// necessary but NOT sufficient to prove the HNSW retrieval path preserves
    /// recall. This guard operates ABOVE the threshold (N = 256), where
    /// `search()` traverses the real HNSW graph.
    ///
    /// Ground truth is computed EXHAUSTIVELY in-test (cosine over every fixture
    /// vector), NOT via the engine's exact brute-force branch — that branch only
    /// fires at `N <= 128` (`src/vector/hnsw.rs`), below this operating point, so
    /// it cannot be the oracle here. All experiences share identical importance,
    /// empty applications, and same-batch `last_reinforced`, so their energy term
    /// is constant; the energy-weighted blended score is therefore monotonic in
    /// cosine similarity, and the cosine top-k is an exact oracle for the engine's
    /// top-k.
    ///
    /// MEASUREMENT IS AVERAGED OVER MANY QUERIES (fix-up iteration 1). A single
    /// query's `recall@10` is 0.1-quantized (only 9/10 or 10/10 are possible);
    /// against *approximate* HNSW at N=256 that single number flips between 0.900
    /// and 1.000 across builds — a flaky measurement, not a production defect. We
    /// therefore average `recall@K` over `Q = 100` distinct fixed-seed queries:
    /// the **mean recall@K** is continuous and stable, and averages out per-build
    /// HNSW non-determinism. The floor is calibrated to the MEASURED stable
    /// baseline (see `RECALL_FLOOR` below): the guard's job is to catch a
    /// *regression* from future tuning, so a baseline-relative floor is the
    /// principled choice — an arbitrary 0.95 the untuned approximate path cannot
    /// reliably clear would only manufacture flake. Test-only: changes no runtime
    /// behavior and lands whether or not the runtime path was tuned.
    #[test]
    fn weighted_search_recall_at_k_above_threshold() {
        use crate::experience::NewExperience;

        // N strictly above BRUTE_FORCE_THRESHOLD (128) so the HNSW path fires.
        const N: u64 = 256;
        const K: usize = 10;
        // Number of distinct fixed-seed queries to average over. Q >= 50 makes
        // the mean recall continuous (not 0.1-quantized) and stable across the
        // per-build HNSW non-determinism that made the single-query guard flaky.
        const Q: u64 = 100;
        // Floor calibrated to the measured stable mean recall@10 at N=256 on the
        // approximate HNSW path. The stable measured baseline is ~0.97 (see
        // report.md §8 for the multi-run characterization), comfortably ≥ 0.96,
        // so the C3 floor stays at 0.95 with margin. This catches a recall
        // *regression* from future over-fetch/ef/k' tuning; it is intentionally
        // baseline-relative, not an arbitrary perfection demand.
        const RECALL_FLOOR: f32 = 0.95;
        // Optional override to prove the assertion is non-vacuous: set
        // PULSEDB_RECALL_GUARD_FLOOR above the baseline and the test MUST fail.
        let recall_floor: f32 = std::env::var("PULSEDB_RECALL_GUARD_FLOOR")
            .ok()
            .and_then(|v| v.parse::<f32>().ok())
            .unwrap_or(RECALL_FLOOR);

        let dir = tempfile::tempdir().unwrap();
        let db = PulseDB::open(dir.path().join("recall-guard.db"), Config::default()).unwrap();
        let collective_id = db.create_collective("recall-guard").unwrap();

        // Build the fixture. Identical importance + empty applications keeps the
        // energy term constant across all records, so the blended-score ranking
        // is monotonic in cosine similarity (the in-test oracle is then exact).
        let mut embeddings: Vec<(ExperienceId, Vec<f32>)> = Vec::with_capacity(N as usize);
        for i in 0..N {
            let embedding = guard_embedding(i);
            let id = db
                .record_experience(NewExperience {
                    collective_id,
                    content: format!("recall-guard experience {i}"),
                    importance: 0.5,
                    embedding: Some(embedding.clone()),
                    ..Default::default()
                })
                .unwrap();
            embeddings.push((id, embedding));
        }

        // Average recall@K over Q distinct fixed-seed queries. Each query seed is
        // disjoint from the fixture seeds (0..N) so no query coincides with a
        // stored vector (a realistic tail). The mean over Q queries is the stable,
        // continuous metric that replaces the flaky single-query measurement.
        let mut recall_sum = 0.0f64;
        for q in 0..Q {
            let query = guard_embedding(N + 1 + q);

            // Ground truth: top-K experience ids by exhaustive cosine over EVERY
            // fixture vector. With constant energy this equals the top-K by the
            // engine's blended score.
            let mut ranked: Vec<(ExperienceId, f32)> = embeddings
                .iter()
                .map(|(id, emb)| (*id, cosine(&query, emb)))
                .collect();
            ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
            let truth_top_k: std::collections::HashSet<ExperienceId> =
                ranked.iter().take(K).map(|(id, _)| *id).collect();

            // Engine result via the energy-weighted HNSW path (beta > 0).
            let results = db
                .search(
                    collective_id,
                    &query,
                    SearchOptions {
                        k: K,
                        filter: SearchFilter::default(),
                        weights: Some(RecallWeights::new(0.5, 0.5)),
                    },
                )
                .unwrap();

            assert_eq!(
                results.len(),
                K,
                "HNSW path returned {} of {K} requested results (N={N}, query {q})",
                results.len()
            );

            let hits = results
                .iter()
                .filter(|r| truth_top_k.contains(&r.experience.id))
                .count();
            recall_sum += hits as f64 / K as f64;
        }

        let mean_recall = (recall_sum / Q as f64) as f32;

        // Visible under `cargo test -- --nocapture` for characterization runs.
        println!(
            "NFR-018 recall guard: mean recall@{K} = {mean_recall:.4} over Q={Q} queries at N={N} \
             (floor {recall_floor})"
        );

        assert!(
            mean_recall >= recall_floor,
            "NFR-018 recall guard: mean recall@{K} = {mean_recall:.4} over Q={Q} queries is below \
             the {recall_floor} floor at N={N} on the approximate HNSW path — a latency win must \
             never be bought with a recall regression below this floor (C3: escalate/defer, not \
             accept)."
        );
    }
}
