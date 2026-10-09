//! r1.s7.w1 — reproducible reads: caller-pinned clock tests.
//!
//! Covers `ReadOptions`/`ReadMode` and the `*_with` read methods:
//! AC-2 pinned-`now` search ranking, AC-3 past-`now` clamping,
//! AC-4 cold-list time pinning, AC-5 `energy_with` vs the pure `energy`,
//! AC-6 active-agent classification under an injected `now`,
//! AC-7 one resolved `now` in context candidates, AC-8 the `Exact`
//! refusal, AC-9 the non-finite-query refusal.

use std::time::Duration;

use pulsedb::{
    energy, CollectiveId, Config, ContextRequest, DecayConfig, ExperienceId, NewActivity,
    NewExperience, PulseDB, PulseDBError, ReadOptions, RecallWeights, SearchFilter, SearchOptions,
    SearchResult, Timestamp,
};
use tempfile::tempdir;

/// Default embedding dimension for tests (D384).
const DIM: usize = 384;

/// Generates a deterministic embedding from a seed.
///
/// Uses a hash-based pseudo-random generator to produce well-separated vectors
/// in the 384-dimensional space.
fn make_embedding(seed: u64) -> Vec<f32> {
    (0..DIM)
        .map(|i| {
            let h = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(i as u64)
                .wrapping_mul(1442695040888963407);
            (h >> 33) as f32 / (u32::MAX as f32) - 0.5
        })
        .collect()
}

/// Helper to open a fresh database with default config.
fn open_db() -> (PulseDB, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let db = PulseDB::open(dir.path().join("test.db"), Config::default()).unwrap();
    (db, dir)
}

/// Helper to open a fresh database with a custom activity staleness threshold.
fn open_db_with_stale_threshold(threshold: Duration) -> (PulseDB, tempfile::TempDir) {
    let mut config = Config::default();
    config.activity.stale_threshold = threshold;
    let dir = tempdir().unwrap();
    let db = PulseDB::open(dir.path().join("test.db"), config).unwrap();
    (db, dir)
}

/// Records one experience and returns its id and stored `last_reinforced`.
fn record(
    db: &PulseDB,
    collective_id: CollectiveId,
    content: &str,
    embedding: Vec<f32>,
    importance: f32,
) -> (ExperienceId, Timestamp) {
    let id = db
        .record_experience(NewExperience {
            collective_id,
            content: content.to_string(),
            embedding: Some(embedding),
            importance,
            ..Default::default()
        })
        .unwrap();
    let stored = db.get_experience(id).unwrap().unwrap();
    (id, stored.last_reinforced)
}

/// The recall blend for one result at a given `now`, from the pure `energy()`.
fn blended_score_at(
    similarity: f32,
    experience: &pulsedb::Experience,
    now: Timestamp,
    cfg: &DecayConfig,
    weights: RecallWeights,
) -> f32 {
    let e = energy(
        experience.importance,
        experience.applications(),
        experience.last_reinforced,
        now,
        cfg,
    );
    weights.similarity * similarity.clamp(0.0, 1.0) + weights.energy * e
}

fn result_ids(results: &[SearchResult]) -> Vec<ExperienceId> {
    results.iter().map(|r| r.experience.id).collect()
}

/// AC-2: `search_with` with a positive energy weight and a pinned `now`
/// returns the same ranking across a sleep, and the ranking the pure
/// `energy()` implies at that `now` — the wall clock would order it the
/// other way.
#[test]
fn search_with_pinned_now_is_clock_independent() {
    let (db, _dir) = open_db();
    let cid = db.create_collective("pinned-search").unwrap();

    let query = make_embedding(1);
    let mut near = query.clone();
    near[0] += 0.2;

    // a: identical embedding (similarity 1.0), low importance.
    // b: near-identical embedding, high importance — the wall clock would
    //    rank b first, the pinned (far-future) now ranks a first.
    let (a_id, last_a) = record(&db, cid, "a", query.clone(), 0.1);
    let (b_id, last_b) = record(&db, cid, "b", near, 1.0);

    let pinned = Timestamp::from_millis(
        last_a.as_millis().max(last_b.as_millis()) + 365 * 24 * 60 * 60 * 1000,
    );
    let weights = RecallWeights::new(0.5, 0.5);
    let options = SearchOptions {
        k: 2,
        filter: SearchFilter::default(),
        weights: Some(weights),
    };
    let read = ReadOptions::new().at(pinned);

    let first = db.search_with(cid, &query, options.clone(), &read).unwrap();
    std::thread::sleep(Duration::from_millis(20));
    let second = db.search_with(cid, &query, options, &read).unwrap();

    assert_eq!(first.len(), 2, "both experiences must be returned");
    assert_eq!(second.len(), 2, "both experiences must be returned");
    assert_eq!(
        result_ids(&first),
        result_ids(&second),
        "ranking must not change between calls separated by a sleep"
    );
    for (left, right) in first.iter().zip(second.iter()) {
        assert_eq!(
            left.similarity.to_bits(),
            right.similarity.to_bits(),
            "similarity scores must be identical at a pinned now"
        );
    }

    assert_eq!(
        result_ids(&first),
        vec![a_id, b_id],
        "pinned-now ranking must follow the pure energy() at that now (a first); \
         a wall-clock read would rank b first"
    );

    let cfg = Config::default().decay;
    let mut previous = f32::INFINITY;
    for r in &first {
        let score = blended_score_at(r.similarity, &r.experience, pinned, &cfg, weights);
        assert!(
            score <= previous + 1e-6,
            "scores must be non-increasing under the pinned now: {score} > {previous}"
        );
        previous = score;
    }
}

/// AC-3: a `now` before every record's `last_reinforced` is accepted and
/// yields the clamped-to-zero-elapsed energies (the pure `energy()` at each
/// record's own `last_reinforced`).
#[test]
fn past_now_is_accepted_and_clamps_elapsed() {
    let (db, _dir) = open_db();
    let cid = db.create_collective("past-now").unwrap();

    let query = make_embedding(7);
    let (a_id, _) = record(&db, cid, "a", make_embedding(11), 0.1);
    let (b_id, _) = record(&db, cid, "b", make_embedding(12), 1.0);

    // 1970 — before every record's last_reinforced.
    let past = Timestamp::from_millis(1);
    let weights = RecallWeights::new(0.0, 1.0); // energy-only: rank == energy order
    let options = SearchOptions {
        k: 2,
        filter: SearchFilter::default(),
        weights: Some(weights),
    };

    let results = db
        .search_with(cid, &query, options, &ReadOptions::new().at(past))
        .unwrap();
    assert_eq!(results.len(), 2, "a past now is accepted");
    assert_eq!(
        result_ids(&results),
        vec![b_id, a_id],
        "clamped energies rank the high-importance record first"
    );

    let cfg = Config::default().decay;
    for r in &results {
        let exp = &r.experience;
        let clamped = energy(
            exp.importance,
            exp.applications(),
            exp.last_reinforced,
            exp.last_reinforced, // elapsed 0 — the clamp value
            &cfg,
        );
        let at_past = energy(
            exp.importance,
            exp.applications(),
            exp.last_reinforced,
            past,
            &cfg,
        );
        assert_eq!(
            at_past, clamped,
            "a past now must clamp elapsed time to zero"
        );
        let at_wall = energy(
            exp.importance,
            exp.applications(),
            exp.last_reinforced,
            Timestamp::now(),
            &cfg,
        );
        assert!(
            at_past >= at_wall,
            "clamped (past-now) energy must be >= wall-clock energy"
        );
    }
}

/// AC-4: `list_cold_experiences_with` at a pinned `now` returns the same set
/// and order across a sleep, and a later `now` can only add colder records.
#[test]
fn cold_list_with_pinned_now_is_clock_independent() {
    let (db, _dir) = open_db();
    let cid = db.create_collective("cold-list").unwrap();

    let (cold_id, last_cold) = record(&db, cid, "coldest", make_embedding(21), 0.5);
    let (warmer_id, last_warmer) = record(&db, cid, "less cold", make_embedding(22), 0.7);

    let last = last_cold.as_millis().max(last_warmer.as_millis());
    let early = Timestamp::from_millis(last); // energies still high — not cold
    let pinned = Timestamp::from_millis(last + 365 * 24 * 60 * 60 * 1000);

    let first = db
        .list_cold_experiences_with(cid, 0.05, 10, &ReadOptions::new().at(pinned))
        .unwrap();
    std::thread::sleep(Duration::from_millis(20));
    let second = db
        .list_cold_experiences_with(cid, 0.05, 10, &ReadOptions::new().at(pinned))
        .unwrap();

    let ids = |pairs: &[(ExperienceId, f32)]| -> Vec<ExperienceId> {
        pairs.iter().map(|(id, _)| *id).collect()
    };
    assert_eq!(
        ids(&first),
        ids(&second),
        "same set and order across a sleep"
    );
    for (left, right) in first.iter().zip(second.iter()) {
        assert_eq!(left.1.to_bits(), right.1.to_bits(), "energies must match");
    }

    // Coldest first: the 0.5-importance record outranks the 0.7 one.
    assert_eq!(
        ids(&first),
        vec![cold_id, warmer_id],
        "coldest-first order at the pinned now"
    );

    // A later `now` can only add colder records: the early set is a subset.
    let early_set = db
        .list_cold_experiences_with(cid, 0.05, 10, &ReadOptions::new().at(early))
        .unwrap();
    assert!(
        early_set.is_empty(),
        "at the records' own time nothing is below the 0.05 floor yet"
    );
    for (id, _) in &early_set {
        assert!(
            first.iter().any(|(later_id, _)| later_id == id),
            "a later now must not drop records"
        );
    }
}

/// AC-5: `energy_with` at a fixed `now` equals `pulsedb::energy` with the
/// same inputs.
#[test]
fn energy_with_matches_pure_energy() {
    let (db, _dir) = open_db();
    let cid = db.create_collective("energy-with").unwrap();

    let query = make_embedding(31);
    let (id, last) = record(&db, cid, "energy", query, 0.42);
    // Exercise the applications term too.
    let applications = db.reinforce_experience(id).unwrap();
    let stored = db.get_experience(id).unwrap().unwrap();
    assert_eq!(stored.applications(), applications);
    assert!(applications > 0, "reinforcement must have landed");

    let cfg = Config::default().decay;
    let at = Timestamp::from_millis(last.as_millis() + 60 * 60 * 1000); // +1 hour

    let via_read = db.energy_with(id, &ReadOptions::new().at(at)).unwrap();
    let pure = energy(
        stored.importance,
        stored.applications(),
        stored.last_reinforced,
        at,
        &cfg,
    );
    assert_eq!(via_read, pure, "energy_with must be the pure energy()");

    // A later pinned now decays further.
    let later = Timestamp::from_millis(at.as_millis() + 30 * 24 * 60 * 60 * 1000);
    let at_later = db.energy_with(id, &ReadOptions::new().at(later)).unwrap();
    assert!(
        at_later < via_read,
        "energy must decay as the pinned now advances"
    );
}

/// Builds a `NewActivity` for the given collective.
fn activity_for(cid: CollectiveId, agent_id: &str) -> NewActivity {
    NewActivity {
        agent_id: agent_id.to_string(),
        collective_id: cid,
        current_task: None,
        context_summary: None,
    }
}

/// AC-6a: `get_active_agents_with` classifies agents by the injected `now`,
/// not the wall clock — an agent already stale at wall time is active at an
/// earlier injected `now`.
#[test]
fn active_agents_follow_injected_now() {
    // 1 ms threshold: the wall clock passes it within the test.
    let (db, _dir) = open_db_with_stale_threshold(Duration::from_millis(1));
    let cid = db.create_collective("agents-now").unwrap();

    // The heartbeat is stamped inside [before, after].
    let before = Timestamp::now();
    db.register_activity(activity_for(cid, "agent-under-test"))
        .unwrap();
    let after = Timestamp::now();

    // Stale at wall time: 20 ms > the 1 ms threshold.
    std::thread::sleep(Duration::from_millis(20));
    assert!(
        db.get_active_agents(cid).unwrap().is_empty(),
        "the agent must be stale at wall time"
    );

    // Active at an earlier injected now (cutoff = before - 1 ms <= heartbeat).
    let at_earlier = db
        .get_active_agents_with(cid, &ReadOptions::new().at(before))
        .unwrap();
    assert_eq!(
        at_earlier.len(),
        1,
        "the agent must be active at an earlier injected now"
    );
    assert_eq!(at_earlier[0].agent_id, "agent-under-test");

    // Stale once the injected now is past the threshold (+2 ms margin).
    let at_later = db
        .get_active_agents_with(
            cid,
            &ReadOptions::new().at(Timestamp::from_millis(after.as_millis() + 2)),
        )
        .unwrap();
    assert!(
        at_later.is_empty(),
        "an agent older than the threshold at the injected now is not active"
    );
}

/// AC-6b: extreme injected times (`i64::MIN` / `i64::MAX`) are accepted and
/// the staleness cutoff does not overflow.
#[test]
fn extreme_now_does_not_overflow() {
    let (db, _dir) = open_db_with_stale_threshold(Duration::from_millis(1));
    let cid = db.create_collective("agents-extreme").unwrap();
    db.register_activity(activity_for(cid, "extreme-agent"))
        .unwrap();

    // i64::MIN: `cutoff = now - threshold` must not overflow; every stored
    // heartbeat is at or after the cutoff, so the agent is active.
    let at_min = db
        .get_active_agents_with(
            cid,
            &ReadOptions::new().at(Timestamp::from_millis(i64::MIN)),
        )
        .unwrap();
    assert_eq!(
        at_min.len(),
        1,
        "at i64::MIN the agent is inside any real cutoff"
    );

    // i64::MAX: every stored heartbeat is stale at the cutoff.
    let at_max = db
        .get_active_agents_with(
            cid,
            &ReadOptions::new().at(Timestamp::from_millis(i64::MAX)),
        )
        .unwrap();
    assert!(
        at_max.is_empty(),
        "at i64::MAX every heartbeat is long past the cutoff"
    );
}

/// AC-7: `get_context_candidates_with` resolves one `now` for both the
/// weighted search and the active-agent filter — a `now` where the two would
/// disagree under two separate clock reads.
#[test]
fn context_candidates_use_one_resolved_now() {
    let (db, _dir) = open_db();
    let cid = db.create_collective("context-one-now").unwrap();

    // Captured before every write: at this now each record's elapsed time
    // clamps to zero and the heartbeat is fresh.
    let before = Timestamp::now();

    let query = make_embedding(41);
    let mut near = query.clone();
    near[0] += 0.2;
    // a: identical embedding, low importance — wins at a far-future now.
    // b: near embedding, high importance — wins at (or near) the records' own time.
    let (a_id, last_a) = record(&db, cid, "a", query.clone(), 0.1);
    let (b_id, last_b) = record(&db, cid, "b", near, 1.0);
    db.register_activity(activity_for(cid, "context-agent"))
        .unwrap();

    let pinned = Timestamp::from_millis(
        last_a.as_millis().max(last_b.as_millis()) + 365 * 24 * 60 * 60 * 1000,
    );

    let request = ContextRequest {
        collective_id: cid,
        query_embedding: query,
        max_similar: 5,
        max_recent: 5,
        include_insights: false,
        include_relations: false,
        include_active_agents: true,
        filter: SearchFilter::default(),
        recall_weights: Some(RecallWeights::new(0.5, 0.5)),
    };

    // At the far-future now: the energy term collapses, so the
    // high-similarity record wins; the heartbeat is long stale.
    let far = db
        .get_context_candidates_with(request.clone(), &ReadOptions::new().at(pinned))
        .unwrap();
    assert_eq!(
        far.similar_experiences[0].experience.id, a_id,
        "the weighted search must use the injected (far-future) now"
    );
    assert!(
        far.active_agents.is_empty(),
        "the active-agent filter must use the same injected now"
    );

    // At the earlier now: the energy term dominates, so the high-importance
    // record wins; the heartbeat is fresh. A two-clock implementation cannot
    // flip both halves with one injected now.
    let early = db
        .get_context_candidates_with(request, &ReadOptions::new().at(before))
        .unwrap();
    assert_eq!(
        early.similar_experiences[0].experience.id, b_id,
        "at the earlier now the energy-weighted record must win"
    );
    assert_eq!(
        early.active_agents.len(),
        1,
        "the heartbeat is fresh at the earlier now"
    );
    assert_eq!(early.active_agents[0].agent_id, "context-agent");
}

/// AC-8: `ReadMode::Exact` on any `*_with` method returns the typed
/// not-implemented error, checked before any other work — nothing falls back
/// silently to approximate results.
#[test]
fn exact_mode_is_refused_until_implemented() {
    let (db, _dir) = open_db();
    let cid = db.create_collective("exact-refused").unwrap();
    let query = make_embedding(51);

    let exact = ReadOptions::new().exact();

    // Every call passes otherwise-invalid arguments, so only a refusal that
    // runs *before* all other validation can produce the expected error.
    let refusals: Vec<(&str, PulseDBError)> = vec![
        (
            "search_with",
            db.search_with(
                cid,
                &query,
                SearchOptions {
                    k: 0,
                    ..Default::default()
                },
                &exact,
            )
            .unwrap_err(),
        ),
        (
            "energy_with",
            db.energy_with(ExperienceId::new(), &exact).unwrap_err(),
        ),
        (
            "list_cold_experiences_with",
            db.list_cold_experiences_with(CollectiveId::nil(), 2.0, 0, &exact)
                .unwrap_err(),
        ),
        (
            "get_active_agents_with",
            db.get_active_agents_with(CollectiveId::nil(), &exact)
                .unwrap_err(),
        ),
        (
            "get_context_candidates_with",
            db.get_context_candidates_with(
                ContextRequest {
                    collective_id: CollectiveId::nil(),
                    query_embedding: vec![],
                    max_similar: 0,
                    max_recent: 0,
                    ..Default::default()
                },
                &exact,
            )
            .unwrap_err(),
        ),
    ];

    for (name, err) in refusals {
        assert!(
            matches!(err, PulseDBError::Validation(_)),
            "{name}: Exact must be refused with a typed input-validation error, got {err:?}"
        );
        assert!(
            err.to_string().contains("exact mode is not implemented"),
            "{name}: expected the not-implemented refusal, got: {err}"
        );
    }

    // The default (Approximate) mode is not refused.
    let results = db
        .search_with(
            cid,
            &query,
            SearchOptions::default(),
            &ReadOptions::default(),
        )
        .unwrap();
    assert!(results.is_empty(), "no experiences recorded yet");
}

/// AC-9: a query vector containing NaN or ±infinity is refused by
/// `search_with` and `get_context_candidates_with` with a typed error.
#[test]
fn non_finite_query_is_refused() {
    let (db, _dir) = open_db();
    let cid = db.create_collective("finite-query").unwrap();
    record(&db, cid, "sanity", make_embedding(61), 0.5);

    let options = SearchOptions {
        k: 5,
        ..Default::default()
    };

    for (label, value) in [
        ("NaN", f32::NAN),
        ("+infinity", f32::INFINITY),
        ("-infinity", f32::NEG_INFINITY),
    ] {
        let mut query = make_embedding(62);
        query[3] = value;

        let err = db
            .search_with(cid, &query, options.clone(), &ReadOptions::default())
            .unwrap_err();
        assert!(
            matches!(err, PulseDBError::Validation(_)),
            "search_with must refuse {label} with a typed error, got {err:?}"
        );
        assert!(
            err.to_string().contains("finite"),
            "search_with {label}: expected the finiteness refusal, got: {err}"
        );

        let err = db
            .get_context_candidates_with(
                ContextRequest {
                    collective_id: cid,
                    query_embedding: query,
                    max_similar: 5,
                    max_recent: 5,
                    ..Default::default()
                },
                &ReadOptions::default(),
            )
            .unwrap_err();
        assert!(
            matches!(err, PulseDBError::Validation(_)),
            "get_context_candidates_with must refuse {label} with a typed error, got {err:?}"
        );
        assert!(
            err.to_string().contains("finite"),
            "get_context_candidates_with {label}: expected the finiteness refusal, got: {err}"
        );
    }

    // A finite query of the same shape is accepted.
    let fine = make_embedding(62);
    let results = db
        .search_with(cid, &fine, options, &ReadOptions::default())
        .unwrap();
    assert_eq!(results.len(), 1, "the finite control query must succeed");
}
