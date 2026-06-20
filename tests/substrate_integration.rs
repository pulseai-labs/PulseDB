//! Integration tests for SubstrateProvider trait and PulseDBSubstrate.
//!
//! Tests the async wrapper layer: PulseDBSubstrate → spawn_blocking → PulseDB.
//! Uses External embedding provider (default), so all experiences must provide
//! pre-computed embeddings of the correct dimension (384 for D384).

use futures::StreamExt;

use pulsedb::{
    CollectiveId, Config, ContextRequest, InsightType, NewActivity, NewDerivedInsight,
    NewExperience, NewExperienceRelation, PulseDB, PulseDBSubstrate, RelationType, SearchFilter,
    SubstrateProvider,
};
use tempfile::tempdir;

/// Default embedding dimension for tests (D384).
const DIM: usize = 384;

/// Creates a dummy embedding of the correct dimension.
fn dummy_embedding() -> Vec<f32> {
    vec![0.1; DIM]
}

/// Creates a distinct embedding seeded by a value (for search ordering tests).
///
/// Uses two components so that different seeds produce genuinely different
/// directions after normalization (a single-component vector always normalizes
/// to the same unit vector regardless of seed).
fn seeded_embedding(seed: f32) -> Vec<f32> {
    let mut emb = vec![0.0; DIM];
    emb[0] = seed;
    emb[1] = 1.0 - seed; // second component ensures distinct direction
                         // Normalize to unit length for cosine similarity
    let norm = emb.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        emb.iter_mut().for_each(|x| *x /= norm);
    }
    emb
}

/// Helper: open DB, create a collective, return substrate + collective ID.
fn setup() -> (PulseDBSubstrate, CollectiveId, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let db = PulseDB::open(&path, Config::default()).unwrap();
    let cid = db.create_collective("test-collective").unwrap();
    let substrate = PulseDBSubstrate::from_db(db);
    (substrate, cid, dir)
}

/// Helper to build a minimal valid NewExperience for a given collective.
fn minimal_experience(collective_id: CollectiveId) -> NewExperience {
    NewExperience {
        collective_id,
        content: "Always validate user input before processing".to_string(),
        embedding: Some(dummy_embedding()),
        ..Default::default()
    }
}

// ============================================================================
// Experience operations
// ============================================================================

#[tokio::test]
async fn test_store_and_get_experience() {
    let (substrate, cid, _dir) = setup();

    let exp_id = substrate
        .store_experience(minimal_experience(cid))
        .await
        .unwrap();

    let retrieved = substrate.get_experience(exp_id).await.unwrap();
    assert!(retrieved.is_some());
    let exp = retrieved.unwrap();
    assert_eq!(exp.id, exp_id);
    assert_eq!(exp.content, "Always validate user input before processing");
    assert_eq!(exp.collective_id, cid);
}

#[tokio::test]
async fn test_get_experience_not_found() {
    let (substrate, _cid, _dir) = setup();

    let fake_id = pulsedb::ExperienceId::new();
    let result = substrate.get_experience(fake_id).await.unwrap();
    assert!(result.is_none());
}

// ============================================================================
// Search operations
// ============================================================================

#[tokio::test]
async fn test_search_similar_returns_tuples() {
    let (substrate, cid, _dir) = setup();

    let emb_a = seeded_embedding(1.0);
    let emb_b = seeded_embedding(2.0);

    substrate
        .store_experience(NewExperience {
            collective_id: cid,
            content: "Experience A".to_string(),
            embedding: Some(emb_a.clone()),
            ..Default::default()
        })
        .await
        .unwrap();

    substrate
        .store_experience(NewExperience {
            collective_id: cid,
            content: "Experience B".to_string(),
            embedding: Some(emb_b),
            ..Default::default()
        })
        .await
        .unwrap();

    // Search with emb_a — Experience A should be most similar
    let results = substrate.search_similar(cid, &emb_a, 10).await.unwrap();
    assert_eq!(results.len(), 2);

    // Verify tuple shape: (Experience, f32)
    let (exp, score) = &results[0];
    assert_eq!(exp.content, "Experience A");
    assert!(*score > 0.0 && *score <= 1.0);

    // Results should be ordered by similarity descending
    assert!(results[0].1 >= results[1].1);
}

#[tokio::test]
async fn test_search_similar_empty_collective() {
    let (substrate, cid, _dir) = setup();
    let results = substrate
        .search_similar(cid, &dummy_embedding(), 10)
        .await
        .unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn test_get_recent() {
    let (substrate, cid, _dir) = setup();

    for i in 0..3 {
        substrate
            .store_experience(NewExperience {
                collective_id: cid,
                content: format!("Experience {i}"),
                embedding: Some(dummy_embedding()),
                ..Default::default()
            })
            .await
            .unwrap();
    }

    let recent = substrate.get_recent(cid, 2).await.unwrap();
    assert_eq!(recent.len(), 2);
    // Most recent first
    assert_eq!(recent[0].content, "Experience 2");
    assert_eq!(recent[1].content, "Experience 1");
}

// ============================================================================
// Relation operations
// ============================================================================

#[tokio::test]
async fn test_store_and_get_relation() {
    let (substrate, cid, _dir) = setup();

    let exp_a = substrate
        .store_experience(NewExperience {
            collective_id: cid,
            content: "Source experience".to_string(),
            embedding: Some(dummy_embedding()),
            ..Default::default()
        })
        .await
        .unwrap();

    let exp_b = substrate
        .store_experience(NewExperience {
            collective_id: cid,
            content: "Target experience".to_string(),
            embedding: Some(dummy_embedding()),
            ..Default::default()
        })
        .await
        .unwrap();

    let _rel_id = substrate
        .store_relation(NewExperienceRelation {
            source_id: exp_a,
            target_id: exp_b,
            relation_type: RelationType::Supports,
            strength: 0.9,
            metadata: None,
        })
        .await
        .unwrap();

    // get_related from source should find target
    let related = substrate.get_related(exp_a).await.unwrap();
    assert_eq!(related.len(), 1);
    assert_eq!(related[0].0.id, exp_b);
    assert_eq!(related[0].1.relation_type, RelationType::Supports);
}

#[tokio::test]
async fn test_get_related_returns_both_directions() {
    let (substrate, cid, _dir) = setup();

    let exp_a = substrate
        .store_experience(NewExperience {
            collective_id: cid,
            content: "A".to_string(),
            embedding: Some(dummy_embedding()),
            ..Default::default()
        })
        .await
        .unwrap();

    let exp_b = substrate
        .store_experience(NewExperience {
            collective_id: cid,
            content: "B".to_string(),
            embedding: Some(dummy_embedding()),
            ..Default::default()
        })
        .await
        .unwrap();

    // A → B (Supports)
    substrate
        .store_relation(NewExperienceRelation {
            source_id: exp_a,
            target_id: exp_b,
            relation_type: RelationType::Supports,
            strength: 0.8,
            metadata: None,
        })
        .await
        .unwrap();

    // Query from B — should find A via incoming relation
    let related = substrate.get_related(exp_b).await.unwrap();
    assert_eq!(related.len(), 1);
    assert_eq!(related[0].0.id, exp_a);
}

// ============================================================================
// Insight operations
// ============================================================================

#[tokio::test]
async fn test_store_and_search_insights() {
    let (substrate, cid, _dir) = setup();

    // Need source experiences for insight validation
    let exp_id = substrate
        .store_experience(NewExperience {
            collective_id: cid,
            content: "Source experience".to_string(),
            embedding: Some(dummy_embedding()),
            ..Default::default()
        })
        .await
        .unwrap();

    let emb = seeded_embedding(1.0);

    substrate
        .store_insight(NewDerivedInsight {
            collective_id: cid,
            content: "Derived pattern insight".to_string(),
            embedding: Some(emb.clone()),
            source_experience_ids: vec![exp_id],
            insight_type: InsightType::Pattern,
            confidence: 0.8,
            domain: vec![],
        })
        .await
        .unwrap();

    // Search with same embedding
    let results = substrate.get_insights(cid, &emb, 5).await.unwrap();
    assert_eq!(results.len(), 1);

    let (insight, score) = &results[0];
    assert_eq!(insight.content, "Derived pattern insight");
    assert!(*score > 0.0 && *score <= 1.0);
}

// ============================================================================
// Activity operations
// ============================================================================

#[tokio::test]
async fn test_get_activities() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let db = PulseDB::open(&path, Config::default()).unwrap();
    let cid = db.create_collective("test-collective").unwrap();

    // Register activity via sync API (SubstrateProvider doesn't expose register)
    db.register_activity(NewActivity {
        agent_id: "claude-opus".to_string(),
        collective_id: cid,
        current_task: Some("Testing".to_string()),
        context_summary: None,
    })
    .unwrap();

    let substrate = PulseDBSubstrate::from_db(db);
    let activities = substrate.get_activities(cid).await.unwrap();
    assert_eq!(activities.len(), 1);
    assert_eq!(activities[0].agent_id, "claude-opus");
}

// ============================================================================
// Context candidates
// ============================================================================

#[tokio::test]
async fn test_get_context_candidates() {
    let (substrate, cid, _dir) = setup();

    let emb = dummy_embedding();

    substrate
        .store_experience(NewExperience {
            collective_id: cid,
            content: "Test experience for context".to_string(),
            embedding: Some(emb.clone()),
            importance: 0.8,
            ..Default::default()
        })
        .await
        .unwrap();

    let candidates = substrate
        .get_context_candidates(ContextRequest {
            collective_id: cid,
            query_embedding: emb,
            max_similar: 10,
            max_recent: 5,
            include_insights: false,
            include_relations: false,
            include_active_agents: false,
            filter: SearchFilter::default(),
            recall_weights: None,
        })
        .await
        .unwrap();

    assert!(!candidates.similar_experiences.is_empty());
    assert!(!candidates.recent_experiences.is_empty());
}

// ============================================================================
// Watch
// ============================================================================

#[tokio::test]
async fn test_watch_delivers_events() {
    let (substrate, cid, _dir) = setup();

    let mut stream = substrate.watch(cid).await.unwrap();

    // Store an experience — should trigger a Created event
    let exp_id = substrate
        .store_experience(minimal_experience(cid))
        .await
        .unwrap();

    let event = tokio::time::timeout(std::time::Duration::from_secs(1), stream.as_mut().next())
        .await
        .expect("timed out waiting for watch event")
        .expect("stream ended unexpectedly");

    assert_eq!(event.experience_id, exp_id);
    assert_eq!(event.collective_id, cid);
    assert_eq!(event.event_type, pulsedb::WatchEventType::Created);
}

// ============================================================================
// Trait object safety
// ============================================================================

#[tokio::test]
async fn test_trait_object_safety() {
    let (substrate, cid, _dir) = setup();

    // This must compile — consumers hold Box<dyn SubstrateProvider>
    let provider: Box<dyn SubstrateProvider> = Box::new(substrate);

    let exp_id = provider
        .store_experience(minimal_experience(cid))
        .await
        .unwrap();

    let retrieved = provider.get_experience(exp_id).await.unwrap();
    assert!(retrieved.is_some());
}

#[tokio::test]
async fn test_reinforce_and_energy_through_trait_object() {
    let (substrate, cid, _dir) = setup();
    let provider: Box<dyn SubstrateProvider> = Box::new(substrate);

    let exp_id = provider
        .store_experience(minimal_experience(cid))
        .await
        .unwrap();

    let before = provider.energy(exp_id).await.unwrap();
    let count = provider.reinforce_experience(exp_id).await.unwrap();
    let after = provider.energy(exp_id).await.unwrap();

    let exp = provider.get_experience(exp_id).await.unwrap().unwrap();
    assert_eq!(count, 1);
    assert_eq!(exp.applications(), 1);
    assert!((0.0..=1.0).contains(&after));
    assert!(after >= before);
}

// ============================================================================
// Clone & concurrency
// ============================================================================

#[tokio::test]
async fn test_clone_and_concurrent_operations() {
    let (substrate, cid, _dir) = setup();

    let mut handles = Vec::new();

    for i in 0..5 {
        let s = substrate.clone();
        handles.push(tokio::spawn(async move {
            s.store_experience(NewExperience {
                collective_id: cid,
                content: format!("Concurrent experience {i}"),
                embedding: Some(vec![0.1; DIM]),
                ..Default::default()
            })
            .await
            .unwrap()
        }));
    }

    let mut exp_ids = Vec::new();
    for handle in handles {
        exp_ids.push(handle.await.unwrap());
    }

    // All 5 should be stored successfully with unique IDs
    assert_eq!(exp_ids.len(), 5);
    // Verify uniqueness via HashSet instead of sort (ExperienceId doesn't impl Ord)
    let unique: std::collections::HashSet<_> = exp_ids.iter().collect();
    assert_eq!(unique.len(), 5);
}

// ============================================================================
// Collective lifecycle (SubstrateProvider methods)
// ============================================================================

#[tokio::test]
async fn test_substrate_create_collective() {
    let dir = tempdir().unwrap();
    let db = PulseDB::open(dir.path().join("test.db"), Config::default()).unwrap();
    let substrate = PulseDBSubstrate::from_db(db);
    let provider: Box<dyn SubstrateProvider> = Box::new(substrate);

    let cid = provider.create_collective("test-project").await.unwrap();

    // Verify collective exists by listing
    let collectives = provider.list_collectives().await.unwrap();
    assert_eq!(collectives.len(), 1);
    assert_eq!(collectives[0].id, cid);
    assert_eq!(collectives[0].name, "test-project");
}

#[tokio::test]
async fn test_substrate_get_or_create_collective_new() {
    let dir = tempdir().unwrap();
    let db = PulseDB::open(dir.path().join("test.db"), Config::default()).unwrap();
    let substrate = PulseDBSubstrate::from_db(db);
    let provider: Box<dyn SubstrateProvider> = Box::new(substrate);

    // First call creates
    let cid = provider
        .get_or_create_collective("my-project")
        .await
        .unwrap();

    let collectives = provider.list_collectives().await.unwrap();
    assert_eq!(collectives.len(), 1);
    assert_eq!(collectives[0].id, cid);
}

#[tokio::test]
async fn test_substrate_get_or_create_collective_idempotent() {
    let dir = tempdir().unwrap();
    let db = PulseDB::open(dir.path().join("test.db"), Config::default()).unwrap();
    let substrate = PulseDBSubstrate::from_db(db);
    let provider: Box<dyn SubstrateProvider> = Box::new(substrate);

    // Call twice with same name
    let cid1 = provider
        .get_or_create_collective("my-project")
        .await
        .unwrap();
    let cid2 = provider
        .get_or_create_collective("my-project")
        .await
        .unwrap();

    // Same ID returned
    assert_eq!(cid1, cid2);

    // Only one collective exists
    let collectives = provider.list_collectives().await.unwrap();
    assert_eq!(collectives.len(), 1);
}

#[tokio::test]
async fn test_substrate_list_collectives_multiple() {
    let dir = tempdir().unwrap();
    let db = PulseDB::open(dir.path().join("test.db"), Config::default()).unwrap();
    let substrate = PulseDBSubstrate::from_db(db);
    let provider: Box<dyn SubstrateProvider> = Box::new(substrate);

    provider.create_collective("alpha").await.unwrap();
    provider.create_collective("beta").await.unwrap();
    provider.create_collective("gamma").await.unwrap();

    let collectives = provider.list_collectives().await.unwrap();
    assert_eq!(collectives.len(), 3);

    let names: Vec<&str> = collectives.iter().map(|c| c.name.as_str()).collect();
    assert!(names.contains(&"alpha"));
    assert!(names.contains(&"beta"));
    assert!(names.contains(&"gamma"));
}

#[tokio::test]
async fn test_substrate_get_or_create_then_store_experience() {
    let dir = tempdir().unwrap();
    let db = PulseDB::open(dir.path().join("test.db"), Config::default()).unwrap();
    let substrate = PulseDBSubstrate::from_db(db);
    let provider: Box<dyn SubstrateProvider> = Box::new(substrate);

    // Create collective through trait, then store experience
    let cid = provider
        .get_or_create_collective("my-project")
        .await
        .unwrap();

    let exp_id = provider
        .store_experience(NewExperience {
            collective_id: cid,
            content: "Test experience through substrate".to_string(),
            embedding: Some(dummy_embedding()),
            ..Default::default()
        })
        .await
        .unwrap();

    let exp = provider.get_experience(exp_id).await.unwrap().unwrap();
    assert_eq!(exp.collective_id, cid);
    assert_eq!(exp.content, "Test experience through substrate");
}
