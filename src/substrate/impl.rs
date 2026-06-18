//! Concrete SubstrateProvider implementation backed by PulseDB.

use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures_core::Stream;
use tokio::task::spawn_blocking;

use crate::activity::Activity;
use crate::collective::Collective;
use crate::db::PulseDB;
use crate::error::PulseDBError;
use crate::experience::{Experience, NewExperience};
use crate::insight::{DerivedInsight, NewDerivedInsight};
use crate::relation::{ExperienceRelation, NewExperienceRelation, RelationDirection};
use crate::search::{ContextCandidates, ContextRequest};
use crate::types::{CollectiveId, ExperienceId, InsightId, RelationId};
use crate::watch::WatchEvent;

use super::SubstrateProvider;

/// Async adapter wrapping [`PulseDB`] for use as a [`SubstrateProvider`].
///
/// Each async method delegates to PulseDB's synchronous API via
/// [`tokio::task::spawn_blocking`], preventing database I/O from blocking
/// the async runtime's worker threads.
///
/// # Construction
///
/// ```rust
/// # fn main() -> pulsedb::Result<()> {
/// # let dir = tempfile::tempdir().unwrap();
/// use std::sync::Arc;
/// use pulsedb::{PulseDB, Config, PulseDBSubstrate};
///
/// let db = Arc::new(PulseDB::open(dir.path().join("a.db"), Config::default())?);
/// let substrate = PulseDBSubstrate::new(db);
///
/// // Or from an owned PulseDB:
/// let db = PulseDB::open(dir.path().join("b.db"), Config::default())?;
/// let substrate = PulseDBSubstrate::from_db(db);
/// # Ok(())
/// # }
/// ```
///
/// # Cloning
///
/// `PulseDBSubstrate` implements `Clone` — cloning is cheap (Arc reference count).
/// Multiple clones share the same underlying database.
#[derive(Clone)]
pub struct PulseDBSubstrate {
    db: Arc<PulseDB>,
}

impl PulseDBSubstrate {
    /// Creates a new substrate provider from a shared `PulseDB` reference.
    pub fn new(db: Arc<PulseDB>) -> Self {
        Self { db }
    }

    /// Creates a new substrate provider, wrapping the given `PulseDB` in an `Arc`.
    pub fn from_db(db: PulseDB) -> Self {
        Self { db: Arc::new(db) }
    }
}

/// Runs a blocking closure on tokio's blocking thread pool.
///
/// Maps `JoinError` (task panic or cancellation) to `PulseDBError::Internal`.
async fn blocking<F, T>(f: F) -> Result<T, PulseDBError>
where
    F: FnOnce() -> Result<T, PulseDBError> + Send + 'static,
    T: Send + 'static,
{
    spawn_blocking(f)
        .await
        .map_err(|e| PulseDBError::internal(format!("blocking task failed: {e}")))?
}

#[async_trait]
impl SubstrateProvider for PulseDBSubstrate {
    async fn store_experience(&self, exp: NewExperience) -> Result<ExperienceId, PulseDBError> {
        let db = Arc::clone(&self.db);
        blocking(move || db.record_experience(exp)).await
    }

    async fn get_experience(&self, id: ExperienceId) -> Result<Option<Experience>, PulseDBError> {
        let db = Arc::clone(&self.db);
        blocking(move || db.get_experience(id)).await
    }

    async fn reinforce_experience(&self, id: ExperienceId) -> Result<u32, PulseDBError> {
        let db = Arc::clone(&self.db);
        blocking(move || db.reinforce_experience(id)).await
    }

    async fn energy(&self, id: ExperienceId) -> Result<f32, PulseDBError> {
        let db = Arc::clone(&self.db);
        blocking(move || db.energy(id)).await
    }

    async fn search_similar(
        &self,
        collective: CollectiveId,
        embedding: &[f32],
        k: usize,
    ) -> Result<Vec<(Experience, f32)>, PulseDBError> {
        let db = Arc::clone(&self.db);
        // Must clone the slice — spawn_blocking requires 'static
        let embedding = embedding.to_vec();
        blocking(move || {
            db.search_similar(collective, &embedding, k).map(|results| {
                results
                    .into_iter()
                    .map(|r| (r.experience, r.similarity))
                    .collect()
            })
        })
        .await
    }

    async fn get_recent(
        &self,
        collective: CollectiveId,
        limit: usize,
    ) -> Result<Vec<Experience>, PulseDBError> {
        let db = Arc::clone(&self.db);
        blocking(move || db.get_recent_experiences(collective, limit)).await
    }

    async fn store_relation(&self, rel: NewExperienceRelation) -> Result<RelationId, PulseDBError> {
        let db = Arc::clone(&self.db);
        blocking(move || db.store_relation(rel)).await
    }

    async fn get_related(
        &self,
        exp_id: ExperienceId,
    ) -> Result<Vec<(Experience, ExperienceRelation)>, PulseDBError> {
        let db = Arc::clone(&self.db);
        blocking(move || db.get_related_experiences(exp_id, RelationDirection::Both)).await
    }

    async fn store_insight(&self, insight: NewDerivedInsight) -> Result<InsightId, PulseDBError> {
        let db = Arc::clone(&self.db);
        blocking(move || db.store_insight(insight)).await
    }

    async fn get_insights(
        &self,
        collective: CollectiveId,
        embedding: &[f32],
        k: usize,
    ) -> Result<Vec<(DerivedInsight, f32)>, PulseDBError> {
        let db = Arc::clone(&self.db);
        let embedding = embedding.to_vec();
        blocking(move || db.get_insights(collective, &embedding, k)).await
    }

    async fn get_activities(
        &self,
        collective: CollectiveId,
    ) -> Result<Vec<Activity>, PulseDBError> {
        let db = Arc::clone(&self.db);
        blocking(move || db.get_active_agents(collective)).await
    }

    async fn get_context_candidates(
        &self,
        request: ContextRequest,
    ) -> Result<ContextCandidates, PulseDBError> {
        let db = Arc::clone(&self.db);
        blocking(move || db.get_context_candidates(request)).await
    }

    async fn watch(
        &self,
        collective: CollectiveId,
    ) -> Result<Pin<Box<dyn Stream<Item = WatchEvent> + Send>>, PulseDBError> {
        // watch_experiences is non-blocking (just channel setup), no spawn_blocking needed
        let stream = self.db.watch_experiences(collective)?;
        Ok(Box::pin(stream))
    }

    async fn create_collective(&self, name: &str) -> Result<CollectiveId, PulseDBError> {
        let db = Arc::clone(&self.db);
        let name = name.to_string();
        blocking(move || db.create_collective(&name)).await
    }

    async fn get_or_create_collective(&self, name: &str) -> Result<CollectiveId, PulseDBError> {
        let db = Arc::clone(&self.db);
        let name = name.to_string();
        blocking(move || {
            // Try to find existing by name
            let collectives = db.list_collectives()?;
            if let Some(existing) = collectives.iter().find(|c| c.name == name) {
                return Ok(existing.id);
            }
            // Not found — create new
            db.create_collective(&name)
        })
        .await
    }

    async fn list_collectives(&self) -> Result<Vec<Collective>, PulseDBError> {
        let db = Arc::clone(&self.db);
        blocking(move || db.list_collectives()).await
    }

    async fn list_experiences(
        &self,
        collective: CollectiveId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Experience>, PulseDBError> {
        let db = Arc::clone(&self.db);
        blocking(move || db.list_experiences(collective, limit, offset)).await
    }

    async fn list_relations(
        &self,
        collective: CollectiveId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ExperienceRelation>, PulseDBError> {
        let db = Arc::clone(&self.db);
        blocking(move || db.list_relations(collective, limit, offset)).await
    }

    async fn list_insights(
        &self,
        collective: CollectiveId,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<DerivedInsight>, PulseDBError> {
        let db = Arc::clone(&self.db);
        blocking(move || db.list_insights(collective, limit, offset)).await
    }

    async fn list_cold_experiences(
        &self,
        collective: CollectiveId,
        below: f32,
        limit: usize,
    ) -> Result<Vec<(ExperienceId, f32)>, PulseDBError> {
        let db = Arc::clone(&self.db);
        blocking(move || db.list_cold_experiences(collective, below, limit)).await
    }
}
