//! Async storage trait for integrating PulseDB with agent frameworks.
//!
//! This module defines the async interface for integrating PulseDB with
//! agent frameworks and orchestration layers. Consumers hold a
//! `Box<dyn SubstrateProvider>` to interact with the database without
//! knowing the concrete storage implementation.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────────┐       ┌──────────────────────┐
//! │   Agent Framework    │       │       PulseDB         │
//! │                      │       │                       │
//! │  Orchestrator ───────┼──────►│  PulseDBSubstrate     │
//! │  Box<dyn Substrate>  │       │  (Arc<PulseDB>)       │
//! │                      │       │                       │
//! │  Agents interact     │       │  spawn_blocking ──►   │
//! │  through the trait   │       │  sync storage ops     │
//! │                      │       │                       │
//! └──────────────────────┘       └──────────────────────┘
//! ```
//!
//! # Example
//!
//! ```rust,no_run
//! # fn main() -> pulsedb::Result<()> {
//! # let dir = tempfile::tempdir().unwrap();
//! use std::sync::Arc;
//! use pulsedb::{PulseDB, Config, PulseDBSubstrate, SubstrateProvider};
//!
//! // Create PulseDB and wrap in substrate
//! let db = Arc::new(PulseDB::open(dir.path().join("test.db"), Config::default())?);
//! let substrate = PulseDBSubstrate::new(db);
//!
//! // Use as trait object
//! let provider: Box<dyn SubstrateProvider> = Box::new(substrate);
//!
//! // All operations are async (shown here for illustration)
//! // let exp_id = provider.store_experience(new_exp).await?;
//! // let results = provider.search_similar(collective, &embedding, 10).await?;
//! # Ok(())
//! # }
//! ```

mod r#impl;

pub use r#impl::PulseDBSubstrate;

use std::pin::Pin;

use async_trait::async_trait;
use futures_core::Stream;

use crate::activity::Activity;
use crate::collective::Collective;
use crate::error::PulseDBError;
use crate::experience::{Experience, NewExperience};
use crate::insight::{DerivedInsight, NewDerivedInsight};
use crate::relation::{ExperienceRelation, NewExperienceRelation};
use crate::search::{ContextCandidates, ContextRequest};
use crate::types::{CollectiveId, ExperienceId, InsightId, RelationId};
use crate::watch::WatchEvent;

/// Async storage interface for agent framework integration.
///
/// This trait abstracts PulseDB's storage capabilities behind an async
/// boundary, enabling agent frameworks to interact with the database
/// without blocking the async runtime.
///
/// # Object Safety
///
/// `SubstrateProvider` is object-safe via `#[async_trait]`, allowing it to
/// be used as `Box<dyn SubstrateProvider>` in any async context.
///
/// # Implementors
///
/// - [`PulseDBSubstrate`] — production implementation wrapping `Arc<PulseDB>`
#[async_trait]
pub trait SubstrateProvider: Send + Sync {
    /// Stores a new experience and returns its assigned ID.
    ///
    /// Generates an embedding (if configured), writes to storage, and
    /// indexes in the collective's HNSW graph.
    async fn store_experience(&self, exp: NewExperience) -> Result<ExperienceId, PulseDBError>;

    /// Retrieves an experience by ID, or `None` if it doesn't exist.
    async fn get_experience(&self, id: ExperienceId) -> Result<Option<Experience>, PulseDBError>;

    /// Reinforces an experience and returns its summed application count.
    ///
    /// The default implementation returns an unsupported-operation error so
    /// existing custom providers remain source-compatible without pretending a
    /// mutation succeeded.
    async fn reinforce_experience(&self, _id: ExperienceId) -> Result<u32, PulseDBError> {
        Err(PulseDBError::internal(
            "SubstrateProvider::reinforce_experience is not supported by this implementation",
        ))
    }

    /// Computes the current temporal energy for an experience.
    ///
    /// The default implementation returns an unsupported-operation error so
    /// existing custom providers remain source-compatible without inventing a
    /// misleading energy value.
    async fn energy(&self, _id: ExperienceId) -> Result<f32, PulseDBError> {
        Err(PulseDBError::internal(
            "SubstrateProvider::energy is not supported by this implementation",
        ))
    }

    /// Searches for experiences similar to the given embedding.
    ///
    /// Returns up to `k` results as `(Experience, similarity_score)` tuples,
    /// sorted by similarity descending (1.0 = identical).
    async fn search_similar(
        &self,
        collective: CollectiveId,
        embedding: &[f32],
        k: usize,
    ) -> Result<Vec<(Experience, f32)>, PulseDBError>;

    /// Retrieves the most recent experiences from a collective.
    ///
    /// Returns up to `limit` experiences sorted by timestamp descending.
    async fn get_recent(
        &self,
        collective: CollectiveId,
        limit: usize,
    ) -> Result<Vec<Experience>, PulseDBError>;

    /// Stores a relation between two experiences.
    async fn store_relation(&self, rel: NewExperienceRelation) -> Result<RelationId, PulseDBError>;

    /// Retrieves all experiences related to the given experience (both directions).
    ///
    /// Returns `(related_experience, relation)` tuples for both outgoing
    /// and incoming relations.
    async fn get_related(
        &self,
        exp_id: ExperienceId,
    ) -> Result<Vec<(Experience, ExperienceRelation)>, PulseDBError>;

    /// Stores a derived insight synthesized from source experiences.
    async fn store_insight(&self, insight: NewDerivedInsight) -> Result<InsightId, PulseDBError>;

    /// Searches for insights similar to the given embedding.
    ///
    /// Returns up to `k` results as `(DerivedInsight, similarity_score)` tuples.
    async fn get_insights(
        &self,
        collective: CollectiveId,
        embedding: &[f32],
        k: usize,
    ) -> Result<Vec<(DerivedInsight, f32)>, PulseDBError>;

    /// Retrieves active (non-stale) agent activities in a collective.
    async fn get_activities(&self, collective: CollectiveId)
        -> Result<Vec<Activity>, PulseDBError>;

    /// Assembles context candidates from all retrieval primitives.
    ///
    /// Orchestrates similarity search, recent experiences, insights,
    /// relations, and active agents into a single response.
    async fn get_context_candidates(
        &self,
        request: ContextRequest,
    ) -> Result<ContextCandidates, PulseDBError>;

    /// Subscribes to real-time experience change events in a collective.
    ///
    /// Returns a `Stream` that yields [`WatchEvent`] values whenever
    /// experiences are created, updated, archived, or deleted.
    async fn watch(
        &self,
        collective: CollectiveId,
    ) -> Result<Pin<Box<dyn Stream<Item = WatchEvent> + Send>>, PulseDBError>;

    /// Creates a new collective (namespace).
    ///
    /// Returns the new collective's ID. Fails if a collective with the
    /// same name already exists.
    async fn create_collective(&self, name: &str) -> Result<CollectiveId, PulseDBError>;

    /// Gets an existing collective by name, or creates it if it doesn't exist.
    ///
    /// This is the recommended method for SDK consumers — idempotent and safe
    /// to call repeatedly with the same name.
    async fn get_or_create_collective(&self, name: &str) -> Result<CollectiveId, PulseDBError>;

    /// Lists all collectives in the database.
    async fn list_collectives(&self) -> Result<Vec<Collective>, PulseDBError>;

    /// Lists experiences in a collective with pagination.
    ///
    /// Returns full `Experience` records including embeddings.
    /// Default implementation returns empty vec for backward compatibility.
    async fn list_experiences(
        &self,
        _collective: CollectiveId,
        _limit: usize,
        _offset: usize,
    ) -> Result<Vec<Experience>, PulseDBError> {
        Ok(vec![])
    }

    /// Lists relations in a collective with pagination.
    ///
    /// Default implementation returns empty vec for backward compatibility.
    async fn list_relations(
        &self,
        _collective: CollectiveId,
        _limit: usize,
        _offset: usize,
    ) -> Result<Vec<ExperienceRelation>, PulseDBError> {
        Ok(vec![])
    }

    /// Lists insights in a collective with pagination.
    ///
    /// Default implementation returns empty vec for backward compatibility.
    async fn list_insights(
        &self,
        _collective: CollectiveId,
        _limit: usize,
        _offset: usize,
    ) -> Result<Vec<DerivedInsight>, PulseDBError> {
        Ok(vec![])
    }

    /// Lists cold (prune-eligible) experiences whose temporal energy is below a
    /// threshold, coldest-first.
    ///
    /// Returns up to `limit` lightweight `(ExperienceId, energy)` pairs sorted by
    /// energy ascending — the read-only signal a reviewer uses to surface
    /// prune-eligible candidates. The substrate exposes only this signal; it
    /// deliberately offers **no** archive/prune mutation (read-only consistency
    /// surface).
    ///
    /// The default implementation returns an unsupported-operation error so
    /// existing custom providers remain source-compatible without inventing a
    /// misleading candidate list.
    async fn list_cold_experiences(
        &self,
        _collective: CollectiveId,
        _below: f32,
        _limit: usize,
    ) -> Result<Vec<(ExperienceId, f32)>, PulseDBError> {
        Err(PulseDBError::internal(
            "SubstrateProvider::list_cold_experiences is not supported by this implementation",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal non-PulseDB provider that implements ONLY the required (non-default)
    /// trait methods, leaving every defaulted method — including
    /// `list_cold_experiences` — at its trait default. Exists solely to assert the
    /// read-only cold-list default returns the unsupported-operation `Err` and that
    /// `SubstrateProvider` stays object-safe (`Box<dyn …>`-able).
    struct StubProvider;

    #[async_trait]
    impl SubstrateProvider for StubProvider {
        async fn store_experience(
            &self,
            _exp: NewExperience,
        ) -> Result<ExperienceId, PulseDBError> {
            unimplemented!("stub")
        }

        async fn get_experience(
            &self,
            _id: ExperienceId,
        ) -> Result<Option<Experience>, PulseDBError> {
            unimplemented!("stub")
        }

        async fn search_similar(
            &self,
            _collective: CollectiveId,
            _embedding: &[f32],
            _k: usize,
        ) -> Result<Vec<(Experience, f32)>, PulseDBError> {
            unimplemented!("stub")
        }

        async fn get_recent(
            &self,
            _collective: CollectiveId,
            _limit: usize,
        ) -> Result<Vec<Experience>, PulseDBError> {
            unimplemented!("stub")
        }

        async fn store_relation(
            &self,
            _rel: NewExperienceRelation,
        ) -> Result<RelationId, PulseDBError> {
            unimplemented!("stub")
        }

        async fn get_related(
            &self,
            _exp_id: ExperienceId,
        ) -> Result<Vec<(Experience, ExperienceRelation)>, PulseDBError> {
            unimplemented!("stub")
        }

        async fn store_insight(
            &self,
            _insight: NewDerivedInsight,
        ) -> Result<InsightId, PulseDBError> {
            unimplemented!("stub")
        }

        async fn get_insights(
            &self,
            _collective: CollectiveId,
            _embedding: &[f32],
            _k: usize,
        ) -> Result<Vec<(DerivedInsight, f32)>, PulseDBError> {
            unimplemented!("stub")
        }

        async fn get_activities(
            &self,
            _collective: CollectiveId,
        ) -> Result<Vec<Activity>, PulseDBError> {
            unimplemented!("stub")
        }

        async fn get_context_candidates(
            &self,
            _request: ContextRequest,
        ) -> Result<ContextCandidates, PulseDBError> {
            unimplemented!("stub")
        }

        async fn watch(
            &self,
            _collective: CollectiveId,
        ) -> Result<Pin<Box<dyn Stream<Item = WatchEvent> + Send>>, PulseDBError> {
            unimplemented!("stub")
        }

        async fn create_collective(&self, _name: &str) -> Result<CollectiveId, PulseDBError> {
            unimplemented!("stub")
        }

        async fn get_or_create_collective(
            &self,
            _name: &str,
        ) -> Result<CollectiveId, PulseDBError> {
            unimplemented!("stub")
        }

        async fn list_collectives(&self) -> Result<Vec<Collective>, PulseDBError> {
            unimplemented!("stub")
        }
        // NOTE: list_cold_experiences is intentionally NOT overridden — the test
        // exercises the trait DEFAULT.
    }

    /// The default `list_cold_experiences` must return the unsupported-operation
    /// `Err` for a non-PulseDB provider (read-only signal stays inert unless a
    /// concrete impl wires it through). Non-vacuous: a real provider, real call,
    /// real assertion on the `Err` payload.
    #[tokio::test]
    async fn default_list_cold_experiences_is_unsupported() {
        let provider = StubProvider;
        let result = provider
            .list_cold_experiences(CollectiveId::nil(), 0.5, 10)
            .await;

        let err = result.expect_err("default list_cold_experiences must return an Err");
        assert!(
            err.to_string()
                .contains("not supported by this implementation"),
            "expected unsupported-operation error, got: {err}"
        );
    }

    /// Guards trait object-safety: `SubstrateProvider` must remain
    /// `Box<dyn …>`-able after adding the cold-list method, and the default is
    /// reachable through the trait object.
    #[tokio::test]
    async fn cold_list_default_is_object_safe() {
        let provider: Box<dyn SubstrateProvider> = Box::new(StubProvider);
        let result = provider
            .list_cold_experiences(CollectiveId::nil(), 0.25, 5)
            .await;
        assert!(result.is_err(), "default via trait object must be Err");
    }
}
