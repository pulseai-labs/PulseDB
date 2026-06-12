//! Core type definitions for PulseDB identifiers and timestamps.
//!
//! This module defines the fundamental ID types used throughout PulseDB.
//! All ID types use UUID v7 for time-ordered unique identification.

use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Collective identifier (UUID v7 for time-ordering).
///
/// Collectives are isolated namespaces for agent experiences, typically one per project.
/// Each collective has its own HNSW index and embedding dimension.
///
/// # Example
/// ```
/// use pulsedb::CollectiveId;
///
/// let id = CollectiveId::new();
/// println!("Created collective: {}", id);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CollectiveId(pub Uuid);

impl CollectiveId {
    /// Creates a new CollectiveId with a UUID v7 (time-ordered).
    #[inline]
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Creates a nil (all zeros) CollectiveId.
    /// Useful for testing or sentinel values.
    #[inline]
    pub fn nil() -> Self {
        Self(Uuid::nil())
    }

    /// Returns the raw UUID bytes for storage.
    #[inline]
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }

    /// Creates a CollectiveId from raw bytes.
    #[inline]
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }
}

impl Default for CollectiveId {
    /// Returns a nil (all zeros) CollectiveId.
    ///
    /// For a new unique ID, use [`CollectiveId::new()`].
    fn default() -> Self {
        Self::nil()
    }
}

impl fmt::Display for CollectiveId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Experience identifier (UUID v7 for time-ordering).
///
/// Experiences are the core unit of learned knowledge in PulseDB.
/// Each experience belongs to exactly one collective.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExperienceId(pub Uuid);

impl ExperienceId {
    /// Creates a new ExperienceId with a UUID v7 (time-ordered).
    #[inline]
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Creates a nil (all zeros) ExperienceId.
    #[inline]
    pub fn nil() -> Self {
        Self(Uuid::nil())
    }

    /// Returns the raw UUID bytes for storage.
    #[inline]
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }

    /// Creates an ExperienceId from raw bytes.
    #[inline]
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }
}

impl Default for ExperienceId {
    /// Returns a nil (all zeros) ExperienceId.
    ///
    /// For a new unique ID, use [`ExperienceId::new()`].
    fn default() -> Self {
        Self::nil()
    }
}

impl fmt::Display for ExperienceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Unique identifier for a PulseDB database instance.
///
/// Each database mints one stable instance id on first open and stores it in
/// metadata. Temporal decay uses this id as the G-counter key for local
/// reinforcement counts; the sync protocol also uses it to identify peers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct InstanceId(pub Uuid);

impl InstanceId {
    /// Creates a new InstanceId with a UUID v7 (time-ordered).
    #[inline]
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Creates a nil (all zeros) InstanceId.
    ///
    /// Useful for tests and explicitly reserved sentinel values.
    #[inline]
    pub fn nil() -> Self {
        Self(Uuid::nil())
    }

    /// Returns the raw UUID bytes for storage.
    #[inline]
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }

    /// Creates an InstanceId from raw bytes.
    #[inline]
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }
}

impl Default for InstanceId {
    /// Returns a nil (all zeros) InstanceId.
    ///
    /// For a new unique ID, use [`InstanceId::new()`].
    fn default() -> Self {
        Self::nil()
    }
}

impl fmt::Display for InstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Unix timestamp in milliseconds.
///
/// Using i64 allows representing dates far into the future and past.
/// Millisecond precision is sufficient for agent operations.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Timestamp(pub i64);

impl Timestamp {
    /// Creates a timestamp for the current moment.
    ///
    /// If the system clock is before the Unix epoch (should never happen
    /// in practice), returns a timestamp of 0 (epoch) rather than panicking.
    #[inline]
    pub fn now() -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        Self(duration.as_millis() as i64)
    }

    /// Creates a timestamp from Unix milliseconds.
    #[inline]
    pub const fn from_millis(millis: i64) -> Self {
        Self(millis)
    }

    /// Returns the timestamp as Unix milliseconds.
    #[inline]
    pub const fn as_millis(&self) -> i64 {
        self.0
    }

    /// Returns big-endian bytes for storage (enables lexicographic ordering).
    #[inline]
    pub fn to_be_bytes(&self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Relation identifier (UUID v7 for time-ordering).
///
/// Relations connect two experiences within the same collective,
/// enabling agents to understand how knowledge connects.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RelationId(pub Uuid);

impl RelationId {
    /// Creates a new RelationId with a UUID v7 (time-ordered).
    #[inline]
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Creates a nil (all zeros) RelationId.
    #[inline]
    pub fn nil() -> Self {
        Self(Uuid::nil())
    }

    /// Returns the raw UUID bytes for storage.
    #[inline]
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }

    /// Creates a RelationId from raw bytes.
    #[inline]
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }
}

impl Default for RelationId {
    /// Returns a nil (all zeros) RelationId.
    ///
    /// For a new unique ID, use [`RelationId::new()`].
    fn default() -> Self {
        Self::nil()
    }
}

impl fmt::Display for RelationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Insight identifier (UUID v7 for time-ordering).
///
/// Insights are derived knowledge synthesized from multiple experiences.
/// Each insight belongs to exactly one collective.
///
/// # Example
/// ```
/// use pulsedb::InsightId;
///
/// let id = InsightId::new();
/// println!("Created insight: {}", id);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InsightId(pub Uuid);

impl InsightId {
    /// Creates a new InsightId with a UUID v7 (time-ordered).
    #[inline]
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Creates a nil (all zeros) InsightId.
    #[inline]
    pub fn nil() -> Self {
        Self(Uuid::nil())
    }

    /// Returns the raw UUID bytes for storage.
    #[inline]
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }

    /// Creates an InsightId from raw bytes.
    #[inline]
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }
}

impl Default for InsightId {
    /// Returns a nil (all zeros) InsightId.
    ///
    /// For a new unique ID, use [`InsightId::new()`].
    fn default() -> Self {
        Self::nil()
    }
}

impl fmt::Display for InsightId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Opaque user identifier.
///
/// PulseDB doesn't handle authentication - the consumer provides user IDs.
/// This allows integration with any auth system (OAuth, API keys, etc.).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UserId(pub String);

impl UserId {
    /// Creates a new UserId from a string.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Returns the user ID as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for UserId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Agent identifier.
///
/// Identifies a specific AI agent instance within a collective.
/// Multiple agents can operate on the same collective simultaneously.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AgentId(pub String);

impl AgentId {
    /// Creates a new AgentId from a string.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Returns the agent ID as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AgentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Task identifier.
///
/// Identifies a specific task or job that an agent is working on.
/// Used for tracking which experiences came from which task.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskId(pub String);

impl TaskId {
    /// Creates a new TaskId from a string.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Returns the task ID as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Embedding vector type alias.
///
/// Embeddings are f32 vectors of fixed dimension (typically 384 or 768).
pub type Embedding = Vec<f32>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collective_id_new_is_unique() {
        let id1 = CollectiveId::new();
        let id2 = CollectiveId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_collective_id_nil() {
        let id = CollectiveId::nil();
        assert_eq!(id.0, Uuid::nil());
    }

    #[test]
    fn test_collective_id_bytes_roundtrip() {
        let id = CollectiveId::new();
        let bytes = *id.as_bytes();
        let restored = CollectiveId::from_bytes(bytes);
        assert_eq!(id, restored);
    }

    #[test]
    fn test_collective_id_serialization() {
        let id = CollectiveId::new();
        let bytes = bincode::serialize(&id).unwrap();
        let restored: CollectiveId = bincode::deserialize(&bytes).unwrap();
        assert_eq!(id, restored);
    }

    #[test]
    fn test_experience_id_new_is_unique() {
        let id1 = ExperienceId::new();
        let id2 = ExperienceId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_experience_id_serialization() {
        let id = ExperienceId::new();
        let bytes = bincode::serialize(&id).unwrap();
        let restored: ExperienceId = bincode::deserialize(&bytes).unwrap();
        assert_eq!(id, restored);
    }

    #[test]
    fn test_relation_id_new_is_unique() {
        let id1 = RelationId::new();
        let id2 = RelationId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_relation_id_nil() {
        let id = RelationId::nil();
        assert_eq!(id.0, Uuid::nil());
    }

    #[test]
    fn test_relation_id_bytes_roundtrip() {
        let id = RelationId::new();
        let bytes = *id.as_bytes();
        let restored = RelationId::from_bytes(bytes);
        assert_eq!(id, restored);
    }

    #[test]
    fn test_relation_id_serialization() {
        let id = RelationId::new();
        let bytes = bincode::serialize(&id).unwrap();
        let restored: RelationId = bincode::deserialize(&bytes).unwrap();
        assert_eq!(id, restored);
    }

    #[test]
    fn test_insight_id_new_is_unique() {
        let id1 = InsightId::new();
        let id2 = InsightId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_insight_id_nil() {
        let id = InsightId::nil();
        assert_eq!(id.0, Uuid::nil());
    }

    #[test]
    fn test_insight_id_bytes_roundtrip() {
        let id = InsightId::new();
        let bytes = *id.as_bytes();
        let restored = InsightId::from_bytes(bytes);
        assert_eq!(id, restored);
    }

    #[test]
    fn test_insight_id_serialization() {
        let id = InsightId::new();
        let bytes = bincode::serialize(&id).unwrap();
        let restored: InsightId = bincode::deserialize(&bytes).unwrap();
        assert_eq!(id, restored);
    }

    #[test]
    fn test_timestamp_now() {
        let t1 = Timestamp::now();
        std::thread::sleep(std::time::Duration::from_millis(1));
        let t2 = Timestamp::now();
        assert!(t1 < t2, "Timestamps should be ordered");
    }

    #[test]
    fn test_timestamp_ordering() {
        let t1 = Timestamp::from_millis(1000);
        let t2 = Timestamp::from_millis(2000);
        assert!(t1 < t2);
    }

    #[test]
    fn test_timestamp_be_bytes() {
        // Big-endian ensures lexicographic ordering matches numeric ordering
        let t1 = Timestamp::from_millis(100);
        let t2 = Timestamp::from_millis(200);
        assert!(t1.to_be_bytes() < t2.to_be_bytes());
    }

    #[test]
    fn test_user_id() {
        let id = UserId::new("user-123");
        assert_eq!(id.as_str(), "user-123");
        assert_eq!(format!("{}", id), "user-123");
    }

    #[test]
    fn test_agent_id() {
        let id = AgentId::new("claude-opus");
        assert_eq!(id.as_str(), "claude-opus");
    }

    #[test]
    fn test_task_id() {
        let id = TaskId::new("task-456");
        assert_eq!(id.as_str(), "task-456");
    }
}
