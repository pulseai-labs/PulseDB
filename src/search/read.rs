//! Caller-supplied read options for time-pinned reads.
//!
//! [`ReadOptions`] carries the two call-time controls a caller can pin on a
//! time-dependent read: the `now` every energy- or recency-dependent
//! evaluation uses, and the retrieval [`ReadMode`]. The paired `*_with`
//! methods on [`PulseDB`](crate::PulseDB) take one `ReadOptions`, so the same
//! read can be replayed against the same instant instead of the wall clock.

use crate::types::Timestamp;

/// Retrieval mode for a read.
///
/// `ReadMode` is `#[non_exhaustive]`: further modes may be added without a
/// breaking change. [`ReadMode::Approximate`] is the default and selects the
/// ANN (HNSW) retrieval path. [`ReadMode::Exact`] is carried by this API but
/// is **refused** by every `*_with` method with a typed error until exact
/// retrieval lands; there is no silent fallback to approximate results.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReadMode {
    /// Approximate (ANN/HNSW) retrieval — the default.
    #[default]
    Approximate,
    /// Exact retrieval. Not implemented yet: every `*_with` read refuses it
    /// with a typed input-validation error.
    Exact,
}

/// Caller-supplied read options: one resolved time and one retrieval mode.
///
/// Built through [`ReadOptions::new`] with the chainable [`ReadOptions::at`]
/// and [`ReadOptions::exact`] methods. Fields are private so new controls can
/// be added without a breaking change (`#[non_exhaustive]`).
///
/// # Time
///
/// `now` pins every energy- or recency-dependent evaluation in the read: two
/// reads with the same `now` return the same answer whatever the wall clock
/// says. Any [`Timestamp`] is accepted, past values included. A past `now` is
/// **not** an as-of storage read — stored fields are never rewound — it only
/// freezes the scoring and staleness time (energy clamps a negative elapsed
/// time to zero). `None` reads the clock once at the call boundary, which is
/// the behaviour of the pre-`ReadOptions` methods.
///
/// # Example
///
/// ```rust
/// # fn main() -> pulsedb::Result<()> {
/// # let dir = tempfile::tempdir().unwrap();
/// # let db = pulsedb::PulseDB::open(dir.path().join("test.db"), pulsedb::Config::default())?;
/// # let collective_id = db.create_collective("example")?;
/// # let query_embedding = vec![0.1f32; 384];
/// use pulsedb::{ReadOptions, SearchFilter, SearchOptions, Timestamp};
///
/// let read = ReadOptions::new().at(Timestamp::from_millis(1_700_000_000_000));
/// let results = db.search_with(
///     collective_id,
///     &query_embedding,
///     SearchOptions {
///         k: 10,
///         filter: SearchFilter::default(),
///         weights: None,
///     },
///     &read,
/// )?;
/// # Ok(())
/// # }
/// ```
#[non_exhaustive]
#[derive(Clone, Debug, Default)]
pub struct ReadOptions {
    now: Option<Timestamp>,
    mode: ReadMode,
}

impl ReadOptions {
    /// Creates options with no pinned time and [`ReadMode::Approximate`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Pins the read's `now`.
    ///
    /// Every energy- or recency-dependent evaluation in the read uses this
    /// instant. Any [`Timestamp`] is accepted; a past instant freezes the
    /// scoring and staleness time without rewinding any stored field.
    pub fn at(mut self, now: Timestamp) -> Self {
        self.now = Some(now);
        self
    }

    /// Selects [`ReadMode::Exact`]. The read refuses it with a typed error
    /// until exact retrieval is implemented.
    pub fn exact(mut self) -> Self {
        self.mode = ReadMode::Exact;
        self
    }

    /// The pinned time, if any.
    ///
    /// `None` means "read the clock once at the call boundary".
    pub fn now(&self) -> Option<Timestamp> {
        self.now
    }

    /// The selected retrieval mode.
    pub fn mode(&self) -> ReadMode {
        self.mode
    }
}
