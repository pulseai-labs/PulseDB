//! Sync progress: the callback consumers see, and the cursor-advance policy
//! both directions share.
//!
//! Consumers implement [`SyncProgressCallback`] to receive progress updates
//! during [`SyncManager::initial_sync()`](super::manager::SyncManager::initial_sync),
//! typically for driving a loading bar in the UI.
//!
//! [`next_progress`] is the other half: one rule, used by the pull side and the
//! push side alike, for how far a cursor may move after an exchange.

/// Callback for reporting sync progress during initial catchup.
///
/// Implement this trait to receive updates as batches of changes are
/// pulled and applied during initial sync.
///
/// # Example
///
/// ```rust
/// use pulsedb::sync::progress::SyncProgressCallback;
///
/// struct ProgressBar;
///
/// impl SyncProgressCallback for ProgressBar {
///     fn on_progress(&self, batch_complete: usize, total_pulled: usize, has_more: bool) {
///         println!("Pulled {} changes (batch of {}), more: {}", total_pulled, batch_complete, has_more);
///     }
/// }
/// ```
pub trait SyncProgressCallback: Send {
    /// Called after each batch of changes is pulled and applied.
    ///
    /// # Arguments
    ///
    /// * `batch_complete` — Number of changes in the batch just applied
    /// * `total_pulled` — Cumulative count of all changes pulled so far
    /// * `has_more` — Whether the remote has more changes to send
    fn on_progress(&self, batch_complete: usize, total_pulled: usize, has_more: bool);
}

/// How far a cursor may advance after one exchange.
///
/// The single rule both directions obey, so "how far did we get?" cannot be
/// answered two different ways on the two legs:
///
/// - `prior` — the position already persisted.
/// - `scanned` — how far the producer actually **scanned**: the last event read
///   before the first eligible change it could not include. Filtered events and
///   events that no longer resolve to an entity advance it; an omitted eligible
///   change does not, and a database error is propagated rather than being
///   classified as an intentional skip.
/// - `failed` — how many changes in the batch FAILED to apply (never the
///   idempotent skips, which are ordinary successful re-sync outcomes).
/// - `safe` — the applier's actual-success `safe_through`: the highest sequence
///   at or below which everything was applied or idempotently skipped.
///
/// With nothing failed, every emitted change succeeded or was genuinely
/// idempotent, so the cursor may take the producer's scan position — which is
/// how an entirely filtered page becomes progress instead of a stall (#90).
/// With anything failed it may take only the actual-success position, or stay
/// where it was: never the filtered tail, and never `failure_sequence - 1`,
/// which would invent progress over a change that was never applied.
///
/// It never retreats: a cursor that has already moved past `safe` stays where
/// it is.
pub(crate) fn next_progress(prior: u64, scanned: u64, failed: usize, safe: Option<u64>) -> u64 {
    if failed == 0 {
        prior.max(scanned)
    } else {
        prior.max(safe.unwrap_or(prior))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The policy, pinned by case rather than by prose.
    #[test]
    fn recovery_v5_next_progress_policy() {
        // A page that emitted nothing because everything was filtered is still
        // progress: the producer scanned to 20 and nothing failed.
        assert_eq!(next_progress(10, 20, 0, None), 20);
        // A failure caps the advance at the actual-success position — never the
        // filtered tail beyond it.
        assert_eq!(next_progress(10, 20, 1, Some(14)), 14);
        // A failure with no justified success leaves the cursor alone.
        assert_eq!(next_progress(10, 20, 1, None), 10);
        // And it never retreats.
        assert_eq!(next_progress(14, 20, 1, Some(12)), 14);
    }

    /// The no-failure arm is a max, not an assignment: a responder that hands
    /// back a position below where this side already is cannot rewind it.
    #[test]
    fn recovery_v5_next_progress_never_retreats_on_success() {
        assert_eq!(next_progress(30, 20, 0, None), 30);
        assert_eq!(next_progress(0, 0, 0, None), 0);
    }
}
