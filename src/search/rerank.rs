//! Weight resolution helpers for recall ranking.

use crate::config::RecallWeights;
use crate::error::Result;

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

#[cfg(test)]
mod tests {
    use super::*;

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
}
