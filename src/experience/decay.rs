//! Temporal decay energy math.

use crate::config::DecayConfig;
use crate::types::Timestamp;

/// Computes closed-form experience energy from scalar inputs.
///
/// The returned value is always clamped to `[0, 1]`. Timestamps are Unix
/// milliseconds; elapsed time is converted to seconds before applying the
/// half-life decay.
#[inline]
pub fn energy(
    importance: f32,
    applications: u32,
    last_reinforced: Timestamp,
    now: Timestamp,
    cfg: &DecayConfig,
) -> f32 {
    let elapsed_ms = now
        .as_millis()
        .saturating_sub(last_reinforced.as_millis())
        .max(0);
    let elapsed_secs = elapsed_ms as f64 / 1000.0;
    let half_life_secs = cfg.half_life.as_secs_f64().max(f64::MIN_POSITIVE);
    let lambda = std::f64::consts::LN_2 / half_life_secs;
    let frequency = 1.0 + f64::from(cfg.freq_weight) * (1.0 + f64::from(applications)).ln();
    let decayed = f64::from(importance) * frequency * (-lambda * elapsed_secs).exp();

    (decayed as f32).clamp(0.0, 1.0)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::energy;
    use crate::config::DecayConfig;
    use crate::types::Timestamp;

    #[test]
    fn energy_uses_unit_frequency_boost_for_zero_applications_at_t0() {
        let cfg = DecayConfig::default();

        let value = energy(0.42, 0, Timestamp(1_000), Timestamp(1_000), &cfg);

        assert!((value - 0.42).abs() < f32::EPSILON);
    }

    proptest! {
        #[test]
        fn energy_is_always_clamped_to_unit_interval(
            importance in -4.0f32..4.0,
            applications in any::<u32>(),
            elapsed_ms in 0i64..(180 * 24 * 60 * 60 * 1000),
        ) {
            let cfg = DecayConfig::default();
            let value = energy(
                importance,
                applications,
                Timestamp(10_000),
                Timestamp(10_000 + elapsed_ms),
                &cfg,
            );

            prop_assert!((0.0..=1.0).contains(&value));
        }

        #[test]
        fn energy_monotonically_decreases_as_time_advances(
            importance in 0.0f32..=1.0,
            applications in 0u32..1_000_000,
            first_elapsed_ms in 0i64..(30 * 24 * 60 * 60 * 1000),
            extra_elapsed_ms in 0i64..(30 * 24 * 60 * 60 * 1000),
        ) {
            let cfg = DecayConfig::default();
            let last_reinforced = Timestamp(20_000);
            let first = energy(
                importance,
                applications,
                last_reinforced,
                Timestamp(last_reinforced.0 + first_elapsed_ms),
                &cfg,
            );
            let later = energy(
                importance,
                applications,
                last_reinforced,
                Timestamp(last_reinforced.0 + first_elapsed_ms + extra_elapsed_ms),
                &cfg,
            );

            prop_assert!(later <= first + f32::EPSILON);
        }

        #[test]
        fn energy_increases_after_reinforcement_below_saturation(
            importance in 0.05f32..=0.95,
            applications in 0u32..1_000_000,
            elapsed_ms in 1_000i64..(30 * 24 * 60 * 60 * 1000),
        ) {
            let cfg = DecayConfig::default();
            let old = energy(
                importance,
                applications,
                Timestamp(100_000),
                Timestamp(100_000 + elapsed_ms),
                &cfg,
            );
            let reinforced = energy(
                importance,
                applications.saturating_add(1),
                Timestamp(100_000 + elapsed_ms),
                Timestamp(100_000 + elapsed_ms),
                &cfg,
            );

            prop_assert!(reinforced >= old);
        }
    }
}
