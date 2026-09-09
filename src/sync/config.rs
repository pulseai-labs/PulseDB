//! Sync configuration types.
//!
//! [`SyncConfig`] controls the behavior of the sync protocol including
//! direction, conflict resolution, batch sizes, and retry policies.

use serde::{Deserialize, Serialize};

use crate::error::ValidationError;
use crate::types::CollectiveId;

use super::wire::MIN_CONTROL_FRAME_BYTES;

/// Default for [`SyncConfig::max_request_bytes`]: 64 MiB.
///
/// The documented **instance sync-body policy**: the largest request body this
/// instance accepts, and the largest it will build. It is generous enough that
/// ordinary batches never approach it and tight enough to refuse a runaway or
/// hostile body long before it can exhaust memory.
///
/// # It is a byte budget, not a batch-size arithmetic problem
///
/// Before 0.8.0 `SyncConfig::validate` floored this at `batch_size` x an
/// estimated per-experience wire size, on the reasoning that a `batch_size` the
/// cap could not cover would build bodies the peer refuses forever. The
/// estimate was necessary but never sufficient — `applications` sat outside it
/// — and the loop it guarded against is now closed at its source: both packers
/// size the **complete candidate frame** exactly ([`wire::encoded_len`]) and
/// send the longest ordered prefix that fits, so a batch is bounded by bytes
/// before it is built. The floor is gone, and with it the claim that a
/// validating configuration produces a fitting batch.
///
/// [`SyncConfig::batch_size`] remains a ceiling on a batch's **count**. It says
/// nothing about encoded bytes and never did.
///
/// What survives, and is genuinely unfixable by packing, is a *single* change
/// that cannot fit a body on its own. That is reported as the typed
/// [`SyncError::ChangeTooLarge`](super::error::SyncError::ChangeTooLarge) with
/// its cursor unadvanced, and the background loop stops retrying it.
///
/// [`wire::encoded_len`]: super::wire::encoded_len
pub const DEFAULT_MAX_REQUEST_BYTES: usize = 64 * 1024 * 1024;

/// Default for [`SyncConfig::max_clock_skew_ms`]: 300 000 ms (5 minutes).
///
/// Wide enough that ordinary NTP drift between peers never trips it, tight
/// enough that a runaway reinforcement timestamp is surfaced. Locked by the r1
/// grill (Q4).
pub const DEFAULT_MAX_CLOCK_SKEW_MS: u64 = 300_000;

// ============================================================================
// SyncDirection
// ============================================================================

/// Direction of sync data flow.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncDirection {
    /// Only push local changes to the remote peer.
    PushOnly,
    /// Only pull remote changes to the local instance.
    PullOnly,
    /// Both push and pull (full bidirectional sync).
    #[default]
    Bidirectional,
}

// ============================================================================
// ConflictResolution
// ============================================================================

/// Strategy for resolving conflicts when the same entity is modified
/// on both peers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictResolution {
    /// Remote (server) changes always win on conflict.
    #[default]
    ServerWins,
    /// The change with the latest timestamp wins.
    LastWriteWins,
}

// ============================================================================
// RetryConfig
// ============================================================================

/// Configuration for retry behavior on transient sync failures.
///
/// Uses exponential backoff with a configurable multiplier and cap.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Maximum number of consecutive retries before giving up.
    pub max_retries: u32,

    /// Initial backoff duration in milliseconds.
    pub initial_backoff_ms: u64,

    /// Maximum backoff duration in milliseconds (cap).
    pub max_backoff_ms: u64,

    /// Multiplier applied to backoff after each retry.
    pub backoff_multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 5,
            initial_backoff_ms: 500,
            max_backoff_ms: 30_000,
            backoff_multiplier: 2.0,
        }
    }
}

// ============================================================================
// SyncConfig
// ============================================================================

/// Configuration for the sync protocol.
///
/// Controls direction, conflict resolution, batch sizes, polling intervals,
/// and which collectives to sync.
///
/// # Example
/// ```
/// use pulsedb::sync::config::{SyncConfig, SyncDirection};
///
/// let config = SyncConfig {
///     direction: SyncDirection::PushOnly,
///     batch_size: 200,
///     ..Default::default()
/// };
/// assert!(config.validate().is_ok());
/// ```
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SyncConfig {
    /// Direction of sync data flow.
    pub direction: SyncDirection,

    /// Strategy for resolving conflicts.
    pub conflict_resolution: ConflictResolution,

    /// Maximum number of changes per sync batch — a ceiling on the batch's
    /// **count**, not a claim about its encoded size.
    ///
    /// Larger batches reduce round trips but increase memory usage.
    /// Default: 250.
    ///
    /// Bytes are a separate, exact constraint:
    /// [`max_request_bytes`](Self::max_request_bytes) is enforced by sizing the
    /// complete candidate frame and sending the longest ordered prefix that
    /// fits. Whichever bound bites first truncates the batch, and the scan
    /// position stops before the first change that was left out — so a
    /// count-truncated or byte-truncated batch never acknowledges an event it
    /// did not send.
    ///
    /// The default came down from 500 in 0.8.0 alongside the removal of the
    /// estimated byte floor that used to constrain the pair; 250 is a
    /// round-trip/memory trade-off now, not an arithmetic requirement.
    pub batch_size: usize,

    /// Interval between push cycles in milliseconds.
    ///
    /// Default: 1000 (1 second)
    pub push_interval_ms: u64,

    /// Interval between pull cycles in milliseconds.
    ///
    /// Default: 1000 (1 second)
    pub pull_interval_ms: u64,

    /// Retry configuration for transient failures.
    pub retry: RetryConfig,

    /// Optional filter: only sync these collectives.
    ///
    /// `None` means sync all collectives.
    pub collectives: Option<Vec<CollectiveId>>,

    /// Whether to sync experience relations.
    ///
    /// Default: true
    pub sync_relations: bool,

    /// Whether to sync derived insights.
    ///
    /// Default: true
    pub sync_insights: bool,

    /// Upper bound, in bytes, on a single sync body — this instance's
    /// **sync-body policy**, applied to what it accepts and to what it builds.
    ///
    /// On the receiving side every byte-level handler
    /// (`SyncServer::handle_*_bytes`) compares `bytes.len()` against it
    /// **before** the frame header is read and before any postcard decode, so
    /// an oversized body is refused with the typed
    /// [`SyncError::PayloadTooLarge`](super::error::SyncError::PayloadTooLarge)
    /// and never reaches the decoder. PulseDB builds no router of its own, so
    /// this is the only cap it enforces at the network edge (ADR-009); a
    /// consumer's framework body limit applies in addition, not instead.
    ///
    /// On the sending side it is one half of the **effective** cap. A push
    /// packs against `min(this, the peer's advertised inbound limit)` — the
    /// handshake carries the peer's own value — and a pull reply against
    /// `min(the requester's stated reply limit, this)`. The requester's stated
    /// limit is itself `min(this, the transport's actual receive limit)`, never
    /// a guessed configuration value.
    ///
    /// Must be at least
    /// [`MIN_CONTROL_FRAME_BYTES`](super::wire::MIN_CONTROL_FRAME_BYTES)
    /// (1 KiB): below that a peer could not exchange its own bounded control
    /// traffic, and no retry repairs that. Default: 64 MiB
    /// ([`DEFAULT_MAX_REQUEST_BYTES`]).
    ///
    /// **What it bounds.** Encoded request and reply bodies, and the
    /// accumulation of a bounded response read. **Not** every decoded object,
    /// not WAL or payload allocations behind the decode, and not the process's
    /// total memory.
    ///
    /// Absent from a deserialized config in a **self-describing** format
    /// (JSON/TOML/YAML — a persisted 0.7.x one), it falls back to the default
    /// rather than failing the load. A **postcard**-encoded 0.7.x config does
    /// NOT load: postcard writes a struct as a fixed-length sequence carrying
    /// no field names, so the buffer ends before this field and the
    /// deserializer hits end-of-input — there is no missing *field* for a
    /// serde default to fill, only absent *bytes*. Re-encode such a config from
    /// a self-describing form, or from `SyncConfig::default()`.
    #[serde(default = "default_max_request_bytes")]
    pub max_request_bytes: usize,

    /// Clock-skew allowance, in milliseconds, for an incoming experience's
    /// `last_reinforced` (#13).
    ///
    /// When a pulled or pushed reinforcement carries
    /// `last_reinforced > now + max_clock_skew_ms`, the applier counts it in
    /// [`SyncStats::skewed_timestamps`](super::types::SyncStats::skewed_timestamps)
    /// and logs the batch once at `warn` (peer, count, largest skew observed) —
    /// once per batch, not once per change: the condition never self-clears
    /// while the peer's clock is wrong.
    /// The value is still merged exactly as FR-031's max-merge dictates — it is
    /// **never** clamped, rejected or re-timestamped — so two peers keep
    /// converging on the same bytes.
    ///
    /// **The bound is advisory.** A bound that also corrects the value needs a
    /// record-carried time reference to converge on. Protocol v5 deliberately
    /// did **not** add one — that work was left out of this repair rather than
    /// pulled into it, and is assigned to a later protocol version. Until then
    /// this setting only decides what is *surfaced*, never what is *stored*.
    ///
    /// Default: 300 000 (5 minutes, [`DEFAULT_MAX_CLOCK_SKEW_MS`]).
    ///
    /// Absent from a deserialized config in a **self-describing** format
    /// (JSON/TOML/YAML — a persisted 0.7.x one), it falls back to the default
    /// rather than failing the load. A **postcard**-encoded 0.7.x config does
    /// NOT load: postcard writes a struct as a fixed-length sequence carrying
    /// no field names, so the buffer ends before this field and the
    /// deserializer hits end-of-input — there is no missing *field* for a
    /// serde default to fill, only absent *bytes*. Re-encode such a config from
    /// a self-describing form, or from `SyncConfig::default()`.
    #[serde(default = "default_max_clock_skew_ms")]
    pub max_clock_skew_ms: u64,
}

/// `serde` fallback for [`SyncConfig::max_request_bytes`].
///
/// The field was added in 0.8.0. Without this a persisted 0.7.x config in a
/// **self-describing** format (JSON, TOML, YAML) fails to load with
/// ``missing field `max_request_bytes` `` — a hard startup failure, not a
/// fallback.
///
/// **It rescues self-describing formats only.** A **postcard**-encoded 0.7.x
/// `SyncConfig` still fails to load, and no `#[serde(default)]` can change
/// that: postcard writes a struct as a fixed-length sequence carrying no field
/// names, so a 0.7.x buffer simply ends after `sync_insights` and the
/// deserializer hits end-of-input — there is no missing *field* for a default
/// to fill, only absent *bytes*. postcard is a supported representation of this
/// type (see `test_sync_config_postcard_roundtrip`), so a consumer that
/// persisted a `SyncConfig` that way must re-encode it from a self-describing
/// form, or from `SyncConfig::default()`, after upgrading.
/// A versioned or custom deserializer for that case is deferred to a tracked
/// issue.
fn default_max_request_bytes() -> usize {
    DEFAULT_MAX_REQUEST_BYTES
}

/// `serde` fallback for [`SyncConfig::max_clock_skew_ms`]; see
/// [`default_max_request_bytes`].
fn default_max_clock_skew_ms() -> u64 {
    DEFAULT_MAX_CLOCK_SKEW_MS
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            direction: SyncDirection::default(),
            conflict_resolution: ConflictResolution::default(),
            batch_size: 250,
            push_interval_ms: 1000,
            pull_interval_ms: 1000,
            retry: RetryConfig::default(),
            collectives: None,
            sync_relations: true,
            sync_insights: true,
            max_request_bytes: DEFAULT_MAX_REQUEST_BYTES,
            max_clock_skew_ms: DEFAULT_MAX_CLOCK_SKEW_MS,
        }
    }
}

impl SyncConfig {
    /// Validates the sync configuration.
    ///
    /// # Errors
    /// Returns `ValidationError` if:
    /// - `batch_size` is 0
    /// - `push_interval_ms` is 0
    /// - `pull_interval_ms` is 0
    /// - `max_request_bytes` is below
    ///   [`MIN_CONTROL_FRAME_BYTES`](super::wire::MIN_CONTROL_FRAME_BYTES)
    ///
    /// # What this does not check
    ///
    /// It makes **no claim that a batch will fit**, and no longer pretends to.
    /// The pre-0.8.0 floor (`max_request_bytes >= batch_size` x an estimated
    /// per-experience wire size) is gone: bytes are now enforced exactly, at
    /// pack time, against the complete candidate frame, so a count and a cap
    /// are independent settings rather than a pair that has to be argued about.
    ///
    /// The residual is a *single* change too large for any body, which no
    /// batching strategy can send. That surfaces at pack time as
    /// [`SyncError::ChangeTooLarge`](super::error::SyncError::ChangeTooLarge),
    /// with its cursor unadvanced and automatic retry stopped.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.batch_size == 0 {
            return Err(ValidationError::invalid_field(
                "batch_size",
                "must be greater than 0",
            ));
        }
        if self.push_interval_ms == 0 {
            return Err(ValidationError::invalid_field(
                "push_interval_ms",
                "must be greater than 0",
            ));
        }
        if self.pull_interval_ms == 0 {
            return Err(ValidationError::invalid_field(
                "pull_interval_ms",
                "must be greater than 0",
            ));
        }
        // A peer that cannot carry its own bounded control traffic — a
        // maximum-sized handshake, an acknowledgement, a rejection, an empty
        // pull page — is not slow, it is broken, and no retry repairs it.
        if self.max_request_bytes < MIN_CONTROL_FRAME_BYTES {
            return Err(ValidationError::invalid_field(
                "max_request_bytes",
                format!(
                    "must be at least MIN_CONTROL_FRAME_BYTES ({MIN_CONTROL_FRAME_BYTES} bytes), \
                     the budget the largest bounded control frame needs"
                ),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_config_defaults() {
        let config = SyncConfig::default();
        assert_eq!(config.direction, SyncDirection::Bidirectional);
        assert_eq!(config.conflict_resolution, ConflictResolution::ServerWins);
        assert_eq!(config.batch_size, 250);
        assert_eq!(config.push_interval_ms, 1000);
        assert_eq!(config.pull_interval_ms, 1000);
        assert!(config.collectives.is_none());
        assert!(config.sync_relations);
        assert!(config.sync_insights);
        assert_eq!(config.max_request_bytes, 64 * 1024 * 1024);
        assert_eq!(config.max_request_bytes, DEFAULT_MAX_REQUEST_BYTES);
        assert_eq!(config.max_clock_skew_ms, 300_000);
        assert_eq!(config.max_clock_skew_ms, DEFAULT_MAX_CLOCK_SKEW_MS);
    }

    #[test]
    fn test_sync_config_validate_success() {
        let config = SyncConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_sync_config_validate_zero_max_request_bytes() {
        let config = SyncConfig {
            max_request_bytes: 0,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(
            matches!(err, ValidationError::InvalidField { field, .. } if field == "max_request_bytes")
        );
    }

    #[test]
    fn test_sync_config_validate_zero_batch_size() {
        let config = SyncConfig {
            batch_size: 0,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(
            matches!(err, ValidationError::InvalidField { field, .. } if field == "batch_size")
        );
    }

    #[test]
    fn test_sync_config_validate_zero_push_interval() {
        let config = SyncConfig {
            push_interval_ms: 0,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(
            matches!(err, ValidationError::InvalidField { field, .. } if field == "push_interval_ms")
        );
    }

    #[test]
    fn test_sync_config_validate_zero_pull_interval() {
        let config = SyncConfig {
            pull_interval_ms: 0,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(
            matches!(err, ValidationError::InvalidField { field, .. } if field == "pull_interval_ms")
        );
    }

    /// The 1 KiB control-frame minimum is the only byte floor left, and it is
    /// exact: one byte under is refused, the minimum itself is accepted.
    #[test]
    fn recovery_v5_validate_enforces_the_control_frame_minimum() {
        let under = SyncConfig {
            max_request_bytes: MIN_CONTROL_FRAME_BYTES - 1,
            ..Default::default()
        };
        let err = under.validate().unwrap_err();
        assert!(
            matches!(err, ValidationError::InvalidField { ref field, .. } if field == "max_request_bytes"),
            "got {err}"
        );
        assert!(err.to_string().contains("MIN_CONTROL_FRAME_BYTES"), "{err}");

        let at = SyncConfig {
            max_request_bytes: MIN_CONTROL_FRAME_BYTES,
            ..Default::default()
        };
        at.validate()
            .expect("a cap exactly at the control minimum is usable");
    }

    /// The estimated `batch_size` x per-experience floor is **gone**. A large
    /// batch with a small cap now validates, because bytes are enforced exactly
    /// at pack time instead of being predicted here — an estimate that was
    /// never sufficient anyway.
    #[test]
    fn recovery_v5_validate_no_longer_imposes_an_estimated_byte_floor() {
        let config = SyncConfig {
            batch_size: 10_000,
            max_request_bytes: MIN_CONTROL_FRAME_BYTES,
            ..Default::default()
        };
        config.validate().expect(
            "batch_size is a count ceiling; the byte cap is enforced exactly by the packer",
        );

        // And the pairing the old floor refused outright — the pre-0.8.0
        // default batch of 500 against the default cap — validates.
        let old_default = SyncConfig {
            batch_size: 500,
            ..Default::default()
        };
        old_default.validate().unwrap();
    }

    #[test]
    fn test_sync_config_postcard_roundtrip() {
        let config = SyncConfig {
            direction: SyncDirection::PushOnly,
            batch_size: 100,
            collectives: Some(vec![CollectiveId::new()]),
            ..Default::default()
        };
        let bytes = postcard::to_allocvec(&config).unwrap();
        let restored: SyncConfig = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(config.direction, restored.direction);
        assert_eq!(config.batch_size, restored.batch_size);
    }

    /// A persisted 0.7.x `SyncConfig` carries neither `max_request_bytes` nor
    /// `max_clock_skew_ms`. Both were added in 0.8.0, so without
    /// `#[serde(default)]` a config stored in a **self-describing** format
    /// fails to load with ``missing field `max_request_bytes` `` — a hard
    /// startup failure rather than a fallback. (The defaults cannot rescue a
    /// postcard-encoded 0.7.x config; see [`default_max_request_bytes`].)
    #[test]
    fn test_sync_config_deserializes_a_0_7_x_payload_without_the_new_fields() {
        let legacy = r#"{
            "direction": "Bidirectional",
            "conflict_resolution": "ServerWins",
            "batch_size": 500,
            "push_interval_ms": 1000,
            "pull_interval_ms": 1000,
            "retry": {
                "max_retries": 5,
                "initial_backoff_ms": 500,
                "max_backoff_ms": 30000,
                "backoff_multiplier": 2.0
            },
            "collectives": null,
            "sync_relations": true,
            "sync_insights": true
        }"#;

        let config: SyncConfig =
            serde_json::from_str(legacy).expect("a 0.7.x config must still load");

        assert_eq!(config.max_request_bytes, DEFAULT_MAX_REQUEST_BYTES);
        assert_eq!(config.max_clock_skew_ms, DEFAULT_MAX_CLOCK_SKEW_MS);
        // The rest of the payload is preserved.
        assert_eq!(config.batch_size, 500);
        assert_eq!(config.direction, SyncDirection::Bidirectional);

        // 500 was the 0.7.x default `batch_size`, and a carried-forward config
        // still validates: `batch_size` is a count ceiling and the byte cap is
        // enforced exactly by the packer, so the two are no longer a pair that
        // has to be argued about. The interim 0.8.0 floor — which refused this
        // very configuration on an estimate — is gone.
        config
            .validate()
            .expect("a carried-forward batch_size of 500 is a count ceiling, not a byte claim");
    }

    /// An explicit value in the payload still wins over the fallback.
    #[test]
    fn test_sync_config_deserialize_honours_explicit_new_fields() {
        let payload = r#"{
            "direction": "PushOnly",
            "conflict_resolution": "LastWriteWins",
            "batch_size": 10,
            "push_interval_ms": 1000,
            "pull_interval_ms": 1000,
            "retry": {
                "max_retries": 5,
                "initial_backoff_ms": 500,
                "max_backoff_ms": 30000,
                "backoff_multiplier": 2.0
            },
            "collectives": null,
            "sync_relations": true,
            "sync_insights": true,
            "max_request_bytes": 2097152,
            "max_clock_skew_ms": 42
        }"#;

        let config: SyncConfig = serde_json::from_str(payload).unwrap();
        assert_eq!(config.max_request_bytes, 2 * 1024 * 1024);
        assert_eq!(config.max_clock_skew_ms, 42);
    }

    #[test]
    fn test_retry_config_defaults() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.initial_backoff_ms, 500);
        assert_eq!(config.max_backoff_ms, 30_000);
        assert!((config.backoff_multiplier - 2.0).abs() < f64::EPSILON);
    }
}
