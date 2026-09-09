//! Shared versioned codec for the protocol-v5 sync boundary.
//!
//! Every handshake, push and pull body — request AND reply — crosses the wire
//! as one *frame*: a fixed-layout header parsed by raw byte-slicing, followed
//! by exactly one postcard-encoded body.
//!
//! ```text
//! [ SYNC_WIRE_MAGIC[0], SYNC_WIRE_MAGIC[1], WIRE_FORMAT_VERSION, operation ] ++ <body>
//! ```
//!
//! The header answers three questions before a single byte reaches postcard:
//! are these PulseDB sync bytes at all (magic), does this peer speak the same
//! wire format (version), and is this body the message the endpoint is for
//! (operation). Protocol v4 framed only the handshake, so a v4 push or pull
//! body reached the decoder unchecked; under v5 an unframed body is refused as
//! a [`SyncError::WireFormatMismatch`] on every endpoint.
//!
//! # Sizing is exact, not estimated
//!
//! [`encoded_len`] is the *whole frame's* length — header included — computed
//! with pinned postcard's own `serialized_size` over the very value that will
//! be encoded. [`encode_bounded`] refuses a frame over the cap **before**
//! allocating, then allocates exactly the frame's length and encodes into it
//! with `to_slice`. There is no second, hand-maintained per-field size formula
//! anywhere in the sync path.
//!
//! [`FrameSizer`] extends that to the one shape the packers need: a frame whose
//! only variable-length part is a collection of items. postcard writes a
//! collection as `varint(len) ++ elements`, and every other part of the frame
//! is independent of the element count, so the exact length of a frame carrying
//! `n` items is the empty-collection frame with its `varint(0)` swapped for
//! `varint(n)` plus the elements' own sizes. That identity is property-tested
//! against real frames at the 127/128 varint boundary rather than argued.

use serde::de::DeserializeOwned;
use serde::Serialize;

use super::error::SyncError;
use super::{SYNC_WIRE_MAGIC, SYNC_WIRE_PREAMBLE_LEN, WIRE_FORMAT_VERSION};

/// Operation discriminator carried in the frame header.
///
/// It is what makes each byte-level endpoint self-describing: a well-formed
/// push frame delivered to the pull handler is refused by raw byte inspection,
/// without a prior handshake and without reaching the decoder.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum WireOperation {
    /// `HandshakeRequest` / `HandshakeResponse`.
    Handshake = 1,
    /// `PushRequest` / `WireReply<PushAck>`.
    Push = 2,
    /// `PullRequest` / `WireReply<PullPage>`.
    Pull = 3,
}

impl WireOperation {
    /// The discriminator byte written into the frame header.
    pub fn as_byte(self) -> u8 {
        self as u8
    }
}

/// Smallest body cap any peer may configure: 1 KiB.
///
/// Below this a peer could not exchange its own **control** traffic — a
/// maximum-sized handshake, a push acknowledgement, a rejection, an empty pull
/// page — so sync would fail in a way no amount of retrying repairs. Both
/// constructors enforce the effective budget against it.
///
/// The claim is certified rather than asserted:
/// `recovery_v5_bounded_control_frames_fit_the_minimum_budget` builds each
/// bounded control message at its maximum legal size and measures the real
/// frame. It is a statement about **bounded** messages only — a pull request
/// carrying a large `collectives` filter is bounded by `max_request_bytes`,
/// not by this.
pub const MIN_CONTROL_FRAME_BYTES: usize = 1024;

/// The 4-byte frame header for `operation`.
pub fn write_header(operation: WireOperation) -> [u8; SYNC_WIRE_PREAMBLE_LEN] {
    [
        SYNC_WIRE_MAGIC[0],
        SYNC_WIRE_MAGIC[1],
        WIRE_FORMAT_VERSION,
        operation.as_byte(),
    ]
}

/// Validates the frame header by raw byte-slicing and returns the body slice.
///
/// Order is deliberate and load-free: length, magic, wire version, operation.
/// Nothing here deserializes.
pub fn read_header(operation: WireOperation, framed: &[u8]) -> Result<&[u8], SyncError> {
    if framed.len() < SYNC_WIRE_PREAMBLE_LEN || framed[..SYNC_WIRE_MAGIC.len()] != SYNC_WIRE_MAGIC {
        return Err(SyncError::wire_format_bad_magic(WIRE_FORMAT_VERSION));
    }
    let got_version = framed[SYNC_WIRE_MAGIC.len()];
    if got_version != WIRE_FORMAT_VERSION {
        return Err(SyncError::wire_format_version(
            WIRE_FORMAT_VERSION,
            got_version,
        ));
    }
    let got_operation = framed[SYNC_WIRE_MAGIC.len() + 1];
    if got_operation != operation.as_byte() {
        return Err(SyncError::WireOperationMismatch {
            expected: operation.as_byte(),
            got: got_operation,
        });
    }
    Ok(&framed[SYNC_WIRE_PREAMBLE_LEN..])
}

/// The exact length, in bytes, of the frame `value` will encode to — header
/// included.
///
/// Uses pinned postcard's own `serialized_size` over the very value that will
/// be encoded, so it is a measurement of the encoder rather than a second
/// per-field formula that could drift from it. `postcard = "=1.1.3"` is pinned
/// in `Cargo.toml`, which is what makes `experimental::serialized_size` safe to
/// depend on here.
pub fn encoded_len<T: Serialize + ?Sized>(value: &T) -> Result<usize, SyncError> {
    let body = postcard::experimental::serialized_size(value).map_err(SyncError::from)?;
    body.checked_add(SYNC_WIRE_PREAMBLE_LEN).ok_or_else(|| {
        SyncError::serialization("frame length overflows the host's usize".to_string())
    })
}

/// Encodes `value` as an `operation` frame, refusing anything over `cap`
/// **before** allocating.
///
/// The buffer is exactly the frame's length: the size is known before the first
/// byte is written, so an oversized body is never built and then measured.
pub fn encode_bounded<T: Serialize + ?Sized>(
    operation: WireOperation,
    value: &T,
    cap: usize,
) -> Result<Vec<u8>, SyncError> {
    let size = encoded_len(value)?;
    if size > cap {
        return Err(SyncError::PayloadTooLarge { size, max: cap });
    }
    let mut framed = vec![0u8; size];
    framed[..SYNC_WIRE_PREAMBLE_LEN].copy_from_slice(&write_header(operation));
    let written = postcard::to_slice(value, &mut framed[SYNC_WIRE_PREAMBLE_LEN..])
        .map_err(SyncError::from)?
        .len();
    debug_assert_eq!(written + SYNC_WIRE_PREAMBLE_LEN, size);
    Ok(framed)
}

/// Decodes an `operation` frame under `cap`.
///
/// The checks run in the only order that is safe at a network edge: byte cap,
/// then header (magic, wire version, operation), then an **exact** postcard
/// decode. `postcard::from_bytes` silently ignores trailing bytes, so this uses
/// `take_from_bytes` and refuses a body that did not consume its frame — two
/// bodies concatenated, or a padded one, is not a message.
pub fn decode_bounded<T: DeserializeOwned>(
    operation: WireOperation,
    framed: &[u8],
    cap: usize,
) -> Result<T, SyncError> {
    if framed.len() > cap {
        return Err(SyncError::PayloadTooLarge {
            size: framed.len(),
            max: cap,
        });
    }
    let body = read_header(operation, framed)?;
    let (value, rest) = postcard::take_from_bytes::<T>(body).map_err(SyncError::from)?;
    if !rest.is_empty() {
        return Err(SyncError::serialization(format!(
            "{} trailing bytes after the decoded body",
            rest.len()
        )));
    }
    Ok(value)
}

/// The encoded size of one collection ITEM, for [`FrameSizer`].
///
/// Separate from [`encoded_len`] because an item carries no frame header: it is
/// measured as it will appear inside the collection, not as a frame of its own.
pub fn item_len<T: Serialize + ?Sized>(value: &T) -> Result<usize, SyncError> {
    postcard::experimental::serialized_size(value).map_err(SyncError::from)
}

/// Bytes postcard spends on a LEB128 varint for `value`.
pub fn varint_len(value: u64) -> usize {
    let mut len = 1;
    let mut remaining = value >> 7;
    while remaining != 0 {
        len += 1;
        remaining >>= 7;
    }
    len
}

/// Exact frame sizing for a frame whose only variable-length part is a
/// collection of items.
///
/// postcard writes a collection as `varint(len) ++ elements`, and every other
/// part of the frame is independent of the element count. So the frame carrying
/// `n` items is the *empty-collection* frame with its `varint(0)` (one byte)
/// replaced by `varint(n)`, plus the elements' own encoded sizes. That lets a
/// packer test each candidate against the cap in O(1) instead of re-encoding
/// the whole growing batch, while still being **exact** — the identity is
/// property-tested against real frames at the 127/128 varint boundary, not
/// argued.
///
/// The envelope must be [`encoded_len`] of the real frame with an empty
/// collection, so it already carries the header, the identities, the cursor and
/// the reply variant tag.
#[derive(Clone, Copy, Debug)]
pub struct FrameSizer {
    envelope: usize,
    items: usize,
    count: usize,
}

impl FrameSizer {
    /// Starts from the frame's empty-collection length (see [`encoded_len`]).
    pub fn new(envelope: usize) -> Self {
        Self {
            envelope,
            items: 0,
            count: 0,
        }
    }

    /// The frame length if one more item of `item_size` bytes were added.
    pub fn len_with(&self, item_size: usize) -> usize {
        Self {
            envelope: self.envelope,
            items: self.items + item_size,
            count: self.count + 1,
        }
        .len()
    }

    /// The empty-collection frame length this sizer is currently measuring
    /// against.
    ///
    /// Exposed for the one caller that has to move an envelope by a known
    /// DELTA rather than re-measure it: a pull whose scan position walks past
    /// events it will never emit widens the envelope by
    /// `varint_len(b) − varint_len(a)` and nothing else, and re-running
    /// [`encoded_len`] per skipped event would be up to a page of full frame
    /// measurements for a value the identity above already fixes.
    pub fn envelope(&self) -> usize {
        self.envelope
    }

    /// Replaces the envelope, keeping the items already added.
    ///
    /// The envelope is not constant across candidates: a pull reply carries the
    /// **scan position** that the prefix under consideration would report, and
    /// that position's varint widens as the prefix grows (127 → 128,
    /// 16 383 → 16 384, …). Sizing the envelope once, at the start, is wrong by
    /// exactly those bytes on exactly the pages that cross a boundary — which
    /// is where an off-by-one in a byte budget actually bites. So each
    /// candidate is measured against ITS OWN envelope.
    pub fn rebase(&mut self, envelope: usize) {
        self.envelope = envelope;
    }

    /// Adds an item of `item_size` encoded bytes.
    pub fn push(&mut self, item_size: usize) {
        self.items += item_size;
        self.count += 1;
    }

    /// Items added so far.
    pub fn count(&self) -> usize {
        self.count
    }

    /// The exact frame length for the items added so far.
    pub fn len(&self) -> usize {
        // `varint_len(0) == 1`, the byte the empty envelope already spent.
        self.envelope - 1 + varint_len(self.count as u64) + self.items
    }

    /// Whether no item has been added yet.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::types::{
        HandshakeRequest, HandshakeResponse, InstanceId, PullPage, PullRequest, PushAck,
        PushRequest, SyncPosition, WireErrorCode, WireReply, WireResult,
        MAX_HANDSHAKE_CAPABILITIES, MAX_HANDSHAKE_CAPABILITY_BYTES, MAX_WIRE_DETAIL_BYTES,
    };
    use crate::sync::SYNC_PROTOCOL_VERSION;

    fn id(byte: u8) -> InstanceId {
        InstanceId::from_bytes([byte; 16])
    }

    /// The largest [`HandshakeRequest`] the decoder will accept: every
    /// capability slot filled, each at the per-capability byte bound.
    fn max_handshake_request() -> HandshakeRequest {
        HandshakeRequest {
            instance_id: id(0x11),
            protocol_version: SYNC_PROTOCOL_VERSION,
            capabilities: (0..MAX_HANDSHAKE_CAPABILITIES)
                .map(|_| "c".repeat(MAX_HANDSHAKE_CAPABILITY_BYTES))
                .collect(),
        }
    }

    /// The largest [`HandshakeResponse`]: a rejection carrying a reason at the
    /// detail bound.
    fn max_handshake_response() -> HandshakeResponse {
        HandshakeResponse {
            instance_id: id(0x22),
            protocol_version: u32::MAX,
            accepted: false,
            reason: Some("r".repeat(MAX_WIRE_DETAIL_BYTES)),
            receive_limit_bytes: u64::MAX,
        }
    }

    fn max_push_reply() -> WireReply<PushAck> {
        WireReply {
            protocol_version: SYNC_PROTOCOL_VERSION,
            responder: id(0x33),
            result: WireResult::Rejected {
                code: WireErrorCode::InvalidRequest,
                detail: "d".repeat(MAX_WIRE_DETAIL_BYTES),
            },
        }
    }

    fn push_ack_reply() -> WireReply<PushAck> {
        WireReply {
            protocol_version: SYNC_PROTOCOL_VERSION,
            responder: id(0x33),
            result: WireResult::Ok(PushAck {
                wal_owner: id(0x44),
                accepted: u64::MAX,
                rejected: u64::MAX,
                total: u64::MAX,
                safe_through: Some(u64::MAX),
            }),
        }
    }

    fn empty_pull_reply() -> WireReply<PullPage> {
        WireReply {
            protocol_version: SYNC_PROTOCOL_VERSION,
            responder: id(0x55),
            result: WireResult::Ok(PullPage {
                changes: Vec::new(),
                has_more: true,
                scan_position: SyncPosition::new(id(0x55), u64::MAX),
            }),
        }
    }

    fn empty_push_request() -> PushRequest {
        PushRequest {
            protocol_version: SYNC_PROTOCOL_VERSION,
            source_instance: id(0x66),
            target_instance: id(0x77),
            reply_limit_bytes: u64::MAX,
            changes: Vec::new(),
        }
    }

    fn unfiltered_pull_request() -> PullRequest {
        PullRequest {
            protocol_version: SYNC_PROTOCOL_VERSION,
            source_instance: id(0x88),
            target_instance: id(0x99),
            cursor: SyncPosition::new(id(0x99), u64::MAX),
            batch_size: u64::MAX,
            reply_limit_bytes: u64::MAX,
            collectives: None,
        }
    }

    /// [`encoded_len`] is the length of the frame that [`encode_bounded`]
    /// actually produces — header included — not an estimate of it.
    #[test]
    fn recovery_v5_encoded_len_is_the_real_frame_length() {
        let request = max_handshake_request();
        let predicted = encoded_len(&request).unwrap();
        let framed = encode_bounded(WireOperation::Handshake, &request, usize::MAX).unwrap();
        assert_eq!(predicted, framed.len());
        assert_eq!(&framed[..SYNC_WIRE_MAGIC.len()], &SYNC_WIRE_MAGIC);
        assert_eq!(framed[2], WIRE_FORMAT_VERSION);
        assert_eq!(framed[3], WireOperation::Handshake.as_byte());
    }

    /// Round trip through the codec, on every operation.
    #[test]
    fn recovery_v5_frames_round_trip_on_each_operation() {
        let handshake = max_handshake_response();
        let framed = encode_bounded(WireOperation::Handshake, &handshake, usize::MAX).unwrap();
        let back: HandshakeResponse =
            decode_bounded(WireOperation::Handshake, &framed, usize::MAX).unwrap();
        assert_eq!(back.instance_id, handshake.instance_id);
        assert_eq!(back.receive_limit_bytes, handshake.receive_limit_bytes);

        let push = empty_push_request();
        let framed = encode_bounded(WireOperation::Push, &push, usize::MAX).unwrap();
        let back: PushRequest = decode_bounded(WireOperation::Push, &framed, usize::MAX).unwrap();
        assert_eq!(back.target_instance, push.target_instance);

        let pull = unfiltered_pull_request();
        let framed = encode_bounded(WireOperation::Pull, &pull, usize::MAX).unwrap();
        let back: PullRequest = decode_bounded(WireOperation::Pull, &framed, usize::MAX).unwrap();
        assert_eq!(back.cursor, pull.cursor);
    }

    /// A protocol-v4 body — no frame at all — is refused before postcard sees
    /// it, on a data endpoint, with no prior handshake.
    #[test]
    fn recovery_v5_unframed_v4_body_is_refused_before_decode() {
        let legacy = postcard::to_allocvec(&unfiltered_pull_request()).unwrap();
        let err =
            decode_bounded::<PullRequest>(WireOperation::Pull, &legacy, usize::MAX).unwrap_err();
        assert!(err.is_wire_format_mismatch(), "got {err}");
    }

    /// A frame that carries the v4 wire-format version byte is refused with the
    /// version it offered, not with a decode error.
    #[test]
    fn recovery_v5_v4_wire_version_is_refused_with_its_version() {
        let mut framed =
            encode_bounded(WireOperation::Pull, &unfiltered_pull_request(), usize::MAX).unwrap();
        framed[2] = 3;
        let err =
            decode_bounded::<PullRequest>(WireOperation::Pull, &framed, usize::MAX).unwrap_err();
        assert!(
            matches!(
                err,
                SyncError::WireFormatMismatch {
                    expected: WIRE_FORMAT_VERSION,
                    got: Some(3)
                }
            ),
            "got {err}"
        );
    }

    /// A well-formed frame for the WRONG operation is refused: a push body
    /// delivered to the pull endpoint never reaches the decoder.
    #[test]
    fn recovery_v5_wrong_operation_is_refused_before_decode() {
        let framed =
            encode_bounded(WireOperation::Push, &empty_push_request(), usize::MAX).unwrap();
        let err =
            decode_bounded::<PullRequest>(WireOperation::Pull, &framed, usize::MAX).unwrap_err();
        assert!(
            matches!(
                err,
                SyncError::WireOperationMismatch {
                    expected: 3,
                    got: 2
                }
            ),
            "got {err}"
        );
    }

    /// Trailing bytes after an otherwise exact body are refused. postcard's own
    /// `from_bytes` silently ignores them.
    #[test]
    fn recovery_v5_trailing_bytes_are_refused() {
        let mut framed =
            encode_bounded(WireOperation::Pull, &unfiltered_pull_request(), usize::MAX).unwrap();
        framed.push(0x00);
        let err =
            decode_bounded::<PullRequest>(WireOperation::Pull, &framed, usize::MAX).unwrap_err();
        assert!(
            matches!(err, SyncError::Serialization(ref m) if m.contains("trailing")),
            "got {err}"
        );
    }

    /// The byte cap is checked first — before the header, before postcard.
    #[test]
    fn recovery_v5_decode_refuses_an_over_cap_body_first() {
        let framed =
            encode_bounded(WireOperation::Pull, &unfiltered_pull_request(), usize::MAX).unwrap();
        let cap = framed.len() - 1;
        let err = decode_bounded::<PullRequest>(WireOperation::Pull, &framed, cap).unwrap_err();
        assert!(
            matches!(err, SyncError::PayloadTooLarge { size, max } if size == framed.len() && max == cap),
            "got {err}"
        );
        // Exactly at the cap is accepted.
        decode_bounded::<PullRequest>(WireOperation::Pull, &framed, framed.len()).unwrap();
    }

    /// Encoding refuses an over-cap frame rather than building it and finding
    /// out afterwards.
    #[test]
    fn recovery_v5_encode_refuses_an_over_cap_frame() {
        let request = unfiltered_pull_request();
        let exact = encoded_len(&request).unwrap();
        let err = encode_bounded(WireOperation::Pull, &request, exact - 1).unwrap_err();
        assert!(
            matches!(err, SyncError::PayloadTooLarge { size, max } if size == exact && max == exact - 1),
            "got {err}"
        );
        assert_eq!(
            encode_bounded(WireOperation::Pull, &request, exact)
                .unwrap()
                .len(),
            exact
        );
    }

    /// The 1 KiB control-frame minimum is a claim about the maximum-sized
    /// BOUNDED control messages, and it is certified against them here rather
    /// than asserted in a doc comment.
    #[test]
    fn recovery_v5_bounded_control_frames_fit_the_minimum_budget() {
        let sizes = [
            (
                "handshake request",
                encoded_len(&max_handshake_request()).unwrap(),
            ),
            (
                "handshake response",
                encoded_len(&max_handshake_response()).unwrap(),
            ),
            ("push rejection", encoded_len(&max_push_reply()).unwrap()),
            (
                "push acknowledgement",
                encoded_len(&push_ack_reply()).unwrap(),
            ),
            (
                "empty pull reply",
                encoded_len(&empty_pull_reply()).unwrap(),
            ),
            (
                "empty push request",
                encoded_len(&empty_push_request()).unwrap(),
            ),
            (
                "unfiltered pull request",
                encoded_len(&unfiltered_pull_request()).unwrap(),
            ),
        ];
        for (what, size) in sizes {
            assert!(
                size <= MIN_CONTROL_FRAME_BYTES,
                "{what} needs {size} bytes, over the {MIN_CONTROL_FRAME_BYTES}-byte control minimum"
            );
        }
    }

    /// [`FrameSizer`]'s identity — envelope with `varint(0)` swapped for
    /// `varint(n)`, plus the elements — must equal the real frame's length at
    /// the varint width boundary (127 fits one byte, 128 needs two).
    #[test]
    fn recovery_v5_frame_sizer_matches_the_real_frame_at_count_boundaries() {
        let ids: Vec<u64> = (0..300).collect();
        let envelope = encoded_len(&Vec::<u64>::new()).unwrap();
        for n in [0usize, 1, 126, 127, 128, 129, 255, 300] {
            let items: Vec<u64> = ids[..n].to_vec();
            let mut sizer = FrameSizer::new(envelope);
            for item in &items {
                sizer.push(postcard::experimental::serialized_size(item).unwrap());
            }
            assert_eq!(
                sizer.len(),
                encoded_len(&items).unwrap(),
                "FrameSizer disagreed with the real frame at n={n}"
            );
        }
    }

    #[test]
    fn recovery_v5_varint_len_matches_postcard() {
        for value in [0u64, 1, 126, 127, 128, 129, 16_383, 16_384, u64::MAX] {
            assert_eq!(
                varint_len(value),
                postcard::experimental::serialized_size(&value).unwrap(),
                "varint_len disagreed at {value}"
            );
        }
    }
}
