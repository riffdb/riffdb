// req: PRJ-004, STO-012, STO-032
use super::*;
use crate::storage::v1::StoredCommandLocatorV1;

fn frames() -> (Vec<u8>, Vec<u8>) {
    let message = StoredCommandLocatorV1 { commit_sequence: 7 };
    let schema = StoredCommandLocatorV1::record_schema();
    let payload = message.encode_to_vec();
    (
        super::super::encode_v1(schema, &payload).unwrap(),
        super::super::encode(schema, &payload).unwrap(),
    )
}

#[test]
fn caller_scratch_keeps_complete_canonical_checks_without_retained_tls() {
    let (legacy, compact) = frames();
    // Existing fixture encoders validate through TLS; isolate the measured
    // caller-owned decoder from that completed setup work.
    assert!(super::super::release_locator_decode_scratch_v1());
    for encoded in [&legacy, &compact] {
        let mut scratch = Vec::with_capacity(encoded.len());
        let decoded = decode_readable_message_with_caller_scratch_v1::<StoredCommandLocatorV1>(
            encoded,
            &mut scratch,
        )
        .unwrap();
        assert_eq!(decoded.commit_sequence, 7);
        assert_eq!(
            super::super::CANONICAL_SCRATCH.with(|s| s.borrow().capacity()),
            0
        );
    }
}

#[test]
fn caller_scratch_rejects_unused_unknown_fields_and_checksum_damage() {
    let (_, mut compact) = frames();
    compact.extend_from_slice(&[0x10, 1]);
    compact[8..12].copy_from_slice(&4_u32.to_be_bytes());
    let crc =
        super::super::payload_crc32c(&compact[super::super::COMPACT_RECORD_HEADER_V2_BYTES..]);
    compact[12..16].copy_from_slice(&crc.to_be_bytes());
    let mut scratch = Vec::with_capacity(compact.len());
    assert!(matches!(
        decode_readable_message_with_caller_scratch_v1::<StoredCommandLocatorV1>(
            &compact,
            &mut scratch,
        ),
        Err(CallerScratchDecodeErrorV1::Envelope(
            EnvelopeError::InvalidPayload(super::super::PayloadValidationError::NonCanonical,)
        ))
    ));
    compact[12] ^= 1;
    assert!(matches!(
        decode_readable_message_with_caller_scratch_v1::<StoredCommandLocatorV1>(
            &compact,
            &mut scratch,
        ),
        Err(CallerScratchDecodeErrorV1::Envelope(
            EnvelopeError::ChecksumMismatch
        ))
    ));
}

#[test]
fn caller_scratch_refuses_before_decode_when_capacity_is_one_byte_short() {
    let (legacy, _) = frames();
    let mut scratch = Vec::with_capacity(legacy.len() - 1);
    let before = scratch.capacity();
    assert!(matches!(
        decode_readable_message_with_caller_scratch_v1::<StoredCommandLocatorV1>(
            &legacy,
            &mut scratch,
        ),
        Err(CallerScratchDecodeErrorV1::CapacityUnavailable)
    ));
    assert_eq!(scratch.capacity(), before);
    assert!(scratch.is_empty());
}
