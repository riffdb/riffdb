#![forbid(unsafe_code)]
//! Physical allocation uses a distinct, canonical, bounded durable envelope.
// req: REP-003, STO-012, REC-001

use riffdb_proto::{durable::encode_current_message, storage::v1 as wire};
use riffdb_storage_api::{
    ChangelogTransactionAllocator as Allocator, ChangelogTransactionSequence as Sequence,
    proto_codec::{
        decode_application_sequence_allocator_v1,
        decode_changelog_transaction_allocator_v3 as decode,
        encode_application_sequence_allocator_v1,
        encode_changelog_transaction_allocator_v3 as encode,
    },
};

#[test]
fn physical_allocator_envelope_rejects_missing_zero_and_foreign_semantics() {
    use wire::stored_changelog_transaction_allocator_v3::State;
    for state in [None, Some(State::NextTransactionSequence(0))] {
        let encoded =
            encode_current_message(&wire::StoredChangelogTransactionAllocatorV3 { state }).unwrap();
        assert!(decode(&encoded).is_err());
    }
    let app = encode_application_sequence_allocator_v1(
        riffdb_storage_api::ApplicationSequenceAllocator::next(
            riffdb_types::CommitSequence::new(1).unwrap(),
        ),
    )
    .unwrap();
    assert!(decode(app.as_bytes()).is_err());
    let physical = encode(Allocator::initial()).unwrap();
    assert!(decode_application_sequence_allocator_v1(physical.as_bytes()).is_err());
    // The abandoned pre-integration prototype payload has never been persisted.
    assert!(decode(&[0, 3, 1, 0, 0, 0, 0, 0, 0, 0, 1]).is_err());
}

#[test]
fn every_physical_allocator_byte_is_checked_and_maximum_exhaustion_roundtrips() {
    for state in [
        Allocator::initial(),
        Allocator::Next(Sequence::new(u64::MAX).unwrap()),
        Allocator::Exhausted,
    ] {
        let encoded = encode(state).unwrap();
        let decoded = decode(encoded.as_bytes()).unwrap();
        assert_eq!(*decoded.value(), state);
        assert_eq!(
            decoded.encoded_content_charge(),
            encoded.encoded_content_charge()
        );
        assert_eq!(encode(*decoded.value()).unwrap(), encoded);
        for offset in 0..encoded.as_bytes().len() {
            let mut corrupt = encoded.as_bytes().to_vec();
            corrupt[offset] ^= 0x80;
            assert!(decode(&corrupt).is_err(), "corrupt byte {offset}");
        }
    }
    let last = Allocator::Next(Sequence::new(u64::MAX).unwrap());
    let (_, exhausted) = last.allocate_one().unwrap();
    let retained = encode(exhausted).unwrap();
    assert!(
        decode(retained.as_bytes())
            .unwrap()
            .value()
            .allocate_one()
            .is_err()
    );
}

#[test]
fn physical_allocator_complete_envelope_vectors_are_frozen() {
    for (state, fixture, frozen) in [
        (
            Allocator::initial(),
            include_str!("../../../fixtures/replication/changelog-allocator-v3-first.hex"),
            "5244423202440001000000029e1e37690801\n",
        ),
        (
            Allocator::Next(Sequence::new(u64::MAX).unwrap()),
            include_str!("../../../fixtures/replication/changelog-allocator-v3-last.hex"),
            "52444232024400010000000be390f85508ffffffffffffffffff01\n",
        ),
        (
            Allocator::Exhausted,
            include_str!("../../../fixtures/replication/changelog-allocator-v3-exhausted.hex"),
            "524442320244000100000002e9e1b6bd1200\n",
        ),
    ] {
        assert_eq!(fixture, frozen);
        let actual = encode(state)
            .unwrap()
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
            + "\n";
        assert_eq!(actual, frozen);
    }
}
