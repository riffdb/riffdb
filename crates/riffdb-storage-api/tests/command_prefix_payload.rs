#![forbid(unsafe_code)]
//! Nested capsule payload codec only; no restore or publication authority.
// req: REP-007

use riffdb_storage_api::{
    AuthoritativeMutationV3 as M, AuthoritativeNamespaceV1 as N, ChangelogV3Error as E,
    CommandPrefixEvidenceV1 as Prefix, MAX_CHANGELOG_FRAME_ENTRIES, MAX_STAGED_WRITE_BYTES,
};
use riffdb_types::{AdministrationSequence, CommitSequence, DualFrontier};

fn prefix(mutations: Vec<M>) -> Prefix {
    Prefix::new(
        DualFrontier::INITIAL,
        DualFrontier::new(CommitSequence::new(1), AdministrationSequence::new(2)),
        mutations,
    )
    .unwrap()
}

fn sample() -> Prefix {
    prefix(vec![
        M::put(N::Entities, b"a", None, b"value").unwrap(),
        M::delete_matching(N::Entities, b"b", b"old").unwrap(),
        M::replace(N::IndexEpochs, b"c", b"old-epoch", b"new-epoch").unwrap(),
    ])
}

#[test]
fn nested_payload_round_trip_preserves_exact_mutations_and_explicit_frontiers() {
    for value in [prefix(vec![]), sample()] {
        let bytes = value.encode_capsule_payload().unwrap();
        assert_eq!(&bytes[..18], &DualFrontier::INITIAL.to_canonical_bytes());
        assert_eq!(&bytes[18..36], &value.covered().to_canonical_bytes());
        assert_eq!(
            u32::from_be_bytes(bytes[36..40].try_into().unwrap()) as usize,
            value.mutations().len()
        );
        assert_eq!(bytes.len(), value.semantic_bytes());
        let decoded = Prefix::decode_capsule_payload(&bytes).unwrap();
        assert_eq!(decoded, value);
        assert_eq!(decoded.encode_capsule_payload().unwrap(), bytes);
    }
}

#[test]
fn every_truncation_and_trailing_data_refuse() {
    let bytes = sample().encode_capsule_payload().unwrap();
    for length in 0..bytes.len() {
        assert!(
            Prefix::decode_capsule_payload(&bytes[..length]).is_err(),
            "length {length}"
        );
    }
    let mut trailing = bytes;
    trailing.push(0);
    assert_eq!(
        Prefix::decode_capsule_payload(&trailing),
        Err(E::InvalidEncoding)
    );
}

#[test]
fn forged_counts_tags_absence_and_lengths_refuse_before_yielding_evidence() {
    let bytes = sample().encode_capsule_payload().unwrap();
    let mut corruptions = Vec::new();
    for (offset, value) in [(0, 2), (1, 1), (18, 0), (42, 2), (43, 2), (52, 1)] {
        let mut altered = bytes.clone();
        altered[offset] = value;
        corruptions.push(altered);
    }
    for offset in [36, 44, 48] {
        let mut altered = bytes.clone();
        altered[offset..offset + 4].copy_from_slice(&u32::MAX.to_be_bytes());
        corruptions.push(altered);
    }
    for namespace in [
        0_u16,
        N::Commits.tag(),
        N::ReplicationSourceHolds.tag(),
        u16::MAX,
    ] {
        let mut altered = bytes.clone();
        altered[40..42].copy_from_slice(&namespace.to_be_bytes());
        corruptions.push(altered);
    }
    for altered in corruptions {
        assert!(Prefix::decode_capsule_payload(&altered).is_err());
    }
    let mut count = bytes;
    count[36..40].copy_from_slice(
        &u32::try_from(MAX_CHANGELOG_FRAME_ENTRIES + 1)
            .unwrap()
            .to_be_bytes(),
    );
    assert_eq!(
        Prefix::decode_capsule_payload(&count),
        Err(E::LimitExceeded)
    );
}

#[test]
fn payload_ceiling_is_exact_and_does_not_claim_outer_envelope_capacity() {
    let bytes = prefix(vec![
        M::put(
            N::Entities,
            b"a",
            None,
            &vec![0x55; MAX_STAGED_WRITE_BYTES - 40 - 44 - 1],
        )
        .unwrap(),
    ])
    .encode_capsule_payload()
    .unwrap();
    assert_eq!(bytes.len(), MAX_STAGED_WRITE_BYTES);
    assert_eq!(
        Prefix::decode_capsule_payload(&bytes)
            .unwrap()
            .encode_capsule_payload()
            .unwrap(),
        bytes
    );
    let mut too_large = bytes;
    too_large.push(0);
    assert_eq!(
        Prefix::decode_capsule_payload(&too_large),
        Err(E::LimitExceeded)
    );
}

#[test]
fn duplicate_and_reordered_wire_mutations_are_not_normalized() {
    let value = sample();
    let bytes = value.encode_capsule_payload().unwrap();
    let first_end = 40 + value.mutations()[0].encoded_len();
    let second_end = first_end + value.mutations()[1].encoded_len();
    let mut duplicate = bytes[..40].to_vec();
    duplicate[36..40].copy_from_slice(&2_u32.to_be_bytes());
    duplicate.extend_from_slice(&bytes[40..first_end]);
    duplicate.extend_from_slice(&bytes[40..first_end]);
    assert_eq!(
        Prefix::decode_capsule_payload(&duplicate),
        Err(E::InvalidEncoding)
    );
    let mut reordered = bytes[..40].to_vec();
    reordered.extend_from_slice(&bytes[first_end..second_end]);
    reordered.extend_from_slice(&bytes[40..first_end]);
    reordered.extend_from_slice(&bytes[second_end..]);
    assert_eq!(
        Prefix::decode_capsule_payload(&reordered),
        Err(E::InvalidEncoding)
    );
}
