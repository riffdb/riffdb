#![forbid(unsafe_code)]
//! Fixed roots are consistency evidence, never a caller-minted durability permit.
// req: REP-003, REC-001, STO-012

use riffdb_storage_api::{
    AuthoritativeTransactionBindingV3, AuthoritativeTransactionV3, ChangelogAttributionV3,
    ChangelogHistoryPointV3 as Point, ChangelogHistoryStateV3 as History,
    ChangelogLineageV3 as Lineage, ChangelogTransactionSequence as Sequence, LeadershipEpochV1,
    ReplicationFollowerStateV3 as Follower,
};
use riffdb_types::{CommitSequence, DatabaseId, DualFrontier};

fn lineage() -> Lineage {
    Lineage::new(
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10]).unwrap(),
        1,
        LeadershipEpochV1::initial(),
    )
    .unwrap()
}

fn point(sequence: u64) -> Point {
    Point::new(
        Sequence::new(sequence).unwrap(),
        [sequence as u8; 32],
        DualFrontier::INITIAL,
    )
}

#[test]
fn history_roots_require_ordered_exact_positions_and_nonregressing_dual_frontiers() {
    assert!(History::new(lineage(), point(1), point(3), point(2)).is_ok());
    for (anchor, tail, minimum) in [
        (point(2), point(1), point(2)),
        (point(1), point(3), point(4)),
        (point(2), point(3), point(1)),
    ] {
        assert!(History::new(lineage(), anchor, tail, minimum).is_err());
    }
    let divergent = Point::new(point(1).sequence(), [0xff; 32], DualFrontier::INITIAL);
    assert!(History::new(lineage(), point(1), divergent, point(1)).is_err());
    let ahead = Point::new(
        point(1).sequence(),
        [1; 32],
        DualFrontier::new(CommitSequence::new(1), None),
    );
    assert!(History::new(lineage(), ahead, point(2), ahead).is_err());
    assert!(Lineage::new(lineage().database_id(), 0, LeadershipEpochV1::initial()).is_err());
}

#[test]
fn history_advancement_checks_the_original_receipt_and_allocator_without_mutating_on_refusal() {
    let initial = History::new(lineage(), point(1), point(1), point(1)).unwrap();
    let binding = AuthoritativeTransactionBindingV3 {
        database_id: lineage().database_id(),
        history_incarnation: 1,
        predecessor: Some(point(1).sequence()),
        sequence: point(2).sequence(),
        predecessor_frontier: DualFrontier::INITIAL,
        covered_frontier: DualFrontier::INITIAL,
        prior_history_hash: point(1).history_hash(),
    };
    let receipt =
        AuthoritativeTransactionV3::new(binding, ChangelogAttributionV3::CleanClose, vec![])
            .unwrap();
    let next = initial.advance(&receipt).unwrap();
    assert_eq!(initial.tail(), point(1));
    assert_eq!(next.tail(), Point::from_receipt(&receipt).unwrap());
    assert_eq!(next.minimum_resume(), point(1));
    assert!(next.validate_terminal_receipt(&receipt).is_ok());
    assert!(initial.validate_terminal_receipt(&receipt).is_err());
    assert!(next.validate_allocator(next.expected_allocator()).is_ok());
    assert!(
        initial
            .validate_allocator(next.expected_allocator())
            .is_err()
    );
    assert!(next.advance(&receipt).is_err());
    for changed in [
        AuthoritativeTransactionBindingV3 {
            prior_history_hash: [0xff; 32],
            ..binding
        },
        AuthoritativeTransactionBindingV3 {
            history_incarnation: 2,
            ..binding
        },
    ] {
        let bad =
            AuthoritativeTransactionV3::new(changed, ChangelogAttributionV3::CleanClose, vec![])
                .unwrap();
        assert!(initial.advance(&bad).is_err());
    }
    let exhausted = History::new(lineage(), point(1), point(u64::MAX), point(1)).unwrap();
    assert_eq!(
        exhausted.expected_allocator(),
        riffdb_storage_api::ChangelogTransactionAllocator::Exhausted
    );
}

#[test]
fn follower_acknowledgement_cannot_lead_or_substitute_its_applied_position() {
    assert!(Follower::detached().attached_state().is_none());
    assert!(Follower::attached(lineage(), point(2), None).is_ok());
    let follower = Follower::attached(lineage(), point(2), Some(point(1))).unwrap();
    let (actual, applied, ack) = follower.attached_state().unwrap();
    assert_eq!(
        (actual, applied, ack),
        (lineage(), point(2), Some(point(1)))
    );
    assert!(Follower::attached(lineage(), point(2), Some(point(3))).is_err());
    let substituted = Point::new(point(2).sequence(), [0xff; 32], DualFrontier::INITIAL);
    assert!(Follower::attached(lineage(), point(2), Some(substituted)).is_err());
    assert!(!format!("{follower:?}").contains("717171"));
}

#[test]
fn retained_history_and_follower_envelopes_roundtrip_and_refuse_every_corrupt_byte() {
    use riffdb_storage_api::proto_codec::{
        decode_changelog_history_state_v3 as decode_history,
        decode_replication_follower_state_v3 as decode_follower,
        encode_changelog_history_state_v3 as encode_history,
        encode_replication_follower_state_v3 as encode_follower,
    };
    let history = History::new(lineage(), point(1), point(3), point(2)).unwrap();
    let encoded_history = encode_history(history).unwrap();
    assert_eq!(
        *decode_history(encoded_history.as_bytes()).unwrap().value(),
        history
    );
    for follower in [
        Follower::detached(),
        Follower::attached(lineage(), point(3), None).unwrap(),
        Follower::attached(lineage(), point(3), Some(point(2))).unwrap(),
    ] {
        let encoded = encode_follower(follower).unwrap();
        assert_eq!(
            *decode_follower(encoded.as_bytes()).unwrap().value(),
            follower
        );
        assert!(decode_history(encoded.as_bytes()).is_err());
        assert!(decode_follower(encoded_history.as_bytes()).is_err());
        for (bytes, is_history) in [
            (encoded.as_bytes(), false),
            (encoded_history.as_bytes(), true),
        ] {
            let refuses = |bytes: &[u8]| {
                if is_history {
                    decode_history(bytes).is_err()
                } else {
                    decode_follower(bytes).is_err()
                }
            };
            for offset in 0..bytes.len() {
                assert!(refuses(&bytes[..offset]));
                let mut corrupt = bytes.to_vec();
                corrupt[offset] ^= 0x80;
                assert!(refuses(&corrupt));
            }
            let mut trailing = bytes.to_vec();
            trailing.push(0);
            assert!(refuses(&trailing));
        }
    }
}

#[test]
fn valid_checksums_do_not_authorize_missing_or_substituted_history_roots() {
    use riffdb_proto::{durable::encode_current_message, storage::v1 as wire};
    use riffdb_storage_api::proto_codec::{
        decode_changelog_history_state_v3, decode_replication_follower_state_v3,
    };
    let position = wire::stored_changelog_history_state_v3::Position {
        transaction_sequence: 1,
        history_hash: vec![1; 32],
        application_sequence: 0,
        administration_sequence: 0,
    };
    let valid = wire::StoredChangelogHistoryStateV3 {
        database_id: lineage().database_id().as_bytes().to_vec(),
        history_incarnation: 1,
        leadership_epoch: 1,
        catalog_digest: lineage().catalog_digest().to_vec(),
        anchor: Some(position.clone()),
        tail: Some(position.clone()),
        minimum_resume: Some(position),
    };
    for arm in 0..10 {
        let mut corrupt = valid.clone();
        match arm {
            0 => corrupt.history_incarnation = 0,
            1 => corrupt.leadership_epoch = 0,
            2 => corrupt.catalog_digest = vec![0xff; 32],
            3 => corrupt.anchor = None,
            4 => corrupt.tail = None,
            5 => corrupt.minimum_resume = None,
            6 => corrupt.tail.as_mut().unwrap().transaction_sequence = 0,
            7 => {
                corrupt
                    .minimum_resume
                    .as_mut()
                    .unwrap()
                    .transaction_sequence = 2
            }
            8 => corrupt.tail.as_mut().unwrap().history_hash = vec![0xff; 32],
            9 => corrupt.anchor.as_mut().unwrap().application_sequence = 1,
            _ => unreachable!(),
        }
        let encoded = encode_current_message(&corrupt).unwrap();
        assert!(
            decode_changelog_history_state_v3(&encoded).is_err(),
            "accepted arm {arm}"
        );
    }
    let absent =
        encode_current_message(&wire::StoredReplicationFollowerStateV3 { state: None }).unwrap();
    assert!(decode_replication_follower_state_v3(&absent).is_err());
    use wire::stored_replication_follower_state_v3::{Attached, State};
    let attached = Attached {
        database_id: valid.database_id,
        history_incarnation: 1,
        leadership_epoch: 1,
        catalog_digest: valid.catalog_digest,
        applied: valid.tail,
        acknowledged: None,
    };
    for arm in 0..4 {
        let mut corrupt = attached.clone();
        match arm {
            0 => corrupt.applied = None,
            1 => corrupt.catalog_digest = vec![0xff; 32],
            2 => {
                corrupt.acknowledged = corrupt.applied.clone();
                corrupt.acknowledged.as_mut().unwrap().transaction_sequence = 2;
            }
            3 => {
                corrupt.acknowledged = corrupt.applied.clone();
                corrupt.acknowledged.as_mut().unwrap().history_hash = vec![0xff; 32];
            }
            _ => unreachable!(),
        }
        let encoded = encode_current_message(&wire::StoredReplicationFollowerStateV3 {
            state: Some(State::Attached(corrupt)),
        })
        .unwrap();
        assert!(decode_replication_follower_state_v3(&encoded).is_err());
    }
}

#[test]
fn history_and_follower_maximum_field_widths_fit_their_registered_payload_bounds() {
    use riffdb_storage_api::proto_codec::{
        encode_changelog_history_state_v3, encode_replication_follower_state_v3,
    };
    use riffdb_types::AdministrationSequence;
    let lineage = Lineage::new(
        lineage().database_id(),
        u64::MAX,
        LeadershipEpochV1::new(u64::MAX).unwrap(),
    )
    .unwrap();
    let maximum = Point::new(
        Sequence::new(u64::MAX).unwrap(),
        [0xff; 32],
        DualFrontier::new(
            CommitSequence::new(u64::MAX),
            AdministrationSequence::new(u64::MAX),
        ),
    );
    let history = encode_changelog_history_state_v3(
        History::new(lineage, maximum, maximum, maximum).unwrap(),
    )
    .unwrap();
    let follower = encode_replication_follower_state_v3(
        Follower::attached(lineage, maximum, Some(maximum)).unwrap(),
    )
    .unwrap();
    // Current compact envelope header is 16 bytes; every payload varint is maximal.
    assert_eq!(history.as_bytes().len(), 16 + 281);
    assert_eq!(follower.as_bytes().len(), 16 + 215);
}

#[test]
fn complete_history_and_follower_envelope_vectors_are_frozen() {
    use riffdb_storage_api::proto_codec::{
        encode_changelog_history_state_v3, encode_replication_follower_state_v3,
    };
    use sha2::{Digest, Sha256};
    let point = Point::new(
        Sequence::new(1).unwrap(),
        [0x77; 32],
        DualFrontier::new(CommitSequence::new(1), None),
    );
    for (encoded, fixture, digest) in [
        (
            encode_changelog_history_state_v3(
                History::new(lineage(), point, point, point).unwrap(),
            )
            .unwrap(),
            include_str!("../../../fixtures/replication/changelog-history-state-v3.hex"),
            "48ad6143ffec76c0457fa8f5ae2a0ce4cbc911a80027feca315a0ba06657e931",
        ),
        (
            encode_replication_follower_state_v3(Follower::detached()).unwrap(),
            include_str!(
                "../../../fixtures/replication/replication-follower-state-v3-detached.hex"
            ),
            "5bcb4517414512149c7f1dd5e5d9353a558f07e41e9ad04048252952e60c5045",
        ),
        (
            encode_replication_follower_state_v3(
                Follower::attached(lineage(), point, Some(point)).unwrap(),
            )
            .unwrap(),
            include_str!(
                "../../../fixtures/replication/replication-follower-state-v3-attached.hex"
            ),
            "94031698d3e54e7863ea3a68aee238fb72a7a07f12191577151c22ea83c85e1f",
        ),
    ] {
        let hex = encoded
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
            + "\n";
        assert_eq!(hex, fixture);
        assert_eq!(
            Sha256::digest(fixture.as_bytes())
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
            digest
        );
    }
}
