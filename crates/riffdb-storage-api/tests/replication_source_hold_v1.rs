#![forbid(unsafe_code)]
//! Closed source-fence codec and refusal evidence, not a reclamation permit.
// req: REP-003, REC-001, STO-012
use riffdb_storage_api::{
    ChangelogHistoryPointV3 as Point, ChangelogLineageV3 as Lineage,
    ChangelogTransactionSequence as Sequence, LeadershipEpochV1, ReplicationSourceHoldIdV1 as Id,
    ReplicationSourceHoldKindV1 as Kind, ReplicationSourceHoldV1 as Hold,
    proto_codec::{
        decode_replication_source_hold_v1 as decode, encode_replication_source_hold_v1 as encode,
    },
};
use riffdb_types::{CommitSequence, DatabaseId, DualFrontier};

fn hold(kind: Kind) -> Hold {
    Hold::new(
        Id::new([0x81; 16]).unwrap(),
        kind,
        Lineage::new(
            DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10]).unwrap(),
            1,
            LeadershipEpochV1::initial(),
        )
        .unwrap(),
        Point::new(
            Sequence::new(1).unwrap(),
            [0x77; 32],
            DualFrontier::new(CommitSequence::new(1), None),
        ),
    )
}

#[test]
fn source_hold_roles_roundtrip_without_identity_or_hash_diagnostics() {
    assert!(Id::new([0; 16]).is_none());
    for kind in [
        Kind::FollowerAcknowledgement,
        Kind::ArchiveAcknowledgement,
        Kind::Bootstrap,
    ] {
        let hold = hold(kind);
        let encoded = encode(hold).unwrap();
        assert_eq!(*decode(encoded.as_bytes()).unwrap().value(), hold);
        assert_eq!(hold.storage_key()[0], kind as u8);
        assert_eq!(&hold.storage_key()[1..], hold.id().as_bytes());
        assert_eq!(format!("{hold:?}"), "ReplicationSourceHoldV1([redacted])");
        assert_eq!(
            format!("{:?}", hold.id()),
            "ReplicationSourceHoldIdV1([redacted])"
        );
        assert!(
            riffdb_storage_api::proto_codec::decode_replication_follower_state_v3(
                encoded.as_bytes()
            )
            .is_err()
        );
        for offset in 0..encoded.as_bytes().len() {
            assert!(decode(&encoded.as_bytes()[..offset]).is_err());
            let mut bad = encoded.as_bytes().to_vec();
            bad[offset] ^= 0x80;
            assert!(decode(&bad).is_err());
        }
        let mut bad = encoded.as_bytes().to_vec();
        bad.push(0);
        assert!(decode(&bad).is_err());
    }
}

#[test]
fn source_hold_checksums_do_not_authorize_unknown_or_incomplete_fences() {
    use riffdb_proto::{
        durable::encode_current_message,
        storage::v1::{
            StoredReplicationSourceHoldV1 as Wire, stored_changelog_history_state_v3::Position,
        },
    };
    let h = hold(Kind::Bootstrap);
    let valid = Wire {
        kind: 3,
        hold_id: h.id().as_bytes().to_vec(),
        database_id: h.lineage().database_id().as_bytes().to_vec(),
        history_incarnation: 1,
        leadership_epoch: 1,
        catalog_digest: h.lineage().catalog_digest().to_vec(),
        fence: Some(Position {
            transaction_sequence: 1,
            history_hash: [0x77; 32].to_vec(),
            application_sequence: 1,
            administration_sequence: 0,
        }),
    };
    assert!(decode(&encode_current_message(&valid).unwrap()).is_ok());
    for arm in 0..11 {
        let mut bad = valid.clone();
        match arm {
            0 => bad.kind = 0,
            1 => bad.kind = 4,
            2 => bad.hold_id = vec![0; 16],
            3 => bad.hold_id.truncate(15),
            4 => bad.database_id.clear(),
            5 => bad.history_incarnation = 0,
            6 => bad.leadership_epoch = 0,
            7 => bad.catalog_digest[0] ^= 1,
            8 => bad.fence = None,
            9 => bad.fence.as_mut().unwrap().transaction_sequence = 0,
            _ => bad.fence.as_mut().unwrap().history_hash.clear(),
        }
        if let Ok(encoded) = encode_current_message(&bad) {
            assert!(decode(&encoded).is_err(), "invalid arm {arm}");
        }
    }
}

#[test]
fn source_hold_vectors_bind_each_closed_kind_to_its_exact_canonical_bytes() {
    for (kind, fixture) in [
        (
            Kind::FollowerAcknowledgement,
            include_str!("../../../fixtures/replication/replication-source-hold-v1-follower.hex"),
        ),
        (
            Kind::ArchiveAcknowledgement,
            include_str!("../../../fixtures/replication/replication-source-hold-v1-archive.hex"),
        ),
        (
            Kind::Bootstrap,
            include_str!("../../../fixtures/replication/replication-source-hold-v1-bootstrap.hex"),
        ),
    ] {
        let bytes = encode(hold(kind)).unwrap();
        let hex = bytes
            .as_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
            + "\n";
        assert_eq!(hex, fixture);
    }
}

#[test]
fn source_hold_maximum_counters_fit_the_registered_payload_bound() {
    use prost::Message;
    use riffdb_proto::storage::v1::{
        StoredReplicationSourceHoldV1 as Wire, stored_changelog_history_state_v3::Position,
    };
    use riffdb_types::AdministrationSequence;
    let h = hold(Kind::Bootstrap);
    let maximal = Hold::new(
        h.id(),
        h.kind(),
        Lineage::new(
            h.lineage().database_id(),
            u64::MAX,
            LeadershipEpochV1::new(u64::MAX).unwrap(),
        )
        .unwrap(),
        Point::new(
            Sequence::new(u64::MAX).unwrap(),
            [0xff; 32],
            DualFrontier::new(
                CommitSequence::new(u64::MAX),
                AdministrationSequence::new(u64::MAX),
            ),
        ),
    );
    let encoded = encode(maximal).unwrap();
    assert_eq!(*decode(encoded.as_bytes()).unwrap().value(), maximal);
    let wire = Wire {
        kind: 3,
        hold_id: maximal.id().as_bytes().to_vec(),
        database_id: maximal.lineage().database_id().as_bytes().to_vec(),
        history_incarnation: u64::MAX,
        leadership_epoch: u64::MAX,
        catalog_digest: maximal.lineage().catalog_digest().to_vec(),
        fence: Some(Position {
            transaction_sequence: u64::MAX,
            history_hash: vec![0xff; 32],
            application_sequence: u64::MAX,
            administration_sequence: u64::MAX,
        }),
    };
    assert_eq!(wire.encoded_len(), 163);
    let schema = riffdb_proto::durable::current_record_schema(
        "riffdb.storage.v1.StoredReplicationSourceHoldV1",
    )
    .unwrap();
    assert_eq!(schema.max_payload_bytes(), 163);
}
