#![forbid(unsafe_code)]
// req: REP-006, REC-001, STO-012
//! Versioned policy bytes; decoding never authorizes registration or release.
use riffdb_storage_api::{
    ChangelogHistoryPointV3 as Point, ChangelogLineageV3 as Lineage,
    ChangelogTransactionSequence as Sequence, FollowerHoldBudget,
    FollowerRegistrationPhaseV1 as Phase, LeadershipEpochV1, ReplicationSourceHoldIdV1 as Id,
    ReplicationSourceHoldKindV1 as Kind, ReplicationSourceHoldV1 as Hold,
    ReplicationSourceHoldV2 as Registration,
    proto_codec::{
        decode_replication_source_hold_v1, decode_replication_source_hold_v2 as decode,
        encode_replication_source_hold_v1, encode_replication_source_hold_v2 as encode,
    },
};
use riffdb_types::{AdministrationSequence, CommitSequence, DatabaseId, DualFrontier};

fn point(sequence: u64, application: u64) -> Point {
    Point::new(
        Sequence::new(sequence).unwrap(),
        [0x77; 32],
        DualFrontier::new(
            CommitSequence::new(application),
            AdministrationSequence::new(1),
        ),
    )
}

fn hold(fence: Point) -> Hold {
    Hold::new(
        Id::new([0x81; 16]).unwrap(),
        Kind::FollowerAcknowledgement,
        Lineage::new(
            DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10]).unwrap(),
            1,
            LeadershipEpochV1::initial(),
        )
        .unwrap(),
        fence,
    )
}

fn registration(phase: Phase) -> Registration {
    let registered_at = point(8, 3);
    let fence = if phase == Phase::AwaitingBootstrap {
        registered_at
    } else {
        point(10, 4)
    };
    Registration::new(
        hold(fence),
        registered_at,
        FollowerHoldBudget::new(2).unwrap(),
        CommitSequence::new(9),
        phase,
        Some(point(12, 6)),
    )
    .unwrap()
}

#[test]
fn registered_policy_roundtrips_without_reinterpreting_legacy_holds() {
    for phase in [Phase::AwaitingBootstrap, Phase::Attached, Phase::Retired] {
        let value = registration(phase);
        let encoded = encode(value).unwrap();
        assert_eq!(*decode(encoded.as_bytes()).unwrap().value(), value);
        assert_eq!(value.generation().get(), 9);
        assert_eq!(
            value.hold().storage_key(),
            hold(value.hold().fence()).storage_key()
        );
        assert!(decode_replication_source_hold_v1(encoded.as_bytes()).is_err());
        let legacy = encode_replication_source_hold_v1(value.hold()).unwrap();
        assert!(decode(legacy.as_bytes()).is_err());
        assert_eq!(format!("{value:?}"), "ReplicationSourceHoldV2([redacted])");
        for offset in 0..encoded.as_bytes().len() {
            assert!(decode(&encoded.as_bytes()[..offset]).is_err());
            let mut corrupted = encoded.as_bytes().to_vec();
            corrupted[offset] ^= 0x80;
            assert!(decode(&corrupted).is_err());
        }
        let mut trailing = encoded.as_bytes().to_vec();
        trailing.push(0);
        assert!(decode(&trailing).is_err());
    }
}

#[test]
fn budget_expiry_and_registration_state_are_bound_by_the_successor_bytes() {
    let original = registration(Phase::Attached);
    let bytes = encode(original).unwrap();
    let changed_budget = Registration::new(
        original.hold(),
        original.registered_at(),
        FollowerHoldBudget::new(1).unwrap(),
        original.expires_at(),
        original.phase(),
        original.degraded_at(),
    )
    .unwrap();
    let changed_expiry = Registration::new(
        original.hold(),
        original.registered_at(),
        original.budget(),
        CommitSequence::new(10),
        original.phase(),
        original.degraded_at(),
    )
    .unwrap();
    for changed in [changed_budget, changed_expiry, registration(Phase::Retired)] {
        assert_ne!(encode(changed).unwrap().as_bytes(), bytes.as_bytes());
    }
}

#[test]
fn registration_shape_cannot_forge_budget_exhaustion_or_a_live_bootstrap_fence() {
    let original = registration(Phase::Attached);
    assert!(
        Registration::new(
            original.hold(),
            original.registered_at(),
            original.budget(),
            original.expires_at(),
            Phase::AwaitingBootstrap,
            original.degraded_at(),
        )
        .is_err()
    );
    for invalid in [point(7, 2), point(11, 5)] {
        assert!(
            Registration::new(
                original.hold(),
                original.registered_at(),
                original.budget(),
                original.expires_at(),
                original.phase(),
                Some(invalid),
            )
            .is_err()
        );
    }
    assert!(
        Registration::new(
            original.hold(),
            original.registered_at(),
            original.budget(),
            CommitSequence::new(3),
            original.phase(),
            None,
        )
        .is_err()
    );
}

#[test]
fn resealed_policy_refuses_missing_unknown_and_semantically_false_fields() {
    use riffdb_proto::{
        durable::{decode_readable_message, encode_current_message},
        storage::v1::StoredReplicationSourceHoldV2 as Wire,
    };
    let encoded = encode(registration(Phase::Attached)).unwrap();
    let valid: Wire = decode_readable_message(encoded.as_bytes()).unwrap();
    for arm in 0..18 {
        let mut bad = valid.clone();
        match arm {
            0 => bad.hold = None,
            1 => bad.registered_at = None,
            2 => bad.hold_budget_sequences = 0,
            3 => bad.phase = 0,
            4 => bad.phase = 4,
            5 => bad.expires_at_application_sequence = Some(0),
            6 => bad.expires_at_application_sequence = Some(3),
            7 => bad.hold.as_mut().unwrap().kind = 2,
            8 => bad.hold.as_mut().unwrap().kind = 3,
            9 => bad.registered_at.as_mut().unwrap().transaction_sequence = u64::MAX,
            10 => bad.registered_at.as_mut().unwrap().transaction_sequence = 0,
            11 => bad.registered_at.as_mut().unwrap().history_hash.clear(),
            12 => bad.phase = 1,
            13 => bad.degraded_at.as_mut().unwrap().application_sequence = 5,
            14 => bad.degraded_at.as_mut().unwrap().transaction_sequence = 7,
            15 => bad.hold.as_mut().unwrap().catalog_digest[0] ^= 1,
            16 => bad.registered_at.as_mut().unwrap().application_sequence = 5,
            _ => {
                // Equal physical positions require the same frontier and hash.
                bad.registered_at = bad.hold.as_ref().unwrap().fence.clone();
                bad.registered_at.as_mut().unwrap().history_hash[0] ^= 1;
            }
        }
        // Structural preflight may reject before sealing.
        if let Ok(bytes) = encode_current_message(&bad) {
            assert!(decode(&bytes).is_err(), "invalid arm {arm}");
        }
    }
    let mut expiry_only = valid.clone();
    expiry_only.hold_budget_sequences = u64::MAX;
    expiry_only.expires_at_application_sequence = Some(6);
    assert!(decode(&encode_current_message(&expiry_only).unwrap()).is_ok());
    for phase in [1, 2, 3] {
        let mut no_expiry = valid.clone();
        no_expiry.phase = phase;
        if phase == 1 {
            no_expiry.hold.as_mut().unwrap().fence = no_expiry.registered_at.clone();
        }
        no_expiry.expires_at_application_sequence = None;
        no_expiry.degraded_at = None;
        assert!(decode(&encode_current_message(&no_expiry).unwrap()).is_ok());
    }
}

#[test]
fn registered_policy_maximum_counters_fit_the_registered_payload_bound() {
    use prost::Message;
    use riffdb_proto::{
        durable::{current_record_schema, decode_readable_message},
        storage::v1::StoredReplicationSourceHoldV2 as Wire,
    };
    let point = |n| {
        Point::new(
            Sequence::new(n).unwrap(),
            [0xff; 32],
            DualFrontier::new(CommitSequence::new(n), AdministrationSequence::new(n)),
        )
    };
    let old = hold(point(u64::MAX));
    let maximal_hold = Hold::new(
        old.id(),
        old.kind(),
        Lineage::new(
            old.lineage().database_id(),
            u64::MAX,
            LeadershipEpochV1::new(u64::MAX).unwrap(),
        )
        .unwrap(),
        old.fence(),
    );
    let value = Registration::new(
        maximal_hold,
        point(u64::MAX - 1),
        FollowerHoldBudget::new(u64::MAX).unwrap(),
        CommitSequence::new(u64::MAX),
        Phase::Attached,
        Some(point(u64::MAX)),
    )
    .unwrap();
    let encoded = encode(value).unwrap();
    assert_eq!(*decode(encoded.as_bytes()).unwrap().value(), value);
    assert_eq!(value.generation().get(), u64::MAX);
    let wire: Wire = decode_readable_message(encoded.as_bytes()).unwrap();
    assert_eq!(wire.encoded_len(), 328);
    assert_eq!(
        current_record_schema("riffdb.storage.v1.StoredReplicationSourceHoldV2")
            .unwrap()
            .max_payload_bytes(),
        328
    );
}

#[test]
fn registered_policy_vectors_bind_each_phase_to_canonical_bytes() {
    let fixture_root =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/replication");
    for (phase, name) in [
        (Phase::AwaitingBootstrap, "awaiting-bootstrap"),
        (Phase::Attached, "attached"),
        (Phase::Retired, "retired"),
    ] {
        let bytes = encode(registration(phase)).unwrap();
        let hex = bytes
            .as_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
            + "\n";
        let file = fixture_root.join(format!("replication-source-hold-v2-{name}.hex"));
        if std::env::var_os("RIFFDB_UPDATE_SOURCE_HOLD_V2_FIXTURES").is_some() {
            std::fs::write(&file, &hex).unwrap();
        }
        assert_eq!(hex, std::fs::read_to_string(file).unwrap());
    }
}
