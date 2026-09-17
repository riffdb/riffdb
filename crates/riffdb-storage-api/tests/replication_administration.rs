#![forbid(unsafe_code)]
// req: REP-006, STO-012
//! Administration values reject contradictory lifecycle evidence before encoding.
use riffdb_storage_api::{
    AuditPrincipalV1, ChangelogHistoryPointV3 as Point, ChangelogLineageV3 as Lineage,
    ChangelogTransactionSequence as Sequence, FollowerHoldBudget,
    FollowerRegistrationPhaseV1 as Phase, ReplicationAdministrationActionV1 as Action,
    ReplicationAdministrationOriginV1 as Origin, ReplicationSourceHoldKindV1 as Kind,
    ReplicationSourceHoldStateV1 as State, ReplicationSourceHoldV1 as Hold,
    ReplicationSourceHoldV2 as Policy, StoredReplicationAdministrationV1 as Record,
};
use riffdb_types::{
    ActorId, ActorKind, AdministrationSequence, CapabilityId, CommitSequence, DatabaseId,
    DualFrontier, LeadershipEpochV1, ReplicationFollowerAuditTargetV1 as Target,
    ReplicationSourceHoldIdV1, RequestId, Timestamp,
};
use std::num::NonZeroU64;

fn point(physical: u64, app: u64, admin: u64) -> Point {
    Point::new(
        Sequence::new(physical).unwrap(),
        [physical as u8; 32],
        DualFrontier::new(CommitSequence::new(app), AdministrationSequence::new(admin)),
    )
}
fn hold(fence: Point) -> Hold {
    Hold::new(
        ReplicationSourceHoldIdV1::new([1; 16]).unwrap(),
        Kind::FollowerAcknowledgement,
        Lineage::new(
            DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [1; 10]).unwrap(),
            1,
            LeadershipEpochV1::initial(),
        )
        .unwrap(),
        fence,
    )
}
fn target(value: Hold) -> Target {
    Target::new(
        value.lineage().database_id(),
        value.lineage().history_incarnation(),
        value.lineage().leadership_epoch(),
        value.id(),
    )
    .unwrap()
}
fn origin() -> Origin {
    Origin::Explicit {
        request_id: RequestId::from_unix_milliseconds_and_random(1_700_000_000_000, [2; 10])
            .unwrap(),
        principal: AuditPrincipalV1::new(
            ActorId::new("operator").unwrap(),
            ActorKind::Human,
            CapabilityId::from_unix_milliseconds_and_random(1_700_000_000_000, [3; 10]).unwrap(),
            NonZeroU64::new(1).unwrap(),
        ),
        approval_id: None,
    }
}
fn policy(fence: Point, phase: Phase, expiry: u64, degraded: Option<Point>) -> Policy {
    Policy::new(
        hold(fence),
        point(8, 3, 2),
        FollowerHoldBudget::new(2).unwrap(),
        CommitSequence::new(expiry),
        phase,
        degraded,
    )
    .unwrap()
}
fn record(
    action: Action,
    before: Option<State>,
    after: Policy,
    observed: Point,
    origin: Origin,
) -> Result<Record, riffdb_storage_api::StorageValueError> {
    Record::new(
        AdministrationSequence::new(
            observed
                .frontier()
                .administration()
                .map_or(1, |v| v.get() + 1),
        )
        .unwrap(),
        Timestamp::new(1_700_000_000, 0).unwrap(),
        action,
        target(after.hold()),
        before,
        after,
        observed,
        origin,
    )
}

#[test]
fn registration_preserves_new_and_legacy_fences_without_resurrection() {
    let observed = point(8, 3, 2);
    let pending = policy(observed, Phase::AwaitingBootstrap, 9, None);
    let registered = record(Action::RegisterFollower, None, pending, observed, origin()).unwrap();
    assert_eq!(registered.generation().get(), 9);
    assert_eq!(registered.administration_sequence().get(), 3);
    let legacy = hold(point(6, 2, 1));
    let attached = policy(legacy.fence(), Phase::Attached, 9, None);
    let upgraded = record(
        Action::RegisterFollower,
        Some(State::Legacy(legacy)),
        attached,
        observed,
        origin(),
    )
    .unwrap();
    assert_eq!(upgraded.after().hold(), legacy);
    assert!(record(Action::RegisterFollower, None, attached, observed, origin()).is_err());
    assert!(
        record(
            Action::RegisterFollower,
            Some(State::Registered(pending)),
            pending,
            observed,
            origin()
        )
        .is_err()
    );
    assert!(
        record(
            Action::RegisterFollower,
            Some(State::Legacy(legacy)),
            pending,
            observed,
            origin()
        )
        .is_err()
    );
    assert_eq!(
        format!("{registered:?}"),
        "StoredReplicationAdministrationV1([redacted])"
    );
}

#[test]
fn retirement_preserves_policy_and_expiry_needs_original_receipt_and_degradation() {
    let degraded = point(12, 6, 4);
    let before = policy(point(10, 4, 3), Phase::Attached, 7, Some(degraded));
    let after = policy(before.hold().fence(), Phase::Retired, 7, Some(degraded));
    let observed = point(14, 7, 5);
    let expiry = || Origin::ConfiguredExpiry {
        registration: AdministrationSequence::new(3).unwrap(),
    };
    assert!(
        record(
            Action::RetireFollower,
            Some(State::Registered(before)),
            after,
            observed,
            origin()
        )
        .is_ok()
    );
    assert!(
        record(
            Action::ExpireFollower,
            Some(State::Registered(before)),
            after,
            observed,
            expiry()
        )
        .is_ok()
    );
    assert!(
        record(
            Action::ExpireFollower,
            Some(State::Registered(before)),
            after,
            degraded,
            expiry()
        )
        .is_err()
    );
    assert!(
        record(
            Action::ExpireFollower,
            Some(State::Registered(before)),
            after,
            observed,
            origin()
        )
        .is_err()
    );
    assert!(
        record(
            Action::RetireFollower,
            Some(State::Registered(before)),
            after,
            observed,
            expiry()
        )
        .is_err()
    );
    assert!(
        record(
            Action::ExpireFollower,
            Some(State::Registered(before)),
            after,
            observed,
            Origin::ConfiguredExpiry {
                registration: AdministrationSequence::new(4).unwrap()
            }
        )
        .is_err()
    );
    let healthy = policy(before.hold().fence(), Phase::Attached, 7, None);
    let retired = policy(before.hold().fence(), Phase::Retired, 7, None);
    assert!(
        record(
            Action::ExpireFollower,
            Some(State::Registered(healthy)),
            retired,
            observed,
            expiry()
        )
        .is_err()
    );
    let no_expiry = policy(before.hold().fence(), Phase::Attached, 0, Some(degraded));
    let no_expiry_retired = policy(before.hold().fence(), Phase::Retired, 0, Some(degraded));
    assert!(
        record(
            Action::ExpireFollower,
            Some(State::Registered(no_expiry)),
            no_expiry_retired,
            observed,
            expiry()
        )
        .is_err()
    );
    assert!(
        record(
            Action::RetireFollower,
            Some(State::Registered(after)),
            after,
            observed,
            origin()
        )
        .is_err()
    );
    let changed = policy(before.hold().fence(), Phase::Retired, 8, Some(degraded));
    assert!(
        record(
            Action::RetireFollower,
            Some(State::Registered(before)),
            changed,
            observed,
            origin()
        )
        .is_err()
    );
}

#[test]
fn substituted_target_sequence_or_observation_is_refused() {
    let observed = point(8, 3, 2);
    let after = policy(observed, Phase::AwaitingBootstrap, 9, None);
    let valid = record(Action::RegisterFollower, None, after, observed, origin()).unwrap();
    let foreign = Target::new(
        after.hold().lineage().database_id(),
        2,
        LeadershipEpochV1::initial(),
        after.hold().id(),
    )
    .unwrap();
    for (sequence, target, observed) in [
        (3, foreign, observed),
        (2, valid.target(), observed),
        (4, valid.target(), observed),
        (3, valid.target(), point(9, 3, 2)),
    ] {
        assert!(
            Record::new(
                AdministrationSequence::new(sequence).unwrap(),
                valid.timestamp(),
                Action::RegisterFollower,
                target,
                None,
                after,
                observed,
                origin()
            )
            .is_err()
        );
    }
    let impossible = point(u64::MAX, 3, u64::MAX);
    assert!(
        Record::new(
            AdministrationSequence::new(1).unwrap(),
            valid.timestamp(),
            Action::RegisterFollower,
            valid.target(),
            None,
            after,
            impossible,
            origin()
        )
        .is_err()
    );
}

fn cases() -> [Record; 4] {
    let observed = point(8, 3, 2);
    let pending = policy(observed, Phase::AwaitingBootstrap, 7, None);
    let legacy = hold(point(6, 2, 1));
    let attached = policy(legacy.fence(), Phase::Attached, 7, None);
    let degraded = point(12, 6, 4);
    let before = policy(point(10, 4, 3), Phase::Attached, 7, Some(degraded));
    let after = policy(before.hold().fence(), Phase::Retired, 7, Some(degraded));
    [
        record(Action::RegisterFollower, None, pending, observed, origin()).unwrap(),
        record(
            Action::RegisterFollower,
            Some(State::Legacy(legacy)),
            attached,
            observed,
            origin(),
        )
        .unwrap(),
        record(
            Action::RetireFollower,
            Some(State::Registered(before)),
            after,
            point(14, 7, 5),
            origin(),
        )
        .unwrap(),
        record(
            Action::ExpireFollower,
            Some(State::Registered(before)),
            after,
            point(14, 7, 5),
            Origin::ConfiguredExpiry {
                registration: AdministrationSequence::new(3).unwrap(),
            },
        )
        .unwrap(),
    ]
}

use prost::Message;
use riffdb_proto::{
    durable::{READABLE_RECORD_SCHEMAS, readable_record_registry, readable_record_schema},
    envelope::{RecordRegistry, STORAGE_FORMAT_VERSION_V1, payload_crc32c},
    storage::v1 as wire,
};
use riffdb_storage_api::proto_codec::{
    decode_replication_administration_v1 as decode, decode_replication_source_hold,
    encode_replication_administration_v1 as encode, encode_replication_source_hold_v1,
    encode_replication_source_hold_v2,
};
const RECORD: &str = "riffdb.storage.v1.StoredReplicationAdministrationV1";
fn raw(payload: Vec<u8>) -> Vec<u8> {
    wire::StoredEnvelope {
        storage_format_version: STORAGE_FORMAT_VERSION_V1,
        record_type: RECORD.to_owned(),
        schema_hash: readable_record_schema(RECORD)
            .unwrap()
            .schema_hash()
            .as_bytes()
            .to_vec(),
        payload_crc32c: payload_crc32c(&payload),
        payload,
    }
    .encode_to_vec()
}
fn wire_record(value: &Record) -> wire::StoredReplicationAdministrationV1 {
    let bytes = encode(value).unwrap();
    let envelope = readable_record_registry().decode(bytes.as_bytes()).unwrap();
    wire::StoredReplicationAdministrationV1::decode(envelope.payload()).unwrap()
}

#[test]
fn administration_canonical_fixtures_and_old_registry_refusal_are_exact() {
    let schema = readable_record_schema(RECORD).unwrap();
    assert_eq!((schema.compact_tag(), schema.schema_revision()), (75, 1));
    let export =
        readable_record_schema("riffdb.storage.v1.StoredApplicationExportPageCommitmentV1")
            .unwrap();
    assert_eq!((export.compact_tag(), export.schema_revision()), (74, 1));
    let old = READABLE_RECORD_SCHEMAS
        .iter()
        .copied()
        .filter(|s| s.record_type() != RECORD)
        .collect::<Vec<_>>();
    let old = RecordRegistry::new(&old).unwrap();
    let mut vectors = String::new();
    for value in cases() {
        let bytes = encode(&value).unwrap();
        assert!(old.decode(bytes.as_bytes()).is_err());
        assert_eq!(decode(bytes.as_bytes()).unwrap().value(), &value);
        vectors.push_str(
            &bytes
                .as_bytes()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>(),
        );
        vectors.push('\n');
        for offset in 0..bytes.as_bytes().len() {
            assert!(decode(&bytes.as_bytes()[..offset]).is_err());
            let mut damaged = bytes.as_bytes().to_vec();
            damaged[offset] ^= 0x80;
            assert!(decode(&damaged).is_err(), "corrupted byte {offset}");
        }
        let mut trailing = bytes.as_bytes().to_vec();
        trailing.push(0);
        assert!(decode(&trailing).is_err());
    }
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/proto/durable-replication-administration-v1.hex");
    if let Some(output) = std::env::var_os("RIFFDB_REPLICATION_ADMINISTRATION_VECTOR_OUTPUT") {
        std::fs::write(output, &vectors).unwrap();
    } else {
        assert_eq!(std::fs::read_to_string(path).unwrap(), vectors);
    }
}

#[test]
fn checksum_correct_malformed_lifecycle_records_are_refused() {
    use wire::stored_replication_administration_v1::{Before, Origin as WireOrigin};
    for case in 0..27 {
        let mut value = wire_record(&cases()[0]);
        match case {
            0 => value.administration_sequence = 0,
            1 => value.administration_sequence += 1,
            2 => value.timestamp = None,
            3 => value.timestamp.as_mut().unwrap().nanos = 1_000_000_000,
            4 => value.action = 0,
            5 => value.action = 4,
            6 => value.action = 2,
            7 => value.target = None,
            8 => value.target.as_mut().unwrap().database_id = vec![0; 16],
            9 => value.target.as_mut().unwrap().history_incarnation += 1,
            10 => value.target.as_mut().unwrap().leadership_epoch += 1,
            11 => value.target.as_mut().unwrap().hold_id = vec![2; 16],
            12 => value.registration_generation = 0,
            13 => value.registration_generation += 1,
            14 => value.observed = None,
            15 => value.before = None,
            16 => value.before = Some(Before::Registered(value.after.clone().unwrap())),
            17 => value.after = None,
            18 => value.after.as_mut().unwrap().phase = 3,
            19 => value.after.as_mut().unwrap().hold_budget_sequences = 0,
            20 => value.origin = None,
            21 => value.origin = wire_record(&cases()[3]).origin,
            22..=26 => {
                let WireOrigin::Explicit(explicit) = value.origin.as_mut().unwrap() else {
                    panic!("explicit")
                };
                match case {
                    22 => explicit.request_id = vec![0; 16],
                    23 => explicit.principal = None,
                    24 => explicit.principal.as_mut().unwrap().capability_revision = 0,
                    25 => explicit.approval_id = Some("x".repeat(257)),
                    26 => explicit.principal.as_mut().unwrap().principal_id = "x".repeat(257),
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
        assert!(
            decode(&raw(value.encode_to_vec())).is_err(),
            "malformed case {case}"
        );
    }
    let mut payload = wire_record(&cases()[0]).encode_to_vec();
    payload.extend([0x68, 1]); // unknown field13 with a correct outer checksum
    assert!(decode(&raw(payload)).is_err());
    let mut payload = wire_record(&cases()[0]).encode_to_vec();
    payload.extend([0x38, 0]); // duplicate before field with wrong wire type
    assert!(decode(&raw(payload)).is_err());
    for case in 0..5 {
        let mut value = wire_record(&cases()[3]);
        match case {
            0 => {
                value
                    .after
                    .as_mut()
                    .unwrap()
                    .expires_at_application_sequence = None
            }
            1 => value.after.as_mut().unwrap().degraded_at = None,
            2 => value.after.as_mut().unwrap().hold_budget_sequences += 1,
            3 => value.before = Some(Before::Registered(value.after.clone().unwrap())),
            4 => {
                let WireOrigin::ConfiguredExpiry(expiry) = value.origin.as_mut().unwrap() else {
                    panic!("expiry")
                };
                expiry.registration_administration_sequence += 1;
            }
            _ => unreachable!(),
        }
        assert!(
            decode(&raw(value.encode_to_vec())).is_err(),
            "expiry case {case}"
        );
    }
}

#[test]
fn mixed_hold_reader_preserves_generation_tombstone_and_charge() {
    for value in cases() {
        let legacy = encode_replication_source_hold_v1(value.after().hold()).unwrap();
        let decoded = decode_replication_source_hold(legacy.as_bytes()).unwrap();
        assert_eq!(decoded.value(), &State::Legacy(value.after().hold()));
        assert_eq!(
            decoded.encoded_content_charge().get(),
            legacy.as_bytes().len()
        );
        let policy = encode_replication_source_hold_v2(value.after()).unwrap();
        let decoded = decode_replication_source_hold(policy.as_bytes()).unwrap();
        assert_eq!(decoded.value(), &State::Registered(value.after()));
        assert_eq!(
            decoded.encoded_content_charge().get(),
            policy.as_bytes().len()
        );
        assert!(decode_replication_source_hold(encode(&value).unwrap().as_bytes()).is_err());
    }
}

#[test]
fn accepted_maximum_identifiers_and_counters_fit_the_payload_bound() {
    let observed = point(u64::MAX - 1, u64::MAX - 1, u64::MAX - 1);
    let pending = Policy::new(
        hold(observed),
        observed,
        FollowerHoldBudget::new(u64::MAX).unwrap(),
        CommitSequence::new(u64::MAX),
        Phase::AwaitingBootstrap,
        None,
    )
    .unwrap();
    let Origin::Explicit {
        request_id,
        principal,
        ..
    } = origin()
    else {
        unreachable!()
    };
    let origin = Origin::Explicit {
        request_id,
        principal: AuditPrincipalV1::new(
            ActorId::new("p".repeat(riffdb_types::MAX_ACTOR_ID_BYTES)).unwrap(),
            ActorKind::Human,
            principal.capability_id(),
            NonZeroU64::new(u64::MAX).unwrap(),
        ),
        approval_id: Some(
            riffdb_types::ApprovalId::new("a".repeat(riffdb_types::MAX_APPROVAL_ID_BYTES)).unwrap(),
        ),
    };
    let value = Record::new(
        AdministrationSequence::new(u64::MAX).unwrap(),
        Timestamp::new(i64::MIN, 999_999_999).unwrap(),
        Action::RegisterFollower,
        target(pending.hold()),
        None,
        pending,
        observed,
        origin,
    )
    .unwrap();
    let bytes = encode(&value).unwrap();
    let registry = readable_record_registry();
    let envelope = registry.decode(bytes.as_bytes()).unwrap();
    assert!(envelope.payload().len() <= 2048);
    assert_eq!(decode(bytes.as_bytes()).unwrap().value(), &value);
}
