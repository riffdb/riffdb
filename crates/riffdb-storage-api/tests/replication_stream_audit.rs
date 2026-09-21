#![forbid(unsafe_code)]
// req: REP-003, REP-005
//! The accepted stream operation uses existing audit generations and no result link.
use riffdb_storage_api::proto_codec::{
    decode_service_audit_record, decode_service_audit_record_v2, decode_service_audit_record_v3,
    encode_service_audit_record_v2, encode_service_audit_record_v3,
};
use riffdb_storage_api::{
    AuditPrincipalV1, ServiceAuditAppendIntentV1, StoredServiceAuditRecordV1,
};
use riffdb_types::*;
use std::num::NonZeroU64;

fn uuid(seed: u8) -> [u8; 16] {
    *RequestId::from_unix_milliseconds_and_random(1_700_000_000_000, [seed; 10])
        .unwrap()
        .as_bytes()
}
fn follower(seed: u8) -> ServiceAuditTargetV1 {
    ServiceAuditTargetV1::ReplicationFollower(
        ReplicationFollowerAuditTargetV1::new(
            DatabaseId::from_bytes(uuid(1)).unwrap(),
            2,
            LeadershipEpochV1::new(3).unwrap(),
            ReplicationSourceHoldIdV1::new([seed; 16]).unwrap(),
        )
        .unwrap(),
    )
}
fn principal() -> AuditPrincipalV1 {
    AuditPrincipalV1::new(
        ActorId::new("replication-operator").unwrap(),
        ActorKind::Human,
        CapabilityId::from_bytes(uuid(2)).unwrap(),
        NonZeroU64::MIN,
    )
}
fn record(
    phase: ServiceAuditPhaseV1,
    targets: ServiceAuditTargetsV1,
    link: ServiceAuditLinkV1,
) -> Result<StoredServiceAuditRecordV1, riffdb_storage_api::StorageValueError> {
    StoredServiceAuditRecordV1::from_stored_parts(
        AdministrationSequence::new(9).unwrap(),
        RequestId::from_bytes(uuid(3)).unwrap(),
        Timestamp::new(11, 12).unwrap(),
        ServiceOperationV1::StreamChangelog,
        phase,
        Some(principal()),
        ServiceIngressKindV1::Grpc,
        targets,
        None,
        link,
    )
}
fn append(
    phase: ServiceAuditPhaseV1,
    targets: ServiceAuditTargetsV1,
    link: ServiceAuditLinkV1,
) -> Result<ServiceAuditAppendIntentV1, riffdb_storage_api::StorageValueError> {
    ServiceAuditAppendIntentV1::new(
        RequestId::from_bytes(uuid(3)).unwrap(),
        Timestamp::new(11, 12).unwrap(),
        ServiceOperationV1::StreamChangelog,
        phase,
        principal(),
        ServiceIngressKindV1::Grpc,
        targets,
        None,
        link,
    )
}

#[test]
fn stream_audit_refuses_unselected_targets_and_all_authoritative_result_links() {
    for phase in ServiceAuditPhaseV1::ALL {
        for targets in [
            ServiceAuditTargetsV1::empty(),
            ServiceAuditTargetsV1::new([follower(4)]).unwrap(),
        ] {
            assert!(append(phase, targets.clone(), ServiceAuditLinkV1::None).is_ok());
            assert!(record(phase, targets.clone(), ServiceAuditLinkV1::None).is_ok());
            for link in [
                ServiceAuditLinkV1::Command {
                    commit_sequence: CommitSequence::first(),
                    provenance_id: ProvenanceId::from_bytes(uuid(5)).unwrap(),
                },
                ServiceAuditLinkV1::ControlPlane {
                    administration_sequence: AdministrationSequence::first(),
                },
            ] {
                assert!(append(phase, targets.clone(), link).is_err());
                assert!(record(phase, targets.clone(), link).is_err());
            }
        }
        for targets in [
            vec![follower(4), follower(5)],
            vec![ServiceAuditTargetV1::Commit(CommitSequence::first())],
            vec![
                follower(4),
                ServiceAuditTargetV1::Capability(CapabilityId::from_bytes(uuid(2)).unwrap()),
            ],
        ] {
            let targets = ServiceAuditTargetsV1::new(targets).unwrap();
            assert!(append(phase, targets.clone(), ServiceAuditLinkV1::None).is_err());
            assert!(record(phase, targets, ServiceAuditLinkV1::None).is_err());
        }
    }
}

#[test]
fn stream_audit_vectors_keep_existing_target_generation_selection() {
    use std::fmt::Write;
    let mut vectors = String::new();
    for named in [false, true] {
        for phase in ServiceAuditPhaseV1::ALL {
            let targets = if named {
                ServiceAuditTargetsV1::new([follower(4)]).unwrap()
            } else {
                ServiceAuditTargetsV1::empty()
            };
            let value = record(phase, targets, ServiceAuditLinkV1::None).unwrap();
            let encoded = if named {
                assert!(encode_service_audit_record_v2(&value).is_err());
                encode_service_audit_record_v3(&value).unwrap()
            } else {
                assert!(encode_service_audit_record_v3(&value).is_err());
                encode_service_audit_record_v2(&value).unwrap()
            };
            assert_eq!(
                decode_service_audit_record(encoded.as_bytes())
                    .unwrap()
                    .value(),
                &value
            );
            assert_eq!(value.operation().tag(), 0x3d);
            assert_eq!(
                decode_service_audit_record_v2(encoded.as_bytes()).is_ok(),
                !named
            );
            assert_eq!(
                decode_service_audit_record_v3(encoded.as_bytes()).is_ok(),
                named
            );
            writeln!(
                &mut vectors,
                "{}-{} {}",
                if named { "follower" } else { "tail" },
                phase.tag(),
                encoded
                    .as_bytes()
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>()
            )
            .unwrap();
            for length in 0..encoded.as_bytes().len() {
                assert!(decode_service_audit_record(&encoded.as_bytes()[..length]).is_err());
            }
            let mut trailing = encoded.as_bytes().to_vec();
            trailing.push(0);
            assert!(decode_service_audit_record(&trailing).is_err());
        }
    }
    if let Some(path) = std::env::var_os("RIFFDB_REPLICATION_STREAM_AUDIT_VECTOR_OUTPUT") {
        std::fs::write(path, vectors).unwrap();
    } else {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/replication/stream-audit-v1.hex");
        assert_eq!(std::fs::read_to_string(path).unwrap(), vectors);
    }
}
