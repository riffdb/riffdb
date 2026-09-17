#![forbid(unsafe_code)]
//! Closed lifecycle target and result-link compatibility.
// req: REP-006, STO-012

use riffdb_storage_api::{
    AuditPrincipalV1, ServiceAuditAppendIntentV1, StoredServiceAuditRecordV1,
};
use riffdb_types::*;

fn target() -> ServiceAuditTargetV1 {
    ServiceAuditTargetV1::ReplicationFollower(
        ReplicationFollowerAuditTargetV1::new(
            DatabaseId::from_unix_milliseconds_and_random(1, [1; 10]).unwrap(),
            1,
            LeadershipEpochV1::initial(),
            ReplicationSourceHoldIdV1::new([1; 16]).unwrap(),
        )
        .unwrap(),
    )
}

fn intent(
    operation: ServiceOperationV1,
    phase: ServiceAuditPhaseV1,
    targets: ServiceAuditTargetsV1,
    link: ServiceAuditLinkV1,
) -> Result<ServiceAuditAppendIntentV1, riffdb_storage_api::StorageValueError> {
    ServiceAuditAppendIntentV1::new(
        RequestId::from_unix_milliseconds_and_random(2, [2; 10]).unwrap(),
        Timestamp::new(3, 0).unwrap(),
        operation,
        phase,
        AuditPrincipalV1::new(
            ActorId::new("operator").unwrap(),
            ActorKind::Human,
            CapabilityId::from_unix_milliseconds_and_random(4, [4; 10]).unwrap(),
            std::num::NonZeroU64::MIN,
        ),
        ServiceIngressKindV1::Grpc,
        targets,
        None,
        link,
    )
}

#[test]
fn lifecycle_audit_requires_one_exact_target_and_a_linked_success_in_both_constructors() {
    for operation in [
        ServiceOperationV1::RegisterFollower,
        ServiceOperationV1::RetireFollower,
    ] {
        for phase in ServiceAuditPhaseV1::ALL {
            let targets = ServiceAuditTargetsV1::new([target()]).unwrap();
            let link = if phase == ServiceAuditPhaseV1::Succeeded {
                ServiceAuditLinkV1::ControlPlane {
                    administration_sequence: AdministrationSequence::new(5).unwrap(),
                }
            } else {
                ServiceAuditLinkV1::None
            };
            let valid = intent(operation, phase, targets.clone(), link).unwrap();
            let stored = |targets, link| {
                StoredServiceAuditRecordV1::from_stored_parts(
                    AdministrationSequence::new(10).unwrap(),
                    valid.request_id(),
                    valid.timestamp(),
                    operation,
                    phase,
                    valid.principal().cloned(),
                    valid.ingress(),
                    targets,
                    None,
                    link,
                )
            };
            assert!(stored(targets.clone(), link).is_ok());
            for wrong in [
                ServiceAuditTargetsV1::empty(),
                ServiceAuditTargetsV1::new([ServiceAuditTargetV1::Commit(
                    CommitSequence::new(1).unwrap(),
                )])
                .unwrap(),
                ServiceAuditTargetsV1::new([
                    target(),
                    ServiceAuditTargetV1::Commit(CommitSequence::new(1).unwrap()),
                ])
                .unwrap(),
            ] {
                assert!(intent(operation, phase, wrong.clone(), link).is_err());
                assert!(stored(wrong, link).is_err());
            }
            if phase == ServiceAuditPhaseV1::Succeeded {
                assert!(
                    intent(operation, phase, targets.clone(), ServiceAuditLinkV1::None).is_err()
                );
                assert!(stored(targets, ServiceAuditLinkV1::None).is_err());
            }
        }
    }
}

#[test]
fn lifecycle_operations_use_v3_and_cannot_be_encoded_with_frozen_audit_generations() {
    for operation in [
        ServiceOperationV1::RegisterFollower,
        ServiceOperationV1::RetireFollower,
    ] {
        let input = intent(
            operation,
            ServiceAuditPhaseV1::Denied,
            ServiceAuditTargetsV1::new([target()]).unwrap(),
            ServiceAuditLinkV1::None,
        )
        .unwrap();
        let record =
            StoredServiceAuditRecordV1::from_intent(AdministrationSequence::first(), &input);
        assert!(riffdb_storage_api::encode_service_audit_record_v2(&record).is_err());
        let bytes = riffdb_storage_api::encode_service_audit_record_v3(&record).unwrap();
        assert_eq!(
            riffdb_storage_api::decode_service_audit_record(bytes.as_bytes())
                .unwrap()
                .value(),
            &record
        );
        assert!(riffdb_storage_api::decode_service_audit_record_v1(bytes.as_bytes()).is_err());
        assert!(riffdb_storage_api::decode_service_audit_record_v2(bytes.as_bytes()).is_err());
    }
}
