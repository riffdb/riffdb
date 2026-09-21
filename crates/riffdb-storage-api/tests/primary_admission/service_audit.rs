//! Fence success cannot borrow another operation's, target's or principal's receipt.
// req: REP-005, STO-012
use super::*;
use riffdb_storage_api::{ServiceAuditAppendIntentV1, StoredServiceAuditRecordV1};
use riffdb_types::{
    ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceAuditTargetV1, ServiceAuditTargetsV1,
    ServiceIngressKindV1, ServiceOperationV1,
};

#[test]
fn primary_fence_service_success_binds_operation_target_and_original_principal() {
    let receipt = fence(point(9, 5, 2), 4, 3, 7).unwrap();
    let exact =
        ServiceAuditTargetsV1::new([ServiceAuditTargetV1::ReplicationFollower(receipt.target())])
            .unwrap();
    for operation in ServiceOperationV1::ALL {
        assert_eq!(
            receipt.matches_service_result(
                operation,
                &exact,
                Some(receipt.principal()),
                receipt.approval_id()
            ),
            operation == ServiceOperationV1::FenceReplicationPrimary
        );
    }
    let operation = ServiceOperationV1::FenceReplicationPrimary;
    for targets in [
        ServiceAuditTargetsV1::empty(),
        ServiceAuditTargetsV1::new([ServiceAuditTargetV1::ReplicationFollower(
            fence(point(9, 5, 2), 4, 3, 8).unwrap().target(),
        )])
        .unwrap(),
        ServiceAuditTargetsV1::new([
            ServiceAuditTargetV1::ReplicationFollower(receipt.target()),
            ServiceAuditTargetV1::Commit(CommitSequence::new(5).unwrap()),
        ])
        .unwrap(),
    ] {
        assert!(!receipt.matches_service_result(
            operation,
            &targets,
            Some(receipt.principal()),
            None
        ));
    }
    assert!(!receipt.matches_service_result(operation, &exact, None, None));
    let wrong = AuditPrincipalV1::new(
        ActorId::new("other-operator").unwrap(),
        receipt.principal().actor_kind(),
        receipt.principal().capability_id(),
        receipt.principal().capability_revision(),
    );
    assert!(!receipt.matches_service_result(operation, &exact, Some(&wrong), None));
    let changed_revision = AuditPrincipalV1::new(
        receipt.principal().principal_id().clone(),
        receipt.principal().actor_kind(),
        receipt.principal().capability_id(),
        NonZeroU64::new(2).unwrap(),
    );
    assert!(!receipt.matches_service_result(operation, &exact, Some(&changed_revision), None));
}

#[test]
fn primary_fence_audit_requires_exact_target_and_link_for_new_or_reconstructed_success() {
    let receipt = fence(point(9, 5, 2), 4, 3, 7).unwrap();
    let operation = ServiceOperationV1::FenceReplicationPrimary;
    let targets =
        ServiceAuditTargetsV1::new([ServiceAuditTargetV1::ReplicationFollower(receipt.target())])
            .unwrap();
    for phase in ServiceAuditPhaseV1::ALL {
        let link = if phase == ServiceAuditPhaseV1::Succeeded {
            ServiceAuditLinkV1::ControlPlane {
                administration_sequence: receipt.administration_sequence(),
            }
        } else {
            ServiceAuditLinkV1::None
        };
        let make = |targets, link| {
            ServiceAuditAppendIntentV1::new(
                RequestId::from_unix_milliseconds_and_random(1_700_000_000_001, [8; 10]).unwrap(),
                receipt.timestamp(),
                operation,
                phase,
                receipt.principal().clone(),
                ServiceIngressKindV1::Grpc,
                targets,
                None,
                link,
            )
        };
        let intent = make(targets.clone(), link).unwrap();
        let stored = |targets, link| {
            StoredServiceAuditRecordV1::from_stored_parts(
                AdministrationSequence::new(10).unwrap(),
                intent.request_id(),
                intent.timestamp(),
                operation,
                phase,
                intent.principal().cloned(),
                intent.ingress(),
                targets,
                None,
                link,
            )
        };
        assert!(stored(targets.clone(), link).is_ok());
        assert!(make(ServiceAuditTargetsV1::empty(), link).is_err());
        assert!(stored(ServiceAuditTargetsV1::empty(), link).is_err());
        if phase == ServiceAuditPhaseV1::Succeeded {
            assert!(make(targets.clone(), ServiceAuditLinkV1::None).is_err());
            assert!(stored(targets.clone(), ServiceAuditLinkV1::None).is_err());
        }
    }
}
