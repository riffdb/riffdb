// req: REP-005
use super::*;
use crate::primary_admission_test_support::{ServiceHarness, run_async};
use riffdb_errors::PublicErrorKind;
use riffdb_types::ServiceOperationV1;

#[test]
fn primary_fenced_control_denial_requires_terminal_audit() {
    run_async(async {
        for audit_outage in [false, true] {
            let mut h = ServiceHarness::operations();
            let (context, _cancellation) = h.context(0xa3);
            let request = h.create_capability_request();
            let capability = context.principal().capability_id();
            let target = CapabilityCreateTargetFacts::new(
                context.request_id(),
                capability,
                request.requested().clone(),
            );
            let targets = ServiceAuditTargetMap::create_capability(capability).unwrap();
            let begun = h
                .service
                .inner
                .begin_capability_mutation(
                    &context,
                    OperationRequest::create_capability(target),
                    targets,
                )
                .await
                .unwrap();
            if audit_outage {
                h.stop_coordinator();
            }
            let failure = finish_control_plane_admission(
                h.service.inner.as_ref(),
                &context,
                &begun,
                ControlPlaneExecutionAdmissionError::PrimaryFenced,
            )
            .await;
            assert_eq!(
                failure.public_error().unwrap().kind(),
                if audit_outage {
                    PublicErrorKind::StorageUnavailable
                } else {
                    PublicErrorKind::PrimaryFenced
                }
            );
            if !audit_outage {
                h.stop_coordinator();
            }
            assert_eq!(
                h.audit_phases(0xa3),
                if audit_outage {
                    vec![ServiceAuditPhaseV1::Started]
                } else {
                    vec![ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Denied]
                }
            );
            for record in h.audit_records(0xa3) {
                assert_eq!(record.operation(), ServiceOperationV1::CreateCapability);
                assert_eq!(record.link(), ServiceAuditLinkV1::None);
                assert_eq!(
                    record.principal().unwrap().capability_id(),
                    context.principal().capability_id()
                );
            }
        }
    });
}
