//! Required durable denial independent of optional successful-read auditing.
// req: REP-005
use super::*;
use crate::primary_admission_test_support::{ServiceHarness, run_async};

async fn begin(h: &ServiceHarness, context: &RequestContext, mode: u8) -> BegunInvocation {
    let service = h.service.inner.as_ref();
    let request = if mode == 0 {
        h.observe_budget_request()
    } else {
        h.execute_command_request()
    };
    let active = prepare_active_command(service, context, &request)
        .await
        .unwrap();
    let resolved = &active.resolved;
    let normalized = normalize_command_input(
        resolved.plan(),
        resolved.bundle().bundle().schema(),
        resolved.plan(),
        request.input(),
    )
    .unwrap_or_else(|_| panic!("valid fixture input"));
    let facts = derive_input_command_facts(resolved.plan(), normalized).unwrap();
    let class = if mode == 0 {
        CommandExecutionClass::ReadOnly
    } else {
        CommandExecutionClass::Mutation
    };
    let operation = command_operation(resolved, class, &facts);
    let targets = ServiceAuditTargetMap::execute_command(
        active.catalog_request.lineage().clone(),
        active.catalog_request.version(),
        resolved.plan().command_id(),
    )
    .unwrap();
    if mode == 2 {
        service
            .begin_compound_command_invocation(context, operation, targets)
            .await
            .unwrap()
    } else {
        service
            .begin_invocation(
                context,
                operation,
                targets,
                if mode == 0 {
                    AuditScope::StandardRead
                } else {
                    AuditScope::Intrinsic
                },
            )
            .await
            .unwrap()
    }
}

#[test]
fn primary_fenced_denial_is_durable_without_started_and_after_normal_or_deferred_started() {
    run_async(async {
        for mode in 0..3 {
            let mut h = ServiceHarness::discovery_inventory(1);
            let (context, _cancellation) = h.context(0xa1);
            let begun = begin(&h, &context, mode).await;
            let failure = map_command_admission(
                h.service.inner.as_ref(),
                CommandExecutionAdmissionError::PrimaryFenced,
            );
            let failure = finish_failure(
                h.service.inner.as_ref(),
                &context,
                &begun,
                failure,
                TerminalKind::Ordinary,
            )
            .await;
            assert_eq!(
                failure.public_error().unwrap().kind(),
                PublicErrorKind::PrimaryFenced
            );
            h.stop_coordinator();
            assert_eq!(
                h.audit_phases(0xa1),
                if mode == 0 {
                    vec![ServiceAuditPhaseV1::Denied]
                } else {
                    vec![ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Denied]
                }
            );
            for record in h.audit_records(0xa1) {
                assert_eq!(record.operation(), ServiceOperationV1::ExecuteCommand);
                assert_eq!(record.link(), ServiceAuditLinkV1::None);
                assert_eq!(
                    record.principal().unwrap().capability_id(),
                    context.principal().capability_id()
                );
            }
        }
    });
}

#[test]
fn primary_fenced_denial_audit_outage_returns_unavailable_and_does_not_claim_durability() {
    run_async(async {
        let mut h = ServiceHarness::discovery_inventory(1);
        let (context, _cancellation) = h.context(0xa2);
        let begun = begin(&h, &context, 0).await;
        h.stop_coordinator();
        let failure = finish_failure(
            h.service.inner.as_ref(),
            &context,
            &begun,
            PublicError::primary_fenced().into(),
            TerminalKind::Ordinary,
        )
        .await;
        assert_eq!(
            failure.public_error().unwrap().kind(),
            PublicErrorKind::StorageUnavailable
        );
        assert!(h.audit_phases(0xa2).is_empty());
    });
}
