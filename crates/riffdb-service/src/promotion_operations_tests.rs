// req: REP-005
use super::*;
use crate::primary_admission_test_support::{
    HarnessPolicy, ServiceHarness, database_id, environment, request_id, run_async,
};
use crate::{PortCompletionSender, port_completion_channel};
use riffdb_errors::PublicErrorKind;
use riffdb_policy::{AuthorizationError, Decision, OperationRequest};
use riffdb_storage_api::*;
use riffdb_types::*;
use std::sync::Mutex;
use std::task::{Context, Poll, Waker};

#[derive(Default)]
struct Owner {
    accepted: Mutex<
        Vec<(
            FollowerPromotionSubmission,
            PortCompletionSender<PromoteFollowerResult, FollowerPromotionPortError>,
        )>,
    >,
}
impl FollowerPromotionCoordinatorPort for Owner {
    fn submit(
        &self,
        submission: FollowerPromotionSubmission,
    ) -> Result<PortReceipt<PromoteFollowerResult, FollowerPromotionPortError>, PortAdmissionError>
    {
        let (sender, receipt) = port_completion_channel();
        self.accepted.lock().unwrap().push((submission, sender));
        Ok(receipt)
    }
}
impl Owner {
    fn take(
        &self,
    ) -> (
        FollowerPromotionSubmission,
        PortCompletionSender<PromoteFollowerResult, FollowerPromotionPortError>,
    ) {
        let mut accepted = self.accepted.lock().unwrap();
        assert_eq!(accepted.len(), 1);
        accepted.pop().unwrap()
    }
}

struct Policy {
    delegate: Arc<HarnessPolicy>,
    wrong_request: bool,
    unavailable: bool,
}
impl CurrentPolicyPort for Policy {
    fn authorize(
        &self,
        principal: &AuthenticatedPrincipal,
        request: OperationRequest,
    ) -> Result<Decision, AuthorizationError> {
        self.delegate.authorize(principal, request)
    }
    fn authorize_replication_promotion(
        &self,
        principal: &AuthenticatedPrincipal,
        request: PromoteFollowerRequest,
    ) -> Result<ReplicationPromotionDecision, AuthorizationError> {
        if self.unavailable {
            return Err(AuthorizationError::ClockUnavailable);
        }
        self.delegate.authorize_replication_promotion(
            principal,
            if self.wrong_request {
                selection(42)
            } else {
                request
            },
        )
    }
}

fn selection(seed: u8) -> PromoteFollowerRequest {
    PromoteFollowerRequest::new(
        ReplicationPromotionOperationId::from_unix_milliseconds_and_random(10, [seed; 10]).unwrap(),
        ReplicationFenceOperationId::from_unix_milliseconds_and_random(10, [7; 10]).unwrap(),
        ReplicationFollowerAuditTargetV1::new(
            database_id(),
            1,
            LeadershipEpochV1::initial(),
            ReplicationSourceHoldIdV1::new([8; 16]).unwrap(),
        )
        .unwrap(),
        ChangelogTransactionSequence::new(1).unwrap(),
    )
}

fn service(h: &ServiceHarness, owner: Arc<Owner>) -> FollowerPromotionService {
    FollowerPromotionService::new(database_id(), environment(), h.policy.clone(), owner)
}

fn checked_result(
    request: PromoteFollowerRequest,
    principal: &AuthenticatedPrincipal,
) -> PromoteFollowerResult {
    PromoteFollowerResult::from_committed_record(&checked_record(request, principal), false)
        .unwrap()
}

fn checked_record(
    request: PromoteFollowerRequest,
    principal: &AuthenticatedPrincipal,
) -> StoredPromotionAdministrationV1 {
    let lineage = ChangelogLineageV3::new_with_catalog(
        database_id(),
        1,
        LeadershipEpochV1::initial(),
        AuthoritativeStateCatalogV2.digest(),
    )
    .unwrap();
    let point = |sequence: u64, application: Option<u64>, administration: Option<u64>| {
        ChangelogHistoryPointV3::new(
            ChangelogTransactionSequence::new(sequence).unwrap(),
            [sequence as u8; 32],
            DualFrontier::new(
                application.map(|v| CommitSequence::new(v).unwrap()),
                administration.map(|v| AdministrationSequence::new(v).unwrap()),
            ),
        )
    };
    let anchor = point(1, None, None);
    let applied = point(2, Some(1), None);
    let actor = AuditPrincipalV1::new(
        principal.principal_id().clone(),
        principal.actor_kind(),
        principal.capability_id(),
        principal.capability_revision(),
    );
    let timestamp = principal.authenticated_at();
    let fence = StoredPrimaryFenceAdministrationV1::new(
        AdministrationSequence::new(2).unwrap(),
        timestamp,
        request.fence_operation_id(),
        request_id(4),
        actor.clone(),
        None,
        request.target(),
        request.generation(),
        point(3, Some(1), Some(1)),
    )
    .unwrap();
    let history =
        ChangelogHistoryStateV3::new(lineage, anchor, point(4, Some(1), Some(2)), anchor).unwrap();
    let choice = ReplicationPromotionSelectionV1::new(
        request,
        ReplicationFollowerStateV3::attached(lineage, applied, Some(anchor)).unwrap(),
        PrimaryFenceSourceEvidenceV1::new(fence, applied, history).unwrap(),
    )
    .unwrap();
    let mut attempt =
        ReplicationPromotionReceiptV1::attempted(request, request_id(5), actor, None, timestamp);
    for phase in [
        ReplicationPromotionPhaseV1::Draining,
        ReplicationPromotionPhaseV1::Offline,
    ] {
        attempt
            .advance(ReplicationPromotionStepV1::Phase(phase))
            .unwrap();
    }
    attempt.record_selection(choice).unwrap();
    attempt
        .advance(ReplicationPromotionStepV1::Phase(
            ReplicationPromotionPhaseV1::CutoverPending,
        ))
        .unwrap();
    StoredPromotionAdministrationV1::new(attempt, timestamp, ServiceIngressKindV1::Grpc).unwrap()
}

#[test]
fn promotion_completion_releases_old_policy_and_owner_before_waiting() {
    run_async(async {
        let mut h = ServiceHarness::operations();
        let owner = Arc::new(Owner::default());
        let weak_owner = Arc::downgrade(&owner);
        let policy = Arc::new(Policy {
            delegate: h.policy.clone(),
            wrong_request: false,
            unavailable: false,
        });
        let weak_policy = Arc::downgrade(&policy);
        let service =
            FollowerPromotionService::new(database_id(), environment(), policy, owner.clone());
        let (context, cancellation) = h.context(8);
        let result = checked_result(selection(1), context.principal());
        let mut completion = service.promote_follower(context, selection(1));
        let (submitted, sender) = owner.take();
        let (request, id, principal, ingress, admission) = submitted.into_parts();
        assert_eq!(request, selection(1));
        assert_eq!(id, request_id(8));
        assert_eq!(ingress, ServiceIngressKindV1::Grpc);
        let FollowerPromotionAdmission::Authorized(preparation) = admission else {
            panic!("expected exact initial authority")
        };
        assert_eq!(preparation.request(), request);
        assert_eq!(preparation.principal(), &principal);
        drop(preparation);
        drop(service);
        drop(owner);
        assert!(
            weak_policy.upgrade().is_none(),
            "waiting response retained the old policy graph"
        );
        assert!(
            weak_owner.upgrade().is_none(),
            "waiting response retained the admitting controller"
        );
        assert!(matches!(
            completion
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        cancellation.cancel();
        sender.complete(Ok(result));
        assert_eq!(
            completion.await.unwrap(),
            result,
            "late cancellation discarded an accepted outcome"
        );
        h.stop_coordinator();
        assert!(
            h.audit_records(8).is_empty(),
            "promotion used the ordinary local database audit writer"
        );
    });
}

#[test]
fn promotion_denial_waits_for_external_owner_and_never_grants_drain() {
    run_async(async {
        for mcp in [false, true] {
            let mut h = ServiceHarness::operations();
            let owner = Arc::new(Owner::default());
            let service = service(&h, owner.clone());
            let (context, _cancel) = h.context(9);
            let context = if mcp {
                RequestContext::from_authenticated_mcp_http(
                    context.request_id(),
                    context.principal().clone(),
                    crate::RequestControl::new(context.control().deadline()).0,
                    None,
                )
            } else {
                h.revoke_policy();
                context
            };
            let mut completion = service.promote_follower(context, selection(1));
            let (submitted, sender) = owner.take();
            assert!(matches!(
                submitted.into_parts().4,
                FollowerPromotionAdmission::Denied
            ));
            assert!(matches!(
                completion
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop())),
                Poll::Pending
            ));
            sender.complete(Err(FollowerPromotionPortError::AuthorizationDenied));
            assert_eq!(
                completion.await.unwrap_err().public_error().unwrap().kind(),
                PublicErrorKind::AuthorizationDenied
            );
            h.stop_coordinator();
            assert!(h.audit_records(9).is_empty());
        }
    });
}

#[test]
fn promotion_wrong_preparation_and_clock_failure_are_audited_without_authority() {
    run_async(async {
        for wrong_request in [false, true] {
            let h = ServiceHarness::operations();
            let owner = Arc::new(Owner::default());
            let policy = Arc::new(Policy {
                delegate: h.policy.clone(),
                wrong_request,
                unavailable: !wrong_request,
            });
            let service =
                FollowerPromotionService::new(database_id(), environment(), policy, owner.clone());
            let (context, _cancel) = h.context(10);
            let completion = service.promote_follower(context, selection(1));
            let (submitted, sender) = owner.take();
            assert!(matches!(
                submitted.into_parts().4,
                FollowerPromotionAdmission::Unavailable
            ));
            sender.complete(Err(FollowerPromotionPortError::Unavailable));
            assert_eq!(
                completion.await.unwrap_err().public_error().unwrap().kind(),
                PublicErrorKind::StorageUnavailable
            );
        }
    });
}

#[test]
fn promotion_cancellation_before_submission_releases_all_non_durable_work() {
    run_async(async {
        let h = ServiceHarness::operations();
        let owner = Arc::new(Owner::default());
        let service = service(&h, owner.clone());
        let (context, cancellation) = h.context(11);
        cancellation.cancel();
        assert!(matches!(
            service.promote_follower(context, selection(1)).await,
            Err(ServiceFailure::Cancelled)
        ));
        assert!(owner.accepted.lock().unwrap().is_empty());
    });
}

#[test]
fn promotion_lost_completion_and_substituted_success_never_report_success() {
    run_async(async {
        for substituted in [false, true] {
            let h = ServiceHarness::operations();
            let owner = Arc::new(Owner::default());
            let service = service(&h, owner.clone());
            let (context, _cancel) = h.context(12);
            let wrong = checked_result(selection(2), context.principal());
            let completion = service.promote_follower(context, selection(1));
            let (_submitted, sender) = owner.take();
            if substituted {
                sender.complete(Ok(wrong));
            } else {
                drop(sender);
            }
            assert_eq!(
                completion.await.unwrap_err().public_error().unwrap().kind(),
                PublicErrorKind::OutcomeUnknown
            );
        }
    });
}

#[test]
fn promotion_denied_submission_cannot_release_even_a_matching_success() {
    run_async(async {
        let h = ServiceHarness::operations();
        let owner = Arc::new(Owner::default());
        let service = service(&h, owner.clone());
        let (context, _cancel) = h.context(13);
        let result = checked_result(selection(1), context.principal());
        h.revoke_policy();
        let completion = service.promote_follower(context, selection(1));
        let (submitted, sender) = owner.take();
        assert!(matches!(
            submitted.into_parts().4,
            FollowerPromotionAdmission::Denied
        ));
        sender.complete(Ok(result));
        assert_eq!(
            completion.await.unwrap_err().public_error().unwrap().kind(),
            PublicErrorKind::OutcomeUnknown
        );
    });
}

#[test]
fn source_promotion_retry_rechecks_authority_after_durable_start() {
    run_async(async {
        for revoke_after_start in [false, true] {
            let mut h = ServiceHarness::operations();
            let (context, _cancel) = h.context(0xd1);
            let request = selection(1);
            let record = checked_record(request, context.principal());
            let source = crate::SourcePromotionRetryService::new(h.service.clone(), record);
            if revoke_after_start {
                h.deny_after_next_policy_allows(1);
            } else {
                h.revoke_policy();
            }
            let error = source.promote_follower(context, request).await.unwrap_err();
            assert_eq!(
                error.public_error().unwrap().kind(),
                PublicErrorKind::AuthorizationDenied
            );
            drop(source);
            h.stop_coordinator();
            assert_eq!(
                h.audit_phases(0xd1),
                if revoke_after_start {
                    vec![ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Denied]
                } else {
                    vec![ServiceAuditPhaseV1::Denied]
                }
            );
            for row in h.audit_records(0xd1) {
                assert_eq!(row.operation(), ServiceOperationV1::PromoteFollower);
                assert_eq!(row.link(), ServiceAuditLinkV1::None);
                assert_eq!(
                    row.targets().as_slice(),
                    &[ServiceAuditTargetV1::ReplicationFollower(request.target())]
                );
            }
        }
    });
}

#[test]
fn source_promotion_retry_cancelled_before_start_records_only_cancellation() {
    run_async(async {
        let mut h = ServiceHarness::operations();
        let (context, cancel) = h.context(0xd2);
        let request = selection(1);
        let source = crate::SourcePromotionRetryService::new(
            h.service.clone(),
            checked_record(request, context.principal()),
        );
        cancel.cancel();
        assert!(matches!(
            source.promote_follower(context, request).await,
            Err(ServiceFailure::Cancelled)
        ));
        drop(source);
        h.stop_coordinator();
        assert_eq!(h.audit_phases(0xd2), vec![ServiceAuditPhaseV1::Cancelled]);
    });
}
