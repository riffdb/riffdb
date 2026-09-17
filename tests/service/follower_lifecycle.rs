//! Deterministic admission checks before any lifecycle mutation can be accepted.
// req: REP-006, REP-002, SEC-003
use super::support::{ServiceHarness, run_async};
use riffdb_errors::PublicErrorKind;
use riffdb_service::{AdministrationApplication, RegisterFollowerRequest, RetireFollowerRequest};
use riffdb_types::{ServiceAuditPhaseV1, ServiceAuditTargetV1};

fn request() -> RegisterFollowerRequest {
    RegisterFollowerRequest::new(
        riffdb_types::ReplicationFollowerAuditTargetV1::new(
            super::support::database_id(),
            1,
            riffdb_types::LeadershipEpochV1::initial(),
            riffdb_types::ReplicationSourceHoldIdV1::new([0x79; 16]).unwrap(),
        )
        .unwrap(),
        riffdb_auth::FollowerHoldBudget::new(10).unwrap(),
        None,
    )
}

#[test]
fn lifecycle_authority_is_rechecked_after_started_and_before_coordinator_submission() {
    run_async(async {
        let mut harness = ServiceHarness::operations();
        harness.deny_after_next_policy_allows(1);
        let (context, _) = harness.context(0xd1);
        let error = harness
            .service
            .register_follower(context, request())
            .await
            .unwrap_err();
        assert_eq!(
            error.public_error().unwrap().kind(),
            PublicErrorKind::AuthorizationDenied
        );
        assert_eq!(harness.policy.calls(), 2);
        harness.stop_coordinator();
        let records = harness.audit_records(0xd1);
        assert_eq!(
            records.iter().map(|r| r.phase()).collect::<Vec<_>>(),
            [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Denied]
        );
        for record in records {
            assert_eq!(
                record.targets().as_slice(),
                &[ServiceAuditTargetV1::ReplicationFollower(
                    request().target()
                )]
            );
            assert_eq!(record.link(), riffdb_types::ServiceAuditLinkV1::None);
        }
    });
}

#[test]
fn lifecycle_precancel_and_follower_refusal_never_submit_a_registration() {
    run_async(async {
        let mut harness = ServiceHarness::operations();
        let follower = harness.follower_service();
        let (context, cancellation) = harness.context(0xd2);
        cancellation.cancel();
        let failure = harness
            .service
            .register_follower(context, request())
            .await
            .unwrap_err();
        assert!(matches!(failure, riffdb_service::ServiceFailure::Cancelled));
        let (context, _) = harness.context(0xd3);
        let failure = follower
            .register_follower(context, request())
            .await
            .unwrap_err();
        assert_eq!(
            failure.public_error().unwrap().kind(),
            PublicErrorKind::FollowerMode
        );
        let (context, _) = harness.context(0xd4);
        let failure = follower
            .retire_follower(
                context,
                RetireFollowerRequest::new(
                    request().target(),
                    riffdb_auth::ChangelogTransactionSequence::new(1).unwrap(),
                ),
            )
            .await
            .unwrap_err();
        assert_eq!(
            failure.public_error().unwrap().kind(),
            PublicErrorKind::FollowerMode
        );
        assert_eq!(harness.policy.calls(), 0);
        drop(follower);
        harness.stop_coordinator();
        assert_eq!(harness.audit_phases(0xd2), [ServiceAuditPhaseV1::Cancelled]);
        assert!(harness.audit_phases(0xd3).is_empty());
        assert!(harness.audit_phases(0xd4).is_empty());
    });
}

#[test]
fn lifecycle_mcp_ingress_is_denied_and_audited_before_current_policy() {
    run_async(async {
        let mut harness = ServiceHarness::operations();
        for (seed, retirement) in [(0xd5, false), (0xd6, true)] {
            let (old, _) = harness.context(seed);
            let context = riffdb_service::RequestContext::new(
                old.request_id(),
                old.principal().clone(),
                riffdb_types::ServiceIngressKindV1::McpHttp,
                riffdb_policy::UntrustedInvocationClaims::new(None, None, None, None, None),
                riffdb_service::RequestControl::new(
                    std::time::Instant::now() + std::time::Duration::from_secs(30),
                )
                .0,
                None,
            );
            let result = if retirement {
                harness
                    .service
                    .retire_follower(
                        context,
                        RetireFollowerRequest::new(
                            request().target(),
                            riffdb_auth::ChangelogTransactionSequence::new(1).unwrap(),
                        ),
                    )
                    .await
            } else {
                harness.service.register_follower(context, request()).await
            };
            assert_eq!(
                result.unwrap_err().public_error().unwrap().kind(),
                PublicErrorKind::AuthorizationDenied
            );
        }
        assert_eq!(harness.policy.calls(), 0);
        harness.stop_coordinator();
        for seed in [0xd5, 0xd6] {
            assert_eq!(harness.audit_phases(seed), [ServiceAuditPhaseV1::Denied]);
            assert_eq!(
                harness.audit_records(seed)[0].targets().as_slice(),
                &[ServiceAuditTargetV1::ReplicationFollower(
                    request().target()
                )]
            );
        }
    });
}
