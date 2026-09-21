// req: REP-005
use super::*;
use riffdb_auth::AuthenticatedPrincipal;
use riffdb_errors::PublicErrorKind;
use riffdb_policy::{
    AuthorizationClock, AuthorizationClockError, AuthorizationError, CurrentAuthorizer, Decision,
    NoopAuthorizationTelemetry, OperationRequest, ReplicationPromotionDecision,
};
use riffdb_service::{
    CurrentPolicyPort, FollowerPromotionApplication, FollowerPromotionService,
    PromoteFollowerRequest, RequestContext, RequestControl,
};
use riffdb_storage_api::{ChangelogTransactionSequence, ReplicationPromotionPhaseV1};
use riffdb_testkit::authorization::{
    AuthorizationFixture, AuthorizationFixtureConfig, AuthorizationFixtureTimes,
};
use riffdb_types::*;
use std::{
    num::NonZeroU16,
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

struct Policy(AuthorizationFixture);

#[tokio::test]
async fn restricted_retry_audits_foreign_generation_without_reserving_or_retargeting_it() {
    let root = tempfile::tempdir().unwrap();
    let mut storage = RedbMaintenanceStorage::open_for_promotion_recovery(
        root.path().join("follower.redb"),
        root.path().join("backups"),
    )
    .unwrap();
    let (controller, mut receiver) = PromotionController::channel();
    let policy = policy();
    let service =
        FollowerPromotionService::new(database(), environment(), policy.clone(), controller);
    let original = service.promote_follower(context(&policy, 31), request(1));
    let admitted = receiver
        .try_recv()
        .unwrap()
        .audit_retry(&mut storage, request(1), &environment())
        .unwrap()
        .unwrap();
    let retained = admitted.attempt.clone();
    drop(admitted);
    assert!(original.await.is_err());
    let foreign = service.promote_follower(context(&policy, 32), request(2));
    assert!(
        receiver
            .try_recv()
            .unwrap()
            .audit_retry(&mut storage, request(1), &environment())
            .unwrap()
            .is_none()
    );
    assert_eq!(
        foreign.await.unwrap_err().public_error().unwrap().kind(),
        PublicErrorKind::AuthorizationDenied
    );
    let inventory = storage.promotion_receipts().unwrap();
    assert!(inventory.receipts().contains(&retained));
    assert_eq!(
        inventory.request_for(request(1).operation_id()),
        Some(request(1))
    );
    assert_eq!(inventory.receipts().len(), 2);
    assert!(
        inventory
            .receipts()
            .iter()
            .any(|row| row.request() == request(2)
                && row.steps().last() == Some(&Step::Denied(Failure::AuthorizationDenied)))
    );
}

struct Clock;
impl AuthorizationClock for Clock {
    fn now(&self) -> Result<Timestamp, AuthorizationClockError> {
        Ok(time(15))
    }
}
impl CurrentPolicyPort for Policy {
    fn authorize(
        &self,
        _: &AuthenticatedPrincipal,
        _: OperationRequest,
    ) -> Result<Decision, AuthorizationError> {
        Err(AuthorizationError::CurrentCapabilityUnavailable)
    }
    fn authorize_replication_promotion(
        &self,
        principal: &AuthenticatedPrincipal,
        request: PromoteFollowerRequest,
    ) -> Result<ReplicationPromotionDecision, AuthorizationError> {
        CurrentAuthorizer::new(
            &self.0.current_capability_resolver(),
            &Clock,
            &NoopAuthorizationTelemetry,
            database(),
            environment(),
        )
        .authorize_replication_promotion(principal, request)
    }
}
fn time(second: i64) -> Timestamp {
    Timestamp::new(second, 0).unwrap()
}
fn database() -> DatabaseId {
    DatabaseId::from_unix_milliseconds_and_random(10, [1; 10]).unwrap()
}
fn environment() -> Environment {
    Environment::new("promotion-admission").unwrap()
}
fn request(generation: u64) -> PromoteFollowerRequest {
    PromoteFollowerRequest::new(
        ReplicationPromotionOperationId::from_unix_milliseconds_and_random(10, [3; 10]).unwrap(),
        ReplicationFenceOperationId::from_unix_milliseconds_and_random(10, [4; 10]).unwrap(),
        ReplicationFollowerAuditTargetV1::new(
            database(),
            1,
            LeadershipEpochV1::initial(),
            ReplicationSourceHoldIdV1::new([5; 16]).unwrap(),
        )
        .unwrap(),
        ChangelogTransactionSequence::new(generation).unwrap(),
    )
}
fn policy() -> Arc<Policy> {
    let grant = CapabilityGrantV1::new(
        TenantScope::Global,
        PartitionScopeV1::All,
        CapabilityPermissionsV1::new(vec![
            CapabilityPermissionV1::unparameterized(
                CapabilityPermissionKindV1::AdministerCapabilities,
            )
            .unwrap(),
        ])
        .unwrap(),
        Vec::new(),
        NonZeroU16::MIN,
        Vec::new(),
    )
    .unwrap();
    Arc::new(Policy(
        AuthorizationFixture::new(AuthorizationFixtureConfig::new(
            database(),
            environment(),
            ActorId::new("promotion-operator").unwrap(),
            ActorKind::Human,
            Audience::new("riffdb-promotion-test").unwrap(),
            AuthorizationFixtureTimes::new(time(10), time(20), time(14)),
            grant,
        ))
        .unwrap(),
    ))
}
fn context(policy: &Policy, seed: u8) -> RequestContext {
    RequestContext::from_authenticated_grpc(
        RequestId::from_unix_milliseconds_and_random(10, [seed; 10]).unwrap(),
        policy.0.authenticated_principal().clone(),
        RequestControl::new(Instant::now() + Duration::from_secs(30)).0,
        None,
    )
}

#[tokio::test]
async fn promotion_owner_persists_attempt_before_releasing_any_lifecycle_work() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("follower.redb");
    let backups = root.path().join("backups");
    let mut storage = RedbMaintenanceStorage::open_for_promotion_recovery(&path, &backups).unwrap();
    let (controller, mut receiver) = PromotionController::channel();
    let policy = policy();
    let service =
        FollowerPromotionService::new(database(), environment(), policy.clone(), controller);
    let invocation = context(&policy, 6);
    let id = invocation.request_id();
    let mut completion = service.promote_follower(invocation, request(1));
    assert!(storage.promotion_receipts().unwrap().receipts().is_empty());
    assert!(matches!(
        completion
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
    let admitted = receiver
        .try_recv()
        .unwrap()
        .audit(&mut storage, request(1).target(), &environment())
        .unwrap()
        .unwrap();
    assert_eq!(
        admitted.attempt.phase(),
        ReplicationPromotionPhaseV1::Attempted
    );
    assert_eq!(admitted.attempt.request_id(), id);
    assert_eq!(admitted.authorization.request(), request(1));
    assert_eq!(
        admitted.attempt.principal().capability_id(),
        policy.0.authenticated_principal().capability_id()
    );
    assert_eq!(admitted.attempt.timestamp(), time(14));
    assert_eq!(
        storage.promotion_receipts().unwrap().receipts(),
        std::slice::from_ref(&admitted.attempt)
    );
    assert!(
        !path.exists(),
        "external admission opened or changed the database"
    );
    drop(admitted);
    assert_eq!(
        completion.await.unwrap_err().public_error().unwrap().kind(),
        PublicErrorKind::OutcomeUnknown
    );
    drop(storage);
    let reopened = RedbMaintenanceStorage::open_for_promotion_recovery(&path, &backups).unwrap();
    assert_eq!(
        reopened.promotion_receipts().unwrap().receipts()[0].request_id(),
        id
    );
}

#[tokio::test]
async fn promotion_owner_denial_is_durable_before_reply_and_reopens_without_a_writer() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("follower.redb");
    let backups = root.path().join("backups");
    let mut storage = RedbMaintenanceStorage::open_for_promotion_recovery(&path, &backups).unwrap();
    let (controller, mut receiver) = PromotionController::channel();
    let policy = policy();
    let service =
        FollowerPromotionService::new(database(), environment(), policy.clone(), controller);
    let invocation = context(&policy, 7);
    policy.0.revoke_current(time(15)).unwrap();
    let mut completion = service.promote_follower(invocation, request(1));
    assert!(matches!(
        completion
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
    assert!(
        receiver
            .try_recv()
            .unwrap()
            .audit(&mut storage, request(1).target(), &environment())
            .unwrap()
            .is_none()
    );
    let expected = storage.promotion_receipts().unwrap().receipts()[0].clone();
    assert_eq!(
        expected.steps().last(),
        Some(&Step::Denied(Failure::AuthorizationDenied))
    );
    drop(storage);
    let reopened = RedbMaintenanceStorage::open_for_promotion_recovery(&path, &backups).unwrap();
    assert_eq!(
        reopened.promotion_receipts().unwrap().receipts(),
        &[expected]
    );
    assert!(!path.exists());
    assert_eq!(
        completion.await.unwrap_err().public_error().unwrap().kind(),
        PublicErrorKind::AuthorizationDenied
    );
}

#[tokio::test]
async fn promotion_owner_conflict_preserves_original_request_and_both_audit_attempts() {
    let root = tempfile::tempdir().unwrap();
    let mut storage = RedbMaintenanceStorage::open_for_promotion_recovery(
        root.path().join("follower.redb"),
        root.path().join("backups"),
    )
    .unwrap();
    let (controller, mut receiver) = PromotionController::channel();
    let policy = policy();
    let service =
        FollowerPromotionService::new(database(), environment(), policy.clone(), controller);
    let initial = service.promote_follower(context(&policy, 8), request(1));
    let admitted = receiver
        .try_recv()
        .unwrap()
        .audit(&mut storage, request(1).target(), &environment())
        .unwrap()
        .unwrap();
    let original = admitted.attempt.clone();
    drop(admitted);
    assert_eq!(
        initial.await.unwrap_err().public_error().unwrap().kind(),
        PublicErrorKind::OutcomeUnknown
    );
    let conflicting = service.promote_follower(context(&policy, 9), request(2));
    assert!(
        receiver
            .try_recv()
            .unwrap()
            .audit(&mut storage, request(1).target(), &environment())
            .unwrap()
            .is_none()
    );
    assert_eq!(
        conflicting
            .await
            .unwrap_err()
            .public_error()
            .unwrap()
            .kind(),
        PublicErrorKind::IdempotencyKeyReuse
    );
    let inventory = storage.promotion_receipts().unwrap();
    assert_eq!(inventory.receipts().len(), 2);
    assert_eq!(
        inventory.request_for(request(1).operation_id()),
        Some(request(1))
    );
    assert!(inventory.receipts().contains(&original));
    assert!(
        inventory
            .receipts()
            .iter()
            .any(|row| row.steps().last() == Some(&Step::Denied(Failure::SelectionConflict)))
    );
}

#[tokio::test]
async fn promotion_owner_foreign_target_and_storage_corruption_release_no_authority() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("follower.redb");
    let backups = root.path().join("backups");
    let mut storage = RedbMaintenanceStorage::open_for_promotion_recovery(&path, &backups).unwrap();
    let (controller, mut receiver) = PromotionController::channel();
    let policy = policy();
    let service =
        FollowerPromotionService::new(database(), environment(), policy.clone(), controller);
    let foreign = ReplicationFollowerAuditTargetV1::new(
        database(),
        1,
        LeadershipEpochV1::initial(),
        ReplicationSourceHoldIdV1::new([9; 16]).unwrap(),
    )
    .unwrap();
    let completion = service.promote_follower(context(&policy, 10), request(1));
    assert!(
        receiver
            .try_recv()
            .unwrap()
            .audit(&mut storage, foreign, &environment())
            .unwrap()
            .is_none()
    );
    assert_eq!(
        completion.await.unwrap_err().public_error().unwrap().kind(),
        PublicErrorKind::StorageUnavailable
    );
    let before = storage.promotion_receipts().unwrap();
    assert_eq!(
        before.receipts()[0].steps().last(),
        Some(&Step::FailedClosed(Failure::FenceInvalid))
    );
    let malformed = backups.join(".maintenance/replication_promotion/unrecognized");
    std::fs::write(&malformed, b"corrupt inventory").unwrap();
    let completion = service.promote_follower(context(&policy, 11), request(1));
    assert!(
        receiver
            .try_recv()
            .unwrap()
            .audit(&mut storage, request(1).target(), &environment())
            .is_err()
    );
    assert_eq!(
        completion.await.unwrap_err().public_error().unwrap().kind(),
        PublicErrorKind::StorageUnavailable
    );
    assert!(!path.exists());
    std::fs::remove_file(malformed).unwrap();
    assert_eq!(storage.promotion_receipts().unwrap(), before);
}

#[tokio::test]
async fn promotion_handoff_is_bounded_and_receiver_loss_preserves_uncertainty() {
    let (controller, receiver) = PromotionController::channel();
    let policy = policy();
    let service =
        FollowerPromotionService::new(database(), environment(), policy.clone(), controller);
    let accepted = service.promote_follower(context(&policy, 12), request(1));
    let refused = service.promote_follower(context(&policy, 13), request(1));
    assert_eq!(
        refused.await.unwrap_err().public_error().unwrap().kind(),
        PublicErrorKind::StorageUnavailable
    );
    drop(receiver);
    assert_eq!(
        accepted.await.unwrap_err().public_error().unwrap().kind(),
        PublicErrorKind::OutcomeUnknown
    );
}

#[tokio::test]
async fn dropping_promotion_response_preserves_accepted_audit_custody() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("follower.redb");
    let backups = root.path().join("backups");
    let mut storage = RedbMaintenanceStorage::open_for_promotion_recovery(&path, &backups).unwrap();
    let (controller, mut receiver) = PromotionController::channel();
    let policy = policy();
    let service =
        FollowerPromotionService::new(database(), environment(), policy.clone(), controller);
    let response = service.promote_follower(context(&policy, 33), request(1));
    drop(response);
    let mut admitted = receiver
        .try_recv()
        .unwrap()
        .audit(&mut storage, request(1).target(), &environment())
        .unwrap()
        .unwrap();
    assert_eq!(admitted.authorization.request(), request(1));
    admitted
        .attempt
        .advance(Step::FailedClosed(Failure::FenceUnavailable))
        .unwrap();
    storage
        .persist_promotion_receipt(&admitted.attempt)
        .unwrap();
    let retained = admitted.attempt.clone();
    admitted
        .completion
        .complete(Err(FollowerPromotionPortError::Unavailable));
    drop(storage);
    let reopened = RedbMaintenanceStorage::open_for_promotion_recovery(&path, &backups).unwrap();
    assert_eq!(
        reopened.promotion_receipts().unwrap().receipts(),
        &[retained]
    );
    assert!(!path.exists(), "external audit grants no database writer");
}

#[tokio::test]
async fn promotion_policy_unavailability_is_audited_without_retargeting_an_operation() {
    let root = tempfile::tempdir().unwrap();
    let mut storage = RedbMaintenanceStorage::open_for_promotion_recovery(
        root.path().join("follower.redb"),
        root.path().join("backups"),
    )
    .unwrap();
    let (controller, mut receiver) = PromotionController::channel();
    let policy = policy();
    let service =
        FollowerPromotionService::new(database(), environment(), policy.clone(), controller);
    policy.0.make_current_unavailable().unwrap();
    for (seed, generation) in [(14, 1), (15, 2)] {
        let completion = service.promote_follower(context(&policy, seed), request(generation));
        assert!(
            receiver
                .try_recv()
                .unwrap()
                .audit(&mut storage, request(1).target(), &environment())
                .unwrap()
                .is_none()
        );
        assert_eq!(
            completion.await.unwrap_err().public_error().unwrap().kind(),
            PublicErrorKind::StorageUnavailable
        );
    }
    let inventory = storage.promotion_receipts().unwrap();
    assert_eq!(inventory.receipts().len(), 2);
    assert_eq!(
        inventory.request_for(request(1).operation_id()),
        Some(request(1))
    );
    assert_eq!(
        inventory.receipts()[0].steps().last(),
        Some(&Step::FailedClosed(Failure::StorageUnavailable))
    );
    assert_eq!(
        inventory.receipts()[1].steps().last(),
        Some(&Step::Denied(Failure::SelectionConflict))
    );
}
