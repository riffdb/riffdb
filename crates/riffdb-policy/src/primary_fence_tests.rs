// Primary fencing requires its own current permission at both authorization boundaries.
// req: REP-005
use super::*;

fn administrative_grant(
    permissions: Vec<CapabilityPermissionV1>,
    approval: Vec<CapabilityPermissionKindV1>,
) -> CapabilityGrantV1 {
    grant(
        TenantScope::Global,
        PartitionScopeV1::All,
        permissions,
        Vec::new(),
        20,
        approval,
    )
}

#[test]
fn primary_fence_requires_current_distinct_permission_and_exact_database() {
    let administration = CapabilityPermissionKindV1::FenceReplicationPrimary;
    let permission = CapabilityPermissionV1::unparameterized(administration).unwrap();
    let (principal, current, environment) =
        facts(administrative_grant(vec![permission.clone()], Vec::new()));
    let check = |current: &CurrentFacts, selected, now| {
        super::super::primary_fence::evaluate(
            &principal,
            current,
            database_id(),
            &environment,
            timestamp(now),
            selected,
        )
    };
    assert_eq!(check(&current, database_id(), 15), Ok(()));
    let foreign = DatabaseId::from_unix_milliseconds_and_random(9, [9; 10]).unwrap();
    assert!(check(&current, foreign, 15).is_err());
    for now in [9, 20, 21] {
        assert_eq!(
            check(&current, database_id(), now),
            Err(PolicyCode::InactiveOrStaleCapability)
        );
    }
    let mut changed = current.clone();
    changed.activity = CurrentCapabilityActivity::Revoked;
    assert_eq!(
        check(&changed, database_id(), 15),
        Err(PolicyCode::InactiveOrStaleCapability)
    );
    changed = current.clone();
    changed.revision = NonZeroU64::new(2).unwrap();
    assert_eq!(
        check(&changed, database_id(), 15),
        Err(PolicyCode::InactiveOrStaleCapability)
    );
    changed = current.clone();
    changed.audiences.clear();
    assert_eq!(
        check(&changed, database_id(), 15),
        Err(PolicyCode::InactiveOrStaleCapability)
    );
    changed = current.clone();
    for unrelated in [CapabilityPermissionKindV1::ReplicateChangelog, CapabilityPermissionKindV1::AdministerCapabilities] {
        changed.grant = administrative_grant(
            vec![CapabilityPermissionV1::unparameterized(unrelated).unwrap()], Vec::new());
        assert_eq!(check(&changed, database_id(), 15), Err(PolicyCode::MissingPermission));
    }
    changed.grant = administrative_grant(vec![permission], vec![administration]);
    assert_eq!(
        check(&changed, database_id(), 15),
        Err(PolicyCode::ApprovalRequired)
    );
}

#[test]
fn primary_fence_refuses_scoped_and_application_administrators() {
    let permission =
        CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::AdministerCapabilities)
            .unwrap();
    for (tenant, partitions, permissions, refusal) in [
        (
            TenantScope::Tenant(TenantId::new("tenant-a").unwrap()),
            PartitionScopeV1::All,
            vec![permission.clone()],
            PolicyCode::TenantScopeMismatch,
        ),
        (
            TenantScope::Global,
            explicit_partition(),
            vec![permission.clone()],
            PolicyCode::PartitionScopeMismatch,
        ),
        (
            TenantScope::Global,
            PartitionScopeV1::All,
            vec![
                permission,
                CapabilityPermissionV1::ApplicationRoleIdentity(ApplicationRoleHash::from_bytes(
                    [3; 32],
                )),
            ],
            PolicyCode::MissingPermission,
        ),
    ] {
        let (principal, current, environment) = facts(grant(
            tenant,
            partitions,
            permissions,
            Vec::new(),
            20,
            Vec::new(),
        ));
        assert_eq!(
            super::super::primary_fence::evaluate(
                &principal,
                &current,
                database_id(),
                &environment,
                timestamp(15),
                database_id()
            ),
            Err(refusal)
        );
    }
}

#[test]
fn primary_fence_preparation_binds_request_and_reloads_before_every_retry() {
    use riffdb_auth::{CurrentCapabilityResolutionError, PrimaryFenceRequestV1};
    use riffdb_storage_api::ChangelogTransactionSequence;
    use riffdb_types::{
        LeadershipEpochV1, ReplicationFenceOperationId, ReplicationFollowerAuditTargetV1, ReplicationSourceHoldIdV1,
    };
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

    struct ObservedResolver<'a> {
        inner: &'a dyn CurrentCapabilityResolver,
        calls: AtomicUsize,
    }
    impl CurrentCapabilityResolver for ObservedResolver<'_> {
        fn resolve_current(
            &self,
            principal: &AuthenticatedPrincipal,
        ) -> Result<CurrentCapability, CurrentCapabilityResolutionError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.inner.resolve_current(principal)
        }
    }
    struct Clock {
        now: AtomicI64,
        calls: AtomicUsize,
    }
    impl AuthorizationClock for Clock {
        fn now(&self) -> Result<Timestamp, crate::AuthorizationClockError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(timestamp(self.now.load(Ordering::Relaxed)))
        }
    }
    let environment = Environment::new("test").unwrap();
    let fixture = AuthorizationFixture::new(AuthorizationFixtureConfig::new(
        database_id(),
        environment.clone(),
        ActorId::new("operator-secret-name").unwrap(),
        ActorKind::Human,
        Audience::new("grpc").unwrap(),
        AuthorizationFixtureTimes::new(timestamp(100), timestamp(1_000), timestamp(150)),
        administrative_grant(
            vec![
                CapabilityPermissionV1::unparameterized(
                    CapabilityPermissionKindV1::FenceReplicationPrimary,
                )
                .unwrap(),
            ],
            Vec::new(),
        ),
    ))
    .unwrap();
    let inner = fixture.current_capability_resolver();
    let resolver = ObservedResolver {
        inner: &inner,
        calls: AtomicUsize::new(0),
    };
    let clock = Clock {
        now: AtomicI64::new(200),
        calls: AtomicUsize::new(0),
    };
    let authorizer = CurrentAuthorizer::new(
        &resolver,
        &clock,
        &crate::NoopAuthorizationTelemetry,
        database_id(),
        environment.clone(),
    );
    let request_id = RequestId::from_unix_milliseconds_and_random(4, [4; 10]).unwrap();
    let target = ReplicationFollowerAuditTargetV1::new(
        database_id(),
        1,
        LeadershipEpochV1::initial(),
        ReplicationSourceHoldIdV1::new([5; 16]).unwrap(),
    )
    .unwrap();
    let requests = [
        PrimaryFenceRequestV1::new(request_id,
            ReplicationFenceOperationId::from_unix_milliseconds_and_random(7, [7; 10]).unwrap(),
            target, ChangelogTransactionSequence::new(7).unwrap()),
        PrimaryFenceRequestV1::new(request_id,
            ReplicationFenceOperationId::from_unix_milliseconds_and_random(8, [8; 10]).unwrap(),
            target, ChangelogTransactionSequence::new(8).unwrap()),
    ];
    assert_ne!(requests[0], requests[1]);
    assert_eq!(format!("{:?}", requests[0]), "PrimaryFenceRequestV1([REDACTED])");
    for request in requests {
        let PrimaryFenceDecision::Allow(proof) = authorizer
            .authorize_primary_fence(fixture.authenticated_principal(), request)
            .unwrap()
        else {
            panic!("global administrator must prepare the exact request");
        };
        assert_eq!(proof.request(), request);
        assert_eq!(proof.principal(), fixture.authenticated_principal());
        assert_eq!(proof.environment(), &environment);
        assert_eq!(proof.authorized_at(), timestamp(200));
        assert_eq!(
            format!("{proof:?}"),
            "AuthorizedPrimaryFencePreparation([REDACTED])"
        );
        let current = inner
            .resolve_current(fixture.authenticated_principal())
            .unwrap();
        let current = transaction_current_facts(&current).unwrap();
        let (final_preparation, now) = proof
            .reauthorize(&current, timestamp(250))
            .unwrap()
            .into_parts();
        assert_eq!(final_preparation.request(), request);
        assert_eq!(now, timestamp(250));
    }
    assert_eq!(resolver.calls.load(Ordering::Relaxed), 2);
    assert_eq!(clock.calls.load(Ordering::Relaxed), 2);
    clock.now.store(1_000, Ordering::Relaxed);
    for request in requests {
        assert!(matches!(
            authorizer
                .authorize_primary_fence(fixture.authenticated_principal(), request)
                .unwrap(),
            PrimaryFenceDecision::Deny(PolicyCode::InactiveOrStaleCapability)
        ));
    }
    assert_eq!(resolver.calls.load(Ordering::Relaxed), 4);
    assert_eq!(clock.calls.load(Ordering::Relaxed), 4);

    // Final authorization cannot trust the earlier grant, environment or actor,
    // even when initial preparation still succeeds through its old resolver.
    clock.now.store(200, Ordering::Relaxed);
    for request in requests {
        for changed in 0..8 {
            let PrimaryFenceDecision::Allow(proof) = authorizer
                .authorize_primary_fence(fixture.authenticated_principal(), request)
                .unwrap()
            else {
                panic!("initial grant");
            };
            let current = inner
                .resolve_current(fixture.authenticated_principal())
                .unwrap();
            let mut current = transaction_current_facts(&current).unwrap();
            match changed {
                0 => current.activity = CapabilityActivity::Revoked,
                1 => current.revision = NonZeroU64::new(2).unwrap(),
                2 => current.environment = Environment::new("another-environment").unwrap(),
                3 => current.audiences = vec![Audience::new("another-audience").unwrap()],
                4 => current.principal_id = ActorId::new("another-operator").unwrap(),
                5 => {
                    current.grant = administrative_grant(
                        vec![
                            CapabilityPermissionV1::unparameterized(
                                CapabilityPermissionKindV1::ReplicateChangelog,
                            )
                            .unwrap(),
                        ],
                        Vec::new(),
                    )
                }
                6 => {
                    current.grant = administrative_grant(
                        vec![
                            CapabilityPermissionV1::unparameterized(
                                CapabilityPermissionKindV1::FenceReplicationPrimary,
                            )
                            .unwrap(),
                        ],
                        vec![CapabilityPermissionKindV1::FenceReplicationPrimary],
                    )
                }
                _ => current.expires_at = timestamp(250),
            }
            assert!(
                proof.reauthorize(&current, timestamp(250)).is_err(),
                "changed fact {changed}"
            );
        }
    }
}
