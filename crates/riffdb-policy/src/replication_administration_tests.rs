// Lifecycle authority is distinct from permission to consume replication bytes.
// req: REP-006
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
fn follower_lifecycle_requires_current_global_administration_and_exact_database() {
    let administration = CapabilityPermissionKindV1::AdministerCapabilities;
    let permission = CapabilityPermissionV1::unparameterized(administration).unwrap();
    let (principal, current, environment) =
        facts(administrative_grant(vec![permission.clone()], Vec::new()));
    let check = |current: &CurrentFacts, selected, now| {
        super::super::replication_administration::evaluate(
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
    changed.grant = administrative_grant(
        vec![
            CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ReplicateChangelog)
                .unwrap(),
        ],
        Vec::new(),
    );
    assert_eq!(
        check(&changed, database_id(), 15),
        Err(PolicyCode::MissingPermission)
    );
    changed.grant = administrative_grant(vec![permission], vec![administration]);
    assert_eq!(
        check(&changed, database_id(), 15),
        Err(PolicyCode::ApprovalRequired)
    );
}

#[test]
fn follower_lifecycle_refuses_scoped_and_application_administrators() {
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
            super::super::replication_administration::evaluate(
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
fn follower_lifecycle_preparation_binds_request_and_reloads_before_every_retry() {
    use riffdb_auth::{CurrentCapabilityResolutionError, ReplicationAdministrationRequestV1};
    use riffdb_storage_api::{ChangelogTransactionSequence, FollowerHoldBudget};
    use riffdb_types::{
        LeadershipEpochV1, ReplicationFollowerAuditTargetV1, ReplicationSourceHoldIdV1,
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
                    CapabilityPermissionKindV1::AdministerCapabilities,
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
        ReplicationAdministrationRequestV1::register(
            request_id,
            target,
            FollowerHoldBudget::new(3).unwrap(),
            CommitSequence::new(9),
        ),
        ReplicationAdministrationRequestV1::retire(
            request_id,
            target,
            ChangelogTransactionSequence::new(7).unwrap(),
        ),
    ];
    for request in requests {
        let ReplicationAdministrationDecision::Allow(proof) = authorizer
            .authorize_replication_administration(fixture.authenticated_principal(), request)
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
            "AuthorizedReplicationAdministrationPreparation([REDACTED])"
        );
    }
    assert_eq!(resolver.calls.load(Ordering::Relaxed), 2);
    assert_eq!(clock.calls.load(Ordering::Relaxed), 2);
    clock.now.store(1_000, Ordering::Relaxed);
    for request in requests {
        assert!(matches!(
            authorizer
                .authorize_replication_administration(fixture.authenticated_principal(), request)
                .unwrap(),
            ReplicationAdministrationDecision::Deny(PolicyCode::InactiveOrStaleCapability)
        ));
    }
    assert_eq!(resolver.calls.load(Ordering::Relaxed), 4);
    assert_eq!(clock.calls.load(Ordering::Relaxed), 4);
}
