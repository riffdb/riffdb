//! Production actor + policy + drained redb fence; no remote promotion claim.
// req: REP-005, REC-001, STO-012
use super::*;
use riffdb_auth::{
    AuthenticatedPrincipal, AuthenticationClock, AuthenticationClockError, AuthenticationContext,
    CapabilityAuthenticator, CapabilityDigestKeyProvider, CapabilityReaderCurrentResolver,
    CredentialAuthenticator, NoopAuthenticationTelemetry, OpaqueCredential, RawCapabilityToken,
};
use riffdb_policy::{AuthorizedPrimaryFencePreparation, PrimaryFenceDecision};
use riffdb_storage_api::{CapabilityLookupResult, StorageError, StoredCapabilityRecordV1};
use riffdb_storage_api::{
    PrimaryFenceRequestV1, PrimaryFenceResultV1, ReplicationPrimaryAdmissionReadPort,
};
use riffdb_storage_redb::RedbCommitProfile;
use riffdb_types::ReplicationFenceOperationId;

const FENCE_TOKEN: &[u8] = b"AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";
fn digest_keys() -> CapabilityDigestKeyProvider {
    CapabilityDigestKeyProvider::parse_document(
        b"riffdb-capability-digest-keys-v1\n1:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n",
    ).unwrap()
}
struct FenceClock;
impl AuthenticationClock for FenceClock {
    fn now(&self) -> Result<Timestamp, AuthenticationClockError> {
        Ok(timestamp(BASE_SECONDS + 20))
    }
}
// Initial preparation uses a captured authenticated record; final authorization
// always reloads production redb state inside the drained fence transaction.
struct FenceFixture {
    principal: AuthenticatedPrincipal,
    record: StoredCapabilityRecordV1,
}
impl CapabilityReader for FenceFixture {
    fn read_capability(
        &self,
        id: CapabilityId,
    ) -> Result<Option<StoredCapabilityRecordV1>, StorageError> {
        Ok((id == self.record.capability_id()).then(|| self.record.clone()))
    }
    fn resolve_capability_digests(
        &self,
        candidates: &[CapabilityTokenDigest],
    ) -> Result<CapabilityLookupResult, StorageError> {
        Ok(if candidates.contains(&self.record.token_digest()) {
            CapabilityLookupResult::Found(Box::new(self.record.clone()))
        } else {
            CapabilityLookupResult::NotFound
        })
    }
}

fn open(database: &TestDatabase, profile: RedbCommitProfile) -> RedbOperationalPorts {
    open_operational(RedbStore::open_with_commit_profile(&database.0, profile).unwrap())
}
fn prepare(profile: RedbCommitProfile) -> (TestDatabase, FenceFixture, PrimaryFenceRequestV1) {
    let database = TestDatabase::create("primary-fence-coordinator");
    let fixture = authorization_fixture();
    let coordinator = start_coordinator(
        open(&database, profile),
        Arc::new(CountingAdministrationClock::working()),
        Arc::new(CountingAuthorizationClock::new()),
    );
    bootstrap_authorizer(
        &coordinator,
        &bootstrap_requested_record(),
        capability_digest(0x64),
        request_id(0x64),
    );
    let fence_id = capability_id(0x75);
    let requested = NormalizedCapabilityCreateRecord::new(
        database_id(),
        environment(),
        ActorId::new("fence-operator").unwrap(),
        ActorKind::Human,
        NonZeroU32::new(300).unwrap(),
        vec![audience()],
        CapabilityGrantV1::new(
            TenantScope::Global,
            PartitionScopeV1::All,
            CapabilityPermissionsV1::new(vec![
                CapabilityPermissionV1::unparameterized(
                    CapabilityPermissionKindV1::FenceReplicationPrimary,
                )
                .unwrap(),
            ])
            .unwrap(),
            vec![],
            NonZeroU16::MIN,
            vec![],
        )
        .unwrap(),
    )
    .unwrap();
    let created = block_on(
        block_on(coordinator.control_plane_executor().reserve_capacity())
            .unwrap()
            .submit_capability_create(
                CapabilityCreatePreparation::new(
                    authorize_create(&fixture, fence_id, &requested, request_id(0x75)),
                    digest_keys()
                        .current_digest(&RawCapabilityToken::parse_canonical(FENCE_TOKEN).unwrap()),
                )
                .unwrap(),
            )
            .unwrap()
            .completion(),
    )
    .unwrap();
    assert!(matches!(
        created.outcome(),
        CapabilityCreateOutcome::Created(_)
    ));
    let registered = execute(
        &coordinator,
        authorize(
            &fixture,
            Request::register(
                request_id(0x71),
                target(),
                FollowerHoldBudget::new(100).unwrap(),
                None,
            ),
        ),
    )
    .unwrap();
    let ResultV1::Applied(original) = registered.outcome() else {
        panic!("registered");
    };
    coordinator.shutdown().unwrap();
    let ports = open(&database, profile);
    // Construct source-side attachment through the existing artifact owner.
    // Transport/receiver publication proofs are covered by the follower suite.
    let repository = ports
        .bootstrap_repository(&database.0.with_extension("artifacts"))
        .unwrap();
    let mut build = repository.begin(target().hold_id()).unwrap();
    while !build.advance().unwrap() {}
    let held = build.finish().unwrap();
    let manifest = held.manifest();
    drop(held);
    repository
        .attach(manifest, manifest.fence().history().tail())
        .unwrap();
    assert!(matches!(
        repository.begin(target().hold_id()),
        Err(riffdb_storage_api::ChangelogCursorErrorV3::InvalidPosition)
    ));
    let request = PrimaryFenceRequestV1::new(
        request_id(0x72),
        ReplicationFenceOperationId::from_bytes(uuid_bytes(0x73)).unwrap(),
        target(),
        original.generation(),
    );
    drop(repository);
    let principal = CapabilityAuthenticator::new(
        &ports,
        &digest_keys(),
        &FenceClock,
        &NoopAuthenticationTelemetry,
    )
    .authenticate(
        OpaqueCredential::new(FENCE_TOKEN),
        &AuthenticationContext::new(database_id(), environment(), audience()),
    )
    .unwrap();
    let fixture = FenceFixture {
        principal,
        record: ports.read_capability(fence_id).unwrap().unwrap(),
    };
    drop(ports);
    (database, fixture, request)
}
fn authorized(
    fixture: &FenceFixture,
    request: PrimaryFenceRequestV1,
) -> AuthorizedPrimaryFencePreparation {
    let resolver = CapabilityReaderCurrentResolver::new(fixture);
    let PrimaryFenceDecision::Allow(prepared) = CurrentAuthorizer::new(
        &resolver,
        &ValueAuthorizationClock(timestamp(BASE_SECONDS + 20)),
        &NoopAuthorizationTelemetry,
        database_id(),
        environment(),
    )
    .authorize_primary_fence(&fixture.principal, request)
    .unwrap() else {
        panic!("fence authority");
    };
    *prepared
}
fn submit(
    coordinator: &RunningCommandCoordinator,
    prepared: AuthorizedPrimaryFencePreparation,
) -> Result<riffdb_commit::PrimaryFenceExecutionResult, riffdb_commit::ControlPlaneExecutionError> {
    let permit = block_on(coordinator.control_plane_executor().reserve_capacity()).unwrap();
    let receipt = block_on(permit.submit_primary_fence(prepared)).unwrap();
    block_on(receipt.completion())
}
fn retry(request: PrimaryFenceRequestV1) -> PrimaryFenceRequestV1 {
    PrimaryFenceRequestV1::new(
        request_id(0x74),
        request.operation_id(),
        request.target(),
        request.generation(),
    )
}

#[test]
fn primary_fence_real_coordinator_commits_replays_and_seeds_restart_refusal() {
    for profile in [RedbCommitProfile::Standard, RedbCommitProfile::Hardened] {
        let (database, fixture, request) = prepare(profile);
        let clock = Arc::new(CountingAuthorizationClock::new());
        let coordinator = start_coordinator(
            open(&database, profile),
            Arc::new(CountingAdministrationClock::working()),
            clock.clone(),
        );
        let result = submit(&coordinator, authorized(&fixture, request)).unwrap();
        let PrimaryFenceResultV1::Applied(original) = result.outcome() else {
            panic!("new fence");
        };
        assert_eq!(
            result.terminal_audit(),
            ControlPlaneTerminalAudit::Succeeded(ServiceAuditLinkV1::ControlPlane {
                administration_sequence: original.administration_sequence()
            })
        );
        let replay = submit(&coordinator, authorized(&fixture, retry(request))).unwrap();
        assert_eq!(
            replay.outcome(),
            &PrimaryFenceResultV1::Replayed(original.clone())
        );
        assert_eq!(clock.calls(), 2);
        let control = coordinator.control_plane_executor();
        assert!(matches!(
            block_on(control.reserve_capacity())
                .unwrap()
                .submit_replication_maintenance(),
            Err(riffdb_commit::ControlPlaneExecutionAdmissionError::PrimaryFenced)
        ));
        assert_eq!(
            control.lifecycle_state(),
            CoordinatorLifecycleState::Accepting
        );
        coordinator.shutdown().unwrap();
        let ports = open(&database, profile);
        assert_eq!(
            ports.read_replication_primary_admission().unwrap().fence(),
            Some(original.as_ref())
        );
        let coordinator = start_coordinator(
            ports,
            Arc::new(CountingAdministrationClock::working()),
            Arc::new(CountingAuthorizationClock::new()),
        );
        let replay = submit(&coordinator, authorized(&fixture, retry(request))).unwrap();
        assert_eq!(
            replay.outcome(),
            &PrimaryFenceResultV1::Replayed(original.clone())
        );
        assert!(matches!(
            block_on(coordinator.control_plane_executor().reserve_capacity())
                .unwrap()
                .submit_replication_maintenance(),
            Err(riffdb_commit::ControlPlaneExecutionAdmissionError::PrimaryFenced)
        ));
        coordinator.shutdown().unwrap();
        let coordinator = start_coordinator(
            open(&database, profile),
            Arc::new(CountingAdministrationClock::working()),
            Arc::new(ValueAuthorizationClock(timestamp(BASE_SECONDS + 900))),
        );
        assert_eq!(
            submit(&coordinator, authorized(&fixture, retry(request)))
                .unwrap_err()
                .kind(),
            ControlPlaneExecutionErrorKind::AuthorizationDenied
        );
        coordinator.shutdown().unwrap();
        assert_eq!(
            open(&database, profile)
                .read_replication_primary_admission()
                .unwrap()
                .fence(),
            Some(original.as_ref())
        );
    }
}

#[test]
fn primary_fence_real_coordinator_uncertainty_fences_and_restart_resolves_exact_receipt() {
    for profile in [RedbCommitProfile::Standard, RedbCommitProfile::Hardened] {
        let (database, fixture, request) = prepare(profile);
        let controller =
            RedbTestController::return_unknown_after_commit(RedbTestOperation::PrimaryFence);
        let ports = open_operational(
            RedbStore::open_with_test_controller_and_commit_profile(
                &database.0,
                profile,
                controller,
            )
            .unwrap(),
        );
        let coordinator = start_coordinator(
            ports,
            Arc::new(CountingAdministrationClock::working()),
            Arc::new(CountingAuthorizationClock::new()),
        );
        assert_eq!(
            submit(&coordinator, authorized(&fixture, request))
                .unwrap_err()
                .kind(),
            ControlPlaneExecutionErrorKind::OutcomeUnknown
        );
        assert_eq!(
            coordinator.command_executor().lifecycle_state(),
            CoordinatorLifecycleState::Fenced
        );
        assert!(
            coordinator
                .command_executor()
                .try_reserve_capacity()
                .is_err()
        );
        coordinator.shutdown().unwrap();
        let ports = open(&database, profile);
        let admission = ports.read_replication_primary_admission().unwrap();
        let original = admission.fence().unwrap();
        assert_eq!(original.operation_id(), request.operation_id());
        let coordinator = start_coordinator(
            ports,
            Arc::new(CountingAdministrationClock::working()),
            Arc::new(CountingAuthorizationClock::new()),
        );
        let replay = submit(&coordinator, authorized(&fixture, retry(request))).unwrap();
        assert_eq!(
            replay.outcome(),
            &PrimaryFenceResultV1::Replayed(Box::new(original.clone()))
        );
        coordinator.shutdown().unwrap();
    }
}
