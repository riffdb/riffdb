#![forbid(unsafe_code)]

//! Real-redb evidence for the typed control-plane executor and its shared actor queue.

#[path = "control_plane_replication.rs"]
mod replication;
#[path = "control_plane_replication_administration.rs"]
mod replication_administration;

use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use riffdb_catalog::{
    CatalogPreparationResult, prepare_catalog_activation, validate_catalog_history,
};
use riffdb_commit::{
    AdministrationClock, AdministrationClockError, AdmissionClock, AdmissionClockError,
    ApplicationCommitNotificationError, ApplicationCommitNotificationSink,
    CapabilityBootstrapExecutionResult, CapabilityBootstrapOutcome, CapabilityBootstrapPreparation,
    CapabilityCreateOutcome, CapabilityCreatePreparation, CapabilityRevokeOutcome,
    CapabilityRevokePreparation, CatalogDeploymentOutcome, CatalogDeploymentPreparation,
    ControlPlaneExecutionErrorKind, ControlPlaneTerminalAudit, CoordinatorDurability,
    CoordinatorLifecycleState, CoordinatorWorkloadCapacity, ProvenanceIdSource,
    ProvenanceIdSourceError, RunningCommandCoordinator,
};
use riffdb_conflict::{ConflictManager, ConflictManagerConfig, ShardedConflictManager};
use riffdb_contract_compiler::compile_contract_source;
use riffdb_policy::{
    AbsentCapabilityRevokeTargetFacts, AuthorizationClock, AuthorizationClockError,
    CapabilityActivity, CapabilityCreateTargetFacts, CapabilityRevokeTargetFacts,
    CurrentAuthorizer, Decision, NoopAuthorizationTelemetry, NormalizedCapabilityCreateRecord,
    OperationRequest, TrustedAudienceCatalog,
};
use riffdb_storage_api::{
    AdministrationAuditReader, AdministrationAuditScan, AdministrationAuditScanRequest,
    CapabilityLifecycleV1, CapabilityReader, DatabaseInitializationPort,
    DatabaseInitializationResult, EvidencePageLimit, ReadableCapabilityDigestInventory,
    ReadableDigestKey, ReadableIdempotencyDigestInventory, StartupValidationInputs,
    StorageScanLimit, StoredAdministrationAuditRecordV1, StructuralEvidenceCursor,
    StructuralEvidenceOpen, StructuralEvidencePage, StructuralEvidenceSession,
};
use riffdb_storage_redb::{
    RedbDormantPorts, RedbOperationalPorts, RedbStore, RedbTestController, RedbTestOperation,
};
use riffdb_testkit::authorization::{
    AuthorizationFixture, AuthorizationFixtureConfig, AuthorizationFixtureTimes,
};
use riffdb_testkit::scratch::ScratchDir;
use riffdb_types::{
    ActorId, ActorKind, AdministrationSequence, Audience, CapabilityGrantV1, CapabilityId,
    CapabilityPermissionKindV1, CapabilityPermissionV1, CapabilityPermissionsV1,
    CapabilityTokenDigest, CommitSequence, ContractVersion, DatabaseId, DigestKeyId, Environment,
    PartitionScopeV1, ProvenanceId, RequestId, RevocationReasonCodeV1, ServiceAuditLinkV1,
    ServiceAuditPhaseV1, ServiceAuditTargetV1, ServiceAuditTargetsV1, ServiceIngressKindV1,
    TenantScope, Timestamp,
};

const CONTRACT_SOURCE: &str = include_str!("../../contracts/examples/budget.riff");
const BASE_SECONDS: i64 = 1_700_300_000;

/// Field 1 is held only for its whole-directory cleanup on `Drop`.
struct TestDatabase(PathBuf, #[allow(dead_code)] ScratchDir);

impl TestDatabase {
    fn create(label: &str) -> Self {
        let scratch = ScratchDir::new(&format!("control-plane-{label}"))
            .expect("create control-plane scratch directory");
        let path = scratch.join("db.redb");
        let mut store = RedbStore::open(&path).expect("create control-plane database");
        assert_eq!(
            store
                .initialize_database(database_id())
                .expect("initialize database"),
            DatabaseInitializationResult::Installed(database_id())
        );
        drop(store);
        Self(path, scratch)
    }

    fn open(&self) -> RedbOperationalPorts {
        open_operational(RedbStore::open(&self.0).expect("open control-plane database"))
    }

    fn open_with_controller(&self, controller: RedbTestController) -> RedbOperationalPorts {
        open_operational(
            RedbStore::open_with_test_controller(&self.0, controller)
                .expect("open controlled database"),
        )
    }
}

struct CountingAdministrationClock {
    next_seconds: AtomicU64,
    calls: AtomicUsize,
    fail: bool,
}

impl CountingAdministrationClock {
    fn working() -> Self {
        Self {
            next_seconds: AtomicU64::new(BASE_SECONDS as u64),
            calls: AtomicUsize::new(0),
            fail: false,
        }
    }

    fn failing() -> Self {
        Self {
            next_seconds: AtomicU64::new(BASE_SECONDS as u64),
            calls: AtomicUsize::new(0),
            fail: true,
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }
}

impl AdministrationClock for CountingAdministrationClock {
    fn now(&self) -> Result<Timestamp, AdministrationClockError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if self.fail {
            return Err(AdministrationClockError);
        }
        let seconds = self.next_seconds.fetch_add(1, Ordering::Relaxed) as i64;
        Timestamp::new(seconds, 0).map_err(|_| AdministrationClockError)
    }
}

struct ValueAdministrationClock(Timestamp);

impl AdministrationClock for ValueAdministrationClock {
    fn now(&self) -> Result<Timestamp, AdministrationClockError> {
        Ok(self.0)
    }
}

struct CountingAuthorizationClock {
    calls: AtomicUsize,
}

impl CountingAuthorizationClock {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }
}

impl AuthorizationClock for CountingAuthorizationClock {
    fn now(&self) -> Result<Timestamp, AuthorizationClockError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(timestamp(BASE_SECONDS + 20))
    }
}

struct FixedAuthorizationClock;

impl AuthorizationClock for FixedAuthorizationClock {
    fn now(&self) -> Result<Timestamp, AuthorizationClockError> {
        Ok(timestamp(BASE_SECONDS + 10))
    }
}

struct ValueAuthorizationClock(Timestamp);

impl AuthorizationClock for ValueAuthorizationClock {
    fn now(&self) -> Result<Timestamp, AuthorizationClockError> {
        Ok(self.0)
    }
}

struct UnusedAdmissionClock;

impl AdmissionClock for UnusedAdmissionClock {
    fn now(&self) -> Result<Timestamp, AdmissionClockError> {
        Err(AdmissionClockError)
    }
}

struct UnusedProvenanceSource;

impl riffdb_commit::ServiceUuidV7Source for UnusedProvenanceSource {
    fn next_uuid_v7(&self) -> Result<[u8; 16], riffdb_commit::ServiceUuidV7SourceError> {
        Err(riffdb_commit::ServiceUuidV7SourceError)
    }
}

impl ProvenanceIdSource for UnusedProvenanceSource {
    fn next_provenance_id(&self) -> Result<ProvenanceId, ProvenanceIdSourceError> {
        Err(ProvenanceIdSourceError)
    }
}

fn start_coordinator(
    ports: RedbOperationalPorts,
    administration_clock: Arc<dyn AdministrationClock>,
    authorization_clock: Arc<dyn AuthorizationClock>,
) -> RunningCommandCoordinator {
    let conflicts: Arc<dyn ConflictManager> = Arc::new(
        ShardedConflictManager::new(ConflictManagerConfig::default())
            .expect("start conflict manager"),
    );
    RunningCommandCoordinator::start(
        CoordinatorWorkloadCapacity::new(16).expect("nonzero workload capacity"),
        CoordinatorDurability::Sync,
        ports,
        conflicts,
        Arc::new(UnusedAdmissionClock),
        Arc::new(UnusedProvenanceSource),
        administration_clock,
        authorization_clock,
        Arc::new(UnusedProvenanceSource),
        Arc::new(DiscardApplicationCommitNotifications),
    )
    .expect("start coordinator")
}

struct DiscardApplicationCommitNotifications;

impl ApplicationCommitNotificationSink for DiscardApplicationCommitNotifications {
    fn publish_first_commit(
        &self,
        _: CommitSequence,
    ) -> Result<(), ApplicationCommitNotificationError> {
        Ok(())
    }
}

struct FailingAuthorizationClock {
    calls: AtomicUsize,
}

impl FailingAuthorizationClock {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }
}

impl AuthorizationClock for FailingAuthorizationClock {
    fn now(&self) -> Result<Timestamp, AuthorizationClockError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Err(AuthorizationClockError)
    }
}

#[test]
fn control_plane_paths_share_order_and_preserve_exact_transition_evidence() {
    let database = TestDatabase::create("semantics");
    let administration_clock = Arc::new(CountingAdministrationClock::working());
    let authorization_clock = Arc::new(CountingAuthorizationClock::new());
    let coordinator = start_coordinator(
        database.open(),
        Arc::clone(&administration_clock) as Arc<dyn AdministrationClock>,
        Arc::clone(&authorization_clock) as Arc<dyn AuthorizationClock>,
    );
    let executor = coordinator.control_plane_executor();
    let fixture = authorization_fixture();
    let requested = bootstrap_requested_record();
    let digest = capability_digest(0x51);

    let bootstrap = block_on(executor.reserve_capacity())
        .expect("reserve bootstrap")
        .submit_capability_bootstrap(bootstrap_preparation(&requested, digest, request_id(0x31)))
        .expect("submit bootstrap");
    let CapabilityBootstrapExecutionResult::Completed(completion) =
        block_on(bootstrap.completion()).expect("bootstrap committed")
    else {
        panic!("bootstrap must create state");
    };
    let (bootstrap_outcome, terminal) = completion.into_parts();
    let CapabilityBootstrapOutcome::Created(bootstrap_transition) = bootstrap_outcome else {
        panic!("first bootstrap must be created");
    };
    assert_eq!(
        bootstrap_transition.administration_sequence(),
        administration_sequence(2)
    );
    block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve bootstrap terminal")
            .submit_capability_bootstrap_terminal(terminal)
            .expect("submit bootstrap terminal")
            .completion(),
    )
    .expect("append bootstrap terminal");

    let replay = block_on(executor.reserve_capacity())
        .expect("reserve bootstrap replay")
        .submit_capability_bootstrap(bootstrap_preparation(&requested, digest, request_id(0x32)))
        .expect("submit bootstrap replay");
    let CapabilityBootstrapExecutionResult::Completed(replay) =
        block_on(replay.completion()).expect("bootstrap replay resolved")
    else {
        panic!("bootstrap replay must resolve");
    };
    let (replay_outcome, replay_terminal) = replay.into_parts();
    let CapabilityBootstrapOutcome::Replayed(replay_transition) = replay_outcome else {
        panic!("second bootstrap must replay");
    };
    assert_eq!(
        replay_transition.administration_sequence(),
        bootstrap_transition.administration_sequence(),
        "replay retains the original authoritative transition"
    );
    block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve replay terminal")
            .submit_capability_bootstrap_terminal(replay_terminal)
            .expect("submit replay terminal")
            .completion(),
    )
    .expect("append replay terminal");

    let catalog = prepared_catalog(CONTRACT_SOURCE);
    let catalog_replay = catalog.clone();
    let first_catalog = CatalogDeploymentPreparation::new(
        request_id(0x33),
        catalog,
        authorize_catalog(&fixture, CONTRACT_SOURCE, None),
    )
    .expect("bind first deployment");
    let second_catalog = CatalogDeploymentPreparation::new(
        request_id(0x34),
        catalog_replay,
        authorize_catalog(&fixture, CONTRACT_SOURCE, None),
    )
    .expect("bind replay deployment");
    let first_receipt = block_on(executor.reserve_capacity())
        .expect("reserve first deployment")
        .submit_catalog_deployment(first_catalog)
        .expect("submit first deployment");
    let second_receipt = block_on(executor.reserve_capacity())
        .expect("reserve replay deployment")
        .submit_catalog_deployment(second_catalog)
        .expect("submit replay deployment");
    let second_result = block_on(second_receipt.completion()).expect("replay deployment result");
    let first_result = block_on(first_receipt.completion()).expect("first deployment result");
    assert!(matches!(
        first_result.outcome(),
        CatalogDeploymentOutcome::Activated(_)
    ));
    assert!(matches!(
        second_result.outcome(),
        CatalogDeploymentOutcome::AlreadyActive(_)
    ));
    assert_eq!(
        first_result.terminal_audit(),
        second_result.terminal_audit(),
        "the queued replay recovers the first transition sequence"
    );
    assert!(matches!(
        first_result.terminal_audit(),
        ControlPlaneTerminalAudit::Succeeded(ServiceAuditLinkV1::ControlPlane {
            administration_sequence: sequence
        }) if sequence == administration_sequence(6)
    ));

    let conflicting_source = CONTRACT_SOURCE.replace("BudgetAllocated", "FundsAllocated");
    let conflicting = CatalogDeploymentPreparation::new(
        request_id(0x35),
        prepared_catalog(&conflicting_source),
        authorize_catalog(&fixture, &conflicting_source, None),
    )
    .expect("bind conflicting deployment");
    let conflict = block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve conflicting deployment")
            .submit_catalog_deployment(conflicting)
            .expect("submit conflicting deployment")
            .completion(),
    )
    .expect("typed bundle conflict");
    assert_eq!(
        conflict.outcome(),
        &CatalogDeploymentOutcome::BundleConflict
    );
    assert_eq!(conflict.terminal_audit(), ControlPlaneTerminalAudit::Failed);

    let child = child_requested_record("created-agent", 300);
    let created_id = capability_id(0x61);
    let create = CapabilityCreatePreparation::new(
        authorize_create(&fixture, created_id, &child, request_id(0x41)),
        capability_digest(0x61),
    )
    .expect("bind create");
    let created = block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve create")
            .submit_capability_create(create)
            .expect("submit create")
            .completion(),
    )
    .expect("create committed");
    let CapabilityCreateOutcome::Created(created_transition) = created.outcome() else {
        panic!("first create must commit");
    };
    assert_eq!(created_transition.identity().capability_id(), created_id);
    assert!(matches!(
        created.terminal_audit(),
        ControlPlaneTerminalAudit::Succeeded(_)
    ));

    let create_replay = CapabilityCreatePreparation::new(
        authorize_create(&fixture, created_id, &child, request_id(0x40)),
        capability_digest(0x60),
    )
    .expect("bind create replay");
    let replayed_create = block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve create replay")
            .submit_capability_create(create_replay)
            .expect("submit create replay")
            .completion(),
    )
    .expect("create replay resolved");
    assert!(matches!(
        replayed_create.outcome(),
        CapabilityCreateOutcome::AlreadyCreatedTokenUnavailable(identity)
            if identity.capability_id() == created_id
    ));
    assert_eq!(
        replayed_create.terminal_audit(),
        created.terminal_audit(),
        "create replay privately retains the original transition sequence"
    );

    let conflicting_child = child_requested_record("different-created-agent", 300);
    let create_conflict = CapabilityCreatePreparation::new(
        authorize_create(&fixture, created_id, &conflicting_child, request_id(0x49)),
        capability_digest(0x69),
    )
    .expect("bind conflicting create");
    let conflicting_create = block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve conflicting create")
            .submit_capability_create(create_conflict)
            .expect("submit conflicting create")
            .completion(),
    )
    .expect("create conflict is typed");
    assert_eq!(
        conflicting_create.outcome(),
        &CapabilityCreateOutcome::CapabilityIdConflict
    );
    assert_eq!(
        conflicting_create.terminal_audit(),
        ControlPlaneTerminalAudit::Failed
    );

    let active_target = revoke_target(
        created_id,
        &child,
        NonZeroU64::MIN,
        CapabilityActivity::Active,
    );
    let stale_revoke = CapabilityRevokePreparation::new(authorize_revoke(
        &fixture,
        active_target.clone(),
        request_id(0x42),
    ))
    .expect("bind stale revoke");
    let first_revoke = CapabilityRevokePreparation::new(authorize_revoke(
        &fixture,
        active_target,
        request_id(0x43),
    ))
    .expect("bind first revoke");
    let revoked = block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve first revoke")
            .submit_capability_revoke(first_revoke)
            .expect("submit first revoke")
            .completion(),
    )
    .expect("revoke committed");
    let CapabilityRevokeOutcome::Revoked(revoked_transition) = *revoked.outcome() else {
        panic!("first revoke must commit");
    };
    let changed = block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve stale revoke")
            .submit_capability_revoke(stale_revoke)
            .expect("submit stale revoke")
            .completion(),
    )
    .expect("stale target is typed continuation state");
    assert_eq!(
        changed.outcome(),
        &CapabilityRevokeOutcome::CapabilityPreparationChanged
    );
    assert_eq!(changed.terminal_audit(), None);

    let revoked_target = revoke_target(
        created_id,
        &child,
        NonZeroU64::new(2).expect("revision two"),
        CapabilityActivity::Revoked,
    );
    let replay_revoke = CapabilityRevokePreparation::new(authorize_revoke(
        &fixture,
        revoked_target,
        request_id(0x44),
    ))
    .expect("bind revoked replay");
    let replayed_revoke = block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve revoked replay")
            .submit_capability_revoke(replay_revoke)
            .expect("submit revoked replay")
            .completion(),
    )
    .expect("already-revoked result");
    let CapabilityRevokeOutcome::AlreadyRevoked(replayed_transition) = *replayed_revoke.outcome()
    else {
        panic!("second revoke must recover the original transition");
    };
    assert_eq!(
        replayed_transition, revoked_transition,
        "already-revoked exposes the original transition sequence"
    );
    assert_eq!(
        replayed_revoke.terminal_audit(),
        revoked.terminal_audit(),
        "already-revoked terminal audit links the original transition"
    );

    let appeared_id = capability_id(0x62);
    let absent_preparation = CapabilityRevokePreparation::new(authorize_absent_revoke(
        &fixture,
        appeared_id,
        request_id(0x45),
    ))
    .expect("bind absent revoke");
    let appeared_child = child_requested_record("appeared-agent", 300);
    let appeared_create = CapabilityCreatePreparation::new(
        authorize_create(&fixture, appeared_id, &appeared_child, request_id(0x46)),
        capability_digest(0x62),
    )
    .expect("bind appeared create");
    block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve appeared create")
            .submit_capability_create(appeared_create)
            .expect("submit appeared create")
            .completion(),
    )
    .expect("appeared target created");
    let appeared = block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve absence-bound revoke")
            .submit_capability_revoke(absent_preparation)
            .expect("submit absence-bound revoke")
            .completion(),
    )
    .expect("target appearance classified");
    assert_eq!(
        appeared.outcome(),
        &CapabilityRevokeOutcome::CapabilityPreparationChanged
    );

    let absent_id = capability_id(0x63);
    let absent = CapabilityRevokePreparation::new(authorize_absent_revoke(
        &fixture,
        absent_id,
        request_id(0x47),
    ))
    .expect("bind absent target");
    let not_found = block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve absent revoke")
            .submit_capability_revoke(absent)
            .expect("submit absent revoke")
            .completion(),
    )
    .expect("absent target checked");
    assert_eq!(
        not_found.outcome(),
        &CapabilityRevokeOutcome::CapabilityNotFound
    );
    assert_eq!(
        not_found.terminal_audit(),
        Some(ControlPlaneTerminalAudit::Failed)
    );

    let bootstrap_conflict = bootstrap_requested_record_for("different-human");
    let conflict = block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve bootstrap conflict")
            .submit_capability_bootstrap(bootstrap_preparation(
                &bootstrap_conflict,
                digest,
                request_id(0x48),
            ))
            .expect("submit bootstrap conflict")
            .completion(),
    )
    .expect("bootstrap conflict is typed");
    assert!(matches!(
        conflict,
        CapabilityBootstrapExecutionResult::BootstrapConflict
    ));

    assert_eq!(administration_clock.calls(), 8);
    assert_eq!(authorization_clock.calls(), 9);
    assert_eq!(
        executor.lifecycle_state(),
        CoordinatorLifecycleState::Accepting
    );
    coordinator.shutdown().expect("shutdown coordinator");
}

#[test]
fn catalog_transaction_current_mismatch_is_a_failed_no_transition_result() {
    let database = TestDatabase::create("catalog-current-mismatch");
    let coordinator = start_coordinator(
        database.open(),
        Arc::new(CountingAdministrationClock::working()),
        Arc::new(CountingAuthorizationClock::new()),
    );
    bootstrap_authorizer(
        &coordinator,
        &bootstrap_requested_record(),
        capability_digest(0x64),
        request_id(0x64),
    );
    let executor = coordinator.control_plane_executor();
    let fixture = authorization_fixture();
    let stale_source = CONTRACT_SOURCE.replacen("LegalSpend", "StaleLegalSpend", 1);
    let stale = CatalogDeploymentPreparation::new(
        request_id(0x65),
        prepared_catalog(&stale_source),
        authorize_catalog(&fixture, &stale_source, None),
    )
    .expect("bind catalog candidate prepared against absence");
    let active = CatalogDeploymentPreparation::new(
        request_id(0x66),
        prepared_catalog(CONTRACT_SOURCE),
        authorize_catalog(&fixture, CONTRACT_SOURCE, None),
    )
    .expect("bind active catalog candidate");

    let activated = block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve active catalog deployment")
            .submit_catalog_deployment(active)
            .expect("submit active catalog deployment")
            .completion(),
    )
    .expect("activate first catalog");
    assert!(matches!(
        activated.outcome(),
        CatalogDeploymentOutcome::Activated(_)
    ));

    let mismatch = block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve stale catalog deployment")
            .submit_catalog_deployment(stale)
            .expect("submit stale catalog deployment")
            .completion(),
    )
    .expect("transaction-current mismatch is typed result data");
    assert!(matches!(
        mismatch.outcome(),
        CatalogDeploymentOutcome::ExpectedActiveVersionMismatch {
            actual: Some(version)
        } if *version == ContractVersion::new(1).expect("version one")
    ));
    assert_eq!(
        mismatch.terminal_audit(),
        ControlPlaneTerminalAudit::Failed,
        "a catalog CAS mismatch has no authoritative transition sequence"
    );
    assert_eq!(
        executor.lifecycle_state(),
        CoordinatorLifecycleState::Accepting
    );
    coordinator.shutdown().expect("shutdown coordinator");
}

#[test]
fn transaction_current_revoked_authorizer_denies_without_target_write() {
    let database = TestDatabase::create("revoked-authorizer");
    let coordinator = start_coordinator(
        database.open(),
        Arc::new(CountingAdministrationClock::working()),
        Arc::new(CountingAuthorizationClock::new()),
    );
    let bootstrap_requested = bootstrap_requested_record();
    bootstrap_authorizer(
        &coordinator,
        &bootstrap_requested,
        capability_digest(0x67),
        request_id(0x67),
    );
    let executor = coordinator.control_plane_executor();
    let fixture = authorization_fixture();
    let target_id = capability_id(0x68);
    let child = child_requested_record("revoked-authorizer-child", 300);
    let stale_create = CapabilityCreatePreparation::new(
        authorize_create(&fixture, target_id, &child, request_id(0x68)),
        capability_digest(0x68),
    )
    .expect("bind create before authorizer revocation");
    let revoke_authorizer = CapabilityRevokePreparation::new(authorize_revoke(
        &fixture,
        bootstrap_revoke_target(&bootstrap_requested),
        request_id(0x69),
    ))
    .expect("bind authorizer revocation");

    let revoked = block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve authorizer revocation")
            .submit_capability_revoke(revoke_authorizer)
            .expect("submit authorizer revocation")
            .completion(),
    )
    .expect("revoke authorizer");
    assert!(matches!(
        revoked.outcome(),
        CapabilityRevokeOutcome::Revoked(_)
    ));

    let denied = block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve stale create")
            .submit_capability_create(stale_create)
            .expect("submit stale create")
            .completion(),
    )
    .expect_err("transaction-current revoked authorizer must deny");
    assert_eq!(
        denied.kind(),
        ControlPlaneExecutionErrorKind::AuthorizationDenied
    );
    assert_eq!(
        executor.lifecycle_state(),
        CoordinatorLifecycleState::Accepting
    );
    coordinator.shutdown().expect("shutdown coordinator");

    let ports = database.open();
    let authorizer = ports
        .read_capability(authorizing_capability_id())
        .expect("read authorizer")
        .expect("authorizer remains retained");
    assert_eq!(
        authorizer.revision(),
        NonZeroU64::new(2).expect("revision two")
    );
    assert!(matches!(
        authorizer.lifecycle(),
        CapabilityLifecycleV1::Revoked { .. }
    ));
    assert_eq!(
        ports
            .read_capability(target_id)
            .expect("read denied target"),
        None,
        "authorization drift must write no target capability"
    );
}

#[test]
fn transaction_current_expired_authorizer_denies_without_target_write() {
    let database = TestDatabase::create("expired-authorizer");
    let coordinator = start_coordinator(
        database.open(),
        Arc::new(CountingAdministrationClock::working()),
        Arc::new(ValueAuthorizationClock(timestamp(BASE_SECONDS + 900))),
    );
    bootstrap_authorizer(
        &coordinator,
        &bootstrap_requested_record(),
        capability_digest(0x6a),
        request_id(0x6a),
    );
    let executor = coordinator.control_plane_executor();
    let fixture = authorization_fixture();
    let target_id = capability_id(0x6b);
    let child = child_requested_record("expired-authorizer-child", 1);
    let create = CapabilityCreatePreparation::new(
        authorize_create(&fixture, target_id, &child, request_id(0x6b)),
        capability_digest(0x6b),
    )
    .expect("bind create while authorizer is initially valid");

    let denied = block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve expired-authorizer create")
            .submit_capability_create(create)
            .expect("submit expired-authorizer create")
            .completion(),
    )
    .expect_err("transaction-current expiry must deny");
    assert_eq!(
        denied.kind(),
        ControlPlaneExecutionErrorKind::AuthorizationDenied
    );
    assert_eq!(
        executor.lifecycle_state(),
        CoordinatorLifecycleState::Accepting
    );
    coordinator.shutdown().expect("shutdown coordinator");

    let ports = database.open();
    let authorizer = ports
        .read_capability(authorizing_capability_id())
        .expect("read authorizer")
        .expect("expired authorizer remains retained");
    assert_eq!(authorizer.revision(), NonZeroU64::MIN);
    assert_eq!(authorizer.lifecycle(), &CapabilityLifecycleV1::Active);
    assert_eq!(
        ports
            .read_capability(target_id)
            .expect("read denied target"),
        None,
        "expiry denial must write no target capability"
    );
}

#[test]
fn control_plane_proven_abort_stays_live_and_unknown_commit_fences() {
    let before_database = TestDatabase::create("proven-abort");
    let administration_clock = Arc::new(CountingAdministrationClock::working());
    let authorization_clock = Arc::new(CountingAuthorizationClock::new());
    let coordinator = start_coordinator(
        before_database.open_with_controller(RedbTestController::return_before_commit(
            RedbTestOperation::CapabilityAdministration,
        )),
        administration_clock,
        authorization_clock,
    );
    let executor = coordinator.control_plane_executor();
    let requested = bootstrap_requested_record();
    let digest = capability_digest(0x71);
    let bootstrap = block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve bootstrap")
            .submit_capability_bootstrap(bootstrap_preparation(
                &requested,
                digest,
                request_id(0x71),
            ))
            .expect("submit bootstrap")
            .completion(),
    )
    .expect("bootstrap committed");
    let CapabilityBootstrapExecutionResult::Completed(bootstrap) = bootstrap else {
        panic!("bootstrap must create state");
    };
    let (_, terminal) = bootstrap.into_parts();
    block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve bootstrap terminal")
            .submit_capability_bootstrap_terminal(terminal)
            .expect("submit bootstrap terminal")
            .completion(),
    )
    .expect("append bootstrap terminal");

    let fixture = authorization_fixture();
    let child = child_requested_record("proven-abort-agent", 300);
    let target_id = capability_id(0x71);
    let error = block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve proven abort")
            .submit_capability_create(
                CapabilityCreatePreparation::new(
                    authorize_create(&fixture, target_id, &child, request_id(0x72)),
                    capability_digest(0x72),
                )
                .expect("bind proven-abort create"),
            )
            .expect("submit proven abort")
            .completion(),
    )
    .expect_err("ordinary capability before-commit failure is unavailable");
    assert_eq!(
        error.kind(),
        ControlPlaneExecutionErrorKind::StorageUnavailable
    );
    assert_eq!(
        executor.lifecycle_state(),
        CoordinatorLifecycleState::Accepting
    );
    let retry = block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve retry")
            .submit_capability_create(
                CapabilityCreatePreparation::new(
                    authorize_create(&fixture, target_id, &child, request_id(0x73)),
                    capability_digest(0x73),
                )
                .expect("bind retry create"),
            )
            .expect("submit retry")
            .completion(),
    )
    .expect("proven abort may retry on same actor");
    assert!(matches!(
        retry.outcome(),
        CapabilityCreateOutcome::Created(_)
    ));
    coordinator.shutdown().expect("shutdown proven-abort actor");

    let unknown_database = TestDatabase::create("unknown");
    let requested = bootstrap_requested_record();
    let digest = capability_digest(0x74);
    let coordinator = start_coordinator(
        unknown_database.open_with_controller(RedbTestController::return_unknown_after_commit(
            RedbTestOperation::CapabilityBootstrap,
        )),
        Arc::new(CountingAdministrationClock::working()),
        Arc::new(CountingAuthorizationClock::new()),
    );
    let executor = coordinator.control_plane_executor();
    let unknown = block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve unknown write")
            .submit_capability_bootstrap(bootstrap_preparation(
                &requested,
                digest,
                request_id(0x74),
            ))
            .expect("submit unknown write")
            .completion(),
    )
    .expect_err("after-commit acknowledgement is uncertain");
    assert_eq!(
        unknown.kind(),
        ControlPlaneExecutionErrorKind::OutcomeUnknown
    );
    assert_eq!(
        executor.lifecycle_state(),
        CoordinatorLifecycleState::Fenced
    );
    assert!(block_on(executor.reserve_capacity()).is_err());
    coordinator.shutdown().expect("shutdown fenced actor");

    let recovered = start_coordinator(
        unknown_database.open(),
        Arc::new(CountingAdministrationClock::working()),
        Arc::new(CountingAuthorizationClock::new()),
    );
    let recovered_executor = recovered.control_plane_executor();
    let replay = block_on(
        block_on(recovered_executor.reserve_capacity())
            .expect("reserve recovered replay")
            .submit_capability_bootstrap(bootstrap_preparation(
                &requested,
                digest,
                request_id(0x75),
            ))
            .expect("submit recovered replay")
            .completion(),
    )
    .expect("recovery resolves committed bootstrap");
    let CapabilityBootstrapExecutionResult::Completed(replay) = replay else {
        panic!("recovery must replay committed bootstrap");
    };
    assert!(matches!(
        replay.outcome(),
        CapabilityBootstrapOutcome::Replayed(_)
    ));
    recovered.shutdown().expect("shutdown recovered actor");
}

#[test]
fn required_bootstrap_audit_outages_stop_readiness_and_never_claim_success() {
    let compound_database = TestDatabase::create("bootstrap-compound-proven-abort");
    let compound_request_id = request_id(0x81);
    let coordinator = start_coordinator(
        compound_database.open_with_controller(RedbTestController::return_before_commit(
            RedbTestOperation::CapabilityBootstrap,
        )),
        Arc::new(CountingAdministrationClock::working()),
        Arc::new(CountingAuthorizationClock::new()),
    );
    let executor = coordinator.control_plane_executor();
    let error = block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve bootstrap")
            .submit_capability_bootstrap(bootstrap_preparation(
                &bootstrap_requested_record(),
                capability_digest(0x81),
                compound_request_id,
            ))
            .expect("submit bootstrap")
            .completion(),
    )
    .expect_err("compound audit write is proven aborted");
    assert_eq!(
        error.kind(),
        ControlPlaneExecutionErrorKind::StorageUnavailable
    );
    assert_eq!(
        executor.lifecycle_state(),
        CoordinatorLifecycleState::Stopped
    );
    assert!(block_on(executor.reserve_capacity()).is_err());
    coordinator.shutdown().expect("shutdown stopped actor");

    let ports = compound_database.open();
    assert_eq!(
        ports
            .read_capability(authorizing_capability_id())
            .expect("read bootstrap target"),
        None,
        "compound proven abort writes neither capability nor audit"
    );
    assert!(service_audit_phases(&ports, compound_request_id).is_empty());
    drop(ports);

    let terminal_database = TestDatabase::create("bootstrap-terminal-proven-abort");
    let terminal_request_id = request_id(0x82);
    let coordinator = start_coordinator(
        terminal_database.open_with_controller(RedbTestController::return_before_commit(
            RedbTestOperation::ServiceAudit,
        )),
        Arc::new(CountingAdministrationClock::working()),
        Arc::new(CountingAuthorizationClock::new()),
    );
    let executor = coordinator.control_plane_executor();
    let bootstrap = block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve bootstrap")
            .submit_capability_bootstrap(bootstrap_preparation(
                &bootstrap_requested_record(),
                capability_digest(0x82),
                terminal_request_id,
            ))
            .expect("submit bootstrap")
            .completion(),
    )
    .expect("compound bootstrap committed");
    let CapabilityBootstrapExecutionResult::Completed(bootstrap) = bootstrap else {
        panic!("bootstrap must create state");
    };
    let (_, terminal) = bootstrap.into_parts();
    let error = block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve terminal")
            .submit_capability_bootstrap_terminal(terminal)
            .expect("submit terminal")
            .completion(),
    )
    .expect_err("required terminal audit write is proven aborted");
    assert_eq!(
        error.kind(),
        ControlPlaneExecutionErrorKind::StorageUnavailable
    );
    assert_eq!(
        executor.lifecycle_state(),
        CoordinatorLifecycleState::Stopped
    );
    assert!(block_on(executor.reserve_capacity()).is_err());
    coordinator.shutdown().expect("shutdown stopped actor");

    let ports = terminal_database.open();
    assert!(
        ports
            .read_capability(authorizing_capability_id())
            .expect("read committed bootstrap target")
            .is_some(),
        "terminal outage does not roll back the completed compound transition"
    );
    assert_eq!(
        service_audit_phases(&ports, terminal_request_id),
        vec![ServiceAuditPhaseV1::Started],
        "proven-abort terminal outage must not persist or claim success"
    );
}

#[test]
fn administration_clock_failure_stops_readiness_without_a_write() {
    let database = TestDatabase::create("clock-failure");
    let administration_clock = Arc::new(CountingAdministrationClock::failing());
    let coordinator = start_coordinator(
        database.open(),
        Arc::clone(&administration_clock) as Arc<dyn AdministrationClock>,
        Arc::new(CountingAuthorizationClock::new()),
    );
    let executor = coordinator.control_plane_executor();
    let error = block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve bootstrap")
            .submit_capability_bootstrap(bootstrap_preparation(
                &bootstrap_requested_record(),
                capability_digest(0x75),
                request_id(0x75),
            ))
            .expect("submit bootstrap")
            .completion(),
    )
    .expect_err("clock failure is fail closed");
    assert_eq!(
        error.kind(),
        ControlPlaneExecutionErrorKind::StorageUnavailable
    );
    assert_eq!(administration_clock.calls(), 1);
    assert_eq!(
        executor.lifecycle_state(),
        CoordinatorLifecycleState::Stopped
    );
    coordinator.shutdown().expect("shutdown stopped actor");

    let reopened = database.open();
    drop(reopened);
}

#[test]
fn authorization_clock_failure_is_live_internal_and_allows_terminal_audit() {
    let database = TestDatabase::create("authorization-clock-failure");
    let administration_clock = Arc::new(CountingAdministrationClock::working());
    let authorization_clock = Arc::new(FailingAuthorizationClock::new());
    let coordinator = start_coordinator(
        database.open(),
        Arc::clone(&administration_clock) as Arc<dyn AdministrationClock>,
        Arc::clone(&authorization_clock) as Arc<dyn AuthorizationClock>,
    );
    let executor = coordinator.control_plane_executor();
    let fixture = authorization_fixture();
    let requested = bootstrap_requested_record();
    let digest = capability_digest(0x76);
    let bootstrap = block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve bootstrap")
            .submit_capability_bootstrap(bootstrap_preparation(
                &requested,
                digest,
                request_id(0x76),
            ))
            .expect("submit bootstrap")
            .completion(),
    )
    .expect("bootstrap committed without authorization clock");
    let CapabilityBootstrapExecutionResult::Completed(bootstrap) = bootstrap else {
        panic!("bootstrap must create state");
    };
    let (_, terminal) = bootstrap.into_parts();

    let target_id = capability_id(0x77);
    let child = child_requested_record("clock-failed-agent", 300);
    let create = CapabilityCreatePreparation::new(
        authorize_create(&fixture, target_id, &child, request_id(0x77)),
        capability_digest(0x77),
    )
    .expect("bind create");
    let error = block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve create")
            .submit_capability_create(create)
            .expect("submit create")
            .completion(),
    )
    .expect_err("authorization clock failure is internal");
    assert_eq!(error.kind(), ControlPlaneExecutionErrorKind::InternalDefect);
    assert_eq!(authorization_clock.calls(), 1);
    assert_eq!(
        executor.lifecycle_state(),
        CoordinatorLifecycleState::Accepting
    );

    block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve required terminal")
            .submit_capability_bootstrap_terminal(terminal)
            .expect("submit required terminal")
            .completion(),
    )
    .expect("same actor remains available for terminal audit");
    assert_eq!(administration_clock.calls(), 2);
    coordinator.shutdown().expect("shutdown live actor");

    let reopened = database.open();
    assert_eq!(
        reopened
            .read_capability(target_id)
            .expect("read target after reopen"),
        None,
        "clock failure abandoned the transaction without a capability write"
    );
}

#[test]
fn create_expiry_overflow_is_live_internal_and_abandons_the_transaction() {
    let database = TestDatabase::create("create-expiry-overflow");
    let issued_at = timestamp(i64::MAX - 1_000);
    let expires_at = timestamp(i64::MAX - 100);
    let service_authorization_time = timestamp(i64::MAX - 700);
    let transaction_authorization_time = timestamp(i64::MAX - 200);
    let coordinator = start_coordinator(
        database.open(),
        Arc::new(ValueAdministrationClock(issued_at)),
        Arc::new(ValueAuthorizationClock(transaction_authorization_time)),
    );
    let executor = coordinator.control_plane_executor();
    let requested = bootstrap_requested_record();
    let bootstrap = block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve bootstrap")
            .submit_capability_bootstrap(bootstrap_preparation(
                &requested,
                capability_digest(0x7a),
                request_id(0x7a),
            ))
            .expect("submit bootstrap")
            .completion(),
    )
    .expect("bootstrap committed near the timestamp boundary");
    let CapabilityBootstrapExecutionResult::Completed(bootstrap) = bootstrap else {
        panic!("bootstrap must create state");
    };
    let (_, terminal) = bootstrap.into_parts();

    let fixture =
        authorization_fixture_with_times(issued_at, expires_at, service_authorization_time);
    let target_id = capability_id(0x7b);
    let child = child_requested_record("overflow-agent", 500);
    let create = CapabilityCreatePreparation::new(
        authorize_create_at(
            &fixture,
            target_id,
            &child,
            request_id(0x7b),
            service_authorization_time,
        ),
        capability_digest(0x7b),
    )
    .expect("bind create");
    let error = block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve create")
            .submit_capability_create(create)
            .expect("submit create")
            .completion(),
    )
    .expect_err("checked expiry must reject overflow");
    assert_eq!(error.kind(), ControlPlaneExecutionErrorKind::InternalDefect);
    assert_eq!(
        executor.lifecycle_state(),
        CoordinatorLifecycleState::Accepting,
        "normal capability arithmetic failure is proven pre-commit"
    );

    block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve required terminal")
            .submit_capability_bootstrap_terminal(terminal)
            .expect("submit required terminal")
            .completion(),
    )
    .expect("same actor remains available for terminal audit");
    coordinator.shutdown().expect("shutdown live actor");

    let reopened = database.open();
    assert_eq!(
        reopened
            .read_capability(target_id)
            .expect("read target after reopen"),
        None,
        "overflow abandoned the transaction without a capability write"
    );
}

#[test]
fn bootstrap_expiry_overflow_stops_readiness_without_a_write() {
    let database = TestDatabase::create("bootstrap-expiry-overflow");
    let coordinator = start_coordinator(
        database.open(),
        Arc::new(ValueAdministrationClock(timestamp(i64::MAX - 100))),
        Arc::new(CountingAuthorizationClock::new()),
    );
    let executor = coordinator.control_plane_executor();
    let requested = bootstrap_requested_record();
    let target_id = authorizing_capability_id();
    let error = block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve bootstrap")
            .submit_capability_bootstrap(bootstrap_preparation(
                &requested,
                capability_digest(0x7c),
                request_id(0x7c),
            ))
            .expect("submit bootstrap")
            .completion(),
    )
    .expect_err("bootstrap expiry cannot be represented");
    assert_eq!(
        error.kind(),
        ControlPlaneExecutionErrorKind::StorageUnavailable
    );
    assert_eq!(
        executor.lifecycle_state(),
        CoordinatorLifecycleState::Stopped
    );
    coordinator.shutdown().expect("shutdown stopped actor");

    let reopened = database.open();
    assert_eq!(
        reopened
            .read_capability(target_id)
            .expect("read bootstrap target after reopen"),
        None,
        "bootstrap overflow must not write the compound start or capability"
    );
}

#[test]
fn bootstrap_preparation_rejects_non_human_or_non_admin_candidates() {
    let digest = capability_digest(0x78);
    let agent = child_requested_record("agent-bootstrap", 900);
    assert!(
        CapabilityBootstrapPreparation::new(
            authorizing_capability_id(),
            &agent,
            vec![digest],
            digest,
            request_id(0x78),
            ServiceIngressKindV1::Grpc,
            ServiceAuditTargetsV1::new([ServiceAuditTargetV1::Capability(
                authorizing_capability_id(),
            )])
            .expect("target"),
        )
        .is_err()
    );

    let human_without_admin = NormalizedCapabilityCreateRecord::new(
        database_id(),
        environment(),
        ActorId::new("human-without-admin").expect("principal"),
        ActorKind::Human,
        NonZeroU32::new(900).expect("duration"),
        vec![audience()],
        child_grant(),
    )
    .expect("bounded human request");
    assert!(
        CapabilityBootstrapPreparation::new(
            authorizing_capability_id(),
            &human_without_admin,
            vec![digest],
            digest,
            request_id(0x79),
            ServiceIngressKindV1::Grpc,
            ServiceAuditTargetsV1::new([ServiceAuditTargetV1::Capability(
                authorizing_capability_id(),
            )])
            .expect("target"),
        )
        .is_err()
    );
}

fn bootstrap_preparation(
    requested: &NormalizedCapabilityCreateRecord,
    digest: CapabilityTokenDigest,
    request_id: RequestId,
) -> CapabilityBootstrapPreparation {
    CapabilityBootstrapPreparation::new(
        authorizing_capability_id(),
        requested,
        vec![digest],
        digest,
        request_id,
        ServiceIngressKindV1::Grpc,
        ServiceAuditTargetsV1::new([ServiceAuditTargetV1::Capability(authorizing_capability_id())])
            .expect("bootstrap target"),
    )
    .expect("checked bootstrap preparation")
}

fn bootstrap_authorizer(
    coordinator: &RunningCommandCoordinator,
    requested: &NormalizedCapabilityCreateRecord,
    digest: CapabilityTokenDigest,
    request_id: RequestId,
) {
    let executor = coordinator.control_plane_executor();
    let result = block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve bootstrap")
            .submit_capability_bootstrap(bootstrap_preparation(requested, digest, request_id))
            .expect("submit bootstrap")
            .completion(),
    )
    .expect("bootstrap committed");
    let CapabilityBootstrapExecutionResult::Completed(completion) = result else {
        panic!("bootstrap must create authorizer");
    };
    let (_, terminal) = completion.into_parts();
    block_on(
        block_on(executor.reserve_capacity())
            .expect("reserve bootstrap terminal")
            .submit_capability_bootstrap_terminal(terminal)
            .expect("submit bootstrap terminal")
            .completion(),
    )
    .expect("append bootstrap terminal");
}

fn prepared_catalog(source: &str) -> riffdb_catalog::PreparedCatalogActivation {
    let bundle = compile_contract_source(source).expect("compile catalog candidate");
    let CatalogPreparationResult::Prepared(prepared) =
        prepare_catalog_activation(bundle, None, None).expect("prepare catalog candidate")
    else {
        panic!("genesis candidate must prepare");
    };
    prepared
}

fn authorize_catalog(
    fixture: &AuthorizationFixture,
    source: &str,
    expected: Option<ContractVersion>,
) -> riffdb_policy::AuthorizedCatalogDeployment {
    let bundle = compile_contract_source(source).expect("compile authorized candidate");
    let lineage = bundle.lineage().clone();
    let version = bundle.contract_version();
    let hash = bundle.bundle_hash();
    let resolver = fixture.current_capability_resolver();
    let decision = CurrentAuthorizer::new(
        &resolver,
        &FixedAuthorizationClock,
        &NoopAuthorizationTelemetry,
        database_id(),
        environment(),
    )
    .authorize(
        fixture.authenticated_principal(),
        OperationRequest::deploy_contract(lineage.clone(), version, hash, expected),
    )
    .expect("catalog policy decision");
    let Decision::Allow(authorized) = decision else {
        panic!("fixture must authorize deployment");
    };
    authorized
        .into_catalog_deployment(&lineage, version, hash, expected)
        .expect("bind exact deployment")
}

fn authorize_create(
    fixture: &AuthorizationFixture,
    capability_id: CapabilityId,
    requested: &NormalizedCapabilityCreateRecord,
    request_id: RequestId,
) -> riffdb_policy::AuthorizedCapabilityMutationPreparation {
    authorize_create_at(
        fixture,
        capability_id,
        requested,
        request_id,
        timestamp(BASE_SECONDS + 10),
    )
}

fn authorize_create_at(
    fixture: &AuthorizationFixture,
    capability_id: CapabilityId,
    requested: &NormalizedCapabilityCreateRecord,
    request_id: RequestId,
    authorization_time: Timestamp,
) -> riffdb_policy::AuthorizedCapabilityMutationPreparation {
    let target = CapabilityCreateTargetFacts::new(request_id, capability_id, requested.clone());
    let trusted = TrustedAudienceCatalog::new(vec![audience()]).expect("trusted audience");
    let resolver = fixture.current_capability_resolver();
    let decision = CurrentAuthorizer::new(
        &resolver,
        &ValueAuthorizationClock(authorization_time),
        &NoopAuthorizationTelemetry,
        database_id(),
        environment(),
    )
    .with_trusted_audience_catalog(&trusted)
    .authorize(
        fixture.authenticated_principal(),
        OperationRequest::create_capability(target),
    )
    .expect("create policy decision");
    let Decision::PrepareCapabilityMutation(preparation) = decision else {
        panic!("fixture must authorize create");
    };
    *preparation
}

fn authorize_revoke(
    fixture: &AuthorizationFixture,
    mut target: CapabilityRevokeTargetFacts,
    request_id: RequestId,
) -> riffdb_policy::AuthorizedCapabilityMutationPreparation {
    target = CapabilityRevokeTargetFacts::new(
        request_id,
        target.capability_id(),
        target.revision(),
        target.activity(),
        target.database_id(),
        target.environment().clone(),
        target.principal_id().clone(),
        target.actor_kind(),
        target.audiences().to_vec(),
        target.issued_at(),
        target.expires_at(),
        target.grant().clone(),
    )
    .expect("rebind request ID");
    let resolver = fixture.current_capability_resolver();
    let decision = CurrentAuthorizer::new(
        &resolver,
        &FixedAuthorizationClock,
        &NoopAuthorizationTelemetry,
        database_id(),
        environment(),
    )
    .authorize(
        fixture.authenticated_principal(),
        OperationRequest::revoke_capability(target, RevocationReasonCodeV1::Requested),
    )
    .expect("revoke policy decision");
    let Decision::PrepareCapabilityMutation(preparation) = decision else {
        panic!("fixture must authorize revoke");
    };
    *preparation
}

fn authorize_absent_revoke(
    fixture: &AuthorizationFixture,
    capability_id: CapabilityId,
    request_id: RequestId,
) -> riffdb_policy::AuthorizedCapabilityMutationPreparation {
    let target = AbsentCapabilityRevokeTargetFacts::new(
        request_id,
        capability_id,
        database_id(),
        environment(),
    );
    let resolver = fixture.current_capability_resolver();
    let decision = CurrentAuthorizer::new(
        &resolver,
        &FixedAuthorizationClock,
        &NoopAuthorizationTelemetry,
        database_id(),
        environment(),
    )
    .authorize(
        fixture.authenticated_principal(),
        OperationRequest::revoke_absent_capability(target, RevocationReasonCodeV1::Requested),
    )
    .expect("absent-revoke policy decision");
    let Decision::PrepareCapabilityMutation(preparation) = decision else {
        panic!("fixture must authorize absent revoke");
    };
    *preparation
}

fn revoke_target(
    capability_id: CapabilityId,
    requested: &NormalizedCapabilityCreateRecord,
    revision: NonZeroU64,
    activity: CapabilityActivity,
) -> CapabilityRevokeTargetFacts {
    CapabilityRevokeTargetFacts::new(
        request_id(0x7f),
        capability_id,
        revision,
        activity,
        requested.database_id(),
        requested.environment().clone(),
        requested.principal_id().clone(),
        requested.actor_kind(),
        requested.audiences().to_vec(),
        timestamp(BASE_SECONDS + 20),
        timestamp(BASE_SECONDS + 20 + i64::from(requested.requested_lifetime_seconds().get())),
        requested.grant().clone(),
    )
    .expect("revoke target facts")
}

fn bootstrap_revoke_target(
    requested: &NormalizedCapabilityCreateRecord,
) -> CapabilityRevokeTargetFacts {
    CapabilityRevokeTargetFacts::new(
        request_id(0x7e),
        authorizing_capability_id(),
        NonZeroU64::MIN,
        CapabilityActivity::Active,
        requested.database_id(),
        requested.environment().clone(),
        requested.principal_id().clone(),
        requested.actor_kind(),
        requested.audiences().to_vec(),
        timestamp(BASE_SECONDS),
        timestamp(BASE_SECONDS + i64::from(requested.requested_lifetime_seconds().get())),
        requested.grant().clone(),
    )
    .expect("bootstrap revoke target facts")
}

fn authorization_fixture() -> AuthorizationFixture {
    authorization_fixture_with_times(
        timestamp(BASE_SECONDS),
        timestamp(BASE_SECONDS + 900),
        timestamp(BASE_SECONDS + 10),
    )
}

fn authorization_fixture_with_times(
    issued_at: Timestamp,
    expires_at: Timestamp,
    authentication_time: Timestamp,
) -> AuthorizationFixture {
    AuthorizationFixture::new(AuthorizationFixtureConfig::new(
        database_id(),
        environment(),
        ActorId::new("control-plane-maintainer").expect("principal"),
        ActorKind::Human,
        audience(),
        AuthorizationFixtureTimes::new(issued_at, expires_at, authentication_time),
        administrator_grant(),
    ))
    .expect("authorization fixture")
}

fn bootstrap_requested_record() -> NormalizedCapabilityCreateRecord {
    bootstrap_requested_record_for("control-plane-maintainer")
}

fn bootstrap_requested_record_for(principal: &str) -> NormalizedCapabilityCreateRecord {
    NormalizedCapabilityCreateRecord::new(
        database_id(),
        environment(),
        ActorId::new(principal).expect("principal"),
        ActorKind::Human,
        NonZeroU32::new(900).expect("duration"),
        vec![audience()],
        administrator_grant(),
    )
    .expect("bootstrap requested record")
}

fn child_requested_record(principal: &str, duration: u32) -> NormalizedCapabilityCreateRecord {
    NormalizedCapabilityCreateRecord::new(
        database_id(),
        environment(),
        ActorId::new(principal).expect("child principal"),
        ActorKind::Agent,
        NonZeroU32::new(duration).expect("duration"),
        vec![audience()],
        child_grant(),
    )
    .expect("child requested record")
}

fn administrator_grant() -> CapabilityGrantV1 {
    let permissions = CapabilityPermissionsV1::new(vec![
        CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::DeployContract)
            .expect("deploy permission"),
        CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ReadHealth)
            .expect("health permission"),
        CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::CreateCapability)
            .expect("create permission"),
        CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::RevokeCapability)
            .expect("revoke permission"),
        CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::AdministerCapabilities)
            .expect("admin permission"),
    ])
    .expect("canonical permissions");
    CapabilityGrantV1::new(
        TenantScope::Global,
        PartitionScopeV1::All,
        permissions,
        Vec::new(),
        NonZeroU16::new(100).expect("row limit"),
        Vec::new(),
    )
    .expect("administrator grant")
}

fn child_grant() -> CapabilityGrantV1 {
    CapabilityGrantV1::new(
        TenantScope::Global,
        PartitionScopeV1::All,
        CapabilityPermissionsV1::new(vec![
            CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ReadHealth)
                .expect("health permission"),
        ])
        .expect("canonical permissions"),
        Vec::new(),
        NonZeroU16::new(10).expect("row limit"),
        Vec::new(),
    )
    .expect("child grant")
}

fn service_audit_phases(
    ports: &RedbOperationalPorts,
    request_id: RequestId,
) -> Vec<ServiceAuditPhaseV1> {
    let scan = ports
        .scan_administration_audit(AdministrationAuditScanRequest::new(
            None,
            StorageScanLimit::new(64).expect("audit scan limit"),
        ))
        .expect("scan administration audit");
    let AdministrationAuditScan::ExactEnd { records } = scan else {
        panic!("bounded control-plane fixture must reach exact audit end");
    };
    records
        .into_iter()
        .filter_map(|item| match item.into_parts().0 {
            StoredAdministrationAuditRecordV1::Service(record)
                if record.request_id() == request_id =>
            {
                Some(record.phase())
            }
            StoredAdministrationAuditRecordV1::Service(_)
            | StoredAdministrationAuditRecordV1::Catalog(_)
            | StoredAdministrationAuditRecordV1::Capability(_)
            | StoredAdministrationAuditRecordV1::QueryModule(_)
            | StoredAdministrationAuditRecordV1::ReactiveModule(_)
            | StoredAdministrationAuditRecordV1::Retention(_)
            | StoredAdministrationAuditRecordV1::Replication(_) => None,
        })
        .collect()
}

fn open_operational(store: RedbStore) -> RedbOperationalPorts {
    let mut session = store
        .begin_structural_evidence(startup_inputs())
        .expect("begin structural validation");
    let database_id = session.database_id();
    let open_session_id = session.open_session_id();
    let limit = EvidencePageLimit::new(64).expect("evidence page limit");
    let mut cursor = StructuralEvidenceCursor::start(database_id, open_session_id);
    let structural_end = loop {
        match session
            .read_structural_evidence(cursor, limit)
            .expect("read structural evidence")
        {
            StructuralEvidencePage::Page { findings, next, .. } => {
                assert!(findings.is_empty(), "valid fixture has no findings");
                cursor = next;
            }
            StructuralEvidencePage::ExactEnd(end) => break end,
        }
    };
    let (history, historical_end) = validate_catalog_history(&mut session)
        .expect("validate catalog history")
        .into_parts();
    let opened = session
        .finish(structural_end, historical_end)
        .expect("finish structural validation");
    let riffdb_catalog::CatalogHistoryOutcome::Ready(history) = history else {
        panic!("V2-only control-plane fixture must not require index migration");
    };
    let riffdb_storage_api::StructuralOpenOutcome::Clean(opened) = opened else {
        panic!("V2-only control-plane fixture must finish with a clean structural open");
    };
    assert!(history.matches(opened.database_id(), opened.open_session_id()));
    let (_, _, _, dormant): (_, _, _, RedbDormantPorts) = opened.into_parts();
    dormant
        .into_operational_after_catalog_validation()
        .expect("activate operational ports")
}

fn startup_inputs() -> StartupValidationInputs {
    let key = ReadableDigestKey::v1(DigestKeyId::new(1).expect("digest key ID"));
    StartupValidationInputs::new(
        timestamp(BASE_SECONDS),
        ReadableCapabilityDigestInventory::new(vec![key]).expect("capability inventory"),
        ReadableIdempotencyDigestInventory::new(vec![key]).expect("idempotency inventory"),
    )
}

fn capability_digest(fill: u8) -> CapabilityTokenDigest {
    CapabilityTokenDigest::from_hmac_bytes(DigestKeyId::new(1).expect("digest key ID"), [fill; 32])
}

fn block_on<Output>(future: impl std::future::Future<Output = Output>) -> Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("build test runtime")
        .block_on(future)
}

fn database_id() -> DatabaseId {
    DatabaseId::from_bytes(uuid_bytes(0x11)).expect("database UUIDv7")
}

fn authorizing_capability_id() -> CapabilityId {
    capability_id(0x21)
}

fn capability_id(seed: u8) -> CapabilityId {
    CapabilityId::from_bytes(uuid_bytes(seed)).expect("capability UUIDv7")
}

fn request_id(seed: u8) -> RequestId {
    RequestId::from_bytes(uuid_bytes(seed)).expect("request UUIDv7")
}

fn administration_sequence(value: u64) -> AdministrationSequence {
    AdministrationSequence::try_from(value).expect("nonzero administration sequence")
}

fn environment() -> Environment {
    Environment::new("integration").expect("environment")
}

fn audience() -> Audience {
    Audience::new("grpc").expect("audience")
}

fn timestamp(seconds: i64) -> Timestamp {
    Timestamp::new(seconds, 0).expect("timestamp")
}

fn uuid_bytes(fill: u8) -> [u8; 16] {
    let mut bytes = [fill; 16];
    bytes[6] = 0x70 | (fill & 0x0f);
    bytes[8] = 0x80 | (fill & 0x3f);
    bytes
}
