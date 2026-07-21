#![forbid(unsafe_code)]

//! Public coordinator and real-redb evidence for ordered service-audit appends.

use std::future::Future;
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use riffdb_catalog::validate_catalog_history;
use riffdb_commit::{
    AdministrationAuditAdmissionError, AdministrationAuditExecutionError,
    AdministrationAuditInputView, AdministrationClock, AdministrationClockError, AdmissionClock,
    AdmissionClockError, CommandExecutionAdmissionError, CoordinatorDurability,
    CoordinatorLifecycleState, CoordinatorWorkloadCapacity, ProvenanceIdSource,
    ProvenanceIdSourceError, RunningCommandCoordinator,
};
use riffdb_conflict::{ConflictManager, ConflictManagerConfig, ShardedConflictManager};
use riffdb_storage_api::{
    AdministrationAuditReader, AdministrationAuditScan, AdministrationAuditScanRequest,
    AuthoritativePointReader, DatabaseInitializationPort, DatabaseInitializationResult,
    EvidencePageLimit, ReadableCapabilityDigestInventory, ReadableDigestKey,
    ReadableIdempotencyDigestInventory, StartupValidationInputs, StorageErrorKind,
    StorageScanLimit, StoredAdministrationAuditRecordV1, StoredServiceAuditRecordV1,
    StructuralEvidenceCursor, StructuralEvidenceOpen, StructuralEvidencePage,
    StructuralEvidenceSession,
};
use riffdb_storage_redb::{
    RedbDormantPorts, RedbOperationalPorts, RedbStore, RedbTestController, RedbTestOperation,
    RedbTestPhase,
};
use riffdb_types::{
    ActorId, ActorKind, AdministrationSequence, ApprovalId, CapabilityId, CommandId,
    CommitSequence, ContractLineage, ContractVersion, DatabaseId, DigestKeyId, RequestId,
    ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceAuditTargetV1, ServiceAuditTargetsV1,
    ServiceIngressKindV1, ServiceOperationV1, Timestamp,
};

const BASE_SECONDS: i64 = 1_700_100_000;
static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

struct AuditDatabase(PathBuf);

impl AuditDatabase {
    fn create(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "riffdb-service-audit-ordering-{label}-{}-{}.redb",
            std::process::id(),
            NEXT_PATH.fetch_add(1, Ordering::Relaxed)
        ));
        let mut store = RedbStore::open(&path).expect("create service-audit database");
        assert_eq!(
            store
                .initialize_database(database_id())
                .expect("initialize service-audit database"),
            DatabaseInitializationResult::Installed(database_id())
        );
        drop(store);
        Self(path)
    }

    fn open(&self) -> RedbOperationalPorts {
        open_operational(RedbStore::open(&self.0).expect("open service-audit database"))
    }

    fn open_with_controller(&self, controller: RedbTestController) -> RedbOperationalPorts {
        open_operational(
            RedbStore::open_with_test_controller(&self.0, controller)
                .expect("open controlled service-audit database"),
        )
    }
}

impl Drop for AuditDatabase {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[derive(Clone)]
struct CheckedAuditInput {
    request_id: RequestId,
    operation: ServiceOperationV1,
    phase: ServiceAuditPhaseV1,
    principal_id: ActorId,
    actor_kind: ActorKind,
    capability_id: CapabilityId,
    capability_revision: NonZeroU64,
    ingress: ServiceIngressKindV1,
    targets: ServiceAuditTargetsV1,
    approval_id: Option<ApprovalId>,
    link: ServiceAuditLinkV1,
}

impl CheckedAuditInput {
    fn with_phase(&self, phase: ServiceAuditPhaseV1) -> Self {
        Self {
            phase,
            ..self.clone()
        }
    }
}

impl AdministrationAuditInputView for CheckedAuditInput {
    fn request_id(&self) -> &RequestId {
        &self.request_id
    }

    fn operation(&self) -> &ServiceOperationV1 {
        &self.operation
    }

    fn phase(&self) -> &ServiceAuditPhaseV1 {
        &self.phase
    }

    fn principal_id(&self) -> &ActorId {
        &self.principal_id
    }

    fn actor_kind(&self) -> &ActorKind {
        &self.actor_kind
    }

    fn capability_id(&self) -> &CapabilityId {
        &self.capability_id
    }

    fn capability_revision(&self) -> &NonZeroU64 {
        &self.capability_revision
    }

    fn ingress(&self) -> &ServiceIngressKindV1 {
        &self.ingress
    }

    fn targets(&self) -> &ServiceAuditTargetsV1 {
        &self.targets
    }

    fn approval_id(&self) -> Option<&ApprovalId> {
        self.approval_id.as_ref()
    }

    fn link(&self) -> &ServiceAuditLinkV1 {
        &self.link
    }
}

struct UnusedAdmissionClock;

impl AdmissionClock for UnusedAdmissionClock {
    fn now(&self) -> Result<Timestamp, AdmissionClockError> {
        Err(AdmissionClockError)
    }
}

struct ScriptedAdministrationClock {
    seconds: Vec<i64>,
    calls: AtomicUsize,
}

impl ScriptedAdministrationClock {
    fn new(seconds: impl IntoIterator<Item = i64>) -> Self {
        Self {
            seconds: seconds.into_iter().collect(),
            calls: AtomicUsize::new(0),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }
}

impl AdministrationClock for ScriptedAdministrationClock {
    fn now(&self) -> Result<Timestamp, AdministrationClockError> {
        let index = self.calls.fetch_add(1, Ordering::Relaxed);
        let seconds = self
            .seconds
            .get(index)
            .copied()
            .ok_or(AdministrationClockError)?;
        Timestamp::new(seconds, 0).map_err(|_| AdministrationClockError)
    }
}

struct UnusedProvenanceSource;

impl ProvenanceIdSource for UnusedProvenanceSource {
    fn next_provenance_id(&self) -> Result<riffdb_types::ProvenanceId, ProvenanceIdSourceError> {
        Err(ProvenanceIdSourceError)
    }
}

fn start_coordinator(
    ports: RedbOperationalPorts,
    administration_clock: Arc<ScriptedAdministrationClock>,
) -> RunningCommandCoordinator {
    let conflicts: Arc<dyn ConflictManager> = Arc::new(
        ShardedConflictManager::new(ConflictManagerConfig::default())
            .expect("start conflict manager"),
    );
    let admission_clock: Arc<dyn AdmissionClock> = Arc::new(UnusedAdmissionClock);
    let administration_clock: Arc<dyn AdministrationClock> = administration_clock;
    let provenance_source: Arc<dyn ProvenanceIdSource> = Arc::new(UnusedProvenanceSource);
    RunningCommandCoordinator::start(
        CoordinatorWorkloadCapacity::new(4).expect("nonzero workload capacity"),
        CoordinatorDurability::Sync,
        ports,
        conflicts,
        admission_clock,
        administration_clock,
        provenance_source,
    )
    .expect("start production coordinator")
}

fn audit_input(request_seed: u8, phase: ServiceAuditPhaseV1) -> CheckedAuditInput {
    CheckedAuditInput {
        request_id: request_id(request_seed),
        operation: ServiceOperationV1::ExplainCommand,
        phase,
        principal_id: ActorId::new(format!("audit-principal-{request_seed}"))
            .expect("bounded principal"),
        actor_kind: ActorKind::Human,
        capability_id: CapabilityId::from_bytes(uuid_bytes(request_seed.wrapping_add(0x20)))
            .expect("capability UUIDv7"),
        capability_revision: NonZeroU64::new(7).expect("nonzero capability revision"),
        ingress: ServiceIngressKindV1::Grpc,
        targets: explain_targets(),
        approval_id: Some(ApprovalId::new("reviewed").expect("bounded approval ID")),
        link: ServiceAuditLinkV1::None,
    }
}

fn explain_targets() -> ServiceAuditTargetsV1 {
    let lineage = ContractLineage::new("com.example.audit").expect("contract lineage");
    ServiceAuditTargetsV1::new([
        ServiceAuditTargetV1::Command {
            lineage: lineage.clone(),
            command_id: CommandId::new(9).expect("command ID"),
        },
        ServiceAuditTargetV1::ContractVersion {
            lineage,
            version: ContractVersion::new(3).expect("contract version"),
        },
    ])
    .expect("canonical audit targets")
}

fn submit(
    executor: &riffdb_commit::AdministrationAuditExecutor,
    input: CheckedAuditInput,
) -> riffdb_commit::AdministrationAuditReceipt {
    block_on(executor.reserve_capacity())
        .expect("reserve audit workload capacity")
        .submit(Box::new(input))
        .expect("synchronously accept audit input")
}

fn scan_service_records(ports: &RedbOperationalPorts) -> Vec<StoredServiceAuditRecordV1> {
    let scan = ports
        .scan_administration_audit(AdministrationAuditScanRequest::new(
            None,
            StorageScanLimit::new(64).expect("scan limit"),
        ))
        .expect("scan administration audit");
    let AdministrationAuditScan::ExactEnd { records } = scan else {
        panic!("bounded audit fixture must reach exact end");
    };
    records
        .into_iter()
        .map(|item| match item.into_parts().0 {
            StoredAdministrationAuditRecordV1::Service(record) => record,
            StoredAdministrationAuditRecordV1::Catalog(_)
            | StoredAdministrationAuditRecordV1::Capability(_) => {
                panic!("service-only fixture contains a control-plane record")
            }
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
        .expect("validate empty catalog history")
        .into_parts();
    let opened = session
        .finish(structural_end, historical_end)
        .expect("finish structural validation");
    assert!(
        history.matches(opened.database_id(), opened.open_session_id()),
        "catalog proof belongs to this structural-open session"
    );
    let (_, _, dormant): (_, _, RedbDormantPorts) = opened.into_parts();
    dormant
        .into_operational_after_catalog_validation()
        .expect("activate structurally checked redb ports")
}

fn startup_inputs() -> StartupValidationInputs {
    let key = ReadableDigestKey::v1(DigestKeyId::new(1).expect("digest key ID"));
    StartupValidationInputs::new(
        timestamp(BASE_SECONDS),
        ReadableCapabilityDigestInventory::new(vec![key]).expect("capability key inventory"),
        ReadableIdempotencyDigestInventory::new(vec![key]).expect("idempotency key inventory"),
    )
}

fn block_on<Output>(future: impl Future<Output = Output>) -> Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("build test runtime")
        .block_on(future)
}

fn database_id() -> DatabaseId {
    DatabaseId::from_bytes(uuid_bytes(0x11)).expect("database UUIDv7")
}

fn request_id(seed: u8) -> RequestId {
    RequestId::from_bytes(uuid_bytes(seed)).expect("request UUIDv7")
}

fn timestamp(seconds: i64) -> Timestamp {
    Timestamp::new(seconds, 0).expect("canonical timestamp")
}

fn uuid_bytes(seed: u8) -> [u8; 16] {
    let mut bytes = [seed; 16];
    bytes[6] = 0x70 | (seed & 0x0f);
    bytes[8] = 0x80 | (seed & 0x3f);
    bytes
}

#[test]
fn production_actor_persists_fifo_order_without_timestamp_order_or_sequence_gaps() {
    let database = AuditDatabase::create("fifo");
    let clock = Arc::new(ScriptedAdministrationClock::new([
        BASE_SECONDS + 30,
        BASE_SECONDS + 10,
        BASE_SECONDS + 20,
        BASE_SECONDS,
        BASE_SECONDS + 40,
    ]));
    let running = start_coordinator(database.open(), Arc::clone(&clock));
    let executor = running.administration_audit_executor();
    let started = audit_input(0x31, ServiceAuditPhaseV1::Started);
    let denied = audit_input(0x32, ServiceAuditPhaseV1::Denied);
    let succeeded = started.with_phase(ServiceAuditPhaseV1::Succeeded);
    let failed = audit_input(0x33, ServiceAuditPhaseV1::Failed);

    let started_receipt = submit(&executor, started.clone());
    let denied_receipt = submit(&executor, denied.clone());
    drop(denied_receipt);
    let succeeded_receipt = submit(&executor, succeeded.clone());
    assert_eq!(block_on(started_receipt.completion()), Ok(()));
    assert_eq!(block_on(succeeded_receipt.completion()), Ok(()));

    assert_eq!(
        block_on(submit(&executor, succeeded.clone()).completion()),
        Err(AdministrationAuditExecutionError::PhaseConflict),
        "a second terminal is rejected without consuming a sequence"
    );
    assert_eq!(
        block_on(submit(&executor, failed.clone()).completion()),
        Ok(())
    );
    running.shutdown().expect("drain and join coordinator");
    assert_eq!(clock.calls(), 5, "each attempted append samples once");

    let ports = database.open();
    let records = scan_service_records(&ports);
    assert_eq!(records.len(), 4);
    let expected = [
        (&started, BASE_SECONDS + 30),
        (&denied, BASE_SECONDS + 10),
        (&succeeded, BASE_SECONDS + 20),
        (&failed, BASE_SECONDS + 40),
    ];
    for (index, (record, (input, expected_seconds))) in records.iter().zip(expected).enumerate() {
        assert_eq!(
            record.administration_sequence(),
            AdministrationSequence::try_from((index + 1) as u64).expect("nonzero sequence")
        );
        assert_eq!(record.request_id(), input.request_id);
        assert_eq!(record.timestamp(), timestamp(expected_seconds));
        assert_eq!(record.operation(), input.operation);
        assert_eq!(record.phase(), input.phase);
        assert_eq!(record.ingress(), input.ingress);
        assert_eq!(record.targets(), &input.targets);
        assert_eq!(record.approval_id(), input.approval_id.as_ref());
        assert_eq!(record.link(), input.link);
        let principal = record.principal().expect("authenticated audit principal");
        assert_eq!(principal.principal_id(), &input.principal_id);
        assert_eq!(principal.actor_kind(), input.actor_kind);
        assert_eq!(principal.capability_id(), input.capability_id);
        assert_eq!(principal.capability_revision(), input.capability_revision);
    }
    assert_eq!(
        records[0]
            .targets()
            .as_slice()
            .iter()
            .map(ServiceAuditTargetV1::tag)
            .collect::<Vec<_>>(),
        vec![0x02, 0x04],
        "the executor preserves the checked canonical target order"
    );
    assert_eq!(
        ports
            .read_commit(CommitSequence::first())
            .expect("read application sequence space"),
        None,
        "service-audit appends allocate no application commit sequence"
    );
}

#[test]
fn post_commit_unknown_fences_shared_admission_before_completion() {
    let database = AuditDatabase::create("unknown");
    let controller =
        RedbTestController::return_unknown_after_commit(RedbTestOperation::ServiceAudit);
    let clock = Arc::new(ScriptedAdministrationClock::new([BASE_SECONDS + 50]));
    let running = start_coordinator(
        database.open_with_controller(controller.clone()),
        Arc::clone(&clock),
    );
    let executor = running.administration_audit_executor();
    let command_executor = running.command_executor();
    let held_permit = block_on(executor.reserve_capacity()).expect("reserve before uncertainty");
    let input = audit_input(0x41, ServiceAuditPhaseV1::Started);
    let result = block_on(submit(&executor, input.clone()).completion());
    let Err(AdministrationAuditExecutionError::Storage(error)) = result else {
        panic!("post-commit failpoint must return an uncertain storage result");
    };
    assert_eq!(error.kind(), StorageErrorKind::CommitStatusUnknown);
    assert_eq!(clock.calls(), 1);
    assert_eq!(
        executor.lifecycle_state(),
        CoordinatorLifecycleState::Fenced
    );
    assert!(matches!(
        held_permit.submit(Box::new(audit_input(0x42, ServiceAuditPhaseV1::Denied))),
        Err(AdministrationAuditAdmissionError::Fenced)
    ));
    assert!(matches!(
        block_on(executor.reserve_capacity()),
        Err(AdministrationAuditAdmissionError::Fenced)
    ));
    assert!(matches!(
        block_on(command_executor.reserve_capacity()),
        Err(CommandExecutionAdmissionError::Fenced)
    ));
    running.shutdown().expect("join fenced coordinator");

    let service_phases = controller
        .events()
        .into_iter()
        .filter(|event| event.operation() == RedbTestOperation::ServiceAudit)
        .map(|event| event.phase())
        .collect::<Vec<_>>();
    assert_eq!(
        service_phases,
        vec![
            RedbTestPhase::BeforeEngineCommit,
            RedbTestPhase::AfterEngineCommit
        ]
    );

    let ports = database.open();
    let records = scan_service_records(&ports);
    assert_eq!(
        records.len(),
        1,
        "the uncertain append committed exactly once"
    );
    assert_eq!(
        records[0].administration_sequence(),
        AdministrationSequence::first()
    );
    assert_eq!(records[0].request_id(), input.request_id);
    assert_eq!(records[0].phase(), ServiceAuditPhaseV1::Started);
    assert_eq!(records[0].targets(), &input.targets);
    assert_eq!(
        records[0].link(),
        ServiceAuditLinkV1::None,
        "the coordinator does not infer a terminal after uncertainty"
    );
    assert_eq!(
        ports
            .read_commit(CommitSequence::first())
            .expect("read application sequence space"),
        None,
        "the uncertain audit commit allocated no application sequence"
    );
}
