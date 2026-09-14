#![forbid(unsafe_code)]

//! Child-process crash and reopen evidence for the redb storage boundary.

use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use redb::{
    Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition, TableHandle,
};
use riffdb_catalog::{
    CatalogHistoryOutcome, CatalogIndexMigrationContext, CatalogIndexMigrationDriveError,
    CatalogIndexMigrationDriver, ValidatedContractBundle, validate_catalog_history,
};
use riffdb_contract_compiler::compile_contract_source;
use riffdb_storage_api::{
    AdministrationAuditReader, AdministrationAuditScan, AdministrationAuditScanRequest,
    AdministrationSequenceAllocator, AdmissionLookupResultV1, AdmissionRepository,
    AdmissionRequestV1, AdmissionResultV1, AffectedEntityV1, AffectedEpochCurrentState,
    AffectedIndexEpochTargets, ApplicationCommandTransactionPort, AssignedCommandSequence,
    AtomicCommandRecordSet, AuditPrincipalV1, AuditedAdmissionRepository,
    AuditedAdmissionRequestV1, AuditedAdmissionResultV1, AuthoritativeIndexScanPage,
    AuthoritativeIndexScanRequest, AuthoritativePointReader, AuthoritativeScanReader,
    BootstrapDigestCandidatesV1, BootstrapServiceAuditStartV1, CandidateAdmissionResult,
    CandidateCapacityResult, CandidateStartResult, CapabilityBootstrapAdministrationRepository,
    CapabilityBootstrapIntentV1, CapabilityBootstrapResult, CapabilityGrantV1,
    CapabilityPermissionKindV1, CapabilityPermissionV1, CapabilityPermissionsV1, CapabilityReader,
    CapabilityRequestedRecordV1, CatalogActivationIntentV1, CatalogActivationResult,
    CatalogAdministrationRepository, ChangelogEmissionStateV1, ChangelogEntryClassV1,
    ChangelogEntryClassV2, ChangelogFrameConsumer, ChangelogFrameConsumerV2, ChangelogFrameV1,
    ChangelogFrameV2, ChangelogResyncReasonV1, ChangelogStreamValidatorV1,
    ChangelogV2RotationReceipt, CommandCandidateAdmission, CommandCandidateAffectedEpochRead,
    CommandCandidateAwaitingCapacity, CommandCandidateAwaitingValidation,
    CommandCandidateCapacityReserved, CommandCandidateSequenceAssigned, CommandCandidateStateRead,
    CommandWriteSetPlanV1, CoordinateConsumerAcknowledgementV1, CurrentIndexGenerationObservation,
    DatabaseIdentityProbe, DatabaseIdentityProbePort, DatabaseInitializationPort, DeclaredOutcome,
    DeferredCommandEpoch, DeferredCommandEpochPort, DeferredCommandFence,
    DeferredNonEmptyCommandBatch, DeleteAwareEntityFollowerV2, DurabilityMode,
    DurableKeySchemaBindingV1, EmptyCommandBatch, EncodedChangelogFrameV1, EncodedChangelogFrameV2,
    EncodedWriteSetUpperBoundResultV1, EntityMutation, EntityObservation, EntityPostImage,
    EntityReplicaBootstrapManifestV2, EntityTarget, EntityTransitionFingerprint, EvaluationBudget,
    EventConsumerIdentityV1, EventConsumerTransitionResultV1, EventIntent,
    EventPolicyAdmissionFenceV1, EventPolicyAdmissionObservationV1, EventRoutePageLimit,
    EventRouteScanRequestV1, EventRouteScanV1, EventRouteUpperFenceV1, EvidencePageLimit,
    ExecutablePlanRef, ExecutionFailureAdmissionRechecked, ExecutionFailureAdmissionResult,
    ExecutionFailureAwaitingDecision, ExecutionFailureTransitionPort,
    ExecutionFailureTransitionRequestV1, ExpectedEntityState, IdempotencyIdentity,
    IdempotencyKeyDigest, IdempotencyLookupCandidatesV1, IndexEntryMutationV1, IndexEpochAdvanceV1,
    IndexEpochPosition, IndexRangeTarget, MAX_INDEX_MIGRATION_PAGE_BYTES,
    MAX_INDEX_MIGRATION_PAGE_ENTRIES, NonEmptyCommandBatch, OpenSessionId, OutboxClaimV1,
    OutboxDestinationIdV1, OutboxPageLimit, OutboxRepository, OutboxStatusObservationV1,
    OutboxStatusReadResultV1, OutboxTransitionResultV1, PartitionEventRouteReader,
    PartitionIndexTarget, PartitionScopeV1, PendingOutboxScanV1, PreEvaluationCommitContext,
    ReadSnapshot, ReadableCapabilityDigestInventory, ReadableDigestKey,
    ReadableIdempotencyDigestInventory, ServiceAuditAppendIntentV1, ServiceAuditAppendRepository,
    ServiceAuditAppendResult, SnapshotReader, SnapshotRequest, StartupValidationInputs,
    StorageScanLimit, StoredAdministrationAuditRecordV1, StoredAdmissionStateV1,
    StoredAdmittedProvenanceClaimsV1, StoredCapabilityRecordV1, StoredContractBundleV1,
    StoredDurableEventV1, StoredEntityRecordV1, StoredIndexEntryV1, StoredIndexEntryV2,
    StoredOutcomeV1, StoredPendingAdmissionV1, StoredProvenanceRecordV1, StoredReadDependenciesV1,
    StoredServiceAuditRecordV1, StructuralEvidenceCursor, StructuralEvidenceOpen,
    StructuralEvidencePage, StructuralEvidenceSession, StructuralFinding, StructuralFindingCode,
    StructuralFindingScope, StructuralOpenOutcome, StructurallyOpened,
    ValidatedPrefixEntityTransitionCounts, command_write_set_upper_bound_v1, decode_index_entry_v1,
    decode_index_entry_v2, decode_index_migration_row, derive_event_hash_v1,
    encode_administration_sequence_allocator_v1, encode_index_entry_v1_fixture,
    encode_index_entry_v2, encode_record_registry_v2, encode_service_audit_record_v2,
};
use riffdb_storage_redb::{
    ProtectedEventConsumerLeaseV1, ProtectedEventConsumerResolutionV1, RedbCommitProfile,
    RedbDormantPorts, RedbDurabilityEpoch, RedbOperationalPorts, RedbStartupIndexMigrationPort,
    RedbStore, RedbTestController, RedbTestOperation,
};
use riffdb_types::{
    ActorId, ActorKind, AdministrationSequence, AggregateTypeId, Audience, CanonicalInputHash,
    CanonicalRecord, CanonicalValue, CapabilityId, CapabilityTokenDigest, CommitSequence,
    DatabaseId, DigestKeyId, EntityKeyBuilder, EntityTypeId, EntityVersion, Environment,
    EventConsumerName, EventId, EventLeaseToken, EventTypeId, ExecutionFailureCode, FieldId,
    IndexEntryKey, IndexEntryKeyBuilder, IndexId, LogicalTime, MAX_CANONICAL_DOCUMENT_BYTES,
    OutcomeId, PartitionKeyBuilder, PartitionKeyHash, ProvenanceId, QueryParameterHash,
    ReactiveModuleHash, ReactiveOperationName, RequestId, ServiceAuditLinkV1, ServiceAuditPhaseV1,
    ServiceAuditTargetV1, ServiceAuditTargetsV1, ServiceIngressKindV1, ServiceOperationV1,
    TenantId, TenantScope, Timestamp, hash_partition_key,
};

const CHILD_MODE: &str = "RIFFDB_STORAGE_RECOVERY_CHILD_MODE";
mod changelog_compatibility;
#[path = "v3_command_attribution.rs"]
mod v3_command_attribution;
#[path = "v3_command_receipts.rs"]
mod v3_command_receipts;

const CHILD_PATH: &str = "RIFFDB_STORAGE_RECOVERY_CHILD_PATH";
const CHILD_COMMIT_PROFILE: &str = "RIFFDB_STORAGE_RECOVERY_CHILD_COMMIT_PROFILE";
const CHILD_PRUNE_TARGET: &str = "RIFFDB_STORAGE_RECOVERY_CHILD_PRUNE_TARGET";
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
const SECONDARY_INDEXES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("secondary_indexes");
const AUDIT: TableDefinition<&[u8], &[u8]> = TableDefinition::new("audit");
const VALIDATED_PREFIX_ENTITY_HEADS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("validated_prefix_entity_heads");
const META_ADMINISTRATION_SEQUENCE: &str = "next_administration_sequence";
static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

const STORAGE_RECOVERY_CONTRACT: &str = r#"
contract StorageRecovery version 1 {
  entity Row {
    key (id: u64)
    field value: u64
    index ByValue(value)
  }

  event RowCreated {
    partition_by (id)
    policy_anchor current Row(id: id)
    id: u64
    value: u64
  }

  aggregate Rows {
    root Row
    partition_by id
    conflict_key (id)
  }

  row policy RowAccess on Row {
    allow read when true
  }

  command CreateRow {
    input idempotency_key: string<128>
    input id: u64
    input value: u64

    idempotency_key idempotency_key
    create Row(id) as row
      else RowAlreadyExists { id: id }

    set row.value = value

    emit RowCreated { id: id, value: value }
    return RowCreatedOutcome { row: row }
  }
}
"#;

/// Two aggregates on one partition route, and one command that writes both
/// (ADR-0170).
///
/// Kept separate from [`STORAGE_RECOVERY_CONTRACT`] rather than folded into it.
/// Every fixture in this file is bound to that contract's bundle hash, plan
/// hash, and entity/index identifiers, so extending it to carry a second
/// aggregate would restate the durable identity of every existing arm to prove
/// one new property.
///
/// `Row` and `Ledger` both partition by `id`, so `Adjust` derives one route
/// across two aggregates: exactly the shape `RDB-C017` used to refuse and
/// ADR-0170 admits. Its atomicity across a restart is the open WP-721 proof.
const CROSS_AGGREGATE_RECOVERY_CONTRACT: &str = r#"
contract CrossAggregateRecovery version 1 {
  entity Row {
    key (id: u64)
    field value: u64
    index ByValue(value)
    delete_policy no_inbound
  }

  entity Ledger {
    key (id: u64, entry: u64)
    field amount: u64
    index ByAmount(amount)
    delete_policy no_inbound
  }

  event Adjusted {
    partition_by (id)
    id: u64
    value: u64
  }

  aggregate Rows {
    root Row
    partition_by id
    conflict_key (id)
  }

  aggregate Ledgers {
    root Ledger
    partition_by id
    conflict_key (id, entry)
  }

  command Adjust {
    input idempotency_key: string<128>
    input id: u64
    input entry: u64
    input value: u64

    idempotency_key idempotency_key
    create Row(id) as row
      else RowAlreadyExists { id: id }
    create Ledger(id, entry) as ledger
      else LedgerAlreadyExists { id: id }

    set row.value = value
    set ledger.amount = value

    emit Adjusted { id: id, value: value }
    return AdjustedOutcome { row: row }
  }
}
"#;

/// Whole-directory scope for one test's database: `.0` is the database path
/// inside a [`ScratchScope`] that removes the directory — database plus every
/// side file it grows (journal, checkpoint, spare, durable-format marker, …)
/// — on `Drop`, pass, fail, or panic. Cleanup no longer depends on a
/// hand-maintained side-file list.
struct TestDatabasePath(
    PathBuf,
    // Held only so `Drop` removes the whole scope.
    #[allow(dead_code)] ScratchScope,
);

impl TestDatabasePath {
    fn new(label: &str) -> Self {
        let scope = ScratchScope::new(label);
        Self(scope.path().join("db.redb"), scope)
    }
}

/// Crash-harness scratch directory under `CARGO_TARGET_TMPDIR`.
///
/// Children armed to `SIGABRT` write only under paths the parent hands them,
/// so this parent-scope guard covers their artifacts too. A killed *parent*
/// skips `Drop`; its directories embed the parent pid and are swept by the
/// next run (dead pid, or older than 24 hours as a pid-reuse belt),
/// mirroring `riffdb-bench-root::sweep_stale` and
/// `riffdb_testkit::scratch::ScratchDir`, which this harness cannot import
/// because riffdb-testkit depends on riffdb-storage-redb.
struct ScratchScope(PathBuf);

const SCRATCH_SCOPE_PREFIX: &str = "riffdb-storage-recovery-";

impl ScratchScope {
    fn new(label: &str) -> Self {
        static SWEEP_ONCE: OnceLock<()> = OnceLock::new();
        let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR"));
        if root.exists() {
            SWEEP_ONCE.get_or_init(|| sweep_dead_scratch_scopes(&root));
        } else {
            std::fs::create_dir_all(&root).expect("create scratch root");
        }
        // create_dir (not create_dir_all) plus retry: after pid reuse a
        // stale scope carrying our pid survives the sweep, and silently
        // inheriting its database and journals would make a *recovery* test
        // non-hermetic. Terminates because the ordinal is monotonic.
        loop {
            let ordinal = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
            let path = root.join(format!(
                "{SCRATCH_SCOPE_PREFIX}{label}-{}-{ordinal}",
                std::process::id()
            ));
            match std::fs::create_dir(&path) {
                Ok(()) => return Self(path),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => panic!("create test scope directory: {error}"),
            }
        }
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for ScratchScope {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Removes scope directories whose embedded pid is dead or whose mtime is
/// older than 24 hours. Best-effort: failures only lose hygiene, never tests.
fn sweep_dead_scratch_scopes(root: &Path) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    let now = std::time::SystemTime::now();
    let max_age = std::time::Duration::from_secs(24 * 60 * 60);
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(pid) = path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.strip_prefix(SCRATCH_SCOPE_PREFIX))
            .and_then(extract_scope_pid)
        else {
            continue;
        };
        if pid == std::process::id() {
            continue;
        }
        let pid_dead = !Path::new("/proc").join(pid.to_string()).exists();
        let too_old = entry
            .metadata()
            .ok()
            .and_then(|meta| meta.modified().ok())
            .and_then(|mtime| now.duration_since(mtime).ok())
            .is_some_and(|age| age > max_age);
        if pid_dead || too_old {
            match std::fs::remove_dir_all(&path) {
                Ok(()) => eprintln!(
                    "storage_recovery_matrix: swept stale scope {} \
                     (pid_dead={pid_dead} too_old={too_old})",
                    path.display()
                ),
                Err(error) => eprintln!(
                    "storage_recovery_matrix: failed to sweep {}: {error}",
                    path.display()
                ),
            }
        }
    }
}

/// Strict `…-<pid>-<ordinal>` tail parse; anything else is not ours to sweep.
fn extract_scope_pid(tail: &str) -> Option<u32> {
    let mut parts = tail.rsplit('-');
    let ordinal = parts.next()?;
    let pid = parts.next()?;
    if ordinal.is_empty() || !ordinal.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    pid.parse().ok()
}

fn database_id() -> DatabaseId {
    DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10])
        .expect("valid deterministic database ID")
}

fn uuid_bytes(fill: u8) -> [u8; 16] {
    let mut bytes = [fill; 16];
    bytes[6] = 0x70 | (fill & 0x0f);
    bytes[8] = 0x80 | (fill & 0x3f);
    bytes
}

fn ordinal_uuid_bytes(fill: u8, ordinal: u64) -> [u8; 16] {
    if ordinal <= u64::from(u8::MAX) {
        return uuid_bytes(fill.wrapping_add(ordinal as u8));
    }
    let mut bytes = uuid_bytes(fill);
    bytes[9..].copy_from_slice(&ordinal.to_be_bytes()[1..]);
    bytes
}

fn run_crashing_child(mode: &str, path: &Path) {
    run_crashing_child_with_profile(mode, path, RedbCommitProfile::Standard);
}

#[cfg(feature = "test-fixtures")]
fn run_externally_killed_command_child(path: &Path) {
    let marker = path.with_extension("external-kill.ready");
    let mut child = Command::new(std::env::current_exe().expect("current test executable"))
        .arg("--exact")
        .arg("process_recovery_child")
        .arg("--nocapture")
        .env(CHILD_MODE, "external-before-command-batch-commit")
        .env(CHILD_PATH, path)
        .env(CHILD_COMMIT_PROFILE, "hardened")
        .env("RIFFDB_TEST_REDB_EXTERNAL_KILL_BARRIER", &marker)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn externally killed recovery child");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !marker.exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "redb child never reached its before-commit barrier"
        );
        std::thread::yield_now();
    }
    assert_eq!(std::fs::read(&marker).expect("read barrier"), b"armed\n");
    child.kill().expect("SIGKILL exact redb child");
    let status = child.wait().expect("wait for SIGKILL child");
    assert!(!status.success());
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(status.signal(), Some(9), "runner must send SIGKILL");
    }
    std::fs::remove_file(marker).expect("remove barrier marker");
}

/// Every armed child terminates through `std::process::abort()` (SIGABRT).
/// Discriminating on the signal keeps 'before' arms honest: a child that
/// panics without reaching its failpoint exits with a plain nonzero code and
/// must fail here instead of passing the parent's negative assertions vacuously.
fn assert_child_aborted(status: std::process::ExitStatus, label: &str) {
    assert!(
        !status.success(),
        "the armed {label} child must terminate abruptly"
    );
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        const SIGABRT: i32 = 6;
        assert_eq!(
            status.signal(),
            Some(SIGABRT),
            "the armed {label} child must die on SIGABRT at its failpoint, \
             not exit through an unrelated panic (status {status})"
        );
    }
}

fn run_crashing_child_prune(mode: &str, path: &Path, prune_target: u64) {
    let status = Command::new(std::env::current_exe().expect("current test executable"))
        .arg("--exact")
        .arg("process_recovery_child")
        .arg("--nocapture")
        .env(CHILD_MODE, mode)
        .env(CHILD_PATH, path)
        .env(CHILD_COMMIT_PROFILE, "standard")
        .env(CHILD_PRUNE_TARGET, prune_target.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("run recovery prune child");
    assert_child_aborted(status, "prune");
}

fn run_crashing_child_with_profile(mode: &str, path: &Path, profile: RedbCommitProfile) {
    let profile = match profile {
        RedbCommitProfile::Standard => "standard",
        RedbCommitProfile::Hardened => "hardened",
    };
    let status = Command::new(std::env::current_exe().expect("current test executable"))
        .arg("--exact")
        .arg("process_recovery_child")
        .arg("--nocapture")
        .env(CHILD_MODE, mode)
        .env(CHILD_PATH, path)
        .env(CHILD_COMMIT_PROFILE, profile)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("run recovery child");
    assert_child_aborted(status, "recovery");
}

fn complete_startup_pass(
    store: RedbStore,
) -> (
    OpenSessionId,
    CatalogHistoryOutcome,
    StructuralOpenOutcome<RedbDormantPorts, RedbStartupIndexMigrationPort>,
) {
    let digest_key = DigestKeyId::new(1).expect("digest key ID");
    let inputs = StartupValidationInputs::new(
        Timestamp::new(1_700_000_000, 0).expect("startup timestamp"),
        ReadableCapabilityDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("capability digest inventory"),
        ReadableIdempotencyDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("idempotency digest inventory"),
    );
    let mut session = store
        .begin_structural_evidence(inputs)
        .expect("begin exclusive structural evidence");
    let database_id = session.database_id();
    let open_session_id = session.open_session_id();
    let limit = EvidencePageLimit::new(64).expect("page limit");

    let mut structural_cursor = StructuralEvidenceCursor::start(database_id, open_session_id);
    let structural_end = loop {
        match session
            .read_structural_evidence(structural_cursor, limit)
            .expect("read structural evidence")
        {
            StructuralEvidencePage::Page { findings, next, .. } => {
                assert!(
                    findings.is_empty(),
                    "a valid recovery fixture has no findings: {findings:?}"
                );
                structural_cursor = next;
            }
            StructuralEvidencePage::ExactEnd(end) => break end,
        }
    };

    let (catalog_outcome, historical_end) = validate_catalog_history(&mut session)
        .expect("catalog validates the complete historical stream")
        .into_parts();
    let outcome = session
        .finish(structural_end, historical_end)
        .expect("finish structural evidence");
    (open_session_id, catalog_outcome, outcome)
}

fn complete_structural_open(store: RedbStore) -> StructurallyOpened<RedbDormantPorts> {
    let (_, catalog_outcome, outcome) = complete_startup_pass(store);
    assert!(matches!(catalog_outcome, CatalogHistoryOutcome::Ready(_)));
    let opened = match outcome {
        StructuralOpenOutcome::Clean(opened) => opened,
        StructuralOpenOutcome::MigrationRequired(_) => {
            panic!("V2 recovery fixture must not require index migration")
        }
    };
    assert_eq!(opened.database_id(), database_id());
    opened
}

fn collect_structural_findings(store: RedbStore) -> Vec<StructuralFinding> {
    let digest_key = DigestKeyId::new(1).expect("digest key ID");
    let inputs = StartupValidationInputs::new(
        Timestamp::new(1_700_000_000, 0).expect("startup timestamp"),
        ReadableCapabilityDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("capability digest inventory"),
        ReadableIdempotencyDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("idempotency digest inventory"),
    );
    let mut session = store
        .begin_structural_evidence(inputs)
        .expect("begin structural evidence");
    let mut cursor =
        StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
    let mut findings = Vec::new();
    loop {
        match session
            .read_structural_evidence(
                cursor,
                EvidencePageLimit::new(64).expect("structural page limit"),
            )
            .expect("read structural evidence")
        {
            StructuralEvidencePage::Page {
                findings: page,
                next,
                ..
            } => {
                findings.extend(page);
                cursor = next;
            }
            StructuralEvidencePage::ExactEnd(_) => return findings,
        }
    }
}

fn open_operational(store: RedbStore) -> RedbOperationalPorts {
    let opened = complete_structural_open(store);
    let (_, _, _, dormant) = opened.into_parts();
    dormant
        .into_operational_after_catalog_validation()
        .expect("activate storage fixture; WP-130 owns catalog-proof composition")
}

/// Which durable admission state the committing writer expects to find.
///
/// The two shapes are not interchangeable durable histories: the fused shape
/// never writes a `Pending` row or a physical audit row, while the two-phase
/// shape leaves both behind for the commit to consume.
#[derive(Clone, Copy, Eq, PartialEq)]
enum AdmissionShape {
    /// One transaction proves every candidate identity vacant and commits the
    /// terminal record — the shape every other fixture in this matrix uses.
    VacantTerminal,
    /// A durable `Pending` row already exists, admitted by an earlier
    /// transaction that also appended its physical `Started` audit row.
    ExistingPending,
}

#[derive(Clone)]
struct CommandFixture {
    candidates: IdempotencyLookupCandidatesV1,
    pending: StoredPendingAdmissionV1,
    context: PreEvaluationCommitContext,
    intent: riffdb_storage_api::CommitIntent,
    affected_targets: AffectedIndexEpochTargets,
    write_plan: CommandWriteSetPlanV1,
    snapshot: ReadSnapshot,
    records: AtomicCommandRecordSet,
    target: EntityTarget,
    range: IndexRangeTarget,
    index_key: IndexEntryKey,
}

fn validated_contract_bundle() -> &'static ValidatedContractBundle {
    static BUNDLE: OnceLock<ValidatedContractBundle> = OnceLock::new();
    BUNDLE.get_or_init(|| {
        ValidatedContractBundle::from_compiler_bundle(
            compile_contract_source(STORAGE_RECOVERY_CONTRACT)
                .expect("compile indexed storage recovery contract"),
        )
        .expect("validate indexed storage recovery bundle")
    })
}

fn plan() -> ExecutablePlanRef {
    let bundle = validated_contract_bundle();
    let command = bundle
        .bundle()
        .commands()
        .first()
        .expect("storage recovery command");
    ExecutablePlanRef::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        command.command_id(),
        command.plan_hash(),
    )
}

fn contract_bundle() -> StoredContractBundleV1 {
    validated_contract_bundle()
        .to_stored()
        .expect("stored contract bundle")
}

fn record(value: u64) -> CanonicalRecord {
    CanonicalRecord::new(vec![(
        FieldId::new(1).expect("field ID"),
        CanonicalValue::U64(value),
    )])
    .expect("canonical record")
}

/// Ordinal-parameterized fixture target: ordinal 1 is the original fixture
/// shape (entity/partition value 7); later ordinals commit disjoint rows.
fn target_and_index_at(ordinal: u64) -> (EntityTarget, IndexEntryKey, IndexRangeTarget) {
    let entity_type_id = EntityTypeId::new(1).expect("entity type ID");
    let mut entity_key = EntityKeyBuilder::new(entity_type_id);
    entity_key
        .push_u64(6 + ordinal)
        .expect("entity key component");
    let entity_key = entity_key.finish().expect("entity key");
    let target = EntityTarget::new(entity_type_id, entity_key.clone()).expect("entity target");

    let index_id = IndexId::new(1).expect("index ID");
    let mut index_key = IndexEntryKeyBuilder::new(index_id);
    index_key.push_u64(10).expect("index component");
    let index_key = index_key.finish(entity_key).expect("index entry key");
    let mut prefix = riffdb_storage_api::IndexRangePrefixBuilder::new(index_id);
    prefix.push_u64(10).expect("range component");
    let mut partition = PartitionKeyBuilder::new(AggregateTypeId::new(1).expect("aggregate"));
    partition
        .push_u64(6 + ordinal)
        .expect("partition component");
    let range = IndexRangeTarget::new(partition.finish().expect("partition key"), prefix.finish());
    (target, index_key, range)
}

fn command_fixture() -> CommandFixture {
    command_fixture_at(1)
}

/// Ordinal-parameterized committed command: ordinal 1 reproduces the original
/// fixture exactly; ordinal N commits sequence N over a disjoint entity.
fn command_fixture_at(ordinal: u64) -> CommandFixture {
    build_command_fixture(
        ordinal,
        ordinal,
        None,
        AdmissionShape::VacantTerminal,
        false,
    )
}

/// The same command as `command_fixture_at`, committed as the second phase of a
/// two-phase admission instead of a fused vacant-terminal one.
fn two_phase_command_fixture_at(ordinal: u64) -> CommandFixture {
    build_command_fixture(
        ordinal,
        ordinal,
        None,
        AdmissionShape::ExistingPending,
        false,
    )
}

/// A second write to the SAME entity as `prior`, committed at `ordinal`.
///
/// This is the ADR-0083 supersession chain in fixture form: two commit sequences
/// whose entity post-images occupy one physical key with different bytes. It is
/// what makes intermediate-frame prefix exactness observable at all — with only
/// disjoint entities, a frame derived from a later snapshot would be
/// indistinguishable from one derived correctly.
fn superseding_command_fixture_at(
    ordinal: u64,
    target_ordinal: u64,
    prior: &CommandFixture,
) -> CommandFixture {
    build_command_fixture(
        ordinal,
        target_ordinal,
        Some(prior),
        AdmissionShape::VacantTerminal,
        false,
    )
}

fn deleting_command_fixture_at(ordinal: u64, prior: &CommandFixture) -> CommandFixture {
    build_command_fixture(
        ordinal,
        1,
        Some(prior),
        AdmissionShape::VacantTerminal,
        true,
    )
}

fn build_command_fixture(
    ordinal: u64,
    target_ordinal: u64,
    prior: Option<&CommandFixture>,
    admission_shape: AdmissionShape,
    delete: bool,
) -> CommandFixture {
    let plan = plan();
    let sequence = CommitSequence::new(ordinal).expect("fixture ordinal");
    let ordinal_u8 = u8::try_from(ordinal % 256).expect("bounded fixture ordinal");
    let (target, index_key, range) = target_and_index_at(target_ordinal);
    // A superseding write must carry different canonical bytes, or the frame
    // under test could not distinguish the two post-images.
    let payload = if prior.is_some() { 2 } else { 1 };
    let prior_entity = prior.map(|fixture| fixture.records.entities()[0].post_image().clone());
    let prior_epoch = prior.map_or(IndexEpochPosition::BeforeFirst, |fixture| {
        IndexEpochPosition::Value(fixture.write_plan.index_epochs()[0].next())
    });
    let tenant_scope = TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant"));
    let principal = ActorId::new("principal-a").expect("principal");
    let actor = riffdb_types::AdmittedActorContext::new(
        principal.clone(),
        ActorKind::Human,
        tenant_scope.clone(),
        None,
    );
    let mut idempotency_digest = [0x40_u8.wrapping_add(ordinal_u8); 32];
    if ordinal > u64::from(u8::MAX) {
        idempotency_digest[..8].copy_from_slice(&ordinal.to_be_bytes());
    }
    let identity = IdempotencyIdentity::new(
        database_id(),
        Environment::new("test").expect("environment"),
        tenant_scope,
        principal,
        plan.contract_lineage().clone(),
        plan.command_id(),
        IdempotencyKeyDigest::from_hmac_bytes(
            DigestKeyId::new(1).expect("digest key"),
            idempotency_digest,
        ),
    );
    let request_id = RequestId::from_bytes(ordinal_uuid_bytes(0x30, ordinal)).expect("request ID");
    let provenance_id =
        ProvenanceId::from_bytes(ordinal_uuid_bytes(0x50, ordinal)).expect("provenance ID");
    let logical_time =
        LogicalTime::new(Timestamp::new(1_700_000_001, 0).expect("logical timestamp"));
    let mut partition = PartitionKeyBuilder::new(AggregateTypeId::new(1).expect("aggregate"));
    partition
        .push_u64(6 + target_ordinal)
        .expect("partition component");
    let partition = partition.finish().expect("partition key");
    let pending = StoredPendingAdmissionV1::new(
        identity.clone(),
        CanonicalInputHash::from_bytes([0x42; 32]),
        request_id,
        plan.clone(),
        logical_time,
        actor.clone(),
        partition.clone(),
        StoredAdmittedProvenanceClaimsV1::default(),
    )
    .expect("pending admission");

    let snapshot_request =
        SnapshotRequest::new(plan.clone(), vec![target.clone()], Vec::new(), Vec::new())
            .expect("snapshot request");
    let snapshot = ReadSnapshot::new(
        &snapshot_request,
        None,
        vec![prior_entity.clone().map_or_else(
            || EntityObservation::Absent(target.clone()),
            EntityObservation::Present,
        )],
        Vec::new(),
        Vec::new(),
    )
    .expect("read snapshot");
    let post_image = EntityPostImage::new(target.clone(), plan.contract_version(), record(payload))
        .expect("entity post-image");
    let event_type_id = EventTypeId::new(1).expect("event type");
    let event_policy_anchor = riffdb_storage_api::StoredEventPolicyAnchorV1::new(
        DurableKeySchemaBindingV1::from_plan(&plan),
        event_type_id,
        target.clone(),
        riffdb_types::RowPolicyName::new("RowAccess").expect("row policy name"),
    );
    let event_intent =
        EventIntent::new_anchored(event_type_id, record(payload), event_policy_anchor.clone())
            .expect("anchored event intent");
    let declared_outcome =
        DeclaredOutcome::new(OutcomeId::new(1).expect("outcome ID"), record(payload))
            .expect("declared outcome");
    let entity_mutation = if delete {
        let entity = prior_entity
            .as_ref()
            .expect("delete requires a live predecessor");
        EntityMutation::Delete {
            expected_version: entity.entity_version(),
            prior_image: EntityPostImage::new(
                entity.target().clone(),
                plan.contract_version(),
                entity.fields().clone(),
            )
            .expect("delete predecessor image"),
        }
    } else {
        prior_entity.as_ref().map_or_else(
            || EntityMutation::Create(post_image.clone()),
            |entity| EntityMutation::Replace {
                expected_version: entity.entity_version(),
                post_image: post_image.clone(),
            },
        )
    };
    let evaluated = riffdb_storage_api::EvaluatedCommand::new(
        &snapshot,
        vec![entity_mutation],
        vec![event_intent],
        declared_outcome.clone(),
        EvaluationBudget::v1(),
    )
    .expect("evaluated command");
    let partition_hash = hash_partition_key(partition.as_bytes());
    let context = PreEvaluationCommitContext::new(pending.clone(), partition_hash, Vec::new())
        .expect("commit context");
    let candidates =
        IdempotencyLookupCandidatesV1::new(vec![identity.clone()]).expect("lookup candidates");
    let intent = match admission_shape {
        AdmissionShape::VacantTerminal => {
            riffdb_storage_api::CommitIntent::new_for_vacant_terminal_admission(
                context.clone(),
                candidates.clone(),
                evaluated,
                provenance_id,
            )
            .expect("fused terminal commit intent")
        }
        AdmissionShape::ExistingPending => {
            riffdb_storage_api::CommitIntent::new(context.clone(), evaluated, provenance_id)
                .expect("existing-pending commit intent")
        }
    };

    let stored_entity = StoredEntityRecordV1::new(
        target.clone(),
        prior_entity
            .as_ref()
            .map_or_else(EntityVersion::first, |entity| {
                entity
                    .entity_version()
                    .checked_next()
                    .expect("superseding entity version")
            }),
        plan.contract_version(),
        DurableKeySchemaBindingV1::from_plan(&plan),
        record(payload),
    )
    .expect("stored entity");
    let mutation = if delete {
        let entity = prior_entity.as_ref().expect("delete predecessor");
        riffdb_storage_api::CommittedEntityMutationV1::delete(
            entity.entity_version(),
            entity.clone(),
        )
        .expect("committed entity delete")
    } else {
        riffdb_storage_api::CommittedEntityMutationV1::new(
            prior_entity
                .as_ref()
                .map_or(ExpectedEntityState::Absent, |entity| {
                    ExpectedEntityState::Present(entity.entity_version())
                }),
            stored_entity,
        )
        .expect("committed entity mutation")
    };
    let index_record = StoredIndexEntryV2::new(
        index_key.clone(),
        DurableKeySchemaBindingV1::from_plan(&plan),
        record(payload),
        pending.partition_key().clone(),
    )
    .expect("stored index entry");
    let index_mutation = if delete {
        IndexEntryMutationV1::Delete(index_key.clone())
    } else {
        IndexEntryMutationV1::Put(index_record)
    };
    let generation = PartitionIndexTarget::new(partition.clone(), index_key.index_id());
    let affected_targets =
        AffectedIndexEpochTargets::new(vec![generation.clone()]).expect("affected targets");
    let affected_current = AffectedEpochCurrentState::new(
        &affected_targets,
        vec![CurrentIndexGenerationObservation::new(
            generation.clone(),
            prior_epoch,
        )],
    )
    .expect("affected current state");
    let epoch_advance = IndexEpochAdvanceV1::new(
        generation,
        DurableKeySchemaBindingV1::from_plan(&plan),
        prior_epoch,
    )
    .expect("epoch advance");
    let upper_bound = match command_write_set_upper_bound_v1(
        &intent,
        std::slice::from_ref(&index_mutation),
        std::slice::from_ref(&epoch_advance),
    )
    .expect("canonical encoded upper bound")
    {
        EncodedWriteSetUpperBoundResultV1::Fits(bound) => bound,
        EncodedWriteSetUpperBoundResultV1::ExceedsAcceptedAggregateCap(_) => {
            panic!("recovery fixture write set must fit the accepted aggregate cap")
        }
    };
    let write_plan = CommandWriteSetPlanV1::new(
        &intent,
        affected_targets.clone(),
        affected_current,
        vec![index_mutation],
        vec![epoch_advance],
        upper_bound,
    )
    .expect("write plan");

    let assignment = AssignedCommandSequence::from_assigned(sequence);
    let event_id = EventId::new(sequence, 0);
    let event_payload = record(payload);
    let event = StoredDurableEventV1::new_anchored(
        event_id,
        event_type_id,
        event_payload.clone(),
        riffdb_storage_api::derive_event_hash_v2(
            event_id,
            event_type_id,
            &event_payload,
            &event_policy_anchor,
        )
        .expect("anchored event hash"),
        event_policy_anchor,
    )
    .expect("durable event");
    let stored_outcome = StoredOutcomeV1::new(
        identity.clone(),
        sequence,
        request_id,
        plan.clone(),
        pending.canonical_input_hash(),
        actor.clone(),
        logical_time,
        partition.clone(),
        partition_hash,
        Vec::new(),
        declared_outcome.clone(),
        StoredAdmittedProvenanceClaimsV1::default(),
        provenance_id,
        DurabilityMode::Sync,
    )
    .expect("stored outcome");
    let mutations = vec![mutation];
    let provenance = StoredProvenanceRecordV1::new(
        provenance_id,
        sequence,
        identity,
        request_id,
        plan.clone(),
        pending.canonical_input_hash(),
        actor.clone(),
        logical_time,
        partition_hash,
        Vec::new(),
        declared_outcome.outcome_id(),
        vec![AffectedEntityV1::from_record(mutations[0].post_image())],
        vec![event_id],
        StoredAdmittedProvenanceClaimsV1::default(),
    )
    .expect("provenance");
    let commit = riffdb_storage_api::StoredCommitRecordV1::new(
        sequence,
        request_id,
        plan,
        pending.canonical_input_hash(),
        actor,
        logical_time,
        partition_hash,
        Vec::new(),
        StoredReadDependenciesV1::from_live(snapshot.read_dependencies())
            .expect("stored dependencies"),
        mutations
            .iter()
            .map(riffdb_storage_api::CommittedEntityReferenceV2::from_live_mutation)
            .collect::<Result<Vec<_>, _>>()
            .expect("live entity references")
            .into_iter()
            .flatten()
            .collect(),
        vec![event.clone()],
        declared_outcome,
        provenance_id,
        vec![event_id],
        DurabilityMode::Sync,
    )
    .expect("commit record");
    let records = AtomicCommandRecordSet::new(
        assignment,
        mutations,
        write_plan.clone(),
        stored_outcome,
        provenance,
        commit,
    )
    .expect("atomic command record set");

    CommandFixture {
        candidates,
        pending,
        context,
        intent,
        affected_targets,
        write_plan,
        snapshot,
        records,
        target,
        range,
        index_key,
    }
}

fn catalog_principal() -> AuditPrincipalV1 {
    AuditPrincipalV1::new(
        ActorId::new("storage-recovery-maintainer").expect("catalog principal"),
        ActorKind::Human,
        CapabilityId::from_bytes(uuid_bytes(0x61)).expect("catalog capability"),
        NonZeroU64::MIN,
    )
}

fn prepare_command_database(path: &Path) {
    let mut store = RedbStore::open(path).expect("open command recovery database");
    store
        .initialize_database(database_id())
        .expect("initialize command recovery database");
    let mut ports = open_operational(store);
    let bundle = contract_bundle();
    let result = ports
        .activate_catalog(&CatalogActivationIntentV1::new(
            None,
            bundle.clone(),
            RequestId::from_bytes(uuid_bytes(0x62)).expect("catalog request"),
            catalog_principal(),
            Timestamp::new(1_700_000_000, 0).expect("catalog timestamp"),
            None,
        ))
        .expect("activate command recovery catalog");
    assert!(
        matches!(
            &result,
            CatalogActivationResult::Activated { active, .. }
                if *active == riffdb_storage_api::ActiveCatalogPointerV1::from_bundle(&bundle)
        ),
        "unexpected catalog activation result: {result:?}"
    );
}

fn protected_event_capability_intent() -> CapabilityBootstrapIntentV1 {
    let capability_id =
        CapabilityId::from_bytes(uuid_bytes(0x73)).expect("protected event capability");
    let issued_at = Timestamp::new(1_699_999_900, 0).expect("capability issue time");
    let token_digest = CapabilityTokenDigest::from_hmac_bytes(
        DigestKeyId::new(7).expect("capability digest key"),
        [0x73; 32],
    );
    let permissions = CapabilityPermissionsV1::new(vec![
        CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::AdministerCapabilities)
            .expect("capability permission"),
    ])
    .expect("capability permissions");
    let grant = CapabilityGrantV1::new(
        TenantScope::Global,
        PartitionScopeV1::All,
        permissions,
        Vec::new(),
        NonZeroU16::MIN,
        Vec::new(),
    )
    .expect("capability grant");
    let requested = CapabilityRequestedRecordV1::new(
        database_id(),
        Environment::new("test").expect("capability environment"),
        ActorId::new("protected-event-test").expect("capability principal"),
        ActorKind::Human,
        NonZeroU32::new(600).expect("capability duration"),
        vec![Audience::new("riffdb-test").expect("capability audience")],
        grant,
    )
    .expect("capability request");
    let start = BootstrapServiceAuditStartV1::new(
        RequestId::from_bytes(uuid_bytes(0x74)).expect("capability bootstrap request"),
        issued_at,
        ServiceIngressKindV1::Grpc,
        ServiceAuditTargetsV1::new([ServiceAuditTargetV1::Capability(capability_id)])
            .expect("capability audit target"),
        None,
    )
    .expect("capability bootstrap audit start");
    CapabilityBootstrapIntentV1::new(
        capability_id,
        requested,
        BootstrapDigestCandidatesV1::new(vec![token_digest], token_digest)
            .expect("capability digest candidates"),
        issued_at,
        Timestamp::new(1_700_000_500, 0).expect("capability expiry"),
        start,
    )
    .expect("protected event capability intent")
}

fn prepare_protected_command_database(
    path: &Path,
    controller: RedbTestController,
) -> (RedbOperationalPorts, StoredCapabilityRecordV1) {
    let mut store = RedbStore::open_with_test_controller(path, controller)
        .expect("open protected command database");
    store
        .initialize_database(database_id())
        .expect("initialize protected command database");
    let mut ports = open_operational(store);
    let intent = protected_event_capability_intent();
    assert!(matches!(
        ports
            .bootstrap_capability(&intent)
            .expect("bootstrap protected event capability"),
        CapabilityBootstrapResult::BootstrapCreated { capability_id, .. }
            if capability_id == intent.capability_id()
    ));
    let capability = ports
        .read_capability(intent.capability_id())
        .expect("read protected event capability")
        .expect("protected event capability exists");
    let bundle = contract_bundle();
    let result = ports
        .activate_catalog(&CatalogActivationIntentV1::new(
            None,
            bundle.clone(),
            RequestId::from_bytes(uuid_bytes(0x75)).expect("protected catalog request"),
            catalog_principal(),
            Timestamp::new(1_700_000_000, 0).expect("protected catalog timestamp"),
            None,
        ))
        .expect("activate protected command catalog");
    assert!(matches!(
        &result,
        CatalogActivationResult::Activated { active, .. }
            if *active == riffdb_storage_api::ActiveCatalogPointerV1::from_bundle(&bundle)
    ));
    (ports, capability)
}

fn commit_command_fixture(ports: &RedbOperationalPorts, fixture: &CommandFixture) {
    commit_command_fixture_with_audit(ports, fixture, command_audit_transition(fixture));
}

fn commit_command_fixture_with_audit(
    ports: &RedbOperationalPorts,
    fixture: &CommandFixture,
    transition: riffdb_storage_api::CommandServiceAuditTransitionV1,
) {
    try_commit_command_fixture_with_audit(ports, fixture, transition)
        .expect("commit complete command graph and audit lifecycle");
}

fn try_commit_command_fixture_with_audit(
    ports: &RedbOperationalPorts,
    fixture: &CommandFixture,
    transition: riffdb_storage_api::CommandServiceAuditTransitionV1,
) -> Result<(), riffdb_storage_api::StorageError> {
    let candidate = ports
        .begin_empty_batch()?
        .begin_candidate(Box::new(fixture.intent.clone()))?;
    let CandidateAdmissionResult::Proceed(candidate) = candidate.recheck_admission()? else {
        panic!("the fixture's expected admission state must proceed");
    };
    let (candidate, current) = candidate.read_transaction_current()?;
    assert_eq!(
        current.bindings()[0].expected_state(),
        fixture.records.entities()[0].expected(),
        "transaction-current state must match the fixture's committed expectation"
    );
    let candidate = candidate
        .plan_validated(fixture.affected_targets.clone())
        .read_affected_epoch_current()?;
    let CandidateCapacityResult::Reserved(candidate) =
        candidate.reserve_capacity(fixture.write_plan.clone())?
    else {
        panic!("small recovery fixture must reserve");
    };
    let candidate = candidate.assign_sequence()?;
    assert_eq!(
        candidate.assignment().assigned(),
        fixture.records.commit().commit_sequence()
    );
    candidate
        .stage(fixture.records.clone())?
        .commit_with_service_audit_transitions(DurabilityMode::Sync, vec![transition])
        .map(|_| ())
}

/// Attempts the retained storage-only command completion path that carries no
/// command service-audit transition and therefore cannot produce a canonical
/// command capsule. Production command callers never select this path.
fn try_commit_uncapsulated_command_fixture(
    ports: &RedbOperationalPorts,
    fixture: &CommandFixture,
) -> Result<(), riffdb_storage_api::StorageError> {
    let candidate = ports
        .begin_empty_batch()?
        .begin_candidate(Box::new(fixture.intent.clone()))?;
    let CandidateAdmissionResult::Proceed(candidate) = candidate.recheck_admission()? else {
        panic!("the fixture's expected admission state must proceed");
    };
    let (candidate, current) = candidate.read_transaction_current()?;
    assert_eq!(
        current.bindings()[0].expected_state(),
        fixture.records.entities()[0].expected(),
        "transaction-current state must match the fixture's committed expectation"
    );
    let candidate = candidate
        .plan_validated(fixture.affected_targets.clone())
        .read_affected_epoch_current()?;
    let CandidateCapacityResult::Reserved(candidate) =
        candidate.reserve_capacity(fixture.write_plan.clone())?
    else {
        panic!("small recovery fixture must reserve");
    };
    let candidate = candidate.assign_sequence()?;
    assert_eq!(
        candidate.assignment().assigned(),
        fixture.records.commit().commit_sequence()
    );
    candidate
        .stage(fixture.records.clone())?
        .commit(DurabilityMode::Sync)
        .map(|_| ())
}

fn apply_unpublished_command_fixture(
    epoch: RedbDurabilityEpoch,
    fixture: &CommandFixture,
) -> RedbDurabilityEpoch {
    let candidate = DeferredCommandEpoch::begin_empty_batch(epoch)
        .expect("begin deferred command batch")
        .begin_candidate(Box::new(fixture.intent.clone()))
        .expect("begin deferred command candidate");
    let CandidateAdmissionResult::Proceed(candidate) = candidate
        .recheck_admission()
        .expect("recheck deferred pending admission")
    else {
        panic!("fresh vacant deferred admission must proceed");
    };
    let (candidate, current) = candidate
        .read_transaction_current()
        .expect("read deferred transaction-current state");
    assert_eq!(
        current.bindings()[0].expected_state(),
        fixture.records.entities()[0].expected(),
        "transaction-current state must match the fixture's committed expectation"
    );
    let candidate = candidate
        .plan_validated(fixture.affected_targets.clone())
        .read_affected_epoch_current()
        .expect("read deferred affected epoch state");
    let CandidateCapacityResult::Reserved(candidate) = candidate
        .reserve_capacity(fixture.write_plan.clone())
        .expect("reserve deferred complete command graph")
    else {
        panic!("small deferred fixture must reserve");
    };
    let candidate = candidate
        .assign_sequence()
        .expect("assign deferred sequence");
    assert_eq!(
        candidate.assignment().assigned(),
        fixture.records.commit().commit_sequence()
    );
    candidate
        .stage(fixture.records.clone())
        .expect("stage deferred complete command graph")
        .apply_unpublished_with_service_audit_transitions(
            DurabilityMode::Sync,
            vec![command_audit_transition(fixture)],
        )
        .expect("apply complete command graph without publication")
}

fn commit_command_group(ports: &RedbOperationalPorts, fixtures: &[CommandFixture]) {
    try_commit_command_group(ports, fixtures, fixtures).expect("commit complete command group");
}

fn try_commit_command_group(
    ports: &RedbOperationalPorts,
    fixtures: &[CommandFixture],
    audit_fixtures: &[CommandFixture],
) -> Result<(), riffdb_storage_api::StorageError> {
    let (first, remaining) = fixtures.split_first().expect("non-empty recovery group");
    let candidate = ports
        .begin_empty_batch()
        .expect("begin serial recovery batch")
        .begin_candidate(Box::new(first.intent.clone()))
        .expect("begin first candidate");
    let CandidateAdmissionResult::Proceed(candidate) = candidate
        .recheck_admission()
        .expect("recheck first admission")
    else {
        panic!("first fresh admission proceeds");
    };
    let (candidate, _) = candidate
        .read_transaction_current()
        .expect("read first current state");
    let candidate = candidate
        .plan_validated(first.affected_targets.clone())
        .read_affected_epoch_current()
        .expect("read first affected state");
    let CandidateCapacityResult::Reserved(candidate) = candidate
        .reserve_capacity(first.write_plan.clone())
        .expect("reserve first graph")
    else {
        panic!("first graph fits");
    };
    let mut batch = candidate
        .assign_sequence()
        .expect("assign first sequence")
        .stage(first.records.clone())
        .expect("stage first graph");

    for fixture in remaining {
        let CandidateStartResult::Started(candidate) = batch
            .begin_candidate(Box::new(fixture.intent.clone()))
            .expect("begin next recovery candidate")
        else {
            panic!("bounded recovery group fits");
        };
        let CandidateAdmissionResult::Proceed(candidate) = candidate
            .recheck_admission()
            .expect("recheck next admission")
        else {
            panic!("fresh grouped admission proceeds");
        };
        let (candidate, _) = candidate
            .read_transaction_current()
            .expect("read next transaction-local state");
        let candidate = candidate
            .plan_validated(fixture.affected_targets.clone())
            .read_affected_epoch_current()
            .expect("read next affected state");
        let CandidateCapacityResult::Reserved(candidate) = candidate
            .reserve_capacity(fixture.write_plan.clone())
            .expect("reserve next graph")
        else {
            panic!("next graph fits");
        };
        batch = candidate
            .assign_sequence()
            .expect("assign next sequence")
            .stage(fixture.records.clone())
            .expect("stage next graph");
    }
    batch
        .commit_with_service_audit_transitions(
            DurabilityMode::Sync,
            audit_fixtures
                .iter()
                .map(command_audit_transition)
                .collect(),
        )
        .map(|_| ())
}

/// The one `Started`/`Succeeded` pair every fixture's command lifecycle uses.
///
/// Both phases of a two-phase admission must present the same common fields, so
/// the durable `Started` written at admission time and the terminal written at
/// commit time are produced here from one definition.
fn command_audit_intents(
    fixture: &CommandFixture,
) -> (ServiceAuditAppendIntentV1, ServiceAuditAppendIntentV1) {
    let principal = catalog_principal();
    let started = ServiceAuditAppendIntentV1::new(
        fixture.pending.admission_request_id(),
        Timestamp::new(1_700_000_002, 0).expect("started timestamp"),
        ServiceOperationV1::ExecuteCommand,
        ServiceAuditPhaseV1::Started,
        principal.clone(),
        ServiceIngressKindV1::Grpc,
        ServiceAuditTargetsV1::empty(),
        None,
        ServiceAuditLinkV1::None,
    )
    .expect("started audit");
    let terminal = ServiceAuditAppendIntentV1::new(
        fixture.pending.admission_request_id(),
        Timestamp::new(1_700_000_003, 0).expect("terminal timestamp"),
        ServiceOperationV1::ExecuteCommand,
        ServiceAuditPhaseV1::Succeeded,
        principal,
        ServiceIngressKindV1::Grpc,
        ServiceAuditTargetsV1::empty(),
        None,
        ServiceAuditLinkV1::Command {
            commit_sequence: fixture.records.commit().commit_sequence(),
            provenance_id: fixture.records.provenance().provenance_id(),
        },
    )
    .expect("terminal audit");
    (started, terminal)
}

fn command_audit_transition(
    fixture: &CommandFixture,
) -> riffdb_storage_api::CommandServiceAuditTransitionV1 {
    let (started, terminal) = command_audit_intents(fixture);
    riffdb_storage_api::CommandServiceAuditTransitionV1::started_and_terminal(started, terminal)
        .expect("fused command audit lifecycle")
}

/// Phase one of a two-phase admission: one durable `Pending` row and its
/// physical `Started` audit row, written atomically by the audited-admission
/// port before any command graph exists.
fn admit_audited_command(
    ports: &RedbOperationalPorts,
    fixture: &CommandFixture,
) -> AuditedAdmissionResultV1 {
    let (started, _) = command_audit_intents(fixture);
    let admission = AdmissionRequestV1::new(fixture.candidates.clone(), &fixture.context)
        .expect("audited admission request");
    let request =
        AuditedAdmissionRequestV1::new(admission, started).expect("audited admission request pair");
    let mut results = ports
        .admit_or_resolve_audited_group(vec![request])
        .expect("audited admission group");
    assert_eq!(results.len(), 1, "one request admits exactly one result");
    results.pop().expect("one audited admission result")
}

/// Phase two: the ordinary candidate chain, contributing only the terminal row
/// because the `Started` row is already durable.
fn commit_two_phase_command_fixture(ports: &RedbOperationalPorts, fixture: &CommandFixture) {
    let (_, terminal) = command_audit_intents(fixture);
    let transition = riffdb_storage_api::CommandServiceAuditTransitionV1::terminal_only(terminal)
        .expect("terminal-only command audit transition");
    commit_command_fixture_with_audit(ports, fixture, transition);
}

fn command_audit_phases(ports: &RedbOperationalPorts) -> Vec<ServiceAuditPhaseV1> {
    let scan = ports
        .scan_administration_audit(AdministrationAuditScanRequest::new(
            None,
            StorageScanLimit::new(64).expect("audit scan limit"),
        ))
        .expect("scan audit");
    let AdministrationAuditScan::ExactEnd { records } = scan else {
        panic!("small recovery audit stream must reach exact end");
    };
    records
        .into_iter()
        .filter_map(|record| match record.into_parts().0 {
            StoredAdministrationAuditRecordV1::Service(service)
                if service.operation() == ServiceOperationV1::ExecuteCommand =>
            {
                Some(service.phase())
            }
            _ => None,
        })
        .collect()
}

fn committed_index_entry(fixture: &CommandFixture) -> StoredIndexEntryV2 {
    let IndexEntryMutationV1::Put(entry) = &fixture.records.index_entries()[0] else {
        panic!("command fixture must install one index entry");
    };
    entry.clone()
}

fn prepare_committed_command_database(path: &Path) -> (CommandFixture, StoredIndexEntryV2) {
    prepare_command_database(path);
    let fixture = command_fixture();
    let ports = open_operational(RedbStore::open(path).expect("reopen command fixture database"));
    commit_command_fixture(&ports, &fixture);
    let index_entry = committed_index_entry(&fixture);
    drop(ports);
    (fixture, index_entry)
}

/// Two real committed commands (sequences 1 and 2, disjoint entities) — the
/// populated-history shape every retention prune test exercises.
fn prepare_two_command_database(path: &Path) -> (CommandFixture, CommandFixture) {
    prepare_command_database(path);
    let first = command_fixture_at(1);
    let second = command_fixture_at(2);
    let ports = open_operational(RedbStore::open(path).expect("reopen command fixture database"));
    commit_command_fixture(&ports, &first);
    commit_command_fixture(&ports, &second);
    drop(ports);
    (first, second)
}

fn migration_index_entry(entity_value: u64, partition_value: u64) -> StoredIndexEntryV2 {
    migration_index_entry_with_covered_values(entity_value, partition_value, record(entity_value))
}

fn migration_index_entry_with_covered_values(
    entity_value: u64,
    partition_value: u64,
    covered_values: CanonicalRecord,
) -> StoredIndexEntryV2 {
    let mut entity_key = EntityKeyBuilder::new(EntityTypeId::new(1).expect("entity type ID"));
    entity_key
        .push_u64(entity_value)
        .expect("entity key component");
    let entity_key = entity_key.finish().expect("entity key");
    let mut index_key = IndexEntryKeyBuilder::new(IndexId::new(1).expect("index ID"));
    index_key.push_u64(10).expect("index component");
    let index_key = index_key.finish(entity_key).expect("index entry key");
    let mut partition = PartitionKeyBuilder::new(AggregateTypeId::new(1).expect("aggregate"));
    partition
        .push_u64(partition_value)
        .expect("partition component");
    StoredIndexEntryV2::new(
        index_key,
        DurableKeySchemaBindingV1::from_plan(&plan()),
        covered_values,
        partition.finish().expect("partition key"),
    )
    .expect("migration V2 row")
}

fn legacy_index_entry(current: &StoredIndexEntryV2) -> StoredIndexEntryV1 {
    StoredIndexEntryV1::new(
        current.key().clone(),
        current.schema_binding().clone(),
        current.covered_values().clone(),
    )
    .expect("legacy migration row")
}

fn write_raw_index_entries(
    path: &Path,
    entries: &[(IndexEntryKey, Vec<u8>)],
) -> Vec<Option<Vec<u8>>> {
    let database = Database::create(path).expect("open raw migration fixture");
    let transaction = database.begin_write().expect("begin raw migration write");
    let mut previous = Vec::with_capacity(entries.len());
    {
        let mut table = transaction
            .open_table(SECONDARY_INDEXES)
            .expect("open raw secondary-index table");
        for (key, envelope) in entries {
            previous.push(
                table
                    .insert(key.as_bytes(), envelope.as_slice())
                    .expect("write raw index envelope")
                    .map(|value| value.value().to_vec()),
            );
        }
    }
    transaction.commit().expect("commit raw migration fixture");
    previous
}

fn read_raw_index_entries(path: &Path) -> Vec<(Vec<u8>, Vec<u8>)> {
    let database = Database::create(path).expect("open raw migration result");
    let transaction = database.begin_read().expect("begin raw migration read");
    let table = transaction
        .open_table(SECONDARY_INDEXES)
        .expect("open raw secondary-index table");
    table
        .iter()
        .expect("iterate raw secondary-index table")
        .map(|entry| {
            let (key, value) = entry.expect("read raw secondary-index row");
            (key.value().to_vec(), value.value().to_vec())
        })
        .collect()
}

#[derive(Debug, Eq, PartialEq)]
struct MigrationControlState {
    tables: Vec<String>,
    metadata: Vec<(String, Vec<u8>)>,
}

fn read_migration_control_state(path: &Path) -> MigrationControlState {
    let database = Database::create(path).expect("open raw migration control state");
    let transaction = database.begin_read().expect("begin migration control read");
    let mut tables = transaction
        .list_tables()
        .expect("list migration fixture tables")
        .map(|table| table.name().to_owned())
        .collect::<Vec<_>>();
    tables.sort();
    let metadata = transaction
        .open_table(META)
        .expect("open migration fixture metadata")
        .iter()
        .expect("iterate migration fixture metadata")
        .filter_map(|entry| {
            let (key, value) = entry.expect("read migration fixture metadata");
            let key = key.value().to_owned();
            // The optional validated-prefix checkpoint and private clean-close
            // lifecycle are rewritten by startup/close and are not migration
            // control markers.
            if key == "validated_prefix_checkpoint/v1" || key == "clean_close_certificate/v1" {
                return None;
            }
            Some((key, value.value().to_vec()))
        })
        .collect();
    MigrationControlState { tables, metadata }
}

#[derive(Debug, Default, Eq, PartialEq)]
struct MigrationObservation {
    v1_rewrites: usize,
    v2_confirms: usize,
    pages: usize,
}

fn drive_index_migration(
    port: RedbStartupIndexMigrationPort,
    context: CatalogIndexMigrationContext,
    controller: &RedbTestController,
) -> (RedbStore, MigrationObservation) {
    let store = CatalogIndexMigrationDriver::new(context, port)
        .expect("bind same-session catalog migration")
        .run()
        .expect("drive catalog-owned migration");
    let (pages, v1_rewrites, v2_confirms) = controller.index_migration_observation();
    (
        store,
        MigrationObservation {
            v1_rewrites,
            v2_confirms,
            pages,
        },
    )
}

fn assert_precommit_command_state(ports: &RedbOperationalPorts, fixture: &CommandFixture) {
    assert_eq!(
        ports
            .lookup_admission(fixture.candidates.clone())
            .expect("lookup precommit admission"),
        AdmissionLookupResultV1::NotFound,
        "a crash before the fused terminal commit must leave no admission"
    );
    assert!(
        command_audit_phases(ports).is_empty(),
        "a precommit crash must leave no partial command audit lifecycle"
    );
    assert_eq!(
        ports.read_entity(&fixture.target).expect("read entity"),
        None
    );
    assert_eq!(
        ports
            .read_stored_outcome(fixture.pending.identity())
            .expect("read terminal outcome"),
        None
    );
    assert_eq!(
        ports
            .read_commit(CommitSequence::first())
            .expect("read commit"),
        None
    );
    assert_eq!(
        ports
            .read_provenance(fixture.records.provenance().provenance_id())
            .expect("read provenance"),
        None
    );
    let event_id = EventId::new(CommitSequence::first(), 0);
    assert_eq!(
        ports
            .read_durable_event(event_id)
            .expect("read durable event"),
        None
    );
    let route_scan = ports
        .scan_partition_event_routes(EventRouteScanRequestV1::initial(
            hash_partition_key(fixture.pending.partition_key().as_bytes()),
            None,
            EventRoutePageLimit::new(NonZeroU16::MIN).expect("event-route limit"),
        ))
        .expect("scan precommit event routes");
    assert!(matches!(
        route_scan,
        EventRouteScanV1::ExactEnd {
            items,
            inclusive_upper: EventRouteUpperFenceV1::BeforeFirst,
        } if items.is_empty()
    ));
    assert_eq!(
        ports
            .read_outbox_status(event_id)
            .expect("read outbox status"),
        OutboxStatusReadResultV1::AuthoritativeIntentMissing
    );

    let limit = StorageScanLimit::new(1).expect("scan limit");
    let index = ports
        .scan_index(
            AuthoritativeIndexScanRequest::new(fixture.range.clone(), None, limit)
                .expect("index request"),
        )
        .expect("scan index");
    assert!(matches!(
        index,
        AuthoritativeIndexScanPage::ExactEnd {
            entries,
            epoch: IndexEpochPosition::BeforeFirst,
        } if entries.is_empty()
    ));
    let outbox = ports
        .scan_pending_outbox(
            None,
            OutboxPageLimit::new(NonZeroU16::MIN).expect("outbox limit"),
        )
        .expect("scan pending outbox");
    assert!(matches!(
        outbox,
        PendingOutboxScanV1::ExactEnd { items } if items.is_empty()
    ));

    let snapshot = ports
        .read_snapshot(
            SnapshotRequest::new(
                plan(),
                vec![fixture.target.clone()],
                Vec::new(),
                vec![fixture.range.clone()],
            )
            .expect("snapshot request"),
        )
        .expect("precommit snapshot");
    assert_eq!(snapshot.observed_through(), None);
    assert_eq!(
        snapshot.bindings(),
        &[EntityObservation::Absent(fixture.target.clone())]
    );
    assert_eq!(
        snapshot.ranges()[0].epoch(),
        IndexEpochPosition::BeforeFirst
    );
    assert!(snapshot.ranges()[0].entries().is_empty());
}

fn assert_postcommit_command_state(ports: &RedbOperationalPorts, fixture: &CommandFixture) {
    let AdmissionLookupResultV1::Found(admission) = ports
        .lookup_admission(fixture.candidates.clone())
        .expect("lookup committed admission")
    else {
        panic!("the terminal admission must be present");
    };
    assert_eq!(
        *admission,
        riffdb_storage_api::StoredAdmissionStateV1::StoredOutcome(
            fixture.records.stored_outcome().clone()
        )
    );
    assert_eq!(
        ports.read_entity(&fixture.target).expect("read entity"),
        Some(fixture.records.entities()[0].post_image().clone())
    );
    let stored_outcome = ports
        .read_stored_outcome(fixture.pending.identity())
        .expect("read terminal outcome")
        .expect("committed outcome");
    assert_eq!(&stored_outcome, fixture.records.stored_outcome());
    assert_eq!(
        stored_outcome.partition_key(),
        fixture.pending.partition_key(),
        "the exact admitted partition key must survive commit and reopen"
    );
    assert_eq!(
        ports
            .read_commit(CommitSequence::first())
            .expect("read commit"),
        Some(fixture.records.commit().clone())
    );
    assert_eq!(
        ports
            .read_provenance(fixture.records.provenance().provenance_id())
            .expect("read provenance"),
        Some(fixture.records.provenance().clone())
    );
    assert_eq!(
        command_audit_phases(ports),
        [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Succeeded],
        "the complete command graph and audit lifecycle must recover together"
    );
    let event = &fixture.records.events()[0];
    assert_eq!(
        ports
            .read_durable_event(event.event_id())
            .expect("read durable event"),
        Some(event.clone())
    );
    let route_scan = ports
        .scan_partition_event_routes(EventRouteScanRequestV1::initial(
            hash_partition_key(fixture.pending.partition_key().as_bytes()),
            None,
            EventRoutePageLimit::new(NonZeroU16::MIN).expect("event-route limit"),
        ))
        .expect("scan committed event routes");
    let EventRouteScanV1::ExactEnd {
        items,
        inclusive_upper: EventRouteUpperFenceV1::Inclusive(upper),
    } = route_scan
    else {
        panic!("one committed event route must reach exact end");
    };
    assert_eq!(upper, event.event_id());
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].value().event_id(), event.event_id());
    assert_eq!(items[0].value().event_type_id(), event.event_type_id());
    assert_eq!(items[0].value().event_hash(), event.event_hash());
    assert_eq!(
        ports
            .read_outbox_status(event.event_id())
            .expect("read outbox status"),
        OutboxStatusReadResultV1::Status(OutboxStatusObservationV1::AbsentInitialPending)
    );

    let limit = StorageScanLimit::new(1).expect("scan limit");
    let index = ports
        .scan_index(
            AuthoritativeIndexScanRequest::new(fixture.range.clone(), None, limit)
                .expect("index request"),
        )
        .expect("scan index");
    let AuthoritativeIndexScanPage::ExactEnd { entries, epoch } = index else {
        panic!("one index row must reach exact end");
    };
    assert_eq!(
        epoch,
        IndexEpochPosition::Value(fixture.records.index_epochs()[0].next())
    );
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].value().key(), &fixture.index_key);
    assert_eq!(entries[0].value().covered_values(), &record(1));

    let outbox = ports
        .scan_pending_outbox(
            None,
            OutboxPageLimit::new(NonZeroU16::MIN).expect("outbox limit"),
        )
        .expect("scan pending outbox");
    let PendingOutboxScanV1::ExactEnd { items } = outbox else {
        panic!("one pending outbox tuple must reach exact end");
    };
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].value().event(), event);
    assert_eq!(
        items[0].value().intent(),
        &fixture.records.outbox_intents()[0]
    );
    assert_eq!(
        items[0].value().status(),
        &OutboxStatusObservationV1::AbsentInitialPending
    );

    let snapshot = ports
        .read_snapshot(
            SnapshotRequest::new(
                plan(),
                vec![fixture.target.clone()],
                Vec::new(),
                vec![fixture.range.clone()],
            )
            .expect("snapshot request"),
        )
        .expect("postcommit snapshot");
    assert_eq!(snapshot.observed_through(), Some(CommitSequence::first()));
    assert_eq!(
        snapshot.bindings(),
        &[EntityObservation::Present(
            fixture.records.entities()[0].post_image().clone()
        )]
    );
    assert_eq!(
        snapshot.ranges()[0].epoch(),
        IndexEpochPosition::Value(fixture.records.index_epochs()[0].next())
    );
    assert_eq!(snapshot.ranges()[0].entries().len(), 1);
    assert_eq!(snapshot.ranges()[0].entries()[0].key(), &fixture.index_key);
}

#[test]
fn process_recovery_child() {
    let Ok(mode) = std::env::var(CHILD_MODE) else {
        return;
    };
    let path = PathBuf::from(std::env::var_os(CHILD_PATH).expect("child database path"));
    let profile = match std::env::var(CHILD_COMMIT_PROFILE).as_deref() {
        Ok("standard") => RedbCommitProfile::Standard,
        Ok("hardened") => RedbCommitProfile::Hardened,
        _ => panic!("unknown closed child commit profile"),
    };
    let controller = match mode.as_str() {
        "before-initialization-commit" => {
            RedbTestController::abort_before_commit(RedbTestOperation::Initialization)
        }
        "after-initialization-commit" => {
            RedbTestController::abort_after_commit(RedbTestOperation::Initialization)
        }
        "before-command-batch-commit" => {
            RedbTestController::abort_before_commit(RedbTestOperation::CommandBatch)
        }
        #[cfg(feature = "test-fixtures")]
        "external-before-command-batch-commit" => {
            RedbTestController::wait_before_final_engine_sync_for_external_kill(
                RedbTestOperation::CommandBatch,
                std::env::var_os("RIFFDB_TEST_REDB_EXTERNAL_KILL_BARRIER")
                    .expect("external kill barrier path"),
            )
        }
        "after-command-batch-commit" => {
            RedbTestController::abort_after_commit(RedbTestOperation::CommandBatch)
        }
        "before-fresh-locator-publication" => {
            RedbTestController::abort_before_commit(RedbTestOperation::CommandBatch)
        }
        "after-fresh-locator-publication" => {
            RedbTestController::abort_after_commit(RedbTestOperation::CommandBatch)
        }
        "before-cross-aggregate-commit" => {
            RedbTestController::abort_before_commit(RedbTestOperation::CommandBatch)
        }
        "after-cross-aggregate-commit" => {
            RedbTestController::abort_after_commit(RedbTestOperation::CommandBatch)
        }
        "before-command-group-commit" => {
            RedbTestController::abort_before_commit(RedbTestOperation::CommandBatch)
        }
        "after-command-group-commit" => {
            RedbTestController::abort_after_commit(RedbTestOperation::CommandBatch)
        }
        "before-deferred-command-commit" => {
            RedbTestController::abort_before_commit(RedbTestOperation::DeferredCommandBatch)
        }
        "after-deferred-command-commit" => {
            RedbTestController::abort_after_commit(RedbTestOperation::DeferredCommandBatch)
        }
        "after-command-epoch-tail" => {
            RedbTestController::abort_after_commit(RedbTestOperation::CommandEpochTail)
        }
        "before-index-migration-batch-commit" => {
            RedbTestController::abort_before_commit(RedbTestOperation::IndexMigrationBatch)
        }
        "after-index-migration-batch-commit" => {
            RedbTestController::abort_after_commit(RedbTestOperation::IndexMigrationBatch)
        }
        "before-validated-prefix-checkpoint-commit" => {
            RedbTestController::abort_before_commit(RedbTestOperation::ValidatedPrefixCheckpoint)
        }
        "after-validated-prefix-checkpoint-commit" => {
            RedbTestController::abort_after_commit(RedbTestOperation::ValidatedPrefixCheckpoint)
        }
        "before-graceful-close-barrier" => {
            RedbTestController::abort_before_commit(RedbTestOperation::GracefulCloseBarrier)
        }
        "during-graceful-close-barrier" => {
            RedbTestController::abort_before_commit(RedbTestOperation::GracefulCloseBarrierSuffix)
        }
        "after-graceful-close-barrier" => {
            RedbTestController::abort_after_commit(RedbTestOperation::GracefulCloseBarrier)
        }
        "before-graceful-checkpoint-classification" => RedbTestController::abort_before_commit(
            RedbTestOperation::GracefulCheckpointClassification,
        ),
        "after-graceful-checkpoint-classification" => RedbTestController::abort_after_commit(
            RedbTestOperation::GracefulCheckpointClassification,
        ),
        "before-graceful-clean-commit" => {
            RedbTestController::abort_before_commit(RedbTestOperation::CleanCloseLifecycle)
        }
        "after-graceful-clean-commit" => {
            RedbTestController::abort_after_commit(RedbTestOperation::CleanCloseLifecycle)
        }
        "before-retention-prune-subrange-commit" => {
            RedbTestController::abort_before_commit(RedbTestOperation::RetentionPruneSubrange)
        }
        "after-retention-prune-subrange-commit" => {
            RedbTestController::abort_after_commit(RedbTestOperation::RetentionPruneSubrange)
        }
        _ => panic!("unknown closed child mode"),
    };
    // Offline prune uses RedbOfflineRetention's own controller, not RedbStore.
    if matches!(
        mode.as_str(),
        "before-retention-prune-subrange-commit" | "after-retention-prune-subrange-commit"
    ) {
        let target = std::env::var(CHILD_PRUNE_TARGET)
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(1);
        let maintenance =
            riffdb_storage_redb::RedbOfflineRetention::bind_with_test_controller(&path, controller);
        let _ = maintenance.prune_to(target);
        panic!("the armed prune failpoint did not terminate the child");
    }
    #[cfg(feature = "test-fixtures")]
    let store = if mode == "external-before-command-batch-commit" {
        RedbStore::open_with_external_kill_barrier(path, profile, controller.clone())
            .expect("open child database over the real barrier backend")
    } else {
        RedbStore::open_with_test_controller_and_commit_profile(path, profile, controller.clone())
            .expect("open child database")
    };
    #[cfg(not(feature = "test-fixtures"))]
    let store =
        RedbStore::open_with_test_controller_and_commit_profile(path, profile, controller.clone())
            .expect("open child database");
    match mode.as_str() {
        "before-initialization-commit" | "after-initialization-commit" => {
            let mut store = store;
            let _ = store.initialize_database(database_id());
        }
        "before-command-batch-commit"
        | "external-before-command-batch-commit"
        | "after-command-batch-commit" => {
            let ports = open_operational(store);
            commit_command_fixture(&ports, &command_fixture());
        }
        "before-fresh-locator-publication" | "after-fresh-locator-publication" => {
            let ports = open_operational(store);
            let fixture = two_phase_command_fixture_at(1);
            let request = AdmissionRequestV1::new(fixture.candidates.clone(), &fixture.context)
                .expect("fresh crash-child admission request");
            assert_eq!(
                ports
                    .admit_or_resolve(request)
                    .expect("arm coverage through the exact first command write"),
                AdmissionResultV1::Created(fixture.pending.clone())
            );
            commit_command_fixture(&ports, &fixture);
        }
        "before-cross-aggregate-commit" | "after-cross-aggregate-commit" => {
            let ports = open_operational(store);
            commit_cross_aggregate_fixture(&ports, &cross_aggregate_fixture());
        }
        "before-command-group-commit" | "after-command-group-commit" => {
            let ports = open_operational(store);
            let fixtures = (1..=riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS)
                .map(|ordinal| {
                    command_fixture_at(u64::try_from(ordinal).expect("bounded fixture ordinal"))
                })
                .collect::<Vec<_>>();
            commit_command_group(&ports, &fixtures);
        }
        "before-deferred-command-commit"
        | "after-deferred-command-commit"
        | "after-command-epoch-tail" => {
            let ports = open_operational(store);
            let epoch = ports
                .begin_deferred_command_epoch()
                .expect("begin child durability epoch");
            let epoch = apply_unpublished_command_fixture(epoch, &command_fixture());
            let _ = DeferredCommandEpoch::fence(epoch);
        }
        "before-index-migration-batch-commit" | "after-index-migration-batch-commit" => {
            let (_, catalog_outcome, outcome) = complete_startup_pass(store);
            let CatalogHistoryOutcome::MigrationRequired(context) = catalog_outcome else {
                panic!("migration failpoint child requires catalog migration context");
            };
            let StructuralOpenOutcome::MigrationRequired(port) = outcome else {
                panic!("migration failpoint child requires one V1 index row");
            };
            let _ = drive_index_migration(port, context, &controller);
        }
        "before-validated-prefix-checkpoint-commit"
        | "after-validated-prefix-checkpoint-commit" => {
            // Seeded DB reopens and complete_startup_pass writes the checkpoint on finish.
            let _ = complete_startup_pass(store);
        }
        "during-graceful-close-barrier" => {
            let ports = open_operational(store);
            let epoch = ports
                .begin_deferred_command_epoch()
                .expect("begin acknowledged deferred suffix");
            let epoch = apply_unpublished_command_fixture(epoch, &command_fixture_at(2));
            let committed = DeferredCommandEpoch::fence(epoch)
                .expect("publish acknowledged deferred journal suffix");
            assert_eq!(committed.len(), 1);
            let _ = ports.complete_graceful_close();
        }
        "before-graceful-close-barrier"
        | "after-graceful-close-barrier"
        | "before-graceful-checkpoint-classification"
        | "after-graceful-checkpoint-classification"
        | "before-graceful-clean-commit"
        | "after-graceful-clean-commit" => {
            let ports = open_operational(store);
            let _ = ports.complete_graceful_close();
        }
        _ => unreachable!("controller match rejects unknown modes"),
    }
    panic!("the armed failpoint did not terminate the child");
}

// req: PERF-014
#[cfg(all(feature = "test-fixtures", feature = "benchmark-support"))]
#[test]
fn external_sigkill_at_real_command_precommit_invokes_engine_repair() {
    let path = TestDatabasePath::new("external-sigkill-command-precommit");
    let absent_twin = TestDatabasePath::new("external-sigkill-absent-twin");
    let committed_twin = TestDatabasePath::new("external-sigkill-committed-twin");
    let fixture = command_fixture();

    prepare_command_database(&absent_twin.0);
    let absent = open_operational(RedbStore::open(&absent_twin.0).expect("open absent twin"));
    assert_precommit_command_state(&absent, &fixture);
    absent
        .write_clean_close_lifecycle()
        .expect("certify absent twin");
    drop(absent);
    let absent_started = std::time::Instant::now();
    let absent = open_operational(RedbStore::open(&absent_twin.0).expect("reopen absent twin"));
    let absent_open_us = absent_started.elapsed().as_micros().max(1);
    absent
        .write_clean_close_lifecycle()
        .expect("recertify absent twin");
    drop(absent);
    let absent_snapshot =
        riffdb_storage_redb::benchmark_support::recovery_authority_snapshot_v1(&absent_twin.0)
            .expect("snapshot absent clean twin");
    prepare_command_database(&committed_twin.0);
    let committed =
        open_operational(RedbStore::open(&committed_twin.0).expect("open committed twin"));
    commit_command_fixture(&committed, &fixture);
    committed
        .write_clean_close_lifecycle()
        .expect("certify committed twin");
    drop(committed);
    let committed_started = std::time::Instant::now();
    let committed =
        open_operational(RedbStore::open(&committed_twin.0).expect("reopen committed twin"));
    let committed_open_us = committed_started.elapsed().as_micros().max(1);
    assert_postcommit_command_state(&committed, &fixture);
    committed
        .write_clean_close_lifecycle()
        .expect("recertify committed twin");
    drop(committed);
    let committed_snapshot =
        riffdb_storage_redb::benchmark_support::recovery_authority_snapshot_v1(&committed_twin.0)
            .expect("snapshot committed clean twin");
    prepare_command_database(&path.0);
    let clean = open_operational(RedbStore::open(&path.0).expect("open clean-close fixture"));
    clean
        .write_clean_close_lifecycle()
        .expect("write a matching clean-close certificate");
    drop(clean);
    run_externally_killed_command_child(&path.0);
    riffdb_storage_redb::reset_last_repair_progress_for_tests();
    let repair_started = std::time::Instant::now();
    let ports = open_operational(
        RedbStore::open(&path.0).expect("ordinary reopen of externally killed redb writer"),
    );
    let repair_open_us = repair_started.elapsed().as_micros().max(1);
    assert_ne!(
        riffdb_storage_redb::last_repair_progress_basis_points(),
        riffdb_storage_redb::REPAIR_PROGRESS_SENTINEL,
        "external SIGKILL at the real pre-commit barrier must invoke repair"
    );
    let (outcome, matching_clean_us, matching_snapshot) = match ports
        .lookup_admission(fixture.candidates.clone())
        .expect("resolve the atomic probe")
    {
        AdmissionLookupResultV1::NotFound => {
            assert_precommit_command_state(&ports, &fixture);
            ("absent", absent_open_us, &absent_snapshot)
        }
        AdmissionLookupResultV1::Found(_) => {
            assert_postcommit_command_state(&ports, &fixture);
            ("committed", committed_open_us, &committed_snapshot)
        }
        AdmissionLookupResultV1::MultipleMatches => {
            panic!("atomic probe resolved to multiple outcomes")
        }
    };
    ports
        .write_clean_close_lifecycle()
        .expect("certify recovered engine fixture");
    drop(ports);
    let recovered_snapshot =
        riffdb_storage_redb::benchmark_support::recovery_authority_snapshot_v1(&path.0)
            .expect("snapshot recovered real-engine fixture");
    assert!(
        matching_snapshot.same_authority(&recovered_snapshot),
        "repaired state must match the corresponding predeclared clean twin, including allocators and control metadata"
    );
    assert!(
        matching_snapshot.clean_generation_advance_matches(&recovered_snapshot, 1),
        "repair adds exactly the one lifecycle generation consumed by the killed writer"
    );
    let ratio_millis = repair_open_us.saturating_mul(1_000) / matching_clean_us;
    eprintln!(
        "riffdb-perf-014-repair-v1\toutcome={outcome}\trepair_observed=true\trepair_to_clean_ratio_millis={ratio_millis}"
    );
}

#[test]
fn uncapsulated_then_audited_entity_history_refuses_before_mutation_and_reopens_clean() {
    let path = TestDatabasePath::new("uncapsulated-then-audited");
    prepare_command_database(&path.0);
    let ports = open_operational(RedbStore::open(&path.0).expect("open command database"));
    let fixture = command_fixture();

    let error = try_commit_uncapsulated_command_fixture(&ports, &fixture)
        .expect_err("an entity-bearing uncapsulated command must refuse");
    assert_eq!(
        error.kind(),
        riffdb_storage_api::StorageErrorKind::InvariantViolation
    );
    assert_precommit_command_state(&ports, &fixture);

    commit_command_fixture(&ports, &fixture);
    assert_postcommit_command_state(&ports, &fixture);
    drop(ports);

    let reopened = open_operational(RedbStore::open(&path.0).expect("reopen audited history"));
    assert_postcommit_command_state(&reopened, &fixture);
}

#[test]
fn audited_then_uncapsulated_entity_history_refuses_before_mutation_and_reopens_clean() {
    let path = TestDatabasePath::new("audited-then-uncapsulated");
    prepare_command_database(&path.0);
    let ports = open_operational(RedbStore::open(&path.0).expect("open command database"));
    let first = command_fixture();
    commit_command_fixture(&ports, &first);
    let second = superseding_command_fixture_at(2, 1, &first);

    let error = try_commit_uncapsulated_command_fixture(&ports, &second)
        .expect_err("an entity-bearing uncapsulated successor must refuse");
    assert_eq!(
        error.kind(),
        riffdb_storage_api::StorageErrorKind::InvariantViolation
    );
    assert_eq!(
        ports
            .read_entity(&first.target)
            .expect("read entity after refused successor"),
        Some(first.records.entities()[0].post_image().clone())
    );
    assert_eq!(
        ports
            .lookup_admission(second.candidates.clone())
            .expect("read refused successor admission"),
        AdmissionLookupResultV1::NotFound
    );
    drop(ports);

    let reopened = open_operational(RedbStore::open(&path.0).expect("reopen audited history"));
    assert_postcommit_command_state(&reopened, &first);
    assert_eq!(
        reopened
            .lookup_admission(second.candidates.clone())
            .expect("read refused successor admission after reopen"),
        AdmissionLookupResultV1::NotFound
    );
}

#[test]
fn deferred_command_root_is_invisible_until_the_tail_fence_publishes_it() {
    let path = TestDatabasePath::new("deferred-frontier");
    prepare_command_database(&path.0);
    let ports = open_operational(RedbStore::open(&path.0).expect("reopen command database"));
    let fixture = command_fixture();

    let epoch = ports
        .begin_deferred_command_epoch()
        .expect("begin standard durability epoch");
    let epoch = apply_unpublished_command_fixture(epoch, &fixture);

    assert_precommit_command_state(&ports, &fixture);
    let committed = DeferredCommandEpoch::fence(epoch).expect("fence deferred command epoch");
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[0].batch().outcomes().len(), 1);
    assert_postcommit_command_state(&ports, &fixture);
}

#[test]
fn multiple_deferred_subgroups_publish_together_in_sequence_order() {
    let path = TestDatabasePath::new("deferred-multiple-subgroups");
    prepare_command_database(&path.0);
    let ports = open_operational(RedbStore::open(&path.0).expect("reopen command database"));
    let first = command_fixture_at(1);
    let second = command_fixture_at(2);

    let epoch = ports
        .begin_deferred_command_epoch()
        .expect("begin standard durability epoch");
    let epoch = apply_unpublished_command_fixture(epoch, &first);
    let epoch = apply_unpublished_command_fixture(epoch, &second);
    for fixture in [&first, &second] {
        assert_eq!(
            ports
                .read_entity(&fixture.target)
                .expect("read predecessor entity frontier"),
            None
        );
    }

    let committed = DeferredCommandEpoch::fence(epoch).expect("fence both deferred subgroups");
    assert_eq!(committed.len(), 2);
    assert_eq!(
        committed
            .iter()
            .map(|batch| batch.batch().outcomes()[0].commit_sequence())
            .collect::<Vec<_>>(),
        vec![
            CommitSequence::new(1).expect("sequence one"),
            CommitSequence::new(2).expect("sequence two"),
        ]
    );
    for fixture in [&first, &second] {
        let AdmissionLookupResultV1::Found(admission) = ports
            .lookup_admission(fixture.candidates.clone())
            .expect("lookup fenced subgroup identity")
        else {
            panic!("each fenced subgroup identity must be terminal");
        };
        assert_eq!(
            *admission,
            riffdb_storage_api::StoredAdmissionStateV1::StoredOutcome(
                fixture.records.stored_outcome().clone()
            )
        );
        assert_eq!(
            ports
                .read_entity(&fixture.target)
                .expect("read fenced entity"),
            Some(fixture.records.entities()[0].post_image().clone())
        );
        assert_eq!(
            ports
                .read_commit(fixture.records.commit().commit_sequence())
                .expect("read fenced commit"),
            Some(fixture.records.commit().clone())
        );
    }
    assert_eq!(
        command_audit_phases(&ports),
        vec![
            ServiceAuditPhaseV1::Started,
            ServiceAuditPhaseV1::Succeeded,
            ServiceAuditPhaseV1::Started,
            ServiceAuditPhaseV1::Succeeded,
        ]
    );
}

// req: OUT-001, OUT-002, TXN-042, REC-004, PERF-019
#[test]
fn cold_fresh_database_publications_complete_without_history_scans() {
    const PUBLICATIONS: u64 = 288;

    let path = TestDatabasePath::new("fresh-locator-no-history-scans");
    prepare_command_database(&path.0);
    let prepared = open_operational(RedbStore::open(&path.0).expect("validate fresh database"));
    let _clean = prepared.complete_graceful_close();
    drop(prepared);
    let controller = RedbTestController::observe_index_migration();
    let ports = open_operational(
        RedbStore::open_with_test_controller(&path.0, controller.clone())
            .expect("reopen fresh database"),
    );

    let mut fixtures = Vec::with_capacity(usize::try_from(PUBLICATIONS).expect("bounded proof"));
    for ordinal in 1..=PUBLICATIONS {
        let fixture = two_phase_command_fixture_at(ordinal);
        assert_eq!(
            ports
                .lookup_admission(fixture.candidates.clone())
                .expect("direct-inspection novel identity"),
            AdmissionLookupResultV1::NotFound
        );
        assert_eq!(
            controller.fresh_locator_history_fallback_scans(),
            0,
            "admission {ordinal} must use the exact public-prefix proof"
        );
        let request = AdmissionRequestV1::new(fixture.candidates.clone(), &fixture.context)
            .expect("bounded admission request");
        assert_eq!(
            ports
                .admit_or_resolve(request)
                .unwrap_or_else(|error| panic!("admit novel identity {ordinal}: {error:?}")),
            AdmissionResultV1::Created(fixture.pending.clone())
        );
        fixtures.push(fixture);
    }

    for (pipeline_ordinal, group) in fixtures.chunks_exact(3).enumerate() {
        let first_ordinal = u64::try_from(pipeline_ordinal)
            .expect("bounded pipeline ordinal")
            .checked_mul(3)
            .and_then(|value| value.checked_add(1))
            .expect("bounded first ordinal");
        let mut pipeline = Vec::with_capacity(3);
        for fixture in group {
            let epoch = ports
                .begin_deferred_command_epoch()
                .expect("begin bounded pipelined epoch");
            let fence =
                DeferredCommandEpoch::seal(apply_unpublished_command_fixture(epoch, fixture))
                    .expect("seal bounded pipelined epoch");
            pipeline.push((fixture, fence));
        }
        assert_eq!(pipeline.len(), 3, "each pipeline is exactly A/B/C");
        for (offset, (fixture, fence)) in pipeline.into_iter().enumerate() {
            let expected = first_ordinal + u64::try_from(offset).expect("bounded pipeline offset");
            let committed = fence.wait().expect("publish FIFO pipeline member");
            assert_eq!(committed.len(), 1);
            assert_eq!(
                committed[0].batch().outcomes()[0].commit_sequence(),
                CommitSequence::new(expected).expect("pipeline sequence")
            );
            assert_eq!(
                ports
                    .read_commit(CommitSequence::new(expected).expect("published sequence"))
                    .expect("read FIFO-published commit"),
                Some(fixture.records.commit().clone())
            );
            let AdmissionLookupResultV1::Found(admission) = ports
                .lookup_admission(fixture.candidates.clone())
                .expect("read FIFO-published identity")
            else {
                panic!("each FIFO-published identity must be terminal");
            };
            assert_eq!(
                *admission,
                StoredAdmissionStateV1::StoredOutcome(fixture.records.stored_outcome().clone())
            );
            assert_eq!(
                ports
                    .read_entity(&fixture.target)
                    .expect("read FIFO-published entity"),
                Some(fixture.records.entities()[0].post_image().clone())
            );
        }
    }

    let final_fixture = fixtures.last().expect("final bounded fixture");
    let AdmissionLookupResultV1::Found(final_state) = ports
        .lookup_admission(final_fixture.candidates.clone())
        .expect("exact final admitted outcome")
    else {
        panic!("final identity must resolve to its committed outcome");
    };
    let StoredAdmissionStateV1::StoredOutcome(final_outcome) = *final_state else {
        panic!("final identity must resolve to its committed outcome");
    };
    assert_eq!(
        final_outcome.commit_sequence(),
        CommitSequence::new(PUBLICATIONS).expect("bounded final sequence")
    );
    assert_eq!(ports.transient_index_rebuilds(), 0);
    assert_eq!(ports.transient_index_commit_rows(), 0);
    assert_eq!(controller.fresh_locator_history_fallback_scans(), 0);
}

// req: OUT-001, OUT-002, TXN-042, REC-004, PERF-019
#[test]
fn armed_execution_failure_and_outbox_lanes_preserve_public_and_private_coverage() {
    let failure_path = TestDatabasePath::new("fresh-locator-execution-failure-lane");
    prepare_command_database(&failure_path.0);
    let failure_controller = RedbTestController::observe_index_migration();
    let failure_ports = open_operational(
        RedbStore::open_with_test_controller(&failure_path.0, failure_controller.clone())
            .expect("open execution-failure database"),
    );
    let failure = two_phase_command_fixture_at(1);
    assert!(matches!(
        admit_audited_command(&failure_ports, &failure).admission(),
        AdmissionResultV1::Created(_)
    ));
    let request = ExecutionFailureTransitionRequestV1::new(
        failure.pending.clone(),
        &failure.snapshot,
        ExecutionFailureCode::ArithmeticFault,
    )
    .expect("execution-failure request");
    let ExecutionFailureAdmissionResult::Rechecked(rechecked) = failure_ports
        .begin_execution_failure(request)
        .expect("begin execution-failure lane")
    else {
        panic!("the exact admitted pending row must be rechecked");
    };
    let (awaiting, current) = rechecked
        .read_transaction_current()
        .expect("read exact failure dependencies");
    assert_eq!(current.bindings(), failure.snapshot.bindings());
    let terminal = awaiting
        .terminalize()
        .expect("terminalize real failure lane");
    assert_eq!(terminal.pending(), &failure.pending);
    let failure_continuation = two_phase_command_fixture_at(2);
    assert_eq!(
        failure_ports
            .lookup_admission(failure_continuation.candidates.clone())
            .expect("public-prefix miss after failure"),
        AdmissionLookupResultV1::NotFound
    );
    assert!(matches!(
        admit_audited_command(&failure_ports, &failure_continuation).admission(),
        AdmissionResultV1::Created(_)
    ));
    assert_eq!(failure_controller.fresh_locator_history_fallback_scans(), 0);

    let outbox_path = TestDatabasePath::new("fresh-locator-outbox-lane");
    prepare_command_database(&outbox_path.0);
    let outbox_controller = RedbTestController::observe_index_migration();
    let mut outbox_ports = open_operational(
        RedbStore::open_with_test_controller(&outbox_path.0, outbox_controller.clone())
            .expect("open outbox database"),
    );
    let command = two_phase_command_fixture_at(1);
    assert!(matches!(
        admit_audited_command(&outbox_ports, &command).admission(),
        AdmissionResultV1::Created(_)
    ));
    commit_two_phase_command_fixture(&outbox_ports, &command);
    let event_id = command.records.events()[0].event_id();
    let claim = OutboxClaimV1::new(
        event_id,
        OutboxStatusObservationV1::AbsentInitialPending,
        OutboxDestinationIdV1::new("fresh-locator-proof").expect("destination"),
        Timestamp::new(1_700_000_010, 0).expect("claim time"),
        Timestamp::new(1_700_000_020, 0).expect("lease deadline"),
    )
    .expect("outbox claim");
    assert!(matches!(
        outbox_ports
            .claim_outbox(&claim)
            .expect("claim real outbox row"),
        OutboxTransitionResultV1::Applied(_)
    ));
    let outbox_continuation = two_phase_command_fixture_at(2);
    assert_eq!(
        outbox_ports
            .lookup_admission(outbox_continuation.candidates.clone())
            .expect("public-prefix miss after outbox transition"),
        AdmissionLookupResultV1::NotFound
    );
    assert!(matches!(
        admit_audited_command(&outbox_ports, &outbox_continuation).admission(),
        AdmissionResultV1::Created(_)
    ));
    assert_eq!(outbox_controller.fresh_locator_history_fallback_scans(), 0);
}

fn protected_consumer_identity(seed: u8) -> EventConsumerIdentityV1 {
    EventConsumerIdentityV1::new(
        database_id(),
        ReactiveModuleHash::from_bytes([seed; 32]),
        ReactiveOperationName::new("ProtectedRows").expect("reactive operation"),
        QueryParameterHash::from_bytes([seed.wrapping_add(1); 32]),
        EventConsumerName::new(format!("protected-{seed}")).expect("consumer name"),
    )
}

fn protected_event_admission(
    capability: StoredCapabilityRecordV1,
    event: StoredDurableEventV1,
    current: StoredEntityRecordV1,
    observed_at: Timestamp,
) -> EventPolicyAdmissionFenceV1 {
    EventPolicyAdmissionFenceV1::new(
        capability,
        observed_at,
        vec![
            EventPolicyAdmissionObservationV1::new(event, Some(current), Vec::new(), true)
                .expect("admitted current-row observation"),
        ],
    )
    .expect("protected event admission")
}

// req: OUT-001, OUT-002, TXN-042, REC-004, PERF-019
#[test]
fn every_protected_consumer_mutation_path_preserves_armed_public_and_private_coverage() {
    let path = TestDatabasePath::new("fresh-locator-protected-consumer-lanes");
    let controller = RedbTestController::observe_index_migration();
    let (mut ports, capability) = prepare_protected_command_database(&path.0, controller.clone());

    let command = two_phase_command_fixture_at(1);
    assert!(matches!(
        admit_audited_command(&ports, &command).admission(),
        AdmissionResultV1::Created(_)
    ));
    commit_two_phase_command_fixture(&ports, &command);
    let event = command.records.events()[0].clone();
    assert!(
        event.policy_anchor().is_some(),
        "fixture event must be protected"
    );
    let current = command.records.entities()[0].post_image().clone();
    let event_id = event.event_id();
    let partition_hash = PartitionKeyHash::from_bytes([0x91; 32]);

    let first_identity = protected_consumer_identity(0x81);
    let first_token = EventLeaseToken::from_bytes([0x82; 32]);
    let selected_at = Timestamp::new(1_700_000_010, 0).expect("selection time");
    let selected = ports
        .coordinate_protected_event_consumer_lease(ProtectedEventConsumerLeaseV1 {
            identity: first_identity.clone(),
            partition_hash,
            history_incarnation: 1,
            observed_at: selected_at,
            expires_at: Timestamp::new(1_700_000_020, 0).expect("lease expiry"),
            selected_events: vec![event_id],
            tokens: vec![first_token],
            batch_limit: 1,
            in_flight_limit: 1,
            admission: protected_event_admission(
                capability.clone(),
                event.clone(),
                current.clone(),
                selected_at,
            ),
        })
        .expect("run protected policy-selection path");
    assert_eq!(
        selected.transition,
        EventConsumerTransitionResultV1::Applied
    );
    assert_eq!(selected.leases.len(), 1);

    let after_selection = two_phase_command_fixture_at(2);
    assert_eq!(
        ports
            .lookup_admission(after_selection.candidates.clone())
            .expect("public-prefix miss after protected selection"),
        AdmissionLookupResultV1::NotFound
    );

    let resolved_at = Timestamp::new(1_700_000_011, 0).expect("resolution time");
    assert_eq!(
        ports
            .coordinate_protected_event_consumer_resolution(ProtectedEventConsumerResolutionV1 {
                acknowledgement: CoordinateConsumerAcknowledgementV1 {
                    identity: first_identity,
                    event_id,
                    token: first_token,
                    history_incarnation: 1,
                    observed_at: resolved_at,
                    selected_prefix: vec![event_id],
                },
                retry_at: None,
                admission: protected_event_admission(
                    capability.clone(),
                    event.clone(),
                    current.clone(),
                    resolved_at,
                ),
            })
            .expect("run protected resolution path"),
        EventConsumerTransitionResultV1::Applied
    );

    let recovery_identity = protected_consumer_identity(0x83);
    let recovery_token = EventLeaseToken::from_bytes([0x84; 32]);
    let recovery_selected_at = Timestamp::new(1_700_000_012, 0).expect("recovery selection time");
    let recovery_expiry = Timestamp::new(1_700_000_013, 0).expect("recovery lease expiry");
    let recovery_selected = ports
        .coordinate_protected_event_consumer_lease(ProtectedEventConsumerLeaseV1 {
            identity: recovery_identity.clone(),
            partition_hash,
            history_incarnation: 1,
            observed_at: recovery_selected_at,
            expires_at: recovery_expiry,
            selected_events: vec![event_id],
            tokens: vec![recovery_token],
            batch_limit: 1,
            in_flight_limit: 1,
            admission: protected_event_admission(
                capability.clone(),
                event.clone(),
                current.clone(),
                recovery_selected_at,
            ),
        })
        .expect("seed one protected lease for recovery");
    assert_eq!(recovery_selected.leases.len(), 1);
    let recovered = ports
        .coordinate_protected_event_consumer_lease(ProtectedEventConsumerLeaseV1 {
            identity: recovery_identity,
            partition_hash,
            history_incarnation: 1,
            observed_at: recovery_expiry,
            expires_at: Timestamp::new(1_700_000_030, 0).expect("post-recovery expiry"),
            selected_events: vec![event_id],
            tokens: vec![EventLeaseToken::from_bytes([0x85; 32])],
            batch_limit: 1,
            in_flight_limit: 1,
            admission: protected_event_admission(capability, event, current, recovery_expiry),
        })
        .expect("run protected expired-lease recovery path");
    assert_eq!(
        recovered.transition,
        EventConsumerTransitionResultV1::StateChanged
    );
    assert!(recovered.leases.is_empty());

    assert_eq!(
        ports
            .lookup_admission(after_selection.candidates.clone())
            .expect("public-prefix miss after every protected consumer path"),
        AdmissionLookupResultV1::NotFound
    );
    assert!(matches!(
        admit_audited_command(&ports, &after_selection).admission(),
        AdmissionResultV1::Created(_)
    ));
    assert_eq!(controller.fresh_locator_history_fallback_scans(), 0);
}

// req: OUT-001, OUT-002, TXN-042, REC-004, PERF-019
#[test]
fn fresh_locator_before_and_after_publication_kills_drop_process_proof_and_fail_closed() {
    for (mode, committed) in [
        ("before-fresh-locator-publication", false),
        ("after-fresh-locator-publication", true),
    ] {
        let path = TestDatabasePath::new(mode);
        prepare_command_database(&path.0);
        run_crashing_child(mode, &path.0);

        let ports = open_operational(
            RedbStore::open(&path.0).expect("recover killed fresh-locator publication"),
        );
        let prior = two_phase_command_fixture_at(1);
        let AdmissionLookupResultV1::Found(prior_state) = ports
            .lookup_admission(prior.candidates)
            .expect("prior identity remains exactly readable after restart")
        else {
            panic!("the admitted pre-publication identity must survive both kill boundaries");
        };
        assert_eq!(
            matches!(*prior_state, StoredAdmissionStateV1::StoredOutcome(_)),
            committed,
            "only the after-publication kill may expose the complete command outcome"
        );
        let _clean = ports.complete_graceful_close();
        drop(ports);
        let ports = open_operational(
            RedbStore::open(&path.0).expect("clean reopen after killed publication recovery"),
        );

        let novel = two_phase_command_fixture_at(2);
        assert_eq!(
            ports
                .lookup_admission(novel.candidates.clone())
                .expect("restart novel-key inspection retains its prior result algebra"),
            AdmissionLookupResultV1::NotFound
        );
        let request = AdmissionRequestV1::new(novel.candidates.clone(), &novel.context)
            .expect("post-restart admission request");
        assert_eq!(
            ports
                .admit_or_resolve(request)
                .expect("nonempty first command-write entry keeps current admission semantics"),
            AdmissionResultV1::Created(novel.pending)
        );
        let later = two_phase_command_fixture_at(3);
        assert_eq!(
            ports
                .lookup_admission(later.candidates)
                .expect("disabled coverage keeps bounded operational fallback"),
            AdmissionLookupResultV1::NotFound
        );
    }
}

#[test]
fn multiple_sealed_epochs_publish_in_fifo_order_after_one_journal_flush() {
    let path = TestDatabasePath::new("deferred-multiple-epochs");
    prepare_command_database(&path.0);
    let ports = open_operational(RedbStore::open(&path.0).expect("reopen command database"));
    let first = command_fixture_at(1);
    let second = command_fixture_at(2);

    let first_epoch = ports
        .begin_deferred_command_epoch()
        .expect("begin first standard durability epoch");
    let first_fence =
        DeferredCommandEpoch::seal(apply_unpublished_command_fixture(first_epoch, &first))
            .expect("seal first standard durability epoch");
    let second_epoch = ports
        .begin_deferred_command_epoch()
        .expect("begin second standard durability epoch");
    let second_fence =
        DeferredCommandEpoch::seal(apply_unpublished_command_fixture(second_epoch, &second))
            .expect("seal second standard durability epoch");

    for fixture in [&first, &second] {
        assert_eq!(
            ports
                .read_entity(&fixture.target)
                .expect("read predecessor entity frontier"),
            None
        );
    }
    let first_committed = first_fence.wait().expect("publish first journal frame");
    assert_eq!(first_committed.len(), 1);
    assert_postcommit_command_state(&ports, &first);
    assert_eq!(
        ports
            .read_entity(&second.target)
            .expect("read second unpublished entity"),
        None
    );
    let second_committed = second_fence.wait().expect("publish second journal frame");
    assert_eq!(second_committed.len(), 1);
    let AdmissionLookupResultV1::Found(admission) = ports
        .lookup_admission(second.candidates.clone())
        .expect("lookup second fenced identity")
    else {
        panic!("the second fenced identity must be terminal");
    };
    assert_eq!(
        *admission,
        riffdb_storage_api::StoredAdmissionStateV1::StoredOutcome(
            second.records.stored_outcome().clone()
        )
    );
    assert_eq!(
        ports
            .read_entity(&second.target)
            .expect("read second entity"),
        Some(second.records.entities()[0].post_image().clone())
    );
    assert_eq!(
        ports
            .read_commit(CommitSequence::new(2).expect("sequence two"))
            .expect("read second commit"),
        Some(second.records.commit().clone())
    );
}

#[test]
fn later_command_fence_drives_durable_predecessor_publication() {
    let path = TestDatabasePath::new("deferred-command-publication-order");
    prepare_command_database(&path.0);
    let ports = open_operational(RedbStore::open(&path.0).expect("reopen command database"));
    let first = command_fixture_at(1);
    let second = command_fixture_at(2);

    let first_epoch = ports
        .begin_deferred_command_epoch()
        .expect("begin first standard durability epoch");
    let first_fence =
        DeferredCommandEpoch::seal(apply_unpublished_command_fixture(first_epoch, &first))
            .expect("seal first standard durability epoch");
    let second_epoch = ports
        .begin_deferred_command_epoch()
        .expect("begin second standard durability epoch");
    let second_fence =
        DeferredCommandEpoch::seal(apply_unpublished_command_fixture(second_epoch, &second))
            .expect("seal second standard durability epoch");

    // Independent coordinator work can observe the later durability receipt
    // first. Waiting on it must advance the complete durable prefix rather
    // than attempting a stale predecessor swap or requiring the first caller
    // to run before it can make progress.
    let second_committed = second_fence
        .wait()
        .expect("later fence publishes the durable prefix");
    let first_committed = first_fence
        .wait()
        .expect("predecessor result remains available");

    assert_eq!(first_committed.len(), 1);
    assert_eq!(second_committed.len(), 1);
    for fixture in [&first, &second] {
        assert_eq!(
            ports
                .read_entity(&fixture.target)
                .expect("read published entity"),
            Some(fixture.records.entities()[0].post_image().clone())
        );
        assert_eq!(
            ports
                .read_commit(fixture.records.commit().commit_sequence())
                .expect("read published commit"),
            Some(fixture.records.commit().clone())
        );
    }
    assert_eq!(
        command_audit_phases(&ports),
        [
            ServiceAuditPhaseV1::Started,
            ServiceAuditPhaseV1::Succeeded,
            ServiceAuditPhaseV1::Started,
            ServiceAuditPhaseV1::Succeeded,
        ]
    );
}

#[test]
fn command_and_service_audit_fences_publish_one_interleaved_durable_prefix() {
    let path = TestDatabasePath::new("interleaved-command-audit-publication-order");
    prepare_command_database(&path.0);
    let mut ports = open_operational(RedbStore::open(&path.0).expect("reopen command database"));

    let first_audit = [standalone_audit_intent(0x91, 1_700_000_091)];
    let riffdb_storage_api::ServiceAuditGroupAppend::Submitted(first_audit_fence) = ports
        .submit_service_audit_group(&first_audit)
        .expect("submit audit predecessor")
    else {
        panic!("the standard profile must defer the audit predecessor");
    };

    let command = command_fixture_at(1);
    let command_epoch = ports
        .begin_deferred_command_epoch()
        .expect("begin interleaved command epoch");
    let command_fence =
        DeferredCommandEpoch::seal(apply_unpublished_command_fixture(command_epoch, &command))
            .expect("seal interleaved command epoch");

    let last_audit = [standalone_audit_intent(0x92, 1_700_000_092)];
    let riffdb_storage_api::ServiceAuditGroupAppend::Submitted(last_audit_fence) = ports
        .submit_service_audit_group(&last_audit)
        .expect("submit audit successor")
    else {
        panic!("the standard profile must defer the audit successor");
    };

    // Independent completion workers may observe the middle and tail receipts
    // before the head. Either waiter must publish the complete journal prefix
    // across both frame kinds without treating the other kind as a stale view.
    let command_result = command_fence
        .wait()
        .expect("middle command publishes its audit predecessor");
    let last_audit_result = last_audit_fence
        .wait()
        .expect("tail audit publishes after the command");
    let first_audit_result = first_audit_fence
        .wait()
        .expect("head audit retains its typed result");

    assert_eq!(command_result.len(), 1);
    assert_eq!(first_audit_result.len(), 1);
    assert_eq!(last_audit_result.len(), 1);
    assert_postcommit_command_state(&ports, &command);
}

#[test]
fn query_audit_lifecycle_can_span_an_unpublished_command_frame() {
    let path = TestDatabasePath::new("query-audit-spans-command-publication");
    prepare_command_database(&path.0);
    let mut ports = open_operational(RedbStore::open(&path.0).expect("reopen command database"));
    let request_id = RequestId::from_bytes(uuid_bytes(0x93)).expect("query audit request");
    let started = ServiceAuditAppendIntentV1::new(
        request_id,
        Timestamp::new(1_700_000_093, 0).expect("query start timestamp"),
        ServiceOperationV1::ExecuteQuery,
        ServiceAuditPhaseV1::Started,
        catalog_principal(),
        ServiceIngressKindV1::Grpc,
        ServiceAuditTargetsV1::empty(),
        None,
        ServiceAuditLinkV1::None,
    )
    .expect("query started audit");
    let riffdb_storage_api::ServiceAuditGroupAppend::Submitted(started_fence) = ports
        .submit_service_audit_group(&[started])
        .expect("submit query start")
    else {
        panic!("the standard profile must defer the query start");
    };

    let command = command_fixture_at(1);
    let command_epoch = ports
        .begin_deferred_command_epoch()
        .expect("begin command between query audit phases");
    let command_fence =
        DeferredCommandEpoch::seal(apply_unpublished_command_fixture(command_epoch, &command))
            .expect("seal command between query audit phases");

    let terminal = ServiceAuditAppendIntentV1::new(
        request_id,
        Timestamp::new(1_700_000_094, 0).expect("query terminal timestamp"),
        ServiceOperationV1::ExecuteQuery,
        ServiceAuditPhaseV1::Succeeded,
        catalog_principal(),
        ServiceIngressKindV1::Grpc,
        ServiceAuditTargetsV1::empty(),
        None,
        ServiceAuditLinkV1::None,
    )
    .expect("query terminal audit");
    let riffdb_storage_api::ServiceAuditGroupAppend::Submitted(terminal_fence) = ports
        .submit_service_audit_group(&[terminal])
        .expect("submit query terminal")
    else {
        panic!("the standard profile must defer the query terminal");
    };

    let terminal_result = terminal_fence
        .wait()
        .expect("query terminal publishes the complete mixed prefix");
    let command_result = command_fence
        .wait()
        .expect("interleaved command retains its typed result");
    let started_result = started_fence
        .wait()
        .expect("query start retains its typed result");
    assert_eq!(terminal_result.len(), 1);
    assert_eq!(command_result.len(), 1);
    assert_eq!(started_result.len(), 1);
    assert_postcommit_command_state(&ports, &command);
}

#[test]
fn direct_barrier_publishes_an_already_submitted_command_prefix() {
    let path = TestDatabasePath::new("direct-barrier-publishes-command-prefix");
    prepare_command_database(&path.0);
    let mut ports = open_operational(RedbStore::open(&path.0).expect("reopen command database"));
    let command = command_fixture_at(1);
    let epoch = ports
        .begin_deferred_command_epoch()
        .expect("begin command before direct barrier");
    let command_fence =
        DeferredCommandEpoch::seal(apply_unpublished_command_fixture(epoch, &command))
            .expect("submit command before direct barrier");

    let audit = standalone_audit_intent(0x94, 1_700_000_094);
    let direct_result = ports
        .append_service_audit_group(&[audit])
        .expect("direct barrier publishes and checkpoints the pending command");
    assert!(matches!(
        direct_result.as_slice(),
        [ServiceAuditAppendResult::Appended(_)]
    ));

    let committed = command_fence
        .wait()
        .expect("original command caller retains its result after barrier publication");
    assert_eq!(committed.len(), 1);
    assert_postcommit_command_state(&ports, &command);
}

#[test]
fn direct_commit_reanchors_a_drained_journal_before_the_next_deferred_epoch() {
    let path = TestDatabasePath::new("journal-direct-journal-frontiers");
    prepare_command_database(&path.0);
    let ports = open_operational(RedbStore::open(&path.0).expect("reopen command database"));
    let first = command_fixture_at(1);
    let direct = command_fixture_at(2);
    let third = command_fixture_at(3);

    let first_epoch = ports
        .begin_deferred_command_epoch()
        .expect("begin first journal epoch");
    DeferredCommandEpoch::fence(apply_unpublished_command_fixture(first_epoch, &first))
        .expect("publish first journal epoch");

    commit_command_fixture(&ports, &direct);

    let third_epoch = ports
        .begin_deferred_command_epoch()
        .expect("begin journal epoch after direct commit");
    DeferredCommandEpoch::fence(apply_unpublished_command_fixture(third_epoch, &third))
        .expect("publish journal epoch after direct commit");

    for fixture in [&first, &direct, &third] {
        assert_eq!(
            ports
                .read_entity(&fixture.target)
                .expect("read committed entity"),
            Some(fixture.records.entities()[0].post_image().clone())
        );
        assert_eq!(
            ports
                .read_commit(fixture.records.commit().commit_sequence())
                .expect("read committed command"),
            Some(fixture.records.commit().clone())
        );
    }
    assert_eq!(
        command_audit_phases(&ports),
        vec![
            ServiceAuditPhaseV1::Started,
            ServiceAuditPhaseV1::Succeeded,
            ServiceAuditPhaseV1::Started,
            ServiceAuditPhaseV1::Succeeded,
            ServiceAuditPhaseV1::Started,
            ServiceAuditPhaseV1::Succeeded,
        ]
    );
}

#[test]
fn direct_commit_reanchors_an_unopened_empty_journal_before_a_deferred_epoch() {
    let path = TestDatabasePath::new("direct-empty-journal-frontier");
    prepare_command_database(&path.0);
    let ports = open_operational(RedbStore::open(&path.0).expect("reopen command database"));
    let direct = command_fixture_at(1);
    let deferred = command_fixture_at(2);

    commit_command_fixture(&ports, &direct);

    let epoch = ports
        .begin_deferred_command_epoch()
        .expect("begin journal epoch after the first direct commit");
    DeferredCommandEpoch::fence(apply_unpublished_command_fixture(epoch, &deferred))
        .expect("publish journal epoch after the first direct commit");

    for fixture in [&direct, &deferred] {
        assert_eq!(
            ports
                .read_entity(&fixture.target)
                .expect("read committed entity"),
            Some(fixture.records.entities()[0].post_image().clone())
        );
        assert_eq!(
            ports
                .read_commit(fixture.records.commit().commit_sequence())
                .expect("read committed command"),
            Some(fixture.records.commit().clone())
        );
    }
    assert_eq!(
        command_audit_phases(&ports),
        vec![
            ServiceAuditPhaseV1::Started,
            ServiceAuditPhaseV1::Succeeded,
            ServiceAuditPhaseV1::Started,
            ServiceAuditPhaseV1::Succeeded,
        ]
    );
}

#[test]
fn unknown_tail_status_keeps_the_predecessor_frontier_and_fences_writes() {
    let path = TestDatabasePath::new("deferred-tail-unknown");
    prepare_command_database(&path.0);
    let controller =
        RedbTestController::return_unknown_after_commit(RedbTestOperation::CommandEpochTail);
    let ports = open_operational(
        RedbStore::open_with_test_controller(&path.0, controller)
            .expect("reopen controlled command database"),
    );
    let fixture = command_fixture();
    let epoch = ports
        .begin_deferred_command_epoch()
        .expect("begin controlled durability epoch");
    let epoch = apply_unpublished_command_fixture(epoch, &fixture);

    let error = DeferredCommandEpoch::fence(epoch).expect_err("tail status must be unknown");
    assert_eq!(
        error.kind(),
        riffdb_storage_api::StorageErrorKind::CommitStatusUnknown
    );
    assert_eq!(
        ports
            .read_entity(&fixture.target)
            .expect("fenced handle retains predecessor frontier"),
        None
    );
    let Err(write_error) = ports.begin_empty_batch() else {
        panic!("unknown tail status must fence authoritative writes");
    };
    assert_eq!(
        write_error.kind(),
        riffdb_storage_api::StorageErrorKind::Unavailable
    );
    drop(ports);

    let reopened =
        open_operational(RedbStore::open(&path.0).expect("reopen known-committed epoch"));
    assert_postcommit_command_state(&reopened, &fixture);
}

#[test]
fn hardened_profile_rejects_deferred_command_epochs() {
    let path = TestDatabasePath::new("deferred-hardened-rejected");
    prepare_command_database(&path.0);
    let ports = open_operational(
        RedbStore::open_with_commit_profile(&path.0, RedbCommitProfile::Hardened)
            .expect("reopen hardened command database"),
    );

    let Err(error) = ports.begin_deferred_command_epoch() else {
        panic!("hardened profile must keep independently Immediate groups");
    };
    assert_eq!(
        error.kind(),
        riffdb_storage_api::StorageErrorKind::Unavailable
    );
}

#[test]
// req: STO-022
fn all_v1_index_rows_migrate_exactly_and_require_a_fresh_clean_pass() {
    let path = TestDatabasePath::new("all-v1-index-migration");
    let (_, current) = prepare_committed_command_database(&path.0);
    let current_envelope = encode_index_entry_v2(&current).expect("encode current V2 row");
    let legacy = legacy_index_entry(&current);
    let legacy_envelope =
        encode_index_entry_v1_fixture(&legacy).expect("encode legacy V1 fixture row");
    let previous = write_raw_index_entries(
        &path.0,
        &[(current.key().clone(), legacy_envelope.as_bytes().to_vec())],
    );
    assert_eq!(
        previous,
        vec![Some(current_envelope.as_bytes().to_vec())],
        "the test must replace the normal V2 row with exact canonical V1 bytes"
    );

    let controller = RedbTestController::observe_index_migration();
    let (first_session, catalog_outcome, outcome) = complete_startup_pass(
        RedbStore::open_with_test_controller(&path.0, controller.clone())
            .expect("open all-V1 migration fixture"),
    );
    let CatalogHistoryOutcome::MigrationRequired(context) = catalog_outcome else {
        panic!("a complete all-V1 catalog pass must require migration");
    };
    let StructuralOpenOutcome::MigrationRequired(port) = outcome else {
        panic!("a complete all-V1 startup pass must require migration");
    };
    assert_eq!(context.database_id(), database_id());
    assert_eq!(context.open_session_id(), first_session);
    let (store, observation) = drive_index_migration(port, context, &controller);
    assert_eq!(
        observation,
        MigrationObservation {
            v1_rewrites: 1,
            v2_confirms: 0,
            pages: 1,
        }
    );

    let (fresh_session, catalog_outcome, outcome) = complete_startup_pass(store);
    assert!(matches!(catalog_outcome, CatalogHistoryOutcome::Ready(_)));
    assert_ne!(
        fresh_session, first_session,
        "post-migration validation must use a fresh OpenSessionId"
    );
    let StructuralOpenOutcome::Clean(opened) = outcome else {
        panic!("the complete post-migration pass must be V2-only and clean");
    };
    drop(opened);

    let rows = read_raw_index_entries(&path.0);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, current.key().as_bytes());
    assert_eq!(rows[0].1, current_envelope.as_bytes());
    assert_eq!(
        decode_index_entry_v2(&rows[0].1)
            .expect("migrated V2 row decodes")
            .value(),
        &current
    );
    assert!(
        decode_index_entry_v1(&rows[0].1).is_err(),
        "successful migration must leave no V1 row"
    );
}

#[test]
fn mixed_index_rows_rewrite_v1_and_confirm_v2_without_changing_its_bytes() {
    let path = TestDatabasePath::new("mixed-index-migration");
    let (_, first) = prepare_committed_command_database(&path.0);
    let second = migration_index_entry(8, 8);
    assert!(first.key().as_bytes() < second.key().as_bytes());

    let first_v2_envelope = encode_index_entry_v2(&first).expect("encode first V2 row");
    let first_v1_envelope = encode_index_entry_v1_fixture(&legacy_index_entry(&first))
        .expect("encode first legacy row");
    let second_v2_envelope = encode_index_entry_v2(&second).expect("encode second V2 row");
    let previous = write_raw_index_entries(
        &path.0,
        &[
            (first.key().clone(), first_v1_envelope.as_bytes().to_vec()),
            (second.key().clone(), second_v2_envelope.as_bytes().to_vec()),
        ],
    );
    assert_eq!(previous[0], Some(first_v2_envelope.as_bytes().to_vec()));
    assert_eq!(previous[1], None);
    let before = read_raw_index_entries(&path.0);
    assert_eq!(before.len(), 2);
    assert_eq!(before[1].1, second_v2_envelope.as_bytes());

    let controller = RedbTestController::observe_index_migration();
    let (first_session, catalog_outcome, outcome) = complete_startup_pass(
        RedbStore::open_with_test_controller(&path.0, controller.clone())
            .expect("open mixed migration fixture"),
    );
    let CatalogHistoryOutcome::MigrationRequired(context) = catalog_outcome else {
        panic!("a mixed V1/V2 catalog pass must require migration");
    };
    let StructuralOpenOutcome::MigrationRequired(port) = outcome else {
        panic!("a mixed V1/V2 startup pass must require migration");
    };
    let (store, observation) = drive_index_migration(port, context, &controller);
    assert_eq!(
        observation,
        MigrationObservation {
            v1_rewrites: 1,
            v2_confirms: 1,
            pages: 1,
        }
    );

    let (fresh_session, catalog_outcome, outcome) = complete_startup_pass(store);
    assert!(matches!(catalog_outcome, CatalogHistoryOutcome::Ready(_)));
    assert_ne!(fresh_session, first_session);
    let StructuralOpenOutcome::Clean(opened) = outcome else {
        panic!("mixed migration must be followed by one fresh clean pass");
    };
    drop(opened);

    let after = read_raw_index_entries(&path.0);
    assert_eq!(after.len(), 2);
    assert_eq!(after[0].0, first.key().as_bytes());
    assert_eq!(after[0].1, first_v2_envelope.as_bytes());
    assert_eq!(after[1].0, second.key().as_bytes());
    assert_eq!(
        after[1].1, before[1].1,
        "V2Confirm must perform no write and preserve the exact observed envelope"
    );
    assert_eq!(
        decode_index_entry_v2(&after[0].1)
            .expect("rewritten first row")
            .value(),
        &first
    );
    assert_eq!(
        decode_index_entry_v2(&after[1].1)
            .expect("confirmed second row")
            .value(),
        &second
    );
    assert!(
        after
            .iter()
            .all(|(_, envelope)| decode_index_entry_v1(envelope).is_err())
    );
}

#[test]
fn redb_migration_splits_501_rows_at_the_exact_backend_count_boundary() {
    let path = TestDatabasePath::new("migration-count-boundary");
    let (_, current) = prepare_committed_command_database(&path.0);
    let mut rows = Vec::with_capacity(MAX_INDEX_MIGRATION_PAGE_ENTRIES + 1);
    rows.push(current);
    rows.extend((8_u64..508).map(|value| migration_index_entry(value, value)));
    assert_eq!(rows.len(), MAX_INDEX_MIGRATION_PAGE_ENTRIES + 1);
    rows.sort_by(|left, right| left.key().as_bytes().cmp(right.key().as_bytes()));

    let raw_rows = rows
        .iter()
        .map(|row| {
            let envelope = encode_index_entry_v1_fixture(&legacy_index_entry(row))
                .expect("encode count-boundary V1 row");
            (row.key().clone(), envelope.as_bytes().to_vec())
        })
        .collect::<Vec<_>>();
    write_raw_index_entries(&path.0, &raw_rows);
    let controller = RedbTestController::observe_index_migration();
    let (_, catalog_outcome, outcome) = complete_startup_pass(
        RedbStore::open_with_test_controller(&path.0, controller.clone())
            .expect("open count-boundary migration fixture"),
    );
    let CatalogHistoryOutcome::MigrationRequired(context) = catalog_outcome else {
        panic!("501 V1 catalog rows must require migration");
    };
    let StructuralOpenOutcome::MigrationRequired(port) = outcome else {
        panic!("501 V1 rows must require migration");
    };
    let (store, observation) = drive_index_migration(port, context, &controller);
    assert_eq!(observation.pages, 2);
    assert_eq!(
        observation.v1_rewrites,
        MAX_INDEX_MIGRATION_PAGE_ENTRIES + 1
    );
    assert_eq!(observation.v2_confirms, 0);
    let (_, catalog_outcome, outcome) = complete_startup_pass(store);
    assert!(matches!(catalog_outcome, CatalogHistoryOutcome::Ready(_)));
    assert!(matches!(outcome, StructuralOpenOutcome::Clean(_)));
    drop(outcome);
    assert_eq!(read_raw_index_entries(&path.0).len(), rows.len());
}

#[test]
fn redb_migration_stops_on_instruction_bytes_with_evidence_capacity_remaining() {
    const CANONICAL_RECORD_OVERHEAD: usize = 16;
    let path = TestDatabasePath::new("migration-instruction-ledger");
    let (_, current) = prepare_committed_command_database(&path.0);
    let covered_values = CanonicalRecord::new(vec![(
        FieldId::first(),
        CanonicalValue::bytes(vec![
            0xa5;
            MAX_CANONICAL_DOCUMENT_BYTES - CANONICAL_RECORD_OVERHEAD
        ])
        .expect("maximum bytes value"),
    )])
    .expect("maximum covered values");
    let first = StoredIndexEntryV2::new(
        current.key().clone(),
        current.schema_binding().clone(),
        covered_values.clone(),
        current.partition_key().clone(),
    )
    .expect("first maximum migration row");
    let second = migration_index_entry_with_covered_values(8, 8, covered_values);
    let mut rows = [first, second];
    rows.sort_by(|left, right| left.key().as_bytes().cmp(right.key().as_bytes()));
    let raw_rows = rows
        .iter()
        .map(|row| {
            let envelope = encode_index_entry_v1_fixture(&legacy_index_entry(row))
                .expect("encode maximum V1 row");
            (row.key().clone(), envelope.as_bytes().to_vec())
        })
        .collect::<Vec<_>>();
    let evidence = raw_rows
        .iter()
        .map(|(key, envelope)| {
            decode_index_migration_row(key, envelope).expect("decode maximum migration evidence")
        })
        .collect::<Vec<_>>();
    assert!(
        evidence
            .iter()
            .map(|row| row.evidence_page_charge())
            .sum::<usize>()
            <= MAX_INDEX_MIGRATION_PAGE_BYTES
    );
    assert!(
        evidence
            .iter()
            .map(|row| row.instruction_page_charge())
            .sum::<usize>()
            > MAX_INDEX_MIGRATION_PAGE_BYTES
    );
    assert!(
        evidence
            .iter()
            .all(|row| row.instruction_page_charge() <= MAX_INDEX_MIGRATION_PAGE_BYTES),
        "each row must fit independently so migration can make progress"
    );
    write_raw_index_entries(&path.0, &raw_rows);
    let controller = RedbTestController::observe_index_migration();
    let (_, catalog_outcome, outcome) = complete_startup_pass(
        RedbStore::open_with_test_controller(&path.0, controller.clone())
            .expect("open instruction-ledger fixture"),
    );
    let CatalogHistoryOutcome::MigrationRequired(context) = catalog_outcome else {
        panic!("maximum V1 catalog rows must require migration");
    };
    let StructuralOpenOutcome::MigrationRequired(port) = outcome else {
        panic!("maximum V1 rows must require migration");
    };
    let (store, observation) = drive_index_migration(port, context, &controller);
    assert_eq!(observation.pages, 2);
    assert_eq!(observation.v1_rewrites, 2);
    assert_eq!(observation.v2_confirms, 0);
    let (_, catalog_outcome, outcome) = complete_startup_pass(store);
    assert!(matches!(catalog_outcome, CatalogHistoryOutcome::Ready(_)));
    assert!(matches!(outcome, StructuralOpenOutcome::Clean(_)));
}

#[test]
fn redb_driver_rejects_a_port_from_another_migration_session() {
    let first_path = TestDatabasePath::new("resume-first-session");
    let (_, first) = prepare_committed_command_database(&first_path.0);
    let first_legacy =
        encode_index_entry_v1_fixture(&legacy_index_entry(&first)).expect("encode first V1 row");
    write_raw_index_entries(
        &first_path.0,
        &[(first.key().clone(), first_legacy.as_bytes().to_vec())],
    );
    let (_, first_catalog_outcome, first_outcome) = complete_startup_pass(
        RedbStore::open(&first_path.0).expect("open first migration session"),
    );
    let CatalogHistoryOutcome::MigrationRequired(first_context) = first_catalog_outcome else {
        panic!("first catalog session must require migration");
    };
    let StructuralOpenOutcome::MigrationRequired(first_port) = first_outcome else {
        panic!("first session must require migration");
    };

    let second_path = TestDatabasePath::new("resume-second-session");
    let (_, second) = prepare_committed_command_database(&second_path.0);
    let second_legacy =
        encode_index_entry_v1_fixture(&legacy_index_entry(&second)).expect("encode second V1 row");
    write_raw_index_entries(
        &second_path.0,
        &[(second.key().clone(), second_legacy.as_bytes().to_vec())],
    );
    let (_, second_catalog_outcome, second_outcome) = complete_startup_pass(
        RedbStore::open(&second_path.0).expect("open second migration session"),
    );
    let CatalogHistoryOutcome::MigrationRequired(second_context) = second_catalog_outcome else {
        panic!("second catalog session must require migration");
    };
    let StructuralOpenOutcome::MigrationRequired(second_port) = second_outcome else {
        panic!("second session must require migration");
    };

    let error = match CatalogIndexMigrationDriver::new(first_context, second_port) {
        Ok(_) => panic!("catalog context must reject another session's backend port"),
        Err(error) => error,
    };
    assert!(matches!(error, CatalogIndexMigrationDriveError::Catalog(_)));
    drop((first_port, second_context));
    for path in [&first_path.0, &second_path.0] {
        assert_eq!(
            RedbStore::open(path)
                .expect("dropped migration capability permits reopen")
                .probe_database_identity()
                .expect("probe after dropped migration capability"),
            DatabaseIdentityProbe::Existing(database_id())
        );
    }
}

#[test]
fn crash_before_index_migration_batch_restarts_from_v1_without_a_marker() {
    let path = TestDatabasePath::new("before-index-migration-batch");
    let (_, current) = prepare_committed_command_database(&path.0);
    let current_envelope = encode_index_entry_v2(&current).expect("encode current V2 row");
    let legacy_envelope =
        encode_index_entry_v1_fixture(&legacy_index_entry(&current)).expect("encode legacy V1 row");
    assert_eq!(
        write_raw_index_entries(
            &path.0,
            &[(current.key().clone(), legacy_envelope.as_bytes().to_vec(),)],
        ),
        vec![Some(current_envelope.as_bytes().to_vec())]
    );
    let control_state = read_migration_control_state(&path.0);

    run_crashing_child("before-index-migration-batch-commit", &path.0);

    let rows = read_raw_index_entries(&path.0);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, current.key().as_bytes());
    assert_eq!(
        rows[0].1,
        legacy_envelope.as_bytes(),
        "a precommit crash must leave the exact V1 envelope"
    );
    assert_eq!(
        read_migration_control_state(&path.0),
        control_state,
        "a migration crash must not create a table or metadata marker"
    );

    let controller = RedbTestController::observe_index_migration();
    let (migration_session, catalog_outcome, outcome) = complete_startup_pass(
        RedbStore::open_with_test_controller(&path.0, controller.clone())
            .expect("restart migration from structural step one"),
    );
    let CatalogHistoryOutcome::MigrationRequired(context) = catalog_outcome else {
        panic!("the exact V1 catalog row must require migration after restart");
    };
    let StructuralOpenOutcome::MigrationRequired(port) = outcome else {
        panic!("the exact V1 row must require migration after restart");
    };
    let (store, observation) = drive_index_migration(port, context, &controller);
    assert_eq!(observation.v1_rewrites, 1);
    assert_eq!(observation.v2_confirms, 0);
    drop(store);

    let (clean_session, catalog_outcome, outcome) = complete_startup_pass(
        RedbStore::open(&path.0).expect("fresh reopen after completed migration"),
    );
    assert!(matches!(catalog_outcome, CatalogHistoryOutcome::Ready(_)));
    assert_ne!(clean_session, migration_session);
    let StructuralOpenOutcome::Clean(opened) = outcome else {
        panic!("the fresh post-migration session must be V2-only and clean");
    };
    drop(opened);
    assert_eq!(
        read_raw_index_entries(&path.0),
        vec![(
            current.key().as_bytes().to_vec(),
            current_envelope.as_bytes().to_vec(),
        )]
    );
    assert_eq!(read_migration_control_state(&path.0), control_state);

    let (repeat_session, catalog_outcome, outcome) = complete_startup_pass(
        RedbStore::open(&path.0).expect("repeat clean post-migration reopen"),
    );
    assert!(matches!(catalog_outcome, CatalogHistoryOutcome::Ready(_)));
    assert_ne!(repeat_session, clean_session);
    assert!(matches!(outcome, StructuralOpenOutcome::Clean(_)));
}

#[test]
fn crash_after_index_migration_batch_restarts_from_step_one_and_observes_exact_v2() {
    let path = TestDatabasePath::new("after-index-migration-batch");
    let (_, current) = prepare_committed_command_database(&path.0);
    let current_envelope = encode_index_entry_v2(&current).expect("encode current V2 row");
    let legacy_envelope =
        encode_index_entry_v1_fixture(&legacy_index_entry(&current)).expect("encode legacy V1 row");
    assert_eq!(
        write_raw_index_entries(
            &path.0,
            &[(current.key().clone(), legacy_envelope.as_bytes().to_vec(),)],
        ),
        vec![Some(current_envelope.as_bytes().to_vec())]
    );
    let control_state = read_migration_control_state(&path.0);

    run_crashing_child("after-index-migration-batch-commit", &path.0);

    assert_eq!(
        read_raw_index_entries(&path.0),
        vec![(
            current.key().as_bytes().to_vec(),
            current_envelope.as_bytes().to_vec(),
        )],
        "the after-commit crash must expose the complete exact V2 replacement"
    );
    assert_eq!(
        read_migration_control_state(&path.0),
        control_state,
        "the committed batch must not create a table or metadata marker"
    );

    let (first_session, catalog_outcome, outcome) = complete_startup_pass(
        RedbStore::open(&path.0).expect("restart structural validation from step one"),
    );
    assert!(matches!(catalog_outcome, CatalogHistoryOutcome::Ready(_)));
    let StructuralOpenOutcome::Clean(opened) = outcome else {
        panic!("step-one restart must observe the committed V2 batch as clean");
    };
    drop(opened);

    let (fresh_session, catalog_outcome, outcome) = complete_startup_pass(
        RedbStore::open(&path.0).expect("fresh repeat reopen after uncertain migration commit"),
    );
    assert!(matches!(catalog_outcome, CatalogHistoryOutcome::Ready(_)));
    assert_ne!(fresh_session, first_session);
    let StructuralOpenOutcome::Clean(opened) = outcome else {
        panic!("the repeat post-migration session must remain clean");
    };
    drop(opened);
    assert_eq!(read_migration_control_state(&path.0), control_state);
}

#[test]
fn crash_before_initialization_commit_leaves_a_proven_empty_store() {
    let path = TestDatabasePath::new("before-initialization");
    run_crashing_child("before-initialization-commit", &path.0);

    let store = RedbStore::open(&path.0).expect("recover precommit crash");
    assert_eq!(
        store.probe_database_identity().expect("probe recovery"),
        DatabaseIdentityProbe::NeedsInitialization
    );
}

#[test]
fn crash_after_initialization_commit_preserves_the_durable_database_identity() {
    let path = TestDatabasePath::new("after-initialization");
    run_crashing_child("after-initialization-commit", &path.0);

    let store = RedbStore::open(&path.0).expect("recover postcommit crash");
    assert_eq!(
        store.probe_database_identity().expect("probe recovery"),
        DatabaseIdentityProbe::Existing(database_id())
    );
    assert_eq!(complete_structural_open(store).database_id(), database_id());
}

#[test]
fn sealed_command_audit_links_reject_reordered_staging_evidence_atomically() {
    let path = TestDatabasePath::new("reordered-command-audit-links");
    prepare_command_database(&path.0);
    let ports = open_operational(RedbStore::open(&path.0).expect("open command database"));
    let first = command_fixture_at(1);
    let second = command_fixture_at(2);
    let fixtures = [first.clone(), second.clone()];
    let reordered_audits = [second, first];

    let error = try_commit_command_group(&ports, &fixtures, &reordered_audits)
        .expect_err("audit links cannot be reordered relative to staged graphs");
    assert_eq!(
        error.kind(),
        riffdb_storage_api::StorageErrorKind::InvariantViolation
    );
    assert!(command_audit_phases(&ports).is_empty());
    for fixture in &fixtures {
        assert_precommit_command_state(&ports, fixture);
    }
}

#[test]
fn crash_before_fused_command_commit_preserves_complete_absence() {
    for (label, profile) in [
        ("standard", RedbCommitProfile::Standard),
        ("hardened", RedbCommitProfile::Hardened),
    ] {
        let path = TestDatabasePath::new(&format!("before-command-{label}"));
        prepare_command_database(&path.0);
        run_crashing_child_with_profile("before-command-batch-commit", &path.0, profile);

        let ports = open_operational(RedbStore::open(&path.0).expect("recover precommit crash"));
        assert_precommit_command_state(&ports, &command_fixture());
    }
}

#[test]
// req: REC-001, REC-002
fn crash_after_command_commit_preserves_the_complete_reciprocal_graph() {
    for (label, profile) in [
        ("standard", RedbCommitProfile::Standard),
        ("hardened", RedbCommitProfile::Hardened),
    ] {
        let path = TestDatabasePath::new(&format!("after-command-{label}"));
        prepare_command_database(&path.0);
        run_crashing_child_with_profile("after-command-batch-commit", &path.0, profile);

        let ports = open_operational(RedbStore::open(&path.0).expect("recover postcommit crash"));
        assert_postcommit_command_state(&ports, &command_fixture());
        drop(ports);

        let ports = open_operational(RedbStore::open(&path.0).expect("repeat postcommit recovery"));
        assert_postcommit_command_state(&ports, &command_fixture());
    }
}

#[test]
fn crash_before_or_after_unpublished_subgroup_recovers_the_predecessor_frontier() {
    for mode in [
        "before-deferred-command-commit",
        "after-deferred-command-commit",
    ] {
        let path = TestDatabasePath::new(mode);
        prepare_command_database(&path.0);
        run_crashing_child(mode, &path.0);

        let ports = open_operational(
            RedbStore::open(&path.0).expect("recover unfenced deferred subgroup crash"),
        );
        assert_precommit_command_state(&ports, &command_fixture());
    }
}

#[test]
fn crash_after_epoch_tail_preserves_the_complete_reciprocal_graph() {
    let path = TestDatabasePath::new("after-command-epoch-tail");
    prepare_command_database(&path.0);
    run_crashing_child("after-command-epoch-tail", &path.0);

    let ports =
        open_operational(RedbStore::open(&path.0).expect("recover known-durable epoch tail crash"));
    assert_postcommit_command_state(&ports, &command_fixture());
}

#[test]
fn serial_group_crash_before_commit_leaves_every_command_absent() {
    let path = TestDatabasePath::new("before-command-group");
    prepare_command_database(&path.0);
    run_crashing_child_with_profile(
        "before-command-group-commit",
        &path.0,
        RedbCommitProfile::Standard,
    );

    let ports = open_operational(RedbStore::open(&path.0).expect("recover group precommit crash"));
    for ordinal in 1..=riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS {
        let fixture = command_fixture_at(u64::try_from(ordinal).expect("bounded fixture ordinal"));
        assert_eq!(
            ports
                .lookup_admission(fixture.candidates.clone())
                .expect("lookup absent grouped identity"),
            AdmissionLookupResultV1::NotFound
        );
        assert_eq!(
            ports.read_entity(&fixture.target).expect("read entity"),
            None
        );
        assert_eq!(
            ports
                .read_commit(fixture.records.commit().commit_sequence())
                .expect("read absent grouped commit"),
            None
        );
    }
    assert!(command_audit_phases(&ports).is_empty());
}

#[test]
fn segmented_group_commits_and_reopens_without_a_failpoint() {
    let path = TestDatabasePath::new("segmented-command-group");
    prepare_command_database(&path.0);
    let ports = open_operational(RedbStore::open(&path.0).expect("open grouped database"));
    let fixtures = (1..=riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS)
        .map(|ordinal| command_fixture_at(u64::try_from(ordinal).expect("bounded ordinal")))
        .collect::<Vec<_>>();
    commit_command_group(&ports, &fixtures);
    drop(ports);
    let ports = open_operational(RedbStore::open(&path.0).expect("reopen grouped database"));
    for fixture in fixtures {
        assert_eq!(
            ports
                .read_commit(fixture.records.commit().commit_sequence())
                .expect("read grouped command"),
            Some(fixture.records.commit().clone())
        );
    }
}

/// ADR-0102 regression (WP-493): the *independently supplied* Command audit
/// link — the replay shape, where the appending transaction stages no command
/// and therefore carries no sealed move-only evidence — must resolve through
/// the canonical segment member.
///
/// The pre-segmentation decoder read `commits` at the exact
/// `encode_application_sequence_key(commit_sequence)` and decoded that row as a
/// single commit record. Under segmentation both halves of that are wrong: the
/// row is keyed by the segment's *first* sequence, so every non-first member
/// misses entirely, and the row a first member does find is a
/// `StoredCommandSegmentV1`, not a commit. This commits one segment holding
/// several commands and then replays an independent linked audit against every
/// member, so both the first-member decode and the non-first-member key miss
/// stay covered.
#[test]
fn independent_command_audit_links_resolve_every_member_of_one_segment() {
    let path = TestDatabasePath::new("independent-command-link-segment");
    prepare_command_database(&path.0);
    let ports = open_operational(RedbStore::open(&path.0).expect("open grouped database"));
    let fixtures = (1..=4_u64).map(command_fixture_at).collect::<Vec<_>>();
    commit_command_group(&ports, &fixtures);
    drop(ports);

    // Precondition: one physical row holds every command, so members 2..=4 have
    // no row at their own key and member 1's row is a segment, not a commit.
    let database = Database::open(&path.0).expect("open raw for segment shape");
    let read = database.begin_read().expect("raw read");
    let commits = read
        .open_table(TableDefinition::<&[u8], &[u8]>::new("commits"))
        .expect("commits table");
    assert_eq!(
        commits.len().expect("commits row count"),
        1,
        "the grouped commit must produce exactly one physical segment row"
    );
    drop(commits);
    drop(read);
    drop(database);

    let mut ports = open_operational(RedbStore::open(&path.0).expect("reopen for replay audits"));
    for (index, fixture) in fixtures.iter().enumerate() {
        let request = RequestId::from_bytes(uuid_bytes(
            0xa0_u8.wrapping_add(u8::try_from(index).expect("bounded fixture index")),
        ))
        .expect("replay request ID");
        let principal = catalog_principal();
        let started = ServiceAuditAppendIntentV1::new(
            request,
            Timestamp::new(1_700_000_010, 0).expect("replay started timestamp"),
            ServiceOperationV1::ExecuteCommand,
            ServiceAuditPhaseV1::Started,
            principal.clone(),
            ServiceIngressKindV1::Grpc,
            ServiceAuditTargetsV1::empty(),
            None,
            ServiceAuditLinkV1::None,
        )
        .expect("replay started audit");
        let terminal = ServiceAuditAppendIntentV1::new(
            request,
            Timestamp::new(1_700_000_011, 0).expect("replay terminal timestamp"),
            ServiceOperationV1::ExecuteCommand,
            ServiceAuditPhaseV1::Succeeded,
            principal,
            ServiceIngressKindV1::Grpc,
            ServiceAuditTargetsV1::empty(),
            None,
            ServiceAuditLinkV1::Command {
                commit_sequence: fixture.records.commit().commit_sequence(),
                provenance_id: fixture.records.provenance().provenance_id(),
            },
        )
        .expect("replay terminal audit");
        ports
            .append_service_audit_fused_pair(&started, &terminal)
            .unwrap_or_else(|error| {
                panic!(
                    "replaying the link for segment member {:?} must be admissible: {error:?}",
                    fixture.records.commit().commit_sequence()
                )
            });
    }

    // A link naming a sequence no retained segment covers stays a refused
    // append rather than a corruption signal.
    let principal = catalog_principal();
    let absent_request = RequestId::from_bytes(uuid_bytes(0xaf)).expect("absent-link request ID");
    let absent_started = ServiceAuditAppendIntentV1::new(
        absent_request,
        Timestamp::new(1_700_000_012, 0).expect("absent started timestamp"),
        ServiceOperationV1::ExecuteCommand,
        ServiceAuditPhaseV1::Started,
        principal.clone(),
        ServiceIngressKindV1::Grpc,
        ServiceAuditTargetsV1::empty(),
        None,
        ServiceAuditLinkV1::None,
    )
    .expect("absent-link started audit");
    let absent_terminal = ServiceAuditAppendIntentV1::new(
        absent_request,
        Timestamp::new(1_700_000_013, 0).expect("absent terminal timestamp"),
        ServiceOperationV1::ExecuteCommand,
        ServiceAuditPhaseV1::Succeeded,
        principal,
        ServiceIngressKindV1::Grpc,
        ServiceAuditTargetsV1::empty(),
        None,
        ServiceAuditLinkV1::Command {
            commit_sequence: CommitSequence::new(4_096).expect("uncommitted sequence"),
            provenance_id: fixtures[0].records.provenance().provenance_id(),
        },
    )
    .expect("absent-link terminal audit");
    let refused = ports
        .append_service_audit_fused_pair(&absent_started, &absent_terminal)
        .expect_err("a link past the retained segment is not admissible");
    assert_eq!(
        refused.kind(),
        riffdb_storage_api::StorageErrorKind::InvariantViolation,
        "an unprovable link is a refused append, not corrupt data"
    );

    drop(ports);
    let findings = collect_structural_findings(RedbStore::open(&path.0).expect("reopen replayed"));
    assert!(
        findings.is_empty(),
        "independently linked replay audits must reopen clean: {findings:?}"
    );
}

#[test]
fn retention_splits_a_segment_and_rechains_the_retained_suffix() {
    let path = TestDatabasePath::new("retention-split-command-segment");
    prepare_command_database(&path.0);
    let ports = open_operational(RedbStore::open(&path.0).expect("open grouped database"));
    let fixtures = (1..=riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS)
        .map(|ordinal| command_fixture_at(u64::try_from(ordinal).expect("bounded ordinal")))
        .collect::<Vec<_>>();
    commit_command_group(&ports, &fixtures);
    drop(ports);
    for ordinal in 1..=riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS {
        retention_deliver_outbox_status_raw(
            &path.0,
            u64::try_from(ordinal).expect("bounded ordinal"),
        );
    }
    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    maintenance
        .add_hold("split-cap", 64, "exercise partial segment retention")
        .expect("install split hold");
    maintenance
        .prune_to(32)
        .expect("split segment at watermark");

    let findings = collect_structural_findings(RedbStore::open(&path.0).expect("reopen split"));
    assert!(
        findings.is_empty(),
        "split segment must reopen clean: {findings:?}"
    );
    let ports = open_operational(RedbStore::open(&path.0).expect("open split database"));
    assert_eq!(
        ports
            .read_commit(CommitSequence::new(33).expect("retained sequence"))
            .expect("read retained command"),
        Some(fixtures[32].records.commit().clone())
    );
    assert_eq!(
        ports
            .read_commit(CommitSequence::new(32).expect("pruned sequence"))
            .expect_err("pruned command is typed")
            .kind(),
        riffdb_storage_api::StorageErrorKind::HistoryPruned
    );
}

#[test]
fn serial_group_crash_after_commit_preserves_every_complete_command() {
    let path = TestDatabasePath::new("after-command-group");
    prepare_command_database(&path.0);
    run_crashing_child_with_profile(
        "after-command-group-commit",
        &path.0,
        RedbCommitProfile::Standard,
    );

    let ports = open_operational(RedbStore::open(&path.0).expect("recover group postcommit crash"));
    for ordinal in 1..=riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS {
        let fixture = command_fixture_at(u64::try_from(ordinal).expect("bounded fixture ordinal"));
        let AdmissionLookupResultV1::Found(admission) = ports
            .lookup_admission(fixture.candidates.clone())
            .expect("lookup grouped identity")
        else {
            panic!("each grouped identity is terminal");
        };
        assert_eq!(
            *admission,
            riffdb_storage_api::StoredAdmissionStateV1::StoredOutcome(
                fixture.records.stored_outcome().clone()
            )
        );
        assert_eq!(
            ports
                .read_entity(&fixture.target)
                .expect("read grouped entity"),
            Some(fixture.records.entities()[0].post_image().clone())
        );
        assert_eq!(
            ports
                .read_commit(fixture.records.commit().commit_sequence())
                .expect("read grouped commit"),
            Some(fixture.records.commit().clone())
        );
    }
}

#[test]
fn segment_owned_event_route_requires_no_standalone_row() {
    let path = TestDatabasePath::new("segment-owned-event-route");
    let (fixture, _) = prepare_committed_command_database(&path.0);
    let event_id = fixture.records.events()[0].event_id();
    let partition_hash = hash_partition_key(fixture.pending.partition_key().as_bytes());
    let mut route_key = [0_u8; 44];
    route_key[..32].copy_from_slice(partition_hash.as_bytes());
    route_key[32..].copy_from_slice(&event_id.to_be_bytes());

    assert!(
        !retention_raw_row_present(&path.0, "event_routes", route_key.as_slice()),
        "the command segment, not a duplicate row, owns its event route"
    );
    let findings = collect_structural_findings(RedbStore::open(&path.0).expect("reopen database"));
    assert!(
        findings.is_empty(),
        "the segment manifest reconstructs the exact route: {findings:?}"
    );
}

fn complete_startup_observing_checkpoint(
    store: RedbStore,
) -> (
    OpenSessionId,
    CatalogHistoryOutcome,
    StructuralOpenOutcome<RedbDormantPorts, RedbStartupIndexMigrationPort>,
    bool,
    u64,
) {
    let digest_key = DigestKeyId::new(1).expect("digest key ID");
    let inputs = StartupValidationInputs::new(
        Timestamp::new(1_700_000_000, 0).expect("startup timestamp"),
        ReadableCapabilityDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("capability digest inventory"),
        ReadableIdempotencyDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("idempotency digest inventory"),
    );
    let mut session = store
        .begin_structural_evidence(inputs)
        .expect("begin exclusive structural evidence");
    let checkpoint_verified = session.checkpoint_verified();
    let sampled = session.sampled_window_rows_inspected();
    let database_id = session.database_id();
    let open_session_id = session.open_session_id();
    let limit = EvidencePageLimit::new(64).expect("page limit");

    let mut structural_cursor = StructuralEvidenceCursor::start(database_id, open_session_id);
    let structural_end = loop {
        match session
            .read_structural_evidence(structural_cursor, limit)
            .expect("read structural evidence")
        {
            StructuralEvidencePage::Page { findings, next, .. } => {
                assert!(
                    findings.is_empty(),
                    "a valid recovery fixture has no findings"
                );
                structural_cursor = next;
            }
            StructuralEvidencePage::ExactEnd(end) => break end,
        }
    };

    let (catalog_outcome, historical_end) = validate_catalog_history(&mut session)
        .expect("catalog validates the complete historical stream")
        .into_parts();
    let outcome = session
        .finish(structural_end, historical_end)
        .expect("finish structural evidence");
    (
        open_session_id,
        catalog_outcome,
        outcome,
        checkpoint_verified,
        sampled,
    )
}

#[test]
fn validated_prefix_checkpoint_roundtrip_second_open_uses_fast_path() {
    let path = TestDatabasePath::new("validated-prefix-roundtrip");
    let _ = prepare_committed_command_database(&path.0);
    // Write an S=head checkpoint after the committed command (prepare's open was pre-command).
    let _ = complete_startup_pass(RedbStore::open(&path.0).expect("seed S=head checkpoint"));

    // A subsequent open must verify the checkpoint and take the fast path.
    let (_, _, outcome, verified, sampled) =
        complete_startup_observing_checkpoint(RedbStore::open(&path.0).expect("checkpointed open"));
    assert!(
        matches!(outcome, StructuralOpenOutcome::Clean(_)),
        "checkpointed open must finish clean"
    );
    assert!(
        verified,
        "open after a clean finish must verify the written checkpoint"
    );
    assert!(
        sampled > 0,
        "verified checkpoint with S>0 must execute sampled windows (observed {sampled})"
    );
    drop(outcome);

    // Forced full path: remove checkpoint meta, open full, rewrite, reopen.
    {
        let database = Database::create(&path.0).expect("open");
        let txn = database.begin_write().expect("write");
        {
            let mut meta = txn.open_table(META).expect("meta");
            let _ = meta.remove("validated_prefix_checkpoint/v1");
        }
        txn.commit().expect("commit");
        drop(database);
    }
    let store = RedbStore::open(&path.0).expect("full open");
    let (_, _, outcome, verified_full, _) = complete_startup_observing_checkpoint(store);
    assert!(
        matches!(outcome, StructuralOpenOutcome::Clean(_)),
        "full validation open must finish clean"
    );
    assert!(
        !verified_full,
        "absent checkpoint must force full validation"
    );
    drop(outcome);
    let store = RedbStore::open(&path.0).expect("rewrite open");
    let (_, _, outcome, verified_again, _) = complete_startup_observing_checkpoint(store);
    assert!(matches!(outcome, StructuralOpenOutcome::Clean(_)));
    assert!(verified_again, "rewritten checkpoint must verify");
    drop(outcome);
}

#[test]
fn validated_prefix_checkpoint_binding_mismatch_falls_back_to_full_validation() {
    let path = TestDatabasePath::new("validated-prefix-binding-mismatch");
    let _ = prepare_committed_command_database(&path.0);
    let (_, _, seed_outcome) =
        complete_startup_pass(RedbStore::open(&path.0).expect("seed checkpoint"));
    drop(seed_outcome);

    // Corrupt the checkpoint self-hash by flipping a byte in the meta row.
    {
        let database = Database::create(&path.0).expect("open for doctor");
        let txn = database.begin_write().expect("write");
        {
            let mut meta = txn.open_table(META).expect("meta");
            let key = "validated_prefix_checkpoint/v1";
            let existing = meta
                .get(key)
                .expect("get")
                .expect("checkpoint present")
                .value()
                .to_vec();
            let mut doctored = existing;
            if let Some(last) = doctored.last_mut() {
                *last ^= 0xff;
            }
            meta.insert(key, doctored.as_slice()).expect("insert");
        }
        txn.commit().expect("commit doctor");
        drop(database);
    }

    let (_, _, outcome, verified, _) =
        complete_startup_observing_checkpoint(RedbStore::open(&path.0).expect("reopen"));
    assert!(
        !verified,
        "corrupted self-hash must ignore checkpoint (full validation)"
    );
    assert!(
        matches!(outcome, StructuralOpenOutcome::Clean(_)),
        "full validation must still open cleanly on otherwise-valid data"
    );
    drop(outcome);
}

#[test]
fn validated_prefix_checkpoint_prefix_count_mismatch_fails_closed() {
    use riffdb_storage_api::{
        StoredValidatedPrefixCheckpointV1, StoredValidatedPrefixCheckpointV2,
        proto_codec::{
            decode_validated_prefix_checkpoint_v2, encode_validated_prefix_checkpoint_v2,
        },
    };

    let path = TestDatabasePath::new("validated-prefix-count-mismatch");
    let _ = prepare_committed_command_database(&path.0);
    let _ = complete_startup_pass(RedbStore::open(&path.0).expect("write checkpoint"));

    // Under-report commits_count while leaving rows in place. Load-time
    // CountImpossible does not fire (recorded ≤ full), but at ExactEnd
    // table_len − walked_suffix ≠ recorded_prefix → fail closed.
    {
        let database = Database::create(&path.0).expect("open");
        let txn = database.begin_write().expect("write");
        {
            let mut meta = txn.open_table(META).expect("meta");
            let key = "validated_prefix_checkpoint/v1";
            let existing = meta
                .get(key)
                .expect("get")
                .expect("checkpoint present")
                .value()
                .to_vec();
            let original_v2 = decode_validated_prefix_checkpoint_v2(&existing)
                .expect("decode")
                .into_parts()
                .0;
            let original = original_v2.base();
            assert!(
                original.counts().commits_count >= 1,
                "fixture must record a non-empty commits prefix"
            );
            let mut counts = original.counts();
            counts.commits_count = 0;
            let doctored_base = StoredValidatedPrefixCheckpointV1::new(
                original.database_id(),
                original.history_incarnation(),
                original.registry_digest(),
                original.checkpoint_commit_sequence(),
                original.audit_sequence_bound(),
                counts,
                original.entity_chain_fingerprint(),
                original.retained(),
                original.previous_checkpoint_hash(),
                0,
            )
            .expect("rehash under-reported counts");
            let doctored = StoredValidatedPrefixCheckpointV2::new(
                doctored_base,
                original_v2.entity_counts(),
                original_v2.entity_transition_fingerprint(),
            )
            .expect("rehash delete-aware checkpoint");
            let encoded = encode_validated_prefix_checkpoint_v2(&doctored).expect("encode");
            meta.insert(key, encoded.as_bytes()).expect("insert");
        }
        txn.commit().expect("commit doctor");
    }

    let store = RedbStore::open(&path.0).expect("open doctored DB");
    let digest_key = DigestKeyId::new(1).expect("digest key ID");
    let inputs = StartupValidationInputs::new(
        Timestamp::new(1_700_000_000, 0).expect("startup timestamp"),
        ReadableCapabilityDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("capability digest inventory"),
        ReadableIdempotencyDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("idempotency digest inventory"),
    );
    let mut session = store
        .begin_structural_evidence(inputs)
        .expect("begin accepts bindable checkpoint");
    assert!(
        session.checkpoint_verified(),
        "under-reported counts still bind at load; mismatch is walk-end verified"
    );
    let mut cursor =
        StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
    let limit = EvidencePageLimit::new(64).expect("page limit");
    let mut failed_closed = false;
    loop {
        match session.read_structural_evidence(cursor, limit) {
            Ok(StructuralEvidencePage::Page { findings, next, .. }) => {
                if !findings.is_empty() {
                    failed_closed = true;
                    break;
                }
                cursor = next;
            }
            Ok(StructuralEvidencePage::ExactEnd(_)) => break,
            Err(_) => {
                failed_closed = true;
                break;
            }
        }
    }
    assert!(
        failed_closed,
        "under-reported below-S commits_count must fail closed at walk end"
    );
}

#[test]
fn crash_before_validated_prefix_checkpoint_commit_leaves_no_meta() {
    let path = TestDatabasePath::new("before-validated-prefix-checkpoint");
    let _ = prepare_committed_command_database(&path.0);
    // Strip any checkpoint from prior opens so the crash targets a first write.
    {
        let database = Database::create(&path.0).expect("open");
        let txn = database.begin_write().expect("write");
        {
            let mut meta = txn.open_table(META).expect("meta");
            let _ = meta.remove("validated_prefix_checkpoint/v1");
        }
        txn.commit().expect("commit");
    }
    run_crashing_child("before-validated-prefix-checkpoint-commit", &path.0);

    // After abort-before-commit, meta must still be absent.
    {
        let database = Database::create(&path.0).expect("open");
        let txn = database.begin_read().expect("read");
        let meta = txn.open_table(META).expect("meta");
        assert!(
            meta.get("validated_prefix_checkpoint/v1")
                .expect("get")
                .is_none(),
            "abort before checkpoint commit must leave no durable checkpoint"
        );
    }

    // Reopen: full validation path (no verified checkpoint at begin).
    let store = RedbStore::open(&path.0).expect("reopen after before-checkpoint crash");
    let (_, _, outcome, verified, _) = complete_startup_observing_checkpoint(store);
    assert!(
        matches!(outcome, StructuralOpenOutcome::Clean(_)),
        "recovery after pre-checkpoint crash must open clean"
    );
    assert!(
        !verified,
        "first reopen after pre-checkpoint crash must not verify a checkpoint"
    );
}

#[test]
fn crash_after_validated_prefix_checkpoint_commit_reopens_fast_path() {
    let path = TestDatabasePath::new("after-validated-prefix-checkpoint");
    let _ = prepare_committed_command_database(&path.0);
    run_crashing_child("after-validated-prefix-checkpoint-commit", &path.0);

    let store = RedbStore::open(&path.0).expect("reopen after after-checkpoint crash");
    let (_, _, outcome, verified, _) = complete_startup_observing_checkpoint(store);
    assert!(
        matches!(outcome, StructuralOpenOutcome::Clean(_)),
        "recovery after post-checkpoint crash must open clean"
    );
    assert!(
        verified,
        "checkpoint committed before crash must be verified on reopen"
    );
}

include!("wp774_graceful_close.rs");

#[test]
// req: PERF-014
fn perf_014_unclean_recovery_vs_clean_startup() {
    // Equivalent seeding: BOTH databases carry the identical committed state
    // (initialization + catalog activation, checkpoint from the preparation
    // pass). The unclean copy additionally suffers a real kill mid-CommandBatch
    // (child aborts before the engine commit), which is invisible to committed
    // state by engine atomicity — the two measured drains validate the same
    // rows, so the ratio isolates redb repair + reopen overhead.
    let unclean_path = TestDatabasePath::new("perf-014-unclean");
    prepare_command_database(&unclean_path.0);
    let clean_path = TestDatabasePath::new("perf-014-clean");
    prepare_command_database(&clean_path.0);
    run_crashing_child("before-command-batch-commit", &unclean_path.0);

    // Sentinel-reset the process-global repair observation so "changed" proves
    // the callback fired for THIS open (0% progress is distinguishable from
    // never-fired). Parallel tests in this process could also move it; the
    // sentinel eliminates staleness from earlier repairs, not concurrency.
    riffdb_storage_redb::reset_last_repair_progress_for_tests();
    let unclean_start = std::time::Instant::now();
    let unclean_store = RedbStore::open(&unclean_path.0).expect("open unclean");
    let repair_bps = riffdb_storage_redb::last_repair_progress_basis_points();
    assert_ne!(
        repair_bps,
        riffdb_storage_redb::REPAIR_PROGRESS_SENTINEL,
        "reopen after an aborted mid-batch write must invoke the redb repair callback"
    );
    let _ = complete_startup_pass(unclean_store);
    let unclean_ns = unclean_start.elapsed().as_nanos();

    let clean_start = std::time::Instant::now();
    let _ = complete_startup_pass(RedbStore::open(&clean_path.0).expect("clean reopen"));
    let clean_ns = clean_start.elapsed().as_nanos();

    let ratio = if clean_ns == 0 {
        0.0
    } else {
        unclean_ns as f64 / clean_ns as f64
    };
    println!(
        "{{\"schema\":\"riffdb.perf-014/v1\",\"repair_progress_bps\":{repair_bps},\"unclean_ns\":{unclean_ns},\"clean_ns\":{clean_ns},\"ratio\":{ratio}}}"
    );
    // Honest report only — do not reintroduce PERF-014 gate unless ratio ≤3× reliably.
    let _ = ratio;
}

#[test]
fn validated_prefix_checkpoint_entity_fingerprint_mismatch_falls_back() {
    use riffdb_storage_api::{
        EntityChainFingerprint, StoredValidatedPrefixCheckpointV1,
        StoredValidatedPrefixCheckpointV2,
        proto_codec::{
            decode_validated_prefix_checkpoint_v2, encode_validated_prefix_checkpoint_v2,
        },
    };

    let path = TestDatabasePath::new("validated-prefix-entity-fp");
    let _ = prepare_committed_command_database(&path.0);
    let _ = complete_startup_pass(RedbStore::open(&path.0).expect("write checkpoint"));

    // Rewrite checkpoint with a wrong entity-chain fingerprint (self-hash recomputed so
    // the envelope is valid). Reconstruct-at-S must disagree → ignore → full validation.
    {
        let database = Database::create(&path.0).expect("open");
        let txn = database.begin_write().expect("write");
        {
            let mut meta = txn.open_table(META).expect("meta");
            let key = "validated_prefix_checkpoint/v1";
            let existing = meta
                .get(key)
                .expect("get")
                .expect("checkpoint present")
                .value()
                .to_vec();
            let original_v2 = decode_validated_prefix_checkpoint_v2(&existing)
                .expect("decode checkpoint")
                .into_parts()
                .0;
            let original = original_v2.base();
            let wrong_fp = EntityChainFingerprint::from_bytes([0xab; 32]);
            let doctored_base = StoredValidatedPrefixCheckpointV1::new(
                original.database_id(),
                original.history_incarnation(),
                original.registry_digest(),
                original.checkpoint_commit_sequence(),
                original.audit_sequence_bound(),
                original.counts(),
                wrong_fp,
                original.retained(),
                original.previous_checkpoint_hash(),
                0,
            )
            .expect("rehash doctored checkpoint");
            assert_ne!(
                doctored_base.entity_chain_fingerprint(),
                original.entity_chain_fingerprint()
            );
            let doctored = StoredValidatedPrefixCheckpointV2::new(
                doctored_base,
                original_v2.entity_counts(),
                original_v2.entity_transition_fingerprint(),
            )
            .expect("rehash delete-aware checkpoint");
            let encoded =
                encode_validated_prefix_checkpoint_v2(&doctored).expect("encode doctored");
            meta.insert(key, encoded.as_bytes()).expect("insert");
        }
        txn.commit().expect("commit doctor");
    }

    let store = RedbStore::open(&path.0).expect("open");
    let (_, _, outcome, verified, _) = complete_startup_observing_checkpoint(store);
    assert!(
        !verified,
        "mismatched entity-chain fingerprint must ignore checkpoint (full validation)"
    );
    assert!(
        matches!(outcome, StructuralOpenOutcome::Clean(_)),
        "full validation of otherwise-valid data must open clean"
    );
}

#[test]
fn delete_aware_checkpoint_rejects_each_substituted_head_summary() {
    use riffdb_storage_api::{
        EntityTransitionFingerprint, StoredValidatedPrefixCheckpointV2,
        ValidatedPrefixEntityTransitionCounts,
        proto_codec::{
            decode_validated_prefix_checkpoint_v2, encode_validated_prefix_checkpoint_v2,
        },
    };

    #[derive(Clone, Copy)]
    enum Substitution {
        LiveCount,
        DeletedCount,
        TransitionCount,
        Fingerprint,
    }

    for (name, substitution) in [
        ("live-count", Substitution::LiveCount),
        ("deleted-count", Substitution::DeletedCount),
        ("transition-count", Substitution::TransitionCount),
        ("fingerprint", Substitution::Fingerprint),
    ] {
        let path = TestDatabasePath::new(&format!("v2-checkpoint-{name}"));
        let _ = prepare_committed_command_database(&path.0);
        let _ = complete_startup_pass(RedbStore::open(&path.0).expect("write V2 checkpoint"));
        {
            let database = Database::create(&path.0).expect("open checkpoint fixture");
            let txn = database.begin_write().expect("begin checkpoint mutation");
            {
                let mut meta = txn.open_table(META).expect("open meta");
                let existing = meta
                    .get(CHECKPOINT_META_KEY)
                    .expect("read checkpoint")
                    .expect("checkpoint present")
                    .value()
                    .to_vec();
                let original = decode_validated_prefix_checkpoint_v2(&existing)
                    .expect("decode V2 checkpoint")
                    .into_parts()
                    .0;
                let mut counts = original.entity_counts();
                let mut fingerprint = original.entity_transition_fingerprint();
                match substitution {
                    Substitution::LiveCount => {
                        counts = ValidatedPrefixEntityTransitionCounts {
                            live_entity_count: 0,
                            deleted_entity_count: 0,
                            entity_transition_count: counts.entity_transition_count,
                        };
                    }
                    Substitution::DeletedCount => {
                        counts = ValidatedPrefixEntityTransitionCounts {
                            live_entity_count: 0,
                            deleted_entity_count: 1,
                            entity_transition_count: counts.entity_transition_count,
                        };
                    }
                    Substitution::TransitionCount => {
                        counts.entity_transition_count = counts
                            .entity_transition_count
                            .checked_add(1)
                            .expect("bounded transition count");
                    }
                    Substitution::Fingerprint => {
                        fingerprint = EntityTransitionFingerprint::from_bytes([0xa5; 32]);
                    }
                }
                let doctored = StoredValidatedPrefixCheckpointV2::new(
                    original.base().clone(),
                    counts,
                    fingerprint,
                )
                .expect("self-consistent but state-inexact V2 checkpoint");
                let encoded =
                    encode_validated_prefix_checkpoint_v2(&doctored).expect("encode checkpoint");
                meta.insert(CHECKPOINT_META_KEY, encoded.as_bytes())
                    .expect("replace checkpoint");
            }
            txn.commit().expect("commit checkpoint mutation");
        }

        let (_, _, outcome, verified, _) = complete_startup_observing_checkpoint(
            RedbStore::open(&path.0).expect("open substituted checkpoint"),
        );
        assert!(!verified, "{name} substitution cannot use the fast path");
        assert!(
            matches!(outcome, StructuralOpenOutcome::Clean(_)),
            "{name} substitution must fall back to a clean full validation"
        );
    }
}

#[test]
fn validated_prefix_checkpoint_incarnation_mismatch_falls_back() {
    let path = TestDatabasePath::new("validated-prefix-incarnation");
    let _ = prepare_committed_command_database(&path.0);
    let _ = complete_startup_pass(RedbStore::open(&path.0).expect("write checkpoint"));

    // Rewrite history_incarnation without rewriting the checkpoint binding.
    {
        let database = Database::create(&path.0).expect("open");
        let txn = database.begin_write().expect("write");
        {
            let mut meta = txn.open_table(META).expect("meta");
            let encoded = riffdb_storage_api::proto_codec::encode_history_incarnation_v1(2)
                .expect("encode incarnation 2");
            meta.insert("history_incarnation/v1", encoded.as_bytes())
                .expect("stamp incarnation");
        }
        txn.commit().expect("commit");
    }

    let store = RedbStore::open(&path.0).expect("open");
    let (_, _, outcome, verified, _) = complete_startup_observing_checkpoint(store);
    assert!(
        !verified,
        "incarnation mismatch must ignore checkpoint and full-validate"
    );
    assert!(
        matches!(outcome, StructuralOpenOutcome::Clean(_)),
        "full validation on incarnation-stamped-but-otherwise-valid DB must open clean"
    );
}

#[test]
fn pre_validated_prefix_registry_digest_migrates_on_open() {
    let path = TestDatabasePath::new("pre-validated-prefix-migrate");
    let mut store = RedbStore::open(&path.0).expect("open");
    store
        .initialize_database(database_id())
        .expect("initialize");
    drop(store);

    // Downgrade registry digest to PRE_VALIDATED (44-schema frozen digest).
    const PRE: [u8; 32] = [
        0xe1, 0x59, 0x2f, 0xba, 0x8c, 0x33, 0x8a, 0xee, 0x4e, 0xd7, 0x17, 0x8b, 0x6a, 0x09, 0xbb,
        0xcf, 0x88, 0x26, 0x7e, 0x5c, 0xd4, 0x33, 0xfa, 0x23, 0x31, 0x19, 0x9c, 0xaa, 0x3c, 0xd2,
        0xe8, 0xcd,
    ];
    {
        let database = Database::create(&path.0).expect("open");
        let txn = database.begin_write().expect("write");
        {
            let mut meta = txn.open_table(META).expect("meta");
            let encoded = encode_record_registry_v2(riffdb_types::SchemaHash::from_bytes(PRE))
                .expect("encode pre digest");
            meta.insert("record_registry/v2", encoded.as_bytes())
                .expect("insert");
        }
        txn.commit().expect("commit");
    }

    // Open must migrate PRE_VALIDATED → current without error.
    let store = RedbStore::open(&path.0).expect("migrate from PRE_VALIDATED");
    let _ = complete_startup_pass(store);
}

// ===== RT-A fix round: count-guard, write-gate, binding, and seeded-chain coverage =====

const ENTITIES_RAW: TableDefinition<&[u8], &[u8]> = TableDefinition::new("entities");
const OUTBOX_STATUS_RAW: TableDefinition<&[u8], &[u8]> = TableDefinition::new("outbox_status");

const CHECKPOINT_META_KEY: &str = "validated_prefix_checkpoint/v1";

/// Removes any stored validated-prefix checkpoint via a raw engine transaction.
fn strip_checkpoint_meta(path: &Path) {
    let database = Database::create(path).expect("open for checkpoint strip");
    let txn = database.begin_write().expect("begin checkpoint strip");
    {
        let mut meta = txn.open_table(META).expect("open meta");
        let _ = meta.remove(CHECKPOINT_META_KEY).expect("remove checkpoint");
    }
    txn.commit().expect("commit checkpoint strip");
}

/// Decodes the stored checkpoint, applies `mutate`, re-hashes, and stores it back.
fn rewrite_checkpoint(
    path: &Path,
    mutate: impl FnOnce(
        &riffdb_storage_api::StoredValidatedPrefixCheckpointV1,
    ) -> riffdb_storage_api::StoredValidatedPrefixCheckpointV1,
) {
    use riffdb_storage_api::{
        StoredValidatedPrefixCheckpointV2,
        proto_codec::{
            decode_validated_prefix_checkpoint_v2, encode_validated_prefix_checkpoint_v2,
        },
    };
    let database = Database::create(path).expect("open for checkpoint rewrite");
    let txn = database.begin_write().expect("begin checkpoint rewrite");
    {
        let mut meta = txn.open_table(META).expect("open meta");
        let existing = meta
            .get(CHECKPOINT_META_KEY)
            .expect("get checkpoint")
            .expect("checkpoint present")
            .value()
            .to_vec();
        let original = decode_validated_prefix_checkpoint_v2(&existing)
            .expect("decode checkpoint")
            .into_parts()
            .0;
        let doctored_base = mutate(original.base());
        let doctored = StoredValidatedPrefixCheckpointV2::new(
            doctored_base,
            original.entity_counts(),
            original.entity_transition_fingerprint(),
        )
        .expect("rehash delete-aware checkpoint");
        let encoded = encode_validated_prefix_checkpoint_v2(&doctored).expect("encode doctored");
        meta.insert(CHECKPOINT_META_KEY, encoded.as_bytes())
            .expect("insert doctored");
    }
    txn.commit().expect("commit checkpoint rewrite");
}

fn checkpoint_meta_present(path: &Path) -> bool {
    let database = Database::create(path).expect("open for checkpoint probe");
    let txn = database.begin_read().expect("begin checkpoint probe");
    let meta = txn.open_table(META).expect("open meta");
    meta.get(CHECKPOINT_META_KEY).expect("get").is_some()
}

fn startup_path_observation(path: &Path) -> (bool, bool) {
    let store = RedbStore::open(path).expect("open for startup path observation");
    let inputs = StartupValidationInputs::new(
        Timestamp::new(1_700_000_000, 0).expect("startup timestamp"),
        ReadableCapabilityDigestInventory::new(vec![ReadableDigestKey::v1(
            DigestKeyId::new(1).expect("digest key"),
        )])
        .expect("capability digest inventory"),
        ReadableIdempotencyDigestInventory::new(vec![ReadableDigestKey::v1(
            DigestKeyId::new(1).expect("digest key"),
        )])
        .expect("idempotency digest inventory"),
    );
    let session = store
        .begin_structural_evidence(inputs)
        .expect("begin startup observation");
    (
        session.clean_close_fast_path(),
        session.checkpoint_verified(),
    )
}

/// Deletes the first row of one raw byte-keyed table.
fn delete_first_raw_row(path: &Path, table: TableDefinition<&[u8], &[u8]>) {
    let database = Database::create(path).expect("open for row deletion");
    let txn = database.begin_write().expect("begin row deletion");
    {
        let mut open = txn.open_table(table).expect("open table");
        let victim = {
            let mut iter = open.iter().expect("iterate table");
            iter.next()
                .expect("table must have a row")
                .expect("read row")
                .0
                .value()
                .to_vec()
        };
        assert!(
            open.remove(victim.as_slice())
                .expect("remove row")
                .is_some(),
            "victim row must exist"
        );
    }
    txn.commit().expect("commit row deletion");
}

/// Full structural + historical drain that tolerates findings and errors.
struct TolerantDrain {
    verified: bool,
    ignored_reason: Option<&'static str>,
    findings: Vec<StructuralFinding>,
    structural_error: bool,
    finish_error: bool,
    outcome: Option<StructuralOpenOutcome<RedbDormantPorts, RedbStartupIndexMigrationPort>>,
}

impl TolerantDrain {
    fn refused(&self) -> bool {
        self.structural_error || self.finish_error
    }

    fn authoritative_finding(&self) -> bool {
        self.findings
            .iter()
            .any(|finding| finding.scope() == StructuralFindingScope::Authoritative)
    }
}

fn drain_tolerating_findings(store: RedbStore) -> TolerantDrain {
    let digest_key = DigestKeyId::new(1).expect("digest key ID");
    let inputs = StartupValidationInputs::new(
        Timestamp::new(1_700_000_000, 0).expect("startup timestamp"),
        ReadableCapabilityDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("capability digest inventory"),
        ReadableIdempotencyDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("idempotency digest inventory"),
    );
    let mut session = store
        .begin_structural_evidence(inputs)
        .expect("begin structural evidence");
    let verified = session.checkpoint_verified();
    let ignored_reason = session.checkpoint_ignored_reason();
    let mut report = TolerantDrain {
        verified,
        ignored_reason,
        findings: Vec::new(),
        structural_error: false,
        finish_error: false,
        outcome: None,
    };
    let limit = EvidencePageLimit::new(64).expect("page limit");
    let mut cursor =
        StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
    let structural_end = loop {
        match session.read_structural_evidence(cursor, limit) {
            Ok(StructuralEvidencePage::Page { findings, next, .. }) => {
                report.findings.extend(findings);
                cursor = next;
            }
            Ok(StructuralEvidencePage::ExactEnd(end)) => break end,
            Err(_) => {
                report.structural_error = true;
                return report;
            }
        }
    };
    let Ok(historical) = validate_catalog_history(&mut session) else {
        report.finish_error = true;
        return report;
    };
    let (_, historical_end) = historical.into_parts();
    match session.finish(structural_end, historical_end) {
        Ok(outcome) => report.outcome = Some(outcome),
        Err(_) => report.finish_error = true,
    }
    report
}

/// Seeds one committed-command database with an S = head checkpoint (the
/// preparation passes leave a stale S = 0 checkpoint behind).
fn prepare_checkpointed_command_database(path: &Path) {
    let _ = prepare_committed_command_database(path);
    let _ = complete_startup_pass(RedbStore::open(path).expect("seed S=head checkpoint"));
}

/// ADR-0165: the locator tables carry rows and the ADR-0085-counted tables stay
/// physically empty.
///
/// The second half is the invariant that rejected the obvious placement. If a
/// locator ever lands in `idempotency`, `provenance` or `audit_by_request`,
/// ADR-0085's O(1) checkpoint counts silently start counting it as a terminal
/// outcome or an audit.
#[test]
fn durable_locators_are_written_without_populating_the_counted_tables() {
    let path = TestDatabasePath::new("locators-written-counted-empty");
    let _ = prepare_committed_command_database(&path.0);
    assert!(retention_raw_table_has_rows(
        &path.0,
        "idempotency_locators"
    ));
    assert!(retention_raw_table_has_rows(&path.0, "provenance_locators"));
    assert!(retention_raw_table_has_rows(
        &path.0,
        "audit_by_request_locators"
    ));
    assert!(!retention_raw_table_has_rows(&path.0, "idempotency"));
    assert!(!retention_raw_table_has_rows(&path.0, "provenance"));
    assert!(!retention_raw_table_has_rows(&path.0, "audit_by_request"));
    let findings = collect_structural_findings(RedbStore::open(&path.0).expect("reopen"));
    assert!(
        findings.is_empty(),
        "locator rows must validate: {findings:?}"
    );
}

/// ADR-0165's central guarantee: a durably committed command is recognised as
/// already admitted even with the transient population index dormant.
///
/// Before the locator existed this returned "never admitted", so a retry
/// re-executed the command. The bounded clean-close start is what makes the
/// index dormant, which is exactly the state ADR-0156 readiness leaves it in.
#[test]
// req: REC-004
fn a_committed_command_is_still_recognised_with_the_population_index_dormant() {
    let path = TestDatabasePath::new("locator-admission-dormant");
    let (fixture, _) = prepare_committed_command_database(&path.0);

    // Certify a clean close so the next open takes the bounded path.
    let ports = open_operational(RedbStore::open(&path.0).expect("reopen to certify"));
    ports
        .write_clean_close_lifecycle()
        .expect("write clean-close certificate");
    drop(ports);

    let ports = open_operational(RedbStore::open(&path.0).expect("bounded reopen"));
    assert!(
        ports.clean_close_fast_startup(),
        "the certified database must take the bounded path"
    );
    assert_eq!(
        ports.transient_index_rebuilds(),
        0,
        "the bounded path must leave the population caches cold"
    );

    // Re-admit the same idempotency identity. Proceeding would mean the writer
    // believes this command was never admitted.
    let candidate = ports
        .begin_empty_batch()
        .expect("begin batch")
        .begin_candidate(Box::new(fixture.intent.clone()))
        .expect("begin candidate");
    let outcome = candidate.recheck_admission().expect("recheck admission");
    assert!(
        !matches!(outcome, CandidateAdmissionResult::Proceed(_)),
        "a durably committed command must not be admitted again with the index dormant"
    );
    assert_eq!(
        ports.transient_index_rebuilds(),
        0,
        "recognising it must not have required rebuilding the population index"
    );
}

/// A write on a bounded (index-dormant) start must validate the administration
/// tail against the command-owned audit record the AUDIT row locates.
///
/// The AUDIT row for a command-owned audit holds a `CommandAuditLocatorV1`, not
/// the record. The write-side decode arm used to map that locator to `None`
/// because resolving it had historically needed the transient index, and the
/// derived half is `None` on a bounded start, so the fail-closed `(None, None)`
/// join declared an intact database corrupt. Absence manufactured from a record
/// that is present -- the same class as the outcome and provenance defects, on
/// the write path.
#[test]
fn a_write_after_a_bounded_start_validates_the_command_owned_audit_tail() {
    let path = TestDatabasePath::new("locator-audit-tail-bounded");
    let _ = prepare_committed_command_database(&path.0);

    let ports = open_operational(RedbStore::open(&path.0).expect("reopen to certify"));
    ports
        .write_clean_close_lifecycle()
        .expect("write clean-close certificate");
    drop(ports);

    let ports = open_operational(RedbStore::open(&path.0).expect("bounded reopen"));
    assert!(
        ports.clean_close_fast_startup(),
        "the certified database must take the bounded path"
    );
    assert_eq!(ports.transient_index_rebuilds(), 0);

    // Committing a second command validates the administration tail, which
    // reads the AUDIT row written for the first command's audits.
    let second = command_fixture_at(2);
    let transition = command_audit_transition(&second);
    try_commit_command_fixture_with_audit(&ports, &second, transition)
        .expect("a bounded start must resolve the command-owned audit tail, not report corruption");
}

/// A locator that does not decode fails closed, never absent.
#[test]
fn an_undecodable_idempotency_locator_fails_closed() {
    let path = TestDatabasePath::new("locator-undecodable");
    let (fixture, _) = prepare_committed_command_database(&path.0);
    overwrite_first_row(&path.0, "idempotency_locators", &[0xFF, 0xFF, 0xFF, 0xFF]);
    assert!(
        readmission_is_corrupt(&path.0, &fixture),
        "an undecodable locator must be CorruptData, never absence"
    );
}

/// A locator naming a segment that does not contain the key fails closed.
#[test]
fn an_idempotency_locator_naming_the_wrong_segment_fails_closed() {
    let path = TestDatabasePath::new("locator-wrong-segment");
    let (fixture, _) = prepare_committed_command_database(&path.0);
    // Sequence 9_999 has no segment at all, so nothing owns the key.
    let encoded = riffdb_storage_api::encode_command_locator_v1(
        riffdb_storage_api::StoredCommandLocatorV1::new(
            CommitSequence::new(9_999).expect("absent sequence"),
        ),
    )
    .expect("encode a locator for an absent segment");
    overwrite_first_row(&path.0, "idempotency_locators", encoded.as_bytes());
    assert!(
        readmission_is_corrupt(&path.0, &fixture),
        "a locator naming a segment without the key must be CorruptData, never absence"
    );
}

// req: OUT-001, OUT-002, TXN-042
#[test]
fn fresh_locator_miss_preserves_prior_identity_and_rejects_malformed_locators() {
    let prior_path = TestDatabasePath::new("fresh-locator-prior-identity");
    let (first, second) = prepare_two_command_database(&prior_path.0);
    let ports = open_operational(RedbStore::open(&prior_path.0).expect("reopen two-command store"));
    ports
        .write_clean_close_lifecycle()
        .expect("certify bounded locator-only reopen");
    drop(ports);
    let ports = open_operational(RedbStore::open(&prior_path.0).expect("bounded reopen"));
    assert!(ports.clean_close_fast_startup());
    assert_eq!(ports.transient_index_rebuilds(), 0);
    for fixture in [&first, &second] {
        let AdmissionLookupResultV1::Found(state) = ports
            .lookup_admission(fixture.candidates.clone())
            .expect("every prior locator remains readable")
        else {
            panic!("every prior identity must remain terminal");
        };
        assert_eq!(
            *state,
            StoredAdmissionStateV1::StoredOutcome(fixture.records.stored_outcome().clone())
        );
    }

    let malformed_path = TestDatabasePath::new("fresh-locator-malformed-row");
    let (malformed, _) = prepare_committed_command_database(&malformed_path.0);
    overwrite_first_row(
        &malformed_path.0,
        "idempotency_locators",
        &[0xff, 0xff, 0xff, 0xff],
    );
    assert!(readmission_is_corrupt(&malformed_path.0, &malformed));

    let wrong_capsule_path = TestDatabasePath::new("fresh-locator-wrong-capsule-identity");
    let (expected, _) = prepare_two_command_database(&wrong_capsule_path.0);
    let wrong_locator = riffdb_storage_api::encode_command_locator_v1(
        riffdb_storage_api::StoredCommandLocatorV1::new(
            CommitSequence::new(2).expect("existing wrong command sequence"),
        ),
    )
    .expect("encode locator to a real but differently identified command");
    overwrite_exact_row(
        &wrong_capsule_path.0,
        "idempotency_locators",
        expected
            .pending
            .identity()
            .storage_key()
            .expect("canonical identity key")
            .as_bytes(),
        wrong_locator.as_bytes(),
    );
    assert!(
        readmission_is_corrupt(&wrong_capsule_path.0, &expected),
        "a real capsule with the wrong complete identity must be corruption"
    );
}

// req: OUT-001, OUT-002, TXN-042
#[test]
fn fresh_locator_write_miss_retains_current_semantics_and_gates_only_coverage() {
    let path = TestDatabasePath::new("fresh-locator-write-miss-result-algebra");
    prepare_command_database(&path.0);
    let first = two_phase_command_fixture_at(1);
    let ports = open_operational(RedbStore::open(&path.0).expect("open empty command store"));
    let request =
        AdmissionRequestV1::new(first.candidates, &first.context).expect("first admission request");
    assert_eq!(
        ports.admit_or_resolve(request).expect("first admission"),
        AdmissionResultV1::Created(first.pending)
    );
    drop(ports);

    let second = two_phase_command_fixture_at(2);
    let ports = open_operational(RedbStore::open(&path.0).expect("reopen nonempty command store"));
    assert_eq!(
        ports
            .lookup_admission(second.candidates.clone())
            .expect("operational novel-key miss"),
        AdmissionLookupResultV1::NotFound
    );
    let request = AdmissionRequestV1::new(second.candidates.clone(), &second.context)
        .expect("second admission request");
    assert_eq!(
        ports
            .admit_or_resolve(request)
            .expect("nonempty authority disables only the optimization"),
        AdmissionResultV1::Created(second.pending.clone())
    );
    let candidate = ports
        .begin_empty_batch()
        .expect("begin transaction-adjacent revalidation")
        .begin_candidate(Box::new(second.intent))
        .expect("begin admitted candidate");
    assert!(matches!(
        candidate
            .recheck_admission()
            .expect("transaction-adjacent miss retains Proceed"),
        CandidateAdmissionResult::Proceed(_)
    ));
}

/// Replaces the first row's value in a raw table, leaving its key intact.
fn overwrite_first_row(path: &Path, table_name: &str, value: &[u8]) {
    let database = Database::open(path).expect("open raw");
    let definition = TableDefinition::<&[u8], &[u8]>::new(table_name);
    let write = database.begin_write().expect("begin raw write");
    {
        let read = database.begin_read().expect("raw read");
        let key = read
            .open_table(definition)
            .expect("table")
            .first()
            .expect("first")
            .map(|(key, _)| key.value().to_vec())
            .expect("a locator row to damage");
        let mut table = write.open_table(definition).expect("raw table");
        table.insert(key.as_slice(), value).expect("overwrite");
    }
    write.commit().expect("commit raw damage");
}

fn overwrite_exact_row(path: &Path, table_name: &str, key: &[u8], value: &[u8]) {
    let database = Database::open(path).expect("open raw");
    let definition = TableDefinition::<&[u8], &[u8]>::new(table_name);
    let write = database.begin_write().expect("begin raw write");
    {
        let mut table = write.open_table(definition).expect("raw table");
        assert!(
            table
                .insert(key, value)
                .expect("overwrite exact row")
                .is_some(),
            "the exact locator row must already exist"
        );
    }
    write.commit().expect("commit raw damage");
}

/// Re-admits the fixture over a bounded (index-dormant) open and reports
/// whether the writer failed closed rather than treating the command as absent.
fn readmission_is_corrupt(path: &Path, fixture: &CommandFixture) -> bool {
    let ports = open_operational(RedbStore::open(path).expect("reopen to certify"));
    ports
        .write_clean_close_lifecycle()
        .expect("write clean-close certificate");
    drop(ports);
    let ports = open_operational(RedbStore::open(path).expect("bounded reopen"));
    assert!(ports.clean_close_fast_startup());
    let Ok(batch) = ports.begin_empty_batch() else {
        return true;
    };
    let Ok(candidate) = batch.begin_candidate(Box::new(fixture.intent.clone())) else {
        return true;
    };
    match candidate.recheck_admission() {
        Err(error) => error.kind() == riffdb_storage_api::StorageErrorKind::CorruptData,
        Ok(CandidateAdmissionResult::Proceed(_)) => false,
        Ok(_) => false,
    }
}

#[test]
fn command_derived_checkpoint_tables_are_physically_empty() {
    let path = TestDatabasePath::new("derived-checkpoint-tables-empty");
    prepare_checkpointed_command_database(&path.0);
    assert!(!retention_raw_table_has_rows(&path.0, "audit_by_request"));
    assert!(!retention_raw_table_has_rows(&path.0, "event_routes"));
    assert!(!retention_raw_table_has_rows(&path.0, "idempotency"));
    let findings = collect_structural_findings(RedbStore::open(&path.0).expect("reopen database"));
    assert!(
        findings.is_empty(),
        "derived indexes rebuild from the segment: {findings:?}"
    );
}

#[test]
fn finding_vetoes_checkpoint_write_and_is_never_suppressed() {
    // Reviewer probe D: a garbage OUTBOX_STATUS row below S produces a
    // non-authoritative OutboxDelivery-scope finding on every full validation.
    // The finding must veto the checkpoint write so the next open re-validates
    // and reports it again — the fast path can never silence it.
    let path = TestDatabasePath::new("finding-vetoes-checkpoint");
    let _ = prepare_committed_command_database(&path.0);
    {
        let database = Database::create(&path.0).expect("open for garbage row");
        let txn = database.begin_write().expect("begin garbage row");
        {
            let key = retention_event_key(1);
            let mut statuses = txn.open_table(OUTBOX_STATUS_RAW).expect("open statuses");
            statuses
                .insert(key.as_slice(), &b"garbage-not-a-status"[..])
                .expect("insert garbage status");
        }
        txn.commit().expect("commit garbage row");
    }
    strip_checkpoint_meta(&path.0);

    // Open 1: full validation completes (non-authoritative finding does not
    // refuse the open) but the finding vetoes the checkpoint write.
    let first = drain_tolerating_findings(RedbStore::open(&path.0).expect("first open"));
    assert!(!first.verified, "no checkpoint may be verified after strip");
    assert!(
        !first.findings.is_empty(),
        "the garbage below-S row must produce a finding under full validation"
    );
    assert!(
        !first.authoritative_finding() && !first.refused(),
        "the outbox-scope finding must not refuse the open"
    );
    let outcome = first.outcome.expect("first open completes");
    drop(outcome);
    assert!(
        !checkpoint_meta_present(&path.0),
        "a finding of any scope must veto the checkpoint write"
    );

    // Open 2: no checkpoint exists, so full validation reports the finding
    // again — never suppressed by a range skip.
    let second = drain_tolerating_findings(RedbStore::open(&path.0).expect("second open"));
    assert!(!second.verified, "no checkpoint may have been written");
    assert!(
        !second.findings.is_empty(),
        "the finding must be reported again on the next open"
    );
}

#[test]
fn public_checkpoint_write_is_gated_on_clean_validation() {
    // Veto case: a validation session with findings (garbage OUTBOX_STATUS row)
    // must gate the public shutdown-side write exactly like the startup write.
    let path = TestDatabasePath::new("public-write-gate-veto");
    let _ = prepare_committed_command_database(&path.0);
    {
        let database = Database::create(&path.0).expect("open for garbage row");
        let txn = database.begin_write().expect("begin garbage row");
        {
            let key = retention_event_key(1);
            let mut statuses = txn.open_table(OUTBOX_STATUS_RAW).expect("open statuses");
            statuses
                .insert(key.as_slice(), &b"garbage-not-a-status"[..])
                .expect("insert garbage status");
        }
        txn.commit().expect("commit garbage row");
    }
    strip_checkpoint_meta(&path.0);

    let report = drain_tolerating_findings(RedbStore::open(&path.0).expect("open with finding"));
    assert!(
        !report.findings.is_empty(),
        "fixture must produce a finding"
    );
    let outcome = report.outcome.expect("outbox finding does not refuse open");
    let StructuralOpenOutcome::Clean(opened) = outcome else {
        panic!("fixture must not require migration");
    };
    let (_, _, _, dormant) = opened.into_parts();
    let ports = dormant
        .into_operational_after_catalog_validation()
        .expect("activate operational ports");
    assert!(
        !ports
            .write_validated_prefix_checkpoint()
            .expect("gated write must not error"),
        "a session with findings must veto the public checkpoint write"
    );
    drop(ports);
    assert!(
        !checkpoint_meta_present(&path.0),
        "the vetoed public write must leave no checkpoint"
    );

    // Allowed case: a clean validation session permits the public write.
    let clean_path = TestDatabasePath::new("public-write-gate-clean");
    let _ = prepare_committed_command_database(&clean_path.0);
    let report = drain_tolerating_findings(RedbStore::open(&clean_path.0).expect("clean open"));
    assert!(report.findings.is_empty(), "clean fixture has no findings");
    let StructuralOpenOutcome::Clean(opened) = report.outcome.expect("clean open completes") else {
        panic!("clean fixture must not require migration");
    };
    let (_, _, _, dormant) = opened.into_parts();
    let ports = dormant
        .into_operational_after_catalog_validation()
        .expect("activate operational ports");
    assert!(
        ports
            .write_validated_prefix_checkpoint()
            .expect("public write after clean validation"),
        "a clean validation session must permit the public checkpoint write"
    );
    drop(ports);
    assert!(checkpoint_meta_present(&clean_path.0));
}

#[test]
fn checkpoint_write_failure_after_clean_validation_is_nonfatal() {
    // ADR-0019 A1: a FAILED checkpoint write after clean validation is
    // non-fatal — the open completes and only the next fast path is lost.
    let path = TestDatabasePath::new("checkpoint-write-failure-nonfatal");
    let _ = prepare_committed_command_database(&path.0);
    strip_checkpoint_meta(&path.0);

    let controller =
        RedbTestController::return_before_commit(RedbTestOperation::ValidatedPrefixCheckpoint);
    let store = RedbStore::open_with_test_controller(&path.0, controller)
        .expect("open with armed checkpoint failpoint");
    let (_, catalog_outcome, outcome) = complete_startup_pass(store);
    assert!(matches!(catalog_outcome, CatalogHistoryOutcome::Ready(_)));
    let StructuralOpenOutcome::Clean(opened) = outcome else {
        panic!("clean validation with a failed checkpoint write must still open");
    };
    let (_, _, _, dormant) = opened.into_parts();
    let ports = dormant
        .into_operational_after_catalog_validation()
        .expect("activate operational ports after failed checkpoint write");
    assert_eq!(
        ports.checkpoint_write_failures(),
        1,
        "the failed write must be counted for operator visibility"
    );
    drop(ports);
    assert!(
        !checkpoint_meta_present(&path.0),
        "the failed write must leave no durable checkpoint"
    );

    // The lost fast path is the only cost: the next open full-validates clean.
    let (_, _, outcome, verified, _) =
        complete_startup_observing_checkpoint(RedbStore::open(&path.0).expect("reopen"));
    assert!(matches!(outcome, StructuralOpenOutcome::Clean(_)));
    assert!(!verified, "no checkpoint may exist after the failed write");
}

/// Binding-mismatch coverage: doctors one binding field (self-hash kept valid),
/// asserts the checkpoint is IGNORED with the exact counted reason, and that
/// full validation still opens the otherwise-valid database cleanly.
fn assert_binding_mismatch_falls_back(
    label: &str,
    expected_reason: &'static str,
    mutate: impl FnOnce(
        &riffdb_storage_api::StoredValidatedPrefixCheckpointV1,
    ) -> riffdb_storage_api::StoredValidatedPrefixCheckpointV1,
) {
    let path = TestDatabasePath::new(label);
    prepare_checkpointed_command_database(&path.0);
    rewrite_checkpoint(&path.0, mutate);

    let report = drain_tolerating_findings(RedbStore::open(&path.0).expect("doctored open"));
    assert!(
        !report.verified,
        "{label}: binding mismatch must ignore the checkpoint"
    );
    assert_eq!(
        report.ignored_reason,
        Some(expected_reason),
        "{label}: the session must expose the exact ignore reason"
    );
    assert!(
        report.findings.is_empty() && !report.refused(),
        "{label}: full validation of otherwise-valid data must open cleanly"
    );
    let StructuralOpenOutcome::Clean(opened) = report.outcome.expect("open completes") else {
        panic!("{label}: fixture must not require migration");
    };
    let (_, _, _, dormant) = opened.into_parts();
    let ports = dormant
        .into_operational_after_catalog_validation()
        .expect("activate operational ports");
    let counts = ports.checkpoint_ignore_counts();
    let observed = counts
        .iter()
        .find(|(reason, _)| *reason == expected_reason)
        .map(|(_, count)| *count)
        .expect("known reason");
    assert_eq!(
        observed, 1,
        "{label}: the ignore reason must be counted as {expected_reason} (counts: {counts:?})"
    );
}

#[test]
fn checkpoint_wrong_database_id_falls_back_to_full_validation() {
    assert_binding_mismatch_falls_back(
        "checkpoint-wrong-database-id",
        "database_id_mismatch",
        |original| {
            let other =
                DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_001, [0x27; 10])
                    .expect("distinct database ID");
            assert_ne!(other, original.database_id());
            riffdb_storage_api::StoredValidatedPrefixCheckpointV1::new(
                other,
                original.history_incarnation(),
                original.registry_digest(),
                original.checkpoint_commit_sequence(),
                original.audit_sequence_bound(),
                original.counts(),
                original.entity_chain_fingerprint(),
                original.retained(),
                original.previous_checkpoint_hash(),
                0,
            )
            .expect("rehash wrong database id")
        },
    );
}

#[test]
fn checkpoint_wrong_registry_digest_falls_back_to_full_validation() {
    assert_binding_mismatch_falls_back(
        "checkpoint-wrong-registry-digest",
        "registry_digest_mismatch",
        |original| {
            let wrong = riffdb_types::SchemaHash::from_bytes([0x5c; 32]);
            assert_ne!(wrong, original.registry_digest());
            riffdb_storage_api::StoredValidatedPrefixCheckpointV1::new(
                original.database_id(),
                original.history_incarnation(),
                wrong,
                original.checkpoint_commit_sequence(),
                original.audit_sequence_bound(),
                original.counts(),
                original.entity_chain_fingerprint(),
                original.retained(),
                original.previous_checkpoint_hash(),
                0,
            )
            .expect("rehash wrong digest")
        },
    );
}

#[test]
fn checkpoint_sequence_beyond_head_falls_back_to_full_validation() {
    assert_binding_mismatch_falls_back(
        "checkpoint-sequence-beyond-head",
        "sequence_beyond_head",
        |original| {
            riffdb_storage_api::StoredValidatedPrefixCheckpointV1::new(
                original.database_id(),
                original.history_incarnation(),
                original.registry_digest(),
                original.checkpoint_commit_sequence().saturating_add(999),
                original.audit_sequence_bound(),
                original.counts(),
                original.entity_chain_fingerprint(),
                original.retained(),
                original.previous_checkpoint_hash(),
                0,
            )
            .expect("rehash beyond-head sequence")
        },
    );
}

#[test]
fn doctored_entities_row_falls_back_and_full_pass_reports_the_finding() {
    // Doctor an ENTITIES row away: reconstruction at S disagrees with the
    // fingerprint → checkpoint ignored (counted) → the full pass reports the
    // real authoritative finding and the open refuses.
    let path = TestDatabasePath::new("doctored-entities-row");
    prepare_checkpointed_command_database(&path.0);
    delete_first_raw_row(&path.0, ENTITIES_RAW);

    let report = drain_tolerating_findings(RedbStore::open(&path.0).expect("doctored open"));
    assert!(
        !report.verified,
        "entity-chain fingerprint mismatch must ignore the checkpoint"
    );
    assert!(
        report.authoritative_finding(),
        "the post-fallback full pass must report the real authoritative \
         finding for the vanished ENTITIES row: {:?}",
        report.findings,
    );
    assert!(
        report.refused(),
        "authoritative corruption must refuse the open"
    );
}

#[test]
fn checkpointed_open_accepts_prefix_only_entities_via_seeded_chains() {
    // Prefix-only target under a verified checkpoint: the seeded chain carries
    // the fingerprint-verified version but no post-image hash. The version-only
    // acceptance arm must validate the ENTITIES row cleanly; without it every
    // prefix-only entity would report a false MissingCrossLink.
    let path = TestDatabasePath::new("seeded-chain-prefix-only");
    prepare_checkpointed_command_database(&path.0);

    let (_, _, outcome, verified, _) =
        complete_startup_observing_checkpoint(RedbStore::open(&path.0).expect("checkpointed open"));
    assert!(verified, "the S=head checkpoint must verify");
    assert!(
        matches!(outcome, StructuralOpenOutcome::Clean(_)),
        "prefix-only entities must validate via seeded chains"
    );
}

#[test]
fn audit_shaped_history_gets_nonzero_below_bound_sampling() {
    // The committed-command fixture carries audit rows below the bound; the
    // audit-class sample windows must inspect them (S2: audit histories get
    // nonzero below-S sampling; previously zero when only COMMITS/EVENTS were
    // sampled).
    let path = TestDatabasePath::new("audit-sample-windows");
    prepare_checkpointed_command_database(&path.0);

    let (_, _, outcome, verified, sampled) =
        complete_startup_observing_checkpoint(RedbStore::open(&path.0).expect("checkpointed open"));
    assert!(verified);
    assert!(matches!(outcome, StructuralOpenOutcome::Clean(_)));
    // Fixture: 1 commit + 1 event + 3 audit rows below bounds. Sampling must
    // now cover more than the commit/event classes alone.
    assert!(
        sampled > 2,
        "audit-class rows must be sampled below the bound (observed {sampled})"
    );
}

// ===================================================================
// RT-B retention acceptance spine (ADR-0085 Amendment 2): offline prune
// over REAL committed history. Grown from the independent review's
// runtime probes; every test asserts the spec-mandated behavior.
// ===================================================================

/// Marks the outbox intent at `(sequence, 0)` delivered, raw. The canonical
/// initial state (absent status row) is UNDELIVERED and fences prune.
fn retention_deliver_outbox_status_raw(path: &Path, sequence: u64) {
    let database = Database::open(path).expect("open raw for delivered status");
    let write = database.begin_write().expect("write");
    {
        let mut statuses = write
            .open_table(TableDefinition::<&[u8], &[u8]>::new("outbox_status"))
            .expect("outbox_status");
        let event_id = EventId::new(
            CommitSequence::new(sequence).expect("delivered sequence"),
            0,
        );
        let status = riffdb_storage_api::StoredOutboxStatusV1::delivered(
            event_id,
            std::num::NonZeroU32::MIN,
            riffdb_storage_api::OutboxDestinationIdV1::new("dest-1").expect("dest"),
            Timestamp::new(1_700_000_005, 0).expect("delivered at"),
        );
        let encoded = riffdb_storage_api::proto_codec::encode_outbox_status_v1(&status)
            .expect("encode delivered status");
        statuses
            .insert(retention_event_key(sequence).as_slice(), encoded.as_bytes())
            .expect("insert delivered status");
    }
    write.commit().expect("commit delivered status");
}

const fn retention_event_key(sequence: u64) -> [u8; 12] {
    let mut key = [0u8; 12];
    let bytes = sequence.to_be_bytes();
    let mut i = 0;
    while i < 8 {
        key[i] = bytes[i];
        i += 1;
    }
    key
}

fn retention_raw_row_present(path: &Path, table_name: &str, key: &[u8]) -> bool {
    let database = Database::open(path).expect("open raw");
    let read = database.begin_read().expect("read");
    let table = read
        .open_table(TableDefinition::<&[u8], &[u8]>::new(table_name))
        .expect("table");
    table.get(key).expect("get").is_some()
}

fn retention_raw_table_has_rows(path: &Path, table_name: &str) -> bool {
    let database = Database::open(path).expect("open raw");
    let read = database.begin_read().expect("read");
    let table = read
        .open_table(TableDefinition::<&[u8], &[u8]>::new(table_name))
        .expect("table");
    table.first().expect("first").is_some()
}

fn retention_meta_present(path: &Path, key: &str) -> bool {
    let database = Database::open(path).expect("open raw");
    let read = database.begin_read().expect("read");
    let table = read.open_table(META).expect("meta");
    table.get(key).expect("get").is_some()
}

fn retention_tombstones(path: &Path) -> Vec<riffdb_storage_api::StoredHistoryTombstoneV1> {
    let database = Database::open(path).expect("open raw");
    let read = database.begin_read().expect("read");
    let table = read
        .open_table(TableDefinition::<&[u8], &[u8]>::new("history_tombstones"))
        .expect("tombstones");
    table
        .iter()
        .expect("iter")
        .map(|entry| {
            let (_, value) = entry.expect("entry");
            riffdb_storage_api::proto_codec::decode_history_tombstone_v1(value.value())
                .expect("decode tombstone")
                .into_parts()
                .0
        })
        .collect()
}

fn retention_delete_raw_row(path: &Path, table_name: &str, key: &[u8]) {
    let database = Database::open(path).expect("open raw");
    let write = database.begin_write().expect("write");
    {
        let mut table = write
            .open_table(TableDefinition::<&[u8], &[u8]>::new(table_name))
            .expect("table");
        assert!(
            table.remove(key).expect("remove").is_some(),
            "doctored row must exist before deletion"
        );
    }
    write.commit().expect("commit doctored deletion");
}

#[test]
fn retention_prune_refuses_below_undelivered_outbox_intent() {
    // Commit 1 leaves outbox intent (1,0) in the canonical absent-status
    // (undelivered) initial state; the undelivered low-water mark fences the
    // watermark at 0. Prune REFUSES and deletes nothing.
    let path = TestDatabasePath::new("retention-undelivered-fence");
    let _ = prepare_committed_command_database(&path.0);
    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    maintenance.add_hold("cap", 5, "test cap").expect("hold");
    let outcome = maintenance.prune_to(1);
    assert!(
        outcome.is_err(),
        "prune below an undelivered outbox intent must refuse: {outcome:?}"
    );
    let status = maintenance.status().expect("status after refusal");
    assert_eq!(status.watermark_sequence, 0);
    assert_eq!(status.tombstone_count, 0);
    assert!(retention_raw_row_present(
        &path.0,
        "commits",
        &1u64.to_be_bytes()
    ));
    assert!(
        !retention_raw_table_has_rows(&path.0, "events"),
        "the refused prune leaves segment-owned events in their segment"
    );
}

// req: STO-023, REC-004
#[test]
fn retention_prune_populated_roundtrip_reopens_clean_and_types_pruned_reads() {
    // The mandated populated-history roundtrip: prune → every pruned row gone,
    // tombstone counts exact → reopen validates CLEAN → fresh checkpoint
    // written → reads above the watermark unchanged → reads below typed
    // HistoryPruned (RDB-HISTORY-0102).
    let path = TestDatabasePath::new("retention-populated-roundtrip");
    let (_, second) = prepare_two_command_database(&path.0);
    retention_deliver_outbox_status_raw(&path.0, 1);
    retention_deliver_outbox_status_raw(&path.0, 2);

    let ports = open_operational(RedbStore::open(&path.0).expect("certification open"));
    ports
        .write_clean_close_lifecycle()
        .expect("certify pre-retention lifecycle");
    drop(ports);

    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    maintenance.add_hold("cap", 5, "test cap").expect("hold");
    let digest_key = DigestKeyId::new(1).expect("digest key ID");
    let validation_inputs = |seconds| {
        StartupValidationInputs::new(
            Timestamp::new(seconds, 0).expect("startup timestamp"),
            ReadableCapabilityDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
                .expect("capability digest inventory"),
            ReadableIdempotencyDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
                .expect("idempotency digest inventory"),
        )
    };
    let held = RedbStore::open(&path.0)
        .expect("open after retention hold")
        .begin_structural_evidence(validation_inputs(1_700_000_000))
        .expect("select post-hold startup mode");
    assert!(
        !held.clean_close_fast_path(),
        "retention hold mutation must invalidate the prior certificate"
    );
    drop(held);
    let status = maintenance.prune_to(1).expect("prune populated range");
    assert_eq!(status.watermark_sequence, 1);
    assert_eq!(status.tombstone_count, 1);

    let pruned = RedbStore::open(&path.0)
        .expect("open after retention prune")
        .begin_structural_evidence(validation_inputs(1_700_000_001))
        .expect("select post-prune startup mode");
    assert!(
        !pruned.clean_close_fast_path(),
        "retention prune must keep the old certificate invalid"
    );
    drop(pruned);

    assert!(!retention_raw_row_present(
        &path.0,
        "commits",
        &1u64.to_be_bytes()
    ));
    assert!(retention_raw_row_present(
        &path.0,
        "commits",
        &2u64.to_be_bytes()
    ));
    assert!(!retention_raw_table_has_rows(&path.0, "events"));
    assert!(!retention_raw_table_has_rows(&path.0, "outbox"));
    assert!(!retention_raw_row_present(
        &path.0,
        "outbox_status",
        &retention_event_key(1)
    ));
    assert!(retention_raw_row_present(
        &path.0,
        "outbox_status",
        &retention_event_key(2)
    ));
    let tombstones = retention_tombstones(&path.0);
    assert_eq!(tombstones.len(), 1);
    assert_eq!(
        (
            tombstones[0].first_sequence(),
            tombstones[0].last_sequence(),
            tombstones[0].commits_count(),
            tombstones[0].events_count(),
            tombstones[0].outbox_count(),
            tombstones[0].outbox_status_count(),
        ),
        (1, 1, 1, 1, 1, 1),
        "tombstone must record every pruned row"
    );

    // Reopen: the tombstone-tolerant walk must be CLEAN on a pruned database.
    let findings = collect_structural_findings(RedbStore::open(&path.0).expect("reopen pruned"));
    assert!(
        findings.is_empty(),
        "pruned reopen must validate clean: {findings:?}"
    );

    // A clean full pass writes a FRESH checkpoint bound to the new watermark.
    complete_startup_pass(RedbStore::open(&path.0).expect("checkpoint pass"));
    assert!(
        retention_meta_present(&path.0, "validated_prefix_checkpoint/v1"),
        "clean pruned validation must write a fresh checkpoint"
    );

    // Reads above the watermark are unchanged; reads below are typed pruned.
    let ports = open_operational(RedbStore::open(&path.0).expect("operational reopen"));
    let retained = ports
        .read_commit(second.records.commit().commit_sequence())
        .expect("read retained commit")
        .expect("retained commit body");
    assert_eq!(retained.commit_sequence().get(), 2);
    let retained_event = ports
        .read_durable_event(EventId::new(CommitSequence::new(2).expect("sequence"), 0))
        .expect("read retained event")
        .expect("retained event body");
    assert_eq!(retained_event.event_id().commit_sequence().get(), 2);
    let commit_err = ports
        .read_commit(CommitSequence::first())
        .expect_err("below-watermark commit read must be typed pruned");
    assert_eq!(
        commit_err.kind(),
        riffdb_storage_api::StorageErrorKind::HistoryPruned
    );
    let event_err = ports
        .read_durable_event(EventId::new(CommitSequence::first(), 0))
        .expect_err("below-watermark event read must be typed pruned");
    assert_eq!(
        event_err.kind(),
        riffdb_storage_api::StorageErrorKind::HistoryPruned
    );
}

#[test]
fn retention_prune_cannot_pass_durable_history_end() {
    // One committed command (durable head 1): the application-head fencing
    // input refuses any watermark past the end of durable history, even under
    // a permissive operator hold.
    let path = TestDatabasePath::new("retention-head-fence");
    let _ = prepare_committed_command_database(&path.0);
    retention_deliver_outbox_status_raw(&path.0, 1);
    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    maintenance.add_hold("cap", 100, "test cap").expect("hold");
    let outcome = maintenance.prune_to(50);
    assert!(
        outcome.is_err(),
        "watermark must not pass the durable history end: {outcome:?}"
    );
    let status = maintenance.status().expect("status after refusal");
    assert_eq!(status.watermark_sequence, 0);
    assert_eq!(
        status.max_permissible_watermark,
        Some(1),
        "durable application head must bind the fencing maximum"
    );
    assert_eq!(
        status.fence_binding,
        riffdb_storage_api::RetentionFenceBinding::DurableApplicationHead
    );
}

#[test]
fn retention_prune_refuses_on_empty_history() {
    // Nothing ever committed: the durable head is 0 and pruning anything is
    // impossible by construction, even under an operator hold.
    let path = TestDatabasePath::new("retention-empty-history");
    prepare_command_database(&path.0);
    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    maintenance.add_hold("cap", 5, "test cap").expect("hold");
    let outcome = maintenance.prune_to(1);
    assert!(
        outcome.is_err(),
        "prune over empty history must refuse: {outcome:?}"
    );
}

#[test]
fn retention_prune_deletes_live_checkpoint_first() {
    let path = TestDatabasePath::new("retention-checkpoint-delete");
    prepare_command_database(&path.0);
    let ports = open_operational(RedbStore::open(&path.0).expect("open journal fixture"));
    fence_deferred_epoch(&ports, &[command_fixture()]);
    assert!(
        ports
            .write_validated_prefix_checkpoint()
            .expect("write explicit checkpoint fixture"),
        "clean validation must permit the checkpoint"
    );
    drop(ports);
    let mut journal_path = path.0.as_os_str().to_os_string();
    journal_path.push(".riffjournal");
    let journal_path = PathBuf::from(journal_path);
    assert!(
        journal_path.is_file(),
        "checkpointed standard-profile fixture must retain its active journal"
    );
    assert!(
        retention_meta_present(&path.0, "validated_prefix_checkpoint/v1"),
        "fixture must start with a live checkpoint"
    );
    retention_deliver_outbox_status_raw(&path.0, 1);
    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    maintenance.add_hold("cap", 5, "test cap").expect("hold");
    let _ = maintenance
        .prune_to(1)
        .expect("prune checkpointed database");
    assert!(
        !retention_meta_present(&path.0, "validated_prefix_checkpoint/v1"),
        "prune must delete the validated-prefix checkpoint in its first transaction"
    );
    assert!(
        journal_path.is_file(),
        "offline prune must not silently discard its journal"
    );
    let findings = collect_structural_findings(
        RedbStore::open(&path.0).expect("reopen fully pruned checkpointed database"),
    );
    assert!(
        findings.is_empty(),
        "journal rebase plus tombstone proof must reopen clean: {findings:?}"
    );

    let ports = open_operational(
        RedbStore::open(&path.0).expect("open fully pruned database for successor command"),
    );
    fence_deferred_epoch(&ports, &[command_fixture_at(2)]);
    assert!(
        ports
            .write_validated_prefix_checkpoint()
            .expect("write successor checkpoint fixture")
    );
    drop(ports);
    retention_deliver_outbox_status_raw(&path.0, 2);
    let status = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0)
        .prune_to(2)
        .expect("prune successor command after the first full prune");
    assert_eq!(status.watermark_sequence, 2);
    let findings = collect_structural_findings(
        RedbStore::open(&path.0).expect("reopen after second journal-backed full prune"),
    );
    assert!(
        findings.is_empty(),
        "second journal rebase and prune cycle must reopen clean: {findings:?}"
    );
}

#[test]
fn retention_stale_checkpoint_bound_to_wrong_watermark_is_ignored() {
    // A checkpoint written BEFORE the prune (watermark binding 0) restored
    // after the prune must be IGNORED via WatermarkMismatch and fall back to
    // full validation. The checkpoint is engineered so every other ignore
    // condition passes: removing the watermark ignore-arm would let it verify.
    let path = TestDatabasePath::new("retention-stale-checkpoint");
    prepare_checkpointed_command_database(&path.0);
    // Capture the S=1 checkpoint (bound to watermark 0, counts within the
    // post-prune totals).
    let stale = {
        let database = Database::open(&path.0).expect("open raw");
        let read = database.begin_read().expect("read");
        let meta = read.open_table(META).expect("meta");
        meta.get("validated_prefix_checkpoint/v1")
            .expect("get")
            .expect("live checkpoint")
            .value()
            .to_vec()
    };
    // Commit sequence 2 so the retained head stays above the pruned prefix.
    let second = command_fixture_at(2);
    let ports = open_operational(RedbStore::open(&path.0).expect("reopen for second commit"));
    commit_command_fixture(&ports, &second);
    drop(ports);
    retention_deliver_outbox_status_raw(&path.0, 1);
    retention_deliver_outbox_status_raw(&path.0, 2);
    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    maintenance.add_hold("cap", 5, "test cap").expect("hold");
    let _ = maintenance.prune_to(1).expect("prune");
    assert!(!retention_meta_present(
        &path.0,
        "validated_prefix_checkpoint/v1"
    ));
    {
        let database = Database::open(&path.0).expect("open raw");
        let write = database.begin_write().expect("write");
        {
            let mut meta = write.open_table(META).expect("meta");
            meta.insert("validated_prefix_checkpoint/v1", stale.as_slice())
                .expect("restore stale checkpoint");
        }
        write.commit().expect("commit stale checkpoint");
    }

    let digest_key = DigestKeyId::new(1).expect("digest key ID");
    let inputs = StartupValidationInputs::new(
        Timestamp::new(1_700_000_000, 0).expect("startup timestamp"),
        ReadableCapabilityDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("capability digest inventory"),
        ReadableIdempotencyDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("idempotency digest inventory"),
    );
    let session = RedbStore::open(&path.0)
        .expect("open with stale checkpoint")
        .begin_structural_evidence(inputs)
        .expect("stale checkpoint must not block the open");
    assert!(
        !session.checkpoint_verified(),
        "a checkpoint bound to the wrong watermark must never verify"
    );
    assert_eq!(
        session.checkpoint_ignored_reason(),
        Some("watermark_mismatch"),
        "the stale checkpoint must be ignored by the watermark binding"
    );
    drop(session);
    let findings =
        collect_structural_findings(RedbStore::open(&path.0).expect("full validation reopen"));
    assert!(
        findings.is_empty(),
        "full validation after ignoring the stale checkpoint must be clean: {findings:?}"
    );
}

#[test]
fn retention_absence_above_watermark_remains_corruption() {
    // Deliverable: absence NOT covered by the verified tombstone chain is the
    // same corruption it is today. Doctor away rows ABOVE the watermark; the
    // walk must refuse or produce authoritative findings.
    let path = TestDatabasePath::new("retention-doctored-above");
    let _ = prepare_two_command_database(&path.0);
    retention_deliver_outbox_status_raw(&path.0, 1);
    retention_deliver_outbox_status_raw(&path.0, 2);
    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    maintenance.add_hold("cap", 5, "test cap").expect("hold");
    let _ = maintenance.prune_to(1).expect("prune");
    retention_delete_raw_row(&path.0, "commits", &2u64.to_be_bytes());
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        collect_structural_findings(RedbStore::open(&path.0).expect("reopen doctored database"))
    }));
    match outcome {
        Ok(findings) => assert!(
            !findings.is_empty(),
            "doctored commit above the watermark must stay corruption"
        ),
        Err(_) => {
            // A refused walk (structural error) is an equally valid
            // fail-closed outcome for uncovered absence.
        }
    }
}

#[test]
fn retention_multi_prune_resumes_tombstone_chain() {
    // The chain-resume path: a second prune (and a child-process kill before
    // the resumed sub-range commits) continues the tombstone chain —
    // contiguous, hash-linked, abutting the advanced watermark — and the
    // pruned database still validates clean.
    let path = TestDatabasePath::new("retention-chain-resume");
    let _ = prepare_two_command_database(&path.0);
    retention_deliver_outbox_status_raw(&path.0, 1);
    retention_deliver_outbox_status_raw(&path.0, 2);

    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    maintenance.add_hold("cap", 5, "test cap").expect("hold");
    let status = maintenance.prune_to(1).expect("first prune");
    assert_eq!(status.watermark_sequence, 1);
    drop(maintenance);

    // Child-process kill before the resumed sub-range commits: state stays
    // valid at watermark 1 and the database reopens.
    run_crashing_child_prune("before-retention-prune-subrange-commit", &path.0, 2);

    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    let status = maintenance.status().expect("status after aborted resume");
    assert_eq!(status.watermark_sequence, 1);
    assert_eq!(status.tombstone_count, 1);
    drop(RedbStore::open(&path.0).expect("reopen after aborted resume"));

    // Re-run: the chain RESUMES with a second tombstone linked to the first.
    let status = maintenance.prune_to(2).expect("resumed prune");
    assert_eq!(status.watermark_sequence, 2);
    assert_eq!(status.tombstone_count, 2);
    let tombstones = retention_tombstones(&path.0);
    assert_eq!(tombstones.len(), 2);
    assert_eq!(
        (
            tombstones[0].first_sequence(),
            tombstones[0].last_sequence()
        ),
        (1, 1)
    );
    assert_eq!(
        (
            tombstones[1].first_sequence(),
            tombstones[1].last_sequence()
        ),
        (2, 2)
    );
    assert_eq!(
        tombstones[1].previous_tombstone_hash(),
        Some(tombstones[0].tombstone_hash()),
        "the resumed tombstone must hash-link to its predecessor"
    );
    let findings =
        collect_structural_findings(RedbStore::open(&path.0).expect("reopen fully pruned"));
    assert!(
        findings.is_empty(),
        "fully pruned reopen must validate clean: {findings:?}"
    );
}

#[test]
fn retention_prune_refuses_when_commit_rows_missing_in_range() {
    // Pre-delete verification: a commit row already missing inside the
    // requested range is pre-existing corruption. Prune must REFUSE rather
    // than launder the absence into "verified pruned".
    let path = TestDatabasePath::new("retention-predelete-verification");
    let _ = prepare_committed_command_database(&path.0);
    retention_deliver_outbox_status_raw(&path.0, 1);
    retention_delete_raw_row(&path.0, "commits", &1u64.to_be_bytes());
    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    maintenance.add_hold("cap", 5, "test cap").expect("hold");
    let err = maintenance
        .prune_to(1)
        .expect_err("prune over a missing commit row must refuse");
    assert_eq!(
        err.kind(),
        riffdb_storage_api::StorageErrorKind::CorruptData
    );
    assert!(retention_raw_row_present(
        &path.0,
        "outbox_status",
        &retention_event_key(1)
    ));
    assert!(retention_tombstones(&path.0).is_empty());
}

#[test]
fn retention_semantic_chain_forgery_refuses_startup() {
    // A canonically re-encoded tombstone with a VALID self-hash but forged
    // semantics must refuse startup validation:
    // (a) forged per-table counts (count arithmetic),
    // (b) forged range end (abutment at the watermark).
    for (label, forge_last, forge_counts) in [("counts", 1u64, 7u64), ("gap", 1u64, 1u64)] {
        let path = TestDatabasePath::new("retention-chain-forgery");
        let _ = prepare_two_command_database(&path.0);
        retention_deliver_outbox_status_raw(&path.0, 1);
        retention_deliver_outbox_status_raw(&path.0, 2);
        let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
        maintenance.add_hold("cap", 5, "test cap").expect("hold");
        let target = if label == "gap" { 2 } else { 1 };
        let _ = maintenance.prune_to(target).expect("prune");
        {
            let database = Database::open(&path.0).expect("open raw");
            let write = database.begin_write().expect("write");
            {
                let mut table = write
                    .open_table(TableDefinition::<&[u8], &[u8]>::new("history_tombstones"))
                    .expect("tombstones");
                let (key_bytes, original) = {
                    let (key, value) = table
                        .iter()
                        .expect("iter")
                        .next()
                        .expect("row")
                        .expect("entry");
                    (key.value().to_vec(), value.value().to_vec())
                };
                let original =
                    riffdb_storage_api::proto_codec::decode_history_tombstone_v1(&original)
                        .expect("decode original")
                        .into_parts()
                        .0;
                let forged = riffdb_storage_api::StoredHistoryTombstoneV1::new(
                    original.first_sequence(),
                    forge_last,
                    forge_counts,
                    forge_counts,
                    forge_counts,
                    forge_counts,
                    original.content_digest(),
                    original.previous_tombstone_hash(),
                    original.history_incarnation(),
                )
                .expect("forge canonically valid tombstone");
                let encoded = riffdb_storage_api::proto_codec::encode_history_tombstone_v1(&forged)
                    .expect("encode forged tombstone");
                if label == "gap" {
                    // Drop any later tombstones so the forged [1,1] leaves a
                    // real gap below watermark 2.
                    let keys: Vec<Vec<u8>> = table
                        .iter()
                        .expect("iter")
                        .map(|entry| entry.expect("entry").0.value().to_vec())
                        .collect();
                    for key in keys {
                        let _ = table.remove(key.as_slice()).expect("remove");
                    }
                }
                table
                    .insert(key_bytes.as_slice(), encoded.as_bytes())
                    .expect("rewrite forged tombstone");
            }
            write.commit().expect("commit forgery");
        }
        let digest_key = DigestKeyId::new(1).expect("digest key ID");
        let inputs = StartupValidationInputs::new(
            Timestamp::new(1_700_000_000, 0).expect("startup timestamp"),
            ReadableCapabilityDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
                .expect("capability digest inventory"),
            ReadableIdempotencyDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
                .expect("idempotency digest inventory"),
        );
        let outcome = RedbStore::open(&path.0)
            .expect("plain open (canonical bytes)")
            .begin_structural_evidence(inputs);
        assert!(
            outcome.is_err(),
            "semantic chain forgery ({label}) must refuse startup validation"
        );
    }
}

#[test]
fn retention_projection_frontier_fences_prune() {
    // A live projection whose durable frontier is BeforeFirst fences the
    // watermark at 0; detaching it (audited) releases the fence.
    use riffdb_types::{ContractLineage, ProjectionFrontierKey, ProjectionId, ProjectionPlanHash};

    let path = TestDatabasePath::new("retention-projection-fence");
    let _ = prepare_committed_command_database(&path.0);
    retention_deliver_outbox_status_raw(&path.0, 1);
    let identity = riffdb_types::ProjectionIdentity::new(
        ContractLineage::new("retention-fence").expect("lineage"),
        ProjectionId::new(9).expect("projection id"),
        ProjectionPlanHash::from_bytes([0x33; 32]),
    );
    let control = riffdb_storage_api::StoredProjectionControlV1::new(
        identity.clone(),
        riffdb_types::ProjectionGeneration::first(),
        Some(riffdb_storage_api::ProjectionGenerationPosition::new(
            riffdb_types::ProjectionGeneration::first(),
            riffdb_types::FrontierPosition::BeforeFirst,
        )),
        None,
        Some(riffdb_storage_api::PublishedApplyModeV1::Enabled),
        riffdb_storage_api::ProjectionLifecycleV1::Ready,
        None,
    )
    .expect("projection control");
    {
        let database = Database::open(&path.0).expect("open raw");
        let write = database.begin_write().expect("write");
        {
            let mut controls = write
                .open_table(TableDefinition::<&[u8], &[u8]>::new("projection_frontier"))
                .expect("projection_frontier");
            let key = ProjectionFrontierKey::new(identity.clone());
            let encoded = riffdb_storage_api::proto_codec::encode_projection_control_v1(&control)
                .expect("encode control");
            controls
                .insert(key.as_bytes(), encoded.as_bytes())
                .expect("insert control");
        }
        write.commit().expect("commit control");
    }

    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    maintenance.add_hold("cap", 5, "test cap").expect("hold");
    let err = maintenance
        .prune_to(1)
        .expect_err("projection frontier at BeforeFirst must fence the prune");
    assert_eq!(
        err.kind(),
        riffdb_storage_api::StorageErrorKind::InvariantViolation
    );
    let status = maintenance.status().expect("status");
    assert_eq!(status.max_permissible_watermark, Some(0));
    assert_eq!(
        status.fence_binding,
        riffdb_storage_api::RetentionFenceBinding::ProjectionDurableFrontier
    );

    // Audited detach releases the projection from the fencing minimum.
    maintenance
        .detach_projection(
            ProjectionId::new(9).expect("projection id"),
            "replay budget accepted",
            Timestamp::new(1_700_000_009, 0).expect("timestamp"),
        )
        .expect("detach projection");
    let status = maintenance.prune_to(1).expect("prune after detach");
    assert_eq!(status.watermark_sequence, 1);
}

#[test]
fn retention_pruned_route_resolution_yields_typed_pruned_via_event_replay() {
    // Real-path RDB-HISTORY-0102: routes are retained under prune, and a
    // route resolving below the watermark surfaces the typed pruned outcome
    // through the catalog event-replay join — never an integrity error.
    use riffdb_catalog::ActiveCatalogSnapshot;

    let path = TestDatabasePath::new("retention-replay-pruned");
    let _ = prepare_two_command_database(&path.0);
    retention_deliver_outbox_status_raw(&path.0, 1);
    retention_deliver_outbox_status_raw(&path.0, 2);
    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    maintenance.add_hold("cap", 5, "test cap").expect("hold");
    let _ = maintenance.prune_to(1).expect("prune");
    let route_key = {
        let mut key = [0u8; 44];
        key[..32].copy_from_slice(
            hash_partition_key(command_fixture_at(1).pending.partition_key().as_bytes()).as_bytes(),
        );
        key[32..].copy_from_slice(&retention_event_key(1));
        key
    };
    assert!(
        retention_raw_row_present(&path.0, "event_routes", &route_key),
        "event routes must be RETAINED under prune"
    );

    let ports = open_operational(RedbStore::open(&path.0).expect("operational reopen"));
    let active = ActiveCatalogSnapshot::read(&ports)
        .expect("catalog read")
        .expect("active catalog");
    let replay = active
        .resolve_event_replay(
            "RowCreated",
            [("id", CanonicalValue::U64(7))],
            ["id", "value"],
        )
        .expect("resolve event replay");
    let error = replay
        .replay_page(
            &ports,
            riffdb_catalog::EventReplayPosition::Initial { after: None },
            riffdb_storage_api::EventRoutePageLimit::new(
                std::num::NonZeroU16::new(4).expect("nonzero"),
            )
            .expect("limit"),
            1,
        )
        .expect_err("a route below the watermark must surface typed pruned");
    assert_eq!(
        error.kind(),
        riffdb_catalog::EventReplayErrorKind::Storage(
            riffdb_storage_api::StorageErrorKind::HistoryPruned
        ),
        "pruned replay must be the typed outcome, never integrity"
    );
}

#[test]
fn retention_operator_hold_fences_prune() {
    let path = TestDatabasePath::new("retention-hold-fence");
    let _ = prepare_two_command_database(&path.0);
    retention_deliver_outbox_status_raw(&path.0, 1);
    retention_deliver_outbox_status_raw(&path.0, 2);
    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    maintenance
        .add_hold("legal-hold", 1, "litigation")
        .expect("hold");
    let err = maintenance
        .prune_to(2)
        .expect_err("prune past the operator hold must refuse");
    assert_eq!(
        err.kind(),
        riffdb_storage_api::StorageErrorKind::InvariantViolation
    );
    let status = maintenance.status().expect("status");
    assert_eq!(status.max_permissible_watermark, Some(1));
    assert_eq!(
        status.fence_binding,
        riffdb_storage_api::RetentionFenceBinding::OperatorHold
    );
    let status = maintenance.prune_to(1).expect("prune at the hold fence");
    assert_eq!(status.watermark_sequence, 1);
}

#[test]
fn retention_staged_migration_frontier_fences_prune() {
    // An open staged contract migration's frozen application frontier fences
    // the watermark: history at or below the frozen frontier must survive
    // until the stage resolves.
    use riffdb_storage_api::{
        ContractMigrationArtifactsV1, ContractMigrationJournalStepV1, MigrationScanCursor,
        StoredContractMigrationJournalV1,
    };
    use riffdb_types::{
        ContractBundleHash, ContractMigrationInputHash, ContractMigrationOperationId,
        MigrationBundleHash,
    };

    let path = TestDatabasePath::new("retention-staged-fence");
    let _ = prepare_two_command_database(&path.0);
    retention_deliver_outbox_status_raw(&path.0, 1);
    retention_deliver_outbox_status_raw(&path.0, 2);
    let operation_id =
        ContractMigrationOperationId::from_unix_milliseconds_and_random(2, [0x22; 10])
            .expect("operation id");
    let journal = StoredContractMigrationJournalV1::new(
        database_id(),
        operation_id,
        ContractMigrationInputHash::from_bytes([0x55; 32]),
        ContractMigrationArtifactsV1::new(
            ContractBundleHash::from_bytes([0x66; 32]),
            ContractBundleHash::from_bytes([0x77; 32]),
            MigrationBundleHash::from_bytes([0x88; 32]),
        ),
        ContractMigrationJournalStepV1::Transforming,
        MigrationScanCursor::start(),
        0,
        0,
        1,
        Some(CommitSequence::first()),
        Vec::new(),
        None,
    )
    .expect("staged journal with frozen frontier");
    {
        let database = Database::open(&path.0).expect("open raw");
        let write = database.begin_write().expect("write");
        {
            let mut table = write
                .open_table(TableDefinition::<&[u8], &[u8]>::new(
                    "contract_migration_journal",
                ))
                .expect("journal table");
            let encoded =
                riffdb_storage_api::proto_codec::encode_contract_migration_journal_v1(&journal)
                    .expect("encode journal");
            table
                .insert(operation_id.into_bytes().as_slice(), encoded.as_bytes())
                .expect("insert journal");
        }
        write.commit().expect("commit journal");
    }

    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    maintenance.add_hold("cap", 5, "test cap").expect("hold");
    let err = maintenance
        .prune_to(2)
        .expect_err("prune past the frozen application frontier must refuse");
    assert_eq!(
        err.kind(),
        riffdb_storage_api::StorageErrorKind::InvariantViolation
    );
    let status = maintenance.status().expect("status");
    assert_eq!(status.max_permissible_watermark, Some(1));
    assert_eq!(
        status.fence_binding,
        riffdb_storage_api::RetentionFenceBinding::StagedMigrationFrozenFrontier
    );
}

#[test]
fn retention_after_commit_kill_leaves_durable_subrange() {
    // Child kill AFTER the sub-range transaction committed: the sub-range is
    // durable {deleted rows, tombstone, watermark} and the database reopens
    // valid (chain resumes / state consistent).
    let path = TestDatabasePath::new("retention-after-commit");
    let _ = prepare_committed_command_database(&path.0);
    retention_deliver_outbox_status_raw(&path.0, 1);
    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    maintenance.add_hold("cap", 5, "test cap").expect("hold");
    drop(maintenance);
    run_crashing_child_prune("after-retention-prune-subrange-commit", &path.0, 1);

    let status = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0)
        .status()
        .expect("status after post-commit kill");
    assert_eq!(status.watermark_sequence, 1);
    assert_eq!(status.tombstone_count, 1);
    let findings = collect_structural_findings(
        RedbStore::open(&path.0).expect("reopen after post-commit kill"),
    );
    assert!(findings.is_empty(), "durable sub-range must validate clean");
}

#[test]
fn retention_tombstone_byte_corruption_refuses_open() {
    // A byte-flipped tombstone row fails canonical decode at plain open.
    let path = TestDatabasePath::new("retention-tombstone-byteflip");
    let _ = prepare_committed_command_database(&path.0);
    retention_deliver_outbox_status_raw(&path.0, 1);
    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    maintenance.add_hold("cap", 5, "test cap").expect("hold");
    let _ = maintenance.prune_to(1).expect("prune");
    {
        let database = Database::open(&path.0).expect("open raw");
        let write = database.begin_write().expect("write");
        {
            let mut table = write
                .open_table(TableDefinition::<&[u8], &[u8]>::new("history_tombstones"))
                .expect("tombstones");
            let (key_bytes, mut bytes) = {
                let (key, value) = table
                    .iter()
                    .expect("iter")
                    .next()
                    .expect("row")
                    .expect("entry");
                (key.value().to_vec(), value.value().to_vec())
            };
            let last = bytes.len() - 1;
            bytes[last] ^= 0xff;
            table
                .insert(key_bytes.as_slice(), bytes.as_slice())
                .expect("rewrite");
        }
        write.commit().expect("commit corruption");
    }
    let err = RedbStore::open(&path.0).expect_err("byte-corrupt tombstone must refuse open");
    assert_eq!(
        err.kind(),
        riffdb_storage_api::StorageErrorKind::CorruptData
    );
}

#[test]
fn retention_backup_of_pruned_database_restores_and_validates() {
    use riffdb_storage_api::{
        BackupBuildMetadataV1, OfflineBackupPersistencePort, OfflineRestoreOverwritePolicyV1,
        OfflineRestorePersistencePort, OfflineRestoreResultV1,
    };

    let path = TestDatabasePath::new("retention-backup-pruned");
    let _ = prepare_two_command_database(&path.0);
    retention_deliver_outbox_status_raw(&path.0, 1);
    retention_deliver_outbox_status_raw(&path.0, 2);
    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    maintenance.add_hold("cap", 5, "test cap").expect("hold");
    let status = maintenance.prune_to(1).expect("prune");
    assert_eq!(status.watermark_sequence, 1);

    let root = ScratchScope::new("retention-backup");
    let backup_dir = root.path().join("backup");
    let build = BackupBuildMetadataV1::new(
        "0.1.0",
        "0123456789abcdef",
        "rustc-1.97.0",
        1,
        vec!["retention".to_owned()],
    )
    .expect("build metadata");
    let mut backup = riffdb_storage_redb::RedbOfflineBackup::bind(&path.0, &backup_dir);
    let manifest = backup.create_offline_backup(&build).expect("backup");
    assert_eq!(manifest.retention_watermark_sequence(), Some(1));

    let restore_dir = root.path().join("restored");
    std::fs::create_dir_all(&restore_dir).expect("restore dir");
    let mut restore = riffdb_storage_redb::RedbOfflineRestore::bind(&backup_dir, &restore_dir);
    let result = restore
        .restore_offline_backup(OfflineRestoreOverwritePolicyV1::RefuseNonEmpty)
        .expect("restore");
    assert!(matches!(result, OfflineRestoreResultV1::Restored { .. }));

    let restored = restore_dir.join("database.redb");
    let findings = collect_structural_findings(RedbStore::open(&restored).expect("open restored"));
    assert!(
        findings.is_empty(),
        "restored pruned database must validate clean: {findings:?}"
    );
    let restored_status = riffdb_storage_redb::RedbOfflineRetention::bind(&restored)
        .status()
        .expect("restored status");
    assert_eq!(restored_status.watermark_sequence, 1);
    assert_eq!(restored_status.tombstone_count, 1);
}

// ===========================================================================
// ADR-0100 changelog emitter: exactness, gating, gap-freeness, non-blocking.
//
// The emitter is wired at the ADR-0101 §4 publication edge and derives frames
// only from the snapshot that publication pinned. These tests are the evidence
// for that: a differential reconstruction, the §2 gate obligation in its
// falsifiable form, the typed-lagging boundary, and the writer-independence of
// a stalled consumer.
// ===========================================================================

/// Physical tables the v1 entry classes claim, paired with their class.
///
/// The differential comparison is restricted to exactly these, because these
/// are exactly the tables the emitter promises. A class added without a
/// matching row here would silently escape the exactness proof, so the mapping
/// is asserted against the closed registry.
const CHANGELOG_CLAIMED_TABLES: [(&str, ChangelogEntryClassV1); 8] = [
    ("commits", ChangelogEntryClassV1::Commit),
    ("events", ChangelogEntryClassV1::Event),
    ("event_routes", ChangelogEntryClassV1::EventRoute),
    ("outbox", ChangelogEntryClassV1::OutboxIntent),
    ("provenance", ChangelogEntryClassV1::Provenance),
    ("entities", ChangelogEntryClassV1::Entity),
    ("audit", ChangelogEntryClassV1::AdministrationAudit),
    (
        "audit_by_request",
        ChangelogEntryClassV1::ServiceAuditRequestIndex,
    ),
];

type ClaimedRows = std::collections::BTreeMap<(&'static str, Vec<u8>), Vec<u8>>;

#[derive(Default)]
struct RecordingChangelogConsumer {
    accepted: std::sync::Mutex<Vec<(ChangelogFrameV1, Vec<u8>)>>,
    resyncs: std::sync::Mutex<Vec<ChangelogResyncReasonV1>>,
    stall: std::sync::Mutex<bool>,
    released: std::sync::Condvar,
}

impl RecordingChangelogConsumer {
    fn stalled() -> Self {
        Self {
            stall: std::sync::Mutex::new(true),
            ..Self::default()
        }
    }

    fn release(&self) {
        *self.stall.lock().expect("stall lock") = false;
        self.released.notify_all();
    }

    fn frames(&self) -> Vec<ChangelogFrameV1> {
        self.accepted
            .lock()
            .expect("frame lock")
            .iter()
            .map(|(frame, _)| frame.clone())
            .collect()
    }

    fn encoded(&self) -> Vec<Vec<u8>> {
        self.accepted
            .lock()
            .expect("frame lock")
            .iter()
            .map(|(_, bytes)| bytes.clone())
            .collect()
    }

    fn resyncs(&self) -> Vec<ChangelogResyncReasonV1> {
        self.resyncs.lock().expect("resync lock").clone()
    }
}

impl ChangelogFrameConsumer for RecordingChangelogConsumer {
    fn accept_frame(&self, frame: &ChangelogFrameV1, encoded: &EncodedChangelogFrameV1) {
        let mut stalled = self.stall.lock().expect("stall lock");
        while *stalled {
            stalled = self.released.wait(stalled).expect("stall wait");
        }
        drop(stalled);
        self.accepted
            .lock()
            .expect("frame lock")
            .push((frame.clone(), encoded.as_bytes().to_vec()));
    }

    fn note_resync_required(&self, reason: ChangelogResyncReasonV1) {
        self.resyncs.lock().expect("resync lock").push(reason);
    }
}

#[derive(Default)]
struct RecordingChangelogConsumerV2 {
    accepted: std::sync::Mutex<Vec<(ChangelogFrameV2, Vec<u8>)>>,
    resyncs: std::sync::Mutex<Vec<ChangelogResyncReasonV1>>,
}

impl RecordingChangelogConsumerV2 {
    fn encoded(&self) -> Vec<Vec<u8>> {
        self.accepted
            .lock()
            .expect("V2 frame lock")
            .iter()
            .map(|(_, bytes)| bytes.clone())
            .collect()
    }

    fn resyncs(&self) -> Vec<ChangelogResyncReasonV1> {
        self.resyncs.lock().expect("V2 resync lock").clone()
    }
}

impl ChangelogFrameConsumerV2 for RecordingChangelogConsumerV2 {
    fn accept_frame(&self, frame: &ChangelogFrameV2, encoded: &EncodedChangelogFrameV2) {
        self.accepted
            .lock()
            .expect("V2 frame lock")
            .push((frame.clone(), encoded.as_bytes().to_vec()));
    }

    fn note_resync_required(&self, reason: ChangelogResyncReasonV1) {
        self.resyncs.lock().expect("V2 resync lock").push(reason);
    }
}

/// A port that forwards every advancement except one, to manufacture a gap.
#[derive(Debug)]
struct DroppingChangelogPort {
    inner: std::sync::Arc<dyn riffdb_storage_api::ChangelogPublicationPort>,
    drop_ordinal: u64,
    seen: AtomicU64,
}

impl riffdb_storage_api::ChangelogPublicationPort for DroppingChangelogPort {
    fn observe_published_advancement(
        &self,
        advancement: riffdb_storage_api::PublishedFrontierAdvancement,
    ) {
        if self.seen.fetch_add(1, Ordering::Relaxed) == self.drop_ordinal {
            return;
        }
        self.inner.observe_published_advancement(advancement);
    }
}

/// Reads the claimed tables as one materialized durable state.
///
/// The standard profile's authoritative state is a redb checkpoint plus an
/// exact journal suffix (ADR-0104), so a raw redb read alone would see only the
/// checkpoint. Reopening the store first replays and checkpoints the suffix,
/// which is exactly the state a follower must end up holding.
fn read_claimed_rows(path: &Path) -> ClaimedRows {
    drop(RedbStore::open(path).expect("materialize the durable journal suffix"));
    let database = Database::open(path).expect("open raw database");
    let read = database.begin_read().expect("begin raw read");
    let mut rows = ClaimedRows::new();
    for (name, _) in CHANGELOG_CLAIMED_TABLES {
        let table = read
            .open_table(TableDefinition::<&[u8], &[u8]>::new(name))
            .expect("open claimed table");
        for row in table.iter().expect("iterate claimed table") {
            let (key, value) = row.expect("claimed row");
            rows.insert((name, key.value().to_vec()), value.value().to_vec());
        }
    }
    rows
}

fn table_name_for(class: ChangelogEntryClassV1) -> &'static str {
    CHANGELOG_CLAIMED_TABLES
        .into_iter()
        .find(|(_, candidate)| *candidate == class)
        .map(|(name, _)| name)
        .expect("every closed entry class claims a table")
}

/// Applies every frame's entries onto a starting map, exactly as a follower
/// would: one frame at a time, in order, all entries or none.
fn apply_frames(mut state: ClaimedRows, frames: &[ChangelogFrameV1]) -> ClaimedRows {
    for frame in frames {
        for entry in frame.entries() {
            state.insert(
                (table_name_for(entry.class()), entry.key().to_vec()),
                entry.value().to_vec(),
            );
        }
    }
    state
}

fn start_recording_emitter(
    capacity: usize,
) -> (
    std::sync::Arc<RecordingChangelogConsumer>,
    changelog_compatibility::RedbChangelogEmitterHandle,
) {
    let consumer = std::sync::Arc::new(RecordingChangelogConsumer::default());
    let handle = changelog_compatibility::start_changelog_emitter(
        std::sync::Arc::clone(&consumer) as std::sync::Arc<dyn ChangelogFrameConsumer>,
        capacity,
    )
    .expect("start changelog emitter");
    assert_eq!(handle.emitter().emitted_frames(), 0);
    assert_eq!(handle.emitter().processed_advancements(), 0);
    assert_eq!(
        handle.emitter().emitted_frontier(),
        riffdb_types::DualFrontier::INITIAL
    );
    assert_eq!(handle.emitter().emitted_chain_hash(), [0; 32]);
    (consumer, handle)
}

fn open_with_emitter(
    path: &Path,
    port: std::sync::Arc<dyn riffdb_storage_api::ChangelogPublicationPort>,
) -> RedbOperationalPorts {
    open_operational(
        RedbStore::open_with_changelog_publication_port(path, RedbCommitProfile::Standard, port)
            .expect("open observed database"),
    )
}

fn fence_deferred_epoch(ports: &RedbOperationalPorts, fixtures: &[CommandFixture]) {
    let mut epoch = ports
        .begin_deferred_command_epoch()
        .expect("begin observed durability epoch");
    for fixture in fixtures {
        epoch = apply_unpublished_command_fixture(epoch, fixture);
    }
    DeferredCommandEpoch::fence(epoch).expect("fence observed durability epoch");
}

fn standalone_audit_intent(seed: u8, seconds: i64) -> ServiceAuditAppendIntentV1 {
    ServiceAuditAppendIntentV1::new(
        RequestId::from_bytes(uuid_bytes(seed)).expect("audit request ID"),
        Timestamp::new(seconds, 0).expect("audit timestamp"),
        ServiceOperationV1::GetHealth,
        ServiceAuditPhaseV1::Denied,
        catalog_principal(),
        ServiceIngressKindV1::Grpc,
        ServiceAuditTargetsV1::empty(),
        None,
        ServiceAuditLinkV1::None,
    )
    .expect("standalone audit intent")
}

fn append_standalone_audit_group(ports: &mut RedbOperationalPorts, seeds: [u8; 2]) {
    let intents = [
        standalone_audit_intent(seeds[0], 1_700_000_010),
        standalone_audit_intent(seeds[1], 1_700_000_011),
    ];
    let riffdb_storage_api::ServiceAuditGroupAppend::Submitted(fence) = ports
        .submit_service_audit_group(&intents)
        .expect("submit standalone audit group")
    else {
        panic!("the standard profile must submit a journal fence");
    };
    let results = fence.wait().expect("fence standalone audit group");
    assert_eq!(results.len(), intents.len());
}

#[test]
fn changelog_frames_reconstruct_the_published_snapshot_exactly() {
    let path = TestDatabasePath::new("changelog-exactness");
    prepare_command_database(&path.0);
    // The anchor is the state a follower would hold after bootstrap. Read it
    // while no handle owns the database, then tail the emitted frames onto it.
    let anchor = read_claimed_rows(&path.0);

    let (consumer, emitter) =
        start_recording_emitter(changelog_compatibility::DEFAULT_CHANGELOG_BUFFER_ADVANCEMENTS);
    {
        let mut ports = open_with_emitter(&path.0, emitter.port());
        fence_deferred_epoch(&ports, &[command_fixture_at(1), command_fixture_at(2)]);
        append_standalone_audit_group(&mut ports, [0x81, 0x82]);
        fence_deferred_epoch(&ports, &[command_fixture_at(3)]);
        append_standalone_audit_group(&mut ports, [0x83, 0x84]);
        assert_eq!(
            emitter.emitter().wait_for_emitted(4),
            ChangelogEmissionStateV1::Streaming
        );
    }
    let final_rows = read_claimed_rows(&path.0);
    let frames = consumer.frames();
    assert_eq!(frames.len(), 4, "one frame per published advancement");

    let reconstructed = apply_frames(anchor.clone(), &frames);
    assert_eq!(
        reconstructed, final_rows,
        "frames applied to the anchor must be byte-equal to the final published snapshot"
    );
    assert_ne!(
        reconstructed, anchor,
        "the workload must have changed state"
    );

    // Command frames advance the application component; standalone service-audit
    // frames advance only the administration component.
    let application: Vec<_> = frames
        .iter()
        .map(|frame| {
            (
                frame.header().covered().application(),
                frame.header().covered().administration(),
            )
        })
        .collect();
    assert_eq!(
        application[0].0,
        CommitSequence::new(2),
        "the first frame covers both grouped commands"
    );
    assert_eq!(
        application[1].0, application[0].0,
        "a standalone audit frame leaves the application frontier unchanged"
    );
    assert!(
        application[1].1 > application[0].1,
        "a standalone audit frame advances the administration frontier"
    );
    assert_eq!(application[2].0, CommitSequence::new(3));

    for frame in &frames {
        assert!(
            frame.header().journaled(),
            "every standard-profile publication is covered by a journal flush"
        );
        assert_ne!(
            frame.header().journal_frame_hash(),
            [0; 32],
            "each frame is bound to the journal fence that released it"
        );
        assert!(
            frame
                .header()
                .covered()
                .advances_from(frame.header().predecessor()),
            "a frame always advances its dual frontier"
        );
    }
}

fn empty_entity_bootstrap_frontier(path: &Path) -> riffdb_types::DualFrontier {
    drop(RedbStore::open(path).expect("materialize bootstrap boundary"));
    let database = Database::open(path).expect("open bootstrap boundary");
    let read = database.begin_read().expect("begin bootstrap read");
    assert_eq!(
        read.open_table(TableDefinition::<&[u8], &[u8]>::new("commits"))
            .expect("open commits")
            .len()
            .expect("commits length"),
        0,
        "the V2 tail fixture starts before its first application command"
    );
    assert_eq!(
        read.open_table(ENTITIES_RAW)
            .expect("open entities")
            .len()
            .expect("entities length"),
        0
    );
    assert_eq!(
        read.open_table(TableDefinition::<&[u8], &[u8]>::new("entity_chain_heads",))
            .expect("open entity heads")
            .len()
            .expect("entity-head length"),
        0
    );
    let audit = read.open_table(AUDIT).expect("open audit");
    let mut administration = None;
    for row in audit.iter().expect("iterate audit") {
        let key = row.expect("audit row").0.value().to_vec();
        let sequence = u64::from_be_bytes(
            key.get(key.len().saturating_sub(8)..)
                .expect("audit key sequence")
                .try_into()
                .expect("eight-byte audit sequence"),
        );
        administration = AdministrationSequence::new(sequence);
    }
    riffdb_types::DualFrontier::new(None, administration)
}

fn read_raw_entity_state(path: &Path, key: &[u8]) -> (Vec<u8>, Vec<u8>) {
    drop(RedbStore::open(path).expect("materialize V2 final state"));
    let database = Database::open(path).expect("open V2 final state");
    let read = database.begin_read().expect("begin V2 final read");
    let entity = read
        .open_table(ENTITIES_RAW)
        .expect("open final entities")
        .get(key)
        .expect("get final entity")
        .expect("final entity present")
        .value()
        .to_vec();
    let head = read
        .open_table(TableDefinition::<&[u8], &[u8]>::new("entity_chain_heads"))
        .expect("open final entity heads")
        .get(key)
        .expect("get final entity head")
        .expect("final entity head present")
        .value()
        .to_vec();
    (entity, head)
}

#[test]
fn v2_emitter_and_follower_resume_from_bootstrap_with_exact_entity_heads() {
    let path = TestDatabasePath::new("changelog-v2-entity-tail");
    prepare_command_database(&path.0);
    let predecessor = empty_entity_bootstrap_frontier(&path.0);
    let receipt = ChangelogV2RotationReceipt::new(database_id(), 1, predecessor, [0x52; 32])
        .expect("V2 bootstrap boundary");
    let consumer = std::sync::Arc::new(RecordingChangelogConsumerV2::default());
    let emitter = changelog_compatibility::start_changelog_emitter_v2(
        std::sync::Arc::clone(&consumer) as std::sync::Arc<dyn ChangelogFrameConsumerV2>,
        receipt,
        64,
    )
    .expect("start V2 emitter");

    let first = command_fixture_at(1);
    let second = superseding_command_fixture_at(2, 1, &first);
    {
        let ports = open_with_emitter(&path.0, emitter.port());
        fence_deferred_epoch(&ports, std::slice::from_ref(&first));
        fence_deferred_epoch(&ports, std::slice::from_ref(&second));
        assert_eq!(
            emitter.emitter().wait_for_emitted(2),
            ChangelogEmissionStateV1::Streaming
        );
    }
    assert!(consumer.resyncs().is_empty());
    let encoded = consumer.encoded();
    assert_eq!(encoded.len(), 2);

    let empty_fingerprint = EntityTransitionFingerprint::from_sorted_heads(std::iter::empty())
        .expect("empty head fingerprint");
    let manifest = EntityReplicaBootstrapManifestV2::new(
        receipt,
        predecessor,
        receipt.v2_chain_anchor(),
        ValidatedPrefixEntityTransitionCounts {
            live_entity_count: 0,
            deleted_entity_count: 0,
            entity_transition_count: 0,
        },
        empty_fingerprint,
    )
    .expect("empty entity bootstrap manifest");
    let mut follower = DeleteAwareEntityFollowerV2::from_bootstrap(receipt, manifest)
        .expect("manifest belongs to rotation receipt");
    follower
        .install_bootstrap_page(Vec::new(), true)
        .expect("seal empty entity bootstrap");
    let (first_frame, _) = ChangelogFrameV2::decode(&encoded[0]).expect("decode first V2 frame");
    let incomplete = ChangelogFrameV2::new(
        first_frame.header().binding(),
        first_frame.header().predecessor(),
        first_frame.header().covered(),
        first_frame
            .entries()
            .iter()
            .filter(|entry| entry.class() != ChangelogEntryClassV2::EntityChainHead)
            .cloned()
            .collect(),
    )
    .expect("structurally valid but semantically incomplete frame")
    .encode()
    .expect("encode incomplete frame");
    assert_eq!(
        follower.apply_encoded(incomplete.as_bytes()),
        Err(riffdb_storage_api::ChangelogFrameV2Error::InvalidEntry)
    );
    assert_eq!(
        follower.applied_frontier(),
        predecessor,
        "failed frame validation cannot advance the follower cursor"
    );
    assert!(
        follower
            .entity_value(expected_entity_row(&first).0.as_slice())
            .is_none(),
        "failed frame validation cannot expose a partial entity apply"
    );
    for frame in &encoded {
        follower.apply_encoded(frame).expect("apply V2 frame");
    }

    let (key, expected_entity) = expected_entity_row(&second);
    let (actual_entity, actual_head) = read_raw_entity_state(&path.0, &key);
    assert_eq!(actual_entity, expected_entity);
    assert_eq!(follower.entity_value(&key), Some(actual_entity.as_slice()));
    assert_eq!(
        follower.chain_head_value(&key),
        Some(actual_head.as_slice())
    );
    assert_eq!(
        follower.applied_frontier().application(),
        CommitSequence::new(2)
    );
}

#[test]
fn delete_removes_current_index_state_and_emits_reciprocal_v2_tombstone() {
    let path = TestDatabasePath::new("delete-index-changelog-v2");
    prepare_command_database(&path.0);
    let predecessor = empty_entity_bootstrap_frontier(&path.0);
    let receipt = ChangelogV2RotationReceipt::new(database_id(), 1, predecessor, [0x53; 32])
        .expect("delete V2 bootstrap boundary");
    let consumer = std::sync::Arc::new(RecordingChangelogConsumerV2::default());
    let emitter = changelog_compatibility::start_changelog_emitter_v2(
        std::sync::Arc::clone(&consumer) as std::sync::Arc<dyn ChangelogFrameConsumerV2>,
        receipt,
        64,
    )
    .expect("start delete-aware V2 emitter");

    let create = command_fixture_at(1);
    let delete = deleting_command_fixture_at(2, &create);
    {
        let ports = open_with_emitter(&path.0, emitter.port());
        fence_deferred_epoch(&ports, std::slice::from_ref(&create));
        fence_deferred_epoch(&ports, std::slice::from_ref(&delete));
        assert_eq!(
            emitter.emitter().wait_for_emitted(2),
            ChangelogEmissionStateV1::Streaming
        );
        assert!(
            ports
                .read_entity(&create.target)
                .expect("read deleted entity")
                .is_none(),
            "delete removes the authoritative current entity"
        );
        let page = ports
            .scan_index(
                AuthoritativeIndexScanRequest::new(
                    create.range.clone(),
                    None,
                    StorageScanLimit::new(1).expect("delete index scan limit"),
                )
                .expect("delete index scan request"),
            )
            .expect("scan deleted index range");
        assert!(matches!(
            page,
            AuthoritativeIndexScanPage::ExactEnd { entries, .. } if entries.is_empty()
        ));
    }

    assert!(consumer.resyncs().is_empty());
    let encoded = consumer.encoded();
    assert_eq!(encoded.len(), 2);
    let (delete_frame, _) = ChangelogFrameV2::decode(&encoded[1]).expect("decode delete frame");
    assert_eq!(
        delete_frame
            .entries()
            .iter()
            .filter(|entry| entry.class() == ChangelogEntryClassV2::EntityDeleteTombstone)
            .count(),
        1,
        "one deleted entity has one exact V2 tombstone"
    );
    assert_eq!(
        delete_frame
            .entries()
            .iter()
            .filter(|entry| entry.class() == ChangelogEntryClassV2::EntityChainHead)
            .count(),
        1,
        "the tombstone is reciprocal with one final deleted chain head"
    );

    let empty_fingerprint = EntityTransitionFingerprint::from_sorted_heads(std::iter::empty())
        .expect("empty delete follower fingerprint");
    let manifest = EntityReplicaBootstrapManifestV2::new(
        receipt,
        predecessor,
        receipt.v2_chain_anchor(),
        ValidatedPrefixEntityTransitionCounts {
            live_entity_count: 0,
            deleted_entity_count: 0,
            entity_transition_count: 0,
        },
        empty_fingerprint,
    )
    .expect("empty delete follower manifest");
    let mut follower = DeleteAwareEntityFollowerV2::from_bootstrap(receipt, manifest)
        .expect("delete follower bootstrap identity");
    follower
        .install_bootstrap_page(Vec::new(), true)
        .expect("seal empty delete follower bootstrap");
    for frame in &encoded {
        follower
            .apply_encoded(frame)
            .expect("apply create/delete frame");
    }
    assert!(
        follower
            .entity_value(create.target.key().as_bytes())
            .is_none(),
        "the follower converges to deleted current state"
    );
    assert!(
        follower
            .chain_head_value(create.target.key().as_bytes())
            .is_some(),
        "the follower retains the deletion chain head"
    );

    let ports = open_operational(RedbStore::open(&path.0).expect("reopen deleted database"));
    assert!(
        ports
            .read_entity(&create.target)
            .expect("read deleted entity after recovery")
            .is_none()
    );
    let page = ports
        .scan_index(
            AuthoritativeIndexScanRequest::new(
                create.range.clone(),
                None,
                StorageScanLimit::new(1).expect("recovered delete index scan limit"),
            )
            .expect("recovered delete index scan request"),
        )
        .expect("scan deleted index after recovery");
    assert!(matches!(
        page,
        AuthoritativeIndexScanPage::ExactEnd { entries, .. } if entries.is_empty()
    ));
}

#[test]
fn emitted_frames_form_one_gap_free_checksummed_chain() {
    let path = TestDatabasePath::new("changelog-chain");
    prepare_command_database(&path.0);
    let (consumer, emitter) = start_recording_emitter(64);
    {
        let ports = open_with_emitter(&path.0, emitter.port());
        fence_deferred_epoch(&ports, &[command_fixture_at(1)]);
        fence_deferred_epoch(&ports, &[command_fixture_at(2)]);
        fence_deferred_epoch(&ports, &[command_fixture_at(3)]);
        assert_eq!(
            emitter.emitter().wait_for_emitted(3),
            ChangelogEmissionStateV1::Streaming
        );
    }
    let encoded = consumer.encoded();
    assert_eq!(encoded.len(), 3);
    let frames = consumer.frames();
    let header = frames[0].header();

    let mut validator = ChangelogStreamValidatorV1::anchored_at(
        header.database_id(),
        header.history_incarnation(),
        header.predecessor(),
        [0; 32],
    );
    for bytes in &encoded {
        validator
            .accept(bytes)
            .expect("the chain validates exactly");
    }
    assert_eq!(
        validator.expected_predecessor(),
        frames[2].header().covered()
    );

    // The bound journal frame hash is covered by the chain: editing it in the
    // first frame breaks the second, even after re-checksumming.
    let mut tampered = encoded[0].clone();
    let offset = 76 + 4 * riffdb_storage_api::CHANGELOG_ENTRY_CLASS_COUNT + 4 + 32;
    tampered[offset] ^= 0xff;
    let mut fresh = ChangelogStreamValidatorV1::anchored_at(
        header.database_id(),
        header.history_incarnation(),
        header.predecessor(),
        [0; 32],
    );
    assert!(
        matches!(
            fresh.accept(&tampered),
            Err(riffdb_storage_api::ChangelogFrameError::ChecksumMismatch)
        ),
        "a corrupted authoritative-chain binding fails its own checksum"
    );
}

#[test]
fn the_emitter_never_observes_an_applied_but_unflushed_subgroup() {
    let path = TestDatabasePath::new("changelog-unflushed-gate");
    prepare_command_database(&path.0);
    let (consumer, emitter) = start_recording_emitter(64);
    let ports = open_with_emitter(&path.0, emitter.port());

    // Two subgroups are applied through `Durability::None`. The first is
    // sealed and its flush is in flight; the second is sealed behind it and is
    // therefore APPLIED BUT UNFLUSHED, sitting in the writer-private composite
    // frontier, when the first publication happens. A third is left open and
    // unsealed. None of the later state may appear in the first frame.
    let first = ports
        .begin_deferred_command_epoch()
        .expect("begin first durability epoch");
    let first_fence = DeferredCommandEpoch::seal(apply_unpublished_command_fixture(
        first,
        &command_fixture_at(1),
    ))
    .expect("seal first epoch");

    let sealed_sibling = ports
        .begin_deferred_command_epoch()
        .expect("begin the applied-but-unflushed sibling");
    let sibling_fence = DeferredCommandEpoch::seal(apply_unpublished_command_fixture(
        sealed_sibling,
        &command_fixture_at(2),
    ))
    .expect("seal the applied-but-unflushed sibling");

    // Publication of the first epoch happens while the sibling is private.
    let committed = first_fence.wait().expect("publish the first epoch");
    assert_eq!(committed.len(), 1);
    assert_eq!(
        emitter.emitter().wait_for_emitted(1),
        ChangelogEmissionStateV1::Streaming,
        "the gate must not fire for a correctly published snapshot"
    );

    let frames = consumer.frames();
    assert_eq!(frames.len(), 1);
    let frame = &frames[0];
    assert_eq!(
        frame.header().covered().application(),
        CommitSequence::new(1)
    );
    let held_commit_key = 2_u64.to_be_bytes();
    assert!(
        frame.entries().iter().all(|entry| {
            entry.class() != ChangelogEntryClassV1::Commit
                || entry.key() != held_commit_key.as_slice()
        }),
        "the held applied-but-unflushed subgroup must be invisible to the framing pass"
    );
    assert!(
        frame.entries().iter().any(|entry| {
            entry.class() == ChangelogEntryClassV1::Commit
                && entry.key() == 1_u64.to_be_bytes().as_slice()
        }),
        "the published subgroup must be present"
    );
    assert!(consumer.resyncs().is_empty());

    // Releasing the sibling publishes it as its own frame, in order.
    sibling_fence.wait().expect("publish the sibling epoch");
    assert_eq!(
        emitter.emitter().wait_for_emitted(2),
        ChangelogEmissionStateV1::Streaming
    );
    let frames = consumer.frames();
    assert_eq!(
        frames[1].header().predecessor(),
        frames[0].header().covered()
    );
    assert_eq!(
        frames[1].header().covered().application(),
        CommitSequence::new(2)
    );
    assert!(frames[1].entries().iter().any(|entry| {
        entry.class() == ChangelogEntryClassV1::Commit && entry.key() == held_commit_key.as_slice()
    }));
}

#[test]
fn a_dropped_advancement_stops_the_emitter_with_a_typed_gap() {
    let path = TestDatabasePath::new("changelog-gap");
    prepare_command_database(&path.0);
    let (consumer, emitter) = start_recording_emitter(64);
    let dropping = std::sync::Arc::new(DroppingChangelogPort {
        inner: emitter.port(),
        drop_ordinal: 1,
        seen: AtomicU64::new(0),
    });
    {
        let ports = open_with_emitter(
            &path.0,
            dropping as std::sync::Arc<dyn riffdb_storage_api::ChangelogPublicationPort>,
        );
        fence_deferred_epoch(&ports, &[command_fixture_at(1)]);
        assert_eq!(
            emitter.emitter().wait_for_emitted(1),
            ChangelogEmissionStateV1::Streaming
        );
        fence_deferred_epoch(&ports, &[command_fixture_at(2)]);
        fence_deferred_epoch(&ports, &[command_fixture_at(3)]);
        assert_eq!(
            emitter.emitter().wait_for_resync(),
            ChangelogResyncReasonV1::FrontierGap
        );
    }
    assert_eq!(
        consumer.frames().len(),
        1,
        "the emitter stops rather than emitting across a gap"
    );
    assert_eq!(
        consumer.resyncs(),
        vec![ChangelogResyncReasonV1::FrontierGap]
    );
    assert_eq!(
        emitter.emitter().state(),
        ChangelogEmissionStateV1::Resync(ChangelogResyncReasonV1::FrontierGap)
    );
}

#[test]
fn a_stalled_consumer_never_delays_publication_and_overflow_is_typed_lagging() {
    let path = TestDatabasePath::new("changelog-nonblocking");
    prepare_command_database(&path.0);
    let consumer = std::sync::Arc::new(RecordingChangelogConsumer::stalled());
    let emitter = changelog_compatibility::start_changelog_emitter(
        std::sync::Arc::clone(&consumer) as std::sync::Arc<dyn ChangelogFrameConsumer>,
        1,
    )
    .expect("start changelog emitter");
    {
        let ports = open_with_emitter(&path.0, emitter.port());
        // The consumer is parked inside `accept_frame` for the whole workload.
        // Every publication below must still complete: if the port applied any
        // backpressure this test would never finish.
        for ordinal in 1..=6 {
            fence_deferred_epoch(&ports, &[command_fixture_at(ordinal)]);
        }
        assert_eq!(
            emitter.emitter().observed_advancements(),
            6,
            "the publication edge offered every advancement without blocking"
        );
        assert_eq!(
            emitter.emitter().wait_for_resync(),
            ChangelogResyncReasonV1::BufferOverflow
        );
        // The writer is entirely unaffected: every command is committed.
        for ordinal in 1..=6 {
            let fixture = command_fixture_at(ordinal);
            assert_eq!(
                ports
                    .read_entity(&fixture.target)
                    .expect("read committed entity"),
                Some(fixture.records.entities()[0].post_image().clone())
            );
        }
        consumer.release();
    }
    assert_eq!(
        emitter.emitter().state(),
        ChangelogEmissionStateV1::Resync(ChangelogResyncReasonV1::BufferOverflow),
        "overflow is typed lagging, never backpressure"
    );
}

#[test]
fn the_default_publication_port_observes_nothing_and_changes_no_behavior() {
    let path = TestDatabasePath::new("changelog-default-port");
    prepare_command_database(&path.0);
    let expected = {
        let ports = open_operational(RedbStore::open(&path.0).expect("reopen"));
        fence_deferred_epoch(&ports, &[command_fixture_at(1)]);
        drop(ports);
        read_claimed_rows(&path.0)
    };

    let observed_path = TestDatabasePath::new("changelog-default-port-observed");
    prepare_command_database(&observed_path.0);
    let (_consumer, emitter) = start_recording_emitter(64);
    let ports = open_with_emitter(&observed_path.0, emitter.port());
    fence_deferred_epoch(&ports, &[command_fixture_at(1)]);
    assert_eq!(
        emitter.emitter().wait_for_emitted(1),
        ChangelogEmissionStateV1::Streaming
    );
    drop(ports);
    assert_eq!(
        read_claimed_rows(&observed_path.0),
        expected,
        "installing an observer changes no durable byte"
    );
}

/// Returns the exact stored bytes of a fixture's entity post-image.
fn expected_entity_row(fixture: &CommandFixture) -> (Vec<u8>, Vec<u8>) {
    let post_image = fixture.records.entities()[0].post_image();
    let key = post_image.target().key().as_bytes().to_vec();
    let value = riffdb_storage_api::encode_entity_record_v1(post_image)
        .expect("encode entity post-image")
        .into_bytes();
    (key, value)
}

fn entity_entry_value(frame: &ChangelogFrameV1, key: &[u8]) -> Option<Vec<u8>> {
    frame
        .entries()
        .iter()
        .find(|entry| entry.class() == ChangelogEntryClassV1::Entity && entry.key() == key)
        .map(|entry| entry.value().to_vec())
}

#[test]
fn a_frame_carries_the_post_image_of_its_own_frontier_not_a_later_one() {
    // Prefix exactness at an INTERMEDIATE frame boundary. Two commits write the
    // same physical entity key (the ADR-0083 supersession chain), so the two
    // post-images differ in bytes at one key. A frame derived from a snapshot
    // later than its own covered frontier would carry the wrong one and still
    // converge to the right final state — which is exactly the failure mode a
    // final-state-only differential cannot see.
    let path = TestDatabasePath::new("changelog-supersession");
    prepare_command_database(&path.0);
    let anchor = read_claimed_rows(&path.0);

    let first = command_fixture_at(1);
    let second = superseding_command_fixture_at(2, 1, &first);
    let (entity_key, first_value) = expected_entity_row(&first);
    let (superseding_key, second_value) = expected_entity_row(&second);
    assert_eq!(
        entity_key, superseding_key,
        "the fixtures must occupy one physical entity key"
    );
    assert_ne!(
        first_value, second_value,
        "the two post-images must differ in bytes or nothing is observable"
    );

    let (consumer, emitter) = start_recording_emitter(64);
    {
        let ports = open_with_emitter(&path.0, emitter.port());
        fence_deferred_epoch(&ports, std::slice::from_ref(&first));
        fence_deferred_epoch(&ports, std::slice::from_ref(&second));
        assert_eq!(
            emitter.emitter().wait_for_emitted(2),
            ChangelogEmissionStateV1::Streaming
        );
    }
    let frames = consumer.frames();
    assert_eq!(frames.len(), 2);
    assert_eq!(
        frames[0].header().covered().application(),
        CommitSequence::new(1)
    );
    assert_eq!(
        frames[1].header().covered().application(),
        CommitSequence::new(2)
    );

    // The whole point: frame 1 carries the EARLIER post-image.
    assert_eq!(
        entity_entry_value(&frames[0], &entity_key).as_ref(),
        Some(&first_value),
        "frame 1 must carry the post-image as of its own covered frontier"
    );
    assert_eq!(
        entity_entry_value(&frames[1], &entity_key).as_ref(),
        Some(&second_value),
        "frame 2 must carry the superseding post-image"
    );

    // And the chain still reconstructs the final snapshot exactly, so the
    // intermediate assertion above is an addition to final-state exactness,
    // never a substitute for it.
    let final_rows = read_claimed_rows(&path.0);
    assert_eq!(apply_frames(anchor, &frames), final_rows);
    assert_eq!(
        final_rows
            .get(&("entities", entity_key))
            .expect("the superseded key survives in the final snapshot"),
        &second_value
    );
}

/// The physical AUDIT key: the backend's singleton-namespace prefix byte
/// followed by the big-endian administration sequence.
fn audit_key(sequence: AdministrationSequence) -> [u8; 9] {
    let mut key = [0_u8; 9];
    key[0] = 0x01;
    key[1..].copy_from_slice(&sequence.get().to_be_bytes());
    key
}

/// Replaces one existing physical AUDIT row and returns the bytes it displaced.
///
/// Requiring a prior row keeps the fixture honest: it can only corrupt a row the
/// storage boundary really wrote, never invent an audit stream position.
fn overwrite_raw_audit_row(path: &Path, sequence: AdministrationSequence, value: &[u8]) -> Vec<u8> {
    let database = Database::create(path).expect("open raw audit fixture");
    let transaction = database.begin_write().expect("begin raw audit write");
    let prior = {
        let mut table = transaction.open_table(AUDIT).expect("open raw audit table");
        table
            .insert(audit_key(sequence).as_slice(), value)
            .expect("write raw audit row")
            .map(|prior| prior.value().to_vec())
            .expect("an audit row the storage boundary already wrote")
    };
    transaction.commit().expect("commit raw audit fixture");
    prior
}

fn overwrite_raw_administration_allocator(path: &Path, allocator: AdministrationSequenceAllocator) {
    let encoded = encode_administration_sequence_allocator_v1(allocator)
        .expect("encode administration allocator");
    let database = Database::create(path).expect("open raw allocator fixture");
    let transaction = database.begin_write().expect("begin raw allocator write");
    {
        let mut table = transaction
            .open_table(META)
            .expect("open raw metadata table");
        table
            .insert(META_ADMINISTRATION_SEQUENCE, encoded.as_bytes())
            .expect("write raw administration allocator")
            .expect("an allocator the storage boundary already wrote");
    }
    transaction.commit().expect("commit raw allocator fixture");
}

/// Builds the complete two-phase durable shape: a durable `Pending` row and its
/// physical `Started` audit row from the audited-admission port, then the
/// command commit that consumes both and contributes only the terminal row.
fn prepare_two_phase_admitted_command(path: &Path) -> (CommandFixture, AdministrationSequence) {
    prepare_command_database(path);
    let fixture = two_phase_command_fixture_at(1);
    let ports = open_operational(RedbStore::open(path).expect("reopen for audited admission"));
    let admitted = admit_audited_command(&ports, &fixture);
    let started_sequence = admitted.started().administration_sequence();
    commit_two_phase_command_fixture(&ports, &fixture);
    drop(ports);
    (fixture, started_sequence)
}

/// A two-phase admitted command puts one audit record at one administration
/// sequence twice, and both carriers are legitimate.
///
/// Phase one appends the `Started` row physically, because the durable `Pending`
/// admission it accompanies must be atomic with it. Phase two commits the
/// command against that already-durable start, and the command segment's
/// manifest unconditionally names both audit members — so the segment derives an
/// entry at the same sequence the physical row already occupies.
///
/// The startup allocator matcher used to treat physical and segment-derived
/// audit sequences as disjoint sets and reported that overlap as an
/// authoritative `SequenceDiscontinuity`: the database refused to open on state
/// it had written itself. An equal pair at one sequence is one record — the same
/// join `read_administration_record_readonly` and every neighbouring
/// physical/derived reader already apply.
#[test]
fn two_phase_admitted_command_reopens_clean() {
    let path = TestDatabasePath::new("two-phase-admission");
    let (fixture, started_sequence) = prepare_two_phase_admitted_command(&path.0);
    assert_eq!(
        started_sequence,
        AdministrationSequence::new(2).expect("started administration sequence"),
        "catalog activation owns sequence 1; the audited admission owns sequence 2"
    );

    let findings = collect_structural_findings(
        RedbStore::open(&path.0).expect("reopen after the two-phase commit"),
    );
    assert!(
        findings.is_empty(),
        "a two-phase admitted command is legitimate durable state, not a \
         discontinuity: {findings:?}"
    );

    let ports = open_operational(RedbStore::open(&path.0).expect("open after two-phase commit"));
    assert_eq!(
        command_audit_phases(&ports),
        vec![ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Succeeded],
        "the physically admitted start and the committed terminal read back as \
         one contiguous lifecycle"
    );
    assert_eq!(
        ports
            .lookup_admission(fixture.candidates.clone())
            .expect("look up the committed identity"),
        AdmissionLookupResultV1::Found(Box::new(StoredAdmissionStateV1::StoredOutcome(
            fixture.records.stored_outcome().clone()
        ))),
        "phase two consumed the durable Pending row and left the terminal outcome"
    );

    // A completed startup pass installs the validated-prefix checkpoint, so the
    // next open takes the fast path. The overlap must be tolerated on both.
    drop(ports);
    let findings = collect_structural_findings(
        RedbStore::open(&path.0).expect("reopen after a completed startup pass"),
    );
    assert!(
        findings.is_empty(),
        "the checkpointed reopen must tolerate the overlap too: {findings:?}"
    );
}

/// A clean-close reopen must still resolve command-owned audit members.
///
/// A command-owned `Started`/`Terminal` pair is durable only inside its command
/// segment: `stage_command_service_audit_group_in_write` advances the
/// administration allocator without writing any `AUDIT` row, and no locator row
/// is ever encoded for the pair, so the command-derived transient index is its
/// only lookup structure. Clean-close fast startup (ADR-0156 section 5)
/// deliberately leaves that index cold, and a cold index used to answer "no
/// record" — which the administration read path cannot distinguish from a
/// durably absent record, because it joins the index against the physical row
/// and treats absence on both sides as corruption.
///
/// The stream scan therefore reported `CorruptData` for a database the writer
/// had just closed cleanly, with every audit record intact: the same class of
/// defect as `two_phase_admitted_command_reopens_clean` above, where the
/// database refused state it had written itself. WP-704 requires every
/// integrity fact skipped at clean startup to move onto the operational path
/// that first uses it, so the first administration-audit read populates the
/// index.
#[test]
fn clean_close_fast_startup_resolves_command_owned_audit_members() {
    let path = TestDatabasePath::new("clean-close-command-audit");
    prepare_command_database(&path.0);
    let fixture = command_fixture();
    let ports = open_operational(RedbStore::open(&path.0).expect("open command database"));
    commit_command_fixture(&ports, &fixture);
    assert_eq!(
        command_audit_phases(&ports),
        [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Succeeded],
        "the committing handle resolves the pair from its already-warm index"
    );
    ports
        .write_clean_close_lifecycle()
        .expect("write the final clean-close certificate");
    drop(ports);

    let ports =
        open_operational(RedbStore::open(&path.0).expect("reopen the cleanly closed database"));
    assert!(
        ports.clean_close_fast_startup(),
        "the certificate must select the fast path; without it this arm would \
         not cover a cold command-derived index"
    );
    assert_eq!(
        command_audit_phases(&ports),
        [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Succeeded],
        "a cold index must be populated on first use, never reported as an \
         absent audit record"
    );
}

/// An ordinary catalog read warms the cold index without blocking itself.
///
/// `read_active_catalog` opens a read transaction and then walks the entire
/// administration stream through `validate_administration_stream_readonly`, so
/// on a bounded clean start it is the first thing to need the cold
/// command-derived index — and it needs it while holding its own read
/// transaction open. Warming takes the exclusive mutation gate, so this is the
/// caller-holds-a-read-while-the-warm-takes-the-gate shape, and this arm proves
/// the real caller completes rather than deadlocking.
///
/// The companion unit test
/// `store::tests::warming_the_indexes_commits_under_the_callers_live_read_transaction`
/// forces the barrier's `begin_write` + `commit_durable` to run inside that
/// nesting, which is the part redb has to permit. Here the point is the public
/// caller: a cleanly restarted database must answer an ordinary catalog read.
#[test]
fn clean_close_fast_startup_warms_under_a_live_catalog_read() {
    let path = TestDatabasePath::new("clean-close-catalog-warm");
    prepare_command_database(&path.0);
    let fixture = command_fixture();
    let ports = open_operational(RedbStore::open(&path.0).expect("open command database"));
    commit_command_fixture(&ports, &fixture);
    ports
        .write_clean_close_lifecycle()
        .expect("write the final clean-close certificate");
    drop(ports);

    let ports =
        open_operational(RedbStore::open(&path.0).expect("reopen the cleanly closed database"));
    assert!(
        ports.clean_close_fast_startup(),
        "the certificate must select the fast path, leaving the index cold"
    );
    let active = riffdb_storage_api::CatalogRepository::read_active_catalog(&ports)
        .expect("an ordinary catalog read must answer on a cleanly closed database");
    assert!(
        active.is_some(),
        "the deployed fixture retains an active catalog pointer"
    );
    assert_eq!(
        command_audit_phases(&ports),
        [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Succeeded],
        "the warmed index resolves the command-owned pair"
    );
}

/// The tolerance is for an EQUAL pair only. Two different records at one
/// administration sequence remain a real discontinuity and must still refuse.
#[test]
fn conflicting_physical_and_derived_audit_records_refuse_open() {
    let path = TestDatabasePath::new("audit-pair-conflict");
    let (fixture, started_sequence) = prepare_two_phase_admitted_command(&path.0);

    // Same sequence and same request, different timestamp: the physical row and
    // the segment-derived record now describe two different records at one
    // administration sequence. That is not one record with two carriers.
    let (started, _) = command_audit_intents(&fixture);
    let conflicting = ServiceAuditAppendIntentV1::new(
        started.request_id(),
        Timestamp::new(1_700_000_009, 0).expect("conflicting started timestamp"),
        started.operation(),
        started.phase(),
        started
            .principal()
            .cloned()
            .expect("the durable start is principal-authenticated"),
        started.ingress(),
        started.targets().clone(),
        None,
        started.link(),
    )
    .expect("conflicting started audit");
    let conflicting = StoredServiceAuditRecordV1::from_intent(started_sequence, &conflicting);
    let encoded =
        encode_service_audit_record_v2(&conflicting).expect("encode the conflicting audit row");
    let displaced = overwrite_raw_audit_row(&path.0, started_sequence, encoded.as_bytes());
    assert_ne!(
        displaced,
        encoded.as_bytes(),
        "the fixture must actually change the physical row"
    );

    let findings =
        collect_structural_findings(RedbStore::open(&path.0).expect("reopen after the conflict"));
    assert!(
        findings.iter().any(|finding| {
            finding.scope() == StructuralFindingScope::Authoritative
                && finding.code() == StructuralFindingCode::SequenceDiscontinuity
        }),
        "a physical/derived pair that disagrees at one sequence is a genuine \
         discontinuity and must still be refused: {findings:?}"
    );
}

/// The matcher's other half: an allocator that runs ahead of the audit stream it
/// allocates from is still a discontinuity, overlap or no overlap.
#[test]
fn administration_allocator_ahead_of_the_audit_stream_refuses_open() {
    let path = TestDatabasePath::new("audit-allocator-gap");
    let (_, started_sequence) = prepare_two_phase_admitted_command(&path.0);
    let terminal_sequence = started_sequence
        .checked_next()
        .expect("terminal administration sequence");
    let gap = terminal_sequence
        .checked_next()
        .and_then(AdministrationSequence::checked_next)
        .expect("one sequence beyond the stream's exact frontier");
    overwrite_raw_administration_allocator(&path.0, AdministrationSequenceAllocator::next(gap));

    let findings = collect_structural_findings(
        RedbStore::open(&path.0).expect("reopen after the allocator gap"),
    );
    assert!(
        findings.iter().any(|finding| {
            finding.scope() == StructuralFindingScope::Authoritative
                && finding.code() == StructuralFindingCode::SequenceDiscontinuity
        }),
        "an allocator ahead of the complete physical-plus-derived audit coverage \
         must be refused: {findings:?}"
    );
}

/// First coverage for the audited-admission port: the durable `Pending` row and
/// its physical `Started` audit row are one atomic transition, and re-admitting
/// the same identity resumes the unchanged pending state.
#[test]
fn audited_admission_writes_pending_and_started_atomically() {
    let path = TestDatabasePath::new("audited-admission");
    prepare_command_database(&path.0);
    let fixture = two_phase_command_fixture_at(1);
    let ports = open_operational(RedbStore::open(&path.0).expect("reopen for audited admission"));

    let admitted = admit_audited_command(&ports, &fixture);
    assert_eq!(
        admitted.admission(),
        &AdmissionResultV1::Created(fixture.pending.clone()),
        "a vacant identity is created, not resumed"
    );
    let started = admitted.started();
    assert_eq!(started.phase(), ServiceAuditPhaseV1::Started);
    assert_eq!(started.link(), ServiceAuditLinkV1::None);
    assert_eq!(
        started.request_id(),
        fixture.pending.admission_request_id(),
        "the created admission and its start name one invocation"
    );
    assert_eq!(
        started.administration_sequence(),
        AdministrationSequence::new(2).expect("started administration sequence")
    );
    assert_eq!(
        ports
            .lookup_admission(fixture.candidates.clone())
            .expect("look up the admitted identity"),
        AdmissionLookupResultV1::Found(Box::new(StoredAdmissionStateV1::Pending(
            fixture.pending.clone()
        )))
    );
    assert_eq!(
        command_audit_phases(&ports),
        vec![ServiceAuditPhaseV1::Started]
    );

    // Atomicity is only meaningful across the durable boundary: both rows must
    // survive a reopen together, on their own, without any command graph.
    drop(ports);
    let findings = collect_structural_findings(
        RedbStore::open(&path.0).expect("reopen after the audited admission"),
    );
    assert!(
        findings.is_empty(),
        "a durable Pending row and its physical start are valid state: {findings:?}"
    );
    let ports = open_operational(RedbStore::open(&path.0).expect("open after audited admission"));
    assert_eq!(
        ports
            .lookup_admission(fixture.candidates.clone())
            .expect("look up the admitted identity after reopen"),
        AdmissionLookupResultV1::Found(Box::new(StoredAdmissionStateV1::Pending(
            fixture.pending.clone()
        )))
    );
    assert_eq!(
        command_audit_phases(&ports),
        vec![ServiceAuditPhaseV1::Started]
    );

    // A second invocation of the same identity resumes the unchanged durable
    // pending state and appends its own start; it never creates a second row.
    let (started, _) = command_audit_intents(&fixture);
    let resumed_start = ServiceAuditAppendIntentV1::new(
        RequestId::from_bytes(uuid_bytes(0x77)).expect("resuming request ID"),
        Timestamp::new(1_700_000_004, 0).expect("resuming started timestamp"),
        started.operation(),
        started.phase(),
        started
            .principal()
            .cloned()
            .expect("the durable start is principal-authenticated"),
        started.ingress(),
        started.targets().clone(),
        None,
        started.link(),
    )
    .expect("resuming started audit");
    let request = AuditedAdmissionRequestV1::new(
        AdmissionRequestV1::new(fixture.candidates.clone(), &fixture.context)
            .expect("resuming admission request"),
        resumed_start,
    )
    .expect("resuming audited admission request");
    let mut resumed = ports
        .admit_or_resolve_audited_group(vec![request])
        .expect("resume the audited admission");
    assert_eq!(resumed.len(), 1);
    let resumed: AuditedAdmissionResultV1 = resumed.pop().expect("one resumed result");
    assert_eq!(
        resumed.admission(),
        &AdmissionResultV1::Resumed(fixture.pending.clone()),
        "the existing pending state is replayed unchanged"
    );
    assert_eq!(
        resumed.started().administration_sequence(),
        AdministrationSequence::new(3).expect("resumed administration sequence"),
        "the resumed invocation still appends exactly one new audit row"
    );
    assert_eq!(
        command_audit_phases(&ports),
        vec![ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Started]
    );
}

// ---------------------------------------------------------------------------
// Covering-index durability and the startup/read detection asymmetry.
//
// SPEC.md states that for a V14 covering index the coordinator MUST derive the
// exact canonical covered record from the entity post-image, and that
// "missing, stale, malformed, duplicate, wrong-lineage, or non-derivable
// coverage is corruption and MUST fail closed without entity-read fallback".
// The read path enforces exactly that: it hard-checks the stored cover
// field-ID set against the plan's and returns a backend-integrity error on any
// disagreement.
//
// Startup structural evidence does not. `inspect_index_row` checks key decode,
// value decode, and the schema-binding cross-link, and nothing else — it has
// no contract in scope, so it cannot know what an entry's cover should be.
// These two arms hold both halves of that asymmetry in place: a complete cover
// survives a full close and reopen and serves its covered read, and an
// incomplete cover survives every startup check and is caught only by the
// read. The second arm is the durable evidence that a database in this state
// reports healthy.
// ---------------------------------------------------------------------------

/// The live application contract: `by_board_project_status` covers three
/// fields and the board pages already compile to covered plans against it.
const COVERED_CONTRACT: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../examples/app-baseline/contracts/ticketdesk.riff"
));

/// A board page: a covered plan over `by_board_project_status`.
const COVERED_BOARD_QUERY: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../queries/ticketdesk/board_page_450.riffq"
));

const COVERED_ORGANIZATION: [u8; 16] = [0x11; 16];
const COVERED_PROJECT: [u8; 16] = [0x22; 16];

/// Which cover a fixture entry carries.
#[derive(Clone, Copy, Eq, PartialEq)]
enum FixtureCover {
    /// Exactly the declared cover, derived from the entity post-image.
    Complete,
    /// No covered fields at all — the shape a rebuild that skipped cover
    /// derivation leaves behind. Structurally valid, semantically corrupt.
    Empty,
}

fn covered_contract_bundle() -> &'static ValidatedContractBundle {
    static BUNDLE: OnceLock<ValidatedContractBundle> = OnceLock::new();
    BUNDLE.get_or_init(|| {
        ValidatedContractBundle::from_compiler_bundle(
            compile_contract_source(COVERED_CONTRACT).expect("compile covering-index contract"),
        )
        .expect("validate covering-index bundle")
    })
}

/// Builds one durable covering-index entry for a Ticket, with the requested
/// cover shape. Every component comes from the contract's own schema, so the
/// row is exactly what the engine would write for this contract.
fn covering_index_entry(ordinal: u8, cover: FixtureCover) -> StoredIndexEntryV2 {
    let bundle = covered_contract_bundle();
    let schema = bundle.bundle().schema();
    let ticket = schema
        .entities()
        .iter()
        .find(|entity| entity.name() == "Ticket")
        .expect("Ticket entity");
    let index = ticket
        .indexes()
        .iter()
        .find(|index| index.name() == "by_board_project_status")
        .expect("covering index");
    let status = schema
        .enums()
        .iter()
        .find(|enumeration| enumeration.name() == "TicketStatus")
        .expect("TicketStatus");
    let open = CanonicalValue::Enum {
        type_id: status.id(),
        variant_id: status
            .variants()
            .iter()
            .find(|variant| variant.name() == "Open")
            .expect("Open variant")
            .id(),
    };
    let organization = CanonicalValue::Uuid(COVERED_ORGANIZATION);
    let project = CanonicalValue::Uuid(COVERED_PROJECT);
    let ticket_id = CanonicalValue::Uuid(covered_uuid(ordinal));

    let entity_key = ticket
        .primary_key()
        .encode_entity(&[organization.clone(), ticket_id.clone()])
        .expect("Ticket entity key");
    let index_key = index
        .key_schema()
        .encode_index(
            &[organization.clone(), project, open, ticket_id],
            entity_key,
        )
        .expect("covering index key");
    let partition = schema
        .aggregates()
        .iter()
        .find(|aggregate| aggregate.name() == "Tickets")
        .expect("Tickets aggregate")
        .keys()
        .partition_schema()
        .encode_partition(std::slice::from_ref(&organization))
        .expect("Ticket partition key");

    let covered_values = match cover {
        FixtureCover::Complete => {
            let field = |name: &str| {
                ticket
                    .record()
                    .fields()
                    .iter()
                    .find(|field| field.name() == name)
                    .expect("covered field is declared on Ticket")
                    .id()
            };
            CanonicalRecord::new(vec![
                (
                    field("title"),
                    CanonicalValue::string(format!("covered board ticket {ordinal}"))
                        .expect("title"),
                ),
                (
                    field("reporter_id"),
                    CanonicalValue::Uuid(covered_uuid(0x40 | ordinal)),
                ),
                (
                    field("assignee_id"),
                    CanonicalValue::Uuid(covered_uuid(0x50 | ordinal)),
                ),
            ])
            .expect("complete canonical cover")
        }
        FixtureCover::Empty => CanonicalRecord::new(Vec::new()).expect("empty cover"),
    };

    StoredIndexEntryV2::new(
        index_key,
        DurableKeySchemaBindingV1::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
        ),
        covered_values,
        partition,
    )
    .expect("durable covering-index entry")
}

fn covered_uuid(ordinal: u8) -> [u8; 16] {
    let mut bytes = [0u8; 16];
    bytes[0] = ordinal;
    bytes[6] = 0x70 | (bytes[6] & 0x0f);
    bytes[8] = 0x80 | (bytes[8] & 0x3f);
    bytes
}

/// Activates the covering-index contract and installs the given durable
/// secondary-index rows behind the store, exactly as a rebuild would leave
/// them.
/// A contract whose aggregate root no command can create. `ParcelItem` is the
/// only entity any command writes, so a `Parcels` partition holds children with
/// no root row -- exactly the shape ADR-0169 permits.
const ROOTLESS_CONTRACT: &str = r#"
contract RootlessAggregate version 1 {
  entity ParcelItem {
    key (parcel_id: u64, item_id: u64)
    field quantity: u64
  }

  entity Parcel {
    key (parcel_id: u64)
  }

  aggregate Parcels {
    root Parcel
    child ParcelItem
    partition_by parcel_id
    conflict_key (parcel_id)
  }

  command AddParcelItem {
    input idempotency_key: string<128>
    input parcel_id: u64
    input item_id: u64
    input quantity: u64

    idempotency_key idempotency_key
    create ParcelItem(parcel_id, item_id) as item
      else ParcelItemAlreadyExists { parcel_id: parcel_id }

    set item.quantity = quantity

    return ParcelItemAdded { item: item }
  }
}
"#;

fn rootless_contract_bundle() -> &'static ValidatedContractBundle {
    static BUNDLE: OnceLock<ValidatedContractBundle> = OnceLock::new();
    BUNDLE.get_or_init(|| {
        ValidatedContractBundle::from_compiler_bundle(
            compile_contract_source(ROOTLESS_CONTRACT).expect("compile rootless contract"),
        )
        .expect("validate rootless bundle")
    })
}

/// ADR-0169: an aggregate root need not be materialized.
///
/// The compile is the load-bearing half -- if the compiler grew a rule that
/// every root needs a creating command, this fails here rather than anywhere
/// downstream. The assertions after it state the premise instead of assuming
/// it: the contract really does declare a root, and really does declare no
/// command that can create one.
#[test]
fn an_aggregate_root_that_no_command_creates_compiles_and_activates() {
    let bundle = rootless_contract_bundle();
    let aggregate = bundle
        .bundle()
        .schema()
        .aggregates()
        .iter()
        .find(|aggregate| aggregate.name() == "Parcels")
        .expect("the Parcels aggregate");
    let root = bundle
        .bundle()
        .schema()
        .entity(aggregate.root())
        .expect("the declared root entity");
    assert_eq!(root.name(), "Parcel");
    assert_eq!(
        bundle.bundle().commands().len(),
        1,
        "AddParcelItem is the only command, so nothing can create a Parcel"
    );

    let path = TestDatabasePath::new("rootless-aggregate-activate");
    activate_rootless_catalog(&path.0);
}

/// Offline backup and retention administration are the two maintenance paths
/// ADR-0169 names that the OpenFGA adapter's conformance run cannot reach: it
/// proves reads, writes, changelog, and restart over a rootless aggregate, but
/// never exports, backs up, or prunes one. If either path grew a root lookup,
/// this fails rather than silently skipping the partition.
#[test]
fn offline_backup_and_retention_administer_a_rootless_aggregate() {
    let path = TestDatabasePath::new("rootless-aggregate-maintenance");
    activate_rootless_catalog(&path.0);

    use riffdb_storage_api::{BackupBuildMetadataV1, OfflineBackupPersistencePort};

    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    maintenance
        .status()
        .expect("retention status over a rootless aggregate");
    maintenance
        .add_hold("rootless", 1, "ADR-0169 rootless retention hold")
        .expect("install a hold over a rootless aggregate");

    let scope = ScratchScope::new("rootless-backup");
    let backup_dir = scope.path().join("backup");
    let build = BackupBuildMetadataV1::new(
        "0.1.0",
        "0123456789abcdef",
        "rustc-1.97.0",
        1,
        vec!["rootless".to_owned()],
    )
    .expect("build metadata");
    let manifest = riffdb_storage_redb::RedbOfflineBackup::bind(&path.0, &backup_dir)
        .create_offline_backup(&build)
        .expect("offline backup over a rootless aggregate");
    assert_eq!(manifest.database_id(), database_id());

    let findings = collect_structural_findings(
        RedbStore::open(&path.0).expect("reopen the rootless database"),
    );
    assert!(
        findings.is_empty(),
        "a rootless aggregate produces no structural finding: {findings:?}"
    );
}

/// ADR-0169 names export among the paths a rootless partition must support.
///
/// Export pages by entity type: `read_application_export_entity_page` takes an
/// `EntityTypeId` and builds a key prefix from it, never traversing root to
/// child. This arm holds that shape against a database whose aggregate root no
/// command can create -- the snapshot captures, binds the active lineage, and
/// serves the child entity type while the root type is empty.
///
/// What it does not establish: that a *populated* child page exports. Writing a
/// committed row into a rootless partition needs a command fixture bound to a
/// second contract, and this file's fixture builder is bound to
/// `STORAGE_RECOVERY_CONTRACT` throughout. A fixture assembled wrongly would
/// pass without exercising anything, which is worse than a stated limit.
#[test]
fn export_captures_a_snapshot_over_a_rootless_aggregate() {
    use riffdb_storage_api::ApplicationExportSnapshotPort;

    let path = TestDatabasePath::new("rootless-aggregate-export");
    activate_rootless_catalog(&path.0);

    let ports = open_operational(RedbStore::open(&path.0).expect("reopen the rootless database"));
    let bundle = rootless_contract_bundle();
    let snapshot = ports
        .capture_application_export_snapshot(bundle.lineage())
        .expect("export snapshot over a rootless aggregate");
    assert_eq!(
        snapshot.binding().contract_bundle_hash(),
        bundle.bundle_hash()
    );

    let schema = bundle.bundle().schema();
    let child = schema
        .entities()
        .iter()
        .find(|entity| entity.name() == "ParcelItem")
        .expect("the ParcelItem child");
    let root = schema
        .entities()
        .iter()
        .find(|entity| entity.name() == "Parcel")
        .expect("the Parcel root");

    // The child type is reachable without the root existing, which is the
    // property under test; both pages are empty because nothing was committed.
    let child_page = snapshot
        .read_application_export_entity_page(
            child.id(),
            None,
            StorageScanLimit::new(64).expect("export scan limit"),
        )
        .expect("child entity page over a rootless aggregate");
    assert!(child_page.records().is_empty());
    let root_page = snapshot
        .read_application_export_entity_page(
            root.id(),
            None,
            StorageScanLimit::new(64).expect("export scan limit"),
        )
        .expect("root entity page over a rootless aggregate");
    assert!(
        root_page.records().is_empty(),
        "no command can create a Parcel, so its page must be empty"
    );
}

/// Initializes one database and activates the rootless catalog on it.
fn activate_rootless_catalog(path: &Path) {
    let mut store = RedbStore::open(path).expect("open rootless database");
    store
        .initialize_database(database_id())
        .expect("initialize rootless database");
    let mut ports = open_operational(store);
    let stored = rootless_contract_bundle()
        .to_stored()
        .expect("stored rootless bundle");
    let result = ports
        .activate_catalog(&CatalogActivationIntentV1::new(
            None,
            stored,
            RequestId::from_bytes(uuid_bytes(0x69)).expect("rootless catalog request"),
            catalog_principal(),
            Timestamp::new(1_700_000_000, 0).expect("rootless catalog timestamp"),
            None,
        ))
        .expect("activate the rootless catalog");
    assert!(matches!(result, CatalogActivationResult::Activated { .. }));
}

fn seed_covering_index_database(path: &Path, entries: &[StoredIndexEntryV2]) {
    let mut store = RedbStore::open(path).expect("open covering-index database");
    store
        .initialize_database(database_id())
        .expect("initialize covering-index database");
    let mut ports = open_operational(store);
    let bundle = covered_contract_bundle()
        .to_stored()
        .expect("stored covering-index bundle");
    let result = ports
        .activate_catalog(&CatalogActivationIntentV1::new(
            None,
            bundle.clone(),
            RequestId::from_bytes(uuid_bytes(0x71)).expect("catalog request"),
            catalog_principal(),
            Timestamp::new(1_700_000_000, 0).expect("catalog timestamp"),
            None,
        ))
        .expect("activate the covering-index catalog");
    assert!(
        matches!(
            &result,
            CatalogActivationResult::Activated { active, .. }
                if *active == riffdb_storage_api::ActiveCatalogPointerV1::from_bundle(&bundle)
        ),
        "unexpected covering-index activation result: {result:?}"
    );
    drop(ports);

    let raw = entries
        .iter()
        .map(|entry| {
            (
                entry.key().clone(),
                encode_index_entry_v2(entry)
                    .expect("encode covering entry")
                    .as_bytes()
                    .to_vec(),
            )
        })
        .collect::<Vec<_>>();
    let previous = write_raw_index_entries(path, &raw);
    assert!(
        previous.iter().all(Option::is_none),
        "the fixture must introduce its covering rows, not replace existing ones"
    );
}

/// Compiles the board page and runs it against the reopened database.
fn execute_covered_board_page(
    ports: &RedbOperationalPorts,
) -> Result<riffdb_query_executor::QueryOwnedSnapshot, riffdb_query_executor::QueryExecutionError> {
    let bundle = covered_contract_bundle();
    let catalog = riffdb_query_ir::SymbolicCatalog::from_bundle(bundle.bundle())
        .expect("covering-index symbolic catalog");
    let program = riffdb_query_compiler::compile_query(
        &riffdb_riffql_syntax::parse_query(COVERED_BOARD_QUERY).expect("board page parses"),
        &catalog,
    )
    .expect("board page compiles");
    assert!(
        program.steps()[0].covered_result_layout().is_some(),
        "the board page must compile to a covered plan or these arms prove nothing"
    );
    let status = bundle
        .bundle()
        .schema()
        .enums()
        .iter()
        .find(|enumeration| enumeration.name() == "TicketStatus")
        .expect("TicketStatus");
    let parameters =
        riffdb_query_executor::QueryParameters::checked(std::collections::BTreeMap::from([
            (
                "organization_id".to_owned(),
                CanonicalValue::Uuid(COVERED_ORGANIZATION),
            ),
            (
                "project_id".to_owned(),
                CanonicalValue::Uuid(COVERED_PROJECT),
            ),
            (
                "status".to_owned(),
                CanonicalValue::Enum {
                    type_id: status.id(),
                    variant_id: status
                        .variants()
                        .iter()
                        .find(|variant| variant.name() == "Open")
                        .expect("Open variant")
                        .id(),
                },
            ),
        ]))
        .expect("board page parameters");
    riffdb_query_executor::QueryExecutionPort::execute_query(
        &riffdb_query_executor::StorageQueryExecutor::new(ports),
        &program,
        &parameters,
    )
}

/// A complete cover must survive a full durable close and startup
/// revalidation and still satisfy the covered read that consumes it.
#[test]
fn a_complete_covering_index_cover_survives_reopen_and_serves_its_covered_read() {
    let path = TestDatabasePath::new("covering-index-complete-cover");
    let entries = [
        covering_index_entry(1, FixtureCover::Complete),
        covering_index_entry(2, FixtureCover::Complete),
    ];
    seed_covering_index_database(&path.0, &entries);

    let ports =
        open_operational(RedbStore::open(&path.0).expect("reopen the covering-index database"));
    let snapshot = execute_covered_board_page(&ports).expect("the covered board page executes");
    let covered = snapshot
        .covered_result()
        .expect("the board page retains its positional covered result");
    assert_eq!(covered.entity(), "Ticket");
    assert_eq!(covered.rows().len(), 2, "both covered entries are visible");
    let title = covered
        .fields()
        .position(|field| field == "title")
        .expect("title is a covered field");
    assert_eq!(
        covered
            .rows()
            .iter()
            .map(|row| row[title].clone())
            .collect::<Vec<_>>(),
        vec![
            CanonicalValue::string("covered board ticket 1").expect("first title"),
            CanonicalValue::string("covered board ticket 2").expect("second title"),
        ],
        "the covered read serves the stored cover, in index order, without the entity table"
    );
}

/// An entry whose cover is empty passes EVERY startup structural check and is
/// caught only by the read that needs it.
///
/// This is the detection gap, held in place so it cannot close silently. A
/// database left in this state by a rebuild that skipped cover derivation
/// opens clean, reports no structural finding, validates its complete catalog
/// history — and then fails the board page closed with a backend-integrity
/// error. There is no startup signal, no statistic, and no repair path.
///
/// If a future change teaches startup to inspect covers, this arm fails on the
/// `findings.is_empty()` assertion inside the startup pass and must be
/// rewritten deliberately rather than deleted: a startup finding is fatal to
/// the whole database, not a warning, so making covers a structural finding
/// changes an affected installation from degraded to unopenable.
#[test]
fn an_empty_cover_passes_every_startup_check_and_fails_only_the_covered_read() {
    let path = TestDatabasePath::new("covering-index-empty-cover");
    let entries = [covering_index_entry(1, FixtureCover::Empty)];
    seed_covering_index_database(&path.0, &entries);

    // Startup structural evidence reports nothing at all.
    assert_eq!(
        collect_structural_findings(
            RedbStore::open(&path.0).expect("reopen for structural evidence")
        ),
        Vec::new(),
        "an empty cover is invisible to startup structural evidence"
    );

    // The durable row is exactly what a cover-skipping rebuild leaves.
    let persisted = read_raw_index_entries(&path.0);
    assert_eq!(persisted.len(), 1);
    assert!(
        decode_index_entry_v2(&persisted[0].1)
            .expect("the empty-cover entry decodes as a valid V2 row")
            .value()
            .covered_values()
            .fields()
            .is_empty(),
        "the fixture must persist an entry whose cover is empty"
    );

    // And the complete startup pass — structural drain plus catalog history —
    // opens the database as healthy.
    let ports =
        open_operational(RedbStore::open(&path.0).expect("reopen the empty-cover database"));

    // Only the covered read notices.
    assert_eq!(
        execute_covered_board_page(&ports).map(|_| ()),
        Err(riffdb_query_executor::QueryExecutionError::BackendIntegrity),
        "a covered plan must fail closed on an incomplete cover rather than \
         hydrating from the entity table"
    );
}

// ---- ADR-0170: one command writing two aggregates, atomic across a restart --
//
// The compiler proof, union conflict ownership, and executable IR V19 landed
// with WP-721; SPEC 1.19 recorded this crash arm as the one outstanding proof.
//
// Atomicity here is a property of the single redb write transaction rather than
// of aggregate count, which is a reason to expect the arm to pass — not a
// reason to skip it. The claim under test is that a command whose bindings span
// two aggregates lands all-or-nothing, so a crash cannot leave one aggregate's
// entity and index written while the other's are missing.

fn cross_aggregate_validated_bundle() -> &'static ValidatedContractBundle {
    static BUNDLE: OnceLock<ValidatedContractBundle> = OnceLock::new();
    BUNDLE.get_or_init(|| {
        ValidatedContractBundle::from_compiler_bundle(
            compile_contract_source(CROSS_AGGREGATE_RECOVERY_CONTRACT)
                .expect("compile cross-aggregate recovery contract"),
        )
        .expect("validate cross-aggregate recovery bundle")
    })
}

fn cross_aggregate_plan() -> ExecutablePlanRef {
    let bundle = cross_aggregate_validated_bundle();
    let command = bundle
        .bundle()
        .commands()
        .first()
        .expect("cross-aggregate command");
    // Premise check: this fixture is worthless if the command does not actually
    // span aggregates, which is exactly what an unnoticed contract edit would
    // do to it.
    assert!(
        command.locality().spans_aggregates(),
        "the cross-aggregate recovery command must span aggregates"
    );
    ExecutablePlanRef::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        command.command_id(),
        command.plan_hash(),
    )
}

/// One command's complete two-aggregate write set, plus the per-aggregate
/// coordinates an assertion needs to check each side independently.
#[derive(Clone)]
struct CrossAggregateFixture {
    candidates: IdempotencyLookupCandidatesV1,
    intent: riffdb_storage_api::CommitIntent,
    affected_targets: AffectedIndexEpochTargets,
    write_plan: CommandWriteSetPlanV1,
    records: AtomicCommandRecordSet,
    /// Aggregate 1's root entity (`Row`) and aggregate 2's (`Ledger`).
    targets: [EntityTarget; 2],
    /// Each aggregate's own index entry, stored under its own aggregate's
    /// partition key.
    index_keys: [IndexEntryKey; 2],
    ranges: [IndexRangeTarget; 2],
}

fn cross_aggregate_fixture() -> CrossAggregateFixture {
    let plan = cross_aggregate_plan();
    let sequence = CommitSequence::new(1).expect("cross-aggregate sequence");
    let id = 41_u64;
    let entry = 7_u64;
    let value = 23_u64;

    // Aggregate 1's root: Row(id).
    let row_type = EntityTypeId::new(1).expect("row entity type");
    let mut row_key = EntityKeyBuilder::new(row_type);
    row_key.push_u64(id).expect("row key component");
    let row_key = row_key.finish().expect("row key");
    let row_target = EntityTarget::new(row_type, row_key.clone()).expect("row target");

    // Aggregate 2's root: Ledger(id, entry).
    let ledger_type = EntityTypeId::new(2).expect("ledger entity type");
    let mut ledger_key = EntityKeyBuilder::new(ledger_type);
    ledger_key.push_u64(id).expect("ledger key component");
    ledger_key.push_u64(entry).expect("ledger entry component");
    let ledger_key = ledger_key.finish().expect("ledger key");
    let ledger_target = EntityTarget::new(ledger_type, ledger_key.clone()).expect("ledger target");

    // One partition route for the whole command, namespaced by the locality
    // aggregate. Index entries are partitioned by the command's partition key
    // rather than per-aggregate, matching `command_index`'s `command_partition`.
    let mut partition = PartitionKeyBuilder::new(AggregateTypeId::new(1).expect("aggregate"));
    partition.push_u64(id).expect("partition component");
    let partition = partition.finish().expect("partition key");

    let row_index_id = IndexId::new(1).expect("row index ID");
    let mut row_index_key = IndexEntryKeyBuilder::new(row_index_id);
    row_index_key.push_u64(value).expect("row index component");
    let row_index_key = row_index_key.finish(row_key).expect("row index entry key");
    let mut row_prefix = riffdb_storage_api::IndexRangePrefixBuilder::new(row_index_id);
    row_prefix.push_u64(value).expect("row range component");
    let row_range = IndexRangeTarget::new(partition.clone(), row_prefix.finish());

    // ADR-0170 + per-aggregate index partitioning: the Ledger's index lives
    // under `Ledgers`' partition key, the same route value in its own
    // aggregate namespace. Under the old one-key-per-command rule this entry
    // was unwritable and, if forced, made the database refuse to reopen.
    let mut ledger_partition =
        PartitionKeyBuilder::new(AggregateTypeId::new(2).expect("ledger aggregate"));
    ledger_partition
        .push_u64(id)
        .expect("ledger partition component");
    let ledger_partition = ledger_partition.finish().expect("ledger partition key");
    let ledger_index_id = IndexId::new(2).expect("ledger index ID");
    let mut ledger_index_key = IndexEntryKeyBuilder::new(ledger_index_id);
    ledger_index_key
        .push_u64(value)
        .expect("ledger index component");
    let ledger_index_key = ledger_index_key
        .finish(ledger_key.clone())
        .expect("ledger index entry key");
    let mut ledger_prefix = riffdb_storage_api::IndexRangePrefixBuilder::new(ledger_index_id);
    ledger_prefix
        .push_u64(value)
        .expect("ledger range component");
    let ledger_range = IndexRangeTarget::new(ledger_partition.clone(), ledger_prefix.finish());

    let tenant_scope = TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant"));
    let principal = ActorId::new("principal-a").expect("principal");
    let actor = riffdb_types::AdmittedActorContext::new(
        principal.clone(),
        ActorKind::Human,
        tenant_scope.clone(),
        None,
    );
    let identity = IdempotencyIdentity::new(
        database_id(),
        Environment::new("test").expect("environment"),
        tenant_scope,
        principal,
        plan.contract_lineage().clone(),
        plan.command_id(),
        IdempotencyKeyDigest::from_hmac_bytes(DigestKeyId::new(1).expect("digest key"), [0x71; 32]),
    );
    let request_id = RequestId::from_bytes(uuid_bytes(0x72)).expect("request ID");
    let provenance_id = ProvenanceId::from_bytes(uuid_bytes(0x73)).expect("provenance ID");
    let logical_time =
        LogicalTime::new(Timestamp::new(1_700_000_001, 0).expect("logical timestamp"));
    let pending = StoredPendingAdmissionV1::new(
        identity.clone(),
        CanonicalInputHash::from_bytes([0x74; 32]),
        request_id,
        plan.clone(),
        logical_time,
        actor.clone(),
        partition.clone(),
        StoredAdmittedProvenanceClaimsV1::default(),
    )
    .expect("cross-aggregate pending admission");

    let snapshot_request = SnapshotRequest::new(
        plan.clone(),
        vec![row_target.clone(), ledger_target.clone()],
        Vec::new(),
        Vec::new(),
    )
    .expect("cross-aggregate snapshot request");
    let snapshot = ReadSnapshot::new(
        &snapshot_request,
        None,
        vec![
            EntityObservation::Absent(row_target.clone()),
            EntityObservation::Absent(ledger_target.clone()),
        ],
        Vec::new(),
        Vec::new(),
    )
    .expect("cross-aggregate read snapshot");

    let row_post = EntityPostImage::new(row_target.clone(), plan.contract_version(), record(value))
        .expect("row post-image");
    let ledger_post = EntityPostImage::new(
        ledger_target.clone(),
        plan.contract_version(),
        record(value),
    )
    .expect("ledger post-image");
    let event_intent = EventIntent::new(EventTypeId::new(1).expect("event type"), record(value))
        .expect("event intent");
    let declared_outcome =
        DeclaredOutcome::new(OutcomeId::new(1).expect("outcome ID"), record(value))
            .expect("declared outcome");
    let evaluated = riffdb_storage_api::EvaluatedCommand::new(
        &snapshot,
        vec![
            EntityMutation::Create(row_post.clone()),
            EntityMutation::Create(ledger_post.clone()),
        ],
        vec![event_intent],
        declared_outcome.clone(),
        EvaluationBudget::v1(),
    )
    .expect("cross-aggregate evaluated command");

    let partition_hash = hash_partition_key(partition.as_bytes());
    let context = PreEvaluationCommitContext::new(pending.clone(), partition_hash, Vec::new())
        .expect("cross-aggregate commit context");
    let candidates =
        IdempotencyLookupCandidatesV1::new(vec![identity.clone()]).expect("lookup candidates");
    let intent = riffdb_storage_api::CommitIntent::new_for_vacant_terminal_admission(
        context,
        candidates.clone(),
        evaluated,
        provenance_id,
    )
    .expect("cross-aggregate commit intent");

    let schema_binding = DurableKeySchemaBindingV1::from_plan(&plan);
    let mutations = vec![
        riffdb_storage_api::CommittedEntityMutationV1::new(
            ExpectedEntityState::Absent,
            StoredEntityRecordV1::new(
                row_target.clone(),
                EntityVersion::first(),
                plan.contract_version(),
                schema_binding.clone(),
                record(value),
            )
            .expect("stored row"),
        )
        .expect("committed row mutation"),
        riffdb_storage_api::CommittedEntityMutationV1::new(
            ExpectedEntityState::Absent,
            StoredEntityRecordV1::new(
                ledger_target.clone(),
                EntityVersion::first(),
                plan.contract_version(),
                schema_binding.clone(),
                record(value),
            )
            .expect("stored ledger"),
        )
        .expect("committed ledger mutation"),
    ];

    let index_mutations = vec![
        IndexEntryMutationV1::Put(
            StoredIndexEntryV2::new(
                row_index_key.clone(),
                schema_binding.clone(),
                record(value),
                partition.clone(),
            )
            .expect("stored row index entry"),
        ),
        // Namespaced by `Ledgers`, not by the command's `Rows`.
        IndexEntryMutationV1::Put(
            StoredIndexEntryV2::new(
                ledger_index_key.clone(),
                schema_binding.clone(),
                record(value),
                ledger_partition.clone(),
            )
            .expect("stored ledger index entry"),
        ),
    ];

    let row_generation = PartitionIndexTarget::new(partition.clone(), row_index_id);
    let ledger_generation = PartitionIndexTarget::new(ledger_partition.clone(), ledger_index_id);
    let affected_targets =
        AffectedIndexEpochTargets::new(vec![row_generation.clone(), ledger_generation.clone()])
            .expect("cross-aggregate affected targets");
    let affected_current = AffectedEpochCurrentState::new(
        &affected_targets,
        vec![
            CurrentIndexGenerationObservation::new(
                row_generation.clone(),
                IndexEpochPosition::BeforeFirst,
            ),
            CurrentIndexGenerationObservation::new(
                ledger_generation.clone(),
                IndexEpochPosition::BeforeFirst,
            ),
        ],
    )
    .expect("cross-aggregate affected current state");
    let epoch_advances = vec![
        IndexEpochAdvanceV1::new(
            row_generation,
            schema_binding.clone(),
            IndexEpochPosition::BeforeFirst,
        )
        .expect("row epoch advance"),
        IndexEpochAdvanceV1::new(
            ledger_generation,
            schema_binding.clone(),
            IndexEpochPosition::BeforeFirst,
        )
        .expect("ledger epoch advance"),
    ];

    let upper_bound =
        match command_write_set_upper_bound_v1(&intent, &index_mutations, &epoch_advances)
            .expect("canonical encoded upper bound")
        {
            EncodedWriteSetUpperBoundResultV1::Fits(bound) => bound,
            EncodedWriteSetUpperBoundResultV1::ExceedsAcceptedAggregateCap(_) => {
                panic!("cross-aggregate recovery fixture must fit the accepted aggregate cap")
            }
        };
    let write_plan = CommandWriteSetPlanV1::new(
        &intent,
        affected_targets.clone(),
        affected_current,
        index_mutations,
        epoch_advances,
        upper_bound,
    )
    .expect("cross-aggregate write plan");

    let event_id = EventId::new(sequence, 0);
    let event_type_id = EventTypeId::new(1).expect("event type");
    let event_payload = record(value);
    let event = StoredDurableEventV1::new(
        event_id,
        event_type_id,
        event_payload.clone(),
        derive_event_hash_v1(event_id, event_type_id, &event_payload).expect("event hash"),
    )
    .expect("durable event");
    let stored_outcome = StoredOutcomeV1::new(
        identity.clone(),
        sequence,
        request_id,
        plan.clone(),
        pending.canonical_input_hash(),
        actor.clone(),
        logical_time,
        partition.clone(),
        partition_hash,
        Vec::new(),
        declared_outcome.clone(),
        StoredAdmittedProvenanceClaimsV1::default(),
        provenance_id,
        DurabilityMode::Sync,
    )
    .expect("cross-aggregate stored outcome");
    let provenance = StoredProvenanceRecordV1::new(
        provenance_id,
        sequence,
        identity,
        request_id,
        plan.clone(),
        pending.canonical_input_hash(),
        actor.clone(),
        logical_time,
        partition_hash,
        Vec::new(),
        declared_outcome.outcome_id(),
        mutations
            .iter()
            .map(|mutation| AffectedEntityV1::from_record(mutation.post_image()))
            .collect(),
        vec![event_id],
        StoredAdmittedProvenanceClaimsV1::default(),
    )
    .expect("cross-aggregate provenance");
    let commit = riffdb_storage_api::StoredCommitRecordV1::new(
        sequence,
        request_id,
        plan,
        pending.canonical_input_hash(),
        actor,
        logical_time,
        partition_hash,
        Vec::new(),
        StoredReadDependenciesV1::from_live(snapshot.read_dependencies())
            .expect("stored dependencies"),
        mutations
            .iter()
            .map(riffdb_storage_api::CommittedEntityReferenceV2::from_live_mutation)
            .collect::<Result<Vec<_>, _>>()
            .expect("live entity references")
            .into_iter()
            .flatten()
            .collect(),
        vec![event],
        declared_outcome,
        provenance_id,
        vec![event_id],
        DurabilityMode::Sync,
    )
    .expect("cross-aggregate commit record");
    let records = AtomicCommandRecordSet::new(
        AssignedCommandSequence::from_assigned(sequence),
        mutations,
        write_plan.clone(),
        stored_outcome,
        provenance,
        commit,
    )
    .expect("cross-aggregate atomic command record set");

    CrossAggregateFixture {
        candidates,
        intent,
        affected_targets,
        write_plan,
        records,
        targets: [row_target, ledger_target],
        index_keys: [row_index_key, ledger_index_key],
        ranges: [row_range, ledger_range],
    }
}

fn prepare_cross_aggregate_database(path: &Path) {
    let mut store = RedbStore::open(path).expect("open cross-aggregate recovery database");
    store
        .initialize_database(database_id())
        .expect("initialize cross-aggregate recovery database");
    let mut ports = open_operational(store);
    let bundle = cross_aggregate_validated_bundle()
        .to_stored()
        .expect("stored cross-aggregate bundle");
    let result = ports
        .activate_catalog(&CatalogActivationIntentV1::new(
            None,
            bundle.clone(),
            RequestId::from_bytes(uuid_bytes(0x75)).expect("catalog request"),
            catalog_principal(),
            Timestamp::new(1_700_000_000, 0).expect("catalog timestamp"),
            None,
        ))
        .expect("activate cross-aggregate recovery catalog");
    assert!(
        matches!(
            &result,
            CatalogActivationResult::Activated { active, .. }
                if *active == riffdb_storage_api::ActiveCatalogPointerV1::from_bundle(&bundle)
        ),
        "unexpected cross-aggregate catalog activation result: {result:?}"
    );
}

fn commit_cross_aggregate_fixture(ports: &RedbOperationalPorts, fixture: &CrossAggregateFixture) {
    let candidate = ports
        .begin_empty_batch()
        .expect("begin cross-aggregate batch")
        .begin_candidate(Box::new(fixture.intent.clone()))
        .expect("begin cross-aggregate candidate");
    let CandidateAdmissionResult::Proceed(candidate) =
        candidate.recheck_admission().expect("recheck admission")
    else {
        panic!("the cross-aggregate fixture's admission state must proceed");
    };
    let (candidate, current) = candidate
        .read_transaction_current()
        .expect("read transaction-current state");
    // Both aggregates' bindings must be transaction-current, or the write set
    // under test is not the cross-aggregate one this arm claims to commit.
    assert_eq!(
        current.bindings().len(),
        2,
        "a cross-aggregate command must read both bindings"
    );
    for (position, binding) in current.bindings().iter().enumerate() {
        assert_eq!(
            binding.expected_state(),
            fixture.records.entities()[position].expected(),
            "binding {position} must match the fixture's committed expectation"
        );
    }
    let candidate = candidate
        .plan_validated(fixture.affected_targets.clone())
        .read_affected_epoch_current()
        .expect("read affected epoch current");
    let CandidateCapacityResult::Reserved(candidate) = candidate
        .reserve_capacity(fixture.write_plan.clone())
        .expect("reserve cross-aggregate capacity")
    else {
        panic!("small cross-aggregate fixture must reserve");
    };
    let candidate = candidate
        .assign_sequence()
        .expect("assign cross-aggregate sequence");
    candidate
        .stage(fixture.records.clone())
        .expect("stage cross-aggregate records")
        .commit_with_service_audit_transitions(
            DurabilityMode::Sync,
            vec![cross_aggregate_audit_transition(fixture)],
        )
        .expect("commit cross-aggregate command");
}

/// Neither aggregate's entity or index entry is present, and neither is the
/// command's own record graph.
///
/// The per-aggregate loop is the point: a torn commit that wrote one aggregate
/// and not the other would satisfy a single-target assertion.
fn assert_cross_aggregate_absent(ports: &RedbOperationalPorts, fixture: &CrossAggregateFixture) {
    assert_eq!(
        ports
            .lookup_admission(fixture.candidates.clone())
            .expect("lookup cross-aggregate admission"),
        AdmissionLookupResultV1::NotFound,
        "a crash before the commit must leave no admission"
    );
    assert_eq!(
        ports
            .read_commit(CommitSequence::first())
            .expect("read cross-aggregate commit"),
        None,
        "a crash before the commit must leave no commit record"
    );
    let limit = StorageScanLimit::new(2).expect("scan limit");
    for (position, target) in fixture.targets.iter().enumerate() {
        assert_eq!(
            ports.read_entity(target).expect("read entity"),
            None,
            "aggregate {position}'s entity must be absent after a precommit crash"
        );
    }
    for (position, range) in fixture.ranges.iter().enumerate() {
        let page = ports
            .scan_index(
                AuthoritativeIndexScanRequest::new(range.clone(), None, limit)
                    .expect("index request"),
            )
            .expect("scan index");
        assert!(
            matches!(
                page,
                AuthoritativeIndexScanPage::ExactEnd {
                    ref entries,
                    epoch: IndexEpochPosition::BeforeFirst,
                } if entries.is_empty()
            ),
            "aggregate {position}'s index must be empty after a precommit crash: {page:?}"
        );
    }
}

/// Both aggregates' entities and index entries are present, under one commit.
fn assert_cross_aggregate_present(ports: &RedbOperationalPorts, fixture: &CrossAggregateFixture) {
    let AdmissionLookupResultV1::Found(_) = ports
        .lookup_admission(fixture.candidates.clone())
        .expect("lookup cross-aggregate admission")
    else {
        panic!("a crash after the commit must preserve the admission");
    };
    let commit = ports
        .read_commit(CommitSequence::first())
        .expect("read cross-aggregate commit")
        .expect("a crash after the commit must preserve the commit record");
    // One commit sequence owns both aggregates' entities. Two sequences would
    // mean the write was split, which is the thing ADR-0170 claims it is not.
    assert_eq!(
        commit.entity_references().len(),
        2,
        "one commit record must own both aggregates' entities"
    );
    let limit = StorageScanLimit::new(2).expect("scan limit");
    for (position, target) in fixture.targets.iter().enumerate() {
        let stored = ports
            .read_entity(target)
            .expect("read entity")
            .unwrap_or_else(|| panic!("aggregate {position}'s entity must be present"));
        assert_eq!(
            stored.entity_version(),
            EntityVersion::first(),
            "aggregate {position}'s entity must be at its first version"
        );
    }
    for (position, range) in fixture.ranges.iter().enumerate() {
        let page = ports
            .scan_index(
                AuthoritativeIndexScanRequest::new(range.clone(), None, limit)
                    .expect("index request"),
            )
            .expect("scan index");
        let AuthoritativeIndexScanPage::ExactEnd { entries, .. } = page else {
            panic!("aggregate {position}'s index scan must reach an exact end");
        };
        assert_eq!(
            entries.len(),
            1,
            "aggregate {position}'s index must carry exactly its own entry"
        );
        assert_eq!(
            entries[0].value().key(),
            &fixture.index_keys[position],
            "aggregate {position}'s index entry must be the fixture's"
        );
    }
}

fn cross_aggregate_audit_transition(
    fixture: &CrossAggregateFixture,
) -> riffdb_storage_api::CommandServiceAuditTransitionV1 {
    let principal = catalog_principal();
    let request_id = fixture.records.commit().admission_request_id();
    let started = ServiceAuditAppendIntentV1::new(
        request_id,
        Timestamp::new(1_700_000_002, 0).expect("started timestamp"),
        ServiceOperationV1::ExecuteCommand,
        ServiceAuditPhaseV1::Started,
        principal.clone(),
        ServiceIngressKindV1::Grpc,
        ServiceAuditTargetsV1::empty(),
        None,
        ServiceAuditLinkV1::None,
    )
    .expect("cross-aggregate started audit");
    let terminal = ServiceAuditAppendIntentV1::new(
        request_id,
        Timestamp::new(1_700_000_003, 0).expect("terminal timestamp"),
        ServiceOperationV1::ExecuteCommand,
        ServiceAuditPhaseV1::Succeeded,
        principal,
        ServiceIngressKindV1::Grpc,
        ServiceAuditTargetsV1::empty(),
        None,
        ServiceAuditLinkV1::Command {
            commit_sequence: fixture.records.commit().commit_sequence(),
            provenance_id: fixture.records.provenance().provenance_id(),
        },
    )
    .expect("cross-aggregate terminal audit");
    riffdb_storage_api::CommandServiceAuditTransitionV1::started_and_terminal(started, terminal)
        .expect("fused cross-aggregate audit lifecycle")
}

/// ADR-0170, WP-721: a crash before the commit leaves BOTH aggregates absent.
///
/// The failure this excludes is a torn write in which one aggregate's entity
/// and index entry survive and the other's do not. A single-aggregate arm
/// cannot see that, which is why this one walks both.
// req: PERF-005
#[test]
fn crash_before_cross_aggregate_commit_leaves_both_aggregates_absent() {
    for (label, profile) in [
        ("standard", RedbCommitProfile::Standard),
        ("hardened", RedbCommitProfile::Hardened),
    ] {
        let path = TestDatabasePath::new(&format!("before-cross-aggregate-{label}"));
        prepare_cross_aggregate_database(&path.0);
        run_crashing_child_with_profile("before-cross-aggregate-commit", &path.0, profile);

        let ports = open_operational(
            RedbStore::open(&path.0).expect("recover precommit cross-aggregate crash"),
        );
        assert_cross_aggregate_absent(&ports, &cross_aggregate_fixture());
    }
}

/// ADR-0170, WP-721: a crash after the commit preserves BOTH aggregates, under
/// one commit sequence, and the second recovery observes the same state.
// req: PERF-005
#[test]
fn crash_after_cross_aggregate_commit_preserves_both_aggregates() {
    for (label, profile) in [
        ("standard", RedbCommitProfile::Standard),
        ("hardened", RedbCommitProfile::Hardened),
    ] {
        let path = TestDatabasePath::new(&format!("after-cross-aggregate-{label}"));
        prepare_cross_aggregate_database(&path.0);
        run_crashing_child_with_profile("after-cross-aggregate-commit", &path.0, profile);

        let fixture = cross_aggregate_fixture();
        let ports = open_operational(
            RedbStore::open(&path.0).expect("recover postcommit cross-aggregate crash"),
        );
        assert_cross_aggregate_present(&ports, &fixture);
        drop(ports);

        // Recovery is idempotent: a second open must not complete a partially
        // observed write differently from the first.
        let ports = open_operational(
            RedbStore::open(&path.0).expect("repeat postcommit cross-aggregate recovery"),
        );
        assert_cross_aggregate_present(&ports, &fixture);
    }
}

/// The relaxation ADR-0170 needed is narrow: an index entry may leave the
/// command's *aggregate namespace*, never its *partition route*.
///
/// WP-721's crash arm found that index entries were partitioned by the
/// command's single partition key, namespaced by the locality aggregate, while
/// startup validation resolves an entry's expected namespace from the index
/// owner's aggregate. A second indexed aggregate was therefore unreachable from
/// both directions: keep the command's key and the database would not reopen;
/// use the owner's and the write plan refused it.
///
/// `owning_partition_key` now derives each entry's key from its owning
/// aggregate, so the write plan must accept a foreign namespace on the same
/// route. This pins both halves of that: the same route under another
/// aggregate is admitted, and a *different* route is still refused, because
/// `RDB-C017` rejecting cross-partition writes is the property ADR-0170 kept.
// req: PERF-005
#[test]
fn a_cross_aggregate_index_entry_may_change_namespace_but_not_route() {
    let fixture = cross_aggregate_fixture();
    let route = 41_u64;
    let other_route = 42_u64;
    let value = 23_u64;

    let partition_for = |aggregate: u32, component: u64| {
        let mut builder =
            PartitionKeyBuilder::new(AggregateTypeId::new(aggregate).expect("aggregate"));
        builder.push_u64(component).expect("partition component");
        builder.finish().expect("partition key")
    };
    let schema_binding = DurableKeySchemaBindingV1::from_plan(fixture.records.commit().plan());
    let row_index_id = IndexId::new(1).expect("row index ID");

    let plan_for = |entry_partition: riffdb_types::PartitionKey| {
        let generation = PartitionIndexTarget::new(partition_for(1, route), row_index_id);
        let affected_targets = AffectedIndexEpochTargets::new(vec![generation.clone()])
            .expect("one well-formed epoch target");
        let affected_current = AffectedEpochCurrentState::new(
            &affected_targets,
            vec![CurrentIndexGenerationObservation::new(
                generation.clone(),
                IndexEpochPosition::BeforeFirst,
            )],
        )
        .expect("affected current state");
        let advances = vec![
            IndexEpochAdvanceV1::new(
                generation,
                schema_binding.clone(),
                IndexEpochPosition::BeforeFirst,
            )
            .expect("epoch advance"),
        ];
        let entries = vec![IndexEntryMutationV1::Put(
            StoredIndexEntryV2::new(
                fixture.index_keys[0].clone(),
                schema_binding.clone(),
                record(value),
                entry_partition,
            )
            .expect("an index entry is individually well-formed"),
        )];
        let upper_bound =
            match command_write_set_upper_bound_v1(&fixture.intent, &entries, &advances)
                .expect("canonical encoded upper bound")
            {
                EncodedWriteSetUpperBoundResultV1::Fits(bound) => bound,
                EncodedWriteSetUpperBoundResultV1::ExceedsAcceptedAggregateCap(_) => {
                    panic!("this write set must fit the accepted aggregate cap")
                }
            };
        CommandWriteSetPlanV1::new(
            &fixture.intent,
            affected_targets,
            affected_current,
            entries,
            advances,
            upper_bound,
        )
    };

    // Same route, the index owner's own aggregate: admitted. Without this,
    // ADR-0170's second aggregate could not carry an index at all.
    assert!(
        plan_for(partition_for(2, route)).is_ok(),
        "an index entry in its owning aggregate's namespace on the command's \
         route must be admitted"
    );
    // Same aggregate as the command, unchanged: still admitted.
    assert!(
        plan_for(partition_for(1, route)).is_ok(),
        "the ordinary single-aggregate shape must be unaffected"
    );
    // A different route is a cross-partition write, which ADR-0170 kept
    // refused. Both namespaces are checked so the rule cannot be satisfied by
    // matching the aggregate alone.
    for aggregate in [1, 2] {
        assert!(
            plan_for(partition_for(aggregate, other_route)).is_err(),
            "an index entry addressing another partition route must be \
             refused, aggregate {aggregate}"
        );
    }
}
