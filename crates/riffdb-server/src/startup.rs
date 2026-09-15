//! Checked production startup gate for one redb-backed database.

// The complete component graph consumes these private values during WP-130 composition.
#![allow(dead_code)]

use std::error::Error;
use std::fmt;
use std::path::Path;

use riffdb_catalog::{
    CatalogError, CatalogHistoryOutcome, CatalogIndexMigrationContext,
    CatalogIndexMigrationDriveError, CatalogIndexMigrationDriver, ValidatedCatalogHistory,
    ValidatedContractBundle, validate_catalog_history,
};
use riffdb_commit::{
    DatabaseInitializationDecision, DatabaseInitializationExecutor, InitializedDatabase,
};
use riffdb_storage_api::{
    AdministrationSequenceAllocator, ApplicationSequenceAllocator, DurableFormatAction,
    DurableFormatIdentity, EvidencePageLimit, RetainedMetadataV1, StartupIndexMigrationPort,
    StartupValidationInputs, StorageError, StructuralEvidenceCursor, StructuralEvidenceEnd,
    StructuralEvidencePage, StructuralEvidenceSession, StructuralOpenOutcome, StructurallyOpened,
};
use riffdb_storage_redb::{
    RedbCommitProfile, RedbDormantPorts, RedbDurableFormatPreflight,
    RedbDurableFormatPreflightError, RedbOperationalPorts, RedbStartupIndexMigrationPort,
    RedbStore, preflight_durable_format_path,
};
use riffdb_types::{ContractBundleHash, ContractLineage, ContractVersion, DatabaseId};

use crate::identifiers::{DatabaseIdCandidateSource, ServerIdentifierSourceError};
use crate::startup_census::{StartupStage, timed};

const STARTUP_EVIDENCE_PAGE_ITEMS: u32 = 500;

#[cfg(feature = "test-fixtures")]
const EXTERNAL_KILL_BARRIER_ENV: &str = "RIFFDB_TEST_REDB_EXTERNAL_KILL_BARRIER";

/// Checked durable lifecycle selected from the same startup snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ValidatedStartupLifecycle {
    /// No durable bootstrap marker exists; only restricted Health and bootstrap are legal.
    BootstrapRequired,
    /// Bootstrap is durable, but no active contract exists yet.
    DeploymentRequired,
    /// Bootstrap and an exactly matching active contract are durable.
    ActiveContract,
}

/// Independent capacity state of both authoritative sequence allocators.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ValidatedAllocatorCapacity {
    /// Both allocators can assign another sequence.
    Available,
    /// Only the application allocator is exhausted.
    ApplicationExhausted,
    /// Only the administration allocator is exhausted.
    AdministrationExhausted,
    /// Neither allocator can assign another sequence.
    BothExhausted,
}

/// The private result of joining storage and catalog startup evidence.
///
/// Construction activates the redb ports exactly once, after every checked
/// same-session join. It is not itself a public or durable readiness format.
pub(crate) struct CheckedRedbStartup {
    database_id: DatabaseId,
    retained_metadata: RetainedMetadataV1,
    catalog_history: ValidatedCatalogHistory,
    lifecycle: ValidatedStartupLifecycle,
    allocator_capacity: ValidatedAllocatorCapacity,
    operational_ports: RedbOperationalPorts,
    replication_publications: Option<crate::replication_publication::ReplicationPublishedSnapshots>,
}

impl CheckedRedbStartup {
    pub(crate) const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    pub(crate) const fn retained_metadata(&self) -> &RetainedMetadataV1 {
        &self.retained_metadata
    }

    pub(crate) const fn catalog_history(&self) -> &ValidatedCatalogHistory {
        &self.catalog_history
    }

    pub(crate) const fn lifecycle(&self) -> ValidatedStartupLifecycle {
        self.lifecycle
    }

    pub(crate) const fn allocator_capacity(&self) -> ValidatedAllocatorCapacity {
        self.allocator_capacity
    }

    pub(crate) fn take_replication_publications(
        &mut self,
    ) -> Option<crate::replication_publication::ReplicationPublishedSnapshots> {
        self.replication_publications.take()
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        DatabaseId,
        RetainedMetadataV1,
        ValidatedCatalogHistory,
        ValidatedStartupLifecycle,
        ValidatedAllocatorCapacity,
        RedbOperationalPorts,
    ) {
        (
            self.database_id,
            self.retained_metadata,
            self.catalog_history,
            self.lifecycle,
            self.allocator_capacity,
            self.operational_ports,
        )
    }
}

impl fmt::Debug for CheckedRedbStartup {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CheckedRedbStartup")
            .field("database_id", &self.database_id)
            .field("catalog_history", &"[CHECKED]")
            .field("lifecycle", &self.lifecycle)
            .field("allocator_capacity", &self.allocator_capacity)
            .field("operational_ports", &"[ACTIVATED]")
            .finish()
    }
}

/// Closed fail-closed reason for rejecting a startup proof join.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StartupIntegrityFailure {
    InitializationIdentityMismatch,
    StructuralContinuationMismatch,
    StructuralFinding,
    CatalogSessionMismatch,
    StartupOutcomeMismatch,
    MigrationRepeated,
    RetainedIdentityMismatch,
    ActiveCatalogMismatch,
    InvalidBootstrapLifecycle,
}

enum RedbStartupPass {
    Ready {
        catalog_history: ValidatedCatalogHistory,
        structurally_opened: StructurallyOpened<RedbDormantPorts>,
    },
    MigrationRequired {
        context: CatalogIndexMigrationContext,
        port: RedbStartupIndexMigrationPort,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CatalogStartupOutcomeKind {
    Ready,
    MigrationRequired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StorageStartupOutcomeKind {
    Clean,
    MigrationRequired,
}

/// Closed source-free durable-format reason discovered before redb can open.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StartupFormatFailure {
    /// The marker or filesystem inventory could not prove a safe action.
    Preflight(RedbDurableFormatPreflightError),
    /// The manifest selected one exact offline transition.
    OfflineUpgradeRequired {
        current: DurableFormatIdentity,
        binary: DurableFormatIdentity,
        action: DurableFormatAction,
    },
}

impl fmt::Display for StartupFormatFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Preflight(error) => write!(
                formatter,
                "durable-format preflight failed before database open: {error}; no data was changed"
            ),
            Self::OfflineUpgradeRequired {
                current,
                binary,
                action:
                    DurableFormatAction::OfflineInPlace {
                        backup_required,
                        free_space_source_multiples,
                        downtime_required,
                        one_way,
                        next_command,
                    },
            } => write!(
                formatter,
                "durable-format upgrade required before database open: current=alpha-{}.{} binary=alpha-{}.{} action=offline_in_place backup_required={backup_required} free_space_source_multiples={free_space_source_multiples} downtime_required={downtime_required} one_way={one_way}; next: {} --database-path PATH --backup PATH; no data was changed",
                current.epoch().get(),
                current.writer().get(),
                binary.epoch().get(),
                binary.writer().get(),
                next_command.render(),
            ),
            Self::OfflineUpgradeRequired {
                current,
                binary,
                action:
                    DurableFormatAction::ExportReimportOnly {
                        backup_required,
                        downtime_required,
                        next_command,
                    },
            } => write!(
                formatter,
                "durable-format epoch is incompatible with this binary: current=alpha-{}.{} binary=alpha-{}.{} action=export_reimport_only backup_required={backup_required} downtime_required={downtime_required}; next: {}; no data was changed",
                current.epoch().get(),
                current.writer().get(),
                binary.epoch().get(),
                binary.writer().get(),
                next_command.render(),
            ),
            Self::OfflineUpgradeRequired {
                action: DurableFormatAction::OpenCurrent,
                ..
            } => formatter.write_str(
                "durable-format preflight returned an inconsistent action; no data was changed",
            ),
        }
    }
}

/// Private startup failure retaining only already-safe lower classifications.
#[derive(Clone, Eq, PartialEq)]
pub(crate) enum RedbStartupError {
    Format(StartupFormatFailure),
    Storage(StorageError),
    Catalog(CatalogError),
    Identifier(ServerIdentifierSourceError),
    Integrity(StartupIntegrityFailure),
}

impl fmt::Debug for RedbStartupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Format(error) => formatter.debug_tuple("Format").field(error).finish(),
            Self::Storage(error) => formatter.debug_tuple("Storage").field(error).finish(),
            Self::Catalog(error) => formatter.debug_tuple("Catalog").field(error).finish(),
            Self::Identifier(_) => formatter.write_str("Identifier([REDACTED])"),
            Self::Integrity(reason) => formatter.debug_tuple("Integrity").field(reason).finish(),
        }
    }
}

impl fmt::Display for RedbStartupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Format(error) => error.fmt(formatter),
            Self::Storage(_) | Self::Catalog(_) | Self::Identifier(_) | Self::Integrity(_) => {
                formatter.write_str("database startup validation failed")
            }
        }
    }
}

impl Error for RedbStartupError {}

impl From<StorageError> for RedbStartupError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

impl From<CatalogError> for RedbStartupError {
    fn from(error: CatalogError) -> Self {
        Self::Catalog(error)
    }
}

impl From<ServerIdentifierSourceError> for RedbStartupError {
    fn from(error: ServerIdentifierSourceError) -> Self {
        Self::Identifier(error)
    }
}

/// Opens and completely validates one production redb database.
pub(crate) fn open_redb_startup(
    path: &Path,
    inputs: StartupValidationInputs,
    database_ids: &DatabaseIdCandidateSource,
) -> Result<CheckedRedbStartup, RedbStartupError> {
    open_redb_startup_with_commit_profile(path, inputs, database_ids, RedbCommitProfile::Standard)
}

/// Opens and validates one database under the selected application profile.
pub(crate) fn open_redb_startup_with_commit_profile(
    path: &Path,
    inputs: StartupValidationInputs,
    database_ids: &DatabaseIdCandidateSource,
    application_commit_profile: RedbCommitProfile,
) -> Result<CheckedRedbStartup, RedbStartupError> {
    let (publication, reader) = crate::replication_publication::ReplicationPublication::channel();
    let store = timed(StartupStage::StoreOpen, || {
        preflight_redb_startup_format(path)?;
        open_redb_store(path, application_commit_profile, publication.clone())
    })?;
    let mut checked = complete_redb_startup(store, inputs, || database_ids.next_database_id())?;
    // Seed after the unchanged startup proof join and V3 activation. Restart
    // serves retained history immediately even if no new command arrives.
    riffdb_storage_api::ChangelogPublicationPort::observe_published_snapshot_v3(
        publication.as_ref(),
        checked
            .operational_ports
            .published_changelog_snapshot_v3()?,
    );
    checked.replication_publications = Some(reader);
    Ok(checked)
}

#[cfg(not(feature = "test-fixtures"))]
fn open_redb_store(
    path: &Path,
    application_commit_profile: RedbCommitProfile,
    publication: std::sync::Arc<dyn riffdb_storage_api::ChangelogPublicationPort>,
) -> Result<RedbStore, RedbStartupError> {
    RedbStore::open_with_changelog_publication_port(path, application_commit_profile, publication)
        .map_err(RedbStartupError::from)
}

#[cfg(feature = "test-fixtures")]
fn open_redb_store(
    path: &Path,
    application_commit_profile: RedbCommitProfile,
    publication: std::sync::Arc<dyn riffdb_storage_api::ChangelogPublicationPort>,
) -> Result<RedbStore, RedbStartupError> {
    let Some(marker) = std::env::var_os(EXTERNAL_KILL_BARRIER_ENV) else {
        return RedbStore::open_with_changelog_publication_port(
            path,
            application_commit_profile,
            publication,
        )
        .map_err(RedbStartupError::from);
    };
    RedbStore::open_with_test_controller_and_changelog_publication_port(
        path,
        application_commit_profile,
        riffdb_storage_redb::RedbTestController::wait_before_commit_for_external_kill(
            riffdb_storage_redb::RedbTestOperation::DeferredCommandBatch,
            marker,
        ),
        publication,
    )
    .map_err(RedbStartupError::from)
}

fn preflight_redb_startup_format(path: &Path) -> Result<(), RedbStartupError> {
    match preflight_durable_format_path(path) {
        Ok(
            RedbDurableFormatPreflight::InitializeCurrent | RedbDurableFormatPreflight::OpenCurrent,
        ) => Ok(()),
        Ok(RedbDurableFormatPreflight::OfflineUpgradeRequired {
            current,
            binary,
            action,
        }) => Err(RedbStartupError::Format(
            StartupFormatFailure::OfflineUpgradeRequired {
                current,
                binary,
                action,
            },
        )),
        Err(error) => Err(RedbStartupError::Format(StartupFormatFailure::Preflight(
            error,
        ))),
    }
}

fn complete_redb_startup<Candidate>(
    store: RedbStore,
    inputs: StartupValidationInputs,
    candidate: Candidate,
) -> Result<CheckedRedbStartup, RedbStartupError>
where
    Candidate: FnOnce() -> Result<DatabaseId, ServerIdentifierSourceError>,
{
    let initialized = match DatabaseInitializationExecutor::new(store).probe()? {
        DatabaseInitializationDecision::Existing(initialized) => initialized,
        DatabaseInitializationDecision::NeedsInitialization(permit) => {
            permit.initialize(candidate()?)?.into_initialized_database()
        }
    };
    match run_redb_startup_pass(initialized, inputs.clone())? {
        RedbStartupPass::Ready {
            catalog_history,
            structurally_opened,
        } => complete_ready_redb_startup(catalog_history, structurally_opened),
        RedbStartupPass::MigrationRequired { context, port } => {
            let store = drive_index_migration(context, port)?;
            let reopened = match DatabaseInitializationExecutor::new(store).probe()? {
                DatabaseInitializationDecision::Existing(initialized) => initialized,
                DatabaseInitializationDecision::NeedsInitialization(_) => {
                    return Err(RedbStartupError::Integrity(
                        StartupIntegrityFailure::InitializationIdentityMismatch,
                    ));
                }
            };
            match run_redb_startup_pass(reopened, inputs)? {
                RedbStartupPass::Ready {
                    catalog_history,
                    structurally_opened,
                } => complete_ready_redb_startup(catalog_history, structurally_opened),
                RedbStartupPass::MigrationRequired { .. } => Err(RedbStartupError::Integrity(
                    StartupIntegrityFailure::MigrationRepeated,
                )),
            }
        }
    }
}

fn run_redb_startup_pass(
    initialized: InitializedDatabase<RedbStore>,
    inputs: StartupValidationInputs,
) -> Result<RedbStartupPass, RedbStartupError> {
    let initialized_database_id = initialized.database_id();
    let session = timed(StartupStage::EvidenceBegin, || {
        initialized.begin_structural_evidence(inputs)
    })?;
    run_redb_evidence_session(session, initialized_database_id)
}

fn run_redb_evidence_session(
    mut session: riffdb_storage_redb::RedbStructuralEvidenceSession,
    initialized_database_id: DatabaseId,
) -> Result<RedbStartupPass, RedbStartupError> {
    report_clean_close_startup_selection(&session);
    if session.database_id() != initialized_database_id {
        return Err(RedbStartupError::Integrity(
            StartupIntegrityFailure::InitializationIdentityMismatch,
        ));
    }

    let structural_end = timed(StartupStage::StructuralDrain, || {
        drive_structural_evidence(&mut session)
    })?;
    let validation = timed(StartupStage::CatalogHistory, || {
        validate_catalog_history(&mut session)
    })?;
    let (catalog_outcome, historical_end) = validation.into_parts();
    let storage_outcome = timed(StartupStage::EvidenceFinish, || {
        session.finish(structural_end, historical_end)
    })?;
    validate_startup_outcome_kinds(
        match &catalog_outcome {
            CatalogHistoryOutcome::Ready(_) => CatalogStartupOutcomeKind::Ready,
            CatalogHistoryOutcome::MigrationRequired(_) => {
                CatalogStartupOutcomeKind::MigrationRequired
            }
        },
        match &storage_outcome {
            StructuralOpenOutcome::Clean(_) => StorageStartupOutcomeKind::Clean,
            StructuralOpenOutcome::MigrationRequired(_) => {
                StorageStartupOutcomeKind::MigrationRequired
            }
        },
    )?;
    match (catalog_outcome, storage_outcome) {
        (CatalogHistoryOutcome::Ready(catalog_history), StructuralOpenOutcome::Clean(opened)) => {
            if !catalog_history.matches(initialized_database_id, opened.open_session_id()) {
                return Err(RedbStartupError::Integrity(
                    StartupIntegrityFailure::CatalogSessionMismatch,
                ));
            }
            Ok(RedbStartupPass::Ready {
                catalog_history,
                structurally_opened: opened,
            })
        }
        (
            CatalogHistoryOutcome::MigrationRequired(context),
            StructuralOpenOutcome::MigrationRequired(port),
        ) => {
            if context.database_id() != port.database_id()
                || context.open_session_id() != port.open_session_id()
                || context.database_id() != initialized_database_id
            {
                return Err(RedbStartupError::Integrity(
                    StartupIntegrityFailure::CatalogSessionMismatch,
                ));
            }
            Ok(RedbStartupPass::MigrationRequired { context, port })
        }
        _ => Err(RedbStartupError::Integrity(
            StartupIntegrityFailure::StartupOutcomeMismatch,
        )),
    }
}

/// Names the startup path this open selected, and why, on one stderr line.
///
/// Emitted immediately after the validation session opens — before the pass
/// itself runs — because the cost being diagnosed is the pass. On a large
/// database the complete pass is tens of minutes of SHA-256 and record
/// decoding, and until this line existed the nine preconditions that decline
/// the ADR-0157 bounded path all presented as the identical symptom: a start
/// that never becomes ready. Waiting for the pass to finish before reporting
/// would report only what an operator already knows.
///
/// The reason is a closed enum discriminant carrying no path, key, value, or
/// identity — the same disclosure rule the startup-refusal lines follow. Purely
/// observational: it cannot select startup behavior.
fn report_clean_close_startup_selection(
    session: &riffdb_storage_redb::RedbStructuralEvidenceSession,
) {
    match session.clean_close_declined_reason() {
        None => eprintln!("[riffdbd-diag] trigger=startup clean_close_fast=true"),
        Some(reason) => eprintln!(
            "[riffdbd-diag] trigger=startup clean_close_fast=false decline={reason} \
             (full validation pass; cost scales with retained history)"
        ),
    }
}

fn validate_startup_outcome_kinds(
    catalog: CatalogStartupOutcomeKind,
    storage: StorageStartupOutcomeKind,
) -> Result<(), RedbStartupError> {
    match (catalog, storage) {
        (CatalogStartupOutcomeKind::Ready, StorageStartupOutcomeKind::Clean)
        | (
            CatalogStartupOutcomeKind::MigrationRequired,
            StorageStartupOutcomeKind::MigrationRequired,
        ) => Ok(()),
        (CatalogStartupOutcomeKind::Ready, StorageStartupOutcomeKind::MigrationRequired)
        | (CatalogStartupOutcomeKind::MigrationRequired, StorageStartupOutcomeKind::Clean) => Err(
            RedbStartupError::Integrity(StartupIntegrityFailure::StartupOutcomeMismatch),
        ),
    }
}

fn complete_ready_redb_startup(
    catalog_history: ValidatedCatalogHistory,
    structurally_opened: StructurallyOpened<RedbDormantPorts>,
) -> Result<CheckedRedbStartup, RedbStartupError> {
    let (database_id, open_session_id, retained_metadata, dormant_ports) =
        structurally_opened.into_parts();
    let (lifecycle, allocator_capacity) = validate_ready_join(
        &catalog_history,
        database_id,
        open_session_id,
        &retained_metadata,
    )?;

    // This is the sole activation call. Later replication seeding uses only the
    // validated operational reader; it cannot replace or bypass this proof join.
    let operational_ports = timed(StartupStage::PortActivation, || {
        dormant_ports.into_operational_after_catalog_validation()
    })?;
    Ok(CheckedRedbStartup {
        database_id,
        retained_metadata,
        catalog_history,
        lifecycle,
        allocator_capacity,
        operational_ports,
        replication_publications: None,
    })
}

fn validate_ready_join(
    catalog: &ValidatedCatalogHistory,
    database_id: DatabaseId,
    session: riffdb_storage_api::OpenSessionId,
    retained: &RetainedMetadataV1,
) -> Result<(ValidatedStartupLifecycle, ValidatedAllocatorCapacity), RedbStartupError> {
    if !catalog.matches(database_id, session) {
        return Err(RedbStartupError::Integrity(
            StartupIntegrityFailure::CatalogSessionMismatch,
        ));
    }
    validate_retained_join(
        database_id,
        retained,
        catalog.active().map(active_catalog_identity),
    )
}

/// Complete unchanged structural/catalog/retained proof join, releasing only a
/// follower applier. There is no initialization, migration, or source activation.
pub(crate) struct CheckedRedbFollowerStartup {
    pub(crate) database_id: DatabaseId,
    pub(crate) retained_metadata: RetainedMetadataV1,
    pub(crate) catalog_history: ValidatedCatalogHistory,
    pub(crate) lifecycle: ValidatedStartupLifecycle,
    pub(crate) allocator_capacity: ValidatedAllocatorCapacity,
    pub(crate) applier: riffdb_storage_redb::RedbFollowerApplier,
}

/// Move-only evidence retained from the same unchanged startup that released
/// the receiver's sole applier. It grants no storage mutation capability.
pub(crate) struct FollowerStartupEvidence {
    pub(crate) database_id: DatabaseId,
    pub(crate) retained_metadata: RetainedMetadataV1,
    pub(crate) catalog_history: ValidatedCatalogHistory,
    pub(crate) lifecycle: ValidatedStartupLifecycle,
    pub(crate) allocator_capacity: ValidatedAllocatorCapacity,
}
impl CheckedRedbFollowerStartup {
    pub(crate) fn into_parts(
        self,
    ) -> (
        riffdb_storage_redb::RedbFollowerApplier,
        FollowerStartupEvidence,
    ) {
        (
            self.applier,
            FollowerStartupEvidence {
                database_id: self.database_id,
                retained_metadata: self.retained_metadata,
                catalog_history: self.catalog_history,
                lifecycle: self.lifecycle,
                allocator_capacity: self.allocator_capacity,
            },
        )
    }
}

pub(crate) fn open_redb_follower_startup(
    path: &Path,
    inputs: StartupValidationInputs,
) -> Result<CheckedRedbFollowerStartup, RedbStartupError> {
    open_redb_follower_startup_cancellable(
        path,
        inputs,
        std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
    )
}

pub(crate) fn open_redb_follower_startup_cancellable(
    path: &Path,
    inputs: StartupValidationInputs,
    cancellation: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> Result<CheckedRedbFollowerStartup, RedbStartupError> {
    crate::replication_bootstrap::check_cancel(&cancellation)?;
    let store = crate::replication_bootstrap::recover_follower_projections(
        riffdb_storage_redb::RedbFollowerStore::open(path)?,
        inputs.clone(),
        std::sync::Arc::clone(&cancellation),
    )?;
    let session = store
        .begin_structural_evidence_cancellable(inputs, std::sync::Arc::clone(&cancellation))?;
    let database_id = session.database_id();
    let evidence = run_redb_evidence_session(session, database_id);
    crate::replication_bootstrap::check_cancel(&cancellation)?;
    let RedbStartupPass::Ready {
        catalog_history,
        structurally_opened,
    } = evidence?
    else {
        return Err(RedbStartupError::Integrity(
            StartupIntegrityFailure::StartupOutcomeMismatch,
        ));
    };
    let (database_id, session, retained_metadata, dormant) = structurally_opened.into_parts();
    let (lifecycle, allocator_capacity) =
        validate_ready_join(&catalog_history, database_id, session, &retained_metadata)?;
    let applier = dormant.into_follower_after_catalog_validation_cancellable(&cancellation)?;
    Ok(CheckedRedbFollowerStartup {
        database_id,
        retained_metadata,
        catalog_history,
        lifecycle,
        allocator_capacity,
        applier,
    })
}

fn drive_index_migration(
    context: CatalogIndexMigrationContext,
    port: RedbStartupIndexMigrationPort,
) -> Result<RedbStore, RedbStartupError> {
    CatalogIndexMigrationDriver::new(context, port)
        .map_err(map_index_migration_drive_error)?
        .run()
        .map_err(map_index_migration_drive_error)
}

fn map_index_migration_drive_error(error: CatalogIndexMigrationDriveError) -> RedbStartupError {
    match error {
        CatalogIndexMigrationDriveError::Catalog(error) => RedbStartupError::Catalog(error),
        CatalogIndexMigrationDriveError::Storage(error) => RedbStartupError::Storage(error),
    }
}

fn drive_structural_evidence<S>(session: &mut S) -> Result<S::StructuralEnd, RedbStartupError>
where
    S: StructuralEvidenceSession,
{
    let mut cursor =
        StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
    let limit = EvidencePageLimit::new(STARTUP_EVIDENCE_PAGE_ITEMS).ok_or(
        RedbStartupError::Integrity(StartupIntegrityFailure::StructuralContinuationMismatch),
    )?;
    loop {
        match session.read_structural_evidence(cursor, limit)? {
            StructuralEvidencePage::Page {
                start,
                findings,
                next,
            } => {
                if start != cursor
                    || next.database_id() != cursor.database_id()
                    || next.open_session_id() != cursor.open_session_id()
                    || next.position() <= cursor.position()
                {
                    return Err(RedbStartupError::Integrity(
                        StartupIntegrityFailure::StructuralContinuationMismatch,
                    ));
                }
                if !findings.is_empty() {
                    return Err(RedbStartupError::Integrity(
                        StartupIntegrityFailure::StructuralFinding,
                    ));
                }
                cursor = next;
            }
            StructuralEvidencePage::ExactEnd(end) => {
                if end.cursor() != cursor {
                    return Err(RedbStartupError::Integrity(
                        StartupIntegrityFailure::StructuralContinuationMismatch,
                    ));
                }
                return Ok(end);
            }
        }
    }
}

fn active_catalog_identity(
    active: &ValidatedContractBundle,
) -> (&ContractLineage, ContractVersion, ContractBundleHash) {
    (
        active.lineage(),
        active.contract_version(),
        active.bundle_hash(),
    )
}

fn validate_retained_join(
    database_id: DatabaseId,
    retained: &RetainedMetadataV1,
    validated_active: Option<(&ContractLineage, ContractVersion, ContractBundleHash)>,
) -> Result<(ValidatedStartupLifecycle, ValidatedAllocatorCapacity), RedbStartupError> {
    if retained.database_id() != database_id {
        return Err(RedbStartupError::Integrity(
            StartupIntegrityFailure::RetainedIdentityMismatch,
        ));
    }
    match (retained.active_catalog(), validated_active) {
        (None, None) => {}
        (Some(pointer), Some((lineage, version, hash)))
            if pointer.lineage() == lineage
                && pointer.contract_version() == version
                && pointer.bundle_hash() == hash => {}
        _ => {
            return Err(RedbStartupError::Integrity(
                StartupIntegrityFailure::ActiveCatalogMismatch,
            ));
        }
    }

    let lifecycle = match (
        retained.capability_bootstrap(),
        retained.active_catalog().is_some(),
    ) {
        (None, false) => ValidatedStartupLifecycle::BootstrapRequired,
        (Some(marker), false) if marker.database_id() == database_id => {
            ValidatedStartupLifecycle::DeploymentRequired
        }
        (Some(marker), true) if marker.database_id() == database_id => {
            ValidatedStartupLifecycle::ActiveContract
        }
        _ => {
            return Err(RedbStartupError::Integrity(
                StartupIntegrityFailure::InvalidBootstrapLifecycle,
            ));
        }
    };
    let allocator_capacity = match (
        retained.application_sequence(),
        retained.administration_sequence(),
    ) {
        (ApplicationSequenceAllocator::Next(_), AdministrationSequenceAllocator::Next(_)) => {
            ValidatedAllocatorCapacity::Available
        }
        (ApplicationSequenceAllocator::Exhausted, AdministrationSequenceAllocator::Next(_)) => {
            ValidatedAllocatorCapacity::ApplicationExhausted
        }
        (ApplicationSequenceAllocator::Next(_), AdministrationSequenceAllocator::Exhausted) => {
            ValidatedAllocatorCapacity::AdministrationExhausted
        }
        (ApplicationSequenceAllocator::Exhausted, AdministrationSequenceAllocator::Exhausted) => {
            ValidatedAllocatorCapacity::BothExhausted
        }
    };
    Ok((lifecycle, allocator_capacity))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use riffdb_storage_api::{
        ActiveCatalogPointerV1, CapabilityBootstrapMarkerV1, DormantPortBundle,
        HISTORY_INCARNATION_INITIAL, HistoricalBundleEvidence, HistoricalEvidenceCursor,
        HistoricalEvidenceEnd, HistoricalEvidencePage, OpenSessionId,
        ReadableCapabilityDigestInventory, ReadableDigestKey, ReadableIdempotencyDigestInventory,
        StorageErrorKind, StorageFormatVersion, StructuralFinding, StructuralFindingCode,
        StructuralFindingScope,
    };
    use riffdb_types::{
        AdministrationSequence, CapabilityId, CommitSequence, DigestKeyId, Timestamp,
    };

    use super::*;

    fn database_id(seed: u8) -> DatabaseId {
        DatabaseId::from_unix_milliseconds_and_random(u64::from(seed), [seed; 10])
            .expect("test database ID")
    }

    fn capability_id(seed: u8) -> CapabilityId {
        CapabilityId::from_unix_milliseconds_and_random(u64::from(seed), [seed; 10])
            .expect("test capability ID")
    }

    fn startup_inputs() -> StartupValidationInputs {
        let digest = ReadableDigestKey::v1(DigestKeyId::new(1).expect("digest key ID"));
        StartupValidationInputs::new(
            Timestamp::new(0, 0).expect("test timestamp"),
            ReadableCapabilityDigestInventory::new(vec![digest]).expect("capability inventory"),
            ReadableIdempotencyDigestInventory::new(vec![digest]).expect("idempotency inventory"),
        )
    }

    /// Whole-directory scope plus the database path inside it; dropping the
    /// scope removes the database and every side file it grows (journal,
    /// checkpoint, spare, durable-format marker, …) on pass, fail, or panic.
    fn temporary_database_scope() -> (tempfile::TempDir, std::path::PathBuf) {
        let scope = tempfile::TempDir::with_prefix("riffdb-server-startup-")
            .expect("create startup test scope directory");
        let path = scope.path().join("db.redb");
        (scope, path)
    }

    fn bootstrap_marker(database_id: DatabaseId) -> CapabilityBootstrapMarkerV1 {
        CapabilityBootstrapMarkerV1::new(
            database_id,
            capability_id(9),
            AdministrationSequence::first(),
        )
    }

    fn active_identity() -> (ContractLineage, ContractVersion, ContractBundleHash) {
        (
            ContractLineage::new("legal_spend").expect("lineage"),
            ContractVersion::new(1).expect("contract version"),
            ContractBundleHash::from_bytes([7; 32]),
        )
    }

    fn retained(
        database_id: DatabaseId,
        application: ApplicationSequenceAllocator,
        administration: AdministrationSequenceAllocator,
        active: Option<ActiveCatalogPointerV1>,
        marker: Option<CapabilityBootstrapMarkerV1>,
    ) -> RetainedMetadataV1 {
        RetainedMetadataV1::new(
            StorageFormatVersion::V1,
            database_id,
            application,
            administration,
            HISTORY_INCARNATION_INITIAL,
            active,
            marker,
        )
        .expect("retained metadata")
    }

    #[test]
    fn empty_database_initializes_and_reopen_never_samples_candidate_source() {
        let (_scope, path) = temporary_database_scope();
        let installed = database_id(1);
        let discarded = database_id(2);
        let calls = AtomicUsize::new(0);

        let first = complete_redb_startup(
            RedbStore::open(&path).expect("open empty redb"),
            startup_inputs(),
            || {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok(installed)
            },
        )
        .expect("initialize and validate");
        assert_eq!(first.database_id(), installed);
        assert_eq!(
            first.lifecycle(),
            ValidatedStartupLifecycle::BootstrapRequired
        );
        assert_eq!(
            first.allocator_capacity(),
            ValidatedAllocatorCapacity::Available
        );
        drop(first);

        let reopened = complete_redb_startup(
            RedbStore::open(&path).expect("reopen redb"),
            startup_inputs(),
            || {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok(discarded)
            },
        )
        .expect("validate reopened database");
        assert_eq!(reopened.database_id(), installed);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        drop(reopened);
        // Explicit form of what the deleted cleanup used to observe by
        // accident (remove_file on an absent marker errored): a successful
        // startup publishes the durable-format marker beside the database.
        assert!(
            riffdb_storage_redb::durable_format_marker_path(&path).exists(),
            "successful startup must publish the durable-format marker"
        );
    }

    #[test]
    fn predecessor_format_refuses_before_open_with_one_exact_action() {
        let (_scope, path) = temporary_database_scope();
        let original = b"pre-manifest database bytes remain untouched";
        std::fs::write(&path, original).expect("write predecessor placeholder");

        let error = preflight_redb_startup_format(&path).expect_err("upgrade must be required");
        let RedbStartupError::Format(StartupFormatFailure::OfflineUpgradeRequired {
            current,
            binary,
            action,
        }) = error
        else {
            panic!("unexpected startup failure classification");
        };
        assert_eq!(current.epoch().get(), binary.epoch().get());
        assert_eq!(current.writer().get(), 0);
        assert_eq!(binary.writer().get(), 1);
        assert!(matches!(
            action,
            DurableFormatAction::OfflineInPlace {
                backup_required: true,
                free_space_source_multiples: 2,
                downtime_required: true,
                one_way: true,
                next_command: riffdb_storage_api::SafeFormatCommand::Upgrade,
            }
        ));
        assert_eq!(std::fs::read(&path).expect("read predecessor"), original);
        assert!(!riffdb_storage_redb::durable_format_marker_path(&path).exists());

        let rendered = RedbStartupError::Format(StartupFormatFailure::OfflineUpgradeRequired {
            current,
            binary,
            action,
        })
        .to_string();
        assert!(rendered.contains("current=alpha-1.0 binary=alpha-1.1"));
        assert!(rendered.contains("backup_required=true"));
        assert!(rendered.contains("next: riffdb storage upgrade"));
        assert!(rendered.ends_with("no data was changed"));
    }

    struct FakeDormantPorts;

    impl DormantPortBundle for FakeDormantPorts {
        type CompletionAuthority = ();
    }

    #[derive(Debug, Eq, PartialEq)]
    struct FakeStructuralEnd(StructuralEvidenceCursor);

    impl StructuralEvidenceEnd for FakeStructuralEnd {
        fn cursor(&self) -> StructuralEvidenceCursor {
            self.0
        }
    }

    #[derive(Debug, Eq, PartialEq)]
    struct FakeHistoricalEnd(HistoricalEvidenceCursor);

    impl HistoricalEvidenceEnd for FakeHistoricalEnd {
        fn cursor(&self) -> HistoricalEvidenceCursor {
            self.0
        }
    }

    struct FakeMigrationPort {
        database_id: DatabaseId,
        open_session_id: OpenSessionId,
    }

    impl StartupIndexMigrationPort for FakeMigrationPort {
        fn database_id(&self) -> DatabaseId {
            self.database_id
        }

        fn open_session_id(&self) -> OpenSessionId {
            self.open_session_id
        }
    }

    struct FindingSession {
        database_id: DatabaseId,
        open_session_id: OpenSessionId,
        response: StructuralResponse,
    }

    enum StructuralResponse {
        Finding,
        MismatchedContinuation,
    }

    impl StructuralEvidenceSession for FindingSession {
        type DormantPorts = FakeDormantPorts;
        type StructuralEnd = FakeStructuralEnd;
        type HistoricalEnd = FakeHistoricalEnd;
        type MigrationPort = FakeMigrationPort;

        fn database_id(&self) -> DatabaseId {
            self.database_id
        }

        fn open_session_id(&self) -> OpenSessionId {
            self.open_session_id
        }

        fn read_structural_evidence(
            &mut self,
            cursor: StructuralEvidenceCursor,
            _limit: EvidencePageLimit,
        ) -> Result<StructuralEvidencePage<Self::StructuralEnd>, StorageError> {
            let (start, findings) = match self.response {
                StructuralResponse::Finding => (
                    cursor,
                    vec![StructuralFinding::new(
                        StructuralFindingScope::Authoritative,
                        StructuralFindingCode::CrossLinkMismatch,
                    )],
                ),
                StructuralResponse::MismatchedContinuation => (
                    cursor.advanced(1).expect("mismatched test start"),
                    Vec::new(),
                ),
            };
            let next = start.advanced(1).expect("advancing test cursor");
            StructuralEvidencePage::page(start, findings, next)
                .map_err(|_| StorageError::new(StorageErrorKind::InvariantViolation, None))
        }

        fn read_historical_evidence(
            &mut self,
            _cursor: HistoricalEvidenceCursor,
            _limit: EvidencePageLimit,
        ) -> Result<HistoricalEvidencePage<Self::HistoricalEnd>, StorageError> {
            Err(StorageError::new(StorageErrorKind::Unavailable, None))
        }

        fn read_historical_bundle(
            &mut self,
            _lineage: &ContractLineage,
            _contract_version: ContractVersion,
            _bundle_hash: ContractBundleHash,
        ) -> Result<Option<HistoricalBundleEvidence>, StorageError> {
            Err(StorageError::new(StorageErrorKind::Unavailable, None))
        }

        fn read_integrity_entity(
            &mut self,
            _target: &riffdb_storage_api::EntityTarget,
        ) -> Result<Option<riffdb_storage_api::StoredEntityRecordV1>, StorageError> {
            Err(StorageError::new(StorageErrorKind::Unavailable, None))
        }

        fn read_integrity_unique_occupancy(
            &mut self,
            _target: &riffdb_storage_api::UniqueIndexTarget,
        ) -> Result<riffdb_storage_api::UniqueOccupancyKind, StorageError> {
            Err(StorageError::new(StorageErrorKind::Unavailable, None))
        }

        fn finish(
            self,
            _structural_end: Self::StructuralEnd,
            _historical_end: Self::HistoricalEnd,
        ) -> Result<StructuralOpenOutcome<Self::DormantPorts, Self::MigrationPort>, StorageError>
        {
            Err(StorageError::new(StorageErrorKind::Unavailable, None))
        }
    }

    #[test]
    fn any_structural_finding_fails_before_catalog_validation_or_activation() {
        let mut session = FindingSession {
            database_id: database_id(3),
            open_session_id: OpenSessionId::new(1).expect("open session"),
            response: StructuralResponse::Finding,
        };

        assert_eq!(
            drive_structural_evidence(&mut session),
            Err(RedbStartupError::Integrity(
                StartupIntegrityFailure::StructuralFinding
            ))
        );
    }

    #[test]
    fn structural_pages_must_continue_from_the_exact_requested_cursor() {
        let mut session = FindingSession {
            database_id: database_id(7),
            open_session_id: OpenSessionId::new(2).expect("open session"),
            response: StructuralResponse::MismatchedContinuation,
        };

        assert_eq!(
            drive_structural_evidence(&mut session),
            Err(RedbStartupError::Integrity(
                StartupIntegrityFailure::StructuralContinuationMismatch
            ))
        );
    }

    #[test]
    fn crossed_catalog_and_storage_startup_outcomes_fail_closed() {
        let mismatch = Err(RedbStartupError::Integrity(
            StartupIntegrityFailure::StartupOutcomeMismatch,
        ));
        assert_eq!(
            validate_startup_outcome_kinds(
                CatalogStartupOutcomeKind::Ready,
                StorageStartupOutcomeKind::MigrationRequired,
            ),
            mismatch
        );
        assert_eq!(
            validate_startup_outcome_kinds(
                CatalogStartupOutcomeKind::MigrationRequired,
                StorageStartupOutcomeKind::Clean,
            ),
            mismatch
        );
        assert_eq!(
            validate_startup_outcome_kinds(
                CatalogStartupOutcomeKind::Ready,
                StorageStartupOutcomeKind::Clean,
            ),
            Ok(())
        );
        assert_eq!(
            validate_startup_outcome_kinds(
                CatalogStartupOutcomeKind::MigrationRequired,
                StorageStartupOutcomeKind::MigrationRequired,
            ),
            Ok(())
        );
    }

    #[test]
    fn active_pointer_mismatch_fails_the_same_session_join() {
        let database_id = database_id(4);
        let (lineage, version, hash) = active_identity();
        let metadata = retained(
            database_id,
            ApplicationSequenceAllocator::initial(),
            AdministrationSequenceAllocator::initial(),
            Some(ActiveCatalogPointerV1::new(lineage.clone(), version, hash)),
            Some(bootstrap_marker(database_id)),
        );
        let different_hash = ContractBundleHash::from_bytes([8; 32]);

        assert_eq!(
            validate_retained_join(
                database_id,
                &metadata,
                Some((&lineage, version, different_hash)),
            ),
            Err(RedbStartupError::Integrity(
                StartupIntegrityFailure::ActiveCatalogMismatch
            ))
        );
    }

    #[test]
    fn bootstrap_lifecycle_and_allocator_capacity_are_classified_independently() {
        let database_id = database_id(5);
        let initial = RetainedMetadataV1::initial(database_id);
        assert_eq!(
            validate_retained_join(database_id, &initial, None),
            Ok((
                ValidatedStartupLifecycle::BootstrapRequired,
                ValidatedAllocatorCapacity::Available,
            ))
        );

        let deployment = retained(
            database_id,
            ApplicationSequenceAllocator::initial(),
            AdministrationSequenceAllocator::Exhausted,
            None,
            Some(bootstrap_marker(database_id)),
        );
        assert_eq!(
            validate_retained_join(database_id, &deployment, None),
            Ok((
                ValidatedStartupLifecycle::DeploymentRequired,
                ValidatedAllocatorCapacity::AdministrationExhausted,
            ))
        );

        let application_exhausted = retained(
            database_id,
            ApplicationSequenceAllocator::Exhausted,
            AdministrationSequenceAllocator::initial(),
            None,
            Some(bootstrap_marker(database_id)),
        );
        assert_eq!(
            validate_retained_join(database_id, &application_exhausted, None),
            Ok((
                ValidatedStartupLifecycle::DeploymentRequired,
                ValidatedAllocatorCapacity::ApplicationExhausted,
            ))
        );

        let (lineage, version, hash) = active_identity();
        let active = ActiveCatalogPointerV1::new(lineage.clone(), version, hash);
        let exhausted = retained(
            database_id,
            ApplicationSequenceAllocator::Exhausted,
            AdministrationSequenceAllocator::Exhausted,
            Some(active),
            Some(bootstrap_marker(database_id)),
        );
        assert_eq!(
            validate_retained_join(database_id, &exhausted, Some((&lineage, version, hash)),),
            Ok((
                ValidatedStartupLifecycle::ActiveContract,
                ValidatedAllocatorCapacity::BothExhausted,
            ))
        );
    }

    #[test]
    fn active_catalog_before_bootstrap_is_rejected() {
        let database_id = database_id(6);
        let (lineage, version, hash) = active_identity();
        let metadata = retained(
            database_id,
            ApplicationSequenceAllocator::next(CommitSequence::first()),
            AdministrationSequenceAllocator::initial(),
            Some(ActiveCatalogPointerV1::new(lineage.clone(), version, hash)),
            None,
        );

        assert_eq!(
            validate_retained_join(database_id, &metadata, Some((&lineage, version, hash)),),
            Err(RedbStartupError::Integrity(
                StartupIntegrityFailure::InvalidBootstrapLifecycle
            ))
        );
    }
}
