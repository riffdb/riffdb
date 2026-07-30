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
    AdministrationSequenceAllocator, ApplicationSequenceAllocator, EvidencePageLimit,
    RetainedMetadataV1, StartupIndexMigrationPort, StartupValidationInputs, StorageError,
    StructuralEvidenceCursor, StructuralEvidenceEnd, StructuralEvidencePage,
    StructuralEvidenceSession, StructuralOpenOutcome, StructurallyOpened,
};
use riffdb_storage_redb::{
    RedbCommitProfile, RedbDormantPorts, RedbOperationalPorts, RedbStartupIndexMigrationPort,
    RedbStore,
};
use riffdb_types::{ContractBundleHash, ContractLineage, ContractVersion, DatabaseId};

use crate::identifiers::{DatabaseIdCandidateSource, ServerIdentifierSourceError};

const STARTUP_EVIDENCE_PAGE_ITEMS: u32 = 500;

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

/// Private startup failure retaining only already-safe lower classifications.
#[derive(Clone, Eq, PartialEq)]
pub(crate) enum RedbStartupError {
    Storage(StorageError),
    Catalog(CatalogError),
    Identifier(ServerIdentifierSourceError),
    Integrity(StartupIntegrityFailure),
}

impl fmt::Debug for RedbStartupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => formatter.debug_tuple("Storage").field(error).finish(),
            Self::Catalog(error) => formatter.debug_tuple("Catalog").field(error).finish(),
            Self::Identifier(_) => formatter.write_str("Identifier([REDACTED])"),
            Self::Integrity(reason) => formatter.debug_tuple("Integrity").field(reason).finish(),
        }
    }
}

impl fmt::Display for RedbStartupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("database startup validation failed")
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
    let store = RedbStore::open_with_commit_profile(path, application_commit_profile)?;
    complete_redb_startup(store, inputs, || database_ids.next_database_id())
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
    let mut session = initialized.begin_structural_evidence(inputs)?;
    if session.database_id() != initialized_database_id {
        return Err(RedbStartupError::Integrity(
            StartupIntegrityFailure::InitializationIdentityMismatch,
        ));
    }

    let structural_end = drive_structural_evidence(&mut session)?;
    let validation = validate_catalog_history(&mut session)?;
    let (catalog_outcome, historical_end) = validation.into_parts();
    let storage_outcome = session.finish(structural_end, historical_end)?;
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
    if !catalog_history.matches(database_id, open_session_id) {
        return Err(RedbStartupError::Integrity(
            StartupIntegrityFailure::CatalogSessionMismatch,
        ));
    }

    let active = catalog_history.active().map(active_catalog_identity);
    let (lifecycle, allocator_capacity) =
        validate_retained_join(database_id, &retained_metadata, active)?;

    // This is the sole activation call. No storage observation is made by this module
    // after the same-session proof is released.
    let operational_ports = dormant_ports.into_operational_after_catalog_validation()?;
    Ok(CheckedRedbStartup {
        database_id,
        retained_metadata,
        catalog_history,
        lifecycle,
        allocator_capacity,
        operational_ports,
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
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    use riffdb_storage_api::{
        ActiveCatalogPointerV1, CapabilityBootstrapMarkerV1, DormantPortBundle,
        HistoricalBundleEvidence, HistoricalEvidenceCursor, HistoricalEvidenceEnd,
        HistoricalEvidencePage, OpenSessionId, ReadableCapabilityDigestInventory,
        ReadableDigestKey, ReadableIdempotencyDigestInventory, StorageErrorKind,
        StorageFormatVersion, StructuralFinding, StructuralFindingCode, StructuralFindingScope,
    };
    use riffdb_types::{
        AdministrationSequence, CapabilityId, CommitSequence, DigestKeyId, Timestamp,
    };

    use super::*;

    static NEXT_TEST_PATH: AtomicU64 = AtomicU64::new(1);

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

    fn temporary_database_path() -> std::path::PathBuf {
        let ordinal = NEXT_TEST_PATH.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "riffdb-server-startup-{}-{ordinal}.redb",
            std::process::id()
        ))
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
            active,
            marker,
        )
        .expect("retained metadata")
    }

    #[test]
    fn empty_database_initializes_and_reopen_never_samples_candidate_source() {
        let path = temporary_database_path();
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
        std::fs::remove_file(path).expect("remove test database");
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
