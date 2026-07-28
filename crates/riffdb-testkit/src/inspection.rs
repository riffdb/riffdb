//! Read-only, semantically validated durable-state inspection.

use std::error::Error;
use std::fmt;
use std::fs;
use std::path::Path;

use riffdb_catalog::{CatalogError, CatalogHistoryOutcome, validate_catalog_history};
use riffdb_storage_api::{
    AdministrationAuditReader, AdministrationAuditScan, AdministrationAuditScanRequest,
    AuthoritativePointReader, AuthoritativeScanReader, CommitScanPageV1, CommitScanRequest,
    EntityTarget, EvidencePageLimit, OutboxRepository, OutboxStatusReadResultV1,
    ProjectionQueryReader, ProjectionStatus, RetainedMetadataV1, StartupValidationInputs,
    StorageError, StorageScanLimit, StoredAdministrationAuditRecordV1, StoredCommitRecordV1,
    StoredDurableEventV1, StoredEntityRecordV1, StoredProvenanceRecordV1, StructuralEvidenceCursor,
    StructuralEvidenceOpen, StructuralEvidencePage, StructuralEvidenceSession, StructuralFinding,
    StructuralOpenOutcome, derive_event_hash_v1,
};
use riffdb_storage_redb::{RedbDormantPorts, RedbOperationalPorts, RedbStore};
use riffdb_types::{FrontierPosition, ProjectionIdentity};

/// Maximum explicitly requested entities or projections in one inspection.
pub const MAX_INSPECTION_TARGETS: usize = 256;
/// Maximum commits or administration records retained by one inspection.
pub const MAX_INSPECTED_ORDERED_RECORDS: usize = 10_000;

const PAGE_LIMIT: u16 = 500;

/// Canonically ordered targets selected for one offline inspection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableInspectionRequest {
    entities: Vec<EntityTarget>,
    projections: Vec<ProjectionIdentity>,
}

impl DurableInspectionRequest {
    /// Checks bounded, strictly ordered target inventories.
    pub fn new(
        entities: Vec<EntityTarget>,
        projections: Vec<ProjectionIdentity>,
    ) -> Result<Self, DurableInspectionError> {
        if entities.len() > MAX_INSPECTION_TARGETS || projections.len() > MAX_INSPECTION_TARGETS {
            return Err(DurableInspectionError::TargetLimit);
        }
        if entities.windows(2).any(|pair| pair[0] >= pair[1])
            || projections.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(DurableInspectionError::NonCanonicalTargets);
        }
        Ok(Self {
            entities,
            projections,
        })
    }

    /// Borrows entity point-read targets.
    #[must_use]
    pub fn entities(&self) -> &[EntityTarget] {
        &self.entities
    }

    /// Borrows exact projection identities.
    #[must_use]
    pub fn projections(&self) -> &[ProjectionIdentity] {
        &self.projections
    }
}

/// One entity target and its ordinary present-or-absent result.
#[derive(Clone, Eq, PartialEq)]
pub struct InspectedEntity {
    target: EntityTarget,
    record: Option<StoredEntityRecordV1>,
}

impl InspectedEntity {
    /// Borrows the canonical requested target.
    #[must_use]
    pub const fn target(&self) -> &EntityTarget {
        &self.target
    }

    /// Borrows the complete stored record when present.
    #[must_use]
    pub const fn record(&self) -> Option<&StoredEntityRecordV1> {
        self.record.as_ref()
    }
}

impl fmt::Debug for InspectedEntity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InspectedEntity")
            .field("target", &"[REDACTED]")
            .field("present", &self.record.is_some())
            .finish()
    }
}

/// One durable event and its reciprocal outbox status observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InspectedEvent {
    event: StoredDurableEventV1,
    outbox: OutboxStatusReadResultV1,
}

impl InspectedEvent {
    /// Borrows the complete authoritative event.
    #[must_use]
    pub const fn event(&self) -> &StoredDurableEventV1 {
        &self.event
    }

    /// Borrows the source-validated outbox status.
    #[must_use]
    pub const fn outbox(&self) -> &OutboxStatusReadResultV1 {
        &self.outbox
    }
}

/// Complete read-only result after structural and catalog exact-end validation.
#[derive(Clone, Eq, PartialEq)]
pub struct DurableInspection {
    metadata: RetainedMetadataV1,
    structural_findings: Vec<StructuralFinding>,
    commits: Vec<StoredCommitRecordV1>,
    entities: Vec<InspectedEntity>,
    provenance: Vec<StoredProvenanceRecordV1>,
    events: Vec<InspectedEvent>,
    projections: Vec<ProjectionStatus>,
    administration: Vec<StoredAdministrationAuditRecordV1>,
}

impl DurableInspection {
    /// Borrows the exact six-category retained POC metadata.
    #[must_use]
    pub const fn metadata(&self) -> &RetainedMetadataV1 {
        &self.metadata
    }

    /// Borrows safe findings from the complete structural pass.
    #[must_use]
    pub fn structural_findings(&self) -> &[StructuralFinding] {
        &self.structural_findings
    }

    /// Borrows the complete contiguous commit log.
    #[must_use]
    pub fn commits(&self) -> &[StoredCommitRecordV1] {
        &self.commits
    }

    /// Borrows entity observations in request order.
    #[must_use]
    pub fn entities(&self) -> &[InspectedEntity] {
        &self.entities
    }

    /// Borrows provenance records in commit order.
    #[must_use]
    pub fn provenance(&self) -> &[StoredProvenanceRecordV1] {
        &self.provenance
    }

    /// Borrows authoritative events and reciprocal outbox observations.
    #[must_use]
    pub fn events(&self) -> &[InspectedEvent] {
        &self.events
    }

    /// Borrows projection statuses in request order.
    #[must_use]
    pub fn projections(&self) -> &[ProjectionStatus] {
        &self.projections
    }

    /// Borrows the complete contiguous administration stream.
    #[must_use]
    pub fn administration(&self) -> &[StoredAdministrationAuditRecordV1] {
        &self.administration
    }
}

impl fmt::Debug for DurableInspection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DurableInspection")
            .field("metadata", &self.metadata)
            .field("structural_findings", &self.structural_findings)
            .field("commit_count", &self.commits.len())
            .field("entity_count", &self.entities.len())
            .field("provenance_count", &self.provenance.len())
            .field("event_count", &self.events.len())
            .field("projection_count", &self.projections.len())
            .field("administration_count", &self.administration.len())
            .finish()
    }
}

/// Performs one complete read-only startup pass and captures selected semantics.
///
/// The database must be stopped. This function never accepts a mutation
/// callback, raw table handle, repair option, or migration authority.
pub fn inspect_redb(
    database_path: &Path,
    startup_inputs: StartupValidationInputs,
    request: &DurableInspectionRequest,
) -> Result<DurableInspection, DurableInspectionError> {
    let metadata = match fs::metadata(database_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(DurableInspectionError::MissingDatabase);
        }
        Err(error) => return Err(DurableInspectionError::Io(error)),
    };
    if !metadata.is_file() || metadata.len() == 0 {
        return Err(DurableInspectionError::MissingDatabase);
    }
    let store = RedbStore::open(database_path).map_err(DurableInspectionError::Storage)?;
    let (retained, structural_findings, ports) = open_validated(store, startup_inputs)?;
    let commits = scan_all_commits(&ports)?;
    let administration = scan_all_administration(&ports)?;

    let entities = request
        .entities()
        .iter()
        .map(|target| {
            ports.read_entity(target).map(|record| InspectedEntity {
                target: target.clone(),
                record,
            })
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(DurableInspectionError::Storage)?;

    let mut provenance = Vec::with_capacity(commits.len());
    let event_count = commits.iter().try_fold(0usize, |count, commit| {
        count.checked_add(commit.events().len())
    });
    let event_count = event_count.ok_or(DurableInspectionError::RecordLimit)?;
    if event_count > MAX_INSPECTED_ORDERED_RECORDS {
        return Err(DurableInspectionError::RecordLimit);
    }
    let mut events = Vec::with_capacity(event_count);
    for commit in &commits {
        let record = ports
            .read_provenance(commit.provenance_id())
            .map_err(DurableInspectionError::Storage)?
            .ok_or(DurableInspectionError::MissingReciprocalRecord)?;
        provenance.push(record);
        for expected in commit.events() {
            let event = ports
                .read_durable_event(expected.event_id())
                .map_err(DurableInspectionError::Storage)?
                .ok_or(DurableInspectionError::MissingReciprocalRecord)?;
            if event != *expected
                || derive_event_hash_v1(event.event_id(), event.event_type_id(), event.payload())
                    .map_err(|_| DurableInspectionError::EventHash)?
                    != event.event_hash()
            {
                return Err(DurableInspectionError::EventHash);
            }
            let outbox = ports
                .read_outbox_status(event.event_id())
                .map_err(DurableInspectionError::Storage)?;
            if matches!(outbox, OutboxStatusReadResultV1::AuthoritativeIntentMissing) {
                return Err(DurableInspectionError::MissingReciprocalRecord);
            }
            events.push(InspectedEvent { event, outbox });
        }
    }

    let projections = request
        .projections()
        .iter()
        .map(|identity| ports.read_projection_status(identity))
        .collect::<Result<Vec<_>, _>>()
        .map_err(DurableInspectionError::Storage)?;
    drop(ports);

    Ok(DurableInspection {
        metadata: retained,
        structural_findings,
        commits,
        entities,
        provenance,
        events,
        projections,
        administration,
    })
}

fn open_validated(
    store: RedbStore,
    startup_inputs: StartupValidationInputs,
) -> Result<
    (
        RetainedMetadataV1,
        Vec<StructuralFinding>,
        RedbOperationalPorts,
    ),
    DurableInspectionError,
> {
    let mut session = store
        .begin_structural_evidence(startup_inputs)
        .map_err(DurableInspectionError::Storage)?;
    let database_id = session.database_id();
    let open_session_id = session.open_session_id();
    let limit =
        EvidencePageLimit::new(u32::from(PAGE_LIMIT)).ok_or(DurableInspectionError::RecordLimit)?;
    let mut cursor = StructuralEvidenceCursor::start(database_id, open_session_id);
    let mut findings = Vec::new();
    let structural_end = loop {
        match session
            .read_structural_evidence(cursor, limit)
            .map_err(DurableInspectionError::Storage)?
        {
            StructuralEvidencePage::Page {
                findings: page,
                next,
                ..
            } => {
                findings
                    .len()
                    .checked_add(page.len())
                    .filter(|count| *count <= MAX_INSPECTED_ORDERED_RECORDS)
                    .ok_or(DurableInspectionError::RecordLimit)?;
                findings.extend(page);
                cursor = next;
            }
            StructuralEvidencePage::ExactEnd(end) => break end,
        }
    };
    let (catalog, historical_end) = validate_catalog_history(&mut session)
        .map_err(DurableInspectionError::Catalog)?
        .into_parts();
    let catalog = match catalog {
        CatalogHistoryOutcome::Ready(catalog) => catalog,
        CatalogHistoryOutcome::MigrationRequired(_) => {
            return Err(DurableInspectionError::MigrationRequired);
        }
    };
    if !catalog.matches(database_id, open_session_id) {
        return Err(DurableInspectionError::StartupIdentityMismatch);
    }
    let opened = match session
        .finish(structural_end, historical_end)
        .map_err(DurableInspectionError::Storage)?
    {
        StructuralOpenOutcome::Clean(opened) => opened,
        StructuralOpenOutcome::MigrationRequired(_) => {
            return Err(DurableInspectionError::MigrationRequired);
        }
    };
    if opened.database_id() != database_id || opened.open_session_id() != open_session_id {
        return Err(DurableInspectionError::StartupIdentityMismatch);
    }
    let retained = opened.retained_metadata().clone();
    let (_, _, _, dormant): (_, _, _, RedbDormantPorts) = opened.into_parts();
    let ports = dormant
        .into_operational_after_catalog_validation()
        .map_err(DurableInspectionError::Storage)?;
    Ok((retained, findings, ports))
}

fn scan_all_commits(
    ports: &RedbOperationalPorts,
) -> Result<Vec<StoredCommitRecordV1>, DurableInspectionError> {
    let limit = StorageScanLimit::new(PAGE_LIMIT).ok_or(DurableInspectionError::RecordLimit)?;
    let mut request = CommitScanRequest::initial(limit);
    let mut records = Vec::new();
    loop {
        let page = ports
            .scan_commits(request)
            .map_err(DurableInspectionError::Storage)?;
        append_bounded(
            &mut records,
            page.records().iter().map(|item| item.value().clone()),
        )?;
        match page {
            CommitScanPageV1::ExactEnd { .. } => return Ok(records),
            CommitScanPageV1::Page {
                next_after,
                inclusive_upper: FrontierPosition::AppliedThrough(inclusive_upper),
                ..
            } => {
                request = CommitScanRequest::continuing(next_after, inclusive_upper, limit)
                    .map_err(|_| DurableInspectionError::NonCanonicalPage)?;
            }
            CommitScanPageV1::Page {
                inclusive_upper: FrontierPosition::BeforeFirst,
                ..
            } => return Err(DurableInspectionError::NonCanonicalPage),
        }
    }
}

fn scan_all_administration(
    ports: &RedbOperationalPorts,
) -> Result<Vec<StoredAdministrationAuditRecordV1>, DurableInspectionError> {
    let limit = StorageScanLimit::new(PAGE_LIMIT).ok_or(DurableInspectionError::RecordLimit)?;
    let mut after = None;
    let mut records = Vec::new();
    loop {
        let page = ports
            .scan_administration_audit(AdministrationAuditScanRequest::new(after, limit))
            .map_err(DurableInspectionError::Storage)?;
        let page_records = match &page {
            AdministrationAuditScan::Page { records, .. }
            | AdministrationAuditScan::ExactEnd { records } => records,
        };
        append_bounded(
            &mut records,
            page_records.iter().map(|item| item.value().clone()),
        )?;
        match page {
            AdministrationAuditScan::ExactEnd { .. } => return Ok(records),
            AdministrationAuditScan::Page { next_after, .. } => after = Some(next_after),
        }
    }
}

fn append_bounded<T>(
    destination: &mut Vec<T>,
    values: impl IntoIterator<Item = T>,
) -> Result<(), DurableInspectionError> {
    for value in values {
        if destination.len() == MAX_INSPECTED_ORDERED_RECORDS {
            return Err(DurableInspectionError::RecordLimit);
        }
        destination.push(value);
    }
    Ok(())
}

/// Closed semantic-inspection failure.
#[derive(Debug)]
pub enum DurableInspectionError {
    /// The selected path did not contain a nonempty regular database file.
    MissingDatabase,
    /// Filesystem metadata could not be read.
    Io(std::io::Error),
    /// Concrete storage rejected the complete read-only pass.
    Storage(StorageError),
    /// Catalog rejected complete historical evidence.
    Catalog(CatalogError),
    /// Explicit target count exceeded its bound.
    TargetLimit,
    /// Explicit targets were duplicated or not canonically ordered.
    NonCanonicalTargets,
    /// A bounded scan exceeded the inspector's retained-record ceiling.
    RecordLimit,
    /// A backend page contradicted its own canonical continuation.
    NonCanonicalPage,
    /// A V1 index requires the separately owned migration driver.
    MigrationRequired,
    /// Structural and catalog evidence did not bind the same startup session.
    StartupIdentityMismatch,
    /// A commit's required provenance, event, or outbox intent was absent.
    MissingReciprocalRecord,
    /// A durable event did not reproduce its exact stored hash.
    EventHash,
}

impl fmt::Display for DurableInspectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::MissingDatabase => "durable inspection database is absent",
            Self::Io(_) => "durable inspection filesystem access failed",
            Self::Storage(_) => "durable inspection storage validation failed",
            Self::Catalog(_) => "durable inspection catalog validation failed",
            Self::TargetLimit => "durable inspection target bound exceeded",
            Self::NonCanonicalTargets => "durable inspection targets are not canonical",
            Self::RecordLimit => "durable inspection record bound exceeded",
            Self::NonCanonicalPage => "durable inspection page is not canonical",
            Self::MigrationRequired => "durable inspection requires the migration owner",
            Self::StartupIdentityMismatch => "durable inspection startup identities differ",
            Self::MissingReciprocalRecord => "durable inspection reciprocal record is absent",
            Self::EventHash => "durable inspection event hash does not reproduce",
        })
    }
}

impl Error for DurableInspectionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Storage(error) => Some(error),
            Self::Catalog(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use riffdb_storage_api::{EntityTarget, ReadableDigestKey, StorageValueError};
    use riffdb_types::{DigestKeyId, EntityKeyBuilder, EntityTypeId};

    use super::*;

    fn entity_target(value: u64) -> Result<EntityTarget, StorageValueError> {
        let entity_type = EntityTypeId::new(1).expect("entity type");
        let mut key = EntityKeyBuilder::new(entity_type);
        key.push_u64(value).expect("key component");
        EntityTarget::new(entity_type, key.finish().expect("entity key"))
    }

    #[test]
    fn inspection_targets_are_strictly_ordered_and_unique() {
        let first = entity_target(1).expect("first");
        let second = entity_target(2).expect("second");
        assert!(DurableInspectionRequest::new(vec![first.clone(), second], Vec::new()).is_ok());
        assert!(matches!(
            DurableInspectionRequest::new(vec![first.clone(), first], Vec::new()),
            Err(DurableInspectionError::NonCanonicalTargets)
        ));
    }

    #[test]
    fn missing_database_is_not_initialized_by_inspection() {
        let path =
            std::env::temp_dir().join(format!("riffdb-missing-inspection-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let digest_key = ReadableDigestKey::v1(DigestKeyId::new(1).expect("digest key"));
        let inputs = StartupValidationInputs::new(
            riffdb_types::Timestamp::new(1, 0).expect("timestamp"),
            riffdb_storage_api::ReadableCapabilityDigestInventory::new(vec![digest_key])
                .expect("capability inventory"),
            riffdb_storage_api::ReadableIdempotencyDigestInventory::new(vec![digest_key])
                .expect("idempotency inventory"),
        );
        let request = DurableInspectionRequest::new(Vec::new(), Vec::new()).expect("request");
        assert!(matches!(
            inspect_redb(&path, inputs, &request),
            Err(DurableInspectionError::MissingDatabase)
        ));
        assert!(!path.exists());
    }
}
