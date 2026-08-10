//! Read-only, semantically validated durable-state inspection.

use std::error::Error;
use std::fmt;
use std::fs;
use std::num::NonZeroU16;
use std::path::Path;

use riffdb_catalog::{CatalogError, CatalogHistoryOutcome, validate_catalog_history};
use riffdb_storage_api::{
    AdministrationAuditReader, AdministrationAuditScan, AdministrationAuditScanRequest,
    AdmissionLookupResultV1, AdmissionRepository, AuthoritativeIndexScanPage,
    AuthoritativeIndexScanRequest, AuthoritativePointReader, AuthoritativeScanReader,
    CommitScanPageV1, CommitScanRequest, EntityTarget, EvidencePageLimit, IdempotencyIdentity,
    IdempotencyLookupCandidatesV1, IndexEpochPosition, IndexRangeEntry, IndexRangeTarget,
    OutboxPageLimit, OutboxRepository, OutboxStatusReadResultV1, PendingOutboxScanV1,
    ProjectionQueryReader, ProjectionStatus, RetainedMetadataV1, StartupValidationInputs,
    StorageError, StorageErrorKind, StorageScanLimit, StoredAdministrationAuditRecordV1,
    StoredAdmissionStateV1, StoredCommitRecordV1, StoredDurableEventV1, StoredEntityRecordV1,
    StoredOutboxIntentV1, StoredOutcomeV1, StoredProvenanceRecordV1, StructuralEvidenceCursor,
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
#[derive(Clone, Eq, PartialEq)]
pub struct DurableInspectionRequest {
    entities: Vec<EntityTarget>,
    projections: Vec<ProjectionIdentity>,
    index_ranges: Vec<IndexRangeTarget>,
    admissions: Vec<IdempotencyIdentity>,
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
            index_ranges: Vec::new(),
            admissions: Vec::new(),
        })
    }

    /// Adds bounded, strictly ordered secondary-index range targets.
    ///
    /// Each target is scanned to its exact end through the store's own
    /// `AuthoritativeScanReader::scan_index` surface, which re-validates every
    /// row through the reader's decode and filters to the target's exact
    /// partition, and observes the range's index epoch in the same read view.
    pub fn with_index_ranges(
        mut self,
        index_ranges: Vec<IndexRangeTarget>,
    ) -> Result<Self, DurableInspectionError> {
        if index_ranges.len() > MAX_INSPECTION_TARGETS {
            return Err(DurableInspectionError::TargetLimit);
        }
        if index_ranges.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(DurableInspectionError::NonCanonicalTargets);
        }
        self.index_ranges = index_ranges;
        Ok(self)
    }

    /// Adds bounded admission identities, strictly ordered by storage key.
    ///
    /// Each identity is looked up through the store's own
    /// `AdmissionRepository::lookup_admission` (which decodes locator-backed
    /// idempotency records through the reader's own path) and through
    /// `AuthoritativePointReader::read_stored_outcome` for the terminal
    /// outcome view.
    pub fn with_admissions(
        mut self,
        admissions: Vec<IdempotencyIdentity>,
    ) -> Result<Self, DurableInspectionError> {
        if admissions.len() > MAX_INSPECTION_TARGETS {
            return Err(DurableInspectionError::TargetLimit);
        }
        let keys = admissions
            .iter()
            .map(|identity| {
                identity
                    .storage_key()
                    .map_err(|_| DurableInspectionError::NonCanonicalTargets)
            })
            .collect::<Result<Vec<_>, _>>()?;
        if keys.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(DurableInspectionError::NonCanonicalTargets);
        }
        self.admissions = admissions;
        Ok(self)
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

    /// Borrows exact secondary-index range targets.
    #[must_use]
    pub fn index_ranges(&self) -> &[IndexRangeTarget] {
        &self.index_ranges
    }

    /// Borrows exact admission lookup identities.
    #[must_use]
    pub fn admissions(&self) -> &[IdempotencyIdentity] {
        &self.admissions
    }
}

impl fmt::Debug for DurableInspectionRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Manual impl: `IndexRangeTarget` carries no `Debug` and identities
        // redact; counts identify the request without exposing key material.
        formatter
            .debug_struct("DurableInspectionRequest")
            .field("entity_count", &self.entities.len())
            .field("projection_count", &self.projections.len())
            .field("index_range_count", &self.index_ranges.len())
            .field("admission_count", &self.admissions.len())
            .finish()
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

/// One complete index range: its rows and epoch from the reader's own scan.
#[derive(Clone, Eq, PartialEq)]
pub struct InspectedIndexRange {
    target: IndexRangeTarget,
    epoch: IndexEpochPosition,
    entries: Vec<IndexRangeEntry>,
}

impl InspectedIndexRange {
    /// Borrows the canonical requested range target.
    #[must_use]
    pub const fn target(&self) -> &IndexRangeTarget {
        &self.target
    }

    /// Returns the range epoch observed with the rows.
    #[must_use]
    pub const fn epoch(&self) -> IndexEpochPosition {
        self.epoch
    }

    /// Borrows the complete scanned rows in canonical key order.
    ///
    /// The scan surface exposes `(key, covered_values)`; the row's schema
    /// binding and partition key are validated by the reader's own decode and
    /// partition filter rather than re-exposed here.
    #[must_use]
    pub fn entries(&self) -> &[IndexRangeEntry] {
        &self.entries
    }
}

impl fmt::Debug for InspectedIndexRange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InspectedIndexRange")
            .field("target", &"[REDACTED]")
            .field("epoch", &self.epoch)
            .field("entry_count", &self.entries.len())
            .finish()
    }
}

/// One admission identity and its ordinary present-or-absent durable state.
#[derive(Clone, Eq, PartialEq)]
pub struct InspectedAdmission {
    identity: IdempotencyIdentity,
    state: Option<StoredAdmissionStateV1>,
}

impl InspectedAdmission {
    /// Borrows the exact requested identity.
    #[must_use]
    pub const fn identity(&self) -> &IdempotencyIdentity {
        &self.identity
    }

    /// Borrows the complete stored admission state when present.
    #[must_use]
    pub const fn state(&self) -> Option<&StoredAdmissionStateV1> {
        self.state.as_ref()
    }
}

impl fmt::Debug for InspectedAdmission {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InspectedAdmission")
            .field("identity", &"[REDACTED]")
            .field(
                "state",
                &self.state.as_ref().map(|state| match state {
                    StoredAdmissionStateV1::Pending(_) => "Pending",
                    StoredAdmissionStateV1::StoredOutcome(_) => "StoredOutcome",
                    StoredAdmissionStateV1::ExecutionFailed(_) => "ExecutionFailed",
                }),
            )
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
    structural_pages: u32,
    application_frontier: FrontierPosition,
    commits: Vec<StoredCommitRecordV1>,
    entities: Vec<InspectedEntity>,
    provenance: Vec<StoredProvenanceRecordV1>,
    events: Vec<InspectedEvent>,
    projections: Vec<ProjectionStatus>,
    administration: Vec<StoredAdministrationAuditRecordV1>,
    index_ranges: Vec<InspectedIndexRange>,
    admissions: Vec<InspectedAdmission>,
    outcomes: Vec<StoredOutcomeV1>,
    outbox_intents: Vec<StoredOutboxIntentV1>,
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

    /// Returns how many structural evidence pages the pass actually read.
    ///
    /// An empty [`Self::structural_findings`] claim stands on its own reach
    /// only when at least one page was produced and inspected.
    #[must_use]
    pub const fn structural_pages(&self) -> u32 {
        self.structural_pages
    }

    /// Returns the application frontier frozen by the exclusive commit scan.
    ///
    /// The database is stopped for the whole inspection, so this is the
    /// recovered application frontier: the last contiguous applied commit.
    #[must_use]
    pub const fn application_frontier(&self) -> FrontierPosition {
        self.application_frontier
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

    /// Borrows index-range observations in request order.
    #[must_use]
    pub fn index_ranges(&self) -> &[InspectedIndexRange] {
        &self.index_ranges
    }

    /// Borrows admission observations in request order.
    #[must_use]
    pub fn admissions(&self) -> &[InspectedAdmission] {
        &self.admissions
    }

    /// Borrows terminal outcomes ascending by their own stored sequence.
    ///
    /// Assembled from the requested admission identities through
    /// `read_stored_outcome` — the store's only public outcome read surface —
    /// and keyed by each record's own commit sequence. Identities whose
    /// admission state is pending, failed, or absent contribute no record.
    #[must_use]
    pub fn outcomes(&self) -> &[StoredOutcomeV1] {
        &self.outcomes
    }

    /// Borrows authoritative outbox intent records in event order.
    ///
    /// The population is every reciprocal intent whose effective delivery
    /// state is still pending (`scan_pending_outbox` is the store's only
    /// public read surface exposing the stored intent record, distinct from
    /// the status-read view). On a store where no delivery transition ever
    /// ran this is the complete intent population; the per-event
    /// intent-existence proof for every committed event is separately
    /// enforced by the reciprocal-record validation in the events pass.
    #[must_use]
    pub fn outbox_intents(&self) -> &[StoredOutboxIntentV1] {
        &self.outbox_intents
    }
}

impl fmt::Debug for DurableInspection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DurableInspection")
            .field("metadata", &self.metadata)
            .field("structural_findings", &self.structural_findings)
            .field("structural_pages", &self.structural_pages)
            .field("application_frontier", &self.application_frontier)
            .field("commit_count", &self.commits.len())
            .field("entity_count", &self.entities.len())
            .field("provenance_count", &self.provenance.len())
            .field("event_count", &self.events.len())
            .field("projection_count", &self.projections.len())
            .field("administration_count", &self.administration.len())
            .field("index_range_count", &self.index_ranges.len())
            .field("admission_count", &self.admissions.len())
            .field("outcome_count", &self.outcomes.len())
            .field("outbox_intent_count", &self.outbox_intents.len())
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
    inspect_opened_redb(store, startup_inputs, request)
}

/// Performs the same complete read-only pass over an already-opened store.
///
/// This is the entry point for stores whose media cannot be reached through a
/// filesystem path — the deterministic-simulation harness opens through
/// `RedbStore::open_with_storage_media` and hands the store here. The pass is
/// identical to [`inspect_redb`] from the structural evidence session onward;
/// only the filesystem existence preflight is skipped, because no real file
/// exists.
pub fn inspect_opened_redb(
    store: RedbStore,
    startup_inputs: StartupValidationInputs,
    request: &DurableInspectionRequest,
) -> Result<DurableInspection, DurableInspectionError> {
    let (retained, structural_findings, structural_pages, ports) =
        open_validated(store, startup_inputs)?;
    let (commits, application_frontier) = scan_all_commits(&ports)?;
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

    let index_ranges = request
        .index_ranges()
        .iter()
        .map(|target| scan_full_index_range(&ports, target))
        .collect::<Result<Vec<_>, _>>()?;
    let (admissions, outcomes) = read_admissions_and_outcomes(&ports, request.admissions())?;
    let outbox_intents = scan_all_pending_outbox_intents(&ports)?;
    drop(ports);

    Ok(DurableInspection {
        metadata: retained,
        structural_findings,
        structural_pages,
        application_frontier,
        commits,
        entities,
        provenance,
        events,
        projections,
        administration,
        index_ranges,
        admissions,
        outcomes,
        outbox_intents,
    })
}

fn open_validated(
    store: RedbStore,
    startup_inputs: StartupValidationInputs,
) -> Result<
    (
        RetainedMetadataV1,
        Vec<StructuralFinding>,
        u32,
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
    let mut pages = 0_u32;
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
                pages = pages
                    .checked_add(1)
                    .ok_or(DurableInspectionError::RecordLimit)?;
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
    Ok((retained, findings, pages, ports))
}

fn scan_all_commits(
    ports: &RedbOperationalPorts,
) -> Result<(Vec<StoredCommitRecordV1>, FrontierPosition), DurableInspectionError> {
    let limit = StorageScanLimit::new(PAGE_LIMIT).ok_or(DurableInspectionError::RecordLimit)?;
    let mut request = CommitScanRequest::initial(limit);
    let mut records = Vec::new();
    let mut frontier = None;
    loop {
        let page = ports
            .scan_commits(request)
            .map_err(DurableInspectionError::Storage)?;
        // The first page freezes the applied frontier as its inclusive upper
        // fence; continuation pages must reuse it exactly.
        let fence = page.inclusive_upper();
        match frontier {
            None => frontier = Some(fence),
            Some(frozen) if frozen == fence => {}
            Some(_) => return Err(DurableInspectionError::NonCanonicalPage),
        }
        append_bounded(
            &mut records,
            page.records().iter().map(|item| item.value().clone()),
        )?;
        match page {
            CommitScanPageV1::ExactEnd { .. } => {
                let frontier = frontier.ok_or(DurableInspectionError::NonCanonicalPage)?;
                return Ok((records, frontier));
            }
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

/// Scans one declared index range to its exact end through the reader's own
/// decode, requiring one stable epoch observation across every page.
fn scan_full_index_range(
    ports: &RedbOperationalPorts,
    target: &IndexRangeTarget,
) -> Result<InspectedIndexRange, DurableInspectionError> {
    let limit = StorageScanLimit::new(PAGE_LIMIT).ok_or(DurableInspectionError::RecordLimit)?;
    let mut after = None;
    let mut entries = Vec::new();
    let mut epoch = None;
    loop {
        let request = AuthoritativeIndexScanRequest::new(target.clone(), after, limit)
            .map_err(|_| DurableInspectionError::NonCanonicalPage)?;
        let page = ports
            .scan_index(request)
            .map_err(DurableInspectionError::Storage)?;
        // The database is stopped, so the epoch cannot legitimately move
        // between pages of one range.
        match epoch {
            None => epoch = Some(page.epoch()),
            Some(observed) if observed == page.epoch() => {}
            Some(_) => return Err(DurableInspectionError::NonCanonicalPage),
        }
        append_bounded(
            &mut entries,
            page.entries().iter().map(|item| item.value().clone()),
        )?;
        match page {
            AuthoritativeIndexScanPage::ExactEnd { .. } => {
                let epoch = epoch.ok_or(DurableInspectionError::NonCanonicalPage)?;
                return Ok(InspectedIndexRange {
                    target: target.clone(),
                    epoch,
                    entries,
                });
            }
            AuthoritativeIndexScanPage::Page { next_after, .. } => after = Some(next_after),
        }
    }
}

/// Looks up each declared identity's admission state and terminal outcome.
///
/// Both reads go through the store's own decode paths (`lookup_admission`
/// resolves locator-backed idempotency records; `read_stored_outcome` resolves
/// the capsule path), never raw rows. Outcomes are returned ascending by their
/// own stored commit sequence — the store's only public outcome read surface
/// is identity-keyed, so the sequence-keyed view is assembled here.
fn read_admissions_and_outcomes(
    ports: &RedbOperationalPorts,
    identities: &[IdempotencyIdentity],
) -> Result<(Vec<InspectedAdmission>, Vec<StoredOutcomeV1>), DurableInspectionError> {
    let mut admissions = Vec::with_capacity(identities.len());
    let mut outcomes: Vec<StoredOutcomeV1> = Vec::new();
    for identity in identities {
        let candidates = IdempotencyLookupCandidatesV1::new(vec![identity.clone()])
            .map_err(|_| DurableInspectionError::NonCanonicalTargets)?;
        let state = match ports
            .lookup_admission(candidates)
            .map_err(DurableInspectionError::Storage)?
        {
            AdmissionLookupResultV1::NotFound => None,
            AdmissionLookupResultV1::Found(state) => {
                if state.identity() != identity {
                    return Err(DurableInspectionError::Storage(StorageError::new(
                        StorageErrorKind::InvariantViolation,
                        None,
                    )));
                }
                Some(*state)
            }
            // A single-candidate lookup can never resolve multiple matches.
            AdmissionLookupResultV1::MultipleMatches => {
                return Err(DurableInspectionError::Storage(StorageError::new(
                    StorageErrorKind::InvariantViolation,
                    None,
                )));
            }
        };
        if let Some(outcome) = ports
            .read_stored_outcome(identity)
            .map_err(DurableInspectionError::Storage)?
        {
            if outcome.identity() != identity {
                return Err(DurableInspectionError::Storage(StorageError::new(
                    StorageErrorKind::InvariantViolation,
                    None,
                )));
            }
            outcomes.push(outcome);
        }
        admissions.push(InspectedAdmission {
            identity: identity.clone(),
            state,
        });
    }
    outcomes.sort_by_key(StoredOutcomeV1::commit_sequence);
    Ok((admissions, outcomes))
}

/// Scans every effectively-pending authoritative outbox intent to exact end.
fn scan_all_pending_outbox_intents(
    ports: &RedbOperationalPorts,
) -> Result<Vec<StoredOutboxIntentV1>, DurableInspectionError> {
    let limit = NonZeroU16::new(PAGE_LIMIT)
        .and_then(|value| OutboxPageLimit::new(value).ok())
        .ok_or(DurableInspectionError::RecordLimit)?;
    let mut after = None;
    let mut intents = Vec::new();
    loop {
        let page = ports
            .scan_pending_outbox(after, limit)
            .map_err(DurableInspectionError::Storage)?;
        match page {
            PendingOutboxScanV1::Page { items, next_after } => {
                append_bounded(
                    &mut intents,
                    items.iter().map(|item| item.value().intent().clone()),
                )?;
                after = Some(next_after);
            }
            PendingOutboxScanV1::ExactEnd { items } => {
                append_bounded(
                    &mut intents,
                    items.iter().map(|item| item.value().intent().clone()),
                )?;
                return Ok(intents);
            }
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
    use riffdb_storage_api::{
        EntityTarget, IdempotencyKeyDigest, IndexRangePrefixBuilder, ReadableDigestKey,
        StorageValueError,
    };
    use riffdb_types::{
        ActorId, AggregateTypeId, ContractLineage, DatabaseId, DigestKeyId, EntityKeyBuilder,
        EntityTypeId, Environment, IndexId, PartitionKeyBuilder, TenantId, TenantScope,
    };

    use super::*;

    fn entity_target(value: u64) -> Result<EntityTarget, StorageValueError> {
        let entity_type = EntityTypeId::new(1).expect("entity type");
        let mut key = EntityKeyBuilder::new(entity_type);
        key.push_u64(value).expect("key component");
        EntityTarget::new(entity_type, key.finish().expect("entity key"))
    }

    fn index_range(partition: u64) -> IndexRangeTarget {
        let mut prefix = IndexRangePrefixBuilder::new(IndexId::new(1).expect("index"));
        prefix.push_u64(10).expect("prefix component");
        let mut partition_key = PartitionKeyBuilder::new(AggregateTypeId::new(1).expect("agg"));
        partition_key.push_u64(partition).expect("partition");
        IndexRangeTarget::new(
            partition_key.finish().expect("partition key"),
            prefix.finish(),
        )
    }

    fn admission_identity(digest: u8) -> IdempotencyIdentity {
        IdempotencyIdentity::new(
            DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10])
                .expect("database ID"),
            Environment::new("test").expect("environment"),
            TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant")),
            ActorId::new("principal-a").expect("principal"),
            ContractLineage::new("inspection-test").expect("lineage"),
            riffdb_types::CommandId::new(1).expect("command"),
            IdempotencyKeyDigest::from_hmac_bytes(
                DigestKeyId::new(1).expect("digest key"),
                [digest; 32],
            ),
        )
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
    fn index_range_targets_are_strictly_ordered_and_bounded() {
        let base = DurableInspectionRequest::new(Vec::new(), Vec::new()).expect("request");
        let ordered = if index_range(1) < index_range(2) {
            vec![index_range(1), index_range(2)]
        } else {
            vec![index_range(2), index_range(1)]
        };
        assert!(base.clone().with_index_ranges(ordered.clone()).is_ok());
        assert!(matches!(
            base.clone()
                .with_index_ranges(ordered.into_iter().rev().collect()),
            Err(DurableInspectionError::NonCanonicalTargets)
        ));
        assert!(matches!(
            base.clone()
                .with_index_ranges(vec![index_range(1), index_range(1)]),
            Err(DurableInspectionError::NonCanonicalTargets)
        ));
        let too_many = (0..=u64::try_from(MAX_INSPECTION_TARGETS).expect("bound"))
            .map(index_range)
            .collect::<Vec<_>>();
        assert!(matches!(
            base.with_index_ranges(too_many),
            Err(DurableInspectionError::TargetLimit)
        ));
    }

    #[test]
    fn admission_identities_are_strictly_ordered_by_storage_key_and_bounded() {
        let base = DurableInspectionRequest::new(Vec::new(), Vec::new()).expect("request");
        let first = admission_identity(0x01);
        let second = admission_identity(0x02);
        let ordered = if first.storage_key().expect("key") < second.storage_key().expect("key") {
            vec![first.clone(), second.clone()]
        } else {
            vec![second.clone(), first.clone()]
        };
        assert!(base.clone().with_admissions(ordered.clone()).is_ok());
        assert!(matches!(
            base.clone()
                .with_admissions(ordered.into_iter().rev().collect()),
            Err(DurableInspectionError::NonCanonicalTargets)
        ));
        assert!(matches!(
            base.clone().with_admissions(vec![first.clone(), first]),
            Err(DurableInspectionError::NonCanonicalTargets)
        ));
        let too_many = (0..=MAX_INSPECTION_TARGETS)
            .map(|ordinal| admission_identity(u8::try_from(ordinal % 251).expect("bounded")))
            .collect::<Vec<_>>();
        assert!(matches!(
            base.with_admissions(too_many),
            Err(DurableInspectionError::TargetLimit)
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
