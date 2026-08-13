//! Canonical durable progress for one compiler-owned application reimport stage.

use std::{error::Error, fmt, num::NonZeroU64};

use riffdb_types::{
    ApplicationExportManifestHash, ApplicationExportPageHash, ApplicationExportReceiptHash,
    ApplicationPortabilityManifestHash, ApplicationReimportAuthorityV1,
    ApplicationReimportReceiptHash, CapabilityApplicationReimportScopeV1, CapabilityId, DatabaseId,
    GeneratedArtifactHash, hash_application_reimport_receipt,
};
use serde::{Deserialize, Serialize};

use crate::{
    ApplicationPortabilityManifest, ApplicationReimportReceipt, InstallationSymbol,
    PortableRecordClass, ReimportMappingResult, ReimportObservationResult,
};

/// Canonical durable reimport checkpoint schema.
pub const APPLICATION_REIMPORT_CAMPAIGN_SCHEMA_V1: &str = "riffdb.application-reimport-campaign/v1";
/// Maximum canonical bytes nested into one installation campaign.
pub const MAX_APPLICATION_REIMPORT_CAMPAIGN_BYTES: usize = 2 * 1_024 * 1_024;
/// Maximum source page identities retained by one alpha campaign.
pub const MAX_APPLICATION_REIMPORT_PAGE_HASHES: usize = 4_096;

/// One source-snapshot workflow proof with no row values.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ReimportWorkflowQuiescenceV1 {
    workflow: InstallationSymbol,
    checked_rows: u64,
    quiescent_rows: u64,
}

impl ReimportWorkflowQuiescenceV1 {
    /// Accepts only complete evidence that every checked row was quiescent.
    pub fn new(
        workflow: InstallationSymbol,
        checked_rows: u64,
        quiescent_rows: u64,
        non_quiescent_rows: u64,
    ) -> Result<Self, ApplicationReimportCampaignError> {
        if checked_rows != quiescent_rows || non_quiescent_rows != 0 {
            return Err(ApplicationReimportCampaignError::new(
                ApplicationReimportCampaignErrorKind::WorkflowNotQuiescent,
            ));
        }
        Ok(Self {
            workflow,
            checked_rows,
            quiescent_rows,
        })
    }

    /// Symbolic workflow name.
    #[must_use]
    pub const fn workflow(&self) -> &InstallationSymbol {
        &self.workflow
    }

    /// Rows checked at the exact export snapshot.
    #[must_use]
    pub const fn checked_rows(&self) -> u64 {
        self.checked_rows
    }
}

/// Exact completed portability-export source accepted at campaign start.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationReimportSourceV1 {
    export_manifest_hash: ApplicationExportManifestHash,
    export_receipt_hash: ApplicationExportReceiptHash,
    portability_manifest_hash: ApplicationPortabilityManifestHash,
    source_database_id: DatabaseId,
    target_database_id: DatabaseId,
    rows: u64,
    page_hashes: Vec<ApplicationExportPageHash>,
    workflow_quiescence: Vec<ReimportWorkflowQuiescenceV1>,
}

impl ApplicationReimportSourceV1 {
    /// Checks a completed, nonempty, exact portability source boundary.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        export_manifest_hash: ApplicationExportManifestHash,
        export_receipt_hash: ApplicationExportReceiptHash,
        portability_manifest_hash: ApplicationPortabilityManifestHash,
        source_database_id: DatabaseId,
        target_database_id: DatabaseId,
        rows: u64,
        page_hashes: Vec<ApplicationExportPageHash>,
        mut workflow_quiescence: Vec<ReimportWorkflowQuiescenceV1>,
    ) -> Result<Self, ApplicationReimportCampaignError> {
        if source_database_id == target_database_id
            || page_hashes.is_empty()
            || page_hashes.len() > MAX_APPLICATION_REIMPORT_PAGE_HASHES
        {
            return Err(ApplicationReimportCampaignError::new(
                ApplicationReimportCampaignErrorKind::InvalidSource,
            ));
        }
        workflow_quiescence.sort();
        if workflow_quiescence
            .windows(2)
            .any(|pair| pair[0].workflow == pair[1].workflow)
        {
            return Err(ApplicationReimportCampaignError::new(
                ApplicationReimportCampaignErrorKind::Duplicate,
            ));
        }
        Ok(Self {
            export_manifest_hash,
            export_receipt_hash,
            portability_manifest_hash,
            source_database_id,
            target_database_id,
            rows,
            page_hashes,
            workflow_quiescence,
        })
    }

    /// Exact completed export manifest identity.
    #[must_use]
    pub const fn export_manifest_hash(&self) -> ApplicationExportManifestHash {
        self.export_manifest_hash
    }

    /// Exact completed export receipt identity.
    #[must_use]
    pub const fn export_receipt_hash(&self) -> ApplicationExportReceiptHash {
        self.export_receipt_hash
    }

    /// Exact adapter-owned portability manifest identity.
    #[must_use]
    pub const fn portability_manifest_hash(&self) -> ApplicationPortabilityManifestHash {
        self.portability_manifest_hash
    }

    /// New destination database identity.
    #[must_use]
    pub const fn target_database_id(&self) -> DatabaseId {
        self.target_database_id
    }

    /// Original database identity proving this is not an in-place import.
    #[must_use]
    pub const fn source_database_id(&self) -> DatabaseId {
        self.source_database_id
    }

    /// Exact exported row count across every page.
    #[must_use]
    pub const fn rows(&self) -> u64 {
        self.rows
    }

    /// Exact source page identities in original one-based order.
    #[must_use]
    pub fn page_hashes(&self) -> &[ApplicationExportPageHash] {
        &self.page_hashes
    }
}

/// One compiler-selected mapping's durable aggregate progress.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ReimportMappingProgressV1 {
    class: PortableRecordClass,
    symbol: InstallationSymbol,
    records: u64,
    succeeded: u64,
    replayed: u64,
    outcome_hash: GeneratedArtifactHash,
}

impl ReimportMappingProgressV1 {
    fn empty(class: PortableRecordClass, symbol: InstallationSymbol) -> Self {
        Self {
            class,
            symbol,
            records: 0,
            succeeded: 0,
            replayed: 0,
            outcome_hash: GeneratedArtifactHash::from_bytes([0; 32]),
        }
    }

    fn apply(
        &mut self,
        records: u64,
        replayed: bool,
        outcome_hash: GeneratedArtifactHash,
    ) -> Result<(), ApplicationReimportCampaignError> {
        self.records = self.records.checked_add(records).ok_or_else(limit)?;
        if replayed {
            self.replayed = self.replayed.checked_add(records).ok_or_else(limit)?;
        } else {
            self.succeeded = self.succeeded.checked_add(records).ok_or_else(limit)?;
        }
        self.outcome_hash = outcome_hash;
        Ok(())
    }

    fn terminal(&self) -> Result<ReimportMappingResult, ApplicationReimportCampaignError> {
        ReimportMappingResult::new(
            self.class,
            self.symbol.clone(),
            self.records,
            self.succeeded,
            self.replayed,
            self.outcome_hash,
        )
        .map_err(|_| integrity())
    }

    /// Portable record class selected by the compiler.
    #[must_use]
    pub const fn class(&self) -> PortableRecordClass {
        self.class
    }

    /// Contract symbol selected by the compiler.
    #[must_use]
    pub const fn symbol(&self) -> &InstallationSymbol {
        &self.symbol
    }

    /// Total durable records applied or replayed.
    #[must_use]
    pub const fn records(&self) -> u64 {
        self.records
    }
}

/// Closed lifecycle of the unpublished reimport stage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationReimportCampaignPhaseV1 {
    /// Exact source accepted; the next source page may be applied.
    Applying,
    /// Every page is durable; named observations remain.
    Reconciling,
    /// Receipt sealed and eligible for installation-stage completion.
    Reconciled,
    /// Operator cancelled; destination remains not ready.
    Cancelled,
    /// Terminal safe failure; destination remains not ready.
    Failed,
}

/// Closed redaction-safe terminal failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationReimportFailureV1 {
    /// Current capability identity or revision changed.
    AuthorityChanged,
    /// A page, record, or hash did not match the completed source.
    SourceMismatch,
    /// A compiled reimport command selected a terminal non-success outcome.
    CommandFailed,
    /// Final named observations did not reconcile.
    ObservationMismatch,
    /// Operator cancelled the unpublished campaign.
    Cancelled,
}

/// Exact result of one atomic compiler-owned page command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReimportPageMappingOutcomeV1 {
    class: PortableRecordClass,
    symbol: InstallationSymbol,
    records: u64,
    replayed: bool,
    outcome_hash: GeneratedArtifactHash,
}

impl ReimportPageMappingOutcomeV1 {
    /// Constructs one nonempty value-free mapping outcome.
    pub fn new(
        class: PortableRecordClass,
        symbol: InstallationSymbol,
        records: u64,
        replayed: bool,
        outcome_hash: GeneratedArtifactHash,
    ) -> Result<Self, ApplicationReimportCampaignError> {
        if records == 0 {
            return Err(ApplicationReimportCampaignError::new(
                ApplicationReimportCampaignErrorKind::InvalidProgress,
            ));
        }
        Ok(Self {
            class,
            symbol,
            records,
            replayed,
            outcome_hash,
        })
    }
}

/// Durable resumable checkpoint nested into one exact installation campaign.
#[derive(Clone, Eq, PartialEq)]
pub struct ApplicationReimportCampaignV1 {
    source: ApplicationReimportSourceV1,
    portability_manifest_document: Vec<u8>,
    authority: ApplicationReimportAuthorityV1,
    scope: CapabilityApplicationReimportScopeV1,
    phase: ApplicationReimportCampaignPhaseV1,
    next_page: NonZeroU64,
    rows_applied: u64,
    mappings: Vec<ReimportMappingProgressV1>,
    failure: Option<ApplicationReimportFailureV1>,
    receipt_hash: Option<ApplicationReimportReceiptHash>,
    receipt_document: Option<Vec<u8>>,
}

impl ApplicationReimportCampaignV1 {
    /// Starts one exact source-bound campaign with compiler-selected mappings.
    pub fn start(
        source: ApplicationReimportSourceV1,
        authority: ApplicationReimportAuthorityV1,
        scope: CapabilityApplicationReimportScopeV1,
        manifest: &ApplicationPortabilityManifest,
    ) -> Result<Self, ApplicationReimportCampaignError> {
        if source.portability_manifest_hash != manifest.identity() {
            return Err(ApplicationReimportCampaignError::new(
                ApplicationReimportCampaignErrorKind::ManifestMismatch,
            ));
        }
        let mappings = manifest
            .input()
            .mappings
            .iter()
            .map(|mapping| {
                ReimportMappingProgressV1::empty(mapping.class(), mapping.symbol().clone())
            })
            .collect();
        Ok(Self {
            source,
            portability_manifest_document: manifest.canonical_bytes().to_vec(),
            authority,
            scope,
            phase: ApplicationReimportCampaignPhaseV1::Applying,
            next_page: NonZeroU64::MIN,
            rows_applied: 0,
            mappings,
            failure: None,
            receipt_hash: None,
            receipt_document: None,
        })
    }

    /// Exact source evidence.
    #[must_use]
    pub const fn source(&self) -> &ApplicationReimportSourceV1 {
        &self.source
    }

    /// Canonical adapter manifest retained for restart and reconciliation.
    #[must_use]
    pub fn portability_manifest_document(&self) -> &[u8] {
        &self.portability_manifest_document
    }

    /// Current authority identity frozen at start.
    #[must_use]
    pub const fn authority(&self) -> ApplicationReimportAuthorityV1 {
        self.authority
    }

    /// Capability scope frozen for this campaign.
    #[must_use]
    pub const fn scope(&self) -> CapabilityApplicationReimportScopeV1 {
        self.scope
    }

    /// Aggregate progress in compiler-selected mapping order.
    #[must_use]
    pub fn mappings(&self) -> &[ReimportMappingProgressV1] {
        &self.mappings
    }

    /// Current phase.
    #[must_use]
    pub const fn phase(&self) -> ApplicationReimportCampaignPhaseV1 {
        self.phase
    }

    /// Next exact one-based source page.
    #[must_use]
    pub const fn next_page(&self) -> NonZeroU64 {
        self.next_page
    }

    /// Safe terminal failure, if any.
    #[must_use]
    pub const fn failure(&self) -> Option<ApplicationReimportFailureV1> {
        self.failure
    }

    /// Final reconciliation receipt, present only after success.
    #[must_use]
    pub const fn receipt_hash(&self) -> Option<ApplicationReimportReceiptHash> {
        self.receipt_hash
    }

    /// Canonical terminal receipt bytes retained across restart.
    #[must_use]
    pub fn receipt_document(&self) -> Option<&[u8]> {
        self.receipt_document.as_deref()
    }

    /// Fails closed if routine capability administration changed campaign authority.
    pub fn verify_authority(
        &mut self,
        authority: ApplicationReimportAuthorityV1,
    ) -> Result<(), ApplicationReimportCampaignError> {
        if self.authority == authority {
            return Ok(());
        }
        self.fail(ApplicationReimportFailureV1::AuthorityChanged);
        Err(ApplicationReimportCampaignError::new(
            ApplicationReimportCampaignErrorKind::AuthorityChanged,
        ))
    }

    /// Advances exactly one source page after its ordinary command commits.
    pub fn complete_page(
        &mut self,
        page_number: NonZeroU64,
        page_hash: ApplicationExportPageHash,
        outcomes: Vec<ReimportPageMappingOutcomeV1>,
    ) -> Result<(), ApplicationReimportCampaignError> {
        if self.phase != ApplicationReimportCampaignPhaseV1::Applying
            || page_number != self.next_page
        {
            return Err(ApplicationReimportCampaignError::new(
                ApplicationReimportCampaignErrorKind::PageOutOfOrder,
            ));
        }
        let index = usize::try_from(page_number.get() - 1).map_err(|_| limit())?;
        if self.source.page_hashes.get(index) != Some(&page_hash) {
            self.fail(ApplicationReimportFailureV1::SourceMismatch);
            return Err(ApplicationReimportCampaignError::new(
                ApplicationReimportCampaignErrorKind::PageMismatch,
            ));
        }
        let mut next_mappings = self.mappings.clone();
        let mut page_rows = 0_u64;
        for outcome in outcomes {
            let mapping = next_mappings
                .iter_mut()
                .find(|mapping| mapping.class == outcome.class && mapping.symbol == outcome.symbol)
                .ok_or_else(|| {
                    ApplicationReimportCampaignError::new(
                        ApplicationReimportCampaignErrorKind::MappingMismatch,
                    )
                })?;
            mapping.apply(outcome.records, outcome.replayed, outcome.outcome_hash)?;
            page_rows = page_rows.checked_add(outcome.records).ok_or_else(limit)?;
        }
        let rows_applied = self.rows_applied.checked_add(page_rows).ok_or_else(limit)?;
        if index + 1 == self.source.page_hashes.len() {
            if rows_applied != self.source.rows {
                self.fail(ApplicationReimportFailureV1::SourceMismatch);
                return Err(ApplicationReimportCampaignError::new(
                    ApplicationReimportCampaignErrorKind::SourceCountMismatch,
                ));
            }
            self.phase = ApplicationReimportCampaignPhaseV1::Reconciling;
        } else {
            self.next_page = NonZeroU64::new(page_number.get().checked_add(1).ok_or_else(limit)?)
                .ok_or_else(limit)?;
        }
        self.mappings = next_mappings;
        self.rows_applied = rows_applied;
        Ok(())
    }

    /// Seals terminal reconciliation only against the exact manifest and observations.
    pub fn reconcile(
        &mut self,
        manifest: &ApplicationPortabilityManifest,
        observations: Vec<ReimportObservationResult>,
    ) -> Result<ApplicationReimportReceipt, ApplicationReimportCampaignError> {
        if self.phase != ApplicationReimportCampaignPhaseV1::Reconciling
            || manifest.identity() != self.source.portability_manifest_hash
        {
            return Err(ApplicationReimportCampaignError::new(
                ApplicationReimportCampaignErrorKind::InvalidProgress,
            ));
        }
        let mappings = self
            .mappings
            .iter()
            .map(ReimportMappingProgressV1::terminal)
            .collect::<Result<Vec<_>, _>>()?;
        let result = ApplicationReimportReceipt::reconcile(
            manifest,
            self.source.export_manifest_hash,
            self.source.target_database_id,
            mappings,
            observations,
        );
        let receipt = match result {
            Ok(receipt) => receipt,
            Err(_) => {
                self.fail(ApplicationReimportFailureV1::ObservationMismatch);
                return Err(ApplicationReimportCampaignError::new(
                    ApplicationReimportCampaignErrorKind::ReconciliationMismatch,
                ));
            }
        };
        self.phase = ApplicationReimportCampaignPhaseV1::Reconciled;
        self.failure = None;
        self.receipt_hash = Some(receipt.identity());
        self.receipt_document = Some(receipt.canonical_bytes().to_vec());
        Ok(receipt)
    }

    /// Cancels an unpublished nonterminal destination.
    pub fn cancel(&mut self) -> Result<(), ApplicationReimportCampaignError> {
        match self.phase {
            ApplicationReimportCampaignPhaseV1::Applying
            | ApplicationReimportCampaignPhaseV1::Reconciling => {
                self.phase = ApplicationReimportCampaignPhaseV1::Cancelled;
                self.failure = Some(ApplicationReimportFailureV1::Cancelled);
                Ok(())
            }
            _ => Err(ApplicationReimportCampaignError::new(
                ApplicationReimportCampaignErrorKind::AlreadyTerminal,
            )),
        }
    }

    /// Records a terminal typed command failure without exposing its row values.
    pub fn fail_command(&mut self) -> Result<(), ApplicationReimportCampaignError> {
        match self.phase {
            ApplicationReimportCampaignPhaseV1::Applying
            | ApplicationReimportCampaignPhaseV1::Reconciling => {
                self.fail(ApplicationReimportFailureV1::CommandFailed);
                Ok(())
            }
            _ => Err(ApplicationReimportCampaignError::new(
                ApplicationReimportCampaignErrorKind::AlreadyTerminal,
            )),
        }
    }

    fn fail(&mut self, failure: ApplicationReimportFailureV1) {
        self.phase = ApplicationReimportCampaignPhaseV1::Failed;
        self.failure = Some(failure);
        self.receipt_hash = None;
        self.receipt_document = None;
    }

    /// Encodes exact bounded canonical durable state.
    pub fn encode_canonical(&self) -> Result<Vec<u8>, ApplicationReimportCampaignError> {
        validate(self)?;
        canonical_bytes(&CampaignDto::from_campaign(self))
    }

    /// Strictly decodes, validates, and checks canonical bytes.
    pub fn decode_canonical(bytes: &[u8]) -> Result<Self, ApplicationReimportCampaignError> {
        if bytes.is_empty() || bytes.len() > MAX_APPLICATION_REIMPORT_CAMPAIGN_BYTES {
            return Err(limit());
        }
        let dto: CampaignDto = serde_json::from_slice(bytes).map_err(|_| invalid())?;
        let campaign = dto.into_campaign()?;
        validate(&campaign)?;
        if campaign.encode_canonical()? != bytes {
            return Err(ApplicationReimportCampaignError::new(
                ApplicationReimportCampaignErrorKind::NonCanonical,
            ));
        }
        Ok(campaign)
    }
}

impl fmt::Debug for ApplicationReimportCampaignV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApplicationReimportCampaignV1")
            .field("phase", &self.phase)
            .field("next_page", &self.next_page)
            .field("rows_applied", &self.rows_applied)
            .field("mapping_count", &self.mappings.len())
            .finish_non_exhaustive()
    }
}

/// Closed durable-checkpoint failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationReimportCampaignErrorKind {
    /// Bytes are not the closed canonical schema.
    InvalidEncoding,
    /// Valid JSON did not use the sole canonical representation.
    NonCanonical,
    /// A bounded collection or encoded checkpoint exceeded its maximum.
    LimitExceeded,
    /// Source and destination evidence cannot describe a portability reimport.
    InvalidSource,
    /// The exact portability manifest does not match the source receipt.
    ManifestMismatch,
    /// A workflow contained an active or partially cleared lease.
    WorkflowNotQuiescent,
    /// A supposedly unique symbol or evidence row was repeated.
    Duplicate,
    /// Durable counters or lifecycle fields are internally inconsistent.
    InvalidProgress,
    /// The submitted page was not the sole expected next page.
    PageOutOfOrder,
    /// The submitted page did not match its completed export identity.
    PageMismatch,
    /// A page outcome named something outside the compiler-selected schedule.
    MappingMismatch,
    /// Applied records did not equal the completed export's declared total.
    SourceCountMismatch,
    /// Named destination observations did not match the portability manifest.
    ReconciliationMismatch,
    /// Current capability identity or revision changed at a safe point.
    AuthorityChanged,
    /// A terminal campaign cannot be advanced or cancelled again.
    AlreadyTerminal,
}

/// Static redaction-safe durable-checkpoint error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplicationReimportCampaignError {
    kind: ApplicationReimportCampaignErrorKind,
}

impl ApplicationReimportCampaignError {
    const fn new(kind: ApplicationReimportCampaignErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the closed machine-readable failure class.
    #[must_use]
    pub const fn kind(self) -> ApplicationReimportCampaignErrorKind {
        self.kind
    }
}

impl fmt::Display for ApplicationReimportCampaignError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("application reimport campaign evidence is invalid")
    }
}

impl Error for ApplicationReimportCampaignError {}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CampaignDto {
    schema: String,
    export_manifest_hash: String,
    export_receipt_hash: String,
    portability_manifest_hash: String,
    portability_manifest_document: String,
    source_database_id: String,
    target_database_id: String,
    rows: String,
    page_hashes: Vec<String>,
    workflow_quiescence: Vec<WorkflowDto>,
    capability_id: String,
    capability_revision: String,
    scope: String,
    phase: String,
    next_page: String,
    rows_applied: String,
    mappings: Vec<MappingDto>,
    failure: Option<String>,
    receipt_hash: Option<String>,
    receipt_document: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkflowDto {
    workflow: String,
    checked_rows: String,
    quiescent_rows: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MappingDto {
    class: String,
    symbol: String,
    records: String,
    succeeded: String,
    replayed: String,
    outcome_hash: String,
}

impl CampaignDto {
    fn from_campaign(value: &ApplicationReimportCampaignV1) -> Self {
        Self {
            schema: APPLICATION_REIMPORT_CAMPAIGN_SCHEMA_V1.to_owned(),
            export_manifest_hash: hex32(value.source.export_manifest_hash.as_bytes()),
            export_receipt_hash: hex32(value.source.export_receipt_hash.as_bytes()),
            portability_manifest_hash: hex32(value.source.portability_manifest_hash.as_bytes()),
            portability_manifest_document: String::from_utf8(
                value.portability_manifest_document.clone(),
            )
            .expect("canonical portability manifest is JSON UTF-8"),
            source_database_id: value.source.source_database_id.to_string(),
            target_database_id: value.source.target_database_id.to_string(),
            rows: value.source.rows.to_string(),
            page_hashes: value
                .source
                .page_hashes
                .iter()
                .map(|hash| hex32(hash.as_bytes()))
                .collect(),
            workflow_quiescence: value
                .source
                .workflow_quiescence
                .iter()
                .map(|evidence| WorkflowDto {
                    workflow: evidence.workflow.as_str().to_owned(),
                    checked_rows: evidence.checked_rows.to_string(),
                    quiescent_rows: evidence.quiescent_rows.to_string(),
                })
                .collect(),
            capability_id: value.authority.capability_id().to_string(),
            capability_revision: value.authority.capability_revision().to_string(),
            scope: match value.scope {
                CapabilityApplicationReimportScopeV1::PrincipalFiltered => "principal_filtered",
                CapabilityApplicationReimportScopeV1::WholeApplication => "whole_application",
            }
            .to_owned(),
            phase: phase_name(value.phase).to_owned(),
            next_page: value.next_page.to_string(),
            rows_applied: value.rows_applied.to_string(),
            mappings: value
                .mappings
                .iter()
                .map(|mapping| MappingDto {
                    class: class_name(mapping.class).to_owned(),
                    symbol: mapping.symbol.as_str().to_owned(),
                    records: mapping.records.to_string(),
                    succeeded: mapping.succeeded.to_string(),
                    replayed: mapping.replayed.to_string(),
                    outcome_hash: hex32(mapping.outcome_hash.as_bytes()),
                })
                .collect(),
            failure: value.failure.map(failure_name).map(str::to_owned),
            receipt_hash: value.receipt_hash.map(|hash| hex32(hash.as_bytes())),
            receipt_document: value.receipt_document.as_ref().map(|document| {
                String::from_utf8(document.clone()).expect("canonical receipt is JSON UTF-8")
            }),
        }
    }

    fn into_campaign(
        self,
    ) -> Result<ApplicationReimportCampaignV1, ApplicationReimportCampaignError> {
        if self.schema != APPLICATION_REIMPORT_CAMPAIGN_SCHEMA_V1 {
            return Err(invalid());
        }
        let source = ApplicationReimportSourceV1::new(
            ApplicationExportManifestHash::from_bytes(parse_hex32(&self.export_manifest_hash)?),
            ApplicationExportReceiptHash::from_bytes(parse_hex32(&self.export_receipt_hash)?),
            ApplicationPortabilityManifestHash::from_bytes(parse_hex32(
                &self.portability_manifest_hash,
            )?),
            DatabaseId::from_bytes(parse_uuid(&self.source_database_id)?).map_err(|_| invalid())?,
            DatabaseId::from_bytes(parse_uuid(&self.target_database_id)?).map_err(|_| invalid())?,
            parse_u64(&self.rows)?,
            self.page_hashes
                .iter()
                .map(|value| parse_hex32(value).map(ApplicationExportPageHash::from_bytes))
                .collect::<Result<_, _>>()?,
            self.workflow_quiescence
                .into_iter()
                .map(|value| {
                    ReimportWorkflowQuiescenceV1::new(
                        InstallationSymbol::new(value.workflow).map_err(|_| invalid())?,
                        parse_u64(&value.checked_rows)?,
                        parse_u64(&value.quiescent_rows)?,
                        0,
                    )
                })
                .collect::<Result<_, _>>()?,
        )?;
        let capability_id =
            CapabilityId::from_bytes(parse_uuid(&self.capability_id)?).map_err(|_| invalid())?;
        let capability_revision =
            NonZeroU64::new(parse_u64(&self.capability_revision)?).ok_or_else(invalid)?;
        let scope = match self.scope.as_str() {
            "principal_filtered" => CapabilityApplicationReimportScopeV1::PrincipalFiltered,
            "whole_application" => CapabilityApplicationReimportScopeV1::WholeApplication,
            _ => return Err(invalid()),
        };
        let portability_manifest_document = self.portability_manifest_document.into_bytes();
        let phase = parse_phase(&self.phase)?;
        let mappings = self
            .mappings
            .into_iter()
            .map(|mapping| {
                Ok(ReimportMappingProgressV1 {
                    class: parse_class(&mapping.class)?,
                    symbol: InstallationSymbol::new(mapping.symbol).map_err(|_| invalid())?,
                    records: parse_u64(&mapping.records)?,
                    succeeded: parse_u64(&mapping.succeeded)?,
                    replayed: parse_u64(&mapping.replayed)?,
                    outcome_hash: GeneratedArtifactHash::from_bytes(parse_hex32(
                        &mapping.outcome_hash,
                    )?),
                })
            })
            .collect::<Result<Vec<_>, ApplicationReimportCampaignError>>()?;
        let failure = self.failure.as_deref().map(parse_failure).transpose()?;
        let receipt_hash = self
            .receipt_hash
            .as_deref()
            .map(parse_hex32)
            .transpose()?
            .map(ApplicationReimportReceiptHash::from_bytes);
        let receipt_document = self.receipt_document.map(String::into_bytes);
        Ok(ApplicationReimportCampaignV1 {
            source,
            portability_manifest_document,
            authority: ApplicationReimportAuthorityV1::new(capability_id, capability_revision),
            scope,
            phase,
            next_page: NonZeroU64::new(parse_u64(&self.next_page)?).ok_or_else(invalid)?,
            rows_applied: parse_u64(&self.rows_applied)?,
            mappings,
            failure,
            receipt_hash,
            receipt_document,
        })
    }
}

fn validate(value: &ApplicationReimportCampaignV1) -> Result<(), ApplicationReimportCampaignError> {
    let manifest =
        ApplicationPortabilityManifest::decode_canonical(&value.portability_manifest_document)
            .map_err(|_| invalid())?;
    if manifest.identity() != value.source.portability_manifest_hash
        || manifest.input().mappings.len() != value.mappings.len()
        || manifest
            .input()
            .mappings
            .iter()
            .zip(&value.mappings)
            .any(|(expected, actual)| {
                expected.class() != actual.class || expected.symbol() != &actual.symbol
            })
        || value.mappings.is_empty()
        || value.mappings.len() > crate::MAX_APPLICATION_PORTABLE_MAPPINGS
        || value
            .mappings
            .windows(2)
            .any(|pair| (pair[0].class, &pair[0].symbol) >= (pair[1].class, &pair[1].symbol))
        || value
            .mappings
            .iter()
            .any(|mapping| mapping.succeeded.checked_add(mapping.replayed) != Some(mapping.records))
    {
        return Err(ApplicationReimportCampaignError::new(
            ApplicationReimportCampaignErrorKind::InvalidProgress,
        ));
    }
    let completed_pages = match value.phase {
        ApplicationReimportCampaignPhaseV1::Applying => value.next_page.get() - 1,
        ApplicationReimportCampaignPhaseV1::Reconciling
        | ApplicationReimportCampaignPhaseV1::Reconciled => {
            u64::try_from(value.source.page_hashes.len()).map_err(|_| limit())?
        }
        ApplicationReimportCampaignPhaseV1::Cancelled
        | ApplicationReimportCampaignPhaseV1::Failed => value.next_page.get() - 1,
    };
    if completed_pages > u64::try_from(value.source.page_hashes.len()).map_err(|_| limit())?
        || value.rows_applied
            != value
                .mappings
                .iter()
                .try_fold(0_u64, |sum, mapping| sum.checked_add(mapping.records))
                .ok_or_else(limit)?
    {
        return Err(ApplicationReimportCampaignError::new(
            ApplicationReimportCampaignErrorKind::InvalidProgress,
        ));
    }
    let terminal_failure = matches!(
        value.phase,
        ApplicationReimportCampaignPhaseV1::Cancelled | ApplicationReimportCampaignPhaseV1::Failed
    );
    let reconciled = value.phase == ApplicationReimportCampaignPhaseV1::Reconciled;
    if terminal_failure != value.failure.is_some()
        || reconciled != value.receipt_hash.is_some()
        || reconciled != value.receipt_document.is_some()
        || value
            .receipt_document
            .as_ref()
            .zip(value.receipt_hash)
            .is_some_and(|(document, expected)| {
                hash_application_reimport_receipt(document) != expected
            })
    {
        return Err(ApplicationReimportCampaignError::new(
            ApplicationReimportCampaignErrorKind::InvalidProgress,
        ));
    }
    Ok(())
}

fn canonical_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, ApplicationReimportCampaignError> {
    let mut bytes = serde_json::to_vec(value).map_err(|_| invalid())?;
    bytes.push(b'\n');
    if bytes.len() > MAX_APPLICATION_REIMPORT_CAMPAIGN_BYTES {
        return Err(limit());
    }
    Ok(bytes)
}

fn phase_name(value: ApplicationReimportCampaignPhaseV1) -> &'static str {
    match value {
        ApplicationReimportCampaignPhaseV1::Applying => "applying",
        ApplicationReimportCampaignPhaseV1::Reconciling => "reconciling",
        ApplicationReimportCampaignPhaseV1::Reconciled => "reconciled",
        ApplicationReimportCampaignPhaseV1::Cancelled => "cancelled",
        ApplicationReimportCampaignPhaseV1::Failed => "failed",
    }
}

fn parse_phase(
    value: &str,
) -> Result<ApplicationReimportCampaignPhaseV1, ApplicationReimportCampaignError> {
    match value {
        "applying" => Ok(ApplicationReimportCampaignPhaseV1::Applying),
        "reconciling" => Ok(ApplicationReimportCampaignPhaseV1::Reconciling),
        "reconciled" => Ok(ApplicationReimportCampaignPhaseV1::Reconciled),
        "cancelled" => Ok(ApplicationReimportCampaignPhaseV1::Cancelled),
        "failed" => Ok(ApplicationReimportCampaignPhaseV1::Failed),
        _ => Err(invalid()),
    }
}

fn failure_name(value: ApplicationReimportFailureV1) -> &'static str {
    match value {
        ApplicationReimportFailureV1::AuthorityChanged => "authority_changed",
        ApplicationReimportFailureV1::SourceMismatch => "source_mismatch",
        ApplicationReimportFailureV1::CommandFailed => "command_failed",
        ApplicationReimportFailureV1::ObservationMismatch => "observation_mismatch",
        ApplicationReimportFailureV1::Cancelled => "cancelled",
    }
}

fn parse_failure(
    value: &str,
) -> Result<ApplicationReimportFailureV1, ApplicationReimportCampaignError> {
    match value {
        "authority_changed" => Ok(ApplicationReimportFailureV1::AuthorityChanged),
        "source_mismatch" => Ok(ApplicationReimportFailureV1::SourceMismatch),
        "command_failed" => Ok(ApplicationReimportFailureV1::CommandFailed),
        "observation_mismatch" => Ok(ApplicationReimportFailureV1::ObservationMismatch),
        "cancelled" => Ok(ApplicationReimportFailureV1::Cancelled),
        _ => Err(invalid()),
    }
}

fn class_name(value: PortableRecordClass) -> &'static str {
    match value {
        PortableRecordClass::Entity => "entity",
        PortableRecordClass::Event => "event",
    }
}

fn parse_class(value: &str) -> Result<PortableRecordClass, ApplicationReimportCampaignError> {
    match value {
        "entity" => Ok(PortableRecordClass::Entity),
        "event" => Ok(PortableRecordClass::Event),
        _ => Err(invalid()),
    }
}

fn parse_u64(value: &str) -> Result<u64, ApplicationReimportCampaignError> {
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(invalid());
    }
    value.parse().map_err(|_| invalid())
}

fn parse_uuid(value: &str) -> Result<[u8; 16], ApplicationReimportCampaignError> {
    if value.len() != 36 {
        return Err(invalid());
    }
    let mut compact = [0_u8; 32];
    let mut output = 0_usize;
    for (index, byte) in value.bytes().enumerate() {
        if matches!(index, 8 | 13 | 18 | 23) {
            if byte != b'-' {
                return Err(invalid());
            }
        } else {
            if output == compact.len() {
                return Err(invalid());
            }
            compact[output] = byte;
            output += 1;
        }
    }
    if output != compact.len() {
        return Err(invalid());
    }
    let mut bytes = [0_u8; 16];
    for (index, pair) in compact.chunks_exact(2).enumerate() {
        bytes[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Ok(bytes)
}

fn hex32(value: &[u8; 32]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn parse_hex32(value: &str) -> Result<[u8; 32], ApplicationReimportCampaignError> {
    if value.len() != 64 {
        return Err(invalid());
    }
    let mut bytes = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Ok(bytes)
}

fn hex_nibble(value: u8) -> Result<u8, ApplicationReimportCampaignError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(invalid()),
    }
}

const fn invalid() -> ApplicationReimportCampaignError {
    ApplicationReimportCampaignError::new(ApplicationReimportCampaignErrorKind::InvalidEncoding)
}

const fn limit() -> ApplicationReimportCampaignError {
    ApplicationReimportCampaignError::new(ApplicationReimportCampaignErrorKind::LimitExceeded)
}

const fn integrity() -> ApplicationReimportCampaignError {
    ApplicationReimportCampaignError::new(ApplicationReimportCampaignErrorKind::InvalidProgress)
}
