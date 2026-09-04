use std::fmt;

use riffdb_types::{
    ApplicationExportManifestHash, ApplicationExportReceiptHash, ApplicationInstallationCampaignId,
    ApplicationInstallationPlanHash, ApplicationInstallationReceiptHash, ApplicationLockHash,
    ApplicationManifestHash, ApplicationPortabilityManifestHash, ApplicationReimportReceiptHash,
    ApplicationRoleHash, ApplicationSourceHash, CapabilityId, ContractBundleHash,
    ContractMigrationOperationId, ContractVersion, DatabaseId, GeneratedArtifactHash,
    MigrationBundleHash, hash_application_installation_receipt,
};
use serde::{Deserialize, Serialize};

pub use riffdb_types::MAX_APPLICATION_INSTALLATION_CAMPAIGN_STATE_BYTES;

use crate::{
    ApplicationInstallationPlan, ApplicationReimportCampaignV1, ApplicationReimportSourceV1,
    InstallationArtifact, InstallationArtifactKind, InstallationCampaignError,
    InstallationCampaignErrorKind, InstallationDriver, InstallationSymbol, InstalledSeedEvidence,
    parse_hex16, parse_hex32,
};

/// Canonical schema version for terminal installation receipts.
pub const APPLICATION_INSTALLATION_RECEIPT_SCHEMA_V1: &str =
    "riffdb.application-installation-receipt/v1";
/// Canonical schema version for complete terminal installation receipts.
pub const APPLICATION_INSTALLATION_RECEIPT_SCHEMA_V2: &str =
    "riffdb.application-installation-receipt/v2";
/// Canonical terminal receipt carrying explicit reimport reconciliation identities.
pub const APPLICATION_INSTALLATION_RECEIPT_SCHEMA_V3: &str =
    "riffdb.application-installation-receipt/v3";
/// Latest supported terminal installation-receipt schema.
pub const APPLICATION_INSTALLATION_RECEIPT_SCHEMA_CURRENT: &str =
    APPLICATION_INSTALLATION_RECEIPT_SCHEMA_V3;
/// Canonical schema version for durable resumable campaign state.
pub const APPLICATION_INSTALLATION_CAMPAIGN_STATE_SCHEMA_V1: &str =
    "riffdb.application-installation-campaign-state/v1";
/// Canonical durable campaign state containing the reimport publication gate.
pub const APPLICATION_INSTALLATION_CAMPAIGN_STATE_SCHEMA_V2: &str =
    "riffdb.application-installation-campaign-state/v2";
/// Canonical durable campaign state carrying resumable reimport progress.
pub const APPLICATION_INSTALLATION_CAMPAIGN_STATE_SCHEMA_V3: &str =
    "riffdb.application-installation-campaign-state/v3";
/// Maximum canonical bytes in one redacted terminal receipt.
pub const MAX_INSTALLATION_RECEIPT_BYTES: usize = 2 * 1_024 * 1_024;
/// Compatibility alias for the shared exact campaign-state storage boundary.
pub const MAX_INSTALLATION_CAMPAIGN_STATE_BYTES: usize =
    MAX_APPLICATION_INSTALLATION_CAMPAIGN_STATE_BYTES;

/// Closed, dependency-ordered stages in one application installation campaign.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum InstallationStage {
    /// Validate exact local inputs without remote mutation.
    Preflight,
    /// Deploy or verify the exact contract successor.
    Contract,
    /// Run or verify the explicitly approved migration, when present.
    Migration,
    /// Deploy or verify exact named-query modules.
    QueryModules,
    /// Deploy or verify exact reactive modules.
    ReactiveModules,
    /// Provision or reconcile exact application roles.
    Roles,
    /// Reconstitute exact portable state, or record that reimport is not required.
    Reimport,
    /// Rotate or verify application credentials without exposing their bytes.
    Credentials,
    /// Prove every required public driver against the installed identity.
    DriverProof,
    /// Run bounded command-based seed batches and retain only their receipts.
    Seeds,
    /// Seal the terminal redacted installation receipt.
    Receipt,
}

impl InstallationStage {
    /// Every stage in the only valid execution order.
    pub const ALL: [Self; 11] = [
        Self::Preflight,
        Self::Contract,
        Self::Migration,
        Self::QueryModules,
        Self::ReactiveModules,
        Self::Roles,
        Self::Reimport,
        Self::Credentials,
        Self::DriverProof,
        Self::Seeds,
        Self::Receipt,
    ];

    /// Stable public v1 stage label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Preflight => "preflight",
            Self::Contract => "contract",
            Self::Migration => "migration",
            Self::QueryModules => "query_modules",
            Self::ReactiveModules => "reactive_modules",
            Self::Roles => "roles",
            Self::Reimport => "reimport",
            Self::Credentials => "credentials",
            Self::DriverProof => "driver_proof",
            Self::Seeds => "seeds",
            Self::Receipt => "receipt",
        }
    }
}

/// Exact immutable identity attached to every campaign observation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct InstallationPlanIdentity(ApplicationInstallationPlanHash);

impl InstallationPlanIdentity {
    /// Wraps the compiler-derived plan hash.
    #[must_use]
    pub const fn new(hash: ApplicationInstallationPlanHash) -> Self {
        Self(hash)
    }

    /// Returns the exact compiler-derived plan hash.
    #[must_use]
    pub const fn hash(self) -> ApplicationInstallationPlanHash {
        self.0
    }
}

/// Exact observed role identity after role reconciliation.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct InstalledRoleEvidence {
    name: InstallationSymbol,
    role_hash: ApplicationRoleHash,
}

impl InstalledRoleEvidence {
    /// Creates exact role evidence.
    #[must_use]
    pub const fn new(name: InstallationSymbol, role_hash: ApplicationRoleHash) -> Self {
        Self { name, role_hash }
    }

    /// Symbolic role name.
    #[must_use]
    pub const fn name(&self) -> &InstallationSymbol {
        &self.name
    }

    /// Exact installed role identity.
    #[must_use]
    pub const fn role_hash(&self) -> ApplicationRoleHash {
        self.role_hash
    }
}

/// Exact completion evidence for the publication-gating reimport stage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstalledReimportEvidence {
    /// This ordinary installation has no portable source state.
    NotRequired,
    /// Exact source, mapping, and terminal reconciliation identities.
    Reconciled(InstalledReimportReceiptEvidence),
}

/// Verified terminal identities for a completed reimport stage.
///
/// Fields are deliberately private: only semantic reconciliation against the
/// exact plan, portability manifest, receipt, and target database can create
/// this value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InstalledReimportReceiptEvidence {
    export_manifest_hash: ApplicationExportManifestHash,
    export_receipt_hash: ApplicationExportReceiptHash,
    portability_manifest_hash: ApplicationPortabilityManifestHash,
    reimport_receipt_hash: ApplicationReimportReceiptHash,
}

/// Exact observed credential slot without credential material.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct InstalledCredentialEvidence {
    destination: InstallationSymbol,
    capability_id: CapabilityId,
}

impl InstalledCredentialEvidence {
    /// Creates one slot identity observation.
    #[must_use]
    pub const fn new(destination: InstallationSymbol, capability_id: CapabilityId) -> Self {
        Self {
            destination,
            capability_id,
        }
    }

    /// Symbolic destination name, never a host path.
    #[must_use]
    pub const fn destination(&self) -> &InstallationSymbol {
        &self.destination
    }

    /// Exact installed capability identity, never token bytes.
    #[must_use]
    pub const fn capability_id(&self) -> CapabilityId {
        self.capability_id
    }
}

/// Closed exact evidence for one idempotent campaign stage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InstallationStageEvidence {
    /// Exact local source/lock/manifest validation.
    Preflight {
        /// Author source identity.
        source_hash: ApplicationSourceHash,
        /// Compiler-owned lock identity.
        lock_hash: ApplicationLockHash,
        /// Canonical manifest identity.
        manifest_hash: ApplicationManifestHash,
    },
    /// Exact active contract identity.
    Contract {
        /// Active version.
        version: ContractVersion,
        /// Active bundle hash.
        bundle_hash: ContractBundleHash,
    },
    /// Exact migration completion, or explicit not-required evidence.
    Migration {
        /// Compiled migration identity when the plan requires one.
        migration_hash: Option<MigrationBundleHash>,
    },
    /// Exact deployed named-query artifacts.
    QueryModules(Vec<InstallationArtifact>),
    /// Exact deployed reactive artifacts.
    ReactiveModules(Vec<InstallationArtifact>),
    /// Exact reconciled symbolic roles.
    Roles(Vec<InstalledRoleEvidence>),
    /// Exact reimport terminal evidence, or explicit not-required evidence.
    Reimport(InstalledReimportEvidence),
    /// Exact installed credential identities without token bytes.
    Credentials(Vec<InstalledCredentialEvidence>),
    /// Exact public drivers that proved the installed identity.
    DriverProof(Vec<InstallationDriver>),
    /// Complete per-batch seed counters with no item values.
    Seeds(Vec<InstalledSeedEvidence>),
    /// Terminal receipt identity sealed by the campaign state machine.
    Receipt(ApplicationInstallationReceiptHash),
}

impl InstallationStageEvidence {
    /// Stage represented by this evidence.
    #[must_use]
    pub const fn stage(&self) -> InstallationStage {
        match self {
            Self::Preflight { .. } => InstallationStage::Preflight,
            Self::Contract { .. } => InstallationStage::Contract,
            Self::Migration { .. } => InstallationStage::Migration,
            Self::QueryModules(_) => InstallationStage::QueryModules,
            Self::ReactiveModules(_) => InstallationStage::ReactiveModules,
            Self::Roles(_) => InstallationStage::Roles,
            Self::Reimport(_) => InstallationStage::Reimport,
            Self::Credentials(_) => InstallationStage::Credentials,
            Self::DriverProof(_) => InstallationStage::DriverProof,
            Self::Seeds(_) => InstallationStage::Seeds,
            Self::Receipt(_) => InstallationStage::Receipt,
        }
    }

    /// Validates that this evidence is the exact value declared by a plan.
    ///
    /// This validates identity and bounded shape only. Remote contract,
    /// module, role, and capability evidence is still observed by the server;
    /// callers cannot use this method to authorize or assert those stages.
    pub fn validate_for(
        &self,
        plan: &ApplicationInstallationPlan,
    ) -> Result<(), InstallationCampaignError> {
        validate_stage_evidence(plan, self)
    }
}

/// Closed safe failure observed while executing the current stage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstallationFailureCode {
    /// Local source, lock, manifest, or generated artifact drift.
    LocalArtifactMismatch,
    /// Remote contract, module, role, or capability identity drift.
    RemoteIdentityMismatch,
    /// Required migration approval or exact identity is absent.
    MigrationGateRequired,
    /// Explicit authority widening approval is absent or stale.
    RoleWideningApprovalRequired,
    /// Reimport is incomplete, cancelled, or failed reconciliation.
    ReimportPartial,
    /// Credential destination is occupied by an unexpected identity.
    CredentialDestinationOccupied,
    /// One public driver could not prove the exact installed identity.
    DriverProofFailed,
    /// At least one seed item remains incomplete or failed.
    SeedPartial,
    /// Installer authority is absent or expired.
    AuthorizationDenied,
    /// The selected database is temporarily unavailable.
    ServiceUnavailable,
}

/// Exact bounded next action for a partial campaign.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstallationNextAction {
    /// Validate the exact local source, lock, manifest, and artifacts.
    ValidateLocalArtifacts,
    /// Deploy or verify the exact contract.
    DeployContract,
    /// Apply or observe the exact approved migration.
    ApplyMigration,
    /// Deploy or verify named-query modules.
    DeployQueryModules,
    /// Deploy or verify reactive modules.
    DeployReactiveModules,
    /// Reconcile exact application roles after reviewing any widening.
    ReconcileRoles,
    /// Resume exact compiler-owned reimport and terminal reconciliation.
    ReimportApplication,
    /// Rotate credentials only from the expected predecessor identities.
    RotateCredentials,
    /// Run exact public-driver identity proofs.
    ProveDrivers,
    /// Resume ordinary-command seed batches from their exact checkpoints.
    RunSeeds,
    /// Seal the redacted terminal receipt.
    SealReceipt,
    /// Campaign is terminal and requires no next action.
    None,
}

impl InstallationNextAction {
    /// Derives the one exact action for a stage.
    #[must_use]
    pub const fn for_stage(stage: InstallationStage) -> Self {
        match stage {
            InstallationStage::Preflight => Self::ValidateLocalArtifacts,
            InstallationStage::Contract => Self::DeployContract,
            InstallationStage::Migration => Self::ApplyMigration,
            InstallationStage::QueryModules => Self::DeployQueryModules,
            InstallationStage::ReactiveModules => Self::DeployReactiveModules,
            InstallationStage::Roles => Self::ReconcileRoles,
            InstallationStage::Reimport => Self::ReimportApplication,
            InstallationStage::Credentials => Self::RotateCredentials,
            InstallationStage::DriverProof => Self::ProveDrivers,
            InstallationStage::Seeds => Self::RunSeeds,
            InstallationStage::Receipt => Self::SealReceipt,
        }
    }
}

/// One bounded typed partial-stage failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InstallationFailure {
    stage: InstallationStage,
    code: InstallationFailureCode,
    next_action: InstallationNextAction,
}

impl InstallationFailure {
    /// Current incomplete stage.
    #[must_use]
    pub const fn stage(self) -> InstallationStage {
        self.stage
    }

    /// Stable safe failure code.
    #[must_use]
    pub const fn code(self) -> InstallationFailureCode {
        self.code
    }

    /// Exact recovery action.
    #[must_use]
    pub const fn next_action(self) -> InstallationNextAction {
        self.next_action
    }
}

/// Public campaign phase; partial can never be confused with installed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstallationCampaignPhase {
    /// Campaign is ready to execute its next stage.
    Running,
    /// Campaign stopped with a typed failure and retained completed stages.
    Partial,
    /// Every stage, including receipt sealing, completed exactly.
    Installed,
}

/// Immutable observation of one durable installation campaign.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationInstallationObservation {
    campaign_id: ApplicationInstallationCampaignId,
    plan_hash: ApplicationInstallationPlanHash,
    phase: InstallationCampaignPhase,
    completed: Vec<InstallationStage>,
    next_stage: Option<InstallationStage>,
    next_action: InstallationNextAction,
    failure: Option<InstallationFailure>,
    receipt_hash: Option<ApplicationInstallationReceiptHash>,
}

impl ApplicationInstallationObservation {
    /// Caller-stable campaign identity.
    #[must_use]
    pub const fn campaign_id(&self) -> ApplicationInstallationCampaignId {
        self.campaign_id
    }

    /// Exact immutable plan identity.
    #[must_use]
    pub const fn plan_hash(&self) -> ApplicationInstallationPlanHash {
        self.plan_hash
    }

    /// Current closed phase.
    #[must_use]
    pub const fn phase(&self) -> InstallationCampaignPhase {
        self.phase
    }

    /// Completed stages in dependency order.
    #[must_use]
    pub fn completed(&self) -> &[InstallationStage] {
        &self.completed
    }

    /// Current incomplete stage, absent only after receipt sealing.
    #[must_use]
    pub const fn next_stage(&self) -> Option<InstallationStage> {
        self.next_stage
    }

    /// Exact operator recovery action.
    #[must_use]
    pub const fn next_action(&self) -> InstallationNextAction {
        self.next_action
    }

    /// Typed partial failure, present only in `Partial`.
    #[must_use]
    pub const fn failure(&self) -> Option<InstallationFailure> {
        self.failure
    }

    /// Terminal receipt identity, present only in `Installed`.
    #[must_use]
    pub const fn receipt_hash(&self) -> Option<ApplicationInstallationReceiptHash> {
        self.receipt_hash
    }
}

/// API-neutral deterministic state machine persisted by the shared service.
#[derive(Clone, Eq, PartialEq)]
pub struct ApplicationInstallationCampaign {
    campaign_id: ApplicationInstallationCampaignId,
    plan_hash: ApplicationInstallationPlanHash,
    completed: Vec<InstallationStageEvidence>,
    failure: Option<InstallationFailure>,
    reimport: Option<ApplicationReimportCampaignV1>,
}

impl ApplicationInstallationCampaign {
    /// Starts an empty campaign under one caller-stable identity.
    #[must_use]
    pub const fn start(
        campaign_id: ApplicationInstallationCampaignId,
        plan_hash: ApplicationInstallationPlanHash,
    ) -> Self {
        Self {
            campaign_id,
            plan_hash,
            completed: Vec::new(),
            failure: None,
            reimport: None,
        }
    }

    /// Verifies exact identity and every completed stage before resuming.
    pub fn resume(
        &mut self,
        campaign_id: ApplicationInstallationCampaignId,
        plan: &ApplicationInstallationPlan,
    ) -> Result<ApplicationInstallationObservation, InstallationCampaignError> {
        self.verify_identity(campaign_id, plan)?;
        for evidence in &self.completed {
            validate_stage_evidence(plan, evidence)?;
        }
        self.failure = None;
        Ok(self.observe())
    }

    /// Records one exact idempotent stage completion in dependency order.
    pub fn complete_stage(
        &mut self,
        plan: &ApplicationInstallationPlan,
        evidence: InstallationStageEvidence,
    ) -> Result<ApplicationInstallationObservation, InstallationCampaignError> {
        self.verify_plan(plan)?;
        let stage_index = InstallationStage::ALL
            .iter()
            .position(|stage| *stage == evidence.stage())
            .expect("closed installation stage registry");
        if let Some(completed) = self.completed.get(stage_index) {
            if completed == &evidence {
                return Ok(self.observe());
            }
            return Err(InstallationCampaignError::new(
                InstallationCampaignErrorKind::EvidenceMismatch,
            ));
        }
        if self.is_installed() {
            return Err(InstallationCampaignError::new(
                InstallationCampaignErrorKind::AlreadyTerminal,
            ));
        }
        let expected = self.next_stage().ok_or_else(|| {
            InstallationCampaignError::new(InstallationCampaignErrorKind::AlreadyTerminal)
        })?;
        if evidence.stage() != expected || expected == InstallationStage::Receipt {
            return Err(InstallationCampaignError::new(
                InstallationCampaignErrorKind::StageOutOfOrder,
            ));
        }
        if expected == InstallationStage::Reimport && plan.input().reimport.is_some() {
            return Err(InstallationCampaignError::new(
                InstallationCampaignErrorKind::StageOutOfOrder,
            ));
        }
        validate_stage_evidence(plan, &evidence)?;
        self.completed.push(evidence);
        self.failure = None;
        Ok(self.observe())
    }

    /// Starts the exact nested reimport checkpoint at the sole reimport stage.
    pub fn start_reimport(
        &mut self,
        plan: &ApplicationInstallationPlan,
        source: ApplicationReimportSourceV1,
        authority: riffdb_types::ApplicationReimportAuthorityV1,
        scope: riffdb_types::CapabilityApplicationReimportScopeV1,
        portability_manifest: &crate::ApplicationPortabilityManifest,
    ) -> Result<&ApplicationReimportCampaignV1, InstallationCampaignError> {
        self.verify_plan(plan)?;
        if self.next_stage() != Some(InstallationStage::Reimport) || self.reimport.is_some() {
            return Err(InstallationCampaignError::new(
                InstallationCampaignErrorKind::StageOutOfOrder,
            ));
        }
        let expected = plan.input().reimport.ok_or_else(evidence_mismatch)?;
        if source.export_manifest_hash() != expected.export_manifest_hash()
            || source.export_receipt_hash() != expected.export_receipt_hash()
            || source.portability_manifest_hash() != expected.portability_manifest_hash()
            || portability_manifest.identity() != expected.portability_manifest_hash()
        {
            return Err(evidence_mismatch());
        }
        self.reimport = Some(
            ApplicationReimportCampaignV1::start(source, authority, scope, portability_manifest)
                .map_err(|_| evidence_mismatch())?,
        );
        self.failure = None;
        Ok(self.reimport.as_ref().expect("nested reimport just set"))
    }

    /// Current durable reimport progress, absent before a portability campaign starts.
    #[must_use]
    pub const fn reimport(&self) -> Option<&ApplicationReimportCampaignV1> {
        self.reimport.as_ref()
    }

    /// Mutable reimport progress for the API-neutral service coordinator.
    pub fn reimport_mut(&mut self) -> Option<&mut ApplicationReimportCampaignV1> {
        self.reimport.as_mut()
    }

    /// Verifies and records terminal reimport reconciliation for this exact plan.
    pub fn complete_reimport(
        &mut self,
        plan: &ApplicationInstallationPlan,
        portability_manifest: &crate::ApplicationPortabilityManifest,
        reimport_receipt: &crate::ApplicationReimportReceipt,
        target_database_id: DatabaseId,
    ) -> Result<ApplicationInstallationObservation, InstallationCampaignError> {
        self.verify_plan(plan)?;
        if self.next_stage() != Some(InstallationStage::Reimport) {
            return Err(InstallationCampaignError::new(
                InstallationCampaignErrorKind::StageOutOfOrder,
            ));
        }
        let expected = plan.input().reimport.ok_or_else(evidence_mismatch)?;
        let receipt = reimport_receipt.input();
        let progress = self.reimport.as_ref().ok_or_else(evidence_mismatch)?;
        if portability_manifest.identity() != expected.portability_manifest_hash()
            || portability_manifest.input().contract_lineage != *plan.input().target.lineage()
            || portability_manifest.input().contract_version != plan.input().contract.version()
            || portability_manifest.input().contract_bundle_hash
                != plan.input().contract.bundle_hash()
            || receipt.portability_manifest_hash != expected.portability_manifest_hash()
            || receipt.export_manifest_hash != expected.export_manifest_hash()
            || receipt.target_database_id != target_database_id
            || progress.phase() != crate::ApplicationReimportCampaignPhaseV1::Reconciled
            || progress.receipt_hash() != Some(reimport_receipt.identity())
        {
            return Err(evidence_mismatch());
        }
        self.completed.push(InstallationStageEvidence::Reimport(
            InstalledReimportEvidence::Reconciled(InstalledReimportReceiptEvidence {
                export_manifest_hash: expected.export_manifest_hash(),
                export_receipt_hash: expected.export_receipt_hash(),
                portability_manifest_hash: expected.portability_manifest_hash(),
                reimport_receipt_hash: reimport_receipt.identity(),
            }),
        ));
        self.failure = None;
        Ok(self.observe())
    }

    /// Stops the current stage with a typed partial result and exact next action.
    pub fn record_failure(
        &mut self,
        plan: &ApplicationInstallationPlan,
        code: InstallationFailureCode,
    ) -> Result<ApplicationInstallationObservation, InstallationCampaignError> {
        self.verify_plan(plan)?;
        let stage = self.next_stage().ok_or_else(|| {
            InstallationCampaignError::new(InstallationCampaignErrorKind::AlreadyTerminal)
        })?;
        self.failure = Some(InstallationFailure {
            stage,
            code,
            next_action: InstallationNextAction::for_stage(stage),
        });
        Ok(self.observe())
    }

    /// Seals the terminal redacted receipt; this is the only success transition.
    pub fn seal_receipt(
        &mut self,
        plan: &ApplicationInstallationPlan,
    ) -> Result<ApplicationInstallationReceipt, InstallationCampaignError> {
        self.verify_plan(plan)?;
        if let Some(InstallationStageEvidence::Receipt(retained_hash)) = self.completed.last() {
            return ApplicationInstallationReceipt::seal_matching(self, plan, *retained_hash);
        }
        if self.next_stage() != Some(InstallationStage::Receipt) {
            return Err(InstallationCampaignError::new(
                InstallationCampaignErrorKind::StageOutOfOrder,
            ));
        }
        let receipt = ApplicationInstallationReceipt::seal(self, plan)?;
        self.completed
            .push(InstallationStageEvidence::Receipt(receipt.identity()));
        self.failure = None;
        Ok(receipt)
    }

    /// Current redaction-safe observation.
    #[must_use]
    pub fn observe(&self) -> ApplicationInstallationObservation {
        let next_stage = self.next_stage();
        let receipt_hash = self.completed.last().and_then(|evidence| match evidence {
            InstallationStageEvidence::Receipt(hash) => Some(*hash),
            _ => None,
        });
        let phase = if receipt_hash.is_some() {
            InstallationCampaignPhase::Installed
        } else if self.failure.is_some() {
            InstallationCampaignPhase::Partial
        } else {
            InstallationCampaignPhase::Running
        };
        ApplicationInstallationObservation {
            campaign_id: self.campaign_id,
            plan_hash: self.plan_hash,
            phase,
            completed: self
                .completed
                .iter()
                .map(InstallationStageEvidence::stage)
                .collect(),
            next_stage,
            next_action: next_stage.map_or(InstallationNextAction::None, |stage| {
                self.failure.map_or_else(
                    || InstallationNextAction::for_stage(stage),
                    InstallationFailure::next_action,
                )
            }),
            failure: self.failure,
            receipt_hash,
        }
    }

    /// Completed exact evidence retained for resume verification.
    #[must_use]
    pub fn completed_evidence(&self) -> &[InstallationStageEvidence] {
        &self.completed
    }

    /// Whether and only whether a terminal receipt has been sealed.
    #[must_use]
    pub fn is_installed(&self) -> bool {
        matches!(
            self.completed.last(),
            Some(InstallationStageEvidence::Receipt(_))
        )
    }

    fn verify_identity(
        &self,
        campaign_id: ApplicationInstallationCampaignId,
        plan: &ApplicationInstallationPlan,
    ) -> Result<(), InstallationCampaignError> {
        if self.campaign_id != campaign_id {
            return Err(InstallationCampaignError::new(
                InstallationCampaignErrorKind::CampaignIdentityMismatch,
            ));
        }
        self.verify_plan(plan)
    }

    fn verify_plan(
        &self,
        plan: &ApplicationInstallationPlan,
    ) -> Result<(), InstallationCampaignError> {
        if self.plan_hash != plan.identity() {
            return Err(InstallationCampaignError::new(
                InstallationCampaignErrorKind::PlanIdentityMismatch,
            ));
        }
        Ok(())
    }

    fn next_stage(&self) -> Option<InstallationStage> {
        InstallationStage::ALL.get(self.completed.len()).copied()
    }
}

impl fmt::Debug for ApplicationInstallationCampaign {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApplicationInstallationCampaign")
            .field("campaign_id", &self.campaign_id)
            .field("plan_hash", &self.plan_hash)
            .field("completed_stages", &self.completed.len())
            .field("failure", &self.failure)
            .finish_non_exhaustive()
    }
}

/// One canonical durable plan-plus-campaign record.
///
/// The complete exact plan is retained with stage evidence so restart recovery
/// never infers intent from whichever application files happen to be present.
/// This value contains no bearer credentials, seed values, or host paths.
#[derive(Clone, Eq, PartialEq)]
pub struct ApplicationInstallationCampaignState {
    plan: ApplicationInstallationPlan,
    campaign: ApplicationInstallationCampaign,
    canonical_bytes: Vec<u8>,
}

impl ApplicationInstallationCampaignState {
    /// Captures one fully validated canonical durable state record.
    pub fn capture(
        campaign: &ApplicationInstallationCampaign,
        plan: &ApplicationInstallationPlan,
    ) -> Result<Self, InstallationCampaignError> {
        validate_campaign_state(campaign, plan)?;
        let dto = CampaignStateDto::from_parts(campaign, plan);
        let canonical_bytes = encode_campaign_state_dto(&dto)?;
        Ok(Self {
            plan: plan.clone(),
            campaign: campaign.clone(),
            canonical_bytes,
        })
    }

    /// Strictly decodes, canonicalizes, and semantically revalidates durable state.
    pub fn decode_canonical(bytes: &[u8]) -> Result<Self, InstallationCampaignError> {
        if bytes.is_empty() || bytes.len() > MAX_INSTALLATION_CAMPAIGN_STATE_BYTES {
            return Err(InstallationCampaignError::new(
                InstallationCampaignErrorKind::LimitExceeded,
            ));
        }
        let dto: CampaignStateDto = serde_json::from_slice(bytes).map_err(|_| {
            InstallationCampaignError::new(InstallationCampaignErrorKind::InvalidEncoding)
        })?;
        let schema = CampaignStateSchema::parse(&dto.schema)?;
        let plan = ApplicationInstallationPlan::decode_canonical(dto.plan.as_bytes())
            .map_err(InstallationCampaignError::from_plan_error)?;
        let campaign = dto.to_campaign(&plan, schema)?;
        validate_campaign_state(&campaign, &plan)?;
        let canonical_bytes = encode_campaign_state_dto(&dto)?;
        if canonical_bytes != bytes {
            return Err(InstallationCampaignError::new(
                InstallationCampaignErrorKind::NonCanonical,
            ));
        }
        Ok(Self {
            plan,
            campaign,
            canonical_bytes,
        })
    }

    /// Exact immutable plan recovered with the campaign.
    #[must_use]
    pub const fn plan(&self) -> &ApplicationInstallationPlan {
        &self.plan
    }

    /// Exact resumable campaign state.
    #[must_use]
    pub const fn campaign(&self) -> &ApplicationInstallationCampaign {
        &self.campaign
    }

    /// Canonical durable bytes.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    /// Consumes the record into its exact plan and campaign state.
    #[must_use]
    pub fn into_parts(self) -> (ApplicationInstallationPlan, ApplicationInstallationCampaign) {
        (self.plan, self.campaign)
    }
}

impl fmt::Debug for ApplicationInstallationCampaignState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApplicationInstallationCampaignState")
            .field("plan_hash", &self.plan.identity())
            .field("campaign", &self.campaign)
            .finish_non_exhaustive()
    }
}

/// Terminal, redacted, content-addressed installation receipt.
#[derive(Clone, Eq, PartialEq)]
pub struct ApplicationInstallationReceipt {
    identity: ApplicationInstallationReceiptHash,
    canonical_bytes: Vec<u8>,
    campaign_id: ApplicationInstallationCampaignId,
    plan_hash: ApplicationInstallationPlanHash,
}

impl ApplicationInstallationReceipt {
    fn seal(
        campaign: &ApplicationInstallationCampaign,
        plan: &ApplicationInstallationPlan,
    ) -> Result<Self, InstallationCampaignError> {
        let dto = ReceiptDtoV2::from_campaign(campaign, plan)?;
        Self::from_v2_dto(dto)
    }

    fn seal_v1(
        campaign: &ApplicationInstallationCampaign,
        plan: &ApplicationInstallationPlan,
    ) -> Result<Self, InstallationCampaignError> {
        let input = plan.input();
        let seed_items = input.seeds.iter().try_fold(0_u64, |sum, seed| {
            sum.checked_add(seed.item_count()).ok_or_else(|| {
                InstallationCampaignError::new(InstallationCampaignErrorKind::EvidenceMismatch)
            })
        })?;
        let dto = ReceiptDtoV1 {
            schema: APPLICATION_INSTALLATION_RECEIPT_SCHEMA_V1.to_owned(),
            campaign_id: hex16(campaign.campaign_id.as_bytes()),
            plan_hash: hex32(plan.identity().as_bytes()),
            application: input.application.as_str().to_owned(),
            database: input.target.database().as_str().to_owned(),
            environment: input.target.environment().as_str().to_owned(),
            lineage: input.target.lineage().as_str().to_owned(),
            contract_version: input.contract.version().get(),
            contract_bundle_hash: hex32(input.contract.bundle_hash().as_bytes()),
            source_hash: hex32(input.source_hash.as_bytes()),
            lock_hash: hex32(input.lock_hash.as_bytes()),
            manifest_hash: hex32(input.manifest_hash.as_bytes()),
            artifacts: input
                .artifacts
                .iter()
                .map(|artifact| ReceiptArtifactDto {
                    kind: artifact.kind().tag().to_owned(),
                    name: artifact.name().as_str().to_owned(),
                    content_hash: hex32(artifact.content_hash().as_bytes()),
                })
                .collect(),
            roles: input
                .roles
                .iter()
                .map(|role| ReceiptRoleDto {
                    name: role.name().as_str().to_owned(),
                    role_hash: hex32(role.role_hash().as_bytes()),
                })
                .collect(),
            drivers: input
                .drivers
                .iter()
                .map(|driver| driver.tag().to_owned())
                .collect(),
            seed_batches: u64::try_from(input.seeds.len()).map_err(|_| {
                InstallationCampaignError::new(InstallationCampaignErrorKind::EvidenceMismatch)
            })?,
            seed_items,
            migration_hash: input
                .migration
                .map(|migration| hex32(migration.migration_hash().as_bytes())),
            adapter_manifest_hash: input
                .adapter_manifest_hash
                .map(|hash| hex32(hash.as_bytes())),
        };
        Self::from_v1_dto(dto)
    }

    fn seal_matching(
        campaign: &ApplicationInstallationCampaign,
        plan: &ApplicationInstallationPlan,
        expected: ApplicationInstallationReceiptHash,
    ) -> Result<Self, InstallationCampaignError> {
        let current = Self::seal(campaign, plan)?;
        if current.identity() == expected {
            return Ok(current);
        }
        let legacy = Self::seal_v1(campaign, plan)?;
        if legacy.identity() == expected {
            return Ok(legacy);
        }
        Err(InstallationCampaignError::new(
            InstallationCampaignErrorKind::EvidenceMismatch,
        ))
    }

    fn from_v1_dto(dto: ReceiptDtoV1) -> Result<Self, InstallationCampaignError> {
        let campaign_id = parse_receipt_campaign_id(&dto.campaign_id)?;
        let plan_hash = parse_receipt_plan_hash(&dto.plan_hash)?;
        let mut canonical_bytes = serde_json::to_vec(&dto).map_err(|_| {
            InstallationCampaignError::new(InstallationCampaignErrorKind::InvalidEncoding)
        })?;
        canonical_bytes.push(b'\n');
        if canonical_bytes.len() > MAX_INSTALLATION_RECEIPT_BYTES {
            return Err(InstallationCampaignError::new(
                InstallationCampaignErrorKind::LimitExceeded,
            ));
        }
        Ok(Self {
            identity: hash_application_installation_receipt(&canonical_bytes),
            canonical_bytes,
            campaign_id,
            plan_hash,
        })
    }

    fn from_v2_dto(dto: ReceiptDtoV2) -> Result<Self, InstallationCampaignError> {
        let campaign_id = parse_receipt_campaign_id(&dto.campaign_id)?;
        let plan_hash = parse_receipt_plan_hash(&dto.plan_hash)?;
        let mut canonical_bytes = serde_json::to_vec(&dto).map_err(|_| {
            InstallationCampaignError::new(InstallationCampaignErrorKind::InvalidEncoding)
        })?;
        canonical_bytes.push(b'\n');
        if canonical_bytes.len() > MAX_INSTALLATION_RECEIPT_BYTES {
            return Err(InstallationCampaignError::new(
                InstallationCampaignErrorKind::LimitExceeded,
            ));
        }
        Ok(Self {
            identity: hash_application_installation_receipt(&canonical_bytes),
            canonical_bytes,
            campaign_id,
            plan_hash,
        })
    }

    /// Strictly decodes and identity-checks canonical terminal receipt bytes.
    pub fn decode_canonical(bytes: &[u8]) -> Result<Self, InstallationCampaignError> {
        if bytes.is_empty() || bytes.len() > MAX_INSTALLATION_RECEIPT_BYTES {
            return Err(InstallationCampaignError::new(
                InstallationCampaignErrorKind::LimitExceeded,
            ));
        }
        let probe: ReceiptSchemaProbe = serde_json::from_slice(bytes).map_err(|_| {
            InstallationCampaignError::new(InstallationCampaignErrorKind::InvalidEncoding)
        })?;
        let receipt = match probe.schema.as_str() {
            APPLICATION_INSTALLATION_RECEIPT_SCHEMA_V1 => {
                let dto: ReceiptDtoV1 = serde_json::from_slice(bytes).map_err(|_| {
                    InstallationCampaignError::new(InstallationCampaignErrorKind::InvalidEncoding)
                })?;
                validate_receipt_v1_dto(&dto)?;
                Self::from_v1_dto(dto)?
            }
            APPLICATION_INSTALLATION_RECEIPT_SCHEMA_V2 => {
                let dto: ReceiptDtoV2 = serde_json::from_slice(bytes).map_err(|_| {
                    InstallationCampaignError::new(InstallationCampaignErrorKind::InvalidEncoding)
                })?;
                validate_receipt_v2_dto(&dto)?;
                Self::from_v2_dto(dto)?
            }
            APPLICATION_INSTALLATION_RECEIPT_SCHEMA_V3 => {
                let dto: ReceiptDtoV2 = serde_json::from_slice(bytes).map_err(|_| {
                    InstallationCampaignError::new(InstallationCampaignErrorKind::InvalidEncoding)
                })?;
                validate_receipt_v3_dto(&dto)?;
                Self::from_v2_dto(dto)?
            }
            _ => {
                return Err(InstallationCampaignError::new(
                    InstallationCampaignErrorKind::UnsupportedVersion,
                ));
            }
        };
        if receipt.canonical_bytes() != bytes {
            return Err(InstallationCampaignError::new(
                InstallationCampaignErrorKind::NonCanonical,
            ));
        }
        Ok(receipt)
    }

    /// Terminal receipt identity.
    #[must_use]
    pub const fn identity(&self) -> ApplicationInstallationReceiptHash {
        self.identity
    }

    /// Caller-stable campaign identity.
    #[must_use]
    pub const fn campaign_id(&self) -> ApplicationInstallationCampaignId {
        self.campaign_id
    }

    /// Exact installation plan identity.
    #[must_use]
    pub const fn plan_hash(&self) -> ApplicationInstallationPlanHash {
        self.plan_hash
    }

    /// Canonical redacted compatibility bytes.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }
}

impl fmt::Debug for ApplicationInstallationReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApplicationInstallationReceipt")
            .field("identity", &self.identity)
            .field("campaign_id", &self.campaign_id)
            .field("plan_hash", &self.plan_hash)
            .finish_non_exhaustive()
    }
}

fn validate_stage_evidence(
    plan: &ApplicationInstallationPlan,
    evidence: &InstallationStageEvidence,
) -> Result<(), InstallationCampaignError> {
    let input = plan.input();
    let valid = match evidence {
        InstallationStageEvidence::Preflight {
            source_hash,
            lock_hash,
            manifest_hash,
        } => {
            *source_hash == input.source_hash
                && *lock_hash == input.lock_hash
                && *manifest_hash == input.manifest_hash
        }
        InstallationStageEvidence::Contract {
            version,
            bundle_hash,
        } => *version == input.contract.version() && *bundle_hash == input.contract.bundle_hash(),
        InstallationStageEvidence::Migration { migration_hash } => {
            *migration_hash == input.migration.map(|migration| migration.migration_hash())
        }
        InstallationStageEvidence::QueryModules(artifacts) => {
            artifacts == &artifacts_for(input, InstallationArtifactKind::QueryModule)
        }
        InstallationStageEvidence::ReactiveModules(artifacts) => {
            artifacts == &artifacts_for(input, InstallationArtifactKind::ReactiveModule)
        }
        InstallationStageEvidence::Roles(roles) => {
            let expected = input
                .roles
                .iter()
                .map(|role| InstalledRoleEvidence::new(role.name().clone(), role.role_hash()))
                .collect::<Vec<_>>();
            roles == &expected
        }
        InstallationStageEvidence::Reimport(evidence) => match (input.reimport, evidence) {
            (None, InstalledReimportEvidence::NotRequired) => true,
            (
                Some(expected),
                InstalledReimportEvidence::Reconciled(InstalledReimportReceiptEvidence {
                    export_manifest_hash,
                    export_receipt_hash,
                    portability_manifest_hash,
                    ..
                }),
            ) => {
                *export_manifest_hash == expected.export_manifest_hash()
                    && *export_receipt_hash == expected.export_receipt_hash()
                    && *portability_manifest_hash == expected.portability_manifest_hash()
            }
            _ => false,
        },
        InstallationStageEvidence::Credentials(credentials) => {
            let expected = input
                .credential_destinations
                .iter()
                .map(|destination| {
                    InstalledCredentialEvidence::new(
                        destination.name().clone(),
                        destination.successor(),
                    )
                })
                .collect::<Vec<_>>();
            credentials == &expected
        }
        InstallationStageEvidence::DriverProof(drivers) => drivers == &input.drivers,
        InstallationStageEvidence::Seeds(seeds) => {
            seeds.len() == input.seeds.len()
                && seeds.iter().zip(&input.seeds).all(|(observed, expected)| {
                    observed.name() == expected.name()
                        && observed.content_hash() == expected.content_hash()
                        && observed.succeeded().checked_add(observed.replayed())
                            == Some(expected.item_count())
                })
        }
        InstallationStageEvidence::Receipt(_) => false,
    };
    if valid {
        Ok(())
    } else {
        Err(InstallationCampaignError::new(
            InstallationCampaignErrorKind::EvidenceMismatch,
        ))
    }
}

fn artifacts_for(
    input: &crate::ApplicationInstallationPlanInput,
    kind: InstallationArtifactKind,
) -> Vec<InstallationArtifact> {
    input
        .artifacts
        .iter()
        .filter(|artifact| artifact.kind() == kind)
        .cloned()
        .collect()
}

fn validate_campaign_state(
    campaign: &ApplicationInstallationCampaign,
    plan: &ApplicationInstallationPlan,
) -> Result<(), InstallationCampaignError> {
    campaign.verify_plan(plan)?;
    match (&campaign.reimport, plan.input().reimport) {
        (None, _) => {}
        (Some(progress), Some(expected)) => {
            if progress.source().export_manifest_hash() != expected.export_manifest_hash()
                || progress.source().export_receipt_hash() != expected.export_receipt_hash()
                || progress.source().portability_manifest_hash()
                    != expected.portability_manifest_hash()
            {
                return Err(evidence_mismatch());
            }
            let completed_reimport = campaign
                .completed
                .iter()
                .any(|evidence| matches!(evidence, InstallationStageEvidence::Reimport(_)));
            if completed_reimport
                && progress.phase() != crate::ApplicationReimportCampaignPhaseV1::Reconciled
            {
                return Err(evidence_mismatch());
            }
            if !completed_reimport && campaign.next_stage() != Some(InstallationStage::Reimport) {
                return Err(evidence_mismatch());
            }
        }
        (Some(_), None) => return Err(evidence_mismatch()),
    }
    if campaign.completed.len() > InstallationStage::ALL.len() {
        return Err(InstallationCampaignError::new(
            InstallationCampaignErrorKind::StageOutOfOrder,
        ));
    }
    for (index, evidence) in campaign.completed.iter().enumerate() {
        let expected = InstallationStage::ALL[index];
        if evidence.stage() != expected {
            return Err(InstallationCampaignError::new(
                InstallationCampaignErrorKind::StageOutOfOrder,
            ));
        }
        match evidence {
            InstallationStageEvidence::Receipt(hash) => {
                if index + 1 != InstallationStage::ALL.len() {
                    return Err(InstallationCampaignError::new(
                        InstallationCampaignErrorKind::StageOutOfOrder,
                    ));
                }
                let prefix = ApplicationInstallationCampaign {
                    campaign_id: campaign.campaign_id,
                    plan_hash: campaign.plan_hash,
                    completed: campaign.completed[..index].to_vec(),
                    failure: None,
                    reimport: campaign.reimport.clone(),
                };
                ApplicationInstallationReceipt::seal_matching(&prefix, plan, *hash)?;
            }
            _ => validate_stage_evidence(plan, evidence)?,
        }
    }
    match campaign.failure {
        None => Ok(()),
        Some(failure) => {
            let Some(stage) = campaign.next_stage() else {
                return Err(InstallationCampaignError::new(
                    InstallationCampaignErrorKind::EvidenceMismatch,
                ));
            };
            if failure.stage != stage
                || failure.next_action != InstallationNextAction::for_stage(stage)
            {
                return Err(InstallationCampaignError::new(
                    InstallationCampaignErrorKind::EvidenceMismatch,
                ));
            }
            Ok(())
        }
    }
}

fn encode_campaign_state_dto(dto: &CampaignStateDto) -> Result<Vec<u8>, InstallationCampaignError> {
    let mut bytes = serde_json::to_vec(dto).map_err(|_| {
        InstallationCampaignError::new(InstallationCampaignErrorKind::InvalidEncoding)
    })?;
    bytes.push(b'\n');
    if bytes.len() > MAX_INSTALLATION_CAMPAIGN_STATE_BYTES {
        return Err(InstallationCampaignError::new(
            InstallationCampaignErrorKind::LimitExceeded,
        ));
    }
    Ok(bytes)
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CampaignStateDto {
    schema: String,
    campaign_id: String,
    plan_hash: String,
    plan: String,
    completed: Vec<CampaignEvidenceDto>,
    failure: Option<CampaignFailureDto>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reimport: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CampaignStateSchema {
    V1,
    V2,
    V3,
}

impl CampaignStateSchema {
    fn parse(value: &str) -> Result<Self, InstallationCampaignError> {
        match value {
            APPLICATION_INSTALLATION_CAMPAIGN_STATE_SCHEMA_V1 => Ok(Self::V1),
            APPLICATION_INSTALLATION_CAMPAIGN_STATE_SCHEMA_V2 => Ok(Self::V2),
            APPLICATION_INSTALLATION_CAMPAIGN_STATE_SCHEMA_V3 => Ok(Self::V3),
            _ => Err(InstallationCampaignError::new(
                InstallationCampaignErrorKind::UnsupportedVersion,
            )),
        }
    }
}

impl CampaignStateDto {
    fn from_parts(
        campaign: &ApplicationInstallationCampaign,
        plan: &ApplicationInstallationPlan,
    ) -> Self {
        Self {
            schema: if campaign.reimport.is_some() {
                APPLICATION_INSTALLATION_CAMPAIGN_STATE_SCHEMA_V3
            } else {
                APPLICATION_INSTALLATION_CAMPAIGN_STATE_SCHEMA_V2
            }
            .to_owned(),
            campaign_id: hex16(campaign.campaign_id.as_bytes()),
            plan_hash: hex32(campaign.plan_hash.as_bytes()),
            plan: String::from_utf8(plan.canonical_bytes().to_vec())
                .expect("canonical installation plans are JSON UTF-8"),
            completed: campaign
                .completed
                .iter()
                .map(CampaignEvidenceDto::from_evidence)
                .collect(),
            failure: campaign.failure.map(CampaignFailureDto::from_failure),
            reimport: campaign.reimport.as_ref().map(|progress| {
                String::from_utf8(
                    progress
                        .encode_canonical()
                        .expect("validated reimport progress is canonical"),
                )
                .expect("canonical reimport progress is JSON UTF-8")
            }),
        }
    }

    fn to_campaign(
        &self,
        plan: &ApplicationInstallationPlan,
        schema: CampaignStateSchema,
    ) -> Result<ApplicationInstallationCampaign, InstallationCampaignError> {
        let invalid =
            || InstallationCampaignError::new(InstallationCampaignErrorKind::InvalidEncoding);
        let campaign_id = ApplicationInstallationCampaignId::from_bytes(
            parse_hex16(&self.campaign_id).map_err(InstallationCampaignError::from_plan_error)?,
        )
        .map_err(|_| invalid())?;
        let plan_hash = ApplicationInstallationPlanHash::from_bytes(
            parse_hex32(&self.plan_hash).map_err(InstallationCampaignError::from_plan_error)?,
        );
        if plan_hash != plan.identity() {
            return Err(InstallationCampaignError::new(
                InstallationCampaignErrorKind::PlanIdentityMismatch,
            ));
        }
        let mut completed = self
            .completed
            .iter()
            .map(CampaignEvidenceDto::to_evidence)
            .collect::<Result<Vec<_>, _>>()?;
        if schema == CampaignStateSchema::V1 {
            let role_index = completed
                .iter()
                .position(|evidence| evidence.stage() == InstallationStage::Roles);
            if let Some(role_index) = role_index {
                completed.insert(
                    role_index + 1,
                    InstallationStageEvidence::Reimport(InstalledReimportEvidence::NotRequired),
                );
            }
        }
        let reimport = self
            .reimport
            .as_ref()
            .map(|value| ApplicationReimportCampaignV1::decode_canonical(value.as_bytes()))
            .transpose()
            .map_err(|_| invalid())?;
        if (schema != CampaignStateSchema::V3 && reimport.is_some())
            || (schema == CampaignStateSchema::V3 && reimport.is_none())
        {
            return Err(InstallationCampaignError::new(
                InstallationCampaignErrorKind::UnsupportedVersion,
            ));
        }
        let failure = self
            .failure
            .as_ref()
            .map(CampaignFailureDto::to_failure)
            .transpose()?;
        Ok(ApplicationInstallationCampaign {
            campaign_id,
            plan_hash,
            completed,
            failure,
            reimport,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "stage", rename_all = "snake_case", deny_unknown_fields)]
enum CampaignEvidenceDto {
    Preflight {
        source_hash: String,
        lock_hash: String,
        manifest_hash: String,
    },
    Contract {
        version: u64,
        bundle_hash: String,
    },
    Migration {
        migration_hash: Option<String>,
    },
    QueryModules {
        artifacts: Vec<CampaignArtifactDto>,
    },
    ReactiveModules {
        artifacts: Vec<CampaignArtifactDto>,
    },
    Roles {
        roles: Vec<CampaignRoleDto>,
    },
    Reimport {
        status: String,
        export_manifest_hash: Option<String>,
        export_receipt_hash: Option<String>,
        portability_manifest_hash: Option<String>,
        reimport_receipt_hash: Option<String>,
    },
    Credentials {
        credentials: Vec<CampaignCredentialDto>,
    },
    DriverProof {
        drivers: Vec<String>,
    },
    Seeds {
        seeds: Vec<CampaignSeedDto>,
    },
    Receipt {
        receipt_hash: String,
    },
}

impl CampaignEvidenceDto {
    fn from_evidence(evidence: &InstallationStageEvidence) -> Self {
        match evidence {
            InstallationStageEvidence::Preflight {
                source_hash,
                lock_hash,
                manifest_hash,
            } => Self::Preflight {
                source_hash: hex32(source_hash.as_bytes()),
                lock_hash: hex32(lock_hash.as_bytes()),
                manifest_hash: hex32(manifest_hash.as_bytes()),
            },
            InstallationStageEvidence::Contract {
                version,
                bundle_hash,
            } => Self::Contract {
                version: version.get(),
                bundle_hash: hex32(bundle_hash.as_bytes()),
            },
            InstallationStageEvidence::Migration { migration_hash } => Self::Migration {
                migration_hash: migration_hash.map(|hash| hex32(hash.as_bytes())),
            },
            InstallationStageEvidence::QueryModules(artifacts) => Self::QueryModules {
                artifacts: artifacts.iter().map(CampaignArtifactDto::from).collect(),
            },
            InstallationStageEvidence::ReactiveModules(artifacts) => Self::ReactiveModules {
                artifacts: artifacts.iter().map(CampaignArtifactDto::from).collect(),
            },
            InstallationStageEvidence::Roles(roles) => Self::Roles {
                roles: roles.iter().map(CampaignRoleDto::from).collect(),
            },
            InstallationStageEvidence::Reimport(InstalledReimportEvidence::NotRequired) => {
                Self::Reimport {
                    status: "not_required".to_owned(),
                    export_manifest_hash: None,
                    export_receipt_hash: None,
                    portability_manifest_hash: None,
                    reimport_receipt_hash: None,
                }
            }
            InstallationStageEvidence::Reimport(InstalledReimportEvidence::Reconciled(
                InstalledReimportReceiptEvidence {
                    export_manifest_hash,
                    export_receipt_hash,
                    portability_manifest_hash,
                    reimport_receipt_hash,
                },
            )) => Self::Reimport {
                status: "reconciled".to_owned(),
                export_manifest_hash: Some(hex32(export_manifest_hash.as_bytes())),
                export_receipt_hash: Some(hex32(export_receipt_hash.as_bytes())),
                portability_manifest_hash: Some(hex32(portability_manifest_hash.as_bytes())),
                reimport_receipt_hash: Some(hex32(reimport_receipt_hash.as_bytes())),
            },
            InstallationStageEvidence::Credentials(credentials) => Self::Credentials {
                credentials: credentials
                    .iter()
                    .map(CampaignCredentialDto::from)
                    .collect(),
            },
            InstallationStageEvidence::DriverProof(drivers) => Self::DriverProof {
                drivers: drivers
                    .iter()
                    .map(|driver| driver.tag().to_owned())
                    .collect(),
            },
            InstallationStageEvidence::Seeds(seeds) => Self::Seeds {
                seeds: seeds.iter().map(CampaignSeedDto::from).collect(),
            },
            InstallationStageEvidence::Receipt(hash) => Self::Receipt {
                receipt_hash: hex32(hash.as_bytes()),
            },
        }
    }

    fn to_evidence(&self) -> Result<InstallationStageEvidence, InstallationCampaignError> {
        let invalid =
            || InstallationCampaignError::new(InstallationCampaignErrorKind::InvalidEncoding);
        let hex =
            |value: &str| parse_hex32(value).map_err(InstallationCampaignError::from_plan_error);
        Ok(match self {
            Self::Preflight {
                source_hash,
                lock_hash,
                manifest_hash,
            } => InstallationStageEvidence::Preflight {
                source_hash: ApplicationSourceHash::from_bytes(hex(source_hash)?),
                lock_hash: ApplicationLockHash::from_bytes(hex(lock_hash)?),
                manifest_hash: ApplicationManifestHash::from_bytes(hex(manifest_hash)?),
            },
            Self::Contract {
                version,
                bundle_hash,
            } => InstallationStageEvidence::Contract {
                version: ContractVersion::new(*version).ok_or_else(invalid)?,
                bundle_hash: ContractBundleHash::from_bytes(hex(bundle_hash)?),
            },
            Self::Migration { migration_hash } => InstallationStageEvidence::Migration {
                migration_hash: migration_hash
                    .as_deref()
                    .map(hex)
                    .transpose()?
                    .map(MigrationBundleHash::from_bytes),
            },
            Self::QueryModules { artifacts } => InstallationStageEvidence::QueryModules(
                artifacts
                    .iter()
                    .map(CampaignArtifactDto::to_artifact)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            Self::ReactiveModules { artifacts } => InstallationStageEvidence::ReactiveModules(
                artifacts
                    .iter()
                    .map(CampaignArtifactDto::to_artifact)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            Self::Roles { roles } => InstallationStageEvidence::Roles(
                roles
                    .iter()
                    .map(CampaignRoleDto::to_role)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            Self::Reimport {
                status,
                export_manifest_hash,
                export_receipt_hash,
                portability_manifest_hash,
                reimport_receipt_hash,
            } => match status.as_str() {
                "not_required"
                    if export_manifest_hash.is_none()
                        && export_receipt_hash.is_none()
                        && portability_manifest_hash.is_none()
                        && reimport_receipt_hash.is_none() =>
                {
                    InstallationStageEvidence::Reimport(InstalledReimportEvidence::NotRequired)
                }
                "reconciled"
                    if export_manifest_hash.is_some()
                        && export_receipt_hash.is_some()
                        && portability_manifest_hash.is_some()
                        && reimport_receipt_hash.is_some() =>
                {
                    InstallationStageEvidence::Reimport(InstalledReimportEvidence::Reconciled(
                        InstalledReimportReceiptEvidence {
                            export_manifest_hash: ApplicationExportManifestHash::from_bytes(hex(
                                export_manifest_hash.as_deref().ok_or_else(invalid)?,
                            )?),
                            export_receipt_hash: ApplicationExportReceiptHash::from_bytes(hex(
                                export_receipt_hash.as_deref().ok_or_else(invalid)?,
                            )?),
                            portability_manifest_hash:
                                ApplicationPortabilityManifestHash::from_bytes(hex(
                                    portability_manifest_hash.as_deref().ok_or_else(invalid)?,
                                )?),
                            reimport_receipt_hash: ApplicationReimportReceiptHash::from_bytes(hex(
                                reimport_receipt_hash.as_deref().ok_or_else(invalid)?,
                            )?),
                        },
                    ))
                }
                _ => return Err(invalid()),
            },
            Self::Credentials { credentials } => InstallationStageEvidence::Credentials(
                credentials
                    .iter()
                    .map(CampaignCredentialDto::to_credential)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            Self::DriverProof { drivers } => InstallationStageEvidence::DriverProof(
                drivers
                    .iter()
                    .map(|driver| InstallationDriver::parse(driver).ok_or_else(invalid))
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            Self::Seeds { seeds } => InstallationStageEvidence::Seeds(
                seeds
                    .iter()
                    .map(CampaignSeedDto::to_seed)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            Self::Receipt { receipt_hash } => InstallationStageEvidence::Receipt(
                ApplicationInstallationReceiptHash::from_bytes(hex(receipt_hash)?),
            ),
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CampaignArtifactDto {
    kind: String,
    name: String,
    content_hash: String,
}

impl From<&InstallationArtifact> for CampaignArtifactDto {
    fn from(artifact: &InstallationArtifact) -> Self {
        Self {
            kind: artifact.kind().tag().to_owned(),
            name: artifact.name().as_str().to_owned(),
            content_hash: hex32(artifact.content_hash().as_bytes()),
        }
    }
}

impl CampaignArtifactDto {
    fn to_artifact(&self) -> Result<InstallationArtifact, InstallationCampaignError> {
        let invalid =
            || InstallationCampaignError::new(InstallationCampaignErrorKind::InvalidEncoding);
        Ok(InstallationArtifact::new(
            InstallationArtifactKind::parse(&self.kind).ok_or_else(invalid)?,
            InstallationSymbol::new(self.name.clone())
                .map_err(InstallationCampaignError::from_plan_error)?,
            GeneratedArtifactHash::from_bytes(
                parse_hex32(&self.content_hash)
                    .map_err(InstallationCampaignError::from_plan_error)?,
            ),
        ))
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CampaignRoleDto {
    name: String,
    role_hash: String,
}

impl From<&InstalledRoleEvidence> for CampaignRoleDto {
    fn from(role: &InstalledRoleEvidence) -> Self {
        Self {
            name: role.name().as_str().to_owned(),
            role_hash: hex32(role.role_hash().as_bytes()),
        }
    }
}

impl CampaignRoleDto {
    fn to_role(&self) -> Result<InstalledRoleEvidence, InstallationCampaignError> {
        Ok(InstalledRoleEvidence::new(
            InstallationSymbol::new(self.name.clone())
                .map_err(InstallationCampaignError::from_plan_error)?,
            ApplicationRoleHash::from_bytes(
                parse_hex32(&self.role_hash).map_err(InstallationCampaignError::from_plan_error)?,
            ),
        ))
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CampaignCredentialDto {
    destination: String,
    capability_id: String,
}

impl From<&InstalledCredentialEvidence> for CampaignCredentialDto {
    fn from(credential: &InstalledCredentialEvidence) -> Self {
        Self {
            destination: credential.destination().as_str().to_owned(),
            capability_id: hex16(credential.capability_id().as_bytes()),
        }
    }
}

impl CampaignCredentialDto {
    fn to_credential(&self) -> Result<InstalledCredentialEvidence, InstallationCampaignError> {
        let invalid =
            || InstallationCampaignError::new(InstallationCampaignErrorKind::InvalidEncoding);
        Ok(InstalledCredentialEvidence::new(
            InstallationSymbol::new(self.destination.clone())
                .map_err(InstallationCampaignError::from_plan_error)?,
            CapabilityId::from_bytes(
                parse_hex16(&self.capability_id)
                    .map_err(InstallationCampaignError::from_plan_error)?,
            )
            .map_err(|_| invalid())?,
        ))
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CampaignSeedDto {
    name: String,
    content_hash: String,
    succeeded: u64,
    replayed: u64,
}

impl From<&InstalledSeedEvidence> for CampaignSeedDto {
    fn from(seed: &InstalledSeedEvidence) -> Self {
        Self {
            name: seed.name().as_str().to_owned(),
            content_hash: hex32(seed.content_hash().as_bytes()),
            succeeded: seed.succeeded(),
            replayed: seed.replayed(),
        }
    }
}

impl CampaignSeedDto {
    fn to_seed(&self) -> Result<InstalledSeedEvidence, InstallationCampaignError> {
        Ok(InstalledSeedEvidence::new(
            InstallationSymbol::new(self.name.clone())
                .map_err(InstallationCampaignError::from_plan_error)?,
            GeneratedArtifactHash::from_bytes(
                parse_hex32(&self.content_hash)
                    .map_err(InstallationCampaignError::from_plan_error)?,
            ),
            self.succeeded,
            self.replayed,
        ))
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CampaignFailureDto {
    stage: String,
    code: String,
    next_action: String,
}

impl CampaignFailureDto {
    fn from_failure(failure: InstallationFailure) -> Self {
        Self {
            stage: failure.stage().as_str().to_owned(),
            code: installation_failure_code_tag(failure.code()).to_owned(),
            next_action: installation_next_action_tag(failure.next_action()).to_owned(),
        }
    }

    fn to_failure(&self) -> Result<InstallationFailure, InstallationCampaignError> {
        let invalid =
            || InstallationCampaignError::new(InstallationCampaignErrorKind::InvalidEncoding);
        Ok(InstallationFailure {
            stage: parse_installation_stage(&self.stage).ok_or_else(invalid)?,
            code: parse_installation_failure_code(&self.code).ok_or_else(invalid)?,
            next_action: parse_installation_next_action(&self.next_action).ok_or_else(invalid)?,
        })
    }
}

const fn installation_failure_code_tag(code: InstallationFailureCode) -> &'static str {
    match code {
        InstallationFailureCode::LocalArtifactMismatch => "local_artifact_mismatch",
        InstallationFailureCode::RemoteIdentityMismatch => "remote_identity_mismatch",
        InstallationFailureCode::MigrationGateRequired => "migration_gate_required",
        InstallationFailureCode::RoleWideningApprovalRequired => "role_widening_approval_required",
        InstallationFailureCode::ReimportPartial => "reimport_partial",
        InstallationFailureCode::CredentialDestinationOccupied => "credential_destination_occupied",
        InstallationFailureCode::DriverProofFailed => "driver_proof_failed",
        InstallationFailureCode::SeedPartial => "seed_partial",
        InstallationFailureCode::AuthorizationDenied => "authorization_denied",
        InstallationFailureCode::ServiceUnavailable => "service_unavailable",
    }
}

fn parse_installation_failure_code(value: &str) -> Option<InstallationFailureCode> {
    match value {
        "local_artifact_mismatch" => Some(InstallationFailureCode::LocalArtifactMismatch),
        "remote_identity_mismatch" => Some(InstallationFailureCode::RemoteIdentityMismatch),
        "migration_gate_required" => Some(InstallationFailureCode::MigrationGateRequired),
        "role_widening_approval_required" => {
            Some(InstallationFailureCode::RoleWideningApprovalRequired)
        }
        "reimport_partial" => Some(InstallationFailureCode::ReimportPartial),
        "credential_destination_occupied" => {
            Some(InstallationFailureCode::CredentialDestinationOccupied)
        }
        "driver_proof_failed" => Some(InstallationFailureCode::DriverProofFailed),
        "seed_partial" => Some(InstallationFailureCode::SeedPartial),
        "authorization_denied" => Some(InstallationFailureCode::AuthorizationDenied),
        "service_unavailable" => Some(InstallationFailureCode::ServiceUnavailable),
        _ => None,
    }
}

const fn installation_next_action_tag(action: InstallationNextAction) -> &'static str {
    match action {
        InstallationNextAction::ValidateLocalArtifacts => "validate_local_artifacts",
        InstallationNextAction::DeployContract => "deploy_contract",
        InstallationNextAction::ApplyMigration => "apply_migration",
        InstallationNextAction::DeployQueryModules => "deploy_query_modules",
        InstallationNextAction::DeployReactiveModules => "deploy_reactive_modules",
        InstallationNextAction::ReconcileRoles => "reconcile_roles",
        InstallationNextAction::ReimportApplication => "reimport_application",
        InstallationNextAction::RotateCredentials => "rotate_credentials",
        InstallationNextAction::ProveDrivers => "prove_drivers",
        InstallationNextAction::RunSeeds => "run_seeds",
        InstallationNextAction::SealReceipt => "seal_receipt",
        InstallationNextAction::None => "none",
    }
}

fn parse_installation_next_action(value: &str) -> Option<InstallationNextAction> {
    match value {
        "validate_local_artifacts" => Some(InstallationNextAction::ValidateLocalArtifacts),
        "deploy_contract" => Some(InstallationNextAction::DeployContract),
        "apply_migration" => Some(InstallationNextAction::ApplyMigration),
        "deploy_query_modules" => Some(InstallationNextAction::DeployQueryModules),
        "deploy_reactive_modules" => Some(InstallationNextAction::DeployReactiveModules),
        "reconcile_roles" => Some(InstallationNextAction::ReconcileRoles),
        "reimport_application" => Some(InstallationNextAction::ReimportApplication),
        "rotate_credentials" => Some(InstallationNextAction::RotateCredentials),
        "prove_drivers" => Some(InstallationNextAction::ProveDrivers),
        "run_seeds" => Some(InstallationNextAction::RunSeeds),
        "seal_receipt" => Some(InstallationNextAction::SealReceipt),
        "none" => Some(InstallationNextAction::None),
        _ => None,
    }
}

fn parse_installation_stage(value: &str) -> Option<InstallationStage> {
    InstallationStage::ALL
        .into_iter()
        .find(|stage| stage.as_str() == value)
}

#[derive(Deserialize)]
struct ReceiptSchemaProbe {
    schema: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiptDtoV1 {
    schema: String,
    campaign_id: String,
    plan_hash: String,
    application: String,
    database: String,
    environment: String,
    lineage: String,
    contract_version: u64,
    contract_bundle_hash: String,
    source_hash: String,
    lock_hash: String,
    manifest_hash: String,
    artifacts: Vec<ReceiptArtifactDto>,
    roles: Vec<ReceiptRoleDto>,
    drivers: Vec<String>,
    seed_batches: u64,
    seed_items: u64,
    migration_hash: Option<String>,
    adapter_manifest_hash: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiptDtoV2 {
    schema: String,
    campaign_id: String,
    plan_hash: String,
    application: String,
    database: String,
    environment: String,
    lineage: String,
    contract_version: u64,
    contract_bundle_hash: String,
    source_hash: String,
    lock_hash: String,
    manifest_hash: String,
    artifacts: Vec<ReceiptArtifactDto>,
    roles: Vec<ReceiptRoleDto>,
    credentials: Vec<ReceiptCredentialDto>,
    drivers: Vec<String>,
    seed_checkpoints: Vec<ReceiptSeedCheckpointDto>,
    migration_receipt: Option<ReceiptMigrationReferenceDto>,
    backup_receipt: Option<ReceiptBackupReferenceDto>,
    adapter_manifest_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reimport: Option<ReceiptReimportReferenceDto>,
    terminal_state: String,
    safe_remediation: Vec<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiptArtifactDto {
    kind: String,
    name: String,
    content_hash: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiptRoleDto {
    name: String,
    role_hash: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiptCredentialDto {
    destination: String,
    role: String,
    capability_id: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiptSeedCheckpointDto {
    name: String,
    content_hash: String,
    succeeded: u64,
    replayed: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiptMigrationReferenceDto {
    operation_id: String,
    migration_hash: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiptBackupReferenceDto {
    migration_operation_id: String,
    policy: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiptReimportReferenceDto {
    export_manifest_hash: String,
    export_receipt_hash: String,
    portability_manifest_hash: String,
    reimport_receipt_hash: String,
}

impl ReceiptDtoV2 {
    fn from_campaign(
        campaign: &ApplicationInstallationCampaign,
        plan: &ApplicationInstallationPlan,
    ) -> Result<Self, InstallationCampaignError> {
        let input = plan.input();
        let credentials = campaign
            .completed
            .iter()
            .find_map(|evidence| match evidence {
                InstallationStageEvidence::Credentials(credentials) => Some(credentials),
                _ => None,
            })
            .ok_or_else(evidence_mismatch)?;
        let seed_checkpoints = campaign
            .completed
            .iter()
            .find_map(|evidence| match evidence {
                InstallationStageEvidence::Seeds(seeds) => Some(seeds),
                _ => None,
            })
            .ok_or_else(evidence_mismatch)?;
        let credential_receipts = credentials
            .iter()
            .map(|credential| {
                let destination = input
                    .credential_destinations
                    .iter()
                    .find(|destination| destination.name() == credential.destination())
                    .ok_or_else(evidence_mismatch)?;
                Ok(ReceiptCredentialDto {
                    destination: credential.destination().as_str().to_owned(),
                    role: destination.role().as_str().to_owned(),
                    capability_id: hex16(credential.capability_id().as_bytes()),
                })
            })
            .collect::<Result<Vec<_>, InstallationCampaignError>>()?;
        let operation_id = input
            .migration
            .map(|_| hex16(campaign.campaign_id.as_bytes()));
        let reimport = campaign
            .completed
            .iter()
            .find_map(|evidence| match evidence {
                InstallationStageEvidence::Reimport(evidence) => Some(evidence),
                _ => None,
            })
            .ok_or_else(evidence_mismatch)?;
        let reimport = match (input.reimport, reimport) {
            (None, InstalledReimportEvidence::NotRequired) => None,
            (
                Some(expected),
                InstalledReimportEvidence::Reconciled(InstalledReimportReceiptEvidence {
                    export_manifest_hash,
                    export_receipt_hash,
                    portability_manifest_hash,
                    reimport_receipt_hash,
                }),
            ) if *export_manifest_hash == expected.export_manifest_hash()
                && *export_receipt_hash == expected.export_receipt_hash()
                && *portability_manifest_hash == expected.portability_manifest_hash() =>
            {
                Some(ReceiptReimportReferenceDto {
                    export_manifest_hash: hex32(export_manifest_hash.as_bytes()),
                    export_receipt_hash: hex32(export_receipt_hash.as_bytes()),
                    portability_manifest_hash: hex32(portability_manifest_hash.as_bytes()),
                    reimport_receipt_hash: hex32(reimport_receipt_hash.as_bytes()),
                })
            }
            _ => return Err(evidence_mismatch()),
        };
        let schema = if reimport.is_some() {
            APPLICATION_INSTALLATION_RECEIPT_SCHEMA_V3
        } else {
            APPLICATION_INSTALLATION_RECEIPT_SCHEMA_V2
        };
        Ok(Self {
            schema: schema.to_owned(),
            campaign_id: hex16(campaign.campaign_id.as_bytes()),
            plan_hash: hex32(plan.identity().as_bytes()),
            application: input.application.as_str().to_owned(),
            database: input.target.database().as_str().to_owned(),
            environment: input.target.environment().as_str().to_owned(),
            lineage: input.target.lineage().as_str().to_owned(),
            contract_version: input.contract.version().get(),
            contract_bundle_hash: hex32(input.contract.bundle_hash().as_bytes()),
            source_hash: hex32(input.source_hash.as_bytes()),
            lock_hash: hex32(input.lock_hash.as_bytes()),
            manifest_hash: hex32(input.manifest_hash.as_bytes()),
            artifacts: input
                .artifacts
                .iter()
                .map(|artifact| ReceiptArtifactDto {
                    kind: artifact.kind().tag().to_owned(),
                    name: artifact.name().as_str().to_owned(),
                    content_hash: hex32(artifact.content_hash().as_bytes()),
                })
                .collect(),
            roles: input
                .roles
                .iter()
                .map(|role| ReceiptRoleDto {
                    name: role.name().as_str().to_owned(),
                    role_hash: hex32(role.role_hash().as_bytes()),
                })
                .collect(),
            credentials: credential_receipts,
            drivers: input
                .drivers
                .iter()
                .map(|driver| driver.tag().to_owned())
                .collect(),
            seed_checkpoints: seed_checkpoints
                .iter()
                .map(|seed| ReceiptSeedCheckpointDto {
                    name: seed.name().as_str().to_owned(),
                    content_hash: hex32(seed.content_hash().as_bytes()),
                    succeeded: seed.succeeded(),
                    replayed: seed.replayed(),
                })
                .collect(),
            migration_receipt: input.migration.zip(operation_id.as_ref()).map(
                |(migration, operation_id)| ReceiptMigrationReferenceDto {
                    operation_id: operation_id.clone(),
                    migration_hash: hex32(migration.migration_hash().as_bytes()),
                },
            ),
            backup_receipt: operation_id.map(|migration_operation_id| ReceiptBackupReferenceDto {
                migration_operation_id,
                policy: "required_verified".to_owned(),
            }),
            adapter_manifest_hash: input
                .adapter_manifest_hash
                .map(|hash| hex32(hash.as_bytes())),
            reimport,
            terminal_state: "installed".to_owned(),
            safe_remediation: Vec::new(),
        })
    }
}

fn validate_receipt_v2_dto(dto: &ReceiptDtoV2) -> Result<(), InstallationCampaignError> {
    validate_receipt_dto(dto, APPLICATION_INSTALLATION_RECEIPT_SCHEMA_V2, false)
}

fn validate_receipt_v3_dto(dto: &ReceiptDtoV2) -> Result<(), InstallationCampaignError> {
    validate_receipt_dto(dto, APPLICATION_INSTALLATION_RECEIPT_SCHEMA_V3, true)
}

fn validate_receipt_dto(
    dto: &ReceiptDtoV2,
    expected_schema: &str,
    expects_reimport: bool,
) -> Result<(), InstallationCampaignError> {
    let invalid = || InstallationCampaignError::new(InstallationCampaignErrorKind::InvalidEncoding);
    if dto.schema != expected_schema
        || dto.reimport.is_some() != expects_reimport
        || dto.terminal_state != "installed"
        || !dto.safe_remediation.is_empty()
        || dto.artifacts.is_empty()
        || dto.roles.is_empty()
        || dto.credentials.is_empty()
        || dto.artifacts.len() > crate::MAX_INSTALLATION_ARTIFACTS
        || dto.roles.len() > crate::MAX_INSTALLATION_ROLES
        || dto.credentials.len() > crate::MAX_CREDENTIAL_DESTINATIONS
        || dto.seed_checkpoints.len() > crate::MAX_INSTALLATION_SEEDS
        || dto.migration_receipt.is_some() != dto.backup_receipt.is_some()
    {
        return Err(invalid());
    }
    let campaign_id = parse_receipt_campaign_id(&dto.campaign_id)?;
    parse_receipt_plan_hash(&dto.plan_hash)?;
    InstallationSymbol::new(dto.application.clone())
        .map_err(InstallationCampaignError::from_plan_error)?;
    riffdb_types::DatabaseAlias::new(dto.database.clone()).map_err(|_| invalid())?;
    riffdb_types::Environment::new(dto.environment.clone()).map_err(|_| invalid())?;
    riffdb_types::ContractLineage::new(dto.lineage.clone()).map_err(|_| invalid())?;
    ContractVersion::new(dto.contract_version).ok_or_else(invalid)?;
    for hash in [
        &dto.contract_bundle_hash,
        &dto.source_hash,
        &dto.lock_hash,
        &dto.manifest_hash,
    ] {
        parse_hex32(hash).map_err(InstallationCampaignError::from_plan_error)?;
    }
    if let Some(reimport) = &dto.reimport {
        for hash in [
            &reimport.export_manifest_hash,
            &reimport.export_receipt_hash,
            &reimport.portability_manifest_hash,
            &reimport.reimport_receipt_hash,
        ] {
            parse_hex32(hash).map_err(InstallationCampaignError::from_plan_error)?;
        }
    }
    validate_receipt_artifacts(&dto.artifacts)?;
    validate_receipt_roles(&dto.roles)?;
    if !dto
        .artifacts
        .iter()
        .any(|artifact| artifact.kind == "manifest")
        || !dto
            .artifacts
            .iter()
            .any(|artifact| artifact.kind == "contract_bundle")
    {
        return Err(invalid());
    }
    let mut previous_credential: Option<InstallationSymbol> = None;
    for credential in &dto.credentials {
        let destination = InstallationSymbol::new(credential.destination.clone())
            .map_err(InstallationCampaignError::from_plan_error)?;
        InstallationSymbol::new(credential.role.clone())
            .map_err(InstallationCampaignError::from_plan_error)?;
        if !dto.roles.iter().any(|role| role.name == credential.role) {
            return Err(invalid());
        }
        CapabilityId::from_bytes(
            parse_hex16(&credential.capability_id)
                .map_err(InstallationCampaignError::from_plan_error)?,
        )
        .map_err(|_| invalid())?;
        if previous_credential
            .as_ref()
            .is_some_and(|previous| previous >= &destination)
        {
            return Err(invalid());
        }
        previous_credential = Some(destination);
    }
    if dto.roles.iter().any(|role| {
        !dto.credentials
            .iter()
            .any(|credential| credential.role == role.name)
    }) {
        return Err(invalid());
    }
    validate_receipt_drivers(&dto.drivers)?;
    let mut previous_seed: Option<InstallationSymbol> = None;
    for seed in &dto.seed_checkpoints {
        let name = InstallationSymbol::new(seed.name.clone())
            .map_err(InstallationCampaignError::from_plan_error)?;
        parse_hex32(&seed.content_hash).map_err(InstallationCampaignError::from_plan_error)?;
        let item_count = seed
            .succeeded
            .checked_add(seed.replayed)
            .ok_or_else(invalid)?;
        if item_count == 0
            || item_count > crate::MAX_SEED_BATCH_ITEMS
            || previous_seed
                .as_ref()
                .is_some_and(|previous| previous >= &name)
        {
            return Err(invalid());
        }
        previous_seed = Some(name);
    }
    if let (Some(migration), Some(backup)) = (&dto.migration_receipt, &dto.backup_receipt) {
        let operation_id = parse_receipt_operation_id(&migration.operation_id)?;
        let backup_operation_id = parse_receipt_operation_id(&backup.migration_operation_id)?;
        if operation_id.into_bytes() != campaign_id.into_bytes()
            || operation_id != backup_operation_id
            || backup.policy != "required_verified"
        {
            return Err(invalid());
        }
        parse_hex32(&migration.migration_hash)
            .map_err(InstallationCampaignError::from_plan_error)?;
    }
    if let Some(hash) = &dto.adapter_manifest_hash {
        parse_hex32(hash).map_err(InstallationCampaignError::from_plan_error)?;
    }
    Ok(())
}

fn validate_receipt_v1_dto(dto: &ReceiptDtoV1) -> Result<(), InstallationCampaignError> {
    let invalid = || InstallationCampaignError::new(InstallationCampaignErrorKind::InvalidEncoding);
    ApplicationInstallationCampaignId::from_bytes(
        parse_hex16(&dto.campaign_id).map_err(InstallationCampaignError::from_plan_error)?,
    )
    .map_err(|_| invalid())?;
    parse_hex32(&dto.plan_hash).map_err(InstallationCampaignError::from_plan_error)?;
    InstallationSymbol::new(dto.application.clone())
        .map_err(InstallationCampaignError::from_plan_error)?;
    riffdb_types::DatabaseAlias::new(dto.database.clone()).map_err(|_| invalid())?;
    riffdb_types::Environment::new(dto.environment.clone()).map_err(|_| invalid())?;
    riffdb_types::ContractLineage::new(dto.lineage.clone()).map_err(|_| invalid())?;
    ContractVersion::new(dto.contract_version).ok_or_else(invalid)?;
    parse_hex32(&dto.contract_bundle_hash).map_err(InstallationCampaignError::from_plan_error)?;
    parse_hex32(&dto.source_hash).map_err(InstallationCampaignError::from_plan_error)?;
    parse_hex32(&dto.lock_hash).map_err(InstallationCampaignError::from_plan_error)?;
    parse_hex32(&dto.manifest_hash).map_err(InstallationCampaignError::from_plan_error)?;
    let mut previous_artifact: Option<(InstallationArtifactKind, InstallationSymbol)> = None;
    for artifact in &dto.artifacts {
        let kind = InstallationArtifactKind::parse(&artifact.kind).ok_or_else(invalid)?;
        let name = InstallationSymbol::new(artifact.name.clone())
            .map_err(InstallationCampaignError::from_plan_error)?;
        parse_hex32(&artifact.content_hash).map_err(InstallationCampaignError::from_plan_error)?;
        if previous_artifact
            .as_ref()
            .is_some_and(|previous| previous >= &(kind, name.clone()))
        {
            return Err(invalid());
        }
        previous_artifact = Some((kind, name));
    }
    let mut previous_role: Option<InstallationSymbol> = None;
    for role in &dto.roles {
        let name = InstallationSymbol::new(role.name.clone())
            .map_err(InstallationCampaignError::from_plan_error)?;
        parse_hex32(&role.role_hash).map_err(InstallationCampaignError::from_plan_error)?;
        if previous_role
            .as_ref()
            .is_some_and(|previous| previous >= &name)
        {
            return Err(invalid());
        }
        previous_role = Some(name);
    }
    let mut drivers = dto
        .drivers
        .iter()
        .map(|driver| InstallationDriver::parse(driver).ok_or_else(invalid))
        .collect::<Result<Vec<_>, _>>()?;
    let original = drivers.clone();
    drivers.sort();
    drivers.dedup();
    if original != drivers || dto.seed_batches > crate::MAX_INSTALLATION_SEEDS as u64 {
        return Err(invalid());
    }
    if let Some(hash) = &dto.migration_hash {
        parse_hex32(hash).map_err(InstallationCampaignError::from_plan_error)?;
    }
    if let Some(hash) = &dto.adapter_manifest_hash {
        parse_hex32(hash).map_err(InstallationCampaignError::from_plan_error)?;
    }
    Ok(())
}

fn evidence_mismatch() -> InstallationCampaignError {
    InstallationCampaignError::new(InstallationCampaignErrorKind::EvidenceMismatch)
}

fn parse_receipt_campaign_id(
    value: &str,
) -> Result<ApplicationInstallationCampaignId, InstallationCampaignError> {
    ApplicationInstallationCampaignId::from_bytes(
        parse_hex16(value).map_err(InstallationCampaignError::from_plan_error)?,
    )
    .map_err(|_| InstallationCampaignError::new(InstallationCampaignErrorKind::InvalidEncoding))
}

fn parse_receipt_plan_hash(
    value: &str,
) -> Result<ApplicationInstallationPlanHash, InstallationCampaignError> {
    Ok(ApplicationInstallationPlanHash::from_bytes(
        parse_hex32(value).map_err(InstallationCampaignError::from_plan_error)?,
    ))
}

fn parse_receipt_operation_id(
    value: &str,
) -> Result<ContractMigrationOperationId, InstallationCampaignError> {
    ContractMigrationOperationId::from_bytes(
        parse_hex16(value).map_err(InstallationCampaignError::from_plan_error)?,
    )
    .map_err(|_| InstallationCampaignError::new(InstallationCampaignErrorKind::InvalidEncoding))
}

fn validate_receipt_artifacts(
    artifacts: &[ReceiptArtifactDto],
) -> Result<(), InstallationCampaignError> {
    let invalid = || InstallationCampaignError::new(InstallationCampaignErrorKind::InvalidEncoding);
    let mut previous: Option<(InstallationArtifactKind, InstallationSymbol)> = None;
    for artifact in artifacts {
        let kind = InstallationArtifactKind::parse(&artifact.kind).ok_or_else(invalid)?;
        let name = InstallationSymbol::new(artifact.name.clone())
            .map_err(InstallationCampaignError::from_plan_error)?;
        parse_hex32(&artifact.content_hash).map_err(InstallationCampaignError::from_plan_error)?;
        if previous
            .as_ref()
            .is_some_and(|prior| prior >= &(kind, name.clone()))
        {
            return Err(invalid());
        }
        previous = Some((kind, name));
    }
    Ok(())
}

fn validate_receipt_roles(roles: &[ReceiptRoleDto]) -> Result<(), InstallationCampaignError> {
    let invalid = || InstallationCampaignError::new(InstallationCampaignErrorKind::InvalidEncoding);
    let mut previous: Option<InstallationSymbol> = None;
    for role in roles {
        let name = InstallationSymbol::new(role.name.clone())
            .map_err(InstallationCampaignError::from_plan_error)?;
        parse_hex32(&role.role_hash).map_err(InstallationCampaignError::from_plan_error)?;
        if previous.as_ref().is_some_and(|prior| prior >= &name) {
            return Err(invalid());
        }
        previous = Some(name);
    }
    Ok(())
}

fn validate_receipt_drivers(drivers: &[String]) -> Result<(), InstallationCampaignError> {
    let invalid = || InstallationCampaignError::new(InstallationCampaignErrorKind::InvalidEncoding);
    let mut parsed = drivers
        .iter()
        .map(|driver| InstallationDriver::parse(driver).ok_or_else(invalid))
        .collect::<Result<Vec<_>, _>>()?;
    let original = parsed.clone();
    parsed.sort();
    parsed.dedup();
    if original != parsed {
        return Err(invalid());
    }
    Ok(())
}

fn hex32(bytes: &[u8; 32]) -> String {
    crate::plan::hex32(bytes)
}

fn hex16(bytes: &[u8; 16]) -> String {
    crate::plan::hex16(bytes)
}

#[cfg(test)]
mod tests {
    use super::InstallationStage;
    use crate::APPLICATION_INSTALLATION_PLAN_SCHEMA_V1;

    #[test]
    fn installation_stages_are_closed_and_dependency_ordered() {
        assert_eq!(
            APPLICATION_INSTALLATION_PLAN_SCHEMA_V1,
            "riffdb.application-installation-plan/v1"
        );
        assert_eq!(InstallationStage::ALL.len(), 11);
        assert_eq!(InstallationStage::ALL[0], InstallationStage::Preflight);
        assert_eq!(InstallationStage::ALL[6], InstallationStage::Reimport);
        assert_eq!(InstallationStage::ALL[10], InstallationStage::Receipt);
        assert_eq!(InstallationStage::ALL[10].as_str(), "receipt");
    }
}
