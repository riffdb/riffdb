use std::error::Error;
use std::fmt;

use riffdb_types::{
    ApplicationInstallationCampaignId, ApplicationInstallationPlanHash,
    ApplicationInstallationReceiptHash, ApplicationLockHash, ApplicationManifestHash,
    ApplicationRoleHash, ApplicationSourceHash, CapabilityId, ContractBundleHash, ContractVersion,
    GeneratedArtifactHash, MigrationBundleHash, hash_application_installation_receipt,
};
use serde::{Deserialize, Serialize};

use crate::{
    ApplicationInstallationPlan, InstallationArtifact, InstallationArtifactKind,
    InstallationDriver, InstallationPlanError, InstallationPlanErrorKind, InstallationSymbol,
    parse_hex16, parse_hex32,
};

/// Canonical schema version for terminal installation receipts.
pub const APPLICATION_INSTALLATION_RECEIPT_SCHEMA_V1: &str =
    "riffdb.application-installation-receipt/v1";
/// Maximum canonical bytes in one redacted terminal receipt.
pub const MAX_INSTALLATION_RECEIPT_BYTES: usize = 2 * 1_024 * 1_024;

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
    pub const ALL: [Self; 10] = [
        Self::Preflight,
        Self::Contract,
        Self::Migration,
        Self::QueryModules,
        Self::ReactiveModules,
        Self::Roles,
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

/// Bounded terminal counters for one ordinary-command seed batch.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct InstalledSeedEvidence {
    name: InstallationSymbol,
    content_hash: GeneratedArtifactHash,
    succeeded: u64,
    replayed: u64,
}

impl InstalledSeedEvidence {
    /// Creates exact seed completion evidence without retaining item values.
    #[must_use]
    pub const fn new(
        name: InstallationSymbol,
        content_hash: GeneratedArtifactHash,
        succeeded: u64,
        replayed: u64,
    ) -> Self {
        Self {
            name,
            content_hash,
            succeeded,
            replayed,
        }
    }

    /// Symbolic seed name.
    #[must_use]
    pub const fn name(&self) -> &InstallationSymbol {
        &self.name
    }

    /// Exact seed artifact identity.
    #[must_use]
    pub const fn content_hash(&self) -> GeneratedArtifactHash {
        self.content_hash
    }

    /// Newly completed command items.
    #[must_use]
    pub const fn succeeded(&self) -> u64 {
        self.succeeded
    }

    /// Previously completed command items recovered by idempotency.
    #[must_use]
    pub const fn replayed(&self) -> u64 {
        self.replayed
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
            Self::Credentials(_) => InstallationStage::Credentials,
            Self::DriverProof(_) => InstallationStage::DriverProof,
            Self::Seeds(_) => InstallationStage::Seeds,
            Self::Receipt(_) => InstallationStage::Receipt,
        }
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
        validate_stage_evidence(plan, &evidence)?;
        self.completed.push(evidence);
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
            let receipt = ApplicationInstallationReceipt::seal(self, plan)?;
            if receipt.identity() == *retained_hash {
                return Ok(receipt);
            }
            return Err(InstallationCampaignError::new(
                InstallationCampaignErrorKind::EvidenceMismatch,
            ));
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
        let input = plan.input();
        let seed_items = input.seeds.iter().try_fold(0_u64, |sum, seed| {
            sum.checked_add(seed.item_count()).ok_or_else(|| {
                InstallationCampaignError::new(InstallationCampaignErrorKind::EvidenceMismatch)
            })
        })?;
        let dto = ReceiptDto {
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
            campaign_id: campaign.campaign_id,
            plan_hash: campaign.plan_hash,
        })
    }

    /// Strictly decodes and identity-checks canonical terminal receipt bytes.
    pub fn decode_canonical(bytes: &[u8]) -> Result<Self, InstallationCampaignError> {
        if bytes.is_empty() || bytes.len() > MAX_INSTALLATION_RECEIPT_BYTES {
            return Err(InstallationCampaignError::new(
                InstallationCampaignErrorKind::LimitExceeded,
            ));
        }
        let dto: ReceiptDto = serde_json::from_slice(bytes).map_err(|_| {
            InstallationCampaignError::new(InstallationCampaignErrorKind::InvalidEncoding)
        })?;
        if dto.schema != APPLICATION_INSTALLATION_RECEIPT_SCHEMA_V1 {
            return Err(InstallationCampaignError::new(
                InstallationCampaignErrorKind::UnsupportedVersion,
            ));
        }
        validate_receipt_dto(&dto)?;
        let mut canonical = serde_json::to_vec(&dto).map_err(|_| {
            InstallationCampaignError::new(InstallationCampaignErrorKind::InvalidEncoding)
        })?;
        canonical.push(b'\n');
        if canonical != bytes {
            return Err(InstallationCampaignError::new(
                InstallationCampaignErrorKind::NonCanonical,
            ));
        }
        let campaign_id = ApplicationInstallationCampaignId::from_bytes(
            parse_hex16(&dto.campaign_id).map_err(InstallationCampaignError::from_plan_error)?,
        )
        .map_err(|_| {
            InstallationCampaignError::new(InstallationCampaignErrorKind::InvalidEncoding)
        })?;
        let plan_hash = ApplicationInstallationPlanHash::from_bytes(
            parse_hex32(&dto.plan_hash).map_err(InstallationCampaignError::from_plan_error)?,
        );
        Ok(Self {
            identity: hash_application_installation_receipt(&canonical),
            canonical_bytes: canonical,
            campaign_id,
            plan_hash,
        })
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

/// Closed campaign state-machine failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstallationCampaignErrorKind {
    /// Caller supplied another campaign identity for retained state.
    CampaignIdentityMismatch,
    /// Caller reused a campaign identity for another exact plan.
    PlanIdentityMismatch,
    /// A stage was skipped, repeated, or submitted out of order.
    StageOutOfOrder,
    /// Observed stage state differs from the exact plan.
    EvidenceMismatch,
    /// The campaign already sealed its terminal receipt.
    AlreadyTerminal,
    /// A canonical campaign artifact exceeds its hard bound.
    LimitExceeded,
    /// A canonical campaign artifact is malformed.
    InvalidEncoding,
    /// A canonical campaign artifact has a valid shape but inexact bytes.
    NonCanonical,
    /// A canonical campaign artifact version is unsupported.
    UnsupportedVersion,
}

/// Bounded, redaction-safe campaign error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InstallationCampaignError {
    kind: InstallationCampaignErrorKind,
}

impl InstallationCampaignError {
    const fn new(kind: InstallationCampaignErrorKind) -> Self {
        Self { kind }
    }

    fn from_plan_error(error: InstallationPlanError) -> Self {
        let kind = match error.kind() {
            InstallationPlanErrorKind::LimitExceeded => {
                InstallationCampaignErrorKind::LimitExceeded
            }
            InstallationPlanErrorKind::UnsupportedVersion => {
                InstallationCampaignErrorKind::UnsupportedVersion
            }
            InstallationPlanErrorKind::NonCanonical => InstallationCampaignErrorKind::NonCanonical,
            _ => InstallationCampaignErrorKind::InvalidEncoding,
        };
        Self::new(kind)
    }

    /// Stable campaign failure kind.
    #[must_use]
    pub const fn kind(self) -> InstallationCampaignErrorKind {
        self.kind
    }
}

impl fmt::Display for InstallationCampaignError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            InstallationCampaignErrorKind::CampaignIdentityMismatch => {
                "application installation campaign identity does not match retained state"
            }
            InstallationCampaignErrorKind::PlanIdentityMismatch => {
                "application installation campaign identity is bound to another exact plan"
            }
            InstallationCampaignErrorKind::StageOutOfOrder => {
                "application installation stage is not the exact next stage"
            }
            InstallationCampaignErrorKind::EvidenceMismatch => {
                "application installation stage evidence differs from the exact plan"
            }
            InstallationCampaignErrorKind::AlreadyTerminal => {
                "application installation campaign already has a terminal receipt"
            }
            InstallationCampaignErrorKind::LimitExceeded => {
                "application installation campaign artifact exceeds a hard bound"
            }
            InstallationCampaignErrorKind::InvalidEncoding => {
                "application installation campaign artifact encoding is invalid"
            }
            InstallationCampaignErrorKind::NonCanonical => {
                "application installation campaign artifact encoding is not canonical"
            }
            InstallationCampaignErrorKind::UnsupportedVersion => {
                "application installation campaign artifact version is unsupported"
            }
        })
    }
}

impl Error for InstallationCampaignError {}

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
                    observed.name == *expected.name()
                        && observed.content_hash == expected.content_hash()
                        && observed.succeeded.checked_add(observed.replayed)
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

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiptDto {
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

fn validate_receipt_dto(dto: &ReceiptDto) -> Result<(), InstallationCampaignError> {
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
        assert_eq!(InstallationStage::ALL.len(), 10);
        assert_eq!(InstallationStage::ALL[0], InstallationStage::Preflight);
        assert_eq!(InstallationStage::ALL[9], InstallationStage::Receipt);
        assert_eq!(InstallationStage::ALL[9].as_str(), "receipt");
    }
}
