#![expect(
    clippy::expect_used,
    reason = "validated control-plane preparations retain every operation-specific target and nonzero bound"
)]

//! Typed control-plane preparations and sole-writer execution.

#[path = "replication_administration.rs"]
mod replication_administration;
pub(crate) use replication_administration::drive_replication_administration;
pub(crate) use replication_administration::drive_replication_maintenance;
pub use replication_administration::{
    ReplicationAdministrationExecutionResult, ReplicationAdministrationOutcome,
    ReplicationAdministrationRefusal, ReplicationAdministrationResultReceipt,
};

use std::{error::Error, fmt, num::NonZeroU32, num::NonZeroU64};

use riffdb_catalog::{
    PreparedCatalogActivation, PreparedQueryModuleActivation, PreparedReactiveModulePublication,
};
use riffdb_policy::{
    AbsentCapabilityRevokePreparationChange, AuthorizationClock, AuthorizationClockError,
    AuthorizedCapabilityMutationPreparation, AuthorizedCatalogDeployment, CapabilityActivity,
    CapabilityRevokePreparationChange, CapabilityRevokeTargetFacts,
    NormalizedCapabilityCreateRecord, PolicyCode, ProposedCapabilityCreate,
    ProposedCapabilityMutation, ProposedCapabilityRevoke,
    TransactionAbsentCapabilityRevokeDecision, TransactionCapabilityMutationDecision,
    TransactionCurrentCapabilityExistence, TransactionCurrentCapabilityFacts,
    TransactionCurrentCapabilityVerifier,
};
use riffdb_storage_api::{
    ActiveCatalogPointerV1, AuditPrincipalV1, BootstrapDigestCandidatesV1,
    BootstrapServiceAuditStartV1, CapabilityAdministrationTransactionPort,
    CapabilityBootstrapAdministrationRepository, CapabilityBootstrapIntentV1,
    CapabilityBootstrapResult, CapabilityCreateAwaitingDecision,
    CapabilityCreateCandidateTransaction, CapabilityCreateCandidateV1, CapabilityCreateIntentV1,
    CapabilityCreateResult, CapabilityLifecycleV1, CapabilityMutationCurrentStateV1,
    CapabilityRequestedRecordV1, CapabilityRevokeAwaitingDecision,
    CapabilityRevokeCandidateTransaction, CapabilityRevokeCandidateV1, CapabilityRevokeIntentV1,
    CapabilityRevokeResult, CatalogActivationResult, CatalogAdministrationRepository,
    QueryModuleActivationResult, QueryModuleAdministrationRepository,
    ReactiveModuleAdministrationRepository, ReactiveModulePublicationResult,
    ServiceAuditAppendIntentV1, ServiceAuditAppendRepository, ServiceAuditAppendResult,
    StorageError, StorageErrorKind, StorageValueError, TransactionCurrentCapabilityObservationV1,
};
use riffdb_types::{
    AdministrationSequence, CapabilityId, CapabilityPermissionKindV1, CapabilityTokenDigest,
    ContractBundleHash, ContractLineage, ContractVersion, QueryModuleHash, QueryModuleName,
    QueryModuleVersion, ReactiveModuleHash, RequestId, ServiceAuditLinkV1, ServiceAuditTargetV1,
    ServiceAuditTargetsV1, ServiceIngressKindV1, Timestamp,
};

use crate::{
    AdministrationClock, AdministrationClockError, BootstrapCompoundAuditProof,
    command_execution::CommandExecutionLifecycle,
};

/// Safe failure while binding checked policy/catalog inputs to one operation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ControlPlanePreparationError {
    /// The supplied proof authorizes a different operation or semantic identity.
    AuthorizationMismatch,
    /// Checked values from two semantic owners could not be lowered consistently.
    InternalDefect,
    /// The principal-less bootstrap candidate is not the closed accepted shape.
    InvalidBootstrap,
}

impl fmt::Display for ControlPlanePreparationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AuthorizationMismatch => {
                "control-plane authorization does not match the prepared operation"
            }
            Self::InternalDefect => "control-plane preparation encountered an internal defect",
            Self::InvalidBootstrap => "bootstrap preparation is invalid",
        })
    }
}

impl Error for ControlPlanePreparationError {}

/// Move-only deployment operation accepted only with exact catalog and policy proofs.
#[must_use = "a deployment preparation must be submitted or explicitly discarded"]
pub struct CatalogDeploymentPreparation {
    request_id: RequestId,
    catalog: PreparedCatalogActivation,
    authorization: AuthorizedCatalogDeployment,
}

impl CatalogDeploymentPreparation {
    /// Binds one catalog preparation to the fresh authorization for its exact identity.
    pub fn new(
        request_id: RequestId,
        catalog: PreparedCatalogActivation,
        authorization: AuthorizedCatalogDeployment,
    ) -> Result<Self, ControlPlanePreparationError> {
        let bundle = catalog.bundle();
        if bundle.lineage() != authorization.lineage()
            || bundle.contract_version() != authorization.version()
            || bundle.bundle_hash() != authorization.bundle_hash()
            || catalog.expected_active_version() != authorization.expected_active_version()
        {
            return Err(ControlPlanePreparationError::AuthorizationMismatch);
        }
        Ok(Self {
            request_id,
            catalog,
            authorization,
        })
    }
}

impl fmt::Debug for CatalogDeploymentPreparation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CatalogDeploymentPreparation([REDACTED])")
    }
}

/// Move-only query-module activation bound to an exact deployment authorization.
#[must_use = "a query-module deployment preparation must be submitted or explicitly discarded"]
pub struct QueryModuleDeploymentPreparation {
    request_id: RequestId,
    module: PreparedQueryModuleActivation,
    authorization: AuthorizedCatalogDeployment,
}

impl QueryModuleDeploymentPreparation {
    /// Binds a checked module to authorization for its exact contract identity.
    pub fn new(
        request_id: RequestId,
        module: PreparedQueryModuleActivation,
        authorization: AuthorizedCatalogDeployment,
    ) -> Result<Self, ControlPlanePreparationError> {
        let checked = module.module().module();
        if checked.contract_lineage() != authorization.lineage()
            || checked.contract_version() != authorization.version()
            || checked.contract_hash() != authorization.bundle_hash()
        {
            return Err(ControlPlanePreparationError::AuthorizationMismatch);
        }
        Ok(Self {
            request_id,
            module,
            authorization,
        })
    }
}

impl fmt::Debug for QueryModuleDeploymentPreparation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("QueryModuleDeploymentPreparation([REDACTED])")
    }
}

/// Move-only reactive-module publication bound to exact deployment authorization.
#[must_use = "a reactive-module publication must be submitted or explicitly discarded"]
pub struct ReactiveModulePublicationPreparation {
    request_id: RequestId,
    module: PreparedReactiveModulePublication,
    authorization: AuthorizedCatalogDeployment,
}

impl ReactiveModulePublicationPreparation {
    /// Binds a checked module to authorization for its exact contract identity.
    pub fn new(
        request_id: RequestId,
        module: PreparedReactiveModulePublication,
        authorization: AuthorizedCatalogDeployment,
    ) -> Result<Self, ControlPlanePreparationError> {
        let checked = module.module().plan();
        if checked.contract_lineage() != authorization.lineage()
            || checked.contract_version() != authorization.version()
            || checked.contract_hash() != authorization.bundle_hash()
        {
            return Err(ControlPlanePreparationError::AuthorizationMismatch);
        }
        Ok(Self {
            request_id,
            module,
            authorization,
        })
    }
}

impl fmt::Debug for ReactiveModulePublicationPreparation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ReactiveModulePublicationPreparation([REDACTED])")
    }
}

/// Move-only normal capability-create operation with no raw token material.
#[must_use = "a capability-create preparation must be submitted or explicitly discarded"]
pub struct CapabilityCreatePreparation {
    authorization: AuthorizedCapabilityMutationPreparation,
    requested: CapabilityRequestedRecordV1,
    token_digest: CapabilityTokenDigest,
}

impl CapabilityCreatePreparation {
    /// Binds the current-policy preparation to the generated token's nonsecret digest.
    pub fn new(
        authorization: AuthorizedCapabilityMutationPreparation,
        token_digest: CapabilityTokenDigest,
    ) -> Result<Self, ControlPlanePreparationError> {
        let target = authorization
            .create_target()
            .ok_or(ControlPlanePreparationError::AuthorizationMismatch)?;
        let requested = lower_requested_record(target.normalized_requested_record())
            .map_err(|_| ControlPlanePreparationError::InternalDefect)?;
        Ok(Self {
            authorization,
            requested,
            token_digest,
        })
    }
}

impl fmt::Debug for CapabilityCreatePreparation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CapabilityCreatePreparation([REDACTED])")
    }
}

/// Move-only normal capability-revoke operation for a present or absent target.
#[must_use = "a capability-revoke preparation must be submitted or explicitly discarded"]
pub struct CapabilityRevokePreparation {
    authorization: AuthorizedCapabilityMutationPreparation,
}

impl CapabilityRevokePreparation {
    /// Accepts only a policy-owned present- or absent-target revoke preparation.
    pub fn new(
        authorization: AuthorizedCapabilityMutationPreparation,
    ) -> Result<Self, ControlPlanePreparationError> {
        let is_present = authorization.revoke_target().is_some();
        let is_absent = authorization.absent_revoke_target().is_some();
        if is_present == is_absent || authorization.revoke_reason().is_none() {
            return Err(ControlPlanePreparationError::AuthorizationMismatch);
        }
        Ok(Self { authorization })
    }
}

impl fmt::Debug for CapabilityRevokePreparation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CapabilityRevokePreparation([REDACTED])")
    }
}

/// Move-only checked input for the sole principal-less mutation.
#[must_use = "a bootstrap preparation must be submitted or explicitly discarded"]
pub struct CapabilityBootstrapPreparation {
    capability_id: CapabilityId,
    requested: CapabilityRequestedRecordV1,
    digests: BootstrapDigestCandidatesV1,
    request_id: RequestId,
    ingress: ServiceIngressKindV1,
    targets: ServiceAuditTargetsV1,
    proof: BootstrapCompoundAuditProof,
}

impl CapabilityBootstrapPreparation {
    /// Checks the complete value-only bootstrap shape and seals its compound audit authority.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        capability_id: CapabilityId,
        requested: &NormalizedCapabilityCreateRecord,
        digest_candidates: Vec<CapabilityTokenDigest>,
        current_digest: CapabilityTokenDigest,
        request_id: RequestId,
        ingress: ServiceIngressKindV1,
        targets: ServiceAuditTargetsV1,
    ) -> Result<Self, ControlPlanePreparationError> {
        let requested = lower_requested_record(requested)
            .map_err(|_| ControlPlanePreparationError::InvalidBootstrap)?;
        if requested.actor_kind() != riffdb_types::ActorKind::Human
            || !requested
                .grant()
                .permissions()
                .contains_kind(CapabilityPermissionKindV1::AdministerCapabilities)
        {
            return Err(ControlPlanePreparationError::InvalidBootstrap);
        }
        let digests = BootstrapDigestCandidatesV1::new(digest_candidates, current_digest)
            .map_err(|_| ControlPlanePreparationError::InvalidBootstrap)?;
        if targets.as_slice() != [ServiceAuditTargetV1::Capability(capability_id)] {
            return Err(ControlPlanePreparationError::InvalidBootstrap);
        }
        // Constructing the storage start here would require inventing its clock. The
        // remaining closed shape is checked when the actor supplies that one sample.
        if matches!(ingress, ServiceIngressKindV1::McpHttp) {
            return Err(ControlPlanePreparationError::InvalidBootstrap);
        }
        Ok(Self {
            capability_id,
            requested,
            digests,
            request_id,
            ingress,
            targets,
            proof: BootstrapCompoundAuditProof::checked(),
        })
    }
}

impl fmt::Debug for CapabilityBootstrapPreparation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CapabilityBootstrapPreparation([REDACTED])")
    }
}

/// Closed terminal-audit instruction derived from a known executor result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlPlaneTerminalAudit {
    /// A known new or replayed transition must be linked as succeeded.
    Succeeded(ServiceAuditLinkV1),
    /// A bounded typed result proves no authoritative transition occurred.
    Failed,
}

/// Public semantic identity of an activated catalog pointer.
#[derive(Clone, Eq, PartialEq)]
pub struct ActivatedCatalog {
    lineage: ContractLineage,
    version: ContractVersion,
    bundle_hash: ContractBundleHash,
}

impl ActivatedCatalog {
    /// Returns the activated lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Returns the activated application version.
    #[must_use]
    pub const fn version(&self) -> ContractVersion {
        self.version
    }

    /// Returns the immutable bundle hash.
    #[must_use]
    pub const fn bundle_hash(&self) -> ContractBundleHash {
        self.bundle_hash
    }
}

impl fmt::Debug for ActivatedCatalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ActivatedCatalog([REDACTED])")
    }
}

/// Closed semantic deployment outcome; no storage transition type crosses this boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogDeploymentOutcome {
    /// A new active pointer committed.
    Activated(ActivatedCatalog),
    /// The exact pointer was already active.
    AlreadyActive(ActivatedCatalog),
    /// The transaction-current active version differed from the requested CAS.
    ExpectedActiveVersionMismatch {
        /// Actual transaction-current active version, including absence.
        actual: Option<ContractVersion>,
    },
    /// Immutable bytes conflicted for the same lineage/version identity.
    BundleConflict,
}

/// Deployment outcome plus private evidence for the required terminal audit.
pub struct CatalogDeploymentResult {
    outcome: CatalogDeploymentOutcome,
    transition_sequence: Option<AdministrationSequence>,
}

impl CatalogDeploymentResult {
    /// Borrows the safe semantic outcome.
    #[must_use]
    pub const fn outcome(&self) -> &CatalogDeploymentOutcome {
        &self.outcome
    }

    /// Consumes the result after its terminal audit has been durably handled.
    #[must_use]
    pub fn into_outcome(self) -> CatalogDeploymentOutcome {
        self.outcome
    }

    /// Returns the only valid terminal phase/link classification for this result.
    #[must_use]
    pub fn terminal_audit(&self) -> ControlPlaneTerminalAudit {
        terminal_audit(self.transition_sequence)
    }
}

impl fmt::Debug for CatalogDeploymentResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CatalogDeploymentResult([REDACTED])")
    }
}

/// Symbolic identity returned by one query-module activation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActivatedQueryModule {
    name: QueryModuleName,
    version: QueryModuleVersion,
    hash: QueryModuleHash,
}

impl ActivatedQueryModule {
    /// Module name.
    #[must_use]
    pub const fn name(&self) -> &QueryModuleName {
        &self.name
    }

    /// Module version.
    #[must_use]
    pub const fn version(&self) -> QueryModuleVersion {
        self.version
    }

    /// Immutable content hash.
    #[must_use]
    pub const fn hash(&self) -> QueryModuleHash {
        self.hash
    }
}

/// Closed semantic query-module deployment outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryModuleDeploymentOutcome {
    /// A new exact active pointer committed.
    Activated(ActivatedQueryModule),
    /// The exact module was already active.
    AlreadyActive(ActivatedQueryModule),
    /// The transaction-current pointer differed from the submitted CAS.
    ExpectedActiveMismatch {
        /// Actual module hash, or absence.
        actual: Option<QueryModuleHash>,
    },
    /// Same module name/version is retained with different bytes.
    ModuleVersionConflict,
    /// The exact contract is no longer retained.
    ContractUnavailable,
}

/// Deployment result plus the authoritative transition link.
pub struct QueryModuleDeploymentResult {
    outcome: QueryModuleDeploymentOutcome,
    transition_sequence: Option<AdministrationSequence>,
}

impl QueryModuleDeploymentResult {
    /// Safe semantic outcome.
    #[must_use]
    pub const fn outcome(&self) -> &QueryModuleDeploymentOutcome {
        &self.outcome
    }

    /// Consumes the result after terminal service audit handling.
    #[must_use]
    pub fn into_outcome(self) -> QueryModuleDeploymentOutcome {
        self.outcome
    }

    /// Required terminal service-audit link.
    #[must_use]
    pub fn terminal_audit(&self) -> ControlPlaneTerminalAudit {
        terminal_audit(self.transition_sequence)
    }
}

impl fmt::Debug for QueryModuleDeploymentResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("QueryModuleDeploymentResult([REDACTED])")
    }
}

/// Symbolic identity returned by one immutable reactive publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishedReactiveModule {
    name: String,
    version: u64,
    hash: ReactiveModuleHash,
}

impl PublishedReactiveModule {
    /// Module name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Positive module version.
    #[must_use]
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Immutable content identity.
    #[must_use]
    pub const fn hash(&self) -> ReactiveModuleHash {
        self.hash
    }
}

/// Closed semantic reactive-module publication outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReactiveModulePublicationOutcome {
    /// A new immutable module committed.
    Published(PublishedReactiveModule),
    /// The exact immutable module already existed.
    AlreadyPublished(PublishedReactiveModule),
    /// The name/version pair is retained with different bytes.
    ModuleVersionConflict,
    /// The exact contract is no longer retained.
    ContractUnavailable,
    /// One exact query-module dependency is no longer retained.
    QueryModuleUnavailable(QueryModuleHash),
}

/// Publication result plus authoritative transition link.
pub struct ReactiveModulePublicationExecutionResult {
    outcome: ReactiveModulePublicationOutcome,
    transition_sequence: Option<AdministrationSequence>,
}

impl ReactiveModulePublicationExecutionResult {
    /// Safe semantic outcome.
    #[must_use]
    pub const fn outcome(&self) -> &ReactiveModulePublicationOutcome {
        &self.outcome
    }

    /// Consumes the result after terminal service-audit handling.
    #[must_use]
    pub fn into_outcome(self) -> ReactiveModulePublicationOutcome {
        self.outcome
    }

    /// Required terminal service-audit link.
    #[must_use]
    pub fn terminal_audit(&self) -> ControlPlaneTerminalAudit {
        terminal_audit(self.transition_sequence)
    }
}

impl fmt::Debug for ReactiveModulePublicationExecutionResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ReactiveModulePublicationExecutionResult([REDACTED])")
    }
}

/// Capability identity and lifecycle revision without transition metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapabilityIdentity {
    capability_id: CapabilityId,
    revision: NonZeroU64,
}

impl CapabilityIdentity {
    /// Returns the stable capability identity.
    #[must_use]
    pub const fn capability_id(self) -> CapabilityId {
        self.capability_id
    }

    /// Returns the resulting lifecycle revision.
    #[must_use]
    pub const fn revision(self) -> NonZeroU64 {
        self.revision
    }
}

/// A new or replayed authoritative transition exposed by the API-neutral result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapabilityTransition {
    identity: CapabilityIdentity,
    administration_sequence: AdministrationSequence,
}

impl CapabilityTransition {
    /// Returns the resulting capability identity and revision.
    #[must_use]
    pub const fn identity(self) -> CapabilityIdentity {
        self.identity
    }

    /// Returns the transition's original authoritative sequence.
    #[must_use]
    pub const fn administration_sequence(self) -> AdministrationSequence {
        self.administration_sequence
    }
}

/// Closed normal-create outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapabilityCreateOutcome {
    /// A new capability committed; the service still owns the raw token.
    Created(CapabilityTransition),
    /// Exact replay succeeded but the original raw token is unrecoverable.
    AlreadyCreatedTokenUnavailable(CapabilityIdentity),
    /// The stable ID names different normalized requested content.
    CapabilityIdConflict,
}

/// Capability-create outcome plus private original-sequence audit evidence.
pub struct CapabilityCreateExecutionResult {
    outcome: CapabilityCreateOutcome,
    transition_sequence: Option<AdministrationSequence>,
}

impl CapabilityCreateExecutionResult {
    /// Borrows the safe semantic outcome.
    #[must_use]
    pub const fn outcome(&self) -> &CapabilityCreateOutcome {
        &self.outcome
    }

    /// Returns the exact terminal phase/link classification.
    #[must_use]
    pub fn terminal_audit(&self) -> ControlPlaneTerminalAudit {
        terminal_audit(self.transition_sequence)
    }

    /// Consumes the result after terminal audit handling.
    #[must_use]
    pub fn into_outcome(self) -> CapabilityCreateOutcome {
        self.outcome
    }
}

impl fmt::Debug for CapabilityCreateExecutionResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CapabilityCreateExecutionResult([REDACTED])")
    }
}

/// Closed normal-revoke outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapabilityRevokeOutcome {
    /// A new irreversible revocation committed.
    Revoked(CapabilityTransition),
    /// The capability was already irreversibly revoked by the returned original transition.
    AlreadyRevoked(CapabilityTransition),
    /// No target capability exists.
    CapabilityNotFound,
    /// An absence-authorized target appeared and requires fresh present-target policy.
    CapabilityPreparationChanged,
}

/// Capability-revoke outcome plus private original-sequence audit evidence.
pub struct CapabilityRevokeExecutionResult {
    outcome: CapabilityRevokeOutcome,
    transition_sequence: Option<AdministrationSequence>,
}

impl CapabilityRevokeExecutionResult {
    /// Borrows the safe semantic outcome.
    #[must_use]
    pub const fn outcome(&self) -> &CapabilityRevokeOutcome {
        &self.outcome
    }

    /// Returns the exact terminal phase/link classification when the invocation completes.
    ///
    /// `CapabilityPreparationChanged` is internal continuation state. The service must
    /// reauthorize and resubmit it under the existing start rather than append a terminal.
    #[must_use]
    pub fn terminal_audit(&self) -> Option<ControlPlaneTerminalAudit> {
        if self.outcome == CapabilityRevokeOutcome::CapabilityPreparationChanged {
            None
        } else {
            Some(terminal_audit(self.transition_sequence))
        }
    }

    /// Consumes the result after terminal audit or preparation-change handling.
    #[must_use]
    pub fn into_outcome(self) -> CapabilityRevokeOutcome {
        self.outcome
    }
}

impl fmt::Debug for CapabilityRevokeExecutionResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CapabilityRevokeExecutionResult([REDACTED])")
    }
}

/// Closed successful bootstrap transition shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapabilityBootstrapOutcome {
    /// The one-time bootstrap state was created.
    Created(CapabilityTransition),
    /// Exact credential retry recovered the original transition.
    Replayed(CapabilityTransition),
}

/// Move-only continuation authorizing exactly one principal-less succeeded append.
#[must_use = "a bootstrap terminal preparation must be submitted or explicitly discarded"]
pub struct CapabilityBootstrapTerminalPreparation {
    start: BootstrapServiceAuditStartV1,
    transition_sequence: AdministrationSequence,
}

impl fmt::Debug for CapabilityBootstrapTerminalPreparation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CapabilityBootstrapTerminalPreparation([REDACTED])")
    }
}

/// Successful bootstrap result paired with its mandatory terminal continuation.
pub struct CapabilityBootstrapCompletion {
    outcome: CapabilityBootstrapOutcome,
    terminal: CapabilityBootstrapTerminalPreparation,
}

impl CapabilityBootstrapCompletion {
    /// Borrows the safe successful result while the terminal append remains pending.
    #[must_use]
    pub const fn outcome(&self) -> &CapabilityBootstrapOutcome {
        &self.outcome
    }

    /// Consumes the completion into the result to hold and the continuation to submit.
    pub fn into_parts(
        self,
    ) -> (
        CapabilityBootstrapOutcome,
        CapabilityBootstrapTerminalPreparation,
    ) {
        (self.outcome, self.terminal)
    }
}

impl fmt::Debug for CapabilityBootstrapCompletion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CapabilityBootstrapCompletion([REDACTED])")
    }
}

/// Bootstrap either created/recovered authoritative state or rejected pre-start.
pub enum CapabilityBootstrapExecutionResult {
    /// A known transition requires the separate principal-less succeeded append.
    Completed(CapabilityBootstrapCompletion),
    /// Bootstrap identity or emptiness did not match; no start or transition was written.
    BootstrapConflict,
}

impl fmt::Debug for CapabilityBootstrapExecutionResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Completed(_) => "CapabilityBootstrapExecutionResult::Completed([REDACTED])",
            Self::BootstrapConflict => "CapabilityBootstrapExecutionResult::BootstrapConflict",
        })
    }
}

/// Redacted closed failure kind for accepted control-plane work.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ControlPlaneExecutionErrorKind {
    /// Transaction-current policy denied the capability transition.
    AuthorizationDenied,
    /// A clock or proven-not-committed storage dependency was unavailable.
    StorageUnavailable,
    /// Storage could not distinguish a committed transition from rollback.
    OutcomeUnknown,
    /// Checked internal state was contradictory or corrupt.
    InternalDefect,
    /// The coordinator stopped before this work could complete.
    CoordinatorStopped,
    /// An earlier uncertain authoritative write fenced this work.
    CoordinatorFenced,
}

impl ControlPlaneExecutionErrorKind {
    const fn safe_message(self) -> &'static str {
        match self {
            Self::AuthorizationDenied => "control-plane operation is not authorized",
            Self::StorageUnavailable => "control-plane storage is unavailable",
            Self::OutcomeUnknown => "control-plane outcome is not yet known",
            Self::InternalDefect => "control-plane execution encountered an internal defect",
            Self::CoordinatorStopped => "command coordinator stopped",
            Self::CoordinatorFenced => "command coordinator fenced authoritative writes",
        }
    }
}

/// Public-safe executor failure retaining only private trusted diagnostics.
pub struct ControlPlaneExecutionError {
    kind: ControlPlaneExecutionErrorKind,
    #[allow(dead_code)]
    detail: ControlPlaneExecutionErrorDetail,
}

#[allow(dead_code)]
enum ControlPlaneExecutionErrorDetail {
    None,
    Storage(StorageError),
    AdministrationClock(AdministrationClockError),
    AuthorizationClock(AuthorizationClockError),
    Authorization(PolicyCode),
    Catalog,
    InvalidSemanticValue(StorageValueError),
    CapabilityFacts,
    BootstrapPhaseConflict,
}

impl ControlPlaneExecutionError {
    /// Returns the complete public-safe failure classification.
    #[must_use]
    pub const fn kind(&self) -> ControlPlaneExecutionErrorKind {
        self.kind
    }

    pub(crate) const fn coordinator_stopped() -> Self {
        Self::without_detail(ControlPlaneExecutionErrorKind::CoordinatorStopped)
    }

    pub(crate) const fn coordinator_fenced() -> Self {
        Self::without_detail(ControlPlaneExecutionErrorKind::CoordinatorFenced)
    }

    const fn without_detail(kind: ControlPlaneExecutionErrorKind) -> Self {
        Self {
            kind,
            detail: ControlPlaneExecutionErrorDetail::None,
        }
    }

    const fn authorization(code: PolicyCode) -> Self {
        Self {
            kind: ControlPlaneExecutionErrorKind::AuthorizationDenied,
            detail: ControlPlaneExecutionErrorDetail::Authorization(code),
        }
    }
}

impl fmt::Debug for ControlPlaneExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlPlaneExecutionError")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for ControlPlaneExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.kind.safe_message())
    }
}

impl Error for ControlPlaneExecutionError {}

pub(crate) fn drive_catalog_deployment<R>(
    repository: &mut R,
    clock: &dyn AdministrationClock,
    lifecycle: &dyn CommandExecutionLifecycle,
    preparation: CatalogDeploymentPreparation,
) -> Result<CatalogDeploymentResult, ControlPlaneExecutionError>
where
    R: CatalogAdministrationRepository + ?Sized,
{
    let CatalogDeploymentPreparation {
        request_id,
        catalog,
        authorization,
    } = preparation;
    let timestamp = clock.now().map_err(|error| {
        lifecycle.stop();
        ControlPlaneExecutionError {
            kind: ControlPlaneExecutionErrorKind::StorageUnavailable,
            detail: ControlPlaneExecutionErrorDetail::AdministrationClock(error),
        }
    })?;
    let principal = AuditPrincipalV1::new(
        authorization.principal_id().clone(),
        authorization.actor_kind(),
        authorization.authorizing_capability_id(),
        authorization.authorizing_revision(),
    );
    let approval = authorization.obligations().validated_approval().cloned();
    let intent = catalog
        .into_storage_intent(request_id, principal, timestamp, approval)
        .map_err(|_| {
            lifecycle.stop();
            ControlPlaneExecutionError {
                kind: ControlPlaneExecutionErrorKind::InternalDefect,
                detail: ControlPlaneExecutionErrorDetail::Catalog,
            }
        })?;
    let result = repository
        .activate_catalog(&intent)
        .map_err(|error| classify_write_error(error, lifecycle))?;
    Ok(match result {
        CatalogActivationResult::Activated {
            active,
            administration_sequence,
        } => CatalogDeploymentResult {
            outcome: CatalogDeploymentOutcome::Activated(lower_active_catalog(active)),
            transition_sequence: Some(administration_sequence),
        },
        CatalogActivationResult::AlreadyActive {
            active,
            administration_sequence,
        } => CatalogDeploymentResult {
            outcome: CatalogDeploymentOutcome::AlreadyActive(lower_active_catalog(active)),
            transition_sequence: Some(administration_sequence),
        },
        CatalogActivationResult::ExpectedActiveVersionMismatch { actual } => {
            CatalogDeploymentResult {
                outcome: CatalogDeploymentOutcome::ExpectedActiveVersionMismatch { actual },
                transition_sequence: None,
            }
        }
        CatalogActivationResult::BundleConflict => CatalogDeploymentResult {
            outcome: CatalogDeploymentOutcome::BundleConflict,
            transition_sequence: None,
        },
    })
}

pub(crate) fn drive_query_module_deployment<R>(
    repository: &mut R,
    clock: &dyn AdministrationClock,
    lifecycle: &dyn CommandExecutionLifecycle,
    preparation: QueryModuleDeploymentPreparation,
) -> Result<QueryModuleDeploymentResult, ControlPlaneExecutionError>
where
    R: QueryModuleAdministrationRepository + ?Sized,
{
    let QueryModuleDeploymentPreparation {
        request_id,
        module,
        authorization,
    } = preparation;
    let timestamp = clock.now().map_err(|error| {
        lifecycle.stop();
        ControlPlaneExecutionError {
            kind: ControlPlaneExecutionErrorKind::StorageUnavailable,
            detail: ControlPlaneExecutionErrorDetail::AdministrationClock(error),
        }
    })?;
    let principal = AuditPrincipalV1::new(
        authorization.principal_id().clone(),
        authorization.actor_kind(),
        authorization.authorizing_capability_id(),
        authorization.authorizing_revision(),
    );
    let approval = authorization.obligations().validated_approval().cloned();
    let intent = module
        .into_storage_intent(request_id, principal, timestamp, approval)
        .map_err(|_| {
            lifecycle.stop();
            ControlPlaneExecutionError {
                kind: ControlPlaneExecutionErrorKind::InternalDefect,
                detail: ControlPlaneExecutionErrorDetail::Catalog,
            }
        })?;
    let result = repository
        .activate_query_module(&intent)
        .map_err(|error| classify_write_error(error, lifecycle))?;
    Ok(match result {
        QueryModuleActivationResult::Activated {
            active,
            administration_sequence,
        } => QueryModuleDeploymentResult {
            outcome: QueryModuleDeploymentOutcome::Activated(lower_active_module(&active)),
            transition_sequence: Some(administration_sequence),
        },
        QueryModuleActivationResult::AlreadyActive {
            active,
            administration_sequence,
        } => QueryModuleDeploymentResult {
            outcome: QueryModuleDeploymentOutcome::AlreadyActive(lower_active_module(&active)),
            transition_sequence: Some(administration_sequence),
        },
        QueryModuleActivationResult::ExpectedActiveMismatch { actual } => {
            QueryModuleDeploymentResult {
                outcome: QueryModuleDeploymentOutcome::ExpectedActiveMismatch { actual },
                transition_sequence: None,
            }
        }
        QueryModuleActivationResult::ModuleVersionConflict => QueryModuleDeploymentResult {
            outcome: QueryModuleDeploymentOutcome::ModuleVersionConflict,
            transition_sequence: None,
        },
        QueryModuleActivationResult::ContractUnavailable => QueryModuleDeploymentResult {
            outcome: QueryModuleDeploymentOutcome::ContractUnavailable,
            transition_sequence: None,
        },
    })
}

pub(crate) fn drive_reactive_module_publication<R>(
    repository: &mut R,
    clock: &dyn AdministrationClock,
    lifecycle: &dyn CommandExecutionLifecycle,
    preparation: ReactiveModulePublicationPreparation,
) -> Result<ReactiveModulePublicationExecutionResult, ControlPlaneExecutionError>
where
    R: ReactiveModuleAdministrationRepository + ?Sized,
{
    let ReactiveModulePublicationPreparation {
        request_id,
        module,
        authorization,
    } = preparation;
    let published = PublishedReactiveModule {
        name: module.module().plan().name().to_owned(),
        version: module.module().plan().version(),
        hash: module.module().identity(),
    };
    let timestamp = clock.now().map_err(|error| {
        lifecycle.stop();
        ControlPlaneExecutionError {
            kind: ControlPlaneExecutionErrorKind::StorageUnavailable,
            detail: ControlPlaneExecutionErrorDetail::AdministrationClock(error),
        }
    })?;
    let principal = AuditPrincipalV1::new(
        authorization.principal_id().clone(),
        authorization.actor_kind(),
        authorization.authorizing_capability_id(),
        authorization.authorizing_revision(),
    );
    let approval = authorization.obligations().validated_approval().cloned();
    let intent = module
        .into_storage_intent(request_id, principal, timestamp, approval)
        .map_err(|_| {
            lifecycle.stop();
            ControlPlaneExecutionError {
                kind: ControlPlaneExecutionErrorKind::InternalDefect,
                detail: ControlPlaneExecutionErrorDetail::Catalog,
            }
        })?;
    let result = repository
        .publish_reactive_module(&intent)
        .map_err(|error| classify_write_error(error, lifecycle))?;
    Ok(match result {
        ReactiveModulePublicationResult::Published {
            administration_sequence,
            ..
        } => ReactiveModulePublicationExecutionResult {
            outcome: ReactiveModulePublicationOutcome::Published(published),
            transition_sequence: Some(administration_sequence),
        },
        // Parity with the catalog and query-module already-active arms below: an
        // idempotent republish is a SUCCESS, so its terminal audit names the
        // original publication's transition instead of degrading to Failed.
        ReactiveModulePublicationResult::AlreadyPublished {
            administration_sequence,
            ..
        } => ReactiveModulePublicationExecutionResult {
            outcome: ReactiveModulePublicationOutcome::AlreadyPublished(published),
            transition_sequence: Some(administration_sequence),
        },
        ReactiveModulePublicationResult::ModuleVersionConflict => {
            ReactiveModulePublicationExecutionResult {
                outcome: ReactiveModulePublicationOutcome::ModuleVersionConflict,
                transition_sequence: None,
            }
        }
        ReactiveModulePublicationResult::ContractUnavailable => {
            ReactiveModulePublicationExecutionResult {
                outcome: ReactiveModulePublicationOutcome::ContractUnavailable,
                transition_sequence: None,
            }
        }
        ReactiveModulePublicationResult::QueryModuleUnavailable { module_hash } => {
            ReactiveModulePublicationExecutionResult {
                outcome: ReactiveModulePublicationOutcome::QueryModuleUnavailable(module_hash),
                transition_sequence: None,
            }
        }
    })
}

fn lower_active_module(
    active: &riffdb_storage_api::ActiveQueryModulePointerV1,
) -> ActivatedQueryModule {
    ActivatedQueryModule {
        name: active.module_name().clone(),
        version: active.module_version(),
        hash: active.module_hash(),
    }
}

pub(crate) fn drive_capability_create<R>(
    repository: &R,
    clock: &dyn AuthorizationClock,
    lifecycle: &dyn CommandExecutionLifecycle,
    preparation: CapabilityCreatePreparation,
) -> Result<CapabilityCreateExecutionResult, ControlPlaneExecutionError>
where
    R: CapabilityAdministrationTransactionPort + ?Sized,
{
    let CapabilityCreatePreparation {
        authorization,
        requested,
        token_digest,
    } = preparation;
    let target = authorization
        .create_target()
        .expect("capability-create preparation constructor fixes the operation")
        .clone();
    let principal = audit_principal(&authorization);
    let approval = authorization.validated_approval().cloned();
    let candidate = CapabilityCreateCandidateV1::new(
        target.capability_id(),
        target.request_id(),
        requested.clone(),
        token_digest,
        principal.clone(),
        approval.clone(),
    );
    let transaction = repository
        .begin_capability_create(candidate)
        .map_err(|error| classify_read_error(error, lifecycle))?;
    let (awaiting, current) = transaction
        .read_transaction_current()
        .map_err(|error| classify_read_error(error, lifecycle))?;
    let authorizing = match lower_current_authorizer(&current) {
        Ok(Some(current)) => current,
        Ok(None) => {
            let _candidate = awaiting.abandon();
            return Err(ControlPlaneExecutionError::authorization(
                PolicyCode::InactiveOrStaleCapability,
            ));
        }
        Err(error) => {
            let _candidate = awaiting.abandon();
            lifecycle.stop();
            return Err(error);
        }
    };
    let now = match clock.now() {
        Ok(now) => now,
        Err(error) => {
            let _candidate = awaiting.abandon();
            return Err(ControlPlaneExecutionError {
                kind: ControlPlaneExecutionErrorKind::InternalDefect,
                detail: ControlPlaneExecutionErrorDetail::AuthorizationClock(error),
            });
        }
    };
    let Some(expires_at) = checked_expiry(now, requested.duration_seconds()) else {
        let _candidate = awaiting.abandon();
        return Err(internal_defect(
            ControlPlaneExecutionErrorDetail::CapabilityFacts,
        ));
    };
    let proposed = ProposedCapabilityCreate::new(target, now, expires_at);
    let authorized = match TransactionCurrentCapabilityVerifier::verify_create(
        &authorizing,
        now,
        authorization,
        proposed,
    ) {
        TransactionCapabilityMutationDecision::Allow(authorized) => authorized,
        TransactionCapabilityMutationDecision::Deny(code) => {
            let _candidate = awaiting.abandon();
            return Err(ControlPlaneExecutionError::authorization(code));
        }
        TransactionCapabilityMutationDecision::PreparationChanged(_) => {
            let _candidate = awaiting.abandon();
            lifecycle.stop();
            return Err(internal_defect(
                ControlPlaneExecutionErrorDetail::CapabilityFacts,
            ));
        }
    };
    let (authorized_time, authorization, proposed) = authorized.into_parts();
    let ProposedCapabilityMutation::Create(proposed) = proposed else {
        let _candidate = awaiting.abandon();
        lifecycle.stop();
        return Err(internal_defect(
            ControlPlaneExecutionErrorDetail::CapabilityFacts,
        ));
    };
    let intent = match CapabilityCreateIntentV1::new(
        proposed.target().capability_id(),
        proposed.target().request_id(),
        requested,
        token_digest,
        authorized_time,
        proposed.expires_at(),
        audit_principal(&authorization),
        authorization.validated_approval().cloned(),
    ) {
        Ok(intent) => intent,
        Err(error) => {
            let _candidate = awaiting.abandon();
            lifecycle.stop();
            return Err(internal_defect(
                ControlPlaneExecutionErrorDetail::InvalidSemanticValue(error),
            ));
        }
    };
    let result = awaiting
        .commit_create(intent)
        .map_err(|error| classify_write_error(error, lifecycle))?;
    match result {
        CapabilityCreateResult::Created {
            capability_id,
            revision,
            administration_sequence,
        } => Ok(CapabilityCreateExecutionResult {
            outcome: CapabilityCreateOutcome::Created(CapabilityTransition {
                identity: CapabilityIdentity {
                    capability_id,
                    revision,
                },
                administration_sequence,
            }),
            transition_sequence: Some(administration_sequence),
        }),
        CapabilityCreateResult::AlreadyCreated {
            capability_id,
            revision,
            administration_sequence,
        } => Ok(CapabilityCreateExecutionResult {
            outcome: CapabilityCreateOutcome::AlreadyCreatedTokenUnavailable(CapabilityIdentity {
                capability_id,
                revision,
            }),
            transition_sequence: Some(administration_sequence),
        }),
        CapabilityCreateResult::CapabilityIdConflict => Ok(CapabilityCreateExecutionResult {
            outcome: CapabilityCreateOutcome::CapabilityIdConflict,
            transition_sequence: None,
        }),
        CapabilityCreateResult::TokenDigestCollision => Err(internal_defect(
            ControlPlaneExecutionErrorDetail::CapabilityFacts,
        )),
    }
}

pub(crate) fn drive_capability_revoke<R>(
    repository: &R,
    clock: &dyn AuthorizationClock,
    lifecycle: &dyn CommandExecutionLifecycle,
    preparation: CapabilityRevokePreparation,
) -> Result<CapabilityRevokeExecutionResult, ControlPlaneExecutionError>
where
    R: CapabilityAdministrationTransactionPort + ?Sized,
{
    let CapabilityRevokePreparation { authorization } = preparation;
    let target_id = authorization
        .revoke_target()
        .map(CapabilityRevokeTargetFacts::capability_id)
        .or_else(|| {
            authorization
                .absent_revoke_target()
                .map(|target| target.capability_id())
        })
        .expect("capability-revoke preparation constructor fixes the operation");
    let request_id = authorization
        .revoke_target()
        .map(CapabilityRevokeTargetFacts::request_id)
        .or_else(|| {
            authorization
                .absent_revoke_target()
                .map(|target| target.request_id())
        })
        .expect("capability-revoke preparation constructor fixes the request");
    let reason = authorization
        .revoke_reason()
        .expect("capability-revoke preparation constructor fixes the reason");
    let candidate = CapabilityRevokeCandidateV1::new(
        target_id,
        request_id,
        audit_principal(&authorization),
        authorization.validated_approval().cloned(),
        reason,
    );
    let transaction = repository
        .begin_capability_revoke(candidate)
        .map_err(|error| classify_read_error(error, lifecycle))?;
    let (awaiting, current) = transaction
        .read_transaction_current()
        .map_err(|error| classify_read_error(error, lifecycle))?;
    let authorizing = match lower_current_authorizer(&current) {
        Ok(Some(current)) => current,
        Ok(None) => {
            let _candidate = awaiting.abandon();
            return Err(ControlPlaneExecutionError::authorization(
                PolicyCode::InactiveOrStaleCapability,
            ));
        }
        Err(error) => {
            let _candidate = awaiting.abandon();
            lifecycle.stop();
            return Err(error);
        }
    };
    let now = match clock.now() {
        Ok(now) => now,
        Err(error) => {
            let _candidate = awaiting.abandon();
            return Err(ControlPlaneExecutionError {
                kind: ControlPlaneExecutionErrorKind::InternalDefect,
                detail: ControlPlaneExecutionErrorDetail::AuthorizationClock(error),
            });
        }
    };

    if authorization.absent_revoke_target().is_some() {
        let existence = if current.target().is_some() {
            TransactionCurrentCapabilityExistence::present(target_id)
        } else {
            TransactionCurrentCapabilityExistence::absent(target_id)
        };
        return match TransactionCurrentCapabilityVerifier::verify_absent_revoke(
            &authorizing,
            existence,
            now,
            authorization,
        ) {
            TransactionAbsentCapabilityRevokeDecision::Allow(_) => {
                let _candidate = awaiting.abandon();
                Ok(CapabilityRevokeExecutionResult {
                    outcome: CapabilityRevokeOutcome::CapabilityNotFound,
                    transition_sequence: None,
                })
            }
            TransactionAbsentCapabilityRevokeDecision::PreparationChanged(
                AbsentCapabilityRevokePreparationChange::TargetAppeared,
            ) => {
                let _candidate = awaiting.abandon();
                Ok(CapabilityRevokeExecutionResult {
                    outcome: CapabilityRevokeOutcome::CapabilityPreparationChanged,
                    transition_sequence: None,
                })
            }
            TransactionAbsentCapabilityRevokeDecision::Deny(code) => {
                let _candidate = awaiting.abandon();
                Err(ControlPlaneExecutionError::authorization(code))
            }
        };
    }

    let initial_target = authorization
        .revoke_target()
        .expect("present revoke preparation retains target")
        .clone();
    let Some(current_target) = current.target() else {
        let _candidate = awaiting.abandon();
        lifecycle.stop();
        return Err(internal_defect(
            ControlPlaneExecutionErrorDetail::CapabilityFacts,
        ));
    };
    let current_target = match lower_current_capability(current_target) {
        Ok(current_target) => current_target,
        Err(error) => {
            let _candidate = awaiting.abandon();
            lifecycle.stop();
            return Err(error);
        }
    };
    let proposed = ProposedCapabilityRevoke::new(initial_target, reason, now);
    let authorized = match TransactionCurrentCapabilityVerifier::verify_revoke(
        &authorizing,
        &current_target,
        now,
        authorization,
        proposed,
    ) {
        TransactionCapabilityMutationDecision::Allow(authorized) => authorized,
        TransactionCapabilityMutationDecision::Deny(code) => {
            let _candidate = awaiting.abandon();
            return Err(ControlPlaneExecutionError::authorization(code));
        }
        TransactionCapabilityMutationDecision::PreparationChanged(
            CapabilityRevokePreparationChange::TargetChanged,
        ) => {
            let _candidate = awaiting.abandon();
            return Ok(CapabilityRevokeExecutionResult {
                outcome: CapabilityRevokeOutcome::CapabilityPreparationChanged,
                transition_sequence: None,
            });
        }
    };
    let (authorized_time, authorization, proposed) = authorized.into_parts();
    let ProposedCapabilityMutation::Revoke(proposed) = proposed else {
        let _candidate = awaiting.abandon();
        lifecycle.stop();
        return Err(internal_defect(
            ControlPlaneExecutionErrorDetail::CapabilityFacts,
        ));
    };
    let intent = CapabilityRevokeIntentV1::new(
        proposed.target().capability_id(),
        proposed.target().revision(),
        proposed.target().request_id(),
        authorized_time,
        audit_principal(&authorization),
        authorization.validated_approval().cloned(),
        proposed.reason(),
    );
    let result = awaiting
        .commit_revoke(intent)
        .map_err(|error| classify_write_error(error, lifecycle))?;
    match result {
        CapabilityRevokeResult::Revoked {
            capability_id,
            revision,
            administration_sequence,
        } => Ok(CapabilityRevokeExecutionResult {
            outcome: CapabilityRevokeOutcome::Revoked(CapabilityTransition {
                identity: CapabilityIdentity {
                    capability_id,
                    revision,
                },
                administration_sequence,
            }),
            transition_sequence: Some(administration_sequence),
        }),
        CapabilityRevokeResult::AlreadyRevoked {
            capability_id,
            revision,
            administration_sequence,
        } => Ok(CapabilityRevokeExecutionResult {
            outcome: CapabilityRevokeOutcome::AlreadyRevoked(CapabilityTransition {
                identity: CapabilityIdentity {
                    capability_id,
                    revision,
                },
                administration_sequence,
            }),
            transition_sequence: Some(administration_sequence),
        }),
        CapabilityRevokeResult::CapabilityNotFound => {
            lifecycle.stop();
            Err(internal_defect(
                ControlPlaneExecutionErrorDetail::CapabilityFacts,
            ))
        }
    }
}

pub(crate) fn drive_capability_bootstrap<R>(
    repository: &mut R,
    clock: &dyn AdministrationClock,
    lifecycle: &dyn CommandExecutionLifecycle,
    preparation: CapabilityBootstrapPreparation,
) -> Result<CapabilityBootstrapExecutionResult, ControlPlaneExecutionError>
where
    R: CapabilityBootstrapAdministrationRepository + ?Sized,
{
    let CapabilityBootstrapPreparation {
        capability_id,
        requested,
        digests,
        request_id,
        ingress,
        targets,
        proof,
    } = preparation;
    let _consumed_compound_authority = proof;
    let timestamp = clock.now().map_err(|error| {
        lifecycle.stop();
        ControlPlaneExecutionError {
            kind: ControlPlaneExecutionErrorKind::StorageUnavailable,
            detail: ControlPlaneExecutionErrorDetail::AdministrationClock(error),
        }
    })?;
    let expires_at = checked_expiry(timestamp, requested.duration_seconds()).ok_or_else(|| {
        lifecycle.stop();
        ControlPlaneExecutionError {
            kind: ControlPlaneExecutionErrorKind::StorageUnavailable,
            detail: ControlPlaneExecutionErrorDetail::CapabilityFacts,
        }
    })?;
    let start = BootstrapServiceAuditStartV1::new(request_id, timestamp, ingress, targets, None)
        .map_err(|error| {
            lifecycle.stop();
            internal_defect(ControlPlaneExecutionErrorDetail::InvalidSemanticValue(
                error,
            ))
        })?;
    let intent = CapabilityBootstrapIntentV1::new(
        capability_id,
        requested,
        digests,
        timestamp,
        expires_at,
        start.clone(),
    )
    .map_err(|error| {
        lifecycle.stop();
        internal_defect(ControlPlaneExecutionErrorDetail::InvalidSemanticValue(
            error,
        ))
    })?;
    let result = repository
        .bootstrap_capability(&intent)
        .map_err(|error| classify_required_audit_write_error(error, lifecycle))?;
    Ok(match result {
        CapabilityBootstrapResult::BootstrapCreated {
            capability_id,
            revision,
            administration_sequence,
            invocation_started_sequence: _,
        } => CapabilityBootstrapExecutionResult::Completed(CapabilityBootstrapCompletion {
            outcome: CapabilityBootstrapOutcome::Created(CapabilityTransition {
                identity: CapabilityIdentity {
                    capability_id,
                    revision,
                },
                administration_sequence,
            }),
            terminal: CapabilityBootstrapTerminalPreparation {
                start,
                transition_sequence: administration_sequence,
            },
        }),
        CapabilityBootstrapResult::BootstrapReplayed {
            capability_id,
            revision,
            administration_sequence,
            invocation_started_sequence: _,
        } => CapabilityBootstrapExecutionResult::Completed(CapabilityBootstrapCompletion {
            outcome: CapabilityBootstrapOutcome::Replayed(CapabilityTransition {
                identity: CapabilityIdentity {
                    capability_id,
                    revision,
                },
                administration_sequence,
            }),
            terminal: CapabilityBootstrapTerminalPreparation {
                start,
                transition_sequence: administration_sequence,
            },
        }),
        CapabilityBootstrapResult::BootstrapConflict => {
            CapabilityBootstrapExecutionResult::BootstrapConflict
        }
    })
}

pub(crate) fn drive_capability_bootstrap_terminal<R>(
    repository: &mut R,
    clock: &dyn AdministrationClock,
    lifecycle: &dyn CommandExecutionLifecycle,
    preparation: CapabilityBootstrapTerminalPreparation,
) -> Result<(), ControlPlaneExecutionError>
where
    R: ServiceAuditAppendRepository + ?Sized,
{
    let timestamp = clock.now().map_err(|error| {
        lifecycle.stop();
        ControlPlaneExecutionError {
            kind: ControlPlaneExecutionErrorKind::StorageUnavailable,
            detail: ControlPlaneExecutionErrorDetail::AdministrationClock(error),
        }
    })?;
    let intent = ServiceAuditAppendIntentV1::for_bootstrap_succeeded(
        &preparation.start,
        timestamp,
        preparation.transition_sequence,
    )
    .map_err(|error| {
        lifecycle.stop();
        internal_defect(ControlPlaneExecutionErrorDetail::InvalidSemanticValue(
            error,
        ))
    })?;
    match repository
        .append_service_audit(&intent)
        .map_err(|error| classify_required_audit_write_error(error, lifecycle))?
    {
        ServiceAuditAppendResult::Appended(_) => Ok(()),
        ServiceAuditAppendResult::PhaseConflict => {
            lifecycle.stop();
            Err(internal_defect(
                ControlPlaneExecutionErrorDetail::BootstrapPhaseConflict,
            ))
        }
    }
}

fn lower_requested_record(
    requested: &NormalizedCapabilityCreateRecord,
) -> Result<CapabilityRequestedRecordV1, StorageValueError> {
    CapabilityRequestedRecordV1::new(
        requested.database_id(),
        requested.environment().clone(),
        requested.principal_id().clone(),
        requested.actor_kind(),
        requested.requested_lifetime_seconds(),
        requested.audiences().to_vec(),
        requested.grant().clone(),
    )
}

fn audit_principal(authorization: &AuthorizedCapabilityMutationPreparation) -> AuditPrincipalV1 {
    AuditPrincipalV1::new(
        authorization.authorizing_principal_id().clone(),
        authorization.authorizing_actor_kind(),
        authorization.authorizing_capability_id(),
        authorization.authorizing_revision(),
    )
}

fn lower_current_authorizer(
    current: &CapabilityMutationCurrentStateV1,
) -> Result<Option<TransactionCurrentCapabilityFacts>, ControlPlaneExecutionError> {
    current
        .authorizing()
        .map(lower_current_capability)
        .transpose()
}

fn lower_current_capability(
    current: &TransactionCurrentCapabilityObservationV1,
) -> Result<TransactionCurrentCapabilityFacts, ControlPlaneExecutionError> {
    let activity = match current.lifecycle() {
        CapabilityLifecycleV1::Active => CapabilityActivity::Active,
        CapabilityLifecycleV1::Revoked { .. } => CapabilityActivity::Revoked,
    };
    TransactionCurrentCapabilityFacts::new(
        current.capability_id(),
        current.revision(),
        activity,
        current.database_id(),
        current.environment().clone(),
        current.principal_id().clone(),
        current.actor_kind(),
        current.audiences().to_vec(),
        current.issued_at(),
        current.expires_at(),
        current.grant().clone(),
    )
    .map_err(|_| internal_defect(ControlPlaneExecutionErrorDetail::CapabilityFacts))
}

fn checked_expiry(issued_at: Timestamp, duration: NonZeroU32) -> Option<Timestamp> {
    let seconds = issued_at.seconds().checked_add(i64::from(duration.get()))?;
    Timestamp::new(seconds, issued_at.nanoseconds()).ok()
}

fn lower_active_catalog(active: ActiveCatalogPointerV1) -> ActivatedCatalog {
    ActivatedCatalog {
        lineage: active.lineage().clone(),
        version: active.contract_version(),
        bundle_hash: active.bundle_hash(),
    }
}

fn terminal_audit(
    transition_sequence: Option<AdministrationSequence>,
) -> ControlPlaneTerminalAudit {
    match transition_sequence {
        Some(administration_sequence) => {
            ControlPlaneTerminalAudit::Succeeded(ServiceAuditLinkV1::ControlPlane {
                administration_sequence,
            })
        }
        None => ControlPlaneTerminalAudit::Failed,
    }
}

fn classify_read_error(
    error: StorageError,
    lifecycle: &dyn CommandExecutionLifecycle,
) -> ControlPlaneExecutionError {
    if error.kind() == StorageErrorKind::Unavailable {
        ControlPlaneExecutionError {
            kind: ControlPlaneExecutionErrorKind::StorageUnavailable,
            detail: ControlPlaneExecutionErrorDetail::Storage(error),
        }
    } else {
        lifecycle.stop();
        ControlPlaneExecutionError {
            kind: ControlPlaneExecutionErrorKind::InternalDefect,
            detail: ControlPlaneExecutionErrorDetail::Storage(error),
        }
    }
}

fn classify_write_error(
    error: StorageError,
    lifecycle: &dyn CommandExecutionLifecycle,
) -> ControlPlaneExecutionError {
    match error.kind() {
        StorageErrorKind::Unavailable => ControlPlaneExecutionError {
            kind: ControlPlaneExecutionErrorKind::StorageUnavailable,
            detail: ControlPlaneExecutionErrorDetail::Storage(error),
        },
        StorageErrorKind::CommitStatusUnknown => {
            lifecycle.fence();
            ControlPlaneExecutionError {
                kind: ControlPlaneExecutionErrorKind::OutcomeUnknown,
                detail: ControlPlaneExecutionErrorDetail::Storage(error),
            }
        }
        StorageErrorKind::CorruptData
        | StorageErrorKind::IncompatibleFormat
        | StorageErrorKind::LimitExceeded
        | StorageErrorKind::InvariantViolation
        | StorageErrorKind::SequenceExhausted
        | StorageErrorKind::HistoryPruned => {
            lifecycle.stop();
            ControlPlaneExecutionError {
                kind: ControlPlaneExecutionErrorKind::InternalDefect,
                detail: ControlPlaneExecutionErrorDetail::Storage(error),
            }
        }
    }
}

fn classify_required_audit_write_error(
    error: StorageError,
    lifecycle: &dyn CommandExecutionLifecycle,
) -> ControlPlaneExecutionError {
    if error.kind() == StorageErrorKind::Unavailable {
        lifecycle.stop();
        ControlPlaneExecutionError {
            kind: ControlPlaneExecutionErrorKind::StorageUnavailable,
            detail: ControlPlaneExecutionErrorDetail::Storage(error),
        }
    } else {
        classify_write_error(error, lifecycle)
    }
}

const fn internal_defect(detail: ControlPlaneExecutionErrorDetail) -> ControlPlaneExecutionError {
    ControlPlaneExecutionError {
        kind: ControlPlaneExecutionErrorKind::InternalDefect,
        detail,
    }
}
