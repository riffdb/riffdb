//! Operator-only application reimport service contracts.

use std::fmt;
use std::num::NonZeroU64;
use std::sync::Arc;

use riffdb_application::{
    ApplicationPortabilityManifest, ApplicationReimportCampaignPhaseV1,
    ApplicationReimportCampaignV1, ApplicationReimportReceipt, ReimportObservationResult,
    ReimportPageMappingOutcomeV1,
};
use riffdb_policy::AuthorizedApplicationReimportV1;
use riffdb_types::{
    ApplicationExportPageHash, ApplicationInstallationCampaignId,
    ApplicationPortabilityManifestHash, CapabilityApplicationReimportScopeV1, ContractLineage,
    RequestId, ServiceIngressKindV1,
};

use crate::{
    ApplicationExportPageV1, BoxPortCapacityPermit, CanonicalApplicationExportJsonDocument,
    PortAdmissionError, PortFuture, RequestControl, ServiceDtoError, ServiceFuture,
};

/// Exact validated source and destination intent for one campaign start.
#[derive(Clone)]
pub struct StartApplicationReimportRequestV1 {
    campaign_id: ApplicationInstallationCampaignId,
    lineage: ContractLineage,
    scope: CapabilityApplicationReimportScopeV1,
    portability_manifest: Arc<ApplicationPortabilityManifest>,
    export_manifest: CanonicalApplicationExportJsonDocument,
    export_receipt: CanonicalApplicationExportJsonDocument,
}

impl StartApplicationReimportRequestV1 {
    /// Binds a caller-stable installation campaign to one completed source.
    pub fn new(
        campaign_id: ApplicationInstallationCampaignId,
        lineage: ContractLineage,
        scope: CapabilityApplicationReimportScopeV1,
        portability_manifest: ApplicationPortabilityManifest,
        export_manifest: CanonicalApplicationExportJsonDocument,
        export_receipt: CanonicalApplicationExportJsonDocument,
    ) -> Result<Self, ServiceDtoError> {
        if portability_manifest.input().contract_lineage != lineage {
            return Err(ServiceDtoError::InvalidShape);
        }
        Ok(Self {
            campaign_id,
            lineage,
            scope,
            portability_manifest: Arc::new(portability_manifest),
            export_manifest,
            export_receipt,
        })
    }

    /// Caller-stable installation and reimport identity.
    #[must_use]
    pub const fn campaign_id(&self) -> ApplicationInstallationCampaignId {
        self.campaign_id
    }

    /// Exact protected application lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Exact V7 grant scope required at every safe point.
    #[must_use]
    pub const fn scope(&self) -> CapabilityApplicationReimportScopeV1 {
        self.scope
    }

    /// Exact adapter-owned mapping and observation contract.
    #[must_use]
    pub fn portability_manifest(&self) -> &ApplicationPortabilityManifest {
        self.portability_manifest.as_ref()
    }

    /// Exact completed portability-export manifest document.
    #[must_use]
    pub const fn export_manifest(&self) -> &CanonicalApplicationExportJsonDocument {
        &self.export_manifest
    }

    /// Exact completed portability-export receipt document.
    #[must_use]
    pub const fn export_receipt(&self) -> &CanonicalApplicationExportJsonDocument {
        &self.export_receipt
    }

    /// Consumes the bounded request without cloning the manifest.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        ApplicationInstallationCampaignId,
        ContractLineage,
        CapabilityApplicationReimportScopeV1,
        Arc<ApplicationPortabilityManifest>,
        CanonicalApplicationExportJsonDocument,
        CanonicalApplicationExportJsonDocument,
    ) {
        (
            self.campaign_id,
            self.lineage,
            self.scope,
            self.portability_manifest,
            self.export_manifest,
            self.export_receipt,
        )
    }
}

impl fmt::Debug for StartApplicationReimportRequestV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StartApplicationReimportRequestV1([REDACTED])")
    }
}

/// One exact export page submitted to the sole expected campaign frontier.
#[derive(Clone)]
pub struct ApplyApplicationReimportPageRequestV1 {
    campaign_id: ApplicationInstallationCampaignId,
    page: ApplicationExportPageV1,
}

impl ApplyApplicationReimportPageRequestV1 {
    /// Constructs one page request; page order and hash remain server-checked.
    #[must_use]
    pub const fn new(
        campaign_id: ApplicationInstallationCampaignId,
        page: ApplicationExportPageV1,
    ) -> Self {
        Self { campaign_id, page }
    }

    /// Exact reimport campaign identity.
    #[must_use]
    pub const fn campaign_id(&self) -> ApplicationInstallationCampaignId {
        self.campaign_id
    }

    /// Canonical hash-bearing export page.
    #[must_use]
    pub const fn page(&self) -> &ApplicationExportPageV1 {
        &self.page
    }

    /// Consumes the page for move-only coordinator admission.
    #[must_use]
    pub fn into_parts(self) -> (ApplicationInstallationCampaignId, ApplicationExportPageV1) {
        (self.campaign_id, self.page)
    }
}

impl fmt::Debug for ApplyApplicationReimportPageRequestV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ApplyApplicationReimportPageRequestV1([REDACTED])")
    }
}

/// Exact selector shared by protected status and cancellation operations.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ApplicationReimportOperationRequestV1 {
    campaign_id: ApplicationInstallationCampaignId,
}

impl ApplicationReimportOperationRequestV1 {
    /// Constructs one caller-stable lookup.
    #[must_use]
    pub const fn new(campaign_id: ApplicationInstallationCampaignId) -> Self {
        Self { campaign_id }
    }

    /// Exact reimport campaign identity.
    #[must_use]
    pub const fn campaign_id(self) -> ApplicationInstallationCampaignId {
        self.campaign_id
    }
}

impl fmt::Debug for ApplicationReimportOperationRequestV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ApplicationReimportOperationRequestV1([REDACTED])")
    }
}

/// Protected campaign observation and optional terminal reconciliation receipt.
#[derive(Clone, Eq, PartialEq)]
pub struct ApplicationReimportOperationResultV1 {
    campaign_id: ApplicationInstallationCampaignId,
    lineage: ContractLineage,
    campaign: ApplicationReimportCampaignV1,
    receipt: Option<ApplicationReimportReceipt>,
}

impl ApplicationReimportOperationResultV1 {
    /// Checks that a receipt exists if and only if reconciliation succeeded.
    pub fn new(
        campaign_id: ApplicationInstallationCampaignId,
        lineage: ContractLineage,
        campaign: ApplicationReimportCampaignV1,
        receipt: Option<ApplicationReimportReceipt>,
    ) -> Result<Self, ServiceDtoError> {
        let reconciled = campaign.phase() == ApplicationReimportCampaignPhaseV1::Reconciled;
        if reconciled != receipt.is_some()
            || receipt
                .as_ref()
                .is_some_and(|receipt| campaign.receipt_hash() != Some(receipt.identity()))
        {
            return Err(ServiceDtoError::InvalidShape);
        }
        Ok(Self {
            campaign_id,
            lineage,
            campaign,
            receipt,
        })
    }

    /// Caller-stable installation and reimport identity.
    #[must_use]
    pub const fn campaign_id(&self) -> ApplicationInstallationCampaignId {
        self.campaign_id
    }

    /// Protected exact application lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Current redacted, bounded durable progress.
    #[must_use]
    pub const fn campaign(&self) -> &ApplicationReimportCampaignV1 {
        &self.campaign
    }

    /// Terminal reconciliation receipt, present only after success.
    #[must_use]
    pub const fn receipt(&self) -> Option<&ApplicationReimportReceipt> {
        self.receipt.as_ref()
    }
}

impl fmt::Debug for ApplicationReimportOperationResultV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ApplicationReimportOperationResultV1([REDACTED])")
    }
}

/// Protected lookup result which does not reveal absent campaign identities.
#[derive(Clone, Eq, PartialEq)]
pub enum GetApplicationReimportResultV1 {
    /// No retained campaign exists for the exact authorized identity.
    NotFound,
    /// Current-policy-authorized progress.
    Found(Box<ApplicationReimportOperationResultV1>),
}

/// Minimal protected identity resolved before current safe-point authorization.
#[derive(Clone, Eq, PartialEq)]
pub struct ApplicationReimportPolicyBindingV1 {
    lineage: ContractLineage,
    portability_manifest_hash: ApplicationPortabilityManifestHash,
    scope: CapabilityApplicationReimportScopeV1,
}

/// Exact durable page state prepared before any compiler-owned command runs.
#[derive(Clone)]
pub struct ApplicationReimportPagePreparationV1 {
    lineage: ContractLineage,
    portability_manifest: Arc<ApplicationPortabilityManifest>,
    phase: ApplicationReimportCampaignPhaseV1,
    expected_page: NonZeroU64,
    expected_hash: ApplicationExportPageHash,
}

impl ApplicationReimportPagePreparationV1 {
    /// Constructs one source-bound preparation from retained campaign state.
    pub fn new(
        lineage: ContractLineage,
        portability_manifest: ApplicationPortabilityManifest,
        phase: ApplicationReimportCampaignPhaseV1,
        expected_page: NonZeroU64,
        expected_hash: ApplicationExportPageHash,
    ) -> Self {
        Self {
            lineage,
            portability_manifest: Arc::new(portability_manifest),
            phase,
            expected_page,
            expected_hash,
        }
    }

    /// Exact active lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Exact compiler-owned portability manifest retained by the campaign.
    #[must_use]
    pub fn portability_manifest(&self) -> &ApplicationPortabilityManifest {
        self.portability_manifest.as_ref()
    }

    /// Exact durable phase observed with the retained manifest and final page.
    #[must_use]
    pub const fn phase(&self) -> ApplicationReimportCampaignPhaseV1 {
        self.phase
    }

    /// Exact next page number.
    #[must_use]
    pub const fn expected_page(&self) -> NonZeroU64 {
        self.expected_page
    }

    /// Exact next source page identity.
    #[must_use]
    pub const fn expected_hash(&self) -> ApplicationExportPageHash {
        self.expected_hash
    }
}

impl fmt::Debug for ApplicationReimportPagePreparationV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ApplicationReimportPagePreparationV1([EXACT_SOURCE])")
    }
}

impl ApplicationReimportPolicyBindingV1 {
    /// Retains the exact policy tuple from durable campaign state.
    #[must_use]
    pub const fn new(
        lineage: ContractLineage,
        portability_manifest_hash: ApplicationPortabilityManifestHash,
        scope: CapabilityApplicationReimportScopeV1,
    ) -> Self {
        Self {
            lineage,
            portability_manifest_hash,
            scope,
        }
    }

    /// Exact protected lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Exact adapter manifest identity.
    #[must_use]
    pub const fn portability_manifest_hash(&self) -> ApplicationPortabilityManifestHash {
        self.portability_manifest_hash
    }

    /// Exact V7 scope.
    #[must_use]
    pub const fn scope(&self) -> CapabilityApplicationReimportScopeV1 {
        self.scope
    }
}

/// Move-only authorized campaign start.
pub struct AuthorizedApplicationReimportStartV1 {
    request_id: RequestId,
    ingress: ServiceIngressKindV1,
    request: StartApplicationReimportRequestV1,
    authorization: Box<AuthorizedApplicationReimportV1>,
}

impl AuthorizedApplicationReimportStartV1 {
    pub(crate) const fn new(
        request_id: RequestId,
        ingress: ServiceIngressKindV1,
        request: StartApplicationReimportRequestV1,
        authorization: Box<AuthorizedApplicationReimportV1>,
    ) -> Self {
        Self {
            request_id,
            ingress,
            request,
            authorization,
        }
    }

    /// Separates the exact request from its current move-only proof.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        RequestId,
        ServiceIngressKindV1,
        StartApplicationReimportRequestV1,
        Box<AuthorizedApplicationReimportV1>,
    ) {
        (
            self.request_id,
            self.ingress,
            self.request,
            self.authorization,
        )
    }
}

/// Move-only authorized page application.
pub struct AuthorizedApplicationReimportPageV1 {
    request: ApplyApplicationReimportPageRequestV1,
    authorization: Box<AuthorizedApplicationReimportV1>,
    outcomes: Vec<ReimportPageMappingOutcomeV1>,
}

impl AuthorizedApplicationReimportPageV1 {
    pub(crate) const fn new(
        request: ApplyApplicationReimportPageRequestV1,
        authorization: Box<AuthorizedApplicationReimportV1>,
        outcomes: Vec<ReimportPageMappingOutcomeV1>,
    ) -> Self {
        Self {
            request,
            authorization,
            outcomes,
        }
    }

    /// Separates the exact page from its current move-only proof.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        ApplyApplicationReimportPageRequestV1,
        Box<AuthorizedApplicationReimportV1>,
        Vec<ReimportPageMappingOutcomeV1>,
    ) {
        (self.request, self.authorization, self.outcomes)
    }
}

/// Move-only authorized status or cancellation operation.
pub struct AuthorizedApplicationReimportOperationV1 {
    request: ApplicationReimportOperationRequestV1,
    authorization: Box<AuthorizedApplicationReimportV1>,
}

/// Move-only terminal reconciliation mutation with compiler-produced observations.
pub struct AuthorizedApplicationReimportReconcileV1 {
    campaign_id: ApplicationInstallationCampaignId,
    authorization: Box<AuthorizedApplicationReimportV1>,
    observations: Vec<ReimportObservationResult>,
}

impl AuthorizedApplicationReimportReconcileV1 {
    pub(crate) const fn new(
        campaign_id: ApplicationInstallationCampaignId,
        authorization: Box<AuthorizedApplicationReimportV1>,
        observations: Vec<ReimportObservationResult>,
    ) -> Self {
        Self {
            campaign_id,
            authorization,
            observations,
        }
    }

    /// Separates compiler-produced evidence from the final current V7 proof.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        ApplicationInstallationCampaignId,
        Box<AuthorizedApplicationReimportV1>,
        Vec<ReimportObservationResult>,
    ) {
        (self.campaign_id, self.authorization, self.observations)
    }
}

impl AuthorizedApplicationReimportOperationV1 {
    pub(crate) const fn new(
        request: ApplicationReimportOperationRequestV1,
        authorization: Box<AuthorizedApplicationReimportV1>,
    ) -> Self {
        Self {
            request,
            authorization,
        }
    }

    /// Separates the exact selector from its current move-only proof.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        ApplicationReimportOperationRequestV1,
        Box<AuthorizedApplicationReimportV1>,
    ) {
        (self.request, self.authorization)
    }
}

/// Closed mutation-side failures before public redaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationReimportMutationPortErrorV1 {
    /// Durable application or export identity disagreed with the request.
    IdentityMismatch,
    /// Source page, count, mapping, or workflow proof was invalid.
    SourceMismatch,
    /// Current authority changed at a safe point.
    AuthorityChanged,
    /// Campaign is terminal or at a different stage.
    InvalidPhase,
    /// A compiled command selected a terminal failure.
    CommandFailed,
    /// Storage failed before a safe result was known.
    StorageUnavailable,
    /// Internal durable-state validation failed.
    Integrity,
}

/// Closed observation-side failures before public redaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationReimportObservationPortErrorV1 {
    /// Storage failed before a protected observation was known.
    StorageUnavailable,
    /// Internal durable-state validation failed.
    Integrity,
}

/// Reserved start capacity.
pub type ApplicationReimportStartPermitV1 = BoxPortCapacityPermit<
    AuthorizedApplicationReimportStartV1,
    ApplicationReimportOperationResultV1,
    ApplicationReimportMutationPortErrorV1,
>;
/// Reserved page-application capacity.
pub type ApplicationReimportPagePermitV1 = BoxPortCapacityPermit<
    AuthorizedApplicationReimportPageV1,
    ApplicationReimportOperationResultV1,
    ApplicationReimportMutationPortErrorV1,
>;
/// Reserved terminal reconciliation capacity.
pub type ApplicationReimportReconcilePermitV1 = BoxPortCapacityPermit<
    AuthorizedApplicationReimportReconcileV1,
    ApplicationReimportOperationResultV1,
    ApplicationReimportMutationPortErrorV1,
>;
/// Reserved protected-observation capacity.
pub type ApplicationReimportObservationPermitV1 = BoxPortCapacityPermit<
    AuthorizedApplicationReimportOperationV1,
    Option<ApplicationReimportOperationResultV1>,
    ApplicationReimportObservationPortErrorV1,
>;
/// Reserved cancellation capacity.
pub type ApplicationReimportCancelPermitV1 = BoxPortCapacityPermit<
    AuthorizedApplicationReimportOperationV1,
    Option<ApplicationReimportOperationResultV1>,
    ApplicationReimportMutationPortErrorV1,
>;

/// Bounded coordinator ports implemented by storage-backed service adapters.
pub trait ApplicationReimportCoordinatorPort: Send + Sync {
    /// Resolves only the minimum protected tuple needed for current policy.
    fn resolve_application_reimport_binding(
        &self,
        campaign_id: ApplicationInstallationCampaignId,
        control: &RequestControl,
    ) -> PortFuture<
        '_,
        Option<ApplicationReimportPolicyBindingV1>,
        ApplicationReimportObservationPortErrorV1,
    >;

    /// Loads the exact retained manifest and next-page identity after begin authorization.
    fn prepare_application_reimport_page(
        &self,
        campaign_id: ApplicationInstallationCampaignId,
        control: &RequestControl,
    ) -> PortFuture<
        '_,
        Option<ApplicationReimportPagePreparationV1>,
        ApplicationReimportObservationPortErrorV1,
    >;

    /// Reserves capacity before final start authorization.
    fn reserve_application_reimport_start(
        &self,
        control: &RequestControl,
    ) -> PortFuture<'_, ApplicationReimportStartPermitV1, PortAdmissionError>;

    /// Reserves capacity before final page authorization.
    fn reserve_application_reimport_page(
        &self,
        control: &RequestControl,
    ) -> PortFuture<'_, ApplicationReimportPagePermitV1, PortAdmissionError>;

    /// Reserves capacity before the final current-authority reconciliation seal.
    fn reserve_application_reimport_reconcile(
        &self,
        control: &RequestControl,
    ) -> PortFuture<'_, ApplicationReimportReconcilePermitV1, PortAdmissionError>;

    /// Reserves capacity before a protected status observation.
    fn reserve_application_reimport_observation(
        &self,
        control: &RequestControl,
    ) -> PortFuture<'_, ApplicationReimportObservationPermitV1, PortAdmissionError>;

    /// Reserves capacity before an authorized cancellation transition.
    fn reserve_application_reimport_cancel(
        &self,
        control: &RequestControl,
    ) -> PortFuture<'_, ApplicationReimportCancelPermitV1, PortAdmissionError>;
}

/// Operator-only application-reimport surface.
pub trait ApplicationReimportApplication: Send + Sync {
    /// Starts or exactly resumes one immutable source-bound campaign.
    fn start_application_reimport(
        &self,
        context: crate::RequestContext,
        request_id: RequestId,
        request: StartApplicationReimportRequestV1,
    ) -> ServiceFuture<'_, ApplicationReimportOperationResultV1>;

    /// Applies one exact bounded page through compiler-owned commands.
    fn apply_application_reimport_page(
        &self,
        context: crate::RequestContext,
        request_id: RequestId,
        request: ApplyApplicationReimportPageRequestV1,
    ) -> ServiceFuture<'_, ApplicationReimportOperationResultV1>;

    /// Observes one protected durable checkpoint or terminal receipt.
    fn get_application_reimport(
        &self,
        context: crate::RequestContext,
        request_id: RequestId,
        request: ApplicationReimportOperationRequestV1,
    ) -> ServiceFuture<'_, GetApplicationReimportResultV1>;

    /// Cancels one nonterminal campaign without publishing readiness.
    fn cancel_application_reimport(
        &self,
        context: crate::RequestContext,
        request_id: RequestId,
        request: ApplicationReimportOperationRequestV1,
    ) -> ServiceFuture<'_, GetApplicationReimportResultV1>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn start_request_binds_lineage_and_manifest_without_values_in_debug() {
        let manifest = ApplicationPortabilityManifest::decode_canonical(include_bytes!(
            "../../../fixtures/export/openfga/portability-manifest-v2.json"
        ))
        .expect("manifest");
        let request = StartApplicationReimportRequestV1::new(
            ApplicationInstallationCampaignId::from_unix_milliseconds_and_random(3, [6; 10])
                .expect("campaign"),
            manifest.input().contract_lineage.clone(),
            CapabilityApplicationReimportScopeV1::WholeApplication,
            manifest,
            CanonicalApplicationExportJsonDocument::new(br#"{"manifest":true}"#.to_vec())
                .expect("export manifest"),
            CanonicalApplicationExportJsonDocument::new(br#"{"receipt":true}"#.to_vec())
                .expect("export receipt"),
        )
        .expect("request");
        assert_eq!(
            format!("{request:?}"),
            "StartApplicationReimportRequestV1([REDACTED])"
        );
    }
}
