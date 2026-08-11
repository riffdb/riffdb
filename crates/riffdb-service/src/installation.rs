//! API-neutral exact application installation campaign contracts.

use std::fmt;
use std::sync::Arc;

use riffdb_application::{
    ApplicationInstallationObservation, ApplicationInstallationPlan,
    ApplicationInstallationReceipt, InstallationCampaignPhase, InstallationStageEvidence,
};
use riffdb_policy::AuthorizedApplicationInstallation;
use riffdb_types::{
    ApplicationInstallationCampaignId, ContractLineage, RequestId, ServiceIngressKindV1,
};

use crate::{
    BoxPortCapacityPermit, PortAdmissionError, PortFuture, RequestControl, ServiceDtoError,
};

/// Checked start-or-resume input for one caller-stable campaign.
#[derive(Clone)]
pub struct StartApplicationInstallationRequest {
    campaign_id: ApplicationInstallationCampaignId,
    plan: Arc<ApplicationInstallationPlan>,
    external_completion: Option<InstallationStageEvidence>,
}

impl StartApplicationInstallationRequest {
    /// Binds one caller identity to one immutable canonical plan.
    #[must_use]
    pub fn new(
        campaign_id: ApplicationInstallationCampaignId,
        plan: ApplicationInstallationPlan,
    ) -> Self {
        Self {
            campaign_id,
            plan: Arc::new(plan),
            external_completion: None,
        }
    }

    /// Attaches the only two controller-observed stage completions.
    ///
    /// Remote state stages remain server-observed. This method accepts only
    /// exact driver-proof or seed-receipt evidence declared by the immutable
    /// plan and rejects every other stage before service admission.
    pub fn with_external_completion(
        mut self,
        completion: InstallationStageEvidence,
    ) -> Result<Self, ServiceDtoError> {
        if !matches!(
            completion,
            InstallationStageEvidence::DriverProof(_) | InstallationStageEvidence::Seeds(_)
        ) || completion.validate_for(self.plan.as_ref()).is_err()
        {
            return Err(ServiceDtoError::InvalidShape);
        }
        self.external_completion = Some(completion);
        Ok(self)
    }

    /// Caller-stable campaign identity.
    #[must_use]
    pub const fn campaign_id(&self) -> ApplicationInstallationCampaignId {
        self.campaign_id
    }

    /// Shared immutable content-addressed plan.
    #[must_use]
    pub fn plan(&self) -> &ApplicationInstallationPlan {
        self.plan.as_ref()
    }

    /// Exact externally observed completion, when this resume carries one.
    #[must_use]
    pub const fn external_completion(&self) -> Option<&InstallationStageEvidence> {
        self.external_completion.as_ref()
    }

    /// Consumes the request without cloning the complete plan.
    ///
    /// This compatibility decomposition intentionally omits an attached
    /// external completion. Installation coordinators that support external
    /// stage evidence must use [`Self::into_parts_with_external_completion`].
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        ApplicationInstallationCampaignId,
        Arc<ApplicationInstallationPlan>,
    ) {
        (self.campaign_id, self.plan)
    }

    /// Consumes the request, including exact externally observed completion.
    #[must_use]
    pub fn into_parts_with_external_completion(
        self,
    ) -> (
        ApplicationInstallationCampaignId,
        Arc<ApplicationInstallationPlan>,
        Option<InstallationStageEvidence>,
    ) {
        (self.campaign_id, self.plan, self.external_completion)
    }
}

impl fmt::Debug for StartApplicationInstallationRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StartApplicationInstallationRequest([REDACTED])")
    }
}

/// Checked selector for one caller-stable campaign.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct GetApplicationInstallationRequest {
    campaign_id: ApplicationInstallationCampaignId,
}

impl GetApplicationInstallationRequest {
    /// Constructs one exact campaign lookup.
    #[must_use]
    pub const fn new(campaign_id: ApplicationInstallationCampaignId) -> Self {
        Self { campaign_id }
    }

    /// Caller-stable campaign identity.
    #[must_use]
    pub const fn campaign_id(self) -> ApplicationInstallationCampaignId {
        self.campaign_id
    }
}

impl fmt::Debug for GetApplicationInstallationRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("GetApplicationInstallationRequest([REDACTED])")
    }
}

/// One bounded, lineage-bound campaign observation and optional terminal receipt.
#[derive(Clone, Eq, PartialEq)]
pub struct ApplicationInstallationOperationResult {
    lineage: ContractLineage,
    observation: ApplicationInstallationObservation,
    receipt: Option<ApplicationInstallationReceipt>,
}

impl ApplicationInstallationOperationResult {
    /// Checks terminal/partial shape and exact receipt identities.
    pub fn new(
        lineage: ContractLineage,
        observation: ApplicationInstallationObservation,
        receipt: Option<ApplicationInstallationReceipt>,
    ) -> Result<Self, ServiceDtoError> {
        let installed = observation.phase() == InstallationCampaignPhase::Installed;
        if installed != receipt.is_some()
            || receipt.as_ref().is_some_and(|receipt| {
                receipt.campaign_id() != observation.campaign_id()
                    || receipt.plan_hash() != observation.plan_hash()
                    || Some(receipt.identity()) != observation.receipt_hash()
            })
        {
            return Err(ServiceDtoError::InvalidShape);
        }
        Ok(Self {
            lineage,
            observation,
            receipt,
        })
    }

    /// Protected exact application lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Current typed stage observation.
    #[must_use]
    pub const fn observation(&self) -> &ApplicationInstallationObservation {
        &self.observation
    }

    /// Terminal redacted receipt, present only for installed campaigns.
    #[must_use]
    pub const fn receipt(&self) -> Option<&ApplicationInstallationReceipt> {
        self.receipt.as_ref()
    }
}

impl fmt::Debug for ApplicationInstallationOperationResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ApplicationInstallationOperationResult([REDACTED])")
    }
}

/// Protected result of one campaign lookup.
#[derive(Clone, Eq, PartialEq)]
pub enum GetApplicationInstallationResult {
    /// No campaign exists for the caller-stable identity.
    NotFound,
    /// Current-policy-authorized campaign observation.
    Found(Box<ApplicationInstallationOperationResult>),
}

impl fmt::Debug for GetApplicationInstallationResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NotFound => "GetApplicationInstallationResult::NotFound",
            Self::Found(_) => "GetApplicationInstallationResult::Found([REDACTED])",
        })
    }
}

/// Move-only authorized start/resume submission.
pub struct AuthorizedApplicationInstallationStart {
    request_id: RequestId,
    ingress: ServiceIngressKindV1,
    request: StartApplicationInstallationRequest,
    authorization: Box<AuthorizedApplicationInstallation>,
}

impl AuthorizedApplicationInstallationStart {
    pub(crate) const fn new(
        request_id: RequestId,
        ingress: ServiceIngressKindV1,
        request: StartApplicationInstallationRequest,
        authorization: Box<AuthorizedApplicationInstallation>,
    ) -> Self {
        Self {
            request_id,
            ingress,
            request,
            authorization,
        }
    }

    /// Separates exact immutable input from its fresh move-only proof.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        RequestId,
        ServiceIngressKindV1,
        StartApplicationInstallationRequest,
        Box<AuthorizedApplicationInstallation>,
    ) {
        (
            self.request_id,
            self.ingress,
            self.request,
            self.authorization,
        )
    }
}

impl fmt::Debug for AuthorizedApplicationInstallationStart {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AuthorizedApplicationInstallationStart([REDACTED])")
    }
}

/// Move-only authorized campaign observation submission.
pub struct AuthorizedApplicationInstallationObservation {
    request: GetApplicationInstallationRequest,
    authorization: Box<AuthorizedApplicationInstallation>,
}

impl AuthorizedApplicationInstallationObservation {
    pub(crate) const fn new(
        request: GetApplicationInstallationRequest,
        authorization: Box<AuthorizedApplicationInstallation>,
    ) -> Self {
        Self {
            request,
            authorization,
        }
    }

    /// Separates the exact selector from its fresh proof.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        GetApplicationInstallationRequest,
        Box<AuthorizedApplicationInstallation>,
    ) {
        (self.request, self.authorization)
    }
}

impl fmt::Debug for AuthorizedApplicationInstallationObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AuthorizedApplicationInstallationObservation([REDACTED])")
    }
}

/// Closed failure after a campaign start/resume was submitted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationInstallationStartPortError {
    /// The campaign identity already binds another exact plan.
    InputMismatch,
    /// Durable campaign state is temporarily unavailable.
    Unavailable,
    /// Submission may have persisted but no observation is known.
    OutcomeUnknown,
    /// Retained state violated a checked campaign invariant.
    Integrity,
}

/// Closed failure while resolving or observing campaign state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationInstallationObservationPortError {
    /// Durable campaign state is temporarily unavailable.
    Unavailable,
    /// Retained state violated a checked campaign invariant.
    Integrity,
}

/// Reserved capacity for one authorized campaign start/resume.
pub type ApplicationInstallationStartPermit = BoxPortCapacityPermit<
    AuthorizedApplicationInstallationStart,
    ApplicationInstallationOperationResult,
    ApplicationInstallationStartPortError,
>;

/// Reserved capacity for one authorized campaign observation.
pub type ApplicationInstallationObservationPermit = BoxPortCapacityPermit<
    AuthorizedApplicationInstallationObservation,
    Option<ApplicationInstallationOperationResult>,
    ApplicationInstallationObservationPortError,
>;

/// Server-private durable campaign owner.
///
/// Implementations compose existing deployment/migration/role/credential/
/// driver/seed owners. They do not receive a generic transaction or callback.
pub trait ApplicationInstallationCoordinatorPort: Send + Sync {
    /// Resolves only the protected lineage needed for observation policy.
    fn resolve_campaign_lineage(
        &self,
        campaign_id: ApplicationInstallationCampaignId,
        control: &RequestControl,
    ) -> PortFuture<'_, Option<ContractLineage>, ApplicationInstallationObservationPortError>;

    /// Reserves bounded capacity before the final start authorization safe point.
    fn reserve_start(
        &self,
        control: &RequestControl,
    ) -> PortFuture<'_, ApplicationInstallationStartPermit, PortAdmissionError>;

    /// Reserves bounded capacity before the final observation authorization safe point.
    fn reserve_observation(
        &self,
        control: &RequestControl,
    ) -> PortFuture<'_, ApplicationInstallationObservationPermit, PortAdmissionError>;
}
