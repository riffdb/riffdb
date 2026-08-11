//! Production durable coordinator for exact application-installation campaigns.

use std::fmt;

use riffdb_application::{
    ApplicationInstallationCampaign, ApplicationInstallationCampaignState,
    ApplicationInstallationPlan, InstallationCampaignPhase, InstallationStage,
    InstallationStageEvidence,
};
use riffdb_service::{
    ApplicationInstallationCoordinatorPort, ApplicationInstallationObservationPermit,
    ApplicationInstallationObservationPortError, ApplicationInstallationOperationResult,
    ApplicationInstallationStartPermit, ApplicationInstallationStartPortError,
    AuthorizedApplicationInstallationObservation, AuthorizedApplicationInstallationStart,
    PortAdmissionError, PortFuture, RequestControl,
};
use riffdb_storage_api::{
    ApplicationInstallationCampaignRepository, ApplicationInstallationCampaignWriteResultV1,
    StorageError, StorageErrorKind, StoredApplicationInstallationCampaignV1,
};
use riffdb_types::{ApplicationInstallationCampaignId, ContractLineage};

use crate::port_driver::{BlockingPortDriver, BlockingPortExecutor};
use crate::storage::SharedRedbOperationalPorts;

/// Bounded blocking adapter over durable exact campaign state.
pub(crate) struct ServerApplicationInstallationCoordinator {
    lineage: BlockingPortExecutor<
        ApplicationInstallationCampaignId,
        Option<ContractLineage>,
        ApplicationInstallationObservationPortError,
    >,
    start: BlockingPortExecutor<
        AuthorizedApplicationInstallationStart,
        ApplicationInstallationOperationResult,
        ApplicationInstallationStartPortError,
    >,
    observation: BlockingPortExecutor<
        AuthorizedApplicationInstallationObservation,
        Option<ApplicationInstallationOperationResult>,
        ApplicationInstallationObservationPortError,
    >,
}

impl ServerApplicationInstallationCoordinator {
    pub(crate) fn new(storage: SharedRedbOperationalPorts, driver: &BlockingPortDriver) -> Self {
        let lineage_storage = storage.clone();
        let start_storage = storage.clone();
        Self {
            lineage: driver
                .executor(move |campaign_id| resolve_lineage(&lineage_storage, campaign_id)),
            start: driver.executor(move |request| start_or_resume(start_storage.clone(), request)),
            observation: driver.executor(move |request| observe(&storage, request)),
        }
    }
}

impl ApplicationInstallationCoordinatorPort for ServerApplicationInstallationCoordinator {
    fn resolve_campaign_lineage(
        &self,
        campaign_id: ApplicationInstallationCampaignId,
        control: &RequestControl,
    ) -> PortFuture<'_, Option<ContractLineage>, ApplicationInstallationObservationPortError> {
        let permit = self.lineage.reserve(control);
        Box::pin(async move {
            let permit =
                permit.map_err(|_| ApplicationInstallationObservationPortError::Unavailable)?;
            permit
                .submit(campaign_id)
                .map_err(|_| ApplicationInstallationObservationPortError::Unavailable)?
                .completion()
                .await
                .map_err(|_| ApplicationInstallationObservationPortError::Unavailable)?
        })
    }

    fn reserve_start(
        &self,
        control: &RequestControl,
    ) -> PortFuture<'_, ApplicationInstallationStartPermit, PortAdmissionError> {
        let reservation = self.start.reserve(control);
        Box::pin(async move { reservation })
    }

    fn reserve_observation(
        &self,
        control: &RequestControl,
    ) -> PortFuture<'_, ApplicationInstallationObservationPermit, PortAdmissionError> {
        let reservation = self.observation.reserve(control);
        Box::pin(async move { reservation })
    }
}

impl fmt::Debug for ServerApplicationInstallationCoordinator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServerApplicationInstallationCoordinator([EXACT_DURABLE_STATE])")
    }
}

fn resolve_lineage(
    storage: &SharedRedbOperationalPorts,
    campaign_id: ApplicationInstallationCampaignId,
) -> Result<Option<ContractLineage>, ApplicationInstallationObservationPortError> {
    storage
        .read_application_installation_campaign(campaign_id)
        .map(|record| record.map(|record| record.contract_lineage().clone()))
        .map_err(map_observation_storage)
}

fn start_or_resume(
    mut storage: SharedRedbOperationalPorts,
    request: AuthorizedApplicationInstallationStart,
) -> Result<ApplicationInstallationOperationResult, ApplicationInstallationStartPortError> {
    let (_request_id, _ingress, request, _authorization) = request.into_parts();
    let (campaign_id, plan) = request.into_parts();
    let lineage = plan.input().target.lineage().clone();
    let retained = storage
        .read_application_installation_campaign(campaign_id)
        .map_err(map_start_storage)?;
    let (expected, mut campaign) =
        recover_campaign(retained, campaign_id, plan.as_ref(), &lineage)?;

    // Compilation proves the local identities. Remote stages remain pending
    // until their existing authoritative owners supply exact evidence.
    if campaign.observe().next_stage() == Some(InstallationStage::Preflight) {
        campaign
            .complete_stage(
                plan.as_ref(),
                InstallationStageEvidence::Preflight {
                    source_hash: plan.input().source_hash,
                    lock_hash: plan.input().lock_hash,
                    manifest_hash: plan.input().manifest_hash,
                },
            )
            .map_err(|_| ApplicationInstallationStartPortError::Integrity)?;
    }

    let replacement = encode_state(&campaign, plan.as_ref())
        .map_err(|()| ApplicationInstallationStartPortError::Integrity)?;
    match storage
        .compare_and_swap_application_installation_campaign(expected.as_ref(), &replacement)
        .map_err(map_start_storage)?
    {
        ApplicationInstallationCampaignWriteResultV1::Applied
        | ApplicationInstallationCampaignWriteResultV1::Unchanged => {
            operation_result(lineage, &campaign, plan.as_ref())
                .map_err(|()| ApplicationInstallationStartPortError::Integrity)
        }
        ApplicationInstallationCampaignWriteResultV1::CompareMismatch => {
            Err(ApplicationInstallationStartPortError::OutcomeUnknown)
        }
    }
}

fn recover_campaign(
    retained: Option<StoredApplicationInstallationCampaignV1>,
    campaign_id: ApplicationInstallationCampaignId,
    plan: &ApplicationInstallationPlan,
    lineage: &ContractLineage,
) -> Result<
    (
        Option<StoredApplicationInstallationCampaignV1>,
        ApplicationInstallationCampaign,
    ),
    ApplicationInstallationStartPortError,
> {
    match retained {
        Some(record) => {
            if record.contract_lineage() != lineage || record.plan_hash() != plan.identity() {
                return Err(ApplicationInstallationStartPortError::InputMismatch);
            }
            let state = decode_state(&record)
                .map_err(|()| ApplicationInstallationStartPortError::Integrity)?;
            if state.plan() != plan {
                return Err(ApplicationInstallationStartPortError::InputMismatch);
            }
            let (_, mut campaign) = state.into_parts();
            campaign
                .resume(campaign_id, plan)
                .map_err(|_| ApplicationInstallationStartPortError::Integrity)?;
            Ok((Some(record), campaign))
        }
        None => Ok((
            None,
            ApplicationInstallationCampaign::start(campaign_id, plan.identity()),
        )),
    }
}

fn observe(
    storage: &SharedRedbOperationalPorts,
    request: AuthorizedApplicationInstallationObservation,
) -> Result<
    Option<ApplicationInstallationOperationResult>,
    ApplicationInstallationObservationPortError,
> {
    let (request, _authorization) = request.into_parts();
    let Some(record) = storage
        .read_application_installation_campaign(request.campaign_id())
        .map_err(map_observation_storage)?
    else {
        return Ok(None);
    };
    let state = decode_state(&record)
        .map_err(|()| ApplicationInstallationObservationPortError::Integrity)?;
    let (plan, campaign) = state.into_parts();
    operation_result(record.contract_lineage().clone(), &campaign, &plan)
        .map(Some)
        .map_err(|()| ApplicationInstallationObservationPortError::Integrity)
}

fn encode_state(
    campaign: &ApplicationInstallationCampaign,
    plan: &ApplicationInstallationPlan,
) -> Result<StoredApplicationInstallationCampaignV1, ()> {
    let state = ApplicationInstallationCampaignState::capture(campaign, plan).map_err(|_| ())?;
    StoredApplicationInstallationCampaignV1::new(
        campaign.observe().campaign_id(),
        plan.input().target.lineage().clone(),
        plan.identity(),
        state.canonical_bytes().to_vec(),
    )
    .map_err(|_| ())
}

fn decode_state(
    record: &StoredApplicationInstallationCampaignV1,
) -> Result<ApplicationInstallationCampaignState, ()> {
    let state = ApplicationInstallationCampaignState::decode_canonical(record.canonical_state())
        .map_err(|_| ())?;
    let observation = state.campaign().observe();
    if observation.campaign_id() != record.campaign_id()
        || observation.plan_hash() != record.plan_hash()
        || state.plan().input().target.lineage() != record.contract_lineage()
    {
        return Err(());
    }
    Ok(state)
}

fn operation_result(
    lineage: ContractLineage,
    campaign: &ApplicationInstallationCampaign,
    plan: &ApplicationInstallationPlan,
) -> Result<ApplicationInstallationOperationResult, ()> {
    let observation = campaign.observe();
    let receipt = if observation.phase() == InstallationCampaignPhase::Installed {
        let mut campaign = campaign.clone();
        Some(campaign.seal_receipt(plan).map_err(|_| ())?)
    } else {
        None
    };
    ApplicationInstallationOperationResult::new(lineage, observation, receipt).map_err(|_| ())
}

const fn map_observation_storage(
    error: StorageError,
) -> ApplicationInstallationObservationPortError {
    match error.kind() {
        StorageErrorKind::CorruptData | StorageErrorKind::InvariantViolation => {
            ApplicationInstallationObservationPortError::Integrity
        }
        _ => ApplicationInstallationObservationPortError::Unavailable,
    }
}

const fn map_start_storage(error: StorageError) -> ApplicationInstallationStartPortError {
    match error.kind() {
        StorageErrorKind::CorruptData | StorageErrorKind::InvariantViolation => {
            ApplicationInstallationStartPortError::Integrity
        }
        StorageErrorKind::CommitStatusUnknown => {
            ApplicationInstallationStartPortError::OutcomeUnknown
        }
        _ => ApplicationInstallationStartPortError::Unavailable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan() -> ApplicationInstallationPlan {
        ApplicationInstallationPlan::decode_canonical(include_bytes!(
            "../../../fixtures/installation/application-installation-plan-v1.json"
        ))
        .expect("canonical plan")
    }

    fn campaign_id() -> ApplicationInstallationCampaignId {
        ApplicationInstallationCampaignId::from_unix_milliseconds_and_random(1, [1; 10])
            .expect("campaign ID")
    }

    #[test]
    fn retained_campaign_accepts_only_its_exact_plan_and_lineage() {
        let plan = plan();
        let id = campaign_id();
        let mut campaign = ApplicationInstallationCampaign::start(id, plan.identity());
        campaign
            .complete_stage(
                &plan,
                InstallationStageEvidence::Preflight {
                    source_hash: plan.input().source_hash,
                    lock_hash: plan.input().lock_hash,
                    manifest_hash: plan.input().manifest_hash,
                },
            )
            .expect("preflight");
        let record = encode_state(&campaign, &plan).expect("stored campaign");
        let (_, recovered) = recover_campaign(
            Some(record.clone()),
            id,
            &plan,
            plan.input().target.lineage(),
        )
        .expect("exact retry");
        assert_eq!(recovered, campaign);

        let changed = String::from_utf8(plan.canonical_bytes().to_vec())
            .expect("plan JSON")
            .replacen(&"01".repeat(32), &"0a".repeat(32), 1);
        let changed = ApplicationInstallationPlan::decode_canonical(changed.as_bytes())
            .expect("changed canonical plan");
        assert_eq!(
            recover_campaign(
                Some(record.clone()),
                id,
                &changed,
                plan.input().target.lineage(),
            )
            .expect_err("plan substitution"),
            ApplicationInstallationStartPortError::InputMismatch
        );
        assert_eq!(
            recover_campaign(
                Some(record),
                id,
                &plan,
                &ContractLineage::new("Other").expect("lineage"),
            )
            .expect_err("lineage substitution"),
            ApplicationInstallationStartPortError::InputMismatch
        );
    }
}
