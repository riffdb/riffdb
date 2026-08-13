//! Production durable coordinator for exact application-installation campaigns.

use std::fmt;

use riffdb_application::{
    ApplicationInstallationCampaign, ApplicationInstallationCampaignState,
    ApplicationInstallationPlan, InstallationArtifactKind, InstallationCampaignPhase,
    InstallationFailureCode, InstallationStage, InstallationStageEvidence,
    InstalledCredentialEvidence, InstalledReimportEvidence, InstalledRoleEvidence,
};
use riffdb_service::{
    ApplicationInstallationCoordinatorPort, ApplicationInstallationObservationPermit,
    ApplicationInstallationObservationPortError, ApplicationInstallationOperationResult,
    ApplicationInstallationStartPermit, ApplicationInstallationStartPortError,
    AuthorizedApplicationInstallationObservation, AuthorizedApplicationInstallationStart,
    PortAdmissionError, PortFuture, RequestControl,
};
use riffdb_storage_api::{
    ActiveCatalogPointerV1, ApplicationInstallationCampaignRepository,
    ApplicationInstallationCampaignWriteResultV1, CapabilityLifecycleV1, CapabilityReader,
    CatalogRepository, ContractMigrationOperationKindV1, ContractMigrationReceiptPhaseV1,
    QueryModuleRepository, ReactiveModuleRepository, StorageError, StorageErrorKind,
    StoredApplicationInstallationCampaignV1,
};
use riffdb_types::{
    ApplicationInstallationCampaignId, CapabilityPermissionV1, ContractLineage,
    ContractMigrationOperationId, QueryModuleHash, ReactiveModuleHash,
};

use crate::maintenance_adapter::InstallationMigrationReceiptReader;
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
    pub(crate) fn new(
        storage: SharedRedbOperationalPorts,
        migration_receipts: InstallationMigrationReceiptReader,
        driver: &BlockingPortDriver,
    ) -> Self {
        let lineage_storage = storage.clone();
        let start_storage = storage.clone();
        Self {
            lineage: driver
                .executor(move |campaign_id| resolve_lineage(&lineage_storage, campaign_id)),
            start: driver.executor(move |request| {
                start_or_resume(start_storage.clone(), &migration_receipts, request)
            }),
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
    migration_receipts: &InstallationMigrationReceiptReader,
    request: AuthorizedApplicationInstallationStart,
) -> Result<ApplicationInstallationOperationResult, ApplicationInstallationStartPortError> {
    let (_request_id, _ingress, request, _authorization) = request.into_parts();
    let (campaign_id, plan, external_completion) = request.into_parts_with_external_completion();
    let lineage = plan.input().target.lineage().clone();
    let retained = storage
        .read_application_installation_campaign(campaign_id)
        .map_err(map_start_storage)?;
    let (expected, mut campaign) =
        recover_campaign(retained, campaign_id, plan.as_ref(), &lineage)?;

    advance_observed_stages(&storage, migration_receipts, plan.as_ref(), &mut campaign)?;
    if let Some(completion) = external_completion {
        complete_external_stage(plan.as_ref(), &mut campaign, completion)?;
        advance_observed_stages(&storage, migration_receipts, plan.as_ref(), &mut campaign)?;
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

fn complete_external_stage(
    plan: &ApplicationInstallationPlan,
    campaign: &mut ApplicationInstallationCampaign,
    completion: InstallationStageEvidence,
) -> Result<(), ApplicationInstallationStartPortError> {
    if campaign.observe().next_stage() != Some(completion.stage()) {
        return Err(ApplicationInstallationStartPortError::InputMismatch);
    }
    campaign
        .complete_stage(plan, completion)
        .map(|_| ())
        .map_err(|_| ApplicationInstallationStartPortError::InputMismatch)
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum StageVerification {
    Pending,
    Complete,
    Failed(InstallationFailureCode),
}

fn advance_observed_stages(
    storage: &SharedRedbOperationalPorts,
    migration_receipts: &InstallationMigrationReceiptReader,
    plan: &ApplicationInstallationPlan,
    campaign: &mut ApplicationInstallationCampaign,
) -> Result<(), ApplicationInstallationStartPortError> {
    loop {
        let observation = campaign.observe();
        let Some(stage) = observation.next_stage() else {
            return Ok(());
        };
        let evidence = match stage {
            InstallationStage::Preflight => Some(InstallationStageEvidence::Preflight {
                source_hash: plan.input().source_hash,
                lock_hash: plan.input().lock_hash,
                manifest_hash: plan.input().manifest_hash,
            }),
            InstallationStage::Contract => {
                let active =
                    CatalogRepository::read_active_catalog(storage).map_err(map_start_storage)?;
                match classify_contract_stage(plan, active.as_ref()) {
                    StageVerification::Pending => None,
                    StageVerification::Complete => Some(InstallationStageEvidence::Contract {
                        version: plan.input().contract.version(),
                        bundle_hash: plan.input().contract.bundle_hash(),
                    }),
                    StageVerification::Failed(code) => {
                        campaign
                            .record_failure(plan, code)
                            .map_err(|_| ApplicationInstallationStartPortError::Integrity)?;
                        return Ok(());
                    }
                }
            }
            InstallationStage::Migration => match verify_migration_stage(
                storage,
                migration_receipts,
                plan,
                observation.campaign_id(),
            )? {
                StageVerification::Pending => None,
                StageVerification::Complete => Some(InstallationStageEvidence::Migration {
                    migration_hash: plan
                        .input()
                        .migration
                        .map(|migration| migration.migration_hash()),
                }),
                StageVerification::Failed(code) => {
                    campaign
                        .record_failure(plan, code)
                        .map_err(|_| ApplicationInstallationStartPortError::Integrity)?;
                    return Ok(());
                }
            },
            InstallationStage::QueryModules => match verify_query_modules(storage, plan)? {
                StageVerification::Pending => None,
                StageVerification::Complete => Some(InstallationStageEvidence::QueryModules(
                    artifacts_for(plan, InstallationArtifactKind::QueryModule),
                )),
                StageVerification::Failed(code) => {
                    campaign
                        .record_failure(plan, code)
                        .map_err(|_| ApplicationInstallationStartPortError::Integrity)?;
                    return Ok(());
                }
            },
            InstallationStage::ReactiveModules => match verify_reactive_modules(storage, plan)? {
                StageVerification::Pending => None,
                StageVerification::Complete => Some(InstallationStageEvidence::ReactiveModules(
                    artifacts_for(plan, InstallationArtifactKind::ReactiveModule),
                )),
                StageVerification::Failed(code) => {
                    campaign
                        .record_failure(plan, code)
                        .map_err(|_| ApplicationInstallationStartPortError::Integrity)?;
                    return Ok(());
                }
            },
            InstallationStage::Roles => match verify_roles(storage, plan)? {
                StageVerification::Pending => None,
                StageVerification::Complete => Some(InstallationStageEvidence::Roles(
                    plan.input()
                        .roles
                        .iter()
                        .map(|role| {
                            InstalledRoleEvidence::new(role.name().clone(), role.role_hash())
                        })
                        .collect(),
                )),
                StageVerification::Failed(code) => {
                    campaign
                        .record_failure(plan, code)
                        .map_err(|_| ApplicationInstallationStartPortError::Integrity)?;
                    return Ok(());
                }
            },
            InstallationStage::Reimport if plan.input().reimport.is_none() => Some(
                InstallationStageEvidence::Reimport(InstalledReimportEvidence::NotRequired),
            ),
            InstallationStage::Reimport => None,
            InstallationStage::Credentials => match verify_credentials(storage, plan)? {
                StageVerification::Pending => None,
                StageVerification::Complete => Some(InstallationStageEvidence::Credentials(
                    plan.input()
                        .credential_destinations
                        .iter()
                        .map(|destination| {
                            InstalledCredentialEvidence::new(
                                destination.name().clone(),
                                destination.successor(),
                            )
                        })
                        .collect(),
                )),
                StageVerification::Failed(code) => {
                    campaign
                        .record_failure(plan, code)
                        .map_err(|_| ApplicationInstallationStartPortError::Integrity)?;
                    return Ok(());
                }
            },
            InstallationStage::DriverProof => None,
            InstallationStage::Seeds if plan.input().seeds.is_empty() => {
                Some(InstallationStageEvidence::Seeds(Vec::new()))
            }
            InstallationStage::Seeds => None,
            InstallationStage::Receipt => {
                campaign
                    .seal_receipt(plan)
                    .map_err(|_| ApplicationInstallationStartPortError::Integrity)?;
                return Ok(());
            }
        };
        let Some(evidence) = evidence else {
            return Ok(());
        };
        campaign
            .complete_stage(plan, evidence)
            .map_err(|_| ApplicationInstallationStartPortError::Integrity)?;
    }
}

fn classify_contract_stage(
    plan: &ApplicationInstallationPlan,
    active: Option<&ActiveCatalogPointerV1>,
) -> StageVerification {
    let input = plan.input();
    let Some(active) = active else {
        return StageVerification::Pending;
    };
    let active_is_successor = active.lineage() == input.target.lineage()
        && active.contract_version() == input.contract.version()
        && active.bundle_hash() == input.contract.bundle_hash();
    if active_is_successor {
        return StageVerification::Complete;
    }
    if active.lineage() != input.target.lineage() {
        return StageVerification::Failed(InstallationFailureCode::RemoteIdentityMismatch);
    }
    match input.migration {
        Some(migration)
            if active.lineage() == input.target.lineage()
                && active.contract_version() == migration.parent_version()
                && active.bundle_hash() == migration.parent_bundle_hash() =>
        {
            // The exact candidate is pinned by the plan; the migration stage
            // remains responsible for proving the successor cutover.
            StageVerification::Complete
        }
        Some(_) => StageVerification::Failed(InstallationFailureCode::RemoteIdentityMismatch),
        None => StageVerification::Failed(InstallationFailureCode::MigrationGateRequired),
    }
}

fn verify_migration_stage(
    storage: &SharedRedbOperationalPorts,
    migration_receipts: &InstallationMigrationReceiptReader,
    plan: &ApplicationInstallationPlan,
    campaign_id: ApplicationInstallationCampaignId,
) -> Result<StageVerification, ApplicationInstallationStartPortError> {
    let Some(migration) = plan.input().migration else {
        return Ok(StageVerification::Complete);
    };
    let active = CatalogRepository::read_active_catalog(storage).map_err(map_start_storage)?;
    let Some(active) = active else {
        return Ok(StageVerification::Pending);
    };
    if active.lineage() != plan.input().target.lineage()
        || active.contract_version() != migration.successor_version()
        || active.bundle_hash() != migration.successor_bundle_hash()
    {
        return Ok(
            if active.contract_version() == migration.parent_version()
                && active.bundle_hash() == migration.parent_bundle_hash()
            {
                StageVerification::Pending
            } else {
                StageVerification::Failed(InstallationFailureCode::RemoteIdentityMismatch)
            },
        );
    }
    let edge = storage
        .contract_migration_edge(migration.parent_bundle_hash())
        .map_err(map_start_storage)?;
    let Some(edge) = edge else {
        return Ok(StageVerification::Failed(
            InstallationFailureCode::RemoteIdentityMismatch,
        ));
    };
    let artifacts = edge.retirement().artifacts();
    if artifacts.parent() != migration.parent_bundle_hash()
        || artifacts.candidate() != migration.successor_bundle_hash()
        || artifacts.migration() != migration.migration_hash()
    {
        return Ok(StageVerification::Failed(
            InstallationFailureCode::RemoteIdentityMismatch,
        ));
    }
    let operation_id = ContractMigrationOperationId::from_bytes(campaign_id.into_bytes())
        .map_err(|_| ApplicationInstallationStartPortError::Integrity)?;
    let Some(receipt) = migration_receipts
        .read(operation_id)
        .map_err(map_start_storage)?
    else {
        return Ok(StageVerification::Failed(
            InstallationFailureCode::RemoteIdentityMismatch,
        ));
    };
    let receipt_artifacts = receipt.artifacts();
    Ok(
        if receipt.operation_kind() == ContractMigrationOperationKindV1::Apply
            && receipt.operation_id() == operation_id
            && receipt.current_phase() == ContractMigrationReceiptPhaseV1::Succeeded
            && receipt_artifacts.parent() == migration.parent_bundle_hash()
            && receipt_artifacts.candidate() == migration.successor_bundle_hash()
            && receipt_artifacts.migration() == migration.migration_hash()
            && receipt.backup_name().is_some()
            && receipt.backup_manifest().is_some()
        {
            StageVerification::Complete
        } else {
            StageVerification::Failed(InstallationFailureCode::RemoteIdentityMismatch)
        },
    )
}

fn verify_query_modules(
    storage: &SharedRedbOperationalPorts,
    plan: &ApplicationInstallationPlan,
) -> Result<StageVerification, ApplicationInstallationStartPortError> {
    for artifact in artifacts_for(plan, InstallationArtifactKind::QueryModule) {
        let hash = QueryModuleHash::from_bytes(*artifact.content_hash().as_bytes());
        let Some(module) =
            QueryModuleRepository::read_query_module(storage, hash).map_err(map_start_storage)?
        else {
            return Ok(StageVerification::Pending);
        };
        if module.module_name().as_str() != artifact.name().as_str()
            || module.contract_lineage() != plan.input().target.lineage()
            || module.contract_version() != plan.input().contract.version()
            || module.contract_bundle_hash() != plan.input().contract.bundle_hash()
        {
            return Ok(StageVerification::Failed(
                InstallationFailureCode::RemoteIdentityMismatch,
            ));
        }
    }
    Ok(StageVerification::Complete)
}

fn verify_reactive_modules(
    storage: &SharedRedbOperationalPorts,
    plan: &ApplicationInstallationPlan,
) -> Result<StageVerification, ApplicationInstallationStartPortError> {
    for artifact in artifacts_for(plan, InstallationArtifactKind::ReactiveModule) {
        let hash = ReactiveModuleHash::from_bytes(*artifact.content_hash().as_bytes());
        let Some(module) = ReactiveModuleRepository::read_reactive_module(storage, hash)
            .map_err(map_start_storage)?
        else {
            return Ok(StageVerification::Pending);
        };
        if module.module_name() != artifact.name().as_str()
            || module.contract_lineage() != plan.input().target.lineage()
            || module.contract_version() != plan.input().contract.version()
            || module.contract_bundle_hash() != plan.input().contract.bundle_hash()
        {
            return Ok(StageVerification::Failed(
                InstallationFailureCode::RemoteIdentityMismatch,
            ));
        }
    }
    Ok(StageVerification::Complete)
}

fn verify_roles(
    storage: &SharedRedbOperationalPorts,
    plan: &ApplicationInstallationPlan,
) -> Result<StageVerification, ApplicationInstallationStartPortError> {
    for role in &plan.input().roles {
        let destinations = plan
            .input()
            .credential_destinations
            .iter()
            .filter(|destination| destination.role() == role.name())
            .collect::<Vec<_>>();
        if destinations.is_empty() {
            return Ok(StageVerification::Failed(
                InstallationFailureCode::RemoteIdentityMismatch,
            ));
        }
        for destination in destinations {
            let Some(record) = CapabilityReader::read_capability(storage, destination.successor())
                .map_err(map_start_storage)?
            else {
                return Ok(StageVerification::Pending);
            };
            if !matches!(record.lifecycle(), CapabilityLifecycleV1::Active)
                || record.environment() != plan.input().target.environment()
                || !record
                    .grant()
                    .permissions()
                    .as_slice()
                    .iter()
                    .any(|permission| {
                        matches!(
                            permission,
                            CapabilityPermissionV1::ApplicationRoleIdentity(hash)
                                if *hash == role.role_hash()
                        )
                    })
            {
                return Ok(StageVerification::Failed(
                    InstallationFailureCode::RemoteIdentityMismatch,
                ));
            }
        }
    }
    Ok(StageVerification::Complete)
}

fn verify_credentials(
    storage: &SharedRedbOperationalPorts,
    plan: &ApplicationInstallationPlan,
) -> Result<StageVerification, ApplicationInstallationStartPortError> {
    for destination in &plan.input().credential_destinations {
        let Some(record) = CapabilityReader::read_capability(storage, destination.successor())
            .map_err(map_start_storage)?
        else {
            return Ok(StageVerification::Pending);
        };
        if !matches!(record.lifecycle(), CapabilityLifecycleV1::Active) {
            return Ok(StageVerification::Failed(
                InstallationFailureCode::CredentialDestinationOccupied,
            ));
        }
        if record.environment() != plan.input().target.environment() {
            return Ok(StageVerification::Failed(
                InstallationFailureCode::RemoteIdentityMismatch,
            ));
        }
        if let Some(predecessor) = destination.expected_current() {
            let Some(predecessor) = CapabilityReader::read_capability(storage, predecessor)
                .map_err(map_start_storage)?
            else {
                return Ok(StageVerification::Failed(
                    InstallationFailureCode::CredentialDestinationOccupied,
                ));
            };
            if !matches!(
                predecessor.lifecycle(),
                CapabilityLifecycleV1::Revoked { .. }
            ) {
                return Ok(StageVerification::Pending);
            }
        }
    }
    Ok(StageVerification::Complete)
}

fn artifacts_for(
    plan: &ApplicationInstallationPlan,
    kind: InstallationArtifactKind,
) -> Vec<riffdb_application::InstallationArtifact> {
    plan.input()
        .artifacts
        .iter()
        .filter(|artifact| artifact.kind() == kind)
        .cloned()
        .collect()
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

pub(crate) fn encode_state(
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

pub(crate) fn decode_state(
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
    use riffdb_application::{InstallationDriver, InstalledSeedEvidence};
    use riffdb_storage_api::ActiveCatalogPointerV1;
    use riffdb_types::ContractBundleHash;

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

    #[test]
    fn contract_stage_distinguishes_absent_exact_and_inexact_remote_state() {
        let plan = plan();
        assert_eq!(
            classify_contract_stage(&plan, None),
            StageVerification::Pending
        );
        let exact = ActiveCatalogPointerV1::new(
            plan.input().target.lineage().clone(),
            plan.input().contract.version(),
            plan.input().contract.bundle_hash(),
        );
        assert_eq!(
            classify_contract_stage(&plan, Some(&exact)),
            StageVerification::Complete
        );
        let inexact = ActiveCatalogPointerV1::new(
            plan.input().target.lineage().clone(),
            plan.input().contract.version(),
            ContractBundleHash::from_bytes([0x44; 32]),
        );
        assert_eq!(
            classify_contract_stage(&plan, Some(&inexact)),
            StageVerification::Failed(InstallationFailureCode::MigrationGateRequired)
        );
    }

    #[test]
    fn external_completion_cannot_skip_or_replay_a_campaign_stage() {
        let plan = plan();
        let mut campaign = ApplicationInstallationCampaign::start(campaign_id(), plan.identity());
        let driver_proof = InstallationStageEvidence::DriverProof(vec![
            InstallationDriver::Rust,
            InstallationDriver::TypeScript,
        ]);
        let original = campaign.clone();
        assert_eq!(
            complete_external_stage(&plan, &mut campaign, driver_proof.clone()),
            Err(ApplicationInstallationStartPortError::InputMismatch)
        );
        assert_eq!(campaign, original, "rejected evidence cannot advance state");

        complete_through_credentials(&plan, &mut campaign);
        complete_external_stage(&plan, &mut campaign, driver_proof.clone())
            .expect("exact driver proof advances only its stage");
        assert_eq!(
            campaign.observe().next_stage(),
            Some(InstallationStage::Seeds)
        );
        assert_eq!(
            complete_external_stage(&plan, &mut campaign, driver_proof),
            Err(ApplicationInstallationStartPortError::InputMismatch)
        );

        let seed = plan.input().seeds.first().expect("seed plan");
        complete_external_stage(
            &plan,
            &mut campaign,
            InstallationStageEvidence::Seeds(vec![InstalledSeedEvidence::new(
                seed.name().clone(),
                seed.content_hash(),
                seed.item_count(),
                0,
            )]),
        )
        .expect("exact bounded seed receipt");
        assert_eq!(
            campaign.observe().next_stage(),
            Some(InstallationStage::Receipt),
            "external evidence never asserts the terminal receipt"
        );
    }

    fn complete_through_credentials(
        plan: &ApplicationInstallationPlan,
        campaign: &mut ApplicationInstallationCampaign,
    ) {
        let input = plan.input();
        let stages = [
            InstallationStageEvidence::Preflight {
                source_hash: input.source_hash,
                lock_hash: input.lock_hash,
                manifest_hash: input.manifest_hash,
            },
            InstallationStageEvidence::Contract {
                version: input.contract.version(),
                bundle_hash: input.contract.bundle_hash(),
            },
            InstallationStageEvidence::Migration {
                migration_hash: None,
            },
            InstallationStageEvidence::QueryModules(artifacts_for(
                plan,
                InstallationArtifactKind::QueryModule,
            )),
            InstallationStageEvidence::ReactiveModules(artifacts_for(
                plan,
                InstallationArtifactKind::ReactiveModule,
            )),
            InstallationStageEvidence::Roles(
                input
                    .roles
                    .iter()
                    .map(|role| InstalledRoleEvidence::new(role.name().clone(), role.role_hash()))
                    .collect(),
            ),
            InstallationStageEvidence::Reimport(InstalledReimportEvidence::NotRequired),
            InstallationStageEvidence::Credentials(
                input
                    .credential_destinations
                    .iter()
                    .map(|destination| {
                        InstalledCredentialEvidence::new(
                            destination.name().clone(),
                            destination.successor(),
                        )
                    })
                    .collect(),
            ),
        ];
        for stage in stages {
            campaign
                .complete_stage(plan, stage)
                .expect("authoritative predecessor stage");
        }
    }
}
