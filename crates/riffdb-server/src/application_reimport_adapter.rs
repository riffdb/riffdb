//! Production durable coordinator for compiler-owned application reimport.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use riffdb_application::{
    ApplicationPortabilityManifest, ApplicationReimportCampaignPhaseV1, ApplicationReimportReceipt,
    ApplicationReimportSourceV1, PortableRecordClass, ReimportWorkflowQuiescenceV1,
};
use riffdb_service::{
    ApplicationReimportCancelPermitV1, ApplicationReimportCoordinatorPort,
    ApplicationReimportMutationPortErrorV1, ApplicationReimportObservationPermitV1,
    ApplicationReimportObservationPortErrorV1, ApplicationReimportOperationResultV1,
    ApplicationReimportPagePermitV1, ApplicationReimportPagePreparationV1,
    ApplicationReimportPolicyBindingV1, ApplicationReimportStartPermitV1,
    AuthorizedApplicationReimportOperationV1, AuthorizedApplicationReimportPageV1,
    AuthorizedApplicationReimportStartV1, CanonicalApplicationExportJsonDocument,
    PortAdmissionError, PortFuture, RequestControl,
};
use riffdb_storage_api::{
    ApplicationInstallationCampaignRepository, ApplicationInstallationCampaignWriteResultV1,
    StorageError, StorageErrorKind, StoredApplicationInstallationCampaignV1,
};
use riffdb_types::{
    ApplicationExportManifestHash, ApplicationExportPageHash, ApplicationExportReceiptHash,
    ApplicationInstallationCampaignId, CapabilityApplicationReimportScopeV1, ContractLineage,
    DatabaseId, hash_application_export_manifest, hash_application_export_receipt,
};
use serde::{Deserialize, Serialize};

use crate::installation_adapter::{decode_state, encode_state};
use crate::port_driver::{BlockingPortDriver, BlockingPortExecutor};
use crate::storage::SharedRedbOperationalPorts;

const PORTABILITY_MANIFEST_SCHEMA: &str = "riffdb.application-export-manifest/v2";
const PORTABILITY_RECEIPT_SCHEMA: &str = "riffdb.application-export-receipt/v2";

/// Bounded blocking adapter over the installation campaign's reimport stage.
pub(crate) struct ServerApplicationReimportCoordinator {
    binding: BlockingPortExecutor<
        ApplicationInstallationCampaignId,
        Option<ApplicationReimportPolicyBindingV1>,
        ApplicationReimportObservationPortErrorV1,
    >,
    preparation: BlockingPortExecutor<
        ApplicationInstallationCampaignId,
        Option<ApplicationReimportPagePreparationV1>,
        ApplicationReimportObservationPortErrorV1,
    >,
    start: BlockingPortExecutor<
        AuthorizedApplicationReimportStartV1,
        ApplicationReimportOperationResultV1,
        ApplicationReimportMutationPortErrorV1,
    >,
    page: BlockingPortExecutor<
        AuthorizedApplicationReimportPageV1,
        ApplicationReimportOperationResultV1,
        ApplicationReimportMutationPortErrorV1,
    >,
    observation: BlockingPortExecutor<
        AuthorizedApplicationReimportOperationV1,
        Option<ApplicationReimportOperationResultV1>,
        ApplicationReimportObservationPortErrorV1,
    >,
    cancel: BlockingPortExecutor<
        AuthorizedApplicationReimportOperationV1,
        Option<ApplicationReimportOperationResultV1>,
        ApplicationReimportMutationPortErrorV1,
    >,
}

impl ServerApplicationReimportCoordinator {
    pub(crate) fn new(storage: SharedRedbOperationalPorts, driver: &BlockingPortDriver) -> Self {
        let binding_storage = storage.clone();
        let preparation_storage = storage.clone();
        let start_storage = storage.clone();
        let page_storage = storage.clone();
        let observation_storage = storage.clone();
        Self {
            binding: driver
                .executor(move |campaign_id| resolve_binding(&binding_storage, campaign_id)),
            preparation: driver
                .executor(move |campaign_id| prepare_page(&preparation_storage, campaign_id)),
            start: driver.executor(move |request| start(start_storage.clone(), request)),
            page: driver.executor(move |request| apply_page(&page_storage, request)),
            observation: driver.executor(move |request| observe(&observation_storage, request)),
            cancel: driver.executor(move |request| cancel(storage.clone(), request)),
        }
    }
}

impl ApplicationReimportCoordinatorPort for ServerApplicationReimportCoordinator {
    fn resolve_application_reimport_binding(
        &self,
        campaign_id: ApplicationInstallationCampaignId,
        control: &RequestControl,
    ) -> PortFuture<
        '_,
        Option<ApplicationReimportPolicyBindingV1>,
        ApplicationReimportObservationPortErrorV1,
    > {
        let permit = self.binding.reserve(control);
        Box::pin(async move {
            let permit = permit
                .map_err(|_| ApplicationReimportObservationPortErrorV1::StorageUnavailable)?;
            permit
                .submit(campaign_id)
                .map_err(|_| ApplicationReimportObservationPortErrorV1::StorageUnavailable)?
                .completion()
                .await
                .map_err(|_| ApplicationReimportObservationPortErrorV1::StorageUnavailable)?
        })
    }

    fn reserve_application_reimport_start(
        &self,
        control: &RequestControl,
    ) -> PortFuture<'_, ApplicationReimportStartPermitV1, PortAdmissionError> {
        let reservation = self.start.reserve(control);
        Box::pin(async move { reservation })
    }

    fn prepare_application_reimport_page(
        &self,
        campaign_id: ApplicationInstallationCampaignId,
        control: &RequestControl,
    ) -> PortFuture<
        '_,
        Option<ApplicationReimportPagePreparationV1>,
        ApplicationReimportObservationPortErrorV1,
    > {
        let permit = self.preparation.reserve(control);
        Box::pin(async move {
            permit
                .map_err(|_| ApplicationReimportObservationPortErrorV1::StorageUnavailable)?
                .submit(campaign_id)
                .map_err(|_| ApplicationReimportObservationPortErrorV1::StorageUnavailable)?
                .completion()
                .await
                .map_err(|_| ApplicationReimportObservationPortErrorV1::StorageUnavailable)?
        })
    }

    fn reserve_application_reimport_page(
        &self,
        control: &RequestControl,
    ) -> PortFuture<'_, ApplicationReimportPagePermitV1, PortAdmissionError> {
        let reservation = self.page.reserve(control);
        Box::pin(async move { reservation })
    }

    fn reserve_application_reimport_observation(
        &self,
        control: &RequestControl,
    ) -> PortFuture<'_, ApplicationReimportObservationPermitV1, PortAdmissionError> {
        let reservation = self.observation.reserve(control);
        Box::pin(async move { reservation })
    }

    fn reserve_application_reimport_cancel(
        &self,
        control: &RequestControl,
    ) -> PortFuture<'_, ApplicationReimportCancelPermitV1, PortAdmissionError> {
        let reservation = self.cancel.reserve(control);
        Box::pin(async move { reservation })
    }
}

impl fmt::Debug for ServerApplicationReimportCoordinator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServerApplicationReimportCoordinator([EXACT_DURABLE_STATE])")
    }
}

fn resolve_binding(
    storage: &SharedRedbOperationalPorts,
    campaign_id: ApplicationInstallationCampaignId,
) -> Result<Option<ApplicationReimportPolicyBindingV1>, ApplicationReimportObservationPortErrorV1> {
    let Some(record) = storage
        .read_application_installation_campaign(campaign_id)
        .map_err(map_observation_storage)?
    else {
        return Ok(None);
    };
    let state =
        decode_state(&record).map_err(|()| ApplicationReimportObservationPortErrorV1::Integrity)?;
    let Some(progress) = state.campaign().reimport() else {
        return Ok(None);
    };
    Ok(Some(ApplicationReimportPolicyBindingV1::new(
        record.contract_lineage().clone(),
        progress.source().portability_manifest_hash(),
        progress.scope(),
    )))
}

fn start(
    mut storage: SharedRedbOperationalPorts,
    request: AuthorizedApplicationReimportStartV1,
) -> Result<ApplicationReimportOperationResultV1, ApplicationReimportMutationPortErrorV1> {
    let (_request_id, _ingress, request, authorization) = request.into_parts();
    let (campaign_id, lineage, scope, manifest, export_manifest, export_receipt) =
        request.into_parts();
    if authorization.request().campaign_id() != campaign_id
        || authorization.request().lineage() != &lineage
        || authorization.request().portability_manifest_hash() != manifest.identity()
        || authorization.request().scope() != scope
    {
        return Err(ApplicationReimportMutationPortErrorV1::IdentityMismatch);
    }
    let source = parse_source(
        export_manifest,
        export_receipt,
        manifest.as_ref(),
        authorization.database_id(),
        scope,
    )?;
    let retained = storage
        .read_application_installation_campaign(campaign_id)
        .map_err(map_mutation_storage)?
        .ok_or(ApplicationReimportMutationPortErrorV1::IdentityMismatch)?;
    if retained.contract_lineage() != &lineage {
        return Err(ApplicationReimportMutationPortErrorV1::IdentityMismatch);
    }
    let state =
        decode_state(&retained).map_err(|()| ApplicationReimportMutationPortErrorV1::Integrity)?;
    let (plan, mut campaign) = state.into_parts();
    if campaign.observe().next_stage() != Some(riffdb_application::InstallationStage::Reimport) {
        return Err(ApplicationReimportMutationPortErrorV1::InvalidPhase);
    }
    match campaign.reimport_mut() {
        Some(progress) => {
            if progress.source() != &source
                || progress.scope() != scope
                || progress.portability_manifest_document() != manifest.canonical_bytes()
            {
                return Err(ApplicationReimportMutationPortErrorV1::IdentityMismatch);
            }
            if progress
                .verify_authority(authorization.authority())
                .is_err()
            {
                persist(&mut storage, &retained, &campaign, &plan)?;
                return Err(ApplicationReimportMutationPortErrorV1::AuthorityChanged);
            }
        }
        None => {
            campaign
                .start_reimport(
                    &plan,
                    source,
                    authorization.authority(),
                    scope,
                    manifest.as_ref(),
                )
                .map_err(|_| ApplicationReimportMutationPortErrorV1::IdentityMismatch)?;
        }
    }
    persist(&mut storage, &retained, &campaign, &plan)?;
    operation_result(lineage, &campaign)
}

fn prepare_page(
    storage: &SharedRedbOperationalPorts,
    campaign_id: ApplicationInstallationCampaignId,
) -> Result<Option<ApplicationReimportPagePreparationV1>, ApplicationReimportObservationPortErrorV1>
{
    let Some(record) = storage
        .read_application_installation_campaign(campaign_id)
        .map_err(map_observation_storage)?
    else {
        return Ok(None);
    };
    let state =
        decode_state(&record).map_err(|()| ApplicationReimportObservationPortErrorV1::Integrity)?;
    let (_, campaign) = state.into_parts();
    let progress = campaign
        .reimport()
        .ok_or(ApplicationReimportObservationPortErrorV1::Integrity)?;
    if progress.phase() != ApplicationReimportCampaignPhaseV1::Applying {
        return Err(ApplicationReimportObservationPortErrorV1::Integrity);
    }
    let index = usize::try_from(progress.next_page().get() - 1)
        .map_err(|_| ApplicationReimportObservationPortErrorV1::Integrity)?;
    let expected_hash = *progress
        .source()
        .page_hashes()
        .get(index)
        .ok_or(ApplicationReimportObservationPortErrorV1::Integrity)?;
    let manifest =
        ApplicationPortabilityManifest::decode_canonical(progress.portability_manifest_document())
            .map_err(|_| ApplicationReimportObservationPortErrorV1::Integrity)?;
    Ok(Some(ApplicationReimportPagePreparationV1::new(
        record.contract_lineage().clone(),
        manifest,
        progress.next_page(),
        expected_hash,
    )))
}

fn apply_page(
    storage: &SharedRedbOperationalPorts,
    request: AuthorizedApplicationReimportPageV1,
) -> Result<ApplicationReimportOperationResultV1, ApplicationReimportMutationPortErrorV1> {
    let (request, authorization, outcomes) = request.into_parts();
    let mut storage = storage.clone();
    let retained = storage
        .read_application_installation_campaign(request.campaign_id())
        .map_err(map_mutation_storage)?
        .ok_or(ApplicationReimportMutationPortErrorV1::IdentityMismatch)?;
    let state =
        decode_state(&retained).map_err(|()| ApplicationReimportMutationPortErrorV1::Integrity)?;
    let (plan, mut campaign) = state.into_parts();
    let progress = campaign
        .reimport_mut()
        .ok_or(ApplicationReimportMutationPortErrorV1::InvalidPhase)?;
    let expected_index = usize::try_from(progress.next_page().get() - 1)
        .map_err(|_| ApplicationReimportMutationPortErrorV1::Integrity)?;
    if authorization.request().campaign_id() != request.campaign_id()
        || authorization.request().lineage() != retained.contract_lineage()
        || authorization.request().portability_manifest_hash()
            != progress.source().portability_manifest_hash()
        || authorization.request().scope() != progress.scope()
        || request.page().page_number() != progress.next_page()
        || progress.source().page_hashes().get(expected_index) != Some(&request.page().page_hash())
    {
        return Err(ApplicationReimportMutationPortErrorV1::IdentityMismatch);
    }
    if progress
        .verify_authority(authorization.authority())
        .is_err()
    {
        persist(&mut storage, &retained, &campaign, &plan)?;
        return Err(ApplicationReimportMutationPortErrorV1::AuthorityChanged);
    }
    progress
        .complete_page(
            request.page().page_number(),
            request.page().page_hash(),
            outcomes,
        )
        .map_err(|_| ApplicationReimportMutationPortErrorV1::SourceMismatch)?;
    persist(&mut storage, &retained, &campaign, &plan)?;
    operation_result(retained.contract_lineage().clone(), &campaign)
}

fn observe(
    storage: &SharedRedbOperationalPorts,
    request: AuthorizedApplicationReimportOperationV1,
) -> Result<Option<ApplicationReimportOperationResultV1>, ApplicationReimportObservationPortErrorV1>
{
    let (request, authorization) = request.into_parts();
    let Some(record) = storage
        .read_application_installation_campaign(request.campaign_id())
        .map_err(map_observation_storage)?
    else {
        return Ok(None);
    };
    let state =
        decode_state(&record).map_err(|()| ApplicationReimportObservationPortErrorV1::Integrity)?;
    let (_, campaign) = state.into_parts();
    let progress = campaign
        .reimport()
        .ok_or(ApplicationReimportObservationPortErrorV1::Integrity)?;
    if progress.authority() != authorization.authority() {
        return Err(ApplicationReimportObservationPortErrorV1::Integrity);
    }
    operation_result(record.contract_lineage().clone(), &campaign)
        .map(Some)
        .map_err(|_| ApplicationReimportObservationPortErrorV1::Integrity)
}

fn cancel(
    mut storage: SharedRedbOperationalPorts,
    request: AuthorizedApplicationReimportOperationV1,
) -> Result<Option<ApplicationReimportOperationResultV1>, ApplicationReimportMutationPortErrorV1> {
    let (request, authorization) = request.into_parts();
    let Some(retained) = storage
        .read_application_installation_campaign(request.campaign_id())
        .map_err(map_mutation_storage)?
    else {
        return Ok(None);
    };
    let state =
        decode_state(&retained).map_err(|()| ApplicationReimportMutationPortErrorV1::Integrity)?;
    let (plan, mut campaign) = state.into_parts();
    let progress = campaign
        .reimport_mut()
        .ok_or(ApplicationReimportMutationPortErrorV1::InvalidPhase)?;
    if progress
        .verify_authority(authorization.authority())
        .is_err()
    {
        persist(&mut storage, &retained, &campaign, &plan)?;
        return Err(ApplicationReimportMutationPortErrorV1::AuthorityChanged);
    }
    progress
        .cancel()
        .map_err(|_| ApplicationReimportMutationPortErrorV1::InvalidPhase)?;
    persist(&mut storage, &retained, &campaign, &plan)?;
    operation_result(retained.contract_lineage().clone(), &campaign).map(Some)
}

fn persist(
    storage: &mut SharedRedbOperationalPorts,
    retained: &StoredApplicationInstallationCampaignV1,
    campaign: &riffdb_application::ApplicationInstallationCampaign,
    plan: &riffdb_application::ApplicationInstallationPlan,
) -> Result<(), ApplicationReimportMutationPortErrorV1> {
    let replacement = encode_state(campaign, plan)
        .map_err(|()| ApplicationReimportMutationPortErrorV1::Integrity)?;
    match storage
        .compare_and_swap_application_installation_campaign(Some(retained), &replacement)
        .map_err(map_mutation_storage)?
    {
        ApplicationInstallationCampaignWriteResultV1::Applied
        | ApplicationInstallationCampaignWriteResultV1::Unchanged => Ok(()),
        ApplicationInstallationCampaignWriteResultV1::CompareMismatch => {
            Err(ApplicationReimportMutationPortErrorV1::StorageUnavailable)
        }
    }
}

fn operation_result(
    lineage: ContractLineage,
    campaign: &riffdb_application::ApplicationInstallationCampaign,
) -> Result<ApplicationReimportOperationResultV1, ApplicationReimportMutationPortErrorV1> {
    let progress = campaign
        .reimport()
        .ok_or(ApplicationReimportMutationPortErrorV1::Integrity)?;
    let receipt = if progress.phase() == ApplicationReimportCampaignPhaseV1::Reconciled {
        let manifest = ApplicationPortabilityManifest::decode_canonical(
            progress.portability_manifest_document(),
        )
        .map_err(|_| ApplicationReimportMutationPortErrorV1::Integrity)?;
        Some(
            ApplicationReimportReceipt::decode_canonical(
                progress
                    .receipt_document()
                    .ok_or(ApplicationReimportMutationPortErrorV1::Integrity)?,
                &manifest,
            )
            .map_err(|_| ApplicationReimportMutationPortErrorV1::Integrity)?,
        )
    } else {
        None
    };
    ApplicationReimportOperationResultV1::new(lineage, progress.clone(), receipt)
        .map_err(|_| ApplicationReimportMutationPortErrorV1::Integrity)
}

fn parse_source(
    manifest_document: CanonicalApplicationExportJsonDocument,
    receipt_document: CanonicalApplicationExportJsonDocument,
    portability_manifest: &ApplicationPortabilityManifest,
    target_database_id: DatabaseId,
    requested_scope: CapabilityApplicationReimportScopeV1,
) -> Result<ApplicationReimportSourceV1, ApplicationReimportMutationPortErrorV1> {
    let manifest: PortabilityExportManifestV2 =
        serde_json::from_slice(manifest_document.as_bytes())
            .map_err(|_| ApplicationReimportMutationPortErrorV1::SourceMismatch)?;
    let receipt: PortabilityExportReceiptV2 =
        serde_json::from_slice(receipt_document.as_bytes())
            .map_err(|_| ApplicationReimportMutationPortErrorV1::SourceMismatch)?;
    if serde_json::to_vec(&manifest).ok().as_deref() != Some(manifest_document.as_bytes())
        || serde_json::to_vec(&receipt).ok().as_deref() != Some(receipt_document.as_bytes())
        || manifest.schema != PORTABILITY_MANIFEST_SCHEMA
        || receipt.schema != PORTABILITY_RECEIPT_SCHEMA
        || manifest.contract_lineage != portability_manifest.input().contract_lineage.as_str()
        || manifest.contract_version
            != portability_manifest
                .input()
                .contract_version
                .get()
                .to_string()
        || parse_hex32(&manifest.contract_bundle_hash)
            != Some(*portability_manifest.input().contract_bundle_hash.as_bytes())
        || parse_hex32(&manifest.portability_manifest_hash)
            != Some(*portability_manifest.identity().as_bytes())
        || manifest.phase != "completed"
        || receipt.phase != "completed"
        || !receipt.complete
        || receipt.failure.is_some()
        || !receipt.portability_intent
        || receipt.portability_manifest_hash != manifest.portability_manifest_hash
        || parse_hex32(&receipt.manifest_hash)
            != Some(*hash_application_export_manifest(manifest_document.as_bytes()).as_bytes())
    {
        return Err(ApplicationReimportMutationPortErrorV1::SourceMismatch);
    }
    let expected_scope = match requested_scope {
        CapabilityApplicationReimportScopeV1::PrincipalFiltered => "principal_filtered",
        CapabilityApplicationReimportScopeV1::WholeApplication => "whole_application",
    };
    if manifest.scope != expected_scope {
        return Err(ApplicationReimportMutationPortErrorV1::SourceMismatch);
    }
    let expected_classes = portability_manifest
        .input()
        .mappings
        .iter()
        .map(|mapping| match mapping.class() {
            PortableRecordClass::Entity => "entity",
            PortableRecordClass::Event => "event",
        })
        .collect::<BTreeSet<_>>();
    if manifest
        .selected_classes
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>()
        != expected_classes
        || manifest.page_hashes.len()
            != parse_u64(&manifest.pages)
                .and_then(|value| value.try_into().ok())
                .unwrap_or(usize::MAX)
    {
        return Err(ApplicationReimportMutationPortErrorV1::SourceMismatch);
    }
    let source_database_id = parse_uuid_v7(&manifest.database_id)
        .and_then(|bytes| DatabaseId::from_bytes(bytes).ok())
        .ok_or(ApplicationReimportMutationPortErrorV1::SourceMismatch)?;
    let page_hashes = manifest
        .page_hashes
        .iter()
        .map(|value| parse_hex32(value).map(ApplicationExportPageHash::from_bytes))
        .collect::<Option<Vec<_>>>()
        .ok_or(ApplicationReimportMutationPortErrorV1::SourceMismatch)?;
    let workflow_quiescence = manifest
        .workflow_lease_quiescence
        .into_iter()
        .map(|value| {
            ReimportWorkflowQuiescenceV1::new(
                riffdb_application::InstallationSymbol::new(value.workflow)
                    .map_err(|_| ApplicationReimportMutationPortErrorV1::SourceMismatch)?,
                parse_u64(&value.checked_rows)
                    .ok_or(ApplicationReimportMutationPortErrorV1::SourceMismatch)?,
                parse_u64(&value.quiescent_rows)
                    .ok_or(ApplicationReimportMutationPortErrorV1::SourceMismatch)?,
                parse_u64(&value.non_quiescent_rows)
                    .ok_or(ApplicationReimportMutationPortErrorV1::SourceMismatch)?,
            )
            .map_err(|_| ApplicationReimportMutationPortErrorV1::SourceMismatch)
        })
        .collect::<Result<Vec<_>, _>>()?;
    ApplicationReimportSourceV1::new(
        ApplicationExportManifestHash::from_bytes(
            *hash_application_export_manifest(manifest_document.as_bytes()).as_bytes(),
        ),
        ApplicationExportReceiptHash::from_bytes(
            *hash_application_export_receipt(receipt_document.as_bytes()).as_bytes(),
        ),
        portability_manifest.identity(),
        source_database_id,
        target_database_id,
        parse_u64(&manifest.rows).ok_or(ApplicationReimportMutationPortErrorV1::SourceMismatch)?,
        page_hashes,
        workflow_quiescence,
    )
    .map_err(|_| ApplicationReimportMutationPortErrorV1::SourceMismatch)
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PortabilityExportManifestV2 {
    administration_frontier: Option<String>,
    application_frontier: Option<String>,
    bytes: String,
    class_totals: BTreeMap<String, ExportClassTotalV2>,
    contract_bundle_hash: String,
    contract_lineage: String,
    contract_version: String,
    database_id: String,
    history_incarnation: String,
    omissions: Vec<String>,
    operation_id: String,
    page_hashes: Vec<String>,
    pages: String,
    phase: String,
    portability_manifest_hash: String,
    query_module_hashes: Vec<String>,
    reactive_module_hashes: Vec<String>,
    row_policy: Option<serde_json::Value>,
    rows: String,
    schema: String,
    scope: String,
    selected_classes: Vec<String>,
    workflow_lease_quiescence: Vec<WorkflowLeaseQuiescenceV2>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ExportClassTotalV2 {
    bytes: String,
    pages: String,
    rows: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WorkflowLeaseQuiescenceV2 {
    checked_rows: String,
    non_quiescent_rows: String,
    quiescent_rows: String,
    workflow: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PortabilityExportReceiptV2 {
    capability_id: String,
    capability_revision: String,
    complete: bool,
    failure: Option<String>,
    manifest_hash: String,
    operation_id: String,
    phase: String,
    portability_intent: bool,
    portability_manifest_hash: String,
    principal_actor_kind: String,
    principal_id: String,
    schema: String,
}

fn parse_u64(value: &str) -> Option<u64> {
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    value.parse().ok()
}

fn parse_hex32(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64 {
        return None;
    }
    let mut bytes = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Some(bytes)
}

fn parse_uuid_v7(value: &str) -> Option<[u8; 16]> {
    let mut compact = [0_u8; 32];
    let mut written = 0_usize;
    for (index, byte) in value.bytes().enumerate() {
        if matches!(index, 8 | 13 | 18 | 23) && value.len() == 36 {
            if byte != b'-' {
                return None;
            }
        } else {
            if written == compact.len() {
                return None;
            }
            compact[written] = byte;
            written += 1;
        }
    }
    if written != compact.len() {
        return None;
    }
    let mut bytes = [0_u8; 16];
    for (index, pair) in compact.chunks_exact(2).enumerate() {
        bytes[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Some(bytes)
}

fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

const fn map_observation_storage(error: StorageError) -> ApplicationReimportObservationPortErrorV1 {
    match error.kind() {
        StorageErrorKind::CorruptData | StorageErrorKind::InvariantViolation => {
            ApplicationReimportObservationPortErrorV1::Integrity
        }
        _ => ApplicationReimportObservationPortErrorV1::StorageUnavailable,
    }
}

const fn map_mutation_storage(error: StorageError) -> ApplicationReimportMutationPortErrorV1 {
    match error.kind() {
        StorageErrorKind::CorruptData | StorageErrorKind::InvariantViolation => {
            ApplicationReimportMutationPortErrorV1::Integrity
        }
        _ => ApplicationReimportMutationPortErrorV1::StorageUnavailable,
    }
}
