//! In-memory catalog, audit, and capability storage ports.

use std::num::NonZeroU64;

use riffdb_storage_api::{
    ActiveCatalogPointerV1, AdministrationAuditReader, AdministrationAuditScan,
    AdministrationAuditScanRequest, AuditPrincipalV1, CapabilityAdministrationOperationV1,
    CapabilityAdministrationTransactionPort, CapabilityBootstrapAdministrationRepository,
    CapabilityBootstrapIntentV1, CapabilityBootstrapMarkerV1, CapabilityBootstrapResult,
    CapabilityCreateAwaitingDecision, CapabilityCreateCandidateTransaction,
    CapabilityCreateCandidateV1, CapabilityCreateIntentV1, CapabilityCreateResult,
    CapabilityInventoryPageV1, CapabilityInventoryReader, CapabilityLifecycleV1,
    CapabilityLookupResult, CapabilityMutationCurrentStateV1, CapabilityReader,
    CapabilityRevokeAwaitingDecision, CapabilityRevokeCandidateTransaction,
    CapabilityRevokeCandidateV1, CapabilityRevokeIntentV1, CapabilityRevokeResult,
    CapabilityTokenLookupV1, CatalogActivationIntentV1, CatalogActivationResult,
    CatalogAdministrationRepository, CatalogRepository, EncodedPageItem, MAX_READABLE_DIGEST_KEYS,
    MAX_SCAN_PAGE_BYTES, RetainedMetadataV1, ServiceAuditAppendIntentV1,
    ServiceAuditAppendRepository, ServiceAuditAppendResult, StorageError, StorageErrorKind,
    StorageScanLimit, StoredAdministrationAuditRecordV1, StoredCapabilityAdministrationV1,
    StoredCapabilityRecordV1, StoredCatalogAdministrationV1, StoredContractBundleV1,
    StoredServiceAuditRecordV1, TransactionCurrentCapabilityObservationV1,
};
use riffdb_types::{
    AdministrationSequence, CapabilityId, CapabilityTokenDigest, ContractLineage, ContractVersion,
    RequestId, ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceAuditTargetV1, ServiceOperationV1,
};

use crate::state::{
    CapabilityLookupRow, CatalogActivationIndexRow, CatalogBundleActivationIndexRow,
    CatalogBundleRow, KeyedSyntheticCharge, MemoryMetadataSlot, MemoryState, PreparedMemoryDelta,
    ServiceAuditInvocationIndexRow, ServiceAuditLifecycleIndex, SyntheticRecordCharge,
    bundle_evidence_order_key, bundle_identity_evidence_order_key, memory_record_charge,
    unique_binary_search_by,
};
use crate::store::{MemoryAccess, MemoryOperationalPorts, storage_error};

/// A complete administration post-image prepared while the exclusive gate is held.
///
/// Replacing the small reference-model tables keeps application infallible after
/// every semantic check and makes cross-index updates visibly atomic.
struct AdministrationMutation<O> {
    metadata: RetainedMetadataV1,
    catalog_bundles: Vec<CatalogBundleRow>,
    catalog_activations: Vec<CatalogActivationIndexRow>,
    catalog_bundle_activations: Vec<CatalogBundleActivationIndexRow>,
    administration_audit: Vec<StoredAdministrationAuditRecordV1>,
    service_audit_invocations: Vec<ServiceAuditInvocationIndexRow>,
    capabilities: Vec<StoredCapabilityRecordV1>,
    capability_lookups: Vec<CapabilityLookupRow>,
    administration_charges: Vec<KeyedSyntheticCharge<AdministrationSequence>>,
    output: O,
}

enum AdministrationPreparation<O> {
    NoChange(O),
    Apply(Box<AdministrationMutation<O>>),
}

impl<O> PreparedMemoryDelta for AdministrationPreparation<O> {
    type Output = O;

    fn apply(self, state: &mut MemoryState) -> Self::Output {
        match self {
            Self::NoChange(output) => output,
            Self::Apply(mutation) => (*mutation).apply(state),
        }
    }
}

impl<O> AdministrationMutation<O> {
    fn from_state(state: &MemoryState, output: O) -> Result<Self, StorageError> {
        Ok(Self {
            metadata: retained_metadata(state)?.clone(),
            catalog_bundles: state.catalog_bundles.clone(),
            catalog_activations: state.catalog_activations.clone(),
            catalog_bundle_activations: state.catalog_bundle_activations.clone(),
            administration_audit: state.administration_audit.clone(),
            service_audit_invocations: state.service_audit_invocations.clone(),
            capabilities: state.capabilities.clone(),
            capability_lookups: state.capability_lookups.clone(),
            administration_charges: state.synthetic_charges.administration_audit.clone(),
            output,
        })
    }

    fn apply(self, state: &mut MemoryState) -> O {
        state.metadata = MemoryMetadataSlot::Retained(self.metadata);
        state.catalog_bundles = self.catalog_bundles;
        state.catalog_activations = self.catalog_activations;
        state.catalog_bundle_activations = self.catalog_bundle_activations;
        state.administration_audit = self.administration_audit;
        state.service_audit_invocations = self.service_audit_invocations;
        state.capabilities = self.capabilities;
        state.capability_lookups = self.capability_lookups;
        state.synthetic_charges.administration_audit = self.administration_charges;
        self.output
    }

    fn append_audit(&mut self, record: StoredAdministrationAuditRecordV1) {
        let sequence = record.administration_sequence();
        self.administration_audit.push(record);
        self.administration_charges.push(KeyedSyntheticCharge {
            key: sequence,
            charge: SyntheticRecordCharge::new(memory_record_charge()),
        });
    }
}

fn retained_metadata(state: &MemoryState) -> Result<&RetainedMetadataV1, StorageError> {
    match &state.metadata {
        MemoryMetadataSlot::Retained(metadata) => Ok(metadata),
        MemoryMetadataSlot::Absent => Err(storage_error(StorageErrorKind::CorruptData)),
        #[cfg(test)]
        MemoryMetadataSlot::Corrupt => Err(storage_error(StorageErrorKind::CorruptData)),
    }
}

fn metadata_with_links(
    allocated: RetainedMetadataV1,
    active_catalog: Option<ActiveCatalogPointerV1>,
    capability_bootstrap: Option<CapabilityBootstrapMarkerV1>,
) -> Result<RetainedMetadataV1, StorageError> {
    RetainedMetadataV1::new(
        allocated.storage_format_version(),
        allocated.database_id(),
        allocated.application_sequence(),
        allocated.administration_sequence(),
        active_catalog,
        capability_bootstrap,
    )
    .map_err(|_| storage_error(StorageErrorKind::CorruptData))
}

fn validate_administration_stream(state: &MemoryState) -> Result<(), StorageError> {
    let metadata = retained_metadata(state)?;
    if state.administration_audit.len() != state.synthetic_charges.administration_audit.len() {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }

    let mut expected = Some(AdministrationSequence::first());
    for (record, charge) in state
        .administration_audit
        .iter()
        .zip(&state.synthetic_charges.administration_audit)
    {
        let sequence = record.administration_sequence();
        if expected != Some(sequence)
            || charge.key != sequence
            || charge.charge.encoded_content_charge() != memory_record_charge()
        {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        expected = sequence.checked_next();
    }

    let allocator_matches = match expected {
        Some(next) => {
            metadata.administration_sequence()
                == riffdb_storage_api::AdministrationSequenceAllocator::next(next)
        }
        None => {
            metadata.administration_sequence()
                == riffdb_storage_api::AdministrationSequenceAllocator::Exhausted
        }
    };
    if !allocator_matches {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    Ok(())
}

fn append_sequence(
    state: &MemoryState,
) -> Result<(AdministrationSequence, RetainedMetadataV1), StorageError> {
    validate_administration_stream(state)?;
    let allocation = state.prepare_administration_allocation(1)?;
    let sequence = allocation.assigned()[0];
    Ok((sequence, allocation.into_metadata_post_image()))
}

fn catalog_bundle_position(
    state: &MemoryState,
    lineage: &ContractLineage,
    version: ContractVersion,
) -> Result<Option<usize>, StorageError> {
    let lower_bound = bundle_identity_evidence_order_key(
        lineage,
        version,
        riffdb_types::ContractBundleHash::from_bytes([0; 32]),
    );
    let position = match unique_binary_search_by(&state.catalog_bundles, |row| {
        row.order_key.cmp(&lower_bound)
    })? {
        Ok(index) | Err(index) => index,
    };
    let Some(row) = state.catalog_bundles.get(position) else {
        return Ok(None);
    };
    if row.bundle.lineage() != lineage || row.bundle.contract_version() != version {
        return Ok(None);
    }
    if state.catalog_bundles.get(position + 1).is_some_and(|next| {
        next.bundle.lineage() == lineage && next.bundle.contract_version() == version
    }) {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    Ok(Some(position))
}

fn capability_position(
    state: &MemoryState,
    capability_id: CapabilityId,
) -> Result<Result<usize, usize>, StorageError> {
    unique_binary_search_by(&state.capabilities, |record| {
        record.capability_id().cmp(&capability_id)
    })
}

fn capability_lookup_position(
    state: &MemoryState,
    digest: CapabilityTokenDigest,
) -> Result<Result<usize, usize>, StorageError> {
    unique_binary_search_by(&state.capability_lookups, |row| row.digest.cmp(&digest))
}

fn service_invocation_position(
    state: &MemoryState,
    request_id: RequestId,
) -> Result<Result<usize, usize>, StorageError> {
    unique_binary_search_by(&state.service_audit_invocations, |row| {
        row.request_id.cmp(&request_id)
    })
}

fn administration_record(
    state: &MemoryState,
    sequence: AdministrationSequence,
) -> Result<&StoredAdministrationAuditRecordV1, StorageError> {
    let index = sequence
        .get()
        .checked_sub(1)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    state
        .administration_audit
        .get(index)
        .filter(|record| record.administration_sequence() == sequence)
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))
}

fn checked_capability(
    state: &MemoryState,
    capability_id: CapabilityId,
) -> Result<Option<&StoredCapabilityRecordV1>, StorageError> {
    let metadata = retained_metadata(state)?;
    let Ok(index) = capability_position(state, capability_id)? else {
        return Ok(None);
    };
    let record = &state.capabilities[index];
    if record.database_id() != metadata.database_id() {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    let Ok(lookup_index) = capability_lookup_position(state, record.token_digest())? else {
        return Err(storage_error(StorageErrorKind::CorruptData));
    };
    if state.capability_lookups[lookup_index].value.capability_id() != capability_id {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    Ok(Some(record))
}

fn capability_observation(
    state: &MemoryState,
    capability_id: CapabilityId,
) -> Result<Option<TransactionCurrentCapabilityObservationV1>, StorageError> {
    Ok(checked_capability(state, capability_id)?
        .map(TransactionCurrentCapabilityObservationV1::from_record))
}

fn principal_matches_observation(
    principal: &AuditPrincipalV1,
    observation: Option<&TransactionCurrentCapabilityObservationV1>,
) -> bool {
    observation.is_some_and(|current| {
        current.capability_id() == principal.capability_id()
            && current.revision() == principal.capability_revision()
            && current.principal_id() == principal.principal_id()
            && current.actor_kind() == principal.actor_kind()
    })
}

fn resolve_capability_digests_in_state(
    state: &MemoryState,
    candidates: &[CapabilityTokenDigest],
) -> Result<CapabilityLookupResult, StorageError> {
    retained_metadata(state)?;
    if candidates.is_empty() {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    if candidates.len() > MAX_READABLE_DIGEST_KEYS {
        return Err(storage_error(StorageErrorKind::LimitExceeded));
    }
    let mut ordered = candidates.to_vec();
    ordered.sort_unstable();
    if ordered.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }

    let mut matched = None;
    let mut multiple_matches = false;
    for digest in candidates {
        let Ok(index) = capability_lookup_position(state, *digest)? else {
            continue;
        };
        let lookup = &state.capability_lookups[index];
        let Some(record) = checked_capability(state, lookup.value.capability_id())? else {
            return Err(storage_error(StorageErrorKind::CorruptData));
        };
        if record.token_digest() != *digest {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        if matched.is_some() {
            multiple_matches = true;
        } else {
            matched = Some(record.clone());
        }
    }
    if multiple_matches {
        return Ok(CapabilityLookupResult::MultipleMatches);
    }
    Ok(matched.map_or(CapabilityLookupResult::NotFound, |record| {
        CapabilityLookupResult::Found(Box::new(record))
    }))
}

impl CatalogRepository for MemoryOperationalPorts {
    fn read_active_catalog(&self) -> Result<Option<ActiveCatalogPointerV1>, StorageError> {
        self.read(|state| {
            let active = retained_metadata(state)?.active_catalog().cloned();
            let Some(pointer) = &active else {
                if !state.catalog_activations.is_empty() {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                return Ok(None);
            };
            let Some(last) = state.catalog_activations.last() else {
                return Err(storage_error(StorageErrorKind::CorruptData));
            };
            let StoredAdministrationAuditRecordV1::Catalog(record) =
                administration_record(state, last.administration_sequence)?
            else {
                return Err(storage_error(StorageErrorKind::CorruptData));
            };
            if record.activated() != pointer {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            let Some(bundle) = self.read_contract_bundle_from_state(
                state,
                pointer.lineage(),
                pointer.contract_version(),
            )?
            else {
                return Err(storage_error(StorageErrorKind::CorruptData));
            };
            if !pointer.matches_bundle(&bundle) {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            Ok(active)
        })
    }

    fn read_contract_bundle(
        &self,
        lineage: &ContractLineage,
        contract_version: ContractVersion,
    ) -> Result<Option<StoredContractBundleV1>, StorageError> {
        self.read(|state| {
            retained_metadata(state)?;
            self.read_contract_bundle_from_state(state, lineage, contract_version)
        })
    }
}

impl MemoryOperationalPorts {
    fn read_contract_bundle_from_state(
        &self,
        state: &MemoryState,
        lineage: &ContractLineage,
        version: ContractVersion,
    ) -> Result<Option<StoredContractBundleV1>, StorageError> {
        let Some(index) = catalog_bundle_position(state, lineage, version)? else {
            return Ok(None);
        };
        let row = &state.catalog_bundles[index];
        if row.order_key != bundle_evidence_order_key(&row.bundle) {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        Ok(Some(row.bundle.clone()))
    }
}

impl CatalogAdministrationRepository for MemoryOperationalPorts {
    fn activate_catalog(
        &mut self,
        intent: &CatalogActivationIntentV1,
    ) -> Result<CatalogActivationResult, StorageError> {
        self.apply_prepared(|state| prepare_catalog_activation(state, intent))
    }
}

fn prepare_catalog_activation(
    state: &MemoryState,
    intent: &CatalogActivationIntentV1,
) -> Result<AdministrationPreparation<CatalogActivationResult>, StorageError> {
    validate_administration_stream(state)?;
    let existing = catalog_bundle_position(
        state,
        intent.bundle().lineage(),
        intent.bundle().contract_version(),
    )?;
    if let Some(index) = existing
        && state.catalog_bundles[index].bundle != *intent.bundle()
    {
        return Ok(AdministrationPreparation::NoChange(
            CatalogActivationResult::BundleConflict,
        ));
    }

    let metadata = retained_metadata(state)?;
    let requested = intent.requested_active();
    if metadata.active_catalog() == Some(&requested) {
        let key = bundle_evidence_order_key(intent.bundle());
        let Ok(index) = unique_binary_search_by(&state.catalog_bundle_activations, |row| {
            row.order_key.cmp(&key)
        })?
        else {
            return Err(storage_error(StorageErrorKind::CorruptData));
        };
        let sequence = state.catalog_bundle_activations[index].administration_sequence;
        let StoredAdministrationAuditRecordV1::Catalog(record) =
            administration_record(state, sequence)?
        else {
            return Err(storage_error(StorageErrorKind::CorruptData));
        };
        if record.activated() != &requested {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        return Ok(AdministrationPreparation::NoChange(
            CatalogActivationResult::AlreadyActive {
                active: requested,
                administration_sequence: sequence,
            },
        ));
    }

    let actual = metadata
        .active_catalog()
        .map(ActiveCatalogPointerV1::contract_version);
    if actual != intent.expected_active_version() {
        return Ok(AdministrationPreparation::NoChange(
            CatalogActivationResult::ExpectedActiveVersionMismatch { actual },
        ));
    }

    let (sequence, allocated) = append_sequence(state)?;
    let record = StoredCatalogAdministrationV1::from_committed_intent(
        sequence,
        intent,
        metadata.active_catalog().cloned(),
    )
    .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
    let mut mutation = AdministrationMutation::from_state(
        state,
        CatalogActivationResult::Activated {
            active: requested.clone(),
            administration_sequence: sequence,
        },
    )?;
    let key = bundle_evidence_order_key(intent.bundle());
    if existing.is_none() {
        let insertion = match mutation
            .catalog_bundles
            .binary_search_by(|row| row.order_key.cmp(&key))
        {
            Ok(_) => return Err(storage_error(StorageErrorKind::CorruptData)),
            Err(index) => index,
        };
        mutation
            .catalog_bundles
            .insert(insertion, CatalogBundleRow::new(intent.bundle().clone()));
    }
    mutation
        .catalog_activations
        .push(CatalogActivationIndexRow {
            administration_sequence: sequence,
        });
    match mutation
        .catalog_bundle_activations
        .binary_search_by(|row| row.order_key.cmp(&key))
    {
        Ok(index) => {
            mutation.catalog_bundle_activations[index].administration_sequence = sequence;
        }
        Err(index) => mutation.catalog_bundle_activations.insert(
            index,
            CatalogBundleActivationIndexRow {
                order_key: key,
                administration_sequence: sequence,
            },
        ),
    }
    mutation.append_audit(StoredAdministrationAuditRecordV1::Catalog(record));
    mutation.metadata =
        metadata_with_links(allocated, Some(requested), metadata.capability_bootstrap())?;
    Ok(AdministrationPreparation::Apply(Box::new(mutation)))
}

impl ServiceAuditAppendRepository for MemoryOperationalPorts {
    fn append_service_audit(
        &mut self,
        intent: &ServiceAuditAppendIntentV1,
    ) -> Result<ServiceAuditAppendResult, StorageError> {
        self.apply_prepared(|state| prepare_service_audit_append(state, intent))
    }
}

fn prepare_service_audit_append(
    state: &MemoryState,
    intent: &ServiceAuditAppendIntentV1,
) -> Result<AdministrationPreparation<ServiceAuditAppendResult>, StorageError> {
    validate_administration_stream(state)?;
    let lifecycle_position = service_invocation_position(state, intent.request_id())?;
    let next_lifecycle = match lifecycle_position {
        Err(_) => match intent.phase() {
            ServiceAuditPhaseV1::Started
                if intent.principal().is_some() && intent.link() == ServiceAuditLinkV1::None =>
            {
                Some(ServiceAuditLifecycleIndex::Started {
                    started_sequence: AdministrationSequence::first(),
                    terminal_sequence: None,
                })
            }
            ServiceAuditPhaseV1::Denied
            | ServiceAuditPhaseV1::Cancelled
            | ServiceAuditPhaseV1::Failed
                if intent.principal().is_some() && intent.link() == ServiceAuditLinkV1::None =>
            {
                Some(ServiceAuditLifecycleIndex::Standalone {
                    sequence: AdministrationSequence::first(),
                })
            }
            _ => None,
        },
        Ok(index) => match state.service_audit_invocations[index].lifecycle {
            ServiceAuditLifecycleIndex::Standalone { .. } => None,
            ServiceAuditLifecycleIndex::Started {
                started_sequence,
                terminal_sequence: None,
            } if intent.phase() != ServiceAuditPhaseV1::Started => {
                let StoredAdministrationAuditRecordV1::Service(started) =
                    administration_record(state, started_sequence)?
                else {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                };
                let common_matches = intent.request_id() == started.request_id()
                    && intent.operation() == started.operation()
                    && intent.principal() == started.principal()
                    && intent.ingress() == started.ingress()
                    && intent.targets() == started.targets()
                    && intent.approval_id() == started.approval_id();
                let bootstrap_matches = started.principal().is_some()
                    || (intent.phase() == ServiceAuditPhaseV1::Succeeded
                        && intent.link() == started.link());
                (common_matches && bootstrap_matches).then_some(
                    ServiceAuditLifecycleIndex::Started {
                        started_sequence,
                        terminal_sequence: Some(AdministrationSequence::first()),
                    },
                )
            }
            ServiceAuditLifecycleIndex::Started { .. } => None,
        },
    };
    let Some(mut next_lifecycle) = next_lifecycle else {
        return Ok(AdministrationPreparation::NoChange(
            ServiceAuditAppendResult::PhaseConflict,
        ));
    };
    if !service_link_is_valid(state, intent) {
        return Ok(AdministrationPreparation::NoChange(
            ServiceAuditAppendResult::PhaseConflict,
        ));
    }

    let (sequence, allocated) = append_sequence(state)?;
    match &mut next_lifecycle {
        ServiceAuditLifecycleIndex::Standalone { sequence: target }
        | ServiceAuditLifecycleIndex::Started {
            started_sequence: target,
            terminal_sequence: None,
        } => *target = sequence,
        ServiceAuditLifecycleIndex::Started {
            terminal_sequence: Some(target),
            ..
        } => *target = sequence,
    }
    let record = StoredServiceAuditRecordV1::from_intent(sequence, intent);
    let mut mutation = AdministrationMutation::from_state(
        state,
        ServiceAuditAppendResult::Appended(record.clone()),
    )?;
    match lifecycle_position {
        Ok(index) => mutation.service_audit_invocations[index].lifecycle = next_lifecycle,
        Err(index) => mutation.service_audit_invocations.insert(
            index,
            ServiceAuditInvocationIndexRow {
                request_id: intent.request_id(),
                lifecycle: next_lifecycle,
            },
        ),
    }
    mutation.append_audit(StoredAdministrationAuditRecordV1::Service(record));
    mutation.metadata = allocated;
    Ok(AdministrationPreparation::Apply(Box::new(mutation)))
}

fn service_link_is_valid(state: &MemoryState, intent: &ServiceAuditAppendIntentV1) -> bool {
    match intent.link() {
        ServiceAuditLinkV1::None => true,
        ServiceAuditLinkV1::Command {
            commit_sequence,
            provenance_id,
        } => {
            state
                .commits
                .binary_search_by_key(&commit_sequence, |commit| commit.commit_sequence())
                .ok()
                .is_some_and(|index| state.commits[index].provenance_id() == provenance_id)
                && state
                    .provenance
                    .binary_search_by_key(&provenance_id, |record| record.provenance_id())
                    .ok()
                    .is_some_and(|index| {
                        state.provenance[index].commit_sequence() == commit_sequence
                    })
        }
        ServiceAuditLinkV1::ControlPlane {
            administration_sequence,
        } => {
            let Ok(target) = administration_record(state, administration_sequence) else {
                return false;
            };
            match (intent.operation(), target) {
                (
                    ServiceOperationV1::DeployContract,
                    StoredAdministrationAuditRecordV1::Catalog(_),
                ) => true,
                (
                    ServiceOperationV1::CreateCapability,
                    StoredAdministrationAuditRecordV1::Capability(record),
                ) => {
                    matches!(
                        record.operation(),
                        CapabilityAdministrationOperationV1::Bootstrap
                            | CapabilityAdministrationOperationV1::Create
                    ) && intent
                        .targets()
                        .as_slice()
                        .contains(&ServiceAuditTargetV1::Capability(
                            record.target_capability_id(),
                        ))
                }
                (
                    ServiceOperationV1::RevokeCapability,
                    StoredAdministrationAuditRecordV1::Capability(record),
                ) => {
                    record.operation() == CapabilityAdministrationOperationV1::Revoke
                        && intent
                            .targets()
                            .as_slice()
                            .contains(&ServiceAuditTargetV1::Capability(
                                record.target_capability_id(),
                            ))
                }
                _ => false,
            }
        }
    }
}

impl AdministrationAuditReader for MemoryOperationalPorts {
    fn scan_administration_audit(
        &self,
        request: AdministrationAuditScanRequest,
    ) -> Result<AdministrationAuditScan, StorageError> {
        self.read(|state| {
            validate_administration_stream(state)?;
            let start = request.after().map_or(0, |sequence| {
                usize::try_from(sequence.get()).unwrap_or(usize::MAX)
            });
            let mut records = Vec::new();
            let mut bytes = 0usize;
            for (record, charge) in state
                .administration_audit
                .iter()
                .zip(&state.synthetic_charges.administration_audit)
                .skip(start)
            {
                if records.len() == usize::from(request.limit().get()) {
                    break;
                }
                let next = bytes
                    .checked_add(charge.charge.encoded_content_charge().get())
                    .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
                if next > MAX_SCAN_PAGE_BYTES {
                    break;
                }
                bytes = next;
                records.push(EncodedPageItem::new(
                    record.clone(),
                    charge.charge.encoded_content_charge(),
                ));
            }
            let consumed_end = start.saturating_add(records.len());
            let has_more = consumed_end < state.administration_audit.len();
            if records.is_empty() && has_more {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            AdministrationAuditScan::page(request, records, has_more)
                .map_err(|_| storage_error(StorageErrorKind::CorruptData))
        })
    }
}

impl CapabilityReader for MemoryOperationalPorts {
    fn read_capability(
        &self,
        capability_id: CapabilityId,
    ) -> Result<Option<StoredCapabilityRecordV1>, StorageError> {
        self.read(|state| Ok(checked_capability(state, capability_id)?.cloned()))
    }

    fn resolve_capability_digests(
        &self,
        candidates: &[CapabilityTokenDigest],
    ) -> Result<CapabilityLookupResult, StorageError> {
        self.read(|state| resolve_capability_digests_in_state(state, candidates))
    }
}

impl CapabilityInventoryReader for MemoryOperationalPorts {
    fn scan_capabilities(
        &self,
        after: Option<CapabilityId>,
        limit: StorageScanLimit,
    ) -> Result<CapabilityInventoryPageV1, StorageError> {
        self.read(|state| {
            let mut records = Vec::new();
            let mut bytes = 0usize;
            let mut has_more = false;
            for record in &state.capabilities {
                if after.is_some_and(|after| record.capability_id() <= after) {
                    continue;
                }
                let next_bytes = bytes
                    .checked_add(
                        record
                            .semantic_bytes()
                            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?,
                    )
                    .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
                if records.len() == usize::from(limit.get()) || next_bytes > MAX_SCAN_PAGE_BYTES {
                    has_more = true;
                    break;
                }
                bytes = next_bytes;
                records.push(record.clone());
            }
            CapabilityInventoryPageV1::new(after, limit, records, has_more)
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))
        })
    }
}

/// Gate-owning memory create transaction before transaction-current facts are read.
pub struct MemoryCapabilityCreateCandidate {
    access: MemoryAccess,
    candidate: CapabilityCreateCandidateV1,
}

/// Gate-owning memory create transaction after facts are frozen for policy.
pub struct MemoryCapabilityCreateAwaiting {
    access: MemoryAccess,
    candidate: CapabilityCreateCandidateV1,
    current: CapabilityMutationCurrentStateV1,
}

/// Gate-owning memory revoke transaction before transaction-current facts are read.
pub struct MemoryCapabilityRevokeCandidate {
    access: MemoryAccess,
    candidate: CapabilityRevokeCandidateV1,
}

/// Gate-owning memory revoke transaction after facts are frozen for policy.
pub struct MemoryCapabilityRevokeAwaiting {
    access: MemoryAccess,
    candidate: CapabilityRevokeCandidateV1,
    current: CapabilityMutationCurrentStateV1,
}

impl CapabilityAdministrationTransactionPort for MemoryOperationalPorts {
    type CreateCandidate = MemoryCapabilityCreateCandidate;
    type RevokeCandidate = MemoryCapabilityRevokeCandidate;

    fn begin_capability_create(
        &self,
        candidate: CapabilityCreateCandidateV1,
    ) -> Result<Self::CreateCandidate, StorageError> {
        Ok(MemoryCapabilityCreateCandidate {
            access: self.acquire()?,
            candidate,
        })
    }

    fn begin_capability_revoke(
        &self,
        candidate: CapabilityRevokeCandidateV1,
    ) -> Result<Self::RevokeCandidate, StorageError> {
        Ok(MemoryCapabilityRevokeCandidate {
            access: self.acquire()?,
            candidate,
        })
    }
}

impl CapabilityCreateCandidateTransaction for MemoryCapabilityCreateCandidate {
    type AwaitingDecision = MemoryCapabilityCreateAwaiting;

    fn read_transaction_current(
        self,
    ) -> Result<(Self::AwaitingDecision, CapabilityMutationCurrentStateV1), StorageError> {
        let current = self.access.read(|state| {
            let authorizing =
                capability_observation(state, self.candidate.initiator().capability_id())?;
            let target = capability_observation(state, self.candidate.capability_id())?;
            CapabilityMutationCurrentStateV1::for_create(&self.candidate, authorizing, target)
                .map_err(|_| storage_error(StorageErrorKind::CorruptData))
        })?;
        Ok((
            MemoryCapabilityCreateAwaiting {
                access: self.access,
                candidate: self.candidate,
                current: current.clone(),
            },
            current,
        ))
    }

    fn abandon(self) -> CapabilityCreateCandidateV1 {
        self.candidate
    }
}

impl CapabilityCreateAwaitingDecision for MemoryCapabilityCreateAwaiting {
    fn commit_create(
        self,
        intent: CapabilityCreateIntentV1,
    ) -> Result<CapabilityCreateResult, StorageError> {
        if !intent.matches_candidate(&self.candidate)
            || !principal_matches_observation(intent.initiator(), self.current.authorizing())
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        self.access.write(|state| {
            let observed = current_create_state(state, &self.candidate)?;
            if observed != self.current {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            let prepared = prepare_capability_create(state, &intent)?;
            Ok(prepared.apply(state))
        })
    }

    fn abandon(self) -> CapabilityCreateCandidateV1 {
        self.candidate
    }
}

fn current_create_state(
    state: &MemoryState,
    candidate: &CapabilityCreateCandidateV1,
) -> Result<CapabilityMutationCurrentStateV1, StorageError> {
    CapabilityMutationCurrentStateV1::for_create(
        candidate,
        capability_observation(state, candidate.initiator().capability_id())?,
        capability_observation(state, candidate.capability_id())?,
    )
    .map_err(|_| storage_error(StorageErrorKind::CorruptData))
}

fn prepare_capability_create(
    state: &MemoryState,
    intent: &CapabilityCreateIntentV1,
) -> Result<AdministrationPreparation<CapabilityCreateResult>, StorageError> {
    validate_administration_stream(state)?;
    if intent.requested().database_id() != retained_metadata(state)?.database_id() {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    if let Ok(index) = capability_position(state, intent.capability_id())? {
        let record = checked_capability(state, intent.capability_id())?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        let result = if record.matches_requested(intent.requested()) {
            CapabilityCreateResult::AlreadyCreated {
                capability_id: record.capability_id(),
                revision: record.revision(),
                administration_sequence: record.creation_sequence(),
            }
        } else {
            CapabilityCreateResult::CapabilityIdConflict
        };
        if state.capabilities[index].capability_id() != intent.capability_id() {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        return Ok(AdministrationPreparation::NoChange(result));
    }
    if let Ok(index) = capability_lookup_position(state, intent.token_digest())? {
        let target = state.capability_lookups[index].value.capability_id();
        checked_capability(state, target)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        return Ok(AdministrationPreparation::NoChange(
            CapabilityCreateResult::TokenDigestCollision,
        ));
    }

    let (sequence, allocated) = append_sequence(state)?;
    let capability = StoredCapabilityRecordV1::active(
        intent.capability_id(),
        intent.token_digest(),
        intent.requested().clone(),
        intent.issued_at(),
        intent.expires_at(),
        sequence,
        intent.request_id(),
    )
    .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    let audit = StoredCapabilityAdministrationV1::new(
        sequence,
        intent.request_id(),
        CapabilityAdministrationOperationV1::Create,
        intent.issued_at(),
        Some(intent.initiator().clone()),
        intent.capability_id(),
        NonZeroU64::MIN,
        intent.approval_id().cloned(),
        None,
    )
    .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    let mut mutation = AdministrationMutation::from_state(
        state,
        CapabilityCreateResult::Created {
            capability_id: intent.capability_id(),
            revision: NonZeroU64::MIN,
            administration_sequence: sequence,
        },
    )?;
    let capability_index = match mutation
        .capabilities
        .binary_search_by_key(&intent.capability_id(), |record| record.capability_id())
    {
        Ok(_) => return Err(storage_error(StorageErrorKind::CorruptData)),
        Err(index) => index,
    };
    mutation.capabilities.insert(capability_index, capability);
    let lookup_index = match mutation
        .capability_lookups
        .binary_search_by_key(&intent.token_digest(), |row| row.digest)
    {
        Ok(_) => return Err(storage_error(StorageErrorKind::CorruptData)),
        Err(index) => index,
    };
    mutation.capability_lookups.insert(
        lookup_index,
        CapabilityLookupRow {
            digest: intent.token_digest(),
            value: CapabilityTokenLookupV1::new(intent.capability_id()),
        },
    );
    mutation.append_audit(StoredAdministrationAuditRecordV1::Capability(audit));
    mutation.metadata = allocated;
    Ok(AdministrationPreparation::Apply(Box::new(mutation)))
}

impl CapabilityRevokeCandidateTransaction for MemoryCapabilityRevokeCandidate {
    type AwaitingDecision = MemoryCapabilityRevokeAwaiting;

    fn read_transaction_current(
        self,
    ) -> Result<(Self::AwaitingDecision, CapabilityMutationCurrentStateV1), StorageError> {
        let current = self.access.read(|state| {
            let authorizing =
                capability_observation(state, self.candidate.initiator().capability_id())?;
            let target = capability_observation(state, self.candidate.capability_id())?;
            CapabilityMutationCurrentStateV1::for_revoke(&self.candidate, authorizing, target)
                .map_err(|_| storage_error(StorageErrorKind::CorruptData))
        })?;
        Ok((
            MemoryCapabilityRevokeAwaiting {
                access: self.access,
                candidate: self.candidate,
                current: current.clone(),
            },
            current,
        ))
    }

    fn abandon(self) -> CapabilityRevokeCandidateV1 {
        self.candidate
    }
}

impl CapabilityRevokeAwaitingDecision for MemoryCapabilityRevokeAwaiting {
    fn commit_revoke(
        self,
        intent: CapabilityRevokeIntentV1,
    ) -> Result<CapabilityRevokeResult, StorageError> {
        if !intent.matches_candidate(&self.candidate)
            || !principal_matches_observation(intent.initiator(), self.current.authorizing())
            || self
                .current
                .target()
                .is_some_and(|target| target.revision() != intent.expected_revision())
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        self.access.write(|state| {
            let observed = current_revoke_state(state, &self.candidate)?;
            if observed != self.current {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            let prepared = prepare_capability_revoke(state, &intent)?;
            Ok(prepared.apply(state))
        })
    }

    fn abandon(self) -> CapabilityRevokeCandidateV1 {
        self.candidate
    }
}

fn current_revoke_state(
    state: &MemoryState,
    candidate: &CapabilityRevokeCandidateV1,
) -> Result<CapabilityMutationCurrentStateV1, StorageError> {
    CapabilityMutationCurrentStateV1::for_revoke(
        candidate,
        capability_observation(state, candidate.initiator().capability_id())?,
        capability_observation(state, candidate.capability_id())?,
    )
    .map_err(|_| storage_error(StorageErrorKind::CorruptData))
}

fn prepare_capability_revoke(
    state: &MemoryState,
    intent: &CapabilityRevokeIntentV1,
) -> Result<AdministrationPreparation<CapabilityRevokeResult>, StorageError> {
    validate_administration_stream(state)?;
    let Ok(index) = capability_position(state, intent.capability_id())? else {
        return Ok(AdministrationPreparation::NoChange(
            CapabilityRevokeResult::CapabilityNotFound,
        ));
    };
    let record = checked_capability(state, intent.capability_id())?
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    if record.revision() != intent.expected_revision() {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    if let CapabilityLifecycleV1::Revoked {
        administration_sequence,
        ..
    } = record.lifecycle()
    {
        return Ok(AdministrationPreparation::NoChange(
            CapabilityRevokeResult::AlreadyRevoked {
                capability_id: record.capability_id(),
                revision: record.revision(),
                administration_sequence: *administration_sequence,
            },
        ));
    }

    let (sequence, allocated) = append_sequence(state)?;
    let revoked = record
        .revoked(
            intent.expected_revision(),
            intent.revoked_at(),
            sequence,
            intent.reason(),
        )
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    let audit = StoredCapabilityAdministrationV1::new(
        sequence,
        intent.request_id(),
        CapabilityAdministrationOperationV1::Revoke,
        intent.revoked_at(),
        Some(intent.initiator().clone()),
        intent.capability_id(),
        revoked.revision(),
        intent.approval_id().cloned(),
        Some(intent.reason()),
    )
    .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    let mut mutation = AdministrationMutation::from_state(
        state,
        CapabilityRevokeResult::Revoked {
            capability_id: intent.capability_id(),
            revision: revoked.revision(),
            administration_sequence: sequence,
        },
    )?;
    mutation.capabilities[index] = revoked;
    mutation.append_audit(StoredAdministrationAuditRecordV1::Capability(audit));
    mutation.metadata = allocated;
    Ok(AdministrationPreparation::Apply(Box::new(mutation)))
}

impl CapabilityBootstrapAdministrationRepository for MemoryOperationalPorts {
    fn bootstrap_capability(
        &mut self,
        intent: &CapabilityBootstrapIntentV1,
    ) -> Result<CapabilityBootstrapResult, StorageError> {
        self.apply_prepared(|state| prepare_capability_bootstrap(state, intent))
    }
}

fn prepare_capability_bootstrap(
    state: &MemoryState,
    intent: &CapabilityBootstrapIntentV1,
) -> Result<AdministrationPreparation<CapabilityBootstrapResult>, StorageError> {
    validate_administration_stream(state)?;
    let metadata = retained_metadata(state)?;
    if intent.requested().database_id() != metadata.database_id()
        || !intent
            .start()
            .targets()
            .as_slice()
            .contains(&ServiceAuditTargetV1::Capability(intent.capability_id()))
    {
        return Ok(AdministrationPreparation::NoChange(
            CapabilityBootstrapResult::BootstrapConflict,
        ));
    }

    if let Some(marker) = metadata.capability_bootstrap() {
        return prepare_capability_bootstrap_replay(state, intent, marker);
    }

    let empty = state.capabilities.is_empty()
        && state.capability_lookups.is_empty()
        && state.service_audit_invocations.is_empty()
        && metadata.active_catalog().is_none()
        && state.commits.is_empty()
        && state.administration_audit.is_empty();
    if !empty {
        return Ok(AdministrationPreparation::NoChange(
            CapabilityBootstrapResult::BootstrapConflict,
        ));
    }

    let allocation = state.prepare_administration_allocation(2)?;
    let started_sequence = allocation.assigned()[0];
    let transition_sequence = allocation.assigned()[1];
    let started = StoredServiceAuditRecordV1::from_bootstrap_start(
        started_sequence,
        intent.start(),
        transition_sequence,
    )
    .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    let capability = StoredCapabilityRecordV1::active(
        intent.capability_id(),
        intent.digests().current_write(),
        intent.requested().clone(),
        intent.issued_at(),
        intent.expires_at(),
        transition_sequence,
        intent.start().request_id(),
    )
    .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    let capability_audit = StoredCapabilityAdministrationV1::new(
        transition_sequence,
        intent.start().request_id(),
        CapabilityAdministrationOperationV1::Bootstrap,
        intent.issued_at(),
        None,
        intent.capability_id(),
        NonZeroU64::MIN,
        intent.start().approval_id().cloned(),
        None,
    )
    .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    let marker = CapabilityBootstrapMarkerV1::new(
        metadata.database_id(),
        intent.capability_id(),
        transition_sequence,
    );
    let allocated = allocation.into_metadata_post_image();
    let mut mutation = AdministrationMutation::from_state(
        state,
        CapabilityBootstrapResult::BootstrapCreated {
            capability_id: intent.capability_id(),
            revision: NonZeroU64::MIN,
            administration_sequence: transition_sequence,
            invocation_started_sequence: started_sequence,
        },
    )?;
    mutation.capabilities.push(capability);
    mutation.capability_lookups.push(CapabilityLookupRow {
        digest: intent.digests().current_write(),
        value: CapabilityTokenLookupV1::new(intent.capability_id()),
    });
    mutation
        .service_audit_invocations
        .push(ServiceAuditInvocationIndexRow {
            request_id: intent.start().request_id(),
            lifecycle: ServiceAuditLifecycleIndex::Started {
                started_sequence,
                terminal_sequence: None,
            },
        });
    mutation.append_audit(StoredAdministrationAuditRecordV1::Service(started));
    mutation.append_audit(StoredAdministrationAuditRecordV1::Capability(
        capability_audit,
    ));
    mutation.metadata = metadata_with_links(allocated, None, Some(marker))?;
    Ok(AdministrationPreparation::Apply(Box::new(mutation)))
}

fn prepare_capability_bootstrap_replay(
    state: &MemoryState,
    intent: &CapabilityBootstrapIntentV1,
    marker: CapabilityBootstrapMarkerV1,
) -> Result<AdministrationPreparation<CapabilityBootstrapResult>, StorageError> {
    let capability = validate_bootstrap_graph(state, marker)?.clone();
    let digest_match =
        match resolve_capability_digests_in_state(state, intent.digests().candidates())? {
            CapabilityLookupResult::Found(record) => {
                record.capability_id() == marker.capability_id()
            }
            CapabilityLookupResult::NotFound | CapabilityLookupResult::MultipleMatches => false,
        };
    if marker.capability_id() != intent.capability_id()
        || !capability.matches_requested(intent.requested())
        || !digest_match
        || service_invocation_position(state, intent.start().request_id())?.is_ok()
    {
        return Ok(AdministrationPreparation::NoChange(
            CapabilityBootstrapResult::BootstrapConflict,
        ));
    }

    let (started_sequence, allocated) = append_sequence(state)?;
    let started = StoredServiceAuditRecordV1::from_bootstrap_replay_start(
        started_sequence,
        intent.start(),
        marker.administration_sequence(),
    )
    .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
    let mut mutation = AdministrationMutation::from_state(
        state,
        CapabilityBootstrapResult::BootstrapReplayed {
            capability_id: capability.capability_id(),
            revision: capability.revision(),
            administration_sequence: marker.administration_sequence(),
            invocation_started_sequence: started_sequence,
        },
    )?;
    let insertion = match mutation
        .service_audit_invocations
        .binary_search_by_key(&intent.start().request_id(), |row| row.request_id)
    {
        Ok(_) => return Err(storage_error(StorageErrorKind::CorruptData)),
        Err(index) => index,
    };
    mutation.service_audit_invocations.insert(
        insertion,
        ServiceAuditInvocationIndexRow {
            request_id: intent.start().request_id(),
            lifecycle: ServiceAuditLifecycleIndex::Started {
                started_sequence,
                terminal_sequence: None,
            },
        },
    );
    mutation.append_audit(StoredAdministrationAuditRecordV1::Service(started));
    mutation.metadata = allocated;
    Ok(AdministrationPreparation::Apply(Box::new(mutation)))
}

fn validate_bootstrap_graph(
    state: &MemoryState,
    marker: CapabilityBootstrapMarkerV1,
) -> Result<&StoredCapabilityRecordV1, StorageError> {
    let metadata = retained_metadata(state)?;
    if marker.database_id() != metadata.database_id() {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    let capability = checked_capability(state, marker.capability_id())?
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    if capability.creation_sequence() != marker.administration_sequence() {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    let StoredAdministrationAuditRecordV1::Capability(transition) =
        administration_record(state, marker.administration_sequence())?
    else {
        return Err(storage_error(StorageErrorKind::CorruptData));
    };
    if transition.operation() != CapabilityAdministrationOperationV1::Bootstrap
        || transition.request_id() != capability.creation_request_id()
        || transition.timestamp() != capability.issued_at()
        || transition.target_capability_id() != capability.capability_id()
        || transition.resulting_revision().get() != 1
    {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    let started_sequence = marker
        .administration_sequence()
        .get()
        .checked_sub(1)
        .and_then(AdministrationSequence::new)
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    let StoredAdministrationAuditRecordV1::Service(started) =
        administration_record(state, started_sequence)?
    else {
        return Err(storage_error(StorageErrorKind::CorruptData));
    };
    let lifecycle_valid = service_invocation_position(state, started.request_id())?
        .ok()
        .is_some_and(|index| {
            matches!(
                state.service_audit_invocations[index].lifecycle,
                ServiceAuditLifecycleIndex::Started {
                    started_sequence: indexed,
                    ..
                } if indexed == started_sequence
            )
        });
    if started.request_id() != transition.request_id()
        || started.timestamp() != transition.timestamp()
        || started.operation() != ServiceOperationV1::CreateCapability
        || started.phase() != ServiceAuditPhaseV1::Started
        || started.principal().is_some()
        || transition.initiator().is_some()
        || started.approval_id() != transition.approval_id()
        || started.link()
            != (ServiceAuditLinkV1::ControlPlane {
                administration_sequence: marker.administration_sequence(),
            })
        || !started
            .targets()
            .as_slice()
            .contains(&ServiceAuditTargetV1::Capability(marker.capability_id()))
        || !lifecycle_valid
    {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    Ok(capability)
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU16, NonZeroU32};

    use riffdb_storage_api::{
        AdministrationSequenceAllocator, ApplicationSequenceAllocator, BootstrapDigestCandidatesV1,
        BootstrapServiceAuditStartV1, CapabilityCreateAwaitingDecision,
        CapabilityCreateCandidateTransaction, CapabilityGrantV1, CapabilityPermissionKindV1,
        CapabilityPermissionV1, CapabilityPermissionsV1, CapabilityRequestedRecordV1,
        CapabilityRevokeAwaitingDecision, CapabilityRevokeCandidateTransaction,
        DatabaseInitializationPort, DatabaseInitializationResult, PartitionScopeV1,
        RevocationReasonCodeV1, StorageFormatVersion, StorageScanLimit, StorageValueError,
    };
    use riffdb_types::{
        ActorId, ActorKind, Audience, CommitSequence, ContractBundleHash, DatabaseId, DigestKeyId,
        Environment, ProvenanceId, ServiceAuditTargetsV1, ServiceIngressKindV1, TenantScope,
        Timestamp,
    };

    use super::*;
    use crate::startup::MemoryDormantPorts;
    use crate::store::MemoryStore;

    fn uuid_bytes(seed: u8) -> [u8; 16] {
        let mut bytes = [seed; 16];
        bytes[6] = 0x70 | (seed & 0x0f);
        bytes[8] = 0x80 | (seed & 0x3f);
        bytes
    }

    fn database_id() -> DatabaseId {
        DatabaseId::from_bytes(uuid_bytes(1)).expect("database ID")
    }

    fn capability_id(seed: u8) -> CapabilityId {
        CapabilityId::from_bytes(uuid_bytes(seed)).expect("capability ID")
    }

    fn request_id(seed: u8) -> RequestId {
        RequestId::from_bytes(uuid_bytes(seed)).expect("request ID")
    }

    fn digest(seed: u8) -> CapabilityTokenDigest {
        CapabilityTokenDigest::from_hmac_bytes(
            DigestKeyId::new(u32::from(seed)).expect("digest key ID"),
            [seed; 32],
        )
    }

    fn principal(capability_id: CapabilityId) -> AuditPrincipalV1 {
        AuditPrincipalV1::new(
            ActorId::new("operator").expect("actor ID"),
            ActorKind::Human,
            capability_id,
            NonZeroU64::MIN,
        )
    }

    fn requested_record(database_id: DatabaseId, actor: &str) -> CapabilityRequestedRecordV1 {
        let permissions = CapabilityPermissionsV1::new(vec![
            CapabilityPermissionV1::unparameterized(
                CapabilityPermissionKindV1::AdministerCapabilities,
            )
            .expect("permission"),
        ])
        .expect("permissions");
        let grant = CapabilityGrantV1::new(
            TenantScope::Global,
            PartitionScopeV1::All,
            permissions,
            Vec::new(),
            NonZeroU16::MIN,
            Vec::new(),
        )
        .expect("grant");
        CapabilityRequestedRecordV1::new(
            database_id,
            Environment::new("test").expect("environment"),
            ActorId::new(actor).expect("actor"),
            ActorKind::Human,
            NonZeroU32::new(60).expect("duration"),
            vec![Audience::new("riffdb-test").expect("audience")],
            grant,
        )
        .expect("requested record")
    }

    fn initialized_state() -> MemoryState {
        MemoryState {
            metadata: MemoryMetadataSlot::Retained(RetainedMetadataV1::initial(database_id())),
            ..MemoryState::default()
        }
    }

    fn apply<O>(state: &mut MemoryState, prepared: AdministrationPreparation<O>) -> O {
        prepared.apply(state)
    }

    fn prepare_and_apply<O>(
        state: &mut MemoryState,
        prepare: impl FnOnce(&MemoryState) -> Result<AdministrationPreparation<O>, StorageError>,
    ) -> O {
        let prepared = prepare(state).expect("prepare administration mutation");
        apply(state, prepared)
    }

    fn bundle(lineage: &str, version: u64, hash: u8) -> StoredContractBundleV1 {
        StoredContractBundleV1::new(
            ContractLineage::new(lineage).expect("lineage"),
            ContractVersion::new(version).expect("version"),
            ContractBundleHash::from_bytes([hash; 32]),
            vec![hash],
        )
        .expect("bundle")
    }

    fn catalog_intent(
        expected: Option<ContractVersion>,
        bundle: StoredContractBundleV1,
        request: u8,
    ) -> CatalogActivationIntentV1 {
        CatalogActivationIntentV1::new(
            expected,
            bundle,
            request_id(request),
            principal(capability_id(2)),
            Timestamp::new(i64::from(request), 0).expect("timestamp"),
            None,
        )
    }

    fn bootstrap_intent(
        request: u8,
        capability_id: CapabilityId,
        digest: CapabilityTokenDigest,
        issued_seconds: i64,
    ) -> CapabilityBootstrapIntentV1 {
        let issued_at = Timestamp::new(issued_seconds, 0).expect("issued at");
        let start = BootstrapServiceAuditStartV1::new(
            request_id(request),
            issued_at,
            ServiceIngressKindV1::Grpc,
            ServiceAuditTargetsV1::new([ServiceAuditTargetV1::Capability(capability_id)])
                .expect("targets"),
            None,
        )
        .expect("bootstrap start");
        CapabilityBootstrapIntentV1::new(
            capability_id,
            requested_record(database_id(), "operator"),
            BootstrapDigestCandidatesV1::new(vec![digest], digest).expect("digest candidates"),
            issued_at,
            Timestamp::new(issued_seconds + 60, 0).expect("expiry"),
            start,
        )
        .expect("bootstrap intent")
    }

    #[test]
    fn administration_allocation_covers_first_maximum_and_exhausted_states() {
        let state = initialized_state();
        let first = state
            .prepare_administration_allocation(1)
            .expect("first allocation");
        assert_eq!(first.assigned(), &[AdministrationSequence::first()]);

        let maximum = AdministrationSequence::new(u64::MAX).expect("maximum sequence");
        let maximum_state = MemoryState {
            metadata: MemoryMetadataSlot::Retained(
                RetainedMetadataV1::new(
                    StorageFormatVersion::V1,
                    database_id(),
                    ApplicationSequenceAllocator::initial(),
                    AdministrationSequenceAllocator::next(maximum),
                    None,
                    None,
                )
                .expect("maximum metadata"),
            ),
            ..MemoryState::default()
        };
        let last = maximum_state
            .prepare_administration_allocation(1)
            .expect("last allocation");
        assert_eq!(last.assigned(), &[maximum]);
        assert_eq!(
            last.into_metadata_post_image().administration_sequence(),
            AdministrationSequenceAllocator::Exhausted
        );

        let exhausted = MemoryState {
            metadata: MemoryMetadataSlot::Retained(
                RetainedMetadataV1::new(
                    StorageFormatVersion::V1,
                    database_id(),
                    ApplicationSequenceAllocator::initial(),
                    AdministrationSequenceAllocator::Exhausted,
                    None,
                    None,
                )
                .expect("exhausted metadata"),
            ),
            ..MemoryState::default()
        };
        let Err(error) = exhausted.prepare_administration_allocation(1) else {
            panic!("allocator must be exhausted");
        };
        assert_eq!(error.kind(), StorageErrorKind::SequenceExhausted);
    }

    #[test]
    fn catalog_uses_canonical_order_and_fixed_result_precedence() {
        let mut state = initialized_state();
        let short = bundle("b", 1, 1);
        let short_intent = catalog_intent(None, short.clone(), 10);
        let short_result = prepare_and_apply(&mut state, |state| {
            prepare_catalog_activation(state, &short_intent)
        });
        assert!(matches!(
            short_result,
            CatalogActivationResult::Activated { .. }
        ));

        let long = bundle("aa", 1, 2);
        let long_intent = catalog_intent(
            Some(ContractVersion::new(1).expect("version")),
            long.clone(),
            11,
        );
        let long_result = prepare_and_apply(&mut state, |state| {
            prepare_catalog_activation(state, &long_intent)
        });
        assert!(matches!(
            long_result,
            CatalogActivationResult::Activated { .. }
        ));
        assert!(
            state
                .catalog_bundles
                .windows(2)
                .all(|pair| pair[0].order_key < pair[1].order_key)
        );
        assert_eq!(
            catalog_bundle_position(&state, short.lineage(), short.contract_version())
                .expect("short lookup")
                .map(|index| state.catalog_bundles[index].bundle.clone()),
            Some(short)
        );

        let before = state.administration_audit.len();
        let replay_intent = catalog_intent(None, long.clone(), 12);
        let replay = prepare_and_apply(&mut state, |state| {
            prepare_catalog_activation(state, &replay_intent)
        });
        assert!(matches!(
            replay,
            CatalogActivationResult::AlreadyActive { .. }
        ));
        assert_eq!(state.administration_audit.len(), before);

        let conflict_intent = catalog_intent(None, bundle("aa", 1, 9), 13);
        let conflict = prepare_and_apply(&mut state, |state| {
            prepare_catalog_activation(state, &conflict_intent)
        });
        assert_eq!(conflict, CatalogActivationResult::BundleConflict);
        assert_eq!(state.administration_audit.len(), before);

        let mismatch_intent = catalog_intent(None, bundle("aa", 2, 3), 14);
        let mismatch = prepare_and_apply(&mut state, |state| {
            prepare_catalog_activation(state, &mismatch_intent)
        });
        assert!(matches!(
            mismatch,
            CatalogActivationResult::ExpectedActiveVersionMismatch { .. }
        ));
        assert_eq!(state.administration_audit.len(), before);
    }

    fn audit_intent(
        request: u8,
        operation: ServiceOperationV1,
        phase: ServiceAuditPhaseV1,
        principal: AuditPrincipalV1,
        link: ServiceAuditLinkV1,
    ) -> ServiceAuditAppendIntentV1 {
        ServiceAuditAppendIntentV1::new(
            request_id(request),
            Timestamp::new(i64::from(request), 0).expect("timestamp"),
            operation,
            phase,
            principal,
            ServiceIngressKindV1::Grpc,
            ServiceAuditTargetsV1::empty(),
            None,
            link,
        )
        .expect("audit intent")
    }

    #[test]
    fn service_audit_enforces_lifecycle_and_rejects_unproven_links_without_allocation() {
        let mut state = initialized_state();
        let actor = principal(capability_id(3));
        let started = audit_intent(
            20,
            ServiceOperationV1::GetHealth,
            ServiceAuditPhaseV1::Started,
            actor.clone(),
            ServiceAuditLinkV1::None,
        );
        let result = prepare_and_apply(&mut state, |state| {
            prepare_service_audit_append(state, &started)
        });
        assert!(matches!(result, ServiceAuditAppendResult::Appended(_)));

        let mismatched = audit_intent(
            20,
            ServiceOperationV1::GetStatistics,
            ServiceAuditPhaseV1::Succeeded,
            actor.clone(),
            ServiceAuditLinkV1::None,
        );
        let before = state.administration_audit.len();
        assert_eq!(
            prepare_and_apply(&mut state, |state| {
                prepare_service_audit_append(state, &mismatched)
            }),
            ServiceAuditAppendResult::PhaseConflict
        );
        assert_eq!(state.administration_audit.len(), before);

        let succeeded = audit_intent(
            20,
            ServiceOperationV1::GetHealth,
            ServiceAuditPhaseV1::Succeeded,
            actor.clone(),
            ServiceAuditLinkV1::None,
        );
        assert!(matches!(
            prepare_and_apply(&mut state, |state| {
                prepare_service_audit_append(state, &succeeded)
            }),
            ServiceAuditAppendResult::Appended(_)
        ));
        let after_terminal = state.administration_audit.len();
        assert_eq!(
            prepare_and_apply(&mut state, |state| {
                prepare_service_audit_append(state, &succeeded)
            }),
            ServiceAuditAppendResult::PhaseConflict
        );
        assert_eq!(state.administration_audit.len(), after_terminal);

        let command_start = audit_intent(
            21,
            ServiceOperationV1::ExecuteCommand,
            ServiceAuditPhaseV1::Started,
            actor.clone(),
            ServiceAuditLinkV1::None,
        );
        prepare_and_apply(&mut state, |state| {
            prepare_service_audit_append(state, &command_start)
        });
        assert_eq!(
            ServiceAuditAppendIntentV1::new(
                request_id(21),
                Timestamp::new(22, 0).expect("timestamp"),
                ServiceOperationV1::ExecuteCommand,
                ServiceAuditPhaseV1::Succeeded,
                actor.clone(),
                ServiceIngressKindV1::Grpc,
                ServiceAuditTargetsV1::empty(),
                None,
                ServiceAuditLinkV1::None,
            ),
            Err(StorageValueError::InvalidShape)
        );
        let mispointed = audit_intent(
            21,
            ServiceOperationV1::ExecuteCommand,
            ServiceAuditPhaseV1::Succeeded,
            actor,
            ServiceAuditLinkV1::Command {
                commit_sequence: CommitSequence::first(),
                provenance_id: ProvenanceId::from_bytes(uuid_bytes(30)).expect("provenance ID"),
            },
        );
        let before_mispoint = state.administration_audit.len();
        assert_eq!(
            prepare_and_apply(&mut state, |state| {
                prepare_service_audit_append(state, &mispointed)
            }),
            ServiceAuditAppendResult::PhaseConflict
        );
        assert_eq!(state.administration_audit.len(), before_mispoint);
    }

    #[test]
    fn compound_bootstrap_is_atomic_and_replay_appends_only_a_linked_start() {
        let mut state = initialized_state();
        let root = capability_id(4);
        let root_digest = digest(1);
        let first = bootstrap_intent(40, root, root_digest, 10);
        let created = prepare_and_apply(&mut state, |state| {
            prepare_capability_bootstrap(state, &first)
        });
        assert_eq!(
            created,
            CapabilityBootstrapResult::BootstrapCreated {
                capability_id: root,
                revision: NonZeroU64::MIN,
                administration_sequence: AdministrationSequence::new(2).expect("sequence two"),
                invocation_started_sequence: AdministrationSequence::first(),
            }
        );
        assert_eq!(state.administration_audit.len(), 2);
        assert_eq!(state.capabilities.len(), 1);
        assert_eq!(state.capability_lookups.len(), 1);
        validate_administration_stream(&state).expect("valid compound stream");

        let replay_intent = bootstrap_intent(41, root, root_digest, 20);
        let replayed = prepare_and_apply(&mut state, |state| {
            prepare_capability_bootstrap(state, &replay_intent)
        });
        assert_eq!(
            replayed,
            CapabilityBootstrapResult::BootstrapReplayed {
                capability_id: root,
                revision: NonZeroU64::MIN,
                administration_sequence: AdministrationSequence::new(2).expect("sequence two"),
                invocation_started_sequence: AdministrationSequence::new(3)
                    .expect("sequence three"),
            }
        );
        assert_eq!(state.administration_audit.len(), 3);
        assert_eq!(state.capabilities.len(), 1);
        assert_eq!(state.capability_lookups.len(), 1);

        let conflict = bootstrap_intent(42, root, digest(2), 30);
        let before = state.administration_audit.len();
        assert_eq!(
            prepare_and_apply(&mut state, |state| {
                prepare_capability_bootstrap(state, &conflict)
            }),
            CapabilityBootstrapResult::BootstrapConflict
        );
        assert_eq!(state.administration_audit.len(), before);

        let terminal = ServiceAuditAppendIntentV1::for_bootstrap_succeeded(
            replay_intent.start(),
            Timestamp::new(21, 0).expect("terminal timestamp"),
            AdministrationSequence::new(2).expect("bootstrap transition"),
        )
        .expect("bootstrap terminal");
        assert!(matches!(
            prepare_and_apply(&mut state, |state| {
                prepare_service_audit_append(state, &terminal)
            }),
            ServiceAuditAppendResult::Appended(_)
        ));
        validate_bootstrap_graph(
            &state,
            retained_metadata(&state)
                .expect("metadata")
                .capability_bootstrap()
                .expect("bootstrap marker"),
        )
        .expect("bootstrap graph remains reciprocal");
    }

    fn bootstrap_store() -> (MemoryStore, CapabilityId) {
        let mut store = MemoryStore::new();
        assert_eq!(
            store
                .initialize_database(database_id())
                .expect("initialize database"),
            DatabaseInitializationResult::Installed(database_id())
        );
        let root = capability_id(5);
        let bootstrap = bootstrap_intent(50, root, digest(3), 10);
        let access = store.acquire().expect("bootstrap access");
        access
            .write(|state| {
                let prepared = prepare_capability_bootstrap(state, &bootstrap)?;
                Ok(prepared.apply(state))
            })
            .expect("bootstrap transition");
        drop(access);
        (store, root)
    }

    #[test]
    fn operational_repository_traits_share_one_administration_stream() {
        let mut store = MemoryStore::new();
        store
            .initialize_database(database_id())
            .expect("initialize database");
        let mut ports = MemoryDormantPorts { store }.into_operational();
        let deployed = bundle("trait-path", 1, 7);
        let activation = catalog_intent(None, deployed.clone(), 70);
        assert!(matches!(
            CatalogAdministrationRepository::activate_catalog(&mut ports, &activation)
                .expect("activate through repository trait"),
            CatalogActivationResult::Activated { .. }
        ));
        assert_eq!(
            CatalogRepository::read_contract_bundle(
                &ports,
                deployed.lineage(),
                deployed.contract_version()
            )
            .expect("read through repository trait"),
            Some(deployed)
        );
        assert_eq!(
            CatalogRepository::read_active_catalog(&ports)
                .expect("read active catalog through trait")
                .map(|pointer| pointer.contract_version()),
            Some(ContractVersion::new(1).expect("active version"))
        );

        let denied = audit_intent(
            71,
            ServiceOperationV1::GetStatistics,
            ServiceAuditPhaseV1::Denied,
            principal(capability_id(7)),
            ServiceAuditLinkV1::None,
        );
        assert!(matches!(
            ServiceAuditAppendRepository::append_service_audit(&mut ports, &denied)
                .expect("append through repository trait"),
            ServiceAuditAppendResult::Appended(_)
        ));
        let first_page = AdministrationAuditReader::scan_administration_audit(
            &ports,
            AdministrationAuditScanRequest::new(
                None,
                StorageScanLimit::new(1).expect("scan limit"),
            ),
        )
        .expect("scan through repository trait");
        let AdministrationAuditScan::Page {
            records,
            next_after,
        } = first_page
        else {
            panic!("one-row scan must have a continuation");
        };
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].encoded_content_charge(), memory_record_charge());
        let exact_end = AdministrationAuditReader::scan_administration_audit(
            &ports,
            AdministrationAuditScanRequest::new(
                Some(next_after),
                StorageScanLimit::new(10).expect("scan limit"),
            ),
        )
        .expect("continue scan through repository trait");
        assert!(matches!(
            exact_end,
            AdministrationAuditScan::ExactEnd { records } if records.len() == 1
        ));
        assert_eq!(
            CapabilityReader::read_capability(&ports, capability_id(8))
                .expect("read missing capability through trait"),
            None
        );
        assert_eq!(
            CapabilityReader::resolve_capability_digests(&ports, &[digest(8)])
                .expect("resolve missing digest through trait"),
            CapabilityLookupResult::NotFound
        );
        let inventory = CapabilityInventoryReader::scan_capabilities(
            &ports,
            None,
            StorageScanLimit::new(1).expect("inventory limit"),
        )
        .expect("scan empty capability inventory");
        assert!(inventory.records().is_empty());
        assert!(!inventory.has_more());
    }

    #[test]
    fn administration_ports_and_activation_remain_operational_only() {
        let sources = [
            include_str!("lib.rs"),
            include_str!("store.rs"),
            include_str!("startup.rs"),
            include_str!("state.rs"),
            include_str!("application.rs"),
            include_str!("derived.rs"),
            include_str!("integrity_administration.rs"),
            include_str!("integrity_command.rs"),
            include_str!("integrity_projection.rs"),
            include_str!("administration.rs"),
        ];
        let traits = [
            "CatalogRepository",
            "CatalogAdministrationRepository",
            "ServiceAuditAppendRepository",
            "AdministrationAuditReader",
            "CapabilityReader",
            "CapabilityAdministrationTransactionPort",
            "CapabilityBootstrapAdministrationRepository",
        ];
        for trait_name in traits {
            let operational = format!("impl {trait_name} for MemoryOperationalPorts");
            assert_eq!(
                sources
                    .iter()
                    .filter(|source| source.contains(&operational))
                    .count(),
                1,
                "{trait_name} must have exactly one memory implementation"
            );
            for forbidden in [
                "MemoryStore",
                "MemoryDormantPorts",
                "MemoryStructuralEvidenceSession",
            ] {
                let implementation = format!("impl {trait_name} for {forbidden}");
                assert!(
                    sources
                        .iter()
                        .all(|source| !source.contains(&implementation)),
                    "{trait_name} must not be available on {forbidden}"
                );
            }
        }

        let store_source = include_str!("store.rs");
        assert!(store_source.contains("pub(crate) fn into_operational("));
        assert!(!store_source.contains("pub fn into_operational("));
        assert!(store_source.contains("pub struct MemoryOperationalPorts {\n    shared:"));
        let startup_source = include_str!("startup.rs");
        assert!(startup_source.contains("pub struct MemoryDormantPorts {"));
        assert!(startup_source.contains("pub(crate) store: MemoryStore"));
    }

    fn audit_length(store: &MemoryStore) -> usize {
        let access = store.acquire().expect("read access");
        access
            .read(|state| Ok(state.administration_audit.len()))
            .expect("audit length")
    }

    #[test]
    fn consuming_capability_transactions_reject_substitution_and_replay_create_and_revoke() {
        let (store, root) = bootstrap_store();
        let child = capability_id(6);
        let child_requested = requested_record(database_id(), "delegate");
        let authorizer = principal(root);
        let create_candidate = CapabilityCreateCandidateV1::new(
            child,
            request_id(60),
            child_requested.clone(),
            digest(4),
            authorizer.clone(),
            None,
        );
        let candidate = MemoryCapabilityCreateCandidate {
            access: store.acquire().expect("create access"),
            candidate: create_candidate.clone(),
        };
        let (awaiting, current) = candidate
            .read_transaction_current()
            .expect("current create state");
        assert!(current.authorizing().is_some());
        assert!(current.target().is_none());
        let substituted = CapabilityCreateIntentV1::new(
            child,
            request_id(60),
            child_requested.clone(),
            digest(5),
            Timestamp::new(20, 0).expect("issued"),
            Timestamp::new(80, 0).expect("expires"),
            authorizer.clone(),
            None,
        )
        .expect("substituted intent");
        assert_eq!(
            awaiting
                .commit_create(substituted)
                .expect_err("candidate substitution must fail")
                .kind(),
            StorageErrorKind::InvariantViolation
        );
        assert_eq!(audit_length(&store), 2);

        let candidate = MemoryCapabilityCreateCandidate {
            access: store.acquire().expect("create access"),
            candidate: create_candidate.clone(),
        };
        let (awaiting, _) = candidate
            .read_transaction_current()
            .expect("current create state");
        let create_intent = CapabilityCreateIntentV1::new(
            child,
            request_id(60),
            child_requested.clone(),
            digest(4),
            Timestamp::new(20, 0).expect("issued"),
            Timestamp::new(80, 0).expect("expires"),
            authorizer.clone(),
            None,
        )
        .expect("create intent");
        let created = awaiting.commit_create(create_intent).expect("create child");
        assert!(matches!(created, CapabilityCreateResult::Created { .. }));
        let after_create = audit_length(&store);
        let access = store.acquire().expect("digest read access");
        let multiple = access
            .read(|state| resolve_capability_digests_in_state(state, &[digest(3), digest(4)]))
            .expect("resolve two valid capabilities");
        assert_eq!(multiple, CapabilityLookupResult::MultipleMatches);
        drop(access);

        let replay_candidate = CapabilityCreateCandidateV1::new(
            child,
            request_id(61),
            child_requested,
            digest(6),
            authorizer.clone(),
            None,
        );
        let replay = MemoryCapabilityCreateCandidate {
            access: store.acquire().expect("replay access"),
            candidate: replay_candidate.clone(),
        };
        let (awaiting, _) = replay
            .read_transaction_current()
            .expect("replay current state");
        let replay_intent = CapabilityCreateIntentV1::new(
            child,
            request_id(61),
            replay_candidate.requested().clone(),
            digest(6),
            Timestamp::new(30, 0).expect("issued"),
            Timestamp::new(90, 0).expect("expires"),
            authorizer.clone(),
            None,
        )
        .expect("replay intent");
        assert!(matches!(
            awaiting
                .commit_create(replay_intent)
                .expect("create replay"),
            CapabilityCreateResult::AlreadyCreated { .. }
        ));
        assert_eq!(audit_length(&store), after_create);

        let revoke_candidate = CapabilityRevokeCandidateV1::new(
            child,
            request_id(62),
            authorizer.clone(),
            None,
            RevocationReasonCodeV1::Requested,
        );
        let revoke = MemoryCapabilityRevokeCandidate {
            access: store.acquire().expect("revoke access"),
            candidate: revoke_candidate.clone(),
        };
        let (awaiting, current) = revoke
            .read_transaction_current()
            .expect("revoke current state");
        let revision = current.target().expect("target").revision();
        let revoke_intent = CapabilityRevokeIntentV1::new(
            child,
            revision,
            request_id(62),
            Timestamp::new(40, 0).expect("revoked at"),
            authorizer.clone(),
            None,
            RevocationReasonCodeV1::Requested,
        );
        assert!(matches!(
            awaiting.commit_revoke(revoke_intent).expect("revoke"),
            CapabilityRevokeResult::Revoked { .. }
        ));
        let after_revoke = audit_length(&store);

        let replay_candidate = CapabilityRevokeCandidateV1::new(
            child,
            request_id(63),
            authorizer.clone(),
            None,
            RevocationReasonCodeV1::Requested,
        );
        let replay = MemoryCapabilityRevokeCandidate {
            access: store.acquire().expect("revoke replay access"),
            candidate: replay_candidate,
        };
        let (awaiting, current) = replay
            .read_transaction_current()
            .expect("revoke replay current state");
        let replay_intent = CapabilityRevokeIntentV1::new(
            child,
            current.target().expect("target").revision(),
            request_id(63),
            Timestamp::new(41, 0).expect("replay time"),
            authorizer,
            None,
            RevocationReasonCodeV1::Requested,
        );
        assert!(matches!(
            awaiting
                .commit_revoke(replay_intent)
                .expect("revoke replay"),
            CapabilityRevokeResult::AlreadyRevoked { .. }
        ));
        assert_eq!(audit_length(&store), after_revoke);
    }
}
