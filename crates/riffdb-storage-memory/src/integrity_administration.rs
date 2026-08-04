//! Bounded reciprocal validation for catalog, capability, and service audit state.

use riffdb_storage_api::{
    ActiveCatalogPointerV1, ActiveQueryModulePointerV1, CapabilityAdministrationOperationV1,
    CapabilityLifecycleV1, RetainedMetadataV1, StoredAdministrationAuditRecordV1,
    StoredCapabilityAdministrationV1, StoredCapabilityRecordV1, StoredCatalogAdministrationV1,
    StoredQueryModuleAdministrationV1, StoredServiceAuditRecordV1, StructuralFinding,
    StructuralFindingCode, StructuralFindingScope,
};
use riffdb_types::{
    AdministrationSequence, CapabilityId, RequestId, ServiceAuditLinkV1, ServiceAuditPhaseV1,
    ServiceAuditTargetV1, ServiceOperationV1, hash_query_module, hash_reactive_module,
    hash_reactive_source,
};

use crate::state::{
    MemoryState, ServiceAuditLifecycleIndex, bundle_evidence_order_key,
    bundle_identity_evidence_order_key,
};

pub(crate) fn inspect_bundle_activation(
    state: &MemoryState,
    index: usize,
) -> Option<StructuralFinding> {
    let bundle = &state.catalog_bundles[index].bundle;
    let order_key = bundle_evidence_order_key(bundle);
    let Some(reverse) = bundle_activation(state, &order_key) else {
        return missing();
    };
    let Some(StoredAdministrationAuditRecordV1::Catalog(activation)) =
        administration(state, reverse.administration_sequence)
    else {
        return missing();
    };
    if !activation.activated().matches_bundle(bundle) {
        return mismatch();
    }
    None
}

pub(crate) fn inspect_catalog_activation_index(
    state: &MemoryState,
    index: usize,
) -> Option<StructuralFinding> {
    let row = &state.catalog_activations[index];
    if index > 0
        && state.catalog_activations[index - 1].administration_sequence
            >= row.administration_sequence
    {
        return mismatch();
    }
    let Some(StoredAdministrationAuditRecordV1::Catalog(record)) =
        administration(state, row.administration_sequence)
    else {
        return missing();
    };
    let expected_previous = index.checked_sub(1).and_then(|prior| {
        let sequence = state.catalog_activations[prior].administration_sequence;
        match administration(state, sequence) {
            Some(StoredAdministrationAuditRecordV1::Catalog(prior)) => Some(prior.activated()),
            _ => None,
        }
    });
    if record.previous_active() != expected_previous
        || catalog_bundle(state, record.activated()).is_none()
    {
        return mismatch();
    }
    let key = bundle_identity_evidence_order_key(
        record.activated().lineage(),
        record.activated().contract_version(),
        record.activated().bundle_hash(),
    );
    if bundle_activation(state, &key).is_none() {
        return missing();
    }
    None
}

pub(crate) fn inspect_catalog_bundle_activation_index(
    state: &MemoryState,
    index: usize,
) -> Option<StructuralFinding> {
    let row = &state.catalog_bundle_activations[index];
    if index > 0 && state.catalog_bundle_activations[index - 1].order_key >= row.order_key {
        return mismatch();
    }
    let Some(StoredAdministrationAuditRecordV1::Catalog(record)) =
        administration(state, row.administration_sequence)
    else {
        return missing();
    };
    let expected = bundle_identity_evidence_order_key(
        record.activated().lineage(),
        record.activated().contract_version(),
        record.activated().bundle_hash(),
    );
    if row.order_key != expected
        || catalog_bundle(state, record.activated()).is_none()
        || catalog_activation(state, row.administration_sequence).is_none()
    {
        return mismatch();
    }
    None
}

pub(crate) fn inspect_administration_graph(
    state: &MemoryState,
    index: usize,
) -> Option<StructuralFinding> {
    match &state.administration_audit[index] {
        StoredAdministrationAuditRecordV1::Catalog(record) => inspect_catalog_record(state, record),
        StoredAdministrationAuditRecordV1::QueryModule(record) => {
            inspect_query_module_record(state, record)
        }
        StoredAdministrationAuditRecordV1::ReactiveModule(record) => {
            if state
                .reactive_modules
                .binary_search_by_key(&record.module_hash(), |module| module.module_hash())
                .is_ok()
            {
                None
            } else {
                missing()
            }
        }
        StoredAdministrationAuditRecordV1::Capability(record) => {
            inspect_capability_administration(state, record)
        }
        StoredAdministrationAuditRecordV1::Service(record) => inspect_service_record(state, record),
        // Retention administration records (projection detach/reattach) have
        // no cross-linked peer row; the semantic decode is the check.
        StoredAdministrationAuditRecordV1::Retention(_) => None,
    }
}

pub(crate) fn inspect_query_module(state: &MemoryState, index: usize) -> Option<StructuralFinding> {
    let module = &state.query_modules[index];
    if hash_query_module(module.canonical_bytes()) != module.module_hash()
        || (index > 0 && state.query_modules[index - 1].module_hash() >= module.module_hash())
    {
        return mismatch();
    }
    let contract = ActiveCatalogPointerV1::new(
        module.contract_lineage().clone(),
        module.contract_version(),
        module.contract_bundle_hash(),
    );
    if catalog_bundle(state, &contract).is_none()
        || !state.administration_audit.iter().any(|record| {
            matches!(
                record,
                StoredAdministrationAuditRecordV1::QueryModule(activation)
                    if activation.activated().matches_module(module)
            )
        })
    {
        return missing();
    }
    None
}

/// Validates one retained reactive-module row against everything it names.
///
/// Mirrors redb's `inspect_reactive_module_row`, arm for arm and code for code:
/// artifact and source hash agreement plus the ordering invariant that stands in
/// for redb's key/value key agreement (`CrossLinkMismatch`), the exact contract
/// artifact it compiled against, every exact query-module dependency against
/// that same artifact, and finally its own publication record in the
/// administration stream (`MissingCrossLink`).
///
/// The audit-record direction — a publication record naming a retained row — is
/// already covered by `inspect_administration_graph`. This is the reverse
/// direction, so an orphaned or self-inconsistent row can no longer pass the
/// memory pass silently while redb reports it.
pub(crate) fn inspect_reactive_module(
    state: &MemoryState,
    index: usize,
) -> Option<StructuralFinding> {
    let module = &state.reactive_modules[index];
    if hash_reactive_module(module.canonical_module()) != module.module_hash()
        || hash_reactive_source(module.canonical_source()) != module.source_hash()
        || (index > 0 && state.reactive_modules[index - 1].module_hash() >= module.module_hash())
    {
        return mismatch();
    }
    let contract = ActiveCatalogPointerV1::new(
        module.contract_lineage().clone(),
        module.contract_version(),
        module.contract_bundle_hash(),
    );
    if catalog_bundle(state, &contract).is_none() {
        return missing();
    }
    for dependency_hash in module.query_module_hashes() {
        let Ok(dependency_index) = state
            .query_modules
            .binary_search_by_key(dependency_hash, |dependency| dependency.module_hash())
        else {
            return missing();
        };
        let dependency = &state.query_modules[dependency_index];
        if dependency.contract_lineage() != module.contract_lineage()
            || dependency.contract_version() != module.contract_version()
            || dependency.contract_bundle_hash() != module.contract_bundle_hash()
        {
            return mismatch();
        }
    }
    if !state.administration_audit.iter().any(|record| {
        matches!(
            record,
            StoredAdministrationAuditRecordV1::ReactiveModule(publication)
                if publication.module_hash() == module.module_hash()
        )
    }) {
        return missing();
    }
    None
}

pub(crate) fn inspect_active_query_module(
    state: &MemoryState,
    index: usize,
) -> Option<StructuralFinding> {
    let record = &state.active_query_modules[index];
    if index > 0
        && compare_query_module_contract(
            state.active_query_modules[index - 1].activated(),
            record.activated(),
        ) != std::cmp::Ordering::Less
    {
        return mismatch();
    }
    let Some(StoredAdministrationAuditRecordV1::QueryModule(audit)) =
        administration(state, record.administration_sequence())
    else {
        return missing();
    };
    if audit != record || inspect_query_module_record(state, record).is_some() {
        return mismatch();
    }
    None
}

fn compare_query_module_contract(
    left: &ActiveQueryModulePointerV1,
    right: &ActiveQueryModulePointerV1,
) -> std::cmp::Ordering {
    left.contract_lineage()
        .cmp(right.contract_lineage())
        .then_with(|| left.contract_version().cmp(&right.contract_version()))
        .then_with(|| {
            left.contract_bundle_hash()
                .cmp(&right.contract_bundle_hash())
        })
}

fn same_query_module_contract(
    left: &ActiveQueryModulePointerV1,
    right: &ActiveQueryModulePointerV1,
) -> bool {
    compare_query_module_contract(left, right).is_eq()
}

fn inspect_query_module_record(
    state: &MemoryState,
    record: &StoredQueryModuleAdministrationV1,
) -> Option<StructuralFinding> {
    let Ok(module_index) = state
        .query_modules
        .binary_search_by_key(&record.activated().module_hash(), |module| {
            module.module_hash()
        })
    else {
        return missing();
    };
    if !record
        .activated()
        .matches_module(&state.query_modules[module_index])
    {
        return mismatch();
    }

    let mut previous = None;
    let mut last = None;
    let mut found = false;
    for candidate in &state.administration_audit {
        let StoredAdministrationAuditRecordV1::QueryModule(candidate) = candidate else {
            continue;
        };
        if !same_query_module_contract(candidate.activated(), record.activated()) {
            continue;
        }
        if candidate.administration_sequence() < record.administration_sequence() {
            previous = Some(candidate.activated());
        }
        if candidate.administration_sequence() == record.administration_sequence() {
            found = candidate == record;
        }
        last = Some(candidate);
    }
    let active = state
        .active_query_modules
        .iter()
        .find(|candidate| same_query_module_contract(candidate.activated(), record.activated()));
    if !found || record.previous_active() != previous || last != active {
        return mismatch();
    }
    None
}

pub(crate) fn inspect_service_invocation_index(
    state: &MemoryState,
    index: usize,
) -> Option<StructuralFinding> {
    let row = &state.service_audit_invocations[index];
    if index > 0 && state.service_audit_invocations[index - 1].request_id >= row.request_id {
        return mismatch();
    }
    match row.lifecycle {
        ServiceAuditLifecycleIndex::Standalone { sequence } => {
            let Some(StoredAdministrationAuditRecordV1::Service(record)) =
                administration(state, sequence)
            else {
                return missing();
            };
            if record.request_id() != row.request_id
                || !matches!(
                    record.phase(),
                    ServiceAuditPhaseV1::Denied
                        | ServiceAuditPhaseV1::Cancelled
                        | ServiceAuditPhaseV1::Failed
                )
                || record.principal().is_none()
                || record.link() != ServiceAuditLinkV1::None
            {
                return mismatch();
            }
        }
        ServiceAuditLifecycleIndex::Started {
            started_sequence,
            terminal_sequence,
        } => {
            let Some(StoredAdministrationAuditRecordV1::Service(started)) =
                administration(state, started_sequence)
            else {
                return missing();
            };
            if started.request_id() != row.request_id
                || started.phase() != ServiceAuditPhaseV1::Started
                || !service_link_is_valid(state, started)
            {
                return mismatch();
            }
            if let Some(terminal_sequence) = terminal_sequence {
                if terminal_sequence <= started_sequence {
                    return mismatch();
                }
                let Some(StoredAdministrationAuditRecordV1::Service(terminal)) =
                    administration(state, terminal_sequence)
                else {
                    return missing();
                };
                if terminal.request_id() != row.request_id
                    || terminal.phase() == ServiceAuditPhaseV1::Started
                    || terminal.operation() != started.operation()
                    || terminal.principal() != started.principal()
                    || terminal.ingress() != started.ingress()
                    || terminal.targets() != started.targets()
                    || terminal.approval_id() != started.approval_id()
                    || (started.principal().is_none() && terminal.link() != started.link())
                    || !service_link_is_valid(state, terminal)
                {
                    return mismatch();
                }
            }
        }
    }
    None
}

pub(crate) fn inspect_capability_graph(
    state: &MemoryState,
    capability: &StoredCapabilityRecordV1,
) -> Option<StructuralFinding> {
    let Some(StoredAdministrationAuditRecordV1::Capability(creation)) =
        administration(state, capability.creation_sequence())
    else {
        return missing();
    };
    let expected_creation = if bootstrap_capability(state) == Some(capability.capability_id()) {
        CapabilityAdministrationOperationV1::Bootstrap
    } else {
        CapabilityAdministrationOperationV1::Create
    };
    if creation.operation() != expected_creation
        || creation.request_id() != capability.creation_request_id()
        || creation.timestamp() != capability.issued_at()
        || creation.target_capability_id() != capability.capability_id()
        || creation.resulting_revision().get() != 1
    {
        return mismatch();
    }
    if let CapabilityLifecycleV1::Revoked {
        revoked_at,
        administration_sequence,
        reason,
    } = capability.lifecycle()
    {
        let Some(StoredAdministrationAuditRecordV1::Capability(revocation)) =
            administration(state, *administration_sequence)
        else {
            return missing();
        };
        if revocation.operation() != CapabilityAdministrationOperationV1::Revoke
            || revocation.timestamp() != *revoked_at
            || revocation.target_capability_id() != capability.capability_id()
            || revocation.resulting_revision() != capability.revision()
            || revocation.revocation_reason() != Some(*reason)
        {
            return mismatch();
        }
    }
    None
}

pub(crate) fn bootstrap_is_consistent(state: &MemoryState, metadata: &RetainedMetadataV1) -> bool {
    let Some(marker) = metadata.capability_bootstrap() else {
        return state.capabilities.is_empty();
    };
    if marker.database_id() != metadata.database_id() {
        return false;
    }
    let Some(capability) = capability(state, marker.capability_id()) else {
        return false;
    };
    if capability.creation_sequence() != marker.administration_sequence() {
        return false;
    }
    let Some(StoredAdministrationAuditRecordV1::Capability(transition)) =
        administration(state, marker.administration_sequence())
    else {
        return false;
    };
    if transition.operation() != CapabilityAdministrationOperationV1::Bootstrap
        || transition.request_id() != capability.creation_request_id()
        || transition.timestamp() != capability.issued_at()
        || transition.target_capability_id() != marker.capability_id()
    {
        return false;
    }
    let Some(started_sequence) = marker
        .administration_sequence()
        .get()
        .checked_sub(1)
        .and_then(AdministrationSequence::new)
    else {
        return false;
    };
    let Some(StoredAdministrationAuditRecordV1::Service(started)) =
        administration(state, started_sequence)
    else {
        return false;
    };
    started.request_id() == transition.request_id()
        && started.timestamp() == transition.timestamp()
        && started.operation() == ServiceOperationV1::CreateCapability
        && started.phase() == ServiceAuditPhaseV1::Started
        && started.principal().is_none()
        && started.link()
            == ServiceAuditLinkV1::ControlPlane {
                administration_sequence: marker.administration_sequence(),
            }
        && has_capability_target(started, marker.capability_id())
        && service_invocation(state, started.request_id()).is_some_and(|row| {
            matches!(
                row.lifecycle,
                ServiceAuditLifecycleIndex::Started {
                    started_sequence: indexed,
                    ..
                } if indexed == started_sequence
            )
        })
}

pub(crate) fn active_catalog_matches_last_activation(
    state: &MemoryState,
    metadata: &RetainedMetadataV1,
) -> bool {
    match (state.catalog_activations.last(), metadata.active_catalog()) {
        (None, None) => true,
        (Some(row), Some(active)) => matches!(
            administration(state, row.administration_sequence),
            Some(StoredAdministrationAuditRecordV1::Catalog(record))
                if record.activated() == active
        ),
        (None, Some(_)) | (Some(_), None) => false,
    }
}

fn inspect_catalog_record(
    state: &MemoryState,
    record: &StoredCatalogAdministrationV1,
) -> Option<StructuralFinding> {
    if catalog_activation(state, record.administration_sequence()).is_none() {
        return missing();
    }
    if catalog_bundle(state, record.activated()).is_none() {
        return missing();
    }
    None
}

fn inspect_capability_administration(
    state: &MemoryState,
    record: &StoredCapabilityAdministrationV1,
) -> Option<StructuralFinding> {
    let Some(capability) = capability(state, record.target_capability_id()) else {
        return missing();
    };
    match record.operation() {
        CapabilityAdministrationOperationV1::Bootstrap
        | CapabilityAdministrationOperationV1::Create => {
            let expected = if record.operation() == CapabilityAdministrationOperationV1::Bootstrap {
                bootstrap_capability(state)
            } else {
                Some(record.target_capability_id())
                    .filter(|id| bootstrap_capability(state) != Some(*id))
            };
            if expected != Some(record.target_capability_id())
                || capability.creation_sequence() != record.administration_sequence()
                || capability.creation_request_id() != record.request_id()
                || capability.issued_at() != record.timestamp()
                || record.resulting_revision().get() != 1
            {
                return mismatch();
            }
        }
        CapabilityAdministrationOperationV1::Revoke => {
            let CapabilityLifecycleV1::Revoked {
                revoked_at,
                administration_sequence,
                reason,
            } = capability.lifecycle()
            else {
                return mismatch();
            };
            if *administration_sequence != record.administration_sequence()
                || *revoked_at != record.timestamp()
                || Some(*reason) != record.revocation_reason()
                || capability.revision() != record.resulting_revision()
            {
                return mismatch();
            }
        }
    }
    None
}

fn inspect_service_record(
    state: &MemoryState,
    record: &StoredServiceAuditRecordV1,
) -> Option<StructuralFinding> {
    let Some(index) = service_invocation(state, record.request_id()) else {
        return missing();
    };
    let referenced = match index.lifecycle {
        ServiceAuditLifecycleIndex::Standalone { sequence } => {
            sequence == record.administration_sequence()
        }
        ServiceAuditLifecycleIndex::Started {
            started_sequence,
            terminal_sequence,
        } => {
            started_sequence == record.administration_sequence()
                || terminal_sequence == Some(record.administration_sequence())
        }
    };
    if !referenced || !service_link_is_valid(state, record) {
        return mismatch();
    }
    None
}

fn service_link_is_valid(state: &MemoryState, record: &StoredServiceAuditRecordV1) -> bool {
    match record.link() {
        // Intentionally PERMISSIVE relative to the append side, matching the redb
        // startup pass. `riffdb-storage-api` refuses a `DeployReactiveModule`
        // linkless success when a NEW record is appended; this pass validates
        // already durable records and keeps the released allowance so no retained
        // state is refused over a stale audit shape. Backend parity is the rule
        // here: whatever the redb structural pass tolerates, this tolerates.
        ServiceAuditLinkV1::None => {
            record.principal().is_some()
                && (record.phase() != ServiceAuditPhaseV1::Succeeded
                    || !matches!(
                        record.operation(),
                        ServiceOperationV1::ExecuteCommand
                            | ServiceOperationV1::ResolveCommandOutcome
                            | ServiceOperationV1::DeployContract
                            | ServiceOperationV1::DeployQueryModule
                            | ServiceOperationV1::CreateCapability
                            | ServiceOperationV1::RevokeCapability
                    ))
        }
        ServiceAuditLinkV1::Command {
            commit_sequence,
            provenance_id,
        } => {
            record.phase() == ServiceAuditPhaseV1::Succeeded
                && matches!(
                    record.operation(),
                    ServiceOperationV1::ExecuteCommand | ServiceOperationV1::ResolveCommandOutcome
                )
                && commit(state, commit_sequence)
                    .is_some_and(|commit| commit.provenance_id() == provenance_id)
                && state
                    .provenance
                    .binary_search_by_key(&provenance_id, |value| value.provenance_id())
                    .ok()
                    .is_some_and(|index| {
                        state.provenance[index].commit_sequence() == commit_sequence
                    })
        }
        ServiceAuditLinkV1::ControlPlane {
            administration_sequence,
        } => {
            let Some(target) = administration(state, administration_sequence) else {
                return false;
            };
            let operation_matches = matches!(
                (record.operation(), target),
                (
                    ServiceOperationV1::DeployContract,
                    StoredAdministrationAuditRecordV1::Catalog(_)
                ) | (
                    ServiceOperationV1::DeployQueryModule,
                    StoredAdministrationAuditRecordV1::QueryModule(_)
                ) | (
                    ServiceOperationV1::DeployReactiveModule,
                    StoredAdministrationAuditRecordV1::ReactiveModule(_)
                ) | (
                    ServiceOperationV1::CreateCapability,
                    StoredAdministrationAuditRecordV1::Capability(_)
                ) | (
                    ServiceOperationV1::RevokeCapability,
                    StoredAdministrationAuditRecordV1::Capability(_)
                )
            );
            if !operation_matches {
                return false;
            }
            let operation_kind_matches = match target {
                StoredAdministrationAuditRecordV1::Capability(capability) => {
                    match record.operation() {
                        ServiceOperationV1::CreateCapability => matches!(
                            capability.operation(),
                            CapabilityAdministrationOperationV1::Bootstrap
                                | CapabilityAdministrationOperationV1::Create
                        ),
                        ServiceOperationV1::RevokeCapability => {
                            capability.operation() == CapabilityAdministrationOperationV1::Revoke
                        }
                        _ => true,
                    }
                }
                _ => true,
            };
            let target_matches = match target {
                StoredAdministrationAuditRecordV1::Capability(capability) => {
                    has_capability_target(record, capability.target_capability_id())
                }
                StoredAdministrationAuditRecordV1::Catalog(_)
                | StoredAdministrationAuditRecordV1::QueryModule(_)
                | StoredAdministrationAuditRecordV1::ReactiveModule(_)
                | StoredAdministrationAuditRecordV1::Service(_)
                | StoredAdministrationAuditRecordV1::Retention(_) => true,
            };
            operation_kind_matches
                && target_matches
                && if record.phase() == ServiceAuditPhaseV1::Started {
                    record.principal().is_none()
                        && matches!(
                            target,
                            StoredAdministrationAuditRecordV1::Capability(capability)
                                if capability.operation()
                                    == CapabilityAdministrationOperationV1::Bootstrap
                                    && has_capability_target(
                                        record,
                                        capability.target_capability_id(),
                                    )
                        )
                } else {
                    record.phase() == ServiceAuditPhaseV1::Succeeded
                }
        }
    }
}

fn administration(
    state: &MemoryState,
    sequence: AdministrationSequence,
) -> Option<&StoredAdministrationAuditRecordV1> {
    let index = usize::try_from(sequence.get().checked_sub(1)?).ok()?;
    state
        .administration_audit
        .get(index)
        .filter(|record| record.administration_sequence() == sequence)
}

fn commit(
    state: &MemoryState,
    sequence: riffdb_types::CommitSequence,
) -> Option<&riffdb_storage_api::StoredCommitRecordV1> {
    let index = usize::try_from(sequence.get().checked_sub(1)?).ok()?;
    state
        .commits
        .get(index)
        .filter(|record| record.commit_sequence() == sequence)
}

fn catalog_activation(
    state: &MemoryState,
    sequence: AdministrationSequence,
) -> Option<&crate::state::CatalogActivationIndexRow> {
    let index = state
        .catalog_activations
        .binary_search_by_key(&sequence, |row| row.administration_sequence)
        .ok()?;
    Some(&state.catalog_activations[index])
}

fn bundle_activation<'a>(
    state: &'a MemoryState,
    order_key: &[u8],
) -> Option<&'a crate::state::CatalogBundleActivationIndexRow> {
    let index = state
        .catalog_bundle_activations
        .binary_search_by(|row| row.order_key.as_slice().cmp(order_key))
        .ok()?;
    Some(&state.catalog_bundle_activations[index])
}

fn catalog_bundle<'a>(
    state: &'a MemoryState,
    pointer: &ActiveCatalogPointerV1,
) -> Option<&'a riffdb_storage_api::StoredContractBundleV1> {
    let key = bundle_identity_evidence_order_key(
        pointer.lineage(),
        pointer.contract_version(),
        pointer.bundle_hash(),
    );
    let index = state
        .catalog_bundles
        .binary_search_by(|row| row.order_key.cmp(&key))
        .ok()?;
    let bundle = &state.catalog_bundles[index].bundle;
    pointer.matches_bundle(bundle).then_some(bundle)
}

fn capability(
    state: &MemoryState,
    capability_id: CapabilityId,
) -> Option<&StoredCapabilityRecordV1> {
    let index = state
        .capabilities
        .binary_search_by_key(&capability_id, |record| record.capability_id())
        .ok()?;
    if (index > 0 && state.capabilities[index - 1].capability_id() == capability_id)
        || (index + 1 < state.capabilities.len()
            && state.capabilities[index + 1].capability_id() == capability_id)
    {
        return None;
    }
    Some(&state.capabilities[index])
}

fn bootstrap_capability(state: &MemoryState) -> Option<CapabilityId> {
    match &state.metadata {
        crate::state::MemoryMetadataSlot::Retained(metadata) => metadata
            .capability_bootstrap()
            .map(|marker| marker.capability_id()),
        crate::state::MemoryMetadataSlot::Absent => None,
        #[cfg(test)]
        crate::state::MemoryMetadataSlot::Corrupt => None,
    }
}

fn service_invocation(
    state: &MemoryState,
    request_id: RequestId,
) -> Option<&crate::state::ServiceAuditInvocationIndexRow> {
    let index = state
        .service_audit_invocations
        .binary_search_by_key(&request_id, |row| row.request_id)
        .ok()?;
    Some(&state.service_audit_invocations[index])
}

fn has_capability_target(record: &StoredServiceAuditRecordV1, capability_id: CapabilityId) -> bool {
    record
        .targets()
        .as_slice()
        .contains(&ServiceAuditTargetV1::Capability(capability_id))
}

const fn missing() -> Option<StructuralFinding> {
    Some(StructuralFinding::new(
        StructuralFindingScope::Authoritative,
        StructuralFindingCode::MissingCrossLink,
    ))
}

const fn mismatch() -> Option<StructuralFinding> {
    Some(StructuralFinding::new(
        StructuralFindingScope::Authoritative,
        StructuralFindingCode::CrossLinkMismatch,
    ))
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};

    use riffdb_storage_api::{
        AdministrationSequenceAllocator, ApplicationSequenceAllocator, AuditPrincipalV1,
        BootstrapServiceAuditStartV1, CapabilityBootstrapMarkerV1, CapabilityGrantV1,
        CapabilityPermissionKindV1, CapabilityPermissionV1, CapabilityPermissionsV1,
        CapabilityRequestedRecordV1, CapabilityTokenLookupV1, CatalogActivationIntentV1,
        HISTORY_INCARNATION_INITIAL, PartitionScopeV1, RetainedMetadataV1, RevocationReasonCodeV1,
        ServiceAuditAppendIntentV1, StorageFormatVersion, StoredContractBundleV1,
    };
    use riffdb_types::{
        ActorId, ActorKind, Audience, CapabilityTokenDigest, ContractBundleHash, ContractLineage,
        ContractVersion, DatabaseId, DigestKeyId, Environment, ServiceAuditTargetsV1,
        ServiceIngressKindV1, TenantScope, Timestamp,
    };

    use super::*;
    use crate::state::{
        CapabilityLookupRow, CatalogActivationIndexRow, CatalogBundleActivationIndexRow,
        CatalogBundleRow, MemoryMetadataSlot, ServiceAuditInvocationIndexRow,
    };

    fn uuid_bytes(fill: u8) -> [u8; 16] {
        let mut bytes = [fill; 16];
        bytes[6] = 0x70 | (fill & 0x0f);
        bytes[8] = 0x80 | (fill & 0x3f);
        bytes
    }

    fn database() -> DatabaseId {
        DatabaseId::from_bytes(uuid_bytes(0x11)).expect("database")
    }

    fn capability_id(fill: u8) -> CapabilityId {
        CapabilityId::from_bytes(uuid_bytes(fill)).expect("capability")
    }

    fn request(fill: u8) -> RequestId {
        RequestId::from_bytes(uuid_bytes(fill)).expect("request")
    }

    fn requested_record(database_id: DatabaseId) -> CapabilityRequestedRecordV1 {
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
            ActorId::new("operator").expect("principal"),
            ActorKind::Human,
            NonZeroU32::new(60).expect("duration"),
            vec![Audience::new("riffdb-test").expect("audience")],
            grant,
        )
        .expect("requested record")
    }

    #[test]
    fn catalog_bundle_activation_and_chain_are_reciprocal() {
        let mut state = MemoryState::default();
        let bundle = StoredContractBundleV1::new(
            ContractLineage::new("catalog").expect("lineage"),
            ContractVersion::new(1).expect("version"),
            ContractBundleHash::from_bytes([0x21; 32]),
            b"bundle".to_vec(),
        )
        .expect("bundle");
        let principal = AuditPrincipalV1::new(
            ActorId::new("operator").expect("principal"),
            ActorKind::Human,
            capability_id(0x31),
            NonZeroU64::MIN,
        );
        let intent = CatalogActivationIntentV1::new(
            None,
            bundle.clone(),
            request(0x41),
            principal,
            Timestamp::new(1, 0).expect("timestamp"),
            None,
        );
        let sequence = AdministrationSequence::first();
        let record = StoredCatalogAdministrationV1::from_committed_intent(sequence, &intent, None)
            .expect("activation");
        let order_key = bundle_evidence_order_key(&bundle);
        state.catalog_bundles.push(CatalogBundleRow::new(bundle));
        state.catalog_activations.push(CatalogActivationIndexRow {
            administration_sequence: sequence,
        });
        state
            .catalog_bundle_activations
            .push(CatalogBundleActivationIndexRow {
                order_key,
                administration_sequence: sequence,
            });
        state
            .administration_audit
            .push(StoredAdministrationAuditRecordV1::Catalog(record));

        assert_eq!(inspect_bundle_activation(&state, 0), None);
        assert_eq!(inspect_catalog_activation_index(&state, 0), None);
        assert_eq!(inspect_catalog_bundle_activation_index(&state, 0), None);
        assert_eq!(inspect_administration_graph(&state, 0), None);

        state.catalog_bundle_activations.clear();
        assert_eq!(inspect_bundle_activation(&state, 0), missing());
        assert_eq!(inspect_catalog_activation_index(&state, 0), missing());
    }

    #[test]
    fn capability_create_and_revoke_cross_links_include_time_reason_and_revision() {
        let mut state = MemoryState::default();
        let id = capability_id(0x32);
        let create_request = request(0x42);
        let issued = Timestamp::new(10, 0).expect("issued");
        let expires = Timestamp::new(70, 0).expect("expires");
        let digest = CapabilityTokenDigest::from_hmac_bytes(
            DigestKeyId::new(1).expect("digest key"),
            [0x51; 32],
        );
        let active = StoredCapabilityRecordV1::active(
            id,
            digest,
            requested_record(database()),
            issued,
            expires,
            AdministrationSequence::first(),
            create_request,
        )
        .expect("active capability");
        let initiator = AuditPrincipalV1::new(
            ActorId::new("operator").expect("principal"),
            ActorKind::Human,
            id,
            NonZeroU64::MIN,
        );
        let create = StoredCapabilityAdministrationV1::new(
            AdministrationSequence::first(),
            create_request,
            CapabilityAdministrationOperationV1::Create,
            issued,
            Some(initiator.clone()),
            id,
            NonZeroU64::MIN,
            None,
            None,
        )
        .expect("create audit");
        let revoke_sequence = AdministrationSequence::new(2).expect("sequence two");
        let revoked_at = Timestamp::new(20, 0).expect("revoked");
        let revoked = active
            .revoked(
                NonZeroU64::MIN,
                revoked_at,
                revoke_sequence,
                RevocationReasonCodeV1::Requested,
            )
            .expect("revoked capability");
        let revoke = StoredCapabilityAdministrationV1::new(
            revoke_sequence,
            request(0x43),
            CapabilityAdministrationOperationV1::Revoke,
            revoked_at,
            Some(initiator),
            id,
            NonZeroU64::new(2).expect("revision two"),
            None,
            Some(RevocationReasonCodeV1::Requested),
        )
        .expect("revoke audit");
        state.capabilities.push(revoked);
        state.capability_lookups.push(CapabilityLookupRow {
            digest,
            value: CapabilityTokenLookupV1::new(id),
        });
        state
            .administration_audit
            .push(StoredAdministrationAuditRecordV1::Capability(create));
        state
            .administration_audit
            .push(StoredAdministrationAuditRecordV1::Capability(revoke));

        assert_eq!(
            inspect_capability_graph(&state, &state.capabilities[0]),
            None
        );
        assert_eq!(inspect_administration_graph(&state, 0), None);
        assert_eq!(inspect_administration_graph(&state, 1), None);

        state.administration_audit[1] = StoredAdministrationAuditRecordV1::Capability(
            StoredCapabilityAdministrationV1::new(
                revoke_sequence,
                request(0x43),
                CapabilityAdministrationOperationV1::Revoke,
                Timestamp::new(21, 0).expect("wrong timestamp"),
                Some(AuditPrincipalV1::new(
                    ActorId::new("operator").expect("principal"),
                    ActorKind::Human,
                    id,
                    NonZeroU64::MIN,
                )),
                id,
                NonZeroU64::new(2).expect("revision two"),
                None,
                Some(RevocationReasonCodeV1::Requested),
            )
            .expect("wrong revoke record"),
        );
        assert_eq!(
            inspect_capability_graph(&state, &state.capabilities[0]),
            mismatch()
        );
    }

    #[test]
    fn bootstrap_and_replay_service_lifecycles_use_exact_principal_less_shapes() {
        let mut state = MemoryState::default();
        let database_id = database();
        let id = capability_id(0x33);
        let bootstrap_request = request(0x44);
        let issued = Timestamp::new(30, 0).expect("issued");
        let targets =
            ServiceAuditTargetsV1::new([ServiceAuditTargetV1::Capability(id)]).expect("targets");
        let start = BootstrapServiceAuditStartV1::new(
            bootstrap_request,
            issued,
            ServiceIngressKindV1::Grpc,
            targets,
            None,
        )
        .expect("bootstrap start");
        let start_sequence = AdministrationSequence::first();
        let transition_sequence = AdministrationSequence::new(2).expect("transition sequence");
        let terminal_sequence = AdministrationSequence::new(3).expect("terminal sequence");
        let start_record = StoredServiceAuditRecordV1::from_bootstrap_start(
            start_sequence,
            &start,
            transition_sequence,
        )
        .expect("bootstrap start record");
        let transition = StoredCapabilityAdministrationV1::new(
            transition_sequence,
            bootstrap_request,
            CapabilityAdministrationOperationV1::Bootstrap,
            issued,
            None,
            id,
            NonZeroU64::MIN,
            None,
            None,
        )
        .expect("bootstrap transition");
        let terminal_intent = ServiceAuditAppendIntentV1::for_bootstrap_succeeded(
            &start,
            Timestamp::new(31, 0).expect("terminal timestamp"),
            transition_sequence,
        )
        .expect("bootstrap terminal");
        let terminal = StoredServiceAuditRecordV1::from_intent(terminal_sequence, &terminal_intent);
        let digest = CapabilityTokenDigest::from_hmac_bytes(
            DigestKeyId::new(1).expect("digest key"),
            [0x52; 32],
        );
        let capability = StoredCapabilityRecordV1::active(
            id,
            digest,
            requested_record(database_id),
            issued,
            Timestamp::new(90, 0).expect("expires"),
            transition_sequence,
            bootstrap_request,
        )
        .expect("bootstrap capability");
        state.administration_audit.extend([
            StoredAdministrationAuditRecordV1::Service(start_record),
            StoredAdministrationAuditRecordV1::Capability(transition),
            StoredAdministrationAuditRecordV1::Service(terminal),
        ]);
        state
            .service_audit_invocations
            .push(ServiceAuditInvocationIndexRow {
                request_id: bootstrap_request,
                lifecycle: ServiceAuditLifecycleIndex::Started {
                    started_sequence: start_sequence,
                    terminal_sequence: Some(terminal_sequence),
                },
            });
        state.capabilities.push(capability);
        state.capability_lookups.push(CapabilityLookupRow {
            digest,
            value: CapabilityTokenLookupV1::new(id),
        });
        let marker = CapabilityBootstrapMarkerV1::new(database_id, id, transition_sequence);
        let metadata = RetainedMetadataV1::new(
            StorageFormatVersion::V1,
            database_id,
            ApplicationSequenceAllocator::initial(),
            AdministrationSequenceAllocator::next(
                AdministrationSequence::new(4).expect("next administration sequence"),
            ),
            HISTORY_INCARNATION_INITIAL,
            None,
            Some(marker),
        )
        .expect("metadata");
        state.metadata = MemoryMetadataSlot::Retained(metadata.clone());

        assert!(bootstrap_is_consistent(&state, &metadata));
        assert_eq!(inspect_service_invocation_index(&state, 0), None);
        assert_eq!(inspect_administration_graph(&state, 0), None);
        assert_eq!(inspect_administration_graph(&state, 2), None);
        assert_eq!(
            inspect_capability_graph(&state, &state.capabilities[0]),
            None
        );

        let replay_request = request(0x45);
        let replay_start = BootstrapServiceAuditStartV1::new(
            replay_request,
            Timestamp::new(40, 0).expect("replay time"),
            ServiceIngressKindV1::Grpc,
            ServiceAuditTargetsV1::new([ServiceAuditTargetV1::Capability(id)])
                .expect("replay targets"),
            None,
        )
        .expect("replay start");
        let replay_start_sequence = AdministrationSequence::new(4).expect("replay start sequence");
        let replay_terminal_sequence =
            AdministrationSequence::new(5).expect("replay terminal sequence");
        let replay_record = StoredServiceAuditRecordV1::from_bootstrap_replay_start(
            replay_start_sequence,
            &replay_start,
            transition_sequence,
        )
        .expect("replay record");
        let replay_terminal_intent = ServiceAuditAppendIntentV1::for_bootstrap_succeeded(
            &replay_start,
            Timestamp::new(41, 0).expect("replay terminal time"),
            transition_sequence,
        )
        .expect("replay terminal intent");
        let replay_terminal = StoredServiceAuditRecordV1::from_intent(
            replay_terminal_sequence,
            &replay_terminal_intent,
        );
        state.administration_audit.extend([
            StoredAdministrationAuditRecordV1::Service(replay_record),
            StoredAdministrationAuditRecordV1::Service(replay_terminal),
        ]);
        state
            .service_audit_invocations
            .push(ServiceAuditInvocationIndexRow {
                request_id: replay_request,
                lifecycle: ServiceAuditLifecycleIndex::Started {
                    started_sequence: replay_start_sequence,
                    terminal_sequence: Some(replay_terminal_sequence),
                },
            });

        assert_eq!(inspect_service_invocation_index(&state, 1), None);
        assert_eq!(inspect_administration_graph(&state, 3), None);
        assert_eq!(inspect_administration_graph(&state, 4), None);

        state.service_audit_invocations[1].lifecycle = ServiceAuditLifecycleIndex::Started {
            started_sequence: replay_start_sequence,
            terminal_sequence: Some(replay_start_sequence),
        };
        assert_eq!(inspect_service_invocation_index(&state, 1), mismatch());
    }
}
