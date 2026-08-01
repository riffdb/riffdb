//! Exclusive startup evidence session.

use std::fmt;

use riffdb_catalog::{
    CatalogIndexMigrationApplied, CatalogIndexMigrationBackend, CatalogIndexMigrationBatch,
    CatalogIndexMigrationBundleRequest, CatalogIndexMigrationBundleResponse,
    CatalogIndexMigrationCompletion, CatalogIndexMigrationInstruction,
    CatalogIndexMigrationPendingBatch, CatalogIndexMigrationScan, CatalogIndexMigrationScanRequest,
};
use riffdb_storage_api::{
    CapabilityLifecycleV1, DormantPortBundle, EvidencePageLimit, HistoricalActiveCatalogEvidence,
    HistoricalBundleBytes, HistoricalBundleEvidence, HistoricalCapabilityPartitionEvidenceV1,
    HistoricalEvidenceCursor, HistoricalEvidenceEnd, HistoricalEvidencePage,
    HistoricalPersistedKeyEvidenceV1, HistoricalSemanticEvidence, IndexMigrationCursor,
    IndexMigrationRowEvidence, MAX_RETAINED_QUERY_MODULES, OpenSessionId, ReadableDigestKey,
    RetainedMetadataV1, StartupIndexMigrationPort, StartupValidationInputs, StorageError,
    StorageErrorKind, StorageValueError, StoredAdmissionStateV1, StructuralEvidenceCursor,
    StructuralEvidenceEnd, StructuralEvidenceOpen, StructuralEvidencePage,
    StructuralEvidenceSession, StructuralFinding, StructuralFindingCode, StructuralFindingScope,
    StructuralOpenOutcome, StructurallyOpened, UniqueIndexTarget, UniqueOccupancyKind,
};
use riffdb_types::{ContractBundleHash, ContractLineage, ContractVersion, DatabaseId};

use crate::state::{
    MemoryIndexEntry, MemoryMetadataSlot, MemoryState, bundle_evidence_order_key,
    bundle_identity_evidence_order_key, durable_codec_error_as_storage, memory_record_charge,
    plan_evidence_order_key,
};
use crate::store::{MemoryAccess, MemoryStore, storage_error};

/// Backend-owned dormant ports released only after both evidence streams end.
///
/// This collection is not an operational readiness proof and exposes no
/// storage mutation interface.
pub struct MemoryDormantPorts {
    #[allow(dead_code)]
    pub(crate) store: MemoryStore,
}

/// Memory-backend authority whose constructor is private to a finished session.
pub struct MemoryCompletionAuthority {
    _private: (),
}

/// Exclusive startup-only memory migration capability.
pub struct MemoryStartupIndexMigrationPort {
    access: MemoryAccess,
    database_id: DatabaseId,
    open_session_id: OpenSessionId,
    next_cursor: IndexMigrationCursor,
    #[cfg(test)]
    substitute_before_apply: Option<MemoryIndexEntry>,
}

/// Unforgeable exact-end token for the memory structural stream.
#[derive(Debug)]
pub struct MemoryStructuralEvidenceEnd {
    cursor: StructuralEvidenceCursor,
}

/// Unforgeable exact-end token for the memory historical stream.
#[derive(Debug)]
pub struct MemoryHistoricalEvidenceEnd {
    cursor: HistoricalEvidenceCursor,
}

/// Exclusive memory-backend evidence session.
pub struct MemoryStructuralEvidenceSession {
    access: Option<MemoryAccess>,
    database_id: DatabaseId,
    open_session_id: OpenSessionId,
    retained_metadata: RetainedMetadataV1,
    inputs: StartupValidationInputs,
    structural_total: u64,
    authoritative_finding_seen: bool,
    saw_v1_index: bool,
    historical_position: HistoricalScanPosition,
    last_historical_key: Option<Vec<u8>>,
    next_structural: StructuralEvidenceCursor,
    next_historical: HistoricalEvidenceCursor,
    structural_finished: bool,
    historical_finished: bool,
}

impl fmt::Debug for MemoryStructuralEvidenceSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemoryStructuralEvidenceSession")
            .field("database_id", &self.database_id)
            .field("open_session_id", &self.open_session_id)
            .field("state", &"[EXCLUSIVE]")
            .finish()
    }
}

impl fmt::Debug for MemoryStartupIndexMigrationPort {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemoryStartupIndexMigrationPort")
            .field("database_id", &self.database_id)
            .field("open_session_id", &self.open_session_id)
            .field("next_cursor", &self.next_cursor)
            .field("state", &"[EXCLUSIVE]")
            .finish()
    }
}

struct StartupSnapshot {
    database_id: DatabaseId,
    retained_metadata: RetainedMetadataV1,
    structural_total: u64,
}

impl DormantPortBundle for MemoryDormantPorts {
    type CompletionAuthority = MemoryCompletionAuthority;
}

impl StructuralEvidenceEnd for MemoryStructuralEvidenceEnd {
    fn cursor(&self) -> StructuralEvidenceCursor {
        self.cursor
    }
}

impl HistoricalEvidenceEnd for MemoryHistoricalEvidenceEnd {
    fn cursor(&self) -> HistoricalEvidenceCursor {
        self.cursor
    }
}

impl StructuralEvidenceOpen for MemoryStore {
    type Session = MemoryStructuralEvidenceSession;

    fn begin_structural_evidence(
        self,
        inputs: StartupValidationInputs,
    ) -> Result<Self::Session, StorageError> {
        let access = self.acquire()?;
        let snapshot = access.read(collect_startup_header)?;
        let open_session_id = access.allocate_open_session()?;
        let next_structural =
            StructuralEvidenceCursor::start(snapshot.database_id, open_session_id);
        let next_historical =
            HistoricalEvidenceCursor::start(snapshot.database_id, open_session_id);
        Ok(MemoryStructuralEvidenceSession {
            access: Some(access),
            database_id: snapshot.database_id,
            open_session_id,
            retained_metadata: snapshot.retained_metadata,
            inputs,
            structural_total: snapshot.structural_total,
            authoritative_finding_seen: false,
            saw_v1_index: false,
            historical_position: HistoricalScanPosition::start(),
            last_historical_key: None,
            next_structural,
            next_historical,
            structural_finished: false,
            historical_finished: false,
        })
    }
}

impl StructuralEvidenceSession for MemoryStructuralEvidenceSession {
    type DormantPorts = MemoryDormantPorts;
    type StructuralEnd = MemoryStructuralEvidenceEnd;
    type HistoricalEnd = MemoryHistoricalEvidenceEnd;
    type MigrationPort = MemoryStartupIndexMigrationPort;

    fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    fn open_session_id(&self) -> OpenSessionId {
        self.open_session_id
    }

    fn read_structural_evidence(
        &mut self,
        cursor: StructuralEvidenceCursor,
        limit: EvidencePageLimit,
    ) -> Result<StructuralEvidencePage<Self::StructuralEnd>, StorageError> {
        if self.structural_finished || cursor != self.next_structural {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let position = usize::try_from(cursor.position())
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let total = usize::try_from(self.structural_total)
            .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
        if position > total {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        if position == total {
            self.structural_finished = true;
            return Ok(StructuralEvidencePage::ExactEnd(
                MemoryStructuralEvidenceEnd { cursor },
            ));
        }

        let requested = usize::try_from(limit.get())
            .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
        let count = usize::min(
            usize::min(requested, riffdb_storage_api::MAX_INTEGRITY_FINDINGS),
            total - position,
        );
        let findings = self
            .access
            .as_ref()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
            .read(|state| inspect_structural_slice(state, &self.inputs, position, count))?;
        if findings
            .iter()
            .any(|finding| finding.scope() == StructuralFindingScope::Authoritative)
        {
            self.authoritative_finding_seen = true;
        }
        let inspected =
            u64::try_from(count).map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
        let next = cursor.advanced(inspected).map_err(value_error_as_storage)?;
        let page =
            StructuralEvidencePage::page(cursor, findings, next).map_err(value_error_as_storage)?;
        self.next_structural = next;
        Ok(page)
    }

    fn read_historical_evidence(
        &mut self,
        cursor: HistoricalEvidenceCursor,
        limit: EvidencePageLimit,
    ) -> Result<HistoricalEvidencePage<Self::HistoricalEnd>, StorageError> {
        if self.historical_finished || cursor != self.next_historical {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let selected = self
            .access
            .as_ref()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
            .read(|state| {
                select_historical_page(
                    state,
                    self.historical_position,
                    self.last_historical_key.as_deref(),
                    &self.inputs,
                    limit,
                )
            })?;
        if selected.evidence.is_empty() {
            if !selected.exhausted {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            self.historical_finished = true;
            return Ok(HistoricalEvidencePage::ExactEnd(
                MemoryHistoricalEvidenceEnd { cursor },
            ));
        }

        let count = u64::try_from(selected.evidence.len())
            .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
        let next = cursor.advanced(count).map_err(value_error_as_storage)?;
        let page = HistoricalEvidencePage::page(cursor, selected.evidence, next)
            .map_err(value_error_as_storage)?;
        self.saw_v1_index |= selected.saw_v1_index;
        self.historical_position = selected.next_position;
        self.last_historical_key = selected.last_key;
        self.next_historical = next;
        Ok(page)
    }

    fn read_historical_bundle(
        &mut self,
        lineage: &ContractLineage,
        contract_version: ContractVersion,
        bundle_hash: ContractBundleHash,
    ) -> Result<Option<HistoricalBundleEvidence>, StorageError> {
        self.access
            .as_ref()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
            .read(|state| find_historical_bundle(state, lineage, contract_version, bundle_hash))
    }

    fn read_integrity_entity(
        &mut self,
        target: &riffdb_storage_api::EntityTarget,
    ) -> Result<Option<riffdb_storage_api::StoredEntityRecordV1>, StorageError> {
        self.access
            .as_ref()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
            .read(|state| {
                let position = state.entities.partition_point(|row| row.target() < target);
                let Some(record) = state.entities.get(position) else {
                    return Ok(None);
                };
                if record.target() != target {
                    return Ok(None);
                }
                if state
                    .entities
                    .get(position + 1)
                    .is_some_and(|next| next.target() == target)
                {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                Ok(Some(record.clone()))
            })
    }

    fn read_integrity_unique_occupancy(
        &mut self,
        target: &UniqueIndexTarget,
    ) -> Result<UniqueOccupancyKind, StorageError> {
        self.access
            .as_ref()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
            .read(|state| {
                let prefix = target.prefix().prefix().as_bytes();
                let start = state
                    .index_entries
                    .partition_point(|row| row.key().as_bytes() < prefix);
                let matching = state.index_entries[start..]
                    .iter()
                    .take(2)
                    .take_while(|row| row.key().as_bytes().starts_with(prefix))
                    .collect::<Vec<_>>();
                match matching.as_slice() {
                    [] => Ok(UniqueOccupancyKind::Vacant),
                    [entry]
                        if entry.current_record().is_some()
                            && entry.key() == target.expected_entry() =>
                    {
                        Ok(UniqueOccupancyKind::Owned)
                    }
                    [entry] if entry.current_record().is_some() => {
                        Ok(UniqueOccupancyKind::Conflict)
                    }
                    _ => Err(storage_error(StorageErrorKind::CorruptData)),
                }
            })
    }

    fn finish(
        mut self,
        structural_end: Self::StructuralEnd,
        historical_end: Self::HistoricalEnd,
    ) -> Result<StructuralOpenOutcome<Self::DormantPorts, Self::MigrationPort>, StorageError> {
        if !self.structural_finished
            || !self.historical_finished
            || structural_end.cursor != self.next_structural
            || historical_end.cursor != self.next_historical
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        if self.authoritative_finding_seen {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        let access = self
            .access
            .take()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        if self.saw_v1_index {
            return Ok(StructuralOpenOutcome::MigrationRequired(
                MemoryStartupIndexMigrationPort {
                    access,
                    database_id: self.database_id,
                    open_session_id: self.open_session_id,
                    next_cursor: IndexMigrationCursor::start(
                        self.database_id,
                        self.open_session_id,
                    ),
                    #[cfg(test)]
                    substitute_before_apply: None,
                },
            ));
        }
        let store = access.into_store();
        Ok(StructuralOpenOutcome::Clean(
            StructurallyOpened::from_finished_session(
                self.database_id,
                self.open_session_id,
                self.retained_metadata,
                MemoryDormantPorts { store },
                MemoryCompletionAuthority { _private: () },
            ),
        ))
    }
}

impl StartupIndexMigrationPort for MemoryStartupIndexMigrationPort {
    fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    fn open_session_id(&self) -> OpenSessionId {
        self.open_session_id
    }
}

impl CatalogIndexMigrationBackend for MemoryStartupIndexMigrationPort {
    type Output = MemoryStore;

    fn read_index_migration_page(
        self,
        request: CatalogIndexMigrationScanRequest<Self>,
    ) -> Result<CatalogIndexMigrationScan<Self>, StorageError> {
        let cursor = request.cursor();
        if cursor != self.next_cursor
            || cursor.database_id() != self.database_id
            || cursor.open_session_id() != self.open_session_id
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let evidence = self
            .access
            .read(|state| select_index_migration_page(state, cursor))?;
        if evidence.is_empty() {
            return request.exact_end(self).map_err(value_error_as_storage);
        }
        let count = u64::try_from(evidence.len())
            .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
        let next = cursor.advanced(count).map_err(value_error_as_storage)?;
        let mut backend = self;
        backend.next_cursor = next;
        request
            .page(backend, evidence, next)
            .map_err(value_error_as_storage)
    }

    fn read_historical_bundle(
        self,
        request: CatalogIndexMigrationBundleRequest<Self>,
    ) -> Result<CatalogIndexMigrationBundleResponse<Self>, StorageError> {
        let binding = request.evidence().row().schema_binding();
        let lineage = binding.lineage().clone();
        let version = binding.contract_version();
        let bundle_hash = binding.bundle_hash();
        let bundle = self
            .access
            .read(|state| find_historical_bundle(state, &lineage, version, bundle_hash))?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        request
            .respond(self, bundle)
            .map_err(value_error_as_storage)
    }

    fn apply_index_migration_batch(
        self,
        pending: CatalogIndexMigrationPendingBatch<Self>,
    ) -> Result<CatalogIndexMigrationApplied<Self>, StorageError> {
        let batch = pending.batch();
        if self.next_cursor != batch.next()
            || batch.next().database_id() != self.database_id
            || batch.next().open_session_id() != self.open_session_id
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        #[cfg(test)]
        if let Some(substitute) = &self.substitute_before_apply {
            let key = substitute.key().clone();
            self.access.write(|state| {
                let existing = state
                    .index_entries
                    .iter_mut()
                    .find(|entry| entry.key() == &key)
                    .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
                *existing = substitute.clone();
                Ok(())
            })?;
        }
        apply_index_migration_batch(&self.access, batch)?;
        pending.applied(self).map_err(value_error_as_storage)
    }

    fn finish_index_migration(
        self,
        completion: CatalogIndexMigrationCompletion<Self>,
    ) -> Result<MemoryStore, StorageError> {
        if completion.final_cursor() != self.next_cursor {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        Ok(self.access.into_store())
    }
}

fn collect_startup_header(state: &MemoryState) -> Result<StartupSnapshot, StorageError> {
    let metadata = match &state.metadata {
        MemoryMetadataSlot::Retained(metadata) => metadata,
        MemoryMetadataSlot::Absent => return Err(storage_error(StorageErrorKind::CorruptData)),
        #[cfg(test)]
        MemoryMetadataSlot::Corrupt => {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
    };
    Ok(StartupSnapshot {
        database_id: metadata.database_id(),
        retained_metadata: metadata.clone(),
        structural_total: structural_item_count(state)?,
    })
}

fn structural_item_count(state: &MemoryState) -> Result<u64, StorageError> {
    let mut total = 1usize;
    for length in [
        state.catalog_bundles.len(),
        state.catalog_activations.len(),
        state.catalog_bundle_activations.len(),
        state.query_modules.len(),
        state.active_query_modules.len(),
        state.administration_audit.len(),
        state.service_audit_invocations.len(),
        state.admissions.len(),
        state.entities.len(),
        state.entity_commits.len(),
        state.index_entries.len(),
        state.index_epochs.len(),
        state.historical_plan_references.len(),
        state.historical_persisted_keys.len(),
        state.commits.len(),
        state.commit_admissions.len(),
        state.committed_admissions.len(),
        state.provenance.len(),
        state.events.len(),
        state.event_routes.len(),
        state.outbox_intents.len(),
        state.outbox_statuses.len(),
        state.capabilities.len(),
        state.capability_lookups.len(),
        state.projection_controls.len(),
        state.projection_states.len(),
        state.projection_applies.len(),
    ] {
        total = total
            .checked_add(length)
            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
    }
    #[cfg(test)]
    {
        total = total
            .checked_add(state.injected_structural_findings.len())
            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
    }
    u64::try_from(total).map_err(|_| storage_error(StorageErrorKind::LimitExceeded))
}

fn inspect_structural_slice(
    state: &MemoryState,
    inputs: &StartupValidationInputs,
    start: usize,
    count: usize,
) -> Result<Vec<StructuralFinding>, StorageError> {
    let mut findings = Vec::with_capacity(count);
    let end = start
        .checked_add(count)
        .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
    for position in start..end {
        if let Some(finding) = inspect_structural_item(state, inputs, position)? {
            findings.push(finding);
        }
    }
    Ok(findings)
}

fn inspect_structural_item(
    state: &MemoryState,
    inputs: &StartupValidationInputs,
    position: usize,
) -> Result<Option<StructuralFinding>, StorageError> {
    let mut position = position;
    if position == 0 {
        return Ok(inspect_metadata(state));
    }
    position -= 1;

    macro_rules! locate {
        ($field:ident, $inspection:expr) => {
            if position < state.$field.len() {
                return Ok($inspection);
            }
            position -= state.$field.len();
        };
    }

    locate!(
        catalog_bundles,
        inspect_bundle(state, position).or_else(|| {
            crate::integrity_administration::inspect_bundle_activation(state, position)
        })
    );
    locate!(
        catalog_activations,
        crate::integrity_administration::inspect_catalog_activation_index(state, position)
    );
    locate!(
        catalog_bundle_activations,
        crate::integrity_administration::inspect_catalog_bundle_activation_index(state, position)
    );
    locate!(
        query_modules,
        crate::integrity_administration::inspect_query_module(state, position)
    );
    locate!(
        active_query_modules,
        crate::integrity_administration::inspect_active_query_module(state, position)
    );
    locate!(
        administration_audit,
        inspect_administration(state, position)
    );
    locate!(
        service_audit_invocations,
        crate::integrity_administration::inspect_service_invocation_index(state, position)
    );
    locate!(
        admissions,
        inspect_admission(state, inputs, position)
            .or_else(|| { crate::integrity_command::inspect_admission_graph(state, position) })
    );
    locate!(
        entities,
        inspect_entity(state, position)
            .or_else(|| { crate::integrity_command::inspect_entity_graph(state, position) })
    );
    locate!(
        entity_commits,
        crate::integrity_command::inspect_entity_commit_index(state, position)
    );
    locate!(index_entries, inspect_index_entry(state, position));
    locate!(index_epochs, inspect_index_epoch(state, position));
    locate!(
        historical_plan_references,
        inspect_historical_plan_reference(state, position)
    );
    locate!(
        historical_persisted_keys,
        inspect_historical_persisted_key(state, position)
    );
    locate!(
        commits,
        inspect_commit(state, position)
            .or_else(|| { crate::integrity_command::inspect_commit_graph(state, position) })
    );
    locate!(
        commit_admissions,
        crate::integrity_command::inspect_commit_admission_index(state, position)
    );
    locate!(
        committed_admissions,
        crate::integrity_command::inspect_committed_admission_index(state, position)
    );
    locate!(
        provenance,
        inspect_provenance(state, position)
            .or_else(|| { crate::integrity_command::inspect_provenance_graph(state, position) })
    );
    locate!(
        events,
        crate::integrity_command::inspect_event_graph(state, position)
    );
    locate!(
        event_routes,
        crate::integrity_command::inspect_event_route_graph(state, position)
    );
    locate!(
        outbox_intents,
        crate::integrity_command::inspect_outbox_intent_graph(state, position)
    );
    locate!(outbox_statuses, inspect_outbox_status(state, position));
    locate!(capabilities, inspect_capability(state, inputs, position));
    locate!(
        capability_lookups,
        inspect_capability_lookup(state, position)
    );
    locate!(
        projection_controls,
        crate::integrity_projection::inspect_projection_control(state, position)
    );
    locate!(
        projection_states,
        crate::integrity_projection::inspect_projection_state(state, position)
    );
    locate!(
        projection_applies,
        crate::integrity_projection::inspect_projection_apply(state, position)
    );
    #[cfg(test)]
    {
        if position < state.injected_structural_findings.len() {
            return Ok(Some(state.injected_structural_findings[position]));
        }
    }

    let _ = position;

    Err(storage_error(StorageErrorKind::InvariantViolation))
}

fn authoritative_finding(code: StructuralFindingCode) -> StructuralFinding {
    StructuralFinding::new(StructuralFindingScope::Authoritative, code)
}

fn inspect_metadata(state: &MemoryState) -> Option<StructuralFinding> {
    let MemoryMetadataSlot::Retained(metadata) = &state.metadata else {
        return Some(authoritative_finding(
            StructuralFindingCode::MalformedRecord,
        ));
    };
    if !application_sequence_is_consistent(state, metadata)
        || !administration_sequence_is_consistent(state, metadata)
    {
        return Some(authoritative_finding(
            StructuralFindingCode::SequenceDiscontinuity,
        ));
    }
    if state.query_modules.len() > MAX_RETAINED_QUERY_MODULES {
        return Some(authoritative_finding(StructuralFindingCode::LimitExceeded));
    }
    if !crate::integrity_administration::bootstrap_is_consistent(state, metadata)
        || !crate::integrity_administration::active_catalog_matches_last_activation(state, metadata)
    {
        return Some(authoritative_finding(
            StructuralFindingCode::CrossLinkMismatch,
        ));
    }
    match metadata.active_catalog() {
        None if !state.catalog_bundles.is_empty() || has_application_authoritative_state(state) => {
            Some(authoritative_finding(
                StructuralFindingCode::MissingCrossLink,
            ))
        }
        Some(active)
            if !bundle_identity_is_unique(
                state,
                active.lineage(),
                active.contract_version(),
                active.bundle_hash(),
            ) =>
        {
            Some(authoritative_finding(
                StructuralFindingCode::MissingCrossLink,
            ))
        }
        None | Some(_) => None,
    }
}

fn inspect_bundle(state: &MemoryState, index: usize) -> Option<StructuralFinding> {
    let row = &state.catalog_bundles[index];
    if row.order_key != bundle_evidence_order_key(&row.bundle)
        || (index > 0 && state.catalog_bundles[index - 1].order_key >= row.order_key)
    {
        return Some(authoritative_finding(
            StructuralFindingCode::CrossLinkMismatch,
        ));
    }
    None
}

fn inspect_administration(state: &MemoryState, index: usize) -> Option<StructuralFinding> {
    let expected = u64::try_from(index)
        .ok()
        .and_then(|value| value.checked_add(1))
        .and_then(riffdb_types::AdministrationSequence::new);
    if expected != Some(state.administration_audit[index].administration_sequence()) {
        return Some(authoritative_finding(
            StructuralFindingCode::SequenceDiscontinuity,
        ));
    }
    crate::integrity_administration::inspect_administration_graph(state, index)
}

fn inspect_admission(
    state: &MemoryState,
    inputs: &StartupValidationInputs,
    index: usize,
) -> Option<StructuralFinding> {
    let admission = &state.admissions[index];
    if Some(admission.identity().database_id()) != retained_database_id(state) {
        return Some(authoritative_finding(
            StructuralFindingCode::CrossLinkMismatch,
        ));
    }
    let digest = admission.identity().caller_key_digest();
    if !inputs
        .idempotency_digests()
        .as_slice()
        .contains(&ReadableDigestKey::v1(digest.key_id()))
    {
        return Some(authoritative_finding(
            StructuralFindingCode::DigestUnavailable,
        ));
    }
    let plan = admission_plan(admission);
    if !bundle_exists_for_plan(state, plan) || !historical_plan_reference_exists(state, plan) {
        return Some(authoritative_finding(
            StructuralFindingCode::MissingCrossLink,
        ));
    }
    if index > 0 {
        let prior = state.admissions[index - 1].identity().storage_key();
        let current = admission.identity().storage_key();
        if prior
            .ok()
            .zip(current.ok())
            .is_none_or(|(prior, current)| prior.as_bytes() >= current.as_bytes())
        {
            return Some(authoritative_finding(
                StructuralFindingCode::CrossLinkMismatch,
            ));
        }
    }
    None
}

fn inspect_entity(state: &MemoryState, index: usize) -> Option<StructuralFinding> {
    let record = &state.entities[index];
    let evidence = HistoricalPersistedKeyEvidenceV1::from_entity(record);
    if !bundle_exists_for_binding(state, record.schema_binding())
        || !historical_persisted_key_is_indexed(state, &evidence)
        || (index > 0 && state.entities[index - 1].target() >= record.target())
    {
        return Some(authoritative_finding(
            StructuralFindingCode::MissingCrossLink,
        ));
    }
    None
}

fn inspect_index_entry(state: &MemoryState, index: usize) -> Option<StructuralFinding> {
    let record = &state.index_entries[index];
    let evidence = match decode_memory_index_evidence(record) {
        Ok(evidence) => evidence,
        Err(error) => {
            let code = if error.kind() == StorageErrorKind::LimitExceeded {
                StructuralFindingCode::LimitExceeded
            } else {
                StructuralFindingCode::MalformedRecord
            };
            return Some(authoritative_finding(code));
        }
    };
    if !record.matches_migration_row(evidence.row())
        || !bundle_exists_for_binding(state, evidence.row().schema_binding())
        || (index > 0 && state.index_entries[index - 1].key().as_bytes() >= record.key().as_bytes())
    {
        return Some(authoritative_finding(
            StructuralFindingCode::CrossLinkMismatch,
        ));
    }
    None
}

fn decode_memory_index_evidence(
    record: &MemoryIndexEntry,
) -> Result<IndexMigrationRowEvidence, StorageError> {
    riffdb_storage_api::decode_index_migration_row(record.key(), record.observed_envelope())
        .map_err(durable_codec_error_as_storage)
}

fn inspect_index_epoch(state: &MemoryState, index: usize) -> Option<StructuralFinding> {
    let record = &state.index_epochs[index];
    let evidence = HistoricalPersistedKeyEvidenceV1::from_index_epoch(record);
    if !bundle_exists_for_binding(state, record.schema_binding())
        || !historical_persisted_key_is_indexed(state, &evidence)
        || (index > 0 && state.index_epochs[index - 1].target() >= record.target())
    {
        return Some(authoritative_finding(
            StructuralFindingCode::MissingCrossLink,
        ));
    }
    None
}

fn inspect_historical_plan_reference(
    state: &MemoryState,
    index: usize,
) -> Option<StructuralFinding> {
    let row = &state.historical_plan_references[index];
    if row.order_key != plan_evidence_order_key(&row.plan)
        || !bundle_exists_for_plan(state, &row.plan)
        || (index > 0 && state.historical_plan_references[index - 1].order_key >= row.order_key)
        || !historical_plan_source_exists(state, row)
    {
        return Some(authoritative_finding(
            StructuralFindingCode::MissingCrossLink,
        ));
    }
    None
}

fn inspect_historical_persisted_key(
    state: &MemoryState,
    index: usize,
) -> Option<StructuralFinding> {
    let row = &state.historical_persisted_keys[index];
    if row.order_key != persisted_evidence_order_key(&row.evidence)
        || (index > 0 && state.historical_persisted_keys[index - 1].order_key >= row.order_key)
        || !persisted_evidence_has_source(state, &row.evidence)
    {
        return Some(authoritative_finding(
            StructuralFindingCode::MissingCrossLink,
        ));
    }
    None
}

fn inspect_commit(state: &MemoryState, index: usize) -> Option<StructuralFinding> {
    let record = &state.commits[index];
    let expected = u64::try_from(index)
        .ok()
        .and_then(|value| value.checked_add(1))
        .and_then(riffdb_types::CommitSequence::new);
    if expected != Some(record.commit_sequence()) {
        return Some(authoritative_finding(
            StructuralFindingCode::SequenceDiscontinuity,
        ));
    }
    if !bundle_exists_for_plan(state, record.plan())
        || !historical_plan_reference_exists(state, record.plan())
    {
        return Some(authoritative_finding(
            StructuralFindingCode::MissingCrossLink,
        ));
    }
    None
}

fn inspect_provenance(state: &MemoryState, index: usize) -> Option<StructuralFinding> {
    let record = &state.provenance[index];
    if !bundle_exists_for_plan(state, record.plan())
        || !historical_plan_reference_exists(state, record.plan())
        || (index > 0 && state.provenance[index - 1].provenance_id() >= record.provenance_id())
    {
        return Some(authoritative_finding(
            StructuralFindingCode::MissingCrossLink,
        ));
    }
    None
}

fn inspect_outbox_status(state: &MemoryState, index: usize) -> Option<StructuralFinding> {
    let status = &state.outbox_statuses[index];
    if (index > 0 && state.outbox_statuses[index - 1].event_id() >= status.event_id())
        || find_unique_outbox_intent(state, status.event_id()).is_none()
    {
        return Some(StructuralFinding::new(
            StructuralFindingScope::OutboxDelivery,
            StructuralFindingCode::OrphanedOutboxStatus,
        ));
    }
    None
}

fn inspect_capability(
    state: &MemoryState,
    inputs: &StartupValidationInputs,
    index: usize,
) -> Option<StructuralFinding> {
    let capability = &state.capabilities[index];
    if Some(capability.database_id()) != retained_database_id(state)
        || (index > 0
            && state.capabilities[index - 1].capability_id() >= capability.capability_id())
    {
        return Some(authoritative_finding(
            StructuralFindingCode::CrossLinkMismatch,
        ));
    }
    if find_unique_capability_lookup(state, capability.token_digest())
        .is_none_or(|lookup| lookup.value.capability_id() != capability.capability_id())
    {
        return Some(authoritative_finding(
            StructuralFindingCode::MissingCrossLink,
        ));
    }
    let authorization_time = inputs.authorization_time();
    if active_unexpired_capability_requires_readable_digest(
        capability.lifecycle(),
        capability.expires_at(),
        authorization_time,
    ) && !inputs
        .capability_digests()
        .as_slice()
        .contains(&ReadableDigestKey::v1(capability.token_digest().key_id()))
    {
        return Some(authoritative_finding(
            StructuralFindingCode::DigestUnavailable,
        ));
    }
    crate::integrity_administration::inspect_capability_graph(state, capability)
}

fn active_unexpired_capability_requires_readable_digest(
    lifecycle: &CapabilityLifecycleV1,
    expires_at: riffdb_types::Timestamp,
    authorization_time: riffdb_types::Timestamp,
) -> bool {
    matches!(lifecycle, CapabilityLifecycleV1::Active) && authorization_time < expires_at
}

fn inspect_capability_lookup(state: &MemoryState, index: usize) -> Option<StructuralFinding> {
    let lookup = &state.capability_lookups[index];
    if (index > 0 && state.capability_lookups[index - 1].digest >= lookup.digest)
        || find_unique_capability(state, lookup.value.capability_id())
            .is_none_or(|capability| capability.token_digest() != lookup.digest)
    {
        return Some(authoritative_finding(
            StructuralFindingCode::MissingCrossLink,
        ));
    }
    None
}

fn retained_database_id(state: &MemoryState) -> Option<DatabaseId> {
    match &state.metadata {
        MemoryMetadataSlot::Retained(metadata) => Some(metadata.database_id()),
        MemoryMetadataSlot::Absent => None,
        #[cfg(test)]
        MemoryMetadataSlot::Corrupt => None,
    }
}

fn admission_plan(admission: &StoredAdmissionStateV1) -> &riffdb_storage_api::ExecutablePlanRef {
    match admission {
        StoredAdmissionStateV1::Pending(pending) => pending.plan(),
        StoredAdmissionStateV1::StoredOutcome(outcome) => outcome.plan(),
        StoredAdmissionStateV1::ExecutionFailed(failure) => failure.pending().plan(),
    }
}

fn bundle_exists_for_plan(
    state: &MemoryState,
    plan: &riffdb_storage_api::ExecutablePlanRef,
) -> bool {
    bundle_identity_is_unique(
        state,
        plan.contract_lineage(),
        plan.contract_version(),
        plan.contract_bundle_hash(),
    )
}

fn bundle_exists_for_binding(
    state: &MemoryState,
    binding: &riffdb_storage_api::DurableKeySchemaBindingV1,
) -> bool {
    bundle_identity_is_unique(
        state,
        binding.lineage(),
        binding.contract_version(),
        binding.bundle_hash(),
    )
}

fn historical_plan_reference_exists(
    state: &MemoryState,
    plan: &riffdb_storage_api::ExecutablePlanRef,
) -> bool {
    let order_key = plan_evidence_order_key(plan);
    state
        .historical_plan_references
        .binary_search_by(|row| row.order_key.as_slice().cmp(order_key.as_slice()))
        .ok()
        .is_some_and(|index| {
            let row = &state.historical_plan_references[index];
            row.order_key == plan_evidence_order_key(&row.plan) && row.plan == *plan
        })
}

fn historical_plan_source_exists(
    state: &MemoryState,
    row: &crate::state::HistoricalPlanReferenceRow,
) -> bool {
    use crate::state::HistoricalPlanReferenceSource;

    match &row.source {
        HistoricalPlanReferenceSource::Admission(key) => state
            .admissions
            .binary_search_by(|admission| {
                admission
                    .identity()
                    .storage_key()
                    .map_or(std::cmp::Ordering::Less, |candidate| candidate.cmp(key))
            })
            .ok()
            .is_some_and(|index| admission_plan(&state.admissions[index]) == &row.plan),
        HistoricalPlanReferenceSource::Commit(sequence) => state
            .commits
            .binary_search_by_key(
                sequence,
                riffdb_storage_api::StoredCommitRecordV1::commit_sequence,
            )
            .ok()
            .is_some_and(|index| state.commits[index].plan() == &row.plan),
        HistoricalPlanReferenceSource::Provenance(provenance_id) => state
            .provenance
            .binary_search_by_key(provenance_id, |record| record.provenance_id())
            .ok()
            .is_some_and(|index| state.provenance[index].plan() == &row.plan),
    }
}

fn historical_persisted_key_is_indexed(
    state: &MemoryState,
    evidence: &HistoricalPersistedKeyEvidenceV1,
) -> bool {
    let order_key = persisted_evidence_order_key(evidence);
    state
        .historical_persisted_keys
        .binary_search_by(|row| row.order_key.as_slice().cmp(order_key.as_slice()))
        .ok()
        .is_some_and(|index| state.historical_persisted_keys[index].evidence == *evidence)
}

fn persisted_evidence_has_source(
    state: &MemoryState,
    evidence: &HistoricalPersistedKeyEvidenceV1,
) -> bool {
    match evidence.key() {
        riffdb_storage_api::IrOpaquePersistedKeyV1::Entity {
            entity_type_id,
            key,
        } => state
            .entities
            .binary_search_by(|record| {
                record
                    .target()
                    .entity_type_id()
                    .cmp(entity_type_id)
                    .then_with(|| record.target().key().cmp(key))
            })
            .ok()
            .is_some_and(|index| {
                HistoricalPersistedKeyEvidenceV1::from_entity(&state.entities[index]) == *evidence
            }),
        riffdb_storage_api::IrOpaquePersistedKeyV1::IndexRangePrefix(_) => false,
        riffdb_storage_api::IrOpaquePersistedKeyV1::PartitionIndex(target) => state
            .index_epochs
            .binary_search_by(|record| record.target().cmp(target))
            .ok()
            .is_some_and(|index| {
                HistoricalPersistedKeyEvidenceV1::from_index_epoch(&state.index_epochs[index])
                    == *evidence
            }),
    }
}

fn bundle_identity_is_unique(
    state: &MemoryState,
    lineage: &riffdb_types::ContractLineage,
    version: riffdb_types::ContractVersion,
    hash: riffdb_types::ContractBundleHash,
) -> bool {
    let order_key = bundle_identity_evidence_order_key(lineage, version, hash);
    let found = state
        .catalog_bundles
        .binary_search_by(|row| row.order_key.as_slice().cmp(order_key.as_slice()));
    let Ok(index) = found else {
        return false;
    };
    let row = &state.catalog_bundles[index];
    if row.order_key != bundle_evidence_order_key(&row.bundle) {
        return false;
    }
    let same = |candidate: &crate::state::CatalogBundleRow| {
        candidate.bundle.lineage() == lineage
            && candidate.bundle.contract_version() == version
            && candidate.bundle.bundle_hash() == hash
    };
    (index == 0 || !same(&state.catalog_bundles[index - 1]))
        && (index + 1 == state.catalog_bundles.len() || !same(&state.catalog_bundles[index + 1]))
}

fn find_historical_bundle(
    state: &MemoryState,
    lineage: &ContractLineage,
    contract_version: ContractVersion,
    bundle_hash: ContractBundleHash,
) -> Result<Option<HistoricalBundleEvidence>, StorageError> {
    let order_key = bundle_identity_evidence_order_key(lineage, contract_version, bundle_hash);
    let position = match state
        .catalog_bundles
        .binary_search_by(|row| row.order_key.cmp(&order_key))
    {
        Ok(position) => position,
        Err(position) => {
            let query_matches = |row: &crate::state::CatalogBundleRow| {
                row.bundle.lineage() == lineage
                    && row.bundle.contract_version() == contract_version
                    && row.bundle.bundle_hash() == bundle_hash
            };
            if position
                .checked_sub(1)
                .and_then(|prior| state.catalog_bundles.get(prior))
                .is_some_and(query_matches)
                || state
                    .catalog_bundles
                    .get(position)
                    .is_some_and(query_matches)
            {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            return Ok(None);
        }
    };
    let row = &state.catalog_bundles[position];
    if row.order_key != bundle_evidence_order_key(&row.bundle)
        || row.bundle.lineage() != lineage
        || row.bundle.contract_version() != contract_version
        || row.bundle.bundle_hash() != bundle_hash
        || position
            .checked_sub(1)
            .and_then(|prior| state.catalog_bundles.get(prior))
            .is_some_and(|prior| prior.order_key >= order_key)
        || state
            .catalog_bundles
            .get(position + 1)
            .is_some_and(|next| next.order_key <= order_key)
    {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }

    let bytes = HistoricalBundleBytes::new(row.bundle.canonical_bytes().to_vec())
        .map_err(value_error_as_storage)?;
    Ok(Some(HistoricalBundleEvidence::new(
        row.bundle.lineage().clone(),
        row.bundle.contract_version(),
        row.bundle.bundle_hash(),
        bytes,
    )))
}

fn find_unique_outbox_intent(
    state: &MemoryState,
    event_id: riffdb_types::EventId,
) -> Option<&riffdb_storage_api::StoredOutboxIntentV1> {
    let index = state
        .outbox_intents
        .binary_search_by_key(
            &event_id,
            riffdb_storage_api::StoredOutboxIntentV1::event_id,
        )
        .ok()?;
    if (index > 0 && state.outbox_intents[index - 1].event_id() == event_id)
        || (index + 1 < state.outbox_intents.len()
            && state.outbox_intents[index + 1].event_id() == event_id)
    {
        return None;
    }
    Some(&state.outbox_intents[index])
}

fn find_unique_capability_lookup(
    state: &MemoryState,
    digest: riffdb_types::CapabilityTokenDigest,
) -> Option<&crate::state::CapabilityLookupRow> {
    let index = state
        .capability_lookups
        .binary_search_by_key(&digest, |lookup| lookup.digest)
        .ok()?;
    if (index > 0 && state.capability_lookups[index - 1].digest == digest)
        || (index + 1 < state.capability_lookups.len()
            && state.capability_lookups[index + 1].digest == digest)
    {
        return None;
    }
    Some(&state.capability_lookups[index])
}

fn find_unique_capability(
    state: &MemoryState,
    capability_id: riffdb_types::CapabilityId,
) -> Option<&riffdb_storage_api::StoredCapabilityRecordV1> {
    let index = state
        .capabilities
        .binary_search_by_key(&capability_id, |capability| capability.capability_id())
        .ok()?;
    if (index > 0 && state.capabilities[index - 1].capability_id() == capability_id)
        || (index + 1 < state.capabilities.len()
            && state.capabilities[index + 1].capability_id() == capability_id)
    {
        return None;
    }
    Some(&state.capabilities[index])
}

fn has_application_authoritative_state(state: &MemoryState) -> bool {
    !state.admissions.is_empty()
        || !state.entities.is_empty()
        || !state.index_entries.is_empty()
        || !state.index_epochs.is_empty()
        || !state.historical_plan_references.is_empty()
        || !state.historical_persisted_keys.is_empty()
        || !state.commits.is_empty()
        || !state.entity_commits.is_empty()
        || !state.commit_admissions.is_empty()
        || !state.committed_admissions.is_empty()
        || !state.provenance.is_empty()
        || !state.events.is_empty()
        || !state.outbox_intents.is_empty()
}

fn application_sequence_is_consistent(
    state: &MemoryState,
    metadata: &riffdb_storage_api::RetainedMetadataV1,
) -> bool {
    let allocator = match state.commits.last() {
        None => riffdb_storage_api::ApplicationSequenceAllocator::initial(),
        Some(record) => record.commit_sequence().checked_next().map_or(
            riffdb_storage_api::ApplicationSequenceAllocator::Exhausted,
            riffdb_storage_api::ApplicationSequenceAllocator::next,
        ),
    };
    metadata.application_sequence() == allocator
}

fn administration_sequence_is_consistent(
    state: &MemoryState,
    metadata: &riffdb_storage_api::RetainedMetadataV1,
) -> bool {
    let allocator = match state.administration_audit.last() {
        None => riffdb_storage_api::AdministrationSequenceAllocator::initial(),
        Some(record) => record.administration_sequence().checked_next().map_or(
            riffdb_storage_api::AdministrationSequenceAllocator::Exhausted,
            riffdb_storage_api::AdministrationSequenceAllocator::next,
        ),
    };
    metadata.administration_sequence() == allocator
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HistoricalNamespace {
    Bundles,
    Plans,
    Active,
    PersistedRows,
    CapabilityPartitions,
    End,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct HistoricalScanPosition {
    namespace: HistoricalNamespace,
    offset: usize,
    index_rows_emitted: usize,
    capability_entry_ordinal: usize,
}

impl HistoricalScanPosition {
    const fn start() -> Self {
        Self {
            namespace: HistoricalNamespace::Bundles,
            offset: 0,
            index_rows_emitted: 0,
            capability_entry_ordinal: 0,
        }
    }

    fn advance_namespace(&mut self) {
        self.namespace = match self.namespace {
            HistoricalNamespace::Bundles => HistoricalNamespace::Plans,
            HistoricalNamespace::Plans => HistoricalNamespace::Active,
            HistoricalNamespace::Active => HistoricalNamespace::PersistedRows,
            HistoricalNamespace::PersistedRows => HistoricalNamespace::CapabilityPartitions,
            HistoricalNamespace::CapabilityPartitions | HistoricalNamespace::End => {
                HistoricalNamespace::End
            }
        };
        self.offset = 0;
        self.index_rows_emitted = 0;
        self.capability_entry_ordinal = 0;
    }

    fn advance_capability_record(&mut self) -> Result<(), StorageError> {
        self.offset = self
            .offset
            .checked_add(1)
            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        self.capability_entry_ordinal = 0;
        Ok(())
    }

    fn advance_emitted_item(
        &mut self,
        source: HistoricalCandidateSource,
    ) -> Result<(), StorageError> {
        match source {
            HistoricalCandidateSource::CapabilityPartition => {
                self.capability_entry_ordinal = self
                    .capability_entry_ordinal
                    .checked_add(1)
                    .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            }
            HistoricalCandidateSource::IndexMigrationRow => {
                self.index_rows_emitted = self
                    .index_rows_emitted
                    .checked_add(1)
                    .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            }
            HistoricalCandidateSource::Default | HistoricalCandidateSource::PersistedKey => {
                self.offset = self
                    .offset
                    .checked_add(1)
                    .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            }
        }
        Ok(())
    }
}

struct SelectedHistoricalPage {
    evidence: Vec<HistoricalSemanticEvidence>,
    next_position: HistoricalScanPosition,
    last_key: Option<Vec<u8>>,
    exhausted: bool,
    saw_v1_index: bool,
}

enum HistoricalCandidate<'a> {
    Bundle(&'a riffdb_storage_api::StoredContractBundleV1),
    Plan(&'a riffdb_storage_api::ExecutablePlanRef),
    Active(Option<&'a riffdb_storage_api::ActiveCatalogPointerV1>),
    PersistedKey(&'a HistoricalPersistedKeyEvidenceV1),
    IndexMigrationRow(IndexMigrationRowEvidence),
    CapabilityPartition(HistoricalCapabilityPartitionEvidenceV1),
}

#[derive(Clone, Copy)]
enum HistoricalCandidateSource {
    Default,
    PersistedKey,
    IndexMigrationRow,
    CapabilityPartition,
}

struct HistoricalPageBuilder {
    maximum: usize,
    total_bytes: usize,
    migration_rows: usize,
    migration_evidence_bytes: usize,
    migration_instruction_bytes: usize,
    saw_v1_index: bool,
    evidence: Vec<HistoricalSemanticEvidence>,
    last_key: Option<Vec<u8>>,
}

impl HistoricalPageBuilder {
    fn new(limit: EvidencePageLimit) -> Result<Self, StorageError> {
        let maximum = usize::try_from(limit.get())
            .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
        Ok(Self {
            maximum,
            total_bytes: 0,
            migration_rows: 0,
            migration_evidence_bytes: 0,
            migration_instruction_bytes: 0,
            saw_v1_index: false,
            evidence: Vec::with_capacity(maximum),
            last_key: None,
        })
    }

    fn is_full(&self) -> bool {
        self.evidence.len() == self.maximum
    }

    fn push(
        &mut self,
        candidate: HistoricalCandidate<'_>,
        prior_page_key: Option<&[u8]>,
    ) -> Result<bool, StorageError> {
        let key = candidate.order_key();
        let prior = self.last_key.as_deref().or(prior_page_key);
        if prior.is_some_and(|prior| prior >= key.as_slice()) {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        let next_total = self
            .total_bytes
            .checked_add(candidate.semantic_bytes()?)
            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        if next_total > riffdb_storage_api::MAX_HISTORICAL_EVIDENCE_PAGE_BYTES {
            if self.evidence.is_empty() {
                return Err(storage_error(StorageErrorKind::LimitExceeded));
            }
            return Ok(false);
        }
        if let HistoricalCandidate::IndexMigrationRow(row) = &candidate {
            let next_rows = self
                .migration_rows
                .checked_add(1)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            let next_evidence = self
                .migration_evidence_bytes
                .checked_add(row.evidence_page_charge())
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            let next_instructions = self
                .migration_instruction_bytes
                .checked_add(row.instruction_page_charge())
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            if next_rows > riffdb_storage_api::MAX_INDEX_MIGRATION_PAGE_ENTRIES
                || next_evidence > riffdb_storage_api::MAX_INDEX_MIGRATION_PAGE_BYTES
                || next_instructions > riffdb_storage_api::MAX_INDEX_MIGRATION_PAGE_BYTES
            {
                if self.evidence.is_empty() {
                    return Err(storage_error(StorageErrorKind::LimitExceeded));
                }
                return Ok(false);
            }
            self.migration_rows = next_rows;
            self.migration_evidence_bytes = next_evidence;
            self.migration_instruction_bytes = next_instructions;
            self.saw_v1_index |= row.row().is_v1();
        }
        self.total_bytes = next_total;
        self.evidence.push(candidate.into_evidence()?);
        self.last_key = Some(key);
        Ok(true)
    }

    fn finish(
        self,
        next_position: HistoricalScanPosition,
        exhausted: bool,
    ) -> SelectedHistoricalPage {
        SelectedHistoricalPage {
            evidence: self.evidence,
            next_position,
            last_key: self.last_key,
            exhausted,
            saw_v1_index: self.saw_v1_index,
        }
    }
}

fn next_index_migration_evidence(
    state: &MemoryState,
    after: Option<&[u8]>,
) -> Result<Option<IndexMigrationRowEvidence>, StorageError> {
    let mut selected: Option<(Vec<u8>, IndexMigrationRowEvidence)> = None;
    for (index, record) in state.index_entries.iter().enumerate() {
        if index > 0 && state.index_entries[index - 1].key().as_bytes() >= record.key().as_bytes() {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        let evidence = decode_memory_index_evidence(record)?;
        if !record.matches_migration_row(evidence.row()) {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        let key = index_migration_evidence_order_key(&evidence);
        if after.is_some_and(|after| after >= key.as_slice()) {
            continue;
        }
        if selected
            .as_ref()
            .is_none_or(|(selected_key, _)| key.as_slice() < selected_key.as_slice())
        {
            selected = Some((key, evidence));
        }
    }
    Ok(selected.map(|(_, evidence)| evidence))
}

fn select_historical_page(
    state: &MemoryState,
    position: HistoricalScanPosition,
    prior_page_key: Option<&[u8]>,
    inputs: &StartupValidationInputs,
    limit: EvidencePageLimit,
) -> Result<SelectedHistoricalPage, StorageError> {
    let mut next = position;
    let mut page = HistoricalPageBuilder::new(limit)?;
    'selection: while !page.is_full() {
        let candidate = match next.namespace {
            HistoricalNamespace::Bundles => match state.catalog_bundles.get(next.offset) {
                Some(row) => {
                    let candidate = HistoricalCandidate::Bundle(&row.bundle);
                    if candidate.order_key() != row.order_key {
                        return Err(storage_error(StorageErrorKind::CorruptData));
                    }
                    candidate
                }
                None => {
                    next.advance_namespace();
                    continue;
                }
            },
            HistoricalNamespace::Plans => match state.historical_plan_references.get(next.offset) {
                Some(row) => {
                    let candidate = HistoricalCandidate::Plan(&row.plan);
                    if candidate.order_key() != row.order_key {
                        return Err(storage_error(StorageErrorKind::CorruptData));
                    }
                    candidate
                }
                None => {
                    next.advance_namespace();
                    continue;
                }
            },
            HistoricalNamespace::Active => {
                if next.offset != 0 {
                    next.advance_namespace();
                    continue;
                }
                let active = match &state.metadata {
                    MemoryMetadataSlot::Retained(metadata) => metadata.active_catalog(),
                    MemoryMetadataSlot::Absent => {
                        return Err(storage_error(StorageErrorKind::CorruptData));
                    }
                    #[cfg(test)]
                    MemoryMetadataSlot::Corrupt => {
                        return Err(storage_error(StorageErrorKind::CorruptData));
                    }
                };
                HistoricalCandidate::Active(active)
            }
            HistoricalNamespace::PersistedRows => {
                let persisted = state.historical_persisted_keys.get(next.offset).map(|row| {
                    let candidate = HistoricalCandidate::PersistedKey(&row.evidence);
                    (candidate, &row.order_key)
                });
                if persisted.as_ref().is_some_and(|(candidate, order_key)| {
                    candidate.order_key().as_slice() != order_key.as_slice()
                }) {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                let after = page.last_key.as_deref().or(prior_page_key);
                let index_row = next_index_migration_evidence(state, after)?
                    .map(HistoricalCandidate::IndexMigrationRow);
                match (persisted, index_row) {
                    (None, None) => {
                        if next.index_rows_emitted != state.index_entries.len() {
                            return Err(storage_error(StorageErrorKind::CorruptData));
                        }
                        next.advance_namespace();
                        continue;
                    }
                    (Some((persisted, _)), None) => persisted,
                    (None, Some(index_row)) => index_row,
                    (Some((persisted, _)), Some(index_row)) => {
                        let persisted_key = persisted.order_key();
                        let index_key = index_row.order_key();
                        if persisted_key == index_key {
                            return Err(storage_error(StorageErrorKind::CorruptData));
                        }
                        if persisted_key < index_key {
                            persisted
                        } else {
                            index_row
                        }
                    }
                }
            }
            HistoricalNamespace::CapabilityPartitions => loop {
                let Some(capability) = state.capabilities.get(next.offset) else {
                    next.advance_namespace();
                    continue 'selection;
                };
                if !capability_requires_partition_evidence(capability, inputs.authorization_time())
                {
                    next.advance_capability_record()?;
                    continue;
                }
                let Some(entries) = capability.grant().partition_scope().explicit_entries() else {
                    next.advance_capability_record()?;
                    continue;
                };
                if next.capability_entry_ordinal >= entries.len() {
                    next.advance_capability_record()?;
                    continue;
                }
                let evidence = HistoricalCapabilityPartitionEvidenceV1::from_capability_entry(
                    capability,
                    next.capability_entry_ordinal,
                )
                .map_err(value_error_as_storage)?;
                break HistoricalCandidate::CapabilityPartition(evidence);
            },
            HistoricalNamespace::End => break,
        };
        let source = candidate.source();
        if !page.push(candidate, prior_page_key)? {
            break;
        }
        next.advance_emitted_item(source)?;
    }
    Ok(page.finish(next, next.namespace == HistoricalNamespace::End))
}

fn select_index_migration_page(
    state: &MemoryState,
    cursor: IndexMigrationCursor,
) -> Result<Vec<IndexMigrationRowEvidence>, StorageError> {
    let start = usize::try_from(cursor.position())
        .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
    if start > state.index_entries.len() {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    let mut evidence = Vec::new();
    let mut evidence_bytes = 0usize;
    let mut instruction_bytes = 0usize;
    for index in start..state.index_entries.len() {
        if evidence.len() == riffdb_storage_api::MAX_INDEX_MIGRATION_PAGE_ENTRIES {
            break;
        }
        let record = &state.index_entries[index];
        if index > 0 && state.index_entries[index - 1].key().as_bytes() >= record.key().as_bytes() {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        let row = decode_memory_index_evidence(record)?;
        if !record.matches_migration_row(row.row()) {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        let next_evidence = evidence_bytes
            .checked_add(row.evidence_page_charge())
            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        let next_instructions = instruction_bytes
            .checked_add(row.instruction_page_charge())
            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        if next_evidence > riffdb_storage_api::MAX_INDEX_MIGRATION_PAGE_BYTES
            || next_instructions > riffdb_storage_api::MAX_INDEX_MIGRATION_PAGE_BYTES
        {
            if evidence.is_empty() {
                return Err(storage_error(StorageErrorKind::LimitExceeded));
            }
            break;
        }
        evidence_bytes = next_evidence;
        instruction_bytes = next_instructions;
        evidence.push(row);
    }
    Ok(evidence)
}

fn apply_index_migration_batch(
    access: &MemoryAccess,
    batch: &CatalogIndexMigrationBatch<MemoryStartupIndexMigrationPort>,
) -> Result<(), StorageError> {
    access.write(|state| {
        let mut replacements = Vec::new();
        let mut prior_key: Option<&[u8]> = None;
        for instruction in batch.instructions() {
            let expected = instruction.expected();
            let key = expected.physical_key();
            if prior_key.is_some_and(|prior| prior >= key.as_bytes()) {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            prior_key = Some(key.as_bytes());
            let index = state
                .index_entries
                .binary_search_by(|record| record.key().as_bytes().cmp(key.as_bytes()))
                .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
            if (index > 0 && state.index_entries[index - 1].key() == key)
                || (index + 1 < state.index_entries.len()
                    && state.index_entries[index + 1].key() == key)
            {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            let current = &state.index_entries[index];
            match instruction {
                CatalogIndexMigrationInstruction::V1Rewrite(rewrite) => {
                    let encoded = riffdb_storage_api::encode_index_entry_v2(rewrite.replacement())
                        .map_err(durable_codec_error_as_storage)?;
                    if encoded.encoded_content_charge().get()
                        > expected.conservative_v2_envelope_charge().get()
                    {
                        return Err(storage_error(StorageErrorKind::InvariantViolation));
                    }
                    if current.observed_envelope() == expected.canonical_envelope() {
                        if !current.matches_migration_row(expected.row()) {
                            return Err(storage_error(StorageErrorKind::CorruptData));
                        }
                        replacements.push((
                            index,
                            MemoryIndexEntry::current_from_encoded(
                                rewrite.replacement().clone(),
                                encoded.into_bytes(),
                                memory_record_charge(),
                            ),
                        ));
                    } else if current.observed_envelope() == encoded.as_bytes() {
                        let replay = riffdb_storage_api::decode_index_migration_row(
                            key,
                            current.observed_envelope(),
                        )
                        .map_err(durable_codec_error_as_storage)?;
                        if replay.row().stored_partition()
                            != Some(rewrite.replacement().partition_key())
                            || replay.row().schema_binding()
                                != rewrite.replacement().schema_binding()
                            || replay.row().covered_values()
                                != rewrite.replacement().covered_values()
                        {
                            return Err(storage_error(StorageErrorKind::CorruptData));
                        }
                    } else {
                        return Err(storage_error(StorageErrorKind::CorruptData));
                    }
                }
                CatalogIndexMigrationInstruction::V2Confirm(_) => {
                    if current.observed_envelope() != expected.canonical_envelope()
                        || !current.matches_migration_row(expected.row())
                    {
                        return Err(storage_error(StorageErrorKind::CorruptData));
                    }
                }
            }
        }
        for (index, replacement) in replacements {
            state.index_entries[index] = replacement;
        }
        Ok(())
    })
}

impl HistoricalCandidate<'_> {
    const fn source(&self) -> HistoricalCandidateSource {
        match self {
            Self::PersistedKey(_) => HistoricalCandidateSource::PersistedKey,
            Self::IndexMigrationRow(_) => HistoricalCandidateSource::IndexMigrationRow,
            Self::CapabilityPartition(_) => HistoricalCandidateSource::CapabilityPartition,
            Self::Bundle(_) | Self::Plan(_) | Self::Active(_) => HistoricalCandidateSource::Default,
        }
    }

    fn order_key(&self) -> Vec<u8> {
        let mut key = Vec::new();
        match self {
            Self::Bundle(bundle) => {
                return bundle_evidence_order_key(bundle);
            }
            Self::Plan(plan) => {
                return plan_evidence_order_key(plan);
            }
            Self::Active(None) => key.extend_from_slice(&[0x03, 0x00]),
            Self::Active(Some(active)) => {
                key.extend_from_slice(&[0x03, 0x01]);
                push_lineage(&mut key, active.lineage());
                key.extend_from_slice(&active.contract_version().to_be_bytes());
                key.extend_from_slice(active.bundle_hash().as_bytes());
            }
            Self::PersistedKey(evidence) => return persisted_evidence_order_key(evidence),
            Self::IndexMigrationRow(evidence) => {
                return index_migration_evidence_order_key(evidence);
            }
            Self::CapabilityPartition(evidence) => return evidence.evidence_order_key(),
        }
        key
    }

    fn semantic_bytes(&self) -> Result<usize, StorageError> {
        let bytes = match self {
            Self::Bundle(bundle) => bundle
                .canonical_bytes()
                .len()
                .checked_add(1 + 4 + bundle.lineage().as_bytes().len() + 8 + 32 + 4),
            Self::Plan(plan) => {
                (1 + 4 + plan.contract_lineage().as_bytes().len()).checked_add(8 + 32 + 4 + 32)
            }
            Self::Active(None) => Some(2),
            Self::Active(Some(active)) => {
                (2 + 4 + active.lineage().as_bytes().len()).checked_add(8 + 32)
            }
            Self::PersistedKey(evidence) => persisted_evidence_semantic_bytes(evidence),
            Self::IndexMigrationRow(evidence) => Some(evidence.evidence_page_charge()),
            Self::CapabilityPartition(evidence) => {
                Some(evidence.semantic_bytes().map_err(value_error_as_storage)?)
            }
        };
        bytes.ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))
    }

    fn into_evidence(self) -> Result<HistoricalSemanticEvidence, StorageError> {
        match self {
            Self::Bundle(bundle) => {
                let bytes = HistoricalBundleBytes::new(bundle.canonical_bytes().to_vec())
                    .map_err(value_error_as_storage)?;
                Ok(HistoricalSemanticEvidence::Bundle(
                    HistoricalBundleEvidence::new(
                        bundle.lineage().clone(),
                        bundle.contract_version(),
                        bundle.bundle_hash(),
                        bytes,
                    ),
                ))
            }
            Self::Plan(plan) => Ok(HistoricalSemanticEvidence::PlanReference(plan.clone())),
            Self::Active(active) => Ok(HistoricalSemanticEvidence::ActiveCatalog(active.map(
                |active| {
                    HistoricalActiveCatalogEvidence::new(
                        active.lineage().clone(),
                        active.contract_version(),
                        active.bundle_hash(),
                    )
                },
            ))),
            Self::PersistedKey(evidence) => {
                Ok(HistoricalSemanticEvidence::PersistedKey(evidence.clone()))
            }
            Self::IndexMigrationRow(evidence) => {
                Ok(HistoricalSemanticEvidence::IndexMigrationRow(evidence))
            }
            Self::CapabilityPartition(evidence) => {
                Ok(HistoricalSemanticEvidence::CapabilityPartition(evidence))
            }
        }
    }
}

fn capability_requires_partition_evidence(
    capability: &riffdb_storage_api::StoredCapabilityRecordV1,
    authorization_time: riffdb_types::Timestamp,
) -> bool {
    matches!(capability.lifecycle(), CapabilityLifecycleV1::Active)
        && authorization_time < capability.expires_at()
}

pub(crate) fn persisted_evidence_order_key(evidence: &HistoricalPersistedKeyEvidenceV1) -> Vec<u8> {
    let mut output = Vec::new();
    match evidence.key() {
        riffdb_storage_api::IrOpaquePersistedKeyV1::Entity {
            entity_type_id,
            key,
        } => {
            push_persisted_binding_key(&mut output, evidence.schema(), 0x01);
            output.extend_from_slice(&entity_type_id.to_be_bytes());
            push_bytes(&mut output, key.as_bytes());
        }
        riffdb_storage_api::IrOpaquePersistedKeyV1::IndexRangePrefix(prefix) => {
            push_persisted_binding_key(&mut output, evidence.schema(), 0x02);
            output.extend_from_slice(&prefix.index_id().to_be_bytes());
            push_bytes(&mut output, prefix.as_bytes());
        }
        riffdb_storage_api::IrOpaquePersistedKeyV1::PartitionIndex(target) => {
            push_persisted_binding_key(&mut output, evidence.schema(), 0x03);
            output.extend_from_slice(&target.index_id().to_be_bytes());
            push_bytes(&mut output, target.partition_key().as_bytes());
        }
    }
    output
}

fn index_migration_evidence_order_key(evidence: &IndexMigrationRowEvidence) -> Vec<u8> {
    let mut output = Vec::new();
    push_persisted_binding_key(&mut output, evidence.row().schema_binding(), 0x02);
    output.extend_from_slice(&evidence.physical_key().index_id().to_be_bytes());
    push_bytes(&mut output, evidence.physical_key().as_bytes());
    output
}

fn push_persisted_binding_key(
    output: &mut Vec<u8>,
    binding: &riffdb_storage_api::DurableKeySchemaBindingV1,
    key_tag: u8,
) {
    output.push(0x04);
    push_lineage(output, binding.lineage());
    output.extend_from_slice(&binding.contract_version().to_be_bytes());
    output.extend_from_slice(binding.bundle_hash().as_bytes());
    output.push(key_tag);
}

fn persisted_evidence_semantic_bytes(evidence: &HistoricalPersistedKeyEvidenceV1) -> Option<usize> {
    let key_bytes = match evidence.key() {
        riffdb_storage_api::IrOpaquePersistedKeyV1::Entity { key, .. } => key.as_bytes().len(),
        riffdb_storage_api::IrOpaquePersistedKeyV1::IndexRangePrefix(prefix) => {
            prefix.as_bytes().len()
        }
        riffdb_storage_api::IrOpaquePersistedKeyV1::PartitionIndex(target) => {
            target.partition_key().as_bytes().len()
        }
    };
    let binding = evidence.schema();
    (4 + binding.lineage().as_bytes().len())
        .checked_add(8 + 32)
        .and_then(|value| value.checked_add(key_bytes + 1 + 4 + 4))
        .and_then(|value| value.checked_add(1))
}

fn push_lineage(output: &mut Vec<u8>, lineage: &riffdb_types::ContractLineage) {
    push_bytes(output, lineage.as_bytes());
}

fn push_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    let length = u32::try_from(bytes.len()).expect("foundational bounds fit u32");
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(bytes);
}

fn value_error_as_storage(error: StorageValueError) -> StorageError {
    let kind = match error {
        StorageValueError::LimitExceeded | StorageValueError::SizeOverflow => {
            StorageErrorKind::LimitExceeded
        }
        StorageValueError::Empty
        | StorageValueError::NonCanonicalOrder
        | StorageValueError::Duplicate
        | StorageValueError::IdentityMismatch
        | StorageValueError::InvalidShape => StorageErrorKind::CorruptData,
    };
    storage_error(kind)
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
    use std::sync::OnceLock;

    use riffdb_catalog::{
        CatalogHistoryOutcome, CatalogIndexMigrationDriveError, CatalogIndexMigrationDriver,
        ValidatedContractBundle, validate_catalog_history,
    };
    use riffdb_contract_compiler::compile_contract_source;
    use riffdb_storage_api::{
        ActiveCatalogPointerV1, AdministrationSequenceAllocator, ApplicationSequenceAllocator,
        AuditPrincipalV1, CapabilityBootstrapMarkerV1, CapabilityGrantV1,
        CapabilityPermissionKindV1, CapabilityPermissionV1, CapabilityPermissionsV1,
        CapabilityRequestedRecordV1, CatalogActivationIntentV1, CatalogAdministrationRepository,
        DatabaseIdentityProbe, DatabaseIdentityProbePort, DatabaseInitializationPort,
        DurableKeySchemaBindingV1, ExecutablePlanRef, HistoricalEvidencePage, IndexEpochAdvanceV1,
        IndexEpochPosition, IrOpaquePersistedKeyV1, PartitionScopeV1,
        ReadableCapabilityDigestInventory, ReadableIdempotencyDigestInventory,
        RevocationReasonCodeV1, ScopedPartitionV1, StoredCapabilityRecordV1,
        StoredContractBundleV1, StoredEntityRecordV1, StoredIndexEntryV1, StoredIndexEntryV2,
        StoredProjectionApplyV1, StoredProjectionControlV1, StructuralEvidenceOpen,
        StructuralEvidencePage, StructuralEvidenceSession, StructuralFindingCode,
        StructuralFindingScope,
    };
    use riffdb_types::{
        ActorId, ActorKind, AdministrationSequence, AggregateTypeId, Audience, CanonicalRecord,
        CanonicalValue, CapabilityId, CapabilityTokenDigest, CommandId, CommitSequence,
        ContractBundleHash, ContractLineage, ContractVersion, DatabaseId, DigestKeyId,
        EntityKeyBuilder, EntityTypeId, EntityVersion, Environment, FieldId, IndexEntryKeyBuilder,
        IndexId, MAX_CANONICAL_DOCUMENT_BYTES, PartitionKeyBuilder, PlanHash, ProjectionApplyHash,
        ProjectionApplyKey, ProjectionGeneration, ProjectionId, ProjectionIdentity,
        ProjectionPlanHash, RequestId, TenantScope, Timestamp,
    };

    use super::*;

    const MEMORY_MIGRATION_CONTRACT: &str = r#"
contract MemoryMigration version 1 {
  entity Row {
    key (id: u64)
    field value: u64
    index ByValue(value)
  }

  aggregate Rows {
    root Row
    partition_by id
    conflict_key (id)
  }
}
"#;

    fn validated_migration_bundle() -> &'static ValidatedContractBundle {
        static BUNDLE: OnceLock<ValidatedContractBundle> = OnceLock::new();
        BUNDLE.get_or_init(|| {
            ValidatedContractBundle::from_compiler_bundle(
                compile_contract_source(MEMORY_MIGRATION_CONTRACT)
                    .expect("compile indexed memory migration contract"),
            )
            .expect("validate indexed memory migration bundle")
        })
    }

    fn compiled_migration_legacy_row(
        bundle: &ValidatedContractBundle,
        value: u64,
        covered_values: CanonicalRecord,
    ) -> StoredIndexEntryV1 {
        let entity_schema = bundle
            .bundle()
            .schema()
            .entities()
            .first()
            .expect("migration entity");
        let index_schema = entity_schema.indexes().first().expect("migration index");
        let mut entity = EntityKeyBuilder::new(entity_schema.id());
        entity.push_u64(value).expect("entity component");
        let mut index = IndexEntryKeyBuilder::new(index_schema.id());
        index.push_u64(value).expect("index component");
        StoredIndexEntryV1::new(
            index
                .finish(entity.finish().expect("entity key"))
                .expect("index key"),
            DurableKeySchemaBindingV1::new(
                bundle.lineage().clone(),
                bundle.contract_version(),
                bundle.bundle_hash(),
            ),
            covered_values,
        )
        .expect("legacy migration row")
    }

    fn scan_structural_end(
        session: &mut MemoryStructuralEvidenceSession,
    ) -> MemoryStructuralEvidenceEnd {
        let mut cursor =
            StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
        loop {
            match session
                .read_structural_evidence(cursor, EvidencePageLimit::new(500).expect("page limit"))
                .expect("structural page")
            {
                StructuralEvidencePage::Page { findings, next, .. } => {
                    assert!(findings.is_empty());
                    cursor = next;
                }
                StructuralEvidencePage::ExactEnd(end) => return end,
            }
        }
    }

    fn database(byte: u8) -> DatabaseId {
        let mut bytes = [0; 16];
        bytes[..10].copy_from_slice(&[0x01, 0x8f, 0x00, 0x00, 0x00, 0x00, 0x70, 0x01, 0x80, 0x02]);
        bytes[15] = byte;
        DatabaseId::from_bytes(bytes).expect("UUIDv7 database ID")
    }

    fn inputs() -> StartupValidationInputs {
        inputs_at(1)
    }

    fn inputs_at(seconds: i64) -> StartupValidationInputs {
        let key_id = DigestKeyId::new(1).expect("digest key ID");
        StartupValidationInputs::new(
            Timestamp::new(seconds, 0).expect("timestamp"),
            ReadableCapabilityDigestInventory::new(vec![ReadableDigestKey::v1(key_id)])
                .expect("capability inventory"),
            ReadableIdempotencyDigestInventory::new(vec![ReadableDigestKey::v1(key_id)])
                .expect("idempotency inventory"),
        )
    }

    fn initialized_store(id: DatabaseId) -> MemoryStore {
        let mut store = MemoryStore::new();
        store.initialize_database(id).expect("initialize database");
        store
    }

    fn deployed_store(id: DatabaseId, bundle: StoredContractBundleV1) -> MemoryStore {
        let store = initialized_store(id);
        let reopened = store.reopen();
        let mut ports = MemoryDormantPorts { store }.into_operational();
        let intent = CatalogActivationIntentV1::new(
            None,
            bundle,
            RequestId::from_bytes(uuid_v7(0x70)).expect("request ID"),
            AuditPrincipalV1::new(
                ActorId::new("migration-operator").expect("actor ID"),
                ActorKind::Human,
                capability_id(0x71),
                NonZeroU64::MIN,
            ),
            Timestamp::new(1, 0).expect("timestamp"),
            None,
        );
        ports
            .activate_catalog(&intent)
            .expect("activate catalog fixture");
        drop(ports);
        reopened
    }

    fn migration_binding() -> DurableKeySchemaBindingV1 {
        DurableKeySchemaBindingV1::new(
            ContractLineage::new("migration-boundaries").expect("lineage"),
            ContractVersion::new(1).expect("version"),
            ContractBundleHash::from_bytes([0x74; 32]),
        )
    }

    fn migration_legacy_row(value: u64, covered_values: CanonicalRecord) -> StoredIndexEntryV1 {
        let mut entity = EntityKeyBuilder::new(EntityTypeId::first());
        entity.push_u64(value).expect("entity component");
        let mut index = IndexEntryKeyBuilder::new(IndexId::first());
        index.push_u64(value).expect("index component");
        StoredIndexEntryV1::new(
            index
                .finish(entity.finish().expect("entity key"))
                .expect("index key"),
            migration_binding(),
            covered_values,
        )
        .expect("legacy migration row")
    }

    #[test]
    fn startup_header_clones_all_six_retained_metadata_categories() {
        let id = database(0x31);
        let active = ActiveCatalogPointerV1::new(
            ContractLineage::new("retained").expect("lineage"),
            ContractVersion::new(9).expect("version"),
            ContractBundleHash::from_bytes([0x45; 32]),
        );
        let marker = CapabilityBootstrapMarkerV1::new(
            id,
            capability_id(0x32),
            AdministrationSequence::new(17).expect("administration sequence"),
        );
        let metadata = RetainedMetadataV1::new(
            riffdb_storage_api::StorageFormatVersion::V1,
            id,
            ApplicationSequenceAllocator::Exhausted,
            AdministrationSequenceAllocator::Exhausted,
            riffdb_storage_api::HISTORY_INCARNATION_INITIAL,
            Some(active),
            Some(marker),
        )
        .expect("complete retained metadata");
        let state = MemoryState {
            metadata: MemoryMetadataSlot::Retained(metadata.clone()),
            ..MemoryState::default()
        };

        let snapshot = collect_startup_header(&state).expect("startup header");
        assert_eq!(snapshot.database_id, id);
        assert_eq!(snapshot.retained_metadata, metadata);
    }

    fn uuid_v7(seed: u8) -> [u8; 16] {
        let mut bytes = [seed; 16];
        bytes[6] = 0x70 | (seed & 0x0f);
        bytes[8] = 0x80 | (seed & 0x3f);
        bytes
    }

    fn capability_id(seed: u8) -> CapabilityId {
        CapabilityId::from_bytes(uuid_v7(seed)).expect("capability ID")
    }

    fn scoped_partition(lineage: &ContractLineage, value: u64) -> ScopedPartitionV1 {
        let mut builder =
            PartitionKeyBuilder::new(AggregateTypeId::new(0x0102_0304).expect("aggregate type ID"));
        builder.push_u64(value).expect("partition component");
        ScopedPartitionV1::new(lineage.clone(), builder.finish().expect("partition key"))
    }

    fn active_capability(
        database_id: DatabaseId,
        capability_id: CapabilityId,
        scope: PartitionScopeV1,
        issued_at: i64,
        expires_at: i64,
    ) -> StoredCapabilityRecordV1 {
        let duration = u32::try_from(expires_at - issued_at).expect("positive bounded duration");
        let permissions = CapabilityPermissionsV1::new(vec![
            CapabilityPermissionV1::unparameterized(
                CapabilityPermissionKindV1::AdministerCapabilities,
            )
            .expect("permission"),
        ])
        .expect("permissions");
        let grant = CapabilityGrantV1::new(
            TenantScope::Global,
            scope,
            permissions,
            Vec::new(),
            NonZeroU16::MIN,
            Vec::new(),
        )
        .expect("grant");
        let requested = CapabilityRequestedRecordV1::new(
            database_id,
            Environment::new("test").expect("environment"),
            ActorId::new("subject").expect("actor ID"),
            ActorKind::Human,
            NonZeroU32::new(duration).expect("nonzero duration"),
            vec![Audience::new("riffdb-test").expect("audience")],
            grant,
        )
        .expect("requested capability");
        StoredCapabilityRecordV1::active(
            capability_id,
            CapabilityTokenDigest::from_hmac_bytes(
                DigestKeyId::new(1).expect("digest key ID"),
                [capability_id.as_bytes()[15]; 32],
            ),
            requested,
            Timestamp::new(issued_at, 0).expect("issued at"),
            Timestamp::new(expires_at, 0).expect("expires at"),
            AdministrationSequence::first(),
            RequestId::from_bytes(uuid_v7(capability_id.as_bytes()[15].wrapping_add(1)))
                .expect("request ID"),
        )
        .expect("active capability")
    }

    fn collect_historical(
        store: MemoryStore,
        inputs: StartupValidationInputs,
        limit: EvidencePageLimit,
    ) -> Vec<HistoricalSemanticEvidence> {
        let mut session = store
            .begin_structural_evidence(inputs)
            .expect("begin evidence");
        let mut cursor =
            HistoricalEvidenceCursor::start(session.database_id(), session.open_session_id());
        let mut collected = Vec::new();
        while let HistoricalEvidencePage::Page { evidence, next, .. } = session
            .read_historical_evidence(cursor, limit)
            .expect("historical evidence")
        {
            collected.extend(evidence);
            cursor = next;
        }
        collected
    }

    #[test]
    fn active_unexpired_capability_digest_is_required_even_before_issue_time() {
        let authorization_time = Timestamp::new(10, 0).expect("authorization time");
        let issued_at = Timestamp::new(20, 0).expect("future issue time");
        let expires_at = Timestamp::new(30, 0).expect("expiry");
        assert!(issued_at > authorization_time);
        assert!(active_unexpired_capability_requires_readable_digest(
            &CapabilityLifecycleV1::Active,
            expires_at,
            authorization_time,
        ));
    }

    #[test]
    fn historical_capability_inventory_filters_lifecycle_time_and_all_scope() {
        let id = database(0x41);
        let store = initialized_store(id);
        let lineage = ContractLineage::new("budget").expect("lineage");
        let shared_partition = scoped_partition(&lineage, 1);

        let future_issued = active_capability(
            id,
            capability_id(0x10),
            PartitionScopeV1::explicit(vec![
                scoped_partition(&lineage, 3),
                shared_partition.clone(),
                scoped_partition(&lineage, 2),
            ])
            .expect("explicit scope"),
            100,
            160,
        );
        let exact_expiry = active_capability(
            id,
            capability_id(0x20),
            PartitionScopeV1::explicit(vec![scoped_partition(&lineage, 4)])
                .expect("explicit scope"),
            10,
            70,
        );
        let expired = active_capability(
            id,
            capability_id(0x30),
            PartitionScopeV1::explicit(vec![scoped_partition(&lineage, 5)])
                .expect("explicit scope"),
            9,
            69,
        );
        let all = active_capability(id, capability_id(0x40), PartitionScopeV1::All, 100, 160);
        let revoked = active_capability(
            id,
            capability_id(0x50),
            PartitionScopeV1::explicit(vec![scoped_partition(&lineage, 6)])
                .expect("explicit scope"),
            100,
            160,
        )
        .revoked(
            NonZeroU64::MIN,
            Timestamp::new(101, 0).expect("revoked at"),
            AdministrationSequence::new(2).expect("revoke sequence"),
            RevocationReasonCodeV1::Requested,
        )
        .expect("revoked capability");
        let repeated_in_other_capability = active_capability(
            id,
            capability_id(0x60),
            PartitionScopeV1::explicit(vec![shared_partition.clone()]).expect("explicit scope"),
            100,
            160,
        );

        store
            .acquire()
            .expect("access")
            .write(|state| {
                state.capabilities = vec![
                    future_issued,
                    exact_expiry,
                    expired,
                    all,
                    revoked,
                    repeated_in_other_capability,
                ];
                Ok(())
            })
            .expect("install capabilities");

        let evidence = collect_historical(
            store,
            inputs_at(70),
            EvidencePageLimit::new(2).expect("page limit"),
        );
        assert!(matches!(
            evidence.first(),
            Some(HistoricalSemanticEvidence::ActiveCatalog(None))
        ));
        let partitions = evidence
            .iter()
            .filter_map(|item| match item {
                HistoricalSemanticEvidence::CapabilityPartition(value) => Some(value),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            partitions
                .iter()
                .map(|value| (value.capability_id(), value.entry_ordinal()))
                .collect::<Vec<_>>(),
            [
                (capability_id(0x10), 0),
                (capability_id(0x10), 1),
                (capability_id(0x10), 2),
                (capability_id(0x60), 0),
            ]
        );
        assert_eq!(partitions[0].scoped_partition(), &shared_partition);
        assert_eq!(partitions[3].scoped_partition(), &shared_partition);
    }

    #[test]
    fn historical_capability_pages_preserve_all_1024_sibling_ordinals() {
        let id = database(0x42);
        let store = initialized_store(id);
        let lineage = ContractLineage::new("maximum-siblings").expect("lineage");
        let scope = PartitionScopeV1::explicit(
            (0..riffdb_storage_api::MAX_CAPABILITY_PARTITIONS)
                .map(|value| {
                    scoped_partition(&lineage, u64::try_from(value).expect("bounded ordinal"))
                })
                .collect(),
        )
        .expect("maximum explicit scope");
        let owner = capability_id(0x11);
        let capability = active_capability(id, owner, scope, 100, 160);
        store
            .acquire()
            .expect("access")
            .write(|state| {
                state.capabilities.push(capability);
                Ok(())
            })
            .expect("install capability");

        let mut session = store
            .begin_structural_evidence(inputs_at(70))
            .expect("begin evidence");
        let mut cursor = HistoricalEvidenceCursor::start(id, session.open_session_id());
        let limit = EvidencePageLimit::new(500).expect("maximum count page");
        let mut page_lengths = Vec::new();
        let mut ordinals = Vec::new();
        while let HistoricalEvidencePage::Page { evidence, next, .. } = session
            .read_historical_evidence(cursor, limit)
            .expect("historical page")
        {
            page_lengths.push(evidence.len());
            ordinals.extend(evidence.into_iter().filter_map(|item| match item {
                HistoricalSemanticEvidence::CapabilityPartition(value) => {
                    assert_eq!(value.capability_id(), owner);
                    Some(value.entry_ordinal())
                }
                HistoricalSemanticEvidence::ActiveCatalog(None) => None,
                _ => panic!("unexpected historical item"),
            }));
            cursor = next;
        }
        assert_eq!(page_lengths, [500, 500, 25]);
        assert_eq!(ordinals.len(), 1024);
        assert_eq!(
            ordinals,
            (0..=1023).map(|value| value as u16).collect::<Vec<_>>()
        );
        assert_eq!(cursor.position(), 1025);
    }

    #[test]
    fn historical_capability_partition_matches_the_shared_golden_vector() {
        let id = database(0x43);
        let store = initialized_store(id);
        let capability_id = CapabilityId::from_bytes([
            0x01, 0x8f, 0x00, 0x00, 0x00, 0x00, 0x70, 0x01, 0x80, 0x02, 0x11, 0x22, 0x33, 0x44,
            0x55, 0x66,
        ])
        .expect("golden capability ID");
        let lineage = ContractLineage::new("budget").expect("lineage");
        let scope = PartitionScopeV1::explicit(vec![
            scoped_partition(&lineage, 1),
            scoped_partition(&lineage, 2),
            scoped_partition(&lineage, 0x0102_0304_0506_0708),
        ])
        .expect("explicit scope");
        let capability = active_capability(id, capability_id, scope, 100, 160);
        store
            .acquire()
            .expect("access")
            .write(|state| {
                state.capabilities.push(capability);
                Ok(())
            })
            .expect("install capability");

        let evidence = collect_historical(
            store,
            inputs_at(70),
            EvidencePageLimit::new(8).expect("page limit"),
        );
        let golden = evidence
            .iter()
            .find_map(|item| match item {
                HistoricalSemanticEvidence::CapabilityPartition(value)
                    if value.entry_ordinal() == 2 =>
                {
                    Some(value)
                }
                _ => None,
            })
            .expect("golden evidence");
        assert_eq!(
            golden.evidence_order_key(),
            vec![
                0x05, 0x01, 0x8f, 0x00, 0x00, 0x00, 0x00, 0x70, 0x01, 0x80, 0x02, 0x11, 0x22, 0x33,
                0x44, 0x55, 0x66, 0x00, 0x02, 0x00, 0x00, 0x00, 0x06, b'b', b'u', b'd', b'g', b'e',
                b't', 0x01, 0x02, 0x03, 0x04, 0x00, 0x00, 0x00, 0x0e, 0x50, 0x01, 0x01, 0x02, 0x03,
                0x04, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
            ]
        );
        assert_eq!(golden.semantic_bytes(), Ok(51));
    }

    #[test]
    fn one_item_structural_pages_cross_authoritative_and_projection_families() {
        let id = database(9);
        let store = initialized_store(id);
        let entity_type = EntityTypeId::first();
        let mut entity_key = EntityKeyBuilder::new(entity_type);
        entity_key.push_u64(1).expect("entity component");
        let target = riffdb_storage_api::EntityTarget::new(
            entity_type,
            entity_key.finish().expect("entity key"),
        )
        .expect("entity target");
        let projection = ProjectionIdentity::new(
            ContractLineage::new("integrity-page").expect("lineage"),
            ProjectionId::first(),
            ProjectionPlanHash::from_bytes([0x71; 32]),
        );
        store
            .acquire()
            .expect("access")
            .write(|state| {
                state
                    .catalog_activations
                    .push(crate::state::CatalogActivationIndexRow {
                        administration_sequence: riffdb_types::AdministrationSequence::first(),
                    });
                state
                    .entity_commits
                    .push(crate::state::EntityCommitIndexRow {
                        target,
                        commit_sequence: CommitSequence::first(),
                    });
                state
                    .projection_controls
                    .push(StoredProjectionControlV1::initial(projection.clone()));
                state.projection_applies.push(StoredProjectionApplyV1::new(
                    ProjectionApplyKey::new(
                        projection,
                        ProjectionGeneration::first(),
                        CommitSequence::first(),
                    ),
                    ProjectionApplyHash::from_bytes([0x72; 32]),
                ));
                Ok(())
            })
            .expect("install corrupt families");

        let mut session = store
            .begin_structural_evidence(inputs())
            .expect("begin evidence");
        let mut cursor = StructuralEvidenceCursor::start(id, session.open_session_id());
        let limit = EvidencePageLimit::new(1).expect("one item per page");
        let mut findings = Vec::new();
        while let StructuralEvidencePage::Page {
            findings: page,
            next,
            ..
        } = session
            .read_structural_evidence(cursor, limit)
            .expect("bounded structural page")
        {
            assert!(page.len() <= 1);
            findings.extend(page);
            cursor = next;
        }
        assert!(
            findings
                .iter()
                .any(|finding| { finding.scope() == StructuralFindingScope::Authoritative })
        );
        assert!(
            findings
                .iter()
                .any(|finding| finding.scope() == StructuralFindingScope::Projection)
        );
    }

    #[test]
    fn evidence_cannot_begin_before_initialization() {
        assert_eq!(
            MemoryStore::new()
                .begin_structural_evidence(inputs())
                .expect_err("uninitialized evidence must fail")
                .kind(),
            StorageErrorKind::CorruptData
        );
    }

    #[test]
    fn current_v2_index_row_passes_the_codec_bound_startup_path() {
        let lineage = ContractLineage::new("v2-startup-guard").expect("lineage");
        let version = ContractVersion::new(1).expect("version");
        let bundle_hash = ContractBundleHash::from_bytes([0x61; 32]);
        let binding = DurableKeySchemaBindingV1::new(lineage.clone(), version, bundle_hash);
        let mut entity = EntityKeyBuilder::new(EntityTypeId::new(1).expect("entity type"));
        entity.push_u64(1).expect("entity component");
        let mut index = IndexEntryKeyBuilder::new(IndexId::new(1).expect("index ID"));
        index.push_u64(1).expect("index component");
        let key = index
            .finish(entity.finish().expect("entity key"))
            .expect("index key");
        let mut partition =
            PartitionKeyBuilder::new(AggregateTypeId::new(1).expect("aggregate type"));
        partition.push_u64(1).expect("partition component");
        let row = StoredIndexEntryV2::new(
            key,
            binding,
            CanonicalRecord::new(Vec::new()).expect("covered values"),
            partition.finish().expect("partition key"),
        )
        .expect("V2 row");
        let mut state = MemoryState::default();
        state
            .catalog_bundles
            .push(crate::state::CatalogBundleRow::new(
                StoredContractBundleV1::new(
                    lineage,
                    version,
                    bundle_hash,
                    b"checked bundle".to_vec(),
                )
                .expect("bundle"),
            ));
        state.index_entries.push(
            crate::state::MemoryIndexEntry::current(row, crate::state::memory_record_charge())
                .expect("canonical V2 fixture"),
        );

        assert_eq!(inspect_index_entry(&state, 0), None);
    }

    #[test]
    fn open_session_proofs_are_process_unique_across_independent_stores() {
        let id = database(7);
        let mut left = initialized_store(id)
            .begin_structural_evidence(inputs())
            .expect("begin left evidence");
        let mut right = initialized_store(id)
            .begin_structural_evidence(inputs())
            .expect("begin right evidence");
        assert_ne!(left.open_session_id(), right.open_session_id());

        let structural_end = |session: &mut MemoryStructuralEvidenceSession| {
            let start = StructuralEvidenceCursor::start(id, session.open_session_id());
            let next = match session
                .read_structural_evidence(start, EvidencePageLimit::new(1).expect("page limit"))
                .expect("metadata page")
            {
                StructuralEvidencePage::Page { next, .. } => next,
                StructuralEvidencePage::ExactEnd(_) => panic!("metadata must be inspected"),
            };
            match session
                .read_structural_evidence(next, EvidencePageLimit::new(1).expect("page limit"))
                .expect("structural exact end")
            {
                StructuralEvidencePage::ExactEnd(end) => end,
                StructuralEvidencePage::Page { .. } => panic!("unexpected structural page"),
            }
        };
        let left_end = structural_end(&mut left);
        let right_end = structural_end(&mut right);
        assert_ne!(
            left_end.cursor().open_session_id(),
            right_end.cursor().open_session_id()
        );
    }

    #[test]
    fn cursors_are_exact_session_bound_and_single_use() {
        let id = database(1);
        let mut session = initialized_store(id)
            .begin_structural_evidence(inputs())
            .expect("begin evidence");
        let limit = EvidencePageLimit::new(2).expect("page limit");
        let structural_start = StructuralEvidenceCursor::start(id, session.open_session_id());
        let wrong_database =
            StructuralEvidenceCursor::start(database(2), session.open_session_id());
        assert_eq!(
            session
                .read_structural_evidence(wrong_database, limit)
                .expect_err("database mismatch")
                .kind(),
            StorageErrorKind::InvariantViolation
        );
        let wrong_session_id = OpenSessionId::new(session.open_session_id().get() + 1)
            .expect("different open-session ID");
        let wrong_session = StructuralEvidenceCursor::start(id, wrong_session_id);
        assert_eq!(
            session
                .read_structural_evidence(wrong_session, limit)
                .expect_err("session mismatch")
                .kind(),
            StorageErrorKind::InvariantViolation
        );
        let structural_next = match session
            .read_structural_evidence(structural_start, limit)
            .expect("metadata structural page")
        {
            StructuralEvidencePage::Page { findings, next, .. } => {
                assert!(findings.is_empty());
                next
            }
            StructuralEvidencePage::ExactEnd(_) => panic!("metadata must be inspected"),
        };
        assert_eq!(
            session
                .read_structural_evidence(structural_start, limit)
                .expect_err("page cursor cannot repeat")
                .kind(),
            StorageErrorKind::InvariantViolation
        );
        let structural_end = match session
            .read_structural_evidence(structural_next, limit)
            .expect("structural exact end")
        {
            StructuralEvidencePage::ExactEnd(end) => end,
            StructuralEvidencePage::Page { .. } => panic!("unexpected structural page"),
        };

        let historical_start = HistoricalEvidenceCursor::start(id, session.open_session_id());
        let historical_next = match session
            .read_historical_evidence(historical_start, limit)
            .expect("active-none evidence")
        {
            HistoricalEvidencePage::Page { evidence, next, .. } => {
                assert_eq!(evidence.len(), 1);
                next
            }
            HistoricalEvidencePage::ExactEnd(_) => panic!("active relation must be enumerated"),
        };
        let skipped = historical_next.advanced(1).expect("skipped cursor");
        assert_eq!(
            session
                .read_historical_evidence(skipped, limit)
                .expect_err("skipped cursor")
                .kind(),
            StorageErrorKind::InvariantViolation
        );
        let historical_end = match session
            .read_historical_evidence(historical_next, limit)
            .expect("historical exact end")
        {
            HistoricalEvidencePage::ExactEnd(end) => end,
            HistoricalEvidencePage::Page { .. } => panic!("unexpected historical page"),
        };

        let outcome = session
            .finish(structural_end, historical_end)
            .expect("finish exact session");
        let StructuralOpenOutcome::Clean(opened) = outcome else {
            panic!("V2-only state must open cleanly");
        };
        assert_eq!(opened.database_id(), id);
        assert_eq!(opened.retained_metadata(), &RetainedMetadataV1::initial(id));
        let (_, _, metadata, dormant) = opened.into_parts();
        assert_eq!(metadata, RetainedMetadataV1::initial(id));
        assert!(!dormant.store.gate_is_held());
    }

    #[test]
    fn historical_bundle_lookup_is_cursor_neutral_and_live_through_exact_end() {
        let id = database(10);
        let store = initialized_store(id);
        let lineage = ContractLineage::new("bundle-lookup").expect("lineage");
        let version = ContractVersion::new(3).expect("contract version");
        let bundle_hash = ContractBundleHash::from_bytes([0x41; 32]);
        let canonical_bytes = b"opaque canonical bundle".to_vec();
        let bundle = StoredContractBundleV1::new(
            lineage.clone(),
            version,
            bundle_hash,
            canonical_bytes.clone(),
        )
        .expect("bounded bundle");
        store
            .acquire()
            .expect("access")
            .write(|state| {
                state
                    .catalog_bundles
                    .push(crate::state::CatalogBundleRow::new(bundle));
                Ok(())
            })
            .expect("install historical bundle");

        let mut session = store
            .begin_structural_evidence(inputs())
            .expect("begin evidence");
        assert!(
            session
                .read_historical_bundle(
                    &lineage,
                    version,
                    ContractBundleHash::from_bytes([0x42; 32]),
                )
                .expect("absent lookup")
                .is_none()
        );
        let observed = session
            .read_historical_bundle(&lineage, version, bundle_hash)
            .expect("bundle lookup")
            .expect("historical bundle");
        assert_eq!(observed.lineage(), &lineage);
        assert_eq!(observed.version(), version);
        assert_eq!(observed.bundle_hash(), bundle_hash);
        assert_eq!(observed.bytes().as_bytes(), canonical_bytes);

        let limit = EvidencePageLimit::new(1).expect("page limit");
        let historical_start = HistoricalEvidenceCursor::start(id, session.open_session_id());
        let historical_next = match session
            .read_historical_evidence(historical_start, limit)
            .expect("bundle page after point read")
        {
            HistoricalEvidencePage::Page { evidence, next, .. } => {
                assert!(matches!(
                    evidence.as_slice(),
                    [HistoricalSemanticEvidence::Bundle(bundle)]
                        if bundle.bundle_hash() == bundle_hash
                ));
                next
            }
            HistoricalEvidencePage::ExactEnd(_) => panic!("bundle must be enumerated"),
        };
        assert!(
            session
                .read_historical_bundle(&lineage, version, bundle_hash)
                .expect("lookup between pages")
                .is_some()
        );
        let historical_next = match session
            .read_historical_evidence(historical_next, limit)
            .expect("active-none page after point read")
        {
            HistoricalEvidencePage::Page { next, .. } => next,
            HistoricalEvidencePage::ExactEnd(_) => panic!("active relation must be enumerated"),
        };
        let historical_end = match session
            .read_historical_evidence(historical_next, limit)
            .expect("historical exact end")
        {
            HistoricalEvidencePage::ExactEnd(end) => end,
            HistoricalEvidencePage::Page { .. } => panic!("unexpected historical page"),
        };
        assert!(
            session
                .read_historical_bundle(&lineage, version, bundle_hash)
                .expect("lookup after historical exact end")
                .is_some()
        );

        let mut structural = StructuralEvidenceCursor::start(id, session.open_session_id());
        let structural_end = loop {
            match session
                .read_structural_evidence(structural, limit)
                .expect("structural evidence")
            {
                StructuralEvidencePage::Page { next, .. } => structural = next,
                StructuralEvidencePage::ExactEnd(end) => break end,
            }
        };
        assert!(
            session
                .read_historical_bundle(&lineage, version, bundle_hash)
                .expect("lookup after both exact ends")
                .is_some()
        );
        drop((session, structural_end, historical_end));
    }

    #[test]
    fn historical_bundle_lookup_accepts_exact_maximum_bytes() {
        let id = database(13);
        let store = initialized_store(id);
        let lineage = ContractLineage::new("maximum-bundle-lookup").expect("lineage");
        let version = ContractVersion::new(1).expect("contract version");
        let bundle_hash = ContractBundleHash::from_bytes([0x4f; 32]);
        let bundle = StoredContractBundleV1::new(
            lineage.clone(),
            version,
            bundle_hash,
            vec![0xa5; riffdb_storage_api::MAX_CATALOG_BUNDLE_BYTES],
        )
        .expect("exact maximum bundle");
        store
            .acquire()
            .expect("access")
            .write(|state| {
                state
                    .catalog_bundles
                    .push(crate::state::CatalogBundleRow::new(bundle));
                Ok(())
            })
            .expect("install maximum bundle");

        let mut session = store
            .begin_structural_evidence(inputs())
            .expect("begin evidence");
        let observed = session
            .read_historical_bundle(&lineage, version, bundle_hash)
            .expect("maximum bundle lookup")
            .expect("historical bundle");
        assert_eq!(
            observed.bytes().as_bytes().len(),
            riffdb_storage_api::MAX_CATALOG_BUNDLE_BYTES
        );
        assert!(observed.bytes().as_bytes().iter().all(|byte| *byte == 0xa5));
    }

    #[test]
    fn historical_bundle_lookup_rejects_duplicate_and_noncanonical_rows() {
        let id = database(11);
        let lineage = ContractLineage::new("corrupt-bundle-lookup").expect("lineage");
        let version = ContractVersion::new(1).expect("contract version");
        let bundle_hash = ContractBundleHash::from_bytes([0x51; 32]);
        let bundle = StoredContractBundleV1::new(
            lineage.clone(),
            version,
            bundle_hash,
            b"canonical bundle".to_vec(),
        )
        .expect("bundle");

        let duplicate_store = initialized_store(id);
        duplicate_store
            .acquire()
            .expect("access")
            .write(|state| {
                let row = crate::state::CatalogBundleRow::new(bundle.clone());
                state.catalog_bundles = vec![row.clone(), row];
                Ok(())
            })
            .expect("install duplicate rows");
        let mut duplicate_session = duplicate_store
            .begin_structural_evidence(inputs())
            .expect("begin duplicate evidence");
        assert_eq!(
            duplicate_session
                .read_historical_bundle(&lineage, version, bundle_hash)
                .expect_err("duplicate identity must fail closed")
                .kind(),
            StorageErrorKind::CorruptData
        );

        let noncanonical_store = initialized_store(database(12));
        noncanonical_store
            .acquire()
            .expect("access")
            .write(|state| {
                let mut row = crate::state::CatalogBundleRow::new(bundle);
                row.order_key.push(0xff);
                state.catalog_bundles.push(row);
                Ok(())
            })
            .expect("install noncanonical row");
        let mut noncanonical_session = noncanonical_store
            .begin_structural_evidence(inputs())
            .expect("begin noncanonical evidence");
        assert_eq!(
            noncanonical_session
                .read_historical_bundle(&lineage, version, bundle_hash)
                .expect_err("noncanonical identity key must fail closed")
                .kind(),
            StorageErrorKind::CorruptData
        );
    }

    #[test]
    fn structural_pages_advance_exactly_and_dropped_sessions_release_the_gate() {
        let id = database(3);
        let store = initialized_store(id);
        store
            .acquire()
            .expect("access")
            .write(|state| {
                state.injected_structural_findings.extend([
                    StructuralFinding::new(
                        StructuralFindingScope::Authoritative,
                        StructuralFindingCode::MalformedRecord,
                    ),
                    StructuralFinding::new(
                        StructuralFindingScope::Projection,
                        StructuralFindingCode::ProjectionStateMismatch,
                    ),
                ]);
                Ok(())
            })
            .expect("inject findings");
        let observer = store.reopen();
        let mut session = store
            .begin_structural_evidence(inputs())
            .expect("begin evidence");
        assert!(observer.gate_is_held());

        let limit = EvidencePageLimit::new(1).expect("page limit");
        let mut cursor = StructuralEvidenceCursor::start(id, session.open_session_id());
        let mut findings = Vec::new();
        while let StructuralEvidencePage::Page {
            findings: page,
            next,
            ..
        } = session
            .read_structural_evidence(cursor, limit)
            .expect("bounded structural page")
        {
            findings.extend(page);
            cursor = next;
        }
        assert_eq!(findings.len(), 2);

        drop(session);
        assert!(!observer.gate_is_held());
        assert_eq!(
            observer
                .probe_database_identity()
                .expect("probe after dropped session"),
            DatabaseIdentityProbe::Existing(id)
        );
    }

    #[test]
    fn authoritative_findings_cannot_release_structurally_opened_ports() {
        let id = database(5);
        let store = initialized_store(id);
        store
            .acquire()
            .expect("access")
            .write(|state| {
                state
                    .injected_structural_findings
                    .push(StructuralFinding::new(
                        StructuralFindingScope::Authoritative,
                        StructuralFindingCode::MalformedRecord,
                    ));
                Ok(())
            })
            .expect("inject authoritative finding");
        let mut session = store
            .begin_structural_evidence(inputs())
            .expect("begin evidence");
        let limit = EvidencePageLimit::new(10).expect("page limit");

        let structural_start = StructuralEvidenceCursor::start(id, session.open_session_id());
        let structural_next = match session
            .read_structural_evidence(structural_start, limit)
            .expect("structural page")
        {
            StructuralEvidencePage::Page { findings, next, .. } => {
                assert_eq!(findings.len(), 1);
                next
            }
            StructuralEvidencePage::ExactEnd(_) => panic!("records require a page"),
        };
        let structural_end = match session
            .read_structural_evidence(structural_next, limit)
            .expect("structural exact end")
        {
            StructuralEvidencePage::ExactEnd(end) => end,
            StructuralEvidencePage::Page { .. } => panic!("unexpected structural page"),
        };

        let historical_start = HistoricalEvidenceCursor::start(id, session.open_session_id());
        let historical_next = match session
            .read_historical_evidence(historical_start, limit)
            .expect("historical page")
        {
            HistoricalEvidencePage::Page { next, .. } => next,
            HistoricalEvidencePage::ExactEnd(_) => panic!("active relation requires a page"),
        };
        let historical_end = match session
            .read_historical_evidence(historical_next, limit)
            .expect("historical exact end")
        {
            HistoricalEvidencePage::ExactEnd(end) => end,
            HistoricalEvidencePage::Page { .. } => panic!("unexpected historical page"),
        };

        assert_eq!(
            session
                .finish(structural_end, historical_end)
                .expect_err("authoritative finding must withhold ports")
                .kind(),
            StorageErrorKind::CorruptData
        );
    }

    #[test]
    fn derived_findings_do_not_withhold_structurally_opened_ports() {
        let id = database(6);
        let store = initialized_store(id);
        store
            .acquire()
            .expect("access")
            .write(|state| {
                state
                    .injected_structural_findings
                    .push(StructuralFinding::new(
                        StructuralFindingScope::Projection,
                        StructuralFindingCode::ProjectionStateMismatch,
                    ));
                Ok(())
            })
            .expect("inject derived finding");
        let mut session = store
            .begin_structural_evidence(inputs())
            .expect("begin evidence");
        let limit = EvidencePageLimit::new(2).expect("page limit");

        let mut structural = StructuralEvidenceCursor::start(id, session.open_session_id());
        let structural_end = loop {
            match session
                .read_structural_evidence(structural, limit)
                .expect("structural evidence")
            {
                StructuralEvidencePage::Page { next, .. } => structural = next,
                StructuralEvidencePage::ExactEnd(end) => break end,
            }
        };
        let mut historical = HistoricalEvidenceCursor::start(id, session.open_session_id());
        let historical_end = loop {
            match session
                .read_historical_evidence(historical, limit)
                .expect("historical evidence")
            {
                HistoricalEvidencePage::Page { next, .. } => historical = next,
                HistoricalEvidencePage::ExactEnd(end) => break end,
            }
        };

        let outcome = session
            .finish(structural_end, historical_end)
            .expect("derived findings remain separately degradable");
        let StructuralOpenOutcome::Clean(opened) = outcome else {
            panic!("V2-only state must open cleanly");
        };
        assert_eq!(opened.database_id(), id);
    }

    #[test]
    fn historical_evidence_enumerates_every_schema_bound_persisted_key() {
        let id = database(4);
        let store = initialized_store(id);
        let lineage = ContractLineage::new("budget").expect("lineage");
        let version = ContractVersion::new(1).expect("contract version");
        let bundle_hash = ContractBundleHash::from_bytes([7; 32]);
        let binding = DurableKeySchemaBindingV1::new(lineage.clone(), version, bundle_hash);
        let bundle = StoredContractBundleV1::new(
            lineage,
            version,
            bundle_hash,
            b"canonical bundle".to_vec(),
        )
        .expect("bundle");

        let entity_type = EntityTypeId::new(1).expect("entity type");
        let mut entity_builder = EntityKeyBuilder::new(entity_type);
        entity_builder.push_u64(42).expect("entity component");
        let entity_key = entity_builder.finish().expect("entity key");
        let target = riffdb_storage_api::EntityTarget::new(entity_type, entity_key.clone())
            .expect("entity target");
        let entity = StoredEntityRecordV1::new(
            target,
            EntityVersion::first(),
            version,
            binding.clone(),
            CanonicalRecord::new(Vec::new()).expect("empty record"),
        )
        .expect("entity record");

        let index_id = IndexId::new(2).expect("index ID");
        let mut index_builder = IndexEntryKeyBuilder::new(index_id);
        index_builder.push_u64(9).expect("index component");
        let index_key = index_builder.finish(entity_key).expect("index key");
        let index_entry = StoredIndexEntryV1::new(
            index_key,
            binding.clone(),
            CanonicalRecord::new(Vec::new()).expect("empty record"),
        )
        .expect("index entry");

        let mut partition_builder = PartitionKeyBuilder::new(AggregateTypeId::first());
        partition_builder.push_u64(42).expect("partition component");
        let epoch = IndexEpochAdvanceV1::new(
            riffdb_storage_api::PartitionIndexTarget::new(
                partition_builder.finish().expect("partition key"),
                index_id,
            ),
            binding.clone(),
            IndexEpochPosition::BeforeFirst,
        )
        .expect("epoch advance")
        .post_image()
        .clone();

        store
            .acquire()
            .expect("access")
            .write(|state| {
                let mut persisted = [
                    HistoricalPersistedKeyEvidenceV1::from_entity(&entity),
                    HistoricalPersistedKeyEvidenceV1::from_index_epoch(&epoch),
                ]
                .into_iter()
                .map(|evidence| crate::state::HistoricalPersistedKeyRow {
                    order_key: persisted_evidence_order_key(&evidence),
                    evidence,
                })
                .collect::<Vec<_>>();
                persisted.sort_by(|left, right| left.order_key.cmp(&right.order_key));
                state
                    .catalog_bundles
                    .push(crate::state::CatalogBundleRow::new(bundle));
                state.entities.push(entity);
                state
                    .index_entries
                    .push(crate::state::MemoryIndexEntry::legacy(index_entry));
                state.index_epochs.push(epoch);
                state.historical_persisted_keys = persisted;
                Ok(())
            })
            .expect("install typed fixtures");

        let mut session = store
            .begin_structural_evidence(inputs())
            .expect("begin evidence");
        let mut cursor = HistoricalEvidenceCursor::start(id, session.open_session_id());
        let limit = EvidencePageLimit::new(2).expect("page limit");
        let mut evidence = Vec::new();
        while let HistoricalEvidencePage::Page {
            evidence: page,
            next,
            ..
        } = session
            .read_historical_evidence(cursor, limit)
            .expect("historical page")
        {
            assert!(!page.is_empty());
            assert!(page.len() <= usize::try_from(limit.get()).expect("bounded limit"));
            evidence.extend(page);
            cursor = next;
        }
        assert_eq!(evidence.len(), 5);

        let mut entity_keys = 0;
        let mut index_keys = 0;
        let mut epoch_keys = 0;
        for item in &evidence {
            match item {
                HistoricalSemanticEvidence::PersistedKey(persisted) => {
                    assert_eq!(persisted.schema(), &binding);
                    match persisted.key() {
                        IrOpaquePersistedKeyV1::Entity { .. } => entity_keys += 1,
                        IrOpaquePersistedKeyV1::IndexRangePrefix(_) => epoch_keys += 1,
                        IrOpaquePersistedKeyV1::PartitionIndex(_) => epoch_keys += 1,
                    }
                }
                HistoricalSemanticEvidence::IndexMigrationRow(row) => {
                    assert_eq!(row.row().schema_binding(), &binding);
                    assert!(row.row().is_v1());
                    index_keys += 1;
                }
                HistoricalSemanticEvidence::Bundle(_)
                | HistoricalSemanticEvidence::ContractMigrationEdge(_)
                | HistoricalSemanticEvidence::PlanReference(_)
                | HistoricalSemanticEvidence::ActiveCatalog(_)
                | HistoricalSemanticEvidence::CapabilityPartition(_) => {}
            }
        }
        assert_eq!((entity_keys, index_keys, epoch_keys), (1, 1, 1));
    }

    #[test]
    fn migration_selection_splits_501_rows_without_skip_or_repeat() {
        let state = MemoryState {
            index_entries: (0..=riffdb_storage_api::MAX_INDEX_MIGRATION_PAGE_ENTRIES)
                .map(|value| {
                    MemoryIndexEntry::legacy(migration_legacy_row(
                        u64::try_from(value).expect("bounded ordinal"),
                        CanonicalRecord::new(Vec::new()).expect("covered values"),
                    ))
                })
                .collect(),
            ..MemoryState::default()
        };
        let cursor =
            IndexMigrationCursor::start(database(0x70), OpenSessionId::new(70).expect("session"));
        let first = select_index_migration_page(&state, cursor).expect("first migration page");
        assert_eq!(
            first.len(),
            riffdb_storage_api::MAX_INDEX_MIGRATION_PAGE_ENTRIES
        );
        assert!(
            first.windows(2).all(|rows| {
                rows[0].physical_key().as_bytes() < rows[1].physical_key().as_bytes()
            })
        );
        let next = cursor
            .advanced(
                u64::try_from(riffdb_storage_api::MAX_INDEX_MIGRATION_PAGE_ENTRIES)
                    .expect("bounded count"),
            )
            .expect("next cursor");
        let second = select_index_migration_page(&state, next).expect("second migration page");
        assert_eq!(second.len(), 1);
        assert_eq!(
            first.last().expect("first page row").physical_key(),
            state.index_entries[499].key()
        );
        assert_eq!(second[0].physical_key(), state.index_entries[500].key());
        assert!(
            first
                .last()
                .expect("first page row")
                .physical_key()
                .as_bytes()
                < second[0].physical_key().as_bytes()
        );
        assert!(
            select_index_migration_page(&state, next.advanced(1).expect("exact-end cursor"))
                .expect("exact end")
                .is_empty()
        );
    }

    #[test]
    fn migration_selection_stops_on_instruction_bytes_with_evidence_capacity_remaining() {
        const CANONICAL_RECORD_OVERHEAD: usize = 16;
        let covered_values = CanonicalRecord::new(vec![(
            FieldId::first(),
            CanonicalValue::bytes(vec![
                0xa5;
                MAX_CANONICAL_DOCUMENT_BYTES - CANONICAL_RECORD_OVERHEAD
            ])
            .expect("maximum bytes value"),
        )])
        .expect("maximum covered values");
        let state = MemoryState {
            index_entries: vec![
                MemoryIndexEntry::legacy(migration_legacy_row(1, covered_values.clone())),
                MemoryIndexEntry::legacy(migration_legacy_row(2, covered_values)),
            ],
            ..MemoryState::default()
        };
        let first_evidence =
            decode_memory_index_evidence(&state.index_entries[0]).expect("first maximum valid row");
        let second_evidence = decode_memory_index_evidence(&state.index_entries[1])
            .expect("second maximum valid row");
        assert!(
            first_evidence.instruction_page_charge()
                <= riffdb_storage_api::MAX_INDEX_MIGRATION_PAGE_BYTES,
            "one maximum valid row must fit without chunking"
        );
        assert!(
            first_evidence.evidence_page_charge() + second_evidence.evidence_page_charge()
                <= riffdb_storage_api::MAX_INDEX_MIGRATION_PAGE_BYTES
        );
        assert!(
            first_evidence.instruction_page_charge() + second_evidence.instruction_page_charge()
                > riffdb_storage_api::MAX_INDEX_MIGRATION_PAGE_BYTES
        );

        let cursor =
            IndexMigrationCursor::start(database(0x71), OpenSessionId::new(71).expect("session"));
        let first = select_index_migration_page(&state, cursor).expect("first bounded page");
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].physical_key(), state.index_entries[0].key());
        let second =
            select_index_migration_page(&state, cursor.advanced(1).expect("strict continuation"))
                .expect("second bounded page");
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].physical_key(), state.index_entries[1].key());
    }

    #[test]
    fn migration_batch_mismatch_applies_none_and_drop_releases_the_gate() {
        let id = database(0x72);
        let bundle = validated_migration_bundle();
        let store = deployed_store(id, bundle.to_stored().expect("stored migration bundle"));
        let reopened = store.reopen();
        let original = [
            compiled_migration_legacy_row(
                bundle,
                1,
                CanonicalRecord::new(Vec::new()).expect("covered values"),
            ),
            compiled_migration_legacy_row(
                bundle,
                2,
                CanonicalRecord::new(Vec::new()).expect("covered values"),
            ),
        ];
        store
            .acquire()
            .expect("migration access")
            .write(|state| {
                state.index_entries = original
                    .iter()
                    .cloned()
                    .map(MemoryIndexEntry::legacy)
                    .collect();
                Ok(())
            })
            .expect("seed migration rows");
        let first_before = riffdb_storage_api::encode_index_entry_v1_fixture(&original[0])
            .expect("first V1 envelope")
            .into_bytes();
        let stale_second = StoredIndexEntryV1::new(
            original[1].key().clone(),
            original[1].schema_binding().clone(),
            CanonicalRecord::new(vec![(FieldId::first(), CanonicalValue::U64(99))])
                .expect("stale covered values"),
        )
        .expect("stale second row");
        let stale_second = MemoryIndexEntry::legacy(stale_second);
        let stale_second_bytes = stale_second.observed_envelope().to_vec();

        let mut session = store
            .begin_structural_evidence(inputs())
            .expect("begin migration evidence");
        let structural_end = scan_structural_end(&mut session);
        let (catalog_outcome, historical_end) = validate_catalog_history(&mut session)
            .expect("validate migration history")
            .into_parts();
        let CatalogHistoryOutcome::MigrationRequired(context) = catalog_outcome else {
            panic!("checked V1 rows must require migration");
        };
        let StructuralOpenOutcome::MigrationRequired(mut port) = session
            .finish(structural_end, historical_end)
            .expect("finish migration evidence")
        else {
            panic!("storage must retain its migration capability");
        };
        port.substitute_before_apply = Some(stale_second);
        let error = CatalogIndexMigrationDriver::new(context, port)
            .expect("same-session migration driver")
            .run()
            .expect_err("one mismatch aborts the complete batch");
        assert!(matches!(
            error,
            CatalogIndexMigrationDriveError::Storage(ref error)
                if error.kind() == StorageErrorKind::CorruptData
        ));

        reopened
            .acquire()
            .expect("gate released after failed migration")
            .read(|state| {
                assert!(state.index_entries[0].current_record().is_none());
                assert_eq!(state.index_entries[0].observed_envelope(), first_before);
                assert!(state.index_entries[1].current_record().is_none());
                assert_eq!(
                    state.index_entries[1].observed_envelope(),
                    stale_second_bytes
                );
                Ok(())
            })
            .expect("observe all-or-none state");
        assert!(!reopened.gate_is_held());
    }

    #[test]
    fn v1_startup_migration_is_linear_atomic_and_requires_a_fresh_clean_pass() {
        let id = database(0x73);
        let bundle = validated_migration_bundle();
        let stored_bundle = bundle.to_stored().expect("stored migration bundle");
        let store = deployed_store(id, stored_bundle);
        let binding = DurableKeySchemaBindingV1::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
        );
        let entity_schema = bundle
            .bundle()
            .schema()
            .entities()
            .first()
            .expect("migration entity");
        let index_schema = entity_schema.indexes().first().expect("migration index");
        let mut entity = EntityKeyBuilder::new(entity_schema.id());
        entity.push_u64(7).expect("entity component");
        let mut index = IndexEntryKeyBuilder::new(index_schema.id());
        index.push_u64(9).expect("index component");
        let index_key = index
            .finish(entity.finish().expect("entity key"))
            .expect("index key");
        let legacy = StoredIndexEntryV1::new(
            index_key.clone(),
            binding,
            CanonicalRecord::new(Vec::new()).expect("covered values"),
        )
        .expect("legacy row");
        store
            .acquire()
            .expect("seed access")
            .write(|state| {
                state.index_entries.push(MemoryIndexEntry::legacy(legacy));
                Ok(())
            })
            .expect("seed legacy row");

        let mut session = store
            .begin_structural_evidence(inputs())
            .expect("begin initial evidence");
        let initial_session_id = session.open_session_id();
        let structural_end = scan_structural_end(&mut session);
        let (catalog_outcome, historical_end) = validate_catalog_history(&mut session)
            .expect("catalog validates the complete historical stream")
            .into_parts();
        let CatalogHistoryOutcome::MigrationRequired(context) = catalog_outcome else {
            panic!("checked V1 must require catalog migration");
        };
        let StructuralOpenOutcome::MigrationRequired(port) = session
            .finish(structural_end, historical_end)
            .expect("finish initial pass")
        else {
            panic!("checked V1 must require migration");
        };
        let store = CatalogIndexMigrationDriver::new(context, port)
            .expect("bind same-session catalog migration")
            .run()
            .expect("catalog-owned migration completes");
        store
            .acquire()
            .expect("inspect migrated row")
            .read(|state| {
                let row = state.index_entries.first().expect("migrated row");
                assert_eq!(row.key(), &index_key);
                assert!(row.current_record().is_some());
                Ok(())
            })
            .expect("migrated row is durable");

        let mut fresh = store
            .begin_structural_evidence(inputs())
            .expect("begin fresh complete pass");
        assert_ne!(fresh.open_session_id(), initial_session_id);
        let structural_end = scan_structural_end(&mut fresh);
        let (catalog_outcome, historical_end) = validate_catalog_history(&mut fresh)
            .expect("fresh catalog validation")
            .into_parts();
        assert!(matches!(catalog_outcome, CatalogHistoryOutcome::Ready(_)));
        assert!(matches!(
            fresh
                .finish(structural_end, historical_end)
                .expect("fresh V2-only finish"),
            StructuralOpenOutcome::Clean(_)
        ));
    }

    #[test]
    fn historical_bundle_and_plan_pages_use_length_framed_lineage_order() {
        let id = database(8);
        let store = initialized_store(id);
        let version = ContractVersion::new(1).expect("contract version");
        let bundle_b = StoredContractBundleV1::new(
            ContractLineage::new("b").expect("lineage"),
            version,
            ContractBundleHash::from_bytes([1; 32]),
            b"bundle b".to_vec(),
        )
        .expect("bundle b");
        let bundle_aa = StoredContractBundleV1::new(
            ContractLineage::new("aa").expect("lineage"),
            version,
            ContractBundleHash::from_bytes([2; 32]),
            b"bundle aa".to_vec(),
        )
        .expect("bundle aa");
        let plan_b = ExecutablePlanRef::new(
            bundle_b.lineage().clone(),
            version,
            bundle_b.bundle_hash(),
            CommandId::first(),
            PlanHash::from_bytes([3; 32]),
        );
        let plan_aa = ExecutablePlanRef::new(
            bundle_aa.lineage().clone(),
            version,
            bundle_aa.bundle_hash(),
            CommandId::first(),
            PlanHash::from_bytes([4; 32]),
        );
        assert!(bundle_aa.lineage() < bundle_b.lineage());
        assert!(bundle_evidence_order_key(&bundle_b) < bundle_evidence_order_key(&bundle_aa));
        assert!(plan_aa < plan_b);
        assert!(plan_evidence_order_key(&plan_b) < plan_evidence_order_key(&plan_aa));

        store
            .acquire()
            .expect("access")
            .write(|state| {
                state.catalog_bundles = vec![
                    crate::state::CatalogBundleRow::new(bundle_aa),
                    crate::state::CatalogBundleRow::new(bundle_b),
                ];
                state
                    .catalog_bundles
                    .sort_by(|left, right| left.order_key.cmp(&right.order_key));
                state.historical_plan_references = vec![
                    crate::state::HistoricalPlanReferenceRow::new(
                        plan_aa,
                        crate::state::HistoricalPlanReferenceSource::Commit(
                            CommitSequence::first(),
                        ),
                    ),
                    crate::state::HistoricalPlanReferenceRow::new(
                        plan_b,
                        crate::state::HistoricalPlanReferenceSource::Commit(
                            CommitSequence::first(),
                        ),
                    ),
                ];
                state
                    .historical_plan_references
                    .sort_by(|left, right| left.order_key.cmp(&right.order_key));
                Ok(())
            })
            .expect("install historical fixtures");

        let mut session = store
            .begin_structural_evidence(inputs())
            .expect("begin evidence");
        let mut cursor = HistoricalEvidenceCursor::start(id, session.open_session_id());
        let limit = EvidencePageLimit::new(1).expect("one item per page");
        let mut observed = Vec::new();
        while let HistoricalEvidencePage::Page { evidence, next, .. } = session
            .read_historical_evidence(cursor, limit)
            .expect("canonical historical page")
        {
            assert_eq!(evidence.len(), 1);
            observed.push(match &evidence[0] {
                HistoricalSemanticEvidence::Bundle(bundle) => {
                    format!("bundle:{}", bundle.lineage())
                }
                HistoricalSemanticEvidence::PlanReference(plan) => {
                    format!("plan:{}", plan.contract_lineage())
                }
                HistoricalSemanticEvidence::ActiveCatalog(None) => "active:none".to_owned(),
                HistoricalSemanticEvidence::ActiveCatalog(Some(_))
                | HistoricalSemanticEvidence::ContractMigrationEdge(_)
                | HistoricalSemanticEvidence::PersistedKey(_)
                | HistoricalSemanticEvidence::IndexMigrationRow(_)
                | HistoricalSemanticEvidence::CapabilityPartition(_) => {
                    panic!("unexpected historical evidence")
                }
            });
            cursor = next;
        }

        assert_eq!(
            observed,
            ["bundle:b", "bundle:aa", "plan:b", "plan:aa", "active:none"]
        );
    }
}
