//! The single composite-query implementation over API-neutral storage readers.

use std::collections::BTreeMap;
use std::sync::Arc;

use riffdb_policy::{
    AuthorizedIndexedRelationshipLookupV1, AuthorizedProjectedRowAdmissionV1,
    AuthorizedQueryRowPolicyContextV1, EventPolicyCandidateV1, MAX_PROJECTED_POLICY_CANDIDATES_V1,
    ProjectedPolicyCandidateObservationV1,
};
use riffdb_query_ir::{
    AccessDirection, CoveredResultSourceV1, OperationalAggregateV1, QueryAccessKind,
    QueryAccessProgramV1, QueryAccessStep, QueryPredicateOperator,
};
use riffdb_storage_api::{
    AuthoritativePointReader, CapabilityLifecycleV1, CapabilityReader, EntityTarget,
    EventPolicyAdmissionFenceV1, EventPolicyAdmissionObservationV1,
    EventPolicyRelationshipObservationV1, OwnedSnapshotHandle, OwnedSnapshotReader,
    PartitionIndexTarget, SnapshotIndexDirectionV1, SnapshotIndexRangeRequestV1, StorageError,
    StorageErrorKind, StorageScanLimit, VectorEvidenceIndexScanRequestV1,
    VectorObservationTargetV1,
};
use riffdb_types::{
    ApplicationRoleHash, CanonicalValue, EntityKey, EntityTypeId, EventId, FieldId, IndexEntryKey,
    Timestamp,
};

use crate::{
    BoundPredicate, CoveredResultBatch, LongPatternCandidateBatch, MAX_QUERY_SCANNED_ROWS,
    QueryBackendFault, QueryContinuation, QueryExecutionError, QueryExecutionPort,
    QueryExecutionRequest, QueryNearestPage, QueryOwnedSnapshot, QueryParameters, QueryReadView,
    QueryRow, QueryScanPage, VectorInspectionCandidateV1, VectorInspectionSnapshotV1,
    VectorInspectionTargetV1, covered_row_matches_predicates_v1, execute_in_snapshot,
    execute_operational_page_in_snapshot, execute_page_in_snapshot,
    execute_policy_operational_page_in_snapshot, execute_policy_page_in_snapshot,
    execute_policy_provider_page_in_snapshot, execute_provider_page_in_snapshot,
    validate_query_execution_group,
};

/// Composite-query executor assembled around one API-neutral snapshot source.
#[derive(Clone)]
pub struct StorageQueryExecutor<S> {
    storage: S,
}

/// Closed result of current capability validation before event-row admission.
pub enum EventCandidateAuthorizationV1 {
    /// Current capability matched and every event observation was evaluated.
    Authorized(Box<EventPolicyAdmissionFenceV1>),
    /// Capability identity, revision, lifecycle, grant, or validity changed.
    AuthorizationChanged,
}

impl<S> StorageQueryExecutor<S> {
    /// Assembles execution without granting the backend any query vocabulary.
    #[must_use]
    pub const fn new(storage: S) -> Self {
        Self { storage }
    }

    /// Borrows the lower storage source for non-query composition.
    #[must_use]
    pub const fn storage(&self) -> &S {
        &self.storage
    }
}

impl<S> StorageQueryExecutor<S>
where
    S: OwnedSnapshotReader + CapabilityReader,
{
    /// Evaluates protected event candidates and freezes every neutral
    /// observation needed for the backend's final safe-point recheck.
    pub fn authorize_event_candidates(
        &self,
        event_ids: &[EventId],
        observed_at: Timestamp,
        policy: &AuthorizedQueryRowPolicyContextV1,
    ) -> Result<EventCandidateAuthorizationV1, QueryExecutionError> {
        let Some((capability_id, revision)) = policy.internal_capability_identity() else {
            return Err(QueryExecutionError::BackendIntegrity);
        };
        let expected_grant = policy
            .internal_row_policy_grant()
            .ok_or(QueryExecutionError::BackendIntegrity)?;
        let Some(capability) = self
            .storage
            .read_capability(capability_id)
            .map_err(map_storage_error)?
        else {
            return Ok(EventCandidateAuthorizationV1::AuthorizationChanged);
        };
        if capability.revision() != revision
            || !matches!(capability.lifecycle(), CapabilityLifecycleV1::Active)
            || capability.issued_at() > observed_at
            || observed_at >= capability.expires_at()
            || capability.grant().internal_row_policy() != Some(expected_grant)
        {
            return Ok(EventCandidateAuthorizationV1::AuthorizationChanged);
        }
        let snapshot = self
            .storage
            .open_owned_snapshot()
            .map_err(map_storage_error)?;
        let mut observations = Vec::with_capacity(event_ids.len());
        for event_id in event_ids {
            let event = snapshot
                .read_durable_event(*event_id)
                .map_err(map_storage_error)?
                .ok_or(QueryExecutionError::BackendIntegrity)?;
            let Some(anchor) = event.policy_anchor() else {
                observations.push(
                    EventPolicyAdmissionObservationV1::new(event, None, Vec::new(), false)
                        .map_err(|_| QueryExecutionError::BackendIntegrity)?,
                );
                continue;
            };
            let current = snapshot
                .read_entity(anchor.source())
                .map_err(map_storage_error)?;
            let Some(record) = current else {
                observations.push(
                    EventPolicyAdmissionObservationV1::new(event, None, Vec::new(), false)
                        .map_err(|_| QueryExecutionError::BackendIntegrity)?,
                );
                continue;
            };
            if !policy.protects(anchor.source().entity_type_id()) {
                return Err(QueryExecutionError::BackendIntegrity);
            }
            let lookups = policy
                .relationship_lookups(anchor.source().entity_type_id(), record.fields())
                .map_err(|_| QueryExecutionError::BackendIntegrity)?;
            let mut relationships = Vec::with_capacity(lookups.len());
            let mut evidence = Vec::with_capacity(lookups.len());
            for lookup in &lookups {
                let exists =
                    indexed_relationship_exists(&snapshot, lookup).map_err(map_storage_error)?;
                relationships.push(
                    EventPolicyRelationshipObservationV1::new(
                        lookup.partition().clone(),
                        lookup.index_prefix().to_vec(),
                        exists,
                    )
                    .map_err(|_| QueryExecutionError::BackendIntegrity)?,
                );
                evidence.push(exists);
            }
            let candidate = EventPolicyCandidateV1::new(
                *event_id,
                anchor.source().key().clone(),
                anchor.read_policy().clone(),
            );
            let admitted = policy
                .authorize_event_release(&candidate, record.fields(), &evidence)
                .is_allowed();
            observations.push(
                EventPolicyAdmissionObservationV1::new(
                    event,
                    Some(record),
                    relationships,
                    admitted,
                )
                .map_err(|_| QueryExecutionError::BackendIntegrity)?,
            );
        }
        EventPolicyAdmissionFenceV1::new(capability, observed_at, observations)
            .map(Box::new)
            .map(EventCandidateAuthorizationV1::Authorized)
            .map_err(|_| QueryExecutionError::BackendIntegrity)
    }
}

impl<S> QueryExecutionPort for StorageQueryExecutor<S>
where
    S: OwnedSnapshotReader + Send + Sync,
{
    fn execute_query_page(
        &self,
        program: &QueryAccessProgramV1,
        parameters: &QueryParameters,
        prior: Option<&QueryContinuation>,
    ) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
        let snapshot = self
            .storage
            .open_owned_snapshot()
            .map_err(map_storage_error)?;
        let mut view = StorageQueryView::new(&snapshot, program, parameters)?;
        execute_page_in_snapshot(program, parameters, prior, &mut view)
    }

    fn execute_query_group(
        &self,
        requests: &[QueryExecutionRequest<'_>],
    ) -> Result<Vec<QueryOwnedSnapshot>, QueryExecutionError> {
        validate_query_execution_group(requests)?;
        let snapshot = self
            .storage
            .open_owned_snapshot()
            .map_err(map_storage_error)?;
        requests
            .iter()
            .map(|request| {
                let mut view =
                    StorageQueryView::new(&snapshot, request.program(), request.parameters())?;
                execute_in_snapshot(request.program(), request.parameters(), &mut view)
            })
            .collect()
    }

    fn execute_policy_query_group(
        &self,
        requests: &[QueryExecutionRequest<'_>],
        policy: &AuthorizedQueryRowPolicyContextV1,
    ) -> Result<Vec<QueryOwnedSnapshot>, QueryExecutionError> {
        validate_query_execution_group(requests)?;
        let snapshot = self
            .storage
            .open_owned_snapshot()
            .map_err(map_storage_error)?;
        requests
            .iter()
            .map(|request| {
                let mut view =
                    StorageQueryView::new(&snapshot, request.program(), request.parameters())?;
                execute_policy_page_in_snapshot(
                    request.program(),
                    request.parameters(),
                    None,
                    &mut view,
                    policy,
                )
            })
            .collect()
    }

    fn execute_operational_query_page(
        &self,
        program: &QueryAccessProgramV1,
        aggregates: &[OperationalAggregateV1],
        parameters: &QueryParameters,
        prior: Option<&QueryContinuation>,
    ) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
        let snapshot = self
            .storage
            .open_owned_snapshot()
            .map_err(map_storage_error)?;
        let mut view = StorageQueryView::new(&snapshot, program, parameters)?;
        execute_operational_page_in_snapshot(program, aggregates, parameters, prior, &mut view)
    }

    fn execute_policy_query_page(
        &self,
        program: &QueryAccessProgramV1,
        parameters: &QueryParameters,
        prior: Option<&QueryContinuation>,
        policy: &AuthorizedQueryRowPolicyContextV1,
    ) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
        let snapshot = self
            .storage
            .open_owned_snapshot()
            .map_err(map_storage_error)?;
        let mut view = StorageQueryView::new(&snapshot, program, parameters)?;
        execute_policy_page_in_snapshot(program, parameters, prior, &mut view, policy)
    }

    fn execute_policy_operational_query_page(
        &self,
        program: &QueryAccessProgramV1,
        aggregates: &[OperationalAggregateV1],
        parameters: &QueryParameters,
        prior: Option<&QueryContinuation>,
        policy: &AuthorizedQueryRowPolicyContextV1,
    ) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
        let snapshot = self
            .storage
            .open_owned_snapshot()
            .map_err(map_storage_error)?;
        let mut view = StorageQueryView::new(&snapshot, program, parameters)?;
        execute_policy_operational_page_in_snapshot(
            program, aggregates, parameters, prior, &mut view, policy,
        )
    }

    fn execute_provider_query_page(
        &self,
        program: &QueryAccessProgramV1,
        parameters: &QueryParameters,
        prior: Option<&QueryContinuation>,
        policy_shape: ApplicationRoleHash,
        proof: &riffdb_projection::ResultSetEpochProofV1,
        batches: &[LongPatternCandidateBatch],
    ) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
        let snapshot = self
            .storage
            .open_owned_snapshot()
            .map_err(map_storage_error)?;
        let mut view = StorageQueryView::new(&snapshot, program, parameters)?;
        execute_provider_page_in_snapshot(
            program,
            parameters,
            prior,
            &mut view,
            policy_shape,
            proof,
            batches,
        )
    }

    fn execute_policy_provider_query_page(
        &self,
        program: &QueryAccessProgramV1,
        parameters: &QueryParameters,
        prior: Option<&QueryContinuation>,
        policy: &AuthorizedQueryRowPolicyContextV1,
        policy_shape: ApplicationRoleHash,
        proof: &riffdb_projection::ResultSetEpochProofV1,
        batches: &[LongPatternCandidateBatch],
    ) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
        let snapshot = self
            .storage
            .open_owned_snapshot()
            .map_err(map_storage_error)?;
        let mut view = StorageQueryView::new(&snapshot, program, parameters)?;
        execute_policy_provider_page_in_snapshot(
            program,
            parameters,
            prior,
            &mut view,
            policy,
            policy_shape,
            proof,
            batches,
        )
    }

    fn authorize_projected_candidates(
        &self,
        entity: EntityTypeId,
        candidates: &[EntityKey],
        policy: &AuthorizedQueryRowPolicyContextV1,
    ) -> Result<AuthorizedProjectedRowAdmissionV1, QueryExecutionError> {
        if candidates.len() > MAX_PROJECTED_POLICY_CANDIDATES_V1 {
            return Err(QueryExecutionError::BoundExceeded);
        }
        let snapshot = self
            .storage
            .open_owned_snapshot()
            .map_err(map_storage_error)?;
        authorize_candidates(&snapshot, entity, candidates, policy).map_err(map_storage_error)
    }

    fn inspect_vector_evidence(
        &self,
        target: &VectorInspectionTargetV1,
        policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<VectorInspectionSnapshotV1, QueryExecutionError> {
        let snapshot = self
            .storage
            .open_owned_snapshot()
            .map_err(map_storage_error)?;
        inspect_vector(&snapshot, target, policy)
    }
}

struct StorageQueryView<'a, H> {
    snapshot: &'a H,
    program: &'a QueryAccessProgramV1,
    parameters: &'a QueryParameters,
    head: u64,
}

impl<'a, H> StorageQueryView<'a, H>
where
    H: OwnedSnapshotHandle,
{
    fn new(
        snapshot: &'a H,
        program: &'a QueryAccessProgramV1,
        parameters: &'a QueryParameters,
    ) -> Result<Self, QueryExecutionError> {
        let head = snapshot
            .application_frontier()
            .map_err(map_storage_error)?
            .map_or(0, riffdb_types::CommitSequence::get);
        Ok(Self {
            snapshot,
            program,
            parameters,
            head,
        })
    }

    fn point_with_plan(
        &self,
        step: &QueryAccessStep,
        predicates: &[BoundPredicate],
        plan: &RowMaterializePlan,
        policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<Option<QueryRow>, StorageError> {
        let key_fields = match step.access() {
            QueryAccessKind::Point { key_fields }
            | QueryAccessKind::DependentPointBatch { key_fields, .. }
            | QueryAccessKind::CandidateRootHydration { key_fields, .. } => key_fields,
            _ => return Err(invariant()),
        };
        let values = key_fields
            .iter()
            .map(|field| exact_value(predicates, field))
            .collect::<Result<Vec<_>, _>>()?;
        if values
            .iter()
            .any(|value| matches!(value, CanonicalValue::Null))
        {
            return Ok(None);
        }
        let key = step
            .internal_entity_key_schema()
            .encode_entity(&values)
            .map_err(|_| invariant())?;
        let target = EntityTarget::new(step.internal_entity_id(), key).map_err(|_| invariant())?;
        let Some(record) = self.snapshot.read_entity(&target)? else {
            return Ok(None);
        };
        if !allows_policy_record(
            self.snapshot,
            policy,
            step.internal_entity_id(),
            record.fields(),
        )? {
            return Ok(None);
        }
        plan.materialize(&record).map(Some)
    }

    fn read_range_rows(
        &self,
        step: &QueryAccessStep,
        predicates: &[BoundPredicate],
        limit: u64,
        after: Option<&[u8]>,
        after_inclusive: bool,
        policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<QueryScanPage, StorageError> {
        let direction = access_direction(step)?;
        let schema = step.internal_index_key_schema().ok_or_else(invariant)?;
        let schedule =
            crate::bound_index_range_schedule_v1(step, predicates).map_err(|_| invariant())?;
        let partition_value = partition_value(self.program, step, predicates)?;
        let partition = step
            .internal_partition_key_schema()
            .encode_partition(std::slice::from_ref(partition_value))
            .map_err(|_| invariant())?;
        let target =
            PartitionIndexTarget::new(partition, step.internal_index_id().ok_or_else(invariant)?);
        let epoch = self.snapshot.index_epoch(&target)?;
        let page_limit = usize::try_from(limit).map_err(|_| limit_exceeded())?;
        let fetch_limit = page_limit.saturating_add(1);
        let scan_ceiling = usize::try_from(MAX_QUERY_SCANNED_ROWS).map_err(|_| limit_exceeded())?;
        let plan = RowMaterializePlan::for_step(self.program, step)?;
        let mut rows =
            Vec::<(IndexEntryKey, QueryRow)>::with_capacity(fetch_limit.min(scan_ceiling));
        let mut physical = 0usize;
        let mut eligible = 0usize;

        'ranges: for range in schedule.ranges() {
            let Some(window) = range.resume_window(direction, after, after_inclusive) else {
                continue;
            };
            // `resume_window` has already folded the public cursor into this
            // range. Storage pagination starts at that exact window boundary;
            // only lower pages minted below become `after` values.
            let mut lower_after = None;
            loop {
                let remaining = scan_ceiling.saturating_sub(physical);
                if remaining == 0 {
                    return Err(limit_exceeded());
                }
                let request_limit = u16::try_from(
                    remaining
                        .min(riffdb_storage_api::MAX_SCAN_PAGE_ENTRIES)
                        .min(fetch_limit.saturating_sub(rows.len()).max(1)),
                )
                .ok()
                .and_then(StorageScanLimit::new)
                .ok_or_else(limit_exceeded)?;
                let request = SnapshotIndexRangeRequestV1::new(
                    window.start_inclusive().to_vec(),
                    !window.skip_start_equal(),
                    window.end_exclusive().to_vec(),
                    window.include_end_equal(),
                    storage_direction(direction),
                    lower_after.take(),
                    request_limit,
                )
                .map_err(|_| invariant())?;
                let page = self.snapshot.scan_snapshot_index_range(request)?;
                physical = physical
                    .checked_add(page.entries().len())
                    .ok_or_else(limit_exceeded)?;
                for charged in page.entries() {
                    let entry = charged.value();
                    if entry.partition_key() != target.partition_key() {
                        continue;
                    }
                    eligible = eligible.checked_add(1).ok_or_else(limit_exceeded)?;
                    let decoded = schema.decode_index(entry.key()).map_err(|_| corrupt())?;
                    let entity_target =
                        EntityTarget::new(step.internal_entity_id(), decoded.entity_key().clone())
                            .map_err(|_| corrupt())?;
                    let record = self
                        .snapshot
                        .read_entity(&entity_target)?
                        .ok_or_else(corrupt)?;
                    if !allows_policy_record(
                        self.snapshot,
                        policy,
                        step.internal_entity_id(),
                        record.fields(),
                    )? {
                        continue;
                    }
                    rows.push((entry.key().clone(), plan.materialize(&record)?));
                    if rows.len() == fetch_limit {
                        break 'ranges;
                    }
                }
                let Some(next) = page.next_after() else {
                    break;
                };
                lower_after = Some(next.as_bytes().to_vec());
            }
        }
        let has_more = rows.len() > page_limit;
        if has_more {
            rows.truncate(page_limit);
        }
        let continuation = has_more
            .then(|| rows.last().map(|row| row.0.as_bytes().to_vec()))
            .flatten();
        let rows = rows.into_iter().map(|(_, row)| row).collect();
        let scanned_rows = u64::try_from(eligible).map_err(|_| limit_exceeded())?;
        match continuation {
            Some(continuation) => {
                QueryScanPage::continued(rows, epoch, scanned_rows.max(1), continuation)
                    .ok_or_else(invariant)
            }
            None => {
                QueryScanPage::policy_exact_end(rows, epoch, scanned_rows).ok_or_else(invariant)
            }
        }
    }

    fn read_covered_rows(
        &self,
        step: &QueryAccessStep,
        predicates: &[BoundPredicate],
        limit: u64,
        after: Option<&[u8]>,
        policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<Option<CoveredResultBatch>, StorageError> {
        let Some(layout) = step.covered_result_layout() else {
            return Ok(None);
        };
        if policy.is_some() {
            return Err(invariant());
        }
        let direction = access_direction(step)?;
        let schema = step.internal_index_key_schema().ok_or_else(invariant)?;
        let schedule =
            crate::bound_index_range_schedule_v1(step, predicates).map_err(|_| invariant())?;
        let partition_value = self
            .parameters
            .get(self.program.partition_parameter())
            .ok_or_else(invariant)?;
        let partition = step
            .internal_partition_key_schema()
            .encode_partition(std::slice::from_ref(partition_value))
            .map_err(|_| invariant())?;
        let target =
            PartitionIndexTarget::new(partition, step.internal_index_id().ok_or_else(invariant)?);
        let epoch = self.snapshot.index_epoch(&target)?;
        let page_limit = usize::try_from(limit).map_err(|_| limit_exceeded())?;
        let fetch_limit = page_limit.saturating_add(1);
        let scan_ceiling = usize::try_from(MAX_QUERY_SCANNED_ROWS).map_err(|_| limit_exceeded())?;
        let mut expected_cover_ids = layout.internal_cover_field_ids().to_vec();
        expected_cover_ids.sort_unstable();
        let cover_positions = layout
            .fields()
            .iter()
            .map(|field| match field.internal_source() {
                CoveredResultSourceV1::Cover => expected_cover_ids
                    .binary_search(&field.internal_field_id())
                    .map_err(|_| invariant()),
                CoveredResultSourceV1::IndexKey(_) | CoveredResultSourceV1::EntityKey(_) => {
                    Ok(usize::MAX)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut rows = Vec::<(IndexEntryKey, Vec<CanonicalValue>)>::with_capacity(
            fetch_limit.min(scan_ceiling),
        );
        let mut physical = 0usize;
        let mut eligible = 0usize;
        'ranges: for range in schedule.ranges() {
            let Some(window) = range.resume_window(direction, after, false) else {
                continue;
            };
            // The resumed window owns the public continuation adjustment.
            // Reusing that cursor as a storage-page continuation would reject
            // a later disjoint range whose lower bound sorts after it.
            let mut lower_after = None;
            loop {
                let remaining = scan_ceiling.saturating_sub(physical);
                if remaining == 0 {
                    return Err(limit_exceeded());
                }
                let request_limit = u16::try_from(
                    remaining
                        .min(riffdb_storage_api::MAX_SCAN_PAGE_ENTRIES)
                        .min(fetch_limit.saturating_sub(rows.len()).max(1)),
                )
                .ok()
                .and_then(StorageScanLimit::new)
                .ok_or_else(limit_exceeded)?;
                let request = SnapshotIndexRangeRequestV1::new(
                    window.start_inclusive().to_vec(),
                    !window.skip_start_equal(),
                    window.end_exclusive().to_vec(),
                    window.include_end_equal(),
                    storage_direction(direction),
                    lower_after.take(),
                    request_limit,
                )
                .map_err(|_| invariant())?;
                let page = self.snapshot.scan_snapshot_index_range(request)?;
                physical = physical
                    .checked_add(page.entries().len())
                    .ok_or_else(limit_exceeded)?;
                for charged in page.entries() {
                    let entry = charged.value();
                    if entry.partition_key() != target.partition_key() {
                        continue;
                    }
                    eligible = eligible.checked_add(1).ok_or_else(limit_exceeded)?;
                    let binding = entry.schema_binding();
                    let contract = self.program.contract();
                    if binding.lineage() != contract.lineage()
                        || binding.contract_version() != contract.version()
                        || binding.bundle_hash() != contract.bundle_hash()
                    {
                        return Err(corrupt());
                    }
                    let covered = entry.covered_values().fields();
                    if covered.len() != expected_cover_ids.len()
                        || covered
                            .iter()
                            .zip(&expected_cover_ids)
                            .any(|((actual, _), expected)| actual != expected)
                    {
                        return Err(corrupt());
                    }
                    let decoded = schema.decode_index(entry.key()).map_err(|_| corrupt())?;
                    let entity_values = step
                        .internal_entity_key_schema()
                        .decode_entity(decoded.entity_key())
                        .map_err(|_| corrupt())?;
                    let values = layout
                        .fields()
                        .iter()
                        .zip(&cover_positions)
                        .map(|(field, position)| match field.internal_source() {
                            CoveredResultSourceV1::IndexKey(index) => decoded
                                .values()
                                .get(usize::from(index))
                                .cloned()
                                .ok_or_else(corrupt),
                            CoveredResultSourceV1::EntityKey(index) => entity_values
                                .get(usize::from(index))
                                .cloned()
                                .ok_or_else(corrupt),
                            CoveredResultSourceV1::Cover => covered
                                .get(*position)
                                .map(|(_, value)| value.clone())
                                .ok_or_else(corrupt),
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    if !covered_row_matches_predicates_v1(layout, &values, predicates)
                        .map_err(|_| invariant())?
                    {
                        continue;
                    }
                    rows.push((entry.key().clone(), values));
                    if rows.len() == fetch_limit {
                        break 'ranges;
                    }
                }
                let Some(next) = page.next_after() else {
                    break;
                };
                lower_after = Some(next.as_bytes().to_vec());
            }
        }
        let has_more = rows.len() > page_limit;
        if has_more {
            rows.truncate(page_limit);
        }
        let continuation = has_more
            .then(|| rows.last().map(|row| row.0.as_bytes().to_vec()))
            .flatten();
        let rows = rows.into_iter().map(|(_, values)| values).collect();
        CoveredResultBatch::checked(
            layout.clone(),
            rows,
            epoch,
            u64::try_from(eligible).map_err(|_| limit_exceeded())?,
            0,
            continuation,
        )
        .map(Some)
        .ok_or_else(invariant)
    }
}

impl<H> QueryReadView for StorageQueryView<'_, H>
where
    H: OwnedSnapshotHandle,
{
    type Error = StorageError;

    fn fault(&self, error: &Self::Error) -> QueryBackendFault {
        storage_fault(error)
    }
    fn application_head(&self) -> u64 {
        self.head
    }
    fn point(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[BoundPredicate],
        policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<Option<QueryRow>, Self::Error> {
        let plan = RowMaterializePlan::for_step(self.program, step)?;
        self.point_with_plan(step, predicates, &plan, policy)
    }
    fn dependent_point_batch(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[Vec<BoundPredicate>],
        policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<Vec<Option<QueryRow>>, Self::Error> {
        if !matches!(
            step.access(),
            QueryAccessKind::DependentPointBatch { .. }
                | QueryAccessKind::CandidateRootHydration { .. }
        ) {
            return Err(invariant());
        }
        let plan = RowMaterializePlan::for_step(self.program, step)?;
        predicates
            .iter()
            .map(|predicates| self.point_with_plan(step, predicates, &plan, policy))
            .collect()
    }
    fn scan(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[BoundPredicate],
        limit: u64,
        after: Option<&[u8]>,
        after_inclusive: bool,
        policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<QueryScanPage, Self::Error> {
        self.read_range_rows(step, predicates, limit, after, after_inclusive, policy)
    }
    fn scan_covered(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[BoundPredicate],
        limit: u64,
        after: Option<&[u8]>,
        policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<Option<CoveredResultBatch>, Self::Error> {
        self.read_covered_rows(step, predicates, limit, after, policy)
    }
    fn nearest(
        &mut self,
        _step: &QueryAccessStep,
        _predicates: &[BoundPredicate],
        _k: u32,
        _policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<QueryNearestPage, Self::Error> {
        Err(invariant())
    }
}

fn authorize_candidates<H: OwnedSnapshotHandle>(
    snapshot: &H,
    entity: EntityTypeId,
    candidates: &[EntityKey],
    policy: &AuthorizedQueryRowPolicyContextV1,
) -> Result<AuthorizedProjectedRowAdmissionV1, StorageError> {
    let mut observations = Vec::with_capacity(candidates.len());
    for key in candidates {
        let target = EntityTarget::new(entity, key.clone()).map_err(|_| invariant())?;
        let Some(record) = snapshot.read_entity(&target)? else {
            observations.push(ProjectedPolicyCandidateObservationV1::missing(key.clone()));
            continue;
        };
        let lookups = policy
            .relationship_lookups(entity, record.fields())
            .map_err(|_| invariant())?;
        let evidence = lookups
            .iter()
            .map(|lookup| indexed_relationship_exists(snapshot, lookup))
            .collect::<Result<Vec<_>, _>>()?;
        observations.push(ProjectedPolicyCandidateObservationV1::current(
            key.clone(),
            record.fields().clone(),
            evidence,
        ));
    }
    policy
        .authorize_projected_candidates(entity, observations)
        .map_err(|_| invariant())
}

fn inspect_vector<H: OwnedSnapshotHandle>(
    snapshot: &H,
    target: &VectorInspectionTargetV1,
    policy: Option<&AuthorizedQueryRowPolicyContextV1>,
) -> Result<VectorInspectionSnapshotV1, QueryExecutionError> {
    if target
        .after()
        .is_some_and(|after| after.entity_type_id() != target.entity())
    {
        return Err(QueryExecutionError::InvalidProgram);
    }
    let lower = VectorObservationTargetV1::new(
        target.lineage().clone(),
        target.partition().clone(),
        target.entity(),
        target.field(),
    );
    let observation = snapshot
        .read_vector_observation(&lower)
        .map_err(map_storage_error)?;
    let limit = StorageScanLimit::new(target.limit().get())
        .ok_or(QueryExecutionError::BackendLimitExceeded)?;
    let request = VectorEvidenceIndexScanRequestV1::new(lower, target.after().cloned(), limit)
        .map_err(|_| QueryExecutionError::BackendIntegrity)?;
    let page = snapshot
        .scan_vector_evidence_index(&request)
        .map_err(map_storage_error)?;
    let candidates = page
        .entries()
        .iter()
        .map(|entry| {
            VectorInspectionCandidateV1::new(
                entry.entity_key().clone(),
                entry.newest_source_write(),
                entry
                    .embedding_write()
                    .map(|write| (write.sequence(), write.metadata().clone())),
            )
        })
        .collect::<Vec<_>>();
    let admission = policy
        .map(|policy| {
            let keys = candidates
                .iter()
                .map(|candidate| candidate.entity_key().clone())
                .collect::<Vec<_>>();
            authorize_candidates(snapshot, target.entity(), &keys, policy)
        })
        .transpose()
        .map_err(map_storage_error)?;
    let frontier = snapshot.application_frontier().map_err(map_storage_error)?;
    let (total, stale, models, revision) = observation.map_or_else(
        || (0, 0, BTreeMap::new(), None),
        |observation| {
            (
                observation.total_entities(),
                observation.source_stale_entities(),
                observation
                    .model_counts()
                    .map(|(metadata, count)| (metadata.clone(), count))
                    .collect(),
                Some(observation.revision()),
            )
        },
    );
    Ok(VectorInspectionSnapshotV1::new(
        total,
        stale,
        models,
        revision,
        frontier,
        candidates,
        page.continuation().cloned(),
        page.exact_end(),
        admission,
    ))
}

fn allows_policy_record<H: OwnedSnapshotHandle>(
    snapshot: &H,
    policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    entity: EntityTypeId,
    row: &riffdb_types::CanonicalRecord,
) -> Result<bool, StorageError> {
    let Some(policy) = policy else {
        return Ok(true);
    };
    if !policy.protects(entity) {
        return Ok(true);
    }
    let lookups = policy
        .relationship_lookups(entity, row)
        .map_err(|_| invariant())?;
    let evidence = lookups
        .iter()
        .map(|lookup| indexed_relationship_exists(snapshot, lookup))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(policy.allows(entity, row, &evidence))
}

fn indexed_relationship_exists<H: OwnedSnapshotHandle>(
    snapshot: &H,
    lookup: &AuthorizedIndexedRelationshipLookupV1,
) -> Result<bool, StorageError> {
    let upper = exclusive_prefix_end(lookup.index_prefix()).ok_or_else(invariant)?;
    let limit = StorageScanLimit::new(
        u16::try_from(
            usize::try_from(MAX_QUERY_SCANNED_ROWS)
                .unwrap_or(usize::MAX)
                .min(riffdb_storage_api::MAX_SCAN_PAGE_ENTRIES),
        )
        .map_err(|_| limit_exceeded())?,
    )
    .ok_or_else(limit_exceeded)?;
    let mut after = None;
    let mut inspected = 0usize;
    loop {
        let request = SnapshotIndexRangeRequestV1::new(
            lookup.index_prefix().to_vec(),
            true,
            upper.clone(),
            false,
            SnapshotIndexDirectionV1::Forward,
            after.take(),
            limit,
        )
        .map_err(|_| invariant())?;
        let page = snapshot.scan_snapshot_index_range(request)?;
        for entry in page.entries() {
            if inspected == usize::try_from(MAX_QUERY_SCANNED_ROWS).map_err(|_| limit_exceeded())? {
                return Err(limit_exceeded());
            }
            inspected = inspected.checked_add(1).ok_or_else(limit_exceeded)?;
            if entry.value().partition_key() == lookup.partition() {
                return Ok(true);
            }
        }
        let Some(next) = page.next_after() else {
            return Ok(false);
        };
        after = Some(next.as_bytes().to_vec());
    }
}

fn partition_value<'a>(
    program: &QueryAccessProgramV1,
    step: &QueryAccessStep,
    predicates: &'a [BoundPredicate],
) -> Result<&'a CanonicalValue, StorageError> {
    predicates
        .iter()
        .find(|predicate| predicate.operator() == QueryPredicateOperator::Equal)
        .filter(|predicate| {
            step.predicates().iter().any(|source| {
                source.field() == predicate.field()
                    && matches!(
                        source.value(),
                        riffdb_query_ir::QueryPredicateValue::Parameter(name)
                            if name == program.partition_parameter()
                    )
            })
        })
        .map(BoundPredicate::value)
        .ok_or_else(invariant)
}

fn access_direction(step: &QueryAccessStep) -> Result<AccessDirection, StorageError> {
    match step.access() {
        QueryAccessKind::Index { direction, .. }
        | QueryAccessKind::ExpansionIndex { direction, .. }
        | QueryAccessKind::PartitionSetIndex { direction, .. } => Ok(*direction),
        _ => Err(invariant()),
    }
}

const fn storage_direction(direction: AccessDirection) -> SnapshotIndexDirectionV1 {
    match direction {
        AccessDirection::Forward => SnapshotIndexDirectionV1::Forward,
        AccessDirection::Reverse => SnapshotIndexDirectionV1::Reverse,
    }
}

fn exact_value(predicates: &[BoundPredicate], field: &str) -> Result<CanonicalValue, StorageError> {
    predicates
        .iter()
        .find(|predicate| {
            predicate.field() == field && predicate.operator() == QueryPredicateOperator::Equal
        })
        .map(|predicate| predicate.value().clone())
        .ok_or_else(invariant)
}

struct RowMaterializePlan {
    entity: Arc<str>,
    needed: Vec<(FieldId, Arc<str>)>,
}

impl RowMaterializePlan {
    fn for_step(
        program: &QueryAccessProgramV1,
        step: &QueryAccessStep,
    ) -> Result<Self, StorageError> {
        let access = program
            .internal_entity_access(step.entity())
            .ok_or_else(invariant)?;
        let mut needed = access
            .internal_fields()
            .map(|(name, id)| (id, Arc::<str>::from(name)))
            .collect::<Vec<_>>();
        needed.sort_by_key(|(id, _)| id.get());
        Ok(Self {
            entity: Arc::<str>::from(step.entity()),
            needed,
        })
    }

    fn materialize(
        &self,
        record: &riffdb_storage_api::StoredEntityRecordV1,
    ) -> Result<QueryRow, StorageError> {
        let stored = record.fields().fields();
        let mut fields = BTreeMap::new();
        let mut store_index = 0usize;
        for (need_id, name) in &self.needed {
            while store_index < stored.len() && stored[store_index].0.get() < need_id.get() {
                store_index += 1;
            }
            if store_index >= stored.len() || stored[store_index].0 != *need_id {
                return Err(corrupt());
            }
            fields.insert(Arc::clone(name), stored[store_index].1.clone());
            store_index += 1;
        }
        QueryRow::from_shared(Arc::clone(&self.entity), fields).ok_or_else(limit_exceeded)
    }
}

fn exclusive_prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    let position = upper.iter().rposition(|byte| *byte != u8::MAX)?;
    upper[position] = upper[position].saturating_add(1);
    upper.truncate(position + 1);
    Some(upper)
}

fn map_storage_error(error: StorageError) -> QueryExecutionError {
    match storage_fault(&error) {
        QueryBackendFault::Unavailable => QueryExecutionError::BackendUnavailable,
        QueryBackendFault::Integrity => QueryExecutionError::BackendIntegrity,
        QueryBackendFault::LimitExceeded => QueryExecutionError::BackendLimitExceeded,
    }
}

const fn storage_fault(error: &StorageError) -> QueryBackendFault {
    match error.kind() {
        StorageErrorKind::Unavailable | StorageErrorKind::CommitStatusUnknown => {
            QueryBackendFault::Unavailable
        }
        StorageErrorKind::LimitExceeded => QueryBackendFault::LimitExceeded,
        StorageErrorKind::CorruptData
        | StorageErrorKind::IncompatibleFormat
        | StorageErrorKind::InvariantViolation
        | StorageErrorKind::SequenceExhausted
        | StorageErrorKind::HistoryPruned => QueryBackendFault::Integrity,
    }
}

const fn invariant() -> StorageError {
    StorageError::new(StorageErrorKind::InvariantViolation, None)
}
const fn corrupt() -> StorageError {
    StorageError::new(StorageErrorKind::CorruptData, None)
}
const fn limit_exceeded() -> StorageError {
    StorageError::new(StorageErrorKind::LimitExceeded, None)
}
