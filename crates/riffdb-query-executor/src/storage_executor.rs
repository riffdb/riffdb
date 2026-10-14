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
        let mut relationship_cache = BTreeMap::new();
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
            for lookup in lookups {
                let partition = lookup.partition().clone();
                let index_prefix = lookup.index_prefix().to_vec();
                let exists =
                    cached_indexed_relationship_exists(&snapshot, &mut relationship_cache, lookup)
                        .map_err(map_storage_error)?;
                relationships.push(
                    EventPolicyRelationshipObservationV1::new(partition, index_prefix, exists)
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
    relationship_cache: BTreeMap<RelationshipLookupCacheKey, bool>,
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
            relationship_cache: BTreeMap::new(),
        })
    }

    fn point_with_plan(
        &mut self,
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
            &mut self.relationship_cache,
            policy,
            step.internal_entity_id(),
            record.fields(),
        )? {
            return Ok(None);
        }
        plan.materialize(&record).map(Some)
    }

    fn read_range_rows(
        &mut self,
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
                        &mut self.relationship_cache,
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

#[derive(Clone, Eq, PartialEq)]
struct RelationshipLookupCacheKey(AuthorizedIndexedRelationshipLookupV1);

impl Ord for RelationshipLookupCacheKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (
            self.0.target_entity(),
            self.0.index_id(),
            self.0.partition(),
            self.0.index_prefix(),
        )
            .cmp(&(
                other.0.target_entity(),
                other.0.index_id(),
                other.0.partition(),
                other.0.index_prefix(),
            ))
    }
}

impl PartialOrd for RelationshipLookupCacheKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

fn authorize_candidates<H: OwnedSnapshotHandle>(
    snapshot: &H,
    entity: EntityTypeId,
    candidates: &[EntityKey],
    policy: &AuthorizedQueryRowPolicyContextV1,
) -> Result<AuthorizedProjectedRowAdmissionV1, StorageError> {
    let mut relationship_cache = BTreeMap::new();
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
            .into_iter()
            .map(|lookup| {
                cached_indexed_relationship_exists(snapshot, &mut relationship_cache, lookup)
            })
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
    relationship_cache: &mut BTreeMap<RelationshipLookupCacheKey, bool>,
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
        .into_iter()
        .map(|lookup| cached_indexed_relationship_exists(snapshot, relationship_cache, lookup))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(policy.allows(entity, row, &evidence))
}

fn cached_indexed_relationship_exists<H: OwnedSnapshotHandle>(
    snapshot: &H,
    cache: &mut BTreeMap<RelationshipLookupCacheKey, bool>,
    lookup: AuthorizedIndexedRelationshipLookupV1,
) -> Result<bool, StorageError> {
    let key = RelationshipLookupCacheKey(lookup);
    if let Some(exists) = cache.get(&key) {
        return Ok(*exists);
    }
    let exists = indexed_relationship_exists(snapshot, &key.0)?;
    cache.insert(key, exists);
    Ok(exists)
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

/// Evaluate the ordinary policy kernel on admitted rows from one captured view.
/// All observations and evaluation scratch remain owned until final proof settlement.
#[doc(hidden)]
pub fn authorize_provider_projected_candidates_admitted_v1<
    R: riffdb_storage_api::ProviderPolicySourceReaderV1,
>(
    snapshot: &R,
    entity: EntityTypeId,
    candidates: &[EntityKey],
    policy: &AuthorizedQueryRowPolicyContextV1,
    budget: &riffdb_types::exact_provider_budget_v1::ProviderBudget,
) -> Result<
    std::sync::Arc<
        riffdb_types::exact_provider_budget_v1::ProviderAllocation<
            AuthorizedProjectedRowAdmissionV1,
        >,
    >,
    StorageError,
> {
    if candidates.len() > riffdb_policy::MAX_PROJECTED_POLICY_CANDIDATES_V1 {
        return Err(limit_exceeded());
    }
    let unavailable = || StorageError::new(riffdb_storage_api::StorageErrorKind::Unavailable, None);
    let mut maximum = riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1 + 32 * 1024;
    for key in candidates {
        let bytes = key
            .as_bytes()
            .len()
            .checked_mul(8)
            .and_then(|bytes| {
                bytes.checked_add(
                    8 * std::mem::size_of::<ProjectedPolicyCandidateObservationV1>() + 4096,
                )
            })
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(unavailable)?;
        maximum = maximum.checked_add(bytes).ok_or_else(unavailable)?;
    }
    // Declaration order matters on failure: observations are destroyed before
    // the construction reservation. Final settlement transfers only the proof.
    let mut work = budget.reserve(maximum).map_err(|_| unavailable())?;
    let mut relationship_cache = BTreeMap::new();
    let mut observations = Vec::with_capacity(candidates.len());
    for key in candidates {
        let target = EntityTarget::new(entity, key.clone()).map_err(|_| invariant())?;
        let Some(record) = snapshot.read_provider_entity_record_v1(&target, budget)? else {
            observations.push(ProjectedPolicyCandidateObservationV1::missing(key.clone()));
            continue;
        };
        let additional = policy
            .provider_work_bound_v1(entity, record.fields())
            .map_err(|_| unavailable())?;
        work.absorb(budget.reserve(additional).map_err(|_| unavailable())?)
            .map_err(|_| unavailable())?;
        let lookups = policy
            .relationship_lookups(entity, record.fields())
            .map_err(|_| invariant())?;
        let evidence = lookups
            .into_iter()
            .map(|lookup| {
                let cache_key = RelationshipLookupCacheKey(lookup);
                if let Some(exists) = relationship_cache.get(&cache_key) {
                    return Ok(*exists);
                }
                let exists = admitted_indexed_relationship_exists(snapshot, &cache_key.0, budget)?;
                relationship_cache.insert(cache_key, exists);
                Ok(exists)
            })
            .collect::<Result<Vec<_>, _>>()?;
        observations.push(ProjectedPolicyCandidateObservationV1::current(
            key.clone(),
            record.fields().clone(),
            evidence,
        ));
    }
    let proof = policy
        .authorize_projected_candidates(entity, observations)
        .map_err(|_| invariant())?;
    work.settle_owned_value(proof).map_err(|_| unavailable())
}

fn admitted_indexed_relationship_exists<R: riffdb_storage_api::ProviderPolicySourceReaderV1>(
    snapshot: &R,
    lookup: &AuthorizedIndexedRelationshipLookupV1,
    budget: &riffdb_types::exact_provider_budget_v1::ProviderBudget,
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
        let page = snapshot.read_provider_policy_index_range_v1(&request, budget)?;
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

#[cfg(all(test, feature = "test-fixtures"))]
mod provider_policy_admission_tests {
    use super::*;
    use riffdb_storage_api::{ProviderPolicySourceReaderV1, SnapshotIndexRangePageV1};
    use riffdb_types::exact_provider_budget_v1::{
        EXACT_PROVIDER_BUDGET_BYTES, ProviderAllocation, ProviderBudget,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Source {
        reads: AtomicUsize,
        encoded: Vec<u8>,
    }
    impl ProviderPolicySourceReaderV1 for Source {
        fn read_provider_entity_record_v1(
            &self,
            target: &EntityTarget,
            budget: &ProviderBudget,
        ) -> Result<Option<riffdb_storage_api::AdmittedProviderEntityRecordV1>, StorageError>
        {
            self.reads.fetch_add(1, Ordering::Relaxed);
            if target.key().as_bytes().last() == Some(&2) {
                return Ok(None);
            }
            riffdb_storage_api::decode_provider_entity_record_admitted_v1(&self.encoded, budget)
                .map(Some)
                .map_err(|_| StorageError::new(StorageErrorKind::Unavailable, None))
        }
        fn read_provider_policy_index_range_v1(
            &self,
            _: &SnapshotIndexRangeRequestV1,
            _: &ProviderBudget,
        ) -> Result<Arc<ProviderAllocation<SnapshotIndexRangePageV1>>, StorageError> {
            panic!("unprotected policy cannot request relationship evidence")
        }
    }

    // req: OQ-040, PRJ-004
    #[test]
    fn admitted_policy_observations_refuse_before_reads_and_retain_only_exact_proof() {
        use riffdb_types::{
            ActorId, ActorKind, Audience, CapabilityId, CapabilityPrincipalFactsV1,
            ContractBundleHash, ContractLineage, ContractVersion, DatabaseId, EntityKeyBuilder,
            EntityVersion, Environment, TenantScope,
        };
        let entity = EntityTypeId::first();
        let keys = (1..=2)
            .map(|value| {
                let mut key = EntityKeyBuilder::new(entity);
                key.push_u64(value).unwrap();
                key.finish().unwrap()
            })
            .collect::<Vec<_>>();
        let row = riffdb_types::CanonicalRecord::new(vec![(
            FieldId::first(),
            CanonicalValue::string("x".repeat(262144)).unwrap(),
        )])
        .unwrap();
        let record = riffdb_storage_api::StoredEntityRecordV1::new(
            EntityTarget::new(entity, keys[0].clone()).unwrap(),
            EntityVersion::new(1).unwrap(),
            ContractVersion::new(1).unwrap(),
            riffdb_storage_api::DurableKeySchemaBindingV1::new(
                ContractLineage::new("policy-budget").unwrap(),
                ContractVersion::new(1).unwrap(),
                ContractBundleHash::from_bytes([2; 32]),
            ),
            row,
        )
        .unwrap();
        let source = Source {
            reads: AtomicUsize::new(0),
            encoded: riffdb_storage_api::encode_entity_record_v1(&record)
                .unwrap()
                .as_bytes()
                .to_vec(),
        };
        let principal = riffdb_auth::PrincipalFactBindingV1::new(
            CapabilityId::from_unix_milliseconds_and_random(1, [5; 10]).unwrap(),
            std::num::NonZeroU64::MIN,
            DatabaseId::from_unix_milliseconds_and_random(1, [1; 10]).unwrap(),
            Environment::new("test").unwrap(),
            ActorId::new("reader").unwrap(),
            ActorKind::Human,
            vec![Audience::new("test").unwrap()],
            TenantScope::Global,
            Timestamp::new(1, 0).unwrap(),
            Timestamp::new(100, 0).unwrap(),
            CapabilityPrincipalFactsV1::empty(),
        )
        .unwrap();
        let policy = AuthorizedQueryRowPolicyContextV1::test_fixture(
            principal,
            vec![],
            &riffdb_contract_ir::SchemaIr::new(vec![], vec![], vec![], vec![]).unwrap(),
        )
        .unwrap();
        let budget = ProviderBudget::new();
        let occupied = budget
            .reserve(EXACT_PROVIDER_BUDGET_BYTES - budget.observation().unwrap().fixed_owner_bytes)
            .unwrap();
        assert_eq!(
            authorize_provider_projected_candidates_admitted_v1(
                &source, entity, &keys, &policy, &budget
            )
            .err()
            .unwrap()
            .kind(),
            StorageErrorKind::Unavailable
        );
        assert_eq!(source.reads.load(Ordering::Relaxed), 0);
        drop(occupied);
        let proof = authorize_provider_projected_candidates_admitted_v1(
            &source, entity, &keys, &policy, &budget,
        )
        .unwrap();
        assert!(proof.admits(&keys[0]));
        assert!(!proof.admits(&keys[1]));
        assert!(proof.covers(entity, &keys.iter().cloned().collect()));
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        assert!(budget.observation().unwrap().owned_bytes < 32 * 1024);
        let alias = Arc::clone(&proof);
        drop(proof);
        assert!(budget.observation().unwrap().owned_bytes > 0);
        drop(alias);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }

    // req: OQ-040, PRJ-004
    #[test]
    fn projected_candidate_admission_deduplicates_identical_relationship_lookups() {
        use riffdb_auth::{
            AuthenticationClock, AuthenticationClockError, AuthenticationContext,
            CapabilityAuthenticator, CapabilityDigestKeyProvider, CapabilityReaderCurrentResolver,
            CredentialAuthenticator, NoopAuthenticationTelemetry, OpaqueCredential,
            RawCapabilityToken,
        };
        use riffdb_contract_compiler::compile_contract_source;
        use riffdb_policy::{
            ApplicationExportAuthorizationRequestV1, ApplicationExportDecisionV1,
            ApplicationExportPolicyOperationV1, AuthorizationClock, AuthorizationClockError,
            CurrentAuthorizer, NoopAuthorizationTelemetry,
            resolve_authorized_application_export_row_policy_context,
        };
        use riffdb_storage_api::{
            CapabilityLifecycleV1, CapabilityLookupResult, CapabilityReader, EncodedContentCharge,
            EncodedPageItem, StoredCapabilityRecordV1, StoredIndexEntryV2,
        };
        use riffdb_types::{
            ActorId, ActorKind, AdministrationSequence, ApplicationExportOperationId,
            ApplicationExportSelectionV1, ApplicationRoleHash, Audience,
            CapabilityApplicationExportGrantV1, CapabilityApplicationExportScopeV1,
            CapabilityExportGrantV1, CapabilityGrantV1, CapabilityId, CapabilityPermissionV1,
            CapabilityPermissionsV1, CapabilityPrincipalFactsV1, CapabilityRowPolicyBindingV1,
            CapabilityRowPolicyGrantV1, CapabilityRowPolicyOperationV1, CapabilityTokenDigest,
            ContractBundleHash, ContractVersion, DatabaseId, EntityVersion, Environment,
            PartitionScopeV1, RequestId, RowPolicyName, TenantScope,
        };
        use std::num::{NonZeroU16, NonZeroU64};

        struct FixedClock(Timestamp);
        impl AuthenticationClock for FixedClock {
            fn now(&self) -> Result<Timestamp, AuthenticationClockError> {
                Ok(self.0)
            }
        }
        impl AuthorizationClock for FixedClock {
            fn now(&self) -> Result<Timestamp, AuthorizationClockError> {
                Ok(self.0)
            }
        }
        struct SingleCapabilityReader(StoredCapabilityRecordV1);
        impl CapabilityReader for SingleCapabilityReader {
            fn read_capability(
                &self,
                capability_id: CapabilityId,
            ) -> Result<Option<StoredCapabilityRecordV1>, StorageError> {
                Ok((self.0.capability_id() == capability_id).then(|| self.0.clone()))
            }
            fn resolve_capability_digests(
                &self,
                candidates: &[CapabilityTokenDigest],
            ) -> Result<CapabilityLookupResult, StorageError> {
                if candidates.contains(&self.0.token_digest()) {
                    Ok(CapabilityLookupResult::Found(Box::new(self.0.clone())))
                } else {
                    Ok(CapabilityLookupResult::NotFound)
                }
            }
        }

        let contract_source = r#"
contract TeamPolicy version 1 {
  entity Organization {
    key (organization_id: uuid)
  }

  entity Document {
    key (organization_id: uuid, document_id: uuid)
    field team_id: uuid
  }

  entity TeamMember {
    key (organization_id: uuid, team_id: uuid, subject_id: uuid)
    index by_team_subject (organization_id, team_id, subject_id)
  }

  aggregate Organizations {
    root Organization
    child Document
    child TeamMember
    partition_by organization_id
    conflict_key (organization_id)
  }

  row policy DocumentAccess on Document {
    allow read when exists TeamMember.by_team_subject(
      organization_id,
      team_id,
      principal.id,
    )
  }
}
"#;
        let bundle = compile_contract_source(contract_source).unwrap();
        let doc_schema = bundle
            .schema()
            .entities()
            .iter()
            .find(|entity| entity.name() == "Document")
            .unwrap();
        let doc_entity = doc_schema.id();
        let field_id = |name: &str| {
            doc_schema
                .record()
                .fields()
                .iter()
                .find(|field| field.name() == name)
                .unwrap()
                .id()
        };
        let doc_org_field = field_id("organization_id");
        let doc_id_field = field_id("document_id");
        let doc_team_field = field_id("team_id");
        let make_doc_row = |org: [u8; 16], doc: [u8; 16], team: [u8; 16]| {
            let mut fields = vec![
                (doc_org_field, CanonicalValue::Uuid(org)),
                (doc_id_field, CanonicalValue::Uuid(doc)),
                (doc_team_field, CanonicalValue::Uuid(team)),
            ];
            fields.sort_by_key(|(id, _)| *id);
            riffdb_types::CanonicalRecord::new(fields).unwrap()
        };
        let member_schema = bundle
            .schema()
            .entities()
            .iter()
            .find(|entity| entity.name() == "TeamMember")
            .unwrap();
        let lineage = bundle.lineage().clone();
        let role = ApplicationRoleHash::from_bytes([0x44; 32]);
        let grant = CapabilityGrantV1::new(
            TenantScope::Global,
            PartitionScopeV1::All,
            CapabilityPermissionsV1::new(vec![CapabilityPermissionV1::ApplicationRoleIdentity(
                role,
            )])
            .unwrap(),
            Vec::new(),
            NonZeroU16::new(64).unwrap(),
            Vec::new(),
        )
        .unwrap()
        .with_row_policy(
            CapabilityRowPolicyGrantV1::new(
                role,
                CapabilityPrincipalFactsV1::empty(),
                vec![
                    CapabilityRowPolicyBindingV1::new(
                        lineage.clone(),
                        RowPolicyName::new("DocumentAccess").unwrap(),
                        doc_entity,
                        vec![CapabilityRowPolicyOperationV1::Read],
                    )
                    .unwrap(),
                ],
            )
            .unwrap(),
        )
        .unwrap()
        .with_export(
            CapabilityExportGrantV1::new(vec![
                CapabilityApplicationExportGrantV1::new(
                    lineage.clone(),
                    CapabilityApplicationExportScopeV1::PrincipalFiltered,
                    true,
                    false,
                    false,
                    false,
                )
                .unwrap(),
            ])
            .unwrap(),
        )
        .unwrap();

        let raw_token = b"AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";
        let key_doc = b"riffdb-capability-digest-keys-v1\n1:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n";
        let keys = CapabilityDigestKeyProvider::parse_document(key_doc).unwrap();
        let token = RawCapabilityToken::parse_canonical(raw_token).unwrap();
        let database_id = DatabaseId::from_unix_milliseconds_and_random(1, [0x11; 10]).unwrap();
        let environment = Environment::new("test").unwrap();
        let audience = Audience::new("test").unwrap();
        let record = StoredCapabilityRecordV1::from_stored_parts(
            CapabilityId::from_unix_milliseconds_and_random(1, [0x22; 10]).unwrap(),
            NonZeroU64::MIN,
            keys.current_digest(&token),
            database_id,
            environment.clone(),
            ActorId::new("01010101-0101-0101-0101-010101010101").unwrap(),
            ActorKind::Human,
            vec![audience.clone()],
            Timestamp::new(10, 0).unwrap(),
            Timestamp::new(1_000, 0).unwrap(),
            AdministrationSequence::first(),
            RequestId::from_unix_milliseconds_and_random(1, [0x33; 10]).unwrap(),
            grant,
            CapabilityLifecycleV1::Active,
        )
        .unwrap();
        let cap_reader = SingleCapabilityReader(record);
        let clock = FixedClock(Timestamp::new(100, 0).unwrap());
        let auth_ctx =
            AuthenticationContext::new(database_id, environment.clone(), audience.clone());
        let principal =
            CapabilityAuthenticator::new(&cap_reader, &keys, &clock, &NoopAuthenticationTelemetry)
                .authenticate(OpaqueCredential::new(raw_token), &auth_ctx)
                .unwrap();
        let resolver = CapabilityReaderCurrentResolver::new(&cap_reader);
        let authorizer = CurrentAuthorizer::new(
            &resolver,
            &clock,
            &NoopAuthorizationTelemetry,
            database_id,
            environment,
        );
        let export_req = ApplicationExportAuthorizationRequestV1::new(
            ApplicationExportOperationId::from_unix_milliseconds_and_random(1, [0x55; 10]).unwrap(),
            ApplicationExportSelectionV1::new(
                lineage.clone(),
                CapabilityApplicationExportScopeV1::PrincipalFiltered,
                true,
                false,
                false,
                false,
            )
            .unwrap(),
            ApplicationExportPolicyOperationV1::Page,
        );
        let ApplicationExportDecisionV1::Allow(export_proof) = authorizer
            .authorize_application_export(&principal, export_req)
            .unwrap()
        else {
            panic!("expected allow");
        };
        let policy =
            resolve_authorized_application_export_row_policy_context(&export_proof, &bundle)
                .unwrap()
                .unwrap();

        let org_uuid = [0xaa; 16];
        let team_allowed = [0x10; 16];
        let team_denied = [0x20; 16];
        let schema_binding = riffdb_storage_api::DurableKeySchemaBindingV1::new(
            lineage.clone(),
            ContractVersion::new(1).unwrap(),
            ContractBundleHash::from_bytes([7; 32]),
        );
        let mut records = BTreeMap::new();
        let mut candidate_keys = Vec::new();
        for index in 0..20_u8 {
            let doc_uuid = [index + 1; 16];
            let team_uuid = if index < 10 {
                team_allowed
            } else {
                team_denied
            };
            let key = doc_schema
                .primary_key()
                .encode_entity(&[
                    CanonicalValue::Uuid(org_uuid),
                    CanonicalValue::Uuid(doc_uuid),
                ])
                .unwrap();
            let row = make_doc_row(org_uuid, doc_uuid, team_uuid);
            let stored = riffdb_storage_api::StoredEntityRecordV1::new(
                EntityTarget::new(doc_entity, key.clone()).unwrap(),
                EntityVersion::new(1).unwrap(),
                ContractVersion::new(1).unwrap(),
                schema_binding.clone(),
                row,
            )
            .unwrap();
            records.insert(
                key.clone(),
                riffdb_storage_api::encode_entity_record_v1(&stored)
                    .unwrap()
                    .as_bytes()
                    .to_vec(),
            );
            candidate_keys.push((key, index < 10));
        }
        candidate_keys.sort_by(|left, right| left.0.cmp(&right.0));
        let ordered_keys = candidate_keys
            .iter()
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();

        // Compute the exact index prefix for team_allowed so our mock index reader can match it.
        let allowed_probe_row = make_doc_row(org_uuid, [1; 16], team_allowed);
        let allowed_lookup = policy
            .relationship_lookups(doc_entity, &allowed_probe_row)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let allowed_prefix = allowed_lookup.index_prefix().to_vec();

        let member_values = [
            CanonicalValue::Uuid(org_uuid),
            CanonicalValue::Uuid(team_allowed),
            CanonicalValue::Uuid([0x01; 16]),
        ];
        let member_key = member_schema
            .primary_key()
            .encode_entity(&member_values)
            .unwrap();
        let index_entry_key = member_schema.indexes()[0]
            .key_schema()
            .encode_index(&member_values, member_key)
            .unwrap();
        let allowed_index_entry = StoredIndexEntryV2::new(
            index_entry_key,
            schema_binding,
            riffdb_types::CanonicalRecord::new(Vec::new()).unwrap(),
            allowed_lookup.partition().clone(),
        )
        .unwrap();

        struct RelationshipCountingSource {
            entity_reads: AtomicUsize,
            relationship_reads: AtomicUsize,
            records: BTreeMap<EntityKey, Vec<u8>>,
            stored_records: BTreeMap<EntityKey, riffdb_storage_api::StoredEntityRecordV1>,
            allowed_prefix: Vec<u8>,
            allowed_index_entry: StoredIndexEntryV2,
        }
        impl ProviderPolicySourceReaderV1 for RelationshipCountingSource {
            fn read_provider_entity_record_v1(
                &self,
                target: &EntityTarget,
                budget: &ProviderBudget,
            ) -> Result<Option<riffdb_storage_api::AdmittedProviderEntityRecordV1>, StorageError>
            {
                self.entity_reads.fetch_add(1, Ordering::Relaxed);
                let Some(encoded) = self.records.get(target.key()) else {
                    return Ok(None);
                };
                riffdb_storage_api::decode_provider_entity_record_admitted_v1(encoded, budget)
                    .map(Some)
                    .map_err(|_| StorageError::new(StorageErrorKind::Unavailable, None))
            }
            fn read_provider_policy_index_range_v1(
                &self,
                request: &SnapshotIndexRangeRequestV1,
                budget: &ProviderBudget,
            ) -> Result<Arc<ProviderAllocation<SnapshotIndexRangePageV1>>, StorageError>
            {
                self.relationship_reads.fetch_add(1, Ordering::Relaxed);
                let entries = if request.lower() == self.allowed_prefix.as_slice() {
                    vec![EncodedPageItem::new(
                        self.allowed_index_entry.clone(),
                        EncodedContentCharge::new(64).unwrap(),
                    )]
                } else {
                    Vec::new()
                };
                let page = SnapshotIndexRangePageV1::new(request, entries, false).unwrap();
                let reservation = budget
                    .reserve(1024)
                    .map_err(|_| StorageError::new(StorageErrorKind::Unavailable, None))?;
                reservation
                    .commit_value(page)
                    .map_err(|_| StorageError::new(StorageErrorKind::Unavailable, None))
            }
        }
        impl riffdb_storage_api::AuthoritativePointReader for RelationshipCountingSource {
            fn read_entity(
                &self,
                target: &EntityTarget,
            ) -> Result<Option<riffdb_storage_api::StoredEntityRecordV1>, StorageError>
            {
                self.entity_reads.fetch_add(1, Ordering::Relaxed);
                Ok(self.stored_records.get(target.key()).cloned())
            }
            fn read_stored_outcome(
                &self,
                _: &riffdb_storage_api::IdempotencyIdentity,
            ) -> Result<Option<riffdb_storage_api::StoredOutcomeV1>, StorageError> {
                unreachable!()
            }
            fn read_commit(
                &self,
                _: riffdb_types::CommitSequence,
            ) -> Result<Option<riffdb_storage_api::StoredCommitRecordV1>, StorageError>
            {
                unreachable!()
            }
            fn read_provenance(
                &self,
                _: riffdb_types::ProvenanceId,
            ) -> Result<Option<riffdb_storage_api::StoredProvenanceRecordV1>, StorageError>
            {
                unreachable!()
            }
            fn read_durable_event(
                &self,
                _: riffdb_types::EventId,
            ) -> Result<Option<riffdb_storage_api::StoredDurableEventV1>, StorageError>
            {
                unreachable!()
            }
        }
        impl riffdb_storage_api::AuthoritativeScanReader for RelationshipCountingSource {
            fn scan_index(
                &self,
                _: riffdb_storage_api::AuthoritativeIndexScanRequest,
            ) -> Result<riffdb_storage_api::AuthoritativeIndexScanPage, StorageError> {
                unreachable!()
            }
            fn scan_commits(
                &self,
                _: riffdb_storage_api::CommitScanRequest,
            ) -> Result<riffdb_storage_api::CommitScanPageV1, StorageError> {
                unreachable!()
            }
        }
        impl riffdb_storage_api::FilteredAuthoritativeScanReader for RelationshipCountingSource {
            fn scan_index_filtered(
                &self,
                _: riffdb_storage_api::FilteredAuthoritativeIndexScanRequest,
            ) -> Result<riffdb_storage_api::FilteredAuthoritativeIndexScanPage, StorageError>
            {
                unreachable!()
            }
        }
        impl riffdb_storage_api::SnapshotReader for RelationshipCountingSource {
            fn read_snapshot(
                &self,
                _: riffdb_storage_api::SnapshotRequest,
            ) -> Result<riffdb_storage_api::ReadSnapshot, StorageError> {
                unreachable!()
            }
        }
        impl riffdb_storage_api::SnapshotFenceReader for RelationshipCountingSource {
            fn application_frontier(
                &self,
            ) -> Result<Option<riffdb_types::CommitSequence>, StorageError> {
                Ok(Some(riffdb_types::CommitSequence::first()))
            }
            fn index_epoch(
                &self,
                _: &riffdb_storage_api::PartitionIndexTarget,
            ) -> Result<u64, StorageError> {
                Ok(1)
            }
            fn scan_snapshot_index_range(
                &self,
                request: SnapshotIndexRangeRequestV1,
            ) -> Result<SnapshotIndexRangePageV1, StorageError> {
                self.relationship_reads.fetch_add(1, Ordering::Relaxed);
                let entries = if request.lower() == self.allowed_prefix.as_slice() {
                    vec![EncodedPageItem::new(
                        self.allowed_index_entry.clone(),
                        EncodedContentCharge::new(64).unwrap(),
                    )]
                } else {
                    Vec::new()
                };
                Ok(SnapshotIndexRangePageV1::new(&request, entries, false).unwrap())
            }
        }
        impl riffdb_storage_api::VectorObservationRepository for RelationshipCountingSource {
            fn read_vector_observation(
                &self,
                _: &riffdb_storage_api::VectorObservationTargetV1,
            ) -> Result<Option<riffdb_storage_api::VectorObservationCountsV1>, StorageError>
            {
                unreachable!()
            }
            fn read_vector_health_observation(
                &self,
                _: &riffdb_types::ContractLineage,
            ) -> Result<Option<riffdb_storage_api::VectorHealthObservationV1>, StorageError>
            {
                unreachable!()
            }
        }
        impl riffdb_storage_api::VectorEvidenceIndexRepository for RelationshipCountingSource {
            fn scan_vector_evidence_index(
                &self,
                _: &riffdb_storage_api::VectorEvidenceIndexScanRequestV1,
            ) -> Result<riffdb_storage_api::VectorEvidenceIndexPageV1, StorageError> {
                unreachable!()
            }
        }

        let stored_records = records
            .iter()
            .map(|(key, encoded)| {
                (
                    key.clone(),
                    riffdb_storage_api::decode_entity_record_v1(encoded)
                        .unwrap()
                        .value()
                        .clone(),
                )
            })
            .collect();
        let source = RelationshipCountingSource {
            entity_reads: AtomicUsize::new(0),
            relationship_reads: AtomicUsize::new(0),
            records,
            stored_records,
            allowed_prefix,
            allowed_index_entry,
        };
        let budget = ProviderBudget::new();
        let proof = authorize_provider_projected_candidates_admitted_v1(
            &source,
            doc_entity,
            &ordered_keys,
            &policy,
            &budget,
        )
        .unwrap();

        assert_eq!(source.entity_reads.load(Ordering::Relaxed), 20);
        assert_eq!(
            source.relationship_reads.load(Ordering::Relaxed),
            2,
            "20 candidate rows sharing 2 team relationships must perform 2 index range reads, not 20"
        );
        for (key, expected_allowed) in &candidate_keys {
            assert_eq!(proof.admits(key), *expected_allowed);
        }

        source.entity_reads.store(0, Ordering::Relaxed);
        source.relationship_reads.store(0, Ordering::Relaxed);
        let direct_proof =
            authorize_candidates(&source, doc_entity, &ordered_keys, &policy).unwrap();
        assert_eq!(source.entity_reads.load(Ordering::Relaxed), 20);
        assert_eq!(
            source.relationship_reads.load(Ordering::Relaxed),
            2,
            "authorize_candidates over OwnedSnapshotHandle must also deduplicate identical relationship lookups"
        );
        for (key, expected_allowed) in &candidate_keys {
            assert_eq!(direct_proof.admits(key), *expected_allowed);
        }
    }
}
