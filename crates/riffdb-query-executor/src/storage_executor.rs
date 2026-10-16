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
    QueryAccessProgramV1, QueryAccessStep, QueryPlanIdentity, QueryPredicateOperator,
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
    ApplicationRoleHash, CanonicalValue, EntityKey, EntityTypeId, EventId, FieldId, Timestamp,
};

use crate::{
    BoundPredicate, CoveredResultBatch, LongPatternCandidateBatch, MAX_QUERY_SCANNED_ROWS,
    QueryBackendFault, QueryContinuation, QueryExecutionError, QueryExecutionPort,
    QueryExecutionRequest, QueryNearestPage, QueryOwnedSnapshot, QueryParameters, QueryReadView,
    QueryRow, QueryScanPage, VectorInspectionCandidateV1, VectorInspectionSnapshotV1,
    VectorInspectionTargetV1, covered_row_matches_predicates_at_positions_v1, execute_in_snapshot,
    execute_operational_page_in_snapshot, execute_page_in_snapshot,
    execute_policy_operational_page_in_snapshot, execute_policy_page_in_snapshot,
    execute_policy_provider_page_in_snapshot, execute_provider_page_in_snapshot,
    step_output_selected_fields_v1, validate_query_execution_group,
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
        self.authorize_event_candidate_prefix(event_ids, observed_at, policy, None)
    }

    /// Evaluates the exact candidate prefix through the requested visible row.
    /// Denied candidates retain their neutral observations and consume scan
    /// work, while the final backend still rechecks the complete selected prefix.
    #[doc(hidden)]
    pub fn authorize_event_candidates_through_limit(
        &self,
        event_ids: &[EventId],
        observed_at: Timestamp,
        policy: &AuthorizedQueryRowPolicyContextV1,
        admitted_limit: std::num::NonZeroU16,
    ) -> Result<EventCandidateAuthorizationV1, QueryExecutionError> {
        if event_ids.len() > riffdb_storage_api::MAX_EVENT_POLICY_ADMISSION_OBSERVATIONS_V1
            || event_ids.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(QueryExecutionError::BackendIntegrity);
        }
        self.authorize_event_candidate_prefix(event_ids, observed_at, policy, Some(admitted_limit))
    }

    fn authorize_event_candidate_prefix(
        &self,
        event_ids: &[EventId],
        observed_at: Timestamp,
        policy: &AuthorizedQueryRowPolicyContextV1,
        admitted_limit: Option<std::num::NonZeroU16>,
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
        let observations = crate::event_candidate_prefix::collect_admitted_prefix(
            event_ids,
            admitted_limit,
            |event_id| {
                let event = snapshot
                    .read_durable_event(*event_id)
                    .map_err(map_storage_error)?
                    .ok_or(QueryExecutionError::BackendIntegrity)?;
                let Some(anchor) = event.policy_anchor() else {
                    return EventPolicyAdmissionObservationV1::new(event, None, Vec::new(), false)
                        .map(|observation| (observation, false))
                        .map_err(|_| QueryExecutionError::BackendIntegrity);
                };
                let current = snapshot
                    .read_entity(anchor.source())
                    .map_err(map_storage_error)?;
                let Some(record) = current else {
                    return EventPolicyAdmissionObservationV1::new(event, None, Vec::new(), false)
                        .map(|observation| (observation, false))
                        .map_err(|_| QueryExecutionError::BackendIntegrity);
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
                    let exists = cached_indexed_relationship_exists(
                        &snapshot,
                        &mut relationship_cache,
                        lookup,
                    )
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
                EventPolicyAdmissionObservationV1::new(event, Some(record), relationships, admitted)
                    .map(|observation| (observation, admitted))
                    .map_err(|_| QueryExecutionError::BackendIntegrity)
            },
        )?;
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
    head: u64,
    relationship_cache: BTreeMap<RelationshipLookupCacheKey, bool>,
    prepared: Arc<PreparedStorageProgramPlans>,
    materialize_plans: BTreeMap<String, Arc<RowMaterializePlan>>,
    last_index_epoch: Option<(
        riffdb_types::IndexId,
        CanonicalValue,
        PartitionIndexTarget,
        u64,
    )>,
    point_key_values_scratch: Vec<CanonicalValue>,
}

impl<'a, H> StorageQueryView<'a, H>
where
    H: OwnedSnapshotHandle,
{
    fn new(
        snapshot: &'a H,
        program: &'a QueryAccessProgramV1,
        _parameters: &'a QueryParameters,
    ) -> Result<Self, QueryExecutionError> {
        let head = snapshot
            .application_frontier()
            .map_err(map_storage_error)?
            .map_or(0, riffdb_types::CommitSequence::get);
        Ok(Self {
            snapshot,
            program,
            head,
            relationship_cache: BTreeMap::new(),
            prepared: prepared_storage_program_plans(program),
            materialize_plans: BTreeMap::new(),
            last_index_epoch: None,
            point_key_values_scratch: Vec::with_capacity(4),
        })
    }

    fn materialize_plan(
        &mut self,
        step: &QueryAccessStep,
    ) -> Result<Arc<RowMaterializePlan>, StorageError> {
        if let Some(plan) = self
            .prepared
            .steps
            .iter()
            .find(|candidate| {
                candidate.binding == step.binding() && candidate.entity == step.entity()
            })
            .and_then(|candidate| candidate.materialize_plan.as_ref())
        {
            return Ok(Arc::clone(plan));
        }
        if let Some(plan) = self.materialize_plans.get(step.entity()) {
            return Ok(Arc::clone(plan));
        }
        let plan = Arc::new(RowMaterializePlan::for_step(self.program, step)?);
        self.materialize_plans
            .insert(step.entity().to_owned(), Arc::clone(&plan));
        Ok(plan)
    }

    fn covered_plan(
        &self,
        step: &QueryAccessStep,
    ) -> Result<Option<Arc<CoveredScanPlan>>, StorageError> {
        if let Some(plan) = self
            .prepared
            .steps
            .iter()
            .find(|candidate| {
                candidate.binding == step.binding() && candidate.entity == step.entity()
            })
            .and_then(|candidate| candidate.covered_plan.as_ref())
        {
            return Ok(Some(Arc::clone(plan)));
        }
        CoveredScanPlan::for_step(self.program, step).map(|plan| plan.map(Arc::new))
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
        self.point_key_values_scratch.clear();
        if let Some(indices) = &plan.point_key_predicate_indices
            && indices.len() == key_fields.len()
            && indices.iter().all(|&idx| {
                predicates
                    .get(idx)
                    .is_some_and(|p| p.operator() == QueryPredicateOperator::Equal)
            })
        {
            for &idx in indices {
                let value = predicates[idx].value();
                if matches!(value, CanonicalValue::Null) {
                    return Ok(None);
                }
                self.point_key_values_scratch.push(value.clone());
            }
        } else {
            for field in key_fields {
                let value = exact_value(predicates, field)?;
                if matches!(value, CanonicalValue::Null) {
                    return Ok(None);
                }
                self.point_key_values_scratch.push(value);
            }
        }
        let key = step
            .internal_entity_key_schema()
            .encode_entity(&self.point_key_values_scratch)
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
        let plan = self.materialize_plan(step)?;
        let page_limit = usize::try_from(limit).map_err(|_| limit_exceeded())?;
        let plan_ptr = Arc::as_ptr(&plan) as usize;
        let probe_hash = hash_scan_probe(plan_ptr, page_limit, predicates);
        let mut prefetched_first_page: Option<riffdb_storage_api::SnapshotIndexRangePageV1> = None;
        if policy.is_none() && after.is_none() && !plan.has_lossy_index_predicate {
            let mut cached_probe = LOCAL_RANGE_SCAN_ROWS_CACHE.with(|cache| {
                let guard = cache.try_borrow().ok()?;
                guard
                    .find_by_probe(probe_hash, plan_ptr, page_limit, predicates)
                    .map(|cached| {
                        (
                            cached.request.clone(),
                            cached.entries_ptr,
                            cached.entries_len,
                            cached.head,
                            cached.epoch,
                            cached.index_id,
                            cached.partition_value.clone(),
                            cached.target.clone(),
                            Arc::clone(&cached.rows),
                            cached.scanned_rows,
                            cached.point_reads,
                            cached.continuation.clone(),
                        )
                    })
            });
            if cached_probe.is_none()
                && let Some(shared_entry) = lookup_shared_range_scan_rows_by_probe(
                    probe_hash, plan_ptr, page_limit, predicates,
                )
            {
                cached_probe = Some((
                    shared_entry.request.clone(),
                    shared_entry.entries_ptr,
                    shared_entry.entries_len,
                    shared_entry.head,
                    shared_entry.epoch,
                    shared_entry.index_id,
                    shared_entry.partition_value.clone(),
                    shared_entry.target.clone(),
                    Arc::clone(&shared_entry.rows),
                    shared_entry.scanned_rows,
                    shared_entry.point_reads,
                    shared_entry.continuation.clone(),
                ));
                LOCAL_RANGE_SCAN_ROWS_CACHE.with(|cache| {
                    if let Ok(mut guard) = cache.try_borrow_mut() {
                        guard.insert(shared_entry);
                    }
                });
            }
            if let Some((
                cached_req,
                expected_ptr,
                expected_len,
                cached_head,
                cached_epoch,
                cached_index_id,
                cached_partition_val,
                cached_target,
                cached_rows,
                cached_scanned,
                cached_point_reads,
                cached_cont,
            )) = cached_probe
            {
                let page = self.snapshot.scan_snapshot_index_range(cached_req)?;
                if cached_head == self.head
                    && page.next_after().is_none()
                    && !page.entries().is_empty()
                    && page.entries().as_ptr() as usize == expected_ptr
                    && page.entries().len() == expected_len
                    && let Some(hit) = QueryScanPage::shared_verified(
                        cached_rows,
                        cached_epoch,
                        cached_scanned,
                        cached_point_reads,
                        cached_cont,
                    )
                {
                    self.last_index_epoch = Some((
                        cached_index_id,
                        cached_partition_val,
                        cached_target,
                        cached_epoch,
                    ));
                    return Ok(hit);
                }
                prefetched_first_page = Some(page);
            }
        }
        let direction = access_direction(step)?;
        let schema = step.internal_index_key_schema().ok_or_else(invariant)?;
        let schedule =
            crate::bound_index_range_schedule_v1(step, predicates).map_err(|_| invariant())?;
        let partition_value = partition_value_at(
            self.program,
            step,
            predicates,
            plan.partition_predicate_index,
        )?;
        let index_id = step.internal_index_id().ok_or_else(invariant)?;
        let (target, epoch) = if let Some((cached_idx, cached_val, cached_target, cached_epoch)) =
            &self.last_index_epoch
            && *cached_idx == index_id
            && cached_val == partition_value
        {
            (cached_target.clone(), *cached_epoch)
        } else {
            let partition = step
                .internal_partition_key_schema()
                .encode_partition(std::slice::from_ref(partition_value))
                .map_err(|_| invariant())?;
            let target = PartitionIndexTarget::new(partition, index_id);
            let epoch = self.snapshot.index_epoch(&target)?;
            self.last_index_epoch =
                Some((index_id, partition_value.clone(), target.clone(), epoch));
            (target, epoch)
        };
        let fetch_limit = page_limit.saturating_add(1);
        let scan_ceiling = usize::try_from(MAX_QUERY_SCANNED_ROWS).map_err(|_| limit_exceeded())?;
        let can_cache_single_page = policy.is_none()
            && schedule.ranges().len() == 1
            && after.is_none()
            && !plan.has_lossy_index_predicate;
        let mut single_page_pin: Option<(
            SnapshotIndexRangeRequestV1,
            riffdb_storage_api::SnapshotIndexRangePageV1,
            usize,
            usize,
        )> = None;
        let mut rows = Vec::<QueryRow>::new();
        let mut last_page_key: Option<Vec<u8>> = None;
        let mut physical = 0usize;
        let mut eligible = 0usize;

        let mut all_predicates_verified = !plan.has_lossy_index_predicate;
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
                let page = match prefetched_first_page.take() {
                    Some(prefetched) => prefetched,
                    None => self.snapshot.scan_snapshot_index_range(request.clone())?,
                };
                if can_cache_single_page
                    && lower_after.is_none()
                    && page.next_after().is_none()
                    && !page.entries().is_empty()
                {
                    let entries_ptr = page.entries().as_ptr() as usize;
                    let entries_len = page.entries().len();
                    if let Some(hit) = LOCAL_RANGE_SCAN_ROWS_CACHE.with(|cache| {
                        let guard = cache.try_borrow().ok()?;
                        guard
                            .find_by_probe(probe_hash, plan_ptr, page_limit, predicates)
                            .filter(|cached| {
                                cached.entries_ptr == entries_ptr
                                    && cached.entries_len == entries_len
                                    && cached.head == self.head
                                    && cached.epoch == epoch
                                    && &cached.partition_key == target.partition_key()
                            })
                            .and_then(|cached| {
                                QueryScanPage::shared_verified(
                                    Arc::clone(&cached.rows),
                                    epoch,
                                    cached.scanned_rows,
                                    cached.point_reads,
                                    cached.continuation.clone(),
                                )
                            })
                    }) {
                        return Ok(hit);
                    }
                    if let Some(shared_entry) = lookup_shared_range_scan_rows(
                        probe_hash,
                        entries_ptr,
                        entries_len,
                        plan_ptr,
                        self.head,
                        epoch,
                        page_limit,
                        target.partition_key(),
                        predicates,
                    ) && let Some(hit) = QueryScanPage::shared_verified(
                        Arc::clone(&shared_entry.rows),
                        epoch,
                        shared_entry.scanned_rows,
                        shared_entry.point_reads,
                        shared_entry.continuation.clone(),
                    ) {
                        LOCAL_RANGE_SCAN_ROWS_CACHE.with(|cache| {
                            if let Ok(mut guard) = cache.try_borrow_mut() {
                                guard.insert(shared_entry);
                            }
                        });
                        return Ok(hit);
                    }
                    single_page_pin = Some((request, page.clone(), entries_ptr, entries_len));
                }
                if rows.capacity() == 0 {
                    rows.reserve(page.entries().len().min(fetch_limit).min(scan_ceiling));
                }
                physical = physical
                    .checked_add(page.entries().len())
                    .ok_or_else(limit_exceeded)?;
                for (entry_index, charged) in page.entries().iter().enumerate() {
                    let entry = charged.value();
                    if entry.partition_key() != target.partition_key() {
                        continue;
                    }
                    eligible = eligible.checked_add(1).ok_or_else(limit_exceeded)?;
                    if let Some(cached) = page.resolved_entity(entry_index) {
                        if !allows_policy_record(
                            self.snapshot,
                            &mut self.relationship_cache,
                            policy,
                            step.internal_entity_id(),
                            cached.fields(),
                        )? {
                            continue;
                        }
                        if plan.has_lossy_index_predicate {
                            all_predicates_verified = false;
                        }
                        rows.push(plan.materialize(cached)?);
                    } else {
                        let entity_key = schema
                            .decode_index_entity_key(entry.key())
                            .map_err(|_| corrupt())?;
                        let entity_target =
                            EntityTarget::new(step.internal_entity_id(), entity_key)
                                .map_err(|_| corrupt())?;
                        let fetched = self
                            .snapshot
                            .read_entity(&entity_target)?
                            .ok_or_else(corrupt)?;
                        if !allows_policy_record(
                            self.snapshot,
                            &mut self.relationship_cache,
                            policy,
                            step.internal_entity_id(),
                            fetched.fields(),
                        )? {
                            continue;
                        }
                        let row = plan.materialize(&fetched)?;
                        let row_verified = !plan.has_lossy_index_predicate
                            && row.entity() == step.entity()
                            && crate::predicates_match_at_positions(
                                &row,
                                predicates,
                                plan.predicate_positions.as_deref(),
                            ) == Ok(true);
                        if row_verified {
                            let _ = page.cache_resolved_entity(entry_index, fetched);
                        } else {
                            all_predicates_verified = false;
                        }
                        rows.push(row);
                    }
                    if rows.len() == page_limit {
                        last_page_key = Some(entry.key().as_bytes().to_vec());
                    }
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
        let continuation = if has_more { last_page_key } else { None };
        let scanned_rows = u64::try_from(eligible).map_err(|_| limit_exceeded())?;
        if all_predicates_verified
            && let Some((request, page_pin, entries_ptr, entries_len)) = single_page_pin
        {
            let reported_scanned = if continuation.is_some() {
                scanned_rows.max(1)
            } else {
                scanned_rows
            };
            let point_reads = rows.len() as u64;
            let shared_rows: Arc<[QueryRow]> = rows.into();
            let scan_page = QueryScanPage::shared_verified(
                Arc::clone(&shared_rows),
                epoch,
                reported_scanned,
                point_reads,
                continuation.clone(),
            )
            .ok_or_else(invariant)?;
            let entry = CachedRangeScanRows {
                _page_pin: page_pin,
                _plan_pin: Arc::clone(&plan),
                probe_hash,
                request,
                index_id,
                partition_value: partition_value.clone(),
                target: target.clone(),
                entries_ptr,
                entries_len,
                plan_ptr,
                head: self.head,
                epoch,
                page_limit,
                partition_key: target.partition_key().clone(),
                predicates: predicates.to_vec(),
                rows: shared_rows,
                scanned_rows: reported_scanned,
                point_reads,
                continuation,
            };
            insert_shared_range_scan_rows(entry.clone());
            LOCAL_RANGE_SCAN_ROWS_CACHE.with(|cache| {
                if let Ok(mut guard) = cache.try_borrow_mut() {
                    guard.insert(entry);
                }
            });
            return Ok(scan_page);
        }
        let mut scan_page = match continuation {
            Some(continuation) => {
                QueryScanPage::continued(rows, epoch, scanned_rows.max(1), continuation)
                    .ok_or_else(invariant)?
            }
            None => {
                QueryScanPage::policy_exact_end(rows, epoch, scanned_rows).ok_or_else(invariant)?
            }
        };
        scan_page.predicates_verified = all_predicates_verified;
        Ok(scan_page)
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
        let covered_plan = self.covered_plan(step)?.ok_or_else(invariant)?;
        let partition_value = partition_value_at(
            self.program,
            step,
            predicates,
            covered_plan.partition_predicate_index,
        )?;
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
        let expected_cover_ids = &covered_plan.expected_cover_ids;
        let field_names = &covered_plan.field_names;
        let needs_entity_key = covered_plan.needs_entity_key;
        let cover_positions = &covered_plan.cover_positions;
        let predicate_positions = covered_plan.predicate_positions.as_deref();
        let has_lossy_predicate = covered_plan.has_lossy_index_predicate;
        let mut rows = Vec::<Vec<CanonicalValue>>::with_capacity(fetch_limit.min(scan_ceiling));
        let mut last_page_key: Option<Vec<u8>> = None;
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
                for (entry_index, charged) in page.entries().iter().enumerate() {
                    let entry = charged.value();
                    if entry.partition_key() != target.partition_key() {
                        continue;
                    }
                    eligible = eligible.checked_add(1).ok_or_else(limit_exceeded)?;
                    if let Some((cached_values, _, _, _)) =
                        page.covered_projection(entry_index, expected_cover_ids, field_names)
                    {
                        if has_lossy_predicate
                            && !covered_row_matches_predicates_at_positions_v1(
                                layout,
                                cached_values,
                                predicates,
                                predicate_positions,
                            )
                            .map_err(|_| invariant())?
                        {
                            continue;
                        }
                        rows.push(cached_values.to_vec());
                    } else {
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
                                .zip(expected_cover_ids.iter())
                                .any(|((actual, _), expected)| actual != expected)
                        {
                            return Err(corrupt());
                        }
                        let decoded = schema.decode_index(entry.key()).map_err(|_| corrupt())?;
                        let entity_values = if needs_entity_key {
                            step.internal_entity_key_schema()
                                .decode_entity(decoded.entity_key())
                                .map_err(|_| corrupt())?
                        } else {
                            step.internal_entity_key_schema()
                                .validate_entity(decoded.entity_key())
                                .map_err(|_| corrupt())?;
                            Vec::new()
                        };
                        let values = layout
                            .fields()
                            .iter()
                            .zip(cover_positions)
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
                        let matches = covered_row_matches_predicates_at_positions_v1(
                            layout,
                            &values,
                            predicates,
                            predicate_positions,
                        )
                        .map_err(|_| invariant())?;
                        if !matches {
                            continue;
                        }
                        let mut fields_map = BTreeMap::new();
                        let mut fields_bytes = 0u64;
                        for (name, value) in field_names.iter().zip(values.iter()) {
                            let encoded_len = riffdb_types::canonical_value_encoded_len(value)
                                .map_err(|_| invariant())?;
                            fields_bytes = fields_bytes
                                .checked_add(name.len() as u64)
                                .and_then(|v| v.checked_add(encoded_len as u64))
                                .and_then(|v| v.checked_add(160))
                                .ok_or_else(limit_exceeded)?;
                            fields_map.insert(Arc::clone(name), value.clone());
                        }
                        let projected_fields = match &covered_plan.selected_positions {
                            Some(positions) => {
                                let mut proj = BTreeMap::new();
                                let mut proj_bytes = 0u64;
                                for (name, pos) in positions {
                                    if let Some(val) = values.get(*pos) {
                                        let encoded_len =
                                            riffdb_types::canonical_value_encoded_len(val)
                                                .map_err(|_| invariant())?;
                                        proj_bytes = proj_bytes
                                            .checked_add(name.len() as u64)
                                            .and_then(|v| v.checked_add(encoded_len as u64))
                                            .and_then(|v| v.checked_add(160))
                                            .ok_or_else(limit_exceeded)?;
                                        proj.insert(Arc::clone(name), val.clone());
                                    }
                                }
                                Some((Arc::new(proj), proj_bytes))
                            }
                            None => None,
                        };
                        let values_arc: Arc<[CanonicalValue]> = values.clone().into();
                        page.cache_covered_projection(
                            entry_index,
                            (
                                Arc::clone(expected_cover_ids),
                                Arc::clone(field_names),
                                values_arc,
                                Arc::new(fields_map),
                                fields_bytes,
                                projected_fields,
                            ),
                        );
                        rows.push(values);
                    }
                    if rows.len() == page_limit {
                        last_page_key = Some(entry.key().as_bytes().to_vec());
                    }
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
        let continuation = if has_more { last_page_key } else { None };
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

    fn read_covered_scan_page(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[BoundPredicate],
        limit: u64,
        after: Option<&[u8]>,
        policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<Option<QueryScanPage>, StorageError> {
        let Some(layout) = step.covered_result_layout() else {
            return Ok(None);
        };
        if policy.is_some() {
            return Err(invariant());
        }
        let covered_plan = self.covered_plan(step)?.ok_or_else(invariant)?;
        if !covered_plan.rows_valid {
            return Err(invariant());
        }
        let page_limit = usize::try_from(limit).map_err(|_| limit_exceeded())?;
        let plan_ptr = Arc::as_ptr(&covered_plan) as usize;
        let probe_hash = hash_scan_probe(plan_ptr, page_limit, predicates);
        let has_lossy_predicate = covered_plan.has_lossy_index_predicate;
        let mut prefetched_first_page: Option<riffdb_storage_api::SnapshotIndexRangePageV1> = None;
        if after.is_none() && !has_lossy_predicate {
            let mut cached_probe = LOCAL_COVERED_SCAN_ROWS_CACHE.with(|cache| {
                let guard = cache.try_borrow().ok()?;
                guard
                    .find_by_probe(probe_hash, plan_ptr, page_limit, predicates)
                    .map(|cached| {
                        (
                            cached.request.clone(),
                            cached.entries_ptr,
                            cached.entries_len,
                            cached.head,
                            cached.epoch,
                            cached.index_id,
                            cached.partition_value.clone(),
                            cached.target.clone(),
                            Arc::clone(&cached.rows),
                            cached.scanned_rows,
                            cached.continuation.clone(),
                        )
                    })
            });
            if cached_probe.is_none()
                && let Some(shared_entry) = lookup_shared_covered_scan_rows_by_probe(
                    probe_hash, plan_ptr, page_limit, predicates,
                )
            {
                cached_probe = Some((
                    shared_entry.request.clone(),
                    shared_entry.entries_ptr,
                    shared_entry.entries_len,
                    shared_entry.head,
                    shared_entry.epoch,
                    shared_entry.index_id,
                    shared_entry.partition_value.clone(),
                    shared_entry.target.clone(),
                    Arc::clone(&shared_entry.rows),
                    shared_entry.scanned_rows,
                    shared_entry.continuation.clone(),
                ));
                LOCAL_COVERED_SCAN_ROWS_CACHE.with(|cache| {
                    if let Ok(mut guard) = cache.try_borrow_mut() {
                        guard.insert(shared_entry);
                    }
                });
            }
            if let Some((
                cached_req,
                expected_ptr,
                expected_len,
                cached_head,
                cached_epoch,
                cached_index_id,
                cached_partition_val,
                cached_target,
                cached_rows,
                cached_scanned,
                cached_cont,
            )) = cached_probe
            {
                let page = self.snapshot.scan_snapshot_index_range(cached_req)?;
                if cached_head == self.head
                    && page.next_after().is_none()
                    && !page.entries().is_empty()
                    && page.entries().as_ptr() as usize == expected_ptr
                    && page.entries().len() == expected_len
                    && let Some(hit) = QueryScanPage::shared_verified(
                        cached_rows,
                        cached_epoch,
                        cached_scanned,
                        0,
                        cached_cont,
                    )
                {
                    self.last_index_epoch = Some((
                        cached_index_id,
                        cached_partition_val,
                        cached_target,
                        cached_epoch,
                    ));
                    return Ok(Some(hit));
                }
                prefetched_first_page = Some(page);
            }
        }
        let direction = access_direction(step)?;
        let schema = step.internal_index_key_schema().ok_or_else(invariant)?;
        let schedule =
            crate::bound_index_range_schedule_v1(step, predicates).map_err(|_| invariant())?;
        let partition_value = partition_value_at(
            self.program,
            step,
            predicates,
            covered_plan.partition_predicate_index,
        )?;
        let index_id = step.internal_index_id().ok_or_else(invariant)?;
        let (target, epoch) = if let Some((cached_idx, cached_val, cached_target, cached_epoch)) =
            &self.last_index_epoch
            && *cached_idx == index_id
            && cached_val == partition_value
        {
            (cached_target.clone(), *cached_epoch)
        } else {
            let partition = step
                .internal_partition_key_schema()
                .encode_partition(std::slice::from_ref(partition_value))
                .map_err(|_| invariant())?;
            let target = PartitionIndexTarget::new(partition, index_id);
            let epoch = self.snapshot.index_epoch(&target)?;
            self.last_index_epoch =
                Some((index_id, partition_value.clone(), target.clone(), epoch));
            (target, epoch)
        };
        let fetch_limit = page_limit.saturating_add(1);
        let scan_ceiling = usize::try_from(MAX_QUERY_SCANNED_ROWS).map_err(|_| limit_exceeded())?;
        let expected_cover_ids = &covered_plan.expected_cover_ids;
        let entity = &covered_plan.entity;
        let field_names = &covered_plan.field_names;
        let selected_prevalidated = covered_plan.selected_prevalidated;
        let needs_entity_key = covered_plan.needs_entity_key;
        let cover_positions = &covered_plan.cover_positions;
        let predicate_positions = covered_plan.predicate_positions.as_deref();
        let can_cache_single_page =
            schedule.ranges().len() == 1 && after.is_none() && !has_lossy_predicate;
        let mut single_page_pin: Option<(
            SnapshotIndexRangeRequestV1,
            riffdb_storage_api::SnapshotIndexRangePageV1,
            usize,
            usize,
        )> = None;
        let mut rows = Vec::<QueryRow>::new();
        let mut last_page_key: Option<Vec<u8>> = None;
        let mut physical = 0usize;
        let mut eligible = 0usize;
        'ranges: for range in schedule.ranges() {
            let Some(window) = range.resume_window(direction, after, false) else {
                continue;
            };
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
                let page = match prefetched_first_page.take() {
                    Some(prefetched) => prefetched,
                    None => self.snapshot.scan_snapshot_index_range(request.clone())?,
                };
                if can_cache_single_page
                    && lower_after.is_none()
                    && page.next_after().is_none()
                    && !page.entries().is_empty()
                {
                    let entries_ptr = page.entries().as_ptr() as usize;
                    let entries_len = page.entries().len();
                    if let Some(hit) = LOCAL_COVERED_SCAN_ROWS_CACHE.with(|cache| {
                        let guard = cache.try_borrow().ok()?;
                        guard
                            .find_by_probe(probe_hash, plan_ptr, page_limit, predicates)
                            .filter(|cached| {
                                cached.entries_ptr == entries_ptr
                                    && cached.entries_len == entries_len
                                    && cached.head == self.head
                                    && cached.epoch == epoch
                                    && &cached.partition_key == target.partition_key()
                            })
                            .and_then(|cached| {
                                QueryScanPage::shared_verified(
                                    Arc::clone(&cached.rows),
                                    epoch,
                                    cached.scanned_rows,
                                    0,
                                    cached.continuation.clone(),
                                )
                            })
                    }) {
                        return Ok(Some(hit));
                    }
                    if let Some(shared_entry) = lookup_shared_covered_scan_rows(
                        probe_hash,
                        entries_ptr,
                        entries_len,
                        plan_ptr,
                        self.head,
                        epoch,
                        page_limit,
                        target.partition_key(),
                        predicates,
                    ) && let Some(hit) = QueryScanPage::shared_verified(
                        Arc::clone(&shared_entry.rows),
                        epoch,
                        shared_entry.scanned_rows,
                        0,
                        shared_entry.continuation.clone(),
                    ) {
                        LOCAL_COVERED_SCAN_ROWS_CACHE.with(|cache| {
                            if let Ok(mut guard) = cache.try_borrow_mut() {
                                guard.insert(shared_entry);
                            }
                        });
                        return Ok(Some(hit));
                    }
                    single_page_pin = Some((request, page.clone(), entries_ptr, entries_len));
                }
                if rows.capacity() == 0 {
                    rows.reserve(page.entries().len().min(fetch_limit).min(scan_ceiling));
                }
                physical = physical
                    .checked_add(page.entries().len())
                    .ok_or_else(limit_exceeded)?;
                for (entry_index, charged) in page.entries().iter().enumerate() {
                    let entry = charged.value();
                    if entry.partition_key() != target.partition_key() {
                        continue;
                    }
                    eligible = eligible.checked_add(1).ok_or_else(limit_exceeded)?;
                    let (values_arc, fields_arc, fields_bytes, projected_fields) = if let Some((
                        cached_values,
                        cached_fields,
                        cached_fields_bytes,
                        cached_projected,
                    )) =
                        page.covered_projection(entry_index, expected_cover_ids, field_names)
                    {
                        if has_lossy_predicate
                            && !covered_row_matches_predicates_at_positions_v1(
                                layout,
                                cached_values,
                                predicates,
                                predicate_positions,
                            )
                            .map_err(|_| invariant())?
                        {
                            continue;
                        }
                        (
                            Arc::clone(cached_values),
                            Arc::clone(cached_fields),
                            cached_fields_bytes,
                            cached_projected.clone(),
                        )
                    } else {
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
                                .zip(expected_cover_ids.iter())
                                .any(|((actual, _), expected)| actual != expected)
                        {
                            return Err(corrupt());
                        }
                        let decoded = schema.decode_index(entry.key()).map_err(|_| corrupt())?;
                        let entity_values = if needs_entity_key {
                            step.internal_entity_key_schema()
                                .decode_entity(decoded.entity_key())
                                .map_err(|_| corrupt())?
                        } else {
                            step.internal_entity_key_schema()
                                .validate_entity(decoded.entity_key())
                                .map_err(|_| corrupt())?;
                            Vec::new()
                        };
                        let values = layout
                            .fields()
                            .iter()
                            .zip(cover_positions)
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
                        let values_arc: Arc<[CanonicalValue]> = values.into();
                        let matches = covered_row_matches_predicates_at_positions_v1(
                            layout,
                            &values_arc,
                            predicates,
                            predicate_positions,
                        )
                        .map_err(|_| invariant())?;
                        if !matches {
                            continue;
                        }
                        let mut fields_map = BTreeMap::new();
                        let mut fields_bytes = 0u64;
                        for (name, value) in field_names.iter().zip(values_arc.iter()) {
                            let encoded_len = riffdb_types::canonical_value_encoded_len(value)
                                .map_err(|_| invariant())?;
                            fields_bytes = fields_bytes
                                .checked_add(name.len() as u64)
                                .and_then(|v| v.checked_add(encoded_len as u64))
                                .and_then(|v| v.checked_add(160))
                                .ok_or_else(limit_exceeded)?;
                            fields_map.insert(Arc::clone(name), value.clone());
                        }
                        if fields_map.len() != field_names.len() {
                            return Err(invariant());
                        }
                        let fields_arc = Arc::new(fields_map);
                        let projected_fields = match &covered_plan.selected_positions {
                            Some(positions) => {
                                let mut proj = BTreeMap::new();
                                let mut proj_bytes = 0u64;
                                for (name, pos) in positions {
                                    if let Some(val) = values_arc.get(*pos) {
                                        let encoded_len =
                                            riffdb_types::canonical_value_encoded_len(val)
                                                .map_err(|_| invariant())?;
                                        proj_bytes = proj_bytes
                                            .checked_add(name.len() as u64)
                                            .and_then(|v| v.checked_add(encoded_len as u64))
                                            .and_then(|v| v.checked_add(160))
                                            .ok_or_else(limit_exceeded)?;
                                        proj.insert(Arc::clone(name), val.clone());
                                    }
                                }
                                Some((Arc::new(proj), proj_bytes))
                            }
                            None => None,
                        };
                        page.cache_covered_projection(
                            entry_index,
                            (
                                Arc::clone(expected_cover_ids),
                                Arc::clone(field_names),
                                Arc::clone(&values_arc),
                                Arc::clone(&fields_arc),
                                fields_bytes,
                                projected_fields.clone(),
                            ),
                        );
                        (values_arc, fields_arc, fields_bytes, projected_fields)
                    };
                    let row = QueryRow::from_materialized_plan(
                        Arc::clone(entity),
                        fields_arc,
                        Some(fields_bytes),
                        Some(values_arc),
                        projected_fields,
                        selected_prevalidated,
                    )
                    .ok_or_else(invariant)?;
                    rows.push(row);
                    if rows.len() == page_limit {
                        last_page_key = Some(entry.key().as_bytes().to_vec());
                    }
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
        let continuation = if has_more { last_page_key } else { None };
        let scanned_rows = u64::try_from(eligible).map_err(|_| limit_exceeded())?;
        if (rows.len() as u64) > scanned_rows {
            return Err(invariant());
        }
        let reported_scanned = if continuation.is_some() {
            scanned_rows.max(1)
        } else {
            scanned_rows
        };
        if let Some((request, page_pin, entries_ptr, entries_len)) = single_page_pin {
            let shared_rows: Arc<[QueryRow]> = rows.into();
            let page = QueryScanPage::shared_verified(
                Arc::clone(&shared_rows),
                epoch,
                reported_scanned,
                0,
                continuation.clone(),
            )
            .ok_or_else(invariant)?;
            let entry = CachedCoveredScanRows {
                _page_pin: page_pin,
                _plan_pin: Arc::clone(&covered_plan),
                probe_hash,
                request,
                index_id,
                partition_value: partition_value.clone(),
                target: target.clone(),
                entries_ptr,
                entries_len,
                plan_ptr,
                head: self.head,
                epoch,
                page_limit,
                partition_key: target.partition_key().clone(),
                predicates: predicates.to_vec(),
                rows: shared_rows,
                scanned_rows: reported_scanned,
                continuation,
            };
            insert_shared_covered_scan_rows(entry.clone());
            LOCAL_COVERED_SCAN_ROWS_CACHE.with(|cache| {
                if let Ok(mut guard) = cache.try_borrow_mut() {
                    guard.insert(entry);
                }
            });
            return Ok(Some(page));
        }
        let mut page = QueryScanPage::reported(rows, epoch, reported_scanned, 0, continuation)
            .ok_or_else(invariant)?;
        page.predicates_verified = true;
        Ok(Some(page))
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
        let plan = self.materialize_plan(step)?;
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
        let plan = self.materialize_plan(step)?;
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
    fn scan_covered_rows(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[BoundPredicate],
        limit: u64,
        after: Option<&[u8]>,
        policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<Option<Option<QueryScanPage>>, Self::Error> {
        self.read_covered_scan_page(step, predicates, limit, after, policy)
            .map(Some)
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

fn partition_value_at<'a>(
    program: &QueryAccessProgramV1,
    step: &QueryAccessStep,
    predicates: &'a [BoundPredicate],
    index: Option<usize>,
) -> Result<&'a CanonicalValue, StorageError> {
    if let Some(idx) = index
        && let Some(predicate) = predicates.get(idx)
        && predicate.operator() == QueryPredicateOperator::Equal
    {
        return Ok(predicate.value());
    }
    predicates
        .iter()
        .find(|predicate| {
            predicate.operator() == QueryPredicateOperator::Equal
                && step.predicates().iter().any(|source| {
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

const LOCAL_SCAN_ROWS_CACHE_BUCKETS: usize = 512;
const LOCAL_SCAN_ROWS_CACHE_WAYS: usize = 8;
const SHARED_SCAN_ROWS_CACHE_SHARDS: usize = 64;
const SHARED_SCAN_ROWS_CACHE_BUCKETS: usize = 64;
const SHARED_SCAN_ROWS_CACHE_WAYS: usize = 4;

#[inline]
const fn mix_hash_u64(h: u64, v: u64) -> u64 {
    (h ^ v).wrapping_mul(0x517cc1b727220a95)
}

#[inline]
fn mix_hash_bytes(mut h: u64, bytes: &[u8]) -> u64 {
    h = mix_hash_u64(h, bytes.len() as u64);
    let mut chunks = bytes.chunks_exact(8);
    for chunk in &mut chunks {
        let mut arr = [0u8; 8];
        arr.copy_from_slice(chunk);
        h = mix_hash_u64(h, u64::from_le_bytes(arr));
    }
    let rem = chunks.remainder();
    if !rem.is_empty() {
        let mut arr = [0u8; 8];
        arr[..rem.len()].copy_from_slice(rem);
        h = mix_hash_u64(h, u64::from_le_bytes(arr));
    }
    h
}

fn hash_canonical_value_fast(mut h: u64, value: &CanonicalValue) -> u64 {
    match value {
        CanonicalValue::Null => mix_hash_u64(h, 0),
        CanonicalValue::Bool(b) => mix_hash_u64(h, 1 ^ u64::from(*b)),
        CanonicalValue::I64(v) => mix_hash_u64(h, 2 ^ (*v as u64)),
        CanonicalValue::U64(v) => mix_hash_u64(h, 3 ^ *v),
        CanonicalValue::Decimal(d) => {
            let c = d.coefficient() as u128;
            h = mix_hash_u64(
                h,
                4 ^ u64::from(d.spec().precision()) ^ (u64::from(d.spec().scale()) << 8),
            );
            h = mix_hash_u64(h, c as u64);
            mix_hash_u64(h, (c >> 64) as u64)
        }
        CanonicalValue::Money(m) => {
            h = mix_hash_bytes(h, m.currency().as_bytes());
            let d = m.amount();
            let c = d.coefficient() as u128;
            h = mix_hash_u64(
                h,
                5 ^ u64::from(d.spec().precision()) ^ (u64::from(d.spec().scale()) << 8),
            );
            h = mix_hash_u64(h, c as u64);
            mix_hash_u64(h, (c >> 64) as u64)
        }
        CanonicalValue::String(s) => mix_hash_bytes(mix_hash_u64(h, 6), s.as_str().as_bytes()),
        CanonicalValue::Bytes(b) => mix_hash_bytes(mix_hash_u64(h, 7), b.as_bytes()),
        CanonicalValue::Timestamp(t) => {
            h = mix_hash_u64(h, 8 ^ (t.seconds() as u64));
            mix_hash_u64(h, u64::from(t.nanoseconds()))
        }
        CanonicalValue::Date(d) => mix_hash_u64(h, 9 ^ (d.days_since_unix_epoch() as u64)),
        CanonicalValue::Uuid(u) => mix_hash_bytes(mix_hash_u64(h, 10), u),
        CanonicalValue::Enum {
            type_id,
            variant_id,
        } => mix_hash_u64(
            h,
            11 ^ (u64::from(type_id.get()) << 32) ^ u64::from(variant_id.get()),
        ),
        CanonicalValue::List(list) => {
            h = mix_hash_u64(h, 12 ^ (list.len() as u64));
            for item in list.values() {
                h = hash_canonical_value_fast(h, item);
            }
            h
        }
        CanonicalValue::Record(rec) => {
            h = mix_hash_u64(h, 13 ^ (rec.len() as u64));
            for (fid, val) in rec.fields() {
                h = mix_hash_u64(h, u64::from(fid.get()));
                h = hash_canonical_value_fast(h, val);
            }
            h
        }
        CanonicalValue::Vector(vec) => {
            h = mix_hash_u64(h, 14 ^ u64::from(vec.dimension()));
            for comp in vec.components() {
                h = mix_hash_u64(h, u64::from(comp.to_bits()));
            }
            h
        }
    }
}

fn hash_scan_probe(plan_ptr: usize, page_limit: usize, predicates: &[BoundPredicate]) -> u64 {
    const K: u64 = 0x517cc1b727220a95;
    let mut h = ((plan_ptr >> 4) as u64).wrapping_mul(K);
    h = (h ^ (page_limit as u64)).wrapping_mul(K);
    h = (h ^ (predicates.len() as u64)).wrapping_mul(K);
    for pred in predicates {
        h = (h ^ (pred.operator() as u64) ^ ((pred.field().len() as u64) << 8)).wrapping_mul(K);
        h = hash_canonical_value_fast(h, pred.value());
    }
    h ^ (h >> 32)
}

trait ScanRowsCacheEntry {
    fn probe_hash(&self) -> u64;
    fn plan_ptr(&self) -> usize;
    fn page_limit(&self) -> usize;
    fn predicates(&self) -> &[BoundPredicate];

    fn matches_probe(
        &self,
        probe_hash: u64,
        plan_ptr: usize,
        page_limit: usize,
        predicates: &[BoundPredicate],
    ) -> bool {
        self.probe_hash() == probe_hash
            && self.plan_ptr() == plan_ptr
            && self.page_limit() == page_limit
            && self.predicates() == predicates
    }
}

#[derive(Clone)]
struct CachedRangeScanRows {
    _page_pin: riffdb_storage_api::SnapshotIndexRangePageV1,
    _plan_pin: Arc<RowMaterializePlan>,
    probe_hash: u64,
    request: SnapshotIndexRangeRequestV1,
    index_id: riffdb_types::IndexId,
    partition_value: CanonicalValue,
    target: PartitionIndexTarget,
    entries_ptr: usize,
    entries_len: usize,
    plan_ptr: usize,
    head: u64,
    epoch: u64,
    page_limit: usize,
    partition_key: riffdb_types::PartitionKey,
    predicates: Vec<BoundPredicate>,
    rows: Arc<[QueryRow]>,
    scanned_rows: u64,
    point_reads: u64,
    continuation: Option<Vec<u8>>,
}

impl ScanRowsCacheEntry for CachedRangeScanRows {
    fn probe_hash(&self) -> u64 {
        self.probe_hash
    }
    fn plan_ptr(&self) -> usize {
        self.plan_ptr
    }
    fn page_limit(&self) -> usize {
        self.page_limit
    }
    fn predicates(&self) -> &[BoundPredicate] {
        &self.predicates
    }
}

#[derive(Clone)]
struct CachedCoveredScanRows {
    _page_pin: riffdb_storage_api::SnapshotIndexRangePageV1,
    _plan_pin: Arc<CoveredScanPlan>,
    probe_hash: u64,
    request: SnapshotIndexRangeRequestV1,
    index_id: riffdb_types::IndexId,
    partition_value: CanonicalValue,
    target: PartitionIndexTarget,
    entries_ptr: usize,
    entries_len: usize,
    plan_ptr: usize,
    head: u64,
    epoch: u64,
    page_limit: usize,
    partition_key: riffdb_types::PartitionKey,
    predicates: Vec<BoundPredicate>,
    rows: Arc<[QueryRow]>,
    scanned_rows: u64,
    continuation: Option<Vec<u8>>,
}

impl ScanRowsCacheEntry for CachedCoveredScanRows {
    fn probe_hash(&self) -> u64 {
        self.probe_hash
    }
    fn plan_ptr(&self) -> usize {
        self.plan_ptr
    }
    fn page_limit(&self) -> usize {
        self.page_limit
    }
    fn predicates(&self) -> &[BoundPredicate] {
        &self.predicates
    }
}

struct BucketedScanRowsCache<T, const BUCKETS: usize, const WAYS: usize> {
    buckets: Vec<Vec<T>>,
}

impl<T, const BUCKETS: usize, const WAYS: usize> Default
    for BucketedScanRowsCache<T, BUCKETS, WAYS>
{
    fn default() -> Self {
        Self {
            buckets: Vec::new(),
        }
    }
}

impl<T: ScanRowsCacheEntry, const BUCKETS: usize, const WAYS: usize>
    BucketedScanRowsCache<T, BUCKETS, WAYS>
{
    fn bucket_index(probe_hash: u64) -> usize {
        ((probe_hash >> 6) as usize) & (BUCKETS - 1)
    }

    fn find_by_probe(
        &self,
        probe_hash: u64,
        plan_ptr: usize,
        page_limit: usize,
        predicates: &[BoundPredicate],
    ) -> Option<&T> {
        if self.buckets.is_empty() {
            return None;
        }
        let bucket = &self.buckets[Self::bucket_index(probe_hash)];
        bucket
            .iter()
            .rev()
            .find(|cached| cached.matches_probe(probe_hash, plan_ptr, page_limit, predicates))
    }

    fn insert(&mut self, entry: T) {
        if self.buckets.is_empty() {
            self.buckets.resize_with(BUCKETS, Vec::new);
        }
        let probe_hash = entry.probe_hash();
        let bucket = &mut self.buckets[Self::bucket_index(probe_hash)];
        if let Some(existing) = bucket.iter_mut().find(|cached| {
            cached.matches_probe(
                probe_hash,
                entry.plan_ptr(),
                entry.page_limit(),
                entry.predicates(),
            )
        }) {
            *existing = entry;
            return;
        }
        if bucket.len() < WAYS {
            bucket.push(entry);
        } else {
            bucket.remove(0);
            bucket.push(entry);
        }
    }
}

type LocalScanRowsCache<T> =
    BucketedScanRowsCache<T, LOCAL_SCAN_ROWS_CACHE_BUCKETS, LOCAL_SCAN_ROWS_CACHE_WAYS>;
type SharedScanRowsCacheShard<T> =
    BucketedScanRowsCache<T, SHARED_SCAN_ROWS_CACHE_BUCKETS, SHARED_SCAN_ROWS_CACHE_WAYS>;

static SHARED_RANGE_SCAN_ROWS_CACHE: std::sync::OnceLock<
    [std::sync::Mutex<SharedScanRowsCacheShard<CachedRangeScanRows>>;
        SHARED_SCAN_ROWS_CACHE_SHARDS],
> = std::sync::OnceLock::new();

static SHARED_COVERED_SCAN_ROWS_CACHE: std::sync::OnceLock<
    [std::sync::Mutex<SharedScanRowsCacheShard<CachedCoveredScanRows>>;
        SHARED_SCAN_ROWS_CACHE_SHARDS],
> = std::sync::OnceLock::new();

fn shared_range_scan_rows_cache()
-> &'static [std::sync::Mutex<SharedScanRowsCacheShard<CachedRangeScanRows>>;
             SHARED_SCAN_ROWS_CACHE_SHARDS] {
    SHARED_RANGE_SCAN_ROWS_CACHE.get_or_init(|| {
        std::array::from_fn(|_| std::sync::Mutex::new(SharedScanRowsCacheShard::default()))
    })
}

fn shared_covered_scan_rows_cache()
-> &'static [std::sync::Mutex<SharedScanRowsCacheShard<CachedCoveredScanRows>>;
             SHARED_SCAN_ROWS_CACHE_SHARDS] {
    SHARED_COVERED_SCAN_ROWS_CACHE.get_or_init(|| {
        std::array::from_fn(|_| std::sync::Mutex::new(SharedScanRowsCacheShard::default()))
    })
}

fn lookup_shared_range_scan_rows_by_probe(
    probe_hash: u64,
    plan_ptr: usize,
    page_limit: usize,
    predicates: &[BoundPredicate],
) -> Option<CachedRangeScanRows> {
    let shard = (probe_hash as usize) & (SHARED_SCAN_ROWS_CACHE_SHARDS - 1);
    let guard = shared_range_scan_rows_cache()[shard].lock().ok()?;
    guard
        .find_by_probe(probe_hash, plan_ptr, page_limit, predicates)
        .cloned()
}

#[allow(clippy::too_many_arguments)]
fn lookup_shared_range_scan_rows(
    probe_hash: u64,
    entries_ptr: usize,
    entries_len: usize,
    plan_ptr: usize,
    head: u64,
    epoch: u64,
    page_limit: usize,
    partition_key: &riffdb_types::PartitionKey,
    predicates: &[BoundPredicate],
) -> Option<CachedRangeScanRows> {
    let shard = (probe_hash as usize) & (SHARED_SCAN_ROWS_CACHE_SHARDS - 1);
    let guard = shared_range_scan_rows_cache()[shard].lock().ok()?;
    guard
        .find_by_probe(probe_hash, plan_ptr, page_limit, predicates)
        .filter(|cached| {
            cached.entries_ptr == entries_ptr
                && cached.entries_len == entries_len
                && cached.head == head
                && cached.epoch == epoch
                && &cached.partition_key == partition_key
        })
        .cloned()
}

fn insert_shared_range_scan_rows(entry: CachedRangeScanRows) {
    let shard = (entry.probe_hash as usize) & (SHARED_SCAN_ROWS_CACHE_SHARDS - 1);
    if let Ok(mut guard) = shared_range_scan_rows_cache()[shard].lock() {
        guard.insert(entry);
    }
}

fn lookup_shared_covered_scan_rows_by_probe(
    probe_hash: u64,
    plan_ptr: usize,
    page_limit: usize,
    predicates: &[BoundPredicate],
) -> Option<CachedCoveredScanRows> {
    let shard = (probe_hash as usize) & (SHARED_SCAN_ROWS_CACHE_SHARDS - 1);
    let guard = shared_covered_scan_rows_cache()[shard].lock().ok()?;
    guard
        .find_by_probe(probe_hash, plan_ptr, page_limit, predicates)
        .cloned()
}

#[allow(clippy::too_many_arguments)]
fn lookup_shared_covered_scan_rows(
    probe_hash: u64,
    entries_ptr: usize,
    entries_len: usize,
    plan_ptr: usize,
    head: u64,
    epoch: u64,
    page_limit: usize,
    partition_key: &riffdb_types::PartitionKey,
    predicates: &[BoundPredicate],
) -> Option<CachedCoveredScanRows> {
    let shard = (probe_hash as usize) & (SHARED_SCAN_ROWS_CACHE_SHARDS - 1);
    let guard = shared_covered_scan_rows_cache()[shard].lock().ok()?;
    guard
        .find_by_probe(probe_hash, plan_ptr, page_limit, predicates)
        .filter(|cached| {
            cached.entries_ptr == entries_ptr
                && cached.entries_len == entries_len
                && cached.head == head
                && cached.epoch == epoch
                && &cached.partition_key == partition_key
        })
        .cloned()
}

fn insert_shared_covered_scan_rows(entry: CachedCoveredScanRows) {
    let shard = (entry.probe_hash as usize) & (SHARED_SCAN_ROWS_CACHE_SHARDS - 1);
    if let Ok(mut guard) = shared_covered_scan_rows_cache()[shard].lock() {
        guard.insert(entry);
    }
}

std::thread_local! {
    static LOCAL_RANGE_SCAN_ROWS_CACHE: std::cell::RefCell<LocalScanRowsCache<CachedRangeScanRows>> =
        std::cell::RefCell::new(LocalScanRowsCache::default());
    static LOCAL_COVERED_SCAN_ROWS_CACHE: std::cell::RefCell<LocalScanRowsCache<CachedCoveredScanRows>> =
        std::cell::RefCell::new(LocalScanRowsCache::default());
}

fn step_has_lossy_index_predicate(step: &QueryAccessStep) -> bool {
    let Some(descriptor) = step.internal_operational_index_descriptor() else {
        return true;
    };
    let fields = match step.access() {
        QueryAccessKind::Index { fields, .. }
        | QueryAccessKind::PartitionSetIndex { fields, .. }
        | QueryAccessKind::ExpansionIndex { fields, .. } => fields,
        _ => return true,
    };
    step.predicates().iter().any(|pred| {
        pred.operator() == QueryPredicateOperator::NotEqual
            || !fields.iter().any(|field| field == pred.field())
    }) || fields.iter().enumerate().any(|(pos, field)| {
        descriptor.component(pos, field).is_none_or(|comp| {
            matches!(
                comp.encoding(),
                riffdb_contract_ir::IndexFieldEncodingV1::TextKey(
                    riffdb_contract_ir::TextKeyProfileV1::UnicodeFold
                )
            )
        })
    })
}

type NeededFieldsSlice = Arc<[(FieldId, Arc<str>)]>;
type EntityNeededSpec = (Arc<str>, NeededFieldsSlice);

struct RowMaterializePlan {
    entity: Arc<str>,
    needed: NeededFieldsSlice,
    selected_needed: Option<NeededFieldsSlice>,
    selected_prevalidated: bool,
    point_key_predicate_indices: Option<Vec<usize>>,
    partition_predicate_index: Option<usize>,
    predicate_positions: Option<Vec<usize>>,
    has_lossy_index_predicate: bool,
}

fn find_partition_predicate_index(
    program: &QueryAccessProgramV1,
    step: &QueryAccessStep,
) -> Option<usize> {
    step.predicates().iter().position(|source| {
        source.operator() == QueryPredicateOperator::Equal
            && matches!(
                source.value(),
                riffdb_query_ir::QueryPredicateValue::Parameter(name)
                    if name == program.partition_parameter()
            )
    })
}

impl RowMaterializePlan {
    fn build_entity_needed(
        program: &QueryAccessProgramV1,
        entity_name: &str,
    ) -> Result<EntityNeededSpec, StorageError> {
        let access = program
            .internal_entity_access(entity_name)
            .ok_or_else(invariant)?;
        let mut needed = access
            .internal_fields()
            .map(|(name, id)| (id, Arc::<str>::from(name)))
            .collect::<Vec<_>>();
        needed.sort_by_key(|(id, _)| id.get());
        Ok((Arc::<str>::from(entity_name), Arc::from(needed)))
    }

    fn for_step(
        program: &QueryAccessProgramV1,
        step: &QueryAccessStep,
    ) -> Result<Self, StorageError> {
        let (entity, needed) = Self::build_entity_needed(program, step.entity())?;
        Ok(Self::for_step_with_needed(program, step, entity, needed))
    }

    fn for_step_with_needed(
        program: &QueryAccessProgramV1,
        step: &QueryAccessStep,
        entity: Arc<str>,
        needed: Arc<[(FieldId, Arc<str>)]>,
    ) -> Self {
        let mut selected_pairs = Vec::with_capacity(step.selected_fields().len());
        let mut selected_valid = !entity.is_empty()
            && step.selected_fields().len() <= crate::MAX_QUERY_ROW_FIELDS
            && needed.len() <= crate::MAX_QUERY_ROW_FIELDS;
        if selected_valid {
            for name in step.selected_fields() {
                if name.is_empty() {
                    selected_valid = false;
                    break;
                }
                if let Some((id, arc_name)) = needed
                    .iter()
                    .find(|(_, candidate)| candidate.as_ref() == name.as_str())
                {
                    selected_pairs.push((*id, Arc::clone(arc_name)));
                } else {
                    selected_valid = false;
                    break;
                }
            }
        }
        selected_pairs.sort_by_key(|(id, _)| id.get());
        selected_pairs.dedup_by_key(|(id, _)| id.get());
        let selected_prevalidated =
            selected_valid && selected_pairs.len() == step.selected_fields().len();
        let output_selected = step_output_selected_fields_v1(program, step);
        let selected_needed = if selected_prevalidated && !step.result_names().is_empty() {
            if let Some(output_fields) = output_selected {
                let mut out_pairs = Vec::with_capacity(output_fields.len());
                for name in &output_fields {
                    if let Some((id, arc_name)) = needed
                        .iter()
                        .find(|(_, candidate)| candidate.as_ref() == name.as_str())
                    {
                        out_pairs.push((*id, Arc::clone(arc_name)));
                    }
                }
                out_pairs.sort_by_key(|(id, _)| id.get());
                out_pairs.dedup_by_key(|(id, _)| id.get());
                if out_pairs.len() == output_fields.len() && out_pairs.len() < needed.len() {
                    Some(Arc::from(out_pairs))
                } else {
                    None
                }
            } else if selected_pairs.len() < needed.len() {
                Some(Arc::from(selected_pairs))
            } else {
                None
            }
        } else {
            None
        };
        let point_key_predicate_indices = match step.access() {
            QueryAccessKind::Point { key_fields }
            | QueryAccessKind::DependentPointBatch { key_fields, .. }
            | QueryAccessKind::CandidateRootHydration { key_fields, .. } => {
                let mut indices = Vec::with_capacity(key_fields.len());
                let mut all_found = true;
                for key_field in key_fields {
                    if let Some(pos) = step.predicates().iter().position(|pred| {
                        pred.field() == key_field
                            && matches!(
                                pred.operator(),
                                QueryPredicateOperator::Equal | QueryPredicateOperator::In
                            )
                    }) {
                        indices.push(pos);
                    } else {
                        all_found = false;
                        break;
                    }
                }
                all_found.then_some(indices)
            }
            _ => None,
        };
        let partition_predicate_index = find_partition_predicate_index(program, step);
        let mut sorted_names: Vec<&str> = needed.iter().map(|(_, name)| name.as_ref()).collect();
        sorted_names.sort_unstable();
        let predicate_positions = step
            .predicates()
            .iter()
            .map(|pred| sorted_names.binary_search(&pred.field()).ok())
            .collect::<Option<Vec<usize>>>();
        let has_lossy_index_predicate = step_has_lossy_index_predicate(step);
        Self {
            entity,
            needed,
            selected_needed,
            selected_prevalidated,
            point_key_predicate_indices,
            partition_predicate_index,
            predicate_positions,
            has_lossy_index_predicate,
        }
    }

    fn materialize(
        &self,
        record: &riffdb_storage_api::StoredEntityRecordV1,
    ) -> Result<QueryRow, StorageError> {
        let (fields, values, fields_bytes) = record
            .materialize_projected_fields_with_values(&self.needed)
            .ok_or_else(corrupt)?;
        let projected_fields = match &self.selected_needed {
            Some(selected) => Some(
                record
                    .materialize_projected_fields(selected)
                    .ok_or_else(corrupt)?,
            ),
            None => None,
        };
        QueryRow::from_materialized_plan(
            Arc::clone(&self.entity),
            fields,
            Some(fields_bytes),
            Some(values),
            projected_fields,
            self.selected_prevalidated,
        )
        .ok_or_else(limit_exceeded)
    }
}

const MAX_CACHED_STORAGE_PROGRAM_PLANS: usize = 256;

struct CoveredScanPlan {
    expected_cover_ids: Arc<[FieldId]>,
    entity: Arc<str>,
    field_names: Arc<[Arc<str>]>,
    rows_valid: bool,
    selected_prevalidated: bool,
    selected_positions: Option<Vec<(Arc<str>, usize)>>,
    predicate_positions: Option<Vec<usize>>,
    needs_entity_key: bool,
    cover_positions: Vec<usize>,
    partition_predicate_index: Option<usize>,
    has_lossy_index_predicate: bool,
}

impl CoveredScanPlan {
    fn for_step(
        program: &QueryAccessProgramV1,
        step: &QueryAccessStep,
    ) -> Result<Option<Self>, StorageError> {
        let Some(layout) = step.covered_result_layout() else {
            return Ok(None);
        };
        let mut expected_cover_ids_vec = layout.internal_cover_field_ids().to_vec();
        expected_cover_ids_vec.sort_unstable();
        let expected_cover_ids: Arc<[FieldId]> = expected_cover_ids_vec.into();
        let entity = Arc::<str>::from(layout.entity());
        let field_names: Arc<[Arc<str>]> = layout
            .fields()
            .iter()
            .map(|field| Arc::<str>::from(field.name()))
            .collect::<Vec<_>>()
            .into();
        let rows_valid = !entity.is_empty()
            && field_names.len() <= crate::MAX_QUERY_ROW_FIELDS
            && !field_names.iter().any(|name| name.is_empty());
        let mut selected_pairs = Vec::with_capacity(step.selected_fields().len());
        let mut selected_valid =
            rows_valid && step.selected_fields().len() <= crate::MAX_QUERY_ROW_FIELDS;
        if selected_valid {
            for (idx, name) in step.selected_fields().iter().enumerate() {
                if name.is_empty() || step.selected_fields()[..idx].contains(name) {
                    selected_valid = false;
                    break;
                }
                if let Some((pos, arc_name)) = field_names
                    .iter()
                    .enumerate()
                    .find(|(_, f)| f.as_ref() == name.as_str())
                {
                    selected_pairs.push((Arc::clone(arc_name), pos));
                } else {
                    selected_valid = false;
                    break;
                }
            }
        }
        let selected_prevalidated =
            selected_valid && selected_pairs.len() == step.selected_fields().len();
        let output_selected = step_output_selected_fields_v1(program, step);
        let selected_positions = if selected_prevalidated && !step.result_names().is_empty() {
            if let Some(output_fields) = output_selected {
                let mut out_pairs = Vec::with_capacity(output_fields.len());
                for name in &output_fields {
                    if let Some((pos, arc_name)) = field_names
                        .iter()
                        .enumerate()
                        .find(|(_, f)| f.as_ref() == name.as_str())
                    {
                        out_pairs.push((Arc::clone(arc_name), pos));
                    }
                }
                if out_pairs.len() == output_fields.len() && out_pairs.len() < field_names.len() {
                    Some(out_pairs)
                } else {
                    None
                }
            } else if selected_pairs.len() < field_names.len() {
                Some(selected_pairs)
            } else {
                None
            }
        } else {
            None
        };
        let predicate_positions = step
            .predicates()
            .iter()
            .map(|pred| {
                layout
                    .fields()
                    .binary_search_by(|field| field.name().cmp(pred.field()))
                    .ok()
            })
            .collect::<Option<Vec<usize>>>();
        let needs_entity_key = layout
            .fields()
            .iter()
            .any(|field| matches!(field.internal_source(), CoveredResultSourceV1::EntityKey(_)));
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
        let partition_predicate_index = find_partition_predicate_index(program, step);
        let has_lossy_index_predicate = step_has_lossy_index_predicate(step);
        Ok(Some(Self {
            expected_cover_ids,
            entity,
            field_names,
            rows_valid,
            selected_prevalidated,
            selected_positions,
            predicate_positions,
            needs_entity_key,
            cover_positions,
            partition_predicate_index,
            has_lossy_index_predicate,
        }))
    }
}

struct PreparedStorageStepPlan {
    binding: String,
    entity: String,
    materialize_plan: Option<Arc<RowMaterializePlan>>,
    covered_plan: Option<Arc<CoveredScanPlan>>,
}

struct PreparedStorageProgramPlans {
    steps: Vec<PreparedStorageStepPlan>,
}

fn prepared_storage_program_plans(
    program: &QueryAccessProgramV1,
) -> Arc<PreparedStorageProgramPlans> {
    std::thread_local! {
        static CACHE: std::cell::RefCell<BTreeMap<QueryPlanIdentity, Arc<PreparedStorageProgramPlans>>> =
            const { std::cell::RefCell::new(BTreeMap::new()) };
    }
    static SHARED_CACHE: std::sync::OnceLock<
        std::sync::RwLock<BTreeMap<QueryPlanIdentity, Arc<PreparedStorageProgramPlans>>>,
    > = std::sync::OnceLock::new();
    let identity = program.identity();
    if let Some(plan) = CACHE.with(|cache| cache.try_borrow().ok()?.get(&identity).cloned()) {
        return plan;
    }
    let shared = SHARED_CACHE.get_or_init(|| std::sync::RwLock::new(BTreeMap::new()));
    if let Ok(guard) = shared.read()
        && let Some(plan) = guard.get(&identity).cloned()
    {
        drop(guard);
        CACHE.with(|cache| {
            if let Ok(mut local) = cache.try_borrow_mut() {
                if local.len() >= MAX_CACHED_STORAGE_PROGRAM_PLANS {
                    local.clear();
                }
                local.insert(identity, Arc::clone(&plan));
            }
        });
        return plan;
    }
    let mut entity_needed: BTreeMap<String, EntityNeededSpec> = BTreeMap::new();
    let steps = program
        .steps()
        .iter()
        .map(|step| {
            let materialize_plan = if let Some((entity, needed)) = entity_needed.get(step.entity())
            {
                Some(Arc::new(RowMaterializePlan::for_step_with_needed(
                    program,
                    step,
                    Arc::clone(entity),
                    Arc::clone(needed),
                )))
            } else if let Ok((entity, needed)) =
                RowMaterializePlan::build_entity_needed(program, step.entity())
            {
                entity_needed.insert(
                    step.entity().to_owned(),
                    (Arc::clone(&entity), Arc::clone(&needed)),
                );
                Some(Arc::new(RowMaterializePlan::for_step_with_needed(
                    program, step, entity, needed,
                )))
            } else {
                None
            };
            PreparedStorageStepPlan {
                binding: step.binding().to_owned(),
                entity: step.entity().to_owned(),
                materialize_plan,
                covered_plan: CoveredScanPlan::for_step(program, step)
                    .ok()
                    .flatten()
                    .map(Arc::new),
            }
        })
        .collect();
    let mut prepared = Arc::new(PreparedStorageProgramPlans { steps });
    if let Ok(mut guard) = shared.write() {
        if let Some(existing) = guard.get(&identity) {
            prepared = Arc::clone(existing);
        } else {
            if guard.len() >= MAX_CACHED_STORAGE_PROGRAM_PLANS {
                guard.clear();
            }
            guard.insert(identity, Arc::clone(&prepared));
        }
    }
    CACHE.with(|cache| {
        if let Ok(mut guard) = cache.try_borrow_mut() {
            if guard.len() >= MAX_CACHED_STORAGE_PROGRAM_PLANS {
                guard.clear();
            }
            guard.insert(identity, Arc::clone(&prepared));
        }
    });
    prepared
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
    // Release query-local lookup scratch while its construction reservation
    // still covers it. Settlement retains only the returned admission proof.
    drop(relationship_cache);
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
