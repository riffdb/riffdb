//! One-lock owned composite-query snapshots for the memory reference backend.

#[cfg(test)]
use std::cell::Cell;
use std::collections::BTreeMap;
use std::sync::Arc;

use riffdb_policy::{
    AuthorizedIndexedRelationshipLookupV1, AuthorizedProjectedRowAdmissionV1,
    AuthorizedQueryRowPolicyContextV1, MAX_PROJECTED_POLICY_CANDIDATES_V1,
    ProjectedPolicyCandidateObservationV1,
};
use riffdb_query_executor::{
    BoundPredicate, LongPatternCandidateBatch, QueryBackendFault, QueryContinuation,
    QueryExecutionError, QueryExecutionPort, QueryExecutionRequest, QueryNearestPage,
    QueryOwnedSnapshot, QueryParameters, QueryReadView, QueryRow, QueryScanPage,
    execute_in_snapshot, execute_operational_page_in_snapshot, execute_page_in_snapshot,
    execute_policy_operational_page_in_snapshot, execute_policy_page_in_snapshot,
    execute_policy_provider_page_in_snapshot, execute_provider_page_in_snapshot,
    validate_query_execution_group,
};
use riffdb_query_ir::{
    AccessDirection, OperationalAggregateV1, QueryAccessKind, QueryAccessProgramV1,
    QueryAccessStep, QueryPredicateOperator,
};
use riffdb_storage_api::{EntityTarget, PartitionIndexTarget, StorageError, StorageErrorKind};
use riffdb_types::{
    ApplicationRoleHash, CanonicalValue, EntityKey, EntityTypeId, FieldId, IndexEntryKey,
};

use crate::state::{MemoryIndexEntry, MemoryState, unique_binary_search_by};
use crate::store::{MemoryOperationalPorts, storage_error};

impl QueryExecutionPort for MemoryOperationalPorts {
    fn execute_query_page(
        &self,
        program: &QueryAccessProgramV1,
        parameters: &QueryParameters,
        prior: Option<&QueryContinuation>,
    ) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
        self.read(|state| {
            let mut view = MemoryQueryView {
                state,
                program,
                parameters,
            };
            Ok(execute_page_in_snapshot(
                program, parameters, prior, &mut view,
            ))
        })
        .map_err(map_storage_query_error)?
    }

    fn execute_query_group(
        &self,
        requests: &[QueryExecutionRequest<'_>],
    ) -> Result<Vec<QueryOwnedSnapshot>, QueryExecutionError> {
        validate_query_execution_group(requests)?;
        self.read(|state| {
            Ok(requests
                .iter()
                .map(|request| {
                    let mut view = MemoryQueryView {
                        state,
                        program: request.program(),
                        parameters: request.parameters(),
                    };
                    execute_in_snapshot(request.program(), request.parameters(), &mut view)
                })
                .collect::<Result<Vec<_>, _>>())
        })
        .map_err(map_storage_query_error)?
    }

    fn execute_policy_query_group(
        &self,
        requests: &[QueryExecutionRequest<'_>],
        policy: &AuthorizedQueryRowPolicyContextV1,
    ) -> Result<Vec<QueryOwnedSnapshot>, QueryExecutionError> {
        validate_query_execution_group(requests)?;
        self.read(|state| {
            Ok(requests
                .iter()
                .map(|request| {
                    let mut view = MemoryQueryView {
                        state,
                        program: request.program(),
                        parameters: request.parameters(),
                    };
                    execute_policy_page_in_snapshot(
                        request.program(),
                        request.parameters(),
                        None,
                        &mut view,
                        policy,
                    )
                })
                .collect::<Result<Vec<_>, _>>())
        })
        .map_err(map_storage_query_error)?
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
        self.read(|state| {
            let mut observations = Vec::with_capacity(candidates.len());
            for key in candidates {
                let target = EntityTarget::new(entity, key.clone())
                    .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
                let record = match unique_binary_search_by(&state.entities, |record| {
                    record.target().cmp(&target)
                })? {
                    Ok(index) => &state.entities[index],
                    Err(_) => {
                        observations
                            .push(ProjectedPolicyCandidateObservationV1::missing(key.clone()));
                        continue;
                    }
                };
                let lookups = policy
                    .relationship_lookups(entity, record.fields())
                    .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
                let evidence = lookups
                    .iter()
                    .map(|lookup| indexed_relationship_exists(state, lookup))
                    .collect::<Result<Vec<_>, _>>()?;
                observations.push(ProjectedPolicyCandidateObservationV1::current(
                    key.clone(),
                    record.fields().clone(),
                    evidence,
                ));
            }
            policy
                .authorize_projected_candidates(entity, observations)
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))
        })
        .map_err(map_storage_query_error)
    }

    fn execute_operational_query_page(
        &self,
        program: &QueryAccessProgramV1,
        aggregates: &[OperationalAggregateV1],
        parameters: &QueryParameters,
        prior: Option<&QueryContinuation>,
    ) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
        self.read(|state| {
            let mut view = MemoryQueryView {
                state,
                program,
                parameters,
            };
            Ok(execute_operational_page_in_snapshot(
                program, aggregates, parameters, prior, &mut view,
            ))
        })
        .map_err(map_storage_query_error)?
    }

    fn execute_policy_query_page(
        &self,
        program: &QueryAccessProgramV1,
        parameters: &QueryParameters,
        prior: Option<&QueryContinuation>,
        policy: &AuthorizedQueryRowPolicyContextV1,
    ) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
        self.read(|state| {
            let mut view = MemoryQueryView {
                state,
                program,
                parameters,
            };
            Ok(execute_policy_page_in_snapshot(
                program, parameters, prior, &mut view, policy,
            ))
        })
        .map_err(map_storage_query_error)?
    }

    fn execute_policy_operational_query_page(
        &self,
        program: &QueryAccessProgramV1,
        aggregates: &[OperationalAggregateV1],
        parameters: &QueryParameters,
        prior: Option<&QueryContinuation>,
        policy: &AuthorizedQueryRowPolicyContextV1,
    ) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
        self.read(|state| {
            let mut view = MemoryQueryView {
                state,
                program,
                parameters,
            };
            Ok(execute_policy_operational_page_in_snapshot(
                program, aggregates, parameters, prior, &mut view, policy,
            ))
        })
        .map_err(map_storage_query_error)?
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
        self.read(|state| {
            let mut view = MemoryQueryView {
                state,
                program,
                parameters,
            };
            Ok(execute_provider_page_in_snapshot(
                program,
                parameters,
                prior,
                &mut view,
                policy_shape,
                proof,
                batches,
            ))
        })
        .map_err(map_storage_query_error)?
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
        self.read(|state| {
            let mut view = MemoryQueryView {
                state,
                program,
                parameters,
            };
            Ok(execute_policy_provider_page_in_snapshot(
                program,
                parameters,
                prior,
                &mut view,
                policy,
                policy_shape,
                proof,
                batches,
            ))
        })
        .map_err(map_storage_query_error)?
    }
}

fn map_storage_query_error(error: StorageError) -> QueryExecutionError {
    match storage_query_fault(&error) {
        QueryBackendFault::Unavailable => QueryExecutionError::BackendUnavailable,
        QueryBackendFault::Integrity => QueryExecutionError::BackendIntegrity,
        QueryBackendFault::LimitExceeded => QueryExecutionError::BackendLimitExceeded,
    }
}

const fn storage_query_fault(error: &StorageError) -> QueryBackendFault {
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

struct MemoryQueryView<'a> {
    state: &'a MemoryState,
    program: &'a QueryAccessProgramV1,
    #[allow(dead_code)]
    parameters: &'a QueryParameters,
}

impl QueryReadView for MemoryQueryView<'_> {
    type Error = StorageError;

    fn fault(&self, error: &Self::Error) -> QueryBackendFault {
        storage_query_fault(error)
    }

    fn application_head(&self) -> u64 {
        self.state
            .commits
            .last()
            .map_or(0, |commit| commit.commit_sequence().get())
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
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        // One plan for the whole batch (not per predicate/row).
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
        let direction = match step.access() {
            QueryAccessKind::Index { direction, .. }
            | QueryAccessKind::PartitionSetIndex { direction, .. }
            | QueryAccessKind::ExpansionIndex { direction, .. } => direction,
            _ => return Err(storage_error(StorageErrorKind::InvariantViolation)),
        };
        let schema = step
            .internal_index_key_schema()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        let schedule = riffdb_query_executor::bound_index_range_schedule_v1(step, predicates)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let partition_value = predicates
            .iter()
            .find(|predicate| {
                predicate.operator() == riffdb_query_ir::QueryPredicateOperator::Equal
            })
            .filter(|predicate| {
                step.predicates().iter().any(|source| {
                    source.field() == predicate.field()
                        && matches!(
                            source.value(),
                            riffdb_query_ir::QueryPredicateValue::Parameter(name)
                                if name == self.program.partition_parameter()
                        )
                })
            })
            .map(BoundPredicate::value)
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        let partition = step
            .internal_partition_key_schema()
            .encode_partition(std::slice::from_ref(partition_value))
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let target = PartitionIndexTarget::new(
            partition,
            step.internal_index_id()
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?,
        );
        let epoch = self
            .state
            .index_epochs
            .iter()
            .find(|row| row.target() == &target)
            .map_or(0, |row| row.epoch().get());

        let page_limit =
            usize::try_from(limit).map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
        let fetch_limit = page_limit.saturating_add(1);
        let mut entries = Vec::<(IndexEntryKey, QueryRow)>::new();
        let mut inspected = 0usize;
        let scan_ceiling = usize::try_from(riffdb_query_executor::MAX_QUERY_SCANNED_ROWS)
            .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
        let plan = RowMaterializePlan::for_step(self.program, step)?;
        'ranges: for range in schedule.ranges() {
            let Some(window) = range.resume_window(*direction, after, after_inclusive) else {
                continue;
            };
            let start = self.state.index_entries.partition_point(|entry| {
                entry.key().as_bytes() < window.start_inclusive()
                    || (window.skip_start_equal()
                        && entry.key().as_bytes() == window.start_inclusive())
            });
            let end = self.state.index_entries.partition_point(|entry| {
                entry.key().as_bytes() < window.end_exclusive()
                    || (window.include_end_equal()
                        && entry.key().as_bytes() == window.end_exclusive())
            });
            let matching = self
                .state
                .index_entries
                .get(start..end)
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
            match direction {
                AccessDirection::Forward => {
                    for entry in matching {
                        checked_current(entry)?;
                        if entry
                            .current_record()
                            .is_none_or(|current| current.partition_key() != target.partition_key())
                        {
                            continue;
                        }
                        inspected = inspected
                            .checked_add(1)
                            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
                        if inspected > scan_ceiling {
                            return Err(storage_error(StorageErrorKind::LimitExceeded));
                        }
                        let row = self.materialize_policy_index_row(
                            step,
                            schema,
                            entry.key(),
                            &plan,
                            policy,
                        )?;
                        let Some(row) = row else {
                            continue;
                        };
                        entries.push((entry.key().clone(), row));
                        if entries.len() == fetch_limit {
                            break 'ranges;
                        }
                    }
                }
                AccessDirection::Reverse => {
                    for entry in matching.iter().rev() {
                        checked_current(entry)?;
                        if entry
                            .current_record()
                            .is_none_or(|current| current.partition_key() != target.partition_key())
                        {
                            continue;
                        }
                        inspected = inspected
                            .checked_add(1)
                            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
                        if inspected > scan_ceiling {
                            return Err(storage_error(StorageErrorKind::LimitExceeded));
                        }
                        let row = self.materialize_policy_index_row(
                            step,
                            schema,
                            entry.key(),
                            &plan,
                            policy,
                        )?;
                        let Some(row) = row else {
                            continue;
                        };
                        entries.push((entry.key().clone(), row));
                        if entries.len() == fetch_limit {
                            break 'ranges;
                        }
                    }
                }
            }
        }
        // Continuation only when an extra matching entry was observed. Bound is
        // the last included key; the peeked row is never returned.
        // Charge the peeked row to scanned_rows so fuel accounts for the extra
        // observation that decides continuation minting.
        let has_more = entries.len() > page_limit;
        if has_more {
            entries.truncate(page_limit);
        }
        let scanned_rows =
            u64::try_from(inspected).map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
        let continuation = has_more
            .then(|| entries.last().map(|entry| entry.0.as_bytes().to_vec()))
            .flatten();
        let rows = entries.into_iter().map(|(_, row)| row).collect();
        match continuation {
            Some(continuation) => {
                QueryScanPage::continued(rows, epoch, scanned_rows.max(1), continuation)
                    .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))
            }
            None => QueryScanPage::policy_exact_end(rows, epoch, scanned_rows)
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation)),
        }
    }

    fn nearest(
        &mut self,
        _step: &QueryAccessStep,
        _predicates: &[BoundPredicate],
        _k: u32,
        _policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<QueryNearestPage, Self::Error> {
        // Row-store does not support vector nearest-neighbor search (ADR-0091).
        // Nearest queries must be routed through the columnar projection engine.
        Err(storage_error(StorageErrorKind::InvariantViolation))
    }
}

impl MemoryQueryView<'_> {
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
            QueryAccessKind::Index { .. }
            | QueryAccessKind::ExpansionIndex { .. }
            | QueryAccessKind::PartitionSetIndex { .. }
            | QueryAccessKind::LongPatternCandidate { .. } => {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            QueryAccessKind::Nearest { .. } => {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
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
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let target = EntityTarget::new(step.internal_entity_id(), key)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        match unique_binary_search_by(&self.state.entities, |record| record.target().cmp(&target))?
        {
            Ok(index) => {
                let record = &self.state.entities[index];
                if !self.allows_policy_record(policy, step.internal_entity_id(), record.fields())? {
                    return Ok(None);
                }
                plan.materialize(record).map(Some)
            }
            Err(_) => Ok(None),
        }
    }

    fn materialize_policy_index_row(
        &self,
        step: &QueryAccessStep,
        schema: &riffdb_contract_ir::KeySchema,
        key: &IndexEntryKey,
        plan: &RowMaterializePlan,
        policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<Option<QueryRow>, StorageError> {
        let decoded = schema
            .decode_index(key)
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
        let target = EntityTarget::new(step.internal_entity_id(), decoded.entity_key().clone())
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
        let index =
            unique_binary_search_by(&self.state.entities, |record| record.target().cmp(&target))?
                .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
        let record = &self.state.entities[index];
        if !self.allows_policy_record(policy, step.internal_entity_id(), record.fields())? {
            return Ok(None);
        }
        plan.materialize(record).map(Some)
    }

    fn allows_policy_record(
        &self,
        policy: Option<&AuthorizedQueryRowPolicyContextV1>,
        entity: riffdb_types::EntityTypeId,
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
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let evidence = lookups
            .iter()
            .map(|lookup| self.indexed_relationship_exists(lookup))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(policy.allows(entity, row, &evidence))
    }

    fn indexed_relationship_exists(
        &self,
        lookup: &AuthorizedIndexedRelationshipLookupV1,
    ) -> Result<bool, StorageError> {
        indexed_relationship_exists(self.state, lookup)
    }
}

fn indexed_relationship_exists(
    state: &MemoryState,
    lookup: &AuthorizedIndexedRelationshipLookupV1,
) -> Result<bool, StorageError> {
    let upper = exclusive_prefix_end(lookup.index_prefix())
        .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
    let start = state
        .index_entries
        .partition_point(|entry| entry.key().as_bytes() < lookup.index_prefix());
    let end = state
        .index_entries
        .partition_point(|entry| entry.key().as_bytes() < upper.as_slice());
    let matching = state
        .index_entries
        .get(start..end)
        .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
    if matching.len() >= riffdb_query_executor::MAX_QUERY_SCANNED_ROWS as usize {
        return Err(storage_error(StorageErrorKind::LimitExceeded));
    }
    for entry in matching {
        checked_current(entry)?;
        if entry
            .current_record()
            .is_some_and(|current| current.partition_key() == lookup.partition())
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn exclusive_prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    let position = upper.iter().rposition(|byte| *byte != u8::MAX)?;
    upper[position] = upper[position].saturating_add(1);
    upper.truncate(position + 1);
    Some(upper)
}

fn checked_current(entry: &MemoryIndexEntry) -> Result<(), StorageError> {
    if entry.current_record().is_none() {
        Err(storage_error(StorageErrorKind::IncompatibleFormat))
    } else {
        Ok(())
    }
}

fn exact_value(predicates: &[BoundPredicate], field: &str) -> Result<CanonicalValue, StorageError> {
    predicates
        .iter()
        .find(|predicate| {
            predicate.field() == field && predicate.operator() == QueryPredicateOperator::Equal
        })
        .map(|predicate| predicate.value().clone())
        .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))
}

/// Per-step field-name interning for one-pass row materialization (memory parity).
///
/// Callers build this once per access step (scan loop, single point, or whole
/// dependent-point batch) and share it across every row of that step.
struct RowMaterializePlan {
    entity: Arc<str>,
    needed: Vec<(FieldId, Arc<str>)>,
}

impl RowMaterializePlan {
    fn for_step(
        program: &QueryAccessProgramV1,
        step: &QueryAccessStep,
    ) -> Result<Self, StorageError> {
        note_materialize_plan_build();
        let access = program
            .internal_entity_access(step.entity())
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
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
        // Merge requires needed FieldIds unique (name uniqueness is plan-checked;
        // duplicate FieldIds would yield CorruptData rather than last-wins map).
        for (need_id, name) in &self.needed {
            while store_index < stored.len() && stored[store_index].0.get() < need_id.get() {
                store_index += 1;
            }
            if store_index >= stored.len() || stored[store_index].0 != *need_id {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            fields.insert(Arc::clone(name), stored[store_index].1.clone());
            store_index += 1;
        }
        QueryRow::from_shared(Arc::clone(&self.entity), fields)
            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))
    }
}

#[cfg(test)]
thread_local! {
    static MATERIALIZE_PLAN_BUILDS: Cell<u64> = const { Cell::new(0) };
}

#[cfg(test)]
fn note_materialize_plan_build() {
    MATERIALIZE_PLAN_BUILDS.set(MATERIALIZE_PLAN_BUILDS.get() + 1);
}

#[cfg(not(test))]
const fn note_materialize_plan_build() {}

#[cfg(test)]
fn reset_materialize_plan_builds() {
    MATERIALIZE_PLAN_BUILDS.set(0);
}

#[cfg(test)]
fn materialize_plan_builds() -> u64 {
    MATERIALIZE_PLAN_BUILDS.get()
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use riffdb_auth::PrincipalFactBindingV1;
    use riffdb_contract_compiler::compile_contract_source;
    use riffdb_contract_ir::{
        RowPolicyExpressionNodeV1, RowPolicyOperandV1, RowPolicyOperationV1, RowPolicyPlanV1,
        RowPolicyRuleV1, RowPolicyValueSourceV1, ValueType,
    };
    use riffdb_query_compiler::compile_query;
    use riffdb_query_executor::{
        QueryContinuation, QueryExecutionRequest, QueryParameters, QueryResultValue,
        execute_in_snapshot, execute_page_in_snapshot, execute_policy_page_in_snapshot,
    };
    use riffdb_query_ir::SymbolicCatalog;
    use riffdb_riffql_syntax::parse_query;
    use riffdb_storage_api::{
        DurableKeySchemaBindingV1, EncodedContentCharge, EntityTarget, StoredEntityRecordV1,
        StoredIndexEntryV2, encode_index_entry_v2,
    };
    use riffdb_types::{
        ActorId, ActorKind, AggregateTypeId, Audience, CanonicalRecord, CanonicalValue,
        CapabilityId, CapabilityPrincipalFactsV1, DatabaseId, EntityVersion, Environment,
        PartitionKeyBuilder, TenantScope, Timestamp,
    };

    use super::*;
    use crate::startup::MemoryDormantPorts;
    use crate::store::MemoryStore;

    const CONTRACT: &str = include_str!("../../../examples/app-baseline/contracts/ticketdesk.riff");
    const POINT_QUERY: &str = r#"
query PointTicket(
    $organization_id: Organization.organization_id,
    $ticket_id: Ticket.ticket_id,
) {
    one ticket from Ticket
        where organization_id == $organization_id && ticket_id == $ticket_id
        else NotFound
    return Found { ticket: ticket { ticket_id title } }
    outcomes Found | NotFound
}
"#;
    const MEMBERS_QUERY: &str = r#"
query ProjectMembers(
    $organization_id: Organization.organization_id,
    $project_id: Project.project_id,
    $after: Cursor?,
) {
    many memberships from ProjectMember
        where organization_id == $organization_id && project_id == $project_id
        order by user_id asc
        take 1 after $after
    return Found { members: memberships { user_id role } }
    outcomes Found
}
"#;
    const MEMBERS_RANGE_QUERY: &str = r#"
query ProjectMembersInRange(
    $organization_id: Organization.organization_id,
    $project_id: Project.project_id,
    $lower: ProjectMember.user_id,
    $upper: ProjectMember.user_id,
    $after: Cursor?,
) {
    many memberships from ProjectMember
        where organization_id == $organization_id && project_id == $project_id
          && user_id >= $lower && user_id < $upper
        order by user_id asc
        take 1 after $after
    return Found { members: memberships { user_id role } }
    outcomes Found
}
"#;
    const BINARY_TEXT_INTERVAL_CONTRACT: &str =
        include_str!("../../../fixtures/riffql/wp700-binary-text-interval-contract.riff");
    const BINARY_TEXT_INTERVAL_QUERY: &str =
        include_str!("../../../fixtures/riffql/wp700-binary-text-strict-window.riffq");
    const BINARY_TEXT_COMPLEMENT_QUERY: &str =
        include_str!("../../../fixtures/riffql/wp700-binary-text-complement.riffq");
    const EXPANSION_QUERY: &str =
        include_str!("../../../fixtures/riffql/ticket_comments_expansion.riffql");

    fn binary_text_state(
        bundle: &riffdb_contract_ir::ContractBundle,
        program: &QueryAccessProgramV1,
    ) -> MemoryState {
        let step = &program.steps()[0];
        let entity = &bundle.schema().entities()[0];
        let index = entity
            .indexes()
            .iter()
            .find(|index| index.name() == "by_code")
            .expect("binary-text index");
        let access = program.internal_entity_access("Item").expect("Item access");
        let binding = DurableKeySchemaBindingV1::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
        );
        let organization = CanonicalValue::Uuid([0x71; 16]);
        let mut state = MemoryState::default();
        for (ordinal, code) in ["a", "ab", "abacus", "ac", "doc-3", "doc6"]
            .into_iter()
            .enumerate()
        {
            let item = CanonicalValue::Uuid([u8::try_from(ordinal + 1).expect("ordinal"); 16]);
            let code = CanonicalValue::string(code).expect("code");
            let entity_key = step
                .internal_entity_key_schema()
                .encode_entity(&[organization.clone(), item.clone()])
                .expect("entity key");
            let fields = CanonicalRecord::new(vec![
                (
                    access
                        .internal_field_id("organization_id")
                        .expect("organization"),
                    organization.clone(),
                ),
                (access.internal_field_id("item_id").expect("item"), item),
                (access.internal_field_id("code").expect("code"), code),
            ])
            .expect("fields");
            let record = StoredEntityRecordV1::new(
                EntityTarget::new(step.internal_entity_id(), entity_key.clone()).expect("target"),
                EntityVersion::first(),
                bundle.contract_version(),
                binding.clone(),
                fields.clone(),
            )
            .expect("entity");
            let index_values =
                riffdb_contract_ir::encode_operational_index_values_v1(index, &fields)
                    .expect("index values");
            let index_key = index
                .key_schema()
                .encode_index(&index_values, entity_key)
                .expect("index key");
            let partition = step
                .internal_partition_key_schema()
                .encode_partition(std::slice::from_ref(&organization))
                .expect("partition");
            let index = StoredIndexEntryV2::new(
                index_key,
                binding.clone(),
                CanonicalRecord::new(Vec::new()).expect("cover"),
                partition,
            )
            .expect("index row");
            let encoded = encode_index_entry_v2(&index).expect("encoded index");
            state.entities.push(record);
            state
                .index_entries
                .push(MemoryIndexEntry::current_from_encoded(
                    index,
                    encoded.as_bytes().to_vec(),
                    EncodedContentCharge::new(encoded.as_bytes().len()).expect("charge"),
                ));
        }
        state
            .entities
            .sort_unstable_by(|left, right| left.target().cmp(right.target()));
        state
            .index_entries
            .sort_unstable_by(|left, right| left.key().cmp(right.key()));
        state
    }

    fn memory_binary_text_pages(
        state: &MemoryState,
        program: &QueryAccessProgramV1,
        parameters: &QueryParameters,
    ) -> Vec<String> {
        let mut prior = None;
        let mut codes = Vec::new();
        loop {
            let mut view = MemoryQueryView {
                state,
                program,
                parameters,
            };
            let snapshot = execute_page_in_snapshot(program, parameters, prior.as_ref(), &mut view)
                .expect("binary-text page");
            let Some(QueryResultValue::Many(rows)) = snapshot.fields().get("items") else {
                panic!("items result")
            };
            codes.extend(rows.iter().map(|row| match row.field("code") {
                Some(CanonicalValue::String(code)) => code.as_str().to_owned(),
                other => panic!("code field: {other:?}"),
            }));
            let Some(after) = snapshot.continuation() else {
                break;
            };
            prior = Some(
                QueryContinuation::checked(
                    snapshot
                        .continuation_binding()
                        .expect("continuation binding")
                        .to_owned(),
                    after.to_vec(),
                    snapshot.index_epochs().clone(),
                )
                .expect("continuation"),
            );
        }
        codes
    }

    fn expansion_status(bundle: &riffdb_contract_ir::ContractBundle) -> CanonicalValue {
        let status = bundle
            .schema()
            .enums()
            .iter()
            .find(|enumeration| enumeration.name() == "TicketStatus")
            .expect("TicketStatus");
        CanonicalValue::Enum {
            type_id: status.id(),
            variant_id: status
                .variants()
                .iter()
                .find(|variant| variant.name() == "Open")
                .expect("Open")
                .id(),
        }
    }

    fn expansion_state(
        bundle: &riffdb_contract_ir::ContractBundle,
        program: &QueryAccessProgramV1,
        comments_per_ticket: u8,
    ) -> MemoryState {
        let organization = CanonicalValue::Uuid([1; 16]);
        let project = CanonicalValue::Uuid([2; 16]);
        let status = expansion_status(bundle);
        let binding = DurableKeySchemaBindingV1::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
        );
        let mut state = MemoryState::default();
        for step in program.steps() {
            let access = program
                .internal_entity_access(step.entity())
                .expect("entity access");
            let rows = if step.entity() == "Ticket" {
                2
            } else {
                usize::from(comments_per_ticket) * 2
            };
            for ordinal in 0..rows {
                let ticket_ordinal = if step.entity() == "Ticket" {
                    ordinal + 1
                } else {
                    ordinal / usize::from(comments_per_ticket) + 1
                };
                let ticket = CanonicalValue::Uuid([ticket_ordinal as u8 + 2; 16]);
                let comment = CanonicalValue::Uuid([ordinal as u8 + 20; 16]);
                let created_at = CanonicalValue::Timestamp(
                    Timestamp::new(
                        1_700_000_000
                            + i64::try_from(ordinal % usize::from(comments_per_ticket))
                                .expect("bounded ordinal"),
                        0,
                    )
                    .expect("timestamp"),
                );
                let entity_values = if step.entity() == "Ticket" {
                    vec![organization.clone(), ticket.clone()]
                } else {
                    vec![organization.clone(), ticket.clone(), comment.clone()]
                };
                let entity_key = step
                    .internal_entity_key_schema()
                    .encode_entity(&entity_values)
                    .expect("entity key");
                let fields = if step.entity() == "Ticket" {
                    vec![
                        ("organization_id", organization.clone()),
                        ("ticket_id", ticket.clone()),
                        ("project_id", project.clone()),
                        ("status", status.clone()),
                    ]
                } else {
                    vec![
                        ("organization_id", organization.clone()),
                        ("ticket_id", ticket.clone()),
                        ("comment_id", comment.clone()),
                        ("created_at", created_at.clone()),
                    ]
                };
                let fields = CanonicalRecord::new(
                    fields
                        .into_iter()
                        .map(|(name, value)| {
                            (access.internal_field_id(name).expect("field ID"), value)
                        })
                        .collect(),
                )
                .expect("fields");
                state.entities.push(
                    StoredEntityRecordV1::new(
                        EntityTarget::new(step.internal_entity_id(), entity_key.clone())
                            .expect("target"),
                        EntityVersion::first(),
                        bundle.contract_version(),
                        binding.clone(),
                        fields,
                    )
                    .expect("entity"),
                );
                let index_values = if step.entity() == "Ticket" {
                    vec![
                        organization.clone(),
                        project.clone(),
                        status.clone(),
                        ticket,
                    ]
                } else {
                    vec![organization.clone(), ticket, created_at, comment]
                };
                let index_key = step
                    .internal_index_key_schema()
                    .expect("index schema")
                    .encode_index(&index_values, entity_key)
                    .expect("index key");
                let partition = step
                    .internal_partition_key_schema()
                    .encode_partition(std::slice::from_ref(&organization))
                    .expect("partition");
                let index = StoredIndexEntryV2::new(
                    index_key,
                    binding.clone(),
                    CanonicalRecord::new(Vec::new()).expect("cover"),
                    partition,
                )
                .expect("index");
                let encoded = encode_index_entry_v2(&index).expect("encoded index");
                state
                    .index_entries
                    .push(MemoryIndexEntry::current_from_encoded(
                        index,
                        encoded.as_bytes().to_vec(),
                        EncodedContentCharge::new(encoded.as_bytes().len()).expect("charge"),
                    ));
            }
        }
        state
            .entities
            .sort_unstable_by(|left, right| left.target().cmp(right.target()));
        state
            .index_entries
            .sort_unstable_by(|left, right| left.key().cmp(right.key()));
        state
    }

    // req: OQ-114, OQ-115
    #[test]
    fn expansion_memory_matches_independent_oracle() {
        let bundle = compile_contract_source(CONTRACT).expect("contract");
        let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
        let program = compile_query(&parse_query(EXPANSION_QUERY).expect("query"), &catalog)
            .expect("program");
        let state = expansion_state(&bundle, &program, 8);
        let parameters = QueryParameters::checked(BTreeMap::from([
            ("organization_id".to_owned(), CanonicalValue::Uuid([1; 16])),
            ("project_id".to_owned(), CanonicalValue::Uuid([2; 16])),
            ("status".to_owned(), expansion_status(&bundle)),
        ]))
        .expect("parameters");
        let mut view = MemoryQueryView {
            state: &state,
            program: &program,
            parameters: &parameters,
        };
        let snapshot = execute_in_snapshot(&program, &parameters, &mut view).expect("expansion");
        let Some(QueryResultValue::Many(tickets)) = snapshot.fields().get("tickets") else {
            panic!("ticket rows")
        };
        assert_eq!(tickets.len(), 2);
        assert_eq!(
            tickets[0].nested_rows("comments").expect("comments").len(),
            8
        );
        assert_eq!(
            tickets[1].nested_rows("comments").expect("comments").len(),
            8
        );
        assert_eq!(
            tickets[0].field("ticket_id"),
            Some(&CanonicalValue::Uuid([3; 16]))
        );
        assert_eq!(
            tickets[1].nested_rows("comments").expect("comments")[7].field("comment_id"),
            Some(&CanonicalValue::Uuid([35; 16]))
        );

        let empty_parameters = QueryParameters::checked(BTreeMap::from([
            ("organization_id".to_owned(), CanonicalValue::Uuid([1; 16])),
            ("project_id".to_owned(), CanonicalValue::Uuid([99; 16])),
            ("status".to_owned(), expansion_status(&bundle)),
        ]))
        .expect("empty parameters");
        let mut empty = MemoryQueryView {
            state: &state,
            program: &program,
            parameters: &empty_parameters,
        };
        let empty =
            execute_in_snapshot(&program, &empty_parameters, &mut empty).expect("empty expansion");
        assert!(matches!(
            empty.fields().get("tickets"),
            Some(QueryResultValue::Many(rows)) if rows.is_empty()
        ));

        let policy_state = expansion_state(&bundle, &program, 9);
        let comment_step = program
            .steps()
            .iter()
            .find(|step| step.entity() == "Comment")
            .expect("comment step");
        let created_at = program
            .internal_entity_access("Comment")
            .and_then(|access| access.internal_field_id("created_at"))
            .expect("created_at field");
        let rule = RowPolicyRuleV1::new(
            RowPolicyOperationV1::Read,
            vec![
                RowPolicyExpressionNodeV1::Operand(RowPolicyOperandV1::new(
                    RowPolicyValueSourceV1::RowField(created_at),
                    ValueType::timestamp(),
                )),
                RowPolicyExpressionNodeV1::Operand(RowPolicyOperandV1::new(
                    RowPolicyValueSourceV1::Constant(CanonicalValue::Timestamp(
                        Timestamp::new(1_700_000_000, 0).expect("denied timestamp"),
                    )),
                    ValueType::timestamp(),
                )),
                RowPolicyExpressionNodeV1::NotEqual { left: 0, right: 1 },
            ],
            2,
            comment_step.internal_entity_id(),
            bundle.schema(),
            &BTreeMap::new(),
        )
        .expect("comment policy rule");
        let policy = RowPolicyPlanV1::new(
            "HideFirstComment",
            comment_step.internal_entity_id(),
            vec![rule],
            bundle.schema(),
        )
        .expect("comment policy");
        let principal = PrincipalFactBindingV1::new(
            CapabilityId::from_unix_milliseconds_and_random(1, [0x51; 10]).expect("capability"),
            NonZeroU64::new(1).expect("revision"),
            DatabaseId::from_unix_milliseconds_and_random(1, [0x52; 10]).expect("database"),
            Environment::new("test").expect("environment"),
            ActorId::new("expansion-policy-test").expect("actor"),
            ActorKind::Service,
            vec![Audience::new("riffdb-policy-test").expect("audience")],
            TenantScope::Global,
            Timestamp::new(1, 0).expect("issued"),
            Timestamp::new(10, 0).expect("expires"),
            CapabilityPrincipalFactsV1::empty(),
        )
        .expect("principal facts");
        let policy = AuthorizedQueryRowPolicyContextV1::test_fixture(
            principal,
            vec![policy],
            bundle.schema(),
        )
        .expect("policy context");
        let mut policy_view = MemoryQueryView {
            state: &policy_state,
            program: &program,
            parameters: &parameters,
        };
        let filtered =
            execute_policy_page_in_snapshot(&program, &parameters, None, &mut policy_view, &policy)
                .expect("policy-filtered expansion");
        let Some(QueryResultValue::Many(filtered_tickets)) = filtered.fields().get("tickets")
        else {
            panic!("filtered tickets")
        };
        assert!(filtered_tickets.iter().all(|ticket| {
            ticket
                .nested_rows("comments")
                .is_some_and(|comments| comments.len() == 8)
        }));
        assert!(
            filtered_tickets
                .iter()
                .flat_map(|ticket| { ticket.nested_rows("comments").expect("filtered comments") })
                .all(|comment| {
                    comment.field("created_at")
                        != Some(&CanonicalValue::Timestamp(
                            Timestamp::new(1_700_000_000, 0).expect("denied timestamp"),
                        ))
                })
        );
    }

    // req: DEP-001
    #[test]
    fn point_query_materializes_an_owned_result_from_one_state_view() {
        let bundle = compile_contract_source(CONTRACT).expect("contract");
        let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
        let program =
            compile_query(&parse_query(POINT_QUERY).expect("parse"), &catalog).expect("program");
        let step = &program.steps()[0];
        let organization = CanonicalValue::Uuid([1; 16]);
        let ticket = CanonicalValue::Uuid([2; 16]);
        let key = step
            .internal_entity_key_schema()
            .encode_entity(&[organization.clone(), ticket.clone()])
            .expect("key");
        let target = EntityTarget::new(step.internal_entity_id(), key).expect("target");
        let access = program
            .internal_entity_access("Ticket")
            .expect("Ticket access");
        let fields = CanonicalRecord::new(vec![
            (
                access
                    .internal_field_id("organization_id")
                    .expect("organization field"),
                organization.clone(),
            ),
            (
                access.internal_field_id("ticket_id").expect("ticket field"),
                ticket.clone(),
            ),
            (
                access.internal_field_id("title").expect("title field"),
                CanonicalValue::string("one snapshot").expect("title"),
            ),
        ])
        .expect("fields");
        let record = StoredEntityRecordV1::new(
            target,
            EntityVersion::first(),
            bundle.contract_version(),
            DurableKeySchemaBindingV1::new(
                bundle.lineage().clone(),
                bundle.contract_version(),
                bundle.bundle_hash(),
            ),
            fields,
        )
        .expect("record");
        let mut state = MemoryState::default();
        state.entities.push(record);
        let parameters = QueryParameters::checked(BTreeMap::from([
            ("organization_id".to_owned(), organization),
            ("ticket_id".to_owned(), ticket),
        ]))
        .expect("parameters");
        let mut view = MemoryQueryView {
            state: &state,
            program: &program,
            parameters: &parameters,
        };
        let snapshot = execute_in_snapshot(&program, &parameters, &mut view).expect("execute");
        capture_snapshot("memory-named-point-found", &snapshot);
        assert_eq!(snapshot.outcome(), "Found");
        assert!(matches!(
            snapshot.fields().get("ticket"),
            Some(QueryResultValue::One(row)) if row.field("title").is_some()
        ));

        let absent_parameters = QueryParameters::checked(BTreeMap::from([
            ("organization_id".to_owned(), CanonicalValue::Uuid([1; 16])),
            ("ticket_id".to_owned(), CanonicalValue::Uuid([9; 16])),
        ]))
        .expect("absent parameters");
        let mut absent_view = MemoryQueryView {
            state: &state,
            program: &program,
            parameters: &absent_parameters,
        };
        let absent = execute_in_snapshot(&program, &absent_parameters, &mut absent_view)
            .expect("declared NotFound outcome");
        capture_snapshot("memory-named-point-not-found", &absent);

        let missing_parameters =
            QueryParameters::checked(BTreeMap::new()).expect("empty parameters");
        let mut missing_view = MemoryQueryView {
            state: &state,
            program: &program,
            parameters: &missing_parameters,
        };
        let missing = execute_in_snapshot(&program, &missing_parameters, &mut missing_view)
            .expect_err("missing parameter refusal");
        capture_error("memory-missing-parameter-refusal", &missing);
    }

    // req: DEP-001
    #[test]
    fn index_page_batches_entity_reads_inside_the_same_memory_view() {
        let bundle = compile_contract_source(CONTRACT).expect("contract");
        let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
        let program =
            compile_query(&parse_query(MEMBERS_QUERY).expect("parse"), &catalog).expect("program");
        let step = &program.steps()[0];
        let organization = CanonicalValue::Uuid([1; 16]);
        let project = CanonicalValue::Uuid([2; 16]);
        let binding = DurableKeySchemaBindingV1::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
        );
        let access = program
            .internal_entity_access("ProjectMember")
            .expect("access");
        let mut state = MemoryState::default();
        for (ordinal, partition_ordinal) in [(2_u8, 9_u8), (3_u8, 1_u8), (4_u8, 1_u8)] {
            let user = CanonicalValue::Uuid([ordinal; 16]);
            let key = step
                .internal_entity_key_schema()
                .encode_entity(&[organization.clone(), project.clone(), user.clone()])
                .expect("entity key");
            let target = EntityTarget::new(step.internal_entity_id(), key.clone()).expect("target");
            let fields = CanonicalRecord::new(vec![
                (
                    access
                        .internal_field_id("organization_id")
                        .expect("organization"),
                    organization.clone(),
                ),
                (
                    access.internal_field_id("project_id").expect("project"),
                    project.clone(),
                ),
                (
                    access.internal_field_id("user_id").expect("user"),
                    user.clone(),
                ),
                (
                    access.internal_field_id("role").expect("role"),
                    CanonicalValue::string("member").expect("role"),
                ),
            ])
            .expect("fields");
            state.entities.push(
                StoredEntityRecordV1::new(
                    target,
                    EntityVersion::first(),
                    bundle.contract_version(),
                    binding.clone(),
                    fields,
                )
                .expect("record"),
            );
            let index_key = step
                .internal_index_key_schema()
                .expect("index schema")
                .encode_index(&[organization.clone(), project.clone(), user], key)
                .expect("index key");
            let mut partition =
                PartitionKeyBuilder::new(AggregateTypeId::new(4).expect("aggregate"));
            partition
                .push_uuid(&[partition_ordinal; 16])
                .expect("partition component");
            let index = StoredIndexEntryV2::new(
                index_key,
                binding.clone(),
                CanonicalRecord::new(Vec::new()).expect("cover"),
                partition.finish().expect("partition"),
            )
            .expect("index");
            let encoded = encode_index_entry_v2(&index).expect("encode");
            let charge = EncodedContentCharge::new(encoded.as_bytes().len()).expect("charge");
            state
                .index_entries
                .push(MemoryIndexEntry::current_from_encoded(
                    index,
                    encoded.as_bytes().to_vec(),
                    charge,
                ));
        }
        state
            .entities
            .sort_unstable_by(|left, right| left.target().cmp(right.target()));
        state
            .index_entries
            .sort_unstable_by(|left, right| left.key().cmp(right.key()));
        let parameters = QueryParameters::checked(BTreeMap::from([
            ("organization_id".to_owned(), organization.clone()),
            ("project_id".to_owned(), project.clone()),
        ]))
        .expect("parameters");
        let mut view = MemoryQueryView {
            state: &state,
            program: &program,
            parameters: &parameters,
        };
        let snapshot = execute_in_snapshot(&program, &parameters, &mut view).expect("execute");
        capture_snapshot("memory-named-page-1", &snapshot);
        assert!(matches!(
            snapshot.fields().get("members"),
            Some(QueryResultValue::Many(rows)) if rows.len() == 1
        ));
        let first_user = match snapshot.fields().get("members") {
            Some(QueryResultValue::Many(rows)) => rows[0].field("user_id").cloned(),
            _ => None,
        };
        assert_ne!(
            first_user,
            Some(CanonicalValue::Uuid([2; 16])),
            "a foreign-partition index row must not consume the page limit"
        );
        let cursor = QueryContinuation::checked(
            snapshot
                .continuation_binding()
                .expect("continuation binding")
                .to_owned(),
            snapshot.continuation().expect("continuation").to_vec(),
            snapshot.index_epochs().clone(),
        )
        .expect("cursor");
        let mut stale_epochs = snapshot.index_epochs().clone();
        for epoch in stale_epochs.values_mut() {
            *epoch = epoch.saturating_add(1);
        }
        let stale = QueryContinuation::checked(
            snapshot
                .continuation_binding()
                .expect("continuation binding")
                .to_owned(),
            snapshot.continuation().expect("continuation").to_vec(),
            stale_epochs,
        )
        .expect("stale cursor");
        let mut stale_view = MemoryQueryView {
            state: &state,
            program: &program,
            parameters: &parameters,
        };
        let stale_error =
            execute_page_in_snapshot(&program, &parameters, Some(&stale), &mut stale_view)
                .expect_err("stale cursor refusal");
        capture_error("memory-stale-cursor-refusal", &stale_error);
        let mut second_view = MemoryQueryView {
            state: &state,
            program: &program,
            parameters: &parameters,
        };
        let second =
            execute_page_in_snapshot(&program, &parameters, Some(&cursor), &mut second_view)
                .expect("second page");
        capture_snapshot("memory-named-page-2", &second);
        assert!(matches!(
            second.fields().get("members"),
            Some(QueryResultValue::Many(rows))
                if rows.len() == 1 && rows[0].field("user_id").cloned() != first_user
        ));
        // Continuation lower bound is the last included row, never the peeked
        // next row: the second page must not re-emit the first page's user.
        assert_ne!(
            second
                .fields()
                .get("members")
                .and_then(|value| match value {
                    QueryResultValue::Many(rows) =>
                        rows.first().and_then(|row| row.field("user_id")),
                    _ => None,
                }),
            first_user.as_ref()
        );

        let range_program = compile_query(
            &parse_query(MEMBERS_RANGE_QUERY).expect("parse range"),
            &catalog,
        )
        .expect("range program");
        let range_parameters = QueryParameters::checked(BTreeMap::from([
            ("organization_id".to_owned(), organization.clone()),
            ("project_id".to_owned(), project.clone()),
            ("lower".to_owned(), CanonicalValue::Uuid([3; 16])),
            ("upper".to_owned(), CanonicalValue::Uuid([5; 16])),
        ]))
        .expect("range parameters");
        let mut range_view = MemoryQueryView {
            state: &state,
            program: &range_program,
            parameters: &range_parameters,
        };
        let range_first =
            execute_page_in_snapshot(&range_program, &range_parameters, None, &mut range_view)
                .expect("first range page");
        capture_snapshot("memory-range-page-1", &range_first);
        assert!(matches!(
            range_first.fields().get("members"),
            Some(QueryResultValue::Many(rows))
                if rows.len() == 1
                    && rows[0].field("user_id") == Some(&CanonicalValue::Uuid([3; 16]))
        ));
        let range_cursor = QueryContinuation::checked(
            range_first
                .continuation_binding()
                .expect("range continuation binding")
                .to_owned(),
            range_first
                .continuation()
                .expect("range continuation")
                .to_vec(),
            range_first.index_epochs().clone(),
        )
        .expect("range cursor");
        let mut range_second_view = MemoryQueryView {
            state: &state,
            program: &range_program,
            parameters: &range_parameters,
        };
        let range_second = execute_page_in_snapshot(
            &range_program,
            &range_parameters,
            Some(&range_cursor),
            &mut range_second_view,
        )
        .expect("second range page");
        capture_snapshot("memory-range-page-2", &range_second);
        assert!(matches!(
            range_second.fields().get("members"),
            Some(QueryResultValue::Many(rows))
                if rows.len() == 1
                    && rows[0].field("user_id") == Some(&CanonicalValue::Uuid([4; 16]))
        ));
        assert!(range_second.continuation().is_none());

        // A policy-hidden first row cannot consume `take 1` or become the
        // continuation identity. Only user 4 is authorized, so the same scan
        // must pass user 3 and return user 4 as an exact-end page.
        let user_field = access.internal_field_id("user_id").expect("user field");
        let rule = RowPolicyRuleV1::new(
            RowPolicyOperationV1::Read,
            vec![
                RowPolicyExpressionNodeV1::Operand(RowPolicyOperandV1::new(
                    RowPolicyValueSourceV1::RowField(user_field),
                    ValueType::uuid(),
                )),
                RowPolicyExpressionNodeV1::Operand(RowPolicyOperandV1::new(
                    RowPolicyValueSourceV1::Constant(CanonicalValue::Uuid([4; 16])),
                    ValueType::uuid(),
                )),
                RowPolicyExpressionNodeV1::Equal { left: 0, right: 1 },
            ],
            2,
            step.internal_entity_id(),
            bundle.schema(),
            &BTreeMap::new(),
        )
        .expect("read policy rule");
        let policy = RowPolicyPlanV1::new(
            "OnlyLastMember",
            step.internal_entity_id(),
            vec![rule],
            bundle.schema(),
        )
        .expect("row policy");
        let principal = PrincipalFactBindingV1::new(
            CapabilityId::from_unix_milliseconds_and_random(1, [0x41; 10]).expect("capability"),
            NonZeroU64::new(1).expect("revision"),
            DatabaseId::from_unix_milliseconds_and_random(1, [0x42; 10]).expect("database"),
            Environment::new("test").expect("environment"),
            ActorId::new("policy-test").expect("actor"),
            ActorKind::Service,
            vec![Audience::new("riffdb-policy-test").expect("audience")],
            TenantScope::Global,
            Timestamp::new(1, 0).expect("issued"),
            Timestamp::new(10, 0).expect("expires"),
            CapabilityPrincipalFactsV1::empty(),
        )
        .expect("principal facts");
        let policy = AuthorizedQueryRowPolicyContextV1::test_fixture(
            principal,
            vec![policy],
            bundle.schema(),
        )
        .expect("policy context");
        let mut policy_view = MemoryQueryView {
            state: &state,
            program: &program,
            parameters: &parameters,
        };
        let filtered =
            execute_policy_page_in_snapshot(&program, &parameters, None, &mut policy_view, &policy)
                .expect("policy-filtered page");
        capture_snapshot("memory-row-admission-page", &filtered);
        assert!(matches!(
            filtered.fields().get("members"),
            Some(QueryResultValue::Many(rows))
                if rows.len() == 1
                    && rows[0].field("user_id") == Some(&CanonicalValue::Uuid([4; 16]))
        ));
        assert!(filtered.continuation().is_none());

        let ports = MemoryDormantPorts {
            store: MemoryStore::new(),
        }
        .into_operational();
        let access = ports.acquire().expect("memory write access");
        access
            .write(|stored| {
                *stored = state;
                Ok(())
            })
            .expect("install query state");
        drop(access);
        let requests = [
            QueryExecutionRequest::new(&program, &parameters),
            QueryExecutionRequest::new(&program, &parameters),
        ];
        let grouped = ports
            .execute_policy_query_group(&requests, &policy)
            .expect("policy-protected group");
        assert_eq!(grouped.len(), 2);
        assert!(grouped.iter().all(|snapshot| matches!(
            snapshot.fields().get("members"),
            Some(QueryResultValue::Many(rows))
                if rows.len() == 1
                    && rows[0].field("user_id") == Some(&CanonicalValue::Uuid([4; 16]))
        )));

        let candidate_keys = [3_u8, 4_u8]
            .into_iter()
            .map(|ordinal| {
                step.internal_entity_key_schema()
                    .encode_entity(&[
                        organization.clone(),
                        project.clone(),
                        CanonicalValue::Uuid([ordinal; 16]),
                    ])
                    .expect("candidate key")
            })
            .collect::<Vec<_>>();
        let admission = ports
            .authorize_projected_candidates(step.internal_entity_id(), &candidate_keys, &policy)
            .expect("authoritative projected admission");
        let covered = candidate_keys.iter().cloned().collect();
        assert!(admission.covers(step.internal_entity_id(), &covered));
        assert!(!admission.admits(&candidate_keys[0]));
        assert!(admission.admits(&candidate_keys[1]));
    }

    fn capture_snapshot(name: &str, snapshot: &QueryOwnedSnapshot) {
        let Some(root) = std::env::var_os("RIFFDB_WP754_FIXTURE_OUTPUT") else {
            return;
        };
        let path = std::path::PathBuf::from(root).join(format!("{name}.bin"));
        std::fs::create_dir_all(path.parent().expect("fixture parent")).expect("fixture directory");
        std::fs::write(
            path,
            riffdb_query_executor::encode_query_snapshot_fixture_v1(name, snapshot),
        )
        .expect("write fixture");
    }

    fn capture_error(name: &str, error: &QueryExecutionError) {
        let Some(root) = std::env::var_os("RIFFDB_WP754_FIXTURE_OUTPUT") else {
            return;
        };
        let path = std::path::PathBuf::from(root).join(format!("{name}.bin"));
        std::fs::create_dir_all(path.parent().expect("fixture parent")).expect("fixture directory");
        std::fs::write(
            path,
            riffdb_query_executor::encode_query_error_fixture_v1(name, error),
        )
        .expect("write fixture");
    }

    // req: OQ-036, OQ-043, OQ-057, OQ-058, OQ-059
    #[test]
    fn binary_text_interval_memory_backend_matches_the_shared_cursor_truth_table() {
        let bundle = compile_contract_source(BINARY_TEXT_INTERVAL_CONTRACT).expect("contract");
        let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
        let organization = CanonicalValue::Uuid([0x71; 16]);
        let interval_parameters = QueryParameters::checked(BTreeMap::from([
            ("organization_id".to_owned(), organization.clone()),
            (
                "lower".to_owned(),
                CanonicalValue::string("ab").expect("lower"),
            ),
            (
                "upper".to_owned(),
                CanonicalValue::string("doc6").expect("upper"),
            ),
        ]))
        .expect("parameters");
        let forward = compile_query(
            &parse_query(BINARY_TEXT_INTERVAL_QUERY).expect("forward query"),
            &catalog,
        )
        .expect("forward program");
        let state = binary_text_state(&bundle, &forward);
        assert_eq!(
            memory_binary_text_pages(&state, &forward, &interval_parameters),
            ["abacus", "ac", "doc-3"]
        );

        let reverse_source = BINARY_TEXT_INTERVAL_QUERY
            .replace("code asc", "code desc")
            .replace("item_id asc", "item_id desc");
        let reverse = compile_query(
            &parse_query(&reverse_source).expect("reverse query"),
            &catalog,
        )
        .expect("reverse program");
        assert_eq!(
            memory_binary_text_pages(&state, &reverse, &interval_parameters),
            ["doc-3", "ac", "abacus"]
        );

        let complement = compile_query(
            &parse_query(BINARY_TEXT_COMPLEMENT_QUERY).expect("complement query"),
            &catalog,
        )
        .expect("complement program");
        let complement_parameters = QueryParameters::checked(BTreeMap::from([
            ("organization_id".to_owned(), organization),
            (
                "excluded".to_owned(),
                CanonicalValue::string("ac").expect("excluded"),
            ),
        ]))
        .expect("complement parameters");
        assert_eq!(
            memory_binary_text_pages(&state, &complement, &complement_parameters),
            ["a", "ab", "abacus", "doc-3", "doc6"]
        );
    }

    #[test]
    fn exact_end_page_mints_no_continuation_when_page_fills_the_range() {
        // take 2 with exactly two partition-matching members → exact end.
        const EXACT_END_QUERY: &str = r#"
query list_members_exact(
    $organization_id: Organization.organization_id,
    $project_id: Project.project_id,
) {
    many memberships from ProjectMember
        where organization_id == $organization_id && project_id == $project_id
        order by user_id asc
        take 2
    return Found { members: memberships { user_id role } }
    outcomes Found
}
"#;
        let bundle = compile_contract_source(CONTRACT).expect("contract");
        let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
        let program = compile_query(&parse_query(EXACT_END_QUERY).expect("parse"), &catalog)
            .expect("program");
        let step = &program.steps()[0];
        let organization = CanonicalValue::Uuid([1; 16]);
        let project = CanonicalValue::Uuid([2; 16]);
        let binding = DurableKeySchemaBindingV1::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
        );
        let access = program
            .internal_entity_access("ProjectMember")
            .expect("access");
        let mut state = MemoryState::default();
        for (ordinal, partition_ordinal) in [(2_u8, 9_u8), (3_u8, 1_u8), (4_u8, 1_u8)] {
            let user = CanonicalValue::Uuid([ordinal; 16]);
            let key = step
                .internal_entity_key_schema()
                .encode_entity(&[organization.clone(), project.clone(), user.clone()])
                .expect("entity key");
            let target = EntityTarget::new(step.internal_entity_id(), key.clone()).expect("target");
            let fields = CanonicalRecord::new(vec![
                (
                    access
                        .internal_field_id("organization_id")
                        .expect("organization"),
                    organization.clone(),
                ),
                (
                    access.internal_field_id("project_id").expect("project"),
                    project.clone(),
                ),
                (
                    access.internal_field_id("user_id").expect("user"),
                    user.clone(),
                ),
                (
                    access.internal_field_id("role").expect("role"),
                    CanonicalValue::string("member").expect("role"),
                ),
            ])
            .expect("fields");
            state.entities.push(
                StoredEntityRecordV1::new(
                    target,
                    EntityVersion::first(),
                    bundle.contract_version(),
                    binding.clone(),
                    fields,
                )
                .expect("record"),
            );
            let index_key = step
                .internal_index_key_schema()
                .expect("index schema")
                .encode_index(&[organization.clone(), project.clone(), user], key)
                .expect("index key");
            let mut partition =
                PartitionKeyBuilder::new(AggregateTypeId::new(4).expect("aggregate"));
            partition
                .push_uuid(&[partition_ordinal; 16])
                .expect("partition component");
            let index = StoredIndexEntryV2::new(
                index_key,
                binding.clone(),
                CanonicalRecord::new(Vec::new()).expect("cover"),
                partition.finish().expect("partition"),
            )
            .expect("index");
            let encoded = encode_index_entry_v2(&index).expect("encode");
            let charge = EncodedContentCharge::new(encoded.as_bytes().len()).expect("charge");
            state
                .index_entries
                .push(MemoryIndexEntry::current_from_encoded(
                    index,
                    encoded.as_bytes().to_vec(),
                    charge,
                ));
        }
        state
            .entities
            .sort_unstable_by(|left, right| left.target().cmp(right.target()));
        state
            .index_entries
            .sort_unstable_by(|left, right| left.key().cmp(right.key()));
        let parameters = QueryParameters::checked(BTreeMap::from([
            ("organization_id".to_owned(), organization),
            ("project_id".to_owned(), project),
        ]))
        .expect("parameters");
        let mut view = MemoryQueryView {
            state: &state,
            program: &program,
            parameters: &parameters,
        };
        let snapshot = execute_in_snapshot(&program, &parameters, &mut view).expect("execute");
        assert!(matches!(
            snapshot.fields().get("members"),
            Some(QueryResultValue::Many(rows)) if rows.len() == 2
        ));
        assert!(
            snapshot.continuation().is_none() && snapshot.continuation_binding().is_none(),
            "exact-end page must not mint a continuation"
        );
    }

    /// F5: dependent_point_batch builds the materialize plan once for the batch.
    /// Transcript: re-inline plan inside point() → builds == predicate count → hoist → 1.
    #[test]
    fn dependent_point_batch_builds_one_materialize_plan() {
        use riffdb_query_executor::BoundPredicate;
        use riffdb_query_ir::{QueryAccessKind, QueryPredicateOperator};

        let bundle = compile_contract_source(CONTRACT).expect("contract");
        let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
        let program = compile_query(
            &parse_query(include_str!(
                "../../../queries/ticketdesk/ticket_page.riffq"
            ))
            .expect("parse"),
            &catalog,
        )
        .expect("program");
        let step = program
            .steps()
            .iter()
            .find(|step| matches!(step.access(), QueryAccessKind::DependentPointBatch { .. }))
            .expect("labels dependent batch step");
        let QueryAccessKind::DependentPointBatch { key_fields, .. } = step.access() else {
            unreachable!();
        };
        let null_preds: Vec<BoundPredicate> = key_fields
            .iter()
            .map(|field| {
                BoundPredicate::new(
                    field.clone(),
                    QueryPredicateOperator::Equal,
                    CanonicalValue::Null,
                )
            })
            .collect();
        let batch = vec![null_preds.clone(), null_preds.clone(), null_preds];
        let state = MemoryState::default();
        let parameters = QueryParameters::checked(BTreeMap::from([(
            "organization_id".to_owned(),
            CanonicalValue::Uuid([1; 16]),
        )]))
        .expect("parameters");
        let mut view = MemoryQueryView {
            state: &state,
            program: &program,
            parameters: &parameters,
        };
        reset_materialize_plan_builds();
        let rows = view
            .dependent_point_batch(step, &batch, None)
            .expect("batch executes");
        assert_eq!(rows.len(), 3);
        assert_eq!(
            materialize_plan_builds(),
            1,
            "dependent batch must intern field names once, not per predicate"
        );
    }

    // req: OQ-041, OQ-043
    #[test]
    fn dependent_point_batch_preserves_position_missing_and_policy_semantics() {
        use riffdb_query_executor::BoundPredicate;
        use riffdb_query_ir::{QueryAccessKind, QueryPredicateOperator};

        let bundle = compile_contract_source(CONTRACT).expect("contract");
        let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
        let program = compile_query(
            &parse_query(include_str!(
                "../../../queries/ticketdesk/ticket_page.riffq"
            ))
            .expect("parse"),
            &catalog,
        )
        .expect("program");
        let step = program
            .steps()
            .iter()
            .find(|step| matches!(step.access(), QueryAccessKind::DependentPointBatch { .. }))
            .expect("labels dependent batch step");
        assert!(step.cursor_parameter().is_none());
        let QueryAccessKind::DependentPointBatch { key_fields, .. } = step.access() else {
            unreachable!();
        };
        let organization = CanonicalValue::Uuid([1; 16]);
        let binding = DurableKeySchemaBindingV1::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
        );
        let access = program
            .internal_entity_access("Label")
            .expect("Label access");
        let label_record = |ordinal: u8| {
            let label = CanonicalValue::Uuid([ordinal; 16]);
            let key = step
                .internal_entity_key_schema()
                .encode_entity(&[organization.clone(), label.clone()])
                .expect("label key");
            StoredEntityRecordV1::new(
                EntityTarget::new(step.internal_entity_id(), key).expect("label target"),
                EntityVersion::first(),
                bundle.contract_version(),
                binding.clone(),
                CanonicalRecord::new(vec![
                    (
                        access
                            .internal_field_id("organization_id")
                            .expect("organization field"),
                        organization.clone(),
                    ),
                    (
                        access.internal_field_id("label_id").expect("label field"),
                        label,
                    ),
                    (
                        access.internal_field_id("name").expect("name field"),
                        CanonicalValue::string(format!("label-{ordinal}")).expect("label name"),
                    ),
                ])
                .expect("label fields"),
            )
            .expect("label record")
        };
        let mut state = MemoryState {
            entities: vec![label_record(1), label_record(2)],
            ..MemoryState::default()
        };
        state
            .entities
            .sort_unstable_by(|left, right| left.target().cmp(right.target()));
        let predicates = |ordinal: u8| {
            key_fields
                .iter()
                .map(|field| {
                    BoundPredicate::new(
                        field.clone(),
                        QueryPredicateOperator::Equal,
                        if field == "organization_id" {
                            organization.clone()
                        } else {
                            CanonicalValue::Uuid([ordinal; 16])
                        },
                    )
                })
                .collect::<Vec<_>>()
        };
        let parameters = QueryParameters::checked(BTreeMap::from([(
            "organization_id".to_owned(),
            organization.clone(),
        )]))
        .expect("parameters");
        let mut view = MemoryQueryView {
            state: &state,
            program: &program,
            parameters: &parameters,
        };
        let observed = view
            .dependent_point_batch(step, &[predicates(2), predicates(1), predicates(3)], None)
            .expect("batch");
        assert_eq!(observed.len(), 3);
        assert_eq!(
            observed[0].as_ref().and_then(|row| row.field("label_id")),
            Some(&CanonicalValue::Uuid([2; 16]))
        );
        assert_eq!(
            observed[1].as_ref().and_then(|row| row.field("label_id")),
            Some(&CanonicalValue::Uuid([1; 16]))
        );
        assert!(observed[2].is_none());

        let label_field = access.internal_field_id("label_id").expect("label field");
        let rule = RowPolicyRuleV1::new(
            RowPolicyOperationV1::Read,
            vec![
                RowPolicyExpressionNodeV1::Operand(RowPolicyOperandV1::new(
                    RowPolicyValueSourceV1::RowField(label_field),
                    ValueType::uuid(),
                )),
                RowPolicyExpressionNodeV1::Operand(RowPolicyOperandV1::new(
                    RowPolicyValueSourceV1::Constant(CanonicalValue::Uuid([2; 16])),
                    ValueType::uuid(),
                )),
                RowPolicyExpressionNodeV1::Equal { left: 0, right: 1 },
            ],
            2,
            step.internal_entity_id(),
            bundle.schema(),
            &BTreeMap::new(),
        )
        .expect("label policy rule");
        let policy = RowPolicyPlanV1::new(
            "OnlySecondLabel",
            step.internal_entity_id(),
            vec![rule],
            bundle.schema(),
        )
        .expect("label policy");
        let principal = PrincipalFactBindingV1::new(
            CapabilityId::from_unix_milliseconds_and_random(1, [0x61; 10]).expect("capability"),
            NonZeroU64::new(1).expect("revision"),
            DatabaseId::from_unix_milliseconds_and_random(1, [0x62; 10]).expect("database"),
            Environment::new("test").expect("environment"),
            ActorId::new("dependent-batch-policy-test").expect("actor"),
            ActorKind::Service,
            vec![Audience::new("riffdb-policy-test").expect("audience")],
            TenantScope::Global,
            Timestamp::new(1, 0).expect("issued"),
            Timestamp::new(10, 0).expect("expires"),
            CapabilityPrincipalFactsV1::empty(),
        )
        .expect("principal facts");
        let policy = AuthorizedQueryRowPolicyContextV1::test_fixture(
            principal,
            vec![policy],
            bundle.schema(),
        )
        .expect("policy context");
        let policy_observed = view
            .dependent_point_batch(step, &[predicates(1), predicates(2)], Some(&policy))
            .expect("policy batch");
        assert!(policy_observed[0].is_none());
        assert_eq!(
            policy_observed[1]
                .as_ref()
                .and_then(|row| row.field("label_id")),
            Some(&CanonicalValue::Uuid([2; 16]))
        );
    }
}
