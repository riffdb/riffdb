//! One-lock owned composite-query snapshots for the memory reference backend.

#[cfg(test)]
use std::cell::Cell;
use std::collections::BTreeMap;
use std::sync::Arc;

use riffdb_policy::{AuthorizedIndexedRelationshipLookupV1, AuthorizedQueryRowPolicyContextV1};
use riffdb_query_executor::{
    BoundPredicate, QueryBackendFault, QueryContinuation, QueryExecutionError, QueryExecutionPort,
    QueryExecutionRequest, QueryNearestPage, QueryOwnedSnapshot, QueryParameters, QueryReadView,
    QueryRow, QueryScanPage, execute_in_snapshot, execute_operational_page_in_snapshot,
    execute_page_in_snapshot, execute_policy_operational_page_in_snapshot,
    execute_policy_page_in_snapshot, validate_query_execution_group,
};
use riffdb_query_ir::{
    AccessDirection, OperationalAggregateV1, QueryAccessKind, QueryAccessProgramV1,
    QueryAccessStep, QueryPredicateOperator,
};
use riffdb_storage_api::{EntityTarget, PartitionIndexTarget, StorageError, StorageErrorKind};
use riffdb_types::{CanonicalValue, FieldId, IndexEntryKey};

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
        if !matches!(step.access(), QueryAccessKind::DependentPointBatch { .. }) {
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
        policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<QueryScanPage, Self::Error> {
        let QueryAccessKind::Index { direction, .. } = step.access() else {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        };
        let schema = step
            .internal_index_key_schema()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        let mut prefixes = riffdb_query_executor::bound_index_prefix_bytes_v1(step, predicates)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        prefixes.sort_unstable();
        let unique_len = prefixes.len();
        prefixes.dedup();
        if prefixes.len() != unique_len {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        if *direction == AccessDirection::Reverse {
            prefixes.reverse();
        }
        let partition_value = self
            .parameters
            .get(self.program.partition_parameter())
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
        'prefixes: for prefix in prefixes {
            let upper = exclusive_prefix_end(prefix.as_slice())
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
            if after.is_some_and(|after| match direction {
                AccessDirection::Forward => upper.as_slice() <= after,
                AccessDirection::Reverse => prefix.as_slice() > after,
            }) {
                continue;
            }
            let start =
                self.state
                    .index_entries
                    .partition_point(|entry| match (direction, after) {
                        (AccessDirection::Forward, Some(after))
                            if after.starts_with(prefix.as_slice()) =>
                        {
                            entry.key().as_bytes() <= after
                        }
                        _ => entry.key().as_bytes() < prefix.as_slice(),
                    });
            let end = self
                .state
                .index_entries
                .partition_point(|entry| match (direction, after) {
                    (AccessDirection::Reverse, Some(after))
                        if after.starts_with(prefix.as_slice()) =>
                    {
                        entry.key().as_bytes() < after
                    }
                    _ => entry.key().as_bytes() < upper.as_slice(),
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
                            break 'prefixes;
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
                            break 'prefixes;
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
            | QueryAccessKind::DependentPointBatch { key_fields, .. } => key_fields,
            QueryAccessKind::Index { .. } => {
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
        let upper = exclusive_prefix_end(lookup.index_prefix())
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        let start = self
            .state
            .index_entries
            .partition_point(|entry| entry.key().as_bytes() < lookup.index_prefix());
        let end = self
            .state
            .index_entries
            .partition_point(|entry| entry.key().as_bytes() < upper.as_slice());
        let matching = self
            .state
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
        assert_eq!(snapshot.outcome(), "Found");
        assert!(matches!(
            snapshot.fields().get("ticket"),
            Some(QueryResultValue::One(row)) if row.field("title").is_some()
        ));
    }

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
        let mut second_view = MemoryQueryView {
            state: &state,
            program: &program,
            parameters: &parameters,
        };
        let second =
            execute_page_in_snapshot(&program, &parameters, Some(&cursor), &mut second_view)
                .expect("second page");
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
        let policy = AuthorizedQueryRowPolicyContextV1::test_fixture(principal, vec![policy])
            .expect("policy context");
        let mut policy_view = MemoryQueryView {
            state: &state,
            program: &program,
            parameters: &parameters,
        };
        let filtered =
            execute_policy_page_in_snapshot(&program, &parameters, None, &mut policy_view, &policy)
                .expect("policy-filtered page");
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
}
