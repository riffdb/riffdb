//! One-lock owned composite-query snapshots for the memory reference backend.

use std::collections::BTreeMap;

use riffdb_query_executor::{
    BoundPredicate, QueryBackendFault, QueryContinuation, QueryExecutionError, QueryExecutionPort,
    QueryOwnedSnapshot, QueryParameters, QueryReadView, QueryRow, QueryScanPage,
    execute_page_in_snapshot,
};
use riffdb_query_ir::{
    AccessDirection, QueryAccessKind, QueryAccessProgramV1, QueryAccessStep, QueryPredicateOperator,
};
use riffdb_storage_api::{EntityTarget, PartitionIndexTarget, StorageError, StorageErrorKind};
use riffdb_types::{CanonicalValue, IndexEntryKey};

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
        | StorageErrorKind::SequenceExhausted => QueryBackendFault::Integrity,
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
    ) -> Result<Option<QueryRow>, Self::Error> {
        let key_fields = match step.access() {
            QueryAccessKind::Point { key_fields }
            | QueryAccessKind::DependentPointBatch { key_fields, .. } => key_fields,
            QueryAccessKind::Index { .. } => {
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
            Ok(index) => row_from_record(self.program, step, &self.state.entities[index]).map(Some),
            Err(_) => Ok(None),
        }
    }

    fn dependent_point_batch(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[Vec<BoundPredicate>],
    ) -> Result<Vec<Option<QueryRow>>, Self::Error> {
        if !matches!(step.access(), QueryAccessKind::DependentPointBatch { .. }) {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        predicates
            .iter()
            .map(|predicates| self.point(step, predicates))
            .collect()
    }

    fn scan(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[BoundPredicate],
        limit: u64,
        after: Option<&[u8]>,
    ) -> Result<QueryScanPage, Self::Error> {
        let QueryAccessKind::Index {
            fields, direction, ..
        } = step.access()
        else {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        };
        let schema = step
            .internal_index_key_schema()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        let mut prefixes = index_prefixes(fields, predicates)?
            .into_iter()
            .map(|values| {
                schema
                    .encode_index_prefix(&values)
                    .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))
            })
            .collect::<Result<Vec<_>, _>>()?;
        prefixes.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        let unique_len = prefixes.len();
        prefixes.dedup_by(|left, right| left.as_bytes() == right.as_bytes());
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
        let mut entries = Vec::<(&MemoryIndexEntry, IndexEntryKey)>::new();
        'prefixes: for prefix in prefixes {
            let upper = exclusive_prefix_end(prefix.as_bytes())
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
            if after.is_some_and(|after| match direction {
                AccessDirection::Forward => upper.as_slice() <= after,
                AccessDirection::Reverse => prefix.as_bytes() > after,
            }) {
                continue;
            }
            let start =
                self.state
                    .index_entries
                    .partition_point(|entry| match (direction, after) {
                        (AccessDirection::Forward, Some(after))
                            if after.starts_with(prefix.as_bytes()) =>
                        {
                            entry.key().as_bytes() <= after
                        }
                        _ => entry.key().as_bytes() < prefix.as_bytes(),
                    });
            let end = self
                .state
                .index_entries
                .partition_point(|entry| match (direction, after) {
                    (AccessDirection::Reverse, Some(after))
                        if after.starts_with(prefix.as_bytes()) =>
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
                        entries.push((entry, entry.key().clone()));
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
                        entries.push((entry, entry.key().clone()));
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
        let scanned = entries.len();
        let has_more = scanned > page_limit;
        if has_more {
            entries.truncate(page_limit);
        }
        let scanned_rows =
            u64::try_from(scanned).map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
        let continuation = has_more
            .then(|| entries.last().map(|entry| entry.1.as_bytes().to_vec()))
            .flatten();
        let rows = entries
            .into_iter()
            .map(|(entry, _)| {
                let decoded = schema
                    .decode_index(entry.key())
                    .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
                let target =
                    EntityTarget::new(step.internal_entity_id(), decoded.entity_key().clone())
                        .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
                match unique_binary_search_by(&self.state.entities, |record| {
                    record.target().cmp(&target)
                })? {
                    Ok(index) => row_from_record(self.program, step, &self.state.entities[index]),
                    Err(_) => Err(storage_error(StorageErrorKind::CorruptData)),
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        match continuation {
            Some(continuation) => {
                QueryScanPage::continued(rows, epoch, scanned_rows.max(1), continuation)
                    .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))
            }
            None => Ok(QueryScanPage::exact_end(rows, epoch)),
        }
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

fn index_prefixes(
    fields: &[String],
    predicates: &[BoundPredicate],
) -> Result<Vec<Vec<CanonicalValue>>, StorageError> {
    let mut prefixes = vec![Vec::new()];
    for field in fields {
        let predicate = predicates
            .iter()
            .find(|predicate| predicate.field() == field);
        match predicate.map(BoundPredicate::operator) {
            Some(QueryPredicateOperator::Equal) => {
                let value = predicate
                    .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
                    .value()
                    .clone();
                for prefix in &mut prefixes {
                    prefix.push(value.clone());
                }
            }
            Some(QueryPredicateOperator::In) => {
                let CanonicalValue::List(values) = predicate
                    .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
                    .value()
                else {
                    return Err(storage_error(StorageErrorKind::InvariantViolation));
                };
                if values.values().is_empty() || values.values().len() > 1_024 {
                    return Err(storage_error(StorageErrorKind::LimitExceeded));
                }
                let prior = std::mem::take(&mut prefixes);
                for prefix in prior {
                    for value in values.values() {
                        let mut expanded = prefix.clone();
                        expanded.push(value.clone());
                        prefixes.push(expanded);
                    }
                }
                break;
            }
            _ => break,
        }
    }
    Ok(prefixes)
}

fn row_from_record(
    program: &QueryAccessProgramV1,
    step: &QueryAccessStep,
    record: &riffdb_storage_api::StoredEntityRecordV1,
) -> Result<QueryRow, StorageError> {
    let access = program
        .internal_entity_access(step.entity())
        .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
    let fields_by_id = record
        .fields()
        .fields()
        .iter()
        .map(|(id, value)| (*id, value))
        .collect::<BTreeMap<_, _>>();
    let fields = access
        .internal_fields()
        .map(|(name, id)| {
            fields_by_id
                .get(&id)
                .map(|value| (name.to_owned(), (*value).clone()))
                .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    QueryRow::checked(step.entity().to_owned(), fields)
        .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))
}

#[cfg(test)]
mod tests {
    use riffdb_contract_compiler::compile_contract_source;
    use riffdb_query_compiler::compile_query;
    use riffdb_query_executor::{
        QueryContinuation, QueryParameters, QueryResultValue, execute_in_snapshot,
        execute_page_in_snapshot,
    };
    use riffdb_query_ir::SymbolicCatalog;
    use riffdb_riffql_syntax::parse_query;
    use riffdb_storage_api::{
        DurableKeySchemaBindingV1, EncodedContentCharge, EntityTarget, StoredEntityRecordV1,
        StoredIndexEntryV2, encode_index_entry_v2,
    };
    use riffdb_types::{
        AggregateTypeId, CanonicalRecord, CanonicalValue, EntityVersion, PartitionKeyBuilder,
    };

    use super::*;

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
}
