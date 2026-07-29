//! One-lock owned composite-query snapshots for the memory reference backend.

use std::collections::BTreeMap;

use riffdb_query_executor::{
    BoundPredicate, QueryExecutionError, QueryExecutionPort, QueryOwnedSnapshot, QueryParameters,
    QueryReadView, QueryRow, QueryScanPage, execute_in_snapshot,
};
use riffdb_query_ir::{
    AccessDirection, QueryAccessKind, QueryAccessProgramV1, QueryAccessStep,
    QueryPredicateOperator,
};
use riffdb_storage_api::{EntityTarget, StorageError, StorageErrorKind};
use riffdb_types::{CanonicalValue, IndexEntryKey};

use crate::state::{MemoryIndexEntry, MemoryState, unique_binary_search_by};
use crate::store::{MemoryOperationalPorts, storage_error};

impl QueryExecutionPort for MemoryOperationalPorts {
    fn execute_query(
        &self,
        program: &QueryAccessProgramV1,
        parameters: &QueryParameters,
    ) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
        self.read(|state| {
            let mut view = MemoryQueryView { state, program };
            execute_in_snapshot(program, parameters, &mut view)
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))
        })
        .map_err(|_| QueryExecutionError::BackendUnavailable)
    }
}

struct MemoryQueryView<'a> {
    state: &'a MemoryState,
    program: &'a QueryAccessProgramV1,
}

impl QueryReadView for MemoryQueryView<'_> {
    type Error = StorageError;

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
        let QueryAccessKind::Point { key_fields } = step.access() else {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        };
        let values = key_fields
            .iter()
            .map(|field| exact_value(predicates, field))
            .collect::<Result<Vec<_>, _>>()?;
        if values.iter().any(|value| matches!(value, CanonicalValue::Null)) {
            return Ok(None);
        }
        let key = step
            .internal_entity_key_schema()
            .encode_entity(&values)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let target = EntityTarget::new(step.internal_entity_id(), key)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        match unique_binary_search_by(&self.state.entities, |record| {
            record.target().cmp(&target)
        })? {
            Ok(index) => row_from_record(self.program, step, &self.state.entities[index]).map(Some),
            Err(_) => Ok(None),
        }
    }

    fn scan(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[BoundPredicate],
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
        let prefixes = index_prefixes(fields, predicates)?;
        let epoch_prefix_values = equality_prefix(fields, predicates);
        let epoch_prefix = schema
            .encode_index_prefix(&epoch_prefix_values)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let epoch = self
            .state
            .index_epochs
            .iter()
            .find(|row| row.target().as_bytes() == epoch_prefix.as_bytes())
            .map_or(0, |row| row.epoch().get());

        let mut entries = Vec::<(&MemoryIndexEntry, IndexEntryKey)>::new();
        for prefix_values in prefixes {
            let prefix = schema
                .encode_index_prefix(&prefix_values)
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
            for entry in self
                .state
                .index_entries
                .iter()
                .filter(|entry| entry.key().as_bytes().starts_with(prefix.as_bytes()))
            {
                if entry.current_record().is_none() {
                    return Err(storage_error(StorageErrorKind::IncompatibleFormat));
                }
                entries.push((entry, entry.key().clone()));
                if entries.len() > 501 {
                    return Err(storage_error(StorageErrorKind::LimitExceeded));
                }
            }
        }
        entries.sort_unstable_by(|left, right| left.1.cmp(&right.1));
        entries.dedup_by(|left, right| left.1 == right.1);
        if *direction == AccessDirection::Reverse {
            entries.reverse();
        }
        let scanned_rows = u64::try_from(entries.len().min(500))
            .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
        let has_more = entries.len() > step.maximum_rows() as usize;
        entries.truncate(step.maximum_rows() as usize);
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

fn exact_value(
    predicates: &[BoundPredicate],
    field: &str,
) -> Result<CanonicalValue, StorageError> {
    predicates
        .iter()
        .find(|predicate| {
            predicate.field() == field && predicate.operator() == QueryPredicateOperator::Equal
        })
        .map(|predicate| predicate.value().clone())
        .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))
}

fn equality_prefix(fields: &[String], predicates: &[BoundPredicate]) -> Vec<CanonicalValue> {
    fields
        .iter()
        .map_while(|field| {
            predicates
                .iter()
                .find(|predicate| {
                    predicate.field() == field
                        && predicate.operator() == QueryPredicateOperator::Equal
                })
                .map(|predicate| predicate.value().clone())
        })
        .collect()
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
