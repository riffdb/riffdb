//! One-transaction owned composite-query snapshots for redb.

use std::collections::BTreeMap;
use std::ops::Bound::{Excluded, Included};
use std::sync::atomic::{AtomicU64, Ordering};

use redb::{ReadOnlyTable, ReadTransaction, TableDefinition};
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

use crate::codec::{decode_index_entry_v2, decode_index_epoch_v1};
use crate::error::{precommit_storage_error, storage_error};
use crate::keys::{decode_index_entry_key, encode_partition_index_key};
use crate::layout::{COMMITS, ENTITIES, INDEX_EPOCHS, SECONDARY_INDEXES};
use crate::reads::{read_commit_head, read_entity_record};
use crate::store::RedbOperationalPorts;

type BytesTable = ReadOnlyTable<&'static [u8], &'static [u8]>;

/// Closed counters for falsifying lazy table opens on the composite-query path.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QueryTableOpenCounts {
    /// `COMMITS` opens (commit head is part of the read contract).
    pub commits: u64,
    /// `ENTITIES` opens.
    pub entities: u64,
    /// `SECONDARY_INDEXES` opens.
    pub indexes: u64,
    /// `INDEX_EPOCHS` opens.
    pub epochs: u64,
}

static QUERY_TABLE_OPENS_COMMITS: AtomicU64 = AtomicU64::new(0);
static QUERY_TABLE_OPENS_ENTITIES: AtomicU64 = AtomicU64::new(0);
static QUERY_TABLE_OPENS_INDEXES: AtomicU64 = AtomicU64::new(0);
static QUERY_TABLE_OPENS_EPOCHS: AtomicU64 = AtomicU64::new(0);

/// Resets composite-query table-open counters (test probe).
#[doc(hidden)]
pub fn reset_query_table_open_counts() {
    QUERY_TABLE_OPENS_COMMITS.store(0, Ordering::Relaxed);
    QUERY_TABLE_OPENS_ENTITIES.store(0, Ordering::Relaxed);
    QUERY_TABLE_OPENS_INDEXES.store(0, Ordering::Relaxed);
    QUERY_TABLE_OPENS_EPOCHS.store(0, Ordering::Relaxed);
}

/// Snapshot of composite-query table-open counters since the last reset.
#[doc(hidden)]
#[must_use]
pub fn query_table_open_counts() -> QueryTableOpenCounts {
    QueryTableOpenCounts {
        commits: QUERY_TABLE_OPENS_COMMITS.load(Ordering::Relaxed),
        entities: QUERY_TABLE_OPENS_ENTITIES.load(Ordering::Relaxed),
        indexes: QUERY_TABLE_OPENS_INDEXES.load(Ordering::Relaxed),
        epochs: QUERY_TABLE_OPENS_EPOCHS.load(Ordering::Relaxed),
    }
}

#[derive(Clone, Copy)]
enum QueryTableKind {
    Commits,
    Entities,
    Indexes,
    Epochs,
}

fn record_query_table_open(kind: QueryTableKind) {
    let counter = match kind {
        QueryTableKind::Commits => &QUERY_TABLE_OPENS_COMMITS,
        QueryTableKind::Entities => &QUERY_TABLE_OPENS_ENTITIES,
        QueryTableKind::Indexes => &QUERY_TABLE_OPENS_INDEXES,
        QueryTableKind::Epochs => &QUERY_TABLE_OPENS_EPOCHS,
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

impl QueryExecutionPort for RedbOperationalPorts {
    fn execute_query_page(
        &self,
        program: &QueryAccessProgramV1,
        parameters: &QueryParameters,
        prior: Option<&QueryContinuation>,
    ) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
        let transaction = self.begin_read().map_err(map_storage_query_error)?;
        // Commit head is part of the read contract and always runs first.
        let commits = open_query_table(&transaction, COMMITS, QueryTableKind::Commits)
            .map_err(map_storage_query_error)?;
        let head = read_commit_head(&transaction, &commits)
            .map_err(map_storage_query_error)?
            .map_or(0, riffdb_types::CommitSequence::get);
        drop(commits);
        // Entities / index / epoch tables open on first touch only.
        let mut view = RedbQueryView {
            transaction: &transaction,
            entities: None,
            indexes: None,
            epochs: None,
            head,
            program,
            parameters,
        };
        execute_page_in_snapshot(program, parameters, prior, &mut view)
    }
}

fn open_query_table(
    transaction: &ReadTransaction,
    definition: TableDefinition<&'static [u8], &'static [u8]>,
    kind: QueryTableKind,
) -> Result<BytesTable, StorageError> {
    record_query_table_open(kind);
    transaction
        .open_table(definition)
        .map_err(crate::error::table_error)
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

struct RedbQueryView<'a> {
    transaction: &'a ReadTransaction,
    entities: Option<BytesTable>,
    indexes: Option<BytesTable>,
    epochs: Option<BytesTable>,
    head: u64,
    program: &'a QueryAccessProgramV1,
    parameters: &'a QueryParameters,
}

impl RedbQueryView<'_> {
    fn entities_table(&mut self) -> Result<&BytesTable, StorageError> {
        if self.entities.is_none() {
            self.entities = Some(open_query_table(
                self.transaction,
                ENTITIES,
                QueryTableKind::Entities,
            )?);
        }
        Ok(self.entities.as_ref().expect("entities just opened"))
    }

    fn indexes_table(&mut self) -> Result<&BytesTable, StorageError> {
        if self.indexes.is_none() {
            self.indexes = Some(open_query_table(
                self.transaction,
                SECONDARY_INDEXES,
                QueryTableKind::Indexes,
            )?);
        }
        Ok(self.indexes.as_ref().expect("indexes just opened"))
    }

    fn epochs_table(&mut self) -> Result<&BytesTable, StorageError> {
        if self.epochs.is_none() {
            self.epochs = Some(open_query_table(
                self.transaction,
                INDEX_EPOCHS,
                QueryTableKind::Epochs,
            )?);
        }
        Ok(self.epochs.as_ref().expect("epochs just opened"))
    }
}

impl QueryReadView for RedbQueryView<'_> {
    type Error = StorageError;

    fn fault(&self, error: &Self::Error) -> QueryBackendFault {
        storage_query_fault(error)
    }

    fn application_head(&self) -> u64 {
        self.head
    }

    fn point(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[BoundPredicate],
    ) -> Result<Option<QueryRow>, Self::Error> {
        let key_fields = match step.access() {
            QueryAccessKind::Point { key_fields }
            | QueryAccessKind::DependentPointBatch { key_fields, .. } => key_fields,
            QueryAccessKind::Index { .. } => return Err(invariant()),
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
        let entities = self.entities_table()?;
        read_entity_record(entities, &target)?
            .map(|record| row_from_record(self.program, step, &record))
            .transpose()
    }

    fn dependent_point_batch(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[Vec<BoundPredicate>],
    ) -> Result<Vec<Option<QueryRow>>, Self::Error> {
        if !matches!(step.access(), QueryAccessKind::DependentPointBatch { .. }) {
            return Err(invariant());
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
            return Err(invariant());
        };
        let schema = step.internal_index_key_schema().ok_or_else(invariant)?;
        let partition_value = self
            .parameters
            .get(self.program.partition_parameter())
            .ok_or_else(invariant)?;
        let partition = step
            .internal_partition_key_schema()
            .encode_partition(std::slice::from_ref(partition_value))
            .map_err(|_| invariant())?;
        let generation_target =
            PartitionIndexTarget::new(partition, step.internal_index_id().ok_or_else(invariant)?);
        let epoch = {
            let epochs = self.epochs_table()?;
            read_epoch(epochs, &generation_target)?
        };
        let page_limit =
            usize::try_from(limit).map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
        let fetch_limit = page_limit.saturating_add(1);
        let mut entries = Vec::<(IndexEntryKey, riffdb_storage_api::StoredIndexEntryV2)>::new();
        let mut prefixes = index_prefixes(fields, predicates)?
            .into_iter()
            .map(|values| schema.encode_index_prefix(&values).map_err(|_| invariant()))
            .collect::<Result<Vec<_>, _>>()?;
        prefixes.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        let unique_len = prefixes.len();
        prefixes.dedup_by(|left, right| left.as_bytes() == right.as_bytes());
        if prefixes.len() != unique_len {
            return Err(invariant());
        }
        if *direction == AccessDirection::Reverse {
            prefixes.reverse();
        }

        {
            let indexes = self.indexes_table()?;
            'prefixes: for prefix in prefixes {
                let upper = exclusive_prefix_end(prefix.as_bytes()).ok_or_else(invariant)?;
                if after.is_some_and(|after| match direction {
                    AccessDirection::Forward => upper.as_slice() <= after,
                    AccessDirection::Reverse => prefix.as_bytes() > after,
                }) {
                    continue;
                }
                let lower_bound = match (direction, after) {
                    (AccessDirection::Forward, Some(after))
                        if after.starts_with(prefix.as_bytes()) =>
                    {
                        Excluded(after)
                    }
                    _ => Included(prefix.as_bytes()),
                };
                let upper_bound = match (direction, after) {
                    (AccessDirection::Reverse, Some(after))
                        if after.starts_with(prefix.as_bytes()) =>
                    {
                        Excluded(after)
                    }
                    _ => Excluded(upper.as_slice()),
                };
                let mut range = indexes
                    .range::<&[u8]>((lower_bound, upper_bound))
                    .map_err(precommit_storage_error)?;
                match direction {
                    AccessDirection::Forward => {
                        for entry in &mut range {
                            let decoded = decode_current_index_entry(
                                entry.map_err(precommit_storage_error)?,
                            )?;
                            if decoded.1.partition_key() != generation_target.partition_key() {
                                continue;
                            }
                            entries.push(decoded);
                            if entries.len() == fetch_limit {
                                break 'prefixes;
                            }
                        }
                    }
                    AccessDirection::Reverse => {
                        while let Some(entry) = range.next_back() {
                            let decoded = decode_current_index_entry(
                                entry.map_err(precommit_storage_error)?,
                            )?;
                            if decoded.1.partition_key() != generation_target.partition_key() {
                                continue;
                            }
                            entries.push(decoded);
                            if entries.len() == fetch_limit {
                                break 'prefixes;
                            }
                        }
                    }
                }
            }
        }
        // Continuation only when an extra matching entry was observed. Bound is
        // the last included key; the peeked row is never returned. Charge the
        // peeked observation to scanned_rows for accurate fuel accounting.
        let scanned = entries.len();
        let has_more = scanned > page_limit;
        if has_more {
            entries.truncate(page_limit);
        }
        let scanned_rows =
            u64::try_from(scanned).map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
        let continuation = has_more
            .then(|| entries.last().map(|entry| entry.0.as_bytes().to_vec()))
            .flatten();
        let mut rows = Vec::with_capacity(entries.len());
        for (key, _) in entries {
            let decoded = schema.decode_index(&key).map_err(|_| corrupt())?;
            let target = EntityTarget::new(step.internal_entity_id(), decoded.entity_key().clone())
                .map_err(|_| corrupt())?;
            let record = {
                let entities = self.entities_table()?;
                read_entity_record(entities, &target)?.ok_or_else(corrupt)?
            };
            rows.push(row_from_record(self.program, step, &record)?);
        }
        match continuation {
            Some(continuation) => {
                QueryScanPage::continued(rows, epoch, scanned_rows.max(1), continuation)
                    .ok_or_else(invariant)
            }
            None => Ok(QueryScanPage::exact_end(rows, epoch)),
        }
    }
}

fn decode_current_index_entry(
    entry: (
        redb::AccessGuard<'_, &'static [u8]>,
        redb::AccessGuard<'_, &'static [u8]>,
    ),
) -> Result<(IndexEntryKey, riffdb_storage_api::StoredIndexEntryV2), StorageError> {
    let (physical_key, encoded) = entry;
    let key = decode_index_entry_key(physical_key.value()).map_err(|_| corrupt())?;
    let decoded = decode_index_entry_v2(encoded.value())?.into_parts().0;
    if decoded.key() != &key {
        return Err(corrupt());
    }
    Ok((key, decoded))
}

fn exclusive_prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    let position = upper.iter().rposition(|byte| *byte != u8::MAX)?;
    upper[position] = upper[position].saturating_add(1);
    upper.truncate(position + 1);
    Some(upper)
}

fn read_epoch(table: &BytesTable, target: &PartitionIndexTarget) -> Result<u64, StorageError> {
    let key = encode_partition_index_key(target);
    let Some(encoded) = table.get(key.as_slice()).map_err(precommit_storage_error)? else {
        return Ok(0);
    };
    let epoch = decode_index_epoch_v1(encoded.value())?.into_parts().0;
    if epoch.target() != target {
        return Err(corrupt());
    }
    Ok(epoch.epoch().get())
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
                let value = predicate.ok_or_else(invariant)?.value().clone();
                for prefix in &mut prefixes {
                    prefix.push(value.clone());
                }
            }
            Some(QueryPredicateOperator::In) => {
                let CanonicalValue::List(values) = predicate.ok_or_else(invariant)?.value() else {
                    return Err(invariant());
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
        .ok_or_else(invariant)?;
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
                .ok_or_else(corrupt)
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    QueryRow::checked(step.entity().to_owned(), fields)
        .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))
}

const fn invariant() -> StorageError {
    storage_error(StorageErrorKind::InvariantViolation)
}

const fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use riffdb_contract_compiler::compile_contract_source;
    use riffdb_query_compiler::compile_query;
    use riffdb_query_executor::{
        QueryContinuation, QueryExecutionPort, QueryParameters, QueryResultValue,
    };
    use riffdb_query_ir::SymbolicCatalog;
    use riffdb_riffql_syntax::parse_query;
    use riffdb_storage_api::{
        DatabaseInitializationPort, DurableKeySchemaBindingV1, EntityTarget, StoredEntityRecordV1,
        StoredIndexEntryV2,
    };
    use riffdb_types::{
        AggregateTypeId, CanonicalRecord, CanonicalValue, DatabaseId, EntityVersion,
        PartitionKeyBuilder,
    };

    use super::*;
    use crate::codec::{encode_entity_record_v1, encode_index_entry_v2};
    use crate::keys::encode_entity_key;
    use crate::store::RedbStore;

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
    static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

    struct TestPath(PathBuf);

    impl TestPath {
        fn new() -> Self {
            Self(std::env::temp_dir().join(format!(
                "riffdb-redb-query-{}-{}.redb",
                std::process::id(),
                NEXT_PATH.fetch_add(1, Ordering::Relaxed)
            )))
        }
    }

    impl Drop for TestPath {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    #[test]
    fn point_query_executes_inside_one_redb_read_transaction() {
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
                CanonicalValue::string("one transaction").expect("title"),
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

        let path = TestPath::new();
        let mut store = RedbStore::open(&path.0).expect("store");
        let database_id =
            DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x11; 10])
                .expect("database ID");
        store.initialize_database(database_id).expect("initialize");
        let ports = RedbOperationalPorts {
            shared: Arc::clone(&store.shared),
        };
        let access_write = ports.begin_write().expect("write");
        {
            let encoded = encode_entity_record_v1(&record).expect("encode");
            let mut table = access_write
                .transaction()
                .expect("transaction")
                .open_table(ENTITIES)
                .expect("entities");
            table
                .insert(encode_entity_key(record.target().key()), encoded.as_bytes())
                .expect("insert");
        }
        access_write.commit().expect("commit");

        let parameters = QueryParameters::checked(BTreeMap::from([
            ("organization_id".to_owned(), organization),
            ("ticket_id".to_owned(), ticket),
        ]))
        .expect("parameters");
        crate::reset_query_table_open_counts();
        let snapshot = ports.execute_query(&program, &parameters).expect("query");
        assert_eq!(snapshot.outcome(), "Found");
        assert!(matches!(
            snapshot.fields().get("ticket"),
            Some(QueryResultValue::One(row)) if row.field("title").is_some()
        ));
        let opens = crate::query_table_open_counts();
        assert_eq!(
            opens,
            QueryTableOpenCounts {
                commits: 1,
                entities: 1,
                indexes: 0,
                epochs: 0,
            },
            "point query opens only commits (head) and entities; eager open would also open indexes/epochs"
        );
    }

    #[test]
    fn index_page_batches_entity_reads_inside_the_same_redb_transaction() {
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
        let mut records = Vec::new();
        let mut indexes = Vec::new();
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
            records.push(
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
            indexes.push(
                StoredIndexEntryV2::new(
                    index_key,
                    binding.clone(),
                    CanonicalRecord::new(Vec::new()).expect("cover"),
                    partition.finish().expect("partition"),
                )
                .expect("index row"),
            );
        }

        let path = TestPath::new();
        let mut store = RedbStore::open(&path.0).expect("store");
        let database_id =
            DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x12; 10])
                .expect("database ID");
        store.initialize_database(database_id).expect("initialize");
        let ports = RedbOperationalPorts {
            shared: Arc::clone(&store.shared),
        };
        let access_write = ports.begin_write().expect("write");
        {
            let mut table = access_write
                .transaction()
                .expect("transaction")
                .open_table(ENTITIES)
                .expect("entities");
            for record in &records {
                let encoded = encode_entity_record_v1(record).expect("encode entity");
                table
                    .insert(encode_entity_key(record.target().key()), encoded.as_bytes())
                    .expect("insert entity");
            }
        }
        {
            let mut table = access_write
                .transaction()
                .expect("transaction")
                .open_table(SECONDARY_INDEXES)
                .expect("indexes");
            for index in &indexes {
                let encoded = encode_index_entry_v2(index).expect("encode index");
                table
                    .insert(index.key().as_bytes(), encoded.as_bytes())
                    .expect("insert index");
            }
        }
        access_write.commit().expect("commit");

        let parameters = QueryParameters::checked(BTreeMap::from([
            ("organization_id".to_owned(), organization),
            ("project_id".to_owned(), project),
        ]))
        .expect("parameters");
        crate::reset_query_table_open_counts();
        let snapshot = ports.execute_query(&program, &parameters).expect("query");
        let opens = crate::query_table_open_counts();
        assert_eq!(
            opens,
            QueryTableOpenCounts {
                commits: 1,
                entities: 1,
                indexes: 1,
                epochs: 1,
            },
            "index-step query opens commits, entities, indexes, and epochs exactly once each"
        );
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
        let second = ports
            .execute_query_page(&program, &parameters, Some(&cursor))
            .expect("second page");
        assert!(matches!(
            second.fields().get("members"),
            Some(QueryResultValue::Many(rows))
                if rows.len() == 1 && rows[0].field("user_id").cloned() != first_user
        ));
    }

    #[test]
    fn exact_end_page_mints_no_continuation_when_page_fills_the_range() {
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
        let mut records = Vec::new();
        let mut indexes = Vec::new();
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
            records.push(
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
            indexes.push(
                StoredIndexEntryV2::new(
                    index_key,
                    binding.clone(),
                    CanonicalRecord::new(Vec::new()).expect("cover"),
                    partition.finish().expect("partition"),
                )
                .expect("index row"),
            );
        }

        let path = TestPath::new();
        let mut store = RedbStore::open(&path.0).expect("store");
        let database_id =
            DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x22; 10])
                .expect("database ID");
        store.initialize_database(database_id).expect("initialize");
        let ports = RedbOperationalPorts {
            shared: Arc::clone(&store.shared),
        };
        let access_write = ports.begin_write().expect("write");
        {
            let mut table = access_write
                .transaction()
                .expect("transaction")
                .open_table(ENTITIES)
                .expect("entities");
            for record in &records {
                let encoded = encode_entity_record_v1(record).expect("encode entity");
                table
                    .insert(encode_entity_key(record.target().key()), encoded.as_bytes())
                    .expect("insert entity");
            }
        }
        {
            let mut table = access_write
                .transaction()
                .expect("transaction")
                .open_table(SECONDARY_INDEXES)
                .expect("indexes");
            for index in &indexes {
                let encoded = encode_index_entry_v2(index).expect("encode index");
                table
                    .insert(index.key().as_bytes(), encoded.as_bytes())
                    .expect("insert index");
            }
        }
        access_write.commit().expect("commit");

        let parameters = QueryParameters::checked(BTreeMap::from([
            ("organization_id".to_owned(), organization),
            ("project_id".to_owned(), project),
        ]))
        .expect("parameters");
        let snapshot = ports.execute_query(&program, &parameters).expect("query");
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
