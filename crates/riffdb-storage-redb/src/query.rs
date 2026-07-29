//! One-transaction owned composite-query snapshots for redb.

use std::collections::BTreeMap;

use redb::ReadOnlyTable;
use riffdb_query_executor::{
    BoundPredicate, QueryExecutionError, QueryExecutionPort, QueryOwnedSnapshot, QueryParameters,
    QueryReadView, QueryRow, QueryScanPage, execute_in_snapshot,
};
use riffdb_query_ir::{
    AccessDirection, QueryAccessKind, QueryAccessProgramV1, QueryAccessStep, QueryPredicateOperator,
};
use riffdb_storage_api::{EntityTarget, StorageError, StorageErrorKind};
use riffdb_types::{CanonicalValue, IndexEntryKey};

use crate::codec::{decode_index_entry_v2, decode_index_epoch_v1};
use crate::error::{precommit_storage_error, storage_error};
use crate::keys::decode_index_entry_key;
use crate::layout::{COMMITS, ENTITIES, INDEX_EPOCHS, SECONDARY_INDEXES};
use crate::reads::{read_commit_head, read_entity_record};
use crate::store::RedbOperationalPorts;

type BytesTable = ReadOnlyTable<&'static [u8], &'static [u8]>;

impl QueryExecutionPort for RedbOperationalPorts {
    fn execute_query(
        &self,
        program: &QueryAccessProgramV1,
        parameters: &QueryParameters,
    ) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
        let transaction = self
            .begin_read()
            .map_err(|_| QueryExecutionError::BackendUnavailable)?;
        let entities = transaction
            .open_table(ENTITIES)
            .map_err(|_| QueryExecutionError::BackendUnavailable)?;
        let indexes = transaction
            .open_table(SECONDARY_INDEXES)
            .map_err(|_| QueryExecutionError::BackendUnavailable)?;
        let epochs = transaction
            .open_table(INDEX_EPOCHS)
            .map_err(|_| QueryExecutionError::BackendUnavailable)?;
        let commits = transaction
            .open_table(COMMITS)
            .map_err(|_| QueryExecutionError::BackendUnavailable)?;
        let head = read_commit_head(&commits)
            .map_err(|_| QueryExecutionError::BackendUnavailable)?
            .map_or(0, riffdb_types::CommitSequence::get);
        let mut view = RedbQueryView {
            entities,
            indexes,
            epochs,
            head,
            program,
        };
        execute_in_snapshot(program, parameters, &mut view)
    }
}

struct RedbQueryView<'a> {
    entities: BytesTable,
    indexes: BytesTable,
    epochs: BytesTable,
    head: u64,
    program: &'a QueryAccessProgramV1,
}

impl QueryReadView for RedbQueryView<'_> {
    type Error = StorageError;

    fn application_head(&self) -> u64 {
        self.head
    }

    fn point(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[BoundPredicate],
    ) -> Result<Option<QueryRow>, Self::Error> {
        let QueryAccessKind::Point { key_fields } = step.access() else {
            return Err(invariant());
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
        read_entity_record(&self.entities, &target)?
            .map(|record| row_from_record(self.program, step, &record))
            .transpose()
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
            return Err(invariant());
        };
        let schema = step.internal_index_key_schema().ok_or_else(invariant)?;
        let epoch_values = equality_prefix(fields, predicates);
        let epoch_prefix = schema
            .encode_index_prefix(&epoch_values)
            .map_err(|_| invariant())?;
        let epoch = read_epoch(&self.epochs, epoch_prefix.as_bytes())?;
        let mut entries = Vec::<(IndexEntryKey, riffdb_storage_api::StoredIndexEntryV2)>::new();
        for values in index_prefixes(fields, predicates)? {
            let prefix = schema
                .encode_index_prefix(&values)
                .map_err(|_| invariant())?;
            let mut range = self
                .indexes
                .range(prefix.as_bytes()..)
                .map_err(precommit_storage_error)?;
            for entry in &mut range {
                let (physical_key, encoded) = entry.map_err(precommit_storage_error)?;
                if !physical_key.value().starts_with(prefix.as_bytes()) {
                    break;
                }
                let key = decode_index_entry_key(physical_key.value()).map_err(|_| corrupt())?;
                let decoded = decode_index_entry_v2(encoded.value())?.into_parts().0;
                if decoded.key() != &key {
                    return Err(corrupt());
                }
                entries.push((key, decoded));
                if entries.len() > 501 {
                    return Err(storage_error(StorageErrorKind::LimitExceeded));
                }
            }
        }
        entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        entries.dedup_by(|left, right| left.0 == right.0);
        if *direction == AccessDirection::Reverse {
            entries.reverse();
        }
        let scanned_rows = u64::try_from(entries.len().min(500))
            .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
        let has_more = entries.len() > step.maximum_rows() as usize;
        entries.truncate(step.maximum_rows() as usize);
        let continuation = has_more
            .then(|| entries.last().map(|entry| entry.0.as_bytes().to_vec()))
            .flatten();
        let rows = entries
            .into_iter()
            .map(|(key, _)| {
                let decoded = schema.decode_index(&key).map_err(|_| corrupt())?;
                let target =
                    EntityTarget::new(step.internal_entity_id(), decoded.entity_key().clone())
                        .map_err(|_| corrupt())?;
                let record = read_entity_record(&self.entities, &target)?.ok_or_else(corrupt)?;
                row_from_record(self.program, step, &record)
            })
            .collect::<Result<Vec<_>, _>>()?;
        match continuation {
            Some(continuation) => {
                QueryScanPage::continued(rows, epoch, scanned_rows.max(1), continuation)
                    .ok_or_else(invariant)
            }
            None => Ok(QueryScanPage::exact_end(rows, epoch)),
        }
    }
}

fn read_epoch(table: &BytesTable, prefix: &[u8]) -> Result<u64, StorageError> {
    let Some(encoded) = table.get(prefix).map_err(precommit_storage_error)? else {
        return Ok(0);
    };
    let epoch = decode_index_epoch_v1(encoded.value())?.into_parts().0;
    if epoch.target().as_bytes() != prefix {
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
    use riffdb_query_executor::{QueryExecutionPort, QueryParameters, QueryResultValue};
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
    const MEMBERS_QUERY: &str = include_str!("../../../queries/ticketdesk/project_members.riffq");
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
        let snapshot = ports.execute_query(&program, &parameters).expect("query");
        assert_eq!(snapshot.outcome(), "Found");
        assert!(matches!(
            snapshot.fields().get("ticket"),
            Some(QueryResultValue::One(row)) if row.field("title").is_some()
        ));
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
        for ordinal in [3_u8, 4_u8] {
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
            partition.push_uuid(&[1; 16]).expect("partition component");
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
        let snapshot = ports.execute_query(&program, &parameters).expect("query");
        assert!(matches!(
            snapshot.fields().get("members"),
            Some(QueryResultValue::Many(rows)) if rows.len() == 2
        ));
    }
}
