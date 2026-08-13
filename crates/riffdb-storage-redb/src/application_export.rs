//! Durable exact application-export operation repository and immutable source.

use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};

use redb::ReadableTable;
use riffdb_storage_api::{
    ApplicationExportEventRecordV1, ApplicationExportOperationRepository,
    ApplicationExportOperationWriteResultV1, ApplicationExportSnapshotPort,
    ApplicationExportSnapshotReader, ApplicationExportSourcePageV1,
    ApplicationExportSourceRecordV1, DurableCodecErrorKind, MAX_ACTIVE_APPLICATION_EXPORTS,
    MAX_APPLICATION_EXPORT_CONTINUATION_BYTES, MAX_APPLICATION_EXPORT_SOURCE_PAGE_BYTES,
    MAX_RETAINED_REACTIVE_MODULES, StorageError, StorageErrorKind, StorageScanLimit,
    StoredApplicationExportOperationV1,
};
use riffdb_types::{
    ApplicationExportClassV1, ApplicationExportOperationId, ApplicationExportSnapshotBindingV1,
    ContractLineage, EntityKeyBuilder, EntityTypeId, QueryModuleHash, ReactiveModuleHash,
};

use crate::codec::{
    decode_active_catalog_pointer_v1, decode_application_export_operation_v1,
    decode_database_identity_v1, decode_entity_record_v1, decode_history_incarnation_v1,
    decode_index_entry_v2, decode_provenance_record_v1, decode_query_module_administration_v1,
    decode_reactive_module_v1, encode_application_export_operation_v1,
};
use crate::error::{codec_error, precommit_storage_error, storage_error, table_error};
use crate::hooks::RedbTestOperation;
use crate::journal::JournalTable;
use crate::keys::{
    decode_audit_key, decode_entity_key, decode_event_key, decode_index_entry_key,
    decode_provenance_key, encode_active_query_module_key, encode_contract_bundle_key,
    encode_entity_key,
};
use crate::layout::{
    APPLICATION_EXPORT_OPERATIONS, CATALOG_ACTIVE, CATALOG_ACTIVE_KEY, CONTRACT_BUNDLES,
    META_DATABASE_ID, META_HISTORY_INCARNATION, QUERY_MODULE_ACTIVE, REACTIVE_MODULES,
};
use crate::store::{RedbOperationalPorts, RedbReadAccess};

fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}

/// One pinned redb/composite publication root. The mutex protects redb's
/// transaction value without serializing unrelated database readers; each
/// export operation owns its own root.
struct RedbApplicationExportSnapshot {
    ports: RedbOperationalPorts,
    access: Mutex<RedbReadAccess>,
    binding: ApplicationExportSnapshotBindingV1,
    contract_bundle_bytes: Arc<[u8]>,
    lineage: ContractLineage,
}

impl RedbApplicationExportSnapshot {
    fn decode_source_record(
        &self,
        access: &RedbReadAccess,
        class: ApplicationExportClassV1,
        key: &[u8],
        encoded: &[u8],
    ) -> Result<ApplicationExportSourceRecordV1, StorageError> {
        match class {
            ApplicationExportClassV1::Entity => {
                let physical = decode_entity_key(key).map_err(|_| corrupt())?;
                let record = decode_entity_record_v1(encoded)?.into_parts().0;
                if record.target().key() != &physical
                    || record.schema_binding().lineage() != &self.lineage
                {
                    return Err(corrupt());
                }
                Ok(ApplicationExportSourceRecordV1::Entity(Box::new(record)))
            }
            ApplicationExportClassV1::Event => {
                let physical = decode_event_key(key).map_err(|_| corrupt())?;
                let record = match riffdb_storage_api::decode_durable_event_v2(encoded) {
                    Ok(record) => {
                        let record = record.into_parts().0;
                        if record.event_id() != physical
                            || record.policy_anchor().contract().lineage() != &self.lineage
                        {
                            return Err(corrupt());
                        }
                        ApplicationExportEventRecordV1::V2(record)
                    }
                    Err(error) if error.kind() == DurableCodecErrorKind::UnexpectedRecordType => {
                        let record = riffdb_storage_api::decode_durable_event_v1(encoded)
                            .map_err(codec_error)?
                            .into_parts()
                            .0;
                        if record.event_id() != physical {
                            return Err(corrupt());
                        }
                        ApplicationExportEventRecordV1::V1(record)
                    }
                    Err(error) => return Err(codec_error(error)),
                };
                Ok(ApplicationExportSourceRecordV1::Event(Box::new(record)))
            }
            ApplicationExportClassV1::Provenance => {
                let physical = decode_provenance_key(key).map_err(|_| corrupt())?;
                let record = decode_provenance_record_v1(encoded)?.into_parts().0;
                if record.provenance_id() != physical
                    || record.identity().contract_lineage() != &self.lineage
                {
                    return Err(corrupt());
                }
                Ok(ApplicationExportSourceRecordV1::Provenance(Box::new(
                    record,
                )))
            }
            ApplicationExportClassV1::PublicAudit => {
                let physical = decode_audit_key(key).map_err(|_| corrupt())?;
                let record = crate::administration::read_administration_record_access(
                    &self.ports,
                    access,
                    physical,
                )?;
                Ok(ApplicationExportSourceRecordV1::PublicAudit(Box::new(
                    record,
                )))
            }
        }
    }

    fn read_source_page_range(
        &self,
        class: ApplicationExportClassV1,
        start: Vec<u8>,
        end_exclusive: Option<&[u8]>,
        limit: StorageScanLimit,
    ) -> Result<ApplicationExportSourcePageV1, StorageError> {
        let requested = usize::from(limit.get());
        let inspected = requested
            .checked_add(1)
            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        let table = match class {
            ApplicationExportClassV1::Entity => JournalTable::Entities,
            ApplicationExportClassV1::Event => JournalTable::Events,
            ApplicationExportClassV1::Provenance => JournalTable::Provenance,
            ApplicationExportClassV1::PublicAudit => JournalTable::Audit,
        };
        let access = self
            .access
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let rows = access.read_range_to(table, &start, end_exclusive, inspected)?;
        let mut records = Vec::with_capacity(requested.min(rows.len()));
        let mut encoded_bytes = 0usize;
        let mut continuation = None;
        let mut stopped_early = false;
        for (key, encoded) in rows.iter().take(requested) {
            let next_bytes = encoded_bytes
                .checked_add(encoded.len())
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            if next_bytes > MAX_APPLICATION_EXPORT_SOURCE_PAGE_BYTES {
                stopped_early = true;
                break;
            }
            records.push(self.decode_source_record(&access, class, key, encoded)?);
            encoded_bytes = next_bytes;
            continuation = Some(key.clone());
        }
        let exact_end = !stopped_early && rows.len() <= records.len();
        if exact_end {
            continuation = None;
        } else if continuation.is_none() {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        ApplicationExportSourcePageV1::new(class, records, continuation, exact_end, encoded_bytes)
            .map_err(|error| match error {
                riffdb_storage_api::StorageValueError::LimitExceeded => {
                    storage_error(StorageErrorKind::LimitExceeded)
                }
                _ => storage_error(StorageErrorKind::InvariantViolation),
            })
    }
}

impl ApplicationExportSnapshotReader for RedbApplicationExportSnapshot {
    fn binding(&self) -> &ApplicationExportSnapshotBindingV1 {
        &self.binding
    }

    fn contract_bundle_bytes(&self) -> &[u8] {
        &self.contract_bundle_bytes
    }

    fn read_application_export_source_page(
        &self,
        class: ApplicationExportClassV1,
        after: Option<&[u8]>,
        limit: StorageScanLimit,
    ) -> Result<ApplicationExportSourcePageV1, StorageError> {
        if after.is_some_and(|value| {
            value.is_empty() || value.len() > MAX_APPLICATION_EXPORT_CONTINUATION_BYTES
        }) {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        let mut start = after.map_or_else(Vec::new, ToOwned::to_owned);
        if after.is_some() {
            start.push(0);
        }
        self.read_source_page_range(class, start, None, limit)
    }

    fn read_application_export_entity_page(
        &self,
        entity_type: EntityTypeId,
        after: Option<&[u8]>,
        limit: StorageScanLimit,
    ) -> Result<ApplicationExportSourcePageV1, StorageError> {
        let prefix = EntityKeyBuilder::new(entity_type).as_bytes().to_vec();
        if after.is_some_and(|value| {
            value.is_empty()
                || value.len() > MAX_APPLICATION_EXPORT_CONTINUATION_BYTES
                || !value.starts_with(&prefix)
        }) {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        let mut start = after.map_or_else(|| prefix.clone(), ToOwned::to_owned);
        if after.is_some() {
            start.push(0);
        }
        let end = exclusive_prefix_end(&prefix)
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        self.read_source_page_range(ApplicationExportClassV1::Entity, start, Some(&end), limit)
    }

    fn application_export_indexed_relationship_exists(
        &self,
        index_prefix: &[u8],
        partition: &riffdb_types::PartitionKey,
    ) -> Result<bool, StorageError> {
        if index_prefix.is_empty() {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let upper = exclusive_prefix_end(index_prefix)
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        let access = self
            .access
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let rows = access.read_range(
            JournalTable::SecondaryIndexes,
            index_prefix,
            &upper,
            riffdb_storage_api::MAX_SCAN_PAGE_ENTRIES,
        )?;
        if rows.len() == riffdb_storage_api::MAX_SCAN_PAGE_ENTRIES {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        for (key, encoded) in rows {
            let key = decode_index_entry_key(&key).map_err(|_| corrupt())?;
            let entry = decode_index_entry_v2(&encoded)?.into_parts().0;
            if entry.key() != &key {
                return Err(corrupt());
            }
            if entry.partition_key() == partition {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn read_application_export_policy_anchor(
        &self,
        target: &riffdb_storage_api::EntityTarget,
    ) -> Result<Option<riffdb_storage_api::StoredEntityRecordV1>, StorageError> {
        let access = self
            .access
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let Some(encoded) =
            access.read_value(JournalTable::Entities, encode_entity_key(target.key()))?
        else {
            return Ok(None);
        };
        let record = decode_entity_record_v1(&encoded)?.into_parts().0;
        if record.target() != target || record.schema_binding().lineage() != &self.lineage {
            return Err(corrupt());
        }
        Ok(Some(record))
    }
}

fn exclusive_prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    let position = upper.iter().rposition(|byte| *byte != u8::MAX)?;
    upper[position] = upper[position].checked_add(1)?;
    upper.truncate(position + 1);
    Some(upper)
}

impl ApplicationExportSnapshotPort for RedbOperationalPorts {
    fn capture_application_export_snapshot(
        &self,
        lineage: &ContractLineage,
    ) -> Result<Arc<dyn ApplicationExportSnapshotReader>, StorageError> {
        let access = self.begin_composite_read()?;
        let database_id = access
            .read_value(JournalTable::Meta, META_DATABASE_ID.as_bytes())?
            .ok_or_else(corrupt)
            .and_then(|value| decode_database_identity_v1(&value).map(|item| *item.value()))?;
        let history_incarnation = access
            .read_value(JournalTable::Meta, META_HISTORY_INCARNATION.as_bytes())?
            .ok_or_else(corrupt)
            .and_then(|value| decode_history_incarnation_v1(&value).map(|item| *item.value()))?;
        let history_incarnation = NonZeroU64::new(history_incarnation).ok_or_else(corrupt)?;

        let catalog = access.open_table(CATALOG_ACTIVE).map_err(table_error)?;
        let active = catalog
            .get(CATALOG_ACTIVE_KEY.as_slice())
            .map_err(precommit_storage_error)?
            .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?;
        let active = decode_active_catalog_pointer_v1(active.value())?
            .into_parts()
            .0;
        if active.lineage() != lineage {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        drop(catalog);

        let bundle_key = encode_contract_bundle_key(lineage, active.contract_version())
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let bundles = access.open_table(CONTRACT_BUNDLES).map_err(table_error)?;
        let bundle = bundles
            .get(bundle_key.as_slice())
            .map_err(precommit_storage_error)?
            .ok_or_else(corrupt)?;
        let bundle = crate::codec::decode_contract_bundle_v1(bundle.value())?
            .into_parts()
            .0;
        if !active.matches_bundle(&bundle) {
            return Err(corrupt());
        }
        drop(bundles);

        let active_query_key = encode_active_query_module_key(
            lineage,
            active.contract_version(),
            active.bundle_hash(),
        )
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let active_queries = access
            .open_table(QUERY_MODULE_ACTIVE)
            .map_err(table_error)?;
        let query_modules = active_queries
            .get(active_query_key.as_slice())
            .map_err(precommit_storage_error)?
            .map(|encoded| {
                decode_query_module_administration_v1(encoded.value()).and_then(|item| {
                    let record = item.into_parts().0;
                    let pointer = record.activated();
                    if pointer.contract_lineage() != lineage
                        || pointer.contract_version() != active.contract_version()
                        || pointer.contract_bundle_hash() != active.bundle_hash()
                    {
                        return Err(corrupt());
                    }
                    Ok(pointer.module_hash())
                })
            })
            .transpose()?
            .into_iter()
            .collect::<Vec<QueryModuleHash>>();
        drop(active_queries);

        let reactive = access.open_table(REACTIVE_MODULES).map_err(table_error)?;
        let mut reactive_modules = Vec::<ReactiveModuleHash>::new();
        for row in reactive.iter().map_err(precommit_storage_error)? {
            if reactive_modules.len() == MAX_RETAINED_REACTIVE_MODULES {
                return Err(corrupt());
            }
            let (key, encoded) = row.map_err(precommit_storage_error)?;
            let module = decode_reactive_module_v1(encoded.value())?.into_parts().0;
            if key.value() != module.module_hash().as_bytes() {
                return Err(corrupt());
            }
            if module.contract_lineage() == lineage
                && module.contract_version() == active.contract_version()
                && module.contract_bundle_hash() == active.bundle_hash()
            {
                reactive_modules.push(module.module_hash());
            }
        }
        reactive_modules.sort_unstable();
        if reactive_modules.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(corrupt());
        }

        let binding = ApplicationExportSnapshotBindingV1::new(
            database_id,
            history_incarnation,
            access.application_frontier()?,
            access.administration_frontier()?,
            active.contract_version(),
            active.bundle_hash(),
            query_modules,
            reactive_modules,
        )
        .map_err(|_| corrupt())?;
        Ok(Arc::new(RedbApplicationExportSnapshot {
            ports: RedbOperationalPorts {
                shared: Arc::clone(&self.shared),
            },
            access: Mutex::new(access),
            binding,
            contract_bundle_bytes: Arc::from(bundle.canonical_bytes()),
            lineage: lineage.clone(),
        }))
    }
}

fn decode_row(
    key: &[u8],
    value: &[u8],
) -> Result<StoredApplicationExportOperationV1, StorageError> {
    let operation = decode_application_export_operation_v1(value)?
        .into_parts()
        .0;
    if key != operation.operation_id().as_bytes() {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    Ok(operation)
}

impl ApplicationExportOperationRepository for RedbOperationalPorts {
    fn read_application_export_operation(
        &self,
        operation_id: ApplicationExportOperationId,
    ) -> Result<Option<StoredApplicationExportOperationV1>, StorageError> {
        let transaction = self.begin_read()?;
        let table = transaction
            .open_table(APPLICATION_EXPORT_OPERATIONS)
            .map_err(table_error)?;
        let Some(value) = table
            .get(operation_id.as_bytes().as_slice())
            .map_err(precommit_storage_error)?
        else {
            return Ok(None);
        };
        decode_row(operation_id.as_bytes(), value.value()).map(Some)
    }

    fn compare_and_swap_application_export_operation(
        &mut self,
        expected: Option<&StoredApplicationExportOperationV1>,
        replacement: &StoredApplicationExportOperationV1,
    ) -> Result<ApplicationExportOperationWriteResultV1, StorageError> {
        if expected.is_some_and(|expected| {
            expected.operation_id() != replacement.operation_id()
                || expected.lineage() != replacement.lineage()
        }) {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let access = self.begin_write()?;
        let mut table = access
            .transaction()?
            .open_table(APPLICATION_EXPORT_OPERATIONS)
            .map_err(table_error)?;
        let operation_id = replacement.operation_id();
        let key = operation_id.as_bytes();
        let current = table
            .get(key.as_slice())
            .map_err(precommit_storage_error)?
            .map(|value| decode_row(key, value.value()))
            .transpose()?;
        if current.as_ref() == Some(replacement) {
            drop(table);
            access.abort()?;
            return Ok(ApplicationExportOperationWriteResultV1::Unchanged);
        }
        if current.as_ref() != expected {
            drop(table);
            access.abort()?;
            return Ok(ApplicationExportOperationWriteResultV1::CompareMismatch);
        }
        let encoded = encode_application_export_operation_v1(replacement)?;
        table
            .insert(key.as_slice(), encoded.as_bytes())
            .map_err(precommit_storage_error)?;
        drop(table);
        access.commit_for(RedbTestOperation::ApplicationExportOperation)?;
        Ok(ApplicationExportOperationWriteResultV1::Applied)
    }

    fn list_application_export_operations(
        &self,
        maximum: usize,
    ) -> Result<Vec<StoredApplicationExportOperationV1>, StorageError> {
        if maximum == 0 || maximum > MAX_ACTIVE_APPLICATION_EXPORTS {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        let transaction = self.begin_read()?;
        let table = transaction
            .open_table(APPLICATION_EXPORT_OPERATIONS)
            .map_err(table_error)?;
        let mut operations = Vec::new();
        for row in table.iter().map_err(precommit_storage_error)? {
            let (key, value) = row.map_err(precommit_storage_error)?;
            if operations.len() == maximum {
                return Err(storage_error(StorageErrorKind::LimitExceeded));
            }
            operations.push(decode_row(key.value(), value.value())?);
        }
        Ok(operations)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::path::PathBuf;

    use riffdb_storage_api::{
        ApplicationExportOperationRepository, ApplicationExportOperationWriteResultV1,
        ApplicationExportSnapshotPort, AuditPrincipalV1, CatalogActivationIntentV1,
        CatalogActivationResult, CatalogAdministrationRepository, DatabaseInitializationPort,
        StoredApplicationExportOperationV1, StoredContractBundleV1,
    };
    use riffdb_types::{
        ActorId, ActorKind, ApplicationExportClassV1, ApplicationExportOperationId, CapabilityId,
        ContractBundleHash, ContractLineage, ContractVersion, DatabaseId, RequestId, Timestamp,
    };

    use crate::store::{RedbDormantPorts, RedbStore};

    struct TestPath(
        PathBuf,
        #[allow(dead_code)] crate::test_path::ScopedDirectory,
    );

    impl TestPath {
        fn new() -> Self {
            let scope = crate::test_path::ScopedDirectory::new("application-export");
            Self(scope.join("db.redb"), scope)
        }
    }

    fn uuid(seed: u8) -> [u8; 16] {
        let mut bytes = [seed; 16];
        bytes[6] = 0x70 | (seed & 0x0f);
        bytes[8] = 0x80 | (seed & 0x3f);
        bytes
    }

    fn record(seed: u8, state: &[u8]) -> StoredApplicationExportOperationV1 {
        StoredApplicationExportOperationV1::new(
            ApplicationExportOperationId::from_bytes(uuid(seed)).expect("operation"),
            ContractLineage::new("TicketDesk").expect("lineage"),
            state.to_vec(),
        )
        .expect("record")
    }

    fn request_id(seed: u8) -> RequestId {
        RequestId::from_bytes(uuid(seed)).expect("request")
    }

    fn capability_id(seed: u8) -> CapabilityId {
        CapabilityId::from_bytes(uuid(seed)).expect("capability")
    }

    fn activate_bundle(
        ports: &mut crate::store::RedbOperationalPorts,
        version: u64,
        seed: u8,
    ) -> StoredContractBundleV1 {
        let bundle = StoredContractBundleV1::new(
            ContractLineage::new("TicketDesk").expect("lineage"),
            ContractVersion::new(version).expect("version"),
            ContractBundleHash::from_bytes([seed; 32]),
            vec![seed, 1, 2, 3],
        )
        .expect("bundle");
        let result = ports
            .activate_catalog(&CatalogActivationIntentV1::new(
                (version > 1).then(|| ContractVersion::new(version - 1).expect("prior version")),
                bundle.clone(),
                request_id(seed),
                AuditPrincipalV1::new(
                    ActorId::new("operator").expect("actor"),
                    ActorKind::Human,
                    capability_id(seed),
                    NonZeroU64::MIN,
                ),
                Timestamp::new(i64::from(seed), 0).expect("timestamp"),
                None,
            ))
            .expect("activate");
        assert!(matches!(result, CatalogActivationResult::Activated { .. }));
        bundle
    }

    #[test]
    fn operation_compare_and_swap_is_durable_ordered_and_retry_safe() {
        let path = TestPath::new();
        let mut store = RedbStore::open(&path.0).expect("open");
        store
            .initialize_database(DatabaseId::from_bytes(uuid(1)).expect("database"))
            .expect("initialize");
        let dormant = RedbDormantPorts {
            shared: store.shared,
        };
        let mut ports = dormant
            .into_operational_after_catalog_validation()
            .expect("activate");
        let first = record(3, b"state-one\n");
        let next = record(3, b"state-two\n");
        let earlier = record(2, b"state-earlier\n");

        assert_eq!(
            ports
                .compare_and_swap_application_export_operation(None, &first)
                .expect("insert"),
            ApplicationExportOperationWriteResultV1::Applied
        );
        assert_eq!(
            ports
                .compare_and_swap_application_export_operation(None, &first)
                .expect("retry"),
            ApplicationExportOperationWriteResultV1::Unchanged
        );
        assert_eq!(
            ports
                .compare_and_swap_application_export_operation(None, &next)
                .expect("mismatch"),
            ApplicationExportOperationWriteResultV1::CompareMismatch
        );
        assert_eq!(
            ports
                .compare_and_swap_application_export_operation(Some(&first), &next)
                .expect("advance"),
            ApplicationExportOperationWriteResultV1::Applied
        );
        assert_eq!(
            ports
                .compare_and_swap_application_export_operation(None, &earlier)
                .expect("earlier"),
            ApplicationExportOperationWriteResultV1::Applied
        );
        assert_eq!(
            ports.list_application_export_operations(2).expect("list"),
            vec![earlier.clone(), next.clone()]
        );
        drop(ports);

        let mut reopened = RedbStore::open(&path.0).expect("reopen");
        reopened
            .initialize_database(DatabaseId::from_bytes(uuid(1)).expect("database"))
            .expect("observe initialized database");
        let reopened = RedbDormantPorts {
            shared: reopened.shared,
        }
        .into_operational_after_catalog_validation()
        .expect("reactivate");
        assert_eq!(
            reopened
                .read_application_export_operation(next.operation_id())
                .expect("read after reopen"),
            Some(next)
        );
        assert_eq!(
            reopened
                .list_application_export_operations(2)
                .expect("list after reopen"),
            vec![earlier, record(3, b"state-two\n")]
        );
    }

    #[test]
    fn captured_snapshot_keeps_one_exact_publication_root_and_bounded_empty_pages() {
        let path = TestPath::new();
        let mut store = RedbStore::open(&path.0).expect("open");
        store
            .initialize_database(DatabaseId::from_bytes(uuid(1)).expect("database"))
            .expect("initialize");
        let dormant = RedbDormantPorts {
            shared: store.shared,
        };
        let mut ports = dormant
            .into_operational_after_catalog_validation()
            .expect("activate ports");
        let first = activate_bundle(&mut ports, 1, 5);
        let snapshot = ports
            .capture_application_export_snapshot(first.lineage())
            .expect("capture first");
        assert_eq!(
            snapshot.binding().contract_version(),
            first.contract_version()
        );
        assert_eq!(snapshot.contract_bundle_bytes(), first.canonical_bytes());

        let second = activate_bundle(&mut ports, 2, 6);
        let current = ports
            .capture_application_export_snapshot(second.lineage())
            .expect("capture second");
        assert_eq!(
            current.binding().contract_version(),
            second.contract_version()
        );
        assert_eq!(
            snapshot.binding().contract_version(),
            first.contract_version()
        );
        assert_eq!(snapshot.contract_bundle_bytes(), first.canonical_bytes());

        for class in [
            ApplicationExportClassV1::Entity,
            ApplicationExportClassV1::Event,
            ApplicationExportClassV1::Provenance,
        ] {
            let page = snapshot
                .read_application_export_source_page(
                    class,
                    None,
                    riffdb_storage_api::StorageScanLimit::new(1).expect("limit"),
                )
                .expect("empty page");
            assert!(page.exact_end());
            assert!(page.records().is_empty());
            assert!(page.continuation().is_none());
        }
        let audit = snapshot
            .read_application_export_source_page(
                ApplicationExportClassV1::PublicAudit,
                None,
                riffdb_storage_api::StorageScanLimit::new(10).expect("limit"),
            )
            .expect("audit page");
        assert!(audit.exact_end());
        assert_eq!(audit.records().len(), 1);
    }
}
