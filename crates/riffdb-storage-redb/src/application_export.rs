//! Durable exact application-export operation repository.

use redb::ReadableTable;
use riffdb_storage_api::{
    ApplicationExportOperationRepository, ApplicationExportOperationWriteResultV1,
    MAX_ACTIVE_APPLICATION_EXPORTS, StorageError, StorageErrorKind,
    StoredApplicationExportOperationV1,
};
use riffdb_types::ApplicationExportOperationId;

use crate::codec::{
    decode_application_export_operation_v1, encode_application_export_operation_v1,
};
use crate::error::{precommit_storage_error, storage_error, table_error};
use crate::hooks::RedbTestOperation;
use crate::layout::APPLICATION_EXPORT_OPERATIONS;
use crate::store::RedbOperationalPorts;

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
    use std::path::PathBuf;

    use riffdb_storage_api::{
        ApplicationExportOperationRepository, ApplicationExportOperationWriteResultV1,
        DatabaseInitializationPort, StoredApplicationExportOperationV1,
    };
    use riffdb_types::{ApplicationExportOperationId, ContractLineage, DatabaseId};

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
}
