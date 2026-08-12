//! Volatile exact application-export operation repository.

use riffdb_storage_api::{
    ApplicationExportOperationRepository, ApplicationExportOperationWriteResultV1,
    MAX_ACTIVE_APPLICATION_EXPORTS, StorageError, StorageErrorKind,
    StoredApplicationExportOperationV1,
};
use riffdb_types::ApplicationExportOperationId;

use crate::store::{MemoryOperationalPorts, storage_error};

fn position(
    operations: &[StoredApplicationExportOperationV1],
    operation_id: ApplicationExportOperationId,
) -> Result<usize, usize> {
    operations.binary_search_by(|candidate| candidate.operation_id().cmp(&operation_id))
}

impl ApplicationExportOperationRepository for MemoryOperationalPorts {
    fn read_application_export_operation(
        &self,
        operation_id: ApplicationExportOperationId,
    ) -> Result<Option<StoredApplicationExportOperationV1>, StorageError> {
        self.read(|state| {
            Ok(position(&state.application_export_operations, operation_id)
                .ok()
                .map(|index| state.application_export_operations[index].clone()))
        })
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
        self.apply_exclusive_mut(|state| {
            let result = position(
                &state.application_export_operations,
                replacement.operation_id(),
            );
            let current = result
                .as_ref()
                .ok()
                .map(|index| &state.application_export_operations[*index]);
            if current == Some(replacement) {
                return Ok(ApplicationExportOperationWriteResultV1::Unchanged);
            }
            if current != expected {
                return Ok(ApplicationExportOperationWriteResultV1::CompareMismatch);
            }
            match result {
                Ok(index) => state.application_export_operations[index] = replacement.clone(),
                Err(index) => state
                    .application_export_operations
                    .insert(index, replacement.clone()),
            }
            Ok(ApplicationExportOperationWriteResultV1::Applied)
        })
    }

    fn list_application_export_operations(
        &self,
        maximum: usize,
    ) -> Result<Vec<StoredApplicationExportOperationV1>, StorageError> {
        if maximum == 0 || maximum > MAX_ACTIVE_APPLICATION_EXPORTS {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        self.read(|state| {
            if state.application_export_operations.len() > maximum {
                return Err(storage_error(StorageErrorKind::LimitExceeded));
            }
            Ok(state.application_export_operations.clone())
        })
    }
}

#[cfg(test)]
mod tests {
    use riffdb_storage_api::{
        ApplicationExportOperationRepository, ApplicationExportOperationWriteResultV1,
        DatabaseInitializationPort, StoredApplicationExportOperationV1,
    };
    use riffdb_types::{ApplicationExportOperationId, ContractLineage, DatabaseId};

    use crate::{MemoryDormantPorts, MemoryStore};

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

    fn ports() -> crate::MemoryOperationalPorts {
        let mut store = MemoryStore::new();
        store
            .initialize_database(DatabaseId::from_bytes(uuid(1)).expect("database"))
            .expect("initialize");
        MemoryDormantPorts { store }.into_operational()
    }

    #[test]
    fn operation_compare_and_swap_is_exact_ordered_and_bounded() {
        let mut ports = ports();
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
            ports
                .read_application_export_operation(next.operation_id())
                .expect("read"),
            Some(next.clone())
        );
        assert_eq!(
            ports.list_application_export_operations(2).expect("list"),
            vec![earlier, next]
        );
        assert!(ports.list_application_export_operations(1).is_err());
        assert!(ports.list_application_export_operations(0).is_err());
    }
}
