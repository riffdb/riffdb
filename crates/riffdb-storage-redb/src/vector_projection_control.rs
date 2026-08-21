//! Durable vector-projection lifecycle and retention controls.

use redb::ReadableTable;
use riffdb_storage_api::{
    StorageError, StorageErrorKind, StoredVectorProjectionControlV1,
    VectorProjectionControlRepository, VectorProjectionControlWriteResultV1,
    VectorProjectionSourceV1,
};
use riffdb_types::FrontierPosition;

use crate::codec::{decode_vector_projection_control_v1, encode_vector_projection_control_v1};
use crate::error::{precommit_storage_error, storage_error, table_error};
use crate::hooks::RedbTestOperation;
use crate::keys::{decode_vector_projection_control_key, encode_vector_projection_control_key};
use crate::layout::VECTOR_PROJECTION_CONTROLS;
use crate::store::RedbOperationalPorts;

const MAX_VECTOR_PROJECTION_CONTROLS: usize = 256;

fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}

fn decode_checked(
    key: &[u8],
    encoded: &[u8],
) -> Result<StoredVectorProjectionControlV1, StorageError> {
    let source = decode_vector_projection_control_key(key).map_err(|_| corrupt())?;
    let control = decode_vector_projection_control_v1(encoded)?.into_parts().0;
    if control.source() != &source {
        return Err(corrupt());
    }
    Ok(control)
}

impl VectorProjectionControlRepository for RedbOperationalPorts {
    fn read_vector_projection_control(
        &self,
        source: &VectorProjectionSourceV1,
    ) -> Result<Option<StoredVectorProjectionControlV1>, StorageError> {
        let key = encode_vector_projection_control_key(source)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let transaction = self.begin_read()?;
        let table = transaction
            .open_table(VECTOR_PROJECTION_CONTROLS)
            .map_err(table_error)?;
        table
            .get(key.as_slice())
            .map_err(precommit_storage_error)?
            .map(|value| decode_checked(&key, value.value()))
            .transpose()
    }

    fn compare_and_set_vector_projection_control(
        &self,
        expected: Option<&StoredVectorProjectionControlV1>,
        replacement: &StoredVectorProjectionControlV1,
    ) -> Result<VectorProjectionControlWriteResultV1, StorageError> {
        if expected.is_some_and(|expected| expected.source() != replacement.source()) {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let key = encode_vector_projection_control_key(replacement.source())
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let access = self.begin_write()?;
        let mut table = access
            .transaction()?
            .open_table(VECTOR_PROJECTION_CONTROLS)
            .map_err(table_error)?;
        let current = table
            .get(key.as_slice())
            .map_err(precommit_storage_error)?
            .map(|value| decode_checked(&key, value.value()))
            .transpose()?;
        if current.as_ref() == Some(replacement) {
            drop(table);
            access.abort()?;
            return Ok(VectorProjectionControlWriteResultV1::Unchanged);
        }
        if current.as_ref() != expected {
            drop(table);
            access.abort()?;
            return Ok(VectorProjectionControlWriteResultV1::CompareMismatch);
        }
        let encoded = encode_vector_projection_control_v1(replacement)?;
        table
            .insert(key.as_slice(), encoded.as_bytes())
            .map_err(precommit_storage_error)?;
        drop(table);
        access.commit_for(RedbTestOperation::ProjectionMutation)?;
        Ok(VectorProjectionControlWriteResultV1::Applied)
    }

    fn attached_vector_projection_frontiers(&self) -> Result<Vec<FrontierPosition>, StorageError> {
        let transaction = self.begin_read()?;
        let table = transaction
            .open_table(VECTOR_PROJECTION_CONTROLS)
            .map_err(table_error)?;
        let mut frontiers = Vec::new();
        for row in table.iter().map_err(precommit_storage_error)? {
            if frontiers.len() == MAX_VECTOR_PROJECTION_CONTROLS {
                return Err(storage_error(StorageErrorKind::LimitExceeded));
            }
            let (key, value) = row.map_err(precommit_storage_error)?;
            let control = decode_checked(key.value(), value.value())?;
            if control.retention_attached() {
                frontiers.push(control.published_frontier());
            }
        }
        Ok(frontiers)
    }
}

#[cfg(test)]
mod tests {
    use riffdb_storage_api::{
        DatabaseInitializationPort, DatabaseInitializationResult, VectorProjectionLifecycleV1,
        VectorProjectionRebuildReasonV1, VectorProjectionReplayLimitsV1,
    };
    use riffdb_types::{
        CommitSequence, ContractLineage, DatabaseId, EntityTypeId, FieldId, ProjectionGeneration,
    };

    use super::*;
    use crate::store::{RedbDormantPorts, RedbStore};

    struct TestPath(
        std::path::PathBuf,
        #[allow(dead_code)] crate::test_path::ScopedDirectory,
    );

    impl TestPath {
        fn new(label: &str) -> Self {
            let scope = crate::test_path::ScopedDirectory::new(label);
            Self(scope.join("db.redb"), scope)
        }
    }

    fn control(lifecycle: VectorProjectionLifecycleV1) -> StoredVectorProjectionControlV1 {
        let source = VectorProjectionSourceV1::new(
            ContractLineage::new("vectors").expect("lineage"),
            EntityTypeId::new(2).expect("entity"),
            FieldId::new(4).expect("field"),
        );
        let limits = VectorProjectionReplayLimitsV1::new(60, 1_024, 10).expect("limits");
        match lifecycle {
            VectorProjectionLifecycleV1::Building => {
                StoredVectorProjectionControlV1::initial(source, [0x11; 32], limits)
            }
            VectorProjectionLifecycleV1::Ready => StoredVectorProjectionControlV1::new(
                source,
                ProjectionGeneration::first(),
                [0x11; 32],
                lifecycle,
                FrontierPosition::AppliedThrough(CommitSequence::new(7).expect("sequence")),
                None,
                None,
                limits,
            )
            .expect("ready"),
            VectorProjectionLifecycleV1::RebuildRequired => StoredVectorProjectionControlV1::new(
                source,
                ProjectionGeneration::new(2).expect("generation"),
                [0x22; 32],
                lifecycle,
                FrontierPosition::AppliedThrough(CommitSequence::new(7).expect("sequence")),
                None,
                Some(VectorProjectionRebuildReasonV1::ReplayBacklog),
                limits,
            )
            .expect("rebuild required"),
            _ => unreachable!("test fixture uses closed subset"),
        }
    }

    fn ports(label: &str) -> (TestPath, RedbOperationalPorts) {
        let path = TestPath::new(label);
        let mut store = RedbStore::open(&path.0).expect("create store");
        let mut database_id = [0x44; 16];
        database_id[6] = 0x74;
        database_id[8] = 0x84;
        let database_id = DatabaseId::from_bytes(database_id).expect("database ID");
        assert!(matches!(
            store.initialize_database(database_id).expect("initialize"),
            DatabaseInitializationResult::Installed(_)
        ));
        let ports = RedbDormantPorts {
            shared: store.shared,
        }
        .into_operational_after_catalog_validation()
        .expect("operational");
        (path, ports)
    }

    #[test]
    fn compare_set_detaches_retention_in_the_same_durable_fact() {
        let (_path, ports) = ports("vector-control-cas");
        let initial = control(VectorProjectionLifecycleV1::Building);
        encode_vector_projection_control_v1(&initial).expect("encode initial control");
        assert_eq!(
            ports
                .compare_and_set_vector_projection_control(None, &initial)
                .expect("create"),
            VectorProjectionControlWriteResultV1::Applied
        );
        let ready = control(VectorProjectionLifecycleV1::Ready);
        assert_eq!(
            ports
                .compare_and_set_vector_projection_control(Some(&initial), &ready)
                .expect("ready"),
            VectorProjectionControlWriteResultV1::Applied
        );
        assert_eq!(
            ports
                .attached_vector_projection_frontiers()
                .expect("attached"),
            vec![ready.published_frontier()]
        );
        let detached = control(VectorProjectionLifecycleV1::RebuildRequired);
        assert_eq!(
            ports
                .compare_and_set_vector_projection_control(Some(&ready), &detached)
                .expect("detach"),
            VectorProjectionControlWriteResultV1::Applied
        );
        assert!(
            ports
                .attached_vector_projection_frontiers()
                .expect("detached")
                .is_empty()
        );
    }
}
