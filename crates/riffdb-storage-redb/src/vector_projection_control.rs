//! Durable vector-projection lifecycle and retention controls.

use riffdb_storage_api::{
    StorageError, StorageErrorKind, StoredVectorProjectionControlV1,
    VectorProjectionControlRepository, VectorProjectionControlWriteResultV1,
    VectorProjectionSourceV1,
};
use riffdb_types::FrontierPosition;

use crate::codec::decode_vector_projection_control_v1;
#[cfg(test)]
use crate::codec::encode_vector_projection_control_v1;
use crate::error::{precommit_storage_error, storage_error, table_error};
use crate::keys::{decode_vector_projection_control_key, encode_vector_projection_control_key};
use crate::layout::VECTOR_PROJECTION_CONTROLS;
use crate::store::RedbOperationalPorts;

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
        let _ = (expected, replacement);
        Err(storage_error(StorageErrorKind::IncompatibleFormat))
    }

    fn attached_vector_projection_frontiers(&self) -> Result<Vec<FrontierPosition>, StorageError> {
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use redb::ReadableDatabase;
    use riffdb_storage_api::{
        ColumnarProjectionControlRepository, DatabaseInitializationPort,
        DatabaseInitializationResult, FreshColumnarProjectionControlV1,
        VectorProjectionLifecycleV1, VectorProjectionRebuildReasonV1,
        VectorProjectionReplayLimitsV1,
    };
    use riffdb_types::{
        ColumnarProjectionReplayLimitsV1, ColumnarProjectionSourceV1, ColumnarProjectionSpecHashV1,
        CommitSequence, ContractLineage, DatabaseId, DefinitionFingerprint, EntityTypeId, FieldId,
        ProjectionGeneration,
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

    // req: PRJ-002, PRJ-004, PRJ-006, PRJ-007, PRJ-010
    #[test]
    fn columnar_control_fresh_rebuild_ignores_every_legacy_selector() {
        let (_path, ports) = ports("vector-control-cas");
        let ready = control(VectorProjectionLifecycleV1::Ready);
        let key = encode_vector_projection_control_key(ready.source()).expect("legacy key");
        let encoded = encode_vector_projection_control_v1(&ready).expect("legacy envelope");
        let mut transaction = ports.shared.database.begin_write().expect("write fixture");
        transaction
            .set_durability(redb::Durability::Immediate)
            .expect("durability");
        {
            let mut table = transaction
                .open_table(VECTOR_PROJECTION_CONTROLS)
                .expect("table");
            table
                .insert(key.as_slice(), encoded.as_bytes())
                .expect("insert fixture");
        }
        transaction.commit().expect("commit fixture");
        assert_eq!(
            ports
                .read_vector_projection_control(ready.source())
                .expect("structural read"),
            Some(ready.clone())
        );
        let source = ColumnarProjectionSourceV1::vector(ready.source().clone());
        let fresh = FreshColumnarProjectionControlV1::new(
            source.clone(),
            DefinitionFingerprint::from_bytes([0x55; 32]),
            ColumnarProjectionSpecHashV1::from_bytes([0x66; 32]),
            ColumnarProjectionReplayLimitsV1::new(60, 1_024, 10).expect("common limits"),
            1,
        )
        .expect("fresh common control");
        assert_eq!(
            ports
                .initialize_fresh_v1(std::slice::from_ref(&fresh))
                .expect("common insert"),
            riffdb_storage_api::ColumnarProjectionControlWriteResultV1::Applied
        );
        let common = ports
            .recover_expected_control(&source)
            .expect("common read")
            .expect("common control");
        assert_eq!(common.highest_generation(), ProjectionGeneration::first());
        assert_eq!(
            common.retention_frontier(),
            Some(FrontierPosition::BeforeFirst)
        );
        assert_ne!(
            common.target_definition_fingerprint().as_bytes(),
            ready.definition_fingerprint(),
            "legacy definition bytes never seed common control"
        );
        let transaction = ports.shared.database.begin_read().expect("retention read");
        assert_eq!(
            crate::retention::min_projection_durable_frontier(
                &transaction,
                &std::collections::BTreeSet::new(),
            )
            .expect("common retention input"),
            Some(0),
            "only the common BeforeFirst fence participates in pruning"
        );
        let error = ports
            .compare_and_set_vector_projection_control(Some(&ready), &ready)
            .expect_err("legacy writes are retired");
        assert_eq!(error.kind(), StorageErrorKind::IncompatibleFormat);
        assert!(
            ports
                .attached_vector_projection_frontiers()
                .expect("legacy retention is inert")
                .is_empty()
        );
    }

    // req: PRJ-007, PRJ-010
    #[test]
    fn legacy_structural_scan_refuses_row_4097_before_decode() {
        let (_path, ports) = ports("legacy-vector-control-bound");
        let mut transaction = ports.shared.database.begin_write().expect("write fixture");
        transaction
            .set_durability(redb::Durability::Immediate)
            .expect("durability");
        {
            let mut table = transaction
                .open_table(VECTOR_PROJECTION_CONTROLS)
                .expect("table");
            for index in 0_u32..4_097 {
                table
                    .insert(index.to_be_bytes().as_slice(), [0xff].as_slice())
                    .expect("insert bounded fixture");
            }
        }
        transaction.commit().expect("commit fixture");
        let error = crate::store::validate_inert_legacy_vector_controls(&ports.shared)
            .expect_err("row 4097 refuses before semantic decode");
        assert_eq!(error.kind(), StorageErrorKind::LimitExceeded);
    }
}
