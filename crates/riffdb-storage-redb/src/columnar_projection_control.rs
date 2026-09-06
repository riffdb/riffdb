//! Durable redb implementation of schema-bound columnar control.

use redb::{ReadableTable, ReadableTableMetadata, WriteTransaction};
use riffdb_storage_api::{
    ApplicationSequenceAllocator, ColumnarProjectionControlRepository,
    ColumnarProjectionControlWriteResultV1, ColumnarProjectionFailureReasonV1,
    ColumnarProjectionLayoutV1, ColumnarProjectionReplayLimitsV1,
    ColumnarProjectionRetentionRepository, FreshColumnarProjectionControlV1, StorageError,
    StorageErrorKind, StoredColumnarProjectionControlV1,
};
use riffdb_types::{
    ColumnarProjectionSourceV1, ColumnarProjectionSpecHashV1, CommitSequence,
    DefinitionFingerprint, FrontierPosition,
};

use crate::codec::{
    decode_application_sequence_allocator_v1, decode_columnar_projection_control_v1,
    encode_columnar_projection_control_v1,
};
use crate::error::{precommit_storage_error, storage_error, table_error};
use crate::hooks::RedbTestOperation;
use crate::keys::{decode_columnar_projection_control_key, encode_columnar_projection_control_key};
use crate::layout::{COLUMNAR_PROJECTION_CONTROLS, META, META_APPLICATION_SEQUENCE};
use crate::store::RedbOperationalPorts;

const MAX_COLUMNAR_PROJECTION_CONTROLS: u64 = 256;

fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}

fn decode_checked(
    key: &[u8],
    encoded: &[u8],
) -> Result<StoredColumnarProjectionControlV1, StorageError> {
    let source = decode_columnar_projection_control_key(key).map_err(|_| corrupt())?;
    let control = decode_columnar_projection_control_v1(encoded)?
        .into_parts()
        .0;
    if control.source() != &source {
        return Err(corrupt());
    }
    Ok(control)
}

fn transaction_current_head(
    transaction: &WriteTransaction,
) -> Result<FrontierPosition, StorageError> {
    let meta = transaction.open_table(META).map_err(table_error)?;
    let encoded = meta
        .get(META_APPLICATION_SEQUENCE)
        .map_err(precommit_storage_error)?
        .ok_or_else(corrupt)?;
    let allocator = decode_application_sequence_allocator_v1(encoded.value())?
        .into_parts()
        .0;
    Ok(match allocator {
        ApplicationSequenceAllocator::Next(next) if next == CommitSequence::first() => {
            FrontierPosition::BeforeFirst
        }
        ApplicationSequenceAllocator::Next(next) => FrontierPosition::AppliedThrough(
            CommitSequence::new(next.get() - 1).ok_or_else(corrupt)?,
        ),
        ApplicationSequenceAllocator::Exhausted => {
            FrontierPosition::AppliedThrough(CommitSequence::new(u64::MAX).ok_or_else(corrupt)?)
        }
    })
}

impl RedbOperationalPorts {
    fn apply_expected_columnar_control(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        transition: impl FnOnce(
            StoredColumnarProjectionControlV1,
            FrontierPosition,
        ) -> Result<
            StoredColumnarProjectionControlV1,
            riffdb_storage_api::ColumnarControlError,
        >,
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        let key = encode_columnar_projection_control_key(expected.source())
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let access = self.begin_write()?;
        let head = transaction_current_head(access.transaction()?)?;
        let mut table = access
            .transaction()?
            .open_table(COLUMNAR_PROJECTION_CONTROLS)
            .map_err(table_error)?;
        let current = table
            .get(key.as_slice())
            .map_err(precommit_storage_error)?
            .map(|value| decode_checked(&key, value.value()))
            .transpose()?;
        if current.as_ref() != Some(expected) {
            drop(table);
            access.abort()?;
            return Ok(ColumnarProjectionControlWriteResultV1::StateChanged);
        }
        let replacement = transition(expected.clone(), head)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        if replacement.source() != expected.source() {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let encoded = encode_columnar_projection_control_v1(&replacement)?;
        access.expect_fresh_locator_byte_insert(COLUMNAR_PROJECTION_CONTROLS, &key)?;
        access.close_fresh_locator_mutation_expectations()?;
        access.record_actual_fresh_locator_byte_insert(COLUMNAR_PROJECTION_CONTROLS, &key)?;
        table
            .insert(key.as_slice(), encoded.as_bytes())
            .map_err(precommit_storage_error)?;
        drop(table);
        access.commit_for(RedbTestOperation::ColumnarProjectionControl)?;
        Ok(ColumnarProjectionControlWriteResultV1::Applied)
    }
}

impl ColumnarProjectionControlRepository for RedbOperationalPorts {
    fn initialize_fresh_v1(
        &self,
        controls: &[FreshColumnarProjectionControlV1],
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        if controls.len() > MAX_COLUMNAR_PROJECTION_CONTROLS as usize {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        let mut encoded = controls
            .iter()
            .map(|initial| {
                let key = encode_columnar_projection_control_key(initial.control().source())
                    .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
                let value = encode_columnar_projection_control_v1(initial.control())?;
                Ok((key, value))
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        encoded.sort_by(|left, right| left.0.cmp(&right.0));
        if encoded.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let access = self.begin_write()?;
        let mut table = access
            .transaction()?
            .open_table(COLUMNAR_PROJECTION_CONTROLS)
            .map_err(table_error)?;
        let current_length = table.len().map_err(precommit_storage_error)?;
        if current_length
            .checked_add(encoded.len() as u64)
            .is_none_or(|length| length > MAX_COLUMNAR_PROJECTION_CONTROLS)
        {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        for (key, _) in &encoded {
            if table
                .get(key.as_slice())
                .map_err(precommit_storage_error)?
                .is_some()
            {
                drop(table);
                access.abort()?;
                return Ok(ColumnarProjectionControlWriteResultV1::StateChanged);
            }
        }
        for (key, _) in &encoded {
            access.expect_fresh_locator_byte_insert(COLUMNAR_PROJECTION_CONTROLS, key)?;
        }
        access.close_fresh_locator_mutation_expectations()?;
        for (key, value) in encoded {
            access.record_actual_fresh_locator_byte_insert(COLUMNAR_PROJECTION_CONTROLS, &key)?;
            table
                .insert(key.as_slice(), value.as_bytes())
                .map_err(precommit_storage_error)?;
        }
        drop(table);
        access.commit_for(RedbTestOperation::ColumnarProjectionControl)?;
        Ok(ColumnarProjectionControlWriteResultV1::Applied)
    }

    fn record_durable_snapshot(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        prepared: &riffdb_storage_api::PreparedColumnarGenerationV1,
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        self.apply_expected_columnar_control(expected, |control, _| {
            let replacement = prepared.replacement_for(&control)?;
            control.record_durable_snapshot(replacement)
        })
    }

    fn record_candidate_frontier(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        replacement: &riffdb_storage_api::PreparedColumnarGenerationV1,
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        self.apply_expected_columnar_control(expected, |control, head| {
            let replacement = replacement.replacement_for(&control)?;
            control.record_candidate_frontier(replacement, head)
        })
    }

    fn advance_published_v1(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        replacement: &riffdb_storage_api::PreparedColumnarGenerationV1,
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        self.apply_expected_columnar_control(expected, |control, head| {
            let replacement = replacement.replacement_for(&control)?;
            control.advance_published_v1(replacement, head)
        })
    }

    fn begin_v2_candidate(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        physical_generation_fingerprint: [u8; 32],
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        self.apply_expected_columnar_control(expected, |control, _| {
            control.begin_v2_candidate(physical_generation_fingerprint)
        })
    }

    fn allocate_same_spec_candidate(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        physical_generation_fingerprint: [u8; 32],
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        self.apply_expected_columnar_control(expected, |control, _| {
            control.allocate_same_spec_candidate(physical_generation_fingerprint)
        })
    }

    fn allocate_unservable_rebuild_candidate(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        target_definition_fingerprint: DefinitionFingerprint,
        target_spec_hash: ColumnarProjectionSpecHashV1,
        replay_limits: ColumnarProjectionReplayLimitsV1,
        layout: ColumnarProjectionLayoutV1,
        physical_generation_fingerprint: Option<[u8; 32]>,
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        self.apply_expected_columnar_control(expected, |control, _| {
            control.allocate_unservable_rebuild_candidate(
                target_definition_fingerprint,
                target_spec_hash,
                replay_limits,
                layout,
                physical_generation_fingerprint,
            )
        })
    }

    fn publish_prepared_generation(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        prepared: &riffdb_storage_api::PreparedColumnarGenerationV1,
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        self.apply_expected_columnar_control(expected, |control, head| {
            let prepared = prepared.replacement_for(&control)?;
            if control.candidate() != Some(&prepared) {
                return Err(riffdb_storage_api::ColumnarControlError);
            }
            control.publish_prepared_generation(head)
        })
    }

    fn record_candidate_failure(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        reason: ColumnarProjectionFailureReasonV1,
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        self.apply_expected_columnar_control(expected, |control, _| {
            control.record_candidate_failure(reason)
        })
    }

    fn replace_failed_candidate(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        layout: ColumnarProjectionLayoutV1,
        physical_generation_fingerprint: Option<[u8; 32]>,
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        self.apply_expected_columnar_control(expected, |control, _| {
            control.replace_failed_candidate(layout, physical_generation_fingerprint)
        })
    }

    fn retarget_initial_candidate(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        target_definition_fingerprint: DefinitionFingerprint,
        target_spec_hash: ColumnarProjectionSpecHashV1,
        replay_limits: ColumnarProjectionReplayLimitsV1,
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        self.apply_expected_columnar_control(expected, |control, _| {
            control.retarget_initial_candidate(
                target_definition_fingerprint,
                target_spec_hash,
                replay_limits,
            )
        })
    }

    fn record_published_failure(
        &self,
        expected: &StoredColumnarProjectionControlV1,
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        self.apply_expected_columnar_control(expected, |control, _| {
            control.record_published_failure()
        })
    }

    fn recover_expected_control(
        &self,
        source: &ColumnarProjectionSourceV1,
    ) -> Result<Option<StoredColumnarProjectionControlV1>, StorageError> {
        let key = encode_columnar_projection_control_key(source)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let transaction = self.begin_read()?;
        let table = transaction
            .open_table(COLUMNAR_PROJECTION_CONTROLS)
            .map_err(table_error)?;
        table
            .get(key.as_slice())
            .map_err(precommit_storage_error)?
            .map(|value| decode_checked(&key, value.value()))
            .transpose()
    }

    fn mark_invalid(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        reason: ColumnarProjectionFailureReasonV1,
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        self.apply_expected_columnar_control(expected, |control, _| control.mark_invalid(reason))
    }
}

impl ColumnarProjectionRetentionRepository for RedbOperationalPorts {
    fn columnar_projection_retention_frontiers(
        &self,
    ) -> Result<Vec<(ColumnarProjectionSourceV1, Option<FrontierPosition>)>, StorageError> {
        let transaction = self.begin_read()?;
        let table = transaction
            .open_table(COLUMNAR_PROJECTION_CONTROLS)
            .map_err(table_error)?;
        if table.len().map_err(precommit_storage_error)? > MAX_COLUMNAR_PROJECTION_CONTROLS {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        let mut frontiers =
            Vec::with_capacity(table.len().map_err(precommit_storage_error)? as usize);
        for row in table.iter().map_err(precommit_storage_error)? {
            let (key, value) = row.map_err(precommit_storage_error)?;
            let control = decode_checked(key.value(), value.value())?;
            frontiers.push((control.source().clone(), control.retention_frontier()));
        }
        Ok(frontiers)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};

    use riffdb_storage_api::{
        DatabaseInitializationPort, DatabaseInitializationResult, FreshColumnarProjectionControlV1,
    };
    use riffdb_types::{ContractLineage, DatabaseId};

    use super::*;
    use crate::hooks::RedbTestController;
    use crate::store::{RedbDormantPorts, RedbStore};

    struct TestPath(
        std::path::PathBuf,
        #[allow(dead_code)] crate::test_path::ScopedDirectory,
    );

    fn source() -> (ColumnarProjectionSourceV1, DefinitionFingerprint) {
        let definition = DefinitionFingerprint::from_bytes([0x11; 32]);
        (
            ColumnarProjectionSourceV1::scalar(
                ContractLineage::new("redb-columnar").expect("lineage"),
                definition,
            ),
            definition,
        )
    }

    fn ports(label: &str, controller: RedbTestController) -> (TestPath, RedbOperationalPorts) {
        let scope = crate::test_path::ScopedDirectory::new(label);
        let path = TestPath(scope.join("db.redb"), scope);
        let mut store = RedbStore::open_with_test_controller(&path.0, controller).expect("open");
        let mut database = [0x44; 16];
        database[6] = 0x74;
        database[8] = 0x84;
        let database = DatabaseId::from_bytes(database).expect("database");
        assert!(matches!(
            store.initialize_database(database).expect("initialize"),
            DatabaseInitializationResult::Installed(_)
        ));
        let ports = RedbDormantPorts {
            shared: store.shared,
        }
        .into_operational_after_catalog_validation()
        .expect("operational");
        (path, ports)
    }

    // req: PRJ-002, PRJ-006, PRJ-007, PRJ-010, OQ-020, OQ-024, OQ-053
    #[test]
    fn redb_columnar_control_expected_cas_recovers_uncertainty() {
        let controller = RedbTestController::return_unknown_after_commit(
            RedbTestOperation::ColumnarProjectionControl,
        );
        let (_path, ports) = ports("columnar-control-unknown", controller);
        let (source, definition) = source();
        let spec = ColumnarProjectionSpecHashV1::from_bytes([0x22; 32]);
        let limits = ColumnarProjectionReplayLimitsV1::new(60, 1_024, 10).expect("limits");
        let fresh =
            FreshColumnarProjectionControlV1::new(source.clone(), definition, spec, limits, 1)
                .expect("fresh control");
        let error = ports
            .initialize_fresh_v1(std::slice::from_ref(&fresh))
            .expect_err("post-commit result is uncertain");
        assert_eq!(error.kind(), StorageErrorKind::CommitStatusUnknown);
        let recovered = ports
            .recover_expected_control(&source)
            .expect("recover")
            .expect("durable control");
        assert_eq!(recovered.source(), &source);
        assert_eq!(
            recovered.retention_frontier(),
            Some(FrontierPosition::BeforeFirst)
        );
        assert_eq!(
            ports
                .columnar_projection_retention_frontiers()
                .expect("retention"),
            vec![(source, Some(FrontierPosition::BeforeFirst))]
        );
    }

    // req: PRJ-002, PRJ-006, PRJ-007, PRJ-010, OQ-020, OQ-024, OQ-053
    #[test]
    fn redb_columnar_control_before_commit_failure_is_proven_absent() {
        let controller =
            RedbTestController::return_before_commit(RedbTestOperation::ColumnarProjectionControl);
        let (_path, ports) = ports("columnar-control-before-commit", controller);
        let (source, definition) = source();
        let fresh = FreshColumnarProjectionControlV1::new(
            source.clone(),
            definition,
            ColumnarProjectionSpecHashV1::from_bytes([0x22; 32]),
            ColumnarProjectionReplayLimitsV1::new(60, 1_024, 10).expect("limits"),
            1,
        )
        .expect("fresh control");
        let error = ports
            .initialize_fresh_v1(std::slice::from_ref(&fresh))
            .expect_err("before-commit injection");
        assert_eq!(error.kind(), StorageErrorKind::Unavailable);
        assert_eq!(
            ports
                .recover_expected_control(&source)
                .expect("recover absent"),
            None
        );
        assert!(
            ports
                .columnar_projection_retention_frontiers()
                .expect("retention")
                .is_empty()
        );
    }

    // req: PRJ-002, PRJ-006, PRJ-007, PRJ-010, OQ-020, OQ-024, OQ-053, OUT-001, OUT-002, TXN-042
    #[test]
    fn redb_columnar_control_concurrent_expected_cas_has_one_winner() {
        let (_path, ports) = ports(
            "columnar-control-concurrent-cas",
            RedbTestController::observe_index_migration(),
        );
        assert!(
            ports
                .arm_exact_empty_fresh_locator_coverage_for_test()
                .expect("arm exact empty coverage")
        );
        let (source, definition) = source();
        let spec = ColumnarProjectionSpecHashV1::from_bytes([0x22; 32]);
        let limits = ColumnarProjectionReplayLimitsV1::new(60, 1_024, 10).expect("limits");
        let fresh =
            FreshColumnarProjectionControlV1::new(source.clone(), definition, spec, limits, 1)
                .expect("fresh control");
        assert_eq!(
            ports
                .initialize_fresh_v1(std::slice::from_ref(&fresh))
                .expect("initialize"),
            ColumnarProjectionControlWriteResultV1::Applied
        );
        let expected = ports
            .recover_expected_control(&source)
            .expect("recover")
            .expect("control");
        let barrier = Arc::new(Barrier::new(3));
        let mut workers = Vec::new();
        for shared in [ports.shared_ports(), ports.shared_ports()] {
            let expected = expected.clone();
            let barrier = Arc::clone(&barrier);
            workers.push(std::thread::spawn(move || {
                barrier.wait();
                shared.record_candidate_failure(
                    &expected,
                    ColumnarProjectionFailureReasonV1::ReplayBacklog,
                )
            }));
        }
        barrier.wait();
        let results = workers
            .into_iter()
            .map(|worker| worker.join().expect("worker").expect("CAS"))
            .collect::<Vec<_>>();
        assert_eq!(
            results
                .iter()
                .filter(|result| **result == ColumnarProjectionControlWriteResultV1::Applied)
                .count(),
            1
        );
        assert_eq!(
            results
                .iter()
                .filter(|result| **result == ColumnarProjectionControlWriteResultV1::StateChanged)
                .count(),
            1
        );
        let failed = ports
            .recover_expected_control(&source)
            .expect("recover winner")
            .expect("failed control");
        assert_eq!(
            ports
                .replace_failed_candidate(&failed, ColumnarProjectionLayoutV1::V1, None)
                .expect("replace exact failed initial Candidate"),
            ColumnarProjectionControlWriteResultV1::Applied
        );
        let replacement = ports
            .recover_expected_control(&source)
            .expect("recover replacement")
            .expect("replacement");
        assert_eq!(
            replacement.lifecycle(),
            riffdb_storage_api::ColumnarProjectionLifecycleV1::Building
        );
        assert_eq!(replacement.highest_generation().get(), 2);
        assert!(
            ports
                .fresh_locator_public_and_private_roles_match_for_test()
                .expect("columnar control lane preserves both roles")
        );
    }
}
