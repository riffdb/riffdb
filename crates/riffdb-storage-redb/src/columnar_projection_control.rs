//! Durable redb implementation of schema-bound columnar control.

use redb::{ReadableTable, ReadableTableMetadata, WriteTransaction};
use riffdb_columnar::{PreparedColumnarGenerationRepository, PreparedColumnarGenerationV1};
use riffdb_storage_api::{
    ApplicationSequenceAllocator, ColumnarProjectionControlRepository,
    ColumnarProjectionControlWriteResultV1, ColumnarProjectionFailureReasonV1,
    ColumnarProjectionLayoutV1, ColumnarProjectionReplayLimitsV1,
    ColumnarProjectionRetentionRepository, FreshColumnarProjectionControlV1,
    HISTORY_INCARNATION_INITIAL, StorageError, StorageErrorKind, StoredColumnarProjectionControlV1,
    StoredColumnarProjectionGenerationV1,
};
use riffdb_types::{
    ColumnarProjectionSourceV1, ColumnarProjectionSpecHashV1, CommitSequence,
    DefinitionFingerprint, FrontierPosition,
};

use crate::codec::{
    decode_application_sequence_allocator_v1, decode_columnar_projection_control_v1,
    decode_history_incarnation_v1, encode_columnar_projection_control_v1,
};
use crate::error::{precommit_storage_error, storage_error, table_error};
use crate::hooks::RedbTestOperation;
use crate::keys::{decode_columnar_projection_control_key, encode_columnar_projection_control_key};
use crate::layout::{
    COLUMNAR_PROJECTION_CONTROLS, META, META_APPLICATION_SEQUENCE, META_HISTORY_INCARNATION,
};
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

fn transaction_current_history_incarnation(
    transaction: &WriteTransaction,
) -> Result<u64, StorageError> {
    let meta = transaction.open_table(META).map_err(table_error)?;
    let encoded = meta
        .get(META_HISTORY_INCARNATION)
        .map_err(precommit_storage_error)?
        .ok_or_else(corrupt)?;
    let incarnation = *decode_history_incarnation_v1(encoded.value())?.value();
    if incarnation < HISTORY_INCARNATION_INITIAL {
        return Err(corrupt());
    }
    Ok(incarnation)
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
        table
            .insert(key.as_slice(), encoded.as_bytes())
            .map_err(precommit_storage_error)?;
        drop(table);
        access.commit_for(RedbTestOperation::ColumnarProjectionControl)?;
        Ok(ColumnarProjectionControlWriteResultV1::Applied)
    }

    fn record_durable_snapshot_pointer(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        replacement: StoredColumnarProjectionGenerationV1,
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        self.apply_expected_columnar_control(expected, |control, _| {
            control.record_durable_snapshot(replacement)
        })
    }

    fn record_candidate_frontier_pointer(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        replacement: StoredColumnarProjectionGenerationV1,
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        self.apply_expected_columnar_control(expected, |control, head| {
            control.record_candidate_frontier(replacement, head)
        })
    }

    fn advance_published_v1_pointer(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        replacement: StoredColumnarProjectionGenerationV1,
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        self.apply_expected_columnar_control(expected, |control, head| {
            control.advance_published_v1(replacement, head)
        })
    }

    fn publish_prepared_generation_pointer(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        prepared: StoredColumnarProjectionGenerationV1,
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        self.apply_expected_columnar_control(expected, |control, head| {
            if control.candidate() != Some(&prepared) {
                return Err(riffdb_storage_api::ColumnarControlError);
            }
            control.publish_prepared_generation(head)
        })
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
        for (key, value) in encoded {
            table
                .insert(key.as_slice(), value.as_bytes())
                .map_err(precommit_storage_error)?;
        }
        drop(table);
        access.commit_for(RedbTestOperation::ColumnarProjectionControl)?;
        Ok(ColumnarProjectionControlWriteResultV1::Applied)
    }

    fn reset_for_current_history_incarnation(
        &self,
        expected: &StoredColumnarProjectionControlV1,
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        let key = encode_columnar_projection_control_key(expected.source())
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let access = self.begin_write()?;
        let current_history_incarnation =
            transaction_current_history_incarnation(access.transaction()?)?;
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
        let replacement = expected
            .clone()
            .reset_for_current_history_incarnation(current_history_incarnation)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let encoded = encode_columnar_projection_control_v1(&replacement)?;
        table
            .insert(key.as_slice(), encoded.as_bytes())
            .map_err(precommit_storage_error)?;
        drop(table);
        access.commit_for(RedbTestOperation::ColumnarProjectionControl)?;
        Ok(ColumnarProjectionControlWriteResultV1::Applied)
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

impl PreparedColumnarGenerationRepository for RedbOperationalPorts {
    fn record_durable_snapshot(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        prepared: &PreparedColumnarGenerationV1,
        process_generation: [u8; 16],
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        let replacement = prepared
            .replacement_for(expected, process_generation)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        self.record_durable_snapshot_pointer(expected, replacement)
    }

    fn record_candidate_frontier(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        replacement: &PreparedColumnarGenerationV1,
        process_generation: [u8; 16],
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        let replacement = replacement
            .replacement_for(expected, process_generation)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        self.record_candidate_frontier_pointer(expected, replacement)
    }

    fn advance_published_v1(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        replacement: &PreparedColumnarGenerationV1,
        process_generation: [u8; 16],
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        let replacement = replacement
            .replacement_for(expected, process_generation)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        self.advance_published_v1_pointer(expected, replacement)
    }

    fn publish_prepared_generation(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        prepared: &PreparedColumnarGenerationV1,
        process_generation: [u8; 16],
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError> {
        let prepared = prepared
            .replacement_for(expected, process_generation)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        self.publish_prepared_generation_pointer(expected, prepared)
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

    fn stamp_current_history(ports: &RedbOperationalPorts, incarnation: u64) {
        let access = ports.begin_write().expect("begin history stamp");
        let encoded = crate::codec::encode_history_incarnation_v1(incarnation)
            .expect("encode history incarnation");
        let mut meta = access
            .transaction()
            .expect("history transaction")
            .open_table(META)
            .expect("open metadata");
        meta.insert(META_HISTORY_INCARNATION, encoded.as_bytes())
            .expect("stamp current history");
        drop(meta);
        access
            .commit_for(RedbTestOperation::Initialization)
            .expect("commit history stamp");
    }

    fn install_control_without_columnar_failpoint(
        ports: &RedbOperationalPorts,
        control: &StoredColumnarProjectionControlV1,
        current_history_incarnation: u64,
    ) {
        let access = ports.begin_write().expect("begin control install");
        let key = encode_columnar_projection_control_key(control.source()).expect("control key");
        let value = encode_columnar_projection_control_v1(control).expect("control value");
        let mut controls = access
            .transaction()
            .expect("control transaction")
            .open_table(COLUMNAR_PROJECTION_CONTROLS)
            .expect("open controls");
        controls
            .insert(key.as_slice(), value.as_bytes())
            .expect("install control");
        drop(controls);
        let encoded = crate::codec::encode_history_incarnation_v1(current_history_incarnation)
            .expect("encode current history");
        let mut meta = access
            .transaction()
            .expect("metadata transaction")
            .open_table(META)
            .expect("open metadata");
        meta.insert(META_HISTORY_INCARNATION, encoded.as_bytes())
            .expect("install current history");
        drop(meta);
        access
            .commit_for(RedbTestOperation::Initialization)
            .expect("commit control fixture");
    }

    // req: REC-001, PRJ-004, PRJ-006, PRJ-008, PRJ-009, PRJ-010
    #[test]
    fn columnar_history_reset_uses_transaction_current_incarnation() {
        let (_path, ports) = ports(
            "columnar-history-reset-current",
            RedbTestController::observe_index_migration(),
        );
        let (source, definition) = source();
        let spec = ColumnarProjectionSpecHashV1::from_bytes([0x72; 32]);
        let limits = ColumnarProjectionReplayLimitsV1::new(60, 1_024, 10).expect("limits");
        let fresh =
            FreshColumnarProjectionControlV1::new(source.clone(), definition, spec, limits, 1)
                .expect("stale fixture");
        assert_eq!(
            ports
                .initialize_fresh_v1(std::slice::from_ref(&fresh))
                .expect("install stale fixture"),
            ColumnarProjectionControlWriteResultV1::Applied
        );
        stamp_current_history(&ports, 7);

        assert_eq!(
            ports
                .reset_for_current_history_incarnation(fresh.control())
                .expect("reset stale control"),
            ColumnarProjectionControlWriteResultV1::Applied
        );
        let reset = ports
            .recover_expected_control(&source)
            .expect("reread reset")
            .expect("reset control");
        let candidate = reset.candidate().expect("fresh candidate");
        assert_eq!(candidate.history_incarnation(), 7);
        assert_eq!(candidate.generation().get(), 2);
        assert_eq!(candidate.frontier(), FrontierPosition::BeforeFirst);
        assert!(candidate.artifact().is_none());
        assert_eq!(reset.target_definition_fingerprint(), definition);
        assert_eq!(reset.target_spec_hash(), spec);
        assert_eq!(reset.replay_limits(), limits);
        assert_eq!(
            ports
                .reset_for_current_history_incarnation(fresh.control())
                .expect("stale expected control is only a mismatch"),
            ColumnarProjectionControlWriteResultV1::StateChanged
        );
        assert!(
            ports.reset_for_current_history_incarnation(&reset).is_err(),
            "a current control cannot reset again"
        );
    }

    // req: REC-001, PRJ-004, PRJ-006, PRJ-008, PRJ-009, PRJ-010
    #[test]
    fn columnar_history_reset_unknown_commit_rereads_exact_control() {
        let controller = RedbTestController::return_unknown_after_commit(
            RedbTestOperation::ColumnarProjectionControl,
        );
        let (path, ports) = ports("columnar-history-reset-unknown", controller);
        let (source, definition) = source();
        let stale = StoredColumnarProjectionControlV1::initialize_fresh_v1(
            source.clone(),
            definition,
            ColumnarProjectionSpecHashV1::from_bytes([0x73; 32]),
            ColumnarProjectionReplayLimitsV1::new(60, 1_024, 10).expect("limits"),
            1,
        )
        .expect("stale fixture");
        install_control_without_columnar_failpoint(&ports, &stale, 2);

        let error = ports
            .reset_for_current_history_incarnation(&stale)
            .expect_err("post-commit reset is uncertain");
        assert_eq!(error.kind(), StorageErrorKind::CommitStatusUnknown);
        let durable = ports
            .recover_expected_control(&source)
            .expect("exact durable reread")
            .expect("reset is durable");
        let candidate = durable.candidate().expect("replacement candidate");
        assert_eq!(candidate.history_incarnation(), 2);
        assert_eq!(candidate.generation().get(), 2);
        assert!(candidate.artifact().is_none());
        drop(ports);
        let store = RedbStore::open(&path.0).expect("reopen after uncertain reset");
        let reopened = RedbDormantPorts {
            shared: store.shared,
        }
        .into_operational_after_catalog_validation()
        .expect("reopen operational ports");
        assert_eq!(
            reopened
                .reset_for_current_history_incarnation(&stale)
                .expect("repeat with old expected control"),
            ColumnarProjectionControlWriteResultV1::StateChanged,
            "repeated restart cannot allocate a second replacement"
        );
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

    // req: PRJ-002, PRJ-006, PRJ-007, PRJ-010, OQ-020, OQ-024, OQ-053
    #[test]
    fn redb_columnar_control_concurrent_expected_cas_has_one_winner() {
        let (_path, ports) = ports(
            "columnar-control-concurrent-cas",
            RedbTestController::observe_index_migration(),
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
    }

    // req: PRJ-002, PRJ-006, PRJ-008, PRJ-009, PRJ-010, OQ-020, OQ-022, OQ-024, OQ-053
    #[test]
    fn columnar_control_pointer_cas_requires_transaction_current_head() {
        let (_path, ports) = ports(
            "columnar-v2-prepared-root-cas",
            RedbTestController::observe_index_migration(),
        );
        let (source, definition) = source();
        let spec = ColumnarProjectionSpecHashV1::from_bytes([0x22; 32]);
        let limits = ColumnarProjectionReplayLimitsV1::new(60, 1_024, 10).expect("limits");
        let fresh =
            FreshColumnarProjectionControlV1::new(source.clone(), definition, spec, limits, 1)
                .expect("fresh");
        assert_eq!(
            ports
                .initialize_fresh_v1(std::slice::from_ref(&fresh))
                .expect("initialize"),
            ColumnarProjectionControlWriteResultV1::Applied
        );

        let initial = ports
            .recover_expected_control(&source)
            .expect("read initial")
            .expect("initial");
        let initial_candidate = initial.candidate().expect("V1 candidate");
        let v1_pointer =
            riffdb_storage_api::StoredColumnarProjectionGenerationV1::prepared_candidate(
                initial_candidate.generation(),
                ColumnarProjectionLayoutV1::V1,
                FrontierPosition::BeforeFirst,
                FrontierPosition::BeforeFirst,
                1,
                riffdb_storage_api::ColumnarProjectionArtifactV1::new(64, [0x31; 32])
                    .expect("V1 artifact"),
                definition,
                spec,
                None,
            )
            .expect("prepared V1 pointer");
        assert_eq!(
            ports
                .record_durable_snapshot_pointer(&initial, v1_pointer)
                .expect("record V1"),
            ColumnarProjectionControlWriteResultV1::Applied
        );
        let prepared_v1_control = ports
            .recover_expected_control(&source)
            .expect("read V1")
            .expect("V1");
        let selected_v1 = prepared_v1_control.candidate().expect("candidate").clone();
        assert_eq!(
            ports
                .publish_prepared_generation_pointer(&prepared_v1_control, selected_v1)
                .expect("publish V1"),
            ColumnarProjectionControlWriteResultV1::Applied
        );

        let ready_v1 = ports
            .recover_expected_control(&source)
            .expect("read ready V1")
            .expect("ready V1");
        let physical = [0x51; 32];
        assert_eq!(
            ports
                .begin_v2_candidate(&ready_v1, physical)
                .expect("allocate V2"),
            ColumnarProjectionControlWriteResultV1::Applied
        );
        let catching_up = ports
            .recover_expected_control(&source)
            .expect("read catching up")
            .expect("catching up");
        let v2_candidate = catching_up.candidate().expect("V2 candidate");
        let v2_pointer =
            riffdb_storage_api::StoredColumnarProjectionGenerationV1::prepared_candidate(
                v2_candidate.generation(),
                ColumnarProjectionLayoutV1::V2,
                FrontierPosition::BeforeFirst,
                FrontierPosition::BeforeFirst,
                1,
                riffdb_storage_api::ColumnarProjectionArtifactV1::new(128, [0x61; 32])
                    .expect("root artifact"),
                definition,
                spec,
                Some(physical),
            )
            .expect("prepared V2 pointer");
        assert_eq!(
            ports
                .record_durable_snapshot_pointer(&catching_up, v2_pointer.clone())
                .expect("select prepared root"),
            ColumnarProjectionControlWriteResultV1::Applied
        );
        let prepared_v2_control = ports
            .recover_expected_control(&source)
            .expect("read prepared V2")
            .expect("prepared V2");

        assert_eq!(
            ports
                .publish_prepared_generation_pointer(&prepared_v2_control, v2_pointer.clone())
                .expect("publish at current head"),
            ColumnarProjectionControlWriteResultV1::Applied
        );
        let published = ports
            .recover_expected_control(&source)
            .expect("read published")
            .expect("published");
        assert_eq!(
            published.published().expect("selected V2").layout(),
            ColumnarProjectionLayoutV1::V2
        );

        assert_eq!(
            ports
                .publish_prepared_generation_pointer(&prepared_v2_control, v2_pointer)
                .expect("stale exact control"),
            ColumnarProjectionControlWriteResultV1::StateChanged,
            "the complete consumed control is compared in the same transaction"
        );

        assert_eq!(
            ports
                .allocate_same_spec_candidate(&published, physical)
                .expect("allocate head-racing candidate"),
            ColumnarProjectionControlWriteResultV1::Applied
        );
        let rebuilding = ports
            .recover_expected_control(&source)
            .expect("read rebuilding")
            .expect("rebuilding");
        let racing_candidate = rebuilding.candidate().expect("racing candidate");
        let ahead = FrontierPosition::AppliedThrough(
            CommitSequence::new(1).expect("frontier ahead of empty current head"),
        );
        let racing_pointer =
            riffdb_storage_api::StoredColumnarProjectionGenerationV1::prepared_candidate(
                racing_candidate.generation(),
                ColumnarProjectionLayoutV1::V2,
                FrontierPosition::BeforeFirst,
                ahead,
                1,
                riffdb_storage_api::ColumnarProjectionArtifactV1::new(129, [0x62; 32])
                    .expect("racing root artifact"),
                definition,
                spec,
                Some(physical),
            )
            .expect("racing pointer");
        assert_eq!(
            ports
                .record_durable_snapshot_pointer(&rebuilding, racing_pointer.clone())
                .expect("record racing root"),
            ColumnarProjectionControlWriteResultV1::Applied
        );
        let racing_control = ports
            .recover_expected_control(&source)
            .expect("read racing control")
            .expect("racing control");
        let error = ports
            .publish_prepared_generation_pointer(&racing_control, racing_pointer)
            .expect_err("candidate frontier above transaction-current head must refuse");
        assert_eq!(error.kind(), StorageErrorKind::InvariantViolation);
        assert_eq!(
            ports
                .recover_expected_control(&source)
                .expect("recover refused head race")
                .expect("control survives refusal"),
            racing_control,
            "head-race refusal cannot mutate the selected V2 or candidate"
        );
    }
}
