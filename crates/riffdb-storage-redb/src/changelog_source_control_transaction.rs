//! Transaction/lease owner for the two closed crate-private control lanes.
//! Drop aborts and releases non-durable capabilities; nothing is exported.

use super::*;

pub(super) struct Barrier {
    shared: Arc<SharedRedb>,
    _lease: ExclusiveLease,
    pub(super) root: Arc<crate::checkpoint_root::CheckpointRoot>,
    observation: Observation,
}

impl Barrier {
    pub(super) fn acquire(shared: Arc<SharedRedb>) -> Result<Self, StorageError> {
        let lease = shared.mutation_gate.acquire()?;
        if shared.write_fenced.load(Ordering::Acquire) {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        // No drain or write may follow final CLEAN, even through a retained handle.
        {
            let root = shared.capture_checkpoint_root()?;
            crate::changelog_v3_roots::validate_retained_history(&root)?
                .ok_or_else(|| storage_error(StorageErrorKind::IncompatibleFormat))?;
            let meta = root.open_table(META).map_err(table_error)?;
            if let Some(row) = meta
                .get(META_CLEAN_CLOSE_LIFECYCLE)
                .map_err(precommit_storage_error)?
            {
                let lifecycle = crate::clean_close::CleanCloseLifecycle::decode(row.value())
                    .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
                if matches!(
                    lifecycle.state(),
                    crate::clean_close::CleanCloseState::Clean(_)
                ) {
                    return Err(storage_error(StorageErrorKind::Unavailable));
                }
            }
        }
        shared.publish_pending_journal_prefix()?;
        shared.checkpoint_published_journal_suffix_for_barrier()?;
        let root = shared.capture_checkpoint_root()?;
        let history = crate::changelog_v3_roots::validate_retained_history(&root)?
            .ok_or_else(|| storage_error(StorageErrorKind::IncompatibleFormat))?;
        let observation = Observation {
            publication: root.identity(),
            durable_epoch: shared.durable_commit_epoch(),
            history,
        };
        Ok(Self {
            shared,
            _lease: lease,
            root,
            observation,
        })
    }

    pub(super) fn observation(&self) -> Observation {
        self.observation
    }

    pub(super) fn begin(self) -> Result<ControlWrite, StorageError> {
        let raw = match self.shared.database.begin_write() {
            Ok(raw) => raw,
            Err(error) => {
                return Err(transaction_error(error));
            }
        };
        let mut write = ControlWrite {
            barrier: self,
            raw: Some(raw),
        };
        let raw = write
            .raw
            .as_mut()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        raw.set_two_phase_commit(true);
        raw.set_durability(Durability::Immediate)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        if crate::changelog_v3_roots::validate_retained_history_for_write(write.transaction()?)?
            != Some(write.history())
        {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        Ok(write)
    }
}

pub(super) struct ControlWrite {
    barrier: Barrier,
    raw: Option<WriteTransaction>,
}

impl ControlWrite {
    // Visible only to this closed owner, never a public or arbitrary callback.
    pub(super) fn transaction(&self) -> Result<&WriteTransaction, StorageError> {
        self.raw
            .as_ref()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))
    }
    pub(super) fn history(&self) -> History {
        self.barrier.observation.history
    }
    pub(super) fn validate(&self) -> Result<(), StorageError> {
        crate::changelog_v3_roots::validate_retained_history_for_write(self.transaction()?)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        Ok(())
    }
    pub(super) fn commit(self) -> Result<Observation, StorageError> {
        self.commit_with_test_operation(None)
    }
    pub(super) fn commit_retention(self) -> Result<Observation, StorageError> {
        self.commit_with_test_operation(Some(RedbTestOperation::RetentionHold))
    }
    fn commit_with_test_operation(
        mut self,
        operation: Option<RedbTestOperation>,
    ) -> Result<Observation, StorageError> {
        if let Some(operation) = operation {
            self.barrier.shared.before_test_commit(operation)?;
        }
        let raw = self
            .raw
            .take()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        let shared = &self.barrier.shared;
        if let Err(error) = shared.commit_durable(raw) {
            shared.fence_writes();
            return Err(error);
        }
        if let Some(operation) = operation
            && let Err(error) = shared.after_test_commit(operation)
        {
            shared.fence_writes();
            return Err(error);
        }
        let published = finish_publication(shared);
        if published.is_err() {
            shared.fence_writes();
            return Err(storage_error(StorageErrorKind::CommitStatusUnknown));
        }
        published
    }
}

fn finish_publication(shared: &SharedRedb) -> Result<Observation, StorageError> {
    shared.refresh_durable_read_frontier()?;
    let root = shared.capture_checkpoint_root()?;
    let history = crate::changelog_v3_roots::read_checkpoint_roots(&root)?
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    shared.observe_changelog_snapshot_v3(RedbReadAccess::Durable(Arc::clone(&root)));
    Ok(Observation {
        publication: root.identity(),
        durable_epoch: shared.durable_commit_epoch(),
        history,
    })
}

impl Drop for ControlWrite {
    fn drop(&mut self) {
        if let Some(raw) = self.raw.take() {
            let shared = &self.barrier.shared;
            if raw.abort().is_err() {
                shared.fence_writes();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    // req: REP-003, REC-001, PERF-007, STO-012
    fn cancelled_source_control_aborts_complete_staged_state_and_releases_its_lease() {
        let (_scope, ports, states) = crate::changelog_v3_control_tests::fixture();
        let epoch = ports.shared.durable_commit_epoch();
        let before = crate::changelog_v3_roots::validate_retained_history(
            &ports.shared.database.begin_read().unwrap(),
        )
        .unwrap();
        let fence = Hold::new(
            riffdb_storage_api::ReplicationSourceHoldIdV1::new([0x52; 16]).unwrap(),
            Kind::Bootstrap,
            states[0].lineage(),
            states[0].tail(),
        );
        let write = Barrier::acquire(Arc::clone(&ports.shared))
            .unwrap()
            .begin()
            .unwrap();
        let (receipt, _) = prepare_control_receipt(
            write.transaction().unwrap(),
            write.history(),
            Source::ReplicationSourceHold,
        )
        .unwrap();
        let encoded = encode_replication_source_hold_v1(fence).unwrap();
        write
            .transaction()
            .unwrap()
            .open_table(SOURCE_HOLDS)
            .unwrap()
            .insert(fence.storage_key().as_slice(), encoded.as_bytes())
            .unwrap();
        receipt.stage(write.transaction().unwrap()).unwrap();
        write.validate().unwrap();
        assert!(ports.shared.mutation_gate.is_held());
        drop(write);
        assert!(!ports.shared.mutation_gate.is_held());
        assert_eq!(ports.shared.durable_commit_epoch(), epoch);
        let pin = ports.shared.database.begin_read().unwrap();
        assert_eq!(
            crate::changelog_v3_roots::validate_retained_history(&pin).unwrap(),
            before
        );
        assert!(pin.open_table(SOURCE_HOLDS).unwrap().is_empty().unwrap());
    }
}
