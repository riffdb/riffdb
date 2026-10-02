//! Transaction/lease owner for the closed crate-private source-control lanes.
//! Drop aborts and releases non-durable capabilities; nothing is exported.

use super::*;

pub(super) struct Barrier {
    shared: Arc<SharedRedb>,
    _lease: ExclusiveLease,
    pub(super) root: Arc<crate::checkpoint_root::CheckpointRoot>,
    observation: Observation,
    paged_validation: bool,
}

impl Barrier {
    pub(super) fn try_validation_root(
        shared: &Arc<SharedRedb>,
    ) -> Result<Option<Arc<crate::checkpoint_root::CheckpointRoot>>, StorageError> {
        let Some(_lease) = shared.mutation_gate.try_acquire()? else {
            return Ok(None);
        };
        if shared.write_fenced.load(Ordering::Acquire) || shared.is_follower_mode() {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        if !drained_for_validation(shared)? {
            return Ok(None);
        }
        shared.capture_checkpoint_root().map(Some)
    }
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
            paged_validation: false,
        })
    }

    /// Admit only the exact root exhaustively validated outside the gate.
    /// A queued writer, checkpoint, or unpublished suffix causes a yield;
    /// this path never drains or waits for another maintenance operation.
    pub(super) fn try_from_validated(
        shared: Arc<SharedRedb>,
        proof: crate::changelog_v3_retained_validation::ValidatedRetainedHistory,
    ) -> Result<Option<Self>, StorageError> {
        let Some(lease) = shared.mutation_gate.try_acquire()? else {
            return Ok(None);
        };
        if shared.write_fenced.load(Ordering::Acquire) || shared.is_follower_mode() {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        if !drained_for_validation(&shared)? {
            return Ok(None);
        }
        let root = shared.capture_checkpoint_root()?;
        if root.identity() != proof.root().identity() {
            return Ok(None);
        }
        let history = crate::changelog_v3_roots::read_checkpoint_roots(&root)?
            .ok_or_else(|| storage_error(StorageErrorKind::IncompatibleFormat))?;
        if history != proof.history() {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
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
        drop(meta);
        // An installed asynchronous checkpoint can retain an empty composite
        // publication over its old base. With no suffix or unpublished work,
        // retire that internal cache exactly as the existing drained barrier
        // does. Captured readers keep their own pins; no frame is drained and
        // the authoritative application/administration frontiers do not move.
        shared.clear_composite_publication()?;
        let observation = Observation {
            publication: root.identity(),
            durable_epoch: shared.durable_commit_epoch(),
            history,
        };
        Ok(Some(Self {
            shared,
            _lease: lease,
            root,
            observation,
            paged_validation: true,
        }))
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
        let history = if write.barrier.paged_validation {
            crate::changelog_v3_roots::read_checkpoint_roots_for_write(write.transaction()?)?
        } else {
            crate::changelog_v3_roots::validate_retained_history_for_write(write.transaction()?)?
        };
        if history != Some(write.history()) {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        Ok(write)
    }
}

fn drained_for_validation(shared: &SharedRedb) -> Result<bool, StorageError> {
    if shared.async_checkpoint_in_flight()? || shared.journal_suffix_activity()?.1 != 0 {
        return Ok(false);
    }
    // Materialization publication precedes off-gate extent sealing. Starting
    // a walk during that rename window can select a locator whose durable
    // filename is still being published. Yield rather than join under gate.
    let sealer = shared
        .journal_sealer
        .lock()
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    Ok(sealer
        .as_ref()
        .is_none_or(std::thread::JoinHandle::is_finished))
}

pub(crate) struct ControlWrite {
    barrier: Barrier,
    raw: Option<WriteTransaction>,
}

impl ControlWrite {
    // Visible only to this closed owner, never a public or arbitrary callback.
    pub(crate) fn transaction(&self) -> Result<&WriteTransaction, StorageError> {
        self.raw
            .as_ref()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))
    }
    pub(super) fn history(&self) -> History {
        self.barrier.observation.history
    }
    pub(crate) fn validate(&self) -> Result<(), StorageError> {
        let history = if self.barrier.paged_validation {
            // Only bounded receipt deletion can construct this owner. The
            // exact immutable preimage was fully validated before admission;
            // stage preserves every surviving row and checks each removal.
            crate::changelog_v3_roots::read_checkpoint_roots_for_write(self.transaction()?)?
        } else {
            crate::changelog_v3_roots::validate_retained_history_for_write(self.transaction()?)?
        };
        history.ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        Ok(())
    }
    pub(super) fn commit(self) -> Result<Observation, StorageError> {
        self.commit_with_test_operation(None)
    }
    pub(super) fn commit_retention(self) -> Result<Observation, StorageError> {
        self.commit_with_test_operation(Some(RedbTestOperation::RetentionHold))
    }
    pub(crate) fn commit_primary_fence(self) -> Result<(), StorageError> {
        self.commit_with_test_operation(Some(RedbTestOperation::PrimaryFence))?;
        Ok(())
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

    // req: STO-030, STO-031, STO-032, REC-001
    #[test]
    fn online_validation_capture_and_admission_yield_until_extent_sealing_finishes() {
        let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
        let mut walk = crate::changelog_v3_retained_validation::RetainedHistoryValidation::new(
            Barrier::try_validation_root(&ports.shared)
                .unwrap()
                .unwrap(),
        )
        .unwrap()
        .unwrap();
        while !walk
            .advance(riffdb_storage_api::EvidencePageLimit::new(1).unwrap())
            .unwrap()
        {}
        let proof = walk.complete().unwrap();
        let (entered, entry) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            entered.send(()).unwrap();
            released.recv().unwrap();
        });
        *ports.shared.journal_sealer.lock().unwrap() = Some(worker);
        entry.recv().unwrap();
        let epoch = ports.shared.durable_commit_epoch();
        assert!(
            Barrier::try_validation_root(&ports.shared)
                .unwrap()
                .is_none()
        );
        assert!(
            Barrier::try_from_validated(Arc::clone(&ports.shared), proof)
                .unwrap()
                .is_none()
        );
        assert!(!ports.shared.mutation_gate.is_held());
        assert_eq!(ports.shared.durable_commit_epoch(), epoch);
        release.send(()).unwrap();
        ports.shared.join_journal_sealer();
        assert!(
            Barrier::try_validation_root(&ports.shared)
                .unwrap()
                .is_some()
        );
        assert!(!ports.shared.mutation_gate.is_held());
    }

    // req: STO-030, STO-031, STO-032, REC-001
    #[test]
    fn completed_online_proof_cannot_admit_after_a_hold_changes_its_publication() {
        let (_scope, ports, states) = crate::changelog_v3_control_tests::fixture();
        let mut walk = crate::changelog_v3_retained_validation::RetainedHistoryValidation::new(
            ports.shared.capture_checkpoint_root().unwrap(),
        )
        .unwrap()
        .unwrap();
        while !walk
            .advance(riffdb_storage_api::EvidencePageLimit::new(1).unwrap())
            .unwrap()
        {}
        let proof = walk.complete().unwrap();
        let root = proof.root().identity();
        let bootstrap = Hold::new(
            riffdb_storage_api::ReplicationSourceHoldIdV1::new([0x63; 16]).unwrap(),
            Kind::Bootstrap,
            states[1].lineage(),
            states[1].tail(),
        );
        ports
            .replication_source_control()
            .register(bootstrap)
            .unwrap();
        assert_ne!(
            root,
            ports.shared.capture_checkpoint_root().unwrap().identity()
        );
        let epoch = ports.shared.durable_commit_epoch();
        assert!(
            Barrier::try_from_validated(Arc::clone(&ports.shared), proof)
                .unwrap()
                .is_none()
        );
        assert_eq!(ports.shared.durable_commit_epoch(), epoch);
        assert!(!ports.shared.mutation_gate.is_held());
        assert_eq!(
            crate::changelog_v3_roots::validate_retained_history(
                &ports.shared.capture_checkpoint_root().unwrap()
            )
            .unwrap()
            .unwrap()
            .minimum_resume(),
            states[0].tail()
        );
    }
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
