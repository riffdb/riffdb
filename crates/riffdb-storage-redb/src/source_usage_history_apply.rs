//! Exact Source-lease preinstall, fixed staging and existing publication recipe.
use super::*;

/// Fixed preinstallation scheduling result. It grants no source or commit
/// authority, and occurs before any pending usage/input obligation is installed.
pub(crate) enum SourceHistoryNoSubmitYield {
    Undrained,
    HeadAdvanced,
}
pub(crate) enum SourceHistoryPreinstallFailure {
    Yield(SourceHistoryNoSubmitYield),
    Rejected(StorageError),
}
impl From<StorageError> for SourceHistoryPreinstallFailure {
    fn from(error: StorageError) -> Self {
        Self::Rejected(error)
    }
}

/// No Source mutation has begun when this owner is minted. It owns a fresh raw
/// transaction and the same actual Source lease; neither can escape to callers.
pub(crate) struct SourceHistoryPreinstall<'turn, 'work> {
    controls: PreparedSourceHistoryControls<'work>,
    lease: &'turn ExclusiveLease,
    raw: Option<WriteTransaction>,
}
impl Drop for SourceHistoryPreinstall<'_, '_> {
    fn drop(&mut self) {
        if let Some(raw) = self.raw.take()
            && raw.abort().is_err()
        {
            self.controls.actual.shared.fence_writes();
        }
    }
}

/// A fixed helper completed physical and logical publication. This conveys no
/// final candidate/receipt/root authority; exact off-gate read-back is required.
pub(crate) struct SourceHistorySubmitted {
    _private: (),
}

/// Constructor-private definite no-submission evidence. Only successful raw
/// abort under the still-held matching lease mints this. It is consumed once by
/// the intent family's existing ExactBefore factory, never a detached scalar.
pub(crate) struct NativeSourceNoSubmission<'turn, 'work> {
    actual: &'work RedbOperationalPorts,
    lease: &'turn ExclusiveLease,
    mutation: &'turn SourceUsageMutation<'work>,
    intent: NativeSourceStepIntent,
}
impl Drop for NativeSourceNoSubmission<'_, '_> {
    fn drop(&mut self) {}
}
impl<'turn, 'work: 'turn> NativeSourceNoSubmission<'turn, 'work> {
    pub(crate) fn actual_ports(&self) -> &'work RedbOperationalPorts {
        self.actual
    }
    pub(crate) fn source_lease(&self) -> &'turn ExclusiveLease {
        self.lease
    }
    pub(crate) fn mutation(&self) -> &'turn SourceUsageMutation<'work> {
        self.mutation
    }
    pub(crate) fn intent(&self) -> &NativeSourceStepIntent {
        &self.intent
    }
    pub(crate) fn require_current(&self) -> Result<(), StorageError> {
        self.mutation
            .verify_for_native_readback(self.actual, self.lease)?;
        self.mutation.require_native_intent(&self.intent)
    }
}
pub(crate) enum SourceHistoryApplyFailure<'turn, 'work> {
    NoSubmission {
        error: StorageError,
        proof: NativeSourceNoSubmission<'turn, 'work>,
    },
    // Raw abort failed or the physical commit/publication was attempted. This
    // never releases quota or the independently engine-owned Original inputs.
    Uncertain(StorageError),
}

impl<'work> PreparedSourceHistoryControls<'work> {
    /// Check ordinary current head/root before SourceUsageMutation installation.
    /// Stale head is a pre-submit yield and cannot strand an input obligation.
    pub(crate) fn preinstall<'turn, 'request, 'candidate, 'stage, 'slot, 'session>(
        self,
        request: &NativeSourceCheckpointRequest<
            'request,
            'candidate,
            'stage,
            'slot,
            'session,
            'work,
        >,
        lease: &'turn ExclusiveLease,
    ) -> Result<SourceHistoryPreinstall<'turn, 'work>, SourceHistoryPreinstallFailure>
    where
        'work: 'turn,
        'work: 'session,
        'session: 'slot,
        'slot: 'stage,
        'stage: 'candidate,
    {
        let actual = request.actual_ports();
        if !std::sync::Arc::ptr_eq(&self.actual.shared, &actual.shared)
            || !lease.is_source_grant_for(&actual.shared.mutation_gate)
            || request.operation() != self.intent.operation()
            || request.captured_locator() != self.intent.locator()
            || request.captured_history().lineage() != self.intent.predecessor_history().lineage()
            || request.transaction_binding_assignment() != Some(self.intent.receipt().binding())
        {
            return Err(integrity().into());
        }
        check_epoch(&self.epoch, actual, &self.intent)?;
        let shared = &actual.shared;
        if shared.database.native_origin_context().anchor_key() != Some(self.origin_key) {
            return Err(unavailable().into());
        }
        if !crate::store::changelog_source_control::drained_for_validation(shared)? {
            return Err(SourceHistoryPreinstallFailure::Yield(
                SourceHistoryNoSubmitYield::Undrained,
            ));
        }
        if shared.durable_commit_epoch() != self.durable_epoch
            || shared.capture_checkpoint_root()?.identity() != self.publication
        {
            return Err(SourceHistoryPreinstallFailure::Yield(
                SourceHistoryNoSubmitYield::HeadAdvanced,
            ));
        }
        let mut raw = shared.database.begin_write().map_err(transaction_error)?;
        raw.set_two_phase_commit(shared.application_commit_profile.uses_two_phase());
        let checked = (|| {
            raw.set_durability(Durability::Immediate)
                .map_err(|_| integrity())?;
            self.check_fixed_before(&raw)?;
            if raw
                .open_table(crate::changelog_v3_activation::HISTORY)
                .map_err(table_error)?
                .get(
                    self.intent
                        .receipt()
                        .binding()
                        .sequence
                        .get()
                        .to_be_bytes()
                        .as_slice(),
                )
                .map_err(precommit_storage_error)?
                .is_some()
            {
                return Err(unavailable());
            }
            // A drained checkpoint already equals the logical current view.
            // Clear the stale Composite publication before any Source staging.
            shared.clear_composite_publication()?;
            check_epoch(&self.epoch, actual, &self.intent)
        })();
        if let Err(error) = checked {
            if raw.abort().is_err() {
                shared.fence_writes();
                return Err(storage_error(StorageErrorKind::CommitStatusUnknown).into());
            }
            return Err(error.into());
        }
        Ok(SourceHistoryPreinstall {
            controls: self,
            lease,
            raw: Some(raw),
        })
    }

    fn check_fixed_before(&self, raw: &WriteTransaction) -> Result<(), StorageError> {
        let meta = raw.open_table(META).map_err(table_error)?;
        for (key, expected) in fixed_keys()?.into_iter().zip(&self.fixed) {
            let value = meta.get(key).map_err(precommit_storage_error)?;
            if value.as_ref().map(|value| value.value()) != expected.as_deref() {
                return Err(unavailable());
            }
        }
        let active = raw.open_table(CATALOG_ACTIVE).map_err(table_error)?;
        if active
            .get(CATALOG_ACTIVE_KEY.as_slice())
            .map_err(precommit_storage_error)?
            .as_ref()
            .map(|value| value.value())
            != Some(self.pointer.as_slice())
        {
            return Err(unavailable());
        }
        let audit = raw.open_table(AUDIT).map_err(table_error)?;
        if audit
            .get(self.origin_key.as_slice())
            .map_err(precommit_storage_error)?
            .as_ref()
            .map(|value| value.value())
            != Some(self.origin.as_slice())
        {
            return Err(unavailable());
        }
        Ok(())
    }

    /// Fixed postimage checks; no terminal receipt decode/hash or root walk.
    fn check_fixed_after(
        &self,
        root: &crate::checkpoint_root::CheckpointRoot,
    ) -> Result<(), StorageError> {
        let meta = root.open_table(META).map_err(table_error)?;
        for (key, before) in fixed_keys()?.into_iter().zip(&self.fixed) {
            let expected = if Some(key) == Old::ChangelogHistoryState.metadata_key() {
                Some(self.encoded_history.as_slice())
            } else if Some(key) == Old::NextChangelogTransaction.metadata_key() {
                Some(self.encoded_allocator.as_slice())
            } else {
                before.as_deref()
            };
            if meta
                .get(key)
                .map_err(precommit_storage_error)?
                .as_ref()
                .map(|value| value.value())
                != expected
            {
                return Err(integrity());
            }
        }
        let active = root.open_table(CATALOG_ACTIVE).map_err(table_error)?;
        if active
            .get(CATALOG_ACTIVE_KEY.as_slice())
            .map_err(precommit_storage_error)?
            .as_ref()
            .map(|value| value.value())
            != Some(self.pointer.as_slice())
        {
            return Err(integrity());
        }
        let audit = root.open_table(AUDIT).map_err(table_error)?;
        if audit
            .get(self.origin_key.as_slice())
            .map_err(precommit_storage_error)?
            .as_ref()
            .map(|value| value.value())
            != Some(self.origin.as_slice())
        {
            return Err(integrity());
        }
        Ok(())
    }
}

impl<'turn, 'work: 'turn> SourceHistoryPreinstall<'turn, 'work> {
    pub(crate) fn apply(
        mut self,
        mutation: &'turn SourceUsageMutation<'work>,
    ) -> Result<SourceHistorySubmitted, SourceHistoryApplyFailure<'turn, 'work>> {
        let staged = (|| {
            mutation.verify_for_native_readback(self.controls.actual, self.lease)?;
            mutation.require_native_intent(&self.controls.intent)?;
            let _physical = mutation.physical_commit_borrow(self.controls.actual, self.lease)?;
            let raw = self.raw.as_ref().ok_or_else(integrity)?;
            for change in self.controls.intent.receipt().mutations() {
                // The same Source-only epoch/intent proved the expected hashes
                // off-gate; this fixed lower seam does no payload hashing.
                crate::changelog_v3_write::apply_mutation(raw, change)?;
            }
            raw.open_table(crate::changelog_v3_activation::HISTORY)
                .map_err(table_error)?
                .insert(
                    self.controls
                        .intent
                        .receipt()
                        .binding()
                        .sequence
                        .get()
                        .to_be_bytes()
                        .as_slice(),
                    self.controls.intent.canonical_receipt(),
                )
                .map_err(precommit_storage_error)?;
            let mut meta = raw.open_table(META).map_err(table_error)?;
            meta.insert(
                Old::ChangelogHistoryState
                    .metadata_key()
                    .ok_or_else(integrity)?,
                self.controls.encoded_history.as_slice(),
            )
            .map_err(precommit_storage_error)?;
            meta.insert(
                Old::NextChangelogTransaction
                    .metadata_key()
                    .ok_or_else(integrity)?,
                self.controls.encoded_allocator.as_slice(),
            )
            .map_err(precommit_storage_error)?;
            Ok(())
        })();
        if let Err(error) = staged {
            return Err(self.abort_before_submission(mutation, error));
        }
        let physical = match mutation.physical_commit_borrow(self.controls.actual, self.lease) {
            Ok(physical) => physical,
            Err(error) => return Err(self.abort_before_submission(mutation, error)),
        };
        let Some(locator) = self.controls.locator.take() else {
            return Err(self.abort_before_submission(mutation, integrity()));
        };
        let Some(raw) = self.raw.take() else {
            return Err(SourceHistoryApplyFailure::Uncertain(integrity()));
        };
        let shared = &self.controls.actual.shared;
        let footprint = crate::source_continuation::SourceContinuationFootprint::captured(
            self.controls.intent.predecessor_history(),
            self.controls.intent.receipt().mutations(),
        );
        if let Err(error) =
            shared.commit_with_prepared_source_usage_and_locator(raw, footprint, locator, physical)
        {
            shared.fence_writes();
            return Err(SourceHistoryApplyFailure::Uncertain(error));
        }
        // Keep the actual mutation and Source lease live through BOTH physical
        // root publication and logical Composite/journal publication refresh.
        let finished =
            (|| {
                shared.refresh_durable_read_frontier()?;
                let root = shared.capture_checkpoint_root()?;
                self.controls.check_fixed_after(&root)?;
                crate::store::changelog_source_control::finish_source_publication_from_checked_root(
                shared, self.controls.intent.predecessor_history(), root, self.controls.successor)?;
                mutation.verify_for_native_readback(self.controls.actual, self.lease)
            })();
        if finished.is_err() {
            shared.fence_writes();
            return Err(SourceHistoryApplyFailure::Uncertain(storage_error(
                StorageErrorKind::CommitStatusUnknown,
            )));
        }
        Ok(SourceHistorySubmitted { _private: () })
    }

    fn abort_before_submission(
        &mut self,
        mutation: &'turn SourceUsageMutation<'work>,
        error: StorageError,
    ) -> SourceHistoryApplyFailure<'turn, 'work> {
        let Some(raw) = self.raw.take() else {
            return SourceHistoryApplyFailure::Uncertain(error);
        };
        if raw.abort().is_err() {
            self.controls.actual.shared.fence_writes();
            return SourceHistoryApplyFailure::Uncertain(storage_error(
                StorageErrorKind::CommitStatusUnknown,
            ));
        }
        // No physical commit has been invoked. The lease remains held and the
        // exact actual pending mutation still binds this same intent alias.
        if mutation
            .verify_for_native_readback(self.controls.actual, self.lease)
            .is_err()
            || mutation
                .require_native_intent(&self.controls.intent)
                .is_err()
        {
            return SourceHistoryApplyFailure::Uncertain(error);
        }
        SourceHistoryApplyFailure::NoSubmission {
            error,
            proof: NativeSourceNoSubmission {
                actual: self.controls.actual,
                lease: self.lease,
                mutation,
                intent: self.controls.intent.clone(),
            },
        }
    }
}
