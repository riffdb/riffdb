//! Owned handoff across the separate structural/catalog proof join. Dropping
//! this dormant capability releases exclusivity without changing durable bytes.

use super::*;
use riffdb_storage_api::{AdministrationSequenceAllocator, ChangelogLineageV3, LeadershipEpochV1};
use riffdb_types::{AdministrationSequence, DualFrontier};

pub(crate) struct PendingV3Activation {
    lease: ExclusiveLease,
    durable_commit_epoch: u64,
    retained: RetainedMetadataV1,
    terminal_execution_failure_rows: u64,
}

impl PendingV3Activation {
    // Only the parent structural session can mint this after both walks end
    // without findings and without any CLEAN or validated-prefix shortcut.
    pub(super) fn from_finished_session(
        lease: ExclusiveLease,
        durable_commit_epoch: u64,
        retained: RetainedMetadataV1,
        terminal_execution_failure_rows: u64,
    ) -> Self {
        Self {
            lease,
            durable_commit_epoch,
            retained,
            terminal_execution_failure_rows,
        }
    }

    pub(crate) fn activate(self, shared: &SharedRedb) -> Result<ExclusiveLease, StorageError> {
        if shared.durable_commit_epoch() != self.durable_commit_epoch {
            return Err(corrupt());
        }
        let lineage = ChangelogLineageV3::new(
            self.retained.database_id(),
            self.retained.history_incarnation(),
            LeadershipEpochV1::initial(),
        )
        .map_err(|_| corrupt())?;
        let application = match self.retained.application_sequence() {
            ApplicationSequenceAllocator::Next(next) => CommitSequence::new(next.get() - 1),
            ApplicationSequenceAllocator::Exhausted => CommitSequence::new(u64::MAX),
        };
        let administration = match self.retained.administration_sequence() {
            AdministrationSequenceAllocator::Next(next) => {
                AdministrationSequence::new(next.get() - 1)
            }
            AdministrationSequenceAllocator::Exhausted => AdministrationSequence::new(u64::MAX),
        };
        let mut transaction = shared.database.begin_write().map_err(transaction_error)?;
        crate::changelog_v3_activation::stage_validated(
            &mut transaction,
            lineage,
            DualFrontier::new(application, administration),
        )?;
        shared.commit_durable(transaction)?;
        crate::changelog_v3_activation::activation_edge("committed");

        // These are the existing post-validation prefix and DIRTY transactions,
        // now after activation so both have their exact V3 receipt. The exclusive
        // lease is returned to the operational handoff, never released between.
        shared.seed_terminal_execution_failure_rows(self.terminal_execution_failure_rows);
        shared.set_startup_validation_clean(true);
        if crate::validated_prefix::write_validated_prefix_checkpoint(
            shared,
            &self.retained,
            crate::validated_prefix::CheckpointPurpose::StartupValidation,
        )
        .is_err()
        {
            shared.note_checkpoint_write_failure();
        }
        shared.advance_dirty_lifecycle_before_activation(
            self.retained.database_id(),
            self.retained.history_incarnation(),
            None,
        )?;
        shared.bounded_clean_startup.store(false, Ordering::Release);
        Ok(self.lease)
    }
}
