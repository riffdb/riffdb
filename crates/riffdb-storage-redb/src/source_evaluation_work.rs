//! Closed native work custody. This owner grants no evaluation or publication proof.
//!
//! One originating engine worker and logical-prefix lease own one staging slot.
//! The slot's exclusive borrow lasts through the disk job and every map alias.
use crate::store::OriginalPrefixRetentionLease;
use riffdb_types::ProjectionSourceWorkingBudgetV1;
use std::sync::{Arc, atomic::AtomicBool};

#[cfg(test)]
pub(crate) struct SourceEvaluationWorkOwner {
    prefix: OriginalPrefixRetentionLease,
    budget: ProjectionSourceWorkingBudgetV1,
    cancellation: Arc<AtomicBool>,
    staging_slot: (),
}

#[cfg(test)]
impl SourceEvaluationWorkOwner {
    pub(crate) fn new(prefix: OriginalPrefixRetentionLease, cancellation: Arc<AtomicBool>) -> Self {
        // Derive the ledger from the actual originating capture. The caller
        // cannot provide another engine or independently selected allowance.
        let budget = prefix.originating_budget().clone();
        Self {
            prefix,
            budget,
            cancellation,
            staging_slot: (),
        }
    }

    /// Splits immutable input from the one exclusive two-pass stage factory.
    /// Each job borrows the factory slot, and maps borrow that job. The next job
    /// cannot exist until the previous map/backend/disk job physically drops.
    pub(crate) fn split_session(
        &mut self,
    ) -> (&OriginalPrefixRetentionLease, SourceStageSlotFactory<'_>) {
        (
            &self.prefix,
            SourceStageSlotFactory {
                exclusive_slot: &mut self.staging_slot,
                prefix: &self.prefix,
                budget: &self.budget,
                cancellation: &self.cancellation,
            },
        )
    }

    pub(crate) fn split_for_stage(
        &mut self,
    ) -> (&OriginalPrefixRetentionLease, SourceStageSlotLease<'_>) {
        (
            &self.prefix,
            SourceStageSlotLease {
                _exclusive_slot: &mut self.staging_slot,
                prefix: &self.prefix,
                budget: &self.budget,
                cancellation: &self.cancellation,
            },
        )
    }
}

// No public fields, Clone, independent constructor, callbacks, or worker claim.
// Consuming this token into the disk job keeps the one slot borrowed until
// complete backend/map teardown and exact private filesystem cleanup.
pub(crate) struct SourceStageSlotLease<'work> {
    _exclusive_slot: &'work mut (),
    prefix: &'work OriginalPrefixRetentionLease,
    budget: &'work ProjectionSourceWorkingBudgetV1,
    cancellation: &'work Arc<AtomicBool>,
}

impl SourceStageSlotLease<'_> {
    pub(crate) fn matches_prefix(&self, prefix: &OriginalPrefixRetentionLease) -> bool {
        std::ptr::eq(self.prefix, prefix)
    }
    pub(crate) fn originating_budget(&self) -> &ProjectionSourceWorkingBudgetV1 {
        self.budget
    }
    pub(crate) fn cancellation(&self) -> &Arc<AtomicBool> {
        self.cancellation
    }
}

// Keep the exclusive work/capture borrow live through physical job destruction.
impl Drop for SourceStageSlotLease<'_> {
    fn drop(&mut self) {}
}

// No constructor outside this owning module; no extra worker/ledger/disk slot.
// Input borrows the parent's separate immutable prefix throughout both passes.
pub(crate) struct SourceStageSlotFactory<'owner> {
    exclusive_slot: &'owner mut (),
    prefix: &'owner OriginalPrefixRetentionLease,
    budget: &'owner ProjectionSourceWorkingBudgetV1,
    cancellation: &'owner Arc<AtomicBool>,
}
impl<'owner> SourceStageSlotFactory<'owner> {
    pub(crate) fn matches_prefix(&self, prefix: &OriginalPrefixRetentionLease) -> bool {
        std::ptr::eq(self.prefix, prefix)
    }
    #[cfg(test)]
    pub(crate) fn cancellation(&self) -> &'owner Arc<AtomicBool> {
        self.cancellation
    }
    pub(crate) fn acquire(&mut self) -> SourceStageSlotLease<'_> {
        SourceStageSlotLease {
            _exclusive_slot: &mut *self.exclusive_slot,
            prefix: self.prefix,
            budget: self.budget,
            cancellation: self.cancellation,
        }
    }
}
impl Drop for SourceStageSlotFactory<'_> {
    fn drop(&mut self) {}
}
