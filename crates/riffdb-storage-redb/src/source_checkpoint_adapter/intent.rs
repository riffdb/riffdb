//! Actual Source-admitted canonical net intent and definite read-back custody.
//!
//! This closed family has no IDs-only constructor. Its prepare/apply/read-back
//! children create these values from one genuine native request and Source lease.
use crate::{RedbOperationalPorts, gate::ExclusiveLease};
use riffdb_projection::source_driver::SourceCheckpointOperationKind;
use riffdb_storage_api::{AuthoritativeTransactionV3, ChangelogHistoryStateV3};
use riffdb_types::{ProjectionSourceLocatorV1, ProjectionSourceWorkAllocationV1};

pub(crate) mod builder;
mod no_submission;
pub(crate) mod publication_recipe;
mod readback;
mod recipes;
#[allow(dead_code)]
pub(crate) mod source_fixed;
pub(crate) mod step_evidence;
pub(super) use no_submission::verified_before;
pub(crate) use source_fixed::CommitAssignedSourceFixedRecipe;
pub(crate) use {
    readback::inspect_physical_source_image,
    recipes::{prepare_allocation_intent, prepare_copy_intent},
};

// One shared admitted allocation survives the worker and any uncertain outcome.
// It contains no stage borrow, EngineOwner, database handle or retained input pin.
pub(crate) type NativeSourceStepIntent =
    ProjectionSourceWorkAllocationV1<NativeSourceCanonicalIntent>;
pub(crate) struct NativeSourceCanonicalIntent {
    operation: SourceCheckpointOperationKind,
    locator: ProjectionSourceLocatorV1,
    predecessor: ChangelogHistoryStateV3,
    receipt: AuthoritativeTransactionV3,
    canonical_receipt: Vec<u8>,
    // Set only from the exact prepared final abort state, never from a mere Err.
    final_abort: bool,
}
riffdb_types::inspect_owned_capacity_struct_v1!(NativeSourceCanonicalIntent {
    owned: [receipt, canonical_receipt],
    inline: [operation, locator, predecessor, final_abort]
});
impl NativeSourceCanonicalIntent {
    pub(crate) fn locator(&self) -> ProjectionSourceLocatorV1 {
        self.locator
    }
    pub(crate) fn operation(&self) -> SourceCheckpointOperationKind {
        self.operation
    }
    pub(crate) fn predecessor_history(&self) -> ChangelogHistoryStateV3 {
        self.predecessor
    }
    pub(crate) fn is_final_abort(&self) -> bool {
        self.final_abort
    }
    pub(crate) fn receipt(&self) -> &AuthoritativeTransactionV3 {
        &self.receipt
    }
    pub(crate) fn canonical_receipt(&self) -> &[u8] {
        &self.canonical_receipt
    }
}

// Private exact read-back classification. Neither numeric census nor an Ok/Err
// transaction result creates this; a matching physical reader must prove it.
enum VerifiedSourceImage {
    ExactBefore,
    ExactAfter,
}
pub(crate) struct NativeVerifiedSourceStep<'engine> {
    actual: &'engine RedbOperationalPorts,
    lease: &'engine ExclusiveLease,
    mutation: &'engine crate::store::source_usage_domain::SourceUsageMutation<'engine>,
    intent: NativeSourceStepIntent,
    image: VerifiedSourceImage,
}
impl NativeVerifiedSourceStep<'_> {
    pub(crate) fn actual_ports(&self) -> &RedbOperationalPorts {
        self.actual
    }
    pub(crate) fn source_lease(&self) -> &ExclusiveLease {
        self.lease
    }
    pub(crate) fn source_version(&self) -> u64 {
        self.mutation.native_version()
    }
    pub(crate) fn mutation(&self) -> &crate::store::source_usage_domain::SourceUsageMutation<'_> {
        self.mutation
    }
    pub(crate) fn locator(&self) -> ProjectionSourceLocatorV1 {
        self.intent.locator
    }
    pub(crate) fn intent(&self) -> &NativeSourceStepIntent {
        &self.intent
    }
    pub(crate) fn matches_intent(&self, intent: &NativeSourceStepIntent) -> bool {
        std::ptr::eq(&*self.intent, &**intent)
    }
    pub(crate) fn is_exact_before(&self) -> bool {
        matches!(self.image, VerifiedSourceImage::ExactBefore)
    }
    pub(crate) fn ends_input_obligation(&self) -> bool {
        match self.image {
            VerifiedSourceImage::ExactBefore => {
                self.intent.operation == SourceCheckpointOperationKind::Allocate
            }
            VerifiedSourceImage::ExactAfter => {
                self.intent.operation == SourceCheckpointOperationKind::Publish
                    || (self.intent.operation == SourceCheckpointOperationKind::Abort
                        && self.intent.final_abort)
            }
        }
    }
}
impl Drop for NativeVerifiedSourceStep<'_> {
    fn drop(&mut self) {}
}
