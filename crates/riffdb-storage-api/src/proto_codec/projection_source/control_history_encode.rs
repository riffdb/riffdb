//! Source-owned encodings for the existing physical history control roles.
//! Reuses the sole ordinary semantic mappers; no durable bytes or authority change.
use super::control_encode::{buffers, work_bound};
use super::*;
use crate::ChangelogHistoryStateV3;

/// Preadmits generated history buffers and its retained canonical envelope.
/// The fixed history value is structural evidence, not a publication grant.
pub fn encode_changelog_history_state_source_admitted_v3(
    value: ChangelogHistoryStateV3,
    budget: &ProjectionSourceWorkingBudgetV1,
) -> Result<
    ProjectionSourceWorkAllocationV1<CanonicalStoredEnvelopeV1>,
    ProjectionSourceRecordDecodeErrorV1,
> {
    // Sole history_to_wire mapper owns DB, catalog and three position hashes.
    let work = budget.reserve(work_bound::<wire::StoredChangelogHistoryStateV3>(buffers(
        &[16, 32, 32, 32, 32],
    )?)?)?;
    let encoded = super::super::changelog::encode_changelog_history_state_v3(value)?;
    if encoded.as_bytes().len() > MAX_PROJECTION_SOURCE_CONTROL_BYTES {
        return Err(DurableCodecError::new(DurableCodecErrorKind::LimitExceeded).into());
    }
    work.settle_owned_value(encoded).map_err(Into::into)
}

/// Preadmits the unchanged fixed scalar transaction allocator and final output.
/// A structural Next value does not assign a transaction sequence.
pub fn encode_changelog_transaction_allocator_source_admitted_v3(
    value: crate::ChangelogTransactionAllocator,
    budget: &ProjectionSourceWorkingBudgetV1,
) -> Result<
    ProjectionSourceWorkAllocationV1<CanonicalStoredEnvelopeV1>,
    ProjectionSourceRecordDecodeErrorV1,
> {
    let work = budget.reserve(work_bound::<wire::StoredChangelogTransactionAllocatorV3>(
        0,
    )?)?;
    let encoded = super::super::metadata::encode_changelog_transaction_allocator_v3(value)?;
    if encoded.as_bytes().len() > MAX_PROJECTION_SOURCE_CONTROL_BYTES {
        return Err(DurableCodecError::new(DurableCodecErrorKind::LimitExceeded).into());
    }
    work.settle_owned_value(encoded).map_err(Into::into)
}
