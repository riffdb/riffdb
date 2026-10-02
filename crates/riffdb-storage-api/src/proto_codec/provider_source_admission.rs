//! Admitted entity reconstruction through the ordinary semantic mapper.
// req: OQ-040
use super::{DurableCodecError, application::entity_from_proto};
use crate::StoredEntityRecordV1;
use riffdb_proto::envelope::{
    ProviderDecodeScratchV1, ProviderSourceDecodeErrorV1, inspect_provider_source_decode_v1,
};
use riffdb_proto::storage::v1 as wire;
use riffdb_types::exact_provider_budget_v1::{
    BudgetUnavailable, ProviderAllocation, ProviderBudget,
};
use riffdb_types::inspect_canonical_decode_capacity_v1;
use std::sync::Arc;

/// Safe separation between retriable admission and ordinary semantic rejection.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderRecordDecodeErrorV1 {
    /// Aggregate capacity or allocation is unavailable.
    AdmissionUnavailable,
    /// Ordinary durable semantic validation failed.
    Codec(DurableCodecError),
}
impl From<BudgetUnavailable> for ProviderRecordDecodeErrorV1 {
    fn from(_: BudgetUnavailable) -> Self {
        Self::AdmissionUnavailable
    }
}
impl From<DurableCodecError> for ProviderRecordDecodeErrorV1 {
    fn from(error: DurableCodecError) -> Self {
        Self::Codec(error)
    }
}
impl From<ProviderSourceDecodeErrorV1> for ProviderRecordDecodeErrorV1 {
    fn from(error: ProviderSourceDecodeErrorV1) -> Self {
        match error {
            ProviderSourceDecodeErrorV1::CapacityUnavailable => Self::AdmissionUnavailable,
            ProviderSourceDecodeErrorV1::Envelope(error) => {
                Self::Codec(DurableCodecError::from_decode_envelope(error))
            }
        }
    }
}
impl std::fmt::Display for ProviderRecordDecodeErrorV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AdmissionUnavailable => {
                f.write_str("provider record decode admission unavailable")
            }
            Self::Codec(error) => std::fmt::Display::fmt(error, f),
        }
    }
}
impl std::error::Error for ProviderRecordDecodeErrorV1 {}

/// Immutable first-party read capture with final-reference ownership.
#[doc(hidden)]
pub type AdmittedProviderEntityRecordV1 = Arc<ProviderAllocation<StoredEntityRecordV1>>;

/// Immutable admitted metadata for the normal entity partition page.
#[doc(hidden)]
pub type AdmittedProviderEntityPartitionPageV1 =
    Arc<ProviderAllocation<crate::AuthoritativeEntityPartitionScanPage>>;

/// Decode a provider entity on the same captured input. The generated wire
/// message, private encoding scratch, semantic allocations and shared canonical
/// fields are admitted before construction. All ordinary semantic checks remain.
#[doc(hidden)]
pub fn decode_provider_entity_record_admitted_v1(
    encoded: &[u8],
    budget: &ProviderBudget,
) -> Result<AdmittedProviderEntityRecordV1, ProviderRecordDecodeErrorV1> {
    let input = inspect_provider_source_decode_v1::<wire::StoredEntityRecordV1>(encoded)
        .map_err(DurableCodecError::from_decode_envelope)?;
    let decoded = inspect_canonical_decode_capacity_v1(input.canonical_payload())
        .map_err(|_| DurableCodecError::corrupt())?;
    // Semantic reconstruction verifies canonical fields by decoding and
    // re-encoding; the mapper also reconstructs key and binding metadata.
    // Four source copies cover the generated clone, verification buffers and
    // retained encoded fields; canonical heap sizing is structural, not a byte
    // length multiplier. Shared-field and containing-root owners are separate.
    let metadata_bytes = u64::try_from(encoded.len())
        .ok()
        .and_then(|bytes| bytes.checked_mul(8))
        .and_then(|bytes| bytes.checked_add(32 * 1024))
        .ok_or(BudgetUnavailable)?;
    let field_bytes = decoded
        .owned_heap_bytes()
        .checked_add(metadata_bytes)
        .ok_or(BudgetUnavailable)?;
    let maximum = field_bytes
        .checked_add(metadata_bytes)
        .ok_or(BudgetUnavailable)?;
    let mut work = budget.reserve(maximum)?;
    let mut scratch = ProviderDecodeScratchV1::new(encoded.len(), budget)?;
    let message = input.decode_with_scratch(&mut scratch)?;
    let record = entity_from_proto((**message).clone())?;
    // Original fields are held in their charged data owner. A semantic record
    // clone shares that owner, so dropping this read cannot release live fields.
    let original = work.split(field_bytes)?.commit_value(record)?;
    let record = StoredEntityRecordV1::from_admitted_provider_original_v1(original);
    drop(message);
    drop(scratch);
    work.commit_value(record).map_err(Into::into)
}
