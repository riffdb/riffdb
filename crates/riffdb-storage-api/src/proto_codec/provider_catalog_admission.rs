//! Opaque catalog transport reconstruction precedes separately admitted IR decoding.
// req: OQ-040, PRJ-004
use super::{
    DurableCodecError, ProviderRecordDecodeErrorV1,
    catalog::{active_from_proto, bundle_from_proto},
    contract_migration::{migration_record_from_proto, write_retirement_from_proto},
};
use crate::{
    ActiveCatalogPointerV1, StoredContractBundleV1, StoredContractMigrationRecordV1,
    StoredContractWriteRetirementV1,
};
use riffdb_proto::envelope::{
    ProviderDecodeScratchV1, ProviderSourceMessageV1, inspect_provider_source_decode_v1,
};
use riffdb_proto::storage::v1 as wire;
use riffdb_types::exact_provider_budget_v1::{
    BudgetUnavailable, ProviderAllocation, ProviderBudget,
};
use riffdb_types::{InspectOwnedCapacityV1, OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1};
use std::sync::Arc;

/// Read-only first-party capture; opaque IR needs its own construction admission.
#[doc(hidden)]
pub fn decode_provider_catalog_bundle_admitted_v1(
    encoded: &[u8],
    budget: &ProviderBudget,
) -> Result<Arc<ProviderAllocation<StoredContractBundleV1>>, ProviderRecordDecodeErrorV1> {
    decode::<wire::StoredContractBundleV1, _>(encoded, budget, bundle_from_proto)
}
/// Read-only first-party capture of the ordinary exact active identity.
#[doc(hidden)]
pub fn decode_provider_active_catalog_pointer_admitted_v1(
    encoded: &[u8],
    budget: &ProviderBudget,
) -> Result<Arc<ProviderAllocation<ActiveCatalogPointerV1>>, ProviderRecordDecodeErrorV1> {
    decode::<wire::ActiveCatalogPointerV1, _>(encoded, budget, active_from_proto)
}
/// Permanent migration evidence reconstructed with ordinary semantic checks.
#[doc(hidden)]
pub fn decode_provider_contract_migration_record_admitted_v1(
    encoded: &[u8],
    budget: &ProviderBudget,
) -> Result<Arc<ProviderAllocation<StoredContractMigrationRecordV1>>, ProviderRecordDecodeErrorV1> {
    decode::<wire::StoredContractMigrationRecordV1, _>(encoded, budget, migration_record_from_proto)
}
/// Exact predecessor write fence reconstructed under the same closed admission.
#[doc(hidden)]
pub fn decode_provider_contract_write_retirement_admitted_v1(
    encoded: &[u8],
    budget: &ProviderBudget,
) -> Result<Arc<ProviderAllocation<StoredContractWriteRetirementV1>>, ProviderRecordDecodeErrorV1> {
    decode::<wire::StoredContractWriteRetirementV1, _>(encoded, budget, write_retirement_from_proto)
}
fn decode<M: ProviderSourceMessageV1 + Clone, T: InspectOwnedCapacityV1>(
    encoded: &[u8],
    budget: &ProviderBudget,
    map: impl FnOnce(M) -> Result<T, DurableCodecError>,
) -> Result<Arc<ProviderAllocation<T>>, ProviderRecordDecodeErrorV1> {
    let input = inspect_provider_source_decode_v1::<M>(encoded)
        .map_err(DurableCodecError::from_decode_envelope)?;
    // These closed records have bounded buffers and shared lineage/principal data.
    // Admit generated cloning, shared identities, mapper buffers and owners
    // before reconstruction; protobuf verification has independent admitted work.
    let maximum = u64::try_from(encoded.len())
        .ok()
        .and_then(|bytes| bytes.checked_mul(8))
        .and_then(|bytes| bytes.checked_add(32 * 1024))
        .and_then(|bytes| bytes.checked_add(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1))
        .ok_or(BudgetUnavailable)?;
    let work = budget.reserve(maximum)?;
    let mut scratch = ProviderDecodeScratchV1::new(encoded.len(), budget)?;
    let message = input.decode_with_scratch(&mut scratch)?;
    let record = map((**message).clone())?;
    drop(message);
    drop(scratch);
    work.settle_shared_value(record).map_err(Into::into)
}
