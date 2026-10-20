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
    BudgetUnavailable, ProviderAllocationOwner, ProviderBudget,
};
use riffdb_types::{InspectOwnedCapacityV1, OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1};

/// Read-only first-party capture; opaque IR needs its own construction admission.
#[doc(hidden)]
pub fn decode_provider_catalog_bundle_admitted_v1(
    encoded: &[u8],
    budget: &ProviderBudget,
) -> Result<ProviderAllocationOwner<StoredContractBundleV1>, ProviderRecordDecodeErrorV1> {
    decode::<wire::StoredContractBundleV1, _>(encoded, budget, bundle_from_proto)
}
/// Read-only first-party capture of the ordinary exact active identity.
#[doc(hidden)]
pub fn decode_provider_active_catalog_pointer_admitted_v1(
    encoded: &[u8],
    budget: &ProviderBudget,
) -> Result<ProviderAllocationOwner<ActiveCatalogPointerV1>, ProviderRecordDecodeErrorV1> {
    decode::<wire::ActiveCatalogPointerV1, _>(encoded, budget, active_from_proto)
}
/// Permanent migration evidence reconstructed with ordinary semantic checks.
#[doc(hidden)]
pub fn decode_provider_contract_migration_record_admitted_v1(
    encoded: &[u8],
    budget: &ProviderBudget,
) -> Result<ProviderAllocationOwner<StoredContractMigrationRecordV1>, ProviderRecordDecodeErrorV1> {
    decode::<wire::StoredContractMigrationRecordV1, _>(encoded, budget, migration_record_from_proto)
}
/// Exact predecessor write fence reconstructed under the same closed admission.
#[doc(hidden)]
pub fn decode_provider_contract_write_retirement_admitted_v1(
    encoded: &[u8],
    budget: &ProviderBudget,
) -> Result<ProviderAllocationOwner<StoredContractWriteRetirementV1>, ProviderRecordDecodeErrorV1> {
    decode::<wire::StoredContractWriteRetirementV1, _>(encoded, budget, write_retirement_from_proto)
}
fn decode<M: ProviderSourceMessageV1 + Clone, T: InspectOwnedCapacityV1>(
    encoded: &[u8],
    budget: &ProviderBudget,
    map: impl FnOnce(M) -> Result<T, DurableCodecError>,
) -> Result<ProviderAllocationOwner<T>, ProviderRecordDecodeErrorV1> {
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
    let record = map((*message).clone())?;
    drop(message);
    drop(scratch);
    work.settle_shared_value(record).map_err(Into::into)
}

/// Decode only the opaque catalog envelope under the originating Source ledger.
/// Canonical IR interpretation and active-plan validation remain Projection's
/// separately admitted responsibility. No provider ledger/cache is constructed.
#[doc(hidden)]
pub fn decode_source_catalog_bundle_admitted_v1(
    encoded: &[u8],
    budget: &riffdb_types::ProjectionSourceWorkingBudgetV1,
) -> Result<
    riffdb_types::ProjectionSourceWorkAllocationV1<StoredContractBundleV1>,
    super::ProjectionSourceRecordDecodeErrorV1,
> {
    use super::ProjectionSourceRecordDecodeErrorV1;
    use riffdb_proto::envelope::{
        CallerScratchDecodeErrorV1, inspect_source_catalog_bundle_transport_v1,
    };
    use riffdb_types::OwnedCapacityTextV1;
    let input = inspect_source_catalog_bundle_transport_v1(encoded)
        .map_err(DurableCodecError::from_decode_envelope)?;
    let scratch_bytes = riffdb_types::owned_allocation_capacity_bound_v1(encoded.len(), 1)
        .map_err(|_| BudgetUnavailable)?;
    let lineage = OwnedCapacityTextV1::retained_allocation_upper_bound(input.lineage_byte_bound())
        .map_err(|_| BudgetUnavailable)?;
    let root =
        ProviderAllocationOwner::<StoredContractBundleV1>::settlement_root_capacity_upper_bound()?;
    // A transient dependency error dies before the settlement inspector is
    // constructed; their complete allowances share one sequential workspace.
    let census_or_error = OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1
        .max(input.transient_decode_error_work_bound_v1()?);
    let maximum = input
        .transport_work_bound_v1()?
        .checked_add(scratch_bytes)
        .and_then(|n| n.checked_add(lineage))
        .and_then(|n| n.checked_add(root))
        .and_then(|n| n.checked_add(census_or_error))
        .ok_or(BudgetUnavailable)?;
    // All new scratch, generated buffers, mapper lineage, census and final
    // owner are paid before any Vec/Prost/materialized semantic allocation.
    let work = budget.reserve(maximum)?;
    let mut scratch = Vec::new();
    scratch
        .try_reserve_exact(encoded.len())
        .map_err(|_| BudgetUnavailable)?;
    let message = input
        .decode_with_caller_scratch(&mut scratch)
        .map_err(|error| match error {
            CallerScratchDecodeErrorV1::Envelope(error) => {
                ProjectionSourceRecordDecodeErrorV1::Codec(DurableCodecError::from_decode_envelope(
                    error,
                ))
            }
            CallerScratchDecodeErrorV1::CapacityUnavailable => {
                ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable
            }
        })?;
    // Consume the actual generated fields through the sole existing mapper;
    // canonical Vec ownership moves once, with no cloned intermediate DTO.
    let record = bundle_from_proto(message)?;
    drop(scratch);
    work.settle_shared_value(record).map_err(Into::into)
}
