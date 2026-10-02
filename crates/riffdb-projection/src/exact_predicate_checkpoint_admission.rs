//! Preflight canonical rows before checkpoint decoding and sparse reconstruction.
// req: OQ-040, PRJ-004
use super::*;
use riffdb_query_ir::{ExactOrderTermV1, ExactOrderTermV2};
use riffdb_types::exact_provider_budget_v1::{
    BudgetUnavailable, ProviderAllocation, ProviderBudget,
};
use riffdb_types::{
    InspectOwnedCapacityV1, OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1,
    OwnedCapacityInspectionErrorV1, OwnedCapacityInspectionV1,
    inspect_canonical_decode_capacity_v1,
};
use std::mem::{align_of, size_of};
use std::sync::Arc;

// The existing program decoder has byte-sized order/term counts, bounded
// four-way recursion, and a 64-KiB input ceiling. This allowance covers two
// worst-case term forests, allocator overhead, partial decode and canonical
// reproduction before normal semantic validation rejects excessive counts.
const PROGRAM_DECODE_WORK: u64 = 32 * 1024 * 1024;
const _: () = assert!(size_of::<ExactOrderTermV1>() <= 64);
const _: () = assert!(size_of::<ExactOrderTermV2>() <= 64);
const _: () = assert!(size_of::<ExactPredicateNodeV1>() <= 128);
const _: () = assert!(riffdb_query_ir::MAX_EXACT_PREDICATE_PROGRAM_BYTES_V1 <= 65_536);
const _: () = assert!(riffdb_query_ir::MAX_EXACT_PREDICATE_DEPTH_V1 <= 4);
const _: () = assert!(riffdb_query_ir::MAX_EXACT_BOOLEAN_BRANCHES_V1 <= 4);

macro_rules! admitted_recovery {
    ($provider:ty, $program:ty, $format:expr, $hashes:expr, $profiles:ident) => {
        impl $provider {
            /// Admit decoding and reconstructed indexes before ordinary recovery.
            #[doc(hidden)]
            pub fn from_checkpoint_bytes_admitted_v1(
                bytes: &[u8],
                budget: &ProviderBudget,
            ) -> Result<Arc<ProviderAllocation<Self>>, ExactPredicateAdmissionErrorV1> {
                let mut reader = checked_reader(bytes, $format)
                    .map_err(ExactPredicateAdmissionErrorV1::Provider)?;
                let encoded_program = reader
                    .bytes()
                    .map_err(ExactPredicateAdmissionErrorV1::Provider)?;
                if encoded_program.len() > riffdb_query_ir::MAX_EXACT_PREDICATE_PROGRAM_BYTES_V1 {
                    return Err(ExactPredicateAdmissionErrorV1::Provider(
                        ExactPredicateProviderErrorV1::Integrity,
                    ));
                }
                // This temporary program is itself admitted before construction.
                // Its reservation drops after its data on every refusal path.
                let preview_work = budget.reserve(PROGRAM_DECODE_WORK)?;
                let program = <$program>::from_canonical_bytes(encoded_program).map_err(|_| {
                    ExactPredicateAdmissionErrorV1::Provider(
                        ExactPredicateProviderErrorV1::Integrity,
                    )
                })?;
                let scratch = budget.reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)?;
                let profiles =
                    $profiles(&program).map_err(ExactPredicateAdmissionErrorV1::Provider)?;
                reader
                    .take($hashes * 32 + 24)
                    .map_err(ExactPredicateAdmissionErrorV1::Provider)?;
                let count = usize::try_from(
                    reader
                        .u32()
                        .map_err(ExactPredicateAdmissionErrorV1::Provider)?,
                )
                .map_err(|_| BudgetUnavailable)?;
                if count
                    > usize::try_from(program.provider_requirement().max_candidates())
                        .map_err(|_| BudgetUnavailable)?
                {
                    return Err(ExactPredicateAdmissionErrorV1::Provider(
                        ExactPredicateProviderErrorV1::BoundExceeded,
                    ));
                }
                let maximum = recovery_maximum(
                    &program,
                    program.orders().len(),
                    profiles.len(),
                    count,
                    &mut reader,
                    bytes.len(),
                )
                .map_err(|_| {
                    ExactPredicateAdmissionErrorV1::Provider(
                        ExactPredicateProviderErrorV1::Integrity,
                    )
                })?;
                reader
                    .finish()
                    .map_err(ExactPredicateAdmissionErrorV1::Provider)?;
                drop(profiles);
                drop(scratch);
                drop(program);
                drop(preview_work);
                let work = budget.reserve(maximum)?;
                let provider = Self::from_checkpoint_bytes(bytes)
                    .map_err(ExactPredicateAdmissionErrorV1::Provider)?;
                work.settle_owned_value(provider).map_err(Into::into)
            }
        }
    };
}
admitted_recovery!(
    ExactPredicatePartitionIndexV4,
    ExactPredicateProgramV1,
    EXACT_PREDICATE_PROVIDER_STATE_FORMAT_V4,
    4,
    referenced_profiles
);
admitted_recovery!(
    ExactPredicatePartitionIndexV5,
    ExactPredicateProgramV2,
    EXACT_PREDICATE_PROVIDER_STATE_FORMAT_V5,
    5,
    referenced_profiles_v2
);

fn checked_reader(
    bytes: &[u8],
    format: u16,
) -> Result<CheckpointReader<'_>, ExactPredicateProviderErrorV1> {
    if bytes.len() < 8 + CHECKSUM_BYTES
        || bytes.len() > MAX_EXACT_PREDICATE_CHECKPOINT_BYTES_V4
        || &bytes[..4] != CHECKPOINT_MAGIC
        || u16::from_be_bytes([bytes[4], bytes[5]]) != format
    {
        return Err(ExactPredicateProviderErrorV1::UnsupportedFormat);
    }
    let end = bytes.len() - CHECKSUM_BYTES;
    if bytes[6..8] != [0, 0]
        || bytes[end..] != hash(HashDomain::ExactResultCheckpoint, &bytes[..end]).as_bytes()[..]
    {
        return Err(ExactPredicateProviderErrorV1::Integrity);
    }
    Ok(CheckpointReader::new(&bytes[8..end]))
}

fn recovery_maximum(
    program: &impl InspectOwnedCapacityV1,
    orders: usize,
    fields: usize,
    count: usize,
    reader: &mut CheckpointReader<'_>,
    encoded_length: usize,
) -> Result<u64, OwnedCapacityInspectionErrorV1> {
    let report = OwnedCapacityInspectionV1::inspect_with(|inspection| {
        for _ in 0..4 {
            inspection.visit(program)?;
        }
        inspection.prospective_btree::<FieldId, ExactComparisonProfileV1>(fields)?;
        inspection.allocation(
            count
                .checked_mul(4 * size_of::<ExactPredicateProviderRowV1>())
                .ok_or(OwnedCapacityInspectionErrorV1)?,
            align_of::<ExactPredicateProviderRowV1>(),
        )?;
        inspection.allocation(
            orders
                .checked_mul(4 * size_of::<ExactOrderIndexV1>())
                .ok_or(OwnedCapacityInspectionErrorV1)?,
            align_of::<ExactOrderIndexV1>(),
        )?;
        for _ in 0..orders {
            inspection.prospective_btree::<FieldId, SparseFieldIndexV1>(fields)?;
            inspection.allocation(
                count
                    .checked_mul(4 * size_of::<usize>())
                    .ok_or(OwnedCapacityInspectionErrorV1)?,
                align_of::<usize>(),
            )?;
        }
        for _ in 0..count {
            let key = reader.bytes().map_err(|_| OwnedCapacityInspectionErrorV1)?;
            inspection.allocation(
                key.len()
                    .checked_mul(4)
                    .ok_or(OwnedCapacityInspectionErrorV1)?,
                1,
            )?;
            let field_count =
                usize::from(reader.u16().map_err(|_| OwnedCapacityInspectionErrorV1)?);
            for _ in 0..4 {
                inspection.prospective_btree::<FieldId, ExactReferenceCellV1>(field_count)?;
            }
            for _ in 0..field_count {
                reader.u32().map_err(|_| OwnedCapacityInspectionErrorV1)?;
                let tag = reader.u8().map_err(|_| OwnedCapacityInspectionErrorV1)?;
                let scalar = match tag {
                    0 | 1 => None,
                    2 => Some(reader.bytes().map_err(|_| OwnedCapacityInspectionErrorV1)?),
                    _ => return Err(OwnedCapacityInspectionErrorV1),
                };
                let payload = scalar
                    .map(|bytes| bytes.get(1..).ok_or(OwnedCapacityInspectionErrorV1))
                    .transpose()?;
                if let Some(payload) = payload {
                    inspection.allocation(
                        payload
                            .len()
                            .checked_mul(4)
                            .ok_or(OwnedCapacityInspectionErrorV1)?,
                        1,
                    )?;
                }
                for _ in 0..orders {
                    inspection.allocation(12 * size_of::<usize>(), align_of::<usize>())?;
                    if let Some(scalar) = scalar {
                        inspection.prospective_btree::<ExactScalarV1, Vec<usize>>(1)?;
                        inspection.allocation(4 * size_of::<usize>(), align_of::<usize>())?;
                        let payload = scalar.get(1..).ok_or(OwnedCapacityInspectionErrorV1)?;
                        inspection.allocation(payload.len(), 1)?;
                        if scalar.first() == Some(&6) {
                            let bytes = payload.len();
                            let suffix = bytes
                                .checked_add(1)
                                .and_then(|next| bytes.checked_mul(next))
                                .map(|total| total / 2)
                                .ok_or(OwnedCapacityInspectionErrorV1)?;
                            for count in [1, 1, bytes] {
                                inspection.prospective_btree::<Vec<u8>, Vec<usize>>(count)?;
                                inspection.allocation(
                                    count
                                        .checked_mul(4 * size_of::<usize>())
                                        .ok_or(OwnedCapacityInspectionErrorV1)?,
                                    align_of::<usize>(),
                                )?;
                            }
                            inspection.allocation(
                                bytes
                                    .checked_mul(4)
                                    .and_then(|base| {
                                        suffix
                                            .checked_mul(2)
                                            .and_then(|suffix| base.checked_add(suffix))
                                    })
                                    .ok_or(OwnedCapacityInspectionErrorV1)?,
                                1,
                            )?;
                        }
                    }
                }
            }
            let output = reader.bytes().map_err(|_| OwnedCapacityInspectionErrorV1)?;
            let heap = inspect_canonical_decode_capacity_v1(output)?.owned_heap_bytes();
            let heap = usize::try_from(heap).map_err(|_| OwnedCapacityInspectionErrorV1)?;
            inspection.allocation(
                heap.checked_mul(4).ok_or(OwnedCapacityInspectionErrorV1)?,
                1,
            )?;
        }
        // Canonical checkpoint reproduction coexists with decoded state.
        inspection.allocation(
            encoded_length
                .checked_mul(4)
                .ok_or(OwnedCapacityInspectionErrorV1)?,
            1,
        )?;
        Ok(())
    })?;
    if !report.shared_allocations().is_empty() {
        return Err(OwnedCapacityInspectionErrorV1);
    }
    report
        .unshared_bytes()
        .checked_add(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .and_then(|bytes| bytes.checked_add(PROGRAM_DECODE_WORK))
        .and_then(|bytes| bytes.checked_add(32768))
        .ok_or(OwnedCapacityInspectionErrorV1)
}
