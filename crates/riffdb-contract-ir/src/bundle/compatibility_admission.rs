//! Admission for the existing compatibility section, not bundle comparison.
// req: OQ-040, PRJ-004
use super::*;
use riffdb_types::{
    OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1,
    exact_provider_budget_v1::{BudgetUnavailable, ProviderAllocation, ProviderBudget},
};
use std::sync::Arc;
use workflow_admission::allocation;

pub(super) struct CompatibilitySectionBound {
    pub(super) heap: u64,
    pub(super) constructors: u64,
    pub(super) canonical: u64,
}
impl CompatibilitySectionBound {
    pub(super) fn inspect(
        bytes: &[u8],
        has_parent: bool,
    ) -> Result<Self, ProviderBundleEncodingErrorV1> {
        let mut reader = Reader::new(bytes);
        decode_compatibility_class(reader.u8()?)?;
        let count = decode_len_with_minimum(&mut reader, "compatibility entries", 4_096, 13)?;
        // A successor with no encoded entries synthesizes one finding using a
        // push into an empty Vec (capacity four). Canonical checking rejects the
        // noncanonical input afterwards, but that allocation must be admitted.
        let capacity = if count == 0 && has_parent { 4 } else { count };
        let mut heap = allocation::<CompatibilityEntry>(capacity)?;
        let mut constructors = allocation::<u8>(8)?; // temporary code string
        for _ in 0..count {
            let code = reader.bytes(8)?;
            std::str::from_utf8(code).map_err(|_| IrValidationError::InvalidText { kind: "IR" })?;
            let path = reader.bytes(1_024)?;
            std::str::from_utf8(path).map_err(|_| IrValidationError::InvalidText { kind: "IR" })?;
            heap = heap
                .checked_add(allocation::<u8>(path.len())?)
                .and_then(|value| {
                    value.checked_add(crate::compatibility::provider_stable_path_vector_bound_v1())
                })
                .ok_or(BudgetUnavailable)?;
            // The existing parser collects borrowed slash-separated parts.
            // Vec growth retains at most max(4, 2 * parts), even for an invalid
            // path containing only slashes. The code buffer has died by then.
            let parts = path.iter().filter(|byte| **byte == b'/').count() + 1;
            let capacity = parts.checked_mul(2).ok_or(BudgetUnavailable)?.max(4);
            constructors = constructors.max(allocation::<&str>(capacity)?);
        }
        if count == 0 && has_parent {
            heap = heap
                .checked_add(allocation::<u8>("contract".len())?)
                .and_then(|value| {
                    value.checked_add(crate::compatibility::provider_stable_path_vector_bound_v1())
                })
                .ok_or(BudgetUnavailable)?;
        }
        reader.finish()?;
        Ok(Self {
            heap,
            constructors,
            // Only the empty successor can grow during ordinary decoding:
            // one code (at most 8 bytes), path "contract" and two u32 lengths
            // add at most 24 bytes. Otherwise output length is unchanged.
            canonical: allocation::<u8>(bytes.len().checked_add(32).ok_or(BudgetUnavailable)?)?,
        })
    }
    pub(super) fn maximum(&self) -> Result<u64, BudgetUnavailable> {
        self.heap
            .checked_add(self.constructors.max(self.canonical))
            .and_then(|bytes| bytes.checked_add(32 * 1024))
            .and_then(|bytes| bytes.checked_add(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1))
            .ok_or(BudgetUnavailable)
    }
}

/// Read the existing compatibility section under provider admission. Complete
/// bundle identity, encoded input ownership and parent comparison are separate.
#[doc(hidden)]
pub fn decode_provider_compatibility_section_admitted_v1(
    bytes: &[u8],
    has_parent: bool,
    budget: &ProviderBudget,
) -> Result<Arc<ProviderAllocation<CompatibilityReport>>, ProviderBundleEncodingErrorV1> {
    checked_len("canonical contract bundle", bytes.len(), MAX_BUNDLE_BYTES)?;
    let sizing = budget.reserve(32 * 1024)?;
    let bound = CompatibilitySectionBound::inspect(bytes, has_parent)?;
    let work = budget.reserve(bound.maximum()?)?;
    drop(sizing);
    let mut reader = Reader::new(bytes);
    let report = decode_compatibility(&mut reader, has_parent)?;
    reader.finish()?;
    {
        let maximum = bytes.len().checked_add(32).ok_or(BudgetUnavailable)?;
        let mut counter = Writer::counting(maximum);
        encode_compatibility(&mut counter, &report)?;
        let expected = counter.encoded_len();
        let mut writer = Writer::with_capacity(expected, expected);
        encode_compatibility(&mut writer, &report)?;
        if writer.finish() != bytes {
            return Err(IrValidationError::NonCanonicalOrder {
                kind: "canonical contract bundle bytes",
            }
            .into());
        }
    }
    work.settle_owned_value(report).map_err(Into::into)
}
