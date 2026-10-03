//! Borrowed native-capacity bounds for the existing V1/V2 ledger section.
// req: OQ-040, PRJ-004
use super::*;
use riffdb_types::{
    OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1,
    exact_provider_budget_v1::{BudgetUnavailable, ProviderAllocation, ProviderBudget},
};
use std::sync::Arc;
use workflow_admission::allocation;

fn add(total: &mut u64, value: u64) -> Result<(), BudgetUnavailable> {
    *total = total.checked_add(value).ok_or(BudgetUnavailable)?;
    Ok(())
}

fn owners(reader: &mut Reader<'_>, heap: &mut u64) -> Result<usize, ProviderBundleEncodingErrorV1> {
    reader.u8()?; // owner kind, checked by the ordinary namespace constructor
    let count = reader.u8()? as usize;
    ensure_fixed_width_items(reader, count, 4, "lineage owner path")?;
    add(heap, allocation::<u32>(count)?)?;
    reader.read(count.checked_mul(4).ok_or(BudgetUnavailable)?)?;
    Ok(count)
}

fn name(reader: &mut Reader<'_>, heap: &mut u64) -> Result<(), ProviderBundleEncodingErrorV1> {
    let value = reader.bytes(256)?;
    std::str::from_utf8(value).map_err(|_| IrValidationError::InvalidText { kind: "IR" })?;
    add(heap, allocation::<u8>(value.len())?)?;
    Ok(())
}

pub(super) struct LedgerSectionBound {
    pub(super) heap: u64,
    pub(super) constructors: u64,
    pub(super) canonical: u64,
}
impl LedgerSectionBound {
    pub(super) fn inspect(bytes: &[u8]) -> Result<Self, ProviderBundleEncodingErrorV1> {
        let mut reader = Reader::new(bytes);
        let version = reader.u32()?;
        if !matches!(
            version,
            LINEAGE_LEDGER_VERSION_V1 | LINEAGE_LEDGER_VERSION_V2
        ) {
            return Err(IrValidationError::UnsupportedVersion {
                kind: "lineage ledger",
                value: version,
            }
            .into());
        }
        let count = decode_len_with_minimum(
            &mut reader,
            "lineage allocations",
            MAX_LINEAGE_ALLOCATION_STATES,
            11,
        )?;
        let mut heap = allocation::<LineageAllocation>(count)?;
        let mut constructors = 0;
        let mut total_entries = 0usize;
        for _ in 0..count {
            decode_namespace_tag(reader.u8()?)?;
            owners(&mut reader, &mut heap)?;
            reader.u32()?; // max allocated, checked against complete history
            let entries = decode_len_with_minimum(
                &mut reader,
                "lineage entries",
                MAX_LINEAGE_LEDGER_ENTRIES,
                11,
            )?;
            total_entries =
                total_entries
                    .checked_add(entries)
                    .ok_or(IrValidationError::SizeOverflow {
                        kind: "lineage entries",
                    })?;
            checked_len("lineage entries", total_entries, MAX_LINEAGE_LEDGER_ENTRIES)?;
            add(&mut heap, allocation::<LineageEntry>(entries)?)?;
            for _ in 0..entries {
                reader.u32()?; // ID, ordinary decoder checks contiguous history
                let count = owners(&mut reader, &mut heap)?;
                name(&mut reader, &mut heap)?;
                decode_lineage_entry_state(reader.u8()?)?;
                // Comparing the identity's allocation namespace copies one
                // owner vector. It is destroyed before the next entry, and
                // before canonical output is allocated. No tree is built here.
                constructors = constructors.max(allocation::<u32>(count)?);
            }
        }
        if version == LINEAGE_LEDGER_VERSION_V2 {
            let count = decode_len_with_minimum(
                &mut reader,
                "lineage aliases",
                MAX_LINEAGE_LEDGER_ENTRIES,
                13,
            )?;
            add(&mut heap, allocation::<LineageAlias>(count)?)?;
            for _ in 0..count {
                decode_namespace_tag(reader.u8()?)?;
                owners(&mut reader, &mut heap)?;
                name(&mut reader, &mut heap)?;
                reader.u32()?; // alias target, complete ledger validation is separate
            }
        }
        reader.finish()?;
        Ok(Self {
            heap,
            constructors,
            canonical: allocation::<u8>(bytes.len())?,
        })
    }
    pub(super) fn maximum(&self) -> Result<u64, BudgetUnavailable> {
        self.heap
            .checked_add(self.constructors.max(self.canonical))
            .and_then(|value| value.checked_add(32 * 1024))
            .and_then(|value| value.checked_add(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1))
            .ok_or(BudgetUnavailable)
    }
}

/// Parse the existing ledger section under provider admission. This preserves
/// ordinary section checks; complete ledger/schema/plan validation, parent
/// lineage history, bundle identity and encoded input ownership remain separate.
#[doc(hidden)]
pub fn decode_provider_ledger_section_admitted_v1(
    bytes: &[u8],
    budget: &ProviderBudget,
) -> Result<Arc<ProviderAllocation<LineageLedgerV1>>, ProviderBundleEncodingErrorV1> {
    checked_len("canonical contract bundle", bytes.len(), MAX_BUNDLE_BYTES)?;
    let sizing = budget.reserve(32 * 1024)?;
    let bound = LedgerSectionBound::inspect(bytes)?;
    let work = budget.reserve(bound.maximum()?)?;
    drop(sizing);
    let mut reader = Reader::new(bytes);
    let ledger = decode_ledger(&mut reader)?;
    reader.finish()?;
    {
        let mut writer = Writer::with_capacity(bytes.len(), bytes.len());
        encode_ledger(&mut writer, &ledger)?;
        if writer.finish() != bytes {
            return Err(IrValidationError::NonCanonicalOrder {
                kind: "canonical contract bundle bytes",
            }
            .into());
        }
    }
    work.settle_owned_value(ledger).map_err(Into::into)
}
