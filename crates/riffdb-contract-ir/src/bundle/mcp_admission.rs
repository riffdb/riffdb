//! Borrowed admission for the existing MCP registry section and constructors.
// req: OQ-040, PRJ-004
use super::*;
use riffdb_types::{
    OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1,
    exact_provider_budget_v1::{BudgetUnavailable, ProviderAllocation, ProviderBudget},
};
use std::{
    mem::{align_of, size_of},
    sync::Arc,
};
use workflow_admission::allocation;

fn text<'a>(reader: &mut Reader<'a>, maximum: usize) -> Result<&'a str, IrValidationError> {
    std::str::from_utf8(reader.bytes(maximum)?)
        .map_err(|_| IrValidationError::InvalidText { kind: "IR" })
}
fn plus(left: u64, right: u64) -> Result<u64, BudgetUnavailable> {
    left.checked_add(right).ok_or(BudgetUnavailable)
}

// ContractLineage's shared string includes the Arc header and the existing
// inspector's 1,024-byte lifecycle allowance. Its source String can overlap
// Arc conversion. Fixed owner work below also covers attachment bookkeeping.
fn lineage_bound(length: usize) -> Result<u64, BudgetUnavailable> {
    let bytes = length
        .checked_add(2 * size_of::<usize>())
        .ok_or(BudgetUnavailable)?;
    let shared = bytes
        .checked_mul(2)
        .and_then(|value| value.checked_add(2 * align_of::<usize>() + 256 + 1024))
        .and_then(|value| u64::try_from(value).ok())
        .ok_or(BudgetUnavailable)?;
    plus(shared, allocation::<u8>(length)?)
}

pub(super) fn name_workspace(
    contract: usize,
    command: usize,
    tool: usize,
) -> Result<u64, BudgetUnavailable> {
    // Registry validation clones one checked tool name while normalization
    // retains two lowercase segments and a formatted complete name. Admission
    // includes format's capacity growth; the doubled final capacity also covers
    // its previous allocation. Entry construction needs no additional clone.
    let output = contract
        .checked_add(command)
        .and_then(|value| value.checked_add("riffdb_cmd_".len() + 1))
        .and_then(|value| value.checked_mul(2))
        .ok_or(BudgetUnavailable)?;
    let segments = plus(allocation::<u8>(contract)?, allocation::<u8>(command)?)?;
    plus(
        plus(segments, allocation::<u8>(output)?)?,
        allocation::<u8>(tool)?,
    )
}

pub(super) struct McpSectionBound {
    pub(super) heap: u64,
    pub(super) shared: u64,
    pub(super) constructors: u64,
    pub(super) canonical: u64,
}
impl McpSectionBound {
    pub(super) fn inspect(bytes: &[u8]) -> Result<Self, ProviderBundleEncodingErrorV1> {
        let mut reader = Reader::new(bytes);
        require_version(
            reader.u32()?,
            crate::MCP_COMMAND_NAME_REGISTRY_VERSION_V2,
            "MCP command-name registry",
        )?;
        let lineage = text(&mut reader, 256)?;
        let contract = text(&mut reader, 256)?;
        let count = decode_len_with_minimum(&mut reader, "MCP command-name entries", 4_096, 8)?;
        let mut heap = plus(
            allocation::<McpCommandNameEntryV2>(count)?,
            allocation::<u8>(contract.len())?,
        )?;
        let shared = lineage_bound(lineage.len())?;
        let mut largest_name = 0;
        for _ in 0..count {
            reader.u32()?;
            let command = text(&mut reader, 256)?;
            let tool = text(&mut reader, crate::MAX_MCP_COMMAND_TOOL_NAME_BYTES)?;
            heap = plus(
                heap,
                plus(
                    allocation::<u8>(command.len())?,
                    allocation::<u8>(tool.len())?,
                )?,
            )?;
            largest_name =
                largest_name.max(name_workspace(contract.len(), command.len(), tool.len())?);
        }
        reader.finish()?;
        Ok(Self {
            heap,
            shared,
            // The collision set contains borrowed names. It lives across name
            // validation; string normalization is bounded by the largest entry.
            constructors: plus(
                plan_hash_admission::tree_bound::<&str>(count)?,
                largest_name,
            )?,
            canonical: allocation::<u8>(bytes.len())?,
        })
    }
    pub(super) fn maximum(&self) -> Result<u64, BudgetUnavailable> {
        plus(
            plus(
                plus(self.heap, self.shared)?,
                self.constructors.max(self.canonical),
            )?,
            32 * 1024 + OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1,
        )
    }
}

/// Decode existing compiler-owned names under first-party provider admission.
/// Complete bundle identity, input ownership, and command completeness remain
/// separate. This does not change MCP visibility or application interfaces.
#[doc(hidden)]
pub fn decode_provider_mcp_registry_section_admitted_v1(
    bytes: &[u8],
    budget: &ProviderBudget,
) -> Result<Arc<ProviderAllocation<McpCommandNameRegistryV2>>, ProviderBundleEncodingErrorV1> {
    checked_len("canonical contract bundle", bytes.len(), MAX_BUNDLE_BYTES)?;
    let sizing = budget.reserve(32 * 1024)?;
    let bound = McpSectionBound::inspect(bytes)?;
    let work = budget.reserve(bound.maximum()?)?;
    drop(sizing);
    let mut reader = Reader::new(bytes);
    let registry = decode_mcp_registry(&mut reader)?;
    reader.finish()?;
    {
        let mut writer = Writer::with_capacity(bytes.len(), bytes.len());
        encode_mcp_registry(&mut writer, &registry)?;
        if writer.finish() != bytes {
            return Err(IrValidationError::NonCanonicalOrder {
                kind: "canonical contract bundle bytes",
            }
            .into());
        }
    }
    work.settle_shared_value(registry).map_err(Into::into)
}
