//! Borrowed admission for the existing MCP registry section and constructors.
// req: OQ-040, PRJ-004
use super::*;
use riffdb_types::{
    OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1,
    exact_provider_budget_v1::{BudgetUnavailable, ProviderAllocationOwner, ProviderBudget},
};
use workflow_admission::allocation;

fn text<'a>(reader: &mut Reader<'a>, maximum: usize) -> Result<&'a str, IrValidationError> {
    reader.string_ref(maximum)
}
fn plus(left: u64, right: u64) -> Result<u64, BudgetUnavailable> {
    left.checked_add(right).ok_or(BudgetUnavailable)
}

// ContractLineage owns the closed text representation, including its sized
// shared shell, boxed bytes and lifecycle control. Quote that actual owning
// type; the source String may coexist during conversion and is separate scratch.
fn lineage_bound(length: usize) -> Result<u64, BudgetUnavailable> {
    let retained = riffdb_types::OwnedCapacityTextV1::retained_allocation_upper_bound(length)
        .map_err(|_| BudgetUnavailable)?;
    plus(retained, allocation::<u8>(length)?)
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
        let (heap, shared, count, largest_name) = Self::walk_from(&mut reader)?;
        reader.finish()?;
        Self::finish_bound(heap, shared, count, largest_name, bytes.len())
    }

    // Prefix admission consumes only this registry's existing grammar. The
    // Source owner uses a fork of its actual Reader before admitting the sole
    // existing decoder; no decoded graph or Source authority is created here.
    pub(super) fn inspect_from(
        reader: &mut Reader<'_>,
    ) -> Result<Self, ProviderBundleEncodingErrorV1> {
        let start = reader.position();
        let (heap, shared, count, largest_name) = Self::walk_from(reader)?;
        let encoded_length = reader
            .position()
            .checked_sub(start)
            .ok_or(BudgetUnavailable)?;
        Self::finish_bound(heap, shared, count, largest_name, encoded_length)
    }

    fn finish_bound(
        heap: u64,
        shared: u64,
        count: usize,
        largest_name: u64,
        encoded_length: usize,
    ) -> Result<Self, ProviderBundleEncodingErrorV1> {
        Ok(Self {
            heap,
            shared,
            // The collision set contains borrowed names. It lives across name
            // validation; string normalization is bounded by the largest entry.
            constructors: plus(
                plan_hash_admission::tree_bound::<&str>(count)?,
                largest_name,
            )?,
            canonical: allocation::<u8>(encoded_length)?,
        })
    }

    // One allocation-free Reader walk and one numeric finish serve both
    // Provider fragments and the Source prefix. String framing and UTF-8 stay
    // owned by Reader; materialization stays in decode_mcp_registry.
    fn walk_from(
        reader: &mut Reader<'_>,
    ) -> Result<(u64, u64, usize, u64), ProviderBundleEncodingErrorV1> {
        require_version(
            reader.u32()?,
            crate::MCP_COMMAND_NAME_REGISTRY_VERSION_V2,
            "MCP command-name registry",
        )?;
        let lineage = text(reader, 256)?;
        let contract = text(reader, 256)?;
        let count = decode_len_with_minimum(reader, "MCP command-name entries", 4_096, 8)?;
        let mut heap = plus(
            allocation::<McpCommandNameEntryV2>(count)?,
            allocation::<u8>(contract.len())?,
        )?;
        let shared = lineage_bound(lineage.len())?;
        let mut largest_name = 0;
        for _ in 0..count {
            reader.u32()?;
            let command = text(reader, 256)?;
            let tool = text(reader, crate::MAX_MCP_COMMAND_TOOL_NAME_BYTES)?;
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
        Ok((heap, shared, count, largest_name))
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
) -> Result<ProviderAllocationOwner<McpCommandNameRegistryV2>, ProviderBundleEncodingErrorV1> {
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
