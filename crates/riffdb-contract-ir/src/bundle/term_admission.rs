//! Borrowed parsing bounds for common key and expression fragments.
// req: OQ-040, PRJ-004
use super::*;
use riffdb_types::{
    OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1,
    exact_provider_budget_v1::{BudgetUnavailable, ProviderAllocation, ProviderBudget},
};
use std::sync::Arc;
use workflow_admission::allocation;

fn add(total: &mut u64, bytes: u64) -> Result<(), BudgetUnavailable> {
    *total = total.checked_add(bytes).ok_or(BudgetUnavailable)?;
    Ok(())
}

pub(super) struct TermBound {
    pub(super) heap: u64,
    pub(super) constructors: u64,
}
impl TermBound {
    pub(super) fn maximum(&self, encoded_len: usize) -> Result<u64, BudgetUnavailable> {
        self.heap
            .checked_add(self.constructors.max(allocation::<u8>(encoded_len)?))
            .and_then(|value| value.checked_add(32 * 1024))
            .and_then(|value| value.checked_add(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1))
            .ok_or(BudgetUnavailable)
    }
}

/// Walk one key within an enclosing borrowed reader. Constructors only move
/// components, sort variants in place, and box the optional nested key.
pub(super) fn key_bound(
    reader: &mut Reader<'_>,
    depth: usize,
) -> Result<TermBound, ProviderBundleEncodingErrorV1> {
    if depth > 1 {
        return Err(IrValidationError::InvalidKey {
            reason: "key schema nesting exceeds the index/entity shape",
        }
        .into());
    }
    let version = reader.u32()?;
    if !matches!(
        version,
        crate::KEY_CODEC_VERSION_V1 | crate::KEY_CODEC_VERSION_V2
    ) {
        return Err(IrValidationError::UnsupportedVersion {
            kind: "key codec",
            value: version,
        }
        .into());
    }
    match reader.u8()? {
        key_purpose_tag::ENTITY | key_purpose_tag::PARTITION | key_purpose_tag::CONFLICT => {
            reader.u32()?;
        }
        key_purpose_tag::INDEX => {
            reader.read(8)?;
        }
        tag => {
            return Err(IrValidationError::UnknownTag {
                kind: "key purpose",
                tag,
            }
            .into());
        }
    }
    let count = decode_len(reader, "key components", 1_024)?;
    let mut heap = allocation::<KeyComponentSchema>(count)?;
    for _ in 0..count {
        add(&mut heap, row_policy_admission::type_heap(reader, 0)?)?;
        if version >= crate::KEY_CODEC_VERSION_V2 {
            match reader.u8()? {
                0 | 1 => {}
                tag => {
                    return Err(IrValidationError::UnknownTag {
                        kind: "key component codec",
                        tag,
                    }
                    .into());
                }
            }
        }
        let variants = decode_len(
            reader,
            "key enum variants",
            crate::MAX_DECLARATIONS_PER_KIND,
        )?;
        add(&mut heap, allocation::<EnumVariantId>(variants)?)?;
        reader.read(variants.checked_mul(4).ok_or(BudgetUnavailable)?)?;
        reader.u32()?;
    }
    reader.u32()?;
    if reader.bool()? {
        let child = key_bound(reader, depth + 1)?;
        add(&mut heap, child.heap)?;
        add(&mut heap, allocation::<KeySchema>(1)?)?;
    }
    Ok(TermBound {
        heap,
        constructors: 0,
    })
}

/// Walk one arena without building values. The input tuple vector survives while
/// the checked typed-node vector and depth vector are constructed. Private type
/// boxes and constant buffers move from input to output without cloning.
pub(super) fn expression_bound(
    reader: &mut Reader<'_>,
    version: u32,
) -> Result<TermBound, ProviderBundleEncodingErrorV1> {
    let count =
        decode_len_with_minimum(reader, "expression arena", crate::MAX_EXPRESSION_NODES, 2)?;
    let mut heap = allocation::<crate::TypedExpression>(count)?;
    for _ in 0..count {
        let tag = reader.u8()?;
        add(&mut heap, row_policy_admission::type_heap(reader, 0)?)?;
        match tag {
            expression_tag::CONSTANT => {
                let bytes = reader.bytes(1024 * 1024)?;
                preflight_ir_canonical_value(bytes)?;
                let bound =
                    riffdb_types::inspect_canonical_decode_capacity_v1(bytes).map_err(|_| {
                        IrValidationError::TypeMismatch {
                            context: "expression constant",
                        }
                    })?;
                add(&mut heap, bound.owned_heap_bytes())?;
            }
            expression_tag::INPUT_FIELD
            | expression_tag::COMPLETE_BINDING
            | expression_tag::SOURCE_EVENT_FIELD => {
                reader.u32()?;
            }
            expression_tag::SERVICE_VALUE if version >= EXECUTABLE_IR_VERSION_V2 => {
                reader.u32()?;
            }
            expression_tag::COLLECTION_ELEMENT_FIELD if version >= EXECUTABLE_IR_VERSION_V5 => {
                reader.u32()?;
            }
            expression_tag::COLLECTION_ELEMENT if version >= EXECUTABLE_IR_VERSION_V5 => {}
            expression_tag::BOUND_FIELD
            | expression_tag::SCHEMA_FIELD
            | expression_tag::ROOT_VALIDATION_FIELD => {
                reader.read(8)?;
            }
            expression_tag::TRANSACTION_TIME | expression_tag::TRANSACTION_DATE => {}
            expression_tag::UNARY => {
                decode_unary_operator(reader.u8()?)?;
                reader.u32()?;
            }
            expression_tag::BINARY => {
                decode_binary_operator(reader.u8()?)?;
                reader.read(8)?;
            }
            tag => {
                return Err(IrValidationError::UnknownTag {
                    kind: "expression",
                    tag,
                }
                .into());
            }
        }
    }
    let constructors = allocation::<(ExpressionKind, ValueType)>(count)?
        .checked_add(allocation::<usize>(count)?)
        .ok_or(BudgetUnavailable)?;
    Ok(TermBound { heap, constructors })
}

/// First-party key fragment admission. Input ownership and complete catalog
/// identity remain the caller's responsibility; ordinary validation is retained.
#[doc(hidden)]
pub fn decode_provider_key_schema_admitted_v1(
    bytes: &[u8],
    budget: &ProviderBudget,
) -> Result<Arc<ProviderAllocation<KeySchema>>, ProviderBundleEncodingErrorV1> {
    checked_len("canonical contract bundle", bytes.len(), MAX_BUNDLE_BYTES)?;
    let sizing = budget.reserve(32 * 1024)?;
    let mut reader = Reader::new(bytes);
    let bound = key_bound(&mut reader, 0)?;
    reader.finish()?;
    let work = budget.reserve(bound.maximum(bytes.len())?)?;
    drop(sizing);
    let mut reader = Reader::new(bytes);
    let value = decode_key_schema(&mut reader, 0)?;
    reader.finish()?;
    {
        let mut writer = Writer::with_capacity(bytes.len(), bytes.len());
        encode_key_schema(&mut writer, &value)?;
        if writer.finish() != bytes {
            return Err(IrValidationError::NonCanonicalOrder {
                kind: "canonical contract bundle bytes",
            }
            .into());
        }
    }
    work.settle_owned_value(value).map_err(Into::into)
}

/// First-party expression fragment admission for existing IR versions. Input
/// ownership, field binding and whole-plan validation remain separate.
#[doc(hidden)]
pub fn decode_provider_expression_arena_admitted_v1(
    bytes: &[u8],
    version: u32,
    budget: &ProviderBudget,
) -> Result<Arc<ProviderAllocation<ExpressionArena>>, ProviderBundleEncodingErrorV1> {
    checked_len("canonical contract bundle", bytes.len(), MAX_BUNDLE_BYTES)?;
    if !(EXECUTABLE_IR_VERSION_V1..=EXECUTABLE_IR_VERSION_V26).contains(&version) {
        return Err(IrValidationError::UnsupportedVersion {
            kind: "expression executable IR",
            value: version,
        }
        .into());
    }
    let sizing = budget.reserve(32 * 1024)?;
    let mut reader = Reader::new(bytes);
    let bound = expression_bound(&mut reader, version)?;
    reader.finish()?;
    let work = budget.reserve(bound.maximum(bytes.len())?)?;
    drop(sizing);
    let mut reader = Reader::new(bytes);
    let value = decode_expression_arena_versioned(&mut reader, version)?;
    reader.finish()?;
    {
        let mut writer = Writer::with_capacity(bytes.len(), bytes.len());
        encode_expression_arena(&mut writer, &value)?;
        if writer.finish() != bytes {
            return Err(IrValidationError::NonCanonicalOrder {
                kind: "canonical contract bundle bytes",
            }
            .into());
        }
    }
    work.settle_owned_value(value).map_err(Into::into)
}
