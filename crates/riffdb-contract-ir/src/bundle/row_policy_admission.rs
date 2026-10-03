//! Admission for the existing policy parser and both constructor fact maps.
// req: OQ-040, PRJ-004
use super::*;
use crate::row_policy::*;
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
fn name(reader: &mut Reader<'_>) -> Result<u64, ProviderBundleEncodingErrorV1> {
    let bytes = reader.bytes(256)?;
    std::str::from_utf8(bytes).map_err(|_| IrValidationError::InvalidText { kind: "IR" })?;
    Ok(allocation::<u8>(bytes.len())?)
}
pub(super) fn type_heap(
    reader: &mut Reader<'_>,
    depth: usize,
) -> Result<u64, ProviderBundleEncodingErrorV1> {
    if depth >= 32 {
        return Err(IrValidationError::LimitExceeded {
            kind: "type nesting",
            actual: depth + 1,
            maximum: 32,
        }
        .into());
    }
    match reader.u8()? {
        value_type_tag::BOOL
        | value_type_tag::I64
        | value_type_tag::U64
        | value_type_tag::TIMESTAMP
        | value_type_tag::DATE
        | value_type_tag::UUID => Ok(0),
        value_type_tag::DECIMAL => {
            reader.read(2)?;
            Ok(0)
        }
        value_type_tag::MONEY => {
            reader.read(3)?;
            Ok(0)
        }
        value_type_tag::STRING
        | value_type_tag::BYTES
        | value_type_tag::ENUM
        | value_type_tag::VECTOR => {
            reader.u32()?;
            Ok(0)
        }
        tag @ (value_type_tag::OPTIONAL | value_type_tag::LIST) => {
            let child = type_heap(reader, depth + 1)?;
            if tag == value_type_tag::LIST {
                reader.u32()?;
            }
            Ok(child
                .checked_add(allocation::<ValueType>(1)?)
                .ok_or(BudgetUnavailable)?)
        }
        value_type_tag::RECORD => {
            decode_record_ref(reader)?;
            Ok(0)
        }
        tag => Err(IrValidationError::UnknownTag {
            kind: "value type",
            tag,
        }
        .into()),
    }
}

fn count(
    reader: &mut Reader<'_>,
    kind: &'static str,
    maximum: usize,
) -> Result<usize, IrValidationError> {
    let count = reader.u32()? as usize;
    checked_len(kind, count, maximum)?;
    Ok(count)
}
fn operand(reader: &mut Reader<'_>, heap: &mut u64) -> Result<u64, ProviderBundleEncodingErrorV1> {
    match reader.u8()? {
        1 => {
            reader.u32()?;
        }
        2 | 3 => {}
        4 => add(heap, name(reader)?)?,
        5 => {
            let bytes = reader.bytes(MAX_PRINCIPAL_FACT_BYTES_V1)?;
            let bound =
                riffdb_types::inspect_canonical_decode_capacity_v1(bytes).map_err(|_| {
                    IrValidationError::InvalidText {
                        kind: "row policy constant",
                    }
                })?;
            add(heap, bound.owned_heap_bytes())?;
        }
        tag => {
            return Err(IrValidationError::UnknownTag {
                kind: "row policy operand",
                tag,
            }
            .into());
        }
    }
    let types = type_heap(reader, 0)?;
    add(heap, types)?;
    Ok(types)
}

pub(super) struct RowPolicySectionBound {
    pub(super) heap: u64,
    pub(super) fact_map: u64,
    pub(super) replacement_vectors: u64,
    pub(super) rule_scratch: u64,
    pub(super) canonical: u64,
}
impl RowPolicySectionBound {
    pub(super) fn inspect(bytes: &[u8]) -> Result<Self, ProviderBundleEncodingErrorV1> {
        let mut reader = Reader::new(bytes);
        require_version(
            reader.u32()?,
            ROW_POLICY_CATALOG_VERSION_V1,
            "row policy catalog",
        )?;
        let fact_count = count(
            &mut reader,
            "principal fact schemas",
            MAX_PRINCIPAL_FACT_SCHEMAS_V1,
        )?;
        let mut facts = 0;
        for _ in 0..fact_count {
            add(&mut facts, name(&mut reader)?)?;
            add(&mut facts, type_heap(&mut reader, 0)?)?;
        }
        let mut heap = allocation::<PrincipalFactSchemaV1>(fact_count)?;
        add(&mut heap, facts)?;
        let fact_map = plan_hash_admission::tree_layout_bound::<String, ValueType>(fact_count)?
            .checked_add(facts)
            // std's map collection may retain an input pair vector while
            // constructing nodes. Its owned names/types move into the map;
            // only the additional vector allocation needs another allowance.
            .and_then(|value| {
                fact_count
                    .checked_mul(2)
                    .map(|count| count.max(4))
                    .and_then(|count| allocation::<(String, ValueType)>(count).ok())
                    .and_then(|bytes| value.checked_add(bytes))
            })
            .ok_or(BudgetUnavailable)?;
        let policy_count = count(&mut reader, "row policies", MAX_ROW_POLICIES_V1)?;
        let policy_vector = allocation::<RowPolicyPlanV1>(policy_count)?;
        add(&mut heap, policy_vector)?;
        let mut largest_rules = 0;
        let mut rule_scratch = 0;
        for _ in 0..policy_count {
            add(&mut heap, name(&mut reader)?)?;
            reader.u32()?; // entity, validated by ordinary constructors
            let rules = count(&mut reader, "row policy rules", MAX_ROW_POLICY_RULES_V1)?;
            let rule_vector = allocation::<RowPolicyRuleV1>(rules)?;
            add(&mut heap, rule_vector)?;
            largest_rules = largest_rules.max(rule_vector);
            for _ in 0..rules {
                reader.u8()?; // operation, validated by ordinary decoding
                reader.u32()?; // root
                let nodes = count(&mut reader, "row policy nodes", MAX_ROW_POLICY_NODES_V1)?;
                add(&mut heap, allocation::<RowPolicyExpressionNodeV1>(nodes)?)?;
                let mut cloned_types = 0;
                for _ in 0..nodes {
                    match reader.u8()? {
                        1 => add(&mut cloned_types, operand(&mut reader, &mut heap)?)?,
                        2 | 3 | 5 | 6 | 7 => {
                            reader.read(8)?;
                        }
                        4 => {
                            reader.u32()?;
                        }
                        8 => {
                            reader.u32()?;
                            reader.bool()?;
                        }
                        9 => {
                            reader.read(8)?; // target entity/index
                            let arguments = count(
                                &mut reader,
                                "row policy index arguments",
                                MAX_ROW_POLICY_INDEX_ARGUMENTS_V1,
                            )?;
                            add(&mut heap, allocation::<RowPolicyOperandV1>(arguments)?)?;
                            for _ in 0..arguments {
                                operand(&mut reader, &mut heap)?;
                            }
                        }
                        tag => {
                            return Err(IrValidationError::UnknownTag {
                                kind: "row policy node",
                                tag,
                            }
                            .into());
                        }
                    }
                }
                let scratch = allocation::<ValueType>(nodes)?
                    .checked_add(allocation::<usize>(nodes)?)
                    .and_then(|value| value.checked_add(cloned_types))
                    .ok_or(BudgetUnavailable)?;
                rule_scratch = rule_scratch.max(scratch);
            }
        }
        reader.finish()?;
        Ok(Self {
            heap,
            fact_map,
            replacement_vectors: policy_vector
                .checked_add(largest_rules)
                .ok_or(BudgetUnavailable)?,
            rule_scratch,
            canonical: allocation::<u8>(bytes.len())?,
        })
    }
    pub(super) fn constructors(&self) -> Result<u64, BudgetUnavailable> {
        // decode_catalog's first map lives throughout RowPolicyCatalog::new,
        // which builds a second map and replacement policy/rule vectors while
        // validating one rule at a time. Nodes and strings move, not clone.
        self.fact_map
            .checked_mul(2)
            .and_then(|value| value.checked_add(self.replacement_vectors))
            .and_then(|value| value.checked_add(self.rule_scratch))
            .ok_or(BudgetUnavailable)
    }
    pub(super) fn maximum(&self) -> Result<u64, BudgetUnavailable> {
        self.heap
            .checked_add(self.constructors()?.max(self.canonical))
            .and_then(|value| value.checked_add(32 * 1024))
            .and_then(|value| value.checked_add(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1))
            .ok_or(BudgetUnavailable)
    }
}

/// Decode the existing row-policy section under first-party provider admission.
/// Structural schema, input ownership and complete bundle identity are separate.
#[doc(hidden)]
pub fn decode_provider_row_policy_section_admitted_v1(
    bytes: &[u8],
    schema: &SchemaIr,
    budget: &ProviderBudget,
) -> Result<Arc<ProviderAllocation<RowPolicyCatalogV1>>, ProviderBundleEncodingErrorV1> {
    checked_len("canonical contract bundle", bytes.len(), MAX_BUNDLE_BYTES)?;
    let sizing = budget.reserve(32 * 1024)?;
    let bound = RowPolicySectionBound::inspect(bytes)?;
    let work = budget.reserve(bound.maximum()?)?;
    drop(sizing);
    let mut reader = Reader::new(bytes);
    let catalog = decode_row_policy_catalog(&mut reader, schema)?;
    reader.finish()?;
    {
        let mut writer = Writer::with_capacity(bytes.len(), bytes.len());
        encode_row_policy_catalog(&mut writer, &catalog)?;
        if writer.finish() != bytes {
            return Err(IrValidationError::NonCanonicalOrder {
                kind: "canonical contract bundle bytes",
            }
            .into());
        }
    }
    work.settle_owned_value(catalog).map_err(Into::into)
}
