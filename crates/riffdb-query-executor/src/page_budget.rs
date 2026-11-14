use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};

use riffdb_query_ir::{
    OperationalAggregateV1, QueryAccessKind, QueryAccessStep, QueryPredicateValue,
};
use riffdb_riffql_syntax::Cardinality;
use riffdb_types::{CanonicalValue, ServiceIngressKindV1};

use crate::{QueryAggregateCell, QueryAggregateRow, QueryResultValue, QueryRow};

const MCP_OUTBOUND_FRAME_MAX_BYTES: usize = 4_194_304;
const MCP_ENVELOPE_FRAMING_RESERVE_BYTES: usize = 16_384;
const WORST_CASE_CONTROL_CHAR_MCP_MULTIPLIER: usize = 13;

thread_local! {
    static ACTIVE_QUERY_PAGE_INGRESS_V1: Cell<ServiceIngressKindV1> =
        const { Cell::new(ServiceIngressKindV1::Grpc) };
    static ACTIVE_MATERIALIZED_SIBLING_MCP_BYTES_V1: Cell<Option<usize>> =
        const { Cell::new(None) };
}

struct QueryPageIngressGuardV1 {
    prior: ServiceIngressKindV1,
}

impl Drop for QueryPageIngressGuardV1 {
    fn drop(&mut self) {
        ACTIVE_QUERY_PAGE_INGRESS_V1.with(|cell| cell.set(self.prior));
    }
}

pub(crate) struct MaterializedSiblingBudgetGuardV1 {
    prior: Option<usize>,
}

impl Drop for MaterializedSiblingBudgetGuardV1 {
    fn drop(&mut self) {
        ACTIVE_MATERIALIZED_SIBLING_MCP_BYTES_V1.with(|cell| cell.set(self.prior));
    }
}

#[doc(hidden)]
pub fn with_ingress_page_budget_v1<R>(ingress: ServiceIngressKindV1, run: impl FnOnce() -> R) -> R {
    let prior = ACTIVE_QUERY_PAGE_INGRESS_V1.with(|cell| {
        let prev = cell.get();
        cell.set(ingress);
        prev
    });
    let _guard = QueryPageIngressGuardV1 { prior };
    run()
}

#[doc(hidden)]
#[must_use]
pub fn is_mcp_page_budget_active_v1() -> bool {
    ACTIVE_QUERY_PAGE_INGRESS_V1.with(|cell| cell.get() == ServiceIngressKindV1::McpHttp)
}

pub(crate) fn enter_materialized_sibling_mcp_bytes_v1(
    sibling_bytes: usize,
) -> MaterializedSiblingBudgetGuardV1 {
    let prior = ACTIVE_MATERIALIZED_SIBLING_MCP_BYTES_V1.with(|cell| {
        let prev = cell.get();
        cell.set(Some(sibling_bytes));
        prev
    });
    MaterializedSiblingBudgetGuardV1 { prior }
}

#[doc(hidden)]
#[must_use]
pub fn has_materialized_sibling_mcp_bytes_v1() -> bool {
    ACTIVE_MATERIALIZED_SIBLING_MCP_BYTES_V1.with(|cell| cell.get().is_some_and(|bytes| bytes > 0))
}

pub(crate) fn is_cursor_paginated_result_step_v1(step: &QueryAccessStep) -> bool {
    step.cardinality() == Cardinality::Many
        && step.cursor_parameter().is_some()
        && !step.result_names().is_empty()
        && matches!(
            step.access(),
            QueryAccessKind::Index { .. }
                | QueryAccessKind::PartitionSetIndex { .. }
                | QueryAccessKind::CandidateRootHydration { .. }
        )
}

fn step_references_any_binding(
    step: &QueryAccessStep,
    aggregates: &[OperationalAggregateV1],
    bindings: &BTreeSet<&str>,
) -> bool {
    if step
        .dependencies()
        .iter()
        .any(|dep| bindings.contains(dep.as_str()))
    {
        return true;
    }
    if let QueryAccessKind::DependentPointBatch { source_binding, .. } = step.access()
        && bindings.contains(source_binding.as_str())
    {
        return true;
    }
    if step.predicates().iter().any(|pred| match pred.value() {
        QueryPredicateValue::BindingField { binding, .. }
        | QueryPredicateValue::BindingFieldSet { binding, .. } => {
            bindings.contains(binding.as_str())
        }
        _ => false,
    }) {
        return true;
    }
    aggregates
        .iter()
        .filter(|agg| agg.source_binding() == step.binding())
        .any(|agg| {
            agg.parent_binding().is_some_and(|p| bindings.contains(p))
                || agg.having().iter().any(|pred| match pred.value() {
                    QueryPredicateValue::BindingField { binding, .. } => {
                        bindings.contains(binding.as_str())
                    }
                    _ => false,
                })
                || agg.measures().iter().any(|m| {
                    m.filter().iter().any(|pred| match pred.value() {
                        QueryPredicateValue::BindingField { binding, .. } => {
                            bindings.contains(binding.as_str())
                        }
                        _ => false,
                    })
                })
        })
}

/// Schedules independent non-paged sibling steps ahead of a cursor-paginated
/// `Many` step so exact materialized sibling encoding sizes are known before
/// scanning the paged binding. Relative topological order among non-paged steps
/// and all steps transitively dependent on the paged binding is preserved.
pub(crate) fn ordered_steps_for_paged_budget(
    steps: &[QueryAccessStep],
    aggregates: &[OperationalAggregateV1],
) -> (Vec<usize>, Option<usize>, usize) {
    let Some(paged_idx) = steps.iter().position(is_cursor_paginated_result_step_v1) else {
        return ((0..steps.len()).collect(), None, 0);
    };

    let mut dep_bindings = BTreeSet::from([steps[paged_idx].binding()]);
    let mut independent_before = Vec::new();
    let mut dependent_after = Vec::new();
    for (idx, step) in steps.iter().enumerate().skip(paged_idx + 1) {
        if step_references_any_binding(step, aggregates, &dep_bindings) {
            dep_bindings.insert(step.binding());
            dependent_after.push(idx);
        } else {
            independent_before.push(idx);
        }
    }

    let mut order = Vec::with_capacity(steps.len());
    order.extend(0..paged_idx);
    order.extend(independent_before);
    order.push(paged_idx);
    order.extend(dependent_after.iter().copied());

    let has_post_paged_work = !dependent_after.is_empty()
        || aggregates
            .iter()
            .any(|agg| dep_bindings.contains(agg.source_binding()));
    let post_paged_worst_case_bytes = if has_post_paged_work {
        let binary_budget = steps[paged_idx]
            .internal_page_byte_budget()
            .and_then(|b| usize::try_from(b).ok())
            .unwrap_or(MCP_OUTBOUND_FRAME_MAX_BYTES);
        MCP_OUTBOUND_FRAME_MAX_BYTES
            .saturating_sub(binary_budget)
            .saturating_mul(WORST_CASE_CONTROL_CHAR_MCP_MULTIPLIER)
    } else {
        0
    };

    (order, Some(paged_idx), post_paged_worst_case_bytes)
}

#[doc(hidden)]
pub fn step_page_byte_budget_v1(step: &QueryAccessStep) -> Option<usize> {
    if !is_cursor_paginated_result_step_v1(step) {
        return None;
    }

    // Both Hosted HTTP MCP and Stdio MCP (`riffdb-mcp`, an ordinary gRPC client)
    // render every query page into a `CallToolResult` carrying `structuredContent`
    // plus JSON-escaped `content[0].text` within `MCP_OUTBOUND_MESSAGE_MAX_BYTES`
    // (4 MiB). Budgeting cursor-paginated `Many` steps against that worst-case
    // presentation envelope guarantees every surface (gRPC, `riffdb-driverd`,
    // CLI `--output json`, Hosted MCP, and Stdio MCP) stays within 4 MiB.
    let active_materialized = ACTIVE_MATERIALIZED_SIBLING_MCP_BYTES_V1.with(Cell::get);
    if let Some(sibling_mcp_bytes) = active_materialized {
        let reserved = sibling_mcp_bytes.saturating_add(MCP_ENVELOPE_FRAMING_RESERVE_BYTES);
        return Some(MCP_OUTBOUND_FRAME_MAX_BYTES.saturating_sub(reserved));
    }

    // Standalone unit-test fallback outside executor sibling tracking:
    // fail closed against worst-case 13x control-character sibling expansion.
    if let Some(budget) = step.internal_page_byte_budget() {
        let binary_budget = usize::try_from(budget).ok()?;
        let fields_overhead = step.selected_fields().iter().fold(0usize, |acc, field| {
            acc.saturating_add(field.len()).saturating_add(32)
        });
        let result_names_overhead = step.result_names().iter().fold(128usize, |acc, name| {
            acc.saturating_add(name.len()).saturating_add(32)
        });
        let own_framing = fields_overhead.saturating_add(result_names_overhead);
        let raw_other = MCP_OUTBOUND_FRAME_MAX_BYTES
            .saturating_sub(binary_budget)
            .saturating_sub(own_framing);
        let reserved = raw_other
            .saturating_mul(WORST_CASE_CONTROL_CHAR_MCP_MULTIPLIER)
            .saturating_add(MCP_ENVELOPE_FRAMING_RESERVE_BYTES);
        Some(MCP_OUTBOUND_FRAME_MAX_BYTES.saturating_sub(reserved))
    } else {
        Some(MCP_OUTBOUND_FRAME_MAX_BYTES.saturating_sub(MCP_ENVELOPE_FRAMING_RESERVE_BYTES))
    }
}

fn estimated_mcp_value_result_bytes_v1(value: &CanonicalValue) -> usize {
    match value {
        CanonicalValue::Null | CanonicalValue::Bool(_) => 16,
        CanonicalValue::I64(_) | CanonicalValue::U64(_) => 64,
        CanonicalValue::Decimal(_)
        | CanonicalValue::Money(_)
        | CanonicalValue::Timestamp(_)
        | CanonicalValue::Date(_)
        | CanonicalValue::Uuid(_)
        | CanonicalValue::Enum { .. } => 160,
        CanonicalValue::String(text) => {
            let mut total = 6usize;
            for &byte in text.as_str().as_bytes() {
                let weight = match byte {
                    b'"' | b'\\' => 6,
                    b'\x08' | b'\x0c' | b'\n' | b'\r' | b'\t' => 5,
                    0x00..=0x1f => 13,
                    _ => 2,
                };
                total = total.saturating_add(weight);
            }
            total
        }
        CanonicalValue::Bytes(bytes) => {
            let base64_len = bytes.as_bytes().len().div_ceil(3).saturating_mul(4);
            base64_len.saturating_mul(2).saturating_add(6)
        }
        CanonicalValue::List(list) => list.values().iter().fold(8usize, |acc, item| {
            acc.saturating_add(estimated_mcp_value_result_bytes_v1(item))
                .saturating_add(4)
        }),
        CanonicalValue::Record(record) => record.fields().iter().fold(16usize, |acc, (_, item)| {
            acc.saturating_add(estimated_mcp_value_result_bytes_v1(item))
                .saturating_add(64)
        }),
        CanonicalValue::Vector(vector) => vector
            .components()
            .len()
            .saturating_mul(48)
            .saturating_add(8),
    }
}

fn estimated_aggregate_cell_mcp_bytes_v1(cell: &QueryAggregateCell) -> usize {
    match cell {
        QueryAggregateCell::Canonical(val) => estimated_mcp_value_result_bytes_v1(val),
        QueryAggregateCell::ExactDecimal { .. } => 192,
        QueryAggregateCell::ExactMean { .. } => 384,
    }
}

fn estimated_aggregate_row_mcp_bytes_v1(row: &QueryAggregateRow) -> usize {
    row.fields()
        .iter()
        .fold(64usize, |acc, (field_name, cell)| {
            acc.saturating_add(field_name.len().saturating_mul(2))
                .saturating_add(32)
                .saturating_add(estimated_aggregate_cell_mcp_bytes_v1(cell))
        })
}

fn estimated_projected_row_mcp_bytes_v1(row: &QueryRow) -> usize {
    let scalar_bytes = row.fields().fold(64usize, |acc, (field_name, val)| {
        acc.saturating_add(field_name.len().saturating_mul(2))
            .saturating_add(32)
            .saturating_add(estimated_mcp_value_result_bytes_v1(val))
    });
    let nested_bytes = row
        .nested_fields()
        .fold(0usize, |acc, (nested_name, nested_rows)| {
            let children = nested_rows.iter().fold(16usize, |inner, child| {
                inner.saturating_add(estimated_projected_row_mcp_bytes_v1(child))
            });
            acc.saturating_add(nested_name.len().saturating_mul(2))
                .saturating_add(32)
                .saturating_add(children)
        });
    scalar_bytes.saturating_add(nested_bytes)
}

pub(crate) fn estimated_result_fields_mcp_bytes_v1(
    result_fields: &BTreeMap<String, QueryResultValue>,
) -> usize {
    result_fields.iter().fold(0usize, |acc, (name, value)| {
        let key_overhead = name.len().saturating_mul(2).saturating_add(48);
        let payload = match value {
            QueryResultValue::One(row) => estimated_projected_row_mcp_bytes_v1(row),
            QueryResultValue::Maybe(None) => 16,
            QueryResultValue::Maybe(Some(row)) => estimated_projected_row_mcp_bytes_v1(row),
            QueryResultValue::Many(rows) => rows.iter().fold(16usize, |sum, row| {
                sum.saturating_add(estimated_projected_row_mcp_bytes_v1(row))
            }),
            QueryResultValue::AggregateOne(agg) => estimated_aggregate_row_mcp_bytes_v1(agg),
            QueryResultValue::AggregateMany(aggs) => aggs.iter().fold(16usize, |sum, agg| {
                sum.saturating_add(estimated_aggregate_row_mcp_bytes_v1(agg))
            }),
        };
        acc.saturating_add(key_overhead).saturating_add(payload)
    })
}

#[doc(hidden)]
pub fn estimated_canonical_value_result_bytes_v1(value: &CanonicalValue) -> usize {
    estimated_mcp_value_result_bytes_v1(value)
}

#[doc(hidden)]
pub fn estimated_query_row_result_bytes_v1(step: &QueryAccessStep, row: &QueryRow) -> usize {
    let copies = step.result_names().len().max(1);
    let base = step.entity().len().saturating_mul(2).saturating_add(64);
    let per_copy = step.selected_fields().iter().fold(base, |acc, field_name| {
        let value_bytes = row
            .field(field_name)
            .map_or(16, estimated_mcp_value_result_bytes_v1);
        acc.saturating_add(32)
            .saturating_add(field_name.len().saturating_mul(2))
            .saturating_add(value_bytes)
    });
    per_copy.saturating_mul(copies)
}

#[doc(hidden)]
pub fn estimated_covered_row_result_bytes_v1(
    step: &QueryAccessStep,
    values: &[CanonicalValue],
) -> usize {
    let copies = step.result_names().len().max(1);
    let base = step.entity().len().saturating_mul(2).saturating_add(64);
    let per_copy = values.iter().enumerate().fold(base, |acc, (idx, value)| {
        let name_len = step.selected_fields().get(idx).map_or(32, String::len);
        acc.saturating_add(32)
            .saturating_add(name_len.saturating_mul(2))
            .saturating_add(estimated_mcp_value_result_bytes_v1(value))
    });
    per_copy.saturating_mul(copies)
}

#[cfg(test)]
mod tests {
    use super::*;

    // req: GEN-005, DRV-007
    #[test]
    fn mcp_safe_page_budget_accounts_for_dual_json_base64_and_control_char_expansion() {
        let wide_str = CanonicalValue::String(
            riffdb_types::CanonicalString::new("M".repeat(380_000)).unwrap(),
        );
        let wide_bytes =
            CanonicalValue::Bytes(riffdb_types::CanonicalBytes::new(vec![0x4d; 520_000]).unwrap());
        let escaped_sibling = CanonicalValue::String(
            riffdb_types::CanonicalString::new("\x01".repeat(300_000)).unwrap(),
        );
        let ascii_row = CanonicalValue::String(
            riffdb_types::CanonicalString::new("A".repeat(100_000)).unwrap(),
        );

        let str_cost = estimated_canonical_value_result_bytes_v1(&wide_str);
        let bytes_cost = estimated_canonical_value_result_bytes_v1(&wide_bytes);
        // 1 wide text+blob row (~2.15 MB dual-JSON) fits; 2 rows (~4.30 MB) split across pages
        // on all ingresses so both Hosted MCP and Stdio MCP (`riffdb-mcp`) stay <= 4 MiB.
        assert!(str_cost + bytes_cost < MCP_OUTBOUND_FRAME_MAX_BYTES);
        assert!((str_cost + bytes_cost) * 2 > MCP_OUTBOUND_FRAME_MAX_BYTES);

        // 300,000 `\x01` control bytes expand 13x across structuredContent + content[0].text
        // (~3.90 MB), leaving room for 1 x 100,000-byte row (~200 KB) but splitting before 2 rows.
        let sibling_cost = estimated_canonical_value_result_bytes_v1(&escaped_sibling);
        let row_cost = estimated_canonical_value_result_bytes_v1(&ascii_row);
        let remaining = MCP_OUTBOUND_FRAME_MAX_BYTES
            .saturating_sub(sibling_cost + MCP_ENVELOPE_FRAMING_RESERVE_BYTES);
        assert!(sibling_cost >= 3_900_000);
        assert!(row_cost < remaining);
        assert!(row_cost * 2 > remaining);
    }
}
