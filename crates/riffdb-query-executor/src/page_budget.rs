use std::cell::Cell;

use riffdb_query_ir::{QueryAccessKind, QueryAccessStep};
use riffdb_riffql_syntax::Cardinality;
use riffdb_types::{CanonicalValue, ServiceIngressKindV1, canonical_value_encoded_len};

use crate::QueryRow;

thread_local! {
    static ACTIVE_QUERY_PAGE_INGRESS_V1: Cell<ServiceIngressKindV1> =
        const { Cell::new(ServiceIngressKindV1::Grpc) };
}

struct QueryPageIngressGuardV1 {
    prior: ServiceIngressKindV1,
}

impl Drop for QueryPageIngressGuardV1 {
    fn drop(&mut self) {
        ACTIVE_QUERY_PAGE_INGRESS_V1.with(|cell| cell.set(self.prior));
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

#[doc(hidden)]
pub fn step_page_byte_budget_v1(step: &QueryAccessStep) -> Option<usize> {
    if step.cardinality() == Cardinality::Many
        && step.cursor_parameter().is_some()
        && !step.result_names().is_empty()
        && matches!(
            step.access(),
            QueryAccessKind::Index { .. }
                | QueryAccessKind::PartitionSetIndex { .. }
                | QueryAccessKind::CandidateRootHydration { .. }
        )
    {
        let binary_budget = if let Some(budget) = step.internal_page_byte_budget() {
            usize::try_from(budget).ok()?
        } else {
            let result_names_overhead = step.result_names().iter().fold(0usize, |acc, name| {
                acc.saturating_add(name.len()).saturating_add(128)
            });
            let fields_overhead = step.selected_fields().iter().fold(0usize, |acc, field| {
                acc.saturating_add(field.len()).saturating_add(32)
            });
            let framing = 1024usize
                .saturating_add(step.entity().len())
                .saturating_add(result_names_overhead)
                .saturating_add(fields_overhead);
            4_194_304usize.saturating_sub(framing)
        };
        if is_mcp_page_budget_active_v1() {
            // Hosted MCP CallToolResult frames carry both `structuredContent` and
            // JSON-escaped `content[0].text` within `MCP_OUTBOUND_MESSAGE_MAX_BYTES`
            // (4 MiB), and base64-encode `bytes` values (~2.67x across both copies).
            let reserved_other = 4_194_304usize.saturating_sub(binary_budget);
            let mcp_reserved = reserved_other.saturating_mul(3).saturating_add(16_384);
            Some(4_194_304usize.saturating_sub(mcp_reserved))
        } else {
            Some(binary_budget)
        }
    } else {
        None
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

fn estimated_grpc_value_result_bytes_v1(value: &CanonicalValue) -> usize {
    let binary_len =
        canonical_value_encoded_len(value).unwrap_or(riffdb_types::MAX_BYTES_VALUE_BYTES);
    let json_len = match value {
        CanonicalValue::Null | CanonicalValue::Bool(_) => 16,
        CanonicalValue::I64(_) | CanonicalValue::U64(_) => 32,
        CanonicalValue::Decimal(_)
        | CanonicalValue::Money(_)
        | CanonicalValue::Timestamp(_)
        | CanonicalValue::Date(_)
        | CanonicalValue::Uuid(_)
        | CanonicalValue::Enum { .. } => 80,
        CanonicalValue::String(text) => {
            let mut total = 16usize;
            for &byte in text.as_str().as_bytes() {
                let weight = match byte {
                    b'"' | b'\\' | b'\x08' | b'\x0c' | b'\n' | b'\r' | b'\t' => 2,
                    0x00..=0x1f => 6,
                    _ => 1,
                };
                total = total.saturating_add(weight);
            }
            total
        }
        CanonicalValue::Bytes(bytes) => bytes
            .as_bytes()
            .len()
            .div_ceil(3)
            .saturating_mul(4)
            .saturating_add(32),
        CanonicalValue::List(list) => list.values().iter().fold(16usize, |acc, item| {
            acc.saturating_add(estimated_grpc_value_result_bytes_v1(item))
                .saturating_add(2)
        }),
        CanonicalValue::Record(record) => record.fields().iter().fold(16usize, |acc, (_, item)| {
            acc.saturating_add(estimated_grpc_value_result_bytes_v1(item))
                .saturating_add(32)
        }),
        CanonicalValue::Vector(vector) => vector
            .components()
            .len()
            .saturating_mul(24)
            .saturating_add(16),
    };
    binary_len.max(json_len)
}

#[doc(hidden)]
pub fn estimated_canonical_value_result_bytes_v1(value: &CanonicalValue) -> usize {
    if is_mcp_page_budget_active_v1() {
        estimated_mcp_value_result_bytes_v1(value)
    } else {
        estimated_grpc_value_result_bytes_v1(value)
    }
}

#[doc(hidden)]
pub fn estimated_query_row_result_bytes_v1(step: &QueryAccessStep, row: &QueryRow) -> usize {
    let copies = step.result_names().len().max(1);
    let field_scale = if is_mcp_page_budget_active_v1() { 2 } else { 1 };
    let base = step
        .entity()
        .len()
        .saturating_mul(field_scale)
        .saturating_add(64 * field_scale);
    let per_copy = step.selected_fields().iter().fold(base, |acc, field_name| {
        let value_bytes = row
            .field(field_name)
            .map_or(2, estimated_canonical_value_result_bytes_v1);
        acc.saturating_add(160 * field_scale)
            .saturating_add(field_name.len().saturating_mul(field_scale))
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
    let field_scale = if is_mcp_page_budget_active_v1() { 2 } else { 1 };
    let base = step
        .entity()
        .len()
        .saturating_mul(field_scale)
        .saturating_add(64 * field_scale);
    let per_copy = values.iter().enumerate().fold(base, |acc, (idx, value)| {
        let name_len = step.selected_fields().get(idx).map_or(32, String::len);
        acc.saturating_add(160 * field_scale)
            .saturating_add(name_len.saturating_mul(field_scale))
            .saturating_add(estimated_canonical_value_result_bytes_v1(value))
    });
    per_copy.saturating_mul(copies)
}

#[cfg(test)]
mod tests {
    use super::*;

    // req: GEN-005, DRV-007
    #[test]
    fn mcp_ingress_page_budget_accounts_for_dual_json_and_base64_expansion() {
        let wide_str = CanonicalValue::String(
            riffdb_types::CanonicalString::new("M".repeat(380_000)).unwrap(),
        );
        let wide_bytes =
            CanonicalValue::Bytes(riffdb_types::CanonicalBytes::new(vec![0x4d; 520_000]).unwrap());
        let grpc_str = estimated_canonical_value_result_bytes_v1(&wide_str);
        let grpc_bytes = estimated_canonical_value_result_bytes_v1(&wide_bytes);
        // gRPC/CLI budget charges base64 + JSON envelope so 3 wide rows fit within
        // ADR-0041's 4 MiB CLI JSON limit while 4 rows split to a continuation page.
        assert!((grpc_str + grpc_bytes) * 3 < 4_194_304);
        assert!((grpc_str + grpc_bytes) * 4 > 4_194_304);
        assert!(!is_mcp_page_budget_active_v1());

        let (mcp_str, mcp_bytes) =
            with_ingress_page_budget_v1(ServiceIngressKindV1::McpHttp, || {
                assert!(is_mcp_page_budget_active_v1());
                (
                    estimated_canonical_value_result_bytes_v1(&wide_str),
                    estimated_canonical_value_result_bytes_v1(&wide_bytes),
                )
            });
        assert!(!is_mcp_page_budget_active_v1());
        assert!(mcp_str >= 760_000);
        assert!(mcp_bytes >= 1_386_000);
        // Two wide rows exceed the 4 MiB MCP CallToolResult frame, forcing a 1-row page split.
        assert!((mcp_str + mcp_bytes) * 2 > 4_194_304);
    }
}
