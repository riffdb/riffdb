#![forbid(unsafe_code)]

//! Exact-contract symbolic catalog and canonical typed RiffQL surface IR.

mod catalog;
mod diagnostic;
mod exact_predicate;
mod exact_text;
mod live;
mod operational;
mod plan;
mod reactive;
mod resolver;
mod result_set;
mod result_set_v2;
mod schema;
mod tokenized_text;

pub use catalog::{
    EntitySymbol, EnumSymbol, FieldSymbol, IndexSymbol, PrincipalFactSymbol, RowPolicySymbol,
    SymbolicCatalog,
};
pub use diagnostic::{
    QueryDiagnostic, QueryDiagnosticCode, QueryDiagnosticStage, QueryDiagnostics,
};
pub use exact_predicate::*;
pub use exact_text::*;
pub use live::*;
pub use operational::*;
pub use plan::{
    AccessDirection, AuthorizationEntityAccess, CoveredResultFieldV1, CoveredResultLayoutV1,
    CoveredResultSourceV1, ProjectedVectorFreshnessV1, ProjectedVectorSourceV1, QueryAccessKind,
    QueryAccessProgramV1, QueryAccessStep, QueryLiteral, QueryPlanExplain, QueryPlanIdentity,
    QueryPredicate, QueryPredicateOperator, QueryPredicateValue, QueryRowLimit,
};
pub use reactive::*;
pub use resolver::{
    BindingSymbol, ExactContractIdentity, QuerySourceMap, ResolvedQueryV1, SecretOutputRequirement,
    SourceMapEntry, SourceSymbolKind, resolve_query_surface,
};
pub use result_set::*;
pub use result_set_v2::*;
pub use schema::{
    NamedFieldSchema, NamedParameterSchema, NamedQuerySchemas, NamedResultBranchSchema,
    NamedTypeSchema, PageBound,
};
pub use tokenized_text::*;

/// Canonical typed query-IR version.
pub const QUERY_IR_VERSION_V1: u32 = 1;
/// Canonical finite operational plan-family IR version.
pub const QUERY_IR_VERSION_OPERATIONAL_V1: u32 = 2;
/// Canonical finite operational plan-family IR with exact aggregate descriptors.
pub const QUERY_IR_VERSION_OPERATIONAL_AGGREGATE_V1: u32 = 3;
/// Canonical query IR carrying exact secret-output requirements.
pub const QUERY_IR_VERSION_SECRET_OUTPUT_V1: u32 = 4;
/// Canonical exact whole-result/count/ordinal query IR version.
pub const QUERY_IR_VERSION_EXACT_RESULT_SET_V1: u32 = 5;
/// Canonical exact result-set IR with one compiler-bound typed equality filter.
pub const QUERY_IR_VERSION_EXACT_FILTERED_RESULT_SET_V1: u32 = 6;
/// Canonical ordinary/operational query IR with a sealed covered-result layout.
pub const QUERY_IR_VERSION_COVERED_RESULT_V1: u32 = 7;
/// Canonical nearest-query IR with one compiler-owned projected source.
pub const QUERY_IR_VERSION_PROJECTED_VECTOR_V1: u32 = 8;
/// Canonical exact predicate and independent-order semantic IR.
pub const QUERY_IR_VERSION_EXACT_PREDICATE_V1: u32 = 9;
/// Canonical exact predicate IR with nullable total-order placement.
pub const QUERY_IR_VERSION_NULLABLE_EXACT_ORDER_V1: u32 = 10;
/// Additive operational aggregate core IR identity.
pub const QUERY_IR_VERSION_EXACT_AGGREGATE_V1: u32 = 11;
/// Canonical query IR carrying compiler-declared bounded runtime page limits.
pub const QUERY_IR_VERSION_BOUNDED_LIMIT_V1: u32 = 12;
/// Canonical compiler-sealed tokenized-text query IR version.
pub const QUERY_IR_VERSION_TOKENIZED_TEXT_V1: u32 = 13;
/// Canonical bounded filtered-result pipeline and enlarged page-limit IR.
pub const QUERY_IR_VERSION_BOUNDED_RESULT_PIPELINE_V1: u32 = 14;
/// Maximum public query schema and canonical IR bytes.
pub const MAX_QUERY_ARTIFACT_BYTES: usize = 4_194_304;
/// Maximum source-map entries.
pub const MAX_SOURCE_MAP_ENTRIES: usize = 131_072;

/// Maximum physical index rows one access step may report as scanned.
///
/// Adapters fetch `take + QUERY_CONTINUATION_PROBE_ROWS` when deciding whether a
/// continuation is minted, and charge the peeked observation to `scanned_rows`.
/// Therefore a legal page `take` must satisfy
/// `take + QUERY_CONTINUATION_PROBE_ROWS <= MAX_QUERY_SCANNED_ROWS`.
pub const MAX_QUERY_SCANNED_ROWS: u64 = riffdb_types::MAX_APPLICATION_QUERY_SCANNED_ROWS;

/// Extra physical rows inspected solely to decide whether a continuation exists.
///
/// Storage adapters request `page_limit + this` candidates; when an extra match
/// is observed the page is truncated to `page_limit` and a continuation is
/// minted, while `scanned_rows` still counts the probe.
pub const QUERY_CONTINUATION_PROBE_ROWS: u64 = 1;

/// Inclusive maximum legal page `take` (static literal or runtime `Limit`).
///
/// Equal to `MAX_QUERY_SCANNED_ROWS - QUERY_CONTINUATION_PROBE_ROWS` so a
/// continuation probe can never push scanned work past the scan ceiling.
#[must_use]
pub const fn max_query_page_take() -> u64 {
    MAX_QUERY_SCANNED_ROWS.saturating_sub(QUERY_CONTINUATION_PROBE_ROWS)
}

/// Physical rows an adapter may report when serving `take` with a continuation probe.
#[must_use]
pub const fn scanned_rows_budget_for_page_take(take: u64) -> Option<u64> {
    take.checked_add(QUERY_CONTINUATION_PROBE_ROWS)
}

/// Whether a declared or submitted page take stays within the scan ceiling.
///
/// Rejects zero and any take where `take + probe > MAX_QUERY_SCANNED_ROWS`.
#[must_use]
pub const fn page_take_within_scan_bound(take: u64) -> bool {
    match scanned_rows_budget_for_page_take(take) {
        Some(scanned) if take > 0 => scanned <= MAX_QUERY_SCANNED_ROWS,
        _ => false,
    }
}

// Resolver diagnostics name this bound as a static string; keep them aligned.
const _: () = assert!(max_query_page_take() == 65_534);
const _: () = assert!(MAX_QUERY_SCANNED_ROWS == 65_535);
const _: () = assert!(QUERY_CONTINUATION_PROBE_ROWS == 1);
