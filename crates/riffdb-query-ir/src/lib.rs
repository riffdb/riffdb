#![forbid(unsafe_code)]

//! Exact-contract symbolic catalog and canonical typed RiffQL surface IR.

mod catalog;
mod diagnostic;
mod live;
mod operational;
mod plan;
mod reactive;
mod resolver;
mod schema;

pub use catalog::{
    EntitySymbol, EnumSymbol, FieldSymbol, IndexSymbol, PrincipalFactSymbol, RowPolicySymbol,
    SymbolicCatalog,
};
pub use diagnostic::{
    QueryDiagnostic, QueryDiagnosticCode, QueryDiagnosticStage, QueryDiagnostics,
};
pub use live::*;
pub use operational::*;
pub use plan::{
    AccessDirection, AuthorizationEntityAccess, QueryAccessKind, QueryAccessProgramV1,
    QueryAccessStep, QueryLiteral, QueryPlanExplain, QueryPlanIdentity, QueryPredicate,
    QueryPredicateOperator, QueryPredicateValue, QueryRowLimit,
};
pub use reactive::*;
pub use resolver::{
    BindingSymbol, ExactContractIdentity, QuerySourceMap, ResolvedQueryV1, SecretOutputRequirement,
    SourceMapEntry, SourceSymbolKind, resolve_query_surface,
};
pub use schema::{
    NamedFieldSchema, NamedParameterSchema, NamedQuerySchemas, NamedResultBranchSchema,
    NamedTypeSchema, PageBound,
};

/// Canonical typed query-IR version.
pub const QUERY_IR_VERSION_V1: u32 = 1;
/// Canonical finite operational plan-family IR version.
pub const QUERY_IR_VERSION_OPERATIONAL_V1: u32 = 2;
/// Canonical finite operational plan-family IR with exact aggregate descriptors.
pub const QUERY_IR_VERSION_OPERATIONAL_AGGREGATE_V1: u32 = 3;
/// Canonical query IR carrying exact secret-output requirements.
pub const QUERY_IR_VERSION_SECRET_OUTPUT_V1: u32 = 4;
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
pub const MAX_QUERY_SCANNED_ROWS: u64 = 500;

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
const _: () = assert!(max_query_page_take() == 499);
const _: () = assert!(MAX_QUERY_SCANNED_ROWS == 500);
const _: () = assert!(QUERY_CONTINUATION_PROBE_ROWS == 1);
