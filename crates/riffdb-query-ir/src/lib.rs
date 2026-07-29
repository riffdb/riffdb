#![forbid(unsafe_code)]

//! Exact-contract symbolic catalog and canonical typed RiffQL surface IR.

mod catalog;
mod diagnostic;
mod plan;
mod resolver;
mod schema;

pub use catalog::{EntitySymbol, EnumSymbol, FieldSymbol, IndexSymbol, SymbolicCatalog};
pub use diagnostic::{
    QueryDiagnostic, QueryDiagnosticCode, QueryDiagnosticStage, QueryDiagnostics,
};
pub use plan::{
    AccessDirection, AuthorizationEntityAccess, QueryAccessKind, QueryAccessProgramV1,
    QueryAccessStep, QueryPlanExplain, QueryPlanIdentity,
};
pub use resolver::{
    BindingSymbol, ExactContractIdentity, QuerySourceMap, ResolvedQueryV1, SourceMapEntry,
    SourceSymbolKind, resolve_query_surface,
};
pub use schema::{
    NamedFieldSchema, NamedParameterSchema, NamedQuerySchemas, NamedResultBranchSchema,
    NamedTypeSchema, PageBound,
};

/// Canonical typed query-IR version.
pub const QUERY_IR_VERSION_V1: u32 = 1;
/// Maximum public query schema and canonical IR bytes.
pub const MAX_QUERY_ARTIFACT_BYTES: usize = 4_194_304;
/// Maximum source-map entries.
pub const MAX_SOURCE_MAP_ENTRIES: usize = 131_072;
