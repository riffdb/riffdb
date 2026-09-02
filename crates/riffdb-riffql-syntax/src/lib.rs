// req: RQL-008
#![forbid(unsafe_code)]

//! Bounded lexer, parser, source AST, and canonical formatter for RiffQL v1.

mod diagnostic;
mod formatter;
mod lexer;
mod parser;
mod syntax;

pub use diagnostic::{DiagnosticCode, ParseDiagnostic, ParseDiagnostics};
pub use formatter::format_query;
pub use parser::{document_query_shape_language_version, parse_query, parse_query_bytes};
pub use syntax::{
    AggregateBinding, AggregateFunction, AggregateMeasure, BinaryOperator, Binding,
    CandidateBinding, CandidateSetExpression, CandidateSource, Cardinality, Direction, Document,
    ExpansionDriver, Expression, FieldSelection, Identifier, Literal, MAX_BOUNDED_SET_ITEMS_V1,
    MAX_PROJECTED_CAUSAL_WAIT_MS, MAX_PROJECTED_LAG_MS, NearestClause, NullPlacement, OrderFamily,
    OrderFamilyVariant, OrderTerm, Parameter, Path, ProjectedFreshness, ProjectedSource, QueryBody,
    RIFFQL_LANGUAGE_VERSION, RIFFQL_LANGUAGE_VERSION_BOUNDED_LIMIT_V1,
    RIFFQL_LANGUAGE_VERSION_BOUNDED_RESULT_PIPELINE_V1, RIFFQL_LANGUAGE_VERSION_EXACT_AGGREGATE_V1,
    RIFFQL_LANGUAGE_VERSION_EXACT_PREDICATE_V1, RIFFQL_LANGUAGE_VERSION_EXACT_RESULT_SET_V1,
    RIFFQL_LANGUAGE_VERSION_NULLABLE_EXACT_ORDER_V1, RIFFQL_LANGUAGE_VERSION_OPERATIONAL_V1,
    RIFFQL_LANGUAGE_VERSION_ORDER_FAMILY_V1, RIFFQL_LANGUAGE_VERSION_PARTITION_SET_V1,
    RIFFQL_LANGUAGE_VERSION_PROJECTED_VECTOR_V1, RIFFQL_LANGUAGE_VERSION_RELATIONAL_OPERATORS_V1,
    RIFFQL_LANGUAGE_VERSION_SECRET_OUTPUT_V1, RIFFQL_LANGUAGE_VERSION_TOKENIZED_TEXT_V1, Selection,
    Span, Spanned, Take, TokenizedMatchClause, TokenizedMatchKind, TokenizedRanking, TypeReference,
    UnaryOperator,
};

/// Maximum accepted UTF-8 query source bytes.
pub const MAX_SOURCE_BYTES: usize = 1_048_576;
/// Maximum identifier bytes.
pub const MAX_IDENTIFIER_BYTES: usize = 256;
/// Maximum tokens or AST nodes.
pub const MAX_SYNTAX_ITEMS: usize = 131_072;
/// Maximum delimiter/expression nesting.
pub const MAX_NESTING: usize = 32;
/// Maximum bindings in one query.
pub const MAX_BINDINGS: usize = 4_096;
/// Maximum compiler-owned candidate sources in one binding.
pub const MAX_CANDIDATE_SOURCES: usize = 8;
/// Maximum aggregate declarations in one query.
pub const MAX_AGGREGATE_BINDINGS: usize = 16;
/// Maximum measures in one aggregate declaration.
pub const MAX_AGGREGATE_MEASURES: usize = 16;
/// Maximum grouping keys in one aggregate declaration.
pub const MAX_AGGREGATE_GROUP_KEYS: usize = 8;
/// Maximum members in one parameter, order, or selection collection.
pub const MAX_COLLECTION_ITEMS: usize = 1_024;
/// Maximum diagnostics returned by one parse.
pub const MAX_DIAGNOSTICS: usize = 32;
