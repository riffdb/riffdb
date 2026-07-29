#![forbid(unsafe_code)]

//! Bounded lexer, parser, source AST, and canonical formatter for RiffQL v1.

mod diagnostic;
mod formatter;
mod lexer;
mod parser;
mod syntax;

pub use diagnostic::{DiagnosticCode, ParseDiagnostic, ParseDiagnostics};
pub use formatter::format_query;
pub use parser::{parse_query, parse_query_bytes};
pub use syntax::{
    BinaryOperator, Binding, Cardinality, Direction, Document, Expression, FieldSelection,
    Identifier, Literal, OrderTerm, Parameter, Path, QueryBody, RIFFQL_LANGUAGE_VERSION, Selection,
    Span, Spanned, Take, TypeReference,
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
/// Maximum members in one parameter, order, or selection collection.
pub const MAX_COLLECTION_ITEMS: usize = 1_024;
/// Maximum diagnostics returned by one parse.
pub const MAX_DIAGNOSTICS: usize = 32;
