#![forbid(unsafe_code)]

//! Source parser and source-oriented AST for RiffDB contract grammar version 1.

pub mod ast;
pub mod diagnostic;
pub mod limits;
pub mod span;

mod lexer;
mod migration;
mod parser;

lalrpop_util::lalrpop_mod!(
    #[allow(clippy::all, unreachable_pub)]
    grammar
);

pub use ast::ContractDocument;
pub use diagnostic::{SyntaxDiagnostic, SyntaxDiagnosticCode, SyntaxDiagnostics};
pub use migration::{
    Migration, MigrationAcknowledgement, MigrationAcknowledgementKind, MigrationDeclaration,
    MigrationDocument, MigrationEnumMap, MigrationEnumMapping, MigrationIdentityKind,
    MigrationRename, MigrationRetirement, MigrationTransform, MigrationTransformClause,
    format_migration, parse_migration, parse_migration_bytes,
};
pub use parser::{parse_contract, parse_contract_bytes};
pub use span::{Span, Spanned};
