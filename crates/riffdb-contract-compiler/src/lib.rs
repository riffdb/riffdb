#![forbid(unsafe_code)]

//! Total compiler from the source-oriented grammar-v1 AST to checked executable IR.

pub mod diagnostic;

mod bundle_lowering;
mod command_analysis;
mod command_lowering;
mod compiler;
mod expression_lowering;
mod hir;
mod literal;
mod locality;
mod mcp_name;
mod migration;
mod projection_lowering;
mod schema_lowering;
mod symbols;
mod typecheck;

pub use compiler::{
    CompilationError, compile_contract_migration_successor, compile_contract_source,
    compile_contract_successor, validate_contract_source,
};
pub use diagnostic::{
    CompilerDiagnostic, CompilerDiagnosticCode, CompilerDiagnostics, DiagnosticBoundsError,
};
pub use migration::compile_migration_source;
