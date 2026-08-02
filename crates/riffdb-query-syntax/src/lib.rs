#![forbid(unsafe_code)]

//! Bounded grammar-v1 syntax for RiffDB reactive modules.

mod diagnostic;
mod formatter;
mod parser;
mod syntax;

pub use diagnostic::{Diagnostic, DiagnosticCode, Span};
pub use formatter::format_module;
pub use parser::parse_module;
pub use syntax::{
    Argument, BinaryOperator, EventSelection, Expression, Hydration, Limits, Literal, Module,
    Operand, Parameter, PartitionBinding, Reaction, Stream, StreamBinding, Subscription,
    UpdateMode, Watch,
};

/// Reactive grammar version implemented by this crate.
pub const REACTIVE_GRAMMAR_VERSION_V1: u32 = 1;
/// Maximum accepted UTF-8 source size.
pub const MAX_REACTIVE_SOURCE_BYTES: usize = 1_048_576;
/// Maximum lexer token count.
pub const MAX_REACTIVE_TOKENS: usize = 131_072;
/// Maximum definitions in one module.
pub const MAX_REACTIVE_DEFINITIONS: usize = 1_024;
/// Maximum parameters or argument bindings on one operation.
pub const MAX_REACTIVE_PARAMETERS: usize = 256;
/// Maximum event types in one stream.
pub const MAX_STREAM_EVENT_TYPES: usize = 32;
/// Maximum selected event fields in one stream.
pub const MAX_STREAM_SELECTED_FIELDS: usize = 256;
/// Maximum predicate expression nodes.
pub const MAX_PREDICATE_NODES: usize = 4_096;
/// Maximum hydration queries in one contextual subscription.
pub const MAX_SUBSCRIPTION_HYDRATIONS: usize = 16;
/// Maximum named reactions in one contextual subscription.
pub const MAX_SUBSCRIPTION_REACTIONS: usize = 32;
/// Maximum parser nesting depth.
pub const MAX_REACTIVE_NESTING: usize = 32;
