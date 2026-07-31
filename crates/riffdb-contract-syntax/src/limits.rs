//! Immutable parser safety bounds for grammar version 1.

/// Maximum accepted source size in UTF-8 bytes.
pub const MAX_SOURCE_BYTES: usize = 1_048_576;
/// Maximum accepted migration-source size in UTF-8 bytes.
pub const MAX_MIGRATION_SOURCE_BYTES: usize = 1_048_576;
/// Maximum identifier length in ASCII bytes.
pub const MAX_IDENTIFIER_BYTES: usize = 256;
/// Maximum delimiter and expression nesting depth.
pub const MAX_NESTING_DEPTH: usize = 32;
/// Maximum number of non-trivia tokens.
pub const MAX_TOKENS: usize = 131_072;
/// Maximum total number of nodes in one syntax tree.
///
/// A node is one `Spanned` wrapper or one public AST struct or enum value.
/// Strings, integers, `Vec`, `Option`, and `Box` containers are not nodes.
pub const MAX_AST_NODES: usize = 131_072;
/// Maximum top-level declarations or items in one declaration.
pub const MAX_DECLARATION_ITEMS: usize = 4_096;
/// Maximum entries in an argument, tuple, variant, or object-field list.
pub const MAX_LIST_ITEMS: usize = 1_024;
/// Maximum diagnostics returned by one parse.
pub const MAX_SYNTAX_DIAGNOSTICS: usize = 32;
/// Maximum expected-token alternatives exposed by one diagnostic.
pub const MAX_EXPECTED_TOKENS: usize = 16;
