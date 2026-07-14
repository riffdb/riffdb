//! Bounded, stable diagnostics produced after parsing.

use std::error::Error;
use std::fmt;

use riffdb_contract_syntax::Span;

/// Maximum number of semantic diagnostics returned by one compilation.
pub const MAX_COMPILER_DIAGNOSTICS: usize = 32;

/// Stable identities for grammar-v1 compiler failures.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CompilerDiagnosticCode {
    /// `RDB-C001`: the application contract version is not a supported nonzero value.
    InvalidContractVersion,
    /// `RDB-C002`: a name is declared more than once in one namespace.
    DuplicateName,
    /// `RDB-C003`: a required declaration or singleton item is missing.
    MissingDeclaration,
    /// `RDB-C004`: a referenced name cannot be resolved in its namespace.
    UnknownName,
    /// `RDB-C005`: a source type is invalid or unsupported in grammar version 1.
    InvalidType,
    /// `RDB-C006`: an expression or constructed field has the wrong exact type.
    TypeMismatch,
    /// `RDB-C007`: an expression is not valid in its semantic context.
    InvalidExpression,
    /// `RDB-C008`: aggregate ownership or root/child shape is invalid.
    InvalidAggregate,
    /// `RDB-C009`: an entity binding is invalid or has ambiguous ownership.
    InvalidBinding,
    /// `RDB-C010`: a mutating command lacks its one required idempotency declaration.
    MissingIdempotency,
    /// `RDB-C011`: the idempotency expression is invalid or leaks its secret input.
    InvalidIdempotency,
    /// `RDB-C012`: creation definite-assignment requirements are not met.
    InvalidCreation,
    /// `RDB-C013`: a mutation target is invalid or written more than once.
    InvalidMutation,
    /// `RDB-C014`: an outcome name or payload shape is inconsistent.
    InvalidOutcome,
    /// `RDB-C015`: an emitted event or payload shape is invalid.
    InvalidEvent,
    /// `RDB-C016`: a partition or conflict key is not derivable from validated inputs.
    ConflictNotInputComputable,
    /// `RDB-C017`: command bindings cannot be proved to use one logical partition.
    CrossPartitionMutation,
    /// `RDB-C019`: a projection operator, filter, key, or measure is unsupported.
    InvalidProjection,
    /// `RDB-C020`: a key, row, schema, or plan maximum exceeds a fixed bound.
    BoundExceeded,
    /// `RDB-C021`: stable lineage identifiers cannot be allocated compatibly.
    StableIdAllocation,
    /// `RDB-C022`: a supplied parent bundle is not a valid predecessor.
    InvalidParent,
    /// `RDB-C023`: checked IR construction rejected compiler output.
    InvalidIr,
    /// `RDB-C201`: an identifier cannot form an ADR-0020 command tool-name segment.
    InvalidCommandToolName,
    /// `RDB-C202`: a complete ADR-0020 command tool name exceeds 128 bytes.
    CommandToolNameTooLong,
    /// `RDB-C203`: two commands normalize to the same ADR-0020 tool name.
    CommandToolNameCollision,
}

impl CompilerDiagnosticCode {
    /// Complete pre-freeze public semantic diagnostic registry in code order.
    pub const ALL: [Self; 25] = [
        Self::InvalidContractVersion,
        Self::DuplicateName,
        Self::MissingDeclaration,
        Self::UnknownName,
        Self::InvalidType,
        Self::TypeMismatch,
        Self::InvalidExpression,
        Self::InvalidAggregate,
        Self::InvalidBinding,
        Self::MissingIdempotency,
        Self::InvalidIdempotency,
        Self::InvalidCreation,
        Self::InvalidMutation,
        Self::InvalidOutcome,
        Self::InvalidEvent,
        Self::ConflictNotInputComputable,
        Self::CrossPartitionMutation,
        Self::InvalidProjection,
        Self::BoundExceeded,
        Self::StableIdAllocation,
        Self::InvalidParent,
        Self::InvalidIr,
        Self::InvalidCommandToolName,
        Self::CommandToolNameTooLong,
        Self::CommandToolNameCollision,
    ];

    /// Returns the immutable external code.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidContractVersion => "RDB-C001",
            Self::DuplicateName => "RDB-C002",
            Self::MissingDeclaration => "RDB-C003",
            Self::UnknownName => "RDB-C004",
            Self::InvalidType => "RDB-C005",
            Self::TypeMismatch => "RDB-C006",
            Self::InvalidExpression => "RDB-C007",
            Self::InvalidAggregate => "RDB-C008",
            Self::InvalidBinding => "RDB-C009",
            Self::MissingIdempotency => "RDB-C010",
            Self::InvalidIdempotency => "RDB-C011",
            Self::InvalidCreation => "RDB-C012",
            Self::InvalidMutation => "RDB-C013",
            Self::InvalidOutcome => "RDB-C014",
            Self::InvalidEvent => "RDB-C015",
            Self::ConflictNotInputComputable => "RDB-C016",
            Self::CrossPartitionMutation => "RDB-C017",
            Self::InvalidProjection => "RDB-C019",
            Self::BoundExceeded => "RDB-C020",
            Self::StableIdAllocation => "RDB-C021",
            Self::InvalidParent => "RDB-C022",
            Self::InvalidIr => "RDB-C023",
            Self::InvalidCommandToolName => "RDB-C201",
            Self::CommandToolNameTooLong => "RDB-C202",
            Self::CommandToolNameCollision => "RDB-C203",
        }
    }

    /// Returns a concise caller-safe summary.
    #[must_use]
    pub const fn summary(self) -> &'static str {
        match self {
            Self::InvalidContractVersion => "contract version must be a supported nonzero integer",
            Self::DuplicateName => "a name is declared more than once in this namespace",
            Self::MissingDeclaration => "a required declaration or singleton item is missing",
            Self::UnknownName => "a referenced declaration, field, or binding is unknown",
            Self::InvalidType => "the declared type is invalid or unsupported",
            Self::TypeMismatch => "an expression does not have the required exact type",
            Self::InvalidExpression => "the expression is invalid in this context",
            Self::InvalidAggregate => "aggregate ownership or key shape is invalid",
            Self::InvalidBinding => "the command binding is invalid or ambiguously owned",
            Self::MissingIdempotency => "a mutating command must declare one idempotency key",
            Self::InvalidIdempotency => {
                "the idempotency expression is invalid or used outside its clause"
            }
            Self::InvalidCreation => "the create binding does not definitely initialize its record",
            Self::InvalidMutation => "the command mutation target is invalid",
            Self::InvalidOutcome => "an outcome name or payload shape is invalid",
            Self::InvalidEvent => "an event name or payload shape is invalid",
            Self::ConflictNotInputComputable => {
                "partition and conflict keys must be computable from validated inputs"
            }
            Self::CrossPartitionMutation => {
                "all command bindings must be statically colocated in one partition"
            }
            Self::InvalidProjection => "the projection uses an unsupported or invalid operation",
            Self::BoundExceeded => "a compiled artifact exceeds a fixed semantic bound",
            Self::StableIdAllocation => {
                "stable semantic identifiers cannot be allocated compatibly"
            }
            Self::InvalidParent => "the parent bundle is not a valid predecessor",
            Self::InvalidIr => "checked executable IR construction rejected the compiled plan",
            Self::InvalidCommandToolName => {
                "an identifier cannot form a valid MCP command tool-name segment"
            }
            Self::CommandToolNameTooLong => "the complete MCP command tool name exceeds 128 bytes",
            Self::CommandToolNameCollision => {
                "two commands normalize to the same MCP command tool name"
            }
        }
    }

    /// Returns static corrective guidance when one applies.
    #[must_use]
    pub const fn help(self) -> Option<&'static str> {
        match self {
            Self::InvalidContractVersion => {
                Some("use a base-10 application version in 1..=u64::MAX")
            }
            Self::DuplicateName => Some("rename or remove one declaration in the shared namespace"),
            Self::MissingDeclaration => Some("add the required grammar-version-1 declaration"),
            Self::UnknownName => Some("reference an exact case-sensitive declared name"),
            Self::InvalidType => Some("use a bounded grammar-version-1 value type"),
            Self::TypeMismatch => Some("make both sides use the same complete static type"),
            Self::InvalidExpression => {
                Some("use an expression allowed by this declaration context")
            }
            Self::InvalidAggregate => {
                Some("declare one root and the required root-key prefix ownership")
            }
            Self::InvalidBinding => Some("bind an entity owned by the command's one aggregate"),
            Self::MissingIdempotency => {
                Some("declare a direct bounded string input as idempotency_key")
            }
            Self::InvalidIdempotency => {
                Some("use one required string<1..=128> input only in the idempotency clause")
            }
            Self::InvalidCreation => {
                Some("assign every required non-key field exactly once before return")
            }
            Self::InvalidMutation => {
                Some("write one declared non-key field through a mutable binding")
            }
            Self::InvalidOutcome => {
                Some("use one consistent typed payload for each declared outcome name")
            }
            Self::InvalidEvent => Some("construct every declared event field with its exact type"),
            Self::ConflictNotInputComputable => {
                Some("derive aggregate keys only from root-key inputs and constants")
            }
            Self::CrossPartitionMutation => {
                Some("make all bindings use the same structural partition derivation")
            }
            Self::InvalidProjection => {
                Some("use equality/conjunction filters and bounded count or sum aggregation")
            }
            Self::BoundExceeded => {
                Some("reduce declared bounds or the number of schema components")
            }
            Self::StableIdAllocation => {
                Some("preserve lineage identities and do not reuse removed identifiers")
            }
            Self::InvalidParent => Some("compile against the exact validated predecessor bundle"),
            Self::InvalidIr => None,
            Self::InvalidCommandToolName => {
                Some("start contract and command identifiers with an ASCII letter")
            }
            Self::CommandToolNameTooLong => {
                Some("shorten the source contract or command identifier")
            }
            Self::CommandToolNameCollision => {
                Some("rename one command so lowercase identifiers remain distinct")
            }
        }
    }
}

/// One semantic compiler failure with an exact primary source span.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompilerDiagnostic {
    code: CompilerDiagnosticCode,
    primary_span: Span,
    related_span: Option<Span>,
}

impl CompilerDiagnostic {
    /// Constructs a diagnostic with one primary source span.
    #[must_use]
    pub const fn new(code: CompilerDiagnosticCode, primary_span: Span) -> Self {
        Self {
            code,
            primary_span,
            related_span: None,
        }
    }

    /// Adds one deterministic related source span, such as the first colliding declaration.
    #[must_use]
    pub const fn with_related_span(mut self, related_span: Span) -> Self {
        self.related_span = Some(related_span);
        self
    }

    /// Returns the stable diagnostic identity.
    #[must_use]
    pub const fn code(&self) -> CompilerDiagnosticCode {
        self.code
    }

    /// Returns the primary half-open source span.
    #[must_use]
    pub const fn primary_span(&self) -> Span {
        self.primary_span
    }

    /// Returns the optional related half-open source span.
    #[must_use]
    pub const fn related_span(&self) -> Option<Span> {
        self.related_span
    }
}

impl fmt::Display for CompilerDiagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code.as_str(), self.code.summary())
    }
}

impl Error for CompilerDiagnostic {}

/// A nonempty, bounded, deterministically ordered semantic diagnostic collection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompilerDiagnostics(Vec<CompilerDiagnostic>);

impl CompilerDiagnostics {
    /// Validates and deterministically orders a nonempty diagnostic collection.
    pub fn new(mut diagnostics: Vec<CompilerDiagnostic>) -> Result<Self, DiagnosticBoundsError> {
        if diagnostics.is_empty() {
            return Err(DiagnosticBoundsError::Empty);
        }
        diagnostics.sort_by_key(|diagnostic| {
            (
                diagnostic.primary_span.start(),
                diagnostic.primary_span.end(),
                diagnostic.code,
                diagnostic.related_span,
            )
        });
        diagnostics.dedup();
        if diagnostics.len() > MAX_COMPILER_DIAGNOSTICS {
            diagnostics.truncate(MAX_COMPILER_DIAGNOSTICS);
        }
        Ok(Self(diagnostics))
    }

    /// Constructs a collection containing one diagnostic.
    #[must_use]
    pub fn single(diagnostic: CompilerDiagnostic) -> Self {
        Self(vec![diagnostic])
    }

    /// Returns diagnostics in deterministic source order.
    #[must_use]
    pub fn as_slice(&self) -> &[CompilerDiagnostic] {
        &self.0
    }

    /// Returns the number of diagnostics.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `false`; the type cannot contain an empty collection.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        false
    }
}

impl fmt::Display for CompilerDiagnostics {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0[0].fmt(formatter)
    }
}

impl Error for CompilerDiagnostics {}

/// Invalid construction of a semantic diagnostic collection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticBoundsError {
    /// A semantic diagnostic collection must not be empty.
    Empty,
}

impl fmt::Display for DiagnosticBoundsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("compiler diagnostic collection must be nonempty")
    }
}

impl Error for DiagnosticBoundsError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostics_are_bounded_deduplicated_and_source_ordered() {
        let late = CompilerDiagnostic::new(
            CompilerDiagnosticCode::UnknownName,
            Span::new(20, 25).expect("valid span"),
        );
        let early = CompilerDiagnostic::new(
            CompilerDiagnosticCode::DuplicateName,
            Span::new(3, 8).expect("valid span"),
        );
        let diagnostics = CompilerDiagnostics::new(vec![late.clone(), early.clone(), late.clone()])
            .expect("valid diagnostics");
        assert_eq!(diagnostics.as_slice(), &[early, late]);
    }

    #[test]
    fn external_messages_are_static_and_related_spans_are_preserved() {
        let primary = Span::new(10, 20).expect("valid primary span");
        let related = Span::new(1, 9).expect("valid related span");
        let diagnostic =
            CompilerDiagnostic::new(CompilerDiagnosticCode::CommandToolNameCollision, primary)
                .with_related_span(related);
        assert_eq!(
            diagnostic.to_string(),
            "RDB-C203: two commands normalize to the same MCP command tool name"
        );
        assert_eq!(diagnostic.related_span(), Some(related));
    }

    #[test]
    fn public_diagnostic_registry_is_complete_unique_and_code_ordered() {
        assert_eq!(CompilerDiagnosticCode::ALL.len(), 25);
        let codes = CompilerDiagnosticCode::ALL.map(CompilerDiagnosticCode::as_str);
        assert!(codes.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(CompilerDiagnosticCode::ALL.iter().all(|code| {
            !code.summary().is_empty() && code.help().is_none_or(|help| !help.is_empty())
        }));
    }

    #[test]
    fn checked_diagnostic_fixture_covers_every_retained_code_once_with_spans() {
        let fixture = include_str!("../../../fixtures/compiler/diagnostics.txt");
        let mut covered = Vec::new();
        for line in fixture.lines() {
            let Some(code) = line.strip_prefix("expected=") else {
                continue;
            };
            covered.push(
                CompilerDiagnosticCode::ALL
                    .iter()
                    .copied()
                    .find(|candidate| candidate.as_str() == code)
                    .expect("fixture code is retained"),
            );
        }
        assert_eq!(covered, CompilerDiagnosticCode::ALL);
        for section in fixture.split("\n[").skip(1) {
            if section.starts_with("RDB-C") {
                assert!(section.contains("\nkind=semantic\n"));
                assert!(section.contains(".primary="));
                assert!(section.contains(".related="));
            }
        }
    }
}
