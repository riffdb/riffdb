use crate::Span;

/// Stable RiffQL syntax diagnostic code.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticCode {
    /// Source byte ceiling exceeded.
    SourceTooLong,
    /// Token or AST node ceiling exceeded.
    TooManySyntaxItems,
    /// Invalid token, string, or literal.
    InvalidToken,
    /// Unexpected token.
    UnexpectedToken,
    /// Unexpected end of input.
    UnexpectedEnd,
    /// Nesting ceiling exceeded.
    NestingTooDeep,
    /// Collection or binding ceiling exceeded.
    TooManyItems,
    /// Mutation, SQL, recursion, or another deferred form was recognized.
    UnsupportedForm,
    /// A `many` binding omitted its positive explicit `take` clause.
    UnboundedMany,
}

impl DiagnosticCode {
    /// Stable external code.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SourceTooLong => "RDB-QS001",
            Self::TooManySyntaxItems => "RDB-QS002",
            Self::InvalidToken => "RDB-QS003",
            Self::UnexpectedToken => "RDB-QS004",
            Self::UnexpectedEnd => "RDB-QS005",
            Self::NestingTooDeep => "RDB-QS006",
            Self::TooManyItems => "RDB-QS007",
            Self::UnsupportedForm => "RDB-QS008",
            Self::UnboundedMany => "RDB-QS009",
        }
    }
}

/// One bounded value-free syntax diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParseDiagnostic {
    code: DiagnosticCode,
    span: Span,
    summary: &'static str,
    help: Option<&'static str>,
}

impl ParseDiagnostic {
    pub(crate) const fn new(
        code: DiagnosticCode,
        span: Span,
        summary: &'static str,
        help: Option<&'static str>,
    ) -> Self {
        Self {
            code,
            span,
            summary,
            help,
        }
    }

    /// Returns the stable diagnostic code.
    #[must_use]
    pub const fn code(&self) -> DiagnosticCode {
        self.code
    }

    /// Returns the primary half-open UTF-8 byte span.
    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }

    /// Returns the static value-free summary.
    #[must_use]
    pub const fn summary(&self) -> &'static str {
        self.summary
    }

    /// Returns optional static remediation.
    #[must_use]
    pub const fn help(&self) -> Option<&'static str> {
        self.help
    }
}

/// Bounded diagnostics returned instead of a partial AST.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParseDiagnostics(Vec<ParseDiagnostic>);

impl ParseDiagnostics {
    pub(crate) fn one(diagnostic: ParseDiagnostic) -> Self {
        Self(vec![diagnostic])
    }

    /// Returns diagnostics in source order.
    #[must_use]
    pub fn as_slice(&self) -> &[ParseDiagnostic] {
        &self.0
    }
}
