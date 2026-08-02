//! Stable, value-free reactive-source diagnostics.

use std::fmt;

/// Half-open byte span in caller-owned reactive source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Span {
    start: usize,
    end: usize,
}

impl Span {
    pub(crate) const fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }

    /// Inclusive start byte.
    #[must_use]
    pub const fn start(self) -> usize {
        self.start
    }

    /// Exclusive end byte.
    #[must_use]
    pub const fn end(self) -> usize {
        self.end
    }
}

/// Closed grammar-v1 diagnostic registry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticCode {
    /// Source bytes or lexer tokens exceed a hard ceiling.
    LimitExceeded,
    /// A source byte cannot begin a grammar-v1 token.
    InvalidCharacter,
    /// The next token does not match the closed grammar.
    UnexpectedToken,
    /// Only grammar version one is accepted.
    UnsupportedVersion,
    /// A declaration repeats a name in the same scope.
    DuplicateName,
    /// A string or integer literal is malformed or out of range.
    InvalidLiteral,
    /// A syntactically recognizable form is outside grammar v1.
    UnsupportedForm,
}

impl DiagnosticCode {
    /// Stable public code.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LimitExceeded => "RDB-RS001",
            Self::InvalidCharacter => "RDB-RS002",
            Self::UnexpectedToken => "RDB-RS003",
            Self::UnsupportedVersion => "RDB-RS004",
            Self::DuplicateName => "RDB-RS005",
            Self::InvalidLiteral => "RDB-RS006",
            Self::UnsupportedForm => "RDB-RS007",
        }
    }
}

/// One bounded diagnostic with a source span and no source literal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Diagnostic {
    code: DiagnosticCode,
    span: Span,
}

impl Diagnostic {
    pub(crate) const fn new(code: DiagnosticCode, span: Span) -> Self {
        Self { code, span }
    }

    /// Stable failure classification.
    #[must_use]
    pub const fn code(self) -> DiagnosticCode {
        self.code
    }

    /// Exact caller-source span.
    #[must_use]
    pub const fn span(self) -> Span {
        self.span
    }
}

impl fmt::Display for Diagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code.as_str())
    }
}

impl std::error::Error for Diagnostic {}
