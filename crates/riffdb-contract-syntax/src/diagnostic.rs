//! Bounded, stable, source-spanned parser diagnostics.

use std::error::Error;
use std::fmt;

use crate::limits::{MAX_EXPECTED_TOKENS, MAX_SYNTAX_DIAGNOSTICS};
use crate::span::Span;

/// Stable syntax diagnostic identities for grammar version 1.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SyntaxDiagnosticCode {
    /// `RDB-S001`: source byte limit exceeded.
    SourceLimit,
    /// `RDB-S002`: token or syntax-node limit exceeded.
    NodeLimit,
    /// `RDB-S003`: invalid token, escape, or literal.
    InvalidToken,
    /// `RDB-S004`: an unexpected token was encountered.
    UnexpectedToken,
    /// `RDB-S005`: source ended before the active production completed.
    UnexpectedEnd,
    /// `RDB-S006`: delimiter or expression nesting is too deep.
    NestingLimit,
    /// `RDB-S007`: a declaration or collection has too many entries.
    CollectionLimit,
    /// `RDB-S008`: recognized syntax is deferred or unsupported.
    UnsupportedSyntax,
}

impl SyntaxDiagnosticCode {
    /// Returns the immutable external code.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SourceLimit => "RDB-S001",
            Self::NodeLimit => "RDB-S002",
            Self::InvalidToken => "RDB-S003",
            Self::UnexpectedToken => "RDB-S004",
            Self::UnexpectedEnd => "RDB-S005",
            Self::NestingLimit => "RDB-S006",
            Self::CollectionLimit => "RDB-S007",
            Self::UnsupportedSyntax => "RDB-S008",
        }
    }

    /// Returns the static caller-safe summary.
    #[must_use]
    pub const fn summary(self) -> &'static str {
        match self {
            Self::SourceLimit => "contract source exceeds the byte limit",
            Self::NodeLimit => "contract syntax exceeds the parser node limit",
            Self::InvalidToken => "contract source contains an invalid token or literal",
            Self::UnexpectedToken => "contract source contains an unexpected token",
            Self::UnexpectedEnd => "contract source ended before the declaration was complete",
            Self::NestingLimit => "contract syntax exceeds the nesting limit",
            Self::CollectionLimit => "contract declaration contains too many items",
            Self::UnsupportedSyntax => "contract source uses unsupported grammar syntax",
        }
    }

    /// Returns static corrective guidance when one applies.
    #[must_use]
    pub const fn help(self) -> Option<&'static str> {
        match self {
            Self::SourceLimit | Self::NodeLimit | Self::CollectionLimit => {
                Some("reduce the contract source to the documented grammar-version-1 bounds")
            }
            Self::InvalidToken | Self::UnexpectedToken | Self::UnexpectedEnd => {
                Some("use the grammar-version-1 spelling shown in the language reference")
            }
            Self::NestingLimit => Some("simplify nested types, expressions, or objects"),
            Self::UnsupportedSyntax => {
                Some("remove the deferred construct or use the bounded public query/policy API")
            }
        }
    }
}

/// One stable parser failure without caller-controlled diagnostic text.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyntaxDiagnostic {
    code: SyntaxDiagnosticCode,
    span: Span,
    expected: Vec<&'static str>,
}

impl SyntaxDiagnostic {
    /// Constructs a diagnostic without expected-token alternatives.
    #[must_use]
    pub const fn new(code: SyntaxDiagnosticCode, span: Span) -> Self {
        Self {
            code,
            span,
            expected: Vec::new(),
        }
    }

    /// Constructs a diagnostic with bounded, static expected-token names.
    pub fn with_expected(
        code: SyntaxDiagnosticCode,
        span: Span,
        expected: Vec<&'static str>,
    ) -> Result<Self, DiagnosticBoundsError> {
        if expected.len() > MAX_EXPECTED_TOKENS {
            return Err(DiagnosticBoundsError::TooManyExpectedTokens);
        }
        Ok(Self {
            code,
            span,
            expected,
        })
    }

    /// Returns the stable diagnostic identity.
    #[must_use]
    pub const fn code(&self) -> SyntaxDiagnosticCode {
        self.code
    }

    /// Returns the primary source span.
    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }

    /// Returns bounded, static expected-token names.
    #[must_use]
    pub fn expected(&self) -> &[&'static str] {
        &self.expected
    }
}

impl fmt::Display for SyntaxDiagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code.as_str(), self.code.summary())
    }
}

impl Error for SyntaxDiagnostic {}

/// A nonempty bounded parser diagnostic collection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyntaxDiagnostics(Vec<SyntaxDiagnostic>);

impl SyntaxDiagnostics {
    /// Validates a nonempty bounded diagnostic collection.
    pub fn new(diagnostics: Vec<SyntaxDiagnostic>) -> Result<Self, DiagnosticBoundsError> {
        if diagnostics.is_empty() {
            return Err(DiagnosticBoundsError::Empty);
        }
        if diagnostics.len() > MAX_SYNTAX_DIAGNOSTICS {
            return Err(DiagnosticBoundsError::TooManyDiagnostics);
        }
        Ok(Self(diagnostics))
    }

    /// Constructs a collection containing one diagnostic.
    #[must_use]
    pub fn single(diagnostic: SyntaxDiagnostic) -> Self {
        Self(vec![diagnostic])
    }

    /// Returns diagnostics in deterministic source order.
    #[must_use]
    pub fn as_slice(&self) -> &[SyntaxDiagnostic] {
        &self.0
    }

    /// Returns the number of diagnostics in this nonempty collection.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `false`; the type cannot contain an empty collection.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        false
    }

    /// Consumes the wrapper and returns its bounded diagnostics.
    #[must_use]
    pub fn into_vec(self) -> Vec<SyntaxDiagnostic> {
        self.0
    }
}

impl fmt::Display for SyntaxDiagnostics {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0[0].fmt(formatter)
    }
}

impl Error for SyntaxDiagnostics {}

/// Failure to construct an approved bounded diagnostic value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticBoundsError {
    /// A diagnostic collection must not be empty.
    Empty,
    /// More than 32 diagnostics were supplied.
    TooManyDiagnostics,
    /// More than 16 expected-token alternatives were supplied.
    TooManyExpectedTokens,
}

impl fmt::Display for DiagnosticBoundsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("syntax diagnostic bounds were exceeded")
    }
}

impl Error for DiagnosticBoundsError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_and_messages_are_closed_and_safe() {
        let span = Span::new(1, 2).expect("valid span");
        let diagnostic = SyntaxDiagnostic::new(SyntaxDiagnosticCode::UnexpectedToken, span);
        assert_eq!(
            diagnostic.to_string(),
            "RDB-S004: contract source contains an unexpected token"
        );
        assert_eq!(diagnostic.span(), span);
    }

    #[test]
    fn constructors_enforce_diagnostic_bounds() {
        let span = Span::ZERO;
        let too_many_expected = vec!["identifier"; MAX_EXPECTED_TOKENS + 1];
        assert_eq!(
            SyntaxDiagnostic::with_expected(
                SyntaxDiagnosticCode::UnexpectedToken,
                span,
                too_many_expected,
            ),
            Err(DiagnosticBoundsError::TooManyExpectedTokens)
        );
        assert_eq!(
            SyntaxDiagnostics::new(Vec::new()),
            Err(DiagnosticBoundsError::Empty)
        );

        let too_many_diagnostics =
            vec![
                SyntaxDiagnostic::new(SyntaxDiagnosticCode::InvalidToken, span);
                MAX_SYNTAX_DIAGNOSTICS + 1
            ];
        assert_eq!(
            SyntaxDiagnostics::new(too_many_diagnostics),
            Err(DiagnosticBoundsError::TooManyDiagnostics)
        );
    }
}
