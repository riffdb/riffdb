use riffdb_riffql_syntax::Span;

/// Stable query resolution/schema diagnostic code.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryDiagnosticCode {
    /// A contract, binding, field, enum, variant, or type name is absent.
    UnknownSymbol,
    /// A path has more than one legal symbolic interpretation.
    AmbiguousSymbol,
    /// A query-local name is declared more than once.
    DuplicateName,
    /// A parameter type is not legal in the query surface.
    InvalidType,
    /// A path cannot be used in its source context.
    InvalidPath,
    /// A bounded schema, map, or artifact ceiling was exceeded.
    ArtifactLimit,
    /// An exact secret output declaration is absent, duplicated, or invalid.
    SecretOutputDeclaration,
}

impl QueryDiagnosticCode {
    /// Stable public diagnostic code.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UnknownSymbol => "RDB-QR001",
            Self::AmbiguousSymbol => "RDB-QR002",
            Self::DuplicateName => "RDB-QR003",
            Self::InvalidType => "RDB-QR004",
            Self::InvalidPath => "RDB-QR005",
            Self::ArtifactLimit => "RDB-QR006",
            Self::SecretOutputDeclaration => "RDB-QR007",
        }
    }
}

/// Closed compiler stage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryDiagnosticStage {
    /// Exact-contract name resolution.
    Resolution,
    /// Name-addressed schema construction.
    Schema,
}

/// One bounded, value-free, source-spanned query diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryDiagnostic {
    code: QueryDiagnosticCode,
    stage: QueryDiagnosticStage,
    primary: Span,
    symbol_path: Vec<String>,
    summary: &'static str,
    help: Option<&'static str>,
}

impl QueryDiagnostic {
    pub(crate) fn new(
        code: QueryDiagnosticCode,
        stage: QueryDiagnosticStage,
        primary: Span,
        symbol_path: Vec<String>,
        summary: &'static str,
        help: Option<&'static str>,
    ) -> Self {
        Self {
            code,
            stage,
            primary,
            symbol_path,
            summary,
            help,
        }
    }

    /// Stable diagnostic code.
    #[must_use]
    pub const fn code(&self) -> QueryDiagnosticCode {
        self.code
    }

    /// Compiler stage.
    #[must_use]
    pub const fn stage(&self) -> QueryDiagnosticStage {
        self.stage
    }

    /// Primary half-open source span.
    #[must_use]
    pub const fn primary(&self) -> Span {
        self.primary
    }

    /// Safe bounded contract/query symbol path.
    #[must_use]
    pub fn symbol_path(&self) -> &[String] {
        &self.symbol_path
    }

    /// Static summary.
    #[must_use]
    pub const fn summary(&self) -> &'static str {
        self.summary
    }

    /// Optional static remediation.
    #[must_use]
    pub const fn help(&self) -> Option<&'static str> {
        self.help
    }
}

/// Bounded diagnostics returned without partial IR.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryDiagnostics(Vec<QueryDiagnostic>);

impl QueryDiagnostics {
    pub(crate) fn one(diagnostic: QueryDiagnostic) -> Self {
        Self(vec![diagnostic])
    }

    /// Diagnostics in deterministic source order.
    #[must_use]
    pub fn as_slice(&self) -> &[QueryDiagnostic] {
        &self.0
    }
}
