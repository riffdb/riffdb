//! Closed, bounded, value-free diagnostics for local application authoring.

use std::fmt::{self, Write as _};

use riffdb_contract_compiler::CompilationError;
use riffdb_query_module::{
    ApplicationLockErrorKind, ApplicationRoleErrorKind, ApplicationSourceErrorKind,
    ManifestErrorKind, QueryCompilationDiagnostics, QueryModuleError, QueryModuleErrorKind,
};
use serde_json::{Value, json};

/// Version of the machine-readable authoring diagnostic shape.
pub const AUTHORING_DIAGNOSTIC_VERSION_V1: u32 = 1;
/// Maximum diagnostics returned by one authoring operation.
pub const MAX_AUTHORING_DIAGNOSTICS: usize = 32;
/// Maximum bytes in either complete human or JSON rendering.
pub const MAX_AUTHORING_DIAGNOSTIC_BYTES: usize = 65_536;
const MAX_SOURCE_PATH_BYTES: usize = 512;
const MAX_SYMBOL_PATH_COMPONENTS: usize = 16;
const MAX_SYMBOL_COMPONENT_BYTES: usize = 256;
const MAX_SUMMARY_BYTES: usize = 1_024;

/// Closed authoring pipeline stage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthoringStage {
    /// Symbolic application-source parsing or validation.
    ApplicationSource,
    /// Contract parser.
    ContractSyntax,
    /// Contract semantic compiler.
    ContractSemantic,
    /// RiffQL parser.
    QuerySyntax,
    /// RiffQL resolver/type checker/planner.
    QueryPlan,
    /// Exact V1 application manifest.
    Manifest,
    /// Symbolic role compilation.
    Role,
    /// Exact compiler-owned application lock.
    Lock,
    /// Generated binding or schema rendering.
    Generation,
    /// Local filesystem boundary.
    Filesystem,
    /// Repository scaffold boundary.
    Scaffold,
}

impl AuthoringStage {
    /// Stable machine spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ApplicationSource => "application_source",
            Self::ContractSyntax => "contract_syntax",
            Self::ContractSemantic => "contract_semantic",
            Self::QuerySyntax => "query_syntax",
            Self::QueryPlan => "query_plan",
            Self::Manifest => "manifest",
            Self::Role => "role",
            Self::Lock => "lock",
            Self::Generation => "generation",
            Self::Filesystem => "filesystem",
            Self::Scaffold => "scaffold",
        }
    }
}

/// One registry-owned stable diagnostic code.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuthoringDiagnosticCode(&'static str);

impl AuthoringDiagnosticCode {
    /// Stable public code.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

/// Closed semantic cause classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthoringCause {
    /// Source shape or syntax is invalid.
    InvalidSyntax,
    /// A symbolic name cannot be resolved.
    UnknownSymbol,
    /// Static types disagree.
    TypeMismatch,
    /// The operation cannot prove one local route.
    NonLocal,
    /// A relationship change cannot be tied to one dominating exact target read.
    MissingRelationshipProof,
    /// A required bounded access path is absent.
    MissingIndex,
    /// Cardinality or total work cannot be bounded.
    Unbounded,
    /// An exact compiled identity is stale or substituted.
    IdentityDrift,
    /// Symbolic role authority cannot be derived safely.
    UnsafeRole,
    /// A hard input, output, or collection bound is exceeded.
    LimitExceeded,
    /// A path is unsafe or not an expected regular workspace file.
    UnsafePath,
    /// Local I/O failed under a closed filesystem class.
    FilesystemFailure,
    /// Generated output cannot be reproduced safely.
    GenerationFailure,
    /// Compiler-owned checked state is inconsistent.
    InternalInvariant,
}

impl AuthoringCause {
    /// Stable machine spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidSyntax => "invalid_syntax",
            Self::UnknownSymbol => "unknown_symbol",
            Self::TypeMismatch => "type_mismatch",
            Self::NonLocal => "non_local",
            Self::MissingRelationshipProof => "missing_relationship_proof",
            Self::MissingIndex => "missing_index",
            Self::Unbounded => "unbounded",
            Self::IdentityDrift => "identity_drift",
            Self::UnsafeRole => "unsafe_role",
            Self::LimitExceeded => "limit_exceeded",
            Self::UnsafePath => "unsafe_path",
            Self::FilesystemFailure => "filesystem_failure",
            Self::GenerationFailure => "generation_failure",
            Self::InternalInvariant => "internal_invariant",
        }
    }
}

/// Closed corrective action. No caller prose enters this registry.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum AuthoringFix {
    /// Correct syntax using the public language reference.
    UseLanguageReference,
    /// Correct or declare the named symbol.
    CorrectSymbol,
    /// Make the declared and required types agree.
    CorrectType,
    /// Supply the complete partition route.
    SupplyPartitionRoute,
    /// Model every atomic create/mutate binding under one aggregate root.
    ModelOneMutationAggregate,
    /// Read the exact relationship target and reuse its key input expressions.
    ProveRelationshipTarget,
    /// Add the compiler-suggested bounded index.
    AddIndex,
    /// Add an explicit positive bound.
    AddBound,
    /// Run the explicit lock update after review.
    WriteLock,
    /// Restore outputs with exact locked generation.
    GenerateLocked,
    /// Narrow or correct the symbolic role.
    NarrowRole,
    /// Use a regular workspace-relative path.
    CorrectPath,
    /// Reduce input to the documented bound.
    ReduceInput,
    /// A closed member requires at least one entry that the source omits.
    SupplyRequiredEntry,
    /// Retry after correcting local permissions or availability.
    CorrectFilesystem,
    /// Contact the operator with the local incident context.
    ContactOperator,
}

impl AuthoringFix {
    /// Stable machine spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UseLanguageReference => "use_language_reference",
            Self::CorrectSymbol => "correct_symbol",
            Self::CorrectType => "correct_type",
            Self::SupplyPartitionRoute => "supply_partition_route",
            Self::ModelOneMutationAggregate => "model_one_mutation_aggregate",
            Self::ProveRelationshipTarget => "prove_relationship_target",
            Self::AddIndex => "add_index",
            Self::AddBound => "add_bound",
            Self::WriteLock => "write_lock",
            Self::GenerateLocked => "generate_locked",
            Self::NarrowRole => "narrow_role",
            Self::CorrectPath => "correct_path",
            Self::ReduceInput => "reduce_input",
            Self::SupplyRequiredEntry => "supply_required_entry",
            Self::CorrectFilesystem => "correct_filesystem",
            Self::ContactOperator => "contact_operator",
        }
    }
}

/// Closed statement about local file effects.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileChangeDisposition {
    /// The operation is read-only and changed no file.
    NoFilesChanged,
    /// Staged files were discarded before publication.
    StagedFilesDiscarded,
    /// The previous accepted generation remains the only usable generation.
    PreviousGenerationRetained,
    /// Generated files may be partial but the exact lock was not published.
    LockNotPublished,
}

impl FileChangeDisposition {
    /// Stable machine spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NoFilesChanged => "no_files_changed",
            Self::StagedFilesDiscarded => "staged_files_discarded",
            Self::PreviousGenerationRetained => "previous_generation_retained",
            Self::LockNotPublished => "lock_not_published",
        }
    }
}

/// Closed retry classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthoringRetry {
    /// Correct symbolic source, then re-run the same operation.
    CorrectSource,
    /// Explicitly review and write a replacement lock.
    ReviewAndWriteLock,
    /// Retry unchanged inputs after local I/O availability is restored.
    RetrySameInputs,
    /// The caller cannot safely retry without operator investigation.
    ContactOperator,
}

impl AuthoringRetry {
    /// Stable machine spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CorrectSource => "correct_source",
            Self::ReviewAndWriteLock => "review_and_write_lock",
            Self::RetrySameInputs => "retry_same_inputs",
            Self::ContactOperator => "contact_operator",
        }
    }
}

/// Checked workspace-relative source pointer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthoringSourcePath(String);

impl AuthoringSourcePath {
    /// Checks one local source path without touching the filesystem.
    pub fn new(path: impl Into<String>) -> Result<Self, AuthoringDiagnosticBoundsError> {
        let path = path.into();
        if path.is_empty()
            || path.len() > MAX_SOURCE_PATH_BYTES
            || path.starts_with('/')
            || path.starts_with('\\')
            || path.contains('\\')
            || path.split('/').any(|part| {
                part.is_empty()
                    || part == "."
                    || part == ".."
                    || part.bytes().any(|byte| byte.is_ascii_control())
            })
        {
            return Err(AuthoringDiagnosticBoundsError);
        }
        Ok(Self(path))
    }

    /// Checked path text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Half-open UTF-8 byte range in caller-owned local source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuthoringSourceSpan {
    start: u32,
    end: u32,
}

impl AuthoringSourceSpan {
    /// Checks an ordered half-open span.
    #[must_use]
    pub const fn new(start: u32, end: u32) -> Option<Self> {
        if start <= end {
            Some(Self { start, end })
        } else {
            None
        }
    }

    /// Inclusive start byte.
    #[must_use]
    pub const fn start(self) -> u32 {
        self.start
    }

    /// Exclusive end byte.
    #[must_use]
    pub const fn end(self) -> u32 {
        self.end
    }
}

/// One complete value-free authoring diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthoringDiagnostic {
    stage: AuthoringStage,
    code: AuthoringDiagnosticCode,
    path: AuthoringSourcePath,
    span: Option<AuthoringSourceSpan>,
    symbol_path: Vec<String>,
    summary: String,
    help: Option<String>,
    cause: AuthoringCause,
    fixes: Vec<AuthoringFix>,
    file_change: FileChangeDisposition,
    retry: AuthoringRetry,
}

impl AuthoringDiagnostic {
    /// Authoring stage.
    #[must_use]
    pub const fn stage(&self) -> AuthoringStage {
        self.stage
    }

    /// Stable diagnostic code.
    #[must_use]
    pub const fn code(&self) -> AuthoringDiagnosticCode {
        self.code
    }

    /// Local source pointer.
    #[must_use]
    pub const fn path(&self) -> &AuthoringSourcePath {
        &self.path
    }

    /// Optional exact source span.
    #[must_use]
    pub const fn span(&self) -> Option<AuthoringSourceSpan> {
        self.span
    }

    /// Bounded symbolic path.
    #[must_use]
    pub fn symbol_path(&self) -> &[String] {
        &self.symbol_path
    }

    /// Bounded value-free summary.
    #[must_use]
    pub fn summary(&self) -> &str {
        &self.summary
    }

    /// Static corrective guidance, when the producing stage supplies one.
    #[must_use]
    pub fn help(&self) -> Option<&str> {
        self.help.as_deref()
    }

    /// Attaches static corrective guidance produced by the compiler.
    #[must_use]
    fn with_help(mut self, help: Option<&str>) -> Self {
        self.help = help.map(str::to_owned);
        self
    }

    /// Closed cause.
    #[must_use]
    pub const fn cause(&self) -> AuthoringCause {
        self.cause
    }

    /// Closed corrective actions.
    #[must_use]
    pub fn fixes(&self) -> &[AuthoringFix] {
        &self.fixes
    }

    /// Local file-change disposition.
    #[must_use]
    pub const fn file_change(&self) -> FileChangeDisposition {
        self.file_change
    }

    /// Retry classification.
    #[must_use]
    pub const fn retry(&self) -> AuthoringRetry {
        self.retry
    }
}

/// Nonempty bounded authoring diagnostic set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthoringDiagnostics(Vec<AuthoringDiagnostic>);

impl AuthoringDiagnostics {
    fn new(diagnostics: Vec<AuthoringDiagnostic>) -> Result<Self, AuthoringDiagnosticBoundsError> {
        if diagnostics.is_empty() || diagnostics.len() > MAX_AUTHORING_DIAGNOSTICS {
            return Err(AuthoringDiagnosticBoundsError);
        }
        Ok(Self(diagnostics))
    }

    /// Diagnostics in deterministic source order.
    #[must_use]
    pub fn as_slice(&self) -> &[AuthoringDiagnostic] {
        &self.0
    }

    /// Converts contract parser/compiler diagnostics without losing spans.
    pub fn from_contract(
        path: AuthoringSourcePath,
        error: &CompilationError,
    ) -> Result<Self, AuthoringDiagnosticBoundsError> {
        let diagnostics = match error {
            CompilationError::Syntax(diagnostics) => diagnostics
                .as_slice()
                .iter()
                .map(|diagnostic| {
                    let span = diagnostic.span();
                    diagnostic_value(
                        AuthoringStage::ContractSyntax,
                        AuthoringDiagnosticCode(diagnostic.code().as_str()),
                        path.clone(),
                        Some((span.start(), span.end())),
                        Vec::new(),
                        diagnostic.code().summary(),
                        AuthoringCause::InvalidSyntax,
                        vec![AuthoringFix::UseLanguageReference],
                    )
                    .map(|value| value.with_help(diagnostic.code().help()))
                })
                .collect::<Result<Vec<_>, _>>()?,
            CompilationError::Semantic(diagnostics) => diagnostics
                .as_slice()
                .iter()
                .map(|diagnostic| {
                    let span = diagnostic.primary_span();
                    let code = diagnostic.code();
                    let (cause, fixes) = contract_semantic_class(code);
                    diagnostic_value(
                        AuthoringStage::ContractSemantic,
                        AuthoringDiagnosticCode(code.as_str()),
                        path.clone(),
                        Some((span.start(), span.end())),
                        Vec::new(),
                        contract_semantic_summary(diagnostic),
                        cause,
                        fixes,
                    )
                    .map(|value| value.with_help(code.help()))
                })
                .collect::<Result<Vec<_>, _>>()?,
        };
        Self::new(diagnostics)
    }

    /// Converts a query-module failure, retaining parser/planner details.
    pub fn from_query_module(
        path: AuthoringSourcePath,
        error: &QueryModuleError,
    ) -> Result<Self, AuthoringDiagnosticBoundsError> {
        let query_symbol = error
            .query_name()
            .map_or_else(Vec::new, |name| vec![name.to_owned()]);
        let diagnostics = match error.diagnostics() {
            Some(QueryCompilationDiagnostics::Syntax(diagnostics)) => diagnostics
                .as_slice()
                .iter()
                .map(|diagnostic| {
                    let span = diagnostic.span();
                    diagnostic_value(
                        AuthoringStage::QuerySyntax,
                        AuthoringDiagnosticCode(diagnostic.code().as_str()),
                        path.clone(),
                        Some((span.start, span.end)),
                        query_symbol.clone(),
                        diagnostic.summary(),
                        AuthoringCause::InvalidSyntax,
                        vec![AuthoringFix::UseLanguageReference],
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
            Some(QueryCompilationDiagnostics::Planner(diagnostics)) => diagnostics
                .as_slice()
                .iter()
                .map(|diagnostic| {
                    let span = diagnostic.primary();
                    let mut symbols = query_symbol.clone();
                    symbols.extend(diagnostic.symbol_path().iter().cloned());
                    let (cause, fix) = query_plan_class(diagnostic.code());
                    diagnostic_value(
                        AuthoringStage::QueryPlan,
                        AuthoringDiagnosticCode(diagnostic.code().as_str()),
                        path.clone(),
                        Some((span.start, span.end)),
                        symbols,
                        query_plan_summary(diagnostic),
                        cause,
                        vec![fix],
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
            None => vec![diagnostic_value(
                AuthoringStage::QueryPlan,
                query_module_code(error.kind()),
                path,
                None,
                query_symbol,
                query_module_summary(error.kind()),
                query_module_cause(error.kind()),
                vec![query_module_fix(error.kind())],
            )?],
        };
        Self::new(diagnostics)
    }

    /// Converts a symbolic application-source failure.
    pub fn from_application_source(
        path: AuthoringSourcePath,
        kind: ApplicationSourceErrorKind,
    ) -> Result<Self, AuthoringDiagnosticBoundsError> {
        Self::from_application_source_member(path, kind, None)
    }

    /// Converts a symbolic application-source failure that names its member.
    ///
    /// The parser knows which member it rejected. Reporting the name turns a
    /// whole-document search into a single edit.
    pub fn from_application_source_member(
        path: AuthoringSourcePath,
        kind: ApplicationSourceErrorKind,
        member: Option<&str>,
    ) -> Result<Self, AuthoringDiagnosticBoundsError> {
        let summary = member.map_or_else(
            || application_source_summary(kind).to_owned(),
            |member| format!("{} at member `{member}`", application_source_summary(kind)),
        );
        Self::new(vec![diagnostic_value(
            AuthoringStage::ApplicationSource,
            application_source_code(kind),
            path,
            None,
            Vec::new(),
            &summary,
            application_source_cause(kind),
            vec![application_source_fix(kind)],
        )?])
    }

    /// Converts a compiler-owned lock failure.
    pub fn from_lock(
        path: AuthoringSourcePath,
        kind: ApplicationLockErrorKind,
    ) -> Result<Self, AuthoringDiagnosticBoundsError> {
        Self::new(vec![diagnostic_value(
            AuthoringStage::Lock,
            application_lock_code(kind),
            path,
            None,
            Vec::new(),
            application_lock_summary(kind),
            application_lock_cause(kind),
            vec![application_lock_fix(kind)],
        )?])
    }

    /// Converts a V1 exact-manifest failure.
    pub fn from_manifest(
        path: AuthoringSourcePath,
        kind: ManifestErrorKind,
    ) -> Result<Self, AuthoringDiagnosticBoundsError> {
        Self::new(vec![diagnostic_value(
            AuthoringStage::Manifest,
            manifest_code(kind),
            path,
            None,
            Vec::new(),
            manifest_summary(kind),
            manifest_cause(kind),
            vec![manifest_fix(kind)],
        )?])
    }

    /// Converts symbolic role derivation failure.
    pub fn from_role(
        path: AuthoringSourcePath,
        kind: ApplicationRoleErrorKind,
        role: Option<&str>,
    ) -> Result<Self, AuthoringDiagnosticBoundsError> {
        Self::new(vec![diagnostic_value(
            AuthoringStage::Role,
            role_code(kind),
            path,
            None,
            role.map_or_else(Vec::new, |role| vec![role.to_owned()]),
            role_summary(kind),
            role_cause(kind),
            vec![role_fix(kind)],
        )?])
    }

    /// Reports a named query whose complete worst-case index-scan work cannot
    /// be represented by the stable application-role scan bound.
    pub fn role_query_scan_budget(
        path: AuthoringSourcePath,
        role: &str,
        query: &str,
    ) -> Result<Self, AuthoringDiagnosticBoundsError> {
        Self::new(vec![diagnostic_value(
            AuthoringStage::Role,
            AuthoringDiagnosticCode("RDB-AR007"),
            path,
            None,
            vec![role.to_owned(), query.to_owned()],
            "named query aggregate index-scan bound exceeds 500 rows",
            AuthoringCause::LimitExceeded,
            vec![AuthoringFix::ReduceInput],
        )?])
    }

    /// Reports one source symbol rejected by a deterministic binding generator.
    pub fn python_name_collision(
        path: AuthoringSourcePath,
        span: (u32, u32),
        symbol_path: Vec<String>,
    ) -> Result<Self, AuthoringDiagnosticBoundsError> {
        Self::new(vec![diagnostic_value(
            AuthoringStage::Generation,
            AuthoringDiagnosticCode("RDB-GEN001"),
            path,
            Some(span),
            symbol_path,
            "application symbols collide after Python name normalization",
            AuthoringCause::GenerationFailure,
            vec![AuthoringFix::CorrectSymbol],
        )?])
    }

    /// Creates one closed filesystem diagnostic.
    pub fn filesystem(
        path: AuthoringSourcePath,
        class: FilesystemDiagnosticClass,
        disposition: FileChangeDisposition,
    ) -> Result<Self, AuthoringDiagnosticBoundsError> {
        let mut diagnostic = diagnostic_value(
            AuthoringStage::Filesystem,
            filesystem_code(class),
            path,
            None,
            Vec::new(),
            filesystem_summary(class),
            match class {
                FilesystemDiagnosticClass::Symlink
                | FilesystemDiagnosticClass::NotRegular
                | FilesystemDiagnosticClass::EscapesWorkspace => AuthoringCause::UnsafePath,
                _ => AuthoringCause::FilesystemFailure,
            },
            vec![match class {
                FilesystemDiagnosticClass::Symlink
                | FilesystemDiagnosticClass::NotRegular
                | FilesystemDiagnosticClass::EscapesWorkspace => AuthoringFix::CorrectPath,
                _ => AuthoringFix::CorrectFilesystem,
            }],
        )?;
        diagnostic.file_change = disposition;
        diagnostic.retry = AuthoringRetry::RetrySameInputs;
        Self::new(vec![diagnostic])
    }

    /// Deterministic bounded human renderer without source snippets.
    pub fn render_human(&self) -> Result<String, AuthoringDiagnosticBoundsError> {
        let mut output = String::new();
        for diagnostic in &self.0 {
            writeln!(
                output,
                "{} [{}] {}",
                diagnostic.code.as_str(),
                diagnostic.stage.as_str(),
                diagnostic.summary
            )
            .map_err(|_| AuthoringDiagnosticBoundsError)?;
            write!(output, "  --> {}", diagnostic.path.as_str())
                .map_err(|_| AuthoringDiagnosticBoundsError)?;
            if let Some(span) = diagnostic.span {
                write!(output, ":{}..{}", span.start, span.end)
                    .map_err(|_| AuthoringDiagnosticBoundsError)?;
            }
            writeln!(output).map_err(|_| AuthoringDiagnosticBoundsError)?;
            if !diagnostic.symbol_path.is_empty() {
                writeln!(output, "  symbol: {}", diagnostic.symbol_path.join("."))
                    .map_err(|_| AuthoringDiagnosticBoundsError)?;
            }
            if let Some(help) = &diagnostic.help {
                writeln!(output, "  help: {help}").map_err(|_| AuthoringDiagnosticBoundsError)?;
            }
            writeln!(output, "  cause: {}", diagnostic.cause.as_str())
                .map_err(|_| AuthoringDiagnosticBoundsError)?;
            writeln!(
                output,
                "  fixes: {}",
                diagnostic
                    .fixes
                    .iter()
                    .map(|fix| fix.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            )
            .map_err(|_| AuthoringDiagnosticBoundsError)?;
            writeln!(
                output,
                "  files: {}; retry: {}",
                diagnostic.file_change.as_str(),
                diagnostic.retry.as_str()
            )
            .map_err(|_| AuthoringDiagnosticBoundsError)?;
        }
        check_render_bound(output)
    }

    /// Canonical bounded JSON renderer used by CLI and builder MCP.
    pub fn render_json(&self) -> Result<String, AuthoringDiagnosticBoundsError> {
        let diagnostics = self
            .0
            .iter()
            .map(|diagnostic| {
                json!({
                    "cause": diagnostic.cause.as_str(),
                    "code": diagnostic.code.as_str(),
                    "file_change": diagnostic.file_change.as_str(),
                    "fixes": diagnostic.fixes.iter().map(|fix| fix.as_str()).collect::<Vec<_>>(),
                    "help": diagnostic.help.as_deref().map_or(Value::Null, |help| json!(help)),
                    "path": diagnostic.path.as_str(),
                    "retry": diagnostic.retry.as_str(),
                    "span": diagnostic.span.map(|span| json!({
                        "end": span.end,
                        "start": span.start,
                    })).unwrap_or(Value::Null),
                    "stage": diagnostic.stage.as_str(),
                    "summary": diagnostic.summary,
                    "symbol_path": diagnostic.symbol_path,
                })
            })
            .collect::<Vec<_>>();
        let mut output = serde_json::to_string(&json!({
            "diagnostics": diagnostics,
            "version": AUTHORING_DIAGNOSTIC_VERSION_V1,
        }))
        .map_err(|_| AuthoringDiagnosticBoundsError)?;
        output.push('\n');
        check_render_bound(output)
    }
}

/// Closed local filesystem failure classes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilesystemDiagnosticClass {
    /// Destination already exists.
    DestinationExists,
    /// Destination parent is absent.
    ParentMissing,
    /// Operation lacks local permission.
    PermissionDenied,
    /// A path component is a symlink.
    Symlink,
    /// The selected path is not a regular file/directory of the required kind.
    NotRegular,
    /// A normalized/canonical path leaves the workspace.
    EscapesWorkspace,
    /// A staged write was interrupted or could not publish.
    InterruptedStaging,
    /// Required local input is absent.
    NotFound,
}

/// A diagnostic would exceed a hard path, symbol, count, span, or render bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuthoringDiagnosticBoundsError;

impl fmt::Display for AuthoringDiagnosticBoundsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("authoring diagnostic exceeds a fixed safe bound")
    }
}

impl std::error::Error for AuthoringDiagnosticBoundsError {}

#[allow(
    clippy::too_many_arguments,
    reason = "the private constructor makes every closed diagnostic field explicit"
)]
fn diagnostic_value(
    stage: AuthoringStage,
    code: AuthoringDiagnosticCode,
    path: AuthoringSourcePath,
    span: Option<(u32, u32)>,
    symbol_path: Vec<String>,
    summary: impl Into<String>,
    cause: AuthoringCause,
    mut fixes: Vec<AuthoringFix>,
) -> Result<AuthoringDiagnostic, AuthoringDiagnosticBoundsError> {
    let summary = summary.into();
    if symbol_path.len() > MAX_SYMBOL_PATH_COMPONENTS
        || summary.is_empty()
        || summary.len() > MAX_SUMMARY_BYTES
        || symbol_path.iter().any(|component| {
            component.is_empty()
                || component.len() > MAX_SYMBOL_COMPONENT_BYTES
                || !component
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        })
    {
        return Err(AuthoringDiagnosticBoundsError);
    }
    fixes.sort();
    fixes.dedup();
    let span = match span {
        Some((start, end)) => {
            Some(AuthoringSourceSpan::new(start, end).ok_or(AuthoringDiagnosticBoundsError)?)
        }
        None => None,
    };
    Ok(AuthoringDiagnostic {
        stage,
        code,
        path,
        span,
        symbol_path,
        summary,
        help: None,
        cause,
        fixes,
        file_change: FileChangeDisposition::NoFilesChanged,
        retry: if cause == AuthoringCause::IdentityDrift {
            AuthoringRetry::ReviewAndWriteLock
        } else if cause == AuthoringCause::InternalInvariant {
            AuthoringRetry::ContactOperator
        } else {
            AuthoringRetry::CorrectSource
        },
    })
}

fn check_render_bound(output: String) -> Result<String, AuthoringDiagnosticBoundsError> {
    if output.len() > MAX_AUTHORING_DIAGNOSTIC_BYTES {
        Err(AuthoringDiagnosticBoundsError)
    } else {
        Ok(output)
    }
}

fn contract_semantic_class(
    code: riffdb_contract_compiler::CompilerDiagnosticCode,
) -> (AuthoringCause, Vec<AuthoringFix>) {
    use riffdb_contract_compiler::CompilerDiagnosticCode as Code;
    match code {
        Code::UnknownName => (
            AuthoringCause::UnknownSymbol,
            vec![AuthoringFix::CorrectSymbol],
        ),
        Code::TypeMismatch | Code::InvalidType => (
            AuthoringCause::TypeMismatch,
            vec![AuthoringFix::CorrectType],
        ),
        Code::CrossPartitionMutation => (
            AuthoringCause::NonLocal,
            vec![
                AuthoringFix::SupplyPartitionRoute,
                AuthoringFix::ModelOneMutationAggregate,
            ],
        ),
        Code::MissingRelationshipRead => (
            AuthoringCause::MissingRelationshipProof,
            vec![AuthoringFix::ProveRelationshipTarget],
        ),
        Code::BoundExceeded => (
            AuthoringCause::LimitExceeded,
            vec![AuthoringFix::ReduceInput],
        ),
        Code::InvalidIr => (
            AuthoringCause::InternalInvariant,
            vec![AuthoringFix::ContactOperator],
        ),
        _ => (
            AuthoringCause::InvalidSyntax,
            vec![AuthoringFix::UseLanguageReference],
        ),
    }
}

fn contract_semantic_summary(diagnostic: &riffdb_contract_compiler::CompilerDiagnostic) -> String {
    use riffdb_contract_compiler::CompilerDiagnosticCode as Code;
    match diagnostic.code() {
        Code::CrossPartitionMutation => {
            "atomic command writes span multiple aggregate roots or partition routes".to_owned()
        }
        Code::MissingRelationshipRead => {
            "relationship proof must read the exact target before mutation and reuse the same key expressions".to_owned()
        }
        _ => diagnostic.summary(),
    }
}

/// Quotes the closed bound observation when the planner supplied one.
///
/// Both numbers are compiler-derived from schema and declared maxima, so this
/// echoes nothing from the query source. Without them the author is told only
/// that some ceiling was exceeded, and is left to rediscover which resource and
/// which ceiling by trial compilation.
fn query_plan_summary(diagnostic: &riffdb_query_compiler::PlannerDiagnostic) -> String {
    diagnostic.bound().map_or_else(
        || diagnostic.summary().to_owned(),
        |bound| {
            format!(
                "{}: {} is statically charged {} against a maximum of {}",
                diagnostic.summary(),
                bound.resource().as_str(),
                bound.actual(),
                bound.maximum()
            )
        },
    )
}

fn query_plan_class(
    code: riffdb_query_compiler::PlannerDiagnosticCode,
) -> (AuthoringCause, AuthoringFix) {
    use riffdb_query_compiler::PlannerDiagnosticCode as Code;
    match code {
        Code::TypeMismatch | Code::Cardinality => {
            (AuthoringCause::TypeMismatch, AuthoringFix::CorrectType)
        }
        Code::NonLocal => (AuthoringCause::NonLocal, AuthoringFix::SupplyPartitionRoute),
        Code::Unindexed | Code::Unordered => (AuthoringCause::MissingIndex, AuthoringFix::AddIndex),
        Code::Unbounded => (AuthoringCause::Unbounded, AuthoringFix::AddBound),
        // The author already declared a bound; it is too large. Telling them to
        // "add a bound" sends them looking for something that is already there.
        Code::CostCeilingExceeded => (AuthoringCause::LimitExceeded, AuthoringFix::ReduceInput),
        Code::ExactTextProvider => (
            AuthoringCause::InvalidSyntax,
            AuthoringFix::UseLanguageReference,
        ),
        Code::InternalInvariant | Code::OperationalFamilyRequired => (
            AuthoringCause::InternalInvariant,
            AuthoringFix::ContactOperator,
        ),
    }
}

macro_rules! static_registry {
    ($code_fn:ident, $summary_fn:ident, $cause_fn:ident, $fix_fn:ident, $kind:ty, {
        $($variant:path => ($code:literal, $summary:literal, $cause:expr, $fix:expr)),+ $(,)?
    }) => {
        const fn $code_fn(kind: $kind) -> AuthoringDiagnosticCode {
            match kind {
                $($variant => AuthoringDiagnosticCode($code),)+
            }
        }
        const fn $summary_fn(kind: $kind) -> &'static str {
            match kind {
                $($variant => $summary,)+
            }
        }
        const fn $cause_fn(kind: $kind) -> AuthoringCause {
            match kind {
                $($variant => $cause,)+
            }
        }
        const fn $fix_fn(kind: $kind) -> AuthoringFix {
            match kind {
                $($variant => $fix,)+
            }
        }
    };
}

static_registry!(
    application_source_code,
    application_source_summary,
    application_source_cause,
    application_source_fix,
    ApplicationSourceErrorKind,
    {
        ApplicationSourceErrorKind::InvalidJson => ("RDB-AS001", "application source is not valid JSON", AuthoringCause::InvalidSyntax, AuthoringFix::UseLanguageReference),
        ApplicationSourceErrorKind::UnsupportedVersion => ("RDB-AS002", "application source schema is unsupported", AuthoringCause::InvalidSyntax, AuthoringFix::UseLanguageReference),
        ApplicationSourceErrorKind::InvalidShape => ("RDB-AS003", "application source has a missing, extra, or wrongly typed member", AuthoringCause::InvalidSyntax, AuthoringFix::UseLanguageReference),
        ApplicationSourceErrorKind::InvalidName => ("RDB-AS004", "application source contains an invalid symbolic name", AuthoringCause::InvalidSyntax, AuthoringFix::CorrectSymbol),
        ApplicationSourceErrorKind::InvalidPath => ("RDB-AS005", "application source contains an unsafe path", AuthoringCause::UnsafePath, AuthoringFix::CorrectPath),
        ApplicationSourceErrorKind::Duplicate => ("RDB-AS006", "application source repeats a declaration", AuthoringCause::InvalidSyntax, AuthoringFix::CorrectSymbol),
        ApplicationSourceErrorKind::UnknownOperation => ("RDB-AS007", "application role names an undeclared query", AuthoringCause::UnknownSymbol, AuthoringFix::CorrectSymbol),
        ApplicationSourceErrorKind::LimitExceeded => ("RDB-AS008", "application source exceeds a hard bound", AuthoringCause::LimitExceeded, AuthoringFix::ReduceInput),
        ApplicationSourceErrorKind::MissingRequiredEntry => ("RDB-AS011", "application source omits an entry a closed member requires", AuthoringCause::InvalidSyntax, AuthoringFix::SupplyRequiredEntry),
        ApplicationSourceErrorKind::NonCanonical => ("RDB-AS009", "application source bytes are not canonical", AuthoringCause::IdentityDrift, AuthoringFix::WriteLock),
        ApplicationSourceErrorKind::IdentityMismatch => ("RDB-AS010", "compiled application does not match symbolic source", AuthoringCause::IdentityDrift, AuthoringFix::WriteLock)
    }
);

static_registry!(
    application_lock_code,
    application_lock_summary,
    application_lock_cause,
    application_lock_fix,
    ApplicationLockErrorKind,
    {
        ApplicationLockErrorKind::InvalidJson => ("RDB-AL001", "application lock is not valid JSON", AuthoringCause::IdentityDrift, AuthoringFix::WriteLock),
        ApplicationLockErrorKind::UnsupportedVersion => ("RDB-AL002", "application lock schema is unsupported", AuthoringCause::IdentityDrift, AuthoringFix::WriteLock),
        ApplicationLockErrorKind::InvalidShape => ("RDB-AL003", "application lock shape is invalid", AuthoringCause::IdentityDrift, AuthoringFix::WriteLock),
        ApplicationLockErrorKind::InvalidPath => ("RDB-AL004", "application lock contains an unsafe output path", AuthoringCause::UnsafePath, AuthoringFix::CorrectPath),
        ApplicationLockErrorKind::Duplicate => ("RDB-AL005", "application lock repeats a declaration", AuthoringCause::IdentityDrift, AuthoringFix::WriteLock),
        ApplicationLockErrorKind::LimitExceeded => ("RDB-AL006", "application lock exceeds a hard bound", AuthoringCause::LimitExceeded, AuthoringFix::ReduceInput),
        ApplicationLockErrorKind::NonCanonical => ("RDB-AL007", "application lock bytes are not canonical", AuthoringCause::IdentityDrift, AuthoringFix::WriteLock),
        ApplicationLockErrorKind::IdentityMismatch => ("RDB-AL008", "application lock contains stale or substituted identities", AuthoringCause::IdentityDrift, AuthoringFix::WriteLock),
        ApplicationLockErrorKind::UnknownOperation => ("RDB-AL009", "application role names an unknown compiled operation", AuthoringCause::UnknownSymbol, AuthoringFix::CorrectSymbol)
    }
);

static_registry!(
    manifest_code,
    manifest_summary,
    manifest_cause,
    manifest_fix,
    ManifestErrorKind,
    {
        ManifestErrorKind::InvalidJson => ("RDB-AM001", "exact application manifest is not valid JSON", AuthoringCause::IdentityDrift, AuthoringFix::GenerateLocked),
        ManifestErrorKind::UnsupportedVersion => ("RDB-AM002", "exact application manifest schema is unsupported", AuthoringCause::IdentityDrift, AuthoringFix::GenerateLocked),
        ManifestErrorKind::InvalidShape => ("RDB-AM003", "exact application manifest shape is invalid", AuthoringCause::IdentityDrift, AuthoringFix::GenerateLocked),
        ManifestErrorKind::InvalidName => ("RDB-AM004", "exact application manifest contains an invalid name", AuthoringCause::IdentityDrift, AuthoringFix::GenerateLocked),
        ManifestErrorKind::InvalidPath => ("RDB-AM005", "exact application manifest contains an unsafe path", AuthoringCause::UnsafePath, AuthoringFix::CorrectPath),
        ManifestErrorKind::Duplicate => ("RDB-AM006", "exact application manifest repeats a declaration", AuthoringCause::IdentityDrift, AuthoringFix::GenerateLocked),
        ManifestErrorKind::UnknownOperation => ("RDB-AM007", "exact application manifest names an unknown query", AuthoringCause::UnknownSymbol, AuthoringFix::CorrectSymbol),
        ManifestErrorKind::LimitExceeded => ("RDB-AM008", "exact application manifest exceeds a hard bound", AuthoringCause::LimitExceeded, AuthoringFix::ReduceInput),
        ManifestErrorKind::NonCanonical => ("RDB-AM009", "exact application manifest is not canonical", AuthoringCause::IdentityDrift, AuthoringFix::GenerateLocked)
    }
);

static_registry!(
    role_code,
    role_summary,
    role_cause,
    role_fix,
    ApplicationRoleErrorKind,
    {
        ApplicationRoleErrorKind::UnknownRole => ("RDB-AR001", "application role is not declared", AuthoringCause::UnknownSymbol, AuthoringFix::CorrectSymbol),
        ApplicationRoleErrorKind::TenantBindingMismatch => ("RDB-AR002", "application role tenant binding does not match its scope", AuthoringCause::UnsafeRole, AuthoringFix::NarrowRole),
        ApplicationRoleErrorKind::ContractMismatch => ("RDB-AR003", "application role contract identity is stale", AuthoringCause::IdentityDrift, AuthoringFix::WriteLock),
        ApplicationRoleErrorKind::ModuleMismatch => ("RDB-AR004", "application role query module identity is stale", AuthoringCause::IdentityDrift, AuthoringFix::WriteLock),
        ApplicationRoleErrorKind::UnknownOperation => ("RDB-AR005", "application role names an unknown operation", AuthoringCause::UnknownSymbol, AuthoringFix::CorrectSymbol),
        ApplicationRoleErrorKind::RequirementLimit => ("RDB-AR006", "application role derived authority exceeds a hard bound", AuthoringCause::UnsafeRole, AuthoringFix::NarrowRole),
        ApplicationRoleErrorKind::UnknownPolicy => ("RDB-AR008", "application role names an unknown or ambiguous row policy", AuthoringCause::UnknownSymbol, AuthoringFix::CorrectSymbol),
        ApplicationRoleErrorKind::PolicyCoverage => ("RDB-AR009", "application role does not select a row policy for every protected operation", AuthoringCause::UnsafeRole, AuthoringFix::NarrowRole),
        ApplicationRoleErrorKind::PrincipalFacts => ("RDB-AR010", "application role principal facts do not match the compiled schemas", AuthoringCause::UnsafeRole, AuthoringFix::NarrowRole)
    }
);

static_registry!(
    query_module_code,
    query_module_summary,
    query_module_cause,
    query_module_fix,
    QueryModuleErrorKind,
    {
        QueryModuleErrorKind::InvalidName => ("RDB-QM001", "query module contains an invalid name", AuthoringCause::InvalidSyntax, AuthoringFix::CorrectSymbol),
        QueryModuleErrorKind::LimitExceeded => ("RDB-QM002", "query module exceeds a hard bound", AuthoringCause::LimitExceeded, AuthoringFix::ReduceInput),
        QueryModuleErrorKind::DuplicateQuery => ("RDB-QM003", "query module repeats a query name", AuthoringCause::InvalidSyntax, AuthoringFix::CorrectSymbol),
        QueryModuleErrorKind::QueryNameMismatch => ("RDB-QM004", "query file declaration does not match its symbolic name", AuthoringCause::UnknownSymbol, AuthoringFix::CorrectSymbol),
        QueryModuleErrorKind::InvalidQuery => ("RDB-QM005", "query source is invalid", AuthoringCause::InvalidSyntax, AuthoringFix::UseLanguageReference),
        QueryModuleErrorKind::InvalidContract => ("RDB-QM006", "contract cannot supply a symbolic query catalog", AuthoringCause::InternalInvariant, AuthoringFix::ContactOperator),
        QueryModuleErrorKind::InvalidEncoding => ("RDB-QM007", "query module encoding is invalid", AuthoringCause::IdentityDrift, AuthoringFix::WriteLock),
        QueryModuleErrorKind::ContractMismatch => ("RDB-QM008", "query module contract identity is stale", AuthoringCause::IdentityDrift, AuthoringFix::WriteLock),
        QueryModuleErrorKind::IdentityMismatch => ("RDB-QM009", "query module identity cannot be reproduced", AuthoringCause::IdentityDrift, AuthoringFix::WriteLock),
        QueryModuleErrorKind::UnsupportedVersion => ("RDB-QM010", "query module version is unsupported", AuthoringCause::IdentityDrift, AuthoringFix::WriteLock)
    }
);

const fn filesystem_code(class: FilesystemDiagnosticClass) -> AuthoringDiagnosticCode {
    AuthoringDiagnosticCode(match class {
        FilesystemDiagnosticClass::DestinationExists => "RDB-FS001",
        FilesystemDiagnosticClass::ParentMissing => "RDB-FS002",
        FilesystemDiagnosticClass::PermissionDenied => "RDB-FS003",
        FilesystemDiagnosticClass::Symlink => "RDB-FS004",
        FilesystemDiagnosticClass::NotRegular => "RDB-FS005",
        FilesystemDiagnosticClass::EscapesWorkspace => "RDB-FS006",
        FilesystemDiagnosticClass::InterruptedStaging => "RDB-FS007",
        FilesystemDiagnosticClass::NotFound => "RDB-FS008",
    })
}

const fn filesystem_summary(class: FilesystemDiagnosticClass) -> &'static str {
    match class {
        FilesystemDiagnosticClass::DestinationExists => "destination already exists",
        FilesystemDiagnosticClass::ParentMissing => "destination parent is missing",
        FilesystemDiagnosticClass::PermissionDenied => "filesystem permission was denied",
        FilesystemDiagnosticClass::Symlink => "application path traverses a symlink",
        FilesystemDiagnosticClass::NotRegular => "application path has the wrong file type",
        FilesystemDiagnosticClass::EscapesWorkspace => "application path escapes the workspace",
        FilesystemDiagnosticClass::InterruptedStaging => "staged output could not be published",
        FilesystemDiagnosticClass::NotFound => "required application input is missing",
    }
}

#[cfg(test)]
mod tests {
    use riffdb_contract_compiler::compile_contract_source;
    use riffdb_query_module::{
        NamedQuerySource, QueryModule, QueryModuleCandidate, QueryModuleName, QueryModuleVersion,
    };

    use super::*;

    const CONTRACT: &str = include_str!("../../../examples/app-baseline/contracts/ticketdesk.riff");

    /// A bounded query over the result-byte ceiling must be reported as a
    /// limit to reduce, not as a missing bound, and must quote the charged
    /// amount and the ceiling.
    ///
    /// Without both numbers an author is told only that "some ceiling" was
    /// exceeded and has to rediscover the ratio by trial compilation. With
    /// them the correction is arithmetic. The numbers are schema-derived, so
    /// this still echoes nothing from the query source.
    #[test]
    fn a_bounded_query_over_the_result_ceiling_reports_the_charge_and_the_ceiling() {
        let bundle = compile_contract_source(CONTRACT).expect("contract");
        let copies = (0..40)
            .map(|index| format!("copy_{index}: tickets {{ title }}"))
            .collect::<Vec<_>>()
            .join("\n        ");
        let query = format!(
            "query Huge(
    $organization_id: Organization.organization_id,
    $project_id: Project.project_id,
    $status: TicketStatus
) {{
    many tickets from Ticket
        where organization_id == $organization_id
          && project_id == $project_id
          && status == $status
        order by ticket_id asc
        take 499
    return Found {{
        {copies}
    }}
    outcomes Found
}}
// SECRET_VALUE_MUST_NOT_APPEAR
"
        );
        let error = QueryModule::compile(
            QueryModuleCandidate::new(
                QueryModuleName::new("diagnostic").expect("name"),
                QueryModuleVersion::new(1).expect("version"),
                vec![NamedQuerySource::new("Huge", &query).expect("query")],
            )
            .expect("candidate"),
            &bundle,
        )
        .expect_err("ceiling");
        let diagnostics = AuthoringDiagnostics::from_query_module(
            AuthoringSourcePath::new("riffdb/queries/huge.riffq").expect("path"),
            &error,
        )
        .expect("diagnostics");
        let diagnostic = &diagnostics.as_slice()[0];

        assert_eq!(diagnostic.code().as_str(), "RDB-QP010");
        assert_eq!(diagnostic.cause(), AuthoringCause::LimitExceeded);
        assert!(diagnostic.fixes().contains(&AuthoringFix::ReduceInput));
        assert!(diagnostic.summary().contains("encoded_result_bytes"));
        assert!(diagnostic.summary().contains("4194304"));

        for rendered in [
            diagnostics.render_human().expect("human"),
            diagnostics.render_json().expect("JSON"),
        ] {
            assert!(rendered.contains("4194304"));
            assert!(!rendered.contains("SECRET_VALUE_MUST_NOT_APPEAR"));
            assert!(!rendered.contains("$organization_id"));
        }
    }

    #[test]
    fn query_planner_diagnostic_retains_original_span_and_never_echoes_values() {
        let bundle = compile_contract_source(CONTRACT).expect("contract");
        let query = r#"
query Bad($organization_id: Organization.organization_id, $title: Ticket.title) {
    many tickets from Ticket
        where organization_id == $organization_id && title == $title
        order by updated_at desc
        take 10
    return Found { tickets: tickets { ticket_id } }
    outcomes Found
}
// SECRET_VALUE_MUST_NOT_APPEAR
"#;
        let error = QueryModule::compile(
            QueryModuleCandidate::new(
                QueryModuleName::new("diagnostic").expect("name"),
                QueryModuleVersion::new(1).expect("version"),
                vec![NamedQuerySource::new("Bad", query).expect("query")],
            )
            .expect("candidate"),
            &bundle,
        )
        .expect_err("unindexed");
        let diagnostics = AuthoringDiagnostics::from_query_module(
            AuthoringSourcePath::new("riffdb/queries/bad.riffq").expect("path"),
            &error,
        )
        .expect("diagnostics");
        let diagnostic = &diagnostics.as_slice()[0];

        assert_eq!(diagnostic.code().as_str(), "RDB-QP003");
        assert_eq!(diagnostic.cause(), AuthoringCause::MissingIndex);
        assert!(
            diagnostic
                .span()
                .is_some_and(|span| span.start() < span.end())
        );
        assert_eq!(diagnostic.symbol_path(), &["Bad", "Ticket"]);
        for rendered in [
            diagnostics.render_human().expect("human"),
            diagnostics.render_json().expect("JSON"),
        ] {
            assert!(!rendered.contains("SECRET_VALUE_MUST_NOT_APPEAR"));
            assert!(!rendered.contains("$organization_id"));
            assert!(!rendered.contains("title == "));
        }
    }

    #[test]
    fn operational_query_without_a_safe_index_fails_closed_through_the_module_compiler() {
        let bundle = compile_contract_source(CONTRACT).expect("contract");
        let query = r#"
query Operational($organization_id: Organization.organization_id, $title: Ticket.title?) {
    many tickets from Ticket
        where organization_id == $organization_id && when $title { title == $title }
        order by updated_at desc
        take 10
    return Found { tickets: tickets { ticket_id } }
    outcomes Found
}
"#;
        let error = QueryModule::compile(
            QueryModuleCandidate::new(
                QueryModuleName::new("diagnostic").expect("name"),
                QueryModuleVersion::new(1).expect("version"),
                vec![NamedQuerySource::new("Operational", query).expect("query")],
            )
            .expect("candidate"),
            &bundle,
        )
        .expect_err("operational module must reject an unindexed family");
        let diagnostics = AuthoringDiagnostics::from_query_module(
            AuthoringSourcePath::new("riffdb/queries/operational.riffq").expect("path"),
            &error,
        )
        .expect("diagnostics");
        let diagnostic = &diagnostics.as_slice()[0];

        assert_eq!(diagnostic.code().as_str(), "RDB-QP003");
        assert_eq!(diagnostic.cause(), AuthoringCause::MissingIndex);
        assert_eq!(diagnostic.fixes(), &[AuthoringFix::AddIndex]);
        assert!(
            diagnostic
                .span()
                .is_some_and(|span| span.start() < span.end())
        );
    }

    #[test]
    fn contract_diagnostics_are_source_spanned_and_json_is_closed() {
        let error = compile_contract_source(
            "contract Broken version 1 { entity Item { key (id: uuid) } SECRET_VALUE }",
        )
        .expect_err("invalid contract");
        let diagnostics = AuthoringDiagnostics::from_contract(
            AuthoringSourcePath::new("riffdb/contract.riff").expect("path"),
            &error,
        )
        .expect("diagnostics");
        let rendered = diagnostics.render_json().expect("JSON");
        let value: serde_json::Value = serde_json::from_str(&rendered).expect("parse");

        assert_eq!(value["version"], 1);
        assert!(value["diagnostics"][0]["span"]["end"].as_u64().is_some());
        assert!(!rendered.contains("SECRET_VALUE"));
        assert!(!rendered.contains("entity Item"));
        // The shape is closed, so a member added here is a deliberate change:
        // `help` carries the compiler's static corrective guidance, which was
        // previously computed and shown only on the MCP surface.
        assert_eq!(
            value["diagnostics"][0].as_object().expect("object").len(),
            11
        );
        assert!(
            value["diagnostics"][0]
                .as_object()
                .expect("object")
                .contains_key("help")
        );
    }

    #[test]
    fn a_contract_diagnostic_carries_the_compilers_corrective_guidance() {
        // The guidance already existed on CompilerDiagnosticCode and reached
        // only the MCP surface; the CLI showed a cause and a fix code with no
        // statement of what a working contract would look like.
        let source = r"
contract Orders version 1 {
  entity Product {
    key (tenant_id: uuid, product_id: uuid)
    field stock: u64
    delete_policy no_inbound
  }
  entity Reservation {
    key (tenant_id: uuid, product_id: uuid, reservation_id: uuid)
    field quantity: u64
    index by_parent (tenant_id, product_id, reservation_id)
    reference reservation_parent (tenant_id, product_id) -> Product(tenant_id, product_id)
    delete_policy no_inbound
  }
  aggregate ProductData {
    root Product
    child Reservation
    partition_by tenant_id
    conflict_key (tenant_id, product_id)
  }
}
";
        let error = riffdb_contract_compiler::compile_contract_source(source)
            .expect_err("the deletion policy cannot be proved");
        let diagnostics = AuthoringDiagnostics::from_contract(
            AuthoringSourcePath::new("riffdb/contract.riff").expect("path"),
            &error,
        )
        .expect("diagnostics");
        let human = diagnostics.render_human().expect("human");
        assert!(human.contains("RDB-C045"), "{human}");
        assert!(human.contains("  help: "), "{human}");
        assert!(
            human.contains("cascade over every inbound relation"),
            "{human}"
        );
    }

    #[test]
    fn a_member_below_its_minimum_is_not_reported_as_an_exceeded_bound() {
        // `query_modules: []` reported RDB-AS008 with `reduce_input`, which is
        // the opposite of the required correction.
        let diagnostics = AuthoringDiagnostics::from_application_source(
            AuthoringSourcePath::new("riffdb.application.json").expect("path"),
            ApplicationSourceErrorKind::MissingRequiredEntry,
        )
        .expect("diagnostics");
        let human = diagnostics.render_human().expect("human");
        assert!(human.contains("RDB-AS011"), "{human}");
        assert!(human.contains("supply_required_entry"), "{human}");
        assert!(!human.contains("reduce_input"), "{human}");
    }

    #[test]
    fn an_invalid_member_is_named_when_the_parser_knows_it() {
        let diagnostics = AuthoringDiagnostics::from_application_source_member(
            AuthoringSourcePath::new("riffdb.application.json").expect("path"),
            ApplicationSourceErrorKind::InvalidShape,
            Some("python"),
        )
        .expect("diagnostics");
        assert!(
            diagnostics
                .render_human()
                .expect("human")
                .contains("at member `python`"),
            "the rejected member must be named"
        );
    }

    #[test]
    fn cross_aggregate_diagnostic_names_both_required_model_corrections() {
        let source = r#"
contract Orders version 1 {
  entity PurchaseOrder { key (store_id: uuid, order_id: uuid) }
  entity Inventory { key (store_id: uuid, product_id: uuid) field available: i64 }
  aggregate OrdersRoot {
    root PurchaseOrder
    partition_by store_id
    conflict_key (store_id, order_id)
  }
  aggregate InventoryRoot {
    root Inventory
    partition_by store_id
    conflict_key (store_id, product_id)
  }
  command Reserve {
    input request_key: string<128>
    input store_id: uuid
    input order_id: uuid
    input product_id: uuid
    idempotency_key request_key
    mutate PurchaseOrder(store_id, order_id) as purchase else OrderMissing {}
    mutate Inventory(store_id, product_id) as inventory else InventoryMissing {}
    set inventory.available = inventory.available - 1
    return Reserved { purchase: purchase, inventory: inventory }
  }
}
"#;
        let error = compile_contract_source(source).expect_err("two mutation aggregates reject");
        let diagnostics = AuthoringDiagnostics::from_contract(
            AuthoringSourcePath::new("riffdb/contract.riff").expect("path"),
            &error,
        )
        .expect("diagnostics");
        let diagnostic = diagnostics
            .as_slice()
            .iter()
            .find(|diagnostic| diagnostic.code().as_str() == "RDB-C017")
            .expect("cross-aggregate diagnostic");
        let expected_start = source.find("Inventory(store_id").expect("second mutation");

        assert_eq!(diagnostic.cause(), AuthoringCause::NonLocal);
        assert_eq!(
            diagnostic.summary(),
            "atomic command writes span multiple aggregate roots or partition routes"
        );
        assert_eq!(
            diagnostic.span().map(|span| (span.start(), span.end())),
            Some((
                u32::try_from(expected_start).expect("start"),
                u32::try_from(expected_start + "Inventory".len()).expect("end")
            ))
        );
        assert_eq!(
            diagnostic.fixes(),
            &[
                AuthoringFix::SupplyPartitionRoute,
                AuthoringFix::ModelOneMutationAggregate,
            ]
        );
        assert!(
            diagnostics
                .render_human()
                .expect("human")
                .contains("fixes: supply_partition_route,model_one_mutation_aggregate")
        );
    }

    #[test]
    fn relationship_diagnostic_names_the_exact_proof_and_its_safe_correction() {
        let source = r#"
contract Blog version 1 {
  entity Site { key (site_id: uuid) }
  entity Author { key (site_id: uuid, author_id: uuid) }
  entity Post {
    key (site_id: uuid, post_id: uuid)
    field author_id: uuid
    reference author_ref (site_id, author_id) -> Author(site_id, author_id)
  }
  aggregate SiteRoot {
    root Site
    child Author
    child Post
    partition_by site_id
    conflict_key (site_id)
  }
  command CreatePost {
    input request_key: string<128>
    input site_id: uuid
    input author_id: uuid
    input post_id: uuid
    idempotency_key request_key
    read Author(site_id, author_id) as author
      else AuthorMissing { author_id: author_id }
    create Post(site_id, post_id) as post
      else PostExists { post_id: post_id }
    set post.author_id = author.author_id
    return Created { post: post }
  }
}
"#;
        let error = compile_contract_source(source).expect_err("expression drift rejects");
        let diagnostics = AuthoringDiagnostics::from_contract(
            AuthoringSourcePath::new("riffdb/contract.riff").expect("path"),
            &error,
        )
        .expect("diagnostics");
        let diagnostic = diagnostics
            .as_slice()
            .iter()
            .find(|diagnostic| diagnostic.code().as_str() == "RDB-C024")
            .expect("relationship diagnostic");
        let expected_start = source.find("author_ref").expect("relationship name");

        assert_eq!(diagnostic.cause(), AuthoringCause::MissingRelationshipProof);
        assert_eq!(
            diagnostic.summary(),
            "relationship proof must read the exact target before mutation and reuse the same key expressions"
        );
        assert_eq!(
            diagnostic.span().map(|span| (span.start(), span.end())),
            Some((
                u32::try_from(expected_start).expect("start"),
                u32::try_from(expected_start + "author_ref".len()).expect("end")
            ))
        );
        assert_eq!(diagnostic.fixes(), &[AuthoringFix::ProveRelationshipTarget]);
        assert!(
            diagnostics
                .render_human()
                .expect("human")
                .contains("fixes: prove_relationship_target")
        );

        let corrected = source.replace(
            "set post.author_id = author.author_id",
            "set post.author_id = author_id",
        );
        compile_contract_source(&corrected).expect("exact key expression reuse compiles");
    }

    #[test]
    fn diagnostic_paths_are_bounded_and_cannot_escape() {
        for path in ["", "../secret", "/etc/passwd", "a//b", "a\\b"] {
            assert!(AuthoringSourcePath::new(path).is_err(), "{path}");
        }
        assert!(AuthoringSourcePath::new("riffdb/queries/page.riffq").is_ok());
    }

    #[test]
    fn python_name_collision_is_source_spanned_and_closed() {
        let diagnostics = AuthoringDiagnostics::python_name_collision(
            AuthoringSourcePath::new("riffdb/contract.riff").expect("path"),
            (41, 47),
            vec![
                "entity".to_owned(),
                "Item".to_owned(),
                "field".to_owned(),
                "class_".to_owned(),
            ],
        )
        .expect("diagnostics");
        let diagnostic = &diagnostics.as_slice()[0];

        assert_eq!(diagnostic.code().as_str(), "RDB-GEN001");
        assert_eq!(diagnostic.stage(), AuthoringStage::Generation);
        assert_eq!(diagnostic.cause(), AuthoringCause::GenerationFailure);
        assert_eq!(
            diagnostic.span().map(|span| (span.start(), span.end())),
            Some((41, 47))
        );
        assert_eq!(
            diagnostic.symbol_path(),
            &["entity", "Item", "field", "class_"]
        );
        assert_eq!(diagnostic.fixes(), &[AuthoringFix::CorrectSymbol]);
    }
}
