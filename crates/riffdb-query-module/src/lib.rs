#![forbid(unsafe_code)]

//! Immutable, exact-contract query modules with a strict canonical codec.

use std::sync::Arc;

mod application_lock;
mod application_manifest;
mod application_role;
mod application_source;
mod exact_text;
mod exact_text_result_set;
mod generation;
mod go_generation;
mod projection_result_set;
mod python_generation;
mod reactive_module;

pub use application_manifest::{
    APPLICATION_MANIFEST_SCHEMA_V1, APPLICATION_MANIFEST_SCHEMA_V2, APPLICATION_MANIFEST_SCHEMA_V3,
    APPLICATION_MANIFEST_SCHEMA_V4, ApplicationManifest, ApplicationManifestSourceMap,
    MAX_APPLICATION_MANIFEST_BYTES, ManifestContract, ManifestError, ManifestErrorKind,
    ManifestGenerationTargets, ManifestQueryModule, ManifestQuerySource, ManifestReactiveModule,
    ManifestRole, ManifestSpan, ManifestTenantScope,
};
pub use application_role::{
    ApplicationRoleError, ApplicationRoleErrorKind, ApplicationRoleFactSchema,
    ApplicationRoleOperation, ApplicationRoleOperationKind, ApplicationRolePolicy,
    ApplicationRoleSecretOutput, CompiledApplicationRole, compile_application_role,
    compile_application_role_v2,
};
pub use application_source::{
    APPLICATION_SOURCE_SCHEMA_V1, APPLICATION_SOURCE_SCHEMA_V2, APPLICATION_SOURCE_SCHEMA_V3,
    APPLICATION_SOURCE_SCHEMA_V4, APPLICATION_SOURCE_SCHEMA_V5, APPLICATION_SOURCE_SCHEMA_V6,
    ApplicationSourceContract, ApplicationSourceError, ApplicationSourceErrorKind,
    ApplicationSourceGeneration, ApplicationSourceManifest, ApplicationSourceMigration,
    ApplicationSourceQuery, ApplicationSourceQueryModule, ApplicationSourceReactiveModule,
    ApplicationSourceRole, ApplicationSourceTenantScope, MAX_APPLICATION_MIGRATIONS,
    MAX_APPLICATION_SOURCE_BYTES,
};
pub use exact_text::*;
pub use exact_text_result_set::*;
pub use generation::{
    GeneratedMcpCommand, GeneratedMcpReactiveTool, GeneratedMcpTool, McpToolGenerationError,
    generate_mcp_commands, generate_mcp_reactive_tools, generate_mcp_tools,
    generate_rust_application_client, generate_rust_client, generate_sdk_only_query_tools,
    generate_typescript_application_client, generate_typescript_client,
};
pub use go_generation::{generate_go_application_client, generate_go_client};
pub use projection_result_set::*;
pub use python_generation::{
    PythonGenerationError, PythonGenerationLocation, generate_python_application_client,
    generate_python_client,
};
pub use reactive_module::{
    ReactiveArgumentBindingError, ReactiveModuleCompilationError, bind_reactive_arguments,
    canonicalize_reactive_source, compile_reactive_source, decode_and_validate_reactive_module,
    reactive_module_query_dependencies, reactive_query_catalog,
};

use riffdb_contract_ir::ContractBundle;
use riffdb_query_compiler::{
    compile_exact_text_query_v1, compile_operational_query_family, compile_query,
    exact_text_artifact_invariant,
};
use riffdb_query_ir::{
    AuthorizationEntityAccess, NamedQuerySchemas, OperationalQueryFamilyV1,
    QUERY_IR_VERSION_EXACT_RESULT_SET_V1, QUERY_IR_VERSION_OPERATIONAL_AGGREGATE_V1,
    QUERY_IR_VERSION_OPERATIONAL_V1, QUERY_IR_VERSION_SECRET_OUTPUT_V1, QUERY_IR_VERSION_V1,
    QueryAccessProgramV1, QuerySourceMap, SecretOutputRequirement, SourceSymbolKind,
    SymbolicCatalog,
};
use riffdb_riffql_syntax::{
    Document, MAX_IDENTIFIER_BYTES, MAX_SOURCE_BYTES, ParseDiagnostics, RIFFQL_LANGUAGE_VERSION,
    RIFFQL_LANGUAGE_VERSION_EXACT_RESULT_SET_V1, RIFFQL_LANGUAGE_VERSION_OPERATIONAL_V1,
    RIFFQL_LANGUAGE_VERSION_SECRET_OUTPUT_V1, format_query, parse_query,
};
use riffdb_types::{
    ContractBundleHash, ContractLineage, ContractVersion, QueryCostVectorV1, QueryModuleHash,
    QueryOperationName, QueryPlanHash, QuerySourceHash, hash_query_module, hash_query_source,
};
use std::fmt;

pub use riffdb_query_ir::{
    CompiledReactiveOperationV1, ProjectionResultSetPlanV1, ProjectionResultSetPlanV2,
    ReactiveModulePlanV1, ReactiveOperationPlanV1, ReactivePredicateNodeV1, ResultSetOutputShapeV1,
    ResultSetWindowBoundsV2, ResultSetWindowV1,
};
pub use riffdb_types::{QueryModuleName, QueryModuleVersion};

const MODULE_MAGIC: &[u8] = b"RIFFDB-QUERY-MODULE\0";
/// Canonical query-module codec version.
pub const QUERY_MODULE_FORMAT_VERSION_V1: u32 = 1;
/// Additive module codec for finite operational plan families.
pub const QUERY_MODULE_FORMAT_VERSION_OPERATIONAL_V1: u32 = 2;
/// Additive module codec for exact operational aggregate descriptors.
pub const QUERY_MODULE_FORMAT_VERSION_OPERATIONAL_AGGREGATE_V1: u32 = 3;
/// Additive module codec carrying exact secret-output requirements.
pub const QUERY_MODULE_FORMAT_VERSION_SECRET_OUTPUT_V1: u32 = 4;
/// Additive module codec carrying exact whole-result/count/ordinal plans.
pub const QUERY_MODULE_FORMAT_VERSION_EXACT_RESULT_SET_V1: u32 = 5;
/// Maximum queries retained in one immutable module.
pub const MAX_MODULE_QUERIES: usize = 4_096;
/// Maximum canonical bytes for one immutable module.
pub const MAX_QUERY_MODULE_BYTES: usize = 16 * 1_024 * 1_024;
const MAX_COMPILER_ID_BYTES: usize = 64;

/// One bounded named RiffQL document awaiting exact-contract compilation.
#[derive(Clone, Eq, PartialEq)]
pub struct NamedQuerySource {
    name: String,
    source: String,
}

impl NamedQuerySource {
    /// Checks a public operation name and bounded UTF-8 source.
    pub fn new(
        name: impl Into<String>,
        source: impl Into<String>,
    ) -> Result<Self, QueryModuleError> {
        let name = name.into();
        let source = source.into();
        if !valid_name(&name) {
            return Err(QueryModuleError::new(QueryModuleErrorKind::InvalidName));
        }
        if source.is_empty() || source.len() > MAX_SOURCE_BYTES {
            return Err(QueryModuleError::new(QueryModuleErrorKind::LimitExceeded));
        }
        Ok(Self { name, source })
    }

    /// Exact declared query name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Submitted RiffQL source.
    #[must_use]
    pub fn source(&self) -> &str {
        &self.source
    }
}

impl fmt::Debug for NamedQuerySource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NamedQuerySource")
            .field("name", &self.name)
            .field("source", &"[REDACTED]")
            .field("source_bytes", &self.source.len())
            .finish()
    }
}

/// Bounded unordered module input. Compilation establishes canonical query order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryModuleCandidate {
    name: QueryModuleName,
    version: QueryModuleVersion,
    queries: Vec<NamedQuerySource>,
}

impl QueryModuleCandidate {
    /// Checks module-level collection bounds and unique names.
    pub fn new(
        name: QueryModuleName,
        version: QueryModuleVersion,
        mut queries: Vec<NamedQuerySource>,
    ) -> Result<Self, QueryModuleError> {
        if queries.len() > MAX_MODULE_QUERIES {
            return Err(QueryModuleError::new(QueryModuleErrorKind::LimitExceeded));
        }
        queries.sort_by(|left, right| left.name.cmp(&right.name));
        if queries.windows(2).any(|pair| pair[0].name >= pair[1].name) {
            return Err(QueryModuleError::new(QueryModuleErrorKind::DuplicateQuery));
        }
        Ok(Self {
            name,
            version,
            queries,
        })
    }
}

/// One exact compiled named operation in a module.
#[derive(Clone, Eq, PartialEq)]
pub struct CompiledNamedQuery {
    name: String,
    canonical_source: String,
    document: Arc<Document>,
    source_hash: QuerySourceHash,
    plan: CompiledNamedQueryPlan,
}

/// Closed versioned executable plan kind for one named query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CompiledNamedQueryPlan {
    /// Original single bounded access program.
    V1(Arc<QueryAccessProgramV1>),
    /// Finite optional-presence plan family.
    OperationalV1(Arc<OperationalQueryFamilyV1>),
    /// Exact text, whole-count, and ordinal-window result set.
    ExactTextResultV1(Arc<CompiledExactTextResultSetV1>),
}

impl CompiledNamedQueryPlan {
    /// Public typed parameter/result schemas shared by every selectable plan.
    #[must_use]
    pub fn schemas(&self) -> &NamedQuerySchemas {
        match self {
            Self::V1(program) => program.surface().schemas(),
            Self::OperationalV1(family) => family.surface().schemas(),
            Self::ExactTextResultV1(exact) => exact.schemas(),
        }
    }

    /// Exact compiler-derived secret outputs for the query.
    #[must_use]
    pub fn secret_outputs(&self) -> &[SecretOutputRequirement] {
        match self {
            Self::V1(program) => program.surface().secret_outputs(),
            Self::OperationalV1(family) => family.surface().secret_outputs(),
            Self::ExactTextResultV1(exact) => exact.secret_outputs(),
        }
    }

    /// Plan or complete plan-family identity.
    #[must_use]
    pub fn identity(&self) -> QueryPlanHash {
        match self {
            Self::V1(program) => program.identity().hash(),
            Self::OperationalV1(family) => family.identity().hash(),
            Self::ExactTextResultV1(exact) => exact.identity(),
        }
    }

    /// Complete authorization required before runtime member selection.
    #[must_use]
    pub fn authorization(&self) -> &[AuthorizationEntityAccess] {
        match self {
            Self::V1(program) => program.authorization(),
            Self::OperationalV1(family) => family.authorization_union(),
            Self::ExactTextResultV1(exact) => exact.authorization(),
        }
    }

    /// Whole-plan or componentwise whole-family maximum cost.
    #[must_use]
    pub fn cost(&self) -> QueryCostVectorV1 {
        match self {
            Self::V1(program) => program.cost(),
            Self::OperationalV1(family) => family.maximum_cost(),
            Self::ExactTextResultV1(exact) => exact.cost(),
        }
    }

    /// Canonical bytes bound by `identity`.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        match self {
            Self::V1(program) => program.canonical_bytes(),
            Self::OperationalV1(family) => family.canonical_bytes(),
            Self::ExactTextResultV1(exact) => exact.canonical_bytes(),
        }
    }

    /// One member used only for invariant metadata shared by the complete plan.
    ///
    /// Callers must use `CompiledNamedQuery::select_program` for execution.
    #[doc(hidden)]
    #[must_use]
    pub fn representative_program(&self) -> &QueryAccessProgramV1 {
        match self {
            Self::V1(program) => program,
            Self::OperationalV1(family) => family.members()[0].program(),
            Self::ExactTextResultV1(exact) => exact.representative_program(),
        }
    }
}

impl CompiledNamedQuery {
    /// Exact public query name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Canonically formatted RiffQL source.
    #[must_use]
    pub fn canonical_source(&self) -> &str {
        &self.canonical_source
    }

    /// Canonical parsed syntax retained from module validation.
    ///
    /// Named execution uses this immutable document instead of reparsing the
    /// same canonical source for every request.
    #[must_use]
    pub fn document(&self) -> &Document {
        &self.document
    }

    /// Shared canonical parsed syntax retained by the checked module.
    ///
    /// Exact module identity makes the document immutable, so application
    /// execution can retain it without cloning the syntax tree.
    #[must_use]
    pub fn shared_document(&self) -> Arc<Document> {
        Arc::clone(&self.document)
    }

    /// Domain-separated canonical-source hash.
    #[must_use]
    pub const fn source_hash(&self) -> QuerySourceHash {
        self.source_hash
    }

    /// Complete checked executable plan or finite family.
    #[must_use]
    pub const fn plan(&self) -> &CompiledNamedQueryPlan {
        &self.plan
    }

    /// Original V1 program, when this is not an operational family.
    #[must_use]
    pub fn ordinary_program(&self) -> Option<&QueryAccessProgramV1> {
        match &self.plan {
            CompiledNamedQueryPlan::V1(program) => Some(program),
            CompiledNamedQueryPlan::OperationalV1(_) => None,
            CompiledNamedQueryPlan::ExactTextResultV1(_) => None,
        }
    }

    /// Shared original V1 program, when present.
    #[must_use]
    pub fn shared_ordinary_program(&self) -> Option<Arc<QueryAccessProgramV1>> {
        match &self.plan {
            CompiledNamedQueryPlan::V1(program) => Some(Arc::clone(program)),
            CompiledNamedQueryPlan::OperationalV1(_) => None,
            CompiledNamedQueryPlan::ExactTextResultV1(_) => None,
        }
    }

    /// Finite operational family, when present.
    #[must_use]
    pub fn operational_family(&self) -> Option<&OperationalQueryFamilyV1> {
        match &self.plan {
            CompiledNamedQueryPlan::V1(_) => None,
            CompiledNamedQueryPlan::OperationalV1(family) => Some(family),
            CompiledNamedQueryPlan::ExactTextResultV1(_) => None,
        }
    }

    /// Exact result-set plan, when this query uses ADR-0131 execution.
    #[must_use]
    pub fn exact_text_result(&self) -> Option<&CompiledExactTextResultSetV1> {
        match &self.plan {
            CompiledNamedQueryPlan::ExactTextResultV1(exact) => Some(exact),
            CompiledNamedQueryPlan::V1(_) | CompiledNamedQueryPlan::OperationalV1(_) => None,
        }
    }

    /// Shared selected member for exact compiler-ordered presence flags.
    #[must_use]
    pub fn select_program(&self, presence: &[bool]) -> Option<Arc<QueryAccessProgramV1>> {
        match &self.plan {
            CompiledNamedQueryPlan::V1(program) if presence.is_empty() => Some(Arc::clone(program)),
            CompiledNamedQueryPlan::V1(_) => None,
            CompiledNamedQueryPlan::OperationalV1(family) => family
                .select(presence)
                .map(riffdb_query_ir::OperationalPlanMemberV1::shared_program),
            CompiledNamedQueryPlan::ExactTextResultV1(_) => None,
        }
    }

    /// Bounded name-only explanation of the program or complete family.
    #[must_use]
    pub fn explain_lines(&self) -> Vec<String> {
        query_explain_lines(self)
    }
}

impl fmt::Debug for CompiledNamedQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompiledNamedQuery")
            .field("name", &self.name)
            .field("source", &"[REDACTED]")
            .field("source_hash", &self.source_hash)
            .field("plan_hash", &self.plan.identity())
            .finish()
    }
}

/// Exact immutable query module.
#[derive(Clone, Eq, PartialEq)]
pub struct QueryModule {
    name: QueryModuleName,
    version: QueryModuleVersion,
    contract_lineage: ContractLineage,
    contract_version: ContractVersion,
    contract_hash: ContractBundleHash,
    contract_compiler: String,
    queries: Vec<CompiledNamedQuery>,
    canonical_bytes: Vec<u8>,
    identity: QueryModuleHash,
}

impl QueryModule {
    /// Parses, resolves, type checks, plans, and canonically encodes one module.
    pub fn compile(
        candidate: QueryModuleCandidate,
        contract: &ContractBundle,
    ) -> Result<Self, QueryModuleError> {
        let catalog = SymbolicCatalog::from_bundle(contract)
            .map_err(|_| QueryModuleError::new(QueryModuleErrorKind::InvalidContract))?;
        let mut queries = Vec::with_capacity(candidate.queries.len());
        for submitted in candidate.queries {
            let document = parse_query(&submitted.source).map_err(|diagnostics| {
                QueryModuleError::query(
                    submitted.name.clone(),
                    QueryCompilationDiagnostics::Syntax(diagnostics),
                )
            })?;
            if document.name.as_ref().map(|name| name.value.as_str())
                != Some(submitted.name.as_str())
            {
                return Err(QueryModuleError::new(
                    QueryModuleErrorKind::QueryNameMismatch,
                ));
            }
            let canonical_source = format_query(&document);
            let _source_checked_plan =
                compile_document_plan(&document, &catalog).map_err(|diagnostics| {
                    QueryModuleError::query(
                        submitted.name.clone(),
                        QueryCompilationDiagnostics::Planner(diagnostics),
                    )
                })?;
            let canonical_document = parse_query(&canonical_source).map_err(|diagnostics| {
                QueryModuleError::query(
                    submitted.name.clone(),
                    QueryCompilationDiagnostics::Syntax(diagnostics),
                )
            })?;
            let plan =
                compile_document_plan(&canonical_document, &catalog).map_err(|diagnostics| {
                    QueryModuleError::query(
                        submitted.name.clone(),
                        QueryCompilationDiagnostics::Planner(diagnostics),
                    )
                })?;
            queries.push(CompiledNamedQuery {
                name: submitted.name,
                source_hash: hash_query_source(canonical_source.as_bytes()),
                canonical_source,
                document: Arc::new(canonical_document),
                plan,
            });
        }
        let canonical_bytes =
            encode_module(&candidate.name, candidate.version, contract, &queries)?;
        let identity = hash_query_module(&canonical_bytes);
        Ok(Self {
            name: candidate.name,
            version: candidate.version,
            contract_lineage: contract.lineage().clone(),
            contract_version: contract.contract_version(),
            contract_hash: contract.bundle_hash(),
            contract_compiler: contract.compiler_version().to_owned(),
            queries,
            canonical_bytes,
            identity,
        })
    }

    /// Strictly decodes module source, recompiles it against the exact supplied
    /// contract, and byte-compares every persisted semantic field.
    pub fn decode_and_validate(
        bytes: &[u8],
        contract: &ContractBundle,
    ) -> Result<Self, QueryModuleError> {
        if bytes.is_empty() || bytes.len() > MAX_QUERY_MODULE_BYTES {
            return Err(QueryModuleError::new(QueryModuleErrorKind::LimitExceeded));
        }
        let decoded = decode_candidate(bytes)?;
        if decoded.contract_lineage != *contract.lineage()
            || decoded.contract_version != contract.contract_version()
            || decoded.contract_hash != contract.bundle_hash()
            || decoded.contract_compiler != contract.compiler_version()
        {
            return Err(QueryModuleError::new(
                QueryModuleErrorKind::ContractMismatch,
            ));
        }
        let module = Self::compile(decoded.candidate, contract)?;
        if module.canonical_bytes != bytes {
            return Err(QueryModuleError::new(
                QueryModuleErrorKind::IdentityMismatch,
            ));
        }
        Ok(module)
    }

    /// Module name.
    #[must_use]
    pub const fn name(&self) -> &QueryModuleName {
        &self.name
    }

    /// Positive module version.
    #[must_use]
    pub const fn version(&self) -> QueryModuleVersion {
        self.version
    }

    /// Exact contract lineage.
    #[must_use]
    pub const fn contract_lineage(&self) -> &ContractLineage {
        &self.contract_lineage
    }

    /// Exact contract version.
    #[must_use]
    pub const fn contract_version(&self) -> ContractVersion {
        self.contract_version
    }

    /// Exact contract bundle hash.
    #[must_use]
    pub const fn contract_hash(&self) -> ContractBundleHash {
        self.contract_hash
    }

    /// Compiler compatibility identity inherited from the exact bundle.
    #[must_use]
    pub fn contract_compiler(&self) -> &str {
        &self.contract_compiler
    }

    /// Named queries in canonical name order.
    #[must_use]
    pub fn queries(&self) -> &[CompiledNamedQuery] {
        &self.queries
    }

    /// Resolves one exact named query.
    #[must_use]
    pub fn query(&self, name: &str) -> Option<&CompiledNamedQuery> {
        self.queries
            .binary_search_by(|query| query.name.as_str().cmp(name))
            .ok()
            .map(|index| &self.queries[index])
    }

    /// Complete canonical bytes covered by the module identity.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    /// Domain-separated immutable module identity.
    #[must_use]
    pub const fn identity(&self) -> QueryModuleHash {
        self.identity
    }

    /// Canonical module codec selected by its contained plan kinds.
    #[must_use]
    pub fn format_version(&self) -> u32 {
        if self
            .queries
            .iter()
            .any(|query| query.exact_text_result().is_some())
        {
            QUERY_MODULE_FORMAT_VERSION_EXACT_RESULT_SET_V1
        } else if self
            .queries
            .iter()
            .any(|query| !query.plan().secret_outputs().is_empty())
        {
            QUERY_MODULE_FORMAT_VERSION_SECRET_OUTPUT_V1
        } else if self.queries.iter().any(|query| {
            query
                .operational_family()
                .is_some_and(|family| !family.aggregates().is_empty())
        }) {
            QUERY_MODULE_FORMAT_VERSION_OPERATIONAL_AGGREGATE_V1
        } else if self
            .queries
            .iter()
            .any(|query| query.operational_family().is_some())
        {
            QUERY_MODULE_FORMAT_VERSION_OPERATIONAL_V1
        } else {
            QUERY_MODULE_FORMAT_VERSION_V1
        }
    }
}

impl fmt::Debug for QueryModule {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueryModule")
            .field("name", &self.name)
            .field("version", &self.version)
            .field("contract_lineage", &self.contract_lineage)
            .field("contract_version", &self.contract_version)
            .field("contract_hash", &self.contract_hash)
            .field("queries", &self.queries)
            .field("canonical_bytes", &"[REDACTED]")
            .field("identity", &self.identity)
            .finish()
    }
}

/// Stable redaction-safe query-module failure classification.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum QueryModuleErrorKind {
    /// A module or operation name is invalid.
    InvalidName,
    /// A module collection or byte limit was exceeded.
    LimitExceeded,
    /// Query names are repeated.
    DuplicateQuery,
    /// Declared query name and module entry name differ.
    QueryNameMismatch,
    /// Query syntax, resolution, typing, locality, or boundedness failed.
    InvalidQuery,
    /// Exact contract could not supply a symbolic catalog.
    InvalidContract,
    /// Durable bytes are malformed or non-canonical.
    InvalidEncoding,
    /// Persisted exact contract identity does not match.
    ContractMismatch,
    /// Recompilation does not reproduce the persisted semantic bytes.
    IdentityMismatch,
    /// A codec or semantic version is unsupported.
    UnsupportedVersion,
}

/// Detailed value-free source diagnostics retained for an invalid named query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryCompilationDiagnostics {
    /// RiffQL syntax diagnostics.
    Syntax(ParseDiagnostics),
    /// RiffQL resolution, type, locality, cardinality, or plan diagnostics.
    Planner(riffdb_query_compiler::PlannerDiagnostics),
}

/// Redaction-safe query-module failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryModuleError {
    kind: QueryModuleErrorKind,
    query_name: Option<String>,
    diagnostics: Option<QueryCompilationDiagnostics>,
}

impl QueryModuleError {
    const fn new(kind: QueryModuleErrorKind) -> Self {
        Self {
            kind,
            query_name: None,
            diagnostics: None,
        }
    }

    fn query(name: String, diagnostics: QueryCompilationDiagnostics) -> Self {
        Self {
            kind: QueryModuleErrorKind::InvalidQuery,
            query_name: Some(name),
            diagnostics: Some(diagnostics),
        }
    }

    /// Stable failure classification.
    #[must_use]
    pub const fn kind(&self) -> QueryModuleErrorKind {
        self.kind
    }

    /// Named query whose source failed, when applicable.
    #[must_use]
    pub fn query_name(&self) -> Option<&str> {
        self.query_name.as_deref()
    }

    /// Preserved value-free syntax or planner diagnostics, when applicable.
    #[must_use]
    pub const fn diagnostics(&self) -> Option<&QueryCompilationDiagnostics> {
        self.diagnostics.as_ref()
    }
}

impl fmt::Display for QueryModuleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            QueryModuleErrorKind::InvalidName => "query module name is invalid",
            QueryModuleErrorKind::LimitExceeded => "query module limit exceeded",
            QueryModuleErrorKind::DuplicateQuery => "query module repeats a query name",
            QueryModuleErrorKind::QueryNameMismatch => "query declaration name does not match",
            QueryModuleErrorKind::InvalidQuery => "query module contains an invalid query",
            QueryModuleErrorKind::InvalidContract => "query module contract is invalid",
            QueryModuleErrorKind::InvalidEncoding => "query module encoding is invalid",
            QueryModuleErrorKind::ContractMismatch => "query module contract identity differs",
            QueryModuleErrorKind::IdentityMismatch => "query module identity does not reproduce",
            QueryModuleErrorKind::UnsupportedVersion => "query module version is unsupported",
        })
    }
}

impl std::error::Error for QueryModuleError {}

fn valid_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_IDENTIFIER_BYTES
        && value.is_ascii()
        && value.bytes().enumerate().all(|(index, byte)| {
            byte == b'_' || byte.is_ascii_alphanumeric() && (index > 0 || !byte.is_ascii_digit())
        })
}

fn compile_document_plan(
    document: &Document,
    catalog: &SymbolicCatalog,
) -> Result<CompiledNamedQueryPlan, riffdb_query_compiler::PlannerDiagnostics> {
    if document.language_version == RIFFQL_LANGUAGE_VERSION_EXACT_RESULT_SET_V1 {
        let compiled = compile_exact_text_query_v1(document, catalog)?;
        let name = document
            .name
            .as_ref()
            .and_then(|name| QueryOperationName::new(name.value.as_str()).ok())
            .ok_or_else(|| {
                exact_text_artifact_invariant(
                    document
                        .name
                        .as_ref()
                        .map_or(riffdb_riffql_syntax::Span { start: 0, end: 0 }, |name| {
                            name.span
                        }),
                )
            })?;
        let binding = ExactTextResultSetBindingV1::new(name, compiled.family, compiled.plan)
            .map_err(|_| {
                exact_text_artifact_invariant(
                    document
                        .name
                        .as_ref()
                        .map_or(riffdb_riffql_syntax::Span { start: 0, end: 0 }, |name| {
                            name.span
                        }),
                )
            })?;
        let exact = CompiledExactTextResultSetV1::new(
            binding,
            compiled.metadata,
            compiled.operator,
            compiled.order,
            compiled.needle_parameter,
            compiled.limit_parameter,
            compiled.offset_parameter,
        )
        .map_err(|_| {
            exact_text_artifact_invariant(
                document
                    .name
                    .as_ref()
                    .map_or(riffdb_riffql_syntax::Span { start: 0, end: 0 }, |name| {
                        name.span
                    }),
            )
        })?;
        Ok(CompiledNamedQueryPlan::ExactTextResultV1(Arc::new(exact)))
    } else if document.language_version == RIFFQL_LANGUAGE_VERSION {
        compile_query(document, catalog)
            .map(|program| CompiledNamedQueryPlan::V1(Arc::new(program)))
    } else {
        compile_operational_query_family(document, catalog)
            .map(|family| CompiledNamedQueryPlan::OperationalV1(Arc::new(family)))
    }
}

fn query_source_map(query: &CompiledNamedQuery) -> &QuerySourceMap {
    match query.plan() {
        CompiledNamedQueryPlan::V1(program) => program.surface().source_map(),
        CompiledNamedQueryPlan::OperationalV1(family) => family.surface().source_map(),
        CompiledNamedQueryPlan::ExactTextResultV1(exact) => exact.source_map(),
    }
}

fn query_explain_lines(query: &CompiledNamedQuery) -> Vec<String> {
    let mut lines = match query.plan() {
        CompiledNamedQueryPlan::V1(program) => program.explain().lines().to_vec(),
        CompiledNamedQueryPlan::OperationalV1(family) => {
            let mut lines = vec![
                format!(
                    "operational.presence_parameters={}",
                    family.presence_parameters().join(",")
                ),
                format!("operational.members={}", family.members().len()),
            ];
            for aggregate in family.aggregates() {
                lines.push(format!(
                    "operational.aggregate.{}.source={}",
                    aggregate.name(),
                    aggregate.source_binding()
                ));
                lines.push(format!(
                    "operational.aggregate.{}.maximum_groups={}",
                    aggregate.name(),
                    match aggregate.maximum_groups() {
                        riffdb_query_ir::PageBound::Literal(value) => value.to_string(),
                        riffdb_query_ir::PageBound::Parameter(name) => format!("${name}"),
                    }
                ));
                for measure in aggregate.measures() {
                    lines.push(format!(
                        "operational.aggregate.{}.measure.{}={}",
                        aggregate.name(),
                        measure.alias(),
                        match measure.function() {
                            riffdb_query_ir::OperationalAggregateFunctionV1::Count => "count",
                            riffdb_query_ir::OperationalAggregateFunctionV1::Sum => "sum",
                            riffdb_query_ir::OperationalAggregateFunctionV1::Min => "min",
                            riffdb_query_ir::OperationalAggregateFunctionV1::Max => "max",
                        }
                    ));
                }
            }
            for member in family.members() {
                for line in member.program().explain().lines() {
                    lines.push(format!(
                        "operational.member.{}.{}",
                        member.presence_mask(),
                        line
                    ));
                }
            }
            lines
        }
        CompiledNamedQueryPlan::ExactTextResultV1(exact) => vec![
            format!("exact_text.operator={:?}", exact.operator()),
            format!("exact_text.order={:?}", exact.order()),
            format!("exact_text.needle_parameter=${}", exact.needle_parameter()),
            format!("exact_text.limit_parameter=${}", exact.limit_parameter()),
            format!("exact_text.offset_parameter=${}", exact.offset_parameter()),
            "exact_text.measure=exact_count".to_owned(),
        ],
    };
    lines.extend(query.plan().secret_outputs().iter().map(|requirement| {
        format!(
            "secret_output.{}={}.{}",
            requirement.result_path().join("."),
            requirement.entity(),
            requirement.field()
        )
    }));
    lines
}

fn encode_module(
    name: &QueryModuleName,
    version: QueryModuleVersion,
    contract: &ContractBundle,
    queries: &[CompiledNamedQuery],
) -> Result<Vec<u8>, QueryModuleError> {
    let operational = queries
        .iter()
        .any(|query| matches!(query.plan(), CompiledNamedQueryPlan::OperationalV1(_)));
    let exact_result = queries
        .iter()
        .any(|query| matches!(query.plan(), CompiledNamedQueryPlan::ExactTextResultV1(_)));
    let operational_aggregate = queries.iter().any(|query| {
        query
            .operational_family()
            .is_some_and(|family| !family.aggregates().is_empty())
    });
    let secret_output = queries
        .iter()
        .any(|query| !query.plan().secret_outputs().is_empty());
    let format_version = if exact_result {
        QUERY_MODULE_FORMAT_VERSION_EXACT_RESULT_SET_V1
    } else if secret_output {
        QUERY_MODULE_FORMAT_VERSION_SECRET_OUTPUT_V1
    } else if operational_aggregate {
        QUERY_MODULE_FORMAT_VERSION_OPERATIONAL_AGGREGATE_V1
    } else if operational {
        QUERY_MODULE_FORMAT_VERSION_OPERATIONAL_V1
    } else {
        QUERY_MODULE_FORMAT_VERSION_V1
    };
    let mut output = Vec::new();
    output.extend_from_slice(MODULE_MAGIC);
    output.extend_from_slice(&format_version.to_be_bytes());
    write_text(&mut output, name.as_str())?;
    output.extend_from_slice(&version.get().to_be_bytes());
    write_text(&mut output, contract.lineage().as_str())?;
    output.extend_from_slice(&contract.contract_version().get().to_be_bytes());
    output.extend_from_slice(contract.bundle_hash().as_bytes());
    output.extend_from_slice(
        &if exact_result {
            RIFFQL_LANGUAGE_VERSION_EXACT_RESULT_SET_V1
        } else if secret_output {
            RIFFQL_LANGUAGE_VERSION_SECRET_OUTPUT_V1
        } else if operational {
            RIFFQL_LANGUAGE_VERSION_OPERATIONAL_V1
        } else {
            RIFFQL_LANGUAGE_VERSION
        }
        .to_be_bytes(),
    );
    output.extend_from_slice(
        &if exact_result {
            QUERY_IR_VERSION_EXACT_RESULT_SET_V1
        } else if secret_output {
            QUERY_IR_VERSION_SECRET_OUTPUT_V1
        } else if operational_aggregate {
            QUERY_IR_VERSION_OPERATIONAL_AGGREGATE_V1
        } else if operational {
            QUERY_IR_VERSION_OPERATIONAL_V1
        } else {
            QUERY_IR_VERSION_V1
        }
        .to_be_bytes(),
    );
    write_text(&mut output, contract.compiler_version())?;
    write_count(&mut output, queries.len())?;
    for query in queries {
        write_text(&mut output, query.name())?;
        write_bytes(&mut output, query.canonical_source().as_bytes())?;
        output.extend_from_slice(query.source_hash().as_bytes());
        if operational || secret_output || exact_result {
            output.push(match query.plan() {
                CompiledNamedQueryPlan::V1(_) => 1,
                CompiledNamedQueryPlan::OperationalV1(_) => 2,
                CompiledNamedQueryPlan::ExactTextResultV1(_) => 3,
            });
        }
        write_bytes(&mut output, query.plan().canonical_bytes())?;
        output.extend_from_slice(query.plan().identity().as_bytes());
        let explain = query_explain_lines(query);
        write_count(&mut output, explain.len())?;
        for line in &explain {
            write_text(&mut output, line)?;
        }
        let entries = query_source_map(query).entries();
        write_count(&mut output, entries.len())?;
        for entry in entries {
            output.extend_from_slice(&entry.span().start.to_be_bytes());
            output.extend_from_slice(&entry.span().end.to_be_bytes());
            output.push(source_kind_tag(entry.kind()));
            write_count(&mut output, entry.symbolic_path().len())?;
            for component in entry.symbolic_path() {
                write_text(&mut output, component)?;
            }
        }
    }
    if output.len() > MAX_QUERY_MODULE_BYTES {
        return Err(QueryModuleError::new(QueryModuleErrorKind::LimitExceeded));
    }
    Ok(output)
}

struct DecodedCandidate {
    candidate: QueryModuleCandidate,
    contract_lineage: ContractLineage,
    contract_version: ContractVersion,
    contract_hash: ContractBundleHash,
    contract_compiler: String,
}

fn decode_candidate(bytes: &[u8]) -> Result<DecodedCandidate, QueryModuleError> {
    let mut input = Reader::new(bytes);
    input.exact(MODULE_MAGIC)?;
    let format_version = input.u32()?;
    if !matches!(
        format_version,
        QUERY_MODULE_FORMAT_VERSION_V1
            | QUERY_MODULE_FORMAT_VERSION_OPERATIONAL_V1
            | QUERY_MODULE_FORMAT_VERSION_OPERATIONAL_AGGREGATE_V1
            | QUERY_MODULE_FORMAT_VERSION_SECRET_OUTPUT_V1
            | QUERY_MODULE_FORMAT_VERSION_EXACT_RESULT_SET_V1
    ) {
        return Err(QueryModuleError::new(
            QueryModuleErrorKind::UnsupportedVersion,
        ));
    }
    let name = QueryModuleName::new(input.text(MAX_IDENTIFIER_BYTES)?)
        .map_err(|_| QueryModuleError::new(QueryModuleErrorKind::InvalidEncoding))?;
    let version = QueryModuleVersion::new(input.u64()?)
        .ok_or_else(|| QueryModuleError::new(QueryModuleErrorKind::InvalidEncoding))?;
    let contract_lineage = ContractLineage::new(input.text(MAX_IDENTIFIER_BYTES)?)
        .map_err(|_| QueryModuleError::new(QueryModuleErrorKind::InvalidEncoding))?;
    let contract_version = ContractVersion::new(input.u64()?)
        .ok_or_else(|| QueryModuleError::new(QueryModuleErrorKind::InvalidEncoding))?;
    let contract_hash = ContractBundleHash::from_bytes(input.array()?);
    let language_version = input.u32()?;
    let ir_version = input.u32()?;
    let versions_match = match format_version {
        QUERY_MODULE_FORMAT_VERSION_V1 => {
            language_version == RIFFQL_LANGUAGE_VERSION && ir_version == QUERY_IR_VERSION_V1
        }
        QUERY_MODULE_FORMAT_VERSION_OPERATIONAL_V1 => {
            language_version == RIFFQL_LANGUAGE_VERSION_OPERATIONAL_V1
                && ir_version == QUERY_IR_VERSION_OPERATIONAL_V1
        }
        QUERY_MODULE_FORMAT_VERSION_OPERATIONAL_AGGREGATE_V1 => {
            language_version == RIFFQL_LANGUAGE_VERSION_OPERATIONAL_V1
                && ir_version == QUERY_IR_VERSION_OPERATIONAL_AGGREGATE_V1
        }
        QUERY_MODULE_FORMAT_VERSION_SECRET_OUTPUT_V1 => {
            language_version == RIFFQL_LANGUAGE_VERSION_SECRET_OUTPUT_V1
                && ir_version == QUERY_IR_VERSION_SECRET_OUTPUT_V1
        }
        QUERY_MODULE_FORMAT_VERSION_EXACT_RESULT_SET_V1 => {
            language_version == RIFFQL_LANGUAGE_VERSION_EXACT_RESULT_SET_V1
                && ir_version == QUERY_IR_VERSION_EXACT_RESULT_SET_V1
        }
        _ => false,
    };
    if !versions_match {
        return Err(QueryModuleError::new(
            QueryModuleErrorKind::UnsupportedVersion,
        ));
    }
    let contract_compiler = input.text(MAX_COMPILER_ID_BYTES)?;
    let query_count = input.count(MAX_MODULE_QUERIES)?;
    if query_count == 0 {
        return Err(QueryModuleError::new(QueryModuleErrorKind::InvalidEncoding));
    }
    let mut queries = Vec::with_capacity(query_count);
    for _ in 0..query_count {
        let query_name = input.text(MAX_IDENTIFIER_BYTES)?;
        let source = input.text(MAX_SOURCE_BYTES)?;
        input.skip(32)?;
        if format_version != QUERY_MODULE_FORMAT_VERSION_V1 && !matches!(input.u8()?, 1 | 2 | 3) {
            return Err(QueryModuleError::new(QueryModuleErrorKind::InvalidEncoding));
        }
        input.bytes(riffdb_query_ir::MAX_QUERY_ARTIFACT_BYTES)?;
        input.skip(32)?;
        for _ in 0..input.count(riffdb_query_ir::MAX_SOURCE_MAP_ENTRIES)? {
            input.text(MAX_SOURCE_BYTES)?;
        }
        for _ in 0..input.count(riffdb_query_ir::MAX_SOURCE_MAP_ENTRIES)? {
            input.u32()?;
            input.u32()?;
            let tag = input.u8()?;
            let maximum_tag = match format_version {
                QUERY_MODULE_FORMAT_VERSION_SECRET_OUTPUT_V1
                | QUERY_MODULE_FORMAT_VERSION_EXACT_RESULT_SET_V1 => 10,
                QUERY_MODULE_FORMAT_VERSION_OPERATIONAL_AGGREGATE_V1 => 9,
                _ => 7,
            };
            if !(1..=maximum_tag).contains(&tag) {
                return Err(QueryModuleError::new(QueryModuleErrorKind::InvalidEncoding));
            }
            for _ in 0..input.count(128)? {
                input.text(MAX_IDENTIFIER_BYTES)?;
            }
        }
        queries.push(NamedQuerySource::new(query_name, source)?);
    }
    input.finish()?;
    let candidate = QueryModuleCandidate::new(name, version, queries)?;
    Ok(DecodedCandidate {
        candidate,
        contract_lineage,
        contract_version,
        contract_hash,
        contract_compiler,
    })
}

fn source_kind_tag(kind: SourceSymbolKind) -> u8 {
    match kind {
        SourceSymbolKind::Parameter => 1,
        SourceSymbolKind::Entity => 2,
        SourceSymbolKind::Field => 3,
        SourceSymbolKind::Enum => 4,
        SourceSymbolKind::EnumVariant => 5,
        SourceSymbolKind::Binding => 6,
        SourceSymbolKind::ResultField => 7,
        SourceSymbolKind::Aggregate => 8,
        SourceSymbolKind::AggregateMeasure => 9,
        SourceSymbolKind::SecretOutput => 10,
    }
}

fn write_count(output: &mut Vec<u8>, value: usize) -> Result<(), QueryModuleError> {
    output.extend_from_slice(
        &u32::try_from(value)
            .map_err(|_| QueryModuleError::new(QueryModuleErrorKind::LimitExceeded))?
            .to_be_bytes(),
    );
    Ok(())
}

fn write_text(output: &mut Vec<u8>, value: &str) -> Result<(), QueryModuleError> {
    write_bytes(output, value.as_bytes())
}

fn write_bytes(output: &mut Vec<u8>, value: &[u8]) -> Result<(), QueryModuleError> {
    write_count(output, value.len())?;
    output.extend_from_slice(value);
    Ok(())
}

struct Reader<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    const fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], QueryModuleError> {
        let end = self
            .offset
            .checked_add(length)
            .filter(|end| *end <= self.input.len())
            .ok_or_else(|| QueryModuleError::new(QueryModuleErrorKind::InvalidEncoding))?;
        let value = &self.input[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn exact(&mut self, expected: &[u8]) -> Result<(), QueryModuleError> {
        if self.take(expected.len())? == expected {
            Ok(())
        } else {
            Err(QueryModuleError::new(QueryModuleErrorKind::InvalidEncoding))
        }
    }

    fn u8(&mut self) -> Result<u8, QueryModuleError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, QueryModuleError> {
        Ok(u32::from_be_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, QueryModuleError> {
        Ok(u64::from_be_bytes(self.array()?))
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], QueryModuleError> {
        self.take(N)?
            .try_into()
            .map_err(|_| QueryModuleError::new(QueryModuleErrorKind::InvalidEncoding))
    }

    fn count(&mut self, maximum: usize) -> Result<usize, QueryModuleError> {
        let value = usize::try_from(self.u32()?)
            .map_err(|_| QueryModuleError::new(QueryModuleErrorKind::InvalidEncoding))?;
        if value > maximum {
            return Err(QueryModuleError::new(QueryModuleErrorKind::LimitExceeded));
        }
        Ok(value)
    }

    fn bytes(&mut self, maximum: usize) -> Result<&'a [u8], QueryModuleError> {
        let length = self.count(maximum)?;
        self.take(length)
    }

    fn text(&mut self, maximum: usize) -> Result<String, QueryModuleError> {
        let bytes = self.bytes(maximum)?;
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|_| QueryModuleError::new(QueryModuleErrorKind::InvalidEncoding))
    }

    fn skip(&mut self, length: usize) -> Result<(), QueryModuleError> {
        self.take(length).map(|_| ())
    }

    fn finish(self) -> Result<(), QueryModuleError> {
        if self.offset == self.input.len() {
            Ok(())
        } else {
            Err(QueryModuleError::new(QueryModuleErrorKind::InvalidEncoding))
        }
    }
}
pub use application_lock::{
    APPLICATION_LOCK_SCHEMA_V1, APPLICATION_LOCK_SCHEMA_V2, APPLICATION_LOCK_SCHEMA_V3,
    APPLICATION_LOCK_SCHEMA_V4, APPLICATION_LOCK_SCHEMA_V5, APPLICATION_LOCK_SCHEMA_V6,
    APPLICATION_LOCK_SCHEMA_V7, APPLICATION_ROLE_DEFINITION_FORMAT_V1,
    APPLICATION_ROLE_DEFINITION_FORMAT_V2, APPLICATION_ROLE_DEFINITION_FORMAT_V3, ApplicationLock,
    ApplicationLockError, ApplicationLockErrorKind, ApplicationMigrationLockInput,
    CONTRACT_BUNDLE_ARTIFACT_PATH, GeneratedApplicationArtifact, GeneratedApplicationArtifactKind,
    LockedApplicationMigration, MAX_APPLICATION_LOCK_BYTES,
};
