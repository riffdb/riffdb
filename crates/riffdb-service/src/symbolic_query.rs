//! Symbolic RiffQL application operations over the shared service boundary.

use std::collections::BTreeMap;
use std::num::NonZeroU16;
use std::sync::Arc;

use riffdb_catalog::{
    ActiveQueryModuleExpectation, PreparedQueryModuleActivation, ValidatedQueryModule,
};
use riffdb_commit::{
    ControlPlaneExecutionErrorKind, ControlPlaneTerminalAudit,
    QueryModuleDeploymentOutcome as CoordinatorQueryModuleDeploymentOutcome,
    QueryModuleDeploymentPreparation,
};
use riffdb_contract_ir::{ValueType, ValueTypeTag};
use riffdb_errors::{
    ApplicationErrorCode, PublicError, ValidationCode, ValidationIssue, ValidationIssues,
    ValidationPath,
};
use riffdb_policy::{
    ApplicationQueryAccessRequirement, ApplicationQueryTarget, AuthorizedApplicationQuery,
    OperationRequest, OperationTenantScope, OutputClassification, PartitionConstraint,
};
use riffdb_query_compiler::{PlannerDiagnostic, compile_query};
use riffdb_query_executor::{
    QueryContinuation, QueryExecutionError, QueryOwnedSnapshot, QueryParameters, QueryResultValue,
    QueryRow,
};
use riffdb_query_ir::{
    NamedTypeSchema, QueryAccessKind, QueryAccessProgramV1, QueryDiagnostic, SymbolicCatalog,
    resolve_query_surface,
};
use riffdb_query_module::{NamedQuerySource, QueryModuleCandidate};
use riffdb_riffql_syntax::{
    Document, MAX_IDENTIFIER_BYTES, ParseDiagnostic, Span, TypeReference, parse_query,
};
use riffdb_types::{
    CanonicalValue, ContractBundleHash, ContractLineage, ContractVersion, QueryModuleHash,
    QueryModuleName, QueryModuleVersion, QueryOperationName, QueryPlanHash, ServiceAuditLinkV1,
    ServiceAuditPhaseV1, ServiceOperationV1, encode_canonical_value, hash_query_parameters,
};

use crate::command_operations::{SubmittedValueMaterializationError, materialize_submitted_value};
use crate::orchestration::AuditScope;
use crate::query_discovery_operations::{
    finish_failure, finish_success, prepare_selected_contract,
};
use crate::wait::{ControlledWaitError, wait_with_control};
use crate::{
    ContractSelection, CursorAccessError, CursorContractIdentity, CursorToken, InternalDefect,
    QueryCursorLookup, QueryCursorState, RequestContext, RiffDbService, RiffDbServiceInner,
    ServiceAuditTargetMap, ServiceFailure, ServiceFuture, ServiceResult, SourceName, SubmittedEnum,
    SubmittedValue,
};

/// Stricter application-surface source ceiling.
pub const MAX_SYMBOLIC_QUERY_SOURCE_BYTES: usize = 262_144;
/// Maximum access steps admitted by one interactive application request.
pub const MAX_SYMBOLIC_QUERY_STEPS: usize = 64;

/// Bounded name-addressed values awaiting query-schema materialization.
#[derive(Clone, Eq, PartialEq)]
pub struct SymbolicQueryParameters(BTreeMap<String, SubmittedValue>);

impl SymbolicQueryParameters {
    /// Checks unique nonempty names and the query parameter ceiling.
    pub fn new(values: BTreeMap<String, SubmittedValue>) -> Result<Self, SymbolicQueryInputError> {
        if values.len() > riffdb_query_executor::MAX_QUERY_PARAMETERS {
            return Err(SymbolicQueryInputError::TooLong);
        }
        if values.keys().any(String::is_empty) {
            return Err(SymbolicQueryInputError::Empty);
        }
        Ok(Self(values))
    }

    /// Iterates values in canonical name order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&str, &SubmittedValue)> {
        self.0.iter().map(|(name, value)| (name.as_str(), value))
    }

    fn get(&self, name: &str) -> Option<&SubmittedValue> {
        self.0.get(name)
    }
}

impl std::fmt::Debug for SymbolicQueryParameters {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SymbolicQueryParameters")
            .field("names", &self.0.keys().collect::<Vec<_>>())
            .field("values", &"[REDACTED]")
            .finish()
    }
}

/// Bounded UTF-8 RiffQL source.
#[derive(Clone, Eq, PartialEq)]
pub struct SymbolicQuerySource(String);

impl SymbolicQuerySource {
    /// Checks the application source ceiling.
    pub fn new(source: String) -> Result<Self, SymbolicQueryInputError> {
        if source.is_empty() {
            return Err(SymbolicQueryInputError::Empty);
        }
        if source.len() > MAX_SYMBOLIC_QUERY_SOURCE_BYTES {
            return Err(SymbolicQueryInputError::TooLong);
        }
        Ok(Self(source))
    }

    /// Borrows exact submitted source.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for SymbolicQuerySource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SymbolicQuerySource")
            .field("bytes", &self.0.len())
            .field("source", &"[REDACTED]")
            .finish()
    }
}

/// Structural application-query input rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SymbolicQueryInputError {
    /// Required input was empty.
    Empty,
    /// A fixed byte or item ceiling was exceeded.
    TooLong,
}

impl std::fmt::Display for SymbolicQueryInputError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Empty => "symbolic query input is empty",
            Self::TooLong => "symbolic query input exceeds its fixed limit",
        })
    }
}

impl std::error::Error for SymbolicQueryInputError {}

/// Contract selection plus an optional exact content-hash assertion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SymbolicContractSelector {
    selection: ContractSelection,
    expected_hash: Option<ContractBundleHash>,
}

impl SymbolicContractSelector {
    /// Selects the transaction-current active contract.
    #[must_use]
    pub const fn active() -> Self {
        Self {
            selection: ContractSelection::Active,
            expected_hash: None,
        }
    }

    /// Selects one immutable lineage/version and asserts its canonical hash.
    #[must_use]
    pub const fn exact(
        lineage: ContractLineage,
        version: ContractVersion,
        expected_hash: ContractBundleHash,
    ) -> Self {
        Self {
            selection: ContractSelection::Exact { lineage, version },
            expected_hash: Some(expected_hash),
        }
    }

    /// Selects through an existing trusted service selector.
    #[must_use]
    pub const fn from_selection(selection: ContractSelection) -> Self {
        Self {
            selection,
            expected_hash: None,
        }
    }

    fn selection(&self) -> &ContractSelection {
        &self.selection
    }

    fn matches(&self, bundle: &riffdb_catalog::ValidatedContractBundle) -> bool {
        self.expected_hash
            .is_none_or(|expected| expected == bundle.bundle_hash())
    }
}

/// One redaction-safe source diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SymbolicDiagnostic {
    code: String,
    summary: String,
    span: Span,
    symbols: Vec<String>,
    suggestion: Option<String>,
}

impl SymbolicDiagnostic {
    /// Stable diagnostic code.
    #[must_use]
    pub fn code(&self) -> &str {
        &self.code
    }

    /// Value-free summary.
    #[must_use]
    pub fn summary(&self) -> &str {
        &self.summary
    }

    /// Half-open UTF-8 source span.
    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }

    /// Bounded symbolic path.
    #[must_use]
    pub fn symbols(&self) -> &[String] {
        &self.symbols
    }

    /// Optional safe remediation.
    #[must_use]
    pub fn suggestion(&self) -> Option<&str> {
        self.suggestion.as_deref()
    }
}

/// Exact compiled query identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SymbolicQueryIdentity {
    lineage: ContractLineage,
    version: ContractVersion,
    bundle_hash: ContractBundleHash,
    name: Option<String>,
    plan_hash: QueryPlanHash,
    module_hash: Option<QueryModuleHash>,
}

impl SymbolicQueryIdentity {
    fn from_program(program: &QueryAccessProgramV1) -> Self {
        Self {
            lineage: program.contract().lineage().clone(),
            version: program.contract().version(),
            bundle_hash: program.contract().bundle_hash(),
            name: program.name().map(str::to_owned),
            plan_hash: program.identity().hash(),
            module_hash: None,
        }
    }

    fn from_named(program: &QueryAccessProgramV1, module_hash: QueryModuleHash) -> Self {
        let mut identity = Self::from_program(program);
        identity.module_hash = Some(module_hash);
        identity
    }

    /// Exact contract lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Exact contract version.
    #[must_use]
    pub const fn version(&self) -> ContractVersion {
        self.version
    }

    /// Exact contract bundle hash.
    #[must_use]
    pub const fn bundle_hash(&self) -> ContractBundleHash {
        self.bundle_hash
    }

    /// Optional declared query name.
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Canonical compiled-plan hash.
    #[must_use]
    pub const fn plan_hash(&self) -> QueryPlanHash {
        self.plan_hash
    }

    /// Exact immutable module identity for named execution.
    #[must_use]
    pub const fn module_hash(&self) -> Option<QueryModuleHash> {
        self.module_hash
    }
}

/// Name-addressed query schema.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SymbolicQuerySchema {
    parameters: Vec<String>,
    outcomes: Vec<String>,
    result_fields: Vec<String>,
}

impl SymbolicQuerySchema {
    fn from_program(program: &QueryAccessProgramV1) -> Self {
        let schemas = program.surface().schemas();
        let mut parameters = schemas
            .parameters()
            .iter()
            .map(|parameter| {
                let optional = if parameter.has_default() {
                    " = default"
                } else {
                    ""
                };
                format!(
                    "${}: {}{}",
                    parameter.name(),
                    render_named_type(parameter.value_type()),
                    optional
                )
            })
            .collect::<Vec<_>>();
        parameters.sort();
        let mut outcomes = schemas
            .results()
            .iter()
            .map(|result| result.name().to_owned())
            .collect::<Vec<_>>();
        outcomes.sort();
        let mut result_fields = schemas
            .results()
            .iter()
            .flat_map(|result| {
                result
                    .fields()
                    .iter()
                    .map(move |field| format!("{}.{}", result.name(), field.name()))
            })
            .collect::<Vec<_>>();
        result_fields.sort();
        result_fields.dedup();
        Self {
            parameters,
            outcomes,
            result_fields,
        }
    }

    /// Parameter declarations in source order.
    #[must_use]
    pub fn parameters(&self) -> &[String] {
        &self.parameters
    }

    /// Declared outcomes in source order.
    #[must_use]
    pub fn outcomes(&self) -> &[String] {
        &self.outcomes
    }

    /// Qualified result fields in canonical order.
    #[must_use]
    pub fn result_fields(&self) -> &[String] {
        &self.result_fields
    }
}

/// Successful compilation descriptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckedSymbolicQuery {
    identity: SymbolicQueryIdentity,
    schema: SymbolicQuerySchema,
}

impl CheckedSymbolicQuery {
    fn from_program(program: &QueryAccessProgramV1) -> Self {
        Self {
            identity: SymbolicQueryIdentity::from_program(program),
            schema: SymbolicQuerySchema::from_program(program),
        }
    }

    fn from_named(program: &QueryAccessProgramV1, module_hash: QueryModuleHash) -> Self {
        Self {
            identity: SymbolicQueryIdentity::from_named(program, module_hash),
            schema: SymbolicQuerySchema::from_program(program),
        }
    }

    /// Exact query identity.
    #[must_use]
    pub const fn identity(&self) -> &SymbolicQueryIdentity {
        &self.identity
    }

    /// Name-addressed schema.
    #[must_use]
    pub const fn schema(&self) -> &SymbolicQuerySchema {
        &self.schema
    }
}

/// Query-check response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CheckSymbolicQueryResult {
    /// Fully resolved, typed, local, authorized-access-derived bounded plan.
    Valid(Box<CheckedSymbolicQuery>),
    /// Bounded compiler diagnostics; no partial program escaped.
    Invalid(Vec<SymbolicDiagnostic>),
}

/// Query-explain response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExplainSymbolicQueryResult {
    /// Fully checked identity, schema, and name-only plan.
    Valid {
        /// Query descriptor.
        query: Box<CheckedSymbolicQuery>,
        /// Stable bounded explain lines.
        lines: Vec<String>,
    },
    /// Bounded compiler diagnostics.
    Invalid(Vec<SymbolicDiagnostic>),
}

/// Symbolic contract catalog result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeSymbolicContractResult {
    lineage: ContractLineage,
    version: ContractVersion,
    bundle_hash: ContractBundleHash,
    catalog: String,
}

impl DescribeSymbolicContractResult {
    /// Exact contract lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Exact contract version.
    #[must_use]
    pub const fn version(&self) -> ContractVersion {
        self.version
    }

    /// Exact contract hash.
    #[must_use]
    pub const fn bundle_hash(&self) -> ContractBundleHash {
        self.bundle_hash
    }

    /// Deterministic name-only catalog text.
    #[must_use]
    pub fn catalog(&self) -> &str {
        &self.catalog
    }
}

/// Request shared by check and explain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompileSymbolicQueryRequest {
    contract: SymbolicContractSelector,
    source: SymbolicQuerySource,
}

impl CompileSymbolicQueryRequest {
    /// Constructs one exact query compilation request.
    #[must_use]
    pub const fn new(contract: SymbolicContractSelector, source: SymbolicQuerySource) -> Self {
        Self { contract, source }
    }

    /// Contract selector.
    #[must_use]
    pub const fn contract(&self) -> &SymbolicContractSelector {
        &self.contract
    }

    /// Exact source.
    #[must_use]
    pub const fn source(&self) -> &SymbolicQuerySource {
        &self.source
    }
}

/// Caller-supplied transaction-current module-pointer expectation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryModuleActiveExpectation {
    /// Replace any transaction-current pointer.
    Any,
    /// Require no active module for the exact contract.
    Absent,
    /// Require one exact active module identity.
    Exact(QueryModuleHash),
}

impl QueryModuleActiveExpectation {
    const fn lower(self) -> ActiveQueryModuleExpectation {
        match self {
            Self::Any => ActiveQueryModuleExpectation::Any,
            Self::Absent => ActiveQueryModuleExpectation::Absent,
            Self::Exact(hash) => ActiveQueryModuleExpectation::Exact(hash),
        }
    }
}

/// Checked immutable query-module deployment request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeployQueryModuleRequest {
    contract: SymbolicContractSelector,
    candidate: QueryModuleCandidate,
    expectation: QueryModuleActiveExpectation,
}

impl DeployQueryModuleRequest {
    /// Groups exact contract selection, bounded module source, and CAS expectation.
    #[must_use]
    pub const fn new(
        contract: SymbolicContractSelector,
        candidate: QueryModuleCandidate,
        expectation: QueryModuleActiveExpectation,
    ) -> Self {
        Self {
            contract,
            candidate,
            expectation,
        }
    }

    /// Checks raw symbolic names, positive version, and named source bounds.
    pub fn from_sources(
        contract: SymbolicContractSelector,
        module_name: String,
        module_version: u64,
        queries: Vec<(String, String)>,
        expectation: QueryModuleActiveExpectation,
    ) -> Result<Self, SymbolicQueryInputError> {
        let name =
            QueryModuleName::new(module_name).map_err(|_| SymbolicQueryInputError::TooLong)?;
        let version =
            QueryModuleVersion::new(module_version).ok_or(SymbolicQueryInputError::Empty)?;
        let queries = queries
            .into_iter()
            .map(|(name, source)| {
                NamedQuerySource::new(name, source).map_err(|_| SymbolicQueryInputError::TooLong)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let candidate = QueryModuleCandidate::new(name, version, queries)
            .map_err(|_| SymbolicQueryInputError::TooLong)?;
        Ok(Self::new(contract, candidate, expectation))
    }
}

/// Name-only immutable module descriptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryModuleDescriptor {
    name: QueryModuleName,
    version: QueryModuleVersion,
    hash: QueryModuleHash,
    contract_lineage: ContractLineage,
    contract_version: ContractVersion,
    contract_hash: ContractBundleHash,
    query_names: Vec<String>,
}

impl QueryModuleDescriptor {
    fn from_module(module: &ValidatedQueryModule) -> Self {
        let module = module.module();
        Self {
            name: module.name().clone(),
            version: module.version(),
            hash: module.identity(),
            contract_lineage: module.contract_lineage().clone(),
            contract_version: module.contract_version(),
            contract_hash: module.contract_hash(),
            query_names: module
                .queries()
                .iter()
                .map(|query| query.name().to_owned())
                .collect(),
        }
    }

    /// Module name.
    #[must_use]
    pub const fn name(&self) -> &QueryModuleName {
        &self.name
    }

    /// Module version.
    #[must_use]
    pub const fn version(&self) -> QueryModuleVersion {
        self.version
    }

    /// Immutable content hash.
    #[must_use]
    pub const fn hash(&self) -> QueryModuleHash {
        self.hash
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

    /// Named operations in canonical order.
    #[must_use]
    pub fn query_names(&self) -> &[String] {
        &self.query_names
    }
}

/// Safe module-deployment response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryModuleDeploymentDisposition {
    /// A new active module committed.
    Activated,
    /// The exact module was already active.
    AlreadyActive,
    /// The active pointer differed from the submitted CAS.
    ExpectedActiveMismatch {
        /// Actual module identity, or absence.
        actual: Option<QueryModuleHash>,
    },
    /// Same module name/version is retained with different content.
    ModuleVersionConflict,
    /// The exact contract ceased to be retained before commit.
    ContractUnavailable,
}

/// Safe module-deployment response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeployQueryModuleResult {
    outcome: QueryModuleDeploymentDisposition,
    module: QueryModuleDescriptor,
}

impl DeployQueryModuleResult {
    /// Closed deployment outcome.
    #[must_use]
    pub const fn outcome(&self) -> &QueryModuleDeploymentDisposition {
        &self.outcome
    }

    /// Submitted immutable module identity.
    #[must_use]
    pub const fn module(&self) -> &QueryModuleDescriptor {
        &self.module
    }
}

/// Active or content-addressed module inspection request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GetQueryModuleRequest {
    contract: SymbolicContractSelector,
    module_hash: Option<QueryModuleHash>,
}

impl GetQueryModuleRequest {
    /// Selects the active module when `module_hash` is absent.
    #[must_use]
    pub const fn new(
        contract: SymbolicContractSelector,
        module_hash: Option<QueryModuleHash>,
    ) -> Self {
        Self {
            contract,
            module_hash,
        }
    }
}

/// Inspected module and its canonical sources.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryModuleInspection {
    descriptor: QueryModuleDescriptor,
    queries: Vec<NamedQuerySource>,
}

impl QueryModuleInspection {
    fn from_module(module: &ValidatedQueryModule) -> Result<Self, SymbolicQueryInputError> {
        let queries = module
            .module()
            .queries()
            .iter()
            .map(|query| {
                NamedQuerySource::new(query.name(), query.canonical_source())
                    .map_err(|_| SymbolicQueryInputError::TooLong)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            descriptor: QueryModuleDescriptor::from_module(module),
            queries,
        })
    }

    /// Immutable module descriptor.
    #[must_use]
    pub const fn descriptor(&self) -> &QueryModuleDescriptor {
        &self.descriptor
    }

    /// Canonical named RiffQL sources.
    #[must_use]
    pub fn queries(&self) -> &[NamedQuerySource] {
        &self.queries
    }
}

/// Named query selection and values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamedSymbolicQueryRequest {
    contract: SymbolicContractSelector,
    query_name: String,
    module_hash: Option<QueryModuleHash>,
    parameters: SymbolicQueryParameters,
    cursor: Option<CursorToken>,
    minimum_application_head: Option<u64>,
}

impl NamedSymbolicQueryRequest {
    /// Constructs active-module or exact-module named execution.
    pub fn new(
        contract: SymbolicContractSelector,
        query_name: String,
        module_hash: Option<QueryModuleHash>,
        parameters: SymbolicQueryParameters,
    ) -> Result<Self, SymbolicQueryInputError> {
        if query_name.is_empty() || query_name.len() > MAX_IDENTIFIER_BYTES {
            return Err(SymbolicQueryInputError::TooLong);
        }
        Ok(Self {
            contract,
            query_name,
            module_hash,
            parameters,
            cursor: None,
            minimum_application_head: None,
        })
    }

    /// Attaches a server-owned cursor.
    #[must_use]
    pub const fn with_cursor(mut self, cursor: CursorToken) -> Self {
        self.cursor = Some(cursor);
        self
    }

    /// Requires a snapshot at or after one positive application sequence.
    #[must_use]
    pub const fn with_minimum_application_head(mut self, minimum: u64) -> Self {
        self.minimum_application_head = Some(minimum);
        self
    }

    /// Minimum authoritative application head required by this read.
    #[must_use]
    pub const fn minimum_application_head(&self) -> Option<u64> {
        self.minimum_application_head
    }
}

/// One ad-hoc symbolic execution request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecuteSymbolicQueryRequest {
    contract: SymbolicContractSelector,
    source: SymbolicQuerySource,
    parameters: SymbolicQueryParameters,
    cursor: Option<CursorToken>,
    minimum_application_head: Option<u64>,
}

impl ExecuteSymbolicQueryRequest {
    /// Constructs one exact ad-hoc request.
    #[must_use]
    pub const fn new(
        contract: SymbolicContractSelector,
        source: SymbolicQuerySource,
        parameters: SymbolicQueryParameters,
    ) -> Self {
        Self {
            contract,
            source,
            parameters,
            cursor: None,
            minimum_application_head: None,
        }
    }

    /// Attaches one opaque continuation token.
    #[must_use]
    pub const fn with_cursor(mut self, cursor: CursorToken) -> Self {
        self.cursor = Some(cursor);
        self
    }

    /// Requires a snapshot at or after one positive application sequence.
    #[must_use]
    pub const fn with_minimum_application_head(mut self, minimum: u64) -> Self {
        self.minimum_application_head = Some(minimum);
        self
    }

    /// Minimum authoritative application head required by this read.
    #[must_use]
    pub const fn minimum_application_head(&self) -> Option<u64> {
        self.minimum_application_head
    }

    /// Contract selector.
    #[must_use]
    pub const fn contract(&self) -> &SymbolicContractSelector {
        &self.contract
    }

    /// Source.
    #[must_use]
    pub const fn source(&self) -> &SymbolicQuerySource {
        &self.source
    }

    /// Canonical name-addressed parameters.
    #[must_use]
    pub const fn parameters(&self) -> &SymbolicQueryParameters {
        &self.parameters
    }

    /// Optional server-owned continuation token.
    #[must_use]
    pub const fn cursor(&self) -> Option<CursorToken> {
        self.cursor
    }
}

/// One name-addressed result record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SymbolicResultRecord {
    entity: String,
    fields: BTreeMap<String, CanonicalValue>,
}

impl SymbolicResultRecord {
    fn from_row(row: &QueryRow) -> Self {
        Self {
            entity: row.entity().to_owned(),
            fields: row
                .fields()
                .map(|(name, value)| (name.to_owned(), value.clone()))
                .collect(),
        }
    }

    /// Contract entity name.
    #[must_use]
    pub fn entity(&self) -> &str {
        &self.entity
    }

    /// Fields in canonical name order.
    #[must_use]
    pub const fn fields(&self) -> &BTreeMap<String, CanonicalValue> {
        &self.fields
    }
}

/// Result field with declared cardinality.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SymbolicResultField {
    /// Exactly one record.
    One(SymbolicResultRecord),
    /// Zero or one record.
    Maybe(Option<SymbolicResultRecord>),
    /// Bounded ordered records.
    Many(Vec<SymbolicResultRecord>),
}

/// Complete one-snapshot execution result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecuteSymbolicQueryResult {
    identity: SymbolicQueryIdentity,
    outcome: String,
    application_head: u64,
    fields: BTreeMap<String, SymbolicResultField>,
    enum_variant_names: BTreeMap<(u32, u32), String>,
    next_cursor: Option<CursorToken>,
}

impl ExecuteSymbolicQueryResult {
    fn from_snapshot(
        program: &QueryAccessProgramV1,
        snapshot: &QueryOwnedSnapshot,
        bundle: &riffdb_contract_ir::ContractBundle,
    ) -> Self {
        let fields = snapshot
            .fields()
            .iter()
            .map(|(name, value)| {
                let value = match value {
                    QueryResultValue::One(row) => {
                        SymbolicResultField::One(SymbolicResultRecord::from_row(row))
                    }
                    QueryResultValue::Maybe(row) => {
                        SymbolicResultField::Maybe(row.as_ref().map(SymbolicResultRecord::from_row))
                    }
                    QueryResultValue::Many(rows) => SymbolicResultField::Many(
                        rows.iter().map(SymbolicResultRecord::from_row).collect(),
                    ),
                };
                (name.clone(), value)
            })
            .collect();
        Self {
            identity: SymbolicQueryIdentity::from_program(program),
            outcome: snapshot.outcome().to_owned(),
            application_head: snapshot.application_head(),
            fields,
            enum_variant_names: bundle
                .schema()
                .enums()
                .iter()
                .flat_map(|enumeration| {
                    enumeration.variants().iter().map(move |variant| {
                        (
                            (enumeration.id().get(), variant.id().get()),
                            variant.name().to_owned(),
                        )
                    })
                })
                .collect(),
            next_cursor: None,
        }
    }

    /// Exact compiled identity.
    #[must_use]
    pub const fn identity(&self) -> &SymbolicQueryIdentity {
        &self.identity
    }

    /// Declared business result branch.
    #[must_use]
    pub fn outcome(&self) -> &str {
        &self.outcome
    }

    /// Snapshot application head.
    #[must_use]
    pub const fn application_head(&self) -> u64 {
        self.application_head
    }

    /// Name-addressed result fields.
    #[must_use]
    pub const fn fields(&self) -> &BTreeMap<String, SymbolicResultField> {
        &self.fields
    }

    /// Resolves a canonical enum identity to its contract source name.
    #[must_use]
    pub fn enum_variant_name(&self, type_id: u32, variant_id: u32) -> Option<&str> {
        self.enum_variant_names
            .get(&(type_id, variant_id))
            .map(String::as_str)
    }

    /// Opaque continuation token, when this page is not final.
    #[must_use]
    pub const fn next_cursor(&self) -> Option<CursorToken> {
        self.next_cursor
    }
}

/// Symbolic RiffQL application surface.
pub trait SymbolicQueryApplication: Send + Sync {
    /// Describes the selected contract entirely with source names.
    fn describe_symbolic_contract(
        &self,
        context: RequestContext,
        contract: SymbolicContractSelector,
    ) -> ServiceFuture<'_, DescribeSymbolicContractResult>;

    /// Parses, resolves, type checks, and plans without executing.
    fn check_symbolic_query(
        &self,
        context: RequestContext,
        request: CompileSymbolicQueryRequest,
    ) -> ServiceFuture<'_, CheckSymbolicQueryResult>;

    /// Returns the same check plus a deterministic name-only plan.
    fn explain_symbolic_query(
        &self,
        context: RequestContext,
        request: CompileSymbolicQueryRequest,
    ) -> ServiceFuture<'_, ExplainSymbolicQueryResult>;

    /// Executes one ad-hoc checked query through one engine-owned snapshot.
    fn execute_symbolic_query(
        &self,
        context: RequestContext,
        request: ExecuteSymbolicQueryRequest,
    ) -> ServiceFuture<'_, ExecuteSymbolicQueryResult>;

    /// Compiles and atomically activates one immutable exact-contract module.
    fn deploy_query_module(
        &self,
        context: RequestContext,
        request: DeployQueryModuleRequest,
    ) -> ServiceFuture<'_, DeployQueryModuleResult>;

    /// Inspects an active or content-addressed module.
    fn get_query_module(
        &self,
        context: RequestContext,
        request: GetQueryModuleRequest,
    ) -> ServiceFuture<'_, Option<QueryModuleInspection>>;

    /// Explains one operation from an active or content-addressed module.
    fn explain_named_symbolic_query(
        &self,
        context: RequestContext,
        request: NamedSymbolicQueryRequest,
    ) -> ServiceFuture<'_, ExplainSymbolicQueryResult>;

    /// Executes one operation from an active or content-addressed module.
    fn execute_named_symbolic_query(
        &self,
        context: RequestContext,
        request: NamedSymbolicQueryRequest,
    ) -> ServiceFuture<'_, ExecuteSymbolicQueryResult>;
}

impl SymbolicQueryApplication for RiffDbService {
    fn describe_symbolic_contract(
        &self,
        context: RequestContext,
        contract: SymbolicContractSelector,
    ) -> ServiceFuture<'_, DescribeSymbolicContractResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(ServiceOperationV1::DescribeContract, ingress, async move {
            describe_contract(service, context, contract).await
        })
    }

    fn check_symbolic_query(
        &self,
        context: RequestContext,
        request: CompileSymbolicQueryRequest,
    ) -> ServiceFuture<'_, CheckSymbolicQueryResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(ServiceOperationV1::CheckQuery, ingress, async move {
            check_query(service, context, request).await
        })
    }

    fn explain_symbolic_query(
        &self,
        context: RequestContext,
        request: CompileSymbolicQueryRequest,
    ) -> ServiceFuture<'_, ExplainSymbolicQueryResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(ServiceOperationV1::ExplainQuery, ingress, async move {
            explain_query(service, context, request).await
        })
    }

    fn execute_symbolic_query(
        &self,
        context: RequestContext,
        request: ExecuteSymbolicQueryRequest,
    ) -> ServiceFuture<'_, ExecuteSymbolicQueryResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(ServiceOperationV1::ExecuteQuery, ingress, async move {
            execute_query(service, context, request).await
        })
    }

    fn deploy_query_module(
        &self,
        context: RequestContext,
        request: DeployQueryModuleRequest,
    ) -> ServiceFuture<'_, DeployQueryModuleResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(ServiceOperationV1::DeployQueryModule, ingress, async move {
            deploy_module(service, context, request).await
        })
    }

    fn get_query_module(
        &self,
        context: RequestContext,
        request: GetQueryModuleRequest,
    ) -> ServiceFuture<'_, Option<QueryModuleInspection>> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(ServiceOperationV1::ExplainQuery, ingress, async move {
            inspect_module(service, context, request).await
        })
    }

    fn explain_named_symbolic_query(
        &self,
        context: RequestContext,
        request: NamedSymbolicQueryRequest,
    ) -> ServiceFuture<'_, ExplainSymbolicQueryResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(ServiceOperationV1::ExplainQuery, ingress, async move {
            explain_named_query(service, context, request).await
        })
    }

    fn execute_named_symbolic_query(
        &self,
        context: RequestContext,
        request: NamedSymbolicQueryRequest,
    ) -> ServiceFuture<'_, ExecuteSymbolicQueryResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(ServiceOperationV1::ExecuteQuery, ingress, async move {
            execute_named_query(service, context, request).await
        })
    }
}

async fn describe_contract(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    selection: SymbolicContractSelector,
) -> ServiceResult<DescribeSymbolicContractResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::DescribeContract;
    let bundle =
        prepare_selected_contract(&service, &context, selection.selection(), OPERATION).await?;
    ensure_selected_hash(&selection, &bundle)?;
    let targets =
        ServiceAuditTargetMap::symbolic_query(bundle.lineage().clone(), bundle.contract_version())
            .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let begun = service
        .begin_invocation(
            &context,
            OperationRequest::describe_contract(),
            targets,
            AuditScope::StandardRead,
        )
        .await?;
    let catalog = match SymbolicCatalog::from_bundle(bundle.bundle()) {
        Ok(catalog) => catalog,
        Err(_) => {
            let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
    };
    let result = DescribeSymbolicContractResult {
        lineage: bundle.lineage().clone(),
        version: bundle.contract_version(),
        bundle_hash: bundle.bundle_hash(),
        catalog: render_catalog(&catalog, bundle.bundle().schema()),
    };
    begun.reauthorize(&service, &context).await?;
    finish_success(&service, &context, &begun).await?;
    Ok(result)
}

async fn check_query(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: CompileSymbolicQueryRequest,
) -> ServiceResult<CheckSymbolicQueryResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::CheckQuery;
    let bundle = prepare_selected_contract(
        &service,
        &context,
        request.contract().selection(),
        OPERATION,
    )
    .await?;
    ensure_selected_hash(request.contract(), &bundle)?;
    let begun = begin_symbolic(
        &service,
        &context,
        &bundle,
        OperationRequest::check_ad_hoc_query(),
        OPERATION,
    )
    .await?;
    let result = match compile(request.source().as_str(), bundle.bundle()) {
        Ok(program) => {
            CheckSymbolicQueryResult::Valid(Box::new(CheckedSymbolicQuery::from_program(&program)))
        }
        Err(diagnostics) => CheckSymbolicQueryResult::Invalid(diagnostics),
    };
    begun.reauthorize(&service, &context).await?;
    finish_success(&service, &context, &begun).await?;
    Ok(result)
}

async fn explain_query(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: CompileSymbolicQueryRequest,
) -> ServiceResult<ExplainSymbolicQueryResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::ExplainQuery;
    let bundle = prepare_selected_contract(
        &service,
        &context,
        request.contract().selection(),
        OPERATION,
    )
    .await?;
    ensure_selected_hash(request.contract(), &bundle)?;
    let begun = begin_symbolic(
        &service,
        &context,
        &bundle,
        OperationRequest::explain_ad_hoc_query(),
        OPERATION,
    )
    .await?;
    let result = match compile(request.source().as_str(), bundle.bundle()) {
        Ok(program) => ExplainSymbolicQueryResult::Valid {
            query: Box::new(CheckedSymbolicQuery::from_program(&program)),
            lines: program.explain().lines().to_vec(),
        },
        Err(diagnostics) => ExplainSymbolicQueryResult::Invalid(diagnostics),
    };
    begun.reauthorize(&service, &context).await?;
    finish_success(&service, &context, &begun).await?;
    Ok(result)
}

async fn deploy_module(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: DeployQueryModuleRequest,
) -> ServiceResult<DeployQueryModuleResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::DeployQueryModule;
    let bundle =
        prepare_selected_contract(&service, &context, request.contract.selection(), OPERATION)
            .await?;
    ensure_selected_hash(&request.contract, &bundle)?;
    let module = ValidatedQueryModule::compile(request.candidate, &bundle).map_err(|_| {
        application_validation_failure(
            ValidationCode::InvalidValue,
            ApplicationErrorCode::QueryInvalid,
        )
    })?;
    let descriptor = QueryModuleDescriptor::from_module(&module);
    let operation = OperationRequest::deploy_query_module(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
    );
    let targets =
        ServiceAuditTargetMap::symbolic_query(bundle.lineage().clone(), bundle.contract_version())
            .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let begun = service
        .begin_invocation(&context, operation, targets, AuditScope::Intrinsic)
        .await?;
    let permit = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service.executors.control_plane.reserve_capacity(),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        Ok(Err(_)) => {
            let failure = PublicError::storage_unavailable().into();
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
        Err(error) => {
            let failure = controlled_failure(error);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
    };
    let authorization = begun.reauthorize(&service, &context).await?;
    let authorization = match (*authorization).into_catalog_deployment(
        bundle.lineage(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        Some(bundle.contract_version()),
    ) {
        Ok(authorization) => authorization,
        Err(_) => {
            let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
    };
    let prepared = PreparedQueryModuleActivation::new(module, request.expectation.lower());
    let preparation = match QueryModuleDeploymentPreparation::new(
        context.request_id(),
        prepared,
        authorization,
    ) {
        Ok(preparation) => preparation,
        Err(_) => {
            let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
    };
    let receipt = match permit.submit_query_module_deployment(preparation) {
        Ok(receipt) => receipt,
        Err(_) => {
            let failure = PublicError::storage_unavailable().into();
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
    };
    let result = match receipt.completion().await {
        Ok(result) => result,
        Err(error) => {
            let phase = if error.kind() == ControlPlaneExecutionErrorKind::OutcomeUnknown {
                ServiceAuditPhaseV1::OutcomeUncertain
            } else {
                ServiceAuditPhaseV1::Failed
            };
            let _ = begun
                .finish(&service, &context, phase, ServiceAuditLinkV1::None)
                .await;
            return Err(match error.kind() {
                ControlPlaneExecutionErrorKind::OutcomeUnknown => {
                    PublicError::outcome_unknown().into()
                }
                ControlPlaneExecutionErrorKind::AuthorizationDenied => {
                    PublicError::authorization_denied().into()
                }
                ControlPlaneExecutionErrorKind::StorageUnavailable
                | ControlPlaneExecutionErrorKind::CoordinatorStopped
                | ControlPlaneExecutionErrorKind::CoordinatorFenced => {
                    PublicError::storage_unavailable().into()
                }
                ControlPlaneExecutionErrorKind::InternalDefect => {
                    service.internal_failure(OPERATION, InternalDefect::ProofMismatch)
                }
            });
        }
    };
    let terminal = result.terminal_audit();
    let outcome = match result.into_outcome() {
        CoordinatorQueryModuleDeploymentOutcome::Activated(_) => {
            QueryModuleDeploymentDisposition::Activated
        }
        CoordinatorQueryModuleDeploymentOutcome::AlreadyActive(_) => {
            QueryModuleDeploymentDisposition::AlreadyActive
        }
        CoordinatorQueryModuleDeploymentOutcome::ExpectedActiveMismatch { actual } => {
            QueryModuleDeploymentDisposition::ExpectedActiveMismatch { actual }
        }
        CoordinatorQueryModuleDeploymentOutcome::ModuleVersionConflict => {
            QueryModuleDeploymentDisposition::ModuleVersionConflict
        }
        CoordinatorQueryModuleDeploymentOutcome::ContractUnavailable => {
            QueryModuleDeploymentDisposition::ContractUnavailable
        }
    };
    let (phase, link) = match terminal {
        ControlPlaneTerminalAudit::Succeeded(link) => (ServiceAuditPhaseV1::Succeeded, link),
        ControlPlaneTerminalAudit::Failed => {
            (ServiceAuditPhaseV1::Failed, ServiceAuditLinkV1::None)
        }
    };
    begun
        .finish(&service, &context, phase, link)
        .await
        .map_err(|_| -> ServiceFailure {
            if phase == ServiceAuditPhaseV1::Succeeded {
                PublicError::outcome_unknown().into()
            } else {
                PublicError::storage_unavailable().into()
            }
        })?;
    Ok(DeployQueryModuleResult {
        outcome,
        module: descriptor,
    })
}

async fn inspect_module(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: GetQueryModuleRequest,
) -> ServiceResult<Option<QueryModuleInspection>> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::ExplainQuery;
    let bundle =
        prepare_selected_contract(&service, &context, request.contract.selection(), OPERATION)
            .await?;
    ensure_selected_hash(&request.contract, &bundle)?;
    let begun = begin_symbolic(
        &service,
        &context,
        &bundle,
        OperationRequest::describe_contract(),
        OPERATION,
    )
    .await?;
    let module =
        match load_query_module(&service, &context, bundle, request.module_hash, OPERATION).await {
            Ok(module) => module,
            Err(failure) => return Err(finish_failure(&service, &context, &begun, failure).await),
        };
    let result = module
        .as_ref()
        .map(QueryModuleInspection::from_module)
        .transpose()
        .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    begun.reauthorize(&service, &context).await?;
    finish_success(&service, &context, &begun).await?;
    Ok(result)
}

async fn explain_named_query(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: NamedSymbolicQueryRequest,
) -> ServiceResult<ExplainSymbolicQueryResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::ExplainQuery;
    let bundle =
        prepare_selected_contract(&service, &context, request.contract.selection(), OPERATION)
            .await?;
    ensure_selected_hash(&request.contract, &bundle)?;
    let query_name = QueryOperationName::new(request.query_name.clone())
        .map_err(|_| validation_failure(ValidationCode::InvalidValue))?;
    let module = load_query_module(
        &service,
        &context,
        bundle.clone(),
        request.module_hash,
        OPERATION,
    )
    .await?
    .ok_or_else(|| {
        application_validation_failure(
            ValidationCode::InvalidValue,
            ApplicationErrorCode::ModuleUnavailable,
        )
    })?;
    let module_hash = module.identity();
    let begun = begin_symbolic(
        &service,
        &context,
        &bundle,
        OperationRequest::explain_named_query(bundle.lineage().clone(), module_hash, query_name),
        OPERATION,
    )
    .await?;
    let Some(query) = module.module().query(&request.query_name) else {
        let failure = application_validation_failure(
            ValidationCode::InvalidValue,
            ApplicationErrorCode::QueryUnavailable,
        );
        return Err(finish_failure(&service, &context, &begun, failure).await);
    };
    let result = ExplainSymbolicQueryResult::Valid {
        query: Box::new(CheckedSymbolicQuery::from_named(
            query.program(),
            module.identity(),
        )),
        lines: query.program().explain().lines().to_vec(),
    };
    begun.reauthorize(&service, &context).await?;
    finish_success(&service, &context, &begun).await?;
    Ok(result)
}

async fn execute_named_query(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: NamedSymbolicQueryRequest,
) -> ServiceResult<ExecuteSymbolicQueryResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::ExecuteQuery;
    let bundle =
        prepare_selected_contract(&service, &context, request.contract.selection(), OPERATION)
            .await?;
    ensure_selected_hash(&request.contract, &bundle)?;
    let query_name = QueryOperationName::new(request.query_name.clone())
        .map_err(|_| validation_failure(ValidationCode::InvalidValue))?;
    let module = load_query_module(
        &service,
        &context,
        bundle.clone(),
        request.module_hash,
        OPERATION,
    )
    .await?
    .ok_or_else(|| {
        application_validation_failure(
            ValidationCode::InvalidValue,
            ApplicationErrorCode::ModuleUnavailable,
        )
    })?;
    let module_hash = module.identity();
    let query = module.module().query(&request.query_name).ok_or_else(|| {
        application_validation_failure(
            ValidationCode::InvalidValue,
            ApplicationErrorCode::QueryUnavailable,
        )
    })?;
    execute_compiled_query(
        service,
        context,
        bundle,
        query.shared_program(),
        query.shared_document(),
        Some(module.identity()),
        QueryAuthority::Named {
            module_hash,
            query_name,
        },
        request.parameters,
        request.cursor,
        request.minimum_application_head,
    )
    .await
}

async fn load_query_module(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    contract: riffdb_catalog::ValidatedContractBundle,
    module_hash: Option<QueryModuleHash>,
    operation: ServiceOperationV1,
) -> ServiceResult<Option<ValidatedQueryModule>> {
    let Some(modules) = service.providers.query_modules.as_ref() else {
        return Err(PublicError::storage_unavailable().into());
    };
    let future = match module_hash {
        Some(module_hash) => modules.prepare_query_module(context.control(), contract, module_hash),
        None => modules.prepare_active_query_module(context.control(), contract),
    };
    match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        future,
    )
    .await
    {
        Ok(Ok(module)) => Ok(module),
        Ok(Err(crate::QueryModuleReadError::Unavailable)) => {
            Err(PublicError::storage_unavailable().into())
        }
        Ok(Err(crate::QueryModuleReadError::Integrity)) => {
            Err(service.internal_failure(operation, InternalDefect::ProofMismatch))
        }
        Err(error) => Err(controlled_failure(error)),
    }
}

fn controlled_failure(error: ControlledWaitError) -> ServiceFailure {
    match error {
        ControlledWaitError::Cancelled => ServiceFailure::Cancelled,
        ControlledWaitError::DeadlineExceeded => ServiceFailure::DeadlineExceeded,
    }
}

async fn execute_query(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: ExecuteSymbolicQueryRequest,
) -> ServiceResult<ExecuteSymbolicQueryResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::ExecuteQuery;
    let bundle = prepare_selected_contract(
        &service,
        &context,
        request.contract().selection(),
        OPERATION,
    )
    .await?;
    ensure_selected_hash(request.contract(), &bundle)?;
    let compiled = compile_parts(request.source().as_str(), bundle.bundle()).map_err(|_| {
        application_validation_failure(
            ValidationCode::InvalidValue,
            ApplicationErrorCode::QueryInvalid,
        )
    })?;
    execute_compiled_query(
        service,
        context,
        bundle,
        Arc::new(compiled.program),
        Arc::new(compiled.document),
        None,
        QueryAuthority::AdHoc,
        request.parameters,
        request.cursor,
        request.minimum_application_head,
    )
    .await
}

enum QueryAuthority {
    AdHoc,
    Named {
        module_hash: QueryModuleHash,
        query_name: QueryOperationName,
    },
}

#[allow(clippy::too_many_arguments)]
async fn execute_compiled_query(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    bundle: riffdb_catalog::ValidatedContractBundle,
    program: Arc<QueryAccessProgramV1>,
    document: Arc<Document>,
    module_hash: Option<QueryModuleHash>,
    authority: QueryAuthority,
    submitted: SymbolicQueryParameters,
    cursor: Option<CursorToken>,
    minimum_application_head: Option<u64>,
) -> ServiceResult<ExecuteSymbolicQueryResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::ExecuteQuery;
    if program.steps().len() > MAX_SYMBOLIC_QUERY_STEPS {
        return Err(validation_failure(ValidationCode::InvalidValue));
    }
    let parameters =
        materialize_query_parameters(&service, OPERATION, bundle.bundle(), &document, &submitted)?;
    let parameter_hash = query_parameter_hash(&parameters)
        .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))?;
    let target =
        application_query_target(bundle.bundle(), &program, &parameters, context.ingress())
            .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))?;
    let operation_request = match authority {
        QueryAuthority::AdHoc => OperationRequest::execute_ad_hoc_query(target),
        QueryAuthority::Named {
            module_hash,
            query_name,
        } => OperationRequest::execute_named_query(
            bundle.lineage().clone(),
            module_hash,
            query_name,
            target,
        )
        .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?,
    };
    let cursor_lookup = QueryCursorLookup::new(
        CursorContractIdentity::new(
            program.contract().lineage().clone(),
            program.contract().version(),
            program.contract().bundle_hash(),
        ),
        module_hash,
        program.identity().hash(),
        parameter_hash,
        context.principal().capability_id(),
        context.principal().capability_revision(),
    );
    let begun = begin_symbolic(&service, &context, &bundle, operation_request, OPERATION).await?;
    let prior = match cursor {
        Some(token) => match service.cursors.resolve_query(
            token,
            context.principal().principal_id(),
            &cursor_lookup,
        ) {
            Ok(state) => Some(state),
            Err(CursorAccessError::InvalidCursor) => {
                let failure = application_validation_failure(
                    ValidationCode::InvalidValue,
                    ApplicationErrorCode::CursorInvalid,
                );
                return Err(finish_failure(&service, &context, &begun, failure).await);
            }
            Err(CursorAccessError::Unavailable) => {
                // Cursor resolve unavailability is retryable only when it arises
                // from transient registry/clock state; resolve before the retry
                // loop so a stale/invalid token is never re-run.
                let failure = PublicError::storage_unavailable().into();
                return Err(finish_failure(&service, &context, &begun, failure).await);
            }
        },
        None => None,
    };
    let Some(executor) = service.providers.query_executor.as_ref() else {
        let failure = PublicError::storage_unavailable().into();
        return Err(finish_failure(&service, &context, &begun, failure).await);
    };
    let execution_authorization = begun
        .reauthorize(&service, &context)
        .await?
        .into_application_query()
        .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;

    // Query execution and continuation registration retry together; audit
    // begin/finish stay outside the loop.
    let (snapshot, cursor_guard) = match crate::read_retry::with_read_retry(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service.providers.telemetry.as_ref(),
        OPERATION,
        |_attempt| {
            let execution_authorization = &execution_authorization;
            let program = &program;
            let parameters = &parameters;
            let prior_cont = prior.as_deref().map(QueryCursorState::continuation);
            let cursor_lookup = cursor_lookup.clone();
            let principal = context.principal().principal_id().clone();
            let executor = executor.as_ref();
            let cursors = &service.cursors;
            let service = &service;
            let telemetry = service.providers.telemetry.as_ref();
            async move {
                let snapshot = match execute_authorized_query_page(
                    execution_authorization,
                    executor,
                    program,
                    parameters,
                    prior_cont,
                ) {
                    Ok(snapshot) => snapshot,
                    Err(QueryExecutionError::BackendUnavailable) => {
                        return Err(Ok(
                            crate::read_retry::RetryableReadFault::BackendUnavailable,
                        ));
                    }
                    Err(error) => {
                        return Err(Err(execution_failure(service, OPERATION, error)));
                    }
                };
                let continuation = match (snapshot.continuation_binding(), snapshot.continuation())
                {
                    (Some(binding), Some(lower)) => QueryContinuation::checked(
                        binding.to_owned(),
                        lower.to_vec(),
                        snapshot.index_epochs().clone(),
                    ),
                    (None, None) => None,
                    _ => {
                        return Err(Err(
                            service.internal_failure(OPERATION, InternalDefect::ProofMismatch)
                        ));
                    }
                };
                let cursor_guard = match continuation {
                    Some(continuation) => {
                        match cursors.register_query_unpublished(
                            &principal,
                            cursor_lookup,
                            QueryCursorState::new(continuation),
                        ) {
                            Ok(guard) => {
                                if guard.capacity_evicted() {
                                    telemetry.record(crate::ServiceTelemetryEvent::CursorEvicted);
                                }
                                Some(guard)
                            }
                            Err(_) => {
                                return Err(Ok(
                                    crate::read_retry::RetryableReadFault::CursorUnavailable,
                                ));
                            }
                        }
                    }
                    None => None,
                };
                Ok((snapshot, cursor_guard))
            }
        },
    )
    .await
    {
        Ok(value) => value,
        Err(failure) => return Err(finish_failure(&service, &context, &begun, failure).await),
    };
    if minimum_application_head.is_some_and(|minimum| snapshot.application_head() < minimum) {
        let failure = PublicError::concurrency_deadline_exceeded().into();
        return Err(finish_failure(&service, &context, &begun, failure).await);
    }
    begun.reauthorize(&service, &context).await?;
    let mut result =
        ExecuteSymbolicQueryResult::from_snapshot(&program, &snapshot, bundle.bundle());
    if let Some(module_hash) = module_hash {
        result.identity = SymbolicQueryIdentity::from_named(&program, module_hash);
    }
    finish_success(&service, &context, &begun).await?;
    result.next_cursor = cursor_guard.map(crate::CursorPublicationGuard::publish);
    Ok(result)
}

async fn begin_symbolic(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    bundle: &riffdb_catalog::ValidatedContractBundle,
    request: OperationRequest,
    operation: ServiceOperationV1,
) -> ServiceResult<crate::orchestration::BegunInvocation> {
    let targets =
        ServiceAuditTargetMap::symbolic_query(bundle.lineage().clone(), bundle.contract_version())
            .map_err(|_| service.internal_failure(operation, InternalDefect::ProofMismatch))?;
    service
        .begin_invocation(context, request, targets, AuditScope::StandardRead)
        .await
}

fn ensure_selected_hash(
    selection: &SymbolicContractSelector,
    bundle: &riffdb_catalog::ValidatedContractBundle,
) -> ServiceResult<()> {
    if selection.matches(bundle) {
        Ok(())
    } else {
        Err(PublicError::contract_mismatch(bundle.contract_version()).into())
    }
}

fn compile(
    source: &str,
    bundle: &riffdb_contract_ir::ContractBundle,
) -> Result<QueryAccessProgramV1, Vec<SymbolicDiagnostic>> {
    compile_parts(source, bundle).map(|compiled| compiled.program)
}

struct CompiledQuery {
    program: QueryAccessProgramV1,
    document: Document,
}

fn compile_parts(
    source: &str,
    bundle: &riffdb_contract_ir::ContractBundle,
) -> Result<CompiledQuery, Vec<SymbolicDiagnostic>> {
    let document = parse_query(source).map_err(|diagnostics| {
        diagnostics
            .as_slice()
            .iter()
            .map(parse_diagnostic)
            .collect::<Vec<_>>()
    })?;
    let catalog = SymbolicCatalog::from_bundle(bundle).map_err(|diagnostics| {
        diagnostics
            .as_slice()
            .iter()
            .map(query_diagnostic)
            .collect::<Vec<_>>()
    })?;
    resolve_query_surface(&document, &catalog).map_err(|diagnostics| {
        diagnostics
            .as_slice()
            .iter()
            .map(query_diagnostic)
            .collect::<Vec<_>>()
    })?;
    let program = compile_query(&document, &catalog).map_err(|diagnostics| {
        diagnostics
            .as_slice()
            .iter()
            .map(planner_diagnostic)
            .collect::<Vec<_>>()
    })?;
    Ok(CompiledQuery { program, document })
}

fn materialize_query_parameters(
    service: &RiffDbServiceInner,
    operation: ServiceOperationV1,
    bundle: &riffdb_contract_ir::ContractBundle,
    document: &Document,
    submitted: &SymbolicQueryParameters,
) -> ServiceResult<QueryParameters> {
    if submitted.iter().any(|(name, _)| {
        !document
            .parameters
            .iter()
            .any(|parameter| parameter.name.value.as_str() == name)
    }) {
        return Err(validation_failure(ValidationCode::InvalidValue));
    }
    let mut canonical = BTreeMap::new();
    for parameter in &document.parameters {
        let name = parameter.name.value.as_str();
        let value = submitted.get(name);
        match (&parameter.ty.value, value) {
            (TypeReference::Cursor, None) => continue,
            (TypeReference::Cursor, Some(_)) => {
                return Err(validation_failure(ValidationCode::InvalidValue));
            }
            (TypeReference::Optional(inner), None)
                if matches!(inner.value, TypeReference::Cursor) =>
            {
                continue;
            }
            (TypeReference::Optional(inner), Some(SubmittedValue::Null))
                if matches!(inner.value, TypeReference::Cursor) =>
            {
                continue;
            }
            (TypeReference::Optional(inner), Some(_))
                if matches!(inner.value, TypeReference::Cursor) =>
            {
                return Err(validation_failure(ValidationCode::InvalidValue));
            }
            (TypeReference::Limit, None) if parameter.default.is_some() => continue,
            (_, None) => return Err(validation_failure(ValidationCode::InvalidValue)),
            (TypeReference::Limit, Some(SubmittedValue::U64(value)))
                if (1..=500).contains(value) =>
            {
                canonical.insert(name.to_owned(), CanonicalValue::U64(*value));
            }
            (TypeReference::Limit, Some(_)) => {
                return Err(validation_failure(ValidationCode::TypeMismatch));
            }
            (TypeReference::Set(inner), Some(SubmittedValue::List(values))) => {
                let value_type = query_value_type(bundle, &inner.value).ok_or_else(|| {
                    service.internal_failure(operation, InternalDefect::ProofMismatch)
                })?;
                let values = values
                    .values()
                    .iter()
                    .map(|value| {
                        materialize_natural_query_value(
                            bundle,
                            &value_type,
                            value,
                            service,
                            operation,
                        )
                    })
                    .collect::<ServiceResult<Vec<_>>>()?;
                let value = CanonicalValue::list(values)
                    .map_err(|_| validation_failure(ValidationCode::InvalidValue))?;
                canonical.insert(name.to_owned(), value);
            }
            (TypeReference::Set(_), Some(_)) => {
                return Err(validation_failure(ValidationCode::TypeMismatch));
            }
            (ty, Some(value)) => {
                let value_type = query_value_type(bundle, ty).ok_or_else(|| {
                    service.internal_failure(operation, InternalDefect::ProofMismatch)
                })?;
                let value = materialize_natural_query_value(
                    bundle,
                    &value_type,
                    value,
                    service,
                    operation,
                )?;
                canonical.insert(name.to_owned(), value);
            }
        }
    }
    QueryParameters::checked(canonical)
        .ok_or_else(|| service.internal_failure(operation, InternalDefect::ProofMismatch))
}

fn materialize_natural_query_value(
    bundle: &riffdb_contract_ir::ContractBundle,
    value_type: &ValueType,
    submitted: &SubmittedValue,
    service: &RiffDbServiceInner,
    operation: ServiceOperationV1,
) -> ServiceResult<CanonicalValue> {
    let coerced = coerce_natural_query_value(value_type, submitted).map_err(validation_failure)?;
    materialize_submitted_value(bundle.schema(), value_type, &coerced, Vec::new())
        .map_err(|error| materialization_failure(service, operation, error))
}

fn coerce_natural_query_value(
    value_type: &ValueType,
    submitted: &SubmittedValue,
) -> Result<SubmittedValue, ValidationCode> {
    if let Some(inner) = value_type.optional_inner() {
        if matches!(submitted, SubmittedValue::Null) {
            return Ok(SubmittedValue::Null);
        }
        return coerce_natural_query_value(inner, submitted);
    }
    match (value_type.tag(), submitted) {
        (ValueTypeTag::Enum, SubmittedValue::String(value)) => {
            let name = SourceName::new(value.as_str().to_owned())
                .map_err(|_| ValidationCode::InvalidValue)?;
            Ok(SubmittedValue::Enum(SubmittedEnum::name_only(name)))
        }
        (ValueTypeTag::Uuid, SubmittedValue::String(value)) => {
            { crate::command_operations::parse_natural_uuid(value.as_str()) }
                .map(SubmittedValue::Uuid)
                .ok_or(ValidationCode::InvalidValue)
        }
        (ValueTypeTag::I64, SubmittedValue::U64(value)) => i64::try_from(*value)
            .map(SubmittedValue::I64)
            .map_err(|_| ValidationCode::InvalidValue),
        (ValueTypeTag::U64, SubmittedValue::I64(value)) => u64::try_from(*value)
            .map(SubmittedValue::U64)
            .map_err(|_| ValidationCode::InvalidValue),
        _ => Ok(submitted.clone()),
    }
}

fn query_value_type(
    bundle: &riffdb_contract_ir::ContractBundle,
    ty: &TypeReference,
) -> Option<riffdb_contract_ir::ValueType> {
    match ty {
        TypeReference::Named(path) => match path.0.as_slice() {
            [enumeration] => bundle
                .schema()
                .enums()
                .iter()
                .find(|candidate| candidate.name() == enumeration.value.as_str())
                .map(|enumeration| riffdb_contract_ir::ValueType::enumeration(enumeration.id())),
            [entity, field] => bundle
                .schema()
                .entities()
                .iter()
                .find(|candidate| candidate.name() == entity.value.as_str())
                .and_then(|entity| {
                    entity
                        .record()
                        .fields()
                        .iter()
                        .find(|candidate| candidate.name() == field.value.as_str())
                })
                .map(|field| field.value_type().clone()),
            _ => None,
        },
        TypeReference::Optional(inner) => {
            riffdb_contract_ir::ValueType::optional(query_value_type(bundle, &inner.value)?).ok()
        }
        TypeReference::Set(_) | TypeReference::Cursor | TypeReference::Limit => None,
    }
}

fn materialization_failure(
    service: &RiffDbServiceInner,
    operation: ServiceOperationV1,
    error: SubmittedValueMaterializationError,
) -> ServiceFailure {
    match error {
        SubmittedValueMaterializationError::Public(error) => error.into(),
        SubmittedValueMaterializationError::Integrity => {
            service.internal_failure(operation, InternalDefect::ProofMismatch)
        }
    }
}

fn query_parameter_hash(parameters: &QueryParameters) -> Option<riffdb_types::QueryParameterHash> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"RDBQPARAM\x01");
    bytes.extend_from_slice(&u32::try_from(parameters.iter().len()).ok()?.to_be_bytes());
    for (name, value) in parameters.iter() {
        let name = name.as_bytes();
        bytes.extend_from_slice(&u32::try_from(name.len()).ok()?.to_be_bytes());
        bytes.extend_from_slice(name);
        let value = encode_canonical_value(value).ok()?;
        bytes.extend_from_slice(&u32::try_from(value.len()).ok()?.to_be_bytes());
        bytes.extend_from_slice(&value);
    }
    Some(hash_query_parameters(&bytes))
}

fn application_query_target(
    bundle: &riffdb_contract_ir::ContractBundle,
    program: &QueryAccessProgramV1,
    parameters: &QueryParameters,
    ingress: riffdb_types::ServiceIngressKindV1,
) -> Option<ApplicationQueryTarget> {
    let partition_value = parameters.get(program.partition_parameter())?;
    let mut routed_partition = None;
    let mut accesses = Vec::with_capacity(program.steps().len());
    for step in program.steps() {
        let entity = program
            .authorization()
            .iter()
            .find(|access| access.entity() == step.entity())?;
        let contract_entity = bundle.schema().entity(step.internal_entity_id())?;
        let mut non_key_fields = entity
            .internal_fields()
            .filter_map(|(_, field)| {
                (!contract_entity.primary_key_fields().contains(&field)).then_some(field)
            })
            .collect::<Vec<_>>();
        non_key_fields.sort_unstable();
        non_key_fields.dedup();
        let partition = {
            let aggregate = bundle
                .schema()
                .aggregate_for_entity(step.internal_entity_id())?;
            let Ok(partition) = aggregate
                .keys()
                .partition_schema()
                .encode_partition(std::slice::from_ref(partition_value))
            else {
                return None;
            };
            partition
        };
        // PartitionKey retains the aggregate owner as a namespace. A
        // compiler-proved composite query may cross aggregate owners while
        // every step is still routed by this exact canonical parameter. Keep
        // the first key as the authorization route anchor; successful encoding
        // through every aggregate schema proves the shared typed route value.
        if routed_partition.is_none() {
            routed_partition = Some(partition);
        }
        let Ok(rows) = u16::try_from(step.maximum_rows()) else {
            return None;
        };
        let rows = NonZeroU16::new(rows)?;
        let index_id = match step.access() {
            QueryAccessKind::Index { .. } => Some(step.internal_index_id()?),
            QueryAccessKind::Point { .. } | QueryAccessKind::DependentPointBatch { .. } => None,
        };
        accesses.push(
            ApplicationQueryAccessRequirement::new(
                step.internal_entity_id(),
                index_id,
                non_key_fields,
                rows,
            )
            .ok()?,
        );
    }
    ApplicationQueryTarget::new(
        program.contract().lineage().clone(),
        program.contract().version(),
        program.contract().bundle_hash(),
        program.identity().hash(),
        ingress,
        OperationTenantScope::global_only(),
        routed_partition?,
        accesses,
        program.cost(),
    )
    .ok()
}

fn execute_authorized_query_page(
    authorization: &AuthorizedApplicationQuery,
    executor: &dyn riffdb_query_executor::QueryExecutionPort,
    program: &QueryAccessProgramV1,
    parameters: &QueryParameters,
    prior: Option<&QueryContinuation>,
) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
    let target = authorization.target();
    let obligations = authorization.obligations();
    let exact_target = target.lineage() == program.contract().lineage()
        && target.version() == program.contract().version()
        && target.bundle_hash() == program.contract().bundle_hash()
        && target.plan_hash() == program.identity().hash()
        && target.cost() == program.cost()
        && target.accesses().len() == program.steps().len()
        && obligations.output_classification()
            == OutputClassification::PolicyFilteredApplicationData
        && obligations.partition_constraint()
            == Some(&PartitionConstraint::Exact(target.partition().clone()));
    if !exact_target {
        return Err(QueryExecutionError::InvalidProgram);
    }
    executor.execute_query_page(program, parameters, prior)
}

fn execution_failure(
    service: &RiffDbServiceInner,
    operation: ServiceOperationV1,
    error: QueryExecutionError,
) -> ServiceFailure {
    match error {
        QueryExecutionError::MissingParameter { .. }
        | QueryExecutionError::InvalidParameter { .. } => {
            validation_failure(ValidationCode::InvalidValue)
        }
        QueryExecutionError::StaleCursor | QueryExecutionError::InvalidContinuation => {
            application_validation_failure(
                ValidationCode::InvalidValue,
                ApplicationErrorCode::CursorInvalid,
            )
        }
        QueryExecutionError::BackendUnavailable => PublicError::storage_unavailable().into(),
        QueryExecutionError::BackendIntegrity => {
            service.internal_failure(operation, InternalDefect::LowerIntegrity)
        }
        QueryExecutionError::BackendLimitExceeded
        | QueryExecutionError::BoundExceeded
        | QueryExecutionError::FuelExhausted => ServiceFailure::ResponseTooLarge,
        QueryExecutionError::MissingField { .. }
        | QueryExecutionError::InvalidDependentKey { .. }
        | QueryExecutionError::InvalidProgram
        | QueryExecutionError::UnexpectedCardinality { .. }
        | QueryExecutionError::UnsupportedPredicate => {
            service.internal_failure(operation, InternalDefect::ProofMismatch)
        }
    }
}

fn validation_failure(code: ValidationCode) -> ServiceFailure {
    PublicError::validation(ValidationIssues::one(ValidationIssue::new(
        code,
        ValidationPath::root(),
    )))
    .into()
}

fn application_validation_failure(
    code: ValidationCode,
    application_code: ApplicationErrorCode,
) -> ServiceFailure {
    let error = PublicError::validation(ValidationIssues::one(ValidationIssue::new(
        code,
        ValidationPath::root(),
    )));
    match error.with_application_code_hint(application_code) {
        Ok(error) => error.into(),
        Err(_) => ServiceFailure::from(PublicError::validation(ValidationIssues::one(
            ValidationIssue::new(ValidationCode::InvalidValue, ValidationPath::root()),
        ))),
    }
}

fn parse_diagnostic(diagnostic: &ParseDiagnostic) -> SymbolicDiagnostic {
    SymbolicDiagnostic {
        code: diagnostic.code().as_str().to_owned(),
        summary: diagnostic.summary().to_owned(),
        span: diagnostic.span(),
        symbols: Vec::new(),
        suggestion: diagnostic.help().map(str::to_owned),
    }
}

fn query_diagnostic(diagnostic: &QueryDiagnostic) -> SymbolicDiagnostic {
    SymbolicDiagnostic {
        code: diagnostic.code().as_str().to_owned(),
        summary: diagnostic.summary().to_owned(),
        span: diagnostic.primary(),
        symbols: diagnostic.symbol_path().to_vec(),
        suggestion: diagnostic.help().map(str::to_owned),
    }
}

fn planner_diagnostic(diagnostic: &PlannerDiagnostic) -> SymbolicDiagnostic {
    SymbolicDiagnostic {
        code: diagnostic.code().as_str().to_owned(),
        summary: diagnostic.summary().to_owned(),
        span: diagnostic.primary(),
        symbols: diagnostic.symbol_path().to_vec(),
        suggestion: diagnostic.suggested_index().map(str::to_owned),
    }
}

fn render_named_type(value: &NamedTypeSchema) -> String {
    match value {
        NamedTypeSchema::Scalar(name) => name.clone(),
        NamedTypeSchema::Optional(inner) => format!("{}?", render_named_type(inner)),
        NamedTypeSchema::Set(inner) => format!("Set<{}>", render_named_type(inner)),
        NamedTypeSchema::Record(fields) => {
            let fields = fields
                .iter()
                .map(|field| {
                    format!(
                        "{}: {}",
                        field.name(),
                        render_named_type(field.value_type())
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("{{ {fields} }}")
        }
        NamedTypeSchema::List { element, maximum } => {
            format!("[{}; {maximum:?}]", render_named_type(element))
        }
        NamedTypeSchema::Cursor => "Cursor".to_owned(),
        NamedTypeSchema::Limit => "Limit".to_owned(),
    }
}

fn render_catalog(catalog: &SymbolicCatalog, schema: &riffdb_contract_ir::SchemaIr) -> String {
    let mut lines = Vec::new();
    for enumeration in catalog.enums() {
        lines.push(format!(
            "enum {} {{ {} }}",
            enumeration.name(),
            enumeration.variants().collect::<Vec<_>>().join(", ")
        ));
    }
    for event in schema.events() {
        lines.push(format!("event {} {{", event.name()));
        for field in event.payload().fields() {
            lines.push(format!(
                "  {}: {}",
                field.name(),
                render_contract_type(catalog, field.value_type())
            ));
        }
        match event.partition() {
            Some(partition) => {
                let fields = partition
                    .fields()
                    .iter()
                    .map(|field_id| {
                        event
                            .payload()
                            .field(*field_id)
                            .map(riffdb_contract_ir::FieldSchema::name)
                            .unwrap_or("<invalid-field>")
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                lines.push(format!("  partition by ({fields})"));
            }
            None => lines.push("  application stream unavailable".to_owned()),
        }
        lines.push("}".to_owned());
    }
    for entity in catalog.entities() {
        lines.push(format!("entity {} {{", entity.name()));
        for field in entity.fields() {
            let key = if field.is_key() { " key" } else { "" };
            lines.push(format!(
                "  {}: {}{key}",
                field.name(),
                render_contract_type(catalog, field.value_type())
            ));
        }
        lines.push(format!("  partition by {}", entity.partition_field()));
        for index in entity.indexes() {
            lines.push(format!(
                "  index {}({})",
                index.name(),
                index.fields().join(", ")
            ));
        }
        lines.push("}".to_owned());
    }
    lines.join("\n")
}

fn render_contract_type(
    catalog: &SymbolicCatalog,
    value: &riffdb_contract_ir::ValueType,
) -> String {
    use riffdb_contract_ir::ValueTypeTag;

    match value.tag() {
        ValueTypeTag::Bool => "bool".to_owned(),
        ValueTypeTag::I64 => "i64".to_owned(),
        ValueTypeTag::U64 => "u64".to_owned(),
        ValueTypeTag::Decimal => {
            let spec = value.decimal_spec().expect("tag checked");
            format!("decimal<{},{}>", spec.precision(), spec.scale())
        }
        ValueTypeTag::Money => {
            format!("money<{}>", value.currency().expect("tag checked"))
        }
        ValueTypeTag::String => {
            format!("string<{}>", value.byte_bound().expect("tag checked"))
        }
        ValueTypeTag::Bytes => {
            format!("bytes<{}>", value.byte_bound().expect("tag checked"))
        }
        ValueTypeTag::Timestamp => "timestamp".to_owned(),
        ValueTypeTag::Date => "date".to_owned(),
        ValueTypeTag::Uuid => "uuid".to_owned(),
        ValueTypeTag::Enum => catalog
            .enums()
            .find(|enumeration| {
                enumeration.internal_id() == value.enum_type_id().expect("tag checked")
            })
            .map(|enumeration| enumeration.name().to_owned())
            .unwrap_or_else(|| "<invalid-enum>".to_owned()),
        ValueTypeTag::Optional => format!(
            "{}?",
            render_contract_type(catalog, value.optional_inner().expect("tag checked"))
        ),
        ValueTypeTag::List => {
            let (element, maximum) = value.list_parts().expect("tag checked");
            format!("[{}; {maximum}]", render_contract_type(catalog, element))
        }
        ValueTypeTag::Record => "record".to_owned(),
    }
}

#[cfg(test)]
mod event_catalog_tests {
    use super::*;

    #[test]
    fn symbolic_catalog_names_streamable_and_internal_events_without_numeric_ids() {
        let source = r#"
contract EventCatalog version 1 {
  entity Row {
    key (id: uuid)
    field value: i64
  }
  event Routed {
    partition_by (id)
    id: uuid
    note: optional<string<32>>
  }
  event InternalOnly {
    id: uuid
  }
  aggregate Rows {
    root Row
    partition_by id
    conflict_key (id)
  }
  command Change {
    input idempotency_key: string<128>
    input id: uuid
    idempotency_key idempotency_key
    mutate Row(id) as row else Missing { id: id }
    set row.value = 1
    emit Routed { id: id }
    return Changed { row: row }
  }
}
"#;
        let bundle = riffdb_contract_compiler::compile_contract_source(source)
            .expect("event catalog contract");
        let catalog = SymbolicCatalog::from_bundle(&bundle).expect("symbolic catalog");
        let rendered = render_catalog(&catalog, bundle.schema());

        assert!(
            rendered.contains(
                "event Routed {\n  id: uuid\n  note: string<32>?\n  partition by (id)\n}"
            )
        );
        assert!(
            rendered
                .contains("event InternalOnly {\n  id: uuid\n  application stream unavailable\n}")
        );
        assert!(!rendered.contains("field_id"));
        assert!(!rendered.contains("event_type_id"));
    }
}
